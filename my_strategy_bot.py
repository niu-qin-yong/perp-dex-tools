"""
Modular Trading Bot - Supports multiple exchanges
"""

from operator import truediv
import os
import time
import asyncio
import traceback
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional
from collections import deque

from pydantic import config

from exchanges import ExchangeFactory
from helpers import TradingLogger
from helpers.lark_bot import LarkBot
from helpers.telegram_bot import TelegramBot
from exchanges.base import BaseExchangeClient, OrderResult, OrderInfo, query_retry


@dataclass
class MyStrategyConfig:
    """Configuration class for trading parameters."""
    ticker: str
    contract_id: str
    quantity: Decimal
    take_profit: Decimal
    stop_loss: Decimal
    stop_loss_immediate: Decimal
    tick_size: Decimal
    direction: str
    max_orders: int
    wait_time: int
    exchange: str
    grid_step: Decimal
    stop_price: Decimal
    pause_price: Decimal
    boost_mode: bool
    password: str

    @property
    def close_order_side(self) -> str:
        """Get the close order side based on bot direction."""
        return 'buy' if self.direction == "sell" else 'sell'


@dataclass
class OrderMonitor:
    """Thread-safe order monitoring state."""
    order_id: Optional[str] = None
    filled: bool = False
    filled_price: Optional[Decimal] = None
    filled_qty: Decimal = 0.0

    def reset(self):
        """Reset the monitor state."""
        self.order_id = None
        self.filled = False
        self.filled_price = None
        self.filled_qty = 0.0


class MyStrategyBot:
    """Modular Trading Bot - Main trading logic supporting multiple exchanges."""

    def __init__(self, config: MyStrategyConfig):
        self.config = config
        self.logger = TradingLogger(config.exchange, config.ticker, log_to_console=True)

        # Create exchange client
        try:
            self.exchange_client = ExchangeFactory.create_exchange(
                config.exchange,
                config
            )
        except ValueError as e:
            raise ValueError(f"Failed to create exchange client: {e}")

        # Trading state
        self.active_close_orders = []
        self.last_open_order_time = 0
        self.last_log_time = 0
        self.current_order_open_price = Decimal(0)
        self.current_order_close_price = Decimal(0)
        self.order_history = deque(maxlen=3)
        self.current_order_status = None
        self.open_order_filled_event = asyncio.Event()
        self.close_order_filled_event = asyncio.Event()
        self.order_canceled_event = asyncio.Event()
        self.shutdown_requested = False
        self.loop = None
        self.stop_loss_maker_max_times = 10
        self.retry_times = 0

        # Register order callback
        self._setup_websocket_handlers()

    async def graceful_shutdown(self, reason: str = "Unknown"):
        """Perform graceful shutdown of the trading bot."""
        self.logger.log(f"Starting graceful shutdown: {reason}", "INFO")
        self.shutdown_requested = True

        try:
            # Disconnect from exchange
            await self.exchange_client.disconnect()
            self.logger.log("Graceful shutdown completed", "INFO")

        except Exception as e:
            self.logger.log(f"Error during graceful shutdown: {e}", "ERROR")

    def _setup_websocket_handlers(self):
        """Setup WebSocket handlers for order updates."""
        def order_update_handler(message):
            """Handle order updates from WebSocket."""
            try:
                # Check if this is for our contract
                if message.get('contract_id') != self.config.contract_id:
                    return

                order_id = message.get('order_id')
                filled_price = message.get('price')
                status = message.get('status')
                side = message.get('side', '')
                order_type = message.get('order_type', '')
                filled_size = Decimal(message.get('filled_size'))
                if order_type == "OPEN":
                    self.current_order_status = status

                if status == 'FILLED':
                    if order_type == "OPEN":
                        self.current_order_open_price = Decimal(filled_price)
                        # self.order_filled_amount = filled_size
                        # Ensure thread-safe interaction with asyncio event loop
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.open_order_filled_event.set)
                        else:
                            # Fallback (should not happen after run() starts)
                            self.open_order_filled_event.set()
                    elif order_type == "CLOSE":
                        self.current_order_close_price = Decimal(filled_price)
                        # Ensure thread-safe interaction with asyncio event loop
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.close_order_filled_event.set)
                        else:
                            # Fallback (should not happen after run() starts)
                            self.close_order_filled_event.set()

                    # self.logger.log(f"FILLED callback [{order_type}] [{side}] [{order_id}] {status} "
                    #                 f"{message.get('size')} @ {filled_price}", "INFO")
                    # self.logger.log_transaction(order_id, side, message.get('size'), filled_price, status)
                elif status == "CANCELED":

                    # if order_type == "OPEN":
                    #     print(f'callback Cancel 开仓order 11111 {order_id},{side},self.order_filled_amount:{self.order_filled_amount},filled_size:{filled_size},{filled_price},{status}')
                    #     self.order_filled_amount = filled_size
                    #     print(f'callback Cancel 开仓order 22222 {order_id},{side},self.order_filled_amount:{self.order_filled_amount},filled_size:{filled_size},{filled_price},{status}')
                    # elif order_type == "CLOSE":
                    #     print(f'callback Cancel 平仓order1111 {order_id},{side},self.order_filled_amount:{self.order_filled_amount},filled_size:{filled_size},{filled_price},{status}')
                    #     self.order_filled_amount += filled_size
                    #     print(f'callback Cancel 平仓order2222 {order_id},{side},self.order_filled_amount:{self.order_filled_amount},filled_size:{filled_size},{filled_price},{status}')

                    if self.loop is not None:
                        self.loop.call_soon_threadsafe(self.order_canceled_event.set)
                    else:
                        self.order_canceled_event.set()

                    # cancel时显示成交的也是PARTIALLY_FILLED时成交的, 在一个地方记录即可
                    # if self.order_filled_amount > 0:
                    #     self.logger.log_transaction(order_id, side, self.order_filled_amount, filled_price, status)
                        
                    # PATCH
                    # if self.config.exchange == "extended":
                    #     self.logger.log(f"callback Canceled [{order_type}] [{order_id}] {status} "
                    #                     f"{Decimal(message.get('size')) - filled_size} @ {message.get('price')}", "INFO")
                    # else:
                    #     self.logger.log(f"callback Canceled [{order_type}] [{order_id}] {status} "
                    #                     f"{message.get('size')} @ {message.get('price')}", "INFO")
                elif status == "PARTIALLY_FILLED":
                    # self.order_filled_amount = filled_size

                    # if order_type == "OPEN":
                    #     print(f'callback PARTIALLY_FILLED 开仓order {order_id},{side},{self.order_filled_amount}',{filled_price},{status})
                    # elif order_type == "CLOSE":
                    #     print(f'callback PARTIALLY_FILLED 平仓order {order_id},{side},{self.order_filled_amount}',{filled_price},{status})

                    pass
                    # self.logger.log(f"callback PARTIALLY_FILLED [{order_type}] [{order_id}] {status} "
                    #                 f"{filled_size} @ {message.get('price')}", "INFO")
                elif status == "OPEN":
                    pass
                    # if order_type == "OPEN":
                    #     self.logger.log(f"callback开仓挂单成功: [{order_type}] [{side}] [{order_id}] {status} "
                    #                 f"{message.get('size')} @ {message.get('price')}", "INFO")
                    # elif order_type == "CLOSE":
                    #     self.logger.log(f"callback平仓挂单成功: [{order_type}] [{side}] [{order_id}] {status} "
                    #                 f"{message.get('size')} @ {message.get('price')}", "INFO")
                else:
                    self.logger.log(f"---Here: [{order_type}] [{side}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")

            except Exception as e:
                self.logger.log(f"Error handling order update: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

        # Setup order update handler
        self.exchange_client.setup_order_update_handler(order_update_handler)

    async def _place_and_monitor_open_order(self) -> bool:
        """Place an order and monitor its execution."""
        try:
            # Reset state before placing order
            self.open_order_filled_event.clear()
            self.current_order_status = 'OPEN'
            self.order_filled_amount = Decimal(str(0.0))

            # Place the order
            order_result = await self.exchange_client.place_open_order(
                self.config.contract_id,
                self.config.quantity,
                self.config.direction
            )

            if not order_result.success:
                return False

            self.logger.log(f'[OPEN] 开仓挂单 {self.config.direction} {order_result.order_id} {order_result.size} @ {order_result.price}')

            if order_result.status == 'FILLED':
                return await self._handle_order_result(order_result)
            elif not self.open_order_filled_event.is_set():
                try:
                    await asyncio.wait_for(self.open_order_filled_event.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass

            # Handle order result
            return await self._handle_order_result(order_result)

        except Exception as e:
            self.logger.log(f"Error placing order: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return False

    def _record_and_gen_next_direction(self,close_order_filled_price:Decimal) :
        # record the close order filled price
        self.current_order_close_price = Decimal(close_order_filled_price)

        try:
            # record the order history score
            if self._order_profitable():
                if self.config.direction == "buy":
                    direction_score = 1
                else:
                    direction_score = -1
            else:
                if self.config.direction == "buy":
                    direction_score = -1
                else:
                    direction_score = 1
            self.order_history.append(direction_score)

            # record trade data
            if self.config.direction == 'buy':
                rate = (self.current_order_close_price/self.current_order_open_price-1).quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP)
            else:
                rate = -1*(self.current_order_close_price/self.current_order_open_price-1).quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP)
            self.logger.log_transaction_rate(
                self.config.direction
                ,self.current_order_open_price
                ,self.config.close_order_side
                ,self.current_order_close_price
                ,rate)

            # generate next direction
            score = 0
            for item in self.order_history:
                score += item
            self.config.direction = 'buy' if score >= 0 else 'sell'
            self.logger.log(f'Gen_Next_Direction: {self.config.direction}','INFO')
        except Exception as e:
            self.logger.log(f"Gen_Next_Direction error: {e}","ERROR")
            raise Exception(f"Gen_Next_Direction error: {e}")

    def get_price_stop_loss(self,open_filled_price):
        if self.config.direction == "buy":
            return open_filled_price * (1 - self.config.stop_loss/100)
        else:
            return open_filled_price * (1 + self.config.stop_loss/100) 

    def get_price_stop_loss_immediate(self,open_filled_price):
        if self.config.direction == "buy":
            return open_filled_price * (1 - self.config.stop_loss_immediate/100)
        else:
            return open_filled_price * (1 + self.config.stop_loss_immediate/100)

    async def _check_close_order_whether_active(self, close_order_id) -> bool:
        if self.config.exchange == "lighter":
            current_order_status = self.exchange_client.current_order.status
        else:
            order_info = await self.exchange_client.get_order_info(close_order_id)
            current_order_status = order_info.status

        # the close order still active waiting to be filled
        if current_order_status == "New":
            return True
        else:
            # the close order has been filled
            self.close_order_filled_event.set()
            return False

    async def _cancel_previous_order_and_place_market_order(self,previous_order: OrderResult) -> bool:
            # cancel previous order first
            cancelled = await self._cancel_order(previous_order)
            if not cancelled:
                # the close order may have been filled, no need to do more
                self.logger.log(f"[CLOSE] {previous_order.order_id} {previous_order.side} {previous_order.size} @ {previous_order.price} may have been filled")
                self._record_and_gen_next_direction(previous_order.price)
                return True
            # stop loss immediate with the market order
            market_order = await self.exchange_client.place_market_order(
                self.config.contract_id,
                self.config.quantity-self.order_filled_amount,
                self.config.close_order_side
            )

            if not market_order.success:
                self.logger.log("[CLOSE] stop_loss_immediate 市价平仓失败","ERROR")
                raise Exception(f"[CLOSE] Failed to close stop_loss_immediate market order: {market_order.error_message}")
            
            self.logger.log(f'[CLOSE] stop_loss_immediate 市价平仓成交 {market_order.order_id} {market_order.size} @ {market_order.price}')
            self._record_and_gen_next_direction(market_order.price)
            return True

    async def _place_and_monitor_close_order(self, open_filled_price: Decimal, open_filled_quantity: Decimal) -> bool:
        """Monitor the close order."""

        self.logger.log(f'[OPEN] 开仓成交 {self.config.direction} {open_filled_quantity} @ {open_filled_price}')

        # record the open order filled-price
        self.current_order_open_price = Decimal(open_filled_price)

        # Reset state before placing order
        self.close_order_filled_event.clear()
        # self.current_order_status = 'OPEN'
        self.order_filled_amount = Decimal(str(0))

        self.last_open_order_time = time.time()
        close_side = self.config.close_order_side
        if close_side == 'sell':
            close_price = open_filled_price * (1 + self.config.take_profit/100)
        else:
            close_price = open_filled_price * (1 - self.config.take_profit/100)
        
        # Place close order
        close_order_result = await self.exchange_client.place_close_order(
            self.config.contract_id,
            open_filled_quantity,
            close_price,
            close_side
        )
        if self.config.exchange == "lighter":
            await asyncio.sleep(1)

        if not close_order_result.success:
            self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")
            raise Exception(f"[CLOSE] Failed to place close order: {close_order_result.error_message}")

        self.logger.log(f"[CLOSE]: 止盈maker挂单 {close_order_result.side} {close_order_result.order_id} {close_order_result.size} @ {close_order_result.price} ", "INFO")

        price_stop_loss = self.get_price_stop_loss(open_filled_price)
        def should_wait_close_maker(close_direction: str, new_order_price: Decimal) -> bool:
            if close_direction == "buy":
                return new_order_price < price_stop_loss
            elif close_direction == "sell":
                return new_order_price > price_stop_loss
            return False

        # get current order price
        new_order_price = await self.exchange_client.get_order_price(self.config.direction)

        in_time = (time.time() - self.last_open_order_time) <= self.config.wait_time
        time_left = round(self.config.wait_time - (time.time() - self.last_open_order_time),1)
        should_wait = should_wait_close_maker(self.config.close_order_side,new_order_price)
        close_order_active = await self._check_close_order_whether_active(close_order_result.order_id)
        # print(f'平仓wait while条件111: {in_time},{should_wait},{new_order_price},{price_stop_loss},{current_order_status == "New"}')
        while (in_time and should_wait and close_order_active):

            self.logger.log(f"[CLOSE] [{close_order_result.order_id}] Waiting be filled {self.config.close_order_side} @{close_order_result.price} TimeLeft {time_left}s StopLoss @{price_stop_loss} Current @{new_order_price}", "INFO")
            
            await asyncio.sleep(5)
            
            new_order_price = await self.exchange_client.get_order_price(self.config.direction)  
            in_time = (time.time() - self.last_open_order_time) <= self.config.wait_time
            time_left = round(self.config.wait_time - (time.time() - self.last_open_order_time),1)
            should_wait = should_wait_close_maker(self.config.close_order_side,new_order_price)
            close_order_active = await self._check_close_order_whether_active(close_order_result.order_id)
            
            # print(f'平仓wait while条件222: {in_time},{should_wait},{new_order_price},{price_stop_loss},{current_order_status == "New"}')
        # print(f'平仓wait while条件333: {in_time},{should_wait},{new_order_price},{price_stop_loss},{current_order_status == "New"}')

        def _should_stop_loss_immediate(price_stop_loss_immediate) -> bool:
            if self.config.direction == "buy":
                return new_order_price <= price_stop_loss_immediate
            else:
                return new_order_price >= price_stop_loss_immediate
        price_stop_loss_immediate = self.get_price_stop_loss_immediate(open_filled_price)


        if self.close_order_filled_event.is_set() or not close_order_active:
            self.logger.log(f"[CLOSE]: {close_order_result.order_id} 止盈平仓成功 {close_order_result.side} {close_order_result.size}@{close_order_result.price}", "INFO")
            self._record_and_gen_next_direction(close_order_result.price)
            return True
        elif _should_stop_loss_immediate(price_stop_loss_immediate):
            self.logger.log(f'[CLOSE] 触发 stop_loss_immediate 市价平仓')
            return await self._cancel_previous_order_and_place_market_order(close_order_result)
        else :
            self.logger.log('[CLOSE] 未止盈平仓, 触发Stop Loss或者等待时间已到, 盘口价maker平仓','INFO')
            while self.retry_times < self.stop_loss_maker_max_times :
                # cancel order first, 'close_order_result' is previous take profit order in the first time, then the stop loss order 
                cancelled = await self._cancel_order(close_order_result)
                if not cancelled:
                    self.logger.log(f"[CLOSE] {close_order_result.order_id} may have been filled {close_order_result.side} {close_order_result.size} @ {close_order_result.price}")
                    break
                
                # Get positions
                # position_amt = await self.exchange_client.get_account_positions()
                # if position_amt == self.config.quantity:
                #     # the close order hasn't been filled
                # elif position_amt == 0:
                #     # the close order has been filled
                #     self.close_order_filled_event.set()
                #     break


                # reset Event state
                self.close_order_filled_event.clear()
                # Place close order
                close_side = self.config.close_order_side
                close_price = await self.exchange_client.get_order_price(close_side)
                close_order_result = await self.exchange_client.place_close_order(
                    self.config.contract_id,
                    self.config.quantity-self.order_filled_amount,
                    close_price,
                    close_side
                )
                if self.config.exchange == "lighter":
                    await asyncio.sleep(1)

                self.logger.log(f"[CLOSE] 盘口价平仓maker挂单 Retry {self.retry_times} {close_order_result.order_id} {close_order_result.side} {close_order_result.size} @ {close_order_result.price}")

                if not close_order_result.success:
                    self.logger.log(f"[CLOSE] Failed to place close order:{close_order_result.order_id}, {close_order_result.error_message}", "ERROR")
                    raise Exception(f"[CLOSE] Failed to place close order:{close_order_result.order_id},  {close_order_result.error_message}")

                # wait maker order to be filled
                await asyncio.sleep(5)

                def _should_wait_to_fill(new_order_price: Decimal, close_price: Decimal) -> bool:
                    if self.config.close_order_side == 'sell':
                        # the new price reaches the price_stop_loss_immediate when waiting maker order to fill
                        if new_order_price <= price_stop_loss_immediate:
                            self.retry_times = self.stop_loss_maker_max_times
                            return False
                        return new_order_price >= close_price
                    else:
                        if new_order_price >= price_stop_loss_immediate:
                            self.retry_times = self.stop_loss_maker_max_times
                            return False
                        return new_order_price <= close_price

                new_order_price = await self.exchange_client.get_order_price(close_side)
                while (
                    _should_wait_to_fill(new_order_price,close_price)
                    and  await self._check_close_order_whether_active(close_order_result.order_id)
                ):
                    self.logger.log(f"[CLOSE] Waiting {close_order_result.order_id} to be filled {close_order_result.size} @ {close_order_result.price}", "INFO")
                    await asyncio.sleep(5)
                    new_order_price = await self.exchange_client.get_order_price(close_side)

                self.retry_times += 1

            # reset retry times
            self.retry_times = 0

            # check whether the close order has been filled
            if self.close_order_filled_event.is_set() or not await self._check_close_order_whether_active(close_order_result.order_id):
                self.logger.log(f"[CLOSE] 盘口价maker平仓成交 {close_order_result.order_id} {close_order_result.side} {close_order_result.size} @ {close_order_result.price}")
                self._record_and_gen_next_direction(close_order_result.price)
                return True   

            # place market close order after stop_loss_maker_max_times try
            if self.retry_times == self.stop_loss_maker_max_times:
                self.logger.log("[CLOSE] 盘口价maker平仓尝试次数已达最大 ","INFO")
            else:
                self.logger.log("[CLOSE] 触发市价止损平仓 ","INFO")

            return await self._cancel_previous_order_and_place_market_order(close_order_result)

    def _order_profitable(self) -> bool :
        if self.config.direction == "buy":
            return True if self.current_order_close_price >= self.current_order_open_price else False
        if self.config.direction == "sell":
            return True if self.current_order_close_price <= self.current_order_open_price else False

    async def _cancel_order(self,order:OrderResult) -> bool:
        self.order_canceled_event.clear()
        self.logger.log(f"[CLOSE] Cancelling order {order.order_id} {order.side} {order.size} @ {order.price}, filled_amount: {self.order_filled_amount}", "INFO")

        try:
            cancel_result = await self.exchange_client.cancel_order(order.order_id)
            if self.config.exchange == "lighter":
                start_time = time.time()
                while (time.time() - start_time < 10 and self.exchange_client.current_order.status != 'CANCELED' and
                        self.exchange_client.current_order.status != 'FILLED'):
                    await asyncio.sleep(0.1)

                if self.exchange_client.current_order.status not in ['CANCELED', 'FILLED']:
                    raise Exception(f"[CLOSE] Error cancelling order: {self.exchange_client.current_order.status}")
                else:
                    self.order_filled_amount = self.exchange_client.current_order.filled_size
            else:
                if not cancel_result.success:
                    self.logger.log(f"[CLOSE] Failed to cancel order {order.order_id}: {cancel_result.error_message}", "WARNING")
                    if cancel_result.error_message == "Order not found":
                        # the close order has been filled in this case
                        self.close_order_filled_event.set()
                        return False
                    raise Exception(f"[CLOSE] Error cancelling order: {self.exchange_client.current_order.status} {cancel_result.error_message}")
                else:
                    self.order_canceled_event.set()
                    self.current_order_status = "CANCELED"
                    if self.config.exchange == "backpack" or self.config.exchange == "extended":
                        # 当盘口价maker平仓时, 多次cancel订单时,可能会有多次部分成交, 需要把这些成交汇总, 所以要用'+='
                        # 而 '+=' 不会影响stop loss immediate时的order_filled_amount, 因为之前的order_filled_amount值是0
                        self.order_filled_amount += cancel_result.filled_size
                    else:
                        # Wait for cancel event or timeout
                        if not self.order_canceled_event.is_set():
                            try:
                                await asyncio.wait_for(self.order_canceled_event.wait(), timeout=5)
                            except asyncio.TimeoutError:
                                self.logger.log(f"[CLOSE] canceling order TimeoutError {order.order_id}", "WARNING")
                                raise Exception(f"[CLOSE] cancelling order TimeoutError: {order.order_id}")
                        order_info = await self.exchange_client.get_order_info(order.order_id)
                        self.order_filled_amount += order_info.filled_size                        
                    self.logger.log(f"[CLOSE] Cancle order {order.order_id} Finished, self.order_filled_amount:{self.order_filled_amount}")
                    return True
        except Exception as e:
            self.order_canceled_event.set()
            self.logger.log(f"[CLOSE] Error Canceling order {order.order_id}: {e}", "ERROR")


    async def _handle_order_result(self, order_result) -> bool:
        """Handle the result of an order placement."""
        order_id = order_result.order_id
        filled_price = order_result.price

        if self.open_order_filled_event.is_set() or order_result.status == 'FILLED':
            await self._place_and_monitor_close_order(filled_price,self.config.quantity)
            return True
        else:
            new_order_price = await self.exchange_client.get_order_price(self.config.direction)

            def should_wait(direction: str, new_order_price: Decimal, order_result_price: Decimal) -> bool:
                if direction == "buy":
                    return new_order_price <= order_result_price
                elif direction == "sell":
                    return new_order_price >= order_result_price
                return False

            if self.config.exchange == "lighter":
                current_order_status = self.exchange_client.current_order.status
            else:
                order_info = await self.exchange_client.get_order_info(order_id)
                current_order_status = order_info.status

            while (
                should_wait(self.config.direction, new_order_price, order_result.price)
                and current_order_status == "OPEN"
            ):
                self.logger.log(f"[OPEN] [{order_id}] Waiting for order to be filled @ {order_result.price}", "INFO")
                await asyncio.sleep(5)
                if self.config.exchange == "lighter":
                    current_order_status = self.exchange_client.current_order.status
                else:
                    order_info = await self.exchange_client.get_order_info(order_id)
                    if order_info is not None:
                        current_order_status = order_info.status
                new_order_price = await self.exchange_client.get_order_price(self.config.direction)

            # the open order may have been filled
            if self.open_order_filled_event.is_set() or order_result.status == 'FILLED':
                await self._place_and_monitor_close_order(filled_price,self.config.quantity)
                return True

            self.order_canceled_event.clear()
            # Cancel the order if it's still open
            self.logger.log(f"[OPEN] Cancelling order [{order_id}] and placing a new order", "INFO")
            cancel_result = await self.exchange_client.cancel_order(order_id)
            if self.config.exchange == "lighter":
                start_time = time.time()
                while (time.time() - start_time < 10 and self.exchange_client.current_order.status != 'CANCELED' and
                        self.exchange_client.current_order.status != 'FILLED'):
                    await asyncio.sleep(0.1)

                if self.exchange_client.current_order.status not in ['CANCELED', 'FILLED']:
                    raise Exception(f"[OPEN] Error cancelling order: {self.exchange_client.current_order.status}")
                else:
                    self.order_filled_amount = self.exchange_client.current_order.filled_size
            else:
                try:
                    if not cancel_result.success:
                        self.order_canceled_event.set()
                        self.logger.log(f"[CLOSE] Failed to cancel order {order_id}: {cancel_result.error_message}", "WARNING")
                    else:
                        self.current_order_status = "CANCELED"

                except Exception as e:
                    self.order_canceled_event.set()
                    self.logger.log(f"[CLOSE] Error canceling order {order_id}: {e}", "ERROR")

                if self.config.exchange == "backpack" or self.config.exchange == "extended":
                    self.order_filled_amount = cancel_result.filled_size
                else:
                    # Wait for cancel event or timeout
                    if not self.order_canceled_event.is_set():
                        try:
                            await asyncio.wait_for(self.order_canceled_event.wait(), timeout=5)
                        except asyncio.TimeoutError:
                            self.logger.log(f"[CLOSE] canceling order TimeoutError {order_id}", "WARNING")
                            return False

                    order_info = await self.exchange_client.get_order_info(order_id)
                    self.order_filled_amount = order_info.filled_size

            if self.order_filled_amount > 0:
                self.logger.log(f'取消开仓挂单后, 处理部分成交(实际上可能全部成交) {order_id} {self.order_filled_amount}')
                await self._place_and_monitor_close_order(filled_price,self.order_filled_amount)

            return True

    async def _log_status_periodically(self):
        """Log status information periodically, including positions."""
        if time.time() - self.last_log_time > 60 or self.last_log_time == 0:
            print("--------------------------------")
            try:
                # Get active orders
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)

                # Filter close orders
                self.active_close_orders = []
                for order in active_orders:
                    if order.side == self.config.close_order_side:
                        self.active_close_orders.append({
                            'id': order.order_id,
                            'price': order.price,
                            'size': order.size
                        })

                # Get positions
                position_amt = await self.exchange_client.get_account_positions()

                # Calculate active closing amount
                active_close_amount = sum(
                    Decimal(order.get('size', 0))
                    for order in self.active_close_orders
                    if isinstance(order, dict)
                )

                self.logger.log(f"Current Position: {position_amt} | Active closing amount: {active_close_amount} | "
                                f"Order quantity: {len(self.active_close_orders)}")
                self.last_log_time = time.time()
                # Check for position mismatch
                if abs(position_amt - active_close_amount) >= (2 * self.config.quantity):
                    error_message = f"\n\nERROR: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] "
                    error_message += "Position mismatch detected\n"
                    error_message += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                    error_message += "Please manually rebalance your position and take-profit orders\n"
                    error_message += "请手动平衡当前仓位和正在关闭的仓位\n"
                    error_message += f"current position: {position_amt} | active closing amount: {active_close_amount} | "f"Order quantity: {len(self.active_close_orders)}\n"
                    error_message += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                    self.logger.log(error_message, "ERROR")

                    await self.send_notification(error_message.lstrip())

                    if not self.shutdown_requested:
                        self.shutdown_requested = True

                    mismatch_detected = True
                else:
                    mismatch_detected = False

                return mismatch_detected

            except Exception as e:
                self.logger.log(f"Error in periodic status check: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

            print("--------------------------------")

    async def send_notification(self, message: str):
        lark_token = os.getenv("LARK_TOKEN")
        if lark_token:
            async with LarkBot(lark_token) as lark_bot:
                await lark_bot.send_text(message)

        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if telegram_token and telegram_chat_id:
            with TelegramBot(telegram_token, telegram_chat_id) as tg_bot:
                tg_bot.send_text(message)

    async def run(self):
        """Main trading loop."""
        try:
            self.config.contract_id, self.config.tick_size = await self.exchange_client.get_contract_attributes()

            # Log current TradingConfig
            self.logger.log("=== Trading Configuration ===", "INFO")
            self.logger.log(f"Ticker: {self.config.ticker}", "INFO")
            self.logger.log(f"Contract ID: {self.config.contract_id}", "INFO")
            self.logger.log(f"Quantity: {self.config.quantity}", "INFO")
            self.logger.log(f"Take Profit: {self.config.take_profit}%", "INFO")
            self.logger.log(f"Stop Loss: {self.config.stop_loss}%", "INFO")
            self.logger.log(f"Stop Loss Immediate: {self.config.stop_loss_immediate}%", "INFO")
            self.logger.log(f"Direction: {self.config.direction}", "INFO")
            self.logger.log(f"Max Orders: {self.config.max_orders}", "INFO")
            self.logger.log(f"Wait Time: {self.config.wait_time}s", "INFO")
            self.logger.log(f"Exchange: {self.config.exchange}", "INFO")
            self.logger.log(f"Grid Step: {self.config.grid_step}%", "INFO")
            self.logger.log(f"Stop Price: {self.config.stop_price}", "INFO")
            self.logger.log(f"Pause Price: {self.config.pause_price}", "INFO")
            self.logger.log(f"Boost Mode: {self.config.boost_mode}", "INFO")
            self.logger.log("=============================", "INFO")

            # Capture the running event loop for thread-safe callbacks
            self.loop = asyncio.get_running_loop()
            # Connect to exchange
            await self.exchange_client.connect()

            # wait for connection to establish
            await asyncio.sleep(5)

            # Main trading loop
            while not self.shutdown_requested:
                # Update active orders
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)

                # Filter close orders
                self.active_close_orders = []
                for order in active_orders:
                    if order.side == self.config.close_order_side:
                        self.active_close_orders.append({
                            'id': order.order_id,
                            'price': order.price,
                            'size': order.size
                        })

                # Periodic logging
                mismatch_detected = await self._log_status_periodically()

                if not mismatch_detected:
                    if not active_orders:
                        # place open order
                        await self._place_and_monitor_open_order()
                        continue
                    elif self.active_close_orders:
                        # monitor close order
                        print(f'监控已有order,active_close_orders lens: {len(active_orders)}')
                        # close_order = self.active_close_orders[0]
                        # self._place_and_monitor_close_order(close_order.price,close_order.executedQuantity)
                        await asyncio.sleep(3)
        except KeyboardInterrupt:
            self.logger.log("Bot stopped by user")
            await self.graceful_shutdown("User interruption (Ctrl+C)")
        except Exception as e:
            self.logger.log(f"Critical error: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            await self.graceful_shutdown(f"Critical error: {e}")
        finally:
            # Ensure all connections are closed even if graceful shutdown fails
            try:
                await self.exchange_client.disconnect()
            except Exception as e:
                self.logger.log(f"Error disconnecting from exchange: {e}", "ERROR")
