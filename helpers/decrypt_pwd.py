from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64

def generate_key_from_password(password: str, salt: bytes) -> bytes:
    """从密码和盐生成 Fernet 密钥"""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
    return key

def decrypt_private_key(encrypted_key: str, password: str, salt: bytes) -> str:
    """解密 private key"""
    try:
        key = generate_key_from_password(password, salt)
        fernet = Fernet(key)
        decrypted_key = fernet.decrypt(encrypted_key.encode())
        return decrypted_key.decode()
    except Exception as e:
        raise ValueError("解密失败，可能是密码错误或密文被篡改")