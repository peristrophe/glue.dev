import base64
import boto3
from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Padding


GENERATE_KEY_SPEC = "AES_256"


def generate_data_key(kms_key_id: str) -> tuple[str, str]:
    client = boto3.client("kms")
    response = client.generate_data_key(KeyId=kms_key_id, KeySpec=GENERATE_KEY_SPEC)
    plain_data_key = base64.b64encode(response["Plaintext"]).decode("utf-8")
    cipher_text_blob = base64.b64encode(response["CiphertextBlob"]).decode("utf-8")
    return plain_data_key, cipher_text_blob

def decrypt_data_key(cipher_text_blob: str) -> str:
    client = boto3.client("kms")
    response = client.decrypt(CiphertextBlob=base64.b64decode(cipher_text_blob))
    return base64.b64encode(response["Plaintext"]).decode("utf-8")

def encrypt(plain_text: str, key: str) -> str:
    _key = base64.b64decode(key)
    iv = Random.get_random_bytes(AES.block_size)
    cipher = AES.new(_key, AES.MODE_CBC, iv)
    data = Padding.pad(plain_text.encode("utf-8"), AES.block_size, "pkcs7")
    return base64.b64encode(iv + cipher.encrypt(data)).decode("utf-8")

def decrypt(encrypted_text: str, key: str) -> str:
    _key = base64.b64decode(key)
    encrypted_text = base64.b64decode(encrypted_text)
    iv = encrypted_text[:AES.block_size]
    cipher = AES.new(_key, AES.MODE_CBC, iv)
    data = Padding.unpad(cipher.decrypt(encrypted_text[AES.block_size:]), AES.block_size, "pkcs7")
    return data.decode("utf-8")
