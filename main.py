from cryptography.fernet import Fernet
import os


def encrypt_file(input_file, output_file, key):
    fernet = Fernet(key)
    with open(input_file, 'rb') as file:
        original = file.read()
    encrypted = fernet.encrypt(original)
    with open(output_file, 'wb') as encrypted_file:
        encrypted_file.write(encrypted)


def generate_key():
    return Fernet.generate_key()


if __name__ == "__main__":
    # 生成一个加密密钥
    key = generate_key()
    input_file = 'test.py'
    # 输出加密后的文件路径
    output_file = 'test_encrypted.py'
    encrypt_file(input_file, output_file, key)
