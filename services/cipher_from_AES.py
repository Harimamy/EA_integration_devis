import base64
import hashlib
import random
import string
import sys

from Crypto import Random
from Crypto.Cipher import AES


class AESCipher(object):

    def __init__(self, key):
        self.bs = AES.block_size
        self.key = hashlib.sha256(key.encode()).digest()

    def encrypt(self, raw):
        raw = self._pad(raw)
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return base64.b64encode(iv + cipher.encrypt(raw.encode()))

    def decrypt(self, enc):
        enc = base64.b64decode(enc)
        iv = enc[:AES.block_size]
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return self._unpad(cipher.decrypt(enc[AES.block_size:])).decode('utf-8')

    def _pad(self, s):
        return s + (self.bs - len(s) % self.bs) * chr(self.bs - len(s) % self.bs)

    @staticmethod
    def _unpad(s):
        return s[:-ord(s[len(s)-1:])]


# https://stackoverflow.com/questions/12524994/encrypt-decrypt-using-pycrypto-aes-256
if __name__ == '__main__':
    # mdp encoded: hg2Bcnk17H26pX2RQ7HpyDkMo3sT9cGowBFMvce6ABY=
    # secret key: 7ql9zA1bqqSnoYnt4zw3HppY
    # secret_key = ''.join(random.choices(string.ascii_letters + string.digits, k=24)) # to how to generate the string combine with digits random characters 24
    # print(secret_key)
    # sys.exit()
    Class_Aes = AESCipher('7ql9zA1bqqSnoYnt4zw3HppY')
    encoded_password = Class_Aes.encrypt("123qwertY")
    encoded_user = Class_Aes.encrypt("sa")
    print("the decoded user is ", Class_Aes.decrypt('eNLhe4jeKA+DirrWkj0Jw4zI7XlXLlhFs25jsPTQ3Dw='))
    print("the decoded password is ", Class_Aes.decrypt('hg2Bcnk17H26pX2RQ7HpyDkMo3sT9cGowBFMvce6ABY='))
    # print("here is the answer --------- ", Class_Aes.decrypt(encoded_user))
