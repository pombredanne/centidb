
"""
Cryptography functions for wrapping a Key. Algorithm extracted from Beaker.
"""

from __future__ import absolute_import
import base64
import hashlib
import hmac
import struct
import zlib

__all__ = ['wrap', 'unwrap']

AES = None
Counter = None
KDF = None


def _import_crypto():
    global AES, Counter, KDF
    from Crypto.Cipher import AES
    from Crypto.Util import Counter
    from Crypto.Protocol import KDF


def wrap(secret, data):
    if not AES:
        _import_crypto()

    salt = struct.pack('>L', zlib.crc32(data) & 0xffffffff)
    key = KDF.PBKDF2(secret, salt, 32, 1)
    counter = Counter.new(128, initial_value=0)
    cipher = AES.new(key, AES.MODE_CTR, counter=counter)
    body = cipher.encrypt(data)

    mac = hmac.new(key, None, hashlib.sha256)
    mac.update(salt)
    mac.update(body)
    return ''.join([mac.digest()[:4], salt, body]).encode('hex')


def unwrap(secret, data):
    if not AES:
        _import_crypto()

    rem = len(data) % 4
    if rem:
        data += '=' * rem

    try:
        raw = data.decode('hex')
    except (TypeError, ValueError):
        return

    if len(raw) < 9:
        return

    salt = raw[4:8]
    key = KDF.PBKDF2(secret, salt, 32, 1)

    mac = hmac.new(key, None, hashlib.sha256)
    mac.update(raw[4:])
    if mac.digest()[:4] != raw[:4]:
        return

    counter = Counter.new(128, initial_value=0)
    cipher = AES.new(key, AES.MODE_CTR, counter=counter)
    return cipher.decrypt(raw[8:])
