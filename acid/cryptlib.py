
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

    salth = hashlib.sha1()
    salth.update(data)
    salth.update(secret)
    salt = salth.digest()[:4]

    key = KDF.PBKDF2(secret, salt, 32, 1)
    counter = Counter.new(128, initial_value=0)
    cipher = AES.new(key, AES.MODE_CTR, counter=counter)
    body = cipher.encrypt(data)

    mac = hmac.new(key, None, hashlib.sha1)
    mac.update(salt)
    mac.update(body)
    return encode(''.join([mac.digest()[:4], salt, body]))


def encode(s):
    return base64.urlsafe_b64encode(s).rstrip('=')


def decode(s):
    rem = len(s) % 4
    if rem:
        s += '=' * rem
    try:
        return base64.urlsafe_b64decode(s)
    except (TypeError, ValueError):
        return


def unwrap(secret, data):
    if not AES:
        _import_crypto()

    raw = decode(data)
    if (not raw) or len(raw) < 9:
        return

    salt = raw[4:8]
    key = KDF.PBKDF2(secret, salt, 32, 1)

    mac = hmac.new(key, None, hashlib.sha1)
    mac.update(raw[4:])
    if mac.digest()[:4] != raw[:4]:
        return

    counter = Counter.new(128, initial_value=0)
    cipher = AES.new(key, AES.MODE_CTR, counter=counter)
    body = cipher.decrypt(raw[8:])

    salth = hashlib.sha1()
    salth.update(body)
    salth.update(secret)
    if salth.digest()[:4] == salt:
        return body
