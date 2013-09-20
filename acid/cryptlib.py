
"""
Cryptography functions for wrapping a Key. Algorithm extracted from Beaker.
"""

from __future__ import absolute_import
import base64
import os
import hashlib
import hmac

__all__ = ['wrap', 'unwrap']

AES = None
Counter = None
KDF = None

urandom = open('/dev/urandom', 'r', 65536).read


def _import_crypto():
    global AES, Counter, KDF
    from Crypto.Cipher import AES
    from Crypto.Util import Counter
    from Crypto.Protocol import KDF


def wrap(secret, data):
    if not AES:
        _import_crypto()

    salt = urandom(4)
    key = KDF.PBKDF2(secret, salt, 32, 1)
    counter = Counter.new(128, initial_value=0)
    cipher = AES.new(key, AES.MODE_CTR, counter=counter)
    body = cipher.encrypt(data)

    mac = hmac.new(key, None, hashlib.sha256)
    mac.update(salt)
    mac.update(body)
    raw = ''.join([mac.digest()[:4], salt, body])
    return base64.urlsafe_b64encode(raw).rstrip('=')


def unwrap(secret, data):
    if not AES:
        _import_crypto()

    rem = len(data) % 4
    if rem:
        data += '=' * rem

    try:
        raw = base64.urlsafe_b64decode(data)
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
