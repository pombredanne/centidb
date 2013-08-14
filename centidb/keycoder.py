#
# Copyright 2013, David Wilson.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
SQLite4-like tuple serialization.

See http://keycoder.readthedocs.org/
"""

from __future__ import absolute_import

import functools
import operator
import os
import struct
import sys
import uuid

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO


__all__ = '''invert unpacks packs unpack_int pack_int'''.split()

KIND_NULL = chr(15)
KIND_NEG_INTEGER = chr(20)
KIND_INTEGER = chr(21)
KIND_BOOL = chr(30)
KIND_BLOB = chr(40)
KIND_TEXT = chr(50)
KIND_UUID = chr(90)
KIND_KEY = chr(95)
KIND_SEP = chr(102)
INVERT_TBL = ''.join(chr(c ^ 0xff) for c in xrange(256))


def invert(s):
    """Invert the bits in the bytestring `s`.

    This is used to achieve a descending order for blobs and strings when they
    are part of a compound key, however when they are stored as a 1-tuple, it
    is probably better to simply the corresponding :py:class:`Collection` or
    :py:class:`Index` with ``reverse=True``.
    """
    return s.translate(INVERT_TBL)

def pack_int(v):
    """Given some positive integer of 64-bits or less, return a variable length
    bytestring representation that preserves the integer's order. The
    bytestring size is such that:

        +-------------+------------------------+
        + *Size*      | *Largest integer*      |
        +-------------+------------------------+
        + 1 byte      | <= 240                 |
        +-------------+------------------------+
        + 2 bytes     | <= 2287                |
        +-------------+------------------------+
        + 3 bytes     | <= 67823               |
        +-------------+------------------------+
        + 4 bytes     | <= 16777215            |
        +-------------+------------------------+
        + 5 bytes     | <= 4294967295          |
        +-------------+------------------------+
        + 6 bytes     | <= 1099511627775       |
        +-------------+------------------------+
        + 7 bytes     | <= 281474976710655     |
        +-------------+------------------------+
        + 8 bytes     | <= 72057594037927935   |
        +-------------+------------------------+
        + 9 bytes     | <= (2**64)-1           |
        +-------------+------------------------+
    """
    if v < 240:
        return chr(v)
    elif v <= 2287:
        v -= 240
        d, m = divmod(v, 256)
        return chr(241 + d) + chr(m)
    elif v <= 67823:
        v -= 2288
        d, m = divmod(v, 256)
        return '\xf9' + chr(d) + chr(m)
    elif v <= 16777215:
        return '\xfa' + struct.pack('>L', v)[-3:]
    elif v <= 4294967295:
        return '\xfb' + struct.pack('>L', v)
    elif v <= 1099511627775:
        return '\xfc' + struct.pack('>Q', v)[-5:]
    elif v <= 281474976710655:
        return '\xfd' + struct.pack('>Q', v)[-6:]
    elif v <= 72057594037927935:
        return '\xfe' + struct.pack('>Q', v)[-7:]
    else:
        assert v.bit_length() <= 64
        return '\xff' + struct.pack('>Q', v)

def unpack_int(getc, read):
    """Decode and return an integer encoded by :py:func:`pack_int`.

    `get`:
        Function that returns the next byte of input.
    `read`:
        Function accepting a byte count and returning that many bytes of input.

    ::

        io = StringIO.StringIO(encoded_int)
        i = unpack_int(lambda: io.read(1), io.read)
        # io.tell() is now positioned one byte past end of integer.
    """
    c = getc()
    o = ord(c)
    if o <= 240:
        return o
    elif o <= 248:
        c2 = getc()
        o2 = ord(c2)
        return 240 + (256 * (o - 241) + o2)
    elif o == 249:
        return 2288 + (256*ord(getc())) + ord(getc())
    elif o == 250:
        return struct.unpack('>L', '\x00' + read(3))[0]
    elif o == 251:
        return struct.unpack('>L', read(4))[0]
    elif o == 252:
        return struct.unpack('>Q', '\x00\x00\x00' + read(5))[0]
    elif o == 253:
        return struct.unpack('>Q', '\x00\x00' + read(6))[0]
    elif o == 254:
        return struct.unpack('>Q', '\x00' + read(7))[0]
    elif o == 255:
        return struct.unpack('>Q', read(8))[0]


def unpack_int_s(s):
    io = StringIO.StringIO(s)
    return unpack_int(lambda: io.read(1), io.read)


def encode_str(s):
    shift = 1
    trailer = 0

    out = bytearray(1 + (((len(s) * 8) + 6) / 7))
    pos = 0

    for o in bytearray(s):
        out[pos] = 0x80 | trailer | (o >> shift)
        pos += 1

        if shift < 7:
            trailer = (o << (7 - shift)) & 0xff
            shift += 1
        else:
            out[pos] = 0x80 | o
            pos += 1
            shift = 1
            trailer = 0

    if shift > 1:
        out[pos] = trailer
    return str(out)


def decode_str(getc):
    it = iter(getc, '')
    lb = ord(it.next())
    if not lb:
        return ''
    out = bytearray()
    shift = 1
    for cb in it:
        cb = ord(cb)
        if cb == 0:
            break
        ch = (lb << shift) & 0xff
        ch |= (cb & 0x7f) >> (7 - shift)
        out.append(ch)
        if shift < 7:
            shift += 1
            lb = cb
        else:
            shift = 1
            lb = ord(it.next())
            if not lb:
                break

    return str(out)


def tuplize(o):
    return o if type(o) is tuple else (o,)


def packs(prefix, tups):
    """Encode a list of tuples of primitive values to a bytestring that
    preserves a meaningful lexicographical sort order.

        `prefix`:
            Initial prefix for the bytestring, if any.

    A bytestring is returned such that elements of different types at the same
    position within distinct sequences with otherwise identical prefixes will
    sort in the following order.

        1. ``None``
        2. Negative integers
        3. Positive integers
        4. ``False``
        5. ``True``
        6. Bytestrings (i.e. :py:func:`str`).
        7. Unicode strings.
        8. ``uuid.UUID`` instances.
        9. Sequences with another tuple following the last identical element.

    If `tups` is not exactly a list, it is assumed to a be single key, and will
    be treated as if it were wrapped in a list.

    If the type of any list element is not exactly a tuple, it is assumed to be
    a single primitive value, and will be treated as if it were a 1-tuple key.

    ::

        >>> packs(1)      # Treated like packs([(1,)])
        >>> packs((1,))   # Treated like packs([(1,)])
        >>> packs([1])    # Treated like packs([(1,)])
        >>> packs([(1,)]) # Treated like packs([(1,)])
    """
    ba = bytearray()
    w = ba.append
    e = ba.extend

    if type(tups) is not list:
        tups = [tups]

    e(prefix)
    last = len(tups) - 1
    for i, tup in enumerate(tups):
        if i:
            w(KIND_SEP)
        tup = tuplize(tup)
        tlast = len(tup) - 1
        for j, arg in enumerate(tup):
            type_ = type(arg)
            if arg is None:
                w(KIND_NULL)
            elif type_ is bool:
                w(KIND_BOOL)
                e(pack_int(arg))
            elif type_ is int or type_ is long:
                if arg < 0:
                    w(KIND_NEG_INTEGER)
                    e(pack_int(-arg))
                else:
                    w(KIND_INTEGER)
                    e(pack_int(arg))
            elif type_ is uuid.UUID:
                w(KIND_UUID)
                e(encode_str(arg.get_bytes()))
                w('\x00')
            elif type_ is str:
                w(KIND_BLOB)
                e(encode_str(arg))
            elif type_ is unicode:
                w(KIND_TEXT)
                e(encode_str(arg.encode('utf-8')))
            else:
                raise TypeError('unsupported type: %r' % (arg,))
    return str(ba)


pack = packs


def unpacks(prefix, s, first=False):
    """Decode a bytestring produced by :py:func:`keycoder.packs`, returning the
    list of tuples the string represents.

        `prefix`:
            If specified, a string prefix of this length will be skipped before
            decoding begins. If the passed string does not start with the given
            prefix, None is returned and the string is not decoded.

        `first`:
            Stop work after the first tuple has been decoded and return it
            immediately. Note the return value is the tuple, not a list
            containing the tuple.
    """
    if not s.startswith(prefix):
        return
    s = buffer(s, len(prefix))

    io = StringIO.StringIO(s)
    getc = functools.partial(io.read, 1)
    tups = []
    tup = []
    for c in iter(getc, ''):
        if c == KIND_NULL:
            arg = None
        elif c == KIND_INTEGER:
            arg = unpack_int(getc, io.read)
        elif c == KIND_NEG_INTEGER:
            arg = -unpack_int(getc, io.read)
        elif c == KIND_BOOL:
            arg = bool(unpack_int(getc, io.read))
        elif c == KIND_BLOB:
            arg = decode_str(getc)
        elif c == KIND_TEXT:
            arg = decode_str(getc).decode('utf-8')
        elif c == KIND_UUID:
            arg = uuid.UUID(decode_str(getc))
        elif c == KIND_SEP:
            tups.append(tuple(tup))
            if first:
                return tups[0]
            tup = []
            continue
        else:
            raise ValueError('bad kind %r; key corrupt? %r' % (ord(c), tup))
        tup.append(arg)
    tups.append(tuple(tup))
    return tups[0] if first else tups


def unpack(prefix, s):
    return unpacks(prefix, s, True)


# Hack: disable speedups while testing or reading docstrings.
if os.path.basename(sys.argv[0]) not in ('sphinx-build', 'pydoc') and \
        os.getenv('KEYCODER_NO_SPEEDUPS') is None:
    try:
        from _keycoder import *
    except ImportError:
        pass
