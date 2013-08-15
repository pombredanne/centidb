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
"""

from __future__ import absolute_import

import itertools
import os
import sys
import uuid


__all__ = '''invert unpacks packs unpack_int pack_int'''.split()

KIND_NULL = 15
KIND_NEG_INTEGER = 20
KIND_INTEGER = 21
KIND_BOOL = 30
KIND_BLOB = 40
KIND_TEXT = 50
KIND_UUID = 90
KIND_KEY = 95
KIND_SEP = 102
INVERT_TBL = ''.join(chr(c ^ 0xff) for c in xrange(256))


def invert(s):
    """Invert the bits in the bytestring `s`.

    This is used to achieve a descending order for blobs and strings when they
    are part of a compound key, however when they are stored as a 1-tuple, it
    is probably better to simply the corresponding :py:class:`Collection` or
    :py:class:`Index` with ``reverse=True``.
    """
    return s.translate(INVERT_TBL)


def write_int(v, w):
    """Given a positive integer of 64-bits or less, encode it in a
    variable-length form that preserves the original integer order. Invokes
    `w()` repeatedly with byte ordinals corresponding to the encoded
    representation.

    The output size is such that:

        +-------------+------------------------+
        + *Size*      | *Largest integer*      |
        +-------------+------------------------+
        + 1 byte      | <= 240                 |
        +-------------+------------------------+
        + 2 bytes     | <= 2287                |
        +-------------+------------------------+
        + 3 bytes     | <= 67823               |
        +-------------+------------------------+
        + 4 bytes     | <= 0xffffff            |
        +-------------+------------------------+
        + 5 bytes     | <= 0xffffffff          |
        +-------------+------------------------+
        + 6 bytes     | <= 0xffffffffff        |
        +-------------+------------------------+
        + 7 bytes     | <= 0xffffffffffff      |
        +-------------+------------------------+
        + 8 bytes     | <= 0xffffffffffffff    |
        +-------------+------------------------+
        + 9 bytes     | <= 0xffffffffffffffff  |
        +-------------+------------------------+
    """
    if v < 240:
        w(v)
    elif v <= 2287:
        v -= 240
        w(241 + (v >> 8))
        w(v & 0xff)
    elif v <= 67823:
        v -= 2288
        w(0xf9)
        w((v >> 8))
        w((v & 0xff))
    elif v <= 0xffffff:
        w(0xfa)
        w((v >> 16))
        w((v >> 8) & 0xff)
        w((v & 0xff))
    elif v <= 0xffffffff:
        w(0xfb)
        w((v >> 24))
        w((v >> 16) & 0xff)
        w((v >> 8) & 0xff)
        w((v & 0xff))
    elif v <= 0xffffffffff:
        w(0xfc)
        w((v >> 32))
        w((v >> 24) & 0xff)
        w((v >> 16) & 0xff)
        w((v >> 8) & 0xff)
        w((v & 0xff))
    elif v <= 0xffffffffffff:
        w(0xfd)
        w((v >> 40))
        w((v >> 32) & 0xff)
        w((v >> 24) & 0xff)
        w((v >> 16) & 0xff)
        w((v >> 8) & 0xff)
        w((v & 0xff))
    elif v <= 0xffffffffffffff:
        w(0xfe)
        w((v >> 48))
        w((v >> 40) & 0xff)
        w((v >> 32) & 0xff)
        w((v >> 24) & 0xff)
        w((v >> 16) & 0xff)
        w((v >> 8) & 0xff)
        w((v & 0xff))
    elif v <= 0xffffffffffffffff:
        w(0xff)
        w((v >> 56))
        w((v >> 48) & 0xff)
        w((v >> 40) & 0xff)
        w((v >> 32) & 0xff)
        w((v >> 24) & 0xff)
        w((v >> 16) & 0xff)
        w((v >> 8) & 0xff)
        w((v & 0xff))
    else:
        raise ValueError('Cannot encode integers >= 64 bits, got %d bits (%#x)'
                         % (v.bit_length(), v))


def read_int(getc):
    """Decode and return an integer encoded by :py:func:`write_int`. Invokes
    `getc` repeatedly, which should yield integer bytes from the input stream.
    """
    o = getc()
    if o <= 240:
        return o
    elif o <= 248:
        o2 = getc()
        return 240 + (256 * (o - 241) + o2)
    elif o == 249:
        return 2288 + (256*getc()) + getc()
    elif o == 250:
        return ((getc() << 16) |
                (getc() << 8) |
                (getc()))
    elif o == 251:
        return ((getc() << 24) |
                (getc() << 16) |
                (getc() << 8) |
                (getc()))
    elif o == 252:
        return ((getc() << 32) |
                (getc() << 24) |
                (getc() << 16) |
                (getc() << 8) |
                (getc()))
    elif o == 253:
        return ((getc() << 40) |
                (getc() << 32) |
                (getc() << 24) |
                (getc() << 16) |
                (getc() << 8) |
                (getc()))
    elif o == 254:
        return ((getc() << 48) |
                (getc() << 40) |
                (getc() << 32) |
                (getc() << 24) |
                (getc() << 16) |
                (getc() << 8) |
                (getc()))
    elif o == 255:
        return ((getc() << 56) |
                (getc() << 48) |
                (getc() << 40) |
                (getc() << 32) |
                (getc() << 24) |
                (getc() << 16) |
                (getc() << 8) |
                (getc()))


def write_str(s, w):
    """Encode the bytestring `s` so that no NUL appears in the output. A
    constant-space encoding is used that treats the input as a stream of bits,
    packing each group of 7 bits into a byte with the highest bit always set,
    except for the final byte. A delimiting NUL is appended to the output.
    Invokes `w()` repeatedly with byte ordinals corresponding to the encoded
    representation."""
    shift = 1
    trailer = 0

    for o in bytearray(s):
        w(0x80 | trailer | (o >> shift))
        if shift < 7:
            trailer = (o << (7 - shift)) & 0xff
            shift += 1
        else:
            w(0x80 | o)
            shift = 1
            trailer = 0

    if shift > 1:
        w(trailer)
        if trailer != 0:
            w(0)
    else:
        w(0)


def read_str(getc, it=None):
    """Decode and return a bytestring encoded by :py:func:`read_str`. Invokes
    `getc` repeatedly, which should yield byte ordinals from the input
    stream."""
    if not it:
        it = iter(getc, '')
    lb = getc()
    if not lb:
        return ''
    out = bytearray()
    shift = 1
    for cb in it:
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
            lb = getc()
            if not lb:
                break

    return str(out)


def pack_int(prefix, i):
    """Invoke :py:func:`write_int(i, ba) <write_int>` using a temporary
    :py:class:`bytearray` initialized to contain `prefix`, returning the result
    as a bytestring."""
    ba = bytearray(prefix)
    write_int(i, ba.append)
    return str(ba)


def unpack_int(s):
    """Invoke :py:func:`read_int`, wrapping `s` in a temporary iterator."""
    return read_int(itertools.imap(ord, s).next)


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
    ba = bytearray(prefix)
    w = ba.append

    if type(tups) is not list:
        tups = [tups]

    last = len(tups) - 1
    for i, tup in enumerate(tups):
        if i:
            w(KIND_SEP)
        tup = tuplize(tup)
        for j, arg in enumerate(tup):
            type_ = type(arg)
            if arg is None:
                w(KIND_NULL)
            elif type_ is bool:
                w(KIND_BOOL)
                write_int(arg, w)
            elif type_ is int or type_ is long:
                if arg < 0:
                    w(KIND_NEG_INTEGER)
                    write_int(-arg, w)
                else:
                    w(KIND_INTEGER)
                    write_int(arg, w)
            elif type_ is uuid.UUID:
                w(KIND_UUID)
                write_str(arg.get_bytes(), w)
            elif type_ is str:
                w(KIND_BLOB)
                write_str(arg, w)
            elif type_ is unicode:
                w(KIND_TEXT)
                write_str(arg.encode('utf-8'), w)
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

    it = itertools.imap(ord, s[len(prefix):])
    getc = it.next

    tups = []
    tup = []
    for c in it:
        if c == KIND_NULL:
            arg = None
        elif c == KIND_INTEGER:
            arg = read_int(getc)
        elif c == KIND_NEG_INTEGER:
            arg = -read_int(getc)
        elif c == KIND_BOOL:
            arg = bool(read_int(getc))
        elif c == KIND_BLOB:
            arg = read_str(getc, it)
        elif c == KIND_TEXT:
            arg = read_str(getc, it).decode('utf-8')
        elif c == KIND_UUID:
            arg = uuid.UUID(read_str(getc, it))
        elif c == KIND_SEP:
            tups.append(tuple(tup))
            if first:
                return tups[0]
            tup = []
            continue
        else:
            raise ValueError('bad kind %r; key corrupt? %r' % (c, tup))
        tup.append(arg)
    tups.append(tuple(tup))
    return tups[0] if first else tups


def unpack(prefix, s):
    return unpacks(prefix, s, True)


# Hack: disable speedups while testing or reading docstrings.
if os.path.basename(sys.argv[0]) not in ('sphinx-build', 'pydoc') and \
        os.getenv('CENTIDB_NO_SPEEDUPS') is None:
    try:
        from centidb._keycoder import *
    except ImportError:
        pass
