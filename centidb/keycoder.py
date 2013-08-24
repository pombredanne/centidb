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

import calendar
import datetime
import itertools
import os
import sys
import time
import uuid


__all__ = ['invert', 'unpacks', 'packs', 'unpack_int', 'pack_int']

KIND_NULL = 15
KIND_NEG_INTEGER = 20
KIND_INTEGER = 21
KIND_BOOL = 30
KIND_BLOB = 40
KIND_TEXT = 50
KIND_UUID = 90
KIND_NEG_TIME = 91
KIND_TIME = 92
KIND_SEP = 102
INVERT_TBL = ''.join(chr(c ^ 0xff) for c in xrange(256))

UTCOFFSET_SHIFT = 64 # 16 hours * 4 (15 minute increments)
UTCOFFSET_DIV = 15 * 60 # 15 minutes

_tz_cache = {}


class Key(object):
    """Represents a database key composed of zero or more elements. An element
    may be a string, Unicode, integer, boolean, or UUID value.
    """

    __slots__ = ['args', 'prefix', 'packed', 'batch']
    def __init__(self, *args):
        self.prefix = ''
        if len(args) == 1 and type(args[0]) is tuple:
            args = args[0]
        self.args = args or None
        self.packed = packs('', args) if args else ''
        self.batch = False

    @classmethod
    def from_packed(cls, prefix, packed, batch=False):
        self = cls()
        self.prefix = prefix
        self.packed = packed
        self.batch = False
        return self

    def __add__(self, extra):
        new = Key()
        new.prefix = self.prefix
        new.packed = self.packed + packs('', extra)
        new.batch = False
        return new

    __iadd__ = __add__

    def _with_prefix(self, prefix):
        if self.prefix != prefix:
            self.packed = prefix + self.packed[len(self.prefix):]
            self.prefix = prefix
        return self.packed

    def __iter__(self):
        if self.args is None:
            self.args = unpack(self.prefix, self.packed)
        return iter(self.args)

    def __getitem__(self, i):
        try:
            return self.args[i]
        except TypeError:
            self.args = unpack(self.prefix, self.packed)
            return self.args[i]

    def __hash__(self):
        if self.args is None:
            self.args = unpack(self.prefix, self.packed)
        return hash(self.args)

    def __len__(self):
        if self.args is None:
            self.args = unpack(self.prefix, self.packed)
        return len(self.args)

    def __le__(self, other):
        return self.packed <= keyize(other)._with_prefix(self.prefix)

    def __ge__(self, other):
        return self.packed >= keyize(other)._with_prefix(self.prefix)

    def __lt__(self, other):
        return self.packed < keyize(other)._with_prefix(self.prefix)

    def __gt__(self, other):
        return self.packed > keyize(other)._with_prefix(self.prefix)

    def __eq__(self, other):
        return self.packed == keyize(other)._with_prefix(self.prefix)

    def __ne__(self, other):
        return self.packed != keyize(other)._with_prefix(self.prefix)

    def __cmp__(self, other):
        return cmp(self.packed, keyize(other)._with_prefix(self.prefix))

    def __repr__(self):
        if self.args is None:
            self.args = unpack(self.prefix, self.packed)
        return '<centidb.Key %r>' % (self.args,)



class FixedOffsetZone(datetime.tzinfo):
    ZERO = datetime.timedelta(0)

    def __init__(self, seconds):
        self._offset = datetime.timedelta(seconds=seconds)
        if seconds < 0:
            sign = '-'
            seconds = abs(seconds)
        else:
            sign = '+'
        hours, minutes = divmod(seconds / 60, 60)
        self._name = '%s%02d:%02d' % (sign, hours, minutes)

    def utcoffset(self, dt):
        return self._offset

    def tzname(self, dt):
        return self._name

    def dst(self, dt):
        return self.ZERO

    def __repr__(self):
        return '<%s>' % (self.tzname(None),)


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
    if v <= 240:
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


def read_int(inp, pos, length):
    """Decode and return an integer encoded by :py:func:`write_int`. Invokes
    `getc` repeatedly, which should yield integer bytes from the input stream.
    """
    o = inp[pos]
    if o <= 240:
        return o, pos+1

    have = length - pos
    if o <= 248:
        if have < 2:
            raise ValueError('not enough bytes: need 2')
        o2 = inp[pos+1]
        return 240 + (256 * (o - 241) + o2), pos+2

    if have < (o - 249):
        raise ValueError('not enough bytes: need %d' % (o - 249))

    if o == 249:
        return 2288 + (256*inp[pos+1]) + inp[pos+2], pos+3
    elif o == 250:
        return ((inp[pos+1] << 16) |
                (inp[pos+2] << 8) |
                (inp[pos+3])), pos+4
    elif o == 251:
        return ((inp[pos+1] << 24) |
                (inp[pos+2] << 16) |
                (inp[pos+3] << 8) |
                (inp[pos+4])), pos+5
    elif o == 252:
        return ((inp[pos+1] << 32) |
                (inp[pos+2] << 24) |
                (inp[pos+3] << 16) |
                (inp[pos+4] << 8) |
                (inp[pos+5])), pos+6
    elif o == 253:
        return ((inp[pos+1] << 40) |
                (inp[pos+2] << 32) |
                (inp[pos+3] << 24) |
                (inp[pos+4] << 16) |
                (inp[pos+5] << 8) |
                (inp[pos+6])), pos+7
    elif o == 254:
        return ((inp[pos+1] << 48) |
                (inp[pos+2] << 40) |
                (inp[pos+3] << 32) |
                (inp[pos+4] << 24) |
                (inp[pos+5] << 16) |
                (inp[pos+6] << 8) |
                (inp[pos+7])), pos+8
    elif o == 255:
        return ((inp[pos+1] << 56) |
                (inp[pos+2] << 48) |
                (inp[pos+3] << 40) |
                (inp[pos+4] << 32) |
                (inp[pos+5] << 24) |
                (inp[pos+6] << 16) |
                (inp[pos+7] << 8) |
                (inp[pos+8])), pos+9


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

    if len(s) % 7:
        w(0x80 | trailer)


def read_str(inp, pos, length):
    """Decode and return a bytestring encoded by :py:func:`read_str`. Invokes
    `getc` repeatedly, which should yield byte ordinals from the input
    stream."""
    if pos >= length:
        return '', pos
    lb = inp[pos]
    if lb < 0x80:
        return '', pos
    pos += 1
    out = bytearray()
    shift = 1
    while pos < length:
        cb = inp[pos]
        if cb < 0x80:
            break
        pos += 1
        ch = (lb << shift) & 0xff
        ch |= (cb & 0x7f) >> (7 - shift)
        out.append(ch)
        if shift < 7:
            shift += 1
            lb = cb
        else:
            shift = 1
            if pos >= length:
                break
            lb = inp[pos]
            if lb < 0x80:
                break
            pos += 1

    return str(out), pos


def write_time(dt, w):
    """Encode a datetime.datetime to `w`.
    """
    msec = int(calendar.timegm(dt.utctimetuple())) * 1000
    msec += dt.microsecond / 1000
    msec <<= 7
    if dt.tzinfo:
        offset = int(dt.utcoffset().total_seconds())
    else:
        offset = (calendar.timegm(dt.timetuple()) -
                  calendar.timegm(dt.utctimetuple()))

    msec |= (offset / UTCOFFSET_DIV) + UTCOFFSET_SHIFT
    if msec < 0:
        w(KIND_NEG_TIME)
        write_int(-msec, w)
    else:
        w(KIND_TIME)
        write_int(msec, w)


def read_time(kind, inp, pos, length):
    msec, pos = read_int(inp, pos, length)
    offset = msec & 0x7f
    msec >>= 7
    if kind == KIND_NEG_TIME:
        msec = -msec

    try:
        tz = _tz_cache[offset]
    except KeyError:
        tz = FixedOffsetZone((offset - UTCOFFSET_SHIFT) * UTCOFFSET_DIV)
        _tz_cache[offset] = tz

    return datetime.datetime.fromtimestamp(msec / 1000.0, tz), pos


def pack_int(prefix, i):
    """Invoke :py:func:`write_int(i, ba) <write_int>` using a temporary
    :py:class:`bytearray` initialized to contain `prefix`, returning the result
    as a bytestring."""
    ba = bytearray(prefix)
    write_int(i, ba.append)
    return str(ba)


def unpack_int(s):
    """Invoke :py:func:`read_int`, wrapping `s` in a temporary iterator."""
    ba = bytearray(s)
    return read_int(ba, 0, len(ba))


def keyize(o):
    return o if type(o) is Key else Key(o)


GOOD_TYPES = (tuple, Key)

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
        9. ``datetime.datetime`` instances.
        10. Sequences with another tuple following the last identical element.

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
        if type(tup) not in GOOD_TYPES:
            tup = (tup,)
        for j, arg in enumerate(tup):
            type_ = type(arg)
            if type_ is int or type_ is long:
                if arg < 0:
                    w(KIND_NEG_INTEGER)
                    write_int(-arg, w)
                else:
                    w(KIND_INTEGER)
                    write_int(arg, w)
            elif type_ is str:
                w(KIND_BLOB)
                write_str(arg, w)
            elif type_ is unicode:
                w(KIND_TEXT)
                write_str(arg.encode('utf-8'), w)
            elif arg is None:
                w(KIND_NULL)
            elif type_ is uuid.UUID:
                w(KIND_UUID)
                ba.extend(arg.get_bytes())
            elif type_ is bool:
                w(KIND_BOOL)
                write_int(arg, w)
            elif type_ is datetime.datetime:
                write_time(arg, w)
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

    plength = len(prefix)
    inp = bytearray(s[plength:])
    length = len(inp)
    pos = 0

    tups = []
    tup = []
    while pos < length:
        c = inp[pos]
        pos += 1
        if c == KIND_NULL:
            arg = None
        elif c == KIND_INTEGER:
            arg, pos = read_int(inp, pos, length)
        elif c == KIND_NEG_INTEGER:
            arg, pos = read_int(inp, pos, length)
            arg = -arg
        elif c == KIND_BLOB:
            arg, pos = read_str(inp, pos, length)
        elif c == KIND_TEXT:
            arg, pos = read_str(inp, pos, length)
            arg = arg.decode('utf-8')
        elif c == KIND_TIME or c == KIND_NEG_TIME:
            arg, pos = read_time(c, inp, pos, length)
        elif c == KIND_BOOL:
            arg = bool(inp[pos])
            pos += 1
        elif c == KIND_UUID:
            if (pos + 16) >= length:
                raise ValueError('short UUID read')
            arg = uuid.UUID(None, s[plength + pos:plength + pos + 16])
            pos += 16
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
