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


__all__ = ['Key', 'invert', 'unpacks', 'packs', 'unpack_int', 'pack_int']

KIND_NULL = 0x0f
KIND_NEG_INTEGER = 0x14
KIND_INTEGER = 0x15
KIND_BOOL = 0x1e
KIND_BLOB = 0x28
KIND_TEXT = 0x32
KIND_UUID = 0x5a
KIND_NEG_TIME = 0x5b
KIND_TIME = 0x5c
KIND_SEP = 0x66
INVERT_TBL = ''.join(chr(c ^ 0xff) for c in xrange(256))

UTCOFFSET_SHIFT = 64 # 16 hours * 4 (15 minute increments)
UTCOFFSET_DIV = 15 * 60 # 15 minutes

_tz_cache = {}


def next_greater(s):
    """Given a bytestring `s`, return the most compact bytestring that is
    greater than any value prefixed with `s`, but lower than any other value.

    ::

        >>> assert next_greater('') == '\\x00'
        >>> assert next_greater('\\x00') == '\\x01'
        >>> assert next_greater('\\xff') == '\\xff\\x00'
        >>> assert next_greater('\\x00\\x00') == '\\x00\\x01')
        >>> assert next_greater('\\x00\\xff') == '\\x01')
        >>> assert next_greater('\\xff\\xff') == '\\x01')

    """
    assert s
    # Based on the Plyvel `bytes_increment()` function.
    s2 = s.rstrip('\xff')
    return s2 and (s2[:-1] + chr(ord(s2[-1]) + 1))


class Key(object):
    """Keys are immutable sequences used as indexes into an ordered collection.
    They behave like tuples, except that elements must be bytestrings, Unicode
    strings, signed integers, ``None``, ``True``, ``False``,
    :py:class:`datetime.datetime` instances, or :py:class:`uuid.UUID`
    instances.

    The key's elements are internally stored using an encoding carefully
    designed to ensure a sort order that closely mirrors a tuple with the same
    elements, and that the representation is as compact as possible. Equality
    tests are implemented as string compares, and so are often faster than
    comparing Python tuples.
    
    Keys may own a private buffer to contain their encoded representation, or
    may borrow it from another object. Since Keys can be constructed directly
    from an encoded representation in a shared buffer, it is possible to work
    with a Key as if it were a plain tuple without ever copying or decoding it.

    The internal encoding is described in :ref:`key-encoding`.
    """
    __slots__ = ['args', 'prefix', 'packed']

    def __new__(cls, *args):
        if len(args) == 1:
            type_ = type(args[0])
            if type_ is Key:
                return args[0]
            elif type_ is tuple:
                args = args[0]
        self = object.__new__(cls)
        self.prefix = ''
        self.args = args or None
        self.packed = packs(args) if args else ''
        return self

    def __init__(self, *args):
        pass

    @classmethod
    def from_hex(cls, hex_, secret=None):
        """Construct a Key from its raw form wrapped in hex. `secret` is
        currently unused."""
        return self.from_raw(hex_.decode('hex'))

    @classmethod
    def from_raw(cls, packed, prefix=None, source=None):
        """Construct a Key from its raw form, skipping the bytestring `prefix`
        at the start.

        If `source` is not ``None``, `packed` must be a :py:class:`buffer` and
        `source` should be a *source object* implementing the `Memsink Protocol
        <https://github.com/dw/acid/issues/23>`_."""
        self = cls()
        self.prefix = prefix or ''
        self.packed = str(packed)
        return self

    def __add__(self, extra):
        new = Key()
        new.prefix = self.prefix
        new.packed = packs(extra, self.packed)
        return new

    __iadd__ = __add__

    def next_greater(self):
        """Return the next possible key that is lexigraphically larger than
        this one. Note the returned Key is only useful to implement compares,
        it may not decode to a valid value."""
        return self.from_raw(next_greater(self.packed[len(self.prefix):]))

    def to_raw(self, prefix=None):
        """Get the bytestring representing this Key, optionally prefixed by
        `prefix`."""
        if prefix is None:
            prefix = ''
        if self.prefix != prefix:
            self.packed = prefix + self.packed[len(self.prefix):]
            self.prefix = prefix
        return self.packed

    def to_hex(self, secret=None):
        """Return :py:func:`to_raw('') <to_raw>` encoded in hex. `secret` is
        currently unused."""
        return self.to_raw().encode('hex')

    def __iter__(self):
        if self.args is None:
            self.args = unpacks(self.packed, self.prefix, True)
        return iter(self.args)

    def __getitem__(self, i):
        try:
            return self.args[i]
        except TypeError:
            self.args = unpacks(self.packed, self.prefix, True)
            return self.args[i]

    def __hash__(self):
        if self.args is None:
            self.args = unpacks(self.prefix, self.packed, True)
        return hash(self.args)

    def __len__(self):
        if self.args is None:
            self.args = unpacks(self.packed, self.prefix, True)
        return len(self.args)

    def __le__(self, other):
        return self.packed <= Key(other).to_raw(self.prefix)

    def __ge__(self, other):
        return self.packed >= Key(other).to_raw(self.prefix)

    def __lt__(self, other):
        return self.packed < Key(other).to_raw(self.prefix)

    def __gt__(self, other):
        return self.packed > Key(other).to_raw(self.prefix)

    def __eq__(self, other):
        return self.packed == Key(other).to_raw(self.prefix)

    def __ne__(self, other):
        return self.packed != Key(other).to_raw(self.prefix)

    def __cmp__(self, other):
        return cmp(self.packed, Key(other).to_raw(self.prefix))

    def __repr__(self):
        if self.args is None:
            self.args = unpacks(self.packed, self.prefix, True)
        return 'acid.Key%r' % (self.args,)


class KeyList(object):
    """Represents a potentially lazy-decoded list of :py:class:`Key` objects.
    """
    @classmethod
    def from_raw(cls, packed, prefix=None, source=None):
        """Produce a :py:class:`KeyList` from the buffer or bytestring
        `packed`, ignoring `prefix` bytes at the start of the string. If
        `prefix` does not match the actual prefix in `packed`, return ``None``.

        If `source` is not ``None``, `packed` must be a :py:class:`buffer` and
        `source` should be a *source object* implementing the `Memsink
        Protocol <https://github.com/dw/acid/issues/23>`_."""
        return unpacks(packed, prefix)


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


def write_int(v, w, xor):
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
        w(xor ^ v)
    elif v <= 2287:
        v -= 240
        w(xor ^ (241 + (v >> 8)))
        w(xor ^ (v & 0xff))
    elif v <= 67823:
        v -= 2288
        w(xor ^ (0xf9))
        w(xor ^ ((v >> 8)))
        w(xor ^ ((v & 0xff)))
    elif v <= 0xffffff:
        w(xor ^ 0xfa)
        w(xor ^ (v >> 16))
        w(xor ^ ((v >> 8) & 0xff))
        w(xor ^ ((v & 0xff)))
    elif v <= 0xffffffff:
        w(xor ^ 0xfb)
        w(xor ^ (v >> 24))
        w(xor ^ ((v >> 16) & 0xff))
        w(xor ^ ((v >> 8) & 0xff))
        w(xor ^ ((v & 0xff)))
    elif v <= 0xffffffffff:
        w(xor ^ 0xfc)
        w(xor ^ ((v >> 32)))
        w(xor ^ ((v >> 24) & 0xff))
        w(xor ^ ((v >> 16) & 0xff))
        w(xor ^ ((v >> 8) & 0xff))
        w(xor ^ ((v & 0xff)))
    elif v <= 0xffffffffffff:
        w(xor ^ 0xfd)
        w(xor ^ ((v >> 40)))
        w(xor ^ ((v >> 32) & 0xff))
        w(xor ^ ((v >> 24) & 0xff))
        w(xor ^ ((v >> 16) & 0xff))
        w(xor ^ ((v >> 8) & 0xff))
        w(xor ^ ((v & 0xff)))
    elif v <= 0xffffffffffffff:
        w(xor ^ 0xfe)
        w(xor ^ ((v >> 48)))
        w(xor ^ ((v >> 40) & 0xff))
        w(xor ^ ((v >> 32) & 0xff))
        w(xor ^ ((v >> 24) & 0xff))
        w(xor ^ ((v >> 16) & 0xff))
        w(xor ^ ((v >> 8) & 0xff))
        w(xor ^ ((v & 0xff)))
    elif v <= 0xffffffffffffffff:
        w(xor ^ 0xff)
        w(xor ^ (v >> 56))
        w(xor ^ ((v >> 48) & 0xff))
        w(xor ^ ((v >> 40) & 0xff))
        w(xor ^ ((v >> 32) & 0xff))
        w(xor ^ ((v >> 24) & 0xff))
        w(xor ^ ((v >> 16) & 0xff))
        w(xor ^ ((v >> 8) & 0xff))
        w(xor ^ ((v & 0xff)))
    else:
        raise ValueError('Cannot encode integers >= 64 bits, got %d bits (%#x)'
                         % (v.bit_length(), v))


def read_int(inp, pos, length, xor):
    """Decode and return an integer encoded by :py:func:`write_int`. Invokes
    `getc` repeatedly, which should yield integer bytes from the input stream.
    """
    o = xor ^ inp[pos]
    if o <= 240:
        return o, pos+1

    have = length - pos
    if o <= 248:
        if have < 2:
            raise ValueError('not enough bytes: need 2')
        o2 = xor ^ inp[pos+1]
        return 240 + (256 * (o - 241) + o2), pos+2

    if have < (o - 249):
        raise ValueError('not enough bytes: need %d' % (o - 249))

    if o == 249:
        return 2288 + (256*(xor^inp[pos+1])) + (xor^inp[pos+2]), pos+3
    elif o == 250:
        return ((xor^(inp[pos+1] << 16)) |
                (xor^(inp[pos+2] << 8)) |
                (xor^(inp[pos+3]))), pos+4
    elif o == 251:
        return ((xor^(inp[pos+1] << 24)) |
                (xor^(inp[pos+2] << 16)) |
                (xor^(inp[pos+3] << 8)) |
                (xor^(inp[pos+4]))), pos+5
    elif o == 252:
        return ((xor^(inp[pos+1] << 32)) |
                (xor^(inp[pos+2] << 24)) |
                (xor^(inp[pos+3] << 16)) |
                (xor^(inp[pos+4] << 8)) |
                (xor^(inp[pos+5]))), pos+6
    elif o == 253:
        return ((xor^(inp[pos+1] << 40)) |
                (xor^(inp[pos+2] << 32)) |
                (xor^(inp[pos+3] << 24)) |
                (xor^(inp[pos+4] << 16)) |
                (xor^(inp[pos+5] << 8)) |
                (xor^(inp[pos+6]))), pos+7
    elif o == 254:
        return ((xor^(inp[pos+1] << 48)) |
                (xor^(inp[pos+2] << 40)) |
                (xor^(inp[pos+3] << 32)) |
                (xor^(inp[pos+4] << 24)) |
                (xor^(inp[pos+5] << 16)) |
                (xor^(inp[pos+6] << 8)) |
                (xor^(inp[pos+7]))), pos+8
    elif o == 255:
        return ((xor^(inp[pos+1] << 56)) |
                (xor^(inp[pos+2] << 48)) |
                (xor^(inp[pos+3] << 40)) |
                (xor^(inp[pos+4] << 32)) |
                (xor^(inp[pos+5] << 24)) |
                (xor^(inp[pos+6] << 16)) |
                (xor^(inp[pos+7] << 8)) |
                (xor^(inp[pos+8]))), pos+9


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
        write_int(-msec, w, 0xff)
    else:
        w(KIND_TIME)
        write_int(msec, w, 0)


def read_time(kind, inp, pos, length):
    xor = 0xff if kind == KIND_NEG_TIME else 0
    msec, pos = read_int(inp, pos, length, xor)
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


def pack_int(i, prefix=None):
    """Invoke :py:func:`write_int(i, ba) <write_int>` using a temporary
    :py:class:`bytearray` initialized to contain `prefix`, returning the result
    as a bytestring."""
    ba = bytearray(prefix or '')
    write_int(i, ba.append, 0)
    return str(ba)


def unpack_int(s):
    """Invoke :py:func:`read_int`, wrapping `s` in a temporary iterator."""
    ba = bytearray(s)
    v, pos = read_int(ba, 0, len(ba), 0)
    return v


GOOD_TYPES = (tuple, Key)

def packs(tups, prefix=None):
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
    ba = bytearray(prefix or '')
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
                    write_int(-arg, w, 0xff)
                else:
                    w(KIND_INTEGER)
                    write_int(arg, w, 0)
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
                write_int(arg, w, 0)
            elif type_ is datetime.datetime:
                write_time(arg, w)
            else:
                raise TypeError('unsupported type: %r' % (arg,))
    return str(ba)


pack = packs


def unpacks(s, prefix=None, first=False):
    """Decode a bytestring produced by :py:func:`keylib.packs`, returning the
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
    if not prefix:
        prefix = ''
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
            arg, pos = read_int(inp, pos, length, 0)
        elif c == KIND_NEG_INTEGER:
            arg, pos = read_int(inp, pos, length, 0xff)
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


def unpack(s, prefix=None):
    return unpacks(s, prefix, True)


# Hack: disable speedups while testing or reading docstrings.
if os.path.basename(sys.argv[0]) not in ('sphinx-build', 'pydoc') and \
        os.getenv('ACID_NO_SPEEDUPS') is None:
    try:
        from acid._keylib import *
    except ImportError:
        pass
