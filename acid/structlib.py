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

from __future__ import absolute_import
import functools
import io
import socket


# Wire type constants.
WIRE_TYPE_VARIABLE     = 0
WIRE_TYPE_64           = 1
WIRE_TYPE_DELIMITED    = 2
WIRE_TYPE_32           = 5

INT32_MAX = (1 << 31) - 1
INT32_MIN = -1 << 31

INT64_MAX = (1 << 63) - 1
INT64_MIN = -(1 << 63)
UINT64_MAX = (1 << 64) - 1


#
# Encoding functions.
#

def exact_read(fp, n=1):
    s = fp.read(n)
    if len(s) == n:
        return s
    raise ValueError('could not exact read %d bytes' % (n,))


def read_varint(r):
    number = 0
    shift = 0

    while True:
        ch = r()
        if (not ch) and (not number):
            raise EOFError

        byte = ord(ch)
        number |= (byte & 0x7f) << shift
        shift += 7

        if not byte & 0x80:
            break

    if number > INT64_MAX:
        number -= 1 << 64
    return number


def read_svarint(r):
    value = read_varint(r)
    if value & 1:
        return (value >> 1) ^ ~0

    return value >> 1


def read_key(r):
    i = read_varint(r)
    return i >> 3, i & 0x7


def write_varint(w, i):
    if not i:
        w('\0')
        return

    if not (INT64_MIN <= i <= UINT64_MAX):
        raise ValueError('value too large.')

    if i < 0:
        i += 1 << 64

    while i:
        byte = i & 0x7f
        i >>= 7
        if i:
            byte |= 0x80
        w(chr(byte))


def write_svarint(w, i):
    if i < 0:
        write_varint(w, (i << 1) ^ ~0)
    else:
        write_varint(w, i << 1)


def write_key(w, field, tag):
    write_varint(w, (field << 3) | tag)


#
# Field coder types.
#

class _Coder(object):
    def write_value(self, field, o, w):
        pass

    def read_value(self, field, dct, r):
        pass


class _ScalarCoder(_Coder):
    def write_value(self, field, o, w):
        write_key(w, field.field_id, field.WIRE_TYPE)
        field.write(o, w)

    def read_value(self, field, dct, r):
        dct[field.name] = field.read(r)


class _PackedCoder(_Coder):
    def write_value(self, field, o, w):
        bio = io.BytesIO()
        for elem in o:
            field.write(elem, bio.write)

        s = bio.getvalue()
        write_key(w, field.field_id, WIRE_TYPE_DELIMITED)
        write_varint(w, len(s))
        w(s)

    def read_value(self, field, dct, r):
        n = read_varint(r)
        bio = io.BytesIO(r(n))
        l = []
        while bio.tell() < n:
            l.append(field.read(r))
        dct[field.name] = l


class _FixedPackedCoder(_Coder):
    def __init__(self, item_size):
        self.item_size = item_size

    def write_value(self, field, o, w):
        write_key(w, field.field_id, WIRE_TYPE_DELIMITED)
        write_varint(self.item_size * len(o))
        for elem in o:
            field.write(elem, w)

    def read_value(self, field, dct, r):
        n = read_varint(r) / self.item_size
        dct[field.name] = [field.read(r) for _ in xrange(n)]


class _DelimitedCoder(_Coder):
    def write_value(self, field, o, w):
        for elem in o:
            write_key(w, field.field_id, WIRE_TYPE_DELIMITED)
            field.write(elem, w)

    def read_value(self, field, dct, r):
        l = dct.setdefault(field.name, [])
        l.append(field.read(r))


#
# Field types.
#

class _Field(object):
    TYPES = ()
    WIRE_TYPE = None
    KIND = None

    def __init__(self, field_id, name):
        self.field_id = field_id
        self.name = name

    def readseq(self, r, insert):
        for i in xrange(readvarint(r)):
            insert(self.read(r))


class _BoolField(_Field):
    TYPES = (bool,)
    KIND = 'bool'
    WIRE_TYPE = WIRE_TYPE_VARIABLE
    COLLECTION_CODER = _FixedPackedCoder(1)

    def skip(self, r):
        r(1)

    def read(self, r):
        return bool(r(1))

    def write(self, o, w):
        w(chr(o))


class _DoubleField(_Field):
    TYPES = (float,)
    KIND = 'double'
    WIRE_TYPE = WIRE_TYPE_64
    COLLECTION_CODER = _FixedPackedCoder(8)

    def skip(self, r):
        r(8)

    def read(self, r):
        return struct.unpack('d', r(8))[0]

    def write(self, o, w):
        w(struct.pack('d', o))


class _FixedIntegerField(_Field):
    TYPES = (int, long)

    def skip(self, r):
        r(self.SIZE)

    def read(self, r):
        return struct.unpack(self.FORMAT, r(self.SIZE))[0]

    def write(self, o, w):
        w(struct.pack(self.FORMAT, o))


class _Fixed32Field(_FixedIntegerField):
    KIND = 'fixed32'
    SIZE = 4
    FORMAT = '<l'
    WIRE_TYPE = WIRE_TYPE_32
    COLLECTION_CODER = _FixedPackedCoder(SIZE)


class _Fixed64Field(_FixedIntegerField):
    KIND = 'fixed64'
    SIZE = 8
    FORMAT = '<q'
    WIRE_TYPE = WIRE_TYPE_64
    COLLECTION_CODER = _FixedPackedCoder(SIZE)


class _FixedU32Field(_FixedIntegerField):
    KIND = 'fixedu32'
    SIZE = 4
    FORMAT = '<L'
    WIRE_TYPE = WIRE_TYPE_32
    COLLECTION_CODER = _FixedPackedCoder(SIZE)


class _FixedU64Field(_FixedIntegerField):
    KIND = 'fixedu64'
    SIZE = 8
    FORMAT = '<Q'
    WIRE_TYPE = WIRE_TYPE_64
    COLLECTION_CODER = _FixedPackedCoder(SIZE)


class _FloatField(_Field):
    TYPES = (float,)
    KIND = 'float'
    WIRE_TYPE = WIRE_TYPE_32
    COLLECTION_CODER = _FixedPackedCoder(4)

    def skip(self, r):
        r(4)

    def read(self, r):
        return struct.unpack('f', r(4))[0]

    def write(self, o, w):
        w(struct.pack('f', o))


class _VarField(_Field):
    TYPES = (int, long)
    WIRE_TYPE = WIRE_TYPE_VARIABLE
    COLLECTION_CODER = _PackedCoder()

    def skip(self, r):
        read_varint(r)


class _IntField(_VarField):
    KIND = 'varint'

    def read(self, r):
        return read_varint(r)

    def write(self, o, w):
        write_varint(w, o)


class _SintField(_Field):
    KIND = 'svarint'

    def read(self, r):
        return read_svarint(r)

    def write(self, o, w):
        write_svarint(w, o)


class _Inet4Field(_Field):
    TYPES = (basestring,)
    KIND = 'inet4'
    WIRE_TYPE = WIRE_TYPE_32
    COLLECTION_CODER = _FixedPackedCoder(4)

    def skip(self, r):
        r(4)

    def read(self, r):
        return socket.inet_ntop(socket.AF_INET, r(4))

    def write(self, o, w):
        w(socket.inet_pton(socket.AF_INET, o))


class _Inet4PortField(_Field):
    TYPES = (basestring,)
    KIND = 'inet4port'
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    COLLECTION_CODER = _FixedPackedCoder(6)

    def skip(self, r):
        r(7)

    def read(self, r):
        r(1)
        addr = socket.inet_ntop(socket.AF_INET, r(4))
        port = struct.unpack('<H', r(2))
        return '%s:%s' % (addr, port)

    def write(self, o, w):
        addr, sep, port = o.rpartition(':')
        if not (sep and port.isdigit()):
            raise ValueError('bad inet4port format')
        w(6)
        w(socket.inet_pton(socket.AF_INET, addr))
        w(struct.pack('>H', port))


class _Inet6Field(_Field):
    TYPES = (basestring,)
    KIND = 'inet6'
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    COLLECTION_CODER = _FixedPackedCoder(16)

    def skip(self, r):
        r(17)

    def read(self, r):
        r(1)
        return socket.inet_ntop(socket.AF_INET6, r(16))

    def write(self, o, w):
        w(16)
        w(socket.inet_pton(socket.AF_INET6, o))


class _Inet6PortField(_Field):
    TYPES = (basestring,)
    KIND = 'inet6port'
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    COLLECTION_CODER = _FixedPackedCoder(18)

    def skip(self, r):
        r(19)

    def read(self, r):
        r(1)
        addr = socket.inet_ntop(socket.AF_INET6, r(16))
        port = struct.unpack('<H', r(2))
        return '%s:%s' % (addr, port)

    def write(self, o, w):
        addr, sep, port = o.rpartition(':')
        if not (sep and port.isdigit()):
            raise ValueError('bad inet6port format')
        w(18)
        w(socket.inet_pton(socket.AF_INET6, addr))
        w(struct.pack('>H', port))


class _BytesField(_Field):
    TYPES = (bytes,)
    KIND = 'bytes'
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    COLLECTION_CODER = _DelimitedCoder()

    def skip(self, r):
        r(read_varint(r))

    def read(self, r):
        return r(read_varint(r))

    def write(self, o, w):
        write_varint(w, len(o))
        w(o)


class _StringField(_Field):
    TYPES = (unicode,)
    KIND = 'str'
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    COLLECTION_CODER = _DelimitedCoder()

    def skip(self, r):
        r(read_varint(r))

    def read(self, r):
        return r(read_varint(r)).decode('utf-8')

    def write(self, o, w):
        e = o.encode('utf-8')
        write_varint(w, len(e))
        w(e)


class _StructField(_Field):
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    COLLECTION_CODER = _DelimitedCoder()

    def __init__(self, field_id, name, struct_type):
        super(_StructField, self).__init__(field_id, name)
        self.struct_type = struct_type
        self.TYPES = (struct_type,)

    def skip(self, r):
        r(read_varint(r))

    def read(self, r):
        n = read_varint(r)
        return Struct.from_raw(self.struct_type, r(n))

    def write(self, o, w):
        s = o.to_raw()
        write_varint(w, len(s))
        w(s)


class StructType(object):
    def __init__(self):
        self.fields = []
        self.field_map = {}

    def add_field(self, field_name, field_id, kind, collection):
        if len(self.fields) < (1 + field_id):
            self.fields += [None] * ((1 + field_id) - len(self.fields))
        if self.fields[field_id] is not None:
            raise ValueError('duplicate field ID: %r' % (field_id,))
        if field_name in self.field_map:
            raise ValueError('duplicate field name: %r' % (field_name,))
        klass = FIELD_KINDS.get(kind)
        if klass is None:
            raise ValueError('unknown kind: %r' % (kind,))

        field = klass(field_id, field_name)
        if collection:
            field.coder = field.COLLECTION_CODER
        else:
            field.coder = _ScalarCoder()

        self.fields[field_id] = field
        self.field_map[field_name] = field

    def _skip(self, r, tag):
        if tag == WIRE_TYPE_VARIABLE:
            read_varint(r)
        elif tag == WIRE_TYPE_64:
            r(8)
        elif tag == WIRE_TYPE_DELIMITED:
            r(read_varint(r))
        elif tag == WIRE_TYPE_32:
            r(4)
        assert 0

    def _from_raw(self, buf):
        dct = {}
        bio = io.BytesIO(buf)
        r = functools.partial(exact_read, bio)
        flen = len(self.fields)
        blen = len(buf)
        while bio.tell() < blen:
            field_id, tag = read_key(r)
            if field_id < flen and self.fields[field_id]:
                field = self.fields[field_id]
                field.coder.read_value(field, dct, r)
            else:
                self._skip(r, tag)
        return dct

    def _to_raw(self, dct):
        bio = io.BytesIO()
        for name, value in dct.iteritems():
            field = self.field_map[name]
            field.coder.write_value(field, value, bio.write)
        return bio.getvalue()


class Struct(object):
    __slots__ = ('struct_type', 'buf', 'dct')
    def __init__(self, struct_type):
        self.struct_type = struct_type
        self.buf = ''
        self.dct = {}

    @classmethod
    def from_raw(cls, struct_type, buf, source=None):
        self = cls(struct_type)
        self.buf = buf
        self.reset()
        return self

    def reset(self):
        self.dct = self.struct_type._from_raw(self.buf)

    def to_raw(self):
        return self.struct_type._to_raw(self.dct)

    def __getitem__(self, key):
        return self.dct[key]

    def __setitem__(self, key, value):
        field_type = self.struct_type.field_map.get(key)
        if field_type is None:
            raise TypeError('struct_type has no %r key' % (key,))
        if not isinstance(value, field_type.TYPES):
            raise TypeError('struct_type requires %r, not %r' %\
                            (field_type.TYPES, type(value)))
        self.dct[key] = value

    def __delitem__(self, key):
        del self.dct[key]

    def __repr__(self):
        typ = type(self)
        return '<%s.%s(%s)>' % (typ.__module__, typ.__name__, self.dct)


#: Mapping of _Field subclass to field kind.
FIELD_KINDS = {}
_stack = [_Field]
while _stack:
    _k = _stack.pop()
    _stack.extend(_k.__subclasses__())
    if _k.KIND:
        FIELD_KINDS[_k.KIND] = _k
del _k, _stack
