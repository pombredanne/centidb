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
import struct


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

_undefined = object()


#
# Encoding functions.
#

def read_varint(buf, pos):
    number = 0
    shift = 0

    while True:
        byte = ord(buf[pos])
        pos += 1

        number |= (byte & 0x7f) << shift
        shift += 7

        if not byte & 0x80:
            break

    if number > INT64_MAX:
        number -= 1 << 64
    return pos, number


def read_svarint(buf, pos):
    pos, value = read_varint(buf, pos)
    if value & 1:
        return pos, ((value >> 1) ^ ~0)
    return pos, value >> 1


def read_key(buf, pos):
    pos, i = read_varint(buf, pos)
    return pos, i >> 3, i & 0x7


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

    def read_value(self, field, dct, buf, pos):
        return pos, None


class _ScalarCoder(_Coder):
    def write_value(self, field, o, w):
        write_key(w, field.field_id, field.WIRE_TYPE)
        field.write(o, w)

    def read_value(self, field, dct, buf, pos):
        pos, dct[field.name] = field.read(buf, pos)
        return pos


class _PackedCoder(_Coder):
    def write_value(self, field, o, w):
        bio = io.BytesIO()
        for elem in o:
            field.write(elem, bio.write)

        s = bio.getvalue()
        write_key(w, field.field_id, WIRE_TYPE_DELIMITED)
        write_varint(w, len(s))
        w(s)

    def read_value(self, field, dct, buf, pos):
        pos, n = read_varint(buf, pos)
        bio = memoryview(buf)[pos:pos+n]
        bpos = 0
        l = []
        while bpos < n:
            bpos, value = field.read(bio, bpos)
            l.append(value)
        dct[field.name] = l
        return pos + n


class _FixedPackedCoder(_Coder):
    def __init__(self, item_size):
        self.item_size = item_size

    def write_value(self, field, o, w):
        write_key(w, field.field_id, WIRE_TYPE_DELIMITED)
        write_varint(self.item_size * len(o))
        for elem in o:
            field.write(elem, w)

    def read_value(self, field, dct, buf, pos):
        pos, n = read_varint(buf, pos)
        l = []
        for _ in xrange(n / self.item_size):
            pos, value = field.read(buf, pos)
            l.append(value)
        dct[field.name] = l
        return pos


class _DelimitedCoder(_Coder):
    def write_value(self, field, o, w):
        for elem in o:
            write_key(w, field.field_id, WIRE_TYPE_DELIMITED)
            field.write(elem, w)

    def read_value(self, field, dct, buf, pos):
        pos, value = field.read(buf, pos)
        dct.setdefault(field.name, []).append(value)
        return pos


#
# Field types.
#

class _Field(object):
    TYPES = ()
    WIRE_TYPE = None
    KIND = None

    def __init__(self, field_id, name, collection):
        self.field_id = field_id
        self.name = name
        self.collection = collection
        if collection:
            self.coder = self.COLLECTION_CODER
            self.type_check = self.type_check_collection
        else:
            self.coder = _ScalarCoder()

    def type_check(self, value):
        if not isinstance(value, self.TYPES):
            raise TypeError('field %r requires %r, not %r' %\
                            (self.name, self.TYPES, type(value)))

    def type_check_collection(self, value):
        if not isinstance(value, list):
            raise TypeError('field %r requires %r, not %r' %\
                            (self.name, list, type(value)))
        if not all(isinstance(v, self.TYPES) for v in value):
            raise TypeError('field %r requires list of %r' %\
                            (self.name, self.TYPES))


class _BoolField(_Field):
    TYPES = (bool,)
    KIND = 'bool'
    WIRE_TYPE = WIRE_TYPE_VARIABLE
    COLLECTION_CODER = _FixedPackedCoder(1)

    def read(self, buf, pos):
        return pos+1, buf[pos] == '\x01'

    def write(self, o, w):
        w(chr(o))


class _DoubleField(_Field):
    TYPES = (float,)
    KIND = 'double'
    WIRE_TYPE = WIRE_TYPE_64
    COLLECTION_CODER = _FixedPackedCoder(8)

    def read(self, buf, pos):
        epos = pos + 8
        return epos, struct.unpack('d', buf[pos:epos])[0]

    def write(self, o, w):
        w(struct.pack('d', o))


class _FixedIntegerField(_Field):
    TYPES = (int, long)

    def read(self, buf, pos):
        epos = pos + self.SIZE
        return epos, struct.unpack(self.FORMAT, buf[pos:epos])[0]

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

    def read(self, buf, pos):
        epos = pos + 4
        return epos, struct.unpack('f', buf[pos:epos])[0]

    def write(self, o, w):
        w(struct.pack('f', o))


class _VarField(_Field):
    TYPES = (int, long)
    WIRE_TYPE = WIRE_TYPE_VARIABLE
    COLLECTION_CODER = _PackedCoder()


class _IntField(_VarField):
    KIND = 'varint'

    def read(self, buf, pos):
        return read_varint(buf, pos)

    def write(self, o, w):
        write_varint(w, o)


class _SintField(_Field):
    KIND = 'svarint'

    def read(self, buf, pos):
        return read_svarint(r)

    def write(self, o, w):
        write_svarint(w, o)


class _Inet4Field(_Field):
    TYPES = (basestring,)
    KIND = 'inet4'
    WIRE_TYPE = WIRE_TYPE_32
    COLLECTION_CODER = _FixedPackedCoder(4)

    def read(self, buf, pos):
        epos = pos + 4
        return epos, socket.inet_ntop(socket.AF_INET, buf[pos:epos])

    def write(self, o, w):
        w(socket.inet_pton(socket.AF_INET, o))


class _Inet4PortField(_Field):
    TYPES = (basestring,)
    KIND = 'inet4port'
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    COLLECTION_CODER = _FixedPackedCoder(6)

    def read(self, buf, pos):
        addr = socket.inet_ntop(socket.AF_INET, buf[pos+1:pos+5])
        port = struct.unpack('<H', buf[pos+5:pos+7])
        return pos+7, '%s:%s' % (addr, port)

    def write(self, o, w):
        addr, sep, port = o.rpartition(':')
        if not (sep and port.isdigit()):
            raise ValueError('bad inet4port format')
        w(6)
        w(socket.inet_pton(socket.AF_INET, addr))
        w(struct.pack('>H', int(port, 10)))


class _Inet6Field(_Field):
    TYPES = (basestring,)
    KIND = 'inet6'
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    COLLECTION_CODER = _FixedPackedCoder(16)

    def read(self, buf, pos):
        epos = pos + 17
        return epos, socket.inet_ntop(socket.AF_INET6, buf[pos+1:epos])

    def write(self, o, w):
        write_varint(w, 16)
        w(socket.inet_pton(socket.AF_INET6, o))


class _Inet6PortField(_Field):
    TYPES = (basestring,)
    KIND = 'inet6port'
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    COLLECTION_CODER = _FixedPackedCoder(18)

    def read(self, buf, pos):
        epos = pos + 19
        addr = socket.inet_ntop(socket.AF_INET6, buf[pos+1:pos+17])
        port, = struct.unpack('<H', buf[pos+17:epos])
        return epos, '%s:%s' % (addr, port)

    def write(self, o, w):
        addr, sep, port = o.rpartition(':')
        if not (sep and port.isdigit()):
            raise ValueError('bad inet6port format')
        write_varint(w, 18)
        w(socket.inet_pton(socket.AF_INET6, addr))
        w(struct.pack('<H', int(port, 10)))


class _BytesField(_Field):
    TYPES = (bytes,)
    KIND = 'bytes'
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    COLLECTION_CODER = _DelimitedCoder()

    def read(self, buf, pos):
        pos, n = read_varint(buf, pos)
        epos = pos + n
        return epos, buf[pos:epos]

    def write(self, o, w):
        write_varint(w, len(o))
        w(o)


class _StringField(_Field):
    TYPES = (unicode,)
    KIND = 'str'
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    COLLECTION_CODER = _DelimitedCoder()

    def read(self, buf, pos):
        pos, n = read_varint(buf, pos)
        epos = pos + n
        return epos, buf[pos:epos].decode('utf-8')

    def write(self, o, w):
        e = o.encode('utf-8')
        write_varint(w, len(e))
        w(e)


class _StructField(_Field):
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    COLLECTION_CODER = _DelimitedCoder()

    def __init__(self, field_id, name, collection, struct_type):
        super(_StructField, self).__init__(field_id, name, collection)
        self.struct_type = struct_type
        self.TYPES = (struct_type,)

    def read(self, buf, pos):
        pos, n = read_varint(r)
        epos = pos + n
        return epos, Struct.from_raw(self.struct_type, buf[pos:epos])

    def write(self, o, w):
        s = o.to_raw()
        write_varint(w, len(s))
        w(s)


class StructType(object):
    def __init__(self):
        self.fields = []
        self.field_map = {}
        self.sorted_ids = []

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

        field = klass(field_id, field_name, collection)
        self.fields[field_id] = field
        self.field_map[field_name] = field
        self.sorted_ids.append(field_id)
        self.sorted_ids.sort()

    def _skip(self, buf, pos, tag):
        if tag == WIRE_TYPE_VARIABLE:
            epos, _ = read_varint(buf, pos)
            return epos
        elif tag == WIRE_TYPE_64:
            return pos + 8
        elif tag == WIRE_TYPE_DELIMITED:
            epos, n = read_varint(buf, pos)
            return epos + n
        elif tag == WIRE_TYPE_32:
            return pos + 4
        assert 0

    def _from_raw(self, buf):
        dct = {}
        flen = len(self.fields)
        blen = len(buf)
        pos = 0
        while pos < blen:
            pos, field_id, tag = read_key(buf, pos)
            if field_id < flen and self.fields[field_id]:
                field = self.fields[field_id]
                pos = field.coder.read_value(field, dct, buf, pos)
            else:
                pos = self._skip(buf, pos, tag)
        return dct

    def _to_raw(self, dct):
        bio = io.BytesIO()
        for field_id in self.sorted_ids:
            field = self.fields[field_id]
            value = dct.get(field.name)
            if value is not None:
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

    def __len__(self):
        return len(self.dct)

    def __getitem__(self, key):
        return self.dct[key]

    def __setitem__(self, key, value):
        field_type = self.struct_type.field_map.get(key)
        if field_type is None:
            raise TypeError('struct_type has no %r key' % (key,))
        field_type.type_check(value)
        self.dct[key] = value

    def __delitem__(self, key):
        del self.dct[key]

    def __contains__(self, key):
        return key in self.dct
    has_key = __contains__

    def __iter__(self):
        return iter(self.dct)

    def clear(self):
        self.dct.clear()

    def copy(self):
        new = type(self)(self.struct_type)
        new.buf = self.buf
        new.dct = self.dct.copy()
        return new

    def get(self, key, default=_undefined):
        value = self.dct.get(key, default)
        if value is _undefined:
            raise KeyError(key)
        return value

    def __repr__(self):
        typ = type(self)
        return '<%s.%s(%s)>' % (typ.__module__, typ.__name__, self.dct)

    def items(self):
        return self.dct.items()

    def iteritems(self):
        return self.dct.iteritems()

    def iterkeys(self):
        return self.dct.iterkeys()

    def itervalues(self):
        return self.dct.itervalues()

    def keys(self):
        return self.dct.keys()

    def pop(self, key, default=_undefined):
        value = self.dct.pop(key, default)
        if default is _undefined:
            raise KeyError(key)
        return value

    def popitem(self):
        return self.dct.popitem()

    def setdefault(self, key, default=None):
        return self.dct.setdefault(key, default)

    def update(self, other):
        for key in other:
            self[key] = other[key]

    def values(self):
        return self.dct.values()


#: Mapping of _Field subclass to field kind.
FIELD_KINDS = {}
_stack = [_Field]
while _stack:
    _k = _stack.pop()
    _stack.extend(_k.__subclasses__())
    if _k.KIND:
        FIELD_KINDS[_k.KIND] = _k
del _k, _stack
