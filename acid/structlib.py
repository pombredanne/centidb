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
import io
import socket


# Wire type constants.
LENGTH_VARIABLE     = 0
LENGTH_64           = 1
LENGTH_DELIMITED    = 2
LENGTH_32           = 5

INT32_MAX = (1 << 31) - 1
INT32_MIN = -1 << 31

INT64_MAX = (1 << 63) - 1
INT64_MIN = -(1 << 63)
UINT64_MAX = (1 << 64) - 1


#
# Encoding functions.
#

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

class FieldCoder(object):
    def write_value(self, field, o, w):
        pass

    def read_value(self, field, dct, r):
        pass


class ScalarFieldCoder(FieldCoder):
    def write_value(self, field, o, w):
        write_key(w, field.field_id, field.WIRE_TYPE)
        field.write(o, w)

    def read_value(self, field, dct, r):
        dct[field.name] = field.read(r)


class PackedFieldCoder(FieldCoder):
    def write_value(self, field, o, w):
        bio = io.BytesIO()
        for elem in o:
            field.write(elem, bio.write)

        s = bio.getvalue()
        write_key(w, field.field_id, LENGTH_DELIMITED)
        write_varint(w, len(s))
        w(s)

    def read_value(self, field, dct, r):
        n = read_varint(r)
        bio = io.BytesIO(r(n))
        l = []
        while bio.tell() < n:
            l.append(field.read(r))
        dct[field.name] = l


class FixedPackedFieldCoder(FieldCoder):
    def __init__(self, item_size):
        self.item_size = item_size

    def write_value(self, field, o, w):
        write_key(w, field.field_id, LENGTH_DELIMITED)
        write_varint(self.item_size * len(o))
        for elem in o:
            field.write(elem, w)

    def read_value(self, field, dct, r):
        n = read_varint(r) / self.item_size
        dct[field.name] = [field.read(r) for _ in xrange(n)]


class DelimitedFieldCoder(FieldCoder):
    def write_value(self, field, o, w):
        for elem in o:
            write_key(w, field.field_id, LENGTH_DELIMITED)
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


class _UnpackedField(object):
    def readseq(self, r, insert):
        insert(self.read(r))


class _BoolField(_Field):
    TYPES = (bool,)
    KIND = 'bool'
    WIRE_TYPE = LENGTH_VARIABLE

    def skip(self, r):
        r(1)

    def read(self, r):
        return bool(r(1))

    def write(self, o, w):
        w(chr(o))


class _DoubleField(_Field):
    TYPES = (float,)
    KIND = 'double'
    WIRE_TYPE = LENGTH_64

    def skip(self, r):
        r(8)

    def read(self, r):
        return struct.unpack('d', r(8))[0]

    def write(self, o, w):
        w(struct.pack('d', o))


class _IntegerField(_Field):
    TYPES = (int, long)

    def skip(self, r):
        r(self.SIZE)

    def read(self, r):
        return struct.unpack(self.FORMAT, r(self.SIZE))[0]

    def write(self, o, w):
        w(struct.pack(self.FORMAT, o))


class _Fixed32Field(_IntegerField):
    KIND = 'fixed32'
    SIZE = 4
    FORMAT = '<l'
    WIRE_TYPE = LENGTH_32


class _Fixed64Field(_IntegerField):
    KIND = 'fixed64'
    SIZE = 8
    FORMAT = '<q'
    WIRE_TYPE = LENGTH_64


class _FixedU32Field(_IntegerField):
    KIND = 'fixedu32'
    SIZE = 4
    FORMAT = '<L'
    WIRE_TYPE = LENGTH_32


class _FixedU64Field(_IntegerField):
    KIND = 'fixedu64'
    SIZE = 8
    FORMAT = '<Q'
    WIRE_TYPE = LENGTH_64


class _FloatField(_Field):
    TYPES = (float,)
    KIND = 'float'
    WIRE_TYPE = LENGTH_32

    def skip(self, r):
        r(4)

    def read(self, r):
        return struct.unpack('f', r(4))[0]

    def write(self, o, w):
        w(struct.pack('f', o))


class _Inet4Field(_Field):
    TYPES = (basestring,)
    KIND = 'inet4'
    WIRE_TYPE = LENGTH_32

    def skip(self, r):
        r(4)

    def read(self, r):
        return socket.inet_ntop(socket.AF_INET, r(4))

    def write(self, o, w):
        w(socket.inet_pton(socket.AF_INET, o))


class _Inet4PortField(_Field):
    TYPES = (basestring,)
    KIND = 'inet4port'
    WIRE_TYPE = LENGTH_DELIMITED

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
    WIRE_TYPE = LENGTH_DELIMITED

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
    WIRE_TYPE = LENGTH_DELIMITED

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
    WIRE_TYPE = LENGTH_DELIMITED

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
    WIRE_TYPE = LENGTH_DELIMITED

    def skip(self, r):
        r(read_varint(r))

    def read(self, r):
        return r(read_varint(r)).decode('utf-8')

    def write(self, o, w):
        e = o.encode('utf-8')
        write_varint(w, len(e))
        w(e)


class StructType(object):
    def __init__(self):
        self.fields = []
        self.field_map = {}

    def add_field(self, field_name, field_id, kind, collection):
        if len(self.fields) < (1 + field_id):
            self.fields += [None] * ((1 + field_id) - len(self.fields))
        if self.fields[field_id] is not None:
            raise ValueError('duplicate field ID: %r' % (field_id,))

        klass = FIELD_KINDS.get(kind)
        if klass is None:
            raise ValueError('unknown kind: %r' % (kind,))


class Struct(object):
    __slots__ = ('struct_type', 'dct')
    dct = None
    def __init__(self, struct_type):
        self.struct_type = struct_type



#: Mapping of _Field subclass to field kind.
FIELD_KINDS = {}
_stack = [_Field]
while _stack:
    _k = _stack.pop()
    _stack.extend(_k.__subclasses__())
    if _k.KIND:
        FIELD_KINDS[_k.KIND] = _k
del _k, _stack
