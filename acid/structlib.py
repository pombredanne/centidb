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
import socket
import struct
import types
import uuid

try:
    from __pypy__.builders import StringBuilder
except ImportError:
    StringBuilder = None


# Wire type constants.
WIRE_TYPE_VARIABLE     = 0
WIRE_TYPE_64           = 1
WIRE_TYPE_DELIMITED    = 2
WIRE_TYPE_32           = 5

_undefined = object()


#
# Encoding functions.
#

def read_varint(buf, pos):
    b0 = ord(buf[pos])
    if b0 < 0x80:
        return pos+1, b0

    b1 = ord(buf[pos+1])
    if b1 < 0x80:
        return pos+2, ((b1       ) << 7)  \
                     | (b0 & 0x7f)

    b2 = ord(buf[pos+2])
    if b2 < 0x80:
        return pos+3, ((b2       ) << 14) \
                    | ((b1 & 0x7f) << 7)  \
                    | ((b0 & 0x7f))

    b3 = ord(buf[pos+3])
    if b3 < 0x80:
        return pos+4, ((b3       ) << 21) \
                    | ((b2 & 0x7f) << 14) \
                    | ((b1 & 0x7f) << 7)  \
                    | ((b0 & 0x7f))

    b4 = ord(buf[pos+4])
    if b4 < 0x80:
        return pos+5, ((b4       ) << 28) \
                    | ((b3 & 0x7f) << 21) \
                    | ((b2 & 0x7f) << 14) \
                    | ((b1 & 0x7f) << 7)  \
                    | ((b0 & 0x7f))

    b5 = ord(buf[pos+5])
    if b5 < 0x80:
        return pos+6, ((b5       ) << 35) \
                    | ((b4 & 0x7f) << 28) \
                    | ((b3 & 0x7f) << 21) \
                    | ((b2 & 0x7f) << 14) \
                    | ((b1 & 0x7f) << 7)  \
                    | ((b0 & 0x7f))

    b6 = ord(buf[pos+6])
    if b6 < 0x80:
        return pos+7, ((b6       ) << 42) \
                    | ((b5 & 0x7f) << 35) \
                    | ((b4 & 0x7f) << 28) \
                    | ((b3 & 0x7f) << 21) \
                    | ((b2 & 0x7f) << 14) \
                    | ((b1 & 0x7f) << 7)  \
                    | ((b0 & 0x7f))

    b7 = ord(buf[pos+7])
    if b7 < 0x80:
        return pos+8, ((b7       ) << 49) \
                    | ((b6 & 0x7f) << 42) \
                    | ((b5 & 0x7f) << 35) \
                    | ((b4 & 0x7f) << 28) \
                    | ((b3 & 0x7f) << 21) \
                    | ((b2 & 0x7f) << 14) \
                    | ((b1 & 0x7f) << 7)  \
                    | ((b0 & 0x7f))

    b8 = ord(buf[pos+8])
    if b8 < 0x80:
        return pos+9, ((b8       ) << 56) \
                    | ((b7 & 0x7f) << 49) \
                    | ((b6 & 0x7f) << 42) \
                    | ((b5 & 0x7f) << 35) \
                    | ((b4 & 0x7f) << 28) \
                    | ((b3 & 0x7f) << 21) \
                    | ((b2 & 0x7f) << 14) \
                    | ((b1 & 0x7f) << 7)  \
                    | ((b0 & 0x7f))

    b9 = ord(buf[pos+9])
    if b9 < 0x80:
        n =            ((b9 & 0x01) << 63) \
                     | ((b8 & 0x7f) << 56) \
                     | ((b7 & 0x7f) << 49) \
                     | ((b6 & 0x7f) << 42) \
                     | ((b5 & 0x7f) << 35) \
                     | ((b4 & 0x7f) << 28) \
                     | ((b3 & 0x7f) << 21) \
                     | ((b2 & 0x7f) << 14) \
                     | ((b1 & 0x7f) << 7)  \
                     | ((b0 & 0xff))
        if n > (2**63-1):  # exceeds int64_t, must be signed, so cast it
            n -= (1 << 64)
        return pos+10, n

    assert 0


def write_varint(w, i):
    if i < 0:
        i &= (2**64-1)  # Cast to uint64_t

    if i < (2**7):
        w(chr(               (i)))
    elif i < (2**14):
        w(chr(0x80 | (0x7f & (i))))
        w(chr(               (i >> 7)))
    elif i < (2**21):
        w(chr(0x80 | (0x7f & (i))))
        w(chr(0x80 | (0x7f & (i >> 7))))
        w(chr(               (i >> 14)))
    elif i < (2**28):
        w(chr(0x80 | (0x7f & (i))))
        w(chr(0x80 | (0x7f & (i >> 7))))
        w(chr(0x80 | (0x7f & (i >> 14))))
        w(chr(               (i >> 21)))
    elif i < (2**35):
        w(chr(0x80 | (0x7f & (i))))
        w(chr(0x80 | (0x7f & (i >> 7))))
        w(chr(0x80 | (0x7f & (i >> 14))))
        w(chr(0x80 | (0x7f & (i >> 21))))
        w(chr(               (i >> 28)))
    elif i < (2**42):
        w(chr(0x80 | (0x7f & (i))))
        w(chr(0x80 | (0x7f & (i >> 7))))
        w(chr(0x80 | (0x7f & (i >> 14))))
        w(chr(0x80 | (0x7f & (i >> 21))))
        w(chr(0x80 | (0x7f & (i >> 28))))
        w(chr(               (i >> 35)))
    elif i < (2**49):
        w(chr(0x80 | (0x7f & (i))))
        w(chr(0x80 | (0x7f & (i >> 7))))
        w(chr(0x80 | (0x7f & (i >> 14))))
        w(chr(0x80 | (0x7f & (i >> 21))))
        w(chr(0x80 | (0x7f & (i >> 28))))
        w(chr(0x80 | (0x7f & (i >> 35))))
        w(chr(               (i >> 42)))
    elif i < (2**56):
        w(chr(0x80 | (0x7f & (i))))
        w(chr(0x80 | (0x7f & (i >> 7))))
        w(chr(0x80 | (0x7f & (i >> 14))))
        w(chr(0x80 | (0x7f & (i >> 21))))
        w(chr(0x80 | (0x7f & (i >> 28))))
        w(chr(0x80 | (0x7f & (i >> 35))))
        w(chr(0x80 | (0x7f & (i >> 42))))
        w(chr(               (i >> 49)))
    elif i < (2**63):
        w(chr(0x80 | (0x7f & (i))))
        w(chr(0x80 | (0x7f & (i >> 7))))
        w(chr(0x80 | (0x7f & (i >> 14))))
        w(chr(0x80 | (0x7f & (i >> 21))))
        w(chr(0x80 | (0x7f & (i >> 28))))
        w(chr(0x80 | (0x7f & (i >> 35))))
        w(chr(0x80 | (0x7f & (i >> 42))))
        w(chr(0x80 | (0x7f & (i >> 49))))
        w(chr(               (i >> 56)))
    elif i < (2**64):
        w(chr(0x80 | (0x7f & (i))))
        w(chr(0x80 | (0x7f & (i >> 7))))
        w(chr(0x80 | (0x7f & (i >> 14))))
        w(chr(0x80 | (0x7f & (i >> 21))))
        w(chr(0x80 | (0x7f & (i >> 28))))
        w(chr(0x80 | (0x7f & (i >> 35))))
        w(chr(0x80 | (0x7f & (i >> 42))))
        w(chr(0x80 | (0x7f & (i >> 49))))
        w(chr(0x80 | (0x7f & (i >> 56))))
        w(chr(               (i >> 63)))
    else:
        raise ValueError('value too large.')


def read_svarint(buf, pos):
    pos, value = read_varint(buf, pos)
    if value & 1:
        return pos, ((value >> 1) ^ ~0)
    return pos, value >> 1


def write_svarint(w, i):
    if i < 0:
        write_varint(w, (i << 1) ^ ~0)
    else:
        write_varint(w, i << 1)


def write_key(w, field, tag):
    write_varint(w, (field << 3) | tag)


#
# Skipping functions.
#

def _skip_variable(buf, pos):
    while buf[pos] >= '\x80':
        pos += 1
    return pos + 1

def _skip_64(buf, pos):
    return pos + 8

def _skip_delimited(buf, pos):
    epos, n = read_varint(buf, pos)
    return epos + n

def _skip_32(buf, pos):
    return pos + 4

def _skip_unknown(buf, pos):
    raise IOError('cannot skip unrecognized WIRE_TYPE')

SKIP_MAP = [
    _skip_variable,     # 0
    _skip_64,           # 1
    _skip_delimited,    # 2
    _skip_unknown,      # 3
    _skip_unknown,      # 4
    _skip_32,           # 5
    _skip_unknown,      # 6
    _skip_unknown,      # 7
]


#
# Field coder types.
#

class _Coder(object):
    def write_value(self, field, o, w):
        pass

    def read_value(self, field, buf, pos):
        return pos


class _ScalarCoder(_Coder):
    def make_key(self, field):
        return (field.field_id << 3) | field.WIRE_TYPE

    def write_value(self, field, o, w):
        write_key(w, field.field_id, field.WIRE_TYPE)
        field.write(o, w)

    def read_value(self, field, buf, pos):
        return field.read(buf, pos)


class _PackedCoder(_Coder):
    def make_key(self, field):
        return (field.field_id << 3) | WIRE_TYPE_DELIMITED

    if StringBuilder:
        def write_value(self, field, o, w):
            bio = StringBuilder()
            ww = bio.append
            for elem in o:
                field.write(elem, ww)

            s = bio.build()
            write_key(w, field.field_id, WIRE_TYPE_DELIMITED)
            write_varint(w, len(s))
            w(s)
    else:
        def write_value(self, field, o, w):
            ba = bytearray()
            ww = ba.extend
            for elem in o:
                field.write(elem, ww)

            s = str(ba)
            write_key(w, field.field_id, WIRE_TYPE_DELIMITED)
            write_varint(w, len(s))
            w(s)

    def read_value(self, field, buf, pos):
        pos, n = read_varint(buf, pos)
        bio = memoryview(buf)[pos:pos+n]
        bpos = 0
        l = []
        while bpos < n:
            bpos, value = field.read(bio, bpos)
            l.append(value)
        return pos + n, l


class _FixedPackedCoder(_Coder):
    def __init__(self, item_size):
        self.item_size = item_size

    def make_key(self, field):
        return (field.field_id << 3) | WIRE_TYPE_DELIMITED

    def write_value(self, field, o, w):
        write_key(w, field.field_id, WIRE_TYPE_DELIMITED)
        write_varint(self.item_size * len(o))
        for elem in o:
            field.write(elem, w)

    def read_value(self, field, buf, pos):
        pos, n = read_varint(buf, pos)
        l = []
        for _ in xrange(n / self.item_size):
            pos, value = field.read(buf, pos)
            l.append(value)
        return pos + n, l


class _DelimitedCoder(_Coder):
    def make_key(self, field):
        return (field.field_id << 3) | WIRE_TYPE_DELIMITED

    def write_value(self, field, o, w):
        for elem in o:
            write_key(w, field.field_id, WIRE_TYPE_DELIMITED)
            field.write(elem, w)

    def read_value(self, field, buf, pos):
        l = []
        blen = len(buf)
        wire_key = field.wire_key
        pos2 = pos
        while wire_key == field.wire_key and pos < blen:
            pos, value = field.read(buf, pos2)
            l.append(value)
            pos2, wire_key = read_varint(buf, pos)

        return pos, l


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
        self.wire_key = self.coder.make_key(self)

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
    TYPES = (bool, types.NoneType)
    KIND = 'bool'
    WIRE_TYPE = WIRE_TYPE_VARIABLE
    COLLECTION_CODER = _FixedPackedCoder(1)

    def read(self, buf, pos):
        return pos+1, buf[pos] == '\x01'

    def write(self, o, w):
        w(chr(o))


class _DoubleField(_Field):
    TYPES = (float, types.NoneType)
    KIND = 'double'
    WIRE_TYPE = WIRE_TYPE_64
    COLLECTION_CODER = _FixedPackedCoder(8)

    def read(self, buf, pos):
        epos = pos + 8
        return epos, struct.unpack('d', buf[pos:epos])[0]

    def write(self, o, w):
        w(struct.pack('d', o))


class _FixedIntegerField(_Field):
    TYPES = (int, long, types.NoneType)

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
    TYPES = (float, types.NoneType)
    KIND = 'float'
    WIRE_TYPE = WIRE_TYPE_32
    COLLECTION_CODER = _FixedPackedCoder(4)

    def read(self, buf, pos):
        epos = pos + 4
        return epos, struct.unpack('f', buf[pos:epos])[0]

    def write(self, o, w):
        w(struct.pack('f', o))


class _VarField(_Field):
    TYPES = (int, long, types.NoneType)
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
    TYPES = (basestring, types.NoneType)
    KIND = 'inet4'
    WIRE_TYPE = WIRE_TYPE_32
    COLLECTION_CODER = _FixedPackedCoder(4)

    def read(self, buf, pos):
        epos = pos + 4
        return epos, socket.inet_ntop(socket.AF_INET, buf[pos:epos])

    def write(self, o, w):
        w(socket.inet_pton(socket.AF_INET, o))


class _Inet4PortField(_Field):
    TYPES = (basestring, types.NoneType)
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
    TYPES = (basestring, types.NoneType)
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
    TYPES = (basestring, types.NoneType)
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
    TYPES = (bytes, types.NoneType)
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
    TYPES = (unicode, types.NoneType)
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


class _UuidField(_Field):
    TYPES = (uuid.UUID, types.NoneType)
    KIND = 'uuid'
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    COLLECTION_CODER = _DelimitedCoder()

    def read(self, buf, pos):
        pos, n = read_varint(buf, pos)
        epos = pos + n
        return epos, uuid.UUID(None, buf[pos:epos])

    def write(self, o, w):
        write_varint(w, 16)
        w(o.get_bytes())


class _StructField(_Field):
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    COLLECTION_CODER = _DelimitedCoder()

    def __init__(self, field_id, name, collection, struct_type):
        super(_StructField, self).__init__(field_id, name, collection)
        self.struct_type = struct_type
        self.TYPES = (struct_type, types.NoneType)

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
        self.field_name_map = {}
        self.field_id_map = {}
        self.sorted_by_id = []

    def add_field(self, field_name, field_id, kind, collection):
        if field_id in self.field_id_map:
            raise ValueError('duplicate field ID: %r' % (field_id,))
        if field_name in self.field_name_map:
            raise ValueError('duplicate field name: %r' % (field_name,))
        klass = FIELD_KINDS.get(kind)
        if klass is None:
            raise ValueError('unknown kind: %r' % (kind,))

        field = klass(field_id, field_name, collection)
        self.field_id_map[field_id] = field
        self.field_name_map[field_name] = field
        self.sorted_by_id.append(field)
        self.sorted_by_id.sort(key=lambda f: f.field_id)

    def iter_values(self, buf):
        blen = len(buf)
        pos = 0
        while pos < blen:
            pos, wire_key = read_varint(buf, pos)
            field = self.field_id_map.get(wire_key >> 3)
            if field:
                pos, value = field.coder.read_value(field, buf, pos)
                yield field.name, value
            else:
                pos = SKIP_MAP[wire_key & 0x7](buf, pos)

    def read_value(self, buf, field):
        target_wire_key = field.wire_key
        blen = len(buf)
        pos = 0
        while pos < blen:
            pos, wire_key = read_varint(buf, pos)
            if wire_key == target_wire_key:
                _, value = field.coder.read_value(field, buf, pos)
                return value
            pos = SKIP_MAP[wire_key & 0x7](buf, pos)

    if StringBuilder:
        def _to_raw(self, dct):
            bio = StringBuilder()
            w = bio.append
            for field in self.sorted_by_id:
                value = dct.get(field.name)
                if value is not None:
                    field.coder.write_value(field, value, w)
            return bio.build()
    else:
        def _to_raw(self, dct):
            ba = bytearray()
            w = ba.extend
            for field in self.sorted_by_id:
                value = dct.get(field.name)
                if value is not None:
                    field.coder.write_value(field, value, w)
            return str(ba)


class Struct(object):
    __slots__ = ('struct_type', 'buf', 'obuf', 'dct')
    def __init__(self, struct_type):
        self.struct_type = struct_type
        self.buf = ''
        self.obuf = ''
        self.dct = {}

    @classmethod
    def from_raw(cls, struct_type, buf, source=None):
        self = cls(struct_type)
        self.buf = buf
        self.obuf = buf
        return self

    def reset(self):
        self.buf = self.obuf
        self.dct = {}

    def _explode(self):
        if self.buf:
            for key, value in self.struct_type.iter_values(self.buf):
                self.dct.setdefault(key, value)
            self.buf = None

    def to_raw(self):
        self._explode()
        return self.struct_type._to_raw(self.dct)

    def __len__(self):
        self._explode()
        return sum(1 for v in self.dct.itervalues() if v is not None)
    __nonzero__ = __len__

    def __getitem__(self, key):
        if key not in self.dct:
            field = self.struct_type.field_name_map[key]
            if self.buf:
                value = self.struct_type.read_value(self.buf, field)
                if field.collection:
                    self.dct[key] = value
                if value is not None:
                    return value
        return self.dct[key]

    def get(self, key, default=None):
        value = self.dct.get(key, _undefined)
        if value is _undefined:
            field = self.struct_type.field_name_map.get(key)
            if field is None:
                return default

            if self.buf:
                value = self.struct_type.read_value(self.buf, field)
                self.dct[key] = value
                if value is None:
                    value = default

        return value

    def __setitem__(self, key, value):
        field = self.struct_type.field_name_map[key]
        field.type_check(value)
        self.dct[key] = value

    def __delitem__(self, key):
        self._explode()
        if self.get(key) is None:
            raise KeyError(key)
        self.dct[key] = None

    def __contains__(self, key):
        return self.get(key) is not None
    has_key = __contains__

    def clear(self):
        self.buf = None
        self.dct.clear()

    def copy(self):
        new = type(self)(self.struct_type)
        new.buf = self.buf
        new.dct = self.dct.copy()
        return new

    def __repr__(self):
        typ = type(self)
        d = dict(self.iteritems())
        return '<%s.%s(%s)>' % (typ.__module__, typ.__name__, d)

    def iteritems(self):
        self._explode()
        return (t for t in self.dct.iteritems() if t[1] is not None)

    def iterkeys(self):
        self._explode()
        return (k for k, v in self.dct.iteritems() if v is not None)
    __iter__ = iterkeys

    def itervalues(self):
        self._explode()
        return (v for v in self.dct.itervalues() if v is not None)

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def items(self):
        return list(self.iteritems())

    def pop(self, key, default=_undefined):
        value = self.get(key)
        if value is not None:
            self.dct[key] = None
            return value

        if default is _undefined:
            raise KeyError(key)
        return default

    def popitem(self):
        k = next(self.iterkeys(), None)
        if k is None:
            raise KeyError('Struct is empty')
        return k, self.pop(k)

    def setdefault(self, key, default=None):
        if self.get(key) is None:
            self[key] = default

    def update(self, other):
        for key in other:
            self[key] = other[key]


#: Mapping of _Field subclass to field kind.
FIELD_KINDS = {}
_stack = [_Field]
while _stack:
    _k = _stack.pop()
    _stack.extend(_k.__subclasses__())
    if _k.KIND:
        FIELD_KINDS[_k.KIND] = _k
del _k, _stack
