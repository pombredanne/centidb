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
import array
import socket
import struct
import sys
import types
import uuid

try:
    import __pypy__.builders
except ImportError:
    __pypy__ = None


assert len(array.array('i', [0]).tostring()) == 4
assert len(array.array('l', [0]).tostring()) == 8


# Wire type constants.
WIRE_TYPE_VARIABLE     = 0
WIRE_TYPE_64           = 1
WIRE_TYPE_DELIMITED    = 2
WIRE_TYPE_32           = 5


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
        w(field.wire_key_s)
        field.write(o, w)

    def read_value(self, field, buf, pos):
        return field.read(buf, pos)


class _PackedCoder(_Coder):
    def make_key(self, field):
        return (field.field_id << 3) | WIRE_TYPE_DELIMITED

    if __pypy__:
        def write_value(self, field, o, w):
            bio = __pypy__.builders.StringBuilder()
            ww = bio.append
            for elem in o:
                field.write(elem, ww)

            s = bio.build()
            w(field.wire_key_s)
            write_varint(w, len(s))
            w(s)
    else:
        def write_value(self, field, o, w):
            ba = bytearray()
            ww = ba.extend
            for elem in o:
                field.write(elem, ww)

            s = str(ba)
            w(field.wire_key_s)
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
        w(field.wire_key_s)
        write_varint(w, self.item_size * len(o))
        for elem in o:
            field.write(elem, w)

    def read_value(self, field, buf, pos):
        pos, n = read_varint(buf, pos)
        epos = pos + n
        l = []
        for pos in xrange(pos, epos, self.item_size):
            pos, value = field.read(buf, pos)
            l.append(value)
        return epos, l


class _FixedPackedArrayCoder(_FixedPackedCoder):
    def __init__(self, item_size, type_code):
        super(_FixedPackedArrayCoder, self).__init__(item_size)
        self.type_code = type_code

    if sys.byteorder == 'little':
        def read_value(self, field, buf, pos):
            pos, n = read_varint(buf, pos)
            epos = pos + (n * self.item_size)
            return epos, array.array(self.type_code, buf[pos:epos])
    else:
        def read_value(self, field, buf, pos):
            pos, n = read_varint(buf, pos)
            epos = pos + (n * self.item_size)
            ar = array.array(self.type_code, buf[pos:epos])
            ar.byteswap()
            return epos, ar


class _DelimitedSequence(object):
    def __init__(self, field, buf, offsets):
        self._field = field
        self._buf = buf
        self._offsets = offsets
        self._objs = [None] * len(offsets)

    def __iter__(self):
        for i in xrange(len(self._offsets)):
            yield self[i]

    def __repr__(self):
        return '<DelimitedSequence %s>' % (list(self),)

    def __len__(self):
        return len(self._offsets)

    def __getitem__(self, i):
        obj = self._objs[i]
        if obj is None:
            _, obj = self._field.read(self._buf, self._offsets[i])
            self._objs[i] = obj
        return obj

    def __setitem__(self, i, obj):
        self._objs[i] = obj


class _DelimitedCoder(_Coder):
    def make_key(self, field):
        return (field.field_id << 3) | field.WIRE_TYPE

    def write_value(self, field, o, w):
        for elem in o:
            w(field.wire_key_s)
            field.write(elem, w)

    def _get_offsets(self, field, buf, pos):
        offsets = []
        blen = len(buf)
        wire_key = field.wire_key
        while wire_key == field.wire_key:
            offsets.append(pos)
            pos, n = read_varint(buf, pos)
            pos += n
            if pos >= blen:
                break
            pos, wire_key = read_varint(buf, pos)
        return pos, offsets

    def read_value(self, field, buf, pos):
        pos, offsets = self._get_offsets(field, buf, pos)
        return pos, _DelimitedSequence(field, buf, offsets)


#
# Field types.
#

class _Field(object):
    TYPES = ()
    WIRE_TYPE = None
    KIND = None

    def __init__(self, field_id, name, sequence):
        self.field_id = field_id
        self.name = name
        self.sequence = sequence
        if sequence:
            self.coder = self.SEQUENCE_CODER
        else:
            self.coder = _ScalarCoder()
        self.wire_key = self.coder.make_key(self)

        ba = bytearray()
        write_varint(ba.extend, self.wire_key)
        self.wire_key_s = str(ba)

    def type_check(self, value):
        if not isinstance(value, self.TYPES):
            raise TypeError('field %r requires %r, not %r' %\
                            (self.name, self.TYPES, type(value)))

    SEQUENCE_TYPES = (list, array.array)
    def type_check_sequence(self, value):
        if not isinstance(value, self.SEQUENCE_TYPES):
            raise TypeError('field %r requires %r, not %r' %\
                            (self.name, list, type(value)))
        if not all(isinstance(v, self.TYPES) for v in value):
            raise TypeError('field %r requires list of %r' %\
                            (self.name, self.TYPES))


class _BoolField(_Field):
    TYPES = (bool, types.NoneType)
    KIND = 'bool'
    WIRE_TYPE = WIRE_TYPE_VARIABLE
    SEQUENCE_CODER = _FixedPackedCoder(1)

    def read(self, buf, pos):
        return pos+1, buf[pos] == '\x01'

    def write(self, o, w):
        w(chr(o))


class _DoubleField(_Field):
    TYPES = (float, types.NoneType)
    KIND = 'double'
    WIRE_TYPE = WIRE_TYPE_64
    SEQUENCE_CODER = _FixedPackedArrayCoder(8, 'd')

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


class _I32Field(_FixedIntegerField):
    KIND = 'i32'
    SIZE = 4
    FORMAT = '<l'
    WIRE_TYPE = WIRE_TYPE_32
    SEQUENCE_CODER = _FixedPackedArrayCoder(SIZE, 'i')


class _I64Field(_FixedIntegerField):
    KIND = 'i64'
    SIZE = 8
    FORMAT = '<q'
    WIRE_TYPE = WIRE_TYPE_64
    SEQUENCE_CODER = _FixedPackedArrayCoder(SIZE, 'l')


class _U32Field(_FixedIntegerField):
    KIND = 'u32'
    SIZE = 4
    FORMAT = '<L'
    WIRE_TYPE = WIRE_TYPE_32
    SEQUENCE_CODER = _FixedPackedArrayCoder(SIZE, 'I')


class _U64Field(_FixedIntegerField):
    KIND = 'u64'
    SIZE = 8
    FORMAT = '<Q'
    WIRE_TYPE = WIRE_TYPE_64
    SEQUENCE_CODER = _FixedPackedArrayCoder(SIZE, 'L')


class _FloatField(_Field):
    TYPES = (float, types.NoneType)
    KIND = 'float'
    WIRE_TYPE = WIRE_TYPE_32
    SEQUENCE_CODER = _FixedPackedArrayCoder(4, 'f')

    def read(self, buf, pos):
        epos = pos + 4
        return epos, struct.unpack('f', buf[pos:epos])[0]

    def write(self, o, w):
        w(struct.pack('f', o))


class _VarIntField(_Field):
    TYPES = (int, long, types.NoneType)
    WIRE_TYPE = WIRE_TYPE_VARIABLE
    SEQUENCE_CODER = _PackedCoder()


class _VarIntField(_VarIntField):
    KIND = 'varint'

    def read(self, buf, pos):
        return read_varint(buf, pos)

    def write(self, o, w):
        write_varint(w, o)


class _UVarIntField(_VarIntField):
    KIND = 'uvarint'

    def read(self, buf, pos):
        pos, n = read_varint(buf, pos)
        return pos, n & ((2**64)-1)

    def write(self, o, w):
        write_varint(w, o)


class _SVarIntField(_VarIntField):
    KIND = 'svarint'

    def read(self, buf, pos):
        pos, value = read_varint(buf, pos)
        if value & 1:
            return pos, ((value >> 1) ^ ~0)
        return pos, value >> 1

    def write(self, o, w):
        if i < 0:
            write_varint(w, (i << 1) ^ ~0)
        else:
            write_varint(w, i << 1)


class _Inet4Field(_Field):
    TYPES = (basestring, types.NoneType)
    KIND = 'inet4'
    WIRE_TYPE = WIRE_TYPE_32
    SEQUENCE_CODER = _FixedPackedCoder(4)

    def read(self, buf, pos):
        epos = pos + 4
        return epos, socket.inet_ntop(socket.AF_INET, buf[pos:epos])

    def write(self, o, w):
        w(socket.inet_pton(socket.AF_INET, o))


class _Inet4PortField(_Field):
    TYPES = (basestring, types.NoneType)
    KIND = 'inet4port'
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    SEQUENCE_CODER = _FixedPackedCoder(6)

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
    SEQUENCE_CODER = _FixedPackedCoder(16)

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
    SEQUENCE_CODER = _FixedPackedCoder(18)

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


class _BlobField(_Field):
    TYPES = (bytes, types.NoneType)
    KIND = 'blob'
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    SEQUENCE_CODER = _DelimitedCoder()

    def read(self, buf, pos):
        pos, n = read_varint(buf, pos)
        epos = pos + n
        return epos, buf[pos:epos]

    def write(self, o, w):
        write_varint(w, len(o))
        w(o)


class _TextField(_Field):
    TYPES = (unicode, types.NoneType)
    KIND = 'text'
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    SEQUENCE_CODER = _DelimitedCoder()

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
    SEQUENCE_CODER = _FixedPackedCoder(16)

    def read(self, buf, pos):
        pos, n = read_varint(buf, pos)
        epos = pos + n
        return epos, uuid.UUID(None, buf[pos:epos])

    def write(self, o, w):
        write_varint(w, 16)
        w(o.get_bytes())


class _StructField(_Field):
    WIRE_TYPE = WIRE_TYPE_DELIMITED
    SEQUENCE_CODER = _DelimitedCoder()

    def __init__(self, field_id, name, sequence, struct_type):
        super(_StructField, self).__init__(field_id, name, sequence)
        self.struct_type = struct_type
        self.TYPES = (struct_type.klass, types.NoneType)

    def read(self, buf, pos):
        pos, n = read_varint(buf, pos)
        epos = pos + n
        return epos, self.struct_type.from_raw(buf[pos:epos])

    def write(self, o, w):
        s = o.to_raw()
        write_varint(w, len(s))
        w(s)


class _Property(object):
    def __init__(self, struct_type, field):
        self.struct_type = struct_type
        self.field = field
        self.name = field.name

    def __delete__(self, instance):
        instance.__dict__[self.name] = None


class _ScalarProperty(_Property):
    def __get__(self, instance, owner):
        if self.name not in instance.__dict__:
            return self.struct_type.read_value(instance._buf, self.field)
        return instance.__dict__[self.name]

    def __set__(self, instance, value):
        self.field.type_check(value)
        instance.__dict__[self.name] = value


class _SequenceProperty(_Property):
    def __get__(self, instance, owner):
        if self.name not in instance.__dict__:
            value = self.struct_type.read_value(instance._buf, self.field)
            instance.__dict__[self.name] = value
            return value
        return instance.__dict__[self.name]

    def __set__(self, instance, value):
        self.field.type_check_sequence(value)
        instance.__dict__[self.name] = value


class StructType(object):
    def __init__(self):
        self.field_id_map = {}
        self.sorted_by_id = []
        self.klass = type('stct', (Struct,), {})
        self.new = self.klass
        self.klass._struct_type = self

    def add_field(self, field_name, field_id, kind, sequence):
        if field_id in self.field_id_map:
            raise ValueError('duplicate field ID: %r' % (field_id,))
        if any(f.name == field_name for f in self.sorted_by_id):
            raise ValueError('duplicate field name: %r' % (field_name,))
        if isinstance(kind, StructType):
            field = _StructField(field_id, field_name, sequence, kind)
        else:
            klass = FIELD_KINDS.get(kind)
            if klass is None:
                raise ValueError('unknown kind: %r' % (kind,))
            field = klass(field_id, field_name, sequence)

        pclass = _SequenceProperty if sequence else _ScalarProperty
        setattr(self.klass, field_name, pclass(self, field))

        self.field_id_map[field_id] = field
        self.sorted_by_id.append(field)
        self.sorted_by_id.sort(key=lambda f: f.field_id)

    def iter_raw(self, buf):
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

    if __pypy__:
        def _to_raw(self, dct):
            bio = __pypy__.builders.StringBuilder()
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

    def _explode(self, struct):
        dct = struct.__dict__
        for key, value in self.iter_raw(struct._buf):
            dct.setdefault(key, value)

    def to_raw(self, struct):
        self._explode(struct)
        return self._to_raw(struct.__dict__)

    def from_raw(self, buf, source=None):
        struct = self.klass()
        struct._buf = buf
        return struct

    HIDE_FIELDS = set(['_buf', '_struct_type'])
    def iter_struct(self, struct):
        self._explode(struct)
        dct = struct.__dict__
        return ((k, v) for k, v in dct.iteritems()
                if v is not None and k not in self.HIDE_FIELDS)


class Struct(object):
    _buf = ''
    def __repr__(self):
        typ = type(self)
        d = dict(self._struct_type.iter_struct(self))
        return '<%s.%s(%s)>' % (typ.__module__, typ.__name__, d)


#: Mapping of _Field subclass to field kind.
FIELD_KINDS = {}
_stack = [_Field]
while _stack:
    _k = _stack.pop()
    _stack.extend(_k.__subclasses__())
    if _k.KIND:
        FIELD_KINDS[_k.KIND] = _k
del _k, _stack
