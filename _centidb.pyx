#cython: embedsignature=False
#cython: boundscheck=False
#cython: cdivision=True
#
# Copyright 2013 The Python-lmdb authors, all rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted only as authorized by the OpenLDAP
# Public License.
#
# A copy of this license is available in the file LICENSE in the
# top-level directory of the distribution or, alternatively, at
# <http://www.OpenLDAP.org/license.html>.
#
# OpenLDAP is a registered trademark of the OpenLDAP Foundation.
#
# Individual files and/or contributed packages may be copyright by
# other parties and/or subject to additional restrictions.
#
# This work also contains materials derived from public sources.
#
# Additional information about OpenLDAP can be obtained at
# <http://www.openldap.org/>.

#include "cpython/string.pxd"
include "libc/stdint.pxd"

from uuid import UUID
from struct import pack
from struct import unpack

cdef extern from "arpa/inet.h":
    uint32_t ntohl(uint32_t n)


cdef extern from "Python.h":
    ctypedef struct PyObject
    void *PyString_FromStringAndSize(const unsigned char *v, int len)
    int _PyString_Resize(void*, Py_ssize_t)
    unsigned char *PyString_AS_STRING(void *string)
    Py_ssize_t PyString_GET_SIZE(void *)
    unsigned char *PyString_AS_STRING(void *string)
    void *memcpy(void *s1, const void *s2, size_t n)
    void Py_DECREF(void *o)


cdef class StringReader:
    """
    Basic StringIO-alike class except it can be accessed efficiently from
    Cython.

        `buf`:
            Bytestring to read from.
    """
    cdef bytes buf_
    cdef char *buf
    cdef size_t size
    cdef size_t pos

    def __init__(self, bytes buf not None):
        self.buf_ = buf
        self.buf = <char *> buf
        self.size = len(buf)
        self.pos = 0

    cdef int getchar(self, unsigned char *ch):
        if self.pos < self.size:
            ch[0] = self.buf[self.pos]
            self.pos += 1
            return 1
        return 0

    cdef unsigned char getchar_(self):
        cdef unsigned char ch
        assert self.getchar(&ch)
        return ch

    cdef int getinto(self, unsigned char *s, n):
        cdef int ok = 1
        while n:
            if not self.getchar(s):
                return 0
            s += 1
            n -= 1
        return 1

    cpdef bytes getc(self):
        """Fetch a single byte of input as a bytestring. Returns the empty
        string at EOF."""
        cdef unsigned char c
        if self.getchar(&c):
            return c
        return b''

    cpdef bytes gets(self, size_t n):
        """
        gets(n)

        Fetch `n` multiple bytes of input. Returns the empty string at EOF.
        """
        cdef bytes s
        if self.pos < self.size:
            s = self.buf[self.pos:self.pos + n]
            self.pos += n
            return s
        return b''

cdef class StringWriter:
    """
    Represents efficient append-only access to a bytestring's internal
    buffer, enabling zero copy incremental string construction. Only available
    when speedups module is installed.

    Usually a separate buffer is built before calling
    :py:func:`PyString_FromStringAndSize`, however the CPython API allows for
    mutating a string's internal buffer so long as its reference count is 1.
    This class guarantees the reference count is 1 by preventing further writes
    after a reference to it has been taken via :py:meth:`StringWriter.finalize`.

        `initial_size`:
            Initial buffer size in bytes. When this is exceeded, the buffer is
            doubled until it reaches 512 bytes, after which it grows in 512
            byte increments.

            Larger sizes cause fewer initial reallocations, but is more likely
            to cause the allocator to move the string while truncating it to
            its final size.
    """
    cdef void *s
    cdef size_t pos

    def __init__(self, size_t initial=20):
        self.s = PyString_FromStringAndSize(NULL, initial)
        if self.s is NULL:
            raise MemoryError()
        self.pos = 0

    def __dealloc__(self):
        if self.s is not NULL:
            Py_DECREF(self.s)
        self.s = NULL

    cdef _grow(self):
        cdef size_t cursize = PyString_GET_SIZE(self.s)
        cdef size_t newsize = cursize + max(512, cursize * 2)
        if -1 == _PyString_Resize(&self.s, newsize):
            raise MemoryError()

    cpdef putc(self, unsigned char o):
        """putc(o)

        Append a single ordinal `o` to the buffer, growing it as necessary."""
        assert self.s is not NULL
        if (1 + self.pos) == PyString_GET_SIZE(self.s):
            self._grow()
        cdef unsigned char *s = PyString_AS_STRING(self.s)
        s[self.pos] = o
        self.pos += 1

    cpdef putbytes(self, bytes b):
        """
        putbytes(b)

        Append a bytestring `b` to the buffer, growing it as necessary."""
        assert self.s is not NULL
        cdef size_t blen = len(b)
        while (PyString_GET_SIZE(self.s) - self.pos) < (blen + 1):
            self._grow()
        cdef unsigned char *s = PyString_AS_STRING(self.s)
        memcpy(s + self.pos, <unsigned char *> b, blen)
        self.pos += blen

    cpdef object finalize(self):
        """
        finalize()

        Resize the string to its final size, and return it. The StringWriter
        should be discarded after calling finalize().
        """
        # It should be possible to do better than this.
        if -1 == _PyString_Resize(&self.s, self.pos):
            raise MemoryError
        cdef object ss = <object> self.s
        Py_DECREF(self.s)
        self.s = NULL
        return ss


cdef enum ElementKind:
    KIND_NULL = 15
    KIND_NEG_INTEGER = 20
    KIND_INTEGER = 21
    KIND_BOOL = 30
    KIND_BLOB = 40
    KIND_TEXT = 50
    KIND_UUID = 90
    KIND_KEY = 95
    KIND_SEP = 102

def tuplize(o):
    """Please view module docstrings via Sphinx or pydoc."""
    if type(o) is not tuple:
        o = (o,)
    return o

cdef c_encode_int(StringWriter sw, v):
    cdef unsigned int vi

    if v < 240:
        sw.putc(v)
    elif v <= 2287:
        v -= 240
        d, m = divmod(v, 256)
        sw.putc(241 + d)
        sw.putc(m)
    elif v <= 67823:
        v -= 2288
        d, m = divmod(v, 256)
        sw.putc(0xf9)
        sw.putc(d)
        sw.putc(m)
    elif v <= 16777215:
        sw.putc(0xfa)
        sw.putbytes(pack('>L', v)[-3:])
    elif v <= 4294967295:
        sw.putc(0xfb)
        sw.putbytes(pack('>L', v))
    elif v <= 1099511627775:
        sw.putc(0xfc)
        sw.putbytes(pack('>Q', v)[-5:])
    elif v <= 281474976710655:
        sw.putc(0xfd)
        sw.putbytes(pack('>Q', v)[-6:])
    elif v <= 72057594037927935:
        sw.putc(0xfe)
        sw.putbytes(pack('>Q', v)[-7:])
    else:
        assert v.bit_length() <= 64
        sw.putc(0xff)
        sw.putbytes(pack('>Q', v))

def encode_int(v):
    """Please view module docstrings via Sphinx or pydoc."""
    cdef StringWriter sw = StringWriter()
    c_encode_int(sw, v)
    return sw.finalize()

cdef object encode_str(StringWriter sw, bytes s):
    """Please view module docstrings via Sphinx or pydoc."""
    cdef unsigned char *p = s
    cdef size_t length = len(s)
    cdef unsigned char c

    for i in range(length):
        c = s[i]
        if c == 0:
            sw.putbytes(b'\x01\x01')
        elif c == 1:
            sw.putbytes(b'\x01\x02')
        else:
            sw.putc(c)

cpdef bytes encode_keys(tups, bytes prefix=None, closed=True):
    """Please view module docstrings via Sphinx or pydoc."""
    cdef StringWriter sw = StringWriter()
    if prefix:
        sw.putbytes(prefix)

    if type(tups) is not list:
        tups = [tups]

    last = len(tups) - 1
    for i, tup in enumerate(tups):
        if i:
            sw.putc(KIND_SEP)
        if type(tup) is not tuple:
            tup = (tup,)
        tlast = len(tup) - 1
        for j, arg in enumerate(tup):
            type_ = type(arg)
            if arg is None:
                sw.putc(KIND_NULL)
            elif arg is True or arg is False:
                sw.putc(KIND_BOOL)
                c_encode_int(sw, arg)
            elif type_ is long or type_ is int:
                if arg < 0:
                    sw.putc(KIND_NEG_INTEGER)
                    c_encode_int(sw, -arg)
                else:
                    sw.putc(KIND_INTEGER)
                    c_encode_int(sw, arg)
            elif type_ is UUID:
                sw.putc(KIND_UUID)
                encode_str(sw, arg.get_bytes())
                sw.putc('\x00')
            elif isinstance(arg, str):
                sw.putc(KIND_BLOB)
                encode_str(sw, arg)
                if closed or i != last or j != tlast:
                    sw.putc('\x00')
            elif type_ is unicode:
                sw.putc(KIND_TEXT)
                encode_str(sw, arg.encode('utf-8'))
                if closed or i != last or j != tlast:
                    sw.putc('\x00')
            else:
                raise TypeError('unsupported type: %r' % (arg,))
    return sw.finalize()


cdef class IndexKeyBuilder:
    cdef list indices

    def __init__(self, indices):
        self.indices = indices

    def build(self, key, obj):
        idx_keys = []
        for idx in self.indices:
            lst = idx.func(obj)
            if type(lst) is not list:
                lst = [lst]
            for idx_key in lst:
                idx_keys.append(encode_keys((idx_key, key), idx.prefix))
        return idx_keys

cdef class Record:
    """Please view module docstrings via Sphinx or pydoc."""
    cdef public object coll
    cdef public object data
    cdef public object key
    cdef public object batch
    cdef public object txn_id
    cdef public list index_keys

    def __init__(self, coll, data, _key=None, _batch=False,
            _txn_id=None, _index_keys=None):
        self.coll = coll
        self.data = data
        self.key = _key
        self.batch = _batch
        self.txn_id = _txn_id
        self.index_keys = _index_keys

    def __cmp__(self, other):
        if type(other) is Record:
            return cmp((self.coll, self.data, self.key),
                       (other.coll, other.data, other.key))
        return -1

    def __repr__(self):
        s = ','.join(map(repr, self.key or ()))
        return '<Record %s:(%s) %r>' % (self.coll.info['name'], s, self.data)


cpdef decode_int(StringReader sr):
    """Please view module docstrings via Sphinx or pydoc."""
    cdef unsigned char ch
    cdef unsigned char ch2
    cdef uint8_t buf[8]
    cdef uint64_t u64 = 0

    assert sr.getchar(&ch)
    if ch <= 240:
        u64 = ch
    elif ch <= 248:
        assert sr.getchar(&ch2)
        u64 = 240 + (256 * (ch - 241) + sr.getchar_())
    elif ch == 249:
        u64 = 2288 + (256 * sr.getchar_()) + sr.getchar_()
    elif ch == 250:
        buf[0] = 0
        assert sr.getinto(buf, 3)
        u64 = ntohl((<uint32_t *>buf)[0])
    elif ch == 251:
        assert sr.getinto(buf, 4)
        u64 = ntohl((<uint32_t *>buf)[0])
    elif ch == 252:
        return unpack('>Q', '\x00\x00\x00' + sr.gets(5))[0]
    elif ch == 253:
        return unpack('>Q', '\x00\x00' + sr.gets(6))[0]
    elif ch == 254:
        return unpack('>Q', '\x00' + sr.gets(7))[0]
    elif ch == 255:
        return unpack('>Q', sr.gets(8))[0]
    return u64

cdef bytes decode_str(StringReader sr):
    """Please view module docstrings via Sphinx or pydoc."""
    cdef StringWriter sw = StringWriter()
    cdef unsigned char ch
    while sr.getchar(&ch):
        if ch == 0:
            break
        elif ch == 1:
            assert sr.getchar(&ch)
            if ch == 1:
                sw.putc(0)
            else:
                assert ch == 2
                sw.putc(1)
        else:
            sw.putc(ch)
    return sw.finalize()


cpdef decode_keys(s, prefix=None, first=False):
    """Please view module docstrings via Sphinx or pydoc."""
    cdef StringReader sr = StringReader(s)
    if prefix:
        assert len(sr.gets(len(prefix))) == len(prefix)

    tups = []
    tup = []
    cdef unsigned char c
    while sr.getchar(&c):
        if c == KIND_NULL:
            arg = None
        elif c == KIND_INTEGER:
            arg = decode_int(sr)
        elif c == KIND_NEG_INTEGER:
            arg = -decode_int(sr)
        elif c == KIND_BOOL:
            arg = bool(decode_int(sr))
        elif c == KIND_BLOB:
            arg = decode_str(sr)
        elif c == KIND_TEXT:
            arg = decode_str(sr).decode('utf-8')
        elif c == KIND_UUID:
            arg = UUID(decode_str(sr))
        elif c == KIND_SEP:
            if tup:
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


"""
cdef class Iter:
    cdef object it
    cdef size_t i
    cdef ssize_t max
    cdef bytes hi
    cdef bytes lo
    cdef int values

    def __init__(self, engine, txn, key=None, prefix=None, lo=None, hi=None,
        reverse=False, max=None, values=True):
        from centidb import next_greater
        if prefix:
            self.lo = prefix
            self.hi = next_greater(prefix)
        else:
            self.lo = None
            self.hi = None

        self.i = 0
        self.max = max or -1
        self.values = values
        self.it = (txn or engine).iter(
            (self.hi if reverse else self.lo) or key, reverse)

    def __iter__(self):
        return self

    def __next__(self):
        cdef int i = 0
        cdef bytes k
        cdef object tup = next(self.it)
        if self.max >= 0 and i > max:
            raise StopIteration
        k = tup[0]
        if ((self.lo is not None) and (k < self.lo)) \
                or ((self.hi is not None) and (k > self.hi)):
            raise StopIteration
        i += 1
        if self.values:
            return tup
        else:
            return k

_iter = Iter
"""

__all__ = ['Record', 'StringWriter', 'encode_keys', 'encode_int',
           'decode_keys', 'IndexKeyBuilder', 'tuplize']
