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

import uuid
import struct

cimport cython

cimport cpython


cdef object UUID = uuid.UUID
cdef object pack = struct.pack
cdef object unpack = struct.unpack

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
    void Py_INCREF(void *o)
    void Py_DECREF(void *o)
    PyObject *PyTuple_New(Py_ssize_t len)


@cython.final
@cython.internal
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

@cython.final
@cython.internal
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

    cdef void putc(self, unsigned char o):
        """putc(o)

        Append a single ordinal `o` to the buffer, growing it as necessary."""
        assert self.s is not NULL
        if (1 + self.pos) == PyString_GET_SIZE(self.s):
            self._grow()
        cdef unsigned char *s = PyString_AS_STRING(self.s)
        s[self.pos] = o
        self.pos += 1

    cdef void putbytes(self, unsigned char *s, size_t size):
        """
        putbytes(b)

        Append a bytestring `b` to the buffer, growing it as necessary."""
        assert self.s is not NULL
        while (PyString_GET_SIZE(self.s) - self.pos) < (size + 1):
            self._grow()
        cdef unsigned char *ss = PyString_AS_STRING(self.s)
        memcpy(ss + self.pos, <unsigned char *> s, size)
        self.pos += size

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

cpdef tuplize(o):
    """Please view module docstrings via Sphinx or pydoc."""
    if type(o) is not tuple:
        o = (o,)
    return o

cdef c_encode_int(StringWriter sw, uint64_t v):
    cdef unsigned int vi
    cdef bytes tmp

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
        tmp = pack('>L', v)[-3:]
        sw.putbytes(tmp, 3)
    elif v <= 4294967295:
        sw.putc(0xfb)
        tmp = pack('>L', v)
        sw.putbytes(tmp, 4)
    elif v <= 1099511627775:
        sw.putc(0xfc)
        tmp = pack('>Q', v)[-5:]
        sw.putbytes(tmp, 5)
    elif v <= 281474976710655:
        sw.putc(0xfd)
        tmp = pack('>Q', v)[-6:]
        sw.putbytes(tmp, 6)
    elif v <= 72057594037927935:
        sw.putc(0xfe)
        tmp = pack('>Q', v)[-7:]
        sw.putbytes(tmp, 7)
    else:
        sw.putc(0xff)
        tmp = pack('>Q', v)
        sw.putbytes(tmp, 8)

def encode_int(v):
    """Please view module docstrings via Sphinx or pydoc."""
    assert (v >= 0) and (v.bit_length() <= 64), repr(v)
    cdef StringWriter sw = StringWriter()
    c_encode_int(sw, v)
    return sw.finalize()

cdef object encode_str(StringWriter sw, bytes s):
    """Please view module docstrings via Sphinx or pydoc."""
    cdef unsigned char *p = s
    cdef size_t length = len(s)
    cdef unsigned char c

    cdef int i
    for i in range(length):
        c = p[i]
        if c == 0:
            sw.putbytes("\x01\x01", 2)
        elif c == 1:
            sw.putbytes("\x01\x02", 2)
        else:
            sw.putc(c)


cdef c_encode_value(StringWriter sw, object arg, int closed):
    type_ = type(arg)
    if arg is None:
        sw.putc(KIND_NULL)
    elif type_ is int:
        i64 = arg
        if i64 < 0:
            sw.putc(KIND_NEG_INTEGER)
            c_encode_int(sw, -i64)
        else:
            sw.putc(KIND_INTEGER)
            c_encode_int(sw, i64)
    elif type_ is str:
        sw.putc(KIND_BLOB)
        encode_str(sw, arg)
        if closed:
            sw.putc('\x00')
    elif type_ is unicode:
        sw.putc(KIND_TEXT)
        encode_str(sw, arg.encode('utf-8'))
        if closed:
            sw.putc('\x00')
    elif type_ is bool:
        sw.putc(KIND_BOOL)
        c_encode_int(sw, arg is True)
    elif type_ is long:
        assert arg.bit_length() <= 64
        i64 = arg
        if i64 < 0:
            sw.putc(KIND_NEG_INTEGER)
            c_encode_int(sw, -i64)
        else:
            sw.putc(KIND_INTEGER)
            c_encode_int(sw, i64)
    elif type_ is UUID:
        sw.putc(KIND_UUID)
        encode_str(sw, arg.get_bytes())
        sw.putc('\x00')
    else:
        raise TypeError('unsupported type: %r' % (arg,))


cdef c_encode_key(StringWriter sw, object tup, int closed):
    cdef int i
    cdef int j
    cdef int64_t i64

    cdef size_t tlast = len(tup) - 1
    for j, arg in enumerate(tup):
        c_encode_value(sw, arg, closed and tlast != j)


def encode_keys(tups, bytes prefix=None, int closed=1):
    """Please view module docstrings via Sphinx or pydoc."""
    cdef StringWriter sw = StringWriter()
    cdef size_t llast

    if prefix is not None:
        sw.putbytes(prefix, len(prefix))

    if type(tups) is not list:
        if type(tups) is tuple:
            c_encode_key(sw, tups, closed)
        else:
            c_encode_value(sw, tups, closed)
    else:
        llast = len(tups) - 1
        for i, tup in enumerate(tups):
            if i:
                sw.putc(KIND_SEP)
            if type(tup) is tuple:
                c_encode_key(sw, tup, closed and i != llast)
            else:
                c_encode_value(sw, tup, closed and i != llast)

    return sw.finalize()


cdef c_encode_index_entry(size_t initial, unsigned char *prefix_p,
                          size_t prefix_size, object entry,
                          unsigned char *key_p, size_t key_size):
    cdef StringWriter sw = StringWriter(initial)
    sw.putbytes(prefix_p, prefix_size)
    if type(entry) is tuple:
        c_encode_key(sw, entry, 0)
    else:
        c_encode_value(sw, entry, 0)
    sw.putc(KIND_SEP)
    sw.putbytes(key_p, key_size)
    return sw.finalize()


@cython.final
cdef class IndexKeyBuilder:
    cdef list indices

    def __init__(self, list indices not None):
        self.indices = indices

    def build(self, key, obj):
        cdef StringWriter key_sw = StringWriter()
        c_encode_key(key_sw, key, 1)
        cdef bytes key_enc = key_sw.finalize()
        cdef unsigned char * key_enc_p = key_enc
        cdef size_t key_size = len(key_enc)
        cdef size_t initial = key_size + 20

        cdef bytes prefix
        cdef unsigned char *prefix_p
        cdef size_t prefix_size

        cdef StringWriter sw2

        cdef list idx_keys = []
        cdef tuple args = (obj,)
        cdef object lst

        for idx in self.indices:
            prefix = idx.prefix
            prefix_p = prefix
            prefix_size = len(prefix)

            lst = idx.func(*args)

            if type(lst) is not list:
                idx_keys.append(c_encode_index_entry(
                    initial, prefix_p, prefix_size, lst, key_enc_p, key_size))
            else:
                for idx_key in lst:
                    idx_keys.append(c_encode_index_entry(
                        initial, prefix_p, prefix_size, idx_key, key_enc_p,
                        key_size))
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


cpdef decode_keys(s, bytes prefix=None, int first=False):
    """Please view module docstrings via Sphinx or pydoc."""
    cdef StringReader sr = StringReader(s)
    if prefix:
        assert len(sr.gets(len(prefix))) == len(prefix)

    cdef list tups
    if not first:
        tups = []
    cdef PyObject *tup = NULL
    cdef size_t pos = 0

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
            if first:
                break
            if pos != cpython.PyTuple_GET_SIZE(<object> tup):
                assert -1 != cpython._PyTuple_Resize(&tup, pos)
            Py_INCREF(tup)
            tups.append(<object> tup)
            Py_DECREF(tup)
            tup = NULL
            continue
        else:
            raise ValueError('bad kind %r; key corrupt? %r' % (c, <object>tup))

        if tup is NULL:
            tup = <PyObject *> PyTuple_New(3)
            assert tup
            pos = 0
        if pos == cpython.PyTuple_GET_SIZE(<object> tup):
            assert -1 != cpython._PyTuple_Resize(&tup, cpython.PyTuple_GET_SIZE(<object> tup) + 2)
        #Py_INCREF(<void *> arg)
        cpython.PyTuple_SET_ITEM(<object> tup, pos, arg)
        pos += 1

    if pos != cpython.PyTuple_GET_SIZE(<object> tup):
        assert -1 != cpython._PyTuple_Resize(&tup, pos)
    #Py_INCREF(tup)
    if first:
        return <object>tup
    tups.append(<object> tup)
    Py_DECREF(tup)
    return tups


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

__all__ = ['Record', 'encode_keys', 'encode_int', 'decode_keys',
           'IndexKeyBuilder', 'tuplize']
