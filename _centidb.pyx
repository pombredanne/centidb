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

from uuid import UUID
from struct import pack

cdef extern from "Python.h":
    ctypedef struct PyObject
    void *PyString_FromStringAndSize(const unsigned char *v, int len)
    int _PyString_Resize(void*, Py_ssize_t)
    unsigned char *PyString_AS_STRING(void *string)
    Py_ssize_t PyString_GET_SIZE(void *)
    unsigned char *PyString_AS_STRING(void *string)
    void *memcpy(void *s1, const void *s2, size_t n)
    void Py_DECREF(void *o)


cdef class StringWriter:
    """
    Represents efficient append-only access to a bytestring's internal
    buffer, enabling zero copy incremental string construction. Only available
    when speedups module is installed.
    
    Usually a separate buffer is built before calling
    :py:func:`PyString_FromStringAndSize`, however the CPython API sanctions
    mutating a string's internal buffer so long as its reference count is 1.
    This class guarantees that by preventing further writes to the string after
    a reference to it has been taken.

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
        """Append a single ordinal `o` to the buffer, growing it as
        necessary."""
        assert self.s is not NULL
        if (1 + self.pos) == PyString_GET_SIZE(self.s):
            self._grow()
        cdef unsigned char *s = PyString_AS_STRING(self.s)
        s[self.pos] = o
        self.pos += 1

    cpdef putbytes(self, bytes b):
        """Append a bytestring to the buffer, growing it as necessary."""
        assert self.s is not NULL
        cdef size_t blen = len(b)
        while (PyString_GET_SIZE(self.s) - self.pos) < (blen + 1):
            self._grow()
        cdef unsigned char *s = PyString_AS_STRING(self.s)
        memcpy(s + self.pos, <unsigned char *> b, blen)
        self.pos += blen

    cpdef object finalize(self):
        # It should be possible to do better than this.
        #print 'before:', <int>self.s
        if -1 == _PyString_Resize(&self.s, self.pos):
            raise MemoryError
        #print 'after:', <int>self.s
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
    if type(o) is not tuple:
        o = (o,)
    return o

cdef encode_int(StringWriter sw, v):
    """Given some positive integer of 64-bits or less, return a variable length
    bytestring representation that preserves the integer's order. The
    bytestring size is such that:

        +-------------+------------------------+
        + *Size*      | *Largest integer*      |
        +-------------+------------------------+
        + 1 byte      | <= 240                 |
        +-------------+------------------------+
        + 2 bytes     | <= 2287                |
        +-------------+------------------------+
        + 3 bytes     | <= 67823               |
        +-------------+------------------------+
        + 4 bytes     | <= 16777215            |
        +-------------+------------------------+
        + 5 bytes     | <= 4294967295          |
        +-------------+------------------------+
        + 6 bytes     | <= 1099511627775       |
        +-------------+------------------------+
        + 7 bytes     | <= 281474976710655     |
        +-------------+------------------------+
        + 8 bytes     | <= 72057594037927935   |
        +-------------+------------------------+
        + 9 bytes     | <= (2**64)-1           |
        +-------------+------------------------+
    """
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

cdef object encode_str(StringWriter sw, bytes s):
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

cpdef object encode_keys(tups, bytes prefix=None, closed=True):
    """Encode a sequence of tuples of primitive values to a bytestring that
    preserves a meaningful lexicographical sort order.

        `prefix`:
            Initial prefix for the bytestring, if any.

        `closed`:
            If ``False``, indicates that if the last element of the last tuple
            is a string or blob, its terminator should be omitted. This allows
            open-ended queries on substrings:

            ::

                a_open = encode_keys('a', closed=False) # 0x28 0x61
                a_closed = encode_keys('a')             # 0x28 0x61 0x00
                aa = encode_keys('aa')                  # 0x28 0x61 0x61 0x00
                assert not aa.startswith(a_closed)
                assert aa.startswith(a_open)

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
    """
    cdef StringWriter sw = StringWriter()

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
                encode_int(sw, arg)
            elif type_ is long or type_ is int:
                if arg < 0:
                    sw.putc(KIND_NEG_INTEGER)
                    encode_int(sw, -arg)
                else:
                    sw.putc(KIND_INTEGER)
                    encode_int(sw, arg)
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
    """Wraps a record value with its last saved key, if any.

    :py:class:`Record` instances are usually created by the
    :py:class:`Collection` and :py:class:`Index`
    ``get()``/``put()``/``iter*()`` functions. They are primarily used to track
    index keys that were valid for the record when it was loaded, allowing many
    operations to be avoided if the user deletes or modifies it within the same
    transaction. The class is only required when modifying existing records.

    It is possible to avoid using the class when `Collection.derived_keys =
    True`, however this hurts perfomance as it forces :py:meth:`Collectionput`
    to first check for any existing record with the same key, and therefore for
    any existing index keys that must first be deleted.

    *Note:* you may create :py:class:`Record` instances directly, **but you
    must not modify any attributes except** :py:attr:`Record.data`, or
    construct it using any arguments except `coll` and `data`, otherwise index
    corruption will likely occur.
    """
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

    def __repr__(self):
        s = ','.join(map(repr, self.key or ()))
        return '<Record %s:(%s) %r>' % (self.coll.info['name'], s, self.data)
