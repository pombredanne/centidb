/*
 * Copyright 2013, David Wilson.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define USING_MEMSINK
#include "acid.h"
#include "structmember.h"


static PyTypeObject KeyType;
static PyTypeObject KeyIterType;


/**
 * Construct a KEY_PRIVATE Key from `p[0..size]` and return it.
 */
Key *
acid_make_private_key(uint8_t *p, Py_ssize_t size)
{
    if(size > KEY_MAXSIZE) {
        PyErr_SetString(PyExc_ValueError, "Key is too long .");
        return NULL;
    }

    Key *self = PyObject_Malloc(sizeof(Key) + KEY_PREFIX_SLACK + size);
    if(self) {
        PyObject_Init((PyObject *)self, &KeyType);
        Key_SIZE(self) = size;
        self->flags = KEY_PRIVATE;
        self->p = ((uint8_t *) self) + sizeof(Key) + KEY_PREFIX_SLACK;
        if(p) {
            memcpy(Key_DATA(self), p, size);
        }
    }
    return self;
}

#ifdef HAVE_MEMSINK
/**
 * Construct a KEY_SHARED Key from ...
 */
Key *
acid_make_shared_key(PyObject *source, uint8_t *p, Py_ssize_t size)
{
    if(size > KEY_MAXSIZE) {
        PyErr_SetString(PyExc_ValueError, "Key is too long .");
        return NULL;
    }

    Key *self = PyObject_Malloc(sizeof(Key) + sizeof(SharedKeyInfo));
    // TODO: relies on arch padding rules
    if(self) {
        PyObject_Init((PyObject *)self, &KeyType);
        if(ms_listen(source, (PyObject *) self)) {
            PyObject_Free(self);
            return NULL;
        }
        self->flags = KEY_SHARED;
        self->p = p;
        Key_SIZE(self) = size;
        Key_INFO(self)->source = source;
        Py_INCREF(source);
    }
    return self;
}

/**
 * Struct mem_sink invalidate() callback. Convert a KEY_SHARED instance into a
 * KEY_COPIED instance, or if it is smaller than sizeof(struct ms_node), .
 */
static int invalidate_shared_key(PyObject *source, PyObject *sink)
{
    Key *self = (Key *)sink;
    assert(self->flags == KEY_SHARED);
    PyObject *tmp_source = Key_INFO(self)->source;

    uint8_t *old_data = Key_DATA(self);
    Py_ssize_t size = Key_SIZE(self);

    // Reuse the 12-24 bytes previously used for ShareKeyInfo if the key fits
    // in there, otherwise make a new heap allocation.
    if((size - KEY_PREFIX_SLACK) < sizeof(SharedKeyInfo)) {
        self->p = KEY_PREFIX_SLACK + (uint8_t *)Key_INFO(self);
        self->flags = KEY_PRIVATE;
    } else {
        uint8_t *p;
        if(! ((p = PyObject_Malloc(KEY_PREFIX_SLACK + size)))) {
            return -1;
        }
        self->p = p;
        self->flags = KEY_COPIED;
    }

    // Copy data then deref and forget the old source.
    memcpy(Key_DATA(self), old_data, size);
    Py_DECREF(tmp_source);
    return 0;
}
#endif

/**
 * Calculate key size.
 */
static PyObject *
key_sizeof(Key *self)
{
    size_t sz = sizeof(Key);
    if(self->flags == KEY_SHARED) {
        sz += sizeof(SharedKeyInfo);
    } else {
        sz += Key_SIZE(self);
    }
    return PyInt_FromSize_t(sz);
}

/**
 * Construct a Key from a sequence.
 */
static PyObject *
key_new(PyTypeObject *cls, PyObject *args, PyObject *kwds)
{
    if(PyTuple_GET_SIZE(args) == 1) {
        PyObject *arg = PyTuple_GET_ITEM(args, 0);
        if(Py_TYPE(arg) == &KeyType) {
            Py_INCREF(arg);
            return arg;
        }
        if(PyTuple_CheckExact(arg)) {
            args = arg;
        }
    }

    struct writer wtr;
    if(acid_writer_init(&wtr, 32)) {
        return NULL;
    }

    Py_ssize_t len = PyTuple_GET_SIZE(args);
    for(Py_ssize_t i = 0; i < len; i++) {
        PyObject *arg = PyTuple_GET_ITEM(args, i);
        if(acid_write_element(&wtr, arg)) {
            acid_writer_abort(&wtr);
            return NULL;
        }
    }

    Key *self = acid_make_private_key(acid_writer_ptr(&wtr) - wtr.pos, wtr.pos);
    acid_writer_abort(&wtr);
    return (PyObject *) self;
}

/**
 * Attempt to convert an object to a Key, if it is not already a Key.
 */
Key *
acid_make_key(PyObject *obj)
{
    PyObject *tmp = PyTuple_Pack(1, obj);
    PyObject *out = NULL;
    if(tmp) {
        out = key_new(&KeyType, tmp, NULL);
        Py_DECREF(tmp);
    }
    return (Key *) out;
}

/**
 * Destroy the key by deallocating any private memory, and decrementing the
 * refcount on any shared buffer.
 */
static void
key_dealloc(Key *self)
{
    switch((enum KeyFlags) self->flags) {
#ifdef HAVE_MEMSINK
    case KEY_SHARED:
        ms_cancel(Key_INFO(self)->source, (PyObject *)self);
        Py_DECREF(Key_INFO(self)->source);
        break;
#endif
    case KEY_COPIED:
        PyObject_Free(self->p);
        break;
    case KEY_PRIVATE:
        break;
    }
    PyObject_Free(self);
}

/**
 * Return a string or buffer object representing a key with the given prefix.
 */
PyObject *
acid_key_to_raw(Key *self, Slice *prefix)
{
    Py_ssize_t prefix_len = prefix->e - prefix->p;

    // Does the requested prefix fit in the slack area?
    if(self->flags != KEY_SHARED && prefix_len <= KEY_PREFIX_SLACK) {
        uint8_t *p = Key_DATA(self) - prefix_len;
        memcpy(p, prefix->p, prefix_len);
        return PyBuffer_FromObject(/* base */   (PyObject *)self,
                                   /* offset */ KEY_PREFIX_SLACK - prefix_len,
                                   /* size */   prefix_len + Key_SIZE(self));
    }

    Py_ssize_t need = prefix_len + Key_SIZE(self);
    PyObject *str = PyString_FromStringAndSize(NULL, need);
    if(str) {
        char *p = PyString_AS_STRING(str);
        memcpy(p, prefix->p, prefix_len);
        memcpy(p + prefix_len, Key_DATA(self), Key_SIZE(self));
    }
    return str;
}

/**
 * Return a new string representing the raw bytes in this key. Requires a
 * "prefix" parameter, which may be the empty string, representing the prefix
 * to include on the key.
 */
static PyObject *
key_to_raw(Key *self, PyObject *args)
{
    uint8_t *prefix_s = NULL;
    Py_ssize_t prefix_len = 0;
    if(! PyArg_ParseTuple(args, "|z#", &prefix_s, &prefix_len)) {
        return NULL;
    }
    Slice prefix = {prefix_s, prefix_s+prefix_len};
    return acid_key_to_raw(self, &prefix);
}

/**
 * Return the raw key data wrapped in hex.
 */
static PyObject *
key_to_hex(Key *self, PyObject *args, PyObject *kwds)
{
    PyObject *out = NULL;
    PyObject *raw = PyString_FromStringAndSize((char *)Key_DATA(self),
                                                       Key_SIZE(self));
    if(raw) {
        out = PyObject_CallMethod(raw, "encode", "s", "hex");
        Py_DECREF(raw);
    }
    return out;
}

/**
 * Return the next possible key that is lexigraphically larger than this one.
 * Note the returned Key is only useful to implement compares, it may not
 * decode to a valid value.
 */
static PyObject *
key_next_greater(Key *self, PyObject *args, PyObject *kwds)
{
    Slice slice;
    acid_key_as_slice(&slice, self);
    Py_ssize_t goodlen = acid_next_greater(&slice);
    // All bytes are 0xff, should never happen.
    if(goodlen == -1) {
        Py_RETURN_NONE;
    }

    Key *key = acid_make_private_key(Key_DATA(self), goodlen);
    if(key) {
        Key_DATA(key)[goodlen - 1]++;
    }
    return (PyObject *)key;
}

Key *
acid_key_next_greater(Key *self)
{
    return (Key *) key_next_greater(self, NULL, NULL);
}

/**
 * Convert a key in to_hex() representation back to a Key instance.
 */
static PyObject *
key_from_hex(PyTypeObject *cls, PyObject *args, PyObject *kwds)
{
    PyObject *hex;
    if(! PyArg_ParseTuple(args, "O", &hex)) {
        return NULL;
    }

    if(! PyString_CheckExact(hex)) {
        PyErr_Format(PyExc_TypeError, "parameter must be a string object.");
        return NULL;
    }

    Key *self = NULL;
    PyObject *decoded = PyObject_CallMethod(hex, "decode", "s", "hex");
    if(decoded) {
        self = acid_make_private_key((void *) PyString_AS_STRING(decoded),
                                PyString_GET_SIZE(decoded));
        Py_DECREF(decoded);
    }
    return (PyObject *) self;
}

/**
 * Given a raw bytestring and prefix, return a new Key instance.
 */
static PyObject *
key_from_raw(PyTypeObject *cls, PyObject *args, PyObject *kwds)
{
    char *prefix = "";
    char *raw;
    PyObject *source = NULL;
    Py_ssize_t prefix_len = 0;
    Py_ssize_t raw_len;

    if(! PyArg_ParseTuple(args, "s#|s#O", &raw, &raw_len,
                          &prefix, &prefix_len, &source)) {
        return NULL;
    }
    if(raw_len < prefix_len || memcmp(prefix, raw, prefix_len)) {
        Py_RETURN_NONE;
    }
    raw += prefix_len;
    raw_len -= prefix_len;

#ifdef HAVE_MEMSINK
    Key *out;
    if(source && ms_is_source(source)) {
        out = acid_make_shared_key(source, (void *) raw, raw_len);
    } else {
        out = acid_make_private_key((void *) raw, raw_len);
    }
    return (PyObject *)out;
#else
    return (PyObject *)acid_make_private_key((void *) raw, raw_len);
#endif
}

/**
 * Return a string representation of the Key instance.
 */
static PyObject *
key_repr(Key *self)
{
    PyObject *tup = PySequence_Tuple((PyObject *) self);
    if(! tup) {
        return NULL;
    }

    PyObject *tup_repr = PyObject_Repr(tup);
    Py_DECREF(tup);
    if(! tup_repr) {
        return NULL;
    }

    const char *repr_s = PyString_AS_STRING(tup_repr);
    PyObject *out = PyString_FromFormat("acid.Key%s", repr_s);
    Py_DECREF(tup_repr);
    return out;
}

/**
 * Return a new iterator over the instance.
 */
static PyObject *
key_iter(Key *self)
{
    KeyIter *iter = PyObject_New(KeyIter, &KeyIterType);
    if(iter) {
        iter->pos = 0;
        iter->key = self;
        Py_INCREF(self);
    }
    return (PyObject *) iter;
}

/**
 * Return a hash of the key's content.
 */
static long
key_hash(Key *self)
{
    uint8_t *p = Key_DATA(self);
    uint8_t *e = Key_SIZE(self) + p;
    long h = 0;
    while(p < e) {
        h = (1000003 * h) ^ *p++;
    }
    return h;
}

/**
 * Compare this Key with another.
 */
static PyObject *
key_richcompare(Key *self, PyObject *other, int op)
{
    int cmpres = 0;
    if(Py_TYPE(other) == &KeyType) {
        Key *otherk = (Key *)other;
        Slice s1 = {Key_DATA(self), Key_DATA(self) + Key_SIZE(self)};
        Slice s2 = {Key_DATA(otherk), Key_DATA(otherk) + Key_SIZE(otherk)};
        cmpres = acid_memcmp(&s1, &s2);
    } else if(Py_TYPE(other) == &PyTuple_Type) {
        struct writer wtr;
        if(acid_writer_init(&wtr, 64)) {
            return NULL;
        }

        Py_ssize_t ti = 0;
        Py_ssize_t remain = Key_SIZE(self);
        uint8_t *kp = Key_DATA(self);
        while(remain && ti < PyTuple_GET_SIZE(other)) {
            if(acid_write_element(&wtr, PyTuple_GET_ITEM(other, ti++))) {
                acid_writer_abort(&wtr);
                return NULL;
            }
            uint8_t *p = acid_writer_ptr(&wtr) - wtr.pos;
            Py_ssize_t minsz = (remain < wtr.pos) ? remain : wtr.pos;
            if((cmpres = memcmp(kp, p, minsz))) {
                break;
            }
            kp += minsz;
            remain -= minsz;
            wtr.pos = 0;
        }

        acid_writer_abort(&wtr);
        if(! cmpres) {
            if(remain) {
                cmpres = 1;
            } else if(ti < PyTuple_GET_SIZE(other)) {
                cmpres = -1;
            }
        }
    } else if(op == Py_EQ) {
        Py_RETURN_FALSE;
    } else if(op == Py_NE) {
        Py_RETURN_TRUE;
    } else {
        PyErr_Format(PyExc_TypeError, "Keys cannot be compared with '%s' objects.",
                     other->ob_type->tp_name);
        return NULL;
    }

    int ok = 0;
    switch(op) {
    case Py_LT:
        ok = cmpres < 0;
        break;
    case Py_LE:
        ok = cmpres <= 0;
        break;
    case Py_EQ:
        ok = cmpres == 0;
        break;
    case Py_NE:
        ok = cmpres != 0;
        break;
    case Py_GT:
        ok = cmpres > 0;
        break;
    case Py_GE:
        ok = cmpres >= 0;
    }
    if(ok) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

/**
 * Return the length of the tuple pointed to by `rdr`.
 */
static Py_ssize_t
key_length(Key *self)
{
    struct reader rdr;
    rdr.p = Key_DATA(self);
    rdr.e = Key_SIZE(self) + rdr.p;
    int eof = rdr.p == rdr.e;
    Py_ssize_t len = 0;

    while(! eof) {
        if(acid_skip_element(&rdr, &eof)) {
            return -1;
        }
        len++;
    }
    return len;
}

/**
 * Concatenate a Key with another or a tuple.
 */
static PyObject *
key_concat(Key *self, PyObject *other)
{
    Key *out = NULL;
    struct writer wtr;

    if(Py_TYPE(other) == &KeyType) {
        Key *otherk = (Key *)other;
        out = acid_make_private_key(NULL, Key_SIZE(self) + Key_SIZE(otherk));
        if(out) {
            memcpy(Key_DATA(out), Key_DATA(self), Key_SIZE(self));
            memcpy(Key_DATA(out), Key_DATA(otherk), Key_SIZE(otherk));
        }
    } else if(PyTuple_CheckExact(other)) {
        if(! acid_writer_init(&wtr, Key_SIZE(self) * 2)) {
            memcpy(acid_writer_ptr(&wtr), Key_DATA(self), Key_SIZE(self));
            wtr.pos += Key_SIZE(self);

            Py_ssize_t len = PyTuple_GET_SIZE(other);
            Py_ssize_t i;
            for(i = 0; i < len; i++) {
                if(acid_write_element(&wtr, PyTuple_GET_ITEM(other, i))) {
                    break;
                }
            }
            if(i == len) { // success
                uint8_t *ptr = acid_writer_ptr(&wtr) - wtr.pos;
                out = acid_make_private_key(ptr, wtr.pos);
            }
            acid_writer_abort(&wtr);
        }
    } else {
        PyErr_Format(PyExc_TypeError, "Key.add only accepts tuples or Keys.");
    }
    return (PyObject *) out;
}

/**
 * Fetch a slice of the Key.
 */
static PyObject *
key_subscript(Key *self, PyObject *key)
{
    if(PySlice_Check(key)) {
        // TODO: make this more efficient
        PyObject *tup = PySequence_Tuple((PyObject *) self);
        PyObject *slice = NULL;
        PyObject *newkey = NULL;
        if(tup) {
            PyMappingMethods *funcs = tup->ob_type->tp_as_mapping;
            slice = funcs->mp_subscript((PyObject *) tup, key);
            Py_DECREF(tup);
        }
        if(slice) {
            newkey = key_new(&KeyType, slice, NULL);
            Py_DECREF(slice);
        }
        return newkey;
    } else {
        // Fetch the `i`th item from the Key.
        Py_ssize_t i = PyNumber_AsSsize_t(key, PyExc_OverflowError);
        if(i == -1 && PyErr_Occurred()) {
            return NULL;
        }
        struct reader rdr;
        rdr.p = Key_DATA(self);
        rdr.e = Key_SIZE(self) + rdr.p;
        int eof = rdr.p == rdr.e;

        if(i < 0) {
            // TODO: make this more efficient
            Py_ssize_t len = key_length(self);
            i += len;
            eof |= i < 0;
        }
        while(i-- && !eof) {
            if(acid_skip_element(&rdr, &eof)) {
                return NULL;
            }
        }
        if(eof) {
            PyErr_SetString(PyExc_IndexError, "Key index out of range");
            return NULL;
        }
        return acid_read_element(&rdr);
    }
}

/**
 */
static Py_ssize_t
key_getreadbuffer(Key *self, Py_ssize_t segment, void **pp)
{
    uint8_t *p = self->p;
    Py_ssize_t ret = Key_SIZE(self);
    if(self->flags != KEY_SHARED) {
        ret += KEY_PREFIX_SLACK;
        p -= KEY_PREFIX_SLACK;
    }
    *pp = p;
    return ret;
}

/**
 */
static Py_ssize_t
key_getsegcount(Key *self, Py_ssize_t *lenp)
{
    if(lenp) {
        Py_ssize_t ret = Key_SIZE(self);
        if(self->flags != KEY_SHARED) {
            ret += KEY_PREFIX_SLACK;
        }
        *lenp = ret;
    }
    return 1;
}


static PySequenceMethods key_seq_methods = {
    .sq_length = (lenfunc) key_length,
    .sq_concat = (binaryfunc) key_concat,
    //.sq_item = (ssizeargfunc) key_item,
};

/** Needed to implement slicing. */
static PyMappingMethods key_mapping_methods = {
    .mp_length = (lenfunc) key_length,
    .mp_subscript = (binaryfunc) key_subscript
};

/** Needed to implement buffer interface. */
static PyBufferProcs key_buffer_methods = {
    .bf_getreadbuffer = (readbufferproc)key_getreadbuffer,
    .bf_getsegcount = (segcountproc)key_getsegcount,
    .bf_getcharbuffer = (charbufferproc)key_getreadbuffer
};

static PyMethodDef key_methods[] = {
    {"__sizeof__",  (PyCFunction)key_sizeof,   METH_NOARGS, ""},
    {"from_hex",    (PyCFunction)key_from_hex, METH_VARARGS|METH_CLASS, ""},
    {"from_raw",    (PyCFunction)key_from_raw, METH_VARARGS|METH_CLASS, ""},
    {"to_raw",      (PyCFunction)key_to_raw,   METH_VARARGS,            ""},
    {"to_hex",      (PyCFunction)key_to_hex,   METH_VARARGS|METH_KEYWORDS, ""},
    {"next_greater",(PyCFunction)key_next_greater, METH_NOARGS, ""},
    {0,             0,                         0,                       0}
};

static PyTypeObject KeyType = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "acid._keylib.Key",
    .tp_basicsize = sizeof(Key),
    .tp_itemsize = 1,
    .tp_iter = (getiterfunc) key_iter,
    .tp_hash = (hashfunc) key_hash,
    .tp_richcompare = (richcmpfunc) key_richcompare,
    .tp_new = key_new,
    .tp_dealloc = (destructor) key_dealloc,
    .tp_repr = (reprfunc) key_repr,
    .tp_str = (reprfunc) key_repr, // avoid str() using buffer interface.
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "acid._keylib.Key",
    .tp_methods = key_methods,
    .tp_as_buffer = &key_buffer_methods,
    .tp_as_sequence = &key_seq_methods,
    .tp_as_mapping = &key_mapping_methods
};


PyTypeObject *
acid_init_key_type(void)
{
    if(PyType_Ready(&KeyIterType)) {
        return NULL;
    }

    if(PyType_Ready(&KeyType)) {
        return NULL;
    }

#ifdef HAVE_MEMSINK
    MemSink_IMPORT;

    // KEY_SHARED are tracked in SharedKeyInfo, which is allocated at the end
    // of the Key structure.
    Py_ssize_t offset = offsetof(SharedKeyInfo, sink_node) + sizeof(Key);
    if(ms_init_sink(&KeyType, offset, invalidate_shared_key)) {
        return NULL;
    }
#endif

    return &KeyType;
}


// -------------
// Iterator Type
// -------------


/**
 * Satisfy the iterator protocol by returning a reference to ourself.
 */
static PyObject *
keyiter_iter(KeyIter *self)
{
    Py_INCREF((PyObject *) self);
    return (PyObject *) self;
}

/**
 * Satify the iterator protocol by returning the next element from the key.
 */
static PyObject *
keyiter_next(KeyIter *self)
{
    Py_ssize_t size = Key_SIZE(self->key);
    if(self->pos >= size) {
        return NULL;
    }

    uint8_t *p = Key_DATA(self->key) + self->pos;
    struct reader rdr = {p, p + (size - self->pos)};
    PyObject *elem = acid_read_element(&rdr);
    self->pos = rdr.p - Key_DATA(self->key);
    return elem;
}

/**
 * Do all required to destroy the instance.
 */
static void
keyiter_dealloc(KeyIter *self)
{
    Py_DECREF(self->key);
    PyObject_Del(self);
}


static PyMethodDef keyiter_methods[] = {
    {"next", (PyCFunction)keyiter_next, METH_NOARGS, ""},
    {0, 0, 0, 0}
};

static PyTypeObject KeyIterType = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "acid._keylib.KeyIterator",
    .tp_basicsize = sizeof(KeyIter),
    .tp_iter = (getiterfunc) keyiter_iter,
    .tp_iternext = (iternextfunc) keyiter_next,
    .tp_dealloc = (destructor) keyiter_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "acid._keylib.KeyIterator",
    .tp_methods = keyiter_methods
};
