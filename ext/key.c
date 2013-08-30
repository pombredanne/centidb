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
#include <string.h>

#include "centidb.h"
#include "structmember.h"


static PyTypeObject KeyType;
static PyTypeObject KeyIterType;

enum KeyFlags {
    // Key is stored in a shared buffer.
    KEY_SHARED = 1,
    // Key was stored in a shared buffer, but the buffer expired, so we copied
    // it to a new heap allocation.
    KEY_COPIED = 2,
    // Key was created uniquely for this instance, buffer was included in
    // instance allocation during construction time.
    KEY_PRIVATE = 4
};

typedef struct {
    PyObject_VAR_HEAD
    // Size is tracked in Py_SIZE(Key).
    enum KeyFlags flags;
    // If KEY_SHARED, strong reference to source object.
    PyObject *source;
    // In all cases, points to data.
    uint8_t *p;
} Key;

typedef struct {
    PyObject_HEAD
    // Key we're iterating over.
    Key *key;
    // Current position into key->p.
    Py_ssize_t pos;
} KeyIter;


/**
 * Construct a new Key instance from `p[0..size]` and return it.
 */
static Key *
make_private_key(uint8_t *p, Py_ssize_t size)
{
    Key *self = PyObject_NewVar(Key, &KeyType, size);
    if(self) {
        self->flags = KEY_PRIVATE;
        self->p = (uint8_t *) &self[1];
        memcpy(self->p, p, size);
    }
    return self;
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
    if(! writer_init(&wtr, 32)) {
        return NULL;
    }

    Py_ssize_t len = PyTuple_GET_SIZE(args);
    for(Py_ssize_t i = 0; i < len; i++) {
        PyObject *arg = PyTuple_GET_ITEM(args, i);
        if(! write_element(&wtr, arg)) {
            writer_abort(&wtr);
            return NULL;
        }
    }

    Key *self = make_private_key(writer_ptr(&wtr) - wtr.pos, wtr.pos);
    writer_abort(&wtr);
    return (PyObject *) self;
}

/**
 * Destroy the key by deallocating any private memory, and decrementing the
 * refcount on any shared buffer.
 */
static void
key_dealloc(Key *self)
{
    switch(self->flags) {
    case KEY_SHARED:
        Py_DECREF(self->source);
        // TODO: unlink from notifier list.
        break;
    case KEY_COPIED:
        free(self->p);
        break;
    case KEY_PRIVATE:
        break;
    }
    PyObject_Del(self);
}

/**
 * Return a new string representing the raw bytes in this key. Requires a
 * "prefix" parameter, which may be the empty string, representing the prefix
 * to include on the key.
 */
static PyObject *
key_to_raw(Key *self, PyObject *args)
{
    uint8_t *prefix;
    Py_ssize_t prefix_len;
    if(! PyArg_ParseTuple(args, "s#", &prefix, &prefix_len)) {
        return NULL;
    }

    Py_ssize_t need = prefix_len + Py_SIZE(self);
    PyObject *str = PyString_FromStringAndSize(NULL, need);
    if(str) {
        char *p = PyString_AS_STRING(str);
        memcpy(p, prefix, prefix_len);
        memcpy(p + prefix_len, self->p, Py_SIZE(self));
    }
    return str;
}

/**
 * Return the raw key data wrapped in hex.
 */
static PyObject *
key_to_hex(Key *self, PyObject *args, PyObject *kwds)
{
    PyObject *out = NULL;
    PyObject *raw = PyString_FromStringAndSize((void *)self->p, Py_SIZE(self));
    if(raw) {
        out = PyObject_CallMethod(raw, "encode", "s", "hex");
        Py_DECREF(raw);
    }
    return out;
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
        self = make_private_key((void *) PyString_AS_STRING(decoded),
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
    char *prefix;
    char *raw;
    Py_ssize_t prefix_len;
    Py_ssize_t raw_len;

    if(! PyArg_ParseTuple(args, "s#s#", &prefix, &prefix_len, &raw, &raw_len)) {
        return NULL;
    }
    if(raw_len < prefix_len || memcmp(prefix, raw, prefix_len)) {
        Py_RETURN_NONE;
    }
    return (PyObject *) make_private_key((void *) raw + prefix_len,
                                         raw_len - prefix_len);
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
    PyObject *out = PyString_FromFormat("<centidb.Key %s>", repr_s);
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
 * Return the length of the tuple pointed to by `rdr`.
 */
static Py_ssize_t
key_length(Key *self)
{
    struct reader rdr = {self->p, self->p + Py_SIZE(self)};
    int eof = rdr.p == rdr.e;
    Py_ssize_t len = 0;

    while(! eof) {
        if(skip_element(&rdr, &eof)) {
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
        Py_ssize_t combined = Py_SIZE(self) + Py_SIZE(other);
        if(writer_init(&wtr, combined)) {
            uint8_t *p = writer_ptr(&wtr);
            memcpy(p, self->p, Py_SIZE(self));
            memcpy(p + Py_SIZE(self), ((Key *)other)->p, Py_SIZE(other));
            out = make_private_key(p, combined);
            writer_abort(&wtr);
        }
    } else if(PyTuple_CheckExact(other)) {
        if(writer_init(&wtr, Py_SIZE(self) * 2)) {
            memcpy(writer_ptr(&wtr), self->p, Py_SIZE(self));
            wtr.pos += Py_SIZE(self);

            Py_ssize_t len = PyTuple_GET_SIZE(other);
            Py_ssize_t i;
            for(i = 0; i < len; i++) {
                if(! write_element(&wtr, PyTuple_GET_ITEM(other, i))) {
                    break;
                }
            }
            if(i == len) { // success
                uint8_t *ptr = writer_ptr(&wtr) - wtr.pos;
                out = make_private_key(ptr, wtr.pos);
            }
            writer_abort(&wtr);
        }
    } else {
        PyErr_Format(PyExc_TypeError, "Key.add only accepts tuples or Keys.");
    }
    return (PyObject *) out;
}


static PyObject *
key_item(Key *self, Py_ssize_t i)
{
    struct reader rdr = {self->p, self->p + Py_SIZE(self)};
    int eof = rdr.p == rdr.e;

    if(i < 0) {
        // TODO: can this be made more efficient?
        Py_ssize_t len = key_length(self);
        i = len - i;
        eof |= i < 0;
    }
    while(i-- && !eof) {
        if(skip_element(&rdr, &eof)) {
            return NULL;
        }
    }
    if(eof) {
        PyErr_SetString(PyExc_IndexError, "Key index out of range");
        return NULL;
    }
    return read_element(&rdr);
}


static PyNumberMethods key_number_methods = {
};

static PySequenceMethods key_seq_methods = {
    .sq_length = (lenfunc) key_length,
    .sq_concat = (binaryfunc) key_concat,
    .sq_item = (ssizeargfunc) key_item,
};

static PyMethodDef key_methods[] = {
    {"from_hex",    (PyCFunction)key_from_hex, METH_VARARGS|METH_CLASS, ""},
    {"from_raw",    (PyCFunction)key_from_raw, METH_VARARGS|METH_CLASS, ""},
    {"to_raw",      (PyCFunction)key_to_raw,   METH_VARARGS,            ""},
    {"to_hex",      (PyCFunction)key_to_hex,   METH_VARARGS|METH_KEYWORDS, ""},
    {0,             0,                         0,                       0}
};

static PyTypeObject KeyType = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "centidb._keycoder.Key",
    .tp_basicsize = sizeof(Key),
    .tp_itemsize = 1,
    .tp_iter = (getiterfunc) key_iter,
    .tp_new = key_new,
    .tp_dealloc = (destructor) key_dealloc,
    .tp_repr = (reprfunc) key_repr,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "centidb._keycoder.Key",
    .tp_methods = key_methods,
    .tp_as_number = &key_number_methods,
    .tp_as_sequence = &key_seq_methods
};


PyTypeObject *
init_key_type(void)
{
    if(PyType_Ready(&KeyIterType)) {
        return NULL;
    }

    if(PyType_Ready(&KeyType)) {
        return NULL;
    }

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
    Py_ssize_t size = Py_SIZE(self->key);
    if(self->pos >= size) {
        return NULL;
    }

    uint8_t *p = self->key->p + self->pos;
    struct reader rdr = {p, p + (size - self->pos)};
    PyObject *elem = read_element(&rdr);
    self->pos = (rdr.p - self->key->p);
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
    .tp_name = "centidb._keycoder.KeyIterator",
    .tp_basicsize = sizeof(KeyIter),
    .tp_iter = (getiterfunc) keyiter_iter,
    .tp_iternext = (iternextfunc) keyiter_next,
    .tp_dealloc = (destructor) keyiter_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "centidb._keycoder.KeyIterator",
    .tp_methods = keyiter_methods
};
