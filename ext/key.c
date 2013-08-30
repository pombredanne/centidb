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
static int
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
    if(! writer_init(wtr, 32)) {
        return NULL;
    }

    int len = PyTuple_GET_SIZE(args);
    for(int i = 0; i < len; i++) {
        PyObject *arg = PyTuple_GET_ITEM(args, i);
        if(! write_element(&wtr, arg)) {
            writer_abort(&wtr);
            return NULL;
        }
    }

    Key *self = make_private_key(writer_ptr(&wtr) - wtr->pos);
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
    }
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
    PyObject *raw = PyString_FromStringAndSize(self->s, Py_SIZE(self));
    if(raw) {
        out = PyObject_CallMethod(raw, "encode", "s", "hex"):
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
        self = make_private_key(PyString_AS_STRING(decoded),
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
    return (PyObject *) make_private_key(raw + prefix_len, need);
}

/**
 * Return a string representation of the Key instance.
 */
static PyObject *
key_repr(Key *self)
{
    PyObject *tup = PySequence_Tuple(self);
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


static PyMethodDef offset_methods[] = {
    {"from_hex",    (PyCFunction)key_from_hex, METH_VARARGS|METH_CLASS, ""},
    {"from_raw",    (PyCFunction)key_from_raw, METH_VARARGS|METH_CLASS, ""},
    {"to_raw",      (PyCFunction)key_to_raw,   METH_VARARGS,            ""},
    {"to_hex",      (PyCFunction)key_to_hex,   METH_VARARGS|METH_KW,    ""},
    {0,             0,                         0,                       0}
};

static PyTypeObject KeyType = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "centidb._keycoder.Key",
    .tp_basicsize = sizeof(Key),
    .tp_itemsize = 1,
    .tp_new = (newproc) key_new,
    .tp_dealloc = (destructor) key_dealloc,
    .tp_repr = (reprfunc) key_repr,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "centidb._keycoder.Key",
    .tp_methods = offset_methods
};


PyTypeObject *
init_key_type(void)
{
    if(PyType_Ready(&KeyType)) {
        return NULL;
    }
    return &KeyType;
}
