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

#include "centidb.h"

#include <assert.h>
#include <string.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <structmember.h>


static struct KeyCoderModule KeyCoder;

typedef struct {
    PyObject_HEAD
    PyObject *coll;
    PyObject *data;
    PyObject *key;
    PyObject *batch;
    PyObject *txn_id;
    PyObject *index_keys;
} Record;

static PyMemberDef RecordMembers[] = {
    {"coll", T_OBJECT, offsetof(Record, coll), 0, "collection"},
    {"data", T_OBJECT, offsetof(Record, data), 0, "data"},
    {"key", T_OBJECT, offsetof(Record, key), 0, "key"},
    {"batch", T_OBJECT, offsetof(Record, batch), 0, "batch"},
    {"txn_id", T_OBJECT, offsetof(Record, txn_id), 0, "txn_id"},
    {"index_keys", T_OBJECT, offsetof(Record, index_keys), 0, "index_keys"},
    {NULL}
};


static int record_compare(PyObject *, PyObject *);
static PyObject *record_repr(PyObject *);
static PyObject *record_new(PyTypeObject *, PyObject *, PyObject *);
static void record_dealloc(PyObject *);
static PyTypeObject RecordType = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "_centidb.Record",
    .tp_basicsize = sizeof(Record),
    .tp_dealloc = record_dealloc,
    .tp_compare = record_compare,
    .tp_repr = record_repr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc = "_centidb.Record",
    .tp_new = record_new,
    .tp_members = RecordMembers
};


static PyObject *record_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    Record *self = (Record *)type->tp_alloc(type, 0);
    if(! self) {
        return NULL;
    }

    self->coll = NULL;
    self->data = NULL;
    self->key = NULL;
    self->batch = NULL;
    self->txn_id = NULL;
    self->index_keys = NULL;
    if(! PyArg_ParseTuple(args, "OO|OOOO",
            &self->coll, &self->data, &self->key, &self->batch,
            &self->txn_id, &self->index_keys)) {
        return NULL;
    }
    Py_XINCREF(self->coll);
    Py_XINCREF(self->data);
    Py_XINCREF(self->key);
    Py_XINCREF(self->batch);
    Py_XINCREF(self->txn_id);
    Py_XINCREF(self->index_keys);
    return (PyObject *) self;
}


static void record_dealloc(PyObject *self_)
{
    Record *self = (Record *)self_;
    Py_CLEAR(self->coll);
    Py_CLEAR(self->data);
    Py_CLEAR(self->key);
    Py_CLEAR(self->batch);
    Py_CLEAR(self->txn_id);
    Py_CLEAR(self->index_keys);
    self->ob_type->tp_free(self_);
}


static PyObject *record_repr(PyObject *self)
{
    Record *record = (Record *)self;
    PyObject *info = PyObject_GetAttrString(record->coll, "info");
    if(! info) {
        return NULL;
    }

    PyObject *name_s = PyString_FromString("name");
    if(! name_s) {
        return NULL;
    }

    PyObject *name = PyObject_GetItem(info, name_s);
    Py_DECREF(name_s);
    if(! name) {
        return NULL;
    }

    struct writer wtr;
    if(! KeyCoder.writer_init(&wtr, 40)) {
        return NULL;
    }

    KeyCoder.writer_puts(&wtr, "<Record ", 8);
    KeyCoder.writer_puts(&wtr, PyString_AS_STRING(name),
                               PyString_GET_SIZE(name));
    Py_DECREF(name);
    KeyCoder.writer_puts(&wtr, ":(", 2);
    for(int i = 0; i < PyTuple_GET_SIZE(record->key); i++) {
        if(i) {
            KeyCoder.writer_putc(&wtr, ',');
        }
        PyObject *repr = PyObject_Repr(PyTuple_GET_ITEM(record->key, i));
        if(repr) {
            KeyCoder.writer_puts(&wtr, PyString_AS_STRING(repr),
                                       PyString_GET_SIZE(repr));
            Py_DECREF(repr);
        }
    }
    KeyCoder.writer_puts(&wtr, ") ", 2);
    PyObject *repr = PyObject_Repr(record->data);
    if(repr) {
        KeyCoder.writer_puts(&wtr, PyString_AS_STRING(repr), PyString_GET_SIZE(repr));
        Py_DECREF(repr);
    }

    KeyCoder.writer_putc(&wtr, '>');
    return KeyCoder.writer_fini(&wtr);
}


static int dumb_cmp(PyObject *x, PyObject *y)
{
    if(x) {
        return y ? PyObject_Compare(x, y) : -1;
    } else {
        return y ? 1 : 0;
    }
}


static int record_compare(PyObject *self_, PyObject *other_)
{
    Record *self = (Record *)self_;
    int ret = -1;
    if(Py_TYPE(other_) == &RecordType) {
        Record *other = (Record *)other_;
        ret = PyObject_Compare(self->coll, other->coll);
        if(! ret) {
            ret = dumb_cmp(self->data, other->data);
        }
        if(! ret) {
            ret = dumb_cmp(self->key, other->key);
        }
    }
    return ret;
}


static PyMethodDef CentidbMethods[] = {
    {NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC
init_centidb(void)
{
    // PyCapsule_Import can't import modules from inside packages, so we do it.
    PyObject *tmp = PyImport_ImportModule("centidb._keycoder");
    Py_CLEAR(tmp);

    struct KeyCoderModule *keycoder = PyCapsule_Import("centidb._keycoder._C_API", 0);
    if(! keycoder) {
        return;
    }
    KeyCoder = *(struct KeyCoderModule *) keycoder;

    PyObject *mod = Py_InitModule("centidb._centidb", CentidbMethods);
    if(! mod) {
        return;
    }

    if(-1 == PyType_Ready(&RecordType)) {
        return;
    }
    PyModule_AddObject(mod, "Record", (void *) &RecordType);
}
