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

#include "acid.h"
#include "structmember.h"


static PyTypeObject KeyListType;


static PyObject *
make_key(uint8_t *p, Py_ssize_t length, PyObject *source)
{
#ifdef HAVE_MEMSINK
    Key *out;
    if(source && ms_is_source(source)) {
        out = acid_make_shared_key(source, p, length);
    } else {
        out = acid_make_private_key(p, length);
    }
    return (PyObject *)out;
#else
    return (PyObject *)make_private_key(p, length);
#endif
}

/**
 * Given a raw bytestring and prefix, return a list of Key instances.
 */
static PyObject *
keylist_from_raw(PyTypeObject *cls, PyObject *args, PyObject *kwds)
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

    PyObject *out = PyList_New(0);
    if(! out) {
        return NULL;
    }

    struct reader rdr = {(uint8_t *) raw, (uint8_t *) raw + raw_len};
    int eof = rdr.p == rdr.e;
    uint8_t *start = rdr.p;
    while(! eof) {
        if(acid_skip_element(&rdr, &eof)) {
            Py_DECREF(out);
            return NULL;
        }
        if(eof && start != rdr.p) {
            PyObject *key = make_key(start, rdr.p - start, source);
            if(! key) {
                Py_DECREF(out);
                return NULL;
            }
            if(PyList_Append(out, key)) {
                Py_DECREF(key);
                Py_DECREF(out);
                return NULL;
            }
            Py_DECREF(key);
            rdr.p++;
            start = rdr.p;
            eof = 0;
        }
    }

    return out;
}


static PyMethodDef keylist_methods[] = {
    {"from_raw", (PyCFunction)keylist_from_raw, METH_VARARGS|METH_CLASS, ""},
    {0, 0, 0, 0}
};

static void keylist_dealloc(void)
{
}

static PyTypeObject KeyListType = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "acid._keylib.KeyList",
    .tp_basicsize = sizeof(PyObject),
    //.tp_dealloc = keylist_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "acid._keylib.KeyList",
    .tp_methods = keylist_methods,
};


PyTypeObject *
acid_init_keylist_type(void)
{
    if(PyType_Ready(&KeyListType)) {
        return NULL;
    }
    return &KeyListType;
}
