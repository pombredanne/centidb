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

#ifdef HAVE_MEMSINK
    Key *out;
    if(source && ms_is_source(source)) {
        out = make_shared_key(source, (void *) raw, raw_len);
    } else {
        out = make_private_key((void *) raw, raw_len);
    }
    return (PyObject *)out;
#else
    return (PyObject *)make_private_key((void *) raw, raw_len);
#endif
}

/**
 * Return the length of the tuple pointed to by `rdr`.
 */
static Py_ssize_t
keylist_length(KeyList *self)
{
    if(self->length == -1) {
        struct reader rdr = {self->p, self->p + Py_SIZE(self)};
        int eof = rdr.p == rdr.e;
        Py_ssize_t len = 0;

        while(! eof) {
            if(skip_element(&rdr, &eof)) {
                return -1;
            }
            len++;
        }
    }
    return self->length;
}


static PyMethodDef keylist_methods[] = {
    {"from_raw", (PyCFunction)keylist_from_raw, METH_VARARGS|METH_CLASS, ""},
    {0, 0, 0, 0}
};

static PyTypeObject KeyListType = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "acid._keylib.KeyList",
    .tp_basicsize = sizeof(Key),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "acid._keylib.KeyList",
    .tp_methods = keylist_methods,
};


PyTypeObject *
init_keylist_type(void)
{
    if(! PyType_Ready(&KeyListType)) {
        return NULL;
    }
    return &KeyListType;
}
