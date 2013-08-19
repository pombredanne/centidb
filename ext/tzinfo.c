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
#include "structmember.h"


// Array of pointer to instance of tzinfo implementing our fixed offset types.
// Only one needs to be created per offset.
static PyObject *tzinfos[128];

typedef struct {
    PyObject_HEAD
    int offset_secs;
} FixedOffset;


static int
fixedoffset_init(FixedOffset *self, PyObject *args, PyObject *kwds)
{
    int offset;
    if(! PyArg_ParseTuple(args, "i", &offset)) {
        return -1;
    }

    self->offset_secs = offset;
    return 0;
}


static PyObject *
fixedoffset_utcoffset(FixedOffset *self, PyObject *args)
{
    return PyDelta_FromDSU(0, self->offset_secs, 0);
}


static PyObject *
fixedoffset_dst(FixedOffset *self, PyObject *args)
{
    Py_RETURN_NONE;
}


static PyObject *
fixedoffset_tzname(FixedOffset *self, PyObject *args)
{
    long offset = self->offset_secs;
    char sign = '+';
    if(offset < 0) {
        sign = '-';
        offset = -offset;
    }

    long hours = offset / 60;
    long minutes = offset % 60;
    return PyString_FromFormat("<%c%02ld:%02ld>", sign, hours, minutes);
}


static PyMethodDef offset_methods[] = {
    {"utcoffset",   (PyCFunction)fixedoffset_utcoffset, METH_VARARGS,   ""},
    {"dst",         (PyCFunction)fixedoffset_dst,       METH_VARARGS,   ""},
    {"tzname",      (PyCFunction)fixedoffset_tzname,    METH_VARARGS,   ""},
    {0,             0,                                  0,              0}
};

static PyTypeObject FixedOffsetType = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "centidb._keycoder.FixedOffset",
    .tp_basicsize = sizeof(FixedOffset),
    .tp_init = (initproc) fixedoffset_init,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "_centidb.FixedOffset",
    .tp_methods = offset_methods
};


PyTypeObject *
init_tzinfo_type(void)
{
    PyDateTime_IMPORT;
    FixedOffsetType.tp_base = PyDateTimeAPI->TZInfoType;
    if(PyType_Ready(&FixedOffsetType)) {
        return NULL;
    }
    return &FixedOffsetType;
}


PyObject *
get_fixed_offset(int offset_secs)
{
    int idx = UTCOFFSET_SHIFT + (offset_secs / UTCOFFSET_DIV);
    PyObject *info = tzinfos[idx];
    if(! info) {
        info = PyObject_CallFunction((PyObject *) &FixedOffsetType, "i",
                                     offset_secs);
        if(! info) {
            return NULL;
        }
        tzinfos[idx] = info;
    }
    Py_INCREF(info);
    return info;
}
