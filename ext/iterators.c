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


static PyTypeObject IteratorType;
static PyTypeObject RangeIteratorType;

typedef enum {
    PRED_LE,
    PRED_LT,
    PRED_GT,
    PRED_GE
} Predicate;


typedef struct {
    PyObject_HEAD;

    PyObject *engine;
    PyObject *prefix;
    PyObject *tup;
    Key *lo;
    Key *hi;
    Predicate lo_pred;
    Predicate hi_pred;

    Py_ssize_t max;
    PyObject *it;
} Iterator;


typedef struct {
    Iterator base;
} RangeIterator;



// -------------
// Iterator Type
// -------------


static PyObject *
iter_set_lo(Iterator *self, PyObject *args, PyObject *kwds)
{
    PyObject *key_obj = NULL;
    int closed = 1;

    static char *keywords[] = {"key", "closed", NULL};
    if(PyArg_ParseTupleAndKeywords(args, kwds, "O|i", keywords,
                                   &key_obj, &closed)) {
        return NULL;
    }

    Py_CLEAR(self->lo);
    if(! ((self->lo = acid_make_key(key_obj)))) {
        return NULL;
    }
    self->lo_pred = closed ? PRED_LE : PRED_LT;
    Py_RETURN_NONE;
}


static PyObject *
iter_set_hi(Iterator *self, PyObject *args, PyObject *kwds)
{
    PyObject *key_obj = NULL;
    int closed = 0;

    static char *keywords[] = {"key", "closed", NULL};
    if(PyArg_ParseTupleAndKeywords(args, kwds, "O|i", keywords,
                                   &key_obj, &closed)) {
        return NULL;
    }

    Py_CLEAR(self->hi);
    if(! ((self->hi = acid_make_key(key_obj)))) {
        return NULL;
    }
    self->hi_pred = closed ? PRED_GE : PRED_GT;
    Py_RETURN_NONE;
}


static PyObject *
iter_set_prefix(Iterator *self, PyObject *args, PyObject *kwds)
{
    PyObject *key_obj = NULL;
    static char *keywords[] = {"key", NULL};
    if(PyArg_ParseTupleAndKeywords(args, kwds, "O", keywords,
                                   &key_obj)) {
        return NULL;
    }

    Py_CLEAR(self->lo);
    if(! ((self->lo = acid_make_key(key_obj)))) {
        return NULL;
    }

    Py_CLEAR(self->hi);
    if(! ((self->hi = acid_key_next_greater(self->lo)))) {
        return NULL;
    }

    self->lo_pred = PRED_GE;
    self->hi_pred = PRED_LT;
    Py_RETURN_NONE;
}


static PyObject *
iter_set_exact(Iterator *self, PyObject *args, PyObject *kwds)
{
    PyObject *key_obj = NULL;
    static char *keywords[] = {"key", NULL};
    if(PyArg_ParseTupleAndKeywords(args, kwds, "O", keywords,
                                   &key_obj)) {
        return NULL;
    }

    Py_CLEAR(self->lo);
    Py_CLEAR(self->hi);
    if(! ((self->hi = acid_make_key(key_obj)))) {
        return NULL;
    }
    self->lo = self->hi;
    Py_INCREF(self->lo);
    self->lo_pred = PRED_GE;
    self->hi_pred = PRED_LE;
    Py_RETURN_NONE;
}


static PyObject *
iter_set_max(Iterator *self, PyObject *args, PyObject *kwds)
{
    static char *keywords[] = {"max_", NULL};
    if(PyArg_ParseTupleAndKeywords(args, kwds, "O", keywords,
                                   &self->max)) {
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
iter_iter(Iterator *self)
{
    Py_INCREF(self);
    return NULL;
}


static PyMethodDef iter_methods[] = {
    {"set_lo", (PyCFunction)iter_set_lo, METH_VARARGS|METH_KEYWORDS, ""},
    {"set_hi", (PyCFunction)iter_set_hi, METH_VARARGS|METH_KEYWORDS, ""},
    {"set_prefix", (PyCFunction)iter_set_prefix, METH_VARARGS|METH_KEYWORDS, ""},
    {"set_exact", (PyCFunction)iter_set_exact, METH_VARARGS|METH_KEYWORDS, ""},
    {"set_max", (PyCFunction)iter_set_max, METH_VARARGS|METH_KEYWORDS, ""},
    {0, 0, 0, 0}
};

static PyTypeObject IteratorType = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "acid._iterators.Iterator",
    .tp_basicsize = sizeof(Iterator),
    .tp_iter = (getiterfunc) iter_iter,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "acid._iterators.Iterator",
    .tp_methods = iter_methods
};


// ------------------
// RangeIterator Type
// ------------------


/**
 * Construct a RangeIterator from provided arguments.
 */
static PyObject *
rangeiter_new(PyTypeObject *cls, PyObject *args, PyObject *kwds)
{
    return NULL;
}

/**
 * Satisfy the iterator protocol by returning a reference to ourself.
 */
static PyObject *
rangeiter_iter(KeyIter *self)
{
    Py_INCREF((PyObject *) self);
    return (PyObject *) self;
}

/**
 * Satify the iterator protocol by returning the next element from the key.
 */
static PyObject *
rangeiter_next(KeyIter *self)
{
    return NULL;
}

/**
 * Do all required to destroy the instance.
 */
static void
rangeiter_dealloc(KeyIter *self)
{
    PyObject_Del(self);
}


static PyMethodDef rangeiter_methods[] = {
    {"next", (PyCFunction)rangeiter_next, METH_NOARGS, ""},
    {0, 0, 0, 0}
};

static PyTypeObject RangeIteratorType = {
    PyObject_HEAD_INIT(NULL)
    .tp_new = rangeiter_new,
    .tp_name = "acid._iterators.RangeIterator",
    .tp_basicsize = sizeof(RangeIterator),
    .tp_iter = (getiterfunc) rangeiter_iter,
    .tp_iternext = (iternextfunc) rangeiter_next,
    .tp_dealloc = (destructor) rangeiter_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "acid._iterators.RangeIterator",
    .tp_methods = rangeiter_methods
};


int
acid_init_iterators_module(void)
{
    if(PyType_Ready(&RangeIteratorType)) {
        return -1;
    }

    PyObject *mod = acid_init_module("_iterators", NULL);
    if(! mod) {
        return -1;
    }

    if(PyModule_AddObject(mod, "Iterator", (PyObject *) &IteratorType)) {
        return -1;
    }

    return 0;
}
