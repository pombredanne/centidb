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


static PyTypeObject RangeIteratorType;

enum Predicate {
    PRED_LE;
    PRED_LT;
    PRED_GT;
    PRED_GE;
};


struct RangeIterator {
    PyObject *engine;
    PyObject *prefix;
    PyObject *tup;
    Key *lo;
    Key *hi;
    Predicate lo_pred;
    Predicate hi_pred;

    PyObject *it;
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
init_iterator_module(void)
{
    if(PyType_Ready(&RangeIteratorType)) {
        return -1;
    }

    PyObject *mod = acid_init_module("_iterators");
    if(! mod) {
        return NULL;
    }

    if(PyModule_AddObject(mod, "RangeIterator", (PyObject *) &RangeIteratorType)) {
        return -1;
    }

    return 0;
}
