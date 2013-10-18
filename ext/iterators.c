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

// Forward declarations.
static PyTypeObject IteratorType;
static PyTypeObject RangeIteratorType;


// -------------
// Iterator Type
// -------------


/**
 * Iterator(engine, prefix). 
 *
 * Not exposed to Python.
 */
static Iterator *
iter_new(PyTypeObject *cls, PyObject *args, PyObject *kwds)
{
    PyObject *engine = NULL;
    PyObject *prefix = NULL;

    static char *keywords[] = {"engine", "prefix", NULL};
    if(! PyArg_ParseTupleAndKeywords(args, kwds, "OS", keywords,
                                     &engine, &prefix)) {
        return NULL;
    }

    if(! PyString_GET_SIZE(prefix)) {
        // next_greater() would fail in this case.
        PyErr_SetString(PyExc_ValueError, "'prefix' cannot be 0 bytes.");
        return NULL;
    }

    Iterator *self = PyObject_New(Iterator, cls);
    if(self) {
        Py_INCREF(engine);
        self->engine = engine;
        Py_INCREF(prefix);
        self->prefix = prefix;
        self->lo.key = NULL;
        self->hi.key = NULL;
        self->stop = NULL;
        self->max = -1;
        self->it = NULL;
        self->tup = NULL;
        self->started = 0;
        self->keys = NULL;

        if(! ((self->source = PyObject_GetAttrString(engine, "source")))) {
            if(PyErr_Occurred()) {
                PyErr_Clear();
            }
        } else if(self->source == Py_None) {
            Py_CLEAR(self->source);
        }
    }
    return self;
}

/**
 * Set or replace a bound.
 */
static void set_bound(Bound *bound, Key *key, Predicate pred)
{
    Py_CLEAR(bound->key);
    if(key) {
        Py_INCREF(key);
    }
    bound->key = key;
    bound->pred = pred;
}

/**
 * Fetch the next tuple from the physical iterator, ensuring it's of the right
 * type, and that the key is within the collection prefix. Return 0 on success,
 * or -1 on exhaustion or error. Use PyErr_Occurred() on -1 to test for error.
 */
static int
iter_step(Iterator *self)
{
    Py_CLEAR(self->keys);
    Py_CLEAR(self->tup);
    if(! self->it) {
        return -1;
    }

    if(! ((self->tup = PyIter_Next(self->it)))) {
        Py_CLEAR(self->it);
        return -1;
    }

    if(! (PyTuple_CheckExact(self->tup) &&
          PyTuple_GET_SIZE(self->tup) == 2)) {
        PyErr_SetString(PyExc_TypeError,
            "Engine.iter() must yield (key, value) strings or buffers.");
        return -1;
    }

    struct reader rdr;
    if(acid_make_reader(&rdr, PyTuple_GET_ITEM(self->tup, 0))) {
        return -1;
    }

    Py_ssize_t prefix_len = PyString_GET_SIZE(self->prefix);
    if(((rdr.e - rdr.p) < prefix_len) ||
        memcmp(rdr.p, PyString_AS_STRING(self->prefix), prefix_len)) {
        return -1;
    }

    rdr.p += prefix_len;
    self->keys = acid_keylist_from_raw(rdr.p, rdr.e-rdr.p, self->source);
    if(! self->keys) {
        return -1;
    }
    return 0;
}

/**
 * Clear any references from the iterator to child objects. The iterator may be
 * safely deallocated afterwards.
 */
static void
iter_clear(Iterator *self)
{
    Py_CLEAR(self->engine);
    Py_CLEAR(self->prefix);
    Py_CLEAR(self->source);
    Py_CLEAR(self->lo.key);
    Py_CLEAR(self->hi.key);
    Py_CLEAR(self->it);
    Py_CLEAR(self->tup);
    Py_CLEAR(self->keys);
    self->stop = NULL;
}

/**
 * Iterator.set_lo().
 */
static PyObject *
iter_set_lo(Iterator *self, PyObject *args, PyObject *kwds)
{
    PyObject *key_obj = NULL;
    int closed = 1;

    static char *keywords[] = {"key", "closed", NULL};
    if(! PyArg_ParseTupleAndKeywords(args, kwds, "O|i", keywords,
                                     &key_obj, &closed)) {
        return NULL;
    }

    set_bound(&self->lo, acid_make_key(key_obj),
              closed ? PRED_LE : PRED_LT);
    if(! self->lo.key) {
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Iterator.set_hi().
 */
static PyObject *
iter_set_hi(Iterator *self, PyObject *args, PyObject *kwds)
{
    PyObject *key_obj = NULL;
    int closed = 0;

    static char *keywords[] = {"key", "closed", NULL};
    if(! PyArg_ParseTupleAndKeywords(args, kwds, "O|i", keywords,
                                     &key_obj, &closed)) {
        return NULL;
    }

    set_bound(&self->hi, acid_make_key(key_obj),
              closed ? PRED_GE : PRED_GT);
    if(! self->hi.key) {
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Iterator.set_prefix().
 */
static PyObject *
iter_set_prefix(Iterator *self, PyObject *args, PyObject *kwds)
{
    PyObject *key_obj = NULL;
    static char *keywords[] = {"key", NULL};
    if(! PyArg_ParseTupleAndKeywords(args, kwds, "O", keywords,
                                     &key_obj)) {
        return NULL;
    }

    set_bound(&self->lo, acid_make_key(key_obj), PRED_GE);
    set_bound(&self->hi, acid_key_next_greater(self->lo.key), PRED_LT);
    if(! (self->lo.key && self->hi.key)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Iterator.set_exact().
 */
static PyObject *
iter_set_exact(Iterator *self, PyObject *args, PyObject *kwds)
{
    PyObject *key_obj = NULL;
    static char *keywords[] = {"key", NULL};
    if(! PyArg_ParseTupleAndKeywords(args, kwds, "O", keywords,
                                     &key_obj)) {
        return NULL;
    }

    set_bound(&self->lo, acid_make_key(key_obj), PRED_LE);
    set_bound(&self->hi, self->lo.key, PRED_GE);
    if(! self->lo.key) {
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Iterator.set_max().
 */
static PyObject *
iter_set_max(Iterator *self, PyObject *args, PyObject *kwds)
{
    static char *keywords[] = {"max_", NULL};
    if(! PyArg_ParseTupleAndKeywords(args, kwds, "n", keywords,
                                     &self->max)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Satisfy the iterator protocol by returning a reference to ourself.
 */
static PyObject *
iter_iter(Iterator *self)
{
    Py_INCREF(self);
    return (PyObject *) self;
}

/**
 * Setup the underlying iterator. Return 0 on success or -1 and set an
 * exception on error.
 */
static int
iter_start(Iterator *self, PyObject *key, int reverse)
{
    /* Not using PyObject_CallMethod it produces crap exceptions */
    PyObject *func = PyObject_GetAttrString(self->engine, "iter");
    if(! func) {
        return -1;
    }

    Py_CLEAR(self->it);
    PyObject *py_reverse = reverse ? Py_True : Py_False;
    self->it = PyObject_CallFunction(func, "OO", key, py_reverse);
    Py_DECREF(func);
    Py_DECREF(key);
    if(! self->it) {
        return -1;
    }
    return 0;
}

/**
 * Getter for `Iterator.key`.
 */
static PyObject *iter_get_key(Iterator *self)
{
    PyObject *out = Py_None;
    if(self->keys && PyList_GET_SIZE(self->keys)) {
        out = PyList_GET_ITEM(self->keys, 0);
    }
    Py_INCREF(out);
    return out;
}

/**
 * Getter for `Iterator.keys`.
 */
static PyObject *iter_get_keys(Iterator *self)
{
    PyObject *out = Py_None;
    if(self->keys) {
        out = self->keys;
    }
    Py_INCREF(out);
    return out;
}

/**
 * Getter for `Iterator.data'.
 */
static PyObject *iter_get_data(Iterator *self)
{
    PyObject *out = Py_None;
    if(self->tup) {
        out = PyTuple_GET_ITEM(self->tup, 1);
    }
    Py_INCREF(out);
    return out;
}


static PyGetSetDef iter_props[] = {
    {"key", (getter)iter_get_key, NULL, "", NULL},
    {"keys", (getter)iter_get_keys, NULL, "", NULL},
    {"data", (getter)iter_get_data, NULL, "", NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

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
    .tp_methods = iter_methods,
    .tp_getset = iter_props
};


// ------------------
// RangeIterator Type
// ------------------


/**
 * RangeIterator(engine, prefix).
 */
static PyObject *
rangeiter_new(PyTypeObject *cls, PyObject *args, PyObject *kwds)
{
    return (PyObject *) iter_new(cls, args, kwds);
}

/**
 * RangeIterator.__del__().
 */
static void
rangeiter_dealloc(KeyIter *self)
{
    iter_clear((Iterator *) self);
    PyObject_Del(self);
}

/**
 * Return 1 if the encoded key presented by `s[0..len]` matches the given `key`
 * and `predicate`, otherwise 0.
 */
static int
test_bound(Bound *bound, uint8_t *p, Py_ssize_t len)
{
    int out = 1;
    if(bound) {
        Key *key = bound->key;
        if(key) {
            Slice key_slice;
            acid_key_as_slice(&key_slice, key);
            Slice slice = {p, p+len};
            int rc = acid_memcmp(&key_slice, &slice);
            switch(bound->pred) {
            case PRED_LE:
                out = rc <= 0;
                break;
            case PRED_LT:
                out = rc < 0;
                break;
            case PRED_GT:
                out = rc > 0;
                break;
            case PRED_GE:
                out = rc >= 0;
                break;
            }
        }
    }
    return out;
}

/**
 * RangeIterator.next().
 */
static PyObject *
rangeiter_next(RangeIterator *self)
{
    if(! (self->base.it && self->base.keys)) {
        return NULL;
    }
    if(! self->base.max--) {
        iter_clear(&self->base);
        return NULL;
    }

    /* First iteration was done by forward()/reverse(). */
    if(! self->base.started) {
        self->base.started = 1;
    } else if(iter_step(&self->base)) {
        return NULL;
    }

    Key *k = (Key *)PyList_GET_ITEM(self->base.keys, 0);
    if(! test_bound(self->base.stop, k->p, Key_SIZE(k))) {
        Py_CLEAR(self->base.it);
        Py_CLEAR(self->base.tup);
        Py_CLEAR(self->base.keys);
        return NULL;
    }

    Py_INCREF((PyObject *)self);
    return (PyObject *)self;
}

/**
 * RangeIterator.forward().
 */
static PyObject *
rangeiter_forward(RangeIterator *self)
{
    PyObject *key;
    if(self->base.lo.key) {
        Slice prefix;
        acid_string_as_slice(&prefix, self->base.prefix);
        key = acid_key_to_raw(self->base.lo.key, &prefix);
    } else {
        key = self->base.prefix;
        Py_INCREF(key);
    }

    if(! (key && !iter_start(&self->base, key, 0))) {
        return NULL;
    }

    if(! iter_step(&self->base)) {
        /* When lo(closed=False), skip the start key. */
        Key *k = (Key *)PyList_GET_ITEM(self->base.keys, 0);
        if(! test_bound(&self->base.lo, k->p, Key_SIZE(k))) {
            iter_step(&self->base);
        }
    }

    self->base.started = 0;
    self->base.stop = &self->base.hi;
    Py_INCREF((PyObject *)self);
    return (PyObject *)self;
}

/**
 * RangeIterator.reverse().
 */
static PyObject *
rangeiter_reverse(RangeIterator *self)
{
    Slice prefix;
    acid_string_as_slice(&prefix, self->base.prefix);
    PyObject *key;
    if(self->base.hi.key) {
        key = acid_key_to_raw(self->base.hi.key, &prefix);
    } else {
        key = acid_next_greater_str(&prefix);
    }

    // TODO: may "return without exception set" if next_greater failed.
    if(! (key && !iter_start(&self->base, key, 1))) {
        return NULL;
    }

    /* Fetch the first key. If _step() returns false, then we may have seeked
     * to first record of next prefix, so skip first returned result. */
    if(iter_step(&self->base) && !self->base.it) {
        return NULL;
    }

    /* When hi(closed=False), skip the start key. */
    for(; self->base.it; iter_step(&self->base)) {
        if(! self->base.keys) {
            continue;
        }
        Key *k = (Key *)PyList_GET_ITEM(self->base.keys, 0);
        if(! test_bound(&self->base.hi, k->p, Key_SIZE(k))) {
            continue;
        }
        break;
    }

    self->base.started = 0;
    self->base.stop = &self->base.lo;
    Py_INCREF((PyObject *)self);
    return (PyObject *)self;
}

static PyMethodDef rangeiter_methods[] = {
    {"next", (PyCFunction)rangeiter_next, METH_NOARGS, ""},
    {"forward", (PyCFunction)rangeiter_forward, METH_NOARGS, ""},
    {"reverse", (PyCFunction)rangeiter_reverse, METH_NOARGS, ""},
    {0, 0, 0, 0}
};

static PyTypeObject RangeIteratorType = {
    PyObject_HEAD_INIT(NULL)
    .tp_base = &IteratorType,
    .tp_new = rangeiter_new,
    .tp_dealloc = (destructor) rangeiter_dealloc,
    .tp_name = "acid._iterators.RangeIterator",
    .tp_basicsize = sizeof(RangeIterator),
    .tp_iternext = (iternextfunc) rangeiter_next,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "acid._iterators.RangeIterator",
    .tp_methods = rangeiter_methods
};

/**
 * acid._iterators.from_args().
 */
static PyObject *
py_from_args(PyObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *o;

    if(PyTuple_GET_SIZE(args) != 9) {
        PyErr_SetString(PyExc_TypeError, "from_args requires 9 parameters.");
        return NULL;
    }

    Iterator *it = (Iterator *)PyTuple_GET_ITEM(args, 0);
    if(! PyObject_IsInstance((PyObject *)it, (PyObject *)&IteratorType)) {
        PyErr_SetString(PyExc_TypeError, "from_args arg #1 must be Iterator.");
        return NULL;
    }

    // key=
    if(((o = PyTuple_GET_ITEM(args, 1))) != Py_None) {
        set_bound(&it->lo, acid_make_key(o), PRED_LE);
        set_bound(&it->hi, it->lo.key, PRED_GE);
        if(! it->lo.key) {
            return NULL;
        }
        return PyObject_CallMethod((PyObject *)it, "forward", "");
    // prefix=
    } else if(((o = PyTuple_GET_ITEM(args, 4))) != Py_None) {
        set_bound(&it->lo, acid_make_key(o), PRED_GE);
        set_bound(&it->hi, acid_key_next_greater(it->lo.key), PRED_LT);
        if(! (it->lo.key && it->hi.key)) {
            return NULL;
        }
    } else {
        int include = PyObject_IsTrue(PyTuple_GET_ITEM(args, 7));
        if(((o = PyTuple_GET_ITEM(args, 2))) != Py_None) {
            set_bound(&it->lo, acid_make_key(o), PRED_LE);
            if(! it->lo.key) {
                return NULL;
            }
        }
        if(((o = PyTuple_GET_ITEM(args, 3))) != Py_None) {
            set_bound(&it->hi, acid_make_key(o), include ? PRED_GE : PRED_GT);
            if(! it->hi.key) {
                return NULL;
            }
        }
    }

    if(((o = PyTuple_GET_ITEM(args, 6))) != Py_None) {
        it->max = PyNumber_AsSsize_t(o, NULL);
    }
    o = PyTuple_GET_ITEM(args, 5);
    char *meth = PyObject_IsTrue(o) ? "reverse" : "forward";
    return PyObject_CallMethod((PyObject *)it, meth, "");
}

/**
 * Table of functions exported in the acid._iterators module.
 */
static PyMethodDef IteratorsMethods[] = {
    {"from_args", (PyCFunction)py_from_args, METH_VARARGS, "from_args"},
    {NULL, NULL, 0, NULL}
};

/**
 * Do all required to initialize acid._iterators, returning 0 on success or -1
 * on error.
 */
int
acid_init_iterators_module(void)
{
    if(PyType_Ready(&IteratorType)) {
        return -1;
    }
    if(PyType_Ready(&RangeIteratorType)) {
        return -1;
    }

    PyObject *mod = acid_init_module("_iterators", /*IteratorsMethods*/0);
    if(! mod) {
        return -1;
    }

    if(PyModule_AddObject(mod, "Iterator", (PyObject *) &IteratorType)) {
        return -1;
    }
    if(PyModule_AddObject(mod, "RangeIterator", (PyObject *) &RangeIteratorType)) {
        return -1;
    }

    return 0;
}
