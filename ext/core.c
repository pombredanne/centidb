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

#define _BSD_SOURCE // timegm()
#define _POSIX_C_SOURCE 200809L

#include "acid.h"

#include <arpa/inet.h>
#include <assert.h>
#include <math.h>
#include <stdarg.h>
#include <string.h>
#include <structmember.h>
#include <sys/types.h>
#include <time.h>


PyObject *log_exception;


/**
 * acid.core.dispatch(lst, *args)
 */
static PyObject *
py_dispatch(PyObject *self, PyObject *args)
{
    if(PyTuple_GET_SIZE(args) < 1) {
        PyErr_SetString(PyExc_TypeError, "dispatch() requires at least one argument.");
        return NULL;
    }

    PyObject *lst = PyTuple_GET_ITEM(args, 0);
    if(! PyList_CheckExact(lst)) {
        PyErr_SetString(PyExc_TypeError, "dispatch() first argument must be list.");
        return NULL;
    }

    PyObject *fargs = PyTuple_GetSlice(args, 1, PyTuple_GET_SIZE(args));
    if(! fargs) {
        return NULL;
    }

    Py_ssize_t i;
    PyObject *out = NULL;
    for(i = PyList_GET_SIZE(lst) - 1; i >= 0; i--) {
        PyObject *func = PyList_GET_ITEM(lst, i);
        PyObject *ret = PyObject_CallObject(func, fargs);
        if(ret) {
            Py_DECREF(ret);
        } else {
            PyObject *func_repr = PyObject_Repr(func);
            PyObject *fargs_repr = PyObject_Repr(fargs);
            if(! (func_repr && fargs_repr)) {
                Py_CLEAR(func_repr);
                Py_CLEAR(fargs_repr);
                goto argh;
            }
            PyObject *msg = PyString_FromFormat(
                "While invoking %s(*%s)",
                PyString_AS_STRING(func_repr),
                PyString_AS_STRING(fargs_repr));
            Py_DECREF(func_repr);
            Py_DECREF(fargs_repr);
            if(! msg) {
                goto argh;
            }
            ret = PyObject_CallFunctionObjArgs(log_exception, msg, NULL);
            Py_DECREF(msg);
            if(! ret) {
                goto argh;
            }
            PyErr_Clear();
            Py_DECREF(ret);
            if(PyList_SetSlice(lst, i-1, i, NULL)) {
                goto argh;
            }
        }
    }

    out = Py_None;
    Py_INCREF(out);
argh:
    Py_DECREF(fargs);
    return out;
}


/**
 * Table of functions exported in the acid._keylib module.
 */
static PyMethodDef CoreMethods[] = {
    {"dispatch", py_dispatch, METH_VARARGS, "dispatch"},
    {NULL, NULL, 0, NULL}
};


/**
 * Do all required to initialize acid._core.
 */
int
acid_init_core_module(void)
{
    PyObject *getLogger = acid_import_object("logging", "getLogger", NULL);
    if(! getLogger) {
        return -1;
    }

    PyObject *logger = PyObject_CallFunction(getLogger, "s", "acid.core");
    Py_DECREF(getLogger);
    if(! logger) {
        return -1;
    }

    log_exception = PyObject_GetAttrString(logger, "exception");
    Py_DECREF(logger);
    if(! log_exception) {
        return -1;
    }

    PyObject *mod = acid_init_module("_core", CoreMethods);
    return mod ? 0 : -1;
}
