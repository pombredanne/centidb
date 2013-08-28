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

#ifndef CENTIDB_H
#define CENTIDB_H

#ifndef PY_SSIZE_T_CLEAN
#define PY_SSIZE_T_CLEAN
#endif

#ifdef NDEBUG
#undef NDEBUG
#endif

#define DEBUG(x, y...) printf(x "\n", ## y);

#include <stdint.h>
#include "Python.h"
#include "datetime.h"


// Python 2.5
#ifndef Py_TYPE
#   define Py_TYPE(ob) (((PyObject*)(ob))->ob_type)
#endif

#define LIST_START_SIZE 4
#define TUPLE_START_SIZE 3


enum ElementKind
{
    KIND_NULL = 15,
    KIND_NEG_INTEGER = 20,
    KIND_INTEGER = 21,
    KIND_BOOL = 30,
    KIND_BLOB = 40,
    KIND_TEXT = 50,
    KIND_UUID = 90,
    KIND_NEG_TIME = 91,
    KIND_TIME = 92,
    KIND_SEP = 102
};


struct reader
{
    uint8_t *p;
    uint8_t *e;
};


struct writer
{
    PyObject *s;
    Py_ssize_t pos;
};


// Reference available from CObject centidb._keycoder._C_API.
struct KeyCoderModule
{
    int (*writer_init)(struct writer *wtr, Py_ssize_t initial);
    int (*writer_putc)(struct writer *wtr, uint8_t o);
    int (*writer_puts)(struct writer *wtr, const char *restrict s, Py_ssize_t size);
    PyObject *(*writer_fini)(struct writer *wtr);

    int (*c_encode_value)(struct writer *wtr, PyObject *arg);
    int (*c_encode_key)(struct writer *wtr, PyObject *tup);
};


#define UTCOFFSET_SHIFT 64
#define UTCOFFSET_DIV (15 * 60)

PyTypeObject *init_fixed_offset_type(void);
PyObject *get_fixed_offset(int offset_secs);


#endif /* !CENTIDB_H */
