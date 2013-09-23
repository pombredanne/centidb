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

#ifndef ACID_H
#define ACID_H

#ifndef PY_SSIZE_T_CLEAN
#define PY_SSIZE_T_CLEAN
#endif

#ifdef NDEBUG
#undef NDEBUG
#endif


#include "Python.h"
#include <stdint.h>
#include <stdio.h>

#ifdef HAVE_MEMSINK
#include "memsink.h"
#endif


#define DEBUG(s, ...) fprintf(stderr, \
    "acid: %s:%s:%d: " s "\n", __FILE__, __func__, __LINE__, ## __VA_ARGS__);


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
    long hash;
    // Size is tracked in Py_SIZE(Key).
    enum KeyFlags flags;
    // In all cases, points to data.
    uint8_t *p;
} Key;

// Structure allocated as the variable part of Key when KEY_SHARED.
typedef struct {
    // If KEY_SHARED, strong reference to source object.
    PyObject *source;
#ifdef HAVE_MEMSINK
    // Linked list of consumers monitoring source.
    struct ms_node sink_node;
#endif
} SharedKeyInfo;


typedef struct {
    PyObject_HEAD
    // Key we're iterating over.
    Key *key;
    // Current position into key->p.
    Py_ssize_t pos;
} KeyIter;


#define UTCOFFSET_SHIFT 64
#define UTCOFFSET_DIV (15 * 60)

int writer_init(struct writer *wtr, Py_ssize_t initial);
uint8_t *writer_ptr(struct writer *wtr);
void writer_abort(struct writer *wtr);
int write_element(struct writer *wtr, PyObject *arg);
PyObject *read_element(struct reader *rdr);
int skip_element(struct reader *rdr, int *eof);


PyTypeObject *init_fixed_offset_type(void);
PyObject *get_fixed_offset(int offset_secs);

PyTypeObject *init_key_type(void);


#endif /* !ACID_H */
