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

#define DUMPBUF(x, z) \
    DEBUG("Buf is len %zd '%s'", z, acid_debug_hex(x, z));

#define DUMPSTR(x) \
    DEBUG("Str is len %d '%s'", (int) PyString_GET_SIZE(x), \
          acid_debug_hex(PyString_AS_STRING(x), PyString_GET_SIZE(x)));

#define DUMPKEY(x) \
    DEBUG("Key is len %d '%s'", (int) Key_SIZE(x), \
          acid_debug_hex(x->p, Key_SIZE(x)))


// Python 2.5
#ifndef Py_TYPE
#   define Py_TYPE(ob) (((PyObject*)(ob))->ob_type)
#endif

/** Initial preallocation for lists during _keylib.unpacks(). */
#define LIST_START_SIZE 4

/** Initial preallocation for tuples during _keylib.unpack(). */
#define TUPLE_START_SIZE 3

/** Granularity of UTC offset for KIND_DATETIME. */
#define UTCOFFSET_DIV (15 * 60)

/** Added to UTC offset after division to produce an unsigned int. */
#define UTCOFFSET_SHIFT 64


// Forward declarations.
struct Key;


/**
 * Key element types.
 */
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

/**
 * Represent an iterator predicate.
 */
typedef enum {
    PRED_LE,
    PRED_LT,
    PRED_GT,
    PRED_GE
} Predicate;

/**
 * Represent a (Key, Predicate) pair.
 */
typedef struct {
    /** Strong reference, or NULL if bound inactive. */
    struct Key *key;
    /** Match predicate. */
    Predicate pred;
} Bound;

/**
 * Storage mode for a Key instance.
 */
enum KeyFlags {
    /** Key is stored in a shared buffer. */
    KEY_SHARED = 1,
    /** Key was stored in a shared buffer, but the buffer expired, so we copied
     * it to a new heap allocation. */
    KEY_COPIED = 2,
    /** Key was created uniquely for this instance, buffer was included in
     * instance allocation during construction time. */
    KEY_PRIVATE = 4
};

/**
 * Represents a bounded slice of memory to be read from. Various
 * acid_slice_*() functions use this interface.
 */
typedef struct reader
{
    /** Current read position. */
    uint8_t *p;
    /** Position of last byte. */
    uint8_t *e;
} Slice;

/**
 * Represent a partially constructed, resizable PyString to be written to.
 * Various acid_writer_*() functions use this interface.
 */
struct writer
{
    /** Pointer to partially constructed PyString. Reference to this must only
     * be taken via acid_writer_finalize(). */
    PyObject *s;
    /** Current write offset. */
    Py_ssize_t pos;
};

/**
 * Shared Key information structure. During acid_make_shared_key(), this is
 * allocated as the "variable data" part of the Key PyVarObject. On shared
 * buffer invalidation, its memory may be reused to store the copied key value
 * if it fits.
 */
typedef struct {
    /** Strong reference to the source object. */
    PyObject *source;
#ifdef HAVE_MEMSINK
    /** Linked list of consumers monitoring `source`. */
    struct ms_node sink_node;
#endif
} SharedKeyInfo;


// -------------------
// Instance structures
// -------------------


/**
 * _keylib.Key. The key is contained in `p[0..Key_SIZE(key)]`.
 */
typedef struct Key {
    PyObject_HEAD
    /** In all modes, pointer to start of key. */
    uint8_t *p;
    /** Data size (max 64kb). */
    uint16_t size;
    /** Storage mode. */
    uint16_t /*enum KeyFlags*/ flags;
} Key;

#define KEY_PREFIX_SLACK 0
#define KEY_MAXSIZE UINT16_MAX
#define Key_DATA(k) ((k)->p + KEY_PREFIX_SLACK)
#define Key_SIZE(k) ((k)->size)
// TODO: relies on arch padding rules?
#define Key_INFO(k) ((SharedKeyInfo *) (((uint8_t *)k) + sizeof(Key)))


/**
 * _keylib.KeyIterator.
 */
typedef struct {
    PyObject_HEAD
    /** Strong reference to Key being iterated. */
    Key *key;
    /** Current offset into `key->p`. */
    Py_ssize_t pos;
} KeyIter;

/**
 * _iterators.Iterator.
 */
typedef struct {
    PyObject_HEAD;

    /** Strong reference to engine to be iterated. */
    PyObject *engine;
    /** Strong reference to MemSink source, or NULL. */
    PyObject *source;
    /** String reference to PyString collection prefix. */
    PyObject *prefix;
    /** Lower bound. */
    Bound lo;
    /** Upper bound. */
    Bound hi;
    /* Points to `hi' or `lo', or NULL for no stop bound. */
    Bound *stop;
    /** If >=0, maximum elements to yield, otherwise <0. */
    Py_ssize_t max;
    /** The underlying storage engine iterator. */
    PyObject *it;
    /** Last tuple yielded by `it', or NULL. */
    PyObject *tup;
    /** If 1, next() should fetch new tuple from `it' before yielding. */
    int started;
    /** List of keys decoded from the current physical engine key, or NULL. */
    PyObject *keys;
} Iterator;

/**
 * _iterators.RangeIterator.
 */
typedef struct {
    /** Base Iterator fields. */
    Iterator base;
} RangeIterator;


// ----------
// Prototypes
// ----------


int acid_writer_init(struct writer *wtr, Py_ssize_t initial);
uint8_t *acid_writer_ptr(struct writer *wtr);
void acid_writer_abort(struct writer *wtr);
int acid_write_element(struct writer *wtr, PyObject *arg);
PyObject *acid_read_element(struct reader *rdr);
int acid_skip_element(struct reader *rdr, int *eof);


PyTypeObject *acid_init_fixed_offset_type(void);
PyObject *acid_get_fixed_offset(int offset_secs);

PyObject *
acid_init_module(const char *name, PyMethodDef *methods);
int
acid_init_core_module(void);

PyTypeObject *acid_init_key_type(void);
Key *acid_make_key(PyObject *arg);
Key *acid_key_next_greater(Key *self);
PyObject *acid_key_to_raw(Key *self, Slice *prefix);

const char *
acid_debug_hex(uint8_t *s, Py_ssize_t len);
int
acid_make_reader(Slice *rdr, PyObject *buf);
void
acid_string_as_slice(Slice *slice, PyObject *str);
void
acid_key_as_slice(Slice *slice, Key *key);
Py_ssize_t
acid_next_greater(Slice *slice);
PyObject *
acid_next_greater_str(Slice *slice);
int acid_memcmp(Slice *s1, Slice *s2);

int acid_init_keylib_module(void);
int acid_init_iterators_module(void);

PyObject *
acid_import_object(const char *module, ...);

PyTypeObject *
acid_init_keylist_type(void);
PyObject *
acid_keylist_from_raw(uint8_t *raw, Py_ssize_t raw_len, PyObject *source);

Key *acid_make_private_key(uint8_t *p, Py_ssize_t size);
#ifdef HAVE_MEMSINK
Key *acid_make_shared_key(PyObject *source, uint8_t *p, Py_ssize_t size);
#endif

#endif /* !ACID_H */
