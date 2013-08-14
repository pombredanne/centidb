#ifndef KEYCODER_H
#define KEYCODER_H

#ifndef PY_SSIZE_T_CLEAN
#define PY_SSIZE_T_CLEAN
#endif

#include <stdint.h>
#include "Python.h"


enum ElementKind
{
    KIND_NULL = 15,
    KIND_NEG_INTEGER = 20,
    KIND_INTEGER = 21,
    KIND_BOOL = 30,
    KIND_BLOB = 40,
    KIND_TEXT = 50,
    KIND_UUID = 90,
    KIND_KEY = 95,
    KIND_SEP = 102
};


struct reader
{
    uint8_t *p;
    Py_ssize_t size;
    Py_ssize_t pos;
};


struct writer
{
    PyObject *s;
    Py_ssize_t pos;
};


// Reference available from CObject _keycoder._C_API.
struct KeyCoderModule
{
    int (*reader_init)(struct reader *rdr, uint8_t *p, Py_ssize_t size);

    int (*writer_init)(struct writer *wtr, Py_ssize_t initial);
    int (*writer_putc)(struct writer *wtr, uint8_t o);
    int (*writer_puts)(struct writer *wtr, const char *restrict s, Py_ssize_t size);
    PyObject *(*writer_fini)(struct writer *wtr);

    int (*c_encode_value)(struct writer *wtr, PyObject *arg);
    int (*c_encode_key)(struct writer *wtr, PyObject *tup);
};


#endif /* !KEYCODER_H */
