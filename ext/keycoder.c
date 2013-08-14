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

#ifdef NDEBUG
#undef NDEBUG
#endif

#define PY_SSIZE_T_CLEAN
#include "Python.h"

#include <assert.h>
#include <string.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <structmember.h>

#include "keycoder.h"

// Python 2.5
#ifndef Py_TYPE
#   define Py_TYPE(ob) (((PyObject*)(ob))->ob_type)
#endif

#define DEBUG(x, y...) printf(x "\n", ## y);

#define LIST_START_SIZE 4
#define TUPLE_START_SIZE 3


static uint64_t swap64(uint64_t v)
{
    int n = 1;
    if(*((char *) &n) == 1) {
        v = __builtin_bswap64(v);
    }
    return v;
}


static PyTypeObject *UUID_Type;


static int reader_init(struct reader *rdr, uint8_t *p, Py_ssize_t size)
{
    rdr->p = p;
    rdr->size = size;
    rdr->pos = 0;
    return 1;
}


static int reader_getc(struct reader *rdr, uint8_t *ch)
{
    if(rdr->pos < rdr->size) {
        *ch = rdr->p[rdr->pos++];
        return 1;
    }
    return 0;
}


static int reader_getinto(struct reader *rdr, uint8_t *s, Py_ssize_t n)
{
    if((rdr->size - rdr->pos) < n) {
        PyErr_Format(PyExc_ValueError,
            "expected %lld bytes at position %lld, but only %lld remain.",
            (long long) n, (long long) rdr->pos,
            (long long) (rdr->size - rdr->pos));
        return 0;
    }
    memcpy(s, rdr->p + rdr->pos, n);
    rdr->pos += n;
    return 1;
}


static int writer_init(struct writer *wtr, Py_ssize_t initial)
{
    wtr->pos = 0;
    wtr->s = PyString_FromStringAndSize(NULL, initial);
    return wtr->s != NULL;
}


static int writer_grow(struct writer *wtr)
{
    Py_ssize_t cursize = PyString_GET_SIZE(wtr->s);
    Py_ssize_t newsize = cursize * 2;
    if(newsize > (cursize + 512)) {
        newsize = cursize + 512;
    }
    if(-1 == _PyString_Resize(&(wtr->s), newsize)) {
        PyErr_NoMemory();
        return 0;
    }
    return 1;
}


/* Append a single ordinal `o` to the buffer, growing it as necessary. */
static int writer_putc(struct writer *wtr, uint8_t o)
{
    if(! wtr->s) {
        return 0;
    }

    if((1 + wtr->pos) == PyString_GET_SIZE(wtr->s)) {
        if(! writer_grow(wtr)) {
            return 0;
        }
    }

    ((uint8_t *restrict)PyString_AS_STRING(wtr->s))[wtr->pos++] = o;
    return 1;
}


/* Append a bytestring `b` to the buffer, growing it as necessary. */
static int writer_puts(struct writer *wtr, const char *restrict s, Py_ssize_t size)
{
    assert(wtr->s);
    while((PyString_GET_SIZE(wtr->s) - (wtr->pos + 1)) < size) {
        if(! writer_grow(wtr)) {
            return 0;
        }
    }

    memcpy(PyString_AS_STRING(wtr->s) + wtr->pos, s, size);
    wtr->pos += size;
    return 1;
}


/* Resize the string to its final size, and return it. The StringWriter should
 * be discarded after calling finalize(). */
static PyObject *writer_fini(struct writer *wtr)
{
    if(! wtr->s) {
        return NULL;
    }
    _PyString_Resize(&(wtr->s), wtr->pos);
    PyObject *o = wtr->s;
    wtr->s = NULL;
    return o;
}


static PyObject *tuplize(PyObject *self, PyObject *arg)
{
    if(Py_TYPE(arg) == &PyTuple_Type) {
        Py_INCREF(arg);
    } else {
        PyObject *tup = PyTuple_New(1);
        if(! tup) {
            return NULL;
        }
        Py_INCREF(arg);
        PyTuple_SET_ITEM(tup, 0, arg);
        arg = tup;
    }
    return arg;
}


static int c_pack_int(struct writer *wtr, uint64_t v, enum ElementKind kind)
{
    uint8_t tmp[16];
    uint32_t tmp32;
    uint64_t tmp64;
    int size;

    if(kind) {
        if(! writer_putc(wtr, kind)) {
            return 0;
        }
    }

    if(v <= 240ULL) {
        return writer_putc(wtr, v);
    } else if(v <= 2287ULL) {
        v -= 240ULL;
        tmp[0] = 241 + (uint8_t) (v / 256);
        tmp[1] = (uint8_t) (v % 256);
        size = 2;
    } else if(v <= 67823) {
        v -= 2288ULL;
        tmp[0] = 0xf9;
        tmp[1] = (uint8_t) (v / 256);
        tmp[2] = (uint8_t) (v % 256);
        size = 3;
    } else if(v <= 16777215ULL) {
        tmp[0] = 0xfa;
        tmp32 = htonl((uint32_t) v);
        memcpy(tmp + 1, ((uint8_t *) &tmp32) + 1, 3);
        size = 4;
    } else if(v <= 4294967295ULL) {
        tmp[0] = 0xfb;
        tmp32 = htonl((uint32_t) v);
        memcpy(tmp + 1, ((uint8_t *) &tmp32), 4);
        size = 5;
    } else if(v <= 1099511627775ULL) {
        tmp64 = swap64(v);
        tmp[0] = 0xfc;
        memcpy(tmp + 1, ((uint8_t *) &tmp64) + 3, 5);
        size = 6;
    } else if(v <= 281474976710655ULL) {
        tmp64 = swap64(v);
        tmp[0] = 0xfd;
        memcpy(tmp + 1, ((uint8_t *) &tmp64) + 2, 6);
        size = 7;
    } else if(v <= 72057594037927935ULL) {
        tmp64 = swap64(v);
        tmp[0] = 0xfe;
        memcpy(tmp + 1, ((uint8_t *) &tmp64) + 1, 7);
        size = 8;
    } else {
        tmp[0] = 0xff;
        tmp64 = swap64(v);
        memcpy(tmp + 1, &tmp64, 8);
        size = 9;
    }
    return writer_puts(wtr, (char *)tmp, size);
}


static PyObject *pack_int(PyObject *self, PyObject *arg)
{
    uint64_t v;
    if(Py_TYPE(arg) == &PyInt_Type) {
        long l = PyInt_AsLong(arg);
        if(l < 0) {
            PyErr_SetString(PyExc_OverflowError,
                "pack_int(): v must be >= 0");
            return NULL;
        }
        v = (uint64_t) l;
    } else {
        v = PyLong_AsUnsignedLongLong(arg);
        if(PyErr_Occurred()) {
            return NULL;
        }
    }

    struct writer wtr;
    if(writer_init(&wtr, 9)) {
        if(c_pack_int(&wtr, v, 0)) {
            return writer_fini(&wtr);
        }
    }
    return NULL;
}


static int encode_str(struct writer *wtr, uint8_t *restrict p, Py_ssize_t length,
                      enum ElementKind kind)
{
    if(kind) {
        if(! writer_putc(wtr, kind)) {
            return 0;
        }
    }

    int shift = 1;
    uint8_t trailer = 0;

    int ret = 1;
    while(ret && length--) {
        uint8_t o = *(p++);
        ret = writer_putc(wtr, 0x80 | trailer | (o >> shift));
        if(shift < 7) {
            trailer = (o << (7 - shift));
            shift++;
        } else {
            ret = writer_putc(wtr, 0x80 | o);
            shift = 1;
            trailer = 0;
        }
    }

    if(ret && shift > 1) {
        ret = writer_putc(wtr, 0x80 | trailer);
    }
    if(ret) {
        ret = writer_putc(wtr, 0);
    }
    return ret;
}


static int c_encode_value(struct writer *wtr, PyObject *arg)
{
    int ret = 0;
    PyTypeObject *type = Py_TYPE(arg);

    if(arg == Py_None) {
        ret = writer_putc(wtr, KIND_NULL);
    } else if(type == &PyInt_Type) {
        long v = PyInt_AS_LONG(arg);
        if(v < 0) {
            ret = c_pack_int(wtr, -v, KIND_NEG_INTEGER);
        } else {
            ret = c_pack_int(wtr, v, KIND_INTEGER);
        }
    } else if(type == &PyString_Type) {
        ret = encode_str(wtr, (uint8_t *)PyString_AS_STRING(arg),
                              PyString_GET_SIZE(arg), KIND_BLOB);
    } else if(type == &PyUnicode_Type) {
        PyObject *utf8 = PyUnicode_EncodeUTF8(PyUnicode_AS_UNICODE(arg),
            PyUnicode_GET_SIZE(arg), "strict");
        if(utf8) {
            ret = encode_str(wtr, (uint8_t *)PyString_AS_STRING(utf8),
                                  PyString_GET_SIZE(utf8), KIND_TEXT);
            Py_DECREF(utf8);
        }
    } else if(type == &PyBool_Type) {
        ret = writer_putc(wtr, KIND_BOOL);
        ret = writer_putc(wtr, (uint8_t) (arg == Py_True));
    } else if(type == &PyLong_Type) {
        int64_t i64 = PyLong_AsLongLong(arg);
        if(! PyErr_Occurred()) {
            if(i64 < 0) {
                ret = c_pack_int(wtr, -i64, KIND_NEG_INTEGER);
            } else {
                ret = c_pack_int(wtr, i64, KIND_INTEGER);
            }
        }
    } else if(type == UUID_Type) {
        PyObject *ss = PyObject_CallMethod(arg, "get_bytes", NULL);
        if(ss) {
            assert(Py_TYPE(ss) == &PyString_Type);
            ret = encode_str(wtr, (uint8_t *)PyString_AS_STRING(ss),
                                  PyString_GET_SIZE(ss), KIND_UUID);
            Py_DECREF(ss);
        }
    } else {
        PyErr_Format(PyExc_TypeError, "packs(): got unsupported %.200s",
            arg->ob_type->tp_name);
    }

    return ret;
}


static int c_encode_key(struct writer *wtr, PyObject *tup)
{
    int ret = 1;
    for(Py_ssize_t i = 0; ret && i < PyTuple_GET_SIZE(tup); i++) {
        ret = c_encode_value(wtr, PyTuple_GET_ITEM(tup, i));
    }
    return ret;
}


static PyObject *packs(PyObject *self, PyObject *args)
{
    uint8_t *prefix = NULL;
    Py_ssize_t prefix_size;

    Py_ssize_t arg_count = PyTuple_GET_SIZE(args);
    if(arg_count != 2) {
        PyErr_SetString(PyExc_TypeError,
            "packs() takes exactly 2 arguments.");
        return NULL;
    }

    PyObject *py_prefix = PyTuple_GET_ITEM(args, 0);
    if(Py_TYPE(py_prefix) != &PyString_Type) {
        PyErr_SetString(PyExc_TypeError, "packs() prefix must be str.");
        return NULL;
    }
    prefix = (uint8_t *) PyString_AS_STRING(py_prefix);
    prefix_size = PyString_GET_SIZE(py_prefix);

    struct writer wtr;
    if(! writer_init(&wtr, 20)) {
        return NULL;
    }

    int ret = 1;
    if(prefix) {
        if(! writer_puts(&wtr, (char *)prefix, prefix_size)) {
            return NULL;
        }
    }

    PyObject *tups = PyTuple_GET_ITEM(args, 1);
    PyTypeObject *type = Py_TYPE(tups);

    if(type != &PyList_Type) {
        if(type != &PyTuple_Type) {
            ret = c_encode_value(&wtr, tups);
        } else {
            ret = c_encode_key(&wtr, tups);
        }
    } else {
        for(int i = 0; ret && i < PyList_GET_SIZE(tups); i++) {
            if(i) {
                ret = writer_putc(&wtr, KIND_SEP);
            }
            PyObject *elem = PyList_GET_ITEM(tups, i);
            type = Py_TYPE(elem);
            if(type != &PyTuple_Type) {
                ret = c_encode_value(&wtr, elem);
            } else {
                ret = c_encode_key(&wtr, elem);
            }
        }
    }

    if(ret) {
        return writer_fini(&wtr);
    }
    return NULL;
}


static int c_decode_int(struct reader *rdr, uint64_t *u64)
{
    uint8_t ch = 0;
    uint8_t ch2 = 0;
    uint8_t buf[8];

    if(! reader_getc(rdr, &ch)) {
        return 0;
    }

    if(ch <= 240) {
        *u64 = ch;
    } else if(ch <= 248) {
        if(! reader_getc(rdr, &ch2)) {
            return 0;
        }
        *u64 = 240 + (256 * (ch - 241) + ch2);
    } else if(ch == 249) {
        if(! reader_getinto(rdr, buf, 2)) {
            return 0;
        }
        *u64 = 2288 + (256 * buf[0]) + buf[1];
    } else if(ch == 250) {
        buf[0] = 0;
        if(! reader_getinto(rdr, buf + 1, 3)) {
            return 0;
        }
        *u64 = ntohl(*(uint32_t *) buf);
    } else if(ch == 251) {
        if(! reader_getinto(rdr, buf, 4)) {
            return 0;
        }
        *u64 = ntohl(*(uint32_t *) buf);
    } else if(ch == 252) {
        buf[0] = 0;
        buf[1] = 0;
        buf[2] = 0;
        if(! reader_getinto(rdr, buf+3, 5)) {
            return 0;
        }
        *u64 = swap64(*(uint64_t *) buf);
    } else if(ch == 253) {
        buf[0] = 0;
        buf[1] = 0;
        if(! reader_getinto(rdr, buf+2, 6)) {
            return 0;
        }
        *u64 = swap64(*(uint64_t *) buf);
    } else if(ch == 254) {
        buf[0] = 0;
        if(! reader_getinto(rdr, buf+1, 7)) {
            return 0;
        }
        *u64 = swap64(*(uint64_t *) buf);
    } else if(ch == 255) {
        if(! reader_getinto(rdr, buf, 8)) {
            return 0;
        }
        *u64 = swap64(*(uint64_t *) buf);
    }
    return 1;
}


static PyObject *c_decode_int_(struct reader *rdr, int negate)
{
    uint64_t u64;
    if(! c_decode_int(rdr, &u64)) {
        return NULL;
    }
    PyObject *v = PyLong_FromUnsignedLongLong(u64);
    if(v && negate) {
        PyObject *v2 = PyNumber_Negative(v);
        Py_DECREF(v);
        v = v2;
    }
    return v;
}


static PyObject *decode_str(struct reader *rdr)
{
    struct writer wtr;
    if(! writer_init(&wtr, 20)) {
        return NULL;
    }

    uint8_t lb = 0;
    uint8_t cb;

    int shift = 1;

    int ret = reader_getc(rdr, &lb);
    if(! lb) {
        return writer_fini(&wtr);
    }

    while(ret && reader_getc(rdr, &cb) && (cb != 0)) {
        uint8_t ch = lb << shift;
        ch |= (cb & 0x7f) >> (7 - shift);
        ret = writer_putc(&wtr, ch);
        if(shift < 7) {
            shift++;
            lb = cb;
        } else {
            shift = 1;
            ret = reader_getc(rdr, &lb);
            if(ret && !lb) {
                break;
            }
        }
    }
    return writer_fini(&wtr);
}


static PyObject *unpack(struct reader *rdr)
{
    PyObject *tup = PyTuple_New(TUPLE_START_SIZE);
    if(! tup) {
        return NULL;
    }

    Py_ssize_t tpos = 0;
    uint8_t ch;
    uint64_t u64;

    int go = 1;
    while(go && (rdr->pos < rdr->size)) {
        if(! reader_getc(rdr, &ch)) {
            break;
        }

        PyObject *arg = NULL;
        PyObject *tmp = NULL;

        switch(ch) {
        case KIND_NULL:
            arg = Py_None;
            Py_INCREF(arg);
            break;
        case KIND_INTEGER:
            arg = c_decode_int_(rdr, 0);
            break;
        case KIND_NEG_INTEGER:
            arg = c_decode_int_(rdr, 1);
            break;
        case KIND_BOOL:
            if(c_decode_int(rdr, &u64)) {
                arg = u64 ? Py_True : Py_False;
                Py_INCREF(arg);
            }
            break;
        case KIND_BLOB:
            arg = decode_str(rdr);
            break;
        case KIND_TEXT:
            tmp = decode_str(rdr);
            if(tmp) {
                arg = PyUnicode_DecodeUTF8(PyString_AS_STRING(tmp),
                    PyString_GET_SIZE(tmp), "strict");
            }
            break;
        case KIND_UUID:
            tmp = decode_str(rdr);
            if(tmp) {
                arg = PyObject_CallFunctionObjArgs(
                    (PyObject *)UUID_Type, Py_None, tmp, NULL);
            }
            break;
        case KIND_SEP:
            go = 0;
            break;
        default:
            PyErr_Format(PyExc_ValueError, "bad kind %d; key corrupt?", ch);
            Py_DECREF(tup);
            return NULL;
        }

        if(! go) {
            break;
        }

        Py_CLEAR(tmp);
        if(! arg) {
            Py_DECREF(tup);
            return NULL;
        }
        if(tpos == PyTuple_GET_SIZE(tup)) {
            if(-1 == _PyTuple_Resize(&tup, PyTuple_GET_SIZE(tup) + 2)) {
                return NULL;
            }
        }
        PyTuple_SET_ITEM(tup, tpos++, arg);
    }
    PyTuple_GET_SIZE(tup) = tpos;
    return tup;
}


static PyObject *py_unpack(PyObject *self, PyObject *args)
{
    uint8_t *prefix;
    uint8_t *s;
    Py_ssize_t s_len;
    Py_ssize_t prefix_len;

    if(! PyArg_ParseTuple(args, "s#s#", (char **) &prefix, &prefix_len,
                                        (char **) &s, &s_len)) {
        return NULL;
    }
    if(s_len < prefix_len) {
        PyErr_SetString(PyExc_ValueError,
            "unpacks() input smaller than prefix.");
        return NULL;
    }
    if(memcmp(prefix, s, prefix_len)) {
        Py_RETURN_NONE;
    }

    struct reader rdr;
    if(! reader_init(&rdr, s, s_len)) {
        return NULL;
    }
    rdr.pos += prefix_len;
    return unpack(&rdr);
}


static PyObject *unpacks(PyObject *self, PyObject *args)
{
    uint8_t *prefix;
    uint8_t *s;
    Py_ssize_t prefix_len;
    Py_ssize_t s_len;

    if(! PyArg_ParseTuple(args, "s#s#", (char **) &prefix, &prefix_len,
                                        (char **) &s, &s_len)) {
        return NULL;
    }
    if(s_len < prefix_len) {
        PyErr_SetString(PyExc_ValueError,
            "unpacks() prefix smaller than input.");
        return NULL;
    }
    if(memcmp(prefix, s, prefix_len)) {
        Py_RETURN_NONE;
    }

    struct reader rdr;
    if(! reader_init(&rdr, s, s_len)) {
        return NULL;
    }

    rdr.pos += prefix_len;
    PyObject *tups = PyList_New(LIST_START_SIZE);
    if(! tups) {
        return NULL;
    }

    Py_ssize_t lpos = 0;
    while(rdr.pos < rdr.size) {
        PyObject *tup = unpack(&rdr);
        if(! tup) {
            Py_DECREF(tups);
            return NULL;
        }

        if(lpos < LIST_START_SIZE) {
            PyList_SET_ITEM(tups, lpos++, tup);
        } else {
            if(-1 == PyList_Append(tups, tup)) {
                Py_DECREF(tups);
                Py_DECREF(tup);
                return NULL;
            }
            Py_DECREF(tup);
            lpos++;
        }
    }
    PyTuple_GET_SIZE(tups) = lpos;
    return tups;
}


static struct KeyCoderModule C_API = {
    .reader_init = reader_init,
    .writer_init = writer_init,
    .writer_putc = writer_putc,
    .writer_puts = writer_puts,
    .writer_fini = writer_fini,
    .c_encode_value = c_encode_value,
    .c_encode_key = c_encode_key
};


static PyMethodDef KeyCoderMethods[] = {
    {"tuplize", tuplize, METH_O, "tuplize"},
    {"unpack", py_unpack, METH_VARARGS, "unpack"},
    {"unpacks", unpacks, METH_VARARGS, "unpacks"},
    {"pack", packs, METH_VARARGS, "pack"},
    {"packs", packs, METH_VARARGS, "packs"},
    {"pack_int", pack_int, METH_O, "pack_int"},
    {NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC
init_keycoder(void)
{
    PyObject *mod = PyImport_ImportModule("uuid");
    if(! mod) {
        return;
    }

    PyObject *dct = PyModule_GetDict(mod);
    if(! dct) {
        Py_DECREF(mod);
        return;
    }
    UUID_Type = (PyTypeObject *) PyDict_GetItemString(dct, "UUID");
    assert(PyType_CheckExact((PyObject *) UUID_Type));


    mod = Py_InitModule("centidb._keycoder", KeyCoderMethods);
    if(! mod) {
        return;
    }

    dct = PyModule_GetDict(mod);
    if(! dct) {
        return;
    }

    PyObject *capi = PyCapsule_New(&C_API, "centidb._keycoder._C_API", NULL);
    if(capi) {
        PyDict_SetItemString(dct, "_C_API", capi);
    }
}
