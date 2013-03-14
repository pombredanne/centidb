
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

#define DEBUG(x, y...) printf(x "\n", ## y);

#define LIST_START_SIZE 4
#define TUPLE_START_SIZE 3


static uint64_t swap64(uint64_t v)
{
#define IS_LITTLE_ENDIAN (*(uint16_t *)"\0\xff" >= 0x100)
    if(IS_LITTLE_ENDIAN) {
        v = __builtin_bswap64(v);
    }
    return v;
}


typedef struct {
    PyObject_HEAD
    PyObject *coll;
    PyObject *data;
    PyObject *key;
    PyObject *batch;
    PyObject *txn_id;
    PyObject *index_keys;
} Record;

static PyMemberDef RecordMembers[] = {
    {"coll", T_OBJECT, offsetof(Record, coll), 0, "collection"},
    {"data", T_OBJECT, offsetof(Record, data), 0, "data"},
    {"key", T_OBJECT, offsetof(Record, key), 0, "key"},
    {"batch", T_OBJECT, offsetof(Record, batch), 0, "batch"},
    {"txn_id", T_OBJECT, offsetof(Record, txn_id), 0, "txn_id"},
    {"index_keys", T_OBJECT, offsetof(Record, index_keys), 0, "index_keys"},
    {NULL}
};


static int record_compare(PyObject *, PyObject *);
static PyObject *record_repr(PyObject *);
static PyObject *record_new(PyTypeObject *, PyObject *, PyObject *);
static void record_dealloc(PyObject *);
static PyTypeObject RecordType = {
    PyObject_HEAD_INIT(NULL)
    .tp_name = "_centidb.Record",
    .tp_basicsize = sizeof(Record),
    .tp_dealloc = record_dealloc,
    .tp_compare = record_compare,
    .tp_repr = record_repr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc = "_centidb.Record",
    .tp_new = record_new,
    .tp_members = RecordMembers
};


static PyObject *builder_new(PyTypeObject *, PyObject *, PyObject *);
static void builder_dealloc(PyObject *);
static PyObject *builder_build(PyObject *, PyObject *);

struct IndexInfo {
    PyObject *prefix;
    PyObject *func;
};

typedef struct {
    PyObject_HEAD
    Py_ssize_t size;
    struct IndexInfo *indices;
} IndexKeyBuilder;

static PyMethodDef IndexKeyBuilderMethods[] = {
    {"build", builder_build, METH_VARARGS, "build"},
    {NULL}
};

static PyMemberDef IndexKeyBuilderMembers[] = {
    {NULL}
};

static PyTypeObject IndexKeyBuilderType = {
    PyObject_HEAD_INIT(NULL)
    .tp_alloc = PyType_GenericAlloc,
    .tp_new = builder_new,
    .tp_dealloc = builder_dealloc,
    .tp_members = IndexKeyBuilderMembers,
    .tp_methods = IndexKeyBuilderMethods,
    .tp_name = "_centidb.Record",
    .tp_basicsize = sizeof(Record),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "_centidb.Record"
};

static PyTypeObject *UUID_Type; // TODO
static PyObject *UUID_constructor;

enum ElementKind {
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


struct reader {
    uint8_t *p;
    Py_ssize_t size;
    Py_ssize_t pos;
};

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


struct writer {
    PyObject *s;
    Py_ssize_t pos;
};

static int writer_init(struct writer *wtr, Py_ssize_t initial)
{
    if(! initial) {
        initial = 20;
    }

    wtr->pos = 0;
    wtr->s = PyString_FromStringAndSize(NULL, initial);
    if(! wtr->s) {
        PyErr_NoMemory();
        return 0;
    }
    return 1;
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

    ((uint8_t *)PyString_AS_STRING(wtr->s))[wtr->pos++] = o;
    return 1;
}

/* Append a bytestring `b` to the buffer, growing it as necessary. */
static int writer_puts(struct writer *wtr, const char *s, Py_ssize_t size)
{
    assert(wtr->s);
    while((PyString_GET_SIZE(wtr->s) - wtr->pos) < (size + 1)) {
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
    // It should be possible to do better than this.
    if(-1 == _PyString_Resize(&(wtr->s), wtr->pos)) {
        return PyErr_NoMemory();
    }
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


static int c_encode_int(struct writer *wtr, uint64_t v, enum ElementKind kind)
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

    if(v < 240ULL) {
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


static PyObject *encode_int(PyObject *self, PyObject *arg)
{
    uint64_t v;
    if(Py_TYPE(arg) == &PyInt_Type) {
        long l = PyInt_AsLong(arg);
        if(l < 0) {
            PyErr_SetString(PyExc_OverflowError,
                "encode_int(): v must be >= 0");
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
        if(c_encode_int(&wtr, v, 0)) {
            return writer_fini(&wtr);
        }
    }
    return NULL;
}


static int encode_str(struct writer *wtr, uint8_t *p, Py_ssize_t length,
                      enum ElementKind kind, int closed)
{
    if(kind) {
        if(! writer_putc(wtr, kind)) {
            return 0;
        }
    }

    int ret = 1;
    while(ret && length--) {
        uint8_t c = *(p++);
        switch(c) {
        case 0:
            ret = writer_puts(wtr, "\x01\x01", 2);
            break;
        case 1:
            ret = writer_puts(wtr, "\x01\x02", 2);
            break;
        default:
            ret = writer_putc(wtr, c);
        }
    }

    if(closed) {
        ret = writer_putc(wtr, 0);
    }
    return ret;
}


static int c_encode_value(struct writer *wtr, PyObject *arg, int closed)
{
    int ret = 0;
    PyTypeObject *type = Py_TYPE(arg);

    if(arg == Py_None) {
        ret = writer_putc(wtr, KIND_NULL);
    } else if(type == &PyInt_Type) {
        long v = PyInt_AS_LONG(arg);
        if(v < 0) {
            ret = c_encode_int(wtr, -v, KIND_NEG_INTEGER);
        } else {
            ret = c_encode_int(wtr, v, KIND_INTEGER);
        }
    } else if(type == &PyString_Type) {
        ret = encode_str(wtr, (uint8_t *)PyString_AS_STRING(arg),
                              PyString_GET_SIZE(arg), KIND_BLOB, closed);
    } else if(type == &PyUnicode_Type) {
        PyObject *utf8 = PyUnicode_EncodeUTF8(PyUnicode_AS_UNICODE(arg),
            PyUnicode_GET_SIZE(arg), "strict");
        if(utf8) {
            ret = encode_str(wtr, (uint8_t *)PyString_AS_STRING(utf8),
                                  PyString_GET_SIZE(utf8), KIND_TEXT, closed);
            Py_DECREF(utf8);
        }
    } else if(type == &PyBool_Type) {
        ret = writer_putc(wtr, KIND_BOOL);
        ret = writer_putc(wtr, (uint8_t) (arg == Py_True));
    } else if(type == &PyLong_Type) {
        int64_t i64 = PyLong_AsLongLong(arg);
        if(! PyErr_Occurred()) {
            if(i64 < 0) {
                ret = c_encode_int(wtr, -i64, KIND_NEG_INTEGER);
            } else {
                ret = c_encode_int(wtr, i64, KIND_INTEGER);
            }
        }
    } else if(type == UUID_Type) {
        PyObject *ss = PyObject_CallMethod(arg, "get_bytes", NULL);
        if(ss) {
            assert(Py_TYPE(ss) == &PyString_Type);
            ret = encode_str(wtr, (uint8_t *)PyString_AS_STRING(ss),
                                  PyString_GET_SIZE(ss), KIND_UUID, 1);
            Py_DECREF(ss);
        }
    } else {
        PyErr_Format(PyExc_TypeError, "encode_keys(): got unsupported %.200s",
            arg->ob_type->tp_name);
    }

    return ret;
}


static int c_encode_key(struct writer *wtr, PyObject *tup, int closed)
{
    int ret = 1;
    Py_ssize_t tlast = PyTuple_GET_SIZE(tup) - 1;
    for(Py_ssize_t i = 0; ret && i <= tlast; i++) {
        ret = c_encode_value(wtr, PyTuple_GET_ITEM(tup, i),
                             !((!closed) && tlast == i));
    }
    return ret;
}


static PyObject *encode_keys(PyObject *self, PyObject *args)
{
    uint8_t *prefix = NULL;
    Py_ssize_t prefix_size;
    int closed = 1;

    Py_ssize_t arg_count = PyTuple_GET_SIZE(args);
    if(arg_count == 0 || arg_count > 3) {
        PyErr_SetString(PyExc_TypeError,
            "encode_keys() takes between 1 and 3 arguments.");
        return NULL;
    }
    if(arg_count > 1) {
        PyObject *py_prefix = PyTuple_GET_ITEM(args, 1);
        if(py_prefix != Py_None) {
            if(Py_TYPE(py_prefix) != &PyString_Type) {
                PyErr_SetString(PyExc_TypeError,
                    "encode_keys() prefix must be str().");
                return NULL;
            }
            prefix = (uint8_t *) PyString_AS_STRING(py_prefix);
            prefix_size = PyString_GET_SIZE(py_prefix);
        }
    }
    if(arg_count > 2) {
        closed = PyObject_IsTrue(PyTuple_GET_ITEM(args, 2));
    }

    struct writer wtr;
    if(! writer_init(&wtr, 0)) {
        return NULL;
    }

    int ret = 1;
    if(prefix) {
        if(! writer_puts(&wtr, (char *)prefix, prefix_size)) {
            return NULL;
        }
    }

    PyObject *tups = PyTuple_GET_ITEM(args, 0);
    PyTypeObject *type = Py_TYPE(tups);

    if(type != &PyList_Type) {
        if(type != &PyTuple_Type) {
            ret = c_encode_value(&wtr, tups, closed);
        } else {
            ret = c_encode_key(&wtr, tups, closed);
        }
    } else {
        Py_ssize_t llast = PyList_GET_SIZE(tups) - 1;
        for(int i = 0; ret && i <= llast; i++) {
            if(i) {
                ret = writer_putc(&wtr, KIND_SEP);
            }
            PyObject *elem = PyList_GET_ITEM(tups, i);
            type = Py_TYPE(elem);
            if(type != &PyTuple_Type) {
                ret = c_encode_value(&wtr, elem, closed && i != llast);
            } else {
                ret = c_encode_key(&wtr, elem, closed && i != llast);
            }
        }
    }

    if(ret) {
        return writer_fini(&wtr);
    }
    return NULL;
}


static PyObject *c_encode_index_entry(size_t initial, PyObject *prefix,
        PyObject *entry, PyObject *suffix)
{
    struct writer wtr;
    if(! writer_init(&wtr, initial)) {
        return NULL;
    }

    writer_puts(&wtr, PyString_AS_STRING(prefix), PyString_GET_SIZE(prefix));
    if(Py_TYPE(entry) == &PyTuple_Type) {
        c_encode_key(&wtr, entry, 0);
    } else {
        c_encode_value(&wtr, entry, 0);
    }

    writer_puts(&wtr, PyString_AS_STRING(suffix), PyString_GET_SIZE(suffix));
    return writer_fini(&wtr);
}


static PyObject *builder_new(PyTypeObject *type, PyObject *args,
        PyObject *kwds)
{
    PyObject *indices;
    if(! PyArg_ParseTuple(args, "O!", &PyList_Type, &indices)) {
        return NULL;
    }

    IndexKeyBuilder *self = PyObject_New(IndexKeyBuilder, &IndexKeyBuilderType);
    if(! self) {
        return NULL;
    }

    self->size = PyList_GET_SIZE(indices);
    self->indices = PyMem_Malloc(sizeof(struct IndexInfo) * self->size);
    if(! self->indices) {
        Py_DECREF(self);
        return NULL;
    }

    PyObject *prefix_s = PyString_FromString("prefix");
    PyObject *func_s = PyString_FromString("func");

    for(int i = 0; i < PyList_GET_SIZE(indices); i++) {
        struct IndexInfo *info = &self->indices[i];
        PyObject *index = PyList_GET_ITEM(indices, i);
        info->prefix = PyObject_GetAttr(index, prefix_s);
        assert(info->prefix);
        info->func = PyObject_GetAttr(index, func_s);
        assert(info->func);
    }

    return (PyObject *)self;
}

static void builder_dealloc(PyObject *self_)
{
    IndexKeyBuilder *self = (IndexKeyBuilder *)self_;

    for(int i = 0; i < self->size; i++) {
        struct IndexInfo *info = &self->indices[i];
        Py_CLEAR(info->prefix);
        Py_CLEAR(info->func);
    }
    PyMem_Free(self->indices);
    PyObject_Del(self_);
}

static PyObject *builder_build(PyObject *self_, PyObject *args)
{
    IndexKeyBuilder *self = (IndexKeyBuilder *) self_;

    if(PyTuple_GET_SIZE(args) != 2) {
        PyErr_SetString(PyExc_TypeError,
            "IndexKeyBuilder.build() must be called with (key, obj).");
        return NULL;
    }

    struct writer wtr;
    if(! writer_init(&wtr, 0)) {
        return NULL;
    }

    writer_putc(&wtr, KIND_SEP);
    if(! c_encode_key(&wtr, PyTuple_GET_ITEM(args, 0), 1)) {
        return NULL;
    }

    PyObject *suffix = writer_fini(&wtr);
    if(! suffix) {
        return NULL;
    }

    Py_ssize_t initial = PyString_GET_SIZE(suffix) + 20;
    PyObject *func_args = PyTuple_Pack(1, PyTuple_GET_ITEM(args, 1));
    if(! func_args) {
        Py_DECREF(suffix);
        return NULL;
    }

    PyObject *keys = PyList_New(LIST_START_SIZE);
    if(! keys) {
        Py_DECREF(func_args);
        Py_DECREF(suffix);
        return NULL;
    }

    int count = 0;
    for(int i = 0; i < self->size; i++) {
        struct IndexInfo *info = &self->indices[i];
        PyObject *result = PyObject_Call(info->func, func_args, NULL);
        if(! result) {
            Py_DECREF(keys);
            Py_DECREF(func_args);
            Py_DECREF(suffix);
            return NULL;
        }

        PyTypeObject *type = Py_TYPE(result);
        if(type != &PyList_Type) {
            PyObject *key = c_encode_index_entry(
                initial, info->prefix, result, suffix);
            if(count < LIST_START_SIZE) {
                PyList_SET_ITEM(keys, count, key);
            } else {
                if(PyList_Append(keys, key)) {
                    Py_DECREF(key);
                    Py_DECREF(result);
                    Py_DECREF(keys);
                    Py_DECREF(func_args);
                    Py_DECREF(suffix);
                    return NULL;
                }
            }
            count++;
        } else {
            for(int j = 0; j < PyList_GET_SIZE(result); j++) {
                PyObject *key = c_encode_index_entry(
                    initial, info->prefix, PyList_GET_ITEM(result, j), suffix);
                if(count < LIST_START_SIZE) {
                    PyList_SET_ITEM(keys, count, key);
                } else {
                    if(PyList_Append(keys, key)) {
                        Py_DECREF(key);
                        Py_DECREF(result);
                        Py_DECREF(keys);
                        Py_DECREF(func_args);
                        Py_DECREF(suffix);
                        return NULL;
                    }
                }
                count++;
            }
        }
    }
    Py_SIZE(keys) = count;
    Py_DECREF(func_args);
    Py_DECREF(suffix);
    return keys;
}


static PyObject *record_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    Record *self = (Record *)type->tp_alloc(type, 0);
    if(! self) {
        return NULL;
    }

    self->coll = NULL;
    self->data = NULL;
    self->key = NULL;
    self->batch = NULL;
    self->txn_id = NULL;
    self->index_keys = NULL;
    if(! PyArg_ParseTuple(args, "OO|OOOO",
            &self->coll, &self->data, &self->key, &self->batch,
            &self->txn_id, &self->index_keys)) {
        return NULL;
    }
    Py_XINCREF(self->coll);
    Py_XINCREF(self->data);
    Py_XINCREF(self->key);
    Py_XINCREF(self->batch);
    Py_XINCREF(self->txn_id);
    Py_XINCREF(self->index_keys);
    return (PyObject *) self;
}


static void record_dealloc(PyObject *self_)
{
    Record *self = (Record *)self_;
    Py_CLEAR(self->coll);
    Py_CLEAR(self->data);
    Py_CLEAR(self->key);
    Py_CLEAR(self->batch);
    Py_CLEAR(self->txn_id);
    Py_CLEAR(self->index_keys);
    self->ob_type->tp_free(self_);
}


static PyObject *record_repr(PyObject *self)
{
    return PyString_FromString("Dave");
    Record *record = (Record *)self;
    PyObject *info = PyObject_GetAttrString(record->coll, "info");
    if(! info) {
        return NULL;
    }

    PyObject *name_s = PyString_FromString("name");
    if(! name_s) {
        return NULL;
    }

    PyObject *name = PyObject_GetItem(info, name_s);
    Py_DECREF(name_s);
    if(! name) {
        return NULL;
    }

    struct writer wtr;
    if(! writer_init(&wtr, 40)) {
        return NULL;
    }

    writer_puts(&wtr, "<Record ", 8);
    writer_puts(&wtr, PyString_AS_STRING(name), PyString_GET_SIZE(name));
    Py_DECREF(name);
    writer_puts(&wtr, ":(", 2);
    for(int i = 0; i < PyTuple_GET_SIZE(record->key); i++) {
        if(i) {
            writer_putc(&wtr, ',');
        }
        PyObject *repr = PyObject_Repr(PyTuple_GET_ITEM(record->key, i));
        if(repr) {
            writer_puts(&wtr, PyString_AS_STRING(repr),
                              PyString_GET_SIZE(repr));
            Py_DECREF(repr);
        }
    }
    writer_puts(&wtr, ") ", 2);
    PyObject *repr = PyObject_Repr(record->data);
    if(repr) {
        writer_puts(&wtr, PyString_AS_STRING(repr), PyString_GET_SIZE(repr));
        Py_DECREF(repr);
    }

    writer_putc(&wtr, '>');
    return writer_fini(&wtr);
}


static int dumb_cmp(PyObject *x, PyObject *y)
{
    if(x) {
        return y ? PyObject_Compare(x, y) : -1;
    } else {
        return y ? 1 : 0;
    }
}


static int record_compare(PyObject *self_, PyObject *other_)
{
    Record *self = (Record *)self_;
    int ret = -1;
    if(Py_TYPE(other_) == &RecordType) {
        Record *other = (Record *)other_;
        ret = PyObject_Compare(self->coll, other->coll);
        if(! ret) {
            ret = dumb_cmp(self->data, other->data);
        }
        if(! ret) {
            ret = dumb_cmp(self->key, other->key);
        }
    }
    return ret;
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
    if(! writer_init(&wtr, 0)) {
        return NULL;
    }

    uint8_t ch;
    int ret = 1;
    while(ret && reader_getc(rdr, &ch)) {
        switch(ch) {
        case 0:
            ret = 0;
            break;
        case 1:
            if(! reader_getc(rdr, &ch)) {
                Py_CLEAR(wtr.s);
                ret = 0;
            } else if(ch == 1) {
                ret = writer_putc(&wtr, 0);
            } else {
                ret = writer_putc(&wtr, 1);
            }
            break;
        default:
            ret = writer_putc(&wtr, ch);
        }
    }
    return writer_fini(&wtr);
}


static PyObject *decode_key(struct reader *rdr)
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
                assert(0);
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
    Py_SIZE(tup) = tpos;
    return tup;
}


static PyObject *py_decode_key(PyObject *self, PyObject *args)
{
    PyObject *prefix = NULL;
    uint8_t *s;
    Py_ssize_t s_len;

    if(! PyArg_ParseTuple(args, "s#|S", (char **) &s, &s_len, &prefix)) {
        return NULL;
    }

    struct reader rdr;
    if(! reader_init(&rdr, s, s_len)) {
        return NULL;
    }

    if(prefix) {
        if(rdr.size < PyString_GET_SIZE(prefix)) {
            PyErr_SetString(PyExc_ValueError,
                "decode_keys() input smaller than prefix.");
            return NULL;
        }
        rdr.pos += PyString_GET_SIZE(prefix);
    }

    return decode_key(&rdr);
}


static PyObject *decode_keys(PyObject *self, PyObject *args)
{
    PyObject *prefix = NULL;

    uint8_t *s;
    Py_ssize_t s_len;

    if(! PyArg_ParseTuple(args, "s#|S", (char **) &s, &s_len, &prefix)) {
        return NULL;
    }

    struct reader rdr;
    if(! reader_init(&rdr, s, s_len)) {
        return NULL;
    }

    if(prefix) {
        if(rdr.size < PyString_GET_SIZE(prefix)) {
            PyErr_SetString(PyExc_ValueError,
                "decode_keys() prefix smaller than input.");
            return NULL;
        }
        rdr.pos += PyString_GET_SIZE(prefix);
    }

    PyObject *tups = PyList_New(LIST_START_SIZE);
    if(! tups) {
        return NULL;
    }

    Py_ssize_t lpos = 0;
    while(rdr.pos < rdr.size) {
        PyObject *tup = decode_key(&rdr);
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
    Py_SIZE(tups) = lpos;
    return tups;
}


static PyMethodDef CentidbMethods[] = {
    {"tuplize", tuplize, METH_O, "tuplize"},
    {"decode_key", py_decode_key, METH_VARARGS, "decode_key"},
    {"decode_keys", decode_keys, METH_VARARGS, "decode_keys"},
    {"encode_keys", encode_keys, METH_VARARGS, "encode_keys"},
    {"encode_int", encode_int, METH_O, "encode_int"},
    {NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC
init_centidb(void)
{
    PyObject *mod = Py_InitModule("_centidb", CentidbMethods);
    if(! mod) {
        return;
    }

    if(-1 == PyType_Ready(&RecordType)) {
        return;
    }
    if(-1 == PyType_Ready(&IndexKeyBuilderType)) {
        return;
    }
    PyModule_AddObject(mod, "Record", (void *) &RecordType);
    PyModule_AddObject(mod, "IndexKeyBuilder", (void *) &IndexKeyBuilderType);
}
