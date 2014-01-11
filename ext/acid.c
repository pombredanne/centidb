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

#include <string.h>
#include "acid.h"


/**
 * Return the ASCII hex character for a nibble.
 */
static char nibble(int n)
{
    return (n >= 10) ? ('a' + (n - 10)) : ('0' + n);
}

/**
 * Format a bytestring into NUL terminated ASCII hex using a static buffer, and
 * return a pointer to the buffer.
 */
const char *
acid_debug_hex(uint8_t *s, Py_ssize_t len)
{
    static uint8_t buf[512];
    if(len > sizeof buf) {
        DEBUG("truncating oversize len %d to %d", (int)len, (int)sizeof buf)
        len = sizeof buf;
    }
    uint8_t *p = buf;
    for(int i = 0; i < len; i++) {
        *p++ = nibble((s[i] & 0xF0) >> 4);
        *p++ = nibble((s[i] & 0xF));
        *p++ = ' ';
    }
    buf[len ? ((3 * len) - 1) : 0] = '\0';
    return (char *)buf;
}

/**
 * Given some Python object, try to get at its raw data. For string or bytes
 * objects, this is the object value. For Unicode objects, this is the UTF-8
 * representation of the object value. For all other objects, attempt to invoke
 * the Python 2.x buffer protocol.
 */
int
acid_make_reader(struct reader *rdr, PyObject *buf)
{
    uint8_t *s;
    Py_ssize_t len;

    if(PyBytes_CheckExact(buf)) {
        s = (uint8_t *)PyBytes_AS_STRING(buf);
        len = PyBytes_GET_SIZE(buf);
    }
#if PY_MAJOR_VERSION >= 3
    else if(PyUnicode_CheckExact(buf)) {
        if(! ((s = PyUnicode_AsUTF8AndSize(buf, &len)))) {
            return -1;
        }
    }
#endif
    else if(PyObject_AsReadBuffer(buf, (const void **)&s, &len)) {
        return -1;
    }

    rdr->p = s;
    rdr->e = s + len;
    return 0;
}

/**
 * Convert exactly a PyString into a Slice. No error checking occurs.
 */
void
acid_string_as_slice(Slice *slice, PyObject *str)
{
    uint8_t *p = (uint8_t *)PyString_AS_STRING(str);
    slice->p = p;
    slice->e = p + PyString_GET_SIZE(str);
}

/*
 * Convert exactly a Key to a Slice. No error checking occurs.
 */
void
acid_key_as_slice(Slice *slice, Key *key)
{
    slice->p = key->p;
    slice->e = key->p + Key_SIZE(key);
}

/**
 * Compare the longest possible prefix of 2 strings. If both prefixes match,
 * return -1 if `s1` is shorter than `s2`, 1 if `s1` is longer than `s2`, and 0
 * if both strings are of equal length and identical.
 */
int
acid_memcmp(Slice *s1, Slice *s2)
{
    Py_ssize_t s1len = s1->e - s1->p;
    Py_ssize_t s2len = s2->e - s2->p;
    int rc = memcmp(s1->p, s2->p, (s1len < s2len) ? s1len : s2len);
    if(! rc) {
        if(s1len < s2len) {
            rc = -1;
        } else if(s1len > s2len) {
            rc = 1;
        }
    }
    return rc;
}

/**
 * Import `module`, then iteratively walk its attributes looking for a specific
 * object. Given import_object("sys", "stdout", "write", NULL), would return a
 * new reference to a bound instancemethod for "sys.stdout.write". Return a new
 * reference on success, or set an exception and return NULL on failure.
 */
PyObject *
acid_import_object(const char *module, ...)
{
    va_list ap;
    va_start(ap, module);

    PyObject *obj = PyImport_ImportModule(module);
    if(! obj) {
        return NULL;
    }

    va_start(ap, module);
    const char *name;
    while((name = va_arg(ap, const char *)) != NULL) {
        PyObject *obj2 = PyObject_GetAttrString(obj, name);
        Py_DECREF(obj);
        if(! obj2) {
            va_end(ap);
            return NULL;
        }
        obj = obj2;
    }
    return obj;
}

/**
 * Arrange for a acid._`name` submodule to be created and inserted into
 * sys.modules. `methods` is a NULL-terminated PyMethodDef array of methods to
 * include in the module. Return a new reference to the module on success, or
 * NULL on failure.
 */
PyObject *
acid_init_module(const char *name, PyMethodDef *methods)
{
    char fullname[64];
    snprintf(fullname, sizeof fullname, "acid.%s", name);
    fullname[sizeof fullname - 1] = '\0';

    PyObject *pkg = PyImport_ImportModule("acid");
    if(! pkg) {
        return NULL;
    }

    PyObject *mod = PyImport_AddModule(fullname);
    if(! mod) {
        return NULL;
    }

    if(PyModule_AddObject(pkg, name, mod)) {
        return NULL;
    }

    if(methods) {
        int i;
        for(i = 0; methods[i].ml_name != NULL; i++) {
            PyObject *name = PyString_FromString(methods[i].ml_name);
            if(! name) {
                return NULL;
            }
            PyObject *method = PyCFunction_NewEx(&methods[i], NULL, name);
            if(! method) {
                return NULL;
            }
            if(PyModule_AddObject(mod, methods[i].ml_name, method)) {
                return NULL;
            }
        }
    }

    Py_INCREF(mod);
    return mod;
}

/**
 * Initialize the acid._acid extension and all included submodules.
 */
PyMODINIT_FUNC
init_acid(void)
{
    PyObject *mod = Py_InitModule("acid._acid", NULL);
    if(! mod) {
        return;
    }

    if(acid_init_core_module()) {
        return;
    }
    if(acid_init_keylib_module()) {
        return;
    }
    if(acid_init_iterators_module()) {
        return;
    }
}
