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
 * Compare the longest possible prefix of 2 strings. If both prefixes match,
 * return -1 if `s1` is shorter than `s2`, 1 if `s1` is longer than `s2`, and 0
 * if both strings are of equal length and identical.
 */
int acid_memcmp(uint8_t *s1, Py_ssize_t s1len,
                uint8_t *s2, Py_ssize_t s2len)
{
    int rc = memcmp(s1, s2, (s1len < s2len) ? s1len : s2len);
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
 * Find the longest prefix of `p[0..len]` that does not end with a 0xff byte.
 * Returns -1 if entire string is 0xff bytes, or offset of last non-0xff byte.
 */
Py_ssize_t
acid_next_greater(uint8_t *p, Py_ssize_t len)
{
    uint8_t *orig = p;
    uint8_t *e = p + len;
    uint8_t *l = NULL;

    for(uint8_t *p = orig; p < e; p++) {
        if(*p != 0xff) {
            l = p;
        }
    }

    // All bytes are 0xff, should never happen.
    if(! l) {
        return -1;
    }
    return (l + 1) - orig;
}

/**
 * Like acid_next_greater(), except return a PyString containing the prefix
 * with the last non-0xff incremented by 1. Return NULL on failure.
 */
PyObject *
acid_next_greater_str(uint8_t *p, Py_ssize_t len)
{
    Py_ssize_t goodlen = acid_next_greater(p, len);
    if(goodlen == -1) {
        return NULL;
    }

    PyObject *str = PyString_FromStringAndSize(NULL, goodlen);
    if(str) {
        uint8_t *dst = (uint8_t *)PyString_AS_STRING(str);
        memcpy(dst, p, goodlen);
        dst[goodlen - 1]++;
    }
    return str;
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

    if(acid_init_keylib_module()) {
        return;
    }
    /*if(acid_init_iterators_module()) {
        return;
    }*/
}
