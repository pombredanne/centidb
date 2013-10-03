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
        s = PyBytes_AS_STRING(buf);
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
