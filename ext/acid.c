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
