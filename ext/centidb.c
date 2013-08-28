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

#include "centidb.h"

#include <assert.h>
#include <string.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <structmember.h>

static struct KeyCoderModule KeyCoder;


PyMODINIT_FUNC
init_centidb(void)
{
    // PyCapsule_Import can't import modules from inside packages, so we do it.
    PyObject *tmp = PyImport_ImportModule("centidb._keycoder");
    Py_CLEAR(tmp);

    struct KeyCoderModule *keycoder = PyCapsule_Import("centidb._keycoder._C_API", 0);
    if(! keycoder) {
        return;
    }
    KeyCoder = *(struct KeyCoderModule *) keycoder;

    PyObject *mod = Py_InitModule("centidb._centidb", NULL);
    if(! mod) {
        return;
    }
}
