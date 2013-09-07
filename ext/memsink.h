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

/**
 * Generic lazy-copying shared memory protocol for Python
 * See https://github.com/dw/acid/issues/23
 */

#ifndef MEMSINK_H
#define MEMSINK_H

#define UNUSED __attribute__((unused))
#define MEMSINK_MAGIC   0xCAFE0001
#define MEMSOURCE_MAGIC 0xBABE0001

static int memsink_initted UNUSED = 0;
static PyObject *memsource_attr_str UNUSED;
static PyObject *memsink_attr_str UNUSED;

struct mem_sink_list {
    // Borrowed reference to previous sink in list, or NULL to indicate
    // head of list (in which case borrowed reference is stored directly in
    // source object).
    PyObject *prev;
    // Borrowed reference to next sink in the list.
    PyObject *next;
};

struct mem_source {
    // Changes when ABI changes.
    unsigned long magic;
    // Notify `sink` when `source` buffers expire.
    int (*notify)(PyObject *source, PyObject *sink);
    // Cancel notification of `sink` when `source` buffers expire.
    int (*cancel)(PyObject *source, PyObject *sink);
};

struct mem_sink {
    // Changes when ABI changes.
    unsigned long magic;
    // PyObject offset where SinkList is stored.
    Py_ssize_t list_offset;
    // Invoked when source buffer is about to become invalid.
    int (*invalidate)(PyObject *source, PyObject *sink);
};

static UNUSED int memsink_init(void)
{
    if(! memsink_initted) {
        memsource_attr_str = PyString_InternFromString("__memsource__");
        memsink_attr_str = PyString_InternFromString("__memsink__");
        if(! (memsource_attr_str && memsink_attr_str)) {
            return -1;
        }
    }
    memsink_initted = 1;
    return 0;
}


static UNUSED void *_memsink_get_desc(PyObject *obj, PyObject *attr,
                                      long magic)
{
    PyObject *capsule = PyDict_GetItem(obj->ob_type->tp_dict, attr);
    if(! capsule) {
        return NULL;
    }

    void *desc = PyCapsule_GetPointer(capsule, NULL);
    Py_DECREF(capsule);
    if(desc && (*(long *)desc) != magic) {
        desc = NULL;
    }
    return desc;
}


static UNUSED int memsource_notify(PyObject *source, PyObject *sink)
{
    struct mem_source *ms = _memsink_get_desc(
        source, memsource_attr_str, MEMSOURCE_MAGIC);
    if(ms) {
        return ms->notify(source, sink);
    }
    return -1;
}


static UNUSED int memsource_cancel(PyObject *source, PyObject *sink)
{
    struct mem_source *ms = _memsink_get_desc(
        source, memsource_attr_str, MEMSOURCE_MAGIC);
    if(ms) {
        return ms->cancel(source, sink);
    }
    return -1;
}


static UNUSED struct mem_sink_list *memsink_get_list(PyObject *sink)
{
    struct mem_sink *ms = _memsink_get_desc(
        sink, memsink_attr_str, MEMSINK_MAGIC);
    if(ms) {
        return (struct mem_sink_list *) (((char *) sink) + ms->list_offset);
    }
    return NULL;
}

/**
 * Decorate `type` to include the __memsink__ attribute. `list_offset` is the
 * offset into `type` where `struct mem_sink_list` occurs, and `invalidate` is
 * the callback function that disassociates instances from any shared memory.
 */
static UNUSED int memsink_type_init(PyTypeObject *type, Py_ssize_t list_offset,
                                    int (*invalidate)(PyObject *, PyObject *))
{
    static struct mem_sink mem_sink;
    mem_sink.magic = MEMSINK_MAGIC;
    mem_sink.list_offset = list_offset;
    mem_sink.invalidate = invalidate;

    if(memsink_init()) {
        return -1;
    }

    PyObject *capsule = PyCapsule_New(&mem_sink, NULL, NULL);
    if(! capsule) {
        return -1;
    }

    int ret = PyDict_SetItem(type->tp_dict, memsink_attr_str, capsule);
    Py_DECREF(capsule);
    return ret;
}


static UNUSED int _memsource_notify_impl(PyObject *source, PyObject *sink)
{
    
}

// Cancel notification of `sink` when `source` buffers expire.
static UNUSED int _memsource_cancel_impl(PyObject *source, PyObject *sink)
{

}


static UNUSED int memsource_type_init(PyTypeObject *type)
{
    static struct mem_source ms;
    ms.magic = MEMSOURCE_MAGIC;
    ms.notify = _memsource_notify_impl;
    ms.cancel = _memsource_cancel_impl;

    if(memsink_init()) {
        return -1;
    }

    PyObject *capsule = PyCapsule_New(&ms, NULL, NULL);
    if(! capsule) {
        return -1;
    }

    int ret = PyDict_SetItem(type->tp_dict, memsource_attr_str, capsule);
    Py_DECREF(capsule);
    return ret;
}

#endif /* !MEMSINK_H */
