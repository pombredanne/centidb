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
 * Generic copy-on-invalidate memory protocol for Python. This is code is
 * insane, a better solution is needed. This is headers-only so only a single
 * file needs copied into a project to support the protocol.
 *
 * See https://github.com/dw/acid/issues/23
 *
 * Theory of operation:
 *      1. SourceType represents some read-only chunk of memory, but that
 *         memory may go away at any time.
 *      2. SinkType represents some immutable view of that memory, so it
 *         has no real need to copy or modify it.
 *      3. SourceType and SinkType need to talk to each other to share this
 *         memory safely.
 *
 *      4. Programmer inserts a 'PyObject *sink_head' field anywhere in
 *         SourceType's PyObject struct.
 *      5. After PyType_Ready(&SourceType), programmer calls
 *         ms_init_source(&SourceType, offsetof(Source, sink_head)).
 *      6. In SourceType's implementation, programmer inserts ms_notify(self)
 *         anywhere its memory is about to become invalid.
 *
 *      7. Programmer inserts a 'struct ms_node sink_node' field anywhere in
 *         SinkType's PyObject struct, and a "PyObject *source" field to track
 *         the active SourceType, if any.
 *      8. Programmer writes a 'my_invalidate()' function to handle
 *         when SinkType is told SourceType's memory is about to go away.
 *         Probably it wants to copy the memory, and Py_CLEAR(self->source).
 *      9. After PyType_Ready(&SinkType), programmer calls
 *         ms_init_sink(&SinkType, offsetof(Sink, sink_node), my_invalidate).
 *      10. When SinkType is handed a piece of memory belonging to SourceType,
 *          programmer inserts "ms_listen(self->source, self)".
 *      11. In SinkType's PyTypeObject.tp_dealloc funcion, programmer inserts
 *          "ms_cancel(self->source, self)".
 */

#ifndef MEMSINK_H
#define MEMSINK_H

#include <stdlib.h>

// Silence unused warnings on gcc if possible.
#ifdef __GNUC__
#   define UNUSED __attribute__((unused))
#else
#   define UNUSED
#endif

#define _MS_SINK_MAGIC 0xCAF0
#define _MS_SRC_MAGIC  0xCAF1

/** Has _ms_init() run yet? */
static int _ms_initted UNUSED = 0;
/** Interned PyString "__memsource__" set by _ms_init(). */
static PyObject *_ms_src_attr UNUSED;
/** Interned PyString "__memsink__" set by _ms_init(). */
static PyObject *_ms_sink_attr UNUSED;

/**
 * List node that must appear somewhere within a sink's PyObject structure.
 * offsetof(ThatStruct, this_field) must be passed to ms_init_sink().
 */
struct ms_node {
    /** Borrowed reference to previous sink in list, or NULL to indicate
     * head of list (in which case borrowed reference is stored directly in
     * source object). */
    PyObject *prev;
    /** Borrowed reference to next sink in the list. */
    PyObject *next;
};

/**
 * Internal source descriptor, heap-allocated and stored in a PyCapsule as the
 * source type's __memsource__ attribute.
 */
struct ms_source {
    /** Changes when ABI changes. */
    int magic;
    /** Offset of "PyObject *" list head in type's PyObject struct. */
    Py_ssize_t head_offset;
    /** Notify `sink` when `src` memory expires. */
    int (*listen)(PyObject *src, PyObject *sink);
    /** Cancel notification of `sink` when `src` memory expires. */
    int (*cancel)(PyObject *src, PyObject *sink);
};

/**
 * Internal sink descriptor, heap-allocated and stored in a PyCapsule as the
 * sink type's __memsink__ attribute.
 */
struct ms_sink {
    /** Changes when ABI changes. */
    int magic;
    /** Offset of "struct ms_node" stored in type's PyObject struct. */
    Py_ssize_t node_offset;
    /** Notification receiver invoked when src memory expires. */
    int (*invalidate)(PyObject *src, PyObject *sink);
};

/**
 * Initialize the interned "__memsource__" and "__memsink__" string constants.
 * Return 0 on sucess or -1 on error.
 */
static UNUSED int
_ms_init(void)
{
    if(! _ms_initted) {
        _ms_src_attr = PyString_InternFromString("__memsource__");
        _ms_sink_attr = PyString_InternFromString("__memsink__");
        if(! (_ms_src_attr && _ms_sink_attr)) {
            return -1;
        }
        _ms_initted = 1;
    }
    return 0;
}

/**
 * Fetch a "__memsink__" or "__memsource__" descriptor from `obj`'s type.
 * `attr` is the interned string attribute name, `magic` is the expected struct
 * magic. Return a pointer on success, or return NULL and set an exception on
 * error.
 */
static UNUSED void *
_ms_get_desc(PyObject *obj, PyObject *attr, int magic)
{
    PyObject *capsule;
    void *desc;

    if(! ((capsule = PyDict_GetItem(obj->ob_type->tp_dict, attr)))) {
        return PyErr_Format(PyExc_TypeError, "Type %s lacks '%s' attribute.",
                            obj->ob_type->tp_name, PyString_AS_STRING(attr));
    }

    desc = PyCapsule_GetPointer(capsule, NULL);
    Py_DECREF(capsule);
    if(desc && (*(int *)desc) != magic) {
        return PyErr_Format(PyExc_TypeError,
            "Type %s '%s' magic is incorrect, got %08x, wanted %08x. "
            "Probable memsink.h version mismatch.",
            obj->ob_type->tp_name, PyString_AS_STRING(attr),
            *(int *)desc, magic);
        desc = NULL;
    }
    return desc;
}

#define _MS_SINK_DESC(sink) _ms_get_desc(sink, _ms_sink_attr, _MS_SINK_MAGIC)
#define _MS_SRC_DESC(src) _ms_get_desc(src, _ms_src_attr, _MS_SRC_MAGIC)
#define _MS_FIELD_AT(ptr, offset) ((void *) ((char *)(ptr)) + (offset))

/**
 * Tell `sink` when memory exported by `src` becomes invalid. `src` must be of
 * a type for which ms_init_source() has been invoked, `sink` must be of a type
 * for which ms_init_sink() has been invoked. Return 0 on success or -1 on
 * error.
 */
static UNUSED int
ms_listen(PyObject *src, PyObject *sink)
{
    struct ms_source *desc;
    if((desc = _MS_SRC_DESC(src))) {
        return desc->listen(src, sink);
    }
    return -1;
}

/**
 * Cancel notification of `sink` when memory exported by `src` becomes invalid.
 * `src` must be of a type for which ms_init_source() has been invoked, `sink`
 * must be of a type for which ms_init_sink() has been invoked. Return 0 on
 * success or -1 on error.
 */
static UNUSED int
ms_cancel(PyObject *src, PyObject *sink)
{
    struct ms_source *desc;
    if((desc = _MS_SRC_DESC(src))) {
        return desc->cancel(src, sink);
    }
    return -1;
}

/**
 * Notify subscribers to `src` that its memory is becoming invalid, and cancel
 * their subscription. Return 0 on success or return -1 and set an exception on
 * error.
 */
static UNUSED int
ms_notify(PyObject *src, PyObject **list_head)
{
    PyObject *cur = *list_head;
    while(cur) {
        struct ms_sink *mcur;
        struct ms_node *mnode;
        if(! ((mcur = _MS_SINK_DESC(cur)))) {
            return -1;
        }
        mnode = _MS_FIELD_AT(cur, mcur->node_offset);
        mcur->invalidate(src, cur); // TODO how to handle -1?
        cur = mnode->next;
        mnode->prev = NULL;
        mnode->next = NULL;
    }
    return 0;
}

/**
 * Fetch the struct ms_node from a sink, or return NULL and set an exception on
 * error.
 */
static UNUSED struct ms_node *
_ms_sink_node(PyObject *sink)
{
    struct ms_sink *desc = _MS_SINK_DESC(sink);
    struct ms_node *node = NULL;
    if(desc) {
        node = _MS_FIELD_AT(sink, desc->node_offset);
    }
    return node;
}

/**
 * Default implementation of struct mem_source::listen(). Push `sink` on the
 * front of the list, updating the previous head if one existed.
 */
static UNUSED int
_ms_listen_impl(PyObject *src, PyObject *sink)
{
    struct ms_source *msrc;
    struct ms_node *node;
    PyObject **head;

    if(! (((msrc = _MS_SRC_DESC(src))) &&
          ((node = _ms_sink_node(sink))))) {
        return -1;
    }

    head = _MS_FIELD_AT(src, msrc->head_offset);
    if(*head) {
        struct ms_node *headnode = _ms_sink_node(*head);
        if(! headnode) {
            return -1;
        }
        headnode->prev = sink;
    }
    node->next = *head;
    node->prev = NULL;
    *head = sink;
    return 0;
}

/**
 * Default implementation of struct mem_source::cancel().
 */
static UNUSED int
_ms_cancel_impl(PyObject *src, PyObject *sink)
{
    PyObject **head;
    struct ms_source *srcdesc;
    struct ms_node *sinknode;
    struct ms_node *prevnode = NULL;
    struct ms_node *nextnode = NULL;

    if(! ((srcdesc = _MS_SRC_DESC(src)))) {
        return -1;
    }
    head = _MS_FIELD_AT(src, srcdesc->head_offset);

    if(! ((sinknode = _ms_sink_node(sink)))) {
        return -1;
    }
    if(sinknode->prev && !((prevnode = _ms_sink_node(sinknode->prev)))) {
        return -1;
    }
    if(sinknode->next && !((nextnode = _ms_sink_node(sinknode->next)))) {
        return -1;
    }
    if(nextnode) {
        nextnode->prev = sinknode->prev;
    }
    if(prevnode) {
        prevnode->next = sinknode->next;
    } else {
        if(*head != sink) {
            PyErr_SetString(PyExc_SystemError, "memsink.h list is corrupt.");
            return -1;
        }
        *head = sinknode->next;
    }

    sinknode->prev = NULL;
    sinknode->next = NULL;
    return 0;
}

/**
 * Capsule destructor function.
 */
static UNUSED void _ms_capsule_destroy(PyObject *capsule)
{
    free(PyCapsule_GetPointer(capsule, NULL));
}

/**
 * Code shared between ms_init_sink() and ms_init_source().
 */
static UNUSED void *
_ms_init_type(PyTypeObject *type, PyObject *attr, size_t size)
{
    void *ptr;
    PyObject *capsule;

    if(_ms_init()) {
        return NULL;
    }

    if(! ((ptr = malloc(size)))) {
        return NULL;
    }

    if(! ((capsule = PyCapsule_New(ptr, NULL, _ms_capsule_destroy)))) {
        free(ptr);
        return NULL;
    }

    if(PyDict_SetItem(type->tp_dict, attr, capsule)) {
        ptr = NULL;
    }
    Py_DECREF(capsule);
    return ptr;
}

/**
 * Decorate `type` to include the __memsink__ attribute. `node_offset` is the
 * offset into `type` where `struct ms_node` occurs, and `invalidate` is
 * the callback function that disassociates instances from any shared memory.
 * Return 0 on success or -1 on failure.
 */
static UNUSED int
ms_init_sink(PyTypeObject *type, Py_ssize_t node_offset,
             int (*invalidate)(PyObject *, PyObject *))
{
    struct ms_sink *desc;
    if(! ((desc = _ms_init_type(type, _ms_sink_attr, sizeof *desc)))) {
        return -1;
    }
    desc->magic = _MS_SINK_MAGIC;
    desc->node_offset = node_offset;
    desc->invalidate = invalidate;
    return 0;
}

/**
 * Decorate `type` to include the "__memsource__" attribute. `head_offset` is
 * the offset of the "PyObject *ms_head" in the type's PyObject struct. Return
 * 0 on success or return -1 and set an exception on failure.
 */
static UNUSED int
ms_init_source(PyTypeObject *type, Py_ssize_t head_offset)
{
    struct ms_source *desc;
    if(! ((desc = _ms_init_type(type, _ms_src_attr, sizeof *desc)))) {
        return -1;
    }
    desc->magic = _MS_SRC_MAGIC;
    desc->head_offset = head_offset;
    desc->listen = _ms_listen_impl;
    desc->cancel = _ms_cancel_impl;
    return 0;
}

#endif /* !MEMSINK_H */
