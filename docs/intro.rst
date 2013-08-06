
.. currentmodule:: centidb

Introduction
############

While it is possible to construct all objects from the API reference manually,
a few helpers exist to simplify access to common objects. First we make use of
:py:func:`centidb.open`, which simply wraps the process of constructing an
engine and attaching it to a :py:class:`Store <centidb.Store>`.

Since the library depends on an external engine, an initial consideration might
be which to use. For now, let's forgo the nasty research and settle on the
simplest engine available, :py:class:`ListEngine <centidb.support.ListEngine>`:

::

    import centidb
    store = centidb.open('ListEngine')

We now have a :py:class:`Store <Store>`. Stores manage metadata for a set of
collections, along with any registered encodings and counters. Multiple
:py:class:`Collections <Collection>` may exist, each managing independent sets
of records, like an SQL table.

Let's create a ``people`` collection:

::

    people = store.collection('people')

Underneath a few interesting things just occurred. Since the engine had no
``people`` collection, a key prefix was allocated using :py:meth:`Store.count`,
and records representing the counter and the collection have already been
written.


Insertion
+++++++++

Now let's insert some people using :py:meth:`Collection.put`:

::

    >>> people.put(('Buffy', 'girl'))
    <Record people:(1L) ('Buffy', 'girl')>

    >>> people.put(('Willow', 'girl'))
    <Record people:(2L) ('Willow', 'girl')>

    >>> people.put(('Spike', 'boy'))
    <Record people:(3L) ('Spike', 'boy')>

Since we didn't specify an encoder during construction, the default pickle
encoder is used which allows almost any Python value, although here we use
tuples. Since no key function was given, the collection defaults to
auto-incrementing keys.

More magic is visible underneath:

::

    >>> pprint(engine.items)
    [('\x00(people\x00',                    ' (people\x00\x15\n\x0f'),
     ('\x01(\x01\x01collections_idx\x00',   ' (\x01\x01collections_idx\x00\x15\x0b'),
     ('\x01(key:people\x00',                ' (key:people\x00\x15\x04'),

     ('\n\x15\x01', ' \x80\x02U\x05Buffyq\x01U\x04girlq\x02\x86q\x03.'),
     ('\n\x15\x02', ' \x80\x02U\x06Willowq\x01U\x04girlq\x02\x86q\x03.'),
     ('\n\x15\x03', ' \x80\x02U\x05Spikeq\x01U\x03boyq\x02\x86q\x03.')]

Notice the ``key:people`` counter and freshly inserted people records. Pay
attention to the record keys, occupying only 3 bytes despite their prefix also
encoding the collection.


Exact lookup
++++++++++++

Lookup of a single record is accomplished by :py:meth:`Collection.get`, which
works similarly to :py:meth:`dict.get`. The first parameter is the key to
return, and an optional second parameter specifies a default if the key is
missing:

::

    >>> people.get(2)
    ('Willow', 'girl')

    >>> # No such record.
    >>> people.get(99)
    None

    >>> # Default is returned.
    >>> people.get(99, default=('Angel', 'boy'))
    ('Angel', 'boy')

Be aware that unlike :py:meth:`dict.get`, :py:meth:`Collection.get` and related
methods return freshly decoded *copies* of the associated value. In this
respect :py:class:`Collection` only superficially behaves like a
:py:class:`dict`:

::

    >>> t1 = people.get(2)
    >>> t2 = people.get(2)

    >>> # The copies are equal:
    >>> t1 == t2
    True

    >>> # But they are distinct:
    >>> t1 is t2
    False


Inexact lookup
++++++++++++++

As the engine keeps records in key order, searches and enumerations on this
order are very efficient. :py:meth:`Collection.find` can return the first
matching record from a given key range. For example, to return the lowest and
highest records:

::

    >>> # Find record with lowest key, 1
    >>> people.find()
    ('Buffy', 'girl')

    >>> # Find record with highest key, 3
    >>> people.find(reverse=True)
    ('Spike', 'boy')

We can locate records based only on the relation of their key to some
constraining keys:

::

    >>> # Find first record with 2 <= key < 99.
    >>> people.find(lo=2, hi=99)
    ('Willow', 'girl')

    >>> # Find last record with 2 <= key < 99.
    >>> people.find(lo=2, hi=99, reverse=True)
    ('Spike', 'boy')


Range iteration
+++++++++++++++

Similar to dictionaries a family of methods assist with iteration, however
these methods also allow setting a start/stop key, or a lo/hi range, and
walking in reverse. Refer to :ref:`query-parameters` for the full set of
supported combinations.

:py:meth:`Collection.keys`

    ::

        >>> # All keys, start to end:
        >>> list(people.keys())
        [(1L,), (2L,), (3L,)]

        >>> # All keys, end to start.
        >>> list(people.keys(reverse=True))
        [(3L,), (2L,), (1L,)]

        >>> # Keys from 2 to end:
        >>> list(people.keys(2))
        [(2L,), (3L,)]

        >>> # Keys from 2 to start:
        >>> list(people.keys(2, reverse=True))
        [(2L,), (1L,)]


:py:meth:`Collection.values`

    ::

        >>> # All values, start to end:
        >>> pprint(list(people.values()))
        [('Buffy', 'girl'),
         ('Willow', 'girl'),
         ('Spike', 'boy')]

:py:meth:`Collection.items`

    ::

        >>> # All (key, value) pairs, from 99 to 2:
        >>> pprint(list(people.items(lo=2, hi=99, reverse=True)))
        [((3L,), ('Spike', 'boy')),
         ((2L,), ('Willow', 'girl'))]


Keys & Indices
++++++++++++++

While auto-incrementing keys are useful and efficient to store, they often
prevent the ordered nature of the storage engine from being fully exploited. As
we can efficiently iterate key ranges, by controlling the key we can order the
collection in ways that are very useful for queries.

To make this ordering easy to exploit, keys are treated as tuples of one or
more :py:func:`primitive values <keycoder.packs>`, with the order of
earlier elements taking precedence over later elements, just like a Python
tuple. When written to storage, tuples are carefully encoded so their ordering
is preserved by the engine.

Since multiple values can be provided, powerful grouping hierarchies can be
designed to allow efficient range queries anywhere within the hierarchy, all
without a secondary index.

**Note:** anywhere a key is expected by the library, if a single value is
passed it will be *automatically wrapped in a 1-tuple*. Conversely, it is
important to remember this when handling keys returned by the library — keys
are *always* tuples.


Key functions
-------------

When instantiating :py:class:`Collection` you may provide a key function, which
is responsible for producing record keys. The key function can accept either
one or two parameters. In the first form (*key_func=*), only the record's value
is passed, while in the second form (*txn_key_func=*) a reference to the active
transaction is also passed.

The key may be any supported primitive value, or a tuple of primitive values.
For example, to assign a key based on the time in microseconds:

    ::

        >>> def usec_key(val):
        ...     return int(1e6 * time.time())

        >>> coll = centidb.Collection(store, 'stuff', key_func=usec_key)

Or by UUID:

    ::

        >>> def uuid_key(val):
        ...     return uuid.uuid4()

        >>> coll = centidb.Collection(store, 'stuff', key_func=uuid_key)

Finally, a key function may also be marked as `derived` (`derived_keys=True`),
indicating that if the record value changes, the key function should be
reinvoked to assign a new key.

    ::

        >>> # If username changes, we need to update the record's key.
        >>> def user_name_key(val):
        ...     return val['username']

        >>> coll = centidb.Collection(store, 'users',
        ...     key_func=user_name_key,
        ...     derived_keys=True)


Let's create a new collection, this time storing :py:class:`dicts <dict>` with
some new fields. The collection holds user accounts for an organizational web
application, where each user belongs to a particular department within a
particular region.

::

    users = centidb.Collection(store, 'users')


Compression
+++++++++++

Value compression
-----------------

Values may be compressed by passing a `packer=` argument to
:py:meth:`Collection.put`, or to the :py:class:`Collection` constructor. A
predefined ``ZLIB_PACKER`` is included, however adding new compressors is
simply a case of constructing an :py:class:`Encoder`.

::

    coll.put({"name": "Alfred" }, packer=centidb.ZLIB_PACKER)


Batch compression
-----------------

Batch compression is supported by way of :py:meth:`Collection.batch`: this is
where a range of records have their values combined before being passed to the
compressor. The resulting stream is saved using a special key that still
permits efficient child lookup. The main restriction is that batches cannot
violate the key ordering, meaning only contiguous ranges may be combined. Calls
to :py:func:`Collection.put` will cause any overlapping batch to be split as
part of the operation.

Since it is designed for archival, it is expected that records within a batch
will not be written often. They must also already exist in the store before
batching can occur, although this restriction may be removed in future.

A run of ``examples/batch.py`` illustrates the tradeoffs of compression:

::

    $ PYTHONPATH=. python examples/batch.py

    Before sz 6952.27kb cnt  403                              (8194.45 get/s 49.37 iter/s 10513.34 iterrecs/s)
     After sz 3250.61kb cnt  403 ratio  2.14 (   zlib size  1, 3822.55 get/s 20.28 iter/s 4315.76 iterrecs/s)
     After sz 1878.92kb cnt  203 ratio  3.70 (   zlib size  2, 3156.00 get/s 29.51 iter/s 5280.37 iterrecs/s)
     After sz 1177.15kb cnt  103 ratio  5.91 (   zlib size  4, 2544.36 get/s 30.88 iter/s 6297.81 iterrecs/s)
     After sz 1029.91kb cnt   83 ratio  6.75 (   zlib size  5, 2351.24 get/s 34.14 iter/s 6621.98 iterrecs/s)
     After sz  816.30kb cnt   53 ratio  8.52 (   zlib size  8, 1921.35 get/s 36.16 iter/s 7168.79 iterrecs/s)
     After sz  635.69kb cnt   28 ratio 10.94 (   zlib size 16, 1098.36 get/s 31.94 iter/s 6970.13 iterrecs/s)
     After sz  547.55kb cnt   16 ratio 12.70 (   zlib size 32, 511.96 get/s 34.20 iter/s 6628.68 iterrecs/s)
     After sz  503.59kb cnt   10 ratio 13.81 (   zlib size 64, 288.66 get/s 28.56 iter/s 6507.69 iterrecs/s)

    Before sz 6952.27kb cnt  403                              (8198.30 get/s 50.20 iter/s 10475.25 iterrecs/s)
     After sz 4508.79kb cnt  405 ratio  1.54 ( snappy size  1, 6456.26 get/s 39.59 iter/s 7765.54 iterrecs/s)
     After sz 2994.95kb cnt  205 ratio  2.32 ( snappy size  2, 5314.67 get/s 38.72 iter/s 7860.98 iterrecs/s)
     After sz 2995.79kb cnt  105 ratio  2.32 ( snappy size  4, 4091.23 get/s 38.66 iter/s 8175.65 iterrecs/s)
     After sz 3049.17kb cnt   85 ratio  2.28 ( snappy size  5, 3609.85 get/s 39.07 iter/s 8184.12 iterrecs/s)
     After sz 2953.20kb cnt   55 ratio  2.35 ( snappy size  8, 2789.39 get/s 41.48 iter/s 8308.94 iterrecs/s)
     After sz 2909.70kb cnt   30 ratio  2.39 ( snappy size 16, 1721.41 get/s 42.48 iter/s 8427.12 iterrecs/s)
     After sz 2874.35kb cnt   18 ratio  2.42 ( snappy size 32, 987.66 get/s 39.33 iter/s 8388.72 iterrecs/s)
     After sz 2859.89kb cnt   12 ratio  2.43 ( snappy size 64, 528.00 get/s 35.33 iter/s 8384.39 iterrecs/s)


Auto-increment
++++++++++++++

When no explicit key function is given, :py:class:`Collection` defaults to
generating transactionally assigned auto-incrementing integers using
:py:meth:`Store.count`. Since this doubles the database operations required,
auto-incrementing keys should be used sparingly. Example:

::

    log_msgs = centidb.Collection(store, 'log_msgs')
    log_msgs.put("first")
    log_msgs.put("second")
    log_msgs.put("third")

    assert list(log_msgs.items()) == [
        ((1,), "first"),
        ((2,), "second"),
        ((3,), "third")
    ]

*Note:* as with everywhere, since keys are always tuples, the auto-incrementing
integer was wrapped in a 1-tuple.




.. _query-parameters:

Query Parameters
++++++++++++++++

The following parameters are supported everywhere some kind of key enumeration
may occur using :py:class:`Collection` or :py:class:`Index`, for example all
iteration methods.

    `lo`:
        Lowest key returned. All returned keys will be `>= lo`. If unspecified,
        defaults to the lowest key in the index or collection.

    `hi`:
        Highest key returned. If `include=False`, all returned keys wil be `<
        hi`, otherwise they will be `<= hi`. If unspecified, defaults to the
        highest key in the index or collection.

    `key`, `args`:
        Key or index tuple to begin iteration from. Equivalent to `hi` when
        `reverse=True` or `lo` when `reverse=False`. When given and
        `reverse=True`, `include` is automatically set to ``True``.

    `reverse`:
        Iterate from the end of the range to the start. If unspecified,
        iterates from the start of the range to the end.

    `include`:
        Using `hi` in iteration range. If ``True``, indicates all keys are `<=
        hi`, otherwise they are `< hi`.

    `max`:
        Specifies the maximum number of records to be returned. Defaults to
        ``None``, meaning unlimited.
