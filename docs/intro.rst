
.. currentmodule:: acid

Introduction
############

While it is possible to construct all objects from the :ref:`api-reference`
manually, some helpers exist to simplify common usage. First we make use of
:py:func:`acid.open`, which wraps the process of constructing an
:py:class:`Engine <acid.engines.Engine>` and attaching it to a
:py:class:`Store <acid.Store>`.

Since the library depends on an external engine, an initial consideration might
be which to use. For now, let's forgo the nasty research and settle on the
simplest available, :py:class:`ListEngine <acid.engines.ListEngine>`:

::

    import acid
    store = acid.open('ListEngine')

We now have a :py:class:`Store <Store>`. Stores manage metadata for a set of
collections, along with any registered encodings and counters. Multiple
:py:class:`Collections <Collection>` may exist, each managing independent sets
of records, like an SQL table.

Let's create a ``people`` collection:

::

    store.add_collection('people')

Underneath a few interesting things just occurred. Since the engine had no
``people`` collection, a key prefix was allocated using :py:meth:`Store.count`,
and records representing the counter and the collection were written.


Insertion
+++++++++

Now let's insert some people using :py:meth:`Collection.put`:

::

    >>> store['people'].put(('Buffy', 'girl'))
    acid.Key(1L,)

    >>> store['people'].put(('Willow', 'girl'))
    acid.Key(2L,)

    >>> store['people'].put(('Spike', 'boy'))
    acid.Key(3L,)

Since we didn't specify an encoder during construction, the default pickle
encoder is used which allows almost any Python value, although here we use
tuples. As no key function was given, the collection defaults to
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

    >>> store['people'].get(2)
    ('Willow', 'girl')

    >>> # No such record.
    >>> store['people'].get(99)
    None

    >>> # Default is returned.
    >>> store['people'].get(99, default=('Angel', 'boy'))
    ('Angel', 'boy')

Be aware that unlike :py:meth:`dict.get`, :py:meth:`Collection.get` and related
methods return freshly decoded *copies* of the associated value. In this
respect :py:class:`Collection` only superficially behaves like a
:py:class:`dict`:

::

    >>> t1 = store['people'].get(2)
    >>> t2 = store['people'].get(2)

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
    >>> store['people'].find()
    ('Buffy', 'girl')

    >>> # Find record with highest key, 3
    >>> store['people'].find(reverse=True)
    ('Spike', 'boy')

We can locate records based only on the relation of their key to some
constraining keys:

::

    >>> # Find first record with 2 <= key < 99.
    >>> store['people'].find(lo=2, hi=99)
    ('Willow', 'girl')

    >>> # Find last record with 2 <= key < 99.
    >>> store['people'].find(lo=2, hi=99, reverse=True)
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
        >>> list(store['people'].keys())
        [acid.Key(1L,), acid.Key(2L,), acid.Key(3L,)]

        >>> # All keys, end to start.
        >>> list(store['people'].keys(reverse=True))
        [acid.Key(3L,), acid.Key(2L,), acid.Key(1L,)]

        >>> # Keys from 2 to end:
        >>> list(store['people'].keys(2))
        [acid.Key(2L,), acid.Key(3L,)]

        >>> # Keys from 2 to start:
        >>> list(store['people'].keys(2, reverse=True))
        [acid.Key(2L,), acid.Key(1L,)]


:py:meth:`Collection.values`

    ::

        >>> # All values, start to end:
        >>> pprint(list(store['people'].values()))
        [('Buffy', 'girl'),
         ('Willow', 'girl'),
         ('Spike', 'boy')]

:py:meth:`Collection.items`

    ::

        >>> # All (key, value) pairs, from 99 to 2:
        >>> pprint(list(store['people'].items(lo=2, hi=99, reverse=True)))
        [(acid.Key(3L,), ('Spike', 'boy')),
         (acid.Key(2L,), ('Willow', 'girl'))]


Keys & Indices
++++++++++++++

While auto-incrementing keys are useful and efficient to store, they often
prevent the ordered nature of the storage engine from being fully exploited. As
we can efficiently iterate key ranges, by controlling the key we can order the
collection in ways that are very useful for queries.

To make this ordering easy to exploit, keys are treated as tuples of one or
more :py:func:`primitive values <acid.keylib.packs>`, with the order of
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
is responsible for producing record keys. The key function accepts the record's
value and returns the record's key. The key may be any supported primitive
value, or a tuple of primitive values. For example, to assign a key based on
the time in microseconds:

    ::

        >>> def usec_key(val):
        ...     return int(1e6 * time.time())

        >>> stuff = acid.Collection(store, 'stuff', key_func=usec_key)

Or by UUID:

    ::

        >>> def uuid_key(val):
        ...     return uuid.uuid4()

        >>> stuff = acid.Collection(store, 'stuff', key_func=uuid_key)

Finally, a key function may also be marked as `derived` (`derived_keys=True`),
indicating that if the record value changes, the key function should be
reinvoked to assign a new key.

    ::

        >>> # If username changes, we need to update the record's key.
        >>> def user_name_key(val):
        ...     return val['username']

        >>> users = acid.Collection(store, 'users',
        ...     key_func=user_name_key,
        ...     derived_keys=True)


Let's create a new collection, this time storing :py:class:`dicts <dict>` with
some new fields. The collection holds user accounts for an organizational web
application, where each user belongs to a particular department within a
particular region.

::

    users = acid.Collection(store, 'users')


Auto-increment
--------------

When no explicit key function is given, :py:class:`Collection` defaults to
generating transactionally assigned auto-incrementing integers using
:py:meth:`Store.count`. Since this doubles the database operations required,
auto-incrementing keys should be used sparingly. Example:

::

    log_msgs = acid.Collection(store, 'log_msgs')
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



Indices
-------

A primary benefit of the library is the ability to define *secondary indices*
on a collection. These are essentially separate, hidden collections that
reverse map from record attributes to the record's primary key, similar to
other database systems.

An index can be created by calling :py:meth:`Collection.add_index`, passing it
a name and function implementing the index:

::

    def age_index(person):
        return person['age']

    store['people'].add_index('by_age', age_index)

An index function can return a ``None``, a single primitive value, a tuple of
values, or a list of any of the above. Returning ``None`` indicates that no
index entries should be generated for this record. Returning a primitive value
or tuple specifies a single entry, and returning a list of values or tuples
indicates multiple entries.

.. code-block:: python

    def age_nickname_index(person):
        """Produce one index entry for each (age, nickname)."""
        return [(person['age'], nick) for nick in person['nicknames'])]


    def deceased_name_index(person):
        """Produce an index of names but include only people that died."""
        if person['dead']:
            return person['name']


Compression
+++++++++++

Individual values may be compressed by passing a `packer=` argument to
:py:meth:`Collection.put`, or to the :py:class:`Collection` constructor. A
predefined :py:attr:`acid.encoders.ZLIB` compressor is included, however adding
new compressors is simply a case of constructing n
:py:class:`acid.encoders.Compressor <Compressor>` instance.

::

    store['coll'].put({"name": "Alfred" }, packer=acid.encoders.ZLIB)

Supporting a new custom compressor is trivial:

.. code-block:: python

    import lz4

    # Build an Encoder instance describing the encoding.
    LZ4 = acid.encoders.Compressor('lz4', lz4.loads, lz4.dumps)

    # Register the encoder with the store, which causes allocation of a
    # persistent numeric ID, and saving the encoder's record in the engine.
    store.add_encoder(LZ4)

Note that custom compressors must always be re-registered with
:py:meth:`Store.add_encoder` each time the store is re-opened, otherwise the
library will raise exceptions when a compressed record is encountered.


Batch compression
+++++++++++++++++

Batch compression is supported by way of :py:meth:`Collection.batch`: this is
where a range of records *have their values concatenated* before being passed
to the compressor. The resulting stream is saved using a special key that still
permits efficient child lookup. The main restriction is that batches cannot
violate the key ordering, meaning only contiguous ranges may be combined. Calls
to :py:func:`Collection.put` will cause any overlapping batch to be split as
part of the operation.

Since this feature is designed for archival, records within a batch should not
be written often. They must also already exist in the store before batching can
occur, although this restriction may be removed in future.

.. note::

    Batch compression is currently only possible on a collection. A future
    version may also support the ability to batch compress secondary indices.

Please see :ref:`batch_perf` in the performance chapter for a comparison of
compression parameters.


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
