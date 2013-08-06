
.. raw:: html

    <p>&nbsp;</p>

    <div style="border: 2px solid red; background: #ffefef; color: black;
                padding: 1ex; text-align: center; width: 66%; margin: auto;
                font-size: larger">
        <strong style="color: #7f0000">WORK IN PROGRESS</strong><br>
        <br>
        This is a design prototype, focusing on useful APIs and physical data
        layout. Some parts are missing, other parts are slow and/or nasty. Even
        if finished it may only be useful as the basis for a library that may
        never exist.<br>
        <br>
        <strong style="color: #7f0000">THIS DOCUMENTATION IS
        INCOMPLETE</strong>
    </div>

.. currentmodule:: centidb
.. toctree::
    :hidden:
    :maxdepth: 2

**centidb** is a compromise between the minimalism of key/value stores and the
convenience of SQL. It augments any store offering an ordered-map interface to
add support for keys composed of tuples rather than bytestrings, and easy
maintenance of secondary indices, with key and index functions written directly
in Python syntax.

There is no fixed value type or encoding, key scheme, compressor, or storage
engine, allowing integration with whatever suits a project. Batch compression
allows read performance to be traded for storage efficiency, while still
allowing transparent access to individual records. Arbitrary key ranges may be
compressed and the batch size is configurable.

The name is due to the core being under 500 lines of code excluding docstrings
and speedups, making it over 100 times smaller than alternatives with
comparable features.


Introduction
############

Since the library depends on an external engine, an initial consideration might
be which to use. Let's forgo the nasty research and settle on
:py:class:`ListEngine <centidb.support.ListEngine>`:

::

    import centidb
    import centidb.support

    engine = centidb.support.ListEngine()
    store = centidb.Store(engine)

:py:class:`Stores <Store>` manage metadata for a set of collections,
along with any registered encodings and counters. Multiple
:py:class:`Collections <Collection>` may exist, each managing
independent sets of records, like an SQL table. Let's create a ``people``
collection:

::

    people = centidb.Collection(store, 'people')

Underneath a few interesting things occurred. Since our engine had no
``people`` collection, a key prefix was allocated using :py:meth:`Store.count`,
and records representing the counter and the collection were written:

::
    
    >>> pprint(engine.items)
    [('\x00(people\x00',                  ' (people\x00\x15\n\x0f'),
     ('\x01(\x01\x01collections_idx\x00', ' (\x01\x01collections_idx\x00\x15\x0b')]


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


Reference
#########

Store Class
+++++++++++

.. autoclass:: Store
    :members:

Collection Class
++++++++++++++++

.. autoclass:: Collection
    :members:

Record Class
++++++++++++

.. autoclass:: Record
    :members:

Index Class
+++++++++++

.. autoclass:: Index
    :members:


Engines
#######

Engine Interface
++++++++++++++++

A storage engine or transaction is any object that implements the following
methods. All key and value variables below are ``NUL``-safe bytestrings:

    `get(key)`:
        Return the value of `key` or ``None`` if it does not exist.

    `put(key, value)`:
        Set the value of `key` to `value`, overwriting any prior value.

    `delete(key)`:
        Delete `key` if it exists.

    `iter(key, reverse=False)`:
        Yield `(key, value)` tuples in key order, starting at `key` and moving
        in a fixed direction.

        Key order must match the C `memcmp()
        <http://linux.die.net/man/3/memcmp>`_ function.

        `key`:
            Starting key. The first yielded element should correspond to this
            key if it exists, or the next highest key, or the highest key in
            the store.

        `reverse`:
            If ``False``, iteration proceeds until the lexicographically
            highest key is reached, otherwise it proceeds until the lowest key
            is reached.

    **txn_id** *= None*
        Name for the transaction represented by the object; may be any Python
        value. Omit the attribute for engines or "transaction objects" that do
        not support transactions. If your engine supports transactions but
        cannot provide an ID, simply set it to :py:func:`time.time`.


Predefined Engines
++++++++++++++++++

.. autoclass:: centidb.support.ListEngine
    :members:

.. autoclass:: centidb.support.SkiplistEngine
    :members:

.. autoclass:: centidb.support.LmdbEngine
    :members:

.. autoclass:: centidb.support.PlyvelEngine
    :members:

.. autoclass:: centidb.support.KyotoEngine
    :members:


Encodings
#########

.. autoclass:: Encoder


Predefined Encoders
+++++++++++++++++++

The ``centidb`` module contains the following predefined :py:class:`Encoder`
instances.

    ``KEY_ENCODER``
        Uses :py:func:`keycoder.packs` and
        :py:func:`keycoder.unpacks` to serialize tuples. It is used
        internally to represent keys, counters, and :py:class:`Store` metadata.

    ``PICKLE_ENCODER``
        Uses :py:func:`pickle.dumps` and :py:func:`pickle.loads` with protocol
        2 to serialize any pickleable object. It is the default encoder if no
        specific `encoder=` argument is given to the :py:class:`Collection`
        constructor.

**Compressors**

These are just :py:class:`Encoder` instances with the convention that their
names end in ``_PACKER``.

    ``PLAIN_PACKER``
        Performs no compression; the input is returned unchanged. This is the
        default packer.

    ``ZLIB_PACKER``
        Uses :py:func:`zlib.compress` and :py:func:`zlib.decompress` to provide
        value compression. It may be passed as the `packer=` argument to
        :py:meth:`Collection.put`, or specified as the default using the
        `packer=` argument to the :py:class:`Collection` constructor.


Thrift Integration
++++++++++++++++++

.. autofunction:: centidb.support.make_thrift_encoder


Example
-------

Create a ``myproject.thrift`` file:

::

    struct Person {
        1: string username,
        2: string city,
        3: i32 age
    }

Now define a collection:

::

    # 'myproject' package is generated by 'thrift --genpy myproject.thrift'
    from myproject.ttypes import Person
    from centidb.support import make_thrift_encoder

    coll = centidb.Collection(store, 'people',
        encoder=make_thrift_encoder(Person))
    coll.add_index('username', lambda person: person.username)
    coll.add_index('age_city', lambda person: (person.age, person.city))

    user = Person(username=u'David', age=42, city='Trantor')
    coll.put(user)

    assert coll.indices['username'].get(u'David') == user

    # Minimal overhead:
    packed = coll.encoder.pack(Person(username='dave'))
    assert packed == '\x18\x04dave\x00'


Other Encoders
++++++++++++++

The `centidb.support` module includes helpers for a few more encodings.

.. autofunction:: centidb.support.make_json_encoder
.. autofunction:: centidb.support.make_msgpack_encoder


Index Examples
##############

Index Usage
+++++++++++

::

    import itertools
    import centidb
    from pprint import pprint

    import plyvel
    store = centidb.Store(plyvel.DB('test.ldb', create_if_missing=True))
    people = centidb.Collection(store, 'people', key_func=lambda p: p['name'])
    people.add_index('age', lambda p: p['age'])
    people.add_index('name', lambda p: p['age'])
    people.add_index('city_age', lambda p: (p.get('city'), p['age']))

    make_person = lambda name, city, age: dict(locals())

    people.put(make_person(u'Alfred', u'Nairobi', 46))
    people.put(make_person(u'Jemima', u'Madrid', 64))
    people.put(make_person(u'Mildred', u'Paris', 34))
    people.put(make_person(u'Winnifred', u'Paris', 24))

    # Youngest to oldest:
    pprint(list(people.indices['age'].items()))

    # Oldest to youngest:
    pprint(list(people.indices['age'].values(reverse=True)))

    # Youngest to oldest, by city:
    it = people.indices['city_age'].values()
    for city, items in itertools.groupby(it, lambda p: p['city']):
        print '  ', city
        for person in items:
            print '    ', person

    # Fetch youngest person:
    print people.indices['age'].get()

    # Fetch oldest person:
    print people.indices['age'].get(reverse=True)


Reverse Indices
+++++++++++++++

Built-in support is not yet provided for compound index keys that include
components that are sorted in descending order, however this is easily
emulated:

+-----------+---------------------------------------+
+ *Type*    + *Inversion function*                  |
+-----------+---------------------------------------+
+ Numbers   | ``-i``                                |
+-----------+---------------------------------------+
+ Boolean   + ``not b``                             |
+-----------+---------------------------------------+
+ String    + ``centidb.invert(s)``                 |
+-----------+---------------------------------------+
+ Unicode   + ``centidb.invert(s.encode('utf-8'))`` |
+-----------+---------------------------------------+
+ UUID      + ``centidb.invert(uuid.get_bytes())``  |
+-----------+---------------------------------------+
+ Key       + ``Key(centidb.invert(k))``            |
+-----------+---------------------------------------+

Example:

::

    coll.add_index('name_age_desc',
        lambda person: (person['name'], -person['age']))

Note that if a key contains only a single value, or all the key's components
are in descending order, then transformation is not required as the index
itself may be iterated in reverse:

::

    coll = centidb.Collection(store, 'people',
        key_func=lambda person: person['name'])
    coll.add_index('age', lambda person: person['age'])
    coll.add_index('age_height',
        lambda person: (person['age'], person['height']))

    # Not necessary.
    coll.add_index('name_desc',
        lambda person: centidb.inverse(person['name'].encode('utf-8')))

    # Not necessary.
    coll.add_index('age_desc', lambda person: -person['age'])

    # Not necessary.
    coll.add_index('age_desc_height_desc',
        lambda person: (-person['age'], -person['height']))

    # Equivalent to 'name_desc' index:
    it = coll.items(reverse=True)

    # Equivalent to 'age_desc' index:
    it = coll.index['age'].items(reverse=True)

    # Equivalent to 'age_desc_height_desc' index:
    it = coll.index['age_height'].items(reverse=True)


Covered indices
+++++++++++++++

No built-in support for covered indices is provided yet, however this can be
emulated by encoding the data to be covered as part of the index key:

::

    coll = centidb.Collection(store, 'people')

    age_height_name = coll.add_index('age_height_name',
        lambda person: (person['age'], person['height'], person['name']))

    age_photo = coll.add_index('age_photo',
        lambda person: (person['age'], file(person['photo']).read()))


    coll.put({'name': u'Bob', 'age': 69, 'height': 113})

    # Query by key but omit covered part:
    tup = next(age_height_name.tups((69, 113)))
    name = tup and tup[-1]

    tup = next(age_photo.tups(69))
    photo = tup and tup[-1]

A future version may allow storing arbitrarily encoded values along with index
entries as part of the API.


Declarative Interface
#####################

The `centidb.metadb` module provides an ORM-like metaclass that allows
simplified definition of database models using Python code.

::

    import centidb
    from centidb import metadb


    class Base(metadb.Model):
        """Base for models belonging to this program. Can be used to add common
        fields, and to bind all subclasses to a particular centidb.Store with a
        single call."""


    class User(Base):
        email = metadb.String()
        first = metadb.String()
        last = metadb.String()
        age = metadb.Integer()

        @metadb.constraint
        def sane_age(self):
            """Ensure the user's age is 1..149 if they provided it."""
            return age is None or (0 < age < 150)


    class Item(Base):
        user_id = metadb.Integer()
        data = metadb.String()

        @metadb.constraint
        def sane_user_id(self):
            """Ensure a User model exists for user_id."""
            return User.get(self.user_id) is not None


    def main():
        Base.bind_store(centidb.open('ListEngine'))

        user = User(email='dw@botanicus.net', first='David', last='Wilson')
        user.save()

        user = User.by_key(1)


Model class
+++++++++++

.. autoclass:: centidb.metadb.Model (\**kwargs)
    :members:
    :inherited-members:

.. autoclass:: centidb.metadb.BaseModel (\**kwargs)


Field Types
+++++++++++

.. autoclass:: centidb.metadb.Bool
.. autoclass:: centidb.metadb.Double
.. autoclass:: centidb.metadb.Integer
.. autoclass:: centidb.metadb.String


Specifying an index
+++++++++++++++++++

.. autofunction:: centidb.metadb.index


Specifying a key function
+++++++++++++++++++++++++

.. autofunction:: centidb.metadb.key
.. autofunction:: centidb.metadb.derived_key
.. autofunction:: centidb.metadb.blind


Specifying constraints
++++++++++++++++++++++

.. autofunction:: centidb.metadb.constraint


Compression Examples
####################

Similar records
+++++++++++++++

Batch compression is useful for storing collections of similar data, such as a
collection of web pages sharing common HTML tags, or perhaps even sharing a
common header and footer. By handing the compressor more data with similar
redundancies, it can do a much better job of producing a smaller bitstream
overall.

Imagine you're building a web scraper, fetching data from a handful of domains
that each has its own distinctive layout. You're not sure about the quality of
your scraper, so you wish to store the source pages in case you need to parse
them again due to a scraper bug.

We're storing our pages in a collection with the record key being the page's
URL. This means pages for the same domain will be physically grouped in the
underlying storage engine, and that contiguous ranges of keys exist where all
keys in the range relate to only a single domain.

::

    >>> pages = centidb.Collection(store, 'pages')
    >>> # ...

    >>> pprint(list(pages.keys(max=5)))
    [("http://bbb.com/page?id=1",),
     ("http://bbb.com/page?id=2",),
     ("http://bbb.com/page?id=3",),
     ("http://ccc.com/page?id=1",),
     ("http://ccc.com/page?id=2")]

    >>> # Print the first record:
    >>> pprint(pages.find())
    {
        "url": "http://bbb.com/page?id=1",
        "html": ... # raw HTML
    }

Here we can use :py:meth:`Collection.batch` with the `grouper=` parameter to
compress 10 pages at a time, while ensuring batches contain only pages relating
to a single domain:

::

    >>> import urlparse

    >>> def domain_grouper(obj):
    ...     return urlparse.urlparse(obj['url']).netloc
    ...

    >>> # Rewrite all records in the collection into batches of 10, ensuring
    >>> # pages from distinct domains don't get batched together:
    >>> coll.batch(max_recs=10, grouper=domain_grouper)
    (1000, 100, None) # Found items, made batches, next key


Archiving Data
++++++++++++++



Cookbook
########

Changing key function
+++++++++++++++++++++

The simplest way to migrate to a new key function is to create a new
collection, and iteratively copy from the old collection:

::

    >>> new_coll.puts(old_coll.values())



Performance
###########

All tests run on a mid-2010 Macbook Pro with Crucial M4 512GB SSD (SATA II
mode). Dataset size is ~80mb.

Setup:

* LevelDB default options (async mode) via `Plyvel
  <http://plyvel.readthedocs.org/>`_:

    ::

        engine = centidb.support.PlyvelEngine(name='test.ldb')
        store = centidb.Store(engine)

* `msgpack <http://msgpack.org/>`_ encoder:

    ::

        encoder = centidb.support.make_msgpack_encoder()

* :py:meth:`Collection.put` with `blind=True`

* 236,000 200 byte dict records with 3 string keys and string values, third
  value containing 150 bytes mostly random data:

    ::

        {'stub': '1001155 [.....] 5273067200649406939020424757',
         'name': 'undergrown',
         'location': 'UNDERGROWN'}

* Collection `key_func` returning two string values from the record:

    ::

        key_func = operator.itemgetter('name', 'location')

* 2 1-tuple ~8 byte string index functions defined, each producing a single
  index entry per record:

    ::

        coll.add_index('rev_name', lambda p: p['name'][::-1])
        coll.add_index('rev_location', lambda p: p['location'][::-1])


`put(blind=True)`
++++++++++++++++++

Indices enabled:

    +-------------------------------------+-----------------------------------+
    | *Without speedups*                  | *With speedups*                   |
    +-------------------+-----------------+---------------------+-------------+
    | Records/sec       | Keys/sec        | Records/sec         | Keys/sec    |
    +-------------------+-----------------+---------------------+-------------+
    | 10,500            | ~30,000         | 41,573              | 124,719     |
    +-------------------+-----------------+---------------------+-------------+

Indices disabled:

    +-------------------------------------+-----------------------------------+
    | *Without speedups*                  | *With speedups*                   |
    +-------------------+-----------------+---------------------+-------------+
    | Records/sec       | Keys/sec        | Records/sec         | Keys/sec    |
    +-------------------+-----------------+---------------------+-------------+
    | 28,041            | 28,041          | 52,129              | 52,129      |
    +-------------------+-----------------+---------------------+-------------+

When running with the speedups module installed, the test becomes very
sensitive to changes in the index function, as non-accelerated code consumes an
increasing proportion of runtime. Thus the library's runtime footprint is
already likely dwarfed by the Python code comprising an even moderately complex
host application.


`put(blind=False)`
+++++++++++++++++++

Indices enabled:

    +-------------------------------------+-----------------------------------+
    | *Without speedups*                  | *With speedups*                   |
    +-------------------+-----------------+---------------------+-------------+
    | Records/sec       | Keys/sec        | Records/sec         | Keys/sec    |
    +-------------------+-----------------+---------------------+-------------+
    | 4,915             | ~19,660         | 10,803              | ~43,212     |
    +-------------------+-----------------+---------------------+-------------+

Indices disabled:

    +-------------------------------------+-----------------------------------+
    | *Without speedups*                  | *With speedups*                   |
    +-------------------+-----------------+---------------------+-------------+
    | Records/sec       | Keys/sec        | Records/sec         | Keys/sec    |
    +-------------------+-----------------+---------------------+-------------+
    | 27,928            | 55,856          | 52,594              | 105,188     |
    +-------------------+-----------------+---------------------+-------------+


* Read performance
* Batch compression read performance
* Write performance
* Compared to ZODB, MongoDB, PostgreSQL


Glossary
########

    *Logical Key*
        Refers to a single local record, which may potentially be part of a
        batch of records sharing a single physical key.

    *Physical Key*
        Refers to a single physical record, which may potentially contain
        multiple logical records as part of a batch.

    *Primitive Value*
        A value of any type that :py:func:`keycoder.packs` supports.




Notes
#####

Floats
++++++

Float keys are unsupported, partially because I have not needed them, and their
use can roughly be emulated with ``int(f * 1e9)`` or similar. But mainly it is
to defer a choice: should floats order alongside integers? If not, then sorting
won't behave like SQL or Python, causing user surprise. If yes, then should
integers be treated as floats? If yes, then keys will always decode to float,
causing surprise. If no, then a new encoding is needed, wasting ~2 bytes
(terminator, discriminator).

Another option is always treating numbers as float, but converting to int
during decode if they may be represented exactly. This may be less surprising,
since an int will coerce to float during arithmetic, but may cause
once-per-decade bugs: depending on a database key, the expression ``123 /
db_val`` might perform integer or float division.

A final option is adding a `number_factory=` parameter to
:py:func:`unpacks`, which still requires picking a good default.

Non-tuple Keys
++++++++++++++

Keys composed of a single value have much the same trade-offs and problems as
floats: either a heuristic is employed that always treats 1-tuples as single
values, leading to user surprise in some cases, and ugly logic when writing
generic code, or waste a byte for each single-valued key.

In the non-heuristic case, further problems emerge: if the user calls
``get(1)``, should it return the same result as ``get((1,))``? If yes, two
lookups may be required.

If no, then another problem emerges: staticly typed languages. In a language
where we might have a ``Tuple`` type representing the key tuple, every
interface dealing with keys must be duplicated for the single-valued case.
Meanwhile the same problems with lookups and comparison in a dynamic language
also occur.

Another option is to make the key encoding configurable: this would allow
non-tuple keys at a cost to some convenience, but also enable extra uses. For
example, allowing a pure-integer key encoding that could be used to efficiently
represent a :py:class:`Collection` as an SQL table by leveraging the `OID`
type, or to provide exact emulation of the sort order of other databases (e.g.
App Engine).

Several difficulties arise with parameterizing key encoding. Firstly,
:py:class:`Index` relies on :py:func:`keycoder.packs` to function. One
solution might be to parameterize :py:class:`Index`'s key construction, or
force key encodings to accept lists of keys as part of their interface. A
second issue is that is that the 'innocence' of the key encoding might be
needed to implement `prefix=` queries robustly.

Another option would be to allow alternative key encodings for
:py:class:`Collection` only, with the restriction that keys must still always
be tuples in order to remain compatible with :py:class:`Index`. Needs further
consideration.

Mapping Protocol
++++++++++++++++

There are a bunch of reasons why the mapping protocol isn't supported by
:py:class:`Collection`.

Firstly, the mapping protocol was designed with unordered maps in mind, and
provides no support for range queries. This greatly limits the 'adaptive power'
of grafting the mapping interface on to :py:class:`Collection`.

Secondly, our 'mapping' is only superficial in nature. Given some key, we map
it to **a copy of** it's associated value, not some unique object itself. In
this respect our interface is more like a translator than a mapper.
Implementing an interface that usually returns identical value objects when
repeatedly given the same key would only encourage buggy code, by implying its
usefulness in circumstances that aren't valid.

Thirdly, to get reasonable performance from :py:meth:`Collection.put` requires
that a :py:class:`Record` descriptor is provided, rather than the record value
itself. Attempting to mimic this using the mapping protocol would feel stupid
and broken.

Fourthly, many storage engines permit duplicate keys, which is directly in
contravention to how the mapping protocol works. While :py:class:`Collection`
does not yet support duplicate keys, an obvious future extension would.

Finally, encouraging users to think about extremely distinct implementations as
painlessly interchangeable is a bad idea. Users should understand the code they
are integrating with, rather than being encouraged to treat it as a black box.


Record Format
+++++++++++++

Non-batch
---------

A non-batch record is indicated when key decoding yields a single tuple.

In this case the record key corresponds exactly to the output of
:py:func:`keycoder.packs` for the single key present. The value has a
variable length integer prefix indicating the packer used, and the remainder is
the output of :py:meth:`Encoder.pack` from the collection's associated encoder.

Batch
-----

A batch record is indicated when key decoding yields multiple tuples.

With batch compression, the key corresponds to the reversed list of member
keys. For example, when saving records with keys ``[('a',), ('b',), ('c',)]``,
the batch record key instead encodes the list ``[('c',), ('b',), ('a',)]``.
This allows the correct record to be located with a single ``>=`` iteration
using any member key, and allows a member's existence to be confirmed without
further value decoding.

The value is comprised of a variable-length integer indicating the number of
records present, followed by variable-length integers indicating the unpacked
encoded length for each record, in the original key order (i.e. not reversed).
The count is encoded to permit later addition of a `pure keys` mode, as
mentioned in the Futures section.

After the variable-length integer array comes a final variable length integer
indicating the compressor used. The remainder of the value is the packed
concatenation of the encoded record values, again in key order.


Metadata
++++++++

Only a small amount of metadata is kept in the storage engine. It is encoded
using ``KEY_ENCODER`` to allow easy access from other languages, since
implementations must always support it.


Collections
-----------

The collection metadata starts with ``<Store.prefix>\x00``, where
`<Store.prefix>` is the prefix passed to :py:class:`Store`'s constructor. The
remainder of the key is an encoded string representing the collection name.

The value is a ``KEY_ENCODER``-encoded tuple of these fields:

+-------------------+-------------------------------------------------------+
| *Name*            | *Description*                                         |
+-------------------+-------------------------------------------------------+
| ``name``          | Bytestring collection name.                           |
+-------------------+-------------------------------------------------------+
| ``idx``           | Integer collection index, used to form key prefix.    |
+-------------------+-------------------------------------------------------+
| ``index_for``     | Bytestring parent collection name. If not ``None``,   |
|                   | indicates this collection is an index.                |
+-------------------+-------------------------------------------------------+
| ``key_scheme``    | Bytestring encoder name used for all keys. If         |
|                   | ``None``, indicates first byte of key indicates       |
|                   | encoding. *Not yet implemented.*                      |
+-------------------+-------------------------------------------------------+
| ``value_scheme``  | String encoder name used for all value encodings in   |
|                   | the collection. *Not yet implemented.*                |
+-------------------+-------------------------------------------------------+
| ``packer_scheme`` | String compressor name used to compress all keys.     |
|                   | If ``None``, indicates first bye of value indicates   |
|                   | packer. *Not yet implemented.*                        |
+-------------------+-------------------------------------------------------+

Collection key prefixes are formed simply by encoding the index using
:py:func:`pack_int`. The index itself is assigned by a call to
:py:meth:`Store.count` using the special name ``'\x00collections_idx'``.

Counters
--------

Counter metadata starts with ``<Store.prefix>\x01``. The remainder of the key
is an encoded string representing the counter name.

The value is a ``KEY_ENCODER``-encoded tuple of these fields:

+-------------------+-------------------------------------------------------+
| *Name*            | *Description*                                         |
+-------------------+-------------------------------------------------------+
| ``name``          | Bytestring counter name                               |
+-------------------+-------------------------------------------------------+
| ``value``         | Integer value                                         |
+-------------------+-------------------------------------------------------+

Encodings
---------

All encodings ever used by :py:class:`Store` are kept persistently so the user
need not manually allocate prefixes, potentially in several places spanning
multiple languages. Additionally since the encoding name is stored, a
meaningful diagnostic can be printed if attempts are made to access records
encoded with an unregistered encoder.

Encoding metadata starts with ``<prefix>\x02``. The remainder of the key is an
encoded string representing the encoding or compressor name.

The value is a ``KEY_ENCODER``-encoded tuple of these fields:

+-------------------+-------------------------------------------------------+
| *Name*            | *Description*                                         |
+-------------------+-------------------------------------------------------+
| ``name``          | Bytestring encoding/compressor name                   |
+-------------------+-------------------------------------------------------+
| ``idx``           | Integer compressor index, used to form value prefix   |
+-------------------+-------------------------------------------------------+

Compressor value prefixes are formed simply by encoding the index using
:py:func:`pack_int`. The index itself is assigned by a call to
:py:meth:`Store.count` using the special name ``'\x00encodings_idx'``.

The following entries are assumed to exist, but are never physically written to
the storage engine:

+-------------------+---------+---------------------------------------------+
| ``name``          | ``idx`` | *Description*                               |
+-------------------+---------+---------------------------------------------+
| ``key``           | 1       | Built-in ``KEY_ENCODER``                    |
+-------------------+---------+---------------------------------------------+
| ``pickle``        | 2       | Built-in ``PICKLE_ENCODER``                 |
+-------------------+---------+---------------------------------------------+
| ``plain``         | 3       | Built-in ``PLAIN_PACKER`` (raw bytes)       |
+-------------------+---------+---------------------------------------------+
| ``zlib``          | 4       | Built-in ``ZLIB_PACKER``                    |
+-------------------+---------+---------------------------------------------+


History
+++++++

The first attempt came during 2011 while porting from App Engine and a
Datastore-alike was needed. All alternatives included so much weirdness (Java?
Erlang? JavaScript? BSON? Auto-magico-sharding?
``PageFaultRetryableSection``?!?) that I eventually canned the project,
rendered incapable of picking something as **simple as a database** that was
*good enough*, overwhelmed by false promises, fake distinctions and overstated
greatness in the endless PR veiled by marketing site designs, and driven by
people for whom the embodiment of *elegance* is more often the choice of font
on a Powerpoint slide.

Storing data isn't hard: it has effectively been solved **since at least 1972**
when the B-tree appeared, variants of which comprise the core of SQLite 3, the
core of MongoDB, and just about 90% of all DBMS wheel reinventions existing in
the 40 years since. Yet today when faced with a B-tree adulterated with
JavaScript and a million more dumb concepts, upon rejecting it as **junk** we
are instantly drowned in the torrential cries of a million: *"you just don't
get it!"*. I fear I do get it, all too well, and I hate it.

So this module is borne out of frustration. On a recent project while
experimenting with compression, I again found myself partially implementing
what this module wants to be: a tiny layer that does little but add indices to
Cold War era technology. No "inventions", no lies, no claims to beauty, no
religious debates about scaleability, just 500ish lines that try to do one
thing reasonably.

And so that remains the primary design goal: **size**. The library should be
*small* and *convenient*. Few baked in assumptions, no overcooked
superstructure of pure whack that won't matter anyway in a year, just indexing
and some helpers to make queries work nicely. If you've read this far, then you
hopefully understand why my receptiveness towards extending this library to be
made "awesome" in some way is all but missing. Patch it at your peril, but
please, bug fixes and obvious omissions only.

Futures
+++++++

Probably:

1. Support inverted index keys nicely
2. Avoid key decoding when only used for comparison
3. Unique index constraints, or validation callbacks
4. Better documentation
5. Index and collection type signatures (prevent writes using broken
   configuration)
6. Smaller
7. Safer
8. C++ library
9. Key splitting (better support DBs that dislike large records)
10. putbatch()
11. More future proof metadata format.
12. Convert Index/Collection guts to visitor-style design, replace find/iter
    methods with free functions implemented once.
13. datetime support

Maybe:

1. "Pure keys" mode: when a collection's key is based entirely on the record
   value (e.g. log line timestamp) or a common prefix, batches need only store
   the highest and lowest member keys in their key, since member record keys
   can be perfectly reconstructed. Lookup would expand varint offset array then
   bisect+decode until desired member is found.
2. Value compressed covered indices
3. `Query` object to simplify index intersections.
4. Configurable key scheme
5. Make key/value scheme prefix optional
6. Make indices work as :py:class:`Collection` observers, instead of hard-wired
7. Convert :py:class:`Index` to reuse :py:class:`Collection`
8. User-defined key blob types. Allocate a small range from the key encoding to
   logic that looks up a name for the byte from metadata, then looks up that
   name in a list of factories registered with the store.

Probably not:

1. Support "read-only" :py:class:`Index` object
2. Minimalist validating+indexing network server module
3. `Engine` or :py:class:`Collection` that implements caching on top of another
4. `Engine` that distributes keyspace using configurable scheme
5. :py:class:`Index` and :py:class:`Query` classes that integrate with richer
   APIs, e.g. App Engine
6. MVCC 'middleware' for non-transactional stores
7. :py:class:`Index` and :py:class:`Collection` variants that store the index
   in a single key. Would permit use with non-ordered stores, e.g. filesystem
   dir with SHA1(key)
8. Be generic enough to allow indices and constraints on purely in-memory
   collections, without encoding overhead.
