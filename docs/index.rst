
centidb
=======

`http://github.com/dw/centidb <http://github.com/dw/centidb>`_

.. currentmodule:: centidb
.. toctree::
    :hidden:
    :maxdepth: 2

`centidb` is a tiny database offering a compromise between the minimalism of a
key/value store and the convenience of SQL. It wraps any store offering an
ordered-map interface, adding features that often tempt developers to use more
complex systems.

Functionality is provided for forming ordered compound keys, managing and
querying secondary indices, and a binary encoding that preserves the ordering
of tuples of primitive values. Combining the simplicity of a key/value store
with the convenience of a DBMS indexing system, while absent of any
storage-specific protocol/language/encoding/data model, or the impedence
mismatch that necessitates use of ORMs, it provides for a compelling
programming experience.

Few constraints exist: there is no enforced value type or encoding, key scheme,
compressor, or storage engine, allowing integration with whatever best suits or
is already used by a project.

Batch value compression is supported, trading read performance for improved
compression ratios, while still permitting easy access to data. Arbitrary key
ranges may be selected for compression and the batch size is configurable.

Since it is a Python library, key and index functions are expressed directly in
Python rather than some unrelated language.

Why `centi`-db? Because at under 400 lines of code, it is over 100 times
smaller than alternatives with comparable features.


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

Underneath a few interesting things occurred. Since our in-memory engine had no
``people`` collection, a key prefix was allocated using :py:meth:`Store.count`,
and records representing the counter and the collection were written:

::
    
    >>> pprint(engine.pairs)
    [('\x00(people\x00',                  ' (people\x00\x15\n\x0f'),
     ('\x01(\x01\x01collections_idx\x00', ' (\x01\x01collections_idx\x00\x15\x0b')]

Now let's insert some people:

::

    >>> people.put(('Buffy', 'girl'))
    <Record people:(1) ('Buffy', 'girl')>

    >>> people.put(('Willow', 'girl'))
    <Record people:(2) ('Willow', 'girl')>

    >>> people.put(('Spike', 'boy'))
    <Record people:(3) ('Spike', 'boy')>

    >>> people.get(2)
    ('Willow', 'girl')

Since we didn't specify an encoder during construction, the default pickle
encoder is used which allows almost any Python value, although here we use
tuples. Since no key function was given, the collection defaults to
auto-incrementing keys.

More magic is visible underneath:

::

    >>> pprint(engine.pairs)
    [('\x00(people\x00',                    ' (people\x00\x15\n\x0f'),
     ('\x01(\x01\x01collections_idx\x00',   ' (\x01\x01collections_idx\x00\x15\x0b'),
     ('\x01(key:people\x00',                ' (key:people\x00\x15\x04'),

     ('\n\x15\x01', ' \x80\x02U\x05Buffyq\x01U\x04girlq\x02\x86q\x03.'),
     ('\n\x15\x02', ' \x80\x02U\x06Willowq\x01U\x04girlq\x02\x86q\x03.'),
     ('\n\x15\x03', ' \x80\x02U\x05Spikeq\x01U\x03boyq\x02\x86q\x03.')]

Notice the ``key:people`` counter and freshly inserted people records. Pay
attention to the record keys, occupying only 3 bytes despite their prefix also
encoding the collection.


Keys
++++

We are not limited to simple auto-incrementing keys, in fact keys are always
treated as tuples containing one or more :py:func:`primitive values
<encode_keys>`. The method used to encode the tuples for the storage engine
results in a binary order that is identical to how the tuples would sort in
Python, making working with them very intuitive.

Let's recreate the ``people`` collection, this time 


Value compression
+++++++++++++++++

Values may be compressed by passing a `packer=` argument to
:py:meth:`Collection.put`, or to the :py:class:`Collection` constructor. A
predefined ``ZLIB_PACKER`` is included, however adding new compressors is
simply a case of constructing an :py:class:`Encoder`.

::

    coll.put({"name": "Alfred" }, packer=centidb.ZLIB_PACKER)

Batch compression is supported by way of :py:meth:`Collection.batch`: this is
where a record range has its values combined before passing through the
compressor. The resulting stream is saved using a special key that still
permits efficient child lookup. The main restriction is that batches cannot
violate the key ordering, meaning only contiguous ranges may be combined. Calls
to :py:func:`Collection.put` will cause any overlapping batch to be split as
part of the operation.

Since it is designed for archival, it is expected that records within a batch
will not be written often. They must also already exist in the store before
batching can occur, although this restriction may be removed in future.


Keys & Indices
##############

When instantiating a Collection you may provide a key function, which is
responsible for producing the unique (primary) key for the record. The key
function can accept either one or two parameters. In the first form, only the
record' value is passed, while in the second form .

is passed three parameters:

    `obj`:
        Which is record value itself. Note this is not the Record instance, but
        the :py:attr:`Record.data` (i.e. value) attribute.

    `txn`:
        The transaction this modification is a part of. May be used to
        implement transactional assignment of IDs.

The returned key may be any of the supported primitive values, or a tuple of
primitive values. Note that any non-tuple values returned are automatically
transformed into 1-tuples, and `you should expect this anywhere your code
refers to the record's key`.

For example, to assign a key based on the time in microseconds:

::

    def usec_key(val):
        return int(1e6 * time.time())

Or by UUID:

::

    def uuid_key(val):
        return uuid.uuid4()


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

    assert list(log_msgs.iteritems()) == [
        ((1,), "first"),
        ((2,), "second"),
        ((3,), "third")
    ]

*Note:* as with everywhere, since keys are always tuples, the auto-incrementing
integer was wrapped in a 1-tuple.



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
            key, or if it does not exist, the nearest existent key along the
            direction of movement.

            If `key` is ``None`` and `reverse` is ``True``, iteration should
            begin with the greatest key.

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
        Uses :py:func:`encode_keys` and :py:func:`decode_keys` to serialize
        tuples. It is used internally to represent keys, counters, and
        :py:class:`Store` metadata.

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


Key functions
+++++++++++++

These functions are based on `SQLite 4's key encoding
<http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki>`_, except that:

* Support for ``uuid.UUID`` is added.
* Floats are removed.
* Varints are used for integers.
* Strings use a more scripting-friendly encoding.

.. autofunction:: centidb.encode_keys
.. autofunction:: centidb.decode_keys
.. autofunction:: centidb.invert
.. autofunction:: centidb.next_greater


Varint functions
++++++++++++++++

These functions are based on `SQLite 4's sortable varint encoding
<http://sqlite.org/src4/doc/trunk/www/varint.wiki>`_.

.. autofunction:: centidb.encode_int
.. autofunction:: centidb.decode_int


Examples
########

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
    pprint(list(people.indices['age'].iteritems()))

    # Oldest to youngest:
    pprint(list(people.indices['age'].itervalues(reverse=True)))

    # Youngest to oldest, by city:
    it = people.indices['city_age'].itervalues()
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
    it = coll.iteritems(reverse=True)

    # Equivalent to 'age_desc' index:
    it = coll.index['age'].iteritems(reverse=True)

    # Equivalent to 'age_desc_height_desc' index:
    it = coll.index['age_height'].iteritems(reverse=True)


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
    tup = next(age_height_name.itertups((69, 113)))
    name = tup and tup[-1]

    tup = next(age_photo.itertups(69))
    photo = tup and tup[-1]

A future version may allow storing arbitrarily encoded values along with index
entries as part of the API.


Performance
###########

TBD.

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
        A value of any type that :py:func:`encode_keys` supports.


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
:py:func:`decode_keys`, which still requires picking a good default.

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

Multiple difficulties arise with parameterizing key encoding. Firstly,
:py:class:`Index` relies on :py:func:`encode_keys` to function. One solution
might be to parameterize :py:class:`Index`'s key construction, or force key
encodings to accept lists of keys as part of their interface. A second issue is
that is that the 'innocence' of the key encoding might be needed to implement
`prefix=` queries robustly. Needs further consideration.


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
:py:func:`encode_int`. The index itself is assigned by a call to
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

*Note: not implemented yet.*

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
:py:func:`encode_int`. The index itself is assigned by a call to
:py:meth:`Store.count` using the special name ``'\x00encodings_idx'``.

The following entries always exist (*not yet implemented*):

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
when the B-tree appeared, also known as the core of SQLite 3, the core of
MongoDB, and just about 90% of all DBMS wheel reinventions existing in the 40
years since. Yet today when faced with a B-tree adulterated with JavaScript and
a million more dumb concepts, upon rejecting it as **junk** we are instantly
drowned in the torrential cries of a million: *"you just don't get it!"*. I
fear I do get it, all too well, and I hate it.

So this module is borne out of frustration. On a recent project while
experimenting with compression, I again found myself partially implementing
what this module wants to be: a tiny layer that does little but add indices to
Cold War era technology. No "inventions", no lies, no claims to beauty, no
religious debates about scaleability, just 400ish lines that try to do one
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
5. Index and collection type signatures (prevent writes using an inconsistent
   configuration)
6. Smaller
7. Safer
8. Enough speedups to make a viable middleweight production store
9. C++ library
10. Key splitting (better support DBs that dislike large records)
11. putbatch()

Maybe:

1. "Pure keys" mode: when a collection's key is based entirely on the record
   value (e.g. log line timestamp) or a common prefix, batches need only store
   the highest and lowest member keys in their key, since member record keys
   can be perfectly reconstructed. Lookup would expand varint offset array then
   logarithmic bisect+decode until desired member is found.
2. Value compressed covered indices
3. `Query` object to simplify index intersections.
4. Configurable key scheme
5. Make key/value scheme prefix optional
6. Make indices work as :py:class:`Collection` observers, instead of hard-wired
7. Convert :py:class:`Index` to reuse :py:class:`Collection`

Probably not:

1. Support "read-only" :py:class:`Index` object
2. Minimalist validating+indexing network server module
3. `Engine` or :py:class:`Collection` that implements caching on top of another
4. `Engine` that distributes keyspace using configurable scheme
5. :py:class:`Index` and :py:class:`Query` classes that integrate with richer
   APIs, e.g. App Engine

