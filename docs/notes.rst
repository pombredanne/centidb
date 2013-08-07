
.. currentmodule:: centidb

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



benchy.py
+++++++++

.. raw:: html

    <style>
        .pants th,
        .pants td {
            text-align: right !important;
        }
    </style>

.. csv-table:: ``examples/benchy.py``
    :class: pants
    :header-rows: 1
    :file: benchy.csv


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
