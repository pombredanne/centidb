
.. currentmodule:: centidb

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


History
+++++++

The first attempt came during 2011 while porting from App Engine and a
Datastore-alike was needed. All alternatives included so much weirdness (Java?
Erlang? JavaScript? BSON? Auto-magico-sharding?
`PageFaultRetryableSection
<https://github.com/mongodb/mongo/blob/master/src/mongo/db/pagefault.h#L35>`_?!?)
that I eventually canned the project, rendered incapable of picking something
as **simple as a database** that was *good enough*, overwhelmed by false
promises, fake distinctions and overstated greatness in the endless PR veiled
by marketing site designs, and driven by people for whom the embodiment of
*elegance* is more often the choice of font on a Powerpoint slide.

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


Use cases
+++++++++

The library is experimental, but eventually it should become a small,
convenient way to store data for programs with small to medium size datasets.

Already with the righ storage engine it can offer better guarantees about data
consistency, and vastly better performance than much larger and more
established systems, such as MongoDB. Coupled with :py:class:`LmdbEngine
<centidb.engines.LmdbEngine>` it is even possible to make consistent online
snapshots without resorting to platform trickery, very much unlike MongoDB.

Ideally with the right set of primitives, more of MongoDB's problem domain
could be subsumed. For instance, supporting sharding and replication are
definitely interesting, and there is no reason why either of these features
requires a 300kLOC codebase to implement, or even a 30kLOC codebase.

By removing so much complexity from the simple task of persisting data, more
room is left for pondering *legitimately hard problems*, such as serving an
application's data after it outgrows a single computer or automagically sharded
DBMS cluster.


General ideas
+++++++++++++

By pushing the DBMS into the application itself, numerous layers of indirection
are removed from the lifecycle of a typical request. For example:

* By explicitly naming indices, there is no need for a query planner.

* By explicitly controlling the encoding, there may be no need for ever
  deserializing data before passing it to the user, such as via
  :py:func:`make_json_encoder <centidb.encoders.make_json_encoder>` and
  :py:meth:`get(... raw=True) <centidb.Collection.get>`.

* Through careful buffer control during a transaction, memory copies are
  drastically reduced.

* No need to establish a DBMS connection using the network layer.
* No need to serialize the query.
* No need to context switch to the DBMS.
* No need to deserialize the query.
* While there are more results, no need to endlessly serialize/context
  switch/deserialize the results.

* As the query cost approaches 0, the need to separately cache results is
  obviated:

  * No need to deserialize DBMS-specific data format, only to re-serialize to
    store in Memcache.
  * No need for subtle/bug ridden race avoidance strategies when handling
    updates to multiple copies of data.
  * No need for multiple duplicate copies of data in RAM: one in the OS page
    cache, one in Memcache, and (depending on DBMS) one in the DBMS application
    layer cache.

* No need to monitor buffers and connection limits for the DBMS.

* No need to write some application logic in Javascript or PL/SQL to avoid
  expensive context switch/query cost. Implementing data-specific walks such as
  graph searches can be done more simply and clearly in Python.

* Much finer control over commit strategies. For example when handling updates
  via a greenlet style server, closures describing an update can be queued for
  a dedicated hardware *writer* thread to implement group commit.


Futures
+++++++

Probably:

1. Support inverted index keys nicely
2. Avoid key decoding when only used for comparison
3. Unique index constraints, or validation callbacks
4. Index and collection type signatures (prevent writes using broken
   configuration)
5. putbatch()
6. More future proof metadata format.
7. Convert Index/Collection guts to visitor-style design, replace find/iter
   methods with free functions implemented once.
8. datetime support
9. **join()** function: accept multiple indices producing keys in the same
   order, return an iterator producing the union or intersection of those
   indices.

Maybe:

1. "Pure keys" mode: when a collection's key is based entirely on the record
   value (e.g. log line timestamp) or a common prefix, batches need only store
   the highest and lowest member keys in their key, since member record keys
   can be perfectly reconstructed. Lookup would expand varint offset array then
   bisect+decode until desired member is found.
2. Covered indices: store/compress large values within index records.
3. Make indices work as :py:class:`Collection` observers, instead of hard-wired
4. Convert :py:class:`Index` to reuse :py:class:`Collection`
5. User-defined key blob types. Allocate a small range from the key encoding to
   logic that looks up a name for the byte from metadata, then looks up that
   name in a list of factories registered with the store.
6. C++ library

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
