
.. currentmodule:: acid

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
:py:class:`Index` relies on :py:func:`acid.keylib.packs` to function. One
solution might be to parameterize :py:class:`Index`'s key construction, or
force key encodings to accept lists of keys as part of their interface. A
second issue is that is that the 'innocence' of the key encoding might be
needed to implement `prefix=` queries robustly.

Another option would be to allow alternative key encodings for
:py:class:`Collection` only, with the restriction that keys must still always
be tuples in order to remain compatible with :py:class:`Index`. Needs further
consideration.


History
+++++++

While the idea is older, an implementation did not occur until 2011 after an
abortive migration from App Engine. All alternatives included so much weirdness
(Java? Erlang? Javascript? BSON?) that I canned the project in sheer
displeasure, rendered unable to choose something as **simple as a database**,
overwhelmed by false promises, fake distinctions, and overstated greatness in
the endless PR veiled by marketing site designs, and driven by those for whom
*elegance* is the choice of font on a Powerpoint slide.

Storing data isn't hard: it has effectively been solved **since at least 1972**
when the B-tree appeared, variants of which comprise the core of SQLite,
MongoDB, and 90% of all DBMS wheel reinventions existing since. So this library
is the product of frustration. On a recent project while experimenting with
compression, I again partially implemented what this library should be: a small
layer that implements indexing, and gently placates the use of Cold War era
technology.


Use cases
+++++++++

The library is experimental, but eventually it should become a small,
convenient way to store data for programs with medium sized datasets. Already
with a suitable engine it can offer better durability guarantees, and vastly
better performance than larger and more established systems, such as MongoDB.
Coupled with :py:class:`LmdbEngine <acid.engines.LmdbEngine>` it is even
possible to make consistent online backups without resorting to platform
tricks, unlike MongoDB.

With carefully selected primitives, more of the problem domain could be
supported. For instance, supporting explicit sharding and replication are
interesting, and neither feature should require a 300kLOC codebase to
implement, or even a 3kLOC codebase. By removing complexity from the task of
persisting data, greater room is left to ponder *legitimately hard problems*,
such as serving an application's data after it outgrows a single computer or
magically partitioned DBMS cluster.


General ideas
+++++++++++++

By pushing the DBMS into the application, numerous layers of indirection are
removed from the lifecycle of a typical request. For example:

* By explicitly naming indices, there is no need for a query planner.

* By explicitly controlling the encoding, there may be no need for ever
  deserializing data before passing it to the user, such as via
  :py:func:`make_json_encoder <acid.encoders.make_json_encoder>` and
  :py:meth:`get(... raw=True) <acid.Collection.get>`.

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
    store in Memcache using some completely different encoding (e.g. BSON vs.
    pickle).
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
  a dedicated hardware thread to implement group commit. Similarly it is
  possible to prioritize work for the thread, e.g. by having separate queues
  for paid and free users.


Futures
+++++++

Planned changes are tracked using `Github's issues list
<https://github.com/dw/acid/issues?state=open>`_
