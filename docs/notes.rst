
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

While the idea dates back to around 2009, an initial prototype wasn't attempted
until 2011 following an abortive migration from App Engine. All alternatives
included so much weirdness (Java? Erlang? Javascript? BSON?) that I canned the
project in sheer displeasure, rendered unable to choose something as **simple
as a database**, overwhelmed by false promises, fake distinctions, and
overstated greatness in the endless PR veiled by marketing site designs, and
driven by those for whom *elegance* is the choice of font on a Powerpoint
slide.

Storing data isn't hard: it has effectively been solved **since at least 1972**
when the B-tree appeared, variants of which comprise the core of SQLite,
MongoDB, and 90% of all DBMS wheel reinventions existing since. So this library
is the product of frustration. While experimenting with compression for a hobby
project in late 2012, I again partially implemented what this library should
be: a small layer that implements indexing, and gently placates the use of Cold
War era technology.


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
* No need to parse/deserialize the query.
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

* Finer control over record clustering, and the possibility of avoiding some
  indices, since random lookups are more efficient. Given a key format `(A, B,
  C)`, iterating `(A, B, C)`, `(A, B+1, C)`, `(A, B+2, C)`, ... is
  computationally more efficient, and vastly more storage efficient than an
  explicit index on `(A, B)`.


Compared to SQL, there are of course numerous downsides to this approach,
however they become less relevant as an application's complexity increases:

* Ability to run ad-hoc reporting queries and reporting tools. In a regular
  application, this capability is eventually lost as the application's data set
  exceeds a single SQL database, and as performance constraints prevent running
  large batch queries against production databases.

* Ability to communicate with self-consistent database over the network for
  free. This flexibility is more useful in the design phase than in a
  production application. Once a data set exceeds a single SQL database,
  consistency checking must be moved into the application layer, at which point
  the only sane design is to query and modify the dataset using an application
  layer RPC interface, to avoid risk of causing data inconsistencies.

* Ability to run complex queries without forethought. This is another
  early-phase benefit that degrades over time: once a data set exceeds a single
  database, JOIN capability is lost, and long before that, most complex query
  features must be abandoned as dataset size and request load increases (e.g.
  table scans must be avoided).

* Ability to separately load provision application and database. Depending on
  the application, many data-independant aspects can be split off into separate
  services, for example in a Flickr-style service, moving image thumbnailing
  and resizing to a task queue.

  For any slow data-dependant requests that must be exposed to the user, an SQL
  database suffers the same inability to spread load: an application making a
  complicated set of queries will generate load on both app server and database
  server.

* Ability to setup "free replication". For the time being this is a real
  show-stopper, however a future version of Acid may supply primitives to
  address it in a more scale-agnostic manner than possible with a traditional
  SQL database.

* Well understood approaches to schema migration. We can potentially do better
  by avoiding SQL here, since adding or removing a field need not necessitate
  touching every record. Some migrations can be "free of charge", while others,
  such as adding an index, may more effort. See `ticket 52
  <https://github.com/dw/acid/issues/52>`_


Futures
+++++++

Planned changes are tracked using `Github's issues list
<https://github.com/dw/acid/issues?state=open>`_


Blog Posts
++++++++++

Various aspects of the library's design have discussed in a series of blog
posts:

 * 2014-01-21: `Cowboy optimization: bisecting a function <http://pythonsweetness.tumblr.com/post/74073984682/cowboy-optimization-bisecting-a-function>`
 * 2014-01-13: `Acid batch record format <http://pythonsweetness.tumblr.com/post/73248785592/acid-batch-record-format>`
 * 2013-12-29: `Acid API v2 ideas <http://pythonsweetness.tumblr.com/post/71522621848/acid-api-v2-ideas>`
 * 2013-10-21: `[Acid] Progress <http://pythonsweetness.tumblr.com/post/64705174861/progress>`
 * 2013-09-30: `Why we can’t lazy-decode JSON <http://pythonsweetness.tumblr.com/post/62721087542/why-we-cant-lazy-decode-json>`
 * 2013-09-26: `Thoughts on a better memory abstraction for Python (2) <http://pythonsweetness.tumblr.com/post/62321291712/thoughts-on-a-better-memory-abstraction-for-python-2/>`
 * 2013-08-13: `Thoughts on a better memory abstraction for Python <http://pythonsweetness.tumblr.com/post/58148801190/thoughts-on-a-better-memory-abstraction-for-python>`
