
Concepts
########

.. warning::

    This section is incomplete.

This chapter is a short walk-through of the motivations behind Acid, suitable
applications for it, and some fundamental ideas on how the storage engines Acid
wraps work internally.


Databases
+++++++++

When we think of a database, we usually imagine something like the SQL model: a
collection of tables containing a set of columns and perhaps one or two
indexes, hidden behind a command language. You ask the database a question
about the facts held in the tables, and the database responds with a set of
records answering the question. A client library manages talking to the
database over the network and all aspects of transforming its protocol and
result format into simple types useful from within the user's programming
language.

Database systems provide a huge set of features that are often taken for
granted, underused or misunderstood entirely. A specific example of this is the
*query planner*: an often large and complex component responsible for deciding
on the best approach to read facts from the tables, and which (if any) indexes
defined seem to apply usefully to the query. For the common case of simply
having some component that knows how to store data, and how to quickly answer
basic questions about that data, a complete database system might be a rather
large hammer for a small nail, depending on the application.

There is also a high upfront cost associated with using a traditional database
system. Before data can be stored a *schema* must be designed, and its
implementation managed somehow. This may entail writing a set of DML scripts
full of ``CREATE TABLE`` statements, or spending an afternoon reading your
Object-Relational Mapper's documentation to discover the precise set of
incantations required to represent a simple string or define a relationship.

This fixed cost is a large part of what originally motivated the use of NoSQL
systems: conceptually since there is no schema at all, no design and
implementation step is required. One begins a NoSQL project by simply writing
out some data, and so it is quite compatible with fast prototyping, shipping
early, and minimal effort designs.


Storage Engines
+++++++++++++++

Beneath every database there are one or more *storage engines* responsible for
persisting data durably, and using a method that allows existing data to be
quickly located. Much of a database's complexity is dedicated to the task of
taking high-level questions posed by the user, and mapping them into
fundamental operations against a storage engine.

Storage engines almost unilaterally deal in *(key, value)* pairs as the
primitive unit of data; much of the engine's job is to take *key* and as
efficiently as possible lookup or modify its corresponding *value*, depending
on what operation the database system asks for. Many technologies are used to
implement storage engines, each with their own interesting set of properties,
but for the purposes of Acid we are mainly interested in a particular property.


Unordered Engines
-----------------

An unordered engine receives a *(key, value)* pair and typically uses some
method to distribute the *key* within a space. An example would be a hash table
that computes a hash value for `key`, then uses modulo to assign that value to
a *bucket*. Even though ``aaaa`` and ``aaab`` differ by a single byte, the
former key might be assigned bucket *1412312* while the latter *123*.

This has many useful properties, for example, it ensures that writes, even to
very similar keys, will be evenly distributed across available resources. If
clients of the engine need to hold a lock on a bucket in order to modify it,
then uniformly distributing modifications is a powerful way to reduce lock
contention. Similarly, underlying resources such as a pool of disks might
benefit from the load spread: clients inserting very similar keys (for example,
the current time) will have their writes evenly distributed across available
spindles.

    .. csv-table:: Example "order" of a hashed collection keyed by an integer
        :class: pants
        :header: Key, Forename, Surname, Age, Job

        **3**, Smith, John, 81, Retired
        **0**, Williamson, Martha, 44, Taxi Driver
        **4**, Johnson, Jill, 23, Student
        **2**, Jones, Frederic, 11, Schoolboy
        **1**, Hull, Simon, 33, Cook

Scanning the contents of such an engine is usually possible, however the
results will not be returned in any meaningful order, and any future changes
such as insertion or deletion may cause the previous order to change
unpredictably. Unordered engines optimize for fast fetch or update of
individual records given their key, but do so by sacrificing an important
operation: the *range query*. As we will shortly see, range queries are crucial
to the most useful feature of a traditional database.


Ordered Engines
---------------

Where an unordered engine attempts to distribute record keys uniformly, an
ordered engine could be viewed as doing exactly the opposite: the keys ``aaaa``
and ``aaab`` are stored specifically so that accessing them sequentially (in
"dictionary" order) is fast, and so that the value for ``aaaa`` is physically
located close to the value for ``aaab``. In the usual case, looking up the
value for ``aaaa`` in an ordered engine will likely cause ``aaab`` to be
brought into memory as part of the same IO operation.

    .. csv-table:: The same collection stored in an ordered engine
        :class: pants
        :header: Key, Forename, Surname, Age, Job

        **0**, Williamson, Martha, 44, Taxi Driver
        **1**, Hull, Simon, 33, Cook
        **2**, Jones, Frederic, 11, Schoolboy
        **3**, Smith, John, 81, Retired
        **4**, Johnson, Jill, 23, Student

Just as when a human looks up a word in a paper dictionary, when a database
asks an ordered storage engine to "lookup the definition of ``aaaa``", it is a
very simple matter to discover which words directly precede or follow ``aaaa``,
since these words will appear on the same page of the dictionary.


Indices
+++++++

The majority of row-oriented database systems rely on this ordering behaviour
to efficiently support their most useful feature: indices. An index here is
simply a dictionary where a record's key and its indexable value are swapped:
the key becomes the value, and the value to be indexed becomes the key, like
*(value, key)*. The resulting pair is an index entry, and a collection of index
entries forms an index.

    .. csv-table:: Age Index
        :class: pants
        :header: Index Key, Record key

        **11**, 2
        **23**, 4
        **33**, 1
        **44**, 0
        **81**, 3

Notice what happens when this dictionary is written to the storage engine: we
are guaranteed that the dictionary's order will be maintained, and so we can
quickly discover the key for a record containing, say, *44* in its *Age* field.
To discover which person is aged *44*, all required is to look up the entry for
*44* in this dictionary, then look up its corresponding value (the original
record's key) in the original dictionary.

Suppose instead of asking for one record key, we'd like to discover the keys
for any person between the age of 20 and 40. The database simply asks the
storage engine to find the page where *20* should be, then begins reading
forward, noting each record key until an entry with an index key larger than
*40* is found.

Once again, since the storage engine works hard to keep similar keys close
together, the desired range of values should reside on a small number of
consecutive dictionary pages, and so reading them is fast and easy. In database
terminology, this *find word, or the next greater word, then walk backwards or
forwards* operation is called a *range query*. Range queries are not only
useful for secondary indices, but as we will shortly see, they can also be
applied to a record's primary key.

Range queries are the fundamental operation behind all of Acid's features.
Consequently there is no support for unordered storage engines, and likely
never will be.


Clustering I
++++++++++++

In the SQL data model, little importance is typically attached to a record's
*primary key*, except that it must be unique, and that there is an implicit
index covering it. A primary key may be of any supported column type, or a
combination of column types, however it is traditional to prefer a single
integer.

Many SQL systems support the concept of *clustering*, where a database can be
physically arranged according to the order of one of its indices. In some
versions of SQL Server this clustering behaviour is automatic, and defaults to
the order of the primary key. Other systems, such as SQLite 3, don't support
complex clustering, but export a magic internal ``oid`` column that allows
control of the internal order.

The power of clustering is that it exposes the underlying storage engine
directly to the user, so that they may customize it to match their
application's expected behaviour. If the majority of an application's query
load takes the form of a range query on a particular order, then it might make
sense to order the storage engine identically, since doing so allows a
secondary index scan + large number of random lookups to be translated into a
far smaller number of scans of the main table.

Not only can CPU-intensive lookups be avoided, but since the storage engine's
mandate is to store records with similar keys close together, disk IO is also
reduced.


Clustering II
+++++++++++++

Clustering is not only beneficial to performance, it may also be used to
express hierarchical entity relationships directly in the storage engine.
Consider a classical SQL table:

    .. csv-table:: Disk Folder Structure
        :class: pants
        :header: ID, Name, Parent ID

        **1**, Top Level Directory (User 18231), ``NULL``
        **2**, Music, 1
        **3**, Pictures, 1
        **4**, Downloads, 1
        **5**, Albums, 2
        **6**, Albums, 3
        **7**, Pop, 5
        **8**, Family, 6
        **9**, Movies, 3
        **10**, Work, 6

Given a *File* record with a *Folder ID* attribute, discovering the file's
complete path in a SQL database might require one query and lookup for each
level in the folder hierarchy. Some SQL systems support a ``PIVOT`` operation
that executes the hierarchical lookups on the server, however the SQL model has
no natural type that would allow expressing the hierarchy in an indexable (and
therefore clusterable) form; at best the server will always be performing
lookups instead of scans.

Let's see what happens if we discard SQL's restrictions.

    .. csv-table:: Non-SQL Folder Structure
        :class: pants
        :header: Key, Name, ID

        "**(18231,)**", Top Level Directory (User 18231), 1
        "**(18231, 1)**", Music, 2
        "**(18231, 1, 1)**", Albums, 5
        "**(18231, 1, 1, 1)**", Pop, 7
        "**(18231, 2,)**", Pictures, 3
        "**(18231, 2, 1)**", Albums, 5
        "**(18231, 2, 1, 1)**", Family, 7
        "**(18231, 2, 1, 2)**", Work, 10
        "**(18231, 3)**", Downloads, 4
        "**(18231, 3, 1)**", Movies, 9

The integer key is replaced by a tuple, but unlike a SQL compound primary key
tuple, this tuple varies in length. By assigning integers at each level of the
hierarchy, the user's folder structure can be directly expressed in the storage
engine. Finding the user's folders is now just a range query, with the output
produced by the storage engine pre-sorted in depth order.

Note that to conveniently reference a folder from other data, it is important
to include a secondary index on the *ID* column. This index allows folders to
be found by ID, and a range query to be performed on their key prefix. With a
single engine order and no additional indexes, not only can folders be
enumerated for a full account, but also any level of the subtree.

One more optimization is visible: by prefixing the folder key with the user ID,
the need to lookup the root folder ID in some *User* record has been
eliminated. Knowing just the user ID is sufficient to scan the prefix
**(18231,)**, producing the desired folder structure.


Tuples & Indices
++++++++++++++++

The power and generality of a variable length tuple as the storage engine key
should now be obvious, it is the primary motivation behind Acid. Unlike our
example tuples above, key tuples may contain any combination of ``datetime``
instances, UUIDs, bytestrings and Unicode strings, ``True``, ``False``, and
``None``. When written to the storage engine, a special key encoding ensures
the tuples will maintain an intuitive and predictable order.

Acid's secondary index support reuses this encoding. An index entry need not be
a plain integer, but any variable-length tuple containing arbitrary
combinations of the above types. There is no requirement that a secondary index
contain exactly one tuple for each record: it may contain zero (*conditional
index*) or multiple (*compound index*), whatever a collection's index function
produces.


.. raw:: html

    <!-- 
    Using very little code, Acid attempts to provide a familiar database-like
    feature set running on top of a key/value store.
    -->


Compression
+++++++++++

Physically ordering the storage engine has one last interesting property, in
that it may not only be used to group data logically, but also by some strongly
redundant attribute. In the folder example the logical attribute was the tree
nesting depth; as we will see, there is another kind of locality.

To illustrate this, consider:

    .. csv-table:: Threaded discussion board with redundant English
        :class: pants
        :widths: 20, 15, 65
        :header: Key, User, Text

        "**(18231,)**", 2, Cats are awesome!
        "**(18231, 1)**", 51, "Yes, I too like cats. I agree they are awesome."
        "**(18231, 1, 1)**", 452, You are both wrong. Cats are not awesome.

And:

    .. csv-table:: Heavily redundant time-varying data
        :class: pants
        :header: Key, Symbol, Qty, Price, Avg

        "**(<11:00:01.001>,)**", GOOG, 100, 500.00, 501.01
        "**(<11:00:01.007>,)**", GOOG, 50, 500.00, 501.01
        "**(<11:00:01.020>,)**", GOOG, 100, 499.99, 501.00
        "**(<11:00:01.022>,)**", GOOG, 100, 500.00, 501.00
        "**(<11:00:01.029>,)**", GOOG, 100, 500.01, 501.00


Notice in the first example how the words *cat* and *awesome* appear
repeatedly. In the second example, the entire group of records is a
near-duplicate, the majority of the data remaining static. Consider now the
applications that would use this data: the discussion board will be rendering
subtrees sequentially, and the financial data application may be servicing
chart data requests, again sequentially.

Since both applications generally work with large group of sequential records,
there is opportunity to amortize the cost of some expensive task over the
group. In other words, there is a clear opportunity to efficiently compress the
data as a group, since the redundancy is so high, but in order to do so
requires that a single key is picked to represent the resulting compression
batch.

Indistinguishable from magic
----------------------------

Recall from earlier that regardless of whether a single record is being
fetched, or a range query is being performed, the initial step in both
operations is identical: *find this key or the next greater key*. Given the
keys between **<11:00:01.001>** and **<11:00:01.029>**, if only the uppermost
key exists, then any attempt to look up a preceeding key will cause the storage
engine to find the uppermost key instead, at no extra cost.

Using a careful key encoding, we can exploit this property to locate any key in
a contiguous range that might be compressed as a group, using the same
operation that would be used if we were searching for it as an individual
record. When this special batch key is detected, Acid will transparently
decompress it and step through the compressed records, just as if they were
stored uncompressed directly in the engine. The decompression step is invisible
to caller code, regardless of whether it is fetching a single record or
performing a range query.

There is no inherent restriction on the size of a batch group, only that in
order to exploit larger groups requires a larger query size to amortize the
cost of decompression. It does not make sense to store records in 1MiB
compressed groups if the average query fetches a single record of 100 bytes,
the cost of decompression would greatly overshadow the cost of the query.

The batch size can be carefully tuned to allow an application's space
efficiency to be weighed against its performance goals, and since these
tunables aren't global settings, the batch size can be varied with the age of
the data, for example to trade compression ratio against server traffic
patterns.


.. raw:: html

    <!--
        Record encodings
        Models
        Why???
            In-process/external performance
            Manageability
            Embeddability
            Increasingly makes sense as application complexity increases
            etc
    -->

