
centidb
=======

`http://github.com/dw/centidb <http://github.com/dw/centidb>`_

.. toctree::
    :hidden:
    :maxdepth: 2

`centidb` is a tiny database that provides a tradeoff between the minimalism of
a key/value store and the convenience of SQL. It wraps any store that provides
an ordered-map interface, adding features that often tempt developers to use
more complex systems.

Functionality is provided for forming ordered composite keys, managing and
querying secondary indices, and a binary encoding that lexicographically
preserves the ordering of tuples of primitive values. Combining the simplicity
of a key/value store with the convenience of a DBMS's indexing system, while
absent of any storage-specific protocol/language/encoding/data model, or the
impedence mismatch that necessitates use of ORMs, it provides for a compelling
programming experience.

Few design constraints are made: there is no enforced value type or encoding,
key scheme, compression scheme, or storage engine, allowing integration with
whatever is suited to or already used in a project. In addition to the tuple
encoding, integration with the ``pickle`` module is provided, however new
encodings are easily added.

Batch value compression is supported, trading read performance for
significantly improved compression ratios, while still permitting easy access
to data. Arbitrary key ranges can be selected for compression and the batch
size is controllable.

Since it is a Python library, key and index functions are written directly in
Python rather than some unrelated language.

Why `centi`-db? Because it's >100x smaller than alternatives with comparable
features (<400 LOC excluding speedups vs. ~152 kLOC for Mongo).


Common Parameters
#################

In addition to those described later, each function accepts the following
optional parameters:

``key``:
  Indicates a function (in the style of ``sorted(..., key=)``) that maps lines
  to ordered values to be used for comparison. Provide ``key`` to extract a
  unique ID or timestamp. Lines are compared lexicographically by default.

``lo``:
  Lowest offset in bytes, useful for skipping headers or to constrain a search
  using a previous search. For line oriented search, one byte prior to this
  offset is included in order to ensure the first line is considered complete.
  Defaults to ``0``.

``hi``:
  Highest offset in bytes. If the file being searched is weird (e.g. a UNIX
  special device), specifies the highest bound to access. By default
  ``getsize()`` is used to probe the file size.



Key Functions
#############


Key Function
++++++++++++

When instantiating a Collection you may provide a `key_func`, which is
responsible for producing a key for the record. The key function is passed
three parameters:

    `existing_key`:
        A never-saved record is indicated by `existing_key` being set to
        ``None``, otherwise it is set to the existing key for the record.
        Impure keys can be implemented by returning `existing_key` if it is not
        ``None``, otherwise generating a new key.

    `obj`:
        Which is record value itself. Note this is not the Record instance, but
        the ``Record.data`` (i.e. user data) field.

    `txn`:
        The transaction this modification is a part of. Can be used to
        implement transactional assignment of IDs.

The returned key may be any of the supported primitive values, or a tuple of
primitive values. Note that any non-tuple values returned are automatically
transformed into 1-tuples, and `you should expect this anywhere your code
refers to the record's key`.

For example, to assign a key based on the current time:

::

    def time_key(obj, txn):
        """Generate an integer key based on system time."""
        return existing or int(time.time() * 1e6)

Or by UUID:

::

    def uuid_key(existing, obj, txn):
        """Generate a UUID4."""
        return existing or uuid.uuid4()


Auto-incrementing Keys
----------------------

When no explicit key function is given, collections default to generating
transactionally assigned auto-incrementing integers using `Store.count()`.
Since this doubles the database operations required, auto-incrementing keys
should be used sparingly. Example:

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


Record Class
############

.. autoclass:: centidb.Record
    :members:


Collection Class
################

.. autoclass:: centidb.Collection
    :members:


Index Class
###########

.. autoclass:: centidb.Index
    :members:


Encodings
#########

.. autoclass:: centidb.Encoder


``KEY_ENCODER``
+++++++++++++++

    This is a predefined `Encoder` instance that uses `encode_keys()` and
    `decode_keys()` to serialize tuples. It is used internally to represent
    keys, counters, and store metadata.


``PICKLE_ENCODER``
++++++++++++++++++

    This is a predefined `Encoder` instance that uses `cPickle.dumps()` and
    `cPickle.loads()` to serialize tuples, using pickle protocol version 2. It
    is the default encoder if no specific `Encoder` instance is given to the
    `Collection` constructor.


Thrift Integration
++++++++++++++++++

This uses `Apache Thrift <http://thrift.apache.org/>`_ to serialize values
(which must be be Thrift structs) to a compact binary representation.

Create an `Encoder` factory:

::

    def make_thrift_encoder(klass, factory=None):
        if not factory:
            factory = thrift.protocol.TCompactProtocol.TCompactProtocolFactory()

        def loads(buf):
            transport = thrift.transport.TTransport.TMemoryBuffer(buf)
            proto = factory(transport)
            value = klass()
            value.read(proto)
            return value

        def dumps(value):
            return thrift.TSerialization.serialize(value, factory)

        # Form a name from the Thrift ttypes module and struct name.
        name = 'thrift:%s.%s' % (klass.__module__, klass.__name__)
        return centidb.Encoding(name, loads, dumps)


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

    coll = centidb.Collection(store, 'people',
        encoder=make_thrift_encoder(Person))
    coll.add_index('username', lambda person: person.username)
    coll.add_index('age_city', lambda person: (person.age, person.city))

    user = Person(username='David', age=42, city='Trantor')
    coll.put(user)

    assert coll.indices['username'].get('David') == user


Encoding functions
++++++++++++++++++

.. autofunction:: centidb.encode_keys
.. autofunction:: centidb.decode_keys


Example
#######


Performance
###########


Notes
#####


