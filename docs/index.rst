
acid
####

.. warning::

    This is a prototype, focusing primarily on physical data layout. Many parts
    are missing, slow, broken and/or very nasty.

**Acid** enhances any ordered-map style database engine to provide something
like an *SQLite equivalent for NoSQL*, a sweet spot between the ease of
managing data with an external DBMS, and the high performance and flexibility
of an in-process database.

    :ref:`Secondary Index Management <indices>`

        Secondary indices may be expressed as Python functions, with
        corresponding index entries automatically maintained as the records in
        a collection change. Single lookups, prefix, and range queries are
        supported on both primary keys and secondary indices, iterating forward
        or in reverse.

    :ref:`Order-preserving Tuple Encoding <keys>`

        Keys and index entries are expressed as Python tuples, which are
        written to the database using a space-efficient, reversible encoding
        that preserves the tuple's order. By exposing the engine's order to the
        user, complex clustering hierarchies can be expressed without resorting
        to custom encodings, error-prone string manipulation, or unnecessary
        indices to work around limitations of the database model.

    :ref:`Batch Record Compression <batch-compression>`

        Lookup efficiency may be incrementally traded for storage efficiency,
        by controlling the batch size or by only compressing infrequently
        accessed subsets of a collection. For many applications a combination
        of clustering and compression allows 3x-5x improvement in storage
        efficiency, while still providing read performance exceeding that of an
        external DBMS.

    :ref:`Declarative Interface <meta>`

        A :py:class:`Model <acid.meta.Model>` class is provided that
        transparently maintains a space-efficient record encoding, and provides
        a convenient ORM-like storage interface familiar to many developers.

    :ref:`Configurable Database Engine <engines>`

        `LMDB <http://symas.com/mdb/>`_, `LevelDB
        <https://code.google.com/p/leveldb/>`_, `Kyoto Cabinet
        <http://fallabs.com/kyotocabinet/>`_, and a basic in-memory skiplist
        engine are supported by default. Additional engines may be supported by
        implementing a single Python class. Significant effort is being made to
        integrate efficiently with LMDB, so that in certain configurations, no
        memory copies need occur for the majority of reads within a
        transaction.

    :ref:`Configurable Record Encoding <record-encoder>`

        `JSON <http://json.org/>`_, `MsgPack <http://msgpack.org/>`_, `Pickle
        <http://docs.python.org/2/library/pickle.html>`_, and `Thrift
        <http://thrift.apache.org/>`_ record encodings are supported by
        default. Additional encodings may be supported by instantiating a
        single Python class. Decoding can be disabled during reads, allowing
        the storage encoding to be aligned directly with whatever output a
        server generates (e.g. JSON), without necessitating a pointless
        decode/re-encode step.


.. raw:: html

    <div style="display: none">

.. toctree::
    :maxdepth: 1

    intro
    core
    meta
    engines
    errors
    encoders
    internals
    cookbook
    notes
    format
    perf

.. raw:: html

    </div>
