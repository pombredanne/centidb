
acid
####

.. warning::

    This is a prototype, focusing on useful APIs and physical data layout. Many
    parts are missing, slow, broken and/or very nasty.

`Github Repository <https://github.com/dw/acid/>`_

**Acid** is a toolkit to simplify use of ordered-map style database engines,
providing an *SQLite equivalent for NoSQL* by occupying a sweet spot somewhere
between the conceptual ease of managing data using an SQL database, and the
performance and flexibility of an in-process database.

    :ref:`Secondary Index Management <indices>`

        Secondary indices can be expressed directly as simple Python functions.
        Corresponding index records are automatically updated as records are
        added, deleted, and modified from a collection.

    :ref:`Order-preserving Tuple Encoding <keys>`

        Key and index values are expressed as Python tuples, which are written
        to the database using a space-efficient, reversible encoding that
        preserves the tuple's original order. By exposing the engine's order to
        the user, complex clustering hierarchies can be expressed directly
        without resorting to designing a custom key encoding, error-prone
        string manipulation, or unnecessary secondary indices to work around
        limitations of the database model.

    :ref:`Transparent Batch Compression <batch-compression>`

        Read performance may be incrementally traded for storage efficiency, by
        controlling the batch size and by only compressing infrequently
        accessed subsets of a collection. For many applications a combination
        of clustering and compression allows 5x or more improvement in storage
        efficiency, while still providing read performance exceeding that of an
        external DBMS.

    :ref:`Declarative Interface <meta>`

        A :py:class:`Model <acid.meta.Model>` class is provided that
        transparently maintains a space-efficient record encoding, and provides
        a convenient ORM-like storage interface familiar to many developers.

    :ref:`Configurable Value Encoding <intro>`

        No restrictions are made on record types or encodings,


.. raw:: html

    <div style="display: none">

.. toctree::

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
    :maxdepth: 1

.. raw:: html

    </div>
