
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

.. autoclass:: centidb.engines.ListEngine
    :members:

.. autoclass:: centidb.engines.SkiplistEngine
    :members:

.. autoclass:: centidb.engines.LmdbEngine
    :members:

.. autoclass:: centidb.engines.PlyvelEngine
    :members:

.. autoclass:: centidb.engines.KyotoEngine
    :members:
