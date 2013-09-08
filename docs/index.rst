
acid
####

.. warning::

    This is a prototype, focusing on useful APIs and physical data layout. Many
    parts are missing, slow, broken and/or very nasty.

**Acid** is a compromise between the minimalism of key/value stores and the
convenience of SQL. It augments any store offering an ordered-map interface to
support keys composed of tuples rather than bytestrings, and easy maintenance
of secondary indices, with key and index functions written directly in Python
syntax.

There is no fixed value type or encoding, key scheme, compressor, or storage
engine, allowing integration with whatever suits a project. Batch compression
allows read performance to be traded for storage efficiency, while still
allowing transparent access to individual records. Arbitrary key ranges may be
compressed and the batch size is configurable.

.. toctree::
    intro
    core
    meta
    engines
    errors
    encoders
    cookbook
    notes
    format
    perf
    :maxdepth: 1
