
centidb
#######

.. warning::

    This is a design prototype, focusing on useful APIs and physical data
    layout.

    Some parts are missing, many others are slow, broken and/or nasty. Even if
    finished it may only be useful as the basis for a library that may never
    exist.

**centidb** is a compromise between the minimalism of key/value stores and the
convenience of SQL. It augments any store offering an ordered-map interface to
add support for keys composed of tuples rather than bytestrings, and easy
maintenance of secondary indices, with key and index functions written directly
in Python syntax.

There is no fixed value type or encoding, key scheme, compressor, or storage
engine, allowing integration with whatever suits a project. Batch compression
allows read performance to be traded for storage efficiency, while still
allowing transparent access to individual records. Arbitrary key ranges may be
compressed and the batch size is configurable.

The name is due to the core being under 500 lines of code excluding docstrings
and speedups, making it over 100 times smaller than alternatives with
comparable features.

.. toctree::
    intro
    api
    metadb
    engines
    encoders
    cookbook
    notes
    perf
    :maxdepth: 1
