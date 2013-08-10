
.. currentmodule:: centidb

Performance
###########

All tests run on a mid-2010 Macbook Pro with Crucial M4 512GB SSD (SATA II
mode). Dataset size is ~80mb.

Setup:

* LevelDB default options (async mode) via `Plyvel
  <http://plyvel.readthedocs.org/>`_:

    ::

        store = centidb.open('PlyvelEngine', name='test.ldb',
                             create_if_missing=True)

* `msgpack <http://msgpack.org/>`_ encoder:

    ::

        encoder = centidb.encodings.make_msgpack_encoder()

* :py:meth:`Collection.put` with `blind=True`

* 236,000 200 byte dict records with 3 string keys and string values, third
  value containing 150 bytes mostly random data:

    ::

        {'stub': '1001155 [.....] 5273067200649406939020424757',
         'name': 'undergrown',
         'location': 'UNDERGROWN'}

* Collection `key_func` returning two string values from the record:

    ::

        key_func = operator.itemgetter('name', 'location')

* 2 1-tuple ~8 byte string index functions defined, each producing a single
  index entry per record:

    ::

        coll.add_index('rev_name', lambda p: p['name'][::-1])
        coll.add_index('rev_location', lambda p: p['location'][::-1])


`put(blind=True)`
++++++++++++++++++

Indices enabled:

    +-------------------------------------+-----------------------------------+
    | *Without speedups*                  | *With speedups*                   |
    +-------------------+-----------------+---------------------+-------------+
    | Records/sec       | Keys/sec        | Records/sec         | Keys/sec    |
    +-------------------+-----------------+---------------------+-------------+
    | 10,500            | ~30,000         | 41,573              | 124,719     |
    +-------------------+-----------------+---------------------+-------------+

Indices disabled:

    +-------------------------------------+-----------------------------------+
    | *Without speedups*                  | *With speedups*                   |
    +-------------------+-----------------+---------------------+-------------+
    | Records/sec       | Keys/sec        | Records/sec         | Keys/sec    |
    +-------------------+-----------------+---------------------+-------------+
    | 28,041            | 28,041          | 52,129              | 52,129      |
    +-------------------+-----------------+---------------------+-------------+

When running with the speedups module installed, the test becomes very
sensitive to changes in the index function, as non-accelerated code consumes an
increasing proportion of runtime. Thus the library's runtime footprint is
already likely dwarfed by the Python code comprising an even moderately complex
host application.


`put(blind=False)`
+++++++++++++++++++

Indices enabled:

    +-------------------------------------+-----------------------------------+
    | *Without speedups*                  | *With speedups*                   |
    +-------------------+-----------------+---------------------+-------------+
    | Records/sec       | Keys/sec        | Records/sec         | Keys/sec    |
    +-------------------+-----------------+---------------------+-------------+
    | 4,915             | ~19,660         | 10,803              | ~43,212     |
    +-------------------+-----------------+---------------------+-------------+

Indices disabled:

    +-------------------------------------+-----------------------------------+
    | *Without speedups*                  | *With speedups*                   |
    +-------------------+-----------------+---------------------+-------------+
    | Records/sec       | Keys/sec        | Records/sec         | Keys/sec    |
    +-------------------+-----------------+---------------------+-------------+
    | 27,928            | 55,856          | 52,594              | 105,188     |
    +-------------------+-----------------+---------------------+-------------+


* Read performance
* Batch compression read performance
* Write performance
* Compared to ZODB, MongoDB, PostgreSQL



benchy.py
+++++++++

.. raw:: html

    <style>
        .pants th,
        .pants td {
            text-align: right !important;
        }
    </style>

.. csv-table:: ``examples/benchy.py``
    :class: pants
    :header-rows: 1
    :file: benchy.csv

