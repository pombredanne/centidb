
.. currentmodule:: centidb

Performance
###########

``examples/benchy.py`` drives the library in some basic configurations, showing
relative performance using the various engines available using a single thread.

* Encoder: :py:func:`centidb.encoders.make_msgpack_encoder`
* Generates 236,000 200 byte dict records with 3 string keys and string values,
  third value containing 150 bytes mostly random data:

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

**Variant Explanations**

    *blind*
        Inserts proceed without first checking for an existing record with the
        same key, which is safe if keys are never reused. Checking for a
        previous record is required to ensure indices remain consistent.

    *noblind*
        Inserts check for an existing record, i.e. the default mode.

    *indices*
        2 indices are maintained during insert: one of the lowercase version of
        the word, and one on the uppercase version.

    *noindices*
        No index is maintained during insert. The resulting collection can only
        be searched by key.


**Mode Explanations**

    *insert*
        Insert all keys.

    *rand-index*
        Search for all records using an index, performing searches in random
        order.

    *rand-key*
        Search for all records using their key, performing searches in random
        order.


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



.. _batch_perf:

Batch compression
+++++++++++++++++

A run of ``examples/batch.py`` illustrates the tradeoffs of compression.

.. raw:: html

    <style>
        .pants th,
        .pants td {
            text-align: right !important;
        }
    </style>

.. csv-table:: ``examples/batch.py`` with 777 1.51kb records.
    :class: pants
    :header-rows: 1
    :file: batch-output.csv
