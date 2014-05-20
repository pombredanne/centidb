
Internals
#########

Classes here are not (yet) directly exposed to the user, but present here so
that other user-visible documentation can link to them.



Key Encoding Library
++++++++++++++++++++

.. module:: acid.keylib


Key
---

:py:class:`acid.keylib.Key` is documented in the core interface chapter.


KeyList
-------

.. autoclass:: acid.keylib.KeyList
    :members:


Iterators
+++++++++

.. module:: acid.iterators

Iterator Result
---------------

.. autoclass:: acid.iterators.Result
    :members:


Iterator
--------

.. autoclass:: acid.iterators.Iterator
    :members:


BasicIterator
-------------

.. autoclass:: acid.iterators.BasicIterator
    :members:


BatchIterator
-------------

.. autoclass:: acid.iterators.BatchIterator
    :members:


Strategies
++++++++++

BasicStrategy
-------------

.. autoclass:: acid.core.BasicStrategy
    :members:

BatchStrategy
-------------

.. autoclass:: acid.core.BatchStrategy
    :members:


Contexts
++++++++

TxnContext
----------

.. autoclass:: acid.core.TxnContext

GeventTxnContext
----------------

.. autoclass:: acid.core.GeventTxnContext
