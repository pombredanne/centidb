
What is Acid for?
#################

.. csv-table:: Typical storage engine trade-offs
    :header-rows: 1
    :file: what-options.csv


* kLOC is the sum of the engine+server minus tests, its Python adapter minus tests, and cloc run on PostgreSQL source minus tests, psycopg2
  source minus tests, and SQLalchemy source minus tests.

* MongoDB kLOC is the sum of cloc run on MongoDB source minus tests and pymongo
  source minus tests.

When developing a program that needs to store data, our storage choices often
look like:

* Ad hoc files: manipulation/indices/backup/replication must be manually
  implemented in error-prone code.

* SQL: indices are handled automatically, but all data must be
  shoehorned into relations. Queries use a separate language encourages use of
  huge, complex ORM frameworks just to fetch a single row (43kLOC for
  SQLAlchemy). Backup and replication are well understood problems for most SQL
  databases.

* Object storage like MongoDB. Indices are handled automatically, and data
  usually requires less shoehorning. Queries are less complex than SQL, but
  require a similar amount of forethought to express. Backup and replication
  options are often incomplete or immature for many object stores.

* Raw key/value stores like Kyoto Tyrant or in-process options like LMDB or
  LevelDB. Indices must be implemented manually, but no shoehorning of the
  application's data is required to fit the store's model. Queries are reduced
  to trivial *get/walk* operations. Backup and replication options vary by
  store, but often simply do not exist.

This library seeks to introduce a 5th option, which is to wrap any
tree-structured key/value store to add automatic indices, and wrap common query
styles up.
