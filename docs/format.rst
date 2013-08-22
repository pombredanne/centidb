

Storage Format
##############

This documents any centidb-specific representations data uses inside the
storage engine.


Key encoding
++++++++++++

.. warning::

    This section is incomplete.

The goal of the key encoding is straightforward: given a Python tuple
containing a sequence of primitive values, produce a sequence of bytes that
sorts as closely as possible to how :py:meth:`list.sort` would sort a list of
the equivalent tuples.

All functions provided by the library depend on useful side-effects provided by
the key encoding. The encoding is designed so that most of the library's
complexity is constrained to a single module. Further effort has been made to
ensure the encoding is as space-efficient as possible, since even a single byte
saving can produce a 1GiB reduction in the size of a large enough database.

It would be possible to encode all integers so they sort correctly in binary
form simply by storing their big-endian representation. However for this to
work, all integers must be padded to the maximum supported size. In the case of
a system supporting 64-bit integers, this means wasting a full 8 bytes even for
the number *1*. A 4-tuple key like ``(user_id 613, source_id 15122, timestamp
5124324, event_id 13)``, would require 32+4 bytes, however through careful
encoding the same key can be represented in only 14 bytes.

For a billion-keyed database, this represents a savings of almost 21GiB.


Strings
-------

Bytestrings are re-encoded to include an inline marker used to delimit the end
of the string. This is necessary since we need to detect the start of the next
encoded tuple element, and NUL can't be used since the string may legally
contain NULs. The marker is inserted so that every byte that is not the final
byte has its 8th bit set, with the final byte having it cleared. The input is
treated as a stream of bits, with groups of 7 packed alongside the marker into
every byte. This ensures a constant ``ceil(len(s) * 1.142)`` space overhead
regardless of input.

When the length of the input string is not evenly divisible by 7, the trailer
is encoded in the upper bits of the final byte, with excess lower bits
discarded. As a special case, the empty string is encoded as a single ``0x00``
byte. This cannot conflict with any 1-byte string, since a 1-byte string's
lowest bit would overflow into a trailer byte — 1-byte strings always encode to
2 bytes.

An alternative approach relying on escaping would result in a 2x blowup given a
string containing only the escape character, and it would be impossible to
predict database size given a large number of fixed-width bytestring keys.
Prefixing strings with their length would cause strings to sort by length
rather than lexicographically.


+---------------+---------------+
| Input Length  | Output Length |
+---------------+---------------+
| 0             | 1             |
+---------------+---------------+
| 1             | 2             |
+---------------+---------------+
| 2             | 3             |
+---------------+---------------+
| 3             | 4             |
+---------------+---------------+
| 4             | 5             |
+---------------+---------------+
| 5             | 6             |
+---------------+---------------+
| 6             | 7             |
+---------------+---------------+
| 7             | 8             |
+---------------+---------------+
| 8             | 10            |
+---------------+---------------+
| 9             | 11 etc.       |
+---------------+---------------+



Record format
+++++++++++++

Individual records
------------------

A non-batch record is indicated when key decoding yields a single tuple.

In this case the record key corresponds exactly to the output of
:py:func:`centidb.keycoder.packs` for the single key present. The value has a
variable length integer prefix indicating the packer used, and the remainder is
the output of :py:meth:`Encoder.pack` from the collection's associated encoder.


Batch records
-------------

A batch record is indicated when key decoding yields multiple tuples.

With batch compression, the key corresponds to the reversed list of member
keys. For example, when saving records with keys ``[('a',), ('b',), ('c',)]``,
the batch record key instead encodes the list ``[('c',), ('b',), ('a',)]``.
This allows the correct record to be located with a single ``>=`` iteration
using any member key, and allows a member's existence to be confirmed without
further value decoding.

The value is comprised of a variable-length integer indicating the number of
records present, followed by variable-length integers indicating the unpacked
encoded length for each record, in the original key order (i.e. not reversed).
The count is encoded to permit later addition of a `pure keys` mode, as
mentioned in the Futures section.

After the variable-length integer array comes a final variable length integer
indicating the compressor used. The remainder of the value is the packed
concatenation of the encoded record values, again in key order.


Metadata
++++++++

Only a small amount of metadata is kept in the storage engine. It is encoded
using ``KEY_ENCODER`` to allow easy access from other languages, since
implementations must always support it.


Collections
-----------

The collection metadata starts with ``<Store.prefix>\x00``, where
`<Store.prefix>` is the prefix passed to :py:class:`Store`'s constructor. The
remainder of the key is an encoded string representing the collection name.

The value is a ``KEY_ENCODER``-encoded tuple of these fields:

+-------------------+-------------------------------------------------------+
| *Name*            | *Description*                                         |
+-------------------+-------------------------------------------------------+
| ``name``          | Bytestring collection name.                           |
+-------------------+-------------------------------------------------------+
| ``idx``           | Integer collection index, used to form key prefix.    |
+-------------------+-------------------------------------------------------+
| ``index_for``     | Bytestring parent collection name. If not ``None``,   |
|                   | indicates this collection is an index.                |
+-------------------+-------------------------------------------------------+
| ``key_scheme``    | Bytestring encoder name used for all keys. If         |
|                   | ``None``, indicates first byte of key indicates       |
|                   | encoding. *Not yet implemented.*                      |
+-------------------+-------------------------------------------------------+
| ``value_scheme``  | String encoder name used for all value encodings in   |
|                   | the collection. *Not yet implemented.*                |
+-------------------+-------------------------------------------------------+
| ``packer_scheme`` | String compressor name used to compress all keys.     |
|                   | If ``None``, indicates first bye of value indicates   |
|                   | packer. *Not yet implemented.*                        |
+-------------------+-------------------------------------------------------+

Collection key prefixes are formed simply by encoding the index using
:py:func:`pack_int`. The index itself is assigned by a call to
:py:meth:`Store.count` using the special name ``'\x00collections_idx'``.

Counters
--------

Counter metadata starts with ``<Store.prefix>\x01``. The remainder of the key
is an encoded string representing the counter name.

The value is a ``KEY_ENCODER``-encoded tuple of these fields:

+-------------------+-------------------------------------------------------+
| *Name*            | *Description*                                         |
+-------------------+-------------------------------------------------------+
| ``name``          | Bytestring counter name                               |
+-------------------+-------------------------------------------------------+
| ``value``         | Integer value                                         |
+-------------------+-------------------------------------------------------+

Encodings
---------

All encodings ever used by :py:class:`Store` are kept persistently so the user
need not manually allocate prefixes, potentially in several places spanning
multiple languages. Additionally since the encoding name is stored, a
meaningful diagnostic can be printed if attempts are made to access records
encoded with an unregistered encoder.

Encoding metadata starts with ``<prefix>\x02``. The remainder of the key is an
encoded string representing the encoding or compressor name.

The value is a ``KEY_ENCODER``-encoded tuple of these fields:

+-------------------+-------------------------------------------------------+
| *Name*            | *Description*                                         |
+-------------------+-------------------------------------------------------+
| ``name``          | Bytestring encoding/compressor name                   |
+-------------------+-------------------------------------------------------+
| ``idx``           | Integer compressor index, used to form value prefix   |
+-------------------+-------------------------------------------------------+

Compressor value prefixes are formed simply by encoding the index using
:py:func:`pack_int`. The index itself is assigned by a call to
:py:meth:`Store.count` using the special name ``'\x00encodings_idx'``.

The following entries are assumed to exist, but are never physically written to
the storage engine:

+-------------------+---------+---------------------------------------------+
| ``name``          | ``idx`` | *Description*                               |
+-------------------+---------+---------------------------------------------+
| ``key``           | 1       | Built-in ``KEY_ENCODER``                    |
+-------------------+---------+---------------------------------------------+
| ``pickle``        | 2       | Built-in ``PICKLE_ENCODER``                 |
+-------------------+---------+---------------------------------------------+
| ``plain``         | 3       | Built-in ``PLAIN_PACKER`` (raw bytes)       |
+-------------------+---------+---------------------------------------------+
| ``zlib``          | 4       | Built-in ``ZLIB_PACKER``                    |
+-------------------+---------+---------------------------------------------+
