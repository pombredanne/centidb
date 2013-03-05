#
# Copyright 2013, David Wilson.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Minimalist object DBMS for Python

See http://centidb.readthedocs.org/
"""

from __future__ import absolute_import

import cPickle as pickle
import cStringIO
import collections
import functools
import itertools
import operator
import re
import struct
import time
import uuid
import warnings
import zlib

import plyvel

__all__ = '''invert Store Collection Record Index decode_keys encode_keys
    decode_int encode_int Encoder KEY_ENCODER PICKLE_ENCODER
    ZLIB_ENCODER'''.split()

CollInfo = collections.namedtuple('CollInfo', 'name idx index_for')

KIND_NULL = chr(15)
KIND_NEG_INTEGER = chr(20)
KIND_INTEGER = chr(21)
KIND_BOOL = chr(30)
KIND_BLOB = chr(40)
KIND_TEXT = chr(50)
KIND_UUID = chr(90)
KIND_KEY = chr(95)
KIND_SEP = chr(102)

INVERT_TBL = ''.join(chr(c ^ 0xff) for c in xrange(256))

def invert(s):
    """Invert the bits in the bytestring `s`.

    This is used to achieve a descending order for blobs and strings when they
    are part of a compound key, however when they are stored as a 1-tuple, it
    is probably better to simply the corresponding `Collection` or `Index` with
    ``reverse=True``.
    """
    return s.translate(INVERT_TBL)

def encode_int(v):
    """Given some positive integer of 64-bits or less, return a variable length
    bytestring representation that preserves the integer's order. The
    bytestring size is such that:

        +-------------+------------------------+
        + *Size*      | *Largest integer*      |
        +-------------+------------------------+
        + 1 byte      | <= 240                 |
        +-------------+------------------------+
        + 2 bytes     | <= 2287                |
        +-------------+------------------------+
        + 3 bytes     | <= 67823               |
        +-------------+------------------------+
        + 4 bytes     | <= 16777215            |
        +-------------+------------------------+
        + 5 bytes     | <= 4294967295          |
        +-------------+------------------------+
        + 6 bytes     | <= 1099511627775       |
        +-------------+------------------------+
        + 7 bytes     | <= 281474976710655     |
        +-------------+------------------------+
        + 8 bytes     | <= 72057594037927935   |
        +-------------+------------------------+
        + 9 bytes     | <= (2**64)-1           |
        +-------------+------------------------+
    """
    if v < 240:
        return chr(v)
    elif v <= 2287:
        v -= 240
        d, m = divmod(v, 256)
        return chr(241 + d) + chr(m)
    elif v <= 67823:
        v -= 2288
        d, m = divmod(v, 256)
        return '\xf9' + chr(d) + chr(m)
    elif v <= 16777215:
        return '\xfa' + struct.pack('>L', v)[-3:]
    elif v <= 4294967295:
        return '\xfb' + struct.pack('>L', v)
    elif v <= 1099511627775:
        return '\xfc' + struct.pack('>Q', v)[-5:]
    elif v <= 281474976710655:
        return '\xfd' + struct.pack('>Q', v)[-6:]
    elif v <= 72057594037927935:
        return '\xfe' + struct.pack('>Q', v)[-7:]
    else:
        assert v.bit_length() <= 64
        return '\xff' + struct.pack('>Q', v)

def decode_int(getc, read):
    """Decode and return an integer encoded by `encode_int()`.

    `get`:
        Function that returns the next byte of input.
    `read`:
        Function accepting a byte count and returning that many bytes of input.

    ::

        io = cStringIO.StringIO(encoded_int)
        i = decode_int(lambda: io.read(1), io.read)
        # io.tell() is now positioned one byte past end of integer.
    """
    c = getc()
    o = ord(c)
    if o <= 240:
        return o
    elif o <= 248:
        c2 = getc()
        o2 = ord(c2)
        return 240 + (256 * (o - 241) + o2)
    elif o == 249:
        return 2288 + (256*ord(getc())) + ord(getc())
    elif o == 250:
        return struct.unpack('>L', '\x00' + read(3))[0]
    elif o == 251:
        return struct.unpack('>L', read(4))[0]
    elif o == 252:
        return struct.unpack('>Q', '\x00\x00\x00' + read(5))[0]
    elif o == 253:
        return struct.unpack('>Q', '\x00\x00' + read(6))[0]
    elif o == 254:
        return struct.unpack('>Q', '\x00' + read(7))[0]
    elif o == 255:
        return struct.unpack('>Q', read(8))[0]

def encode_str(s):
    subber = lambda m: '\x01\x01' if m.group(0) == '\x00' else '\x01\x02'
    return re.sub(r'[\x00\x01]', subber, s)

def decode_str(getc):
    io = cStringIO.StringIO()
    while True:
        c = getc()
        if c in '\x00': # matches '' or '\x00'
            return io.getvalue()
        elif c == '\x01':
            c = getc()
            if c == '\x01':
                io.write('\x00')
            else:
                assert c == '\x02'
                io.write('\x01')
        else:
            io.write(c)

def _eat(pred, it, total_only=False):
    if not eat:
        return it
    total = 0
    true = 0
    for elem in it:
        total += 1
        true += elem is not None
    if total_only:
        return total
    return total, true

def tuplize(o):
    return o if isinstance(o, tuple) else (o,)

def encode_keys(tups, prefix='', closed=True):
    """Encode a sequence of tuples of primitive values to a bytestring that
    preserves a meaningful lexicographical sort order.

        `prefix`:
            Initial prefix for the bytestring, if any.

        `closed`:
            If ``False``, indicates that if the last element of the last tuple
            is a string or blob, its terminator should be omitted. This allows
            open-ended queries on substrings:

            ::

                a_open = encode_keys('a', closed=False) # 0x28 0x61
                a_closed = encode_keys('a')             # 0x28 0x61 0x00
                aa = encode_keys('aa')                  # 0x28 0x61 0x61 0x00
                assert not aa.startswith(a_closed)
                assert aa.startswith(a_open)

    A bytestring is returned such that elements of different types at the same
    position within distinct sequences with otherwise identical prefixes will
    sort in the following order.

        1. ``None``
        2. Negative integers
        3. Positive integers
        4. ``False``
        5. ``True``
        6. Bytestrings (i.e. ``str()``).
        7. Unicode strings.
        8. ``uuid.UUID`` instances.
        9. Sequences with another tuple following the last identical element.
    """
    io = cStringIO.StringIO()
    w = io.write
    w(prefix)
    last = len(tups) - 1
    for i, tup in enumerate(tups):
        if i:
            w(KIND_SEP)
        tup = tuplize(tup)
        tlast = len(tup) - 1
        for j, arg in enumerate(tup):
            if arg is None:
                w(KIND_NULL)
            elif isinstance(arg, bool):
                w(KIND_BOOL)
                w(encode_int(arg))
            elif isinstance(arg, (int, long)):
                if arg < 0:
                    w(KIND_NEG_INTEGER)
                    w(encode_int(-arg))
                else:
                    w(KIND_INTEGER)
                    w(encode_int(arg))
            elif isinstance(arg, uuid.UUID):
                w(KIND_UUID)
                w(encode_str(arg.get_bytes()))
            elif isinstance(arg, str):
                w(KIND_BLOB)
                w(encode_str(arg))
                if closed or i != last or j != tlast:
                    w('\x00')
            elif isinstance(arg, unicode):
                w(KIND_TEXT)
                w(encode_str(arg.encode('utf-8')))
                if closed or i != last or j != tlast:
                    w('\x00')
            else:
                raise TypeError('unsupported type: %r' % (arg,))
    return io.getvalue()

def decode_keys(s, prefix=None, first=False):
    """Decode a bytestring produced by `encode_keys()`, returning the list of
    tuples the string represents.

        `prefix`:
            If specified, a string prefix of this length will be skipped before
            decoding begins. A future version may also verify the prefix
            matches.

        `first`:
            Stop work after the first tuple has been decoded and return it
            immediately. Note the return value is the tuple, not a list
            containing the tuple.
    """
    if prefix:
        s = buffer(s, len(prefix))
    io = cStringIO.StringIO(s)
    getc = functools.partial(io.read, 1)
    tups = []
    tup = []
    for c in iter(getc, ''):
        if c == KIND_NULL:
            arg = None
        elif c == KIND_INTEGER:
            arg = decode_int(getc, io.read)
        elif c == KIND_NEG_INTEGER:
            arg = -decode_int(getc, io.read)
        elif c == KIND_BOOL:
            arg = bool(decode_int(getc, io.read))
        elif c == KIND_BLOB:
            arg = decode_str(getc)
        elif c == KIND_TEXT:
            arg = decode_str(getc).decode('utf-8')
        elif c == KIND_UUID:
            arg = uuid.UUID(decode_str(getc))
        elif c == KIND_SEP:
            if tup:
                tups.append(tuple(tup))
            if first:
                return tups[0]
            tup = []
            continue
        else:
            raise ValueError('bad kind %r; key corrupt? %r' % (ord(c), tup))
        tup.append(arg)
    tups.append(tuple(tup))
    return tups[0] if first else tups

class Encoder(object):
    """Instances of this class represents an encoding.

        `name`:
            ASCII string uniquely identifying the encoding. A future version
            may use this to verify the encoding matches what was used to create
            the `Collection`. For encodings used as compressors, this name is
            persisted forever in `Store`'s metadata after first use.

        `unpack`:
            Function to deserialize an encoded value. It may be called with **a
            buffer object containing the encoded bytestring** as its argument,
            and should return the decoded value. If your encoder does not
            support `buffer()` objects (many C extensions do), then convert the
            buffer using `str()`.

        `pack`:
            Function to serialize a value. It is called with the value as its
            sole argument, and should return the encoded bytestring.
    """
    def __init__(self, name, unpack, pack):
        vars(self).update(locals())

#: Encode Python tuples using encode_keys()/decode_keys().
KEY_ENCODER = Encoder('key', lambda s: decode_keys(s, first=True),
                             lambda o: encode_keys((o,)))

#: Encode Python objects using the cPickle version 2 protocol."""
PICKLE_ENCODER = Encoder('pickle', pickle.loads,
                         functools.partial(pickle.dumps, protocol=2))

#: Compress bytestrings using zlib.compress()/zlib.decompress().
ZLIB_ENCODER = Encoder('zlib', zlib.compress, zlib.decompress)

class Index(object):
    """Provides query and manipulation access to a single index on a
    Collection. You should not create this class directly, instead use
    `Collection.add_index()` and the `Collection.indices` mapping.

    `Index.get()` and the iteration methods take a common set of parameters
    that are described below:

        `args`:
            Prefix of the index entries to to be matched, or ``None`` or the
            empty tuple to indicate all index entries should be matched.

        `reverse`:
            If ``True``, iteration should begin with the last naturally ordered
            match returned first, and end with the first naturally ordered
            match returned last.

        `txn`:
            Transaction to use, or ``None`` to indicate the default behaviour
            of the associated `Store`.
    """
    def __init__(self, coll, info, func):
        self.coll = coll
        self.store = coll.store
        self.db = self.store.db
        self.info = info
        self.func = func
        self.prefix = self.store.prefix + encode_int(info.idx)
        self._decode = functools.partial(decode_keys, prefix=self.prefix)

    def iterpairs(self, args=None, reverse=None, txn=None, max=None,
            closed=True, _lst=False):
        """Yield all (tuple, key) pairs in the index, in tuple order. `tuple`
        is the tuple returned by the user's index function, and `key` is the
        key of the matching record."""
        key = encode_keys((args or (),), self.prefix, closed)
        it = self.db.iterator(prefix=key, reverse=reverse, include_value=False)
        if max is not None:
            it = itertools.islice(it, max)
        it = itertools.imap(self._decode, it)
        return it if _lst else itertools.imap(tuple, it)

    def itertups(self, args=None, reverse=None, txn=None, max=None,
            closed=True):
        """Yield all index tuples in the index, in tuple order. The index tuple
        is the part of the entry produced by the user's index function, i.e.
        the index's natural "value"."""
        return itertools.imap(operator.itemgetter(0),
                              self.iterpairs(args, reverse, txn, max))

    def iterkeys(self, args=None, reverse=None, txn=None, max=None,
            closed=True):
        """Yield all keys in the index, in tuple order."""
        return itertools.imap(operator.itemgetter(1),
                              self.iterpairs(args, reverse, txn, max))

    def iteritems(self, args=None, reverse=False, txn=None, rec=None,
            max=None, closed=True):
        """Yield all `(key, value)` items referred to by the index, in tuple
        order. If `rec` is ``True``, `Record` instances are yielded instead of
        record values."""
        for idx_key, key in \
                self.iterpairs(args, reverse, txn, max, closed, _lst=True):
            obj = self.coll.get(key, txn=txn, rec=rec)
            if obj:
                yield idx_key, obj
            else:
                warnings.warn('stale entry in %r, requires rebuild')

    def itervalues(self, args=None, reverse=None, txn=None, rec=None,
            max=None, closed=True):
        """Yield all values referred to by the index, in tuple order. If `rec`
        is ``True``, `Record` instances are yielded instead of record
        values."""
        return itertools.imap(operator.itemgetter(1),
            self.iteritems(args, reverse, txn, rec, max, closed))

    def gets(self, args, reverse=None, txn=None, rec=None, closed=True,
            default=None):
        """Yield `get(x)` for each `x` in the iterable `args`."""
        return (self.get(x, reverse, txn, rec, closed, default) for x in args)

    def get(self, args=None, reverse=None, txn=None, rec=None, closed=True,
            default=None):
        """Return the first matching values referred to by the index, in tuple
        order. If `rec` is ``True`` a `Record` instance is returned of the
        record value."""
        for p in self.iteritems(args, reverse, txn, rec, 1, closed):
            return p[1]
        if rec and default is not None:
            return Record(self.coll, default)
        return default

class Record(object):
    """Wraps a record value with its last saved key, if any.

    `Record` instances are usually created by the `Collection` and `Index`
    ``get()``/``put()``/``iter*()`` functions. They are primarily used to track
    index keys that were valid for the record when it was loaded, allowing many
    operations to be avoided if the user deletes or modifies it within the same
    transaction. The class is only required when modifying existing records.

    It is possible to avoid using the class when `Collection.derived_keys =
    True`, however this hurts perfomance as it forces `put()` to first check
    for any existing record with the same key, and therefore for any existing
    index keys that must first be deleted.

    *Note:* you may create `Record` instances directly, **but you must not
    modify any attributes except** `data`, or construct it using any parameters
    except `coll` and `data`, otherwise index corruption will likely occur.
    """
    def __init__(self, coll, data, _key=None, _batch=False,
            _txn_id=None, _index_keys=None):
        #: `Collection` this record belongs to. This is always reset after a
        #: successful `put()`.
        self.coll = coll
        #: The actual record value.
        self.data = data
        #: Key for this record when it was last saved, or ``None`` if the
        #: record is deleted or has never been saved.
        self.key = _key
        #: True if the record was loaded from a physical key that contained
        #: other records. Used internally to know when to explode batches
        #: during saves.
        self.batch = _batch
        #: Transaction ID this record was visible in. Used internally to
        #: ensure records from distinct transactions aren't mixed.
        self.txn_id = _txn_id
        if _key and _index_keys is None:
            _index_keys = coll._index_keys(_key, data)
        self.index_keys = _index_keys

    def __repr__(self):
        tups = ','.join(map(repr, self.key or ()))
        return '<Record %s:(%s) %r>' % (self.coll.info.name, tups, self.data)

class Collection(object):
    """Provides access to a record collection contained within a `Store`, and
    ensures associated indices update consistently when changes are made.

        `store`:
            `Store` the collection belongs to. If metadata for the collection
            does not already exist, it will be populated during construction.

        `name`:
            ASCII string used to identify the collection, aka. the key of the
            collection itself.

        `key_func`, `txn_key_func`:
            Key generator for records about to be saved. `key_func` takes one
            argument, the record's value, while `txn_key_func` also receives
            the active transaction, to allow transactionally assigning
            identifiers. If neither is given, keys are assigned using a
            transactional counter (like auto-increment in SQL). See
            `counter_name` and `counter_prefix`.

        `derived_keys`:
            If ``True``, indicates the key function derives the key from the
            record's value, and should be invoked for each change. If the key
            changes the previous key and index entries are automatically
            deleted.

            ::

                # Since names are used as keys, if a person record changes
                # name, its key must also change.
                coll = Collection(store, 'people',
                    key_func=lambda person: person['name'],
                    derived_keys=True)

            If ``False`` then record keys are preserved across saves, so long
            as `get(rec=True)` and `put(<Record instance>)` are used.

        `encoder`:
            `Encoding` used to serialize record values to bytestrings; defaults
            to `PICKLE_ENCODER`.

        `packer`:
            `Encoding` used to compress a group of serialized records as a
            unit. Used only if `packer=` isn't specified during `put()` or
            `batch()`; invocations with no `packer=` will be uncompressed if a
            default isn't given here.

        `counter_name`:
            Specifies the name of the `Store` counter to use when generating
            auto-incremented keys. If unspecified, defaults to
            ``"key_counter:<name>"``. Unused when `key_func` or `txn_key_func`
            are specified.

        `counter_prefix`:
            Optional tuple to prefix auto-incremented keys with. If
            unspecified, auto-incremented keys are a 1-tuple containing the
            counter value. Unused when `key_func` or `txn_key_func` are
            specified.
    """
    def __init__(self, store, name, key_func=None, txn_key_func=None,
            derived_keys=False, encoder=None, packer=None, _idx=None,
            counter_name=None, counter_prefix=None):
        """Create an instance; see class docstring."""
        self.store = store
        self.db = store.db
        if _idx is not None:
            self.info = CollInfo(name, _idx, None)
        else:
            self.info = store._get_info(name, idx=_idx)
        self.prefix = store.prefix + encode_int(self.info.idx)
        if not (key_func or txn_key_func):
            counter_name = counter_name or ('key_counter:%s' % self.info.name)
            counter_prefix = counter_prefix or ()
            txn_key_func = lambda txn, _: \
                (counter_prefix + (store.count(counter_name, txn=txn),))
            derived_keys = False
        self.key_func = key_func
        self.txn_key_func = txn_key_func
        self.derived_keys = derived_keys
        self.encoder = encoder or PICKLE_ENCODER
        self.packer = packer
        #: Dict mapping indices added using ``add_index()`` to `Index`
        #: instances representing them.
        #:
        #: ::
        #:
        #:      idx = coll.add_index('some index', lambda v: v[0])
        #:      assert coll.indices['some index'] is idx
        self.indices = {}

    def add_index(self, name, func):
        """Associate an index with the collection. The index metadata will be
        created in the associated `Store` it it does not exist. Returns the
        `Index` instance describing the index. `add_index()` may only be
        invoked once for each unique `name` for each collection.

        *Note:* only index metadata is persistent. You must invoke
        `add_index()` with the same arguments every time you create a
        `Collection` instance.

        `name`:
            ASCII name for the index.

        `func`:
            Index key generation function accepting one argument, the record
            value. It should return a single primitive value, a tuple of
            primitive values, a list of primitive values, or a list of tuples
            of primitive values.

            `Note:` the index function must have no side-effects. Example:

            ::

                coll = Collection(store, 'people')
                coll.add_index('name', lambda person: person['name'])

                coll.put({'name': 'David'})
                coll.put({'name': 'Charles'})
                coll.put({'name': 'Charles'})
                coll.put({'name': 'Andrew'})

                it = coll.indices['name'].iterpairs()
                assert list(it) == [
                    (('Andrew',),   (4,)),
                    (('Charles',),  (2,)),
                    (('Charles',),  (3,)),
                    (('David',),    (1,))
                ]
        """
        assert name not in self.indices
        info_name = 'index:%s:%s' % (self.info.name, name)
        info = self.store._get_info(info_name, index_for=self.info.name)
        index = Index(self, info, func)
        self.indices[name] = index
        return index

    def _decompress(self, s):
        if s.startswith('Z'):
            return zlib.decompress(buffer(s, 1))
        return s[1:]

    def iter_phys_keys(self):
        """Yield lists of tuples representing all the physical keys that exist
        in the collection, in key order. A physical key is simply a sequence of
        logical keys. When the length of the yielded list is >1, this indicates
        multiple logical keys saved to the same physical key."""
        it = self.db.iterator(prefix=self.prefix, include_value=False)
        return (decode_keys(phys, self.prefix)[::-1] for phys in it)

    def _index_keys(self, key, obj):
        idx_keys = []
        for idx in self.indices.itervalues():
            lst = idx.func(obj)
            for idx_key in lst if type(lst) is list else [lst]:
                idx_keys.append(encode_keys((idx_key, key), idx.prefix))
        return idx_keys

    def iteritems(self, key=(), rec=False):
        """Yield all `(key, value)` tuples in the collection, in key order. If
        `rec` is ``True``, `Record` instances are yielded instead of record
        values."""
        key = tuplize(key)
        it = self.db.iterator(start=encode_keys((key,), self.prefix))
        for phys, data in it:
            if not phys.startswith(self.prefix):
                break
            keys = decode_keys(phys, self.prefix)
            obj = self.encoder.unpack(self._decompress(data))
            if rec:
                obj = Record(self, obj, keys[-1], len(keys) > 1)
            yield keys[-(1)], obj

    def itervalues(self, key, rec=False):
        """Yield all values in the collection, in key order. If `rec` is
        ``True``, `Record` instances are yielded instead of record values."""
        return itertools.imap(operator.itemgetter(1), self.iteritems(key, rec))

    def gets(self, keys, default=None, rec=False, txn=None):
        """Yield `get(k)` for each `k` in the iterable `keys`."""
        return (self.get(x, default, rec, txn) for k in keys)

    def get(self, key, default=None, rec=False, txn=None):
        """Fetch a record given its key. If `key` is not a tuple, it is wrapped
        in a 1-tuple. If the record does not exist, return ``None`` or if
        `default` is provided, return it instead. If `rec` is ``True``, return
        a `Record` instance for use when later re-saving the record, otherwise
        only the record's value is returned."""
        key = tuplize(key)
        it = (txn or self.db).iterator(start=encode_keys((key,), self.prefix))
        phys, data = next(it, (None, None))

        if phys and phys.startswith(self.prefix):
            keys = decode_keys(phys, self.prefix)
            obj = self.encoder.unpack(self._decompress(data))
            if keys[-1] == key:
                return Record(self, obj, key, len(keys)>1) if rec else obj
        if default is not None:
            return Record(self, default) if rec else default

    def _split_batch(self, rec, txn):
        assert rec.key and rec.batch
        it = self.db.iterator(
            start=encode_keys((rec.key,), self.prefix))
        phys, data = next(it, (None, None))
        keys = decode_keys(phys, self.prefix)
        assert len(keys) > 1 and rec.key in keys, \
            'Physical key missing: %r' % (rec.key,)

        assert 0
        objs = self.encoder.loads_many(self._decompress(data))
        for i, obj in enumerate(objs):
            if keys[-(1 + i)] != rec.key:
                self.put(Record(self, obj), txn, key=keys[-(1 + i)])
        (txn or self.db).delete(phys)
        rec.key = None
        rec.batch = False

    def _reassign_key(self, rec, txn):
        if rec.key and not self.derived_keys:
            return rec.key
        elif self.txn_key_func:
            return tuplize(self.txn_key_func(txn, rec.data))
        return tuplize(self.key_func(rec.data))

    def puts(self, recs, txn=None, packer=None, eat=True):
        """Invoke `put()` for each element in the iterable `recs`. If `eat` is
        ``True``, returns the number of items processed, otherwise returns an
        iterator that lazily calls `put()` and yields its return value."""
        return _eat(eat, (self.put(rec, txn, packer) for rec in recs), True)

    def putitems(self, it, txn=None, packer=None, eat=True):
        """Invoke `put(y, key=x)` for each (x, y) in the iterable `it`. If
        `eat` is ``True``, returns the number of items processed, otherwise
        returns an iterator that lazily calls `put()` and yields its return
        value."""
        return _eat(eat, (self.put(x, txn, packer, y) for x, y in it), True)

    def put(self, rec, txn=None, packer=None, key=None):
        """Create or overwrite a record.

            `rec`:
                The value to put; may either be a value recognised by the
                collection's `encoder` or a `Record` instance, such as returned
                by ``get(..., rec=True)``. It is strongly advised to prefer use
                of `Record` instances during read-modify-write transactions as
                it allows ``put()`` to avoid many database operations.

            `txn`:
                Transaction to use, or ``None`` to indicate the default
                behaviour of the associated `Store`.

            `packer`:
                Encoding to use to compress the value. Defaults to
                `Collection.packer`, or uncompressed if `Collection.packer` is
                ``None``.

            `key`:
                If specified, overrides the use of collection's key function
                and forces a specific key. Use with caution.
        """
        if not isinstance(rec, Record):
            rec = Record(self, rec)
        obj_key = key or self._reassign_key(rec, txn)
        index_keys = self._index_keys(obj_key, rec.data)

        if rec.coll == self and rec.key:
            delete = (txn or self.db).delete
            if rec.batch:
                # Old key was part of a batch, explode the batch.
                self._split_batch(rec, txn)
            elif rec.key != obj_key:
                # New version has changed key, delete old.
                delete(encode_keys((rec.key,), self.prefix))
            if index_keys != rec.index_keys:
                for index_key in rec.index_keys or ():
                    delete(index_key)
        else:
            # TODO: delete() may be unnecessary when no indices are defined
            # Old key might already exist, so delete it.
            self.delete(obj_key)

        put = (txn or self.db).put
        put(encode_keys((obj_key,), self.prefix),
            ' ' + self.encoder.pack(rec.data))
        for index_key in index_keys:
            put(index_key, '')
        rec.coll = self
        rec.key = obj_key
        rec.index_keys = index_keys
        return rec

    def deletes(self, objs, txn=None, eat=True):
        """Invoke `delete()` for each element in the iterable `objs`. If `eat`
        is ``True``, returns a tuple containing the number of keys processed,
        and the number of items deleted, otherwise returns an iterator that
        lazily calls `deletes()` and yields its return value.

        ::

            keys = request.form['names'].split(',')
            for rec in coll.deletes(key):
                if rec:
                    print '%(name)s was deleted.' % (rec.data,)

            # Summary version.
            keys, deleted = coll.deletes(request.form['names'].split(','))
            print 'Deleted %d names of %d provided.' % (deleted, keys)
        """
        return _eat(eat, (self.delete(obj) for obj in objs))

    def delete(self, obj, txn=None):
        """Delete a record by key or using a `Record` instance. The deleted
        record is returned if it existed.

        `obj`:
            Record to delete; may be a `Record` instance, or a tuple, or a
            primitive value.
        """
        if isinstance(obj, Record):
            rec = obj
        elif isinstance(obj, tuple):
            rec = self.get(obj, rec=True)
        if rec:
            rec_key = rec.key or self._reassign_key(rec, txn)
            if rec.batch:
                self._split_batch(rec, txn)
            else:
                delete = (txn or self.db).delete
                delete(encode_keys((rec_key,), self.prefix))
                for index_key in rec.index_keys or ():
                    delete(index_key)
            rec.key = None
            rec.batch = False
            rec.index_keys = None
            return rec

    def delete_values(self, vals, txn=None, eat=True):
        """Invoke `delete_value()` for each element in the iterable `vals`. If
        `eat` is ``True``, returns a tuple containing the number of keys
        processed, and the number of items deleted, otherwise returns an
        iterator that lazily calls `delete_value()` and yields its return
        value."""
        return _eat(eat, (self.delete_value(v) for v in vals))

    def delete_value(self, val, txn=None):
        """Delete a record value without knowing its key. The deleted record is
        returned, if it existed.

        `Note`: it is impossible (and does not make sense) to delete by value
        when ``derived_keys=False``, since the key function will generate an
        unrelated ID for the value. Example:

        ::

            coll = Collection(store, 'people',
                key_func=lambda person: person['name'],
                derived_keys=True)
            val = {"name": "David"}
            coll.put(val)
            # key_func will generate the correct key:
            call.delete_value(val)
        """
        assert self.derived_keys
        return self.delete(self.key_func(val), txn)

class Store(object):
    """Represents access to the underlying storage engine, and manages
    counters.

        `prefix`:
            Prefix for all keys used by any associated object (record, index,
            counter, metadata). This allows the storage engine's key space to
            be shared amongst several users.
    """
    def __init__(self, db, prefix=''):
        self.db = db
        self.prefix = prefix
        self._info_coll = Collection(self, '\x00collections', _idx=0,
            encoder=KEY_ENCODER, key_func=lambda tup: tup[0])
        self._counter_coll = Collection(self, '\x00counters', _idx=1,
            encoder=KEY_ENCODER, key_func=lambda tup: tup[0])

    def _get_info(self, name, idx=None, index_for=None):
        tup = self._info_coll.get(name)
        if tup:
            assert tup == (name, idx or tup[1], index_for)
            return CollInfo(*tup)
        if idx is None:
            idx = self.count('\x00collections_idx', init=10)
            info = CollInfo(name, idx, index_for)
        return self._info_coll.put(info).data

    def count(self, name, n=1, init=1, txn=None):
        """Increment a counter and return its previous value. The counter is
        created if it doesn't exist.

            `name`:
                Name of the counter.

            `n`:
                Number to add to the counter.

            `init`:
                Initial value to give counter if it doesn't exist.

            `txn`:
                Transaction to use, or ``None`` to indicate the default
                behaviour of the storage engine.
        """
        default = (name, init)
        rec = self._counter_coll.get(name, default, rec=True, txn=txn)
        val = rec.data[1]
        rec.data = (name, val + n)
        self._counter_coll.put(rec)
        return val
