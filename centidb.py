
import cPickle as pickle
import cStringIO
import collections
import functools
import itertools
import operator
import re
import struct
import time
import warnings
import zlib

import plyvel

CollInfo = collections.namedtuple('CollInfo', 'name idx index_for')

KIND_NULL = chr(15)
KIND_NEG_INTEGER = chr(20)
KIND_INTEGER = chr(21)
KIND_BOOL = chr(30)
KIND_BLOB = chr(40)
KIND_TEXT = chr(50)
KIND_KEY = chr(100)
KIND_SEP = chr(101)

class Key(str):
    pass

def encode_int(v):
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
                assert o == '\x02'
                io.write('\x01')
        else:
            io.write(c)

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

    A string is returned such that elements of different types at the same
    position within two distinct sequences with otherwise identical prefixes
    will sort in the following order.

        1. ``None``
        2. Negative integers
        3. Positive integers
        4. ``False``
        5. ``True``
        6. Bytestrings (i.e. ``str()``).
        7. Unicode strings.
        8. Encoded keys (i.e. ``Key()``).
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
            elif isinstance(arg, Key):
                w(KIND_KEY)
                w(encode_str(arg))
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
        elif c == KIND_KEY:
            arg = Key(decode_str(getc))
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

class Encoder:
    """This class represents an encoding format, including its associated name.

        `name`:
            ASCII string uniquely identifying the encoding. A future version
            may use this to verify the encoding matches what was used to create
            `Collection`.

        `loads`:
            Function to deserialize an encoded value. The function is called
            with **a buffer object containing the encoded bytestring** as its
            sole argument, and should return the decoded value. If your encoder
            does not support `buffer()` objects (many C extensions do), then
            pass the value through `str()`.

        `dumps`:
            Function to serialize a value. The function is called with the
            value as its sole argument, and should return the encoded
            bytestring.
    """
    def __init__(self, name, loads, dumps):
        self.loads = loads
        self.dumps = dumps
        self.name = name

#: Encode Python tuples using encode_keys()/decode_keys().
KEY_ENCODER = Encoder('key', lambda s: decode_keys(s, first=True),
                             lambda o: encode_keys((o,)))

#: Encode Python objects using the cPickle version 2 protocol."""
PICKLE_ENCODER = Encoder('pickle', pickle.loads,
                         functools.partial(pickle.dumps, protocol=2))

class Index:
    """Provides query and manipulation access to a single index on a
    Collection. You should not create this class directly, instead use
    `Collection.add_index()` and the `Collection.indices` attribute.
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
        key = encode_keys((args or (),), self.prefix, closed)
        it = self.db.iterator(prefix=key, reverse=reverse, include_value=False)
        if max is not None:
            it = itertools.islice(it, max)
        it = itertools.imap(self._decode, it)
        return it if _lst else itertools.imap(tuple, it)

    def itertups(self, args=None, reverse=None, txn=None, max=None,
            closed=True):
        return itertools.imap(operator.itemgetter(0),
                              self.iterpairs(args, reverse, txn, max))

    def iterkeys(self, args=None, reverse=None, txn=None, max=None,
            closed=True):
        return itertools.imap(operator.itemgetter(1),
                              self.iterpairs(args, reverse, txn, max))

    def iteritems(self, args=None, reverse=False, txn=None, rec=None,
            max=None, closed=True):
        for idx_key, key in \
                self.iterpairs(args, reverse, txn, max, closed, _lst=True):
            obj = self.coll.get(key, txn=txn, rec=rec)
            if obj:
                yield idx_key, obj
            else:
                warnings.warn('stale entry in %r, requires rebuild')

    def itervalues(self, args=None, reverse=None, txn=None, rec=None,
            max=None, closed=True):
        return itertools.imap(operator.itemgetter(1),
            self.iteritems(args, reverse, txn, rec, max, closed))

    def get(self, args=None, reverse=None, txn=None, rec=None, closed=True):
        for p in self.iteritems(args, reverse, txn, rec, 1, closed):
            return p[1]

class Record:
    """Wraps a record value with its last saved key, if any.

    This is primarily used to track index keys that were valid for the record
    when it was loaded, allowing many operations to be avoided if the user
    deletes or modifies it within the same transaction. Use of the class is
    only required when modifying existing records.

    It is possible to avoid using the class when `Collection.derived_keys =
    True`, however this hurts perfomance as it forces `put()` to first check
    for any existing record with the same key, and therefore for any existing
    index keys that must first be deleted.

    **Warning**: you may create instances of this class directly, **but you
    must not** modify its attributes (except `data`), or construct it using any
    parameters except `coll` and `data`, otherwise index corruption will likely
    occur.
    """
    def __init__(self, coll, data, _key=None, _batch=False,
            _txn_id=None, _index_keys=None):
        #: Collection this record belongs to.
        self.coll = coll
        #: The actual record value.
        self.data = data
        #: Key for this record when it was last saved, or None if the record is
        #: deleted or has never been saved.
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

class Collection:
    """Provides access to a record collection contained within a Store, and
    ensures associated indices update consistently when the changes are made.

        `store`:
            Store the collection belongs to. If metadata for the collection
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
            If True, indicates the key function derives the key purely from the
            record value, and should be invoked for each change. If the key
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
            Specifies the value encoder instance to use; see the `KeyEncoder`
            class for an interface specification. If unspecified, defaults to
            `PICKLE_ENCODER`, which assumes record values are any pickleable
            Python object.

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
            derived_keys=False, encoder=None, _idx=None,
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
            counter_name = counter_name or ('key_counter:%s' % info.name)
            counter_prefix = counter_prefix or ()
            txn_key_func = lambda txn, _: \
                (counter_prefix + (store.count(counter_name, txn=txn),))
            derived_keys = False
        self.key_func = key_func
        self.txn_key_func = txn_key_func
        self.derived_keys = derived_keys
        self.encoder = encoder or PICKLE_ENCODER
        #: Dict mapping indices added using ``add_index()`` to `Index`
        #: instances representing them. Example:
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

                # Use default auto-increment key and PICKLE_ENCODER.
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

    def iteritems(self, key, rec=False):
        """Yield all `(key, value)` tuples in the collection, in key order. If
        `rec` is ``True``, `Record` instances are yielded instead of record
        values."""
        key = tuplize(key)
        it = self.db.iterator(start=encode_keys((key,), self.prefix))
        for phys, data in it:
            if not phys.startswith(self.prefix):
                break
            keys = decode_keys(phys, self.prefix)
            obj = self.encoder.loads(self._decompress(data))
            if rec:
                obj = Record(self, obj, keys[-1], len(keys) > 1)
            yield keys[-(1 + i)], obj

    def itervalues(self, key, rec=False):
        """Yield all values in the collection, in key order. If `rec` is
        ``True``, `Record` instances are yielded instead of record values."""
        return itertools.imap(operator.itemgetter(1), self.iteritems(key, rec))

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
            obj = self.encoder.loads(self._decompress(data))
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
            return tuplize(self.txn_key_func(rec.data, txn))
        return tuplize(self.key_func(rec.data))

    def put(self, rec, txn=None, key=None):
        """Create or overwrite a record.

            `rec`:
                The value to put; may either be a value recognised by the
                collection's `encoder` or a `Record` instance, such as returned
                by ``get(..., rec=True)``. It is strongly advised to prefer use
                of `Record` instances during read-modify-write transactions as
                it allows ``put()`` to avoid many database operations.

            `txn`:
                Transaction to use, or None to indicate the default behaviour
                of the associated `Store`.

            `key`:
                If specified, overrides the use of collection's key function
                and forces a specific key. Use with caution.
        """
        if not isinstance(rec, Record):
            rec = Record(self, rec)
        obj_key = key or self._reassign_key(rec, txn)
        index_keys = self._index_keys(obj_key, rec.data)

        if rec.key:
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
            # Old key might already exist, so delete it.
            self.delete(obj_key)

        put = (txn or self.db).put
        put(encode_keys((obj_key,), self.prefix),
            ' ' + self.encoder.dumps(rec.data))
        for index_key in index_keys:
            put(index_key, '')
        rec.key = obj_key
        rec.index_keys = index_keys
        return rec

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
        assert self.derived_keys, \
            "Attempt to delete() by value when using non-derived keys."
        return self.delete(self.key_func(val), txn)

class Store:
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

    def count(self, name, n=1, init=1):
        rec = self._counter_coll.get(name, default=(name, init), rec=True)
        val = rec.data[1]
        rec.data = (name, val + n)
        self._counter_coll.put(rec)
        return val
