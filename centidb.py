
import cPickle as pickle
import cStringIO
import collections
import functools
import itertools
import re
import struct
import time
import warnings
import zlib

import plyvel

CollInfo = collections.namedtuple('CollInfo', 'name idx index_for')

KIND_NULL = chr(10)
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

def encode_tuples(tups, prefix=''):
    io = cStringIO.StringIO()
    w = io.write
    w(prefix)
    for i, tup in enumerate(tups):
        if i:
            w(KIND_SEP)
        for arg in tuplize(tup):
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
                w('\x00')
            elif isinstance(arg, unicode):
                w(KIND_TEXT)
                w(encode_str(arg.encode('utf-8')))
                w('\x00')
            else:
                raise TypeError('unsupported type: %r' % (arg,))
    return io.getvalue()

def encode_tuple(tup, prefix=''):
    return encode_tuples((tup,), prefix)

def decode_tuples(s, prefix=None, first=False):
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
            tups.append(tuple(tup))
            if first:
                return tups[0]
            tup = []
            continue
        else:
            raise ValueError('bad kind %r; key corrupt? %r' % (ord(c), tup))
        tup.append(arg)
    tups.append(tuple(tup))
    return tups

def now():
    return int(time.time() * 1e6)

def make_counter_key(store, name, prefix=None):
    """
    Return a key function that assigns new keys transactionally using a counter
    named ``("key_counter:" + name)``.
    """
    name = 'key_counter:%s' % name
    prefix = tuplize(prefix or ())
    def counter_key_func(obj, txn):
        return prefix + (coll.store.count(name, txn=txn),)

class TupleEncoder:
    """Encode Python tuples using encode_tuple(). Note that the tuple format
    does not support batch value serialization.
    """
    def loads_many(self, s):
        """Decode the serialized tuple."""
        return decode_tuples(s)

    def dumps_many(self, lst):
        """Serialize a list containing one tuple."""
        return encode_tuples(lst)

    def __eq__(self, other):
        """Return True if the other object is a TupleEncoder."""
        return isinstance(other, TupleEncoder)

class PickleEncoder:
    """Encode Python objects using the cPickle version 2 protocol."""
    def loads_many(self, s):
        """Decode the serialized batch."""
        return pickle.loads(s)

    def dumps_many(self, lst):
        """Encode a batch of objects."""
        return pickle.dumps(lst, 2)

class Index:
    def __init__(self, coll, info, func):
        self.coll = coll
        self.store = coll.store
        self.db = self.store.db
        self.info = info
        self.func = func
        self.prefix = self.store.prefix + encode_int(info.idx)
        self._decode = functools.partial(decode_tuples, prefix=self.prefix)

    def iterpairs(self, args=None, reverse=False, txn=None, max=None,
            _lst=False):
        key = encode_tuple(args or (), self.prefix)
        it = self.db.iterator(prefix=key, reverse=reverse, include_value=False)
        if max is not None:
            it = itertools.islice(it, max)
        it = itertools.imap(self._decode, it)
        return it if _lst else itertools.imap(tuple, it)

    def itervalues(self, args=None, reverse=False, txn=None, rec=False,
            max=None):
        for idx_key, key in self.iterpairs(args, reverse, txn, max, _lst=True):
            obj = self.coll.get(key, txn=txn, rec=rec)
            if obj:
                yield obj
            else:
                warnings.warn('stale entry in %r, requires rebuild')

    def get(self, args=(), reverse=True):
        return next(self.iter(args, reverse=reverse), None)

class Record:
    def __init__(self, coll, data, key=None, batch=False,
            index_keys=None):
        self.coll = coll
        self.data = data
        self.key = key
        self.batch = batch
        if key and index_keys is None:
            index_keys = coll.index_keys(key, data)
        self.index_keys = index_keys

    def __repr__(self):
        tups = ','.join(map(repr, self.key or ()))
        return '<Record %s:(%s) %r>' % (self.coll.info.name, tups, self.data)

class Collection:
    """Provides access to a record collection contained within a Store, and
    ensures associated indices are updated consistently when the collection
    changes.

    `key_func`: Primary key function
        If given, must be a function accepting `(existing_key, obj)` arguments
        and is expected to return a new primary key for `obj`.

        The first argument is primary key already assigned to the record, or
        ``None`` if it has never been saved.
    """
    def __init__(self, store, name, key_func=None, txn_key_func=None,
            derived_keys=False, encoder=None, _idx=None):
        """Create or open a collection contained within `store`.
            lol :)
        lol is strong today
        `key_func`:  blerp
        `encoder`: zerpo

        zerpalot
        """
        self.store = store
        self.db = store.db
        if _idx is not None:
            self.info = CollInfo(name, _idx, None)
        else:
            self.info = store._get_info(name, idx=_idx)
        self.prefix = store.prefix + encode_int(self.info.idx)
        self.indices = {}
        self.key_func = key_func
        self.txn_key_func = txn_key_func
        if not (key_func or txn_key_func):
            self.key_func = self._time_key
        self.derived_keys = derived_keys
        self.encoder = encoder or PickleEncoder()

    def _time_key(self, obj):
        return now()

    def add_index(self, name, func):
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

    def phys_keys(self):
        it = self.db.iterator(prefix=self.prefix, include_value=False)
        return (decode_tuples(phys, self.prefix, first=True) for phys in it)

    def index_keys(self, key, obj):
        idx_keys = []
        for idx in self.indices.itervalues():
            lst = idx.func(obj)
            for idx_key in lst if type(lst) is list else [lst]:
                idx_keys.append(encode_tuples((idx_key, key), idx.prefix))
        return idx_keys

    def values(self, rec=True):
        """Return an iterator that yields all records from the collection. If
        `rec` is ``True``, return `Record` instances, otherwise only the
        record's value is returned."""
        for phys, data in self.db.iterator(prefix=self.prefix):
            if not phys.startswith(self.prefix):
                break
            keys = decode_tuples(phys, self.prefix)
            for i, obj in enumerate(self.encoder.loads_many(self._decompress(data))):
                if rec:
                    obj = Record(self, obj, keys[-(i + 1)], len(keys) > 1)
                yield obj

    def iter(self, key, rec=True):
        key = tuplize(key)
        it = self.db.iterator(start=encode_tuple(key, self.prefix))
        for phys, data in it:
            if not phys.startswith(self.prefix):
                break
            keys = decode_tuples(phys, self.prefix)
            for i, obj in enumerate(self.encoder.loads_many(self._decompress(data))):
                if rec:
                    obj = Record(self, obj, keys[-(1 + i)], len(keys) > 1)
                yield keys[-(1 + i)], obj

    def get(self, key, default=None, rec=False, txn=None):
        """Fetch a record given its key. If `key` is not a tuple, it is wrapped
        in a 1-tuple. If the record does not exist, return ``None`` or if
        `default` is provided, return it instead. If `rec` is ``True``, return
        a `Record` instance for use when later re-saving the record, otherwise
        only the record's value is returned."""
        key = tuplize(key)
        it = (txn or self.db).iterator(start=encode_tuple(key, self.prefix))
        phys, data = next(it, (None, None))

        if phys and phys.startswith(self.prefix):
            keys = decode_tuples(phys, self.prefix)
            for i, obj in enumerate(self.encoder.loads_many(self._decompress(data))):
                if keys[-(1 + i)] == key:
                    return Record(self, obj, key, len(keys)>1) if rec else obj
        if default is not None:
            return Record(self, default) if rec else default

    def _split_batch(self, rec, txn):
        assert rec.key and rec.batch
        it = self.db.iterator(
            start=encode_tuple(rec.key, self.prefix))
        phys, data = next(it, (None, None))
        keys = decode_tuples(phys, self.prefix)
        assert len(keys) > 1 and rec.key in keys, \
            'Physical key missing: %r' % (rec.key,)

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
        if not isinstance(rec, Record):
            rec = Record(self, rec)
        obj_key = key or self._reassign_key(rec, txn)
        index_keys = self.index_keys(obj_key, rec.data)

        if rec.key:
            delete = (txn or self.db).delete
            if rec.batch:
                # Old key was part of a batch, explode the batch.
                self._split_batch(rec, txn)
            elif rec.key != obj_key:
                # New version has changed key, delete old.
                delete(encode_tuple(rec.key, self.prefix))
            if index_keys != rec.index_keys:
                for index_key in rec.index_keys or ():
                    delete(index_key)
        else:
            # Old key might already exist, so delete it.
            self.delete(obj_key)

        put = (txn or self.db).put
        put(encode_tuple(obj_key, self.prefix),
            ' ' + self.encoder.dumps_many([rec.data]))
        for index_key in index_keys:
            put(index_key, '')
        rec.key = obj_key
        rec.index_keys = index_keys
        return rec

    def delete(self, obj, txn=None):
        if isinstance(obj, tuple):
            rec = self.get(obj, rec=True)
        elif isinstance(obj, Record):
            rec = obj
        else:
            rec = Record(self, obj)
        if rec:
            rec_key = rec.key or self._reassign_key(rec, txn)
            if rec.batch:
                self._split_batch(rec, txn)
            else:
                delete = (txn or self.db).delete
                delete(encode_tuple(rec_key, self.prefix))
                for index_key in rec.index_keys or ():
                    delete(index_key)
            rec.key = None
            rec.batch = False
            rec.index_keys = None
            return rec

class Store:
    def __init__(self, db, prefix=''):
        self.db = db
        self.prefix = prefix
        self._info_coll = Collection(self, '\x00collections', _idx=0,
            encoder=TupleEncoder(), key_func=lambda tup: tup[0])
        self._counter_coll = Collection(self, '\x00counters', _idx=1,
            encoder=TupleEncoder(), key_func=lambda tup: tup[0])

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
