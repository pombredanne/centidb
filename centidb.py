
import cPickle as pickle
import cStringIO
import collections
import functools
import re
import struct
import time
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

def encode_tuple(args, prefix=''):
    io = cStringIO.StringIO()
    w = io.write
    w(prefix)
    for arg in tuplize(args):
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

def decode_tuple(s, prefix=None, _lst=False):
    if prefix:
        s = buffer(s, len(prefix))
    io = cStringIO.StringIO(s)
    getc = functools.partial(io.read, 1)
    args = []
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
        else:
            raise ValueError('bad kind %r; key corrupt? %r' % (o, args))
        args.append(arg)
    return args if _lst else tuple(args)

class TupleEncoder:
    def loads_many(self, s):
        return [decode_tuple(s)]

    def dumps_many(self, lst):
        assert len(lst) == 1, 'TupleEncoder doesn\'t handle many'
        return encode_tuple(lst[0])

    def __eq__(self, other):
        return isinstance(other, TupleEncoder)

class PickleEncoder:
    def loads_many(self, s):
        return pickle.loads(s)

    def dumps_many(self, lst):
        return pickle.dumps(lst, 2)

class Index:
    def __init__(self, store, info, func):
        self.store = store
        self.db = store.db
        self.info = info
        self.prefix = encode_int(info.idx)
        self.func = func

    def index_keys(self, obj_key, obj):
        index_keys = []
        encoded = encode_tuple(obj_key)
        result = self.func(obj_key, obj)
        if not isinstance(result, list):
            result = [result]
        for args in result:
            args = tuplize(args) + (encoded,)
            index_keys.append(encode_tuple(args, self.prefix))
        return index_keys

    def delete_keys(self, obj_key, obj, wb=None):
        delete = (wb or self.db).delete
        for index_key in self.index_keys(obj_key, obj):
            delete(index_key)

    def add_keys(self, obj_key, obj, wb=None):
        put = (wb or self.db).put
        for index_key in self.index_keys(key, obj):
            put(index_key, '')

    def iter(self, args=(), reverse=False):
        key = encode_tuple(args, self.prefix)
        print 'prefix =', repr(key)
        it = self.db.iterator(prefix=key, reverse=reverse, include_value=False)
        for key in it:
            lst = decode_tuple(key, self.prefix, _lst=True)
            yield tuple(lst[:-1]), decode_tuple(lst[-1])

    def get(self, args=(), reverse=True):
        return next(self.iter(args, reverse=reverse), None)

class Record:
    def __init__(self, coll, data, load_key=None, phys_key=None,
            index_keys=None):
        self.coll = coll
        self.data = data
        self.load_key = load_key
        self.phys_key = phys_key
        self.index_keys = index_keys

    def __repr__(self):
        tups = ','.join(map(repr, self.load_key or ()))
        return '<Record %s:(%s) %r>' % (self.coll.info.name, tups, self.data)

class Collection:
    def __init__(self, store, name, key_func=None, encoder=None, _idx=None):
        self.store = store
        self.db = store.db
        if _idx is not None:
            self.info = CollInfo(name, _idx, None)
        else:
            self.info = store._get_info(name, idx=_idx)
        self.prefix = encode_int(self.info.idx)
        self.indices = {}
        self.key_func = key_func or self._time_key
        self.encoder = encoder or PickleEncoder()

    def _time_key(self, obj_key, obj):
        return obj_key or int(time.time() * 1000000)

    def add_index(self, name, func):
        assert name not in self.indices
        info_name = 'index:%s:%s' % (self.info.name, name)
        info = self.store._get_info(info_name, index_for=self.info.name)
        index = Index(self.store, info, func)
        self.indices[name] = index
        return index

    def _decompress(self, s):
        if s.startswith('Z'):
            return zlib.decompress(buffer(s, 1))
        return s[1:]

    def phys_keys(self):
        it = self.db.iterator(prefix=self.prefix, include_value=False)
        for phys_key in it:
            yield decode_tuple(phys_key, self.prefix)

    def _index_keys(self, obj_key, obj):
        keys = []
        for index in self.indices.itervalues():
            keys.extend(index.index_keys(obj_key, obj))
        return keys

    def _make_record(self, obj, load_key=None, phys_key=None):
        load_key = tuplize(load_key or self.key_func(None, obj))
        return Record(self, obj, load_key, phys_key,
                      self._index_keys(load_key, obj))

    def values(self, rec=True):
        for phys_key, data in self.db.iterator(prefix=self.prefix):
            phys_key = decode_tuple(phys_key, self.prefix)
            for obj in self.encoder.loads_many(self._decompress(data)):
                if rec:
                    obj = self._make_record(obj, phys_key=phys_key)
                yield obj

    def get(self, key, default=None, rec=True):
        key = tuplize(key)
        it = self.db.iterator(start=encode_tuple(key, self.prefix))
        phys_key, data = next(it, (None, None))
        if phys_key and phys_key.startswith(self.prefix):
            for obj in self.encoder.loads_many(self._decompress(data)):
                obj_key = tuplize(self.key_func(None, obj))
                if obj_key == key:
                    if rec:
                        phys_key = decode_tuple(phys_key, self.prefix)
                        return self._make_record(obj, key, phys_key)
                    else:
                        return obj
        if default is not None:
            return self._make_record(default)

    def _split_batch(self, rec, wb):
        assert rec.load_key and rec.phys_key and rec.phys_key != rec.load_key
        it = self.db.iterator(
            start=encode_tuple(rec.phys_key, self.prefix))
        phys_key, data = next(it, (None, None))
        assert rec.phys_key == phys_key, \
            'Physical key missing: %r' % (rec.phys_key,)
        for obj in self.encoder.loads_many(self._decompress(data)):
            obj_key = self.key_func(obj)
            if obj_key != rec.load_key:
                self.put(self._make_record(obj), wb, _obj_key=obj_key)
        (wb or self.db).delete(encode_tuple(rec.phys_key, self.prefix))
        rec.load_key = None
        rec.phys_key = None

    def put(self, rec, wb=None, _obj_key=None):
        if not isinstance(rec, Record):
            rec = self._make_record(rec)
        obj_key = tuplize(_obj_key or self.key_func(rec.load_key, rec.data))
        index_keys = self._index_keys(obj_key, rec.data)

        if rec.load_key:
            delete = (wb or self.db).delete
            if rec.phys_key and rec.load_key != rec.phys_key: # TODO end of batch
                # Old key was part of a batch, explode the batch.
                self._split_batch(rec, wb)
            elif rec.load_key != obj_key:
                # New version has changed key, delete old.
                delete(encode_tuple(rec.load_key, self.prefix))
            if index_keys != rec.index_keys:
                for index_key in rec.index_keys or ():
                    delete(index_key)
        else:
            # Old key might already exist, so delete it.
            self.delete(obj_key)

        put = (wb or self.db).put
        put(encode_tuple(obj_key, self.prefix),
            ' ' + self.encoder.dumps_many([rec.data]))
        for index_key in index_keys:
            put(index_key, '')
        rec.load_key = obj_key
        rec.phys_key = obj_key
        rec.index_keys = index_keys
        return rec

    def delete(self, obj, wb=None):
        if isinstance(obj, tuple):
            rec = self.get(obj)
        elif isinstance(obj, Record):
            rec = obj
        else:
            rec = self.get(self.key_func(obj))
        if rec:
            rec_key = rec.load_key or self.key_func(rec.data)
            if rec.phys_key and rec_key != rec.phys_key:
                self._split_batch(rec, wb)
            else:
                delete = (wb or self.db).delete
                delete(encode_tuple(rec_key, self.prefix))
                for index_key in rec.index_keys or ():
                    delete(index_key)
            rec.load_key = None
            rec.phys_key = None
            rec.index_keys = None
            return rec

class Store:
    def __init__(self, db):
        self.db = db
        self._info_coll = Collection(self, '\x00collections', _idx=0,
            encoder=TupleEncoder(), key_func=lambda _, tup: tup[0])
        self._counter_coll = Collection(self, '\x00counters', _idx=1,
            encoder=TupleEncoder(), key_func=lambda _, tup: tup[0])

    def _get_info(self, name, idx=None, index_for=None):
        rec = self._info_coll.get(name)
        if rec:
            assert rec.data == (name, idx or rec.data.idx, index_for)
            return CollInfo(rec.data)
        if idx is None:
            idx = self.count('\x00collections_idx', init=10)
            info = CollInfo(name, idx, index_for)
        return self._info_coll.put(info).data

    def count(self, name, n=1, init=1):
        rec = self._counter_coll.get(name, default=(name, init))
        val = rec.data[1]
        rec.data = (name, val + n)
        self._counter_coll.put(rec)
        return val
