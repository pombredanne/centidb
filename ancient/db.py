
#
# First incomplete attempt at a centidb-alike AppEngine stand-in, circa 2011.
#

import contextlib

try:
    import cPickle as pickle
except ImportError:
    import pickle

from datakit import encodinglib
import kyotocabinet

from myapp import schema

_encoding = encodinglib.new('pb')

def _loads(key, s):
    key = key if isinstance(key, basestring) else key[0]
    kind = getattr(schema, key)
    return _encoding.loads(kind, s)

def _dumps(s):
    return _encoding.dumps(s)

def _enkey(args, suffix='\00'):
    if not hasattr(args, '__getitem__'):
        args = (args,)

    return '\00'.join(s.encode('utf-8') if s is unicode else str(s)
                      for s in args) + suffix

def _dekey(s, collapse=False):
    t = tuple(ss.decode('utf-8') for ss in s[:-1].split('\00'))
    return t[0] if len(t) == 1 and collapse else t


def open(path):
    global _db
    _db = Database(path)
    for name in dir(_db):
        if not name.startswith('_'):
            globals()[name] = getattr(_db, name)

'''


dave\0 wilson
david\0 wilson

7f
8001

'''

INVERT_TBL = ''.join(chr(c ^ 0xff) for c in xrange(256))

def invert(s):
    """Invert the bits in the bitmap `s` (represented as a string).
    """
    return s.translate(INVERT_TBL)


class IndexTuple(object):
    # Based on http://code.google.com/appengine/docs/python/datastore/entities.html
    TYPE_NULL = 0
    TYPE_INTEGER = 1
    TYPE_BOOL = 2
    TYPE_BYTES = 3
    TYPE_STRING = 4

    def _encode_none(cls, val):
        return chr(cls.TYPE_NULL)

    def _decode_none(cls, s):
        return None

    def _encode_int(cls, i):
        return '%c%020d' % (cls.TYPE_INTEGER, i)

    def _decode_int(cls, s):
        return int(s, 10)

    def _encode_str(cls, s):
        return '%c%s' % (cls.TYPE_BYTES, s.encode('hex'))

    def _decode_str(cls, s):
        return s.decode('hex')

    def _encode_uni(cls, u):
        return '%c%s' % (cls.TYPE_STRING, s.encode('utf-8').encode('hex'))

    def _decode_uni(cls, s):
        return s[1:].decode('hex').decode('utf-8')

    ENC_MAP = {
        int: _encode_int,
        long: _encode_int,
        str: _encode_str,
        unicode: _encode_uni,
        type(None): _encode_none,
    }

    @classmethod
    def encode(cls, tup):
        return '|'.join(cls.ENC_MAP[type(val)](cls, val) for val in tup)

    DEC_MAP = {
        TYPE_NULL: _decode_none,
        TYPE_INTEGER: _decode_int,
        TYPE_BYTES: _decode_str,
        TYPE_STRING: _decode_uni,
    }

    @classmethod
    def decode(cls, s):
        return tuple(cls.DEC_MAP[ss[0]](cls, ss[1:])
                     for ss in s.split('|'))


class Index:
    def __init__(self, db, name, prefix, spec):
        self.db = db
        self.index_db = db
        self.index_prefix = index_prefix
        self.prefix = prefix
        self._parse_spec(spec)

        db.watch('after_get', prefix, self._on_after_get)
        db.watch('before_put', prefix, self._on_before_put)
        db.watch('before_delete', prefix, self._on_after_delete)

    def erase(self):
        pass

    def destroy(self, erase=False):
        self.db.unwatch('after_delete', prefix, self._on_after_delete)
        self.db.unwatch('before_put', prefix, self._on_before_put)
        if erase:
            self.erase()

    def _parse_spec(self, spec):
        self.fields = fields = []
        for s in self.spec.split():
            if s.startswith('-'):
                fields.append((True, s[1:]))
            else:
                fields.append((False, s))

    def _value(self, key, entity):
        return [entity[fld] for rev, fld in self.fields]

    def _on_before_put(self, key, entity):
        pass

    def _on_after_delete(self, key, entity):
        pass

    def rebuild(self):
        self.erase()
        for key, entity in self.db.range(prefix=self.prefix):
            self._index_one(key, entity)

    def query(self, *vals):
        pass

    def iterkeys(self, reverse=False, collapse=False):
        return self.index_db.valrange(prefix=self.my_prefix,
            reverse=reverse, collapse=collapse)

    def iter(self, reverse=False, collapse=False):
        pass


class Database(object):
    def __init__(self, path):
        self._path = path
        self._watches = {}
        self._db = kyotocabinet.DB()
        if not self._db.open(path):
            self._raise()

    def watch(self, event, prefix, cb):
        handlers = self._watched.setdefault(event, [])
        handlers.append((prefix, cb))

    def unwatch(self, event, prefix, cb):
        handlers = self._watches[event]
        handlers.remove((prefix, cb))

    @contextlib.contextmanager
    def transaction(self, hard=False):
        cancelled = [False]
        cancel = lambda: cancelled.__setitem__(0, True)

        self._db.begin_transaction(hard)
        try:
            yield cancel
        except Exception:
            self._db.end_transaction(False)
            raise
        finally:
            self._db.end_transaction(not cancelled[0])

    def _raise(self):
        # TODO: how could .error() possibly be thread safe.
        raise Exception(self._db.error())

    def _sync(self, do=True):
        if do:
            self._db.synchronize()

    def close(self):
        if not self._db.close():
           self. _raise()

    def delete(self, key, sync=True):
        if not self._db.remove(_enkey(key)):
           self. _raise()
        self._sync(sync)

    def deletes(self, keys, sync=True):
        encoded = [_enkey(k) for k in keys]
        print 'eek', encoded
        if encoded:
            if not self._db.remove_bulk(encoded):
               self. _raise()
            self._sync(sync)

    def put(self, key, entity, sync=True):
        if not self._db.set(_enkey(key), _dumps(entity)):
           self. _raise()
        self._sync(sync)

    def puts(self, it):
        if hasattr(it, 'iteritems'):
            it = it.iteritems()
        for key, entity in it:
            put(key, entity, sync=False)
        _sync()

    def get(self, key):
        s = self._db.get(_enkey(key))
        if s is not None:
            return _loads(key, s)

    def gets(self, keys):
        for key in keys:
            yield key, get(key)

    def _walk(self, prefix=None, low=None, high=None,
            reverse=False, max=None, collapse=False):
        if prefix is not None:
            low = _enkey(prefix)
            high = _enkey(prefix, suffix='\xff')
        elif low is None or high is None:
            raise ValueError('must specify prefix, low, or high')
        else:
            low = _enkey(low)
            high = _enkey(high)

        c = self._db.cursor()
        c.jump_back(high) if reverse else c.jump(low)
        step = c.step_back if reverse else c.step

        i = 0
        while c.get_key() <= high:
            if c.get_key() >= low:
                yield _dekey(c.get_key(), collapse), c
                i += 1
            if max and i == max:
                break
            if not step():
                break

    def iter(self, reverse=False, collapse=False):
        c = self._db.cursor()
        c.jump() if reverse else c.jump_back()
        while c.get_key():
            key = _dekey(c.get_key(), collapse)
            yield key, _loads(key, c.get_value())
            c.step_back() if reverse else c.step()

    def iterkeys(reverse=False, collapse=False):
        c = self._db.cursor()
        c.jump() if reverse else c.jump_back()
        while c.get_key():
            yield _dekey(c.get_key(), collapse)
            c.step_back() if reverse else c.step()

    def keyrange(prefix=None, low=None, high=None,
            reverse=False, max=None, collapse=False):
        for key, c in self._walk(prefix, low, high, reverse, max, collapse):
            yield key

    def deleterange(self, prefix=None, low=None, high=None,
            reverse=False, max=None):
        return deletes(keyrange(prefix, low, high, reverse, max))

    def countrange(self, prefix=None, low=None, high=None,
            reverse=False, collapse=False):
        count = 0
        for _ in self._walk(prefix, low, high, reverse, collapse):
            count += 1
        return count

    def countpred(self, pred, prefix=None, low=None, high=None,
            reverse=False, max=None, collapse=False):
        count = 0
        for key, c in self._walk(prefix, low, high, reverse, max, collapse):
            if pred(key, _loads(c.get_value())):
                count += 1
        return count

    def valrange(self, prefix=None, low=None, high=None,
            reverse=False, max=None, collapse=False):
        it = self._walk(prefix, low, high, reverse, max, collapse)
        return (_loads(key, c.get_value()) for key, c in it)

    def range(self, prefix=None, low=None, high=None,
            reverse=False, max=None, collapse=False):
        it = self._walk(prefix, low, high, reverse, max, collapse)
        return ((key, _loads(key, c.get_value())) for key, c in it)
