
from __future__ import absolute_import

import math
import random
import bisect
import functools
import operator
from itertools import chain
from itertools import ifilter
from itertools import imap
from operator import itemgetter

import centidb


class SkipList:
    """Doubly linked non-indexable skip list, providing logarithmic insertion
    and deletion. Keys are any orderable Python object.

        `maxsize`:
            Maximum number of items expected to exist in the list.

    For a million-keyed list on a Core 2, performance is ~27k inserts and ~44k
    searches/sec on CPython, or ~75k inserts and ~100k searches/sec on PyPy.
    """
    def __init__(self, maxsize=65535):
        self.max_level = int(math.log(maxsize, 2))
        self.level = 0
        self.head = self._makeNode(self.max_level, None, None)
        self.nil = self._makeNode(-1, None, None)
        self.tail = self.nil
        self.head[3:] = [self.nil for x in xrange(self.max_level)]
        self._update = [self.head] * (1 + self.max_level)
        self.p = 1/math.e

    def _makeNode(self, level, key, value):
        node = [None] * (4 + level)
        node[0] = key
        node[1] = value
        return node

    def _randomLevel(self):
        lvl = 0
        max_level = self.level + 1
        while random.random() < self.p and lvl < max_level:
            lvl += 1
        return lvl

    @staticmethod
    def reprNode(self):
        links = []
        for i, node in enumerate(self[3:]):
            if node[0]:
                links.append('%d->%r' % (i, node[0]))
        return '<Node %r (%s)>' % (self[0] or 'head', ', '.join(links))

    def items(self, searchKey=None, reverse=False):
        """Return an iterator initially yielding `searchKey`, or the next
        greater key, or the end of the list. Subsequent iterations move
        backwards if `reverse=True`. If `searchKey` is ``None`` then start at
        either the beginning or end of the list."""
        if searchKey is not None:
            update = self._update[:]
            node = self._findLess(update, searchKey)[3]
            if node is self.nil and reverse:
                node = self.tail
        elif reverse:
            node = self.tail
        else:
            node = self.head[3]
        idx = 2 if reverse else 3
        while node[0] is not None:
            yield node[0], node[1]
            node = node[idx]

    def _findLess(self, update, searchKey):
        node = self.head
        for i in xrange(self.level, -1, -1):
            key = node[3 + i][0]
            while key is not None and key < searchKey:
                node = node[3 + i]
                key = node[3 + i][0]
            update[i] = node
        return node

    def insert(self, searchKey, value):
        """Insert `searchKey` into the list with `value`. If `searchKey`
        already exists, its previous value is overwritten."""
        assert searchKey is not None
        update = self._update[:]
        node = self._findLess(update, searchKey)
        prev = node
        node = node[3]
        if node[0] == searchKey:
            node[1] = value
        else:
            lvl = self._randomLevel()
            self.level = max(self.level, lvl)
            node = self._makeNode(lvl, searchKey, value)
            node[2] = prev
            for i in xrange(0, lvl+1):
                node[3 + i] = update[i][3 + i]
                update[i][3 + i] = node
            if node[3] is self.nil:
                self.tail = node
            else:
                node[3][2] = node

    def delete(self, searchKey):
        """Delete `searchKey` from the list, returning ``True`` if it
        existed."""
        update = self._update[:]
        node = self._findLess(update, searchKey)
        node = node[3]
        if node[0] == searchKey:
            node[3][2] = update[0]
            for i in xrange(self.level + 1):
                if update[i][3 + i] is not node:
                    break
                update[i][3 + i] = node[3 + i]
            while self.level > 1 and self.head[3 + self.level].key is None:
                self.level -= 1
            if self.tail is node:
                self.tail = node[2]
            return True

    def search(self, searchKey):
        """Return the value associated with `searchKey`, or ``None`` if
        `searchKey` does not exist."""
        node = self.head
        for i in xrange(self.level, -1, -1):
            key = node[3 + i][0]
            while key is not None and key < searchKey:
                node = node[3 + i]
                key = node[3 + i][0]
        node = node[3]
        if node[0] == searchKey:
            return node[1]


class SkiplistEngine(object):
    """Storage engine that backs onto a `Skip List
    <http://en.wikipedia.org/wiki/Skip_list>`_. Both lookup and insertion
    are logarithmic.

        `maxsize`:
            Maximum expected number of elements. Inserting more than this will
            result in performance degradation.
    """
    def __init__(self, maxsize=65535):
        self.sl = SkipList(maxsize)
        self.get = self.sl.search
        self.put = self.sl.insert
        self.delete = self.sl.delete

    def iter(self, k, reverse=False):
        it = self.sl.items(k, reverse)
        if reverse:
            tup = next(it, None)
            if tup and tup[0] <= k:
                return chain((tup,), it)
        return it


class ListEngine(object):
    """Storage engine that backs onto a sorted list of `(key, value)` tuples.
    Lookup is logarithmic while insertion is linear.

    Primarily useful for unit testing. The constructor receives no arguments.
    """
    def __init__(self):
        #: Sorted list of `(key, value)` tuples.
        self.pairs = []
        #: Size in bytes for stored pairs, i.e.
        #: ``sum(len(k)+len(v) for k, v in pairs)``.
        self.size = 0

    def get(self, k):
        idx = bisect.bisect_left(self.pairs, (k,))
        if idx < len(self.pairs) and self.pairs[idx][0] == k:
            return self.pairs[idx][1]

    def put(self, k, v):
        idx = bisect.bisect_left(self.pairs, (k,))
        if idx < len(self.pairs) and self.pairs[idx][0] == k:
            self.size += len(v) - len(self.pairs[idx][1])
            self.pairs[idx] = (k, v)
        else:
            self.pairs.insert(idx, (k, v))
            self.size += len(k) + len(v)

    def delete(self, k):
        idx = bisect.bisect_left(self.pairs, (k,))
        if idx < len(self.pairs) and self.pairs[idx][0] == k:
            self.size -= len(k) + len(self.pairs[idx][1])
            self.pairs.pop(idx)

    def iter(self, k, reverse=False):
        if not self.pairs:
            return []
        idx = bisect.bisect_left(self.pairs, (k,)) if k else 0
        if reverse:
            idx -= len(self.pairs) == idx
            if self.pairs:
                idx -= self.pairs[idx][0] > k
            xr = xrange(idx, -1, -1)
        else:
            xr = xrange(idx, len(self.pairs))
        return imap(self.pairs.__getitem__, xr)


class PlyvelEngine(object):
    """Storage engine that uses Google LevelDB via the `Plyvel
    <http://plyvel.readthedocs.org/>`_ module.

        `db`:
            If specified, should be a `plyvel.DB` instance for an already open
            database. Otherwise, the remaining keyword args are passed to the
            `plyvel.DB` constructor.
    """
    txn_id = None

    def __init__(self, db=None, wb=None, **kwargs):
        if not db:
            import plyvel
            db = plyvel.DB(**kwargs)
        self.db = db
        self.get = db.get
        self.put = (wb or db).put
        self.delete = (wb or db).delete

    def iter(self, k, reverse=False):
        if reverse:
            return self.db.iterator(stop=k, include_stop=True, reverse=True)
        return self.db.iterator(start=k)


class KyotoEngine(object):
    """Storage engine that uses `Kyoto Cabinet
    <http://fallabs.com/kyotocabinet/>`_. Note a treedb must be used.
    """
    txn_id = None

    def __init__(self, db=None, path=None):
        self.db = db
        if not self.db:
            import kyotocabinet
            self.db = kyotocabinet.DB(path)
        self.get = self.db.get
        self.set = self.db.set
        self.delete = self.db.remove

    def iter(self, k, keys=True, values=True, reverse=False):
        kw = dict(include_key=keys, include_value=values, include_stop=True)
        kw.update(dict(stop=k, reverse=True) if reverse else dict(start=k))
        return self.db.iterator(**kw)

def make_json_encoder():
    """Return an :py:class:`Encoder <centidb.Encoder>` that serializes
    dict/list/string/float/int/bool/None objects using the :py:mod:`json`
    module."""
    import json
    encode = json.JSONEncoder().encode
    return centidb.Encoder('json', json.JSONDecoder().decode,
                           functools.partial(encode, separators=',:'))

def make_msgpack_encoder():
    """Return an :py:class:`Encoder <centidb.Encoder>` that serializes
    dict/list/string/float/int/bool/None objects using `MessagePack
    <http://msgpack.org/>`_ via the `msgpack-python
    <https://pypi.python.org/pypi/msgpack-python/>`_ package."""
    import msgpack
    return centidb.Encoder('msgpack', msgpack.loads, msgpack.dumps)

def make_thrift_encoder(klass, factory=None):
    """
    Return an :py:class:`Encoder <centidb.Encoder>` instance that serializes
    `Apache Thrift <http://thrift.apache.org/>`_ structs using a compact binary
    representation.

    `klass`:
        Thrift-generated struct class the `Encoder` is for.

    `factory`:
        Thrift protocol factory for the desired protocol, defaults to
        `TCompactProtocolFactory`.
    """
    import thrift.protocol.TCompactProtocol
    import thrift.transport.TTransport
    import thrift.TSerialization

    if not factory:
        factory = thrift.protocol.TCompactProtocol.TCompactProtocolFactory()

    def loads(buf):
        transport = thrift.transport.TTransport.TMemoryBuffer(buf)
        proto = factory(transport)
        value = klass()
        value.read(proto)
        return value

    def dumps(value):
        assert isinstance(value, klass)
        return thrift.TSerialization.serialize(value, factory)

    # Form a name from the Thrift ttypes module and struct name.
    name = 'thrift:%s.%s' % (klass.__module__, klass.__name__)
    return centidb.Encoder(name, loads, dumps)
