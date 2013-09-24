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

from __future__ import absolute_import
import bisect
import functools
import itertools
import math
import random


__all__ = ['SkipList', 'SkiplistEngine', 'ListEngine', 'PlyvelEngine',
           'KyotoEngine', 'LmdbEngine']


class Engine(object):
    """
    A storage engine or transaction is any object that implements the following
    methods. Engines need not inherit from this class, it exists purely for
    documentary purposes. All key and value variables below are ``NUL``-safe
    bytestrings.
    """

    #: If present and not ``None``, indicates the `source object` responsible
    #: for producing data in the buffers returned by this engine. The source
    #: object must implement the `Memsink Protocol
    #: <https://github.com/dw/acid/issues/23>`_. This allows
    #: :py:class:`acid.keylib.Key` and :py:class:`acid.structlib.Struct` to
    #: present the result to the user without performing any copies.
    source = None

    def close(self):
        """Close the database connection. The default implementation does
        nothing."""

    def begin(self, write=False):
        """Start a database transaction, returning an :py:class:`Engine`
        instance requests should be directed to for the duration of the
        transaction. The default implementation does nothing, and returns
        itself."""
        return self

    def abort(self):
        """Abort the active database transaction. The default implmentation
        does nothing."""

    def commit(self):
        """Commit the active database transaction. The default implementation
        does nothing."""

    def get(self, key):
        """Return the value of `key` or ``None`` if it does not exist."""
        raise NotImplementedError

    def put(self, key, value):
        """Set the value of `key` to `value`, overwriting any prior value."""
        raise NotImplementedError

    def replace(self, key, value):
        """Replace the value of `key` with `value`, returning its prior value.
        If `key` previously didn't exist, return ``None`` instead. The default
        implementation is implemented using :py:meth:`get` and
        :py:meth:`put`."""
        old = self.get(key)
        self.put(key, value)
        return old

    def delete(self, key):
        """Delete `key` if it exists."""
        raise NotImplementedError

    def iter(self, key, reverse=False):
        """Yield `(key, value)` tuples in key order, starting at `key` and
        moving in a fixed direction.

        Key order must match the C `memcmp()
        <http://linux.die.net/man/3/memcmp>`_ function.

        `key`:
            Starting key. The first yielded element should correspond to this
            key if it exists, or the next highest key, or the highest key in
            the store.

        `reverse`:
            If ``False``, iteration proceeds until the lexicographically
            highest key is reached, otherwise it proceeds until the lowest key
            is reached.
        """
        raise NotImplementedError


class SkipList(object):
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
        max_level = min(self.max_level, self.level + 1)
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
        already exists, its previous value is overwritten. The previous value
        is returned if it existed, otherwise ``None`` is returned."""
        assert searchKey is not None
        update = self._update[:]
        node = self._findLess(update, searchKey)
        prev = node
        node = node[3]
        if node[0] == searchKey:
            old = node[1]
            node[1] = value
        else:
            old = None
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
        return old

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
            while self.level > 0 and self.head[3 + self.level][0] is None:
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


class SkiplistEngine(Engine):
    """Storage engine that backs onto a `Skip List
    <http://en.wikipedia.org/wiki/Skip_list>`_. Lookup and insertion are
    logarithmic.

    This is like :py:class:`ListEngine` but scales well, with overhead
    approaching a regular dict (113 bytes/record vs. 69 bytes/record on amd64).
    Supports around 23k inserts/second or 44k lookups/second,  and tested up to
    2.8 million keys.

        `maxsize`:
            Maximum expected number of elements. Inserting more will result in
            performance degradation.
    """
    def __init__(self, maxsize=65535):
        self.sl = SkipList(maxsize)
        self.get = self.sl.search
        self.put = self.sl.insert
        self.replace = self.sl.insert
        self.delete = self.sl.delete
        self.iter = self.sl.items

    def close(self):
        self.sl = None


class ListEngine(Engine):
    """Storage engine that backs onto a sorted list of `(key, value)` tuples.
    Lookup is logarithmic while insertion is linear.

    Primarily useful for unit testing. The constructor receives no arguments.
    """
    def __init__(self):
        #: Sorted list of `(key, value)` tuples.
        self.items = []
        #: Size in bytes for stored items, i.e.
        #: ``sum(len(k)+len(v) for k, v in items)``.
        self.size = 0

    def get(self, k):
        idx = bisect.bisect_left(self.items, (k,))
        if idx < len(self.items) and self.items[idx][0] == k:
            return self.items[idx][1]

    def put(self, k, v):
        idx = bisect.bisect_left(self.items, (k,))
        if idx < len(self.items) and self.items[idx][0] == k:
            self.size += len(v) - len(self.items[idx][1])
            self.items[idx] = (k, v)
        else:
            self.items.insert(idx, (k, v))
            self.size += len(k) + len(v)

    def delete(self, k):
        idx = bisect.bisect_left(self.items, (k,))
        if idx < len(self.items) and self.items[idx][0] == k:
            self.size -= len(k) + len(self.items[idx][1])
            self.items.pop(idx)

    def iter(self, k, reverse):
        if not self.items:
            return iter([])
        idx = bisect.bisect_left(self.items, (k,)) if k else 0
        if reverse:
            idx -= len(self.items) == idx
            #if self.items:
                #idx -= self.items[idx][0] > k
            xr = xrange(idx, -1, -1)
        else:
            xr = xrange(idx, len(self.items))
        return itertools.imap(self.items[:].__getitem__, xr)


class PlyvelEngine(Engine):
    """Storage engine that uses Google LevelDB via the `Plyvel
    <http://plyvel.readthedocs.org/>`_ module.

        `db`:
            If specified, should be a `plyvel.DB` instance for an already open
            database. Otherwise, the remaining keyword args are passed to the
            `plyvel.DB` constructor.
    """
    def __init__(self, db=None, wb=None, **kwargs):
        if not db:
            import plyvel
            db = plyvel.DB(**kwargs)
        self.db = db
        self.wb = wb
        self.get = db.get
        self.put = (wb or db).put
        self.delete = (wb or db).delete

    def close(self):
        self.db.close()

    def begin(self, write=False):
        wb = self.db.write_batch(sync=True)
        return PlyvelEngine(self.db, wb)

    def commit(self):
        self.wb.write()

    def iter(self, k, reverse):
        it = self.db.iterator()
        it.seek(k)
        if reverse:
            tup = next(it, None)
            it = iter(it.prev, None)
            if tup:
                next(it) # skip back past tup
                it = itertools.chain((tup,), it)
        return it


class KyotoEngine(Engine):
    """Storage engine that uses `Kyoto Cabinet
    <http://fallabs.com/kyotocabinet/>`_. Note a treedb must be used.
    """
    def __init__(self, db=None, path=None):
        self.db = db
        if not self.db:
            import kyotocabinet
            self.db = kyotocabinet.DB()
            assert self.db.open(path)
        self.get = self.db.get
        self.put = self.db.set
        self.delete = self.db.remove

    def iter(self, k, reverse):
        c = self.db.cursor()
        c.jump(k)
        tup = c.get()
        if reverse:
            it = iter((lambda: c.step_back() and c.get()), False)
            if not tup:
                c.jump_back()
                tup = c.get()
            return itertools.chain((tup,), it) if tup else it
        else:
            it = iter((lambda: c.step() and c.get()), False)
            return itertools.chain((tup,), it) if tup else it


class TraceEngine(object):
    """Storage engine that wraps another to provide a complete trace of
    interactions between Acid and the external engine. Used for debugging and
    producing crash reports.

        `engine`:
            :py:class:`lmdb.engines.Engine` to wrap.

        `trace_path`:
            String filesystem path to *overwrite* with a new trace log.

    Each line written to `trace_path` contains the following fields separated
    by a single space character, with the line itself terminated by a single
    newline character.

    Fields:

        * Monotonically incrementing transaction count. Initial value is `1`
          for operations performed against the main :py:class:`Engine` class.

        * String operation identifier.

        * Optionally a hex-encoded string `key`. Possibly the empty
          string.

        * Optionally a hex-encoded string `value`. Possibly the empty string.

    Valid operation identifiers:

        ``close``:
            The engine or transaction was closed.

        ``get``:
            :py:meth:`Engine.get` is about to be invoked for `key`.

        ``got``:
            :py:meth:`Engine.get` was invoked, and returned `value`.
            Value may be the string ``None`` if no record was returned.

        ``put``:
            :py:meth:`Engine.put` is about to be invoked with `key` and
            `value`.

        ``delete``:
            :py:meth:`Engine.delete` is about to be invoked with `key`.

        ``abort``:
            :py:meth:`Engine.abort` is about to be invoked.

        ``begin``:
            :py:meth:`Engine.begin` is about to be invoked. `key` may be
            the string ``True`` if a write transaction was requested,
            otherwise ``False``.

        ``commit``:
            :py:meth:`Engine.commit` is about to be invoked.

        ``iter``:
            :py:meth:`Engine.iter` is about to be invoked for `key`. If
            `value` is ``True``, then reverse iteration was requested,
            otherwise ``False``.

        ``fetch``:
            The next element is about to be retrieved from an iterator
            returned by :py:meth:`Engine.iter`.

        ``yield``:
            The element retrieved by the last ``fetch`` is about to be
            yielded; ``key`` and ``value`` are the key and value returned
            by the engine.
    """
    _counter = 0

    def __init__(self, engine, trace_path=None, _fp=None):
        assert trace_path is not None or _fp is not None
        TraceEngine._counter += 1
        self.idx = TraceEngine._counter
        self.engine = engine
        self.trace_path = None
        self.fp = _fp
        if _fp is None:
            self.fp = open(trace_path, 'w', 1)

    def _trace(self, op, *args):
        args = ' '.join(str(a).encode('hex') for a in args)
        self.fp.write('%s %s %s\n' % (self.idx, op, args))

    def close(self):
        self._trace('close')
        self.engine.close()

    def get(self, key):
        self._trace('get', key)
        v = self.engine.get(key)
        self._trace('got', v)
        return v

    def put(self, key, value):
        self._trace('put', key, value)
        return self.engine.put(key, value)

    def delete(self, key):
        self._trace('delete', key)
        return self.engine.delete(key)

    def abort(self):
        self._trace('abort')
        return self.engine.abort()

    def begin(self, write=False):
        self._trace('begin', write)
        txn = self.engine.begin(write)
        return TraceEngine(txn, _fp=self.fp)

    def commit(self):
        self._trace('commit')
        return self.engine.commit()

    def iter(self, k, reverse):
        self._trace('iter', k, reverse)
        it = self.engine.iter(k, reverse)
        while True:
            self._trace('fetch')
            key, value = next(it)
            self._trace('yield', key, value)
            yield key, value


class LmdbEngine(Engine):
    """Storage engine that uses the OpenLDAP `"Lightning" MDB
    <http://symas.com/mdb/>`_ library via the `py-lmdb
    <http://lmdb.readthedocs.org/>`_ module.

        `env`:
            :py:class:`lmdb.Environment` to use, or ``None`` if `txn` or
            `kwargs` is provided.

        `txn`:
            :py:class:`lmdb.Transaction` to use, or ``None`` if `env` or
            `kwargs` is provided.

        `db`:
            Database handle to use, or ``None`` to use the main database.

        `kwargs`:
            If `env` and `txn` are ``None``, pass these keyword arguments to
            create a new :py:class:`lmdb.Environment`.
    """
    def __init__(self, env=None, txn=None, db=None, **kwargs):
        if not (env or txn):
            import lmdb
            env = lmdb.open(**kwargs)
        self.env = env
        self.txn = txn
        self.db = db
        self.get = (txn or env).get
        self.put = (txn or env).put
        self.delete = (txn or env).delete
        self.cursor = (txn or env).cursor

    def close(self):
        self.env.close()

    def begin(self, write=False, db=None):
        """Start a transaction. Only valid if `txn` was not passed to the
        constructor.

            `write`:
                Start a write transaction
        """
        assert not self.txn
        return LmdbEngine(self.env, self.env.begin(write=write))

    def abort(self):
        self.txn.abort()

    def commit(self):
        self.txn.commit()

    def iter(self, k, reverse):
        return self.cursor(db=self.db)._iter_from(k, reverse)
