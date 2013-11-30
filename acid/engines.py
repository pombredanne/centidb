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
import threading
import urlparse

from acid import errors


__all__ = ['SkipList', 'SkiplistEngine', 'ListEngine', 'PlyvelEngine',
           'KyotoEngine', 'LmdbEngine']

_engines = []
KB = 1024
MB = 1048576


def register(klass):
    """Register a new engine class that supports :py:meth:`Engine.from_url`.
    Allows the engine to be uniformly constructed from a simple string using
    :py:func:`acid.open`. May be used as a decorator.
    """
    _engines.append(klass)
    return klass


def parse_url(url):
    """Split a string URL up into constituent parts, stripping any parameters
    present in the path part and expanding them into their own dictionary.
    """
    parsed = urlparse.urlparse(url)
    path, _, params_str = parsed.path.partition(';')
    if params_str:
        params = {}
        for part in params_str.split(','):
            bits = part.split('=', 1)
            if len(bits) == 1:
                params[bits[0]] = True
            else:
                params[bits[0]] = bits[1]
    else:
        params = {}

    return {
        'scheme': parsed.scheme,
        'netloc': parsed.netloc,
        'path': path,
        'params': params
    }


def from_url(url):
    dct = parse_url(url)
    for klass in _engines:
        engine = klass.from_url(dct)
        if engine:
            return engine

    raise errors.ConfigError('cannot parse ' + repr(url))


class Engine(object):
    """
    A storage engine or transaction is any object that implements the following
    methods. Engines need not inherit from this class, but doing so enables
    various default method implementations. All key and value variables below
    are ``NUL``-safe bytestrings.
    """

    #: If present and not ``None``, indicates the `source object` responsible
    #: for producing data in the buffers returned by this engine. The source
    #: object must implement the `Memsink Protocol
    #: <https://github.com/dw/acid/issues/23>`_. This allows
    #: :py:class:`acid.keylib.Key` and :py:class:`acid.structlib.Struct` to
    #: present the result to the user without performing any copies.
    source = None

    def from_url(cls, dct):
        """Attempt to parse `dct` as a reference to the engine. If the
        reference is valid, return a new engine instance, otherwise return
        ``None``. URLs should be of the form `engine:/path?p1=v1;p2=v2` or
        `engine://<netloc>/path?p1=v1;p2=v2`. `dct` is a dict with fields:

            * `scheme`: URL scheme (e.g. `"engine"`).
            * `netloc`: Empty string if URL was in first form, `netloc` if URL
              was in second form.
            * `path`: String path.
            * `params`: Dict with keys `p1` and `p2`, values are strings if the
              parameter included a value, otherwise ``True``.
        """
    from_url = classmethod(from_url)

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

    def pop(self, key):
        """Delete `key` if it exists, returning the previous value, if any,
        otherwise ``None``. The default implementation is uses :py:meth:`get`
        and :py:meth:`delete`."""
        old = self.get(key)
        self.delete(key)
        return old

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

    def reprNode(self):
        links = []
        for i, node in enumerate(self[3:]):
            if node[0]:
                links.append('%d->%r' % (i, node[0]))
        return '<Node %r (%s)>' % (self[0] or 'head', ', '.join(links))
    reprNode = classmethod(reprNode)

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
        """Delete `searchKey` from the list, returning the old value if it
        existed, otherwise ``None``."""
        update = self._update[:]
        node = self._findLess(update, searchKey)
        node = node[3]
        if node[0] == searchKey:
            old = node[1]
            node[3][2] = update[0]
            for i in xrange(self.level + 1):
                if update[i][3 + i] is not node:
                    break
                update[i][3 + i] = node[3 + i]
            while self.level > 0 and self.head[3 + self.level][0] is None:
                self.level -= 1
            if self.tail is node:
                self.tail = node[2]
            return old

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
    <http://en.wikipedia.org/wiki/Skip_list>`_, lookup and insertion are
    logarithmic. This is like :py:class:`ListEngine` but scales well, with
    overhead approaching a regular dict (113 bytes/record vs. 69 bytes/record
    on amd64). Supports around 23k inserts/second or 44k lookups/second,  and
    tested up to 2.8 million keys.

        `maxsize`:
            Maximum expected number of elements. Inserting more will result in
            performance degradation.

    URL scheme for :py:func:`acid.open`: `"skiplist:/[;maxsize=N]"`
    """
    def __init__(self, maxsize=65535):
        self.sl = SkipList(maxsize)
        self.get = self.sl.search
        self.replace = self.sl.insert
        self.delete = self.sl.delete
        self.pop = self.sl.delete
        self.iter = self.sl.items

    def from_url(cls, dct):
        if dct['scheme'] == 'skiplist':
            return cls(maxsize=int(dct['params'].get('maxsize', 65535)))
    from_url = classmethod(from_url)

    def put(self, key, value):
        self.sl.insert(str(key), str(value))

    def close(self):
        self.sl = None


class ListEngine(Engine):
    """Storage engine that backs onto a sorted list of `(key, value)` tuples.
    Lookup is logarithmic while insertion is linear. Primarily useful for unit
    testing.

    URL scheme for :py:func:`acid.open`: `"list:/"`
    """
    def __init__(self):
        #: Sorted list of `(key, value)` tuples.
        self.items = []
        #: Size in bytes for stored items, i.e.
        #: ``sum(len(k)+len(v) for k, v in items)``.
        self.size = 0

    def from_url(cls, dct):
        if dct['scheme'] == 'list':
            return cls()
    from_url = classmethod(from_url)

    def get(self, k):
        idx = bisect.bisect_left(self.items, (k,))
        if idx < len(self.items) and self.items[idx][0] == k:
            return self.items[idx][1]

    def put(self, k, v):
        # Ensure we don't hold on to buffers.
        k = str(k)
        v = str(v)
        idx = bisect.bisect_left(self.items, (k,))
        if idx < len(self.items) and self.items[idx][0] == k:
            old = self.items[idx][1]
            self.size += len(v) - len(old)
            self.items[idx] = (k, v)
            return old
        else:
            self.items.insert(idx, (k, v))
            self.size += len(k) + len(v)

    def delete(self, k):
        idx = bisect.bisect_left(self.items, (k,))
        if idx < len(self.items) and self.items[idx][0] == k:
            old = self.items[idx][1]
            self.size -= len(k) + len(old)
            self.items.pop(idx)
            return old

    replace = put
    pop = delete

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

    Read transactions are implemented using snapshots, and write transactions
    are implemented using an Engine-internal :py:class:`threading.Lock`. Note
    that write batches are not used, since a consistent view of the partially
    mutated database is required within a transaction.

        `db`:
            If specified, should be a `plyvel.DB` instance for an already open
            database. Otherwise, the remaining keyword args are passed to the
            `plyvel.DB` constructor.

        `lock`:
            If not ``None``, specifies some instance satisfying the
            :py:class:`threading.Lock` interface to use as an alternative lock
            instead of the Engine-internal write lock. This can be used to
            synchronize writes with some other aspect of the application, or to
            replace the lock with e.g. a greenlet-friendly implementation.

    URL scheme for :py:func:`acid.open`:
    `"leveldb:/path/to/env.ldb[p1[;p2=v2]]`". URL parameters:

        `paranoid_checks`:
            Enable "paranoid" engine checks; default off.

        `write_buffer_size=N`:
            Size in MiB of level 0 memtable, higher values improve bulk loads;
            default: 4MiB.

        `max_open_files=N`:
            Maximum number of OS file descriptors to open; default 1000.

        `lru_cache_size=N`:
            Size of block cache in MiB; default 8MiB.

        `block_size=N`:
            Approximate size in KiB of user data packed per block. Note that
            the size specified here corresponds to uncompressed data, the
            actual size of the unit read from disk may be smaller if
            compression is enabled. Default 4KiB

        `block_restart_interval=N`:
            Number of keys between restart points for delta encoding of keys;
            default 16.

        `compression=[compressor]`:
            Block compression scheme to use; default `"snappy"`.

        `bloom_filter_bits=N`:
            Use a bloom filter policy with approximately the specified number
            of bits per key. A good value is 10, which yields a filter with ~1%
            false positive rate. Default: no bloom filter policy.
    """
    def __init__(self, db=None, lock=None, _snapshot=None, **kwargs):
        if not db:
            import plyvel
            db = plyvel.DB(**kwargs)
        self.db = db
        self.snapshot = _snapshot
        self.lock = lock or threading.Lock()

        self.put = db.put
        self.delete = db.delete
        if _snapshot:
            self.get = _snapshot.get
            self._iter = _snapshot.iterator
        else:
            self.get = db.get
            self._iter = db.iterator

    def from_url(cls, dct):
        if dct['scheme'] != 'leveldb':
            return

        return cls(path=dct['path'],
            paranoid_checks=dct.get('paranoid_checks', False),
            write_buffer_size=MB*int(dct.get('writer_buffer_size', 4)),
            max_open_files=int(dct.get('max_open_files', 1000)),
            lru_cache_size=MB*int(dct.get('lru_cache_size', 8)),
            block_size=KB*int(dct.get('block_size', 4)),
            block_restart_interval=int(dct.get('block_restart_interval', 16)),
            compression=dct.get('compression', 'snappy'),
            bloom_filter_bits=int(dct.get('bloom_filter_bits', 0)))
    from_url = classmethod(from_url)

    def close(self):
        self.db.close()

    def begin(self, write=False):
        if write:
            self.lock.acquire()
            snapshot = None
        else:
            snapshot = self.db.snapshot()
        return PlyvelEngine(self.db, self.lock, snapshot)

    def commit(self):
        if not self.snapshot: # write txn
            self.lock.release()

    def abort(self):
        if not self.snapshot: # write txn
            self.lock.release()

    def iter(self, k, reverse):
        it = self._iter()
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

    @property
    def items(self):
        # Convenience passthrough for unit tests.
        return self.engine.items

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
        bits = []
        for arg in args:
            if isinstance(arg, bytes):
                bits.append(arg.encode('hex'))
            elif isinstance(arg, bool):
                bits.append(str(int(arg)))
        self.fp.write('%s %s %s\n' % (self.idx, op, ' '.join(bits)))

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
        iter_id = self._counter
        self._counter += 1

        self._trace('iter', iter_id, k, reverse)
        try:
            it = self.engine.iter(k, reverse)
            while True:
                self._trace('fetch', iter_id)
                key, value = next(it)
                self._trace('yield', iter_id, key, value)
                yield key, value
        except StopIteration:
            self._trace('enditer', iter_id)


class LmdbEngine(Engine):
    """Storage engine that uses the OpenLDAP `"Lightning" MDB
    <http://symas.com/mdb/>`_ library via the `py-lmdb
    <http://lmdb.readthedocs.org/>`_ module.

        `env`:
            :py:class:`lmdb.Environment` to use. If ``None``, the remaining
            keyword args are passed to the :py:class:`lmdb.Environment`
            constructor.

        `db`:
            Named database handle to use, or ``None`` to use the main database.

    URL scheme for :py:func:`acid.open`:
    `"lmdb:/path/to/env.lmdb[;p1[;p2=v2]]"`. URL parameters:

            `map_size=N`:
                Maximum environment map size in MiB; default 4194304MiB.

            `readonly`:
                Open environment read-only; default read-write.

            `nometasync`:
                Disable meta page fsync; default enabled.

            `nosync`
                Disable sync; default enabled.

            `map_async`
                Use ``MS_ASYNC`` with msync, `writemap` equivalent to `nosync`;
                default use ``MS_SYNC``.

            `noreadahead`:
                Instruct LMDB to disable the OS filesystem readahead mechanism,
                which may improve random read performance when a database is
                larger than RAM; readahead is enabled by default.

            `writemap`:
                Use writeable memory mapping; default disabled.

                Note: When enabled, defective operating systems like OS X that
                do not fully support sparse files will attempt to zero-fill the
                database file to match `map_size` at close. Avoid use of
                `writemap` there.

            `nomeminit`:
                Avoiding 0-initialization of malloc buffers when `writemap` is
                disabled. Improves performance at the cost of nondeterministic
                slack areas in the database file, and potential security
                consequences (e.g. accidentally persistenting free'd cleartext
                passwords).

            `max_readers=N`:
                Maximum concurrent read threads; default 126.
    """
    def __init__(self, env=None, txn=None, db=None, **kwargs):
        if not (env or txn):
            import lmdb
            env = lmdb.open(**kwargs)
        self.env = env
        self.txn = txn
        self.db = db
        if txn:
            self.get = txn.get
            self.put = txn.put
            self.pop = txn.pop
            self.replace = txn.replace
            self.delete = txn.delete
            self.cursor = txn.cursor

    def from_url(cls, dct):
        if dct['scheme'] != 'lmdb':
            return
        return cls(path=dct['path'],
                   map_size=MB*int(dct.get('map_size', 4194304)),
                   readonly=bool(dct['params'].get('readonly')),
                   metasync=not dct['params'].get('nometasync'),
                   sync=not dct['params'].get('nosync'),
                   map_async=bool(dct['params'].get('map_async')),
                   readahead=not dct['params'].get('noreadahead'),
                   writemap=bool(dct['params'].get('writemap')),
                   meminit=not bool(dct['params'].get('nomeminit')),
                   max_readers=int(dct['params'].get('max_readers', 126)))
    from_url = classmethod(from_url)

    def close(self):
        self.env.close()

    def begin(self, write=False):
        assert not self.txn
        return LmdbEngine(self.env, self.env.begin(write=write, buffers=True))

    def abort(self):
        self.txn.abort()

    def commit(self):
        self.txn.commit()

    def iter(self, k, reverse):
        return self.cursor(db=self.db)._iter_from(k, reverse)


register(KyotoEngine)
register(ListEngine)
register(LmdbEngine)
register(PlyvelEngine)
register(SkiplistEngine)
