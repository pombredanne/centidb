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

See http://acid.readthedocs.org/
"""

from __future__ import absolute_import
from __future__ import with_statement

import itertools
import logging
import operator
import struct
import threading
import warnings

import acid
from acid import events
from acid import encoders
from acid import errors
from acid import iterators
from acid import keylib

__all__ = ['Store', 'Collection', 'Index', 'open', 'abort', 'add_index']

ITEMGETTER_1 = operator.itemgetter(1)
ATTRGETTER_KEY = operator.attrgetter('key')
ATTRGETTER_DATA = operator.attrgetter('data')

KIND_TABLE = 0
KIND_INDEX = 1
KIND_ENCODER = 2
KIND_COUNTER = 3
KIND_STRUCT = 4

LOG = logging.getLogger('acid.core')


def dispatch(lst, *args):
    """Invoke each `func` in `lst` as `func(*args)`, logging any exceptions and
    removing any exception-raising function."""
    # reversed() avoids list copy.. list is mutated during iteration.
    for i in xrange(len(lst) - 1, -1, -1):
        try:
            if lst[i](*args) is False:
                lst.pop(i)
        except Exception:  # pylint: disable=W0703
            LOG.exception('While invoking %r%r', lst[i], args)
            lst.pop(i)


def bisect_func_right(x, lo, hi, func):
    """Bisect `func(i)`, returning an index such that consecutive values are
    greater than `x`. If `x` is present, the returned index is past its last
    occurrence. EOF is assumed if `func` returns None."""
    while lo < hi:
        mid = (lo + hi) // 2
        k = func(mid)
        if k is not None and x < k:
            hi = mid
        else:
            lo = mid + 1

    return lo


def abort():
    """Trigger a graceful abort of the active transaction."""
    raise errors.AbortError('')


def open(url, trace_path=None, txn_context=None):
    """Instantiate an engine described by `url` and return a new
    :py:class:`Store` wrapping it. See :ref:`engines` for supported URL
    schemes.

        `url`:
            Engine URL; may be prefixed with ``pkgname.modname+`` to cause a
            module to be imported before instantiating the engine. This allows
            third party engines to register via
            :py:func:`acid.engines.register` before the URL is parsed.

        `trace_path`:
            If specified, the engine is wrapped in a
            :py:class:`acid.engines.TraceEngine` to produce a complete log of
            interactions, written to `trace_path`.

        `txn_context`:
            If specified, use `txn_context` instead of the default
            :py:class:`acid.core.TxnContext` implementation.

    Example:

    .. code-block:: python

        # Uses acid.engines.SkiplistEngine
        store = acid.open('skiplist:/')

        # Cause "mypkg.acid" to be imported, then use "myengine:/".
        store = acid.open('mypkg.acid+myengine:/')
    """
    import acid.engines  # pylint: disable=W0621
    engine = acid.engines.from_url(url)
    if trace_path is not None:
        engine = acid.engines.TraceEngine(engine, trace_path=trace_path)
    if txn_context:
        return Store(engine, txn_context=txn_context(engine))
    else:
        return Store(engine)


def add_index(coll, name, func):
    """Associate an index with the collection. Index metadata will be
    created in the storage engine it it does not exist. Returns the `Index`
    instance describing the index. This method may only be invoked once for
    each unique `name` for each collection.

    .. note::
        Only index metadata is persistent. You must invoke
        :py:func:`acid.add_index` with the same arguments every time you create
        a :py:class:`Collection` instance.

    `name`:
        ASCII name for the index.

    `func`:
        Index key generation function accepting one argument, the record
        value. It should return a single primitive value, a tuple of
        primitive values, a list of primitive values, or a list of tuples
        of primitive values.

        .. caution::
            The index function must have no side-effects, as it may be
            invoked repeatedly.

        Example:

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
    # assert name not in coll.indices TODO
    info_name = 'index:%s:%s' % (coll.info['name'], name)
    info = coll.store.get_index_meta(info_name, coll.info['name'])
    index = Index(coll, info, func)
    coll.store._objs[name] = index
    return index


class Index(object):
    """Provides query and manipulation access to a single index on a
    Collection. You should not create this class directly, instead use
    :py:meth:`Collection.add_index` and the :py:attr:`Collection.indices`
    mapping.

    :py:meth:`Index.get` and the iteration methods take a common set of
    arguments that are described below:

        `args`:
            Prefix of the index entries to to be matched, or ``None`` or the
            empty tuple to indicate all index entries should be matched.

        `reverse`:
            If ``True``, iteration should begin with the last naturally ordered
            match returned first, and end with the first naturally ordered
            match returned last.

        `max`:
            Maximum number of index records to return.
    """

    def _index_keys(self, key, obj):
        """Generate a list of encoded keys representing index entries for `obj`
        existing under `key`."""
        res = self.func(obj)
        if type(res) is list:
            return [keylib.packs([ik, key], self.prefix) for ik in res]
        elif res is not None:
            return [keylib.packs([res, key], self.prefix)]

    def _coll_after_delete(self, key, rec):
        """Respond to after_delete() Collection event by removing any index
        entries for the deleted record."""
        keys = self._index_keys(key, rec)
        if keys:
            txn = self.store._txn_context.get()
            for k in keys:
                txn.delete(k)

    def _coll_after_create(self, key, rec):
        """Respond to after_create() Collection event by adding any index
        entries for the new record."""
        keys = self._index_keys(key, rec)
        if keys:
            txn = self.store._txn_context.get()
            for k in keys:
                txn.put(k, '')

    def _coll_after_replace(self, key, oldrec, newrec):
        """Respond to after_replace() Collection event by updating the set of
        index entries for the record."""
        oldkeys = self._index_keys(key, oldrec)
        newkeys = self._index_keys(key, newrec)
        if oldkeys != newkeys:
            txn = self.store._txn_context.get()
            for k in oldkeys:
                txn.delete(k)
            for k in newkeys:
                txn.put(k, '')

    def __repr__(self):
        klass = self.__class__.__name__
        return "<%s.%s %s>" % (__name__, klass, self.info['name'])

    def __init__(self, coll, info, func):
        self.coll = coll
        self.store = coll.store
        self.info = info
        #: The index function.
        self.func = func
        self.prefix = keylib.pack_int(info['idx'], self.store.prefix)

        events.after_delete(self._coll_after_delete, coll)
        events.after_create(self._coll_after_create, coll)
        events.after_replace(self._coll_after_replace, coll)

    def _iter(self, key, lo, hi, prefix, reverse, max, include):
        """Setup a woeful chain of iterators that yields index entries.
        """
        txn = self.store._txn_context.get()
        it = iterators.BasicIterator(txn, self.prefix)
        return iterators.from_args(it,
            key, lo, hi, prefix, reverse, max, include, None)

    def count(self, args=None, lo=None, hi=None, prefix=None, max=None,
              include=False):
        """Return a count of index entries matching the parameter
        specification."""
        it = self._iter(args, lo, hi, prefix, False, max, include)
        return sum(1 for _ in it)

    def pairs(self, args=None, lo=None, hi=None, prefix=None, reverse=None,
              max=None, include=False):
        """Yield all (tuple, key) pairs in the index, in tuple order. `tuple`
        is the tuple returned by the user's index function, and `key` is the
        key of the matching record.

        `Note:` the yielded sequence is a list, not a tuple."""
        it = self._iter(args, lo, hi, prefix, reverse, max, include)
        return (e.keys for e in it)

    def tups(self, args=None, lo=None, prefix=None, hi=None, reverse=None,
             max=None, include=False):
        """Yield all index tuples in the index, in tuple order. The index tuple
        is the part of the entry produced by the user's index function, i.e.
        the index's natural "value"."""
        it = self._iter(args, lo, hi, prefix, reverse, max, include)
        return (e.keys[0] for e in it)

    def keys(self, args=None, lo=None, hi=None, prefix=None, reverse=None,
             max=None, include=False):
        """Yield all keys in the index, in tuple order."""
        it = self._iter(args, lo, hi, prefix, reverse, max, include)
        return (e.keys[1] for e in it)

    def items(self, args=None, lo=None, hi=None, prefix=None, reverse=None,
              max=None, include=False, raw=False):
        """Yield all `(key, value)` items referred to by the index, in tuple
        order."""
        get = self.coll.get
        for e in self._iter(args, lo, hi, prefix, reverse, max, include):
            key = e.keys[1]
            obj = get(key, None, raw)
            if obj:
                yield key, obj
            else:
                warnings.warn('stale entry in %r, requires rebuild' % (self,))

    def values(self, args=None, lo=None, hi=None, prefix=None, reverse=None,
               max=None, include=False, raw=False):
        """Yield all values referred to by the index, in tuple order."""
        it = self.items(args, lo, hi, prefix, reverse, max, include, raw)
        return itertools.imap(ITEMGETTER_1, it)

    def find(self, args=None, lo=None, hi=None, prefix=None, reverse=None,
             include=False, raw=False, default=None):
        """Return the first matching record from the index, or None. Like
        ``next(itervalues(), default)``."""
        it = self.items(args, lo, hi, prefix, reverse, None, include, raw)
        for tup in it:
            return tup[1]
        return default

    def has(self, x):
        """Return ``True`` if an entry with the exact tuple `x` exists in the
        index."""
        it = self._iter(x, None, None, None, None, None, None)
        return next(it, None) is not None

    def get(self, x, default=None, raw=False):
        """Return the first matching record from the index."""
        for tup in self.items(x, raw=raw):
            return tup[1]
        return default


class BasicStrategy(object):
    """Access strategy for 'basic' ordered collections, i.e. those containing
    no batch records, or the metadata collection."""
    def __init__(self, prefix):
        self.prefix = prefix

    def get(self, txn, key):
        """Implement `get()` as `Engine.get(key)`."""
        value = txn.get(key.to_raw(self.prefix))
        if value:
            return bytes(value)

    def put(self, txn, key, data):
        """Implement `put()` as `Engine.put(key)`."""
        txn.put(key.to_raw(self.prefix), data)

    def replace(self, txn, key, data):
        """Implement `replace()` as `Engine.replace(key)`."""
        return txn.replace(key.to_raw(self.prefix), data)

    def delete(self, txn, key):
        """Implement `delete()` as `Engine.delete(key)`."""
        txn.delete(key.to_raw(self.prefix))

    def pop(self, txn, key):
        """Implement `pop()` as `Engine.pop(key)`."""
        return txn.pop(key.to_raw(self.prefix))

    def iter(self, txn):
        """Implement `iter()` using :py:class:`acid.iterators.BasicIterator`.
        """
        return iterators.BasicIterator(txn, self.prefix)


class BatchStrategy(object):
    """Access strategy for ordered collections containing batch records.
    """
    ITERATOR_CLASS = iterators.BatchIterator

    def __init__(self, prefix, store, compressor):
        self.prefix = prefix
        self.store = store
        self.compressor = compressor

    def get(self, txn, key):
        """Implement `get()` using a range query over `>= key`."""
        it = self.ITERATOR_CLASS(txn, self.prefix, self.compressor)
        it.set_exact(key)
        for res in it.forward():
            return bytes(res.data)  # TODO: buf dies at cursor exit

    def _prepare_batch(self, items):
        """Encode a list of records into their batch representation, returning
        a tuple of the encoded physical key and value."""
        keytups = [key for key, _ in reversed(items)]
        phys = keylib.packs(keytups, self.prefix)

        if len(items) == 1:
            out = items[0][1]
        else:
            out = bytearray()
            keylib.write_int(len(items), out.append, 0)
            for _, data in items:
                keylib.write_int(len(data), out.append, 0)
            concat = ''.join(data for _, data in items)
            out.extend(self.compressor.pack(concat))
        return phys, bytes(out)

    def batch(self, lo=None, hi=None, prefix=None, max_recs=None,
              max_bytes=None, max_keylen=None, preserve=True,
              max_phys=None, grouper=None):
        """
        Search the key range *lo..hi* for individual records, combining them
        into a batches.

        Returns `(found, made, last_key)` indicating the number of records
        combined, the number of batches produced, and the last key visited
        before `max_phys` was exceeded.

        Batch size is controlled via `max_recs` and `max_bytes`; at least one
        must not be ``None``. Larger sizes may cause pathological behaviour in
        the storage engine (for example, space inefficiency). Since batches are
        fully decompressed before any member may be accessed via
        :py:meth:`get() <Collection.get>` or :py:meth:`iteritems()
        <Collection.iteritems>`, larger sizes may slow decompression, waste IO
        bandwidth, and temporarily use more RAM.

            `lo`:
                Lowest search key.

            `hi`:
                Highest search key.

            `max_recs`:
                Maximum number of records contained by any single batch. When
                this count is reached, the current batch is saved and a new one
                is created.

            `max_bytes`:
                Maximum size in bytes of the batch record's value after
                compression, or ``None`` for no maximum size. When not
                ``None``, values are recompressed after each member is
                appended, in order to test if `max_bytes` has been reached. This
                is inefficient, but provides the best guarantee of final record
                size. Single records are skipped if they exceed this size when
                compressed individually.

            `preserve`:
                If ``True``, then existing batch records in the database are
                left untouched. When one is found within `lo..hi`, the
                currently building batch is finished and the found batch is
                skipped over.

                If ``False``, found batches are exploded and their members
                contribute to the currently building batch.

            `max_phys`:
                Maximum number of physical keys to visit in any particular
                call. A collection may be incrementally batched by repeatedly
                invoking :py:meth:`Collection.batch` with `max` set, and `lo`
                set to `last_key` of the previous run, until `found` returns
                ``0``. This allows batching to complete over several
                transactions without blocking other users.

            `grouper`:
                Specifies a grouping function used to decide when to avoid
                compressing unrelated records. The function is passed a
                record's value. A new batch is triggered each time the
                function's return value changes.

        """
        assert max_bytes or max_recs, 'max_bytes and/or max_recs is required.'

        txn = self.store._txn_context.get()
        it = self.ITERATOR_CLASS(txn, self.prefix, self.compressor)
        groupval = object()
        items = []

        for r in iterators.from_args(it, None, lo, hi, prefix,
                                     False, None, True, max_phys):
            if preserve and len(r.keys) > 1:
                self._write_batch(txn, items, self.compressor)
            else:
                txn.delete(keylib.packs(r.key, self.prefix))
                items.append((r.key, r.data))
                if max_bytes:
                    _, encoded = self._prepare_batch(items)
                    if len(encoded) > max_bytes:
                        items.pop()
                        self._write_batch(txn, items)
                        items.append((r.key, r.data))
                done = max_recs and len(items) == max_recs
                if (not done) and grouper:
                    val = grouper(self.encoder.unpack(r.key, r.data))
                    done = val != groupval
                    groupval = val
                if done:
                    self._write_batch(txn, items)
        self._write_batch(txn, items)

    def _write_batch(self, txn, items):
        if items:
            phys, data = self._prepare_batch(items)
            txn.put(phys, data)
            del items[:]

    def iter(self, txn):
        """Implement `iter()` using
        :py:class:`acid.iterators.BatchIterator`."""
        return self.ITERATOR_CLASS(txn, self.prefix, self.compressor)

    def pop(self, txn, key):
        """Implement `pop()` using a range query to find the single or batch
        record `key` belongs to and splitting it, saving all records
        individually except for `key`. Return the data for `key` if it existed,
        otherwise ``None``."""
        it = self.ITERATOR_CLASS(txn, self.prefix, self.compressor)
        it.set_lo(key)
        for res in it.forward():
            old = None
            if len(res.keys) == 1:
                if res.key == key:
                    old = res.data
                    txn.delete(it.phys_key)
            elif res.keys[0] >= key >= res.keys[-1]:
                txn.delete(it.phys_key)
                for key_, data in it.batch_items():
                    if key == key_:
                        old = data
                    else:
                        txn.put(key_.to_raw(self.prefix), data)
            return old

    def replace(self, txn, key, data):
        """Implement `replace()` by popping any existing value, then writing
        out the new record and returning the old value."""
        old = self.pop(txn, key)
        txn.put(key.to_raw(self.prefix), data)
        return old

    #: Alias for `pop()` (which satisfies the `delete()` interface).
    delete = pop
    #: Alias for `replace()` (which satisfies the `put()` interface).
    put = replace


class BatchV2Strategy(BatchStrategy):
    """Access strategy for ordered collections containing batch records.
    """
    ITERATOR_CLASS = iterators.BatchV2Iterator

    def _prepare_batch(self, items):
        if len(items) == 1:
            phys = keylib.Key(items[0][0]).to_raw(self.prefix)
            return phys, items[0][1]

        high_key_s = items[-1][0].to_raw()
        low_key_s = items[0][0].to_raw()
        cp_len = iterators.common_prefix_len(high_key_s, low_key_s)

        raw = bytearray(2 + (2 * (1 + len(items))))
        raw[0:2] = struct.pack('>HH', len(items), len(raw))
        for idx, (key, value) in enumerate(items):
            opos = 2 + (2 * idx)
            raw[opos:opos+2] = struct.pack('>H', len(raw))
            suffix = key.to_raw()[cp_len:]
            raw.append(len(suffix))
            raw.extend(suffix)
            raw.extend(value)

        opos = 2 + (2 * len(items))
        raw[opos:opos+2] = struct.pack('>H', len(raw))
        phys = keylib.packs([items[-1][0], items[0][0]], self.prefix)
        return phys, self.compressor.pack(bytes(raw))


class Collection(object):
    """Provides access to a record collection contained within a
    :py:class:`Store`, and ensures associated indices update consistently when
    changes are made.

        `store`:
            :py:class:`Store` the collection belongs to. If metadata for the
            collection does not already exist, it will be populated during
            construction.

        `name`:
            ASCII string used to identify the collection, aka. the key of the
            collection itself.

        `key_func`:
            Function invoked as `func(rec)` to produce a key for the record
            value about to be saved. It should return a tuple that will become
            the record's key. If the function returns a single value, it will
            be wrapped in a 1-tuple. If no function is given, keys are assigned
            using a counter (like auto-increment in SQL). See `counter_name`.

        `encoder`:
            :py:class:`acid.encoders.Encoder` used to serialize record values
            to bytestrings; defaults to :py:attr:`acid.encoders.JSON`.

        `counter_name`:
            Specifies the name of the :py:class:`Store` counter to use when
            generating auto-incremented keys. If unspecified, defaults to
            ``"key:<name>"``. Unused when `key_func` is specified.
    """
    def __init__(self, store, info, key_func=None, encoder=None,
                 compressor=None, counter_name=None):
        """Create an instance; see class docstring."""
        self.store = store
        self.engine = store.engine
        self.info = info

        prefix = keylib.pack_int(info['idx'], self.store.prefix)
        if info.get('strategy', 'batch') == 'batch':
            compressor = compressor or encoders.PLAIN
            self.strategy = BatchV2Strategy(prefix, store, compressor)
        else:
            assert info['strategy'] == 'basic'
            self.strategy = BasicStrategy(prefix)

        if key_func:
            self.key_func = key_func
        else:
            counter_name = counter_name or ('key:%(name)s' % self.info)
            self.key_func = lambda _: store.count(counter_name)

        self.encoder = encoder or encoders.JSON
        self._on_update = []
        self._after_create = []
        self._after_replace = []
        self._after_delete = []
        self._after_update = []
        # Copied verbatim to allow proxying.
        self._on_commit = store._on_commit
        self._after_commit = store._after_commit
        self._after_abort = store._after_abort

    def _listen(self, name, func):
        """Subscribe `func` to the event named `name`."""
        lst = getattr(self, '_' + name, None)
        if lst is None:
            raise TypeError('%r has no %r event' % (self, name))
        lst.append(func)

    def __repr__(self):
        klass = self.__class__.__name__
        return "<%s.%s %s>" % (__name__, klass, self.info['name'])

    def _iter(self, key, lo, hi, prefix, reverse, max_, include, max_phys):
        it = self.strategy.iter(self.store._txn_context.get())
        return iterators.from_args(it, key, lo, hi, prefix, reverse,
                                   max_, include, max_phys)

    def items(self, key=None, lo=None, hi=None, prefix=None, reverse=False,
              max=None, include=False, raw=False):
        """Yield all `(key tuple, value)` tuples in key order."""
        it = self._iter(key, lo, hi, prefix, reverse, max, include, None)
        if raw:
            return ((r.key, bytes(r.data)) for r in it)
        return ((r.key, self.encoder.unpack(r.key, r.data)) for r in it)

    def keys(self, key=None, lo=None, hi=None, prefix=None, reverse=None,
             max=None, include=False):
        """Yield key tuples in key order."""
        it = self._iter(key, lo, hi, prefix, reverse, max, include, None)
        return itertools.imap(ATTRGETTER_KEY, it)

    def values(self, key=None, lo=None, hi=None, prefix=None, reverse=None,
               max=None, include=False, raw=False):
        """Yield record values in key order."""
        it = self._iter(key, lo, hi, prefix, reverse, max, include, None)
        if raw:
            return (bytes(r.data) for r in it)
        return (self.encoder.unpack(r.key, r.data) for r in it)

    def find(self, key=None, lo=None, hi=None, prefix=None, reverse=None,
             include=False, raw=None, default=None):
        """Return the first matching record, or None. Like ``next(itervalues(),
        default)``."""
        it = self._iter(key, lo, hi, prefix, reverse, None, include, None)
        for r in it:
            if raw:
                return bytes(r.data)
            return self.encoder.unpack(r.key, r.data)
        return default

    def findkey(self, key=None, lo=None, hi=None, prefix=None, reverse=None,
                include=False, raw=None, default=None):
        """Return the first matching key, or None. Like ``next(iterkeys(),
        default)``."""
        it = self._iter(key, lo, hi, prefix, reverse, None, include, None)
        for r in it:
            return r.key
        return default

    def get(self, key, default=None, raw=False):
        """Fetch a record given its key. If `key` is not a tuple, it is wrapped
        in a 1-tuple. If the record does not exist, return ``None`` or if
        `default` is provided, return it instead."""
        key = keylib.Key(key)
        data = self.strategy.get(self.store._txn_context.get(), key)
        if data:
            if raw:
                return data
            return self.encoder.unpack(key, data)
        return default

    def put(self, rec, key=None):
        """Create or overwrite a record.

            `rec`:
                The value to put; must be a value recognised by the
                collection's `encoder`.

            `key`:
                Optionaly use this exact key, otherwise assign a key using the
                collection's key function.
        """
        txn = self.store._txn_context.get()
        if key is None:
            key = self.key_func(rec)
        key = keylib.Key(key)
        dispatch(self._on_update, key, rec)
        new = self.encoder.pack(rec)

        # If a listener is registered that must observe the prior record value,
        # use replace(), which may result in a slow path on engines like
        # LevelDB. Otherwise use put(), which does not observe the old value.
        if self._after_replace or self._after_create:
            old = self.strategy.replace(txn, key, new)
            if old:
                oldrec = self.encoder.unpack(key, old)
                dispatch(self._after_replace, key, oldrec, rec)
            elif self._after_create:
                dispatch(self._after_create, key, rec)
        else:
            self.strategy.put(txn, key, new)

        dispatch(self._after_update, key, rec)
        return key

    def delete(self, key):
        """Delete any existing record filed under `key`.
        """
        key = keylib.Key(key)
        txn = self.store._txn_context.get()
        if self._after_delete:
            data = self.strategy.pop(txn, key)
            if data:
                obj = self.encoder.unpack(key, data)
                dispatch(self._after_delete, key, obj)
        else:
            self.strategy.delete(txn, key)


class TxnContext(object):
    """Abstraction for maintaining the local context's transaction. This
    implementation uses TLS.
    """
    def __init__(self, engine):
        self.engine = engine
        self.local = threading.local()

    def mode(self):
        """Return a tristate indicating the active transaction mode: ``None`
        means if no transaction is active, ``False`` if a read-only transaction
        is active, or ``True`` if a write transaction is active."""
        return getattr(self.local, 'write', None)

    def begin(self, write=False):
        if getattr(self.local, 'mode', None) is not None:
            raise errors.TxnError('Transaction already active for this thread')
        self.local.write = write
        return self

    def __enter__(self):
        self.local.txn = self.engine.begin(write=self.local.write)

    def __exit__(self, exc_type, exc_value, traceback):
        handled = True
        if exc_type:
            self.local.txn.abort()
            dispatch(self._after_abort)
            handled = exc_type is errors.AbortError
        else:
            dispatch(self._on_commit)
            self.local.txn.commit()
            dispatch(self._after_commit)
        del self.local.txn
        del self.local.write
        return handled

    def get(self):
        txn = getattr(self.local, 'txn', None)
        if txn:
            return txn
        raise errors.TxnError('Transactions *must* be wrapped in a with: '
                              'block to ensure proper destruction.')


class GeventTxnContext(TxnContext):
    """Like TxnContext except using gevent.local.local().
    """
    def __init__(self, engine):
        TxnContext.__init__(self, engine)
        import gevent.local
        self.local = gevent.local.local()


class Store(object):
    """Represents access to the underlying storage engine, and manages
    counters.

        `txn_context`:
            If not ``None``, override the default
            :py:class:`acid.core.TxnContext` implementation to provide
            request-local storage of the active database transaction. The
            default implementation uses TLS.

        `prefix`:
            Prefix for all keys used by any associated object (record, index,
            counter, metadata). This allows the storage engine's key space to
            be shared amongst several users.
    """
    def __init__(self, engine, txn_context=None, prefix=''):
        self.engine = engine
        self.prefix = prefix
        self._txn_context = txn_context or TxnContext(engine)
        self._on_commit = []
        self._after_commit = []
        self._after_abort = []
        # TODO HACK
        self._txn_context._on_commit = self._on_commit
        self._txn_context._after_commit = self._after_commit
        self._txn_context._after_abort = self._after_abort
        self.begin = self._txn_context.begin
        self._counter_key_cache = {}
        self._encoder_prefix = dict((e, keylib.pack_int(1 + i))
                                    for i, e in enumerate(encoders._ENCODERS))
        self._prefix_encoder = dict((keylib.pack_int(1 + i), e)
                                    for i, e in enumerate(encoders._ENCODERS))

        # ((kind, name, attr), value)
        meta_info = {
            'name': '\x00meta',
            'idx': 9,
            'strategy': 'basic'
        }
        self._meta = Collection(self, meta_info, encoder=encoders.KEY,
                                key_func=lambda t: t[:3])
        self._objs = {}

    def begin(self, write=False):  # pylint: disable=E0202
        """Return a context manager that starts a database transaction when it
        is entered.

        ::

            with store.begin(write=True):
                store['people'].put('me')

        If the execution block completes without raising an exception, then the
        transaction will be committed. Otherwise it will be aborted, and the
        exception will be propagated as normal.

        The :py:func:`acid.abort` function may be used anywhere on the stack to
        gracefully abort the current transaction, and return control to below
        with ``with:`` block without causing an exception to be raised.
        """
        return self._txn_context.begin(write)

    def in_txn(self, func, write=False):
        """Execute `func()` inside a transaction, and return its return value.
        If a transaction is already active, `func()` runs inside the current
        transaction."""
        mode = self._txn_context.mode()
        if mode is None:
            with self._txn_context.begin(write=write):
                return func()
        elif write and not mode:
            raise errors.TxnError('attempted write in a read-only transaction')
        else:
            return func()

    def get_meta(self, kind, name):
        """Fetch a dictionary of metadata for the named object. `kind` may be
        any of the following :py:mod:`acid.core` constants:

            ``acid.core.KIND_TABLE``
            ``acid.core.KIND_INDEX``
            ``acid.core.KIND_ENCODER``
            ``acid.core.KIND_COUNTER``
            ``acid.core.KIND_STRUCT``
        """
        func = lambda: list(self._meta.items(prefix=(kind, name)))
        return dict((a, v) for (n, k, a,), (v,) in self.in_txn(func))

    def set_meta(self, kind, name, dct):
        """Replace the stored metadata for `name` using `dct`."""
        def _set_meta_txn():
            for key in list(self._meta.keys(prefix=(kind, name))):
                self._meta.delete(key)
            for key, value in dct.iteritems():
                self._meta.put(value, key=(kind, name, key))
        return self.in_txn(_set_meta_txn)

    def get_index_meta(self, name, index_for):
        dct = self.get_meta(KIND_INDEX, name)
        if not dct:
            idx = self.count('\x00collections_idx', init=10)
            dct = {'name': name, 'idx': idx, 'index_for': index_for}
            self.set_meta(KIND_INDEX, name, dct)
        return dct

    def rename_collection(self, old, new):
        """Rename the collection named `old` to `new`. Any existing
        :py:class:`Collection` instances will be updated. Raises
        :py:class:`acid.errors.NameInUse` on error."""
        if self.get_meta(KIND_TABLE, new):
            raise errors.NameInUse('collection %r already exists.' % (new,))
        coll = self[old]
        info = self.get_info(KIND_TABLE, old)
        info['name'] = new

    def add_collection(self, name, **kwargs):
        """Shorthand for `acid.Collection(self, **kwargs)`."""
        old = self.get_meta(KIND_TABLE, name)
        encoder = kwargs.get('encoder', encoders.JSON)
        new = {'name': name, 'encoder': encoder.name}
        if old:
            for key, value in old.iteritems():
                if new.setdefault(key, value) != value:
                    raise errors.ConfigError('attribute %r: %r != %r' %\
                                             (key, value, new[key]))
        else:
            new['idx'] = self.count('\x00collections_idx', init=10)
            self.set_meta(KIND_TABLE, name, new)
        return self.__getitem__(name, kwargs)

    def __getitem__(self, name, kwargs={}):
        try:
            return self._objs[name]
        except KeyError:
            info = self.get_meta(KIND_TABLE, name)
            if not info:
                raise
            self._objs[name] = Collection(self, info, **kwargs)
            return self._objs[name]

    def add_encoder(self, encoder):
        """Register an :py:class:`acid.encoders.Encoder` so that
        :py:class:`Collection` can find it during decompression/unpacking."""
        try:
            return self._encoder_prefix[encoder]
        except KeyError:
            dct = self.get_meta(KIND_ENCODER, encoder.name)
            idx = dct.get('idx')
            if not dct:
                idx = self.count('\x00encoder_idx', init=10)
                assert idx <= 240
                self.set_meta(KIND_ENCODER, encoder.name, {'idx': idx})
            self._encoder_prefix[encoder] = keylib.pack_int(idx)
            self._prefix_encoder[keylib.pack_int(idx)] = encoder
            return self._encoder_prefix[encoder]

    def get_encoder(self, prefix):
        """Get a registered :py:class:`acid.encoders.Encoder` given its string
        prefix, or raise an error."""
        try:
            return self._prefix_encoder[prefix]
        except KeyError:
            it = self._meta.items(prefix=KIND_ENCODER)
            dct = dict((v, n) for (k, n, a), v in it if a == 'idx')
            idx = keylib.unpack_int(prefix)
            raise errors.ConfigError('Missing encoder: %r / %d' %\
                                     (dct.get(idx), idx))

    def count(self, name, n=1, init=1):
        """Increment a counter and return its previous value. The counter is
        created if it doesn't exist.

            `name`:
                Name of the counter. Names beginning with ``"\\x00"`` are
                reserved by the implementation.

            `n`:
                Number to add to the counter. If ``0`` or ``None``, return the
                counter's value without incrementing it.

            `init`:
                Initial value to give counter if it doesn't exist.
        """
        try:
            key = self._counter_key_cache[name]
        except KeyError:
            key = keylib.Key(KIND_COUNTER, name, None)
            self._counter_key_cache[name] = key

        value, = self._meta.get(key, default=(init,))
        if n:
            self._meta.put(value + n, key=key)
        return 0L + value
