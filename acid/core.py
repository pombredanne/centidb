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
import functools
import itertools
import operator
import os
import sys
import threading
import warnings

import acid
from acid import encoders
from acid import errors
from acid import iterators
from acid import keylib

__all__ = ['Store', 'Collection', 'Index', 'open', 'abort']

ITEMGETTER_1 = operator.itemgetter(1)
ATTRGETTER_KEY = operator.attrgetter('key')
ATTRGETTER_DATA = operator.attrgetter('data')

KIND_TABLE = 0
KIND_INDEX = 1
KIND_ENCODER = 2
KIND_COUNTER = 3
KIND_STRUCT = 4


def abort():
    """Trigger a graceful abort of the active transaction."""
    raise errors.AbortError('')

def open(url, trace_path=None, txn_context=None, **kwargs):
    """Look up an engine class named by `engine`, instantiate it as
    `engine(**kwargs)` and wrap the result in a :py:class:`Store`. `engine`
    can either be a name from :py:mod:`acid.engines` or a fully qualified
    name for a class in another module.

    ::

        >>> # Uses acid.engines.SkiplistEngine
        >>> acid.open('skiplist:/')

        >>> # Import mymodule.
        >>> acid.open('mypkg.acid+myengine:/')

    If `trace_path` is specified, then the underlying engine is wrapped in a
    :py:class:`acid.engines.TraceEngine` to produce a complete log of
    interactions with the external engine, written to `trace_path`.

    If `txn_context` is speciied, then use `txn_context` instead of the default
    :py:class:`acid.core.TxnContext` implementation.
    """
    import acid.engines
    engine = acid.engines.from_url(url)
    if trace_path is not None:
        engine = acid.engines.TraceEngine(engine, trace_path=trace_path)
    return Store(engine, txn_context=txn_context(engine))

def decode_offsets(s):
    """Given a string, decode an array of offsets at the start of the string. A
    varint indicates the length of the array, followed by one varint for each
    element, which is a delta from the previous element, starting at 0.
    """
    ba = bytearray(s)
    length = len(ba)
    count, pos = keylib.read_int(ba, 0, length, 0)

    out = [0]
    for _ in xrange(count):
        i, pos = keylib.read_int(ba, pos, length, 0)
        out.append(out[-1] + i)
    return out, pos


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
    def __init__(self, coll, info, func):
        self.coll = coll
        self.store = coll.store
        self.info = info
        #: The index function.
        self.func = func
        self.prefix = keylib.pack_int(info['idx'], self.store.prefix)

    def _iter(self, key, lo, hi, prefix, reverse, max, include):
        """Setup a woeful chain of iterators that yields index entries.
        """
        txn = self.store._txn_context.get()
        it = iterators.RangeIterator(txn, self.prefix)
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
            to bytestrings; defaults to :py:attr:`acid.encoders.PICKLE`.

        `counter_name`:
            Specifies the name of the :py:class:`Store` counter to use when
            generating auto-incremented keys. If unspecified, defaults to
            ``"key:<name>"``. Unused when `key_func` is specified.
    """
    def __init__(self, store, info, key_func=None, encoder=None,
                 counter_name=None):
        """Create an instance; see class docstring."""
        self.store = store
        self.engine = store.engine
        self.info = info
        self.prefix = keylib.pack_int(info['idx'], self.store.prefix)
        if not key_func:
            counter_name = counter_name or ('key:%(name)s' % self.info)
            key_func = lambda _: store.count(counter_name)
            info['blind'] = True
        else:
            info.setdefault('blind', False)

        self.key_func = key_func

        self.encoder = encoder or encoders.PICKLE
        self.encoder_prefix = self.store.add_encoder(self.encoder)
        #: Dict mapping indices added using :py:meth:`Collection.add_index` to
        #: :py:class:`Index` instances representing them.
        #:
        #: ::
        #:
        #:      idx = coll.add_index('some index', lambda v: v[0])
        #:      assert coll.indices['some index'] is idx
        self.indices = {}

    def set_blind(self, blind):
        """Set the default blind write behaviour to `blind`.. If ``True``,
        indicates the key function never reassigns the same key twice, for
        example when using a time-based key. In this case, checks for old
        records with the same key may be safely skipped, significantly
        improving performance.

        This mode is always active when a collection has no indices defined,
        and does not need explicitly set in that case.
        """
        self.info['blind'] = bool(blind)
        self.store.set_meta(KIND_TABLE, self.info['name'], self.info)

    def add_index(self, name, func):
        """Associate an index with the collection. Index metadata will be
        created in the storage engine it it does not exist. Returns the `Index`
        instance describing the index. This method may only be invoked once for
        each unique `name` for each collection.

        .. note::
            Only index metadata is persistent. You must invoke
            :py:meth:`Collection.add_index` with the same arguments every time
            you create a :py:class:`Collection` instance.

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
        assert name not in self.indices
        info_name = 'index:%s:%s' % (self.info['name'], name)
        info = self.store.get_index_meta(info_name, self.info['name'])
        index = Index(self, info, func)
        self.indices[name] = index
        return index

    def _iter(self, key, lo, hi, prefix, reverse, max_, include, max_phys):
        txn = self.store._txn_context.get()
        it = iterators.BatchRangeIterator(txn, self.prefix,
                                          self.store.get_encoder)
        return iterators.from_args(it, key, lo, hi, prefix, reverse,
                                   max_, include, max_phys)

    def __getitem__(self, index):
        return self.indices[index]

    def _index_keys(self, key, obj):
        """Generate a list of encoded keys representing index entries for `obj`
        existing under `key`."""
        idx_keys = []
        for idx in self.indices.itervalues():
            res = idx.func(obj)
            if type(res) is list:
                for ikey in res:
                    idx_keys.append(keylib.packs([ikey, key], idx.prefix))
            elif res is not None:
                idx_keys.append(keylib.packs([res, key], idx.prefix))
        return idx_keys

    def items(self, key=None, lo=None, hi=None, prefix=None, reverse=False,
              max=None, include=False, raw=False):
        """Yield all `(key tuple, value)` tuples in key order."""
        it = self._iter(key, lo, hi, prefix, reverse, max, include, None)
        if raw:
            return ((r.key, r.data) for r in it)
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
            return itertools.imap(ATTRGETTER_DATA, it)
        return (self.encoder.unpack(r.key, r.data) for r in it)

    def find(self, key=None, lo=None, hi=None, prefix=None, reverse=None,
             include=False, raw=None, default=None):
        """Return the first matching record, or None. Like ``next(itervalues(),
        default)``."""
        it = self._iter(key, lo, hi, prefix, reverse, None, include, None)
        for r in it:
            if raw:
                return r.data
            return self.encoder.unpack(r.key, r.data)
        return default

    def get(self, key, default=None, raw=False):
        """Fetch a record given its key. If `key` is not a tuple, it is wrapped
        in a 1-tuple. If the record does not exist, return ``None`` or if
        `default` is provided, return it instead."""
        it = self._iter(key, None, None, None, None, None, None, None)
        for r in it:
            if raw:
                return r.data
            return self.encoder.unpack(r.key, r.data)
        return default

    def batch(self, lo=None, hi=None, prefix=None, max_recs=None,
              max_bytes=None, max_keylen=None, preserve=True, packer=None,
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
                appended, in order to test if `maxbytes` has been reached. This
                is inefficient, but provides the best guarantee of final record
                size. Single records are skipped if they exceed this size when
                compressed individually.

            `max_keylen`:
                Maximum size in bytes of the batch record's key part, or
                ``None`` for no maximum size.

            `preserve`:
                If ``True``, then existing batch records in the database are
                left untouched. When one is found within `lo..hi`, the
                currently building batch is finished and the found batch is
                skipped over.

                If ``False``, found batches are exploded and their members
                contribute to the currently building batch.

            `packer`:
                Encoding to use as compressor, defaults to
                :py:attr:`acid.encoders.PLAIN`.

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
        assert max_keylen is None, 'max_keylen is not implemented.'
        assert max_bytes or max_recs, 'max_bytes and/or max_recs is required.'
        txn = self.store._txn_context.get()
        packer = packer or encoders.PLAIN
        it = self._iter(None, lo, hi, prefix, False, None, True, max_phys)
        groupval = None
        items = []

        for r in it:
            if preserve and len(r.keys) > 1:
                self._write_batch(txn, items, packer)
            else:
                txn.delete(keylib.packs(r.key, self.prefix))
                items.append((r.key, r.data))
                if max_bytes:
                    _, encoded = self._prepare_batch(items, packer)
                    if len(encoded) > max_bytes:
                        items.pop()
                        self._write_batch(txn, items, packer)
                        items.append((r.key, r.data))
                done = max_recs and len(items) == max_recs
                if (not done) and grouper:
                    val = grouper(self.encoder.unpack(r.key, r.data))
                    done = val != groupval
                    groupval = val
                if done:
                    self._write_batch(txn, items, packer)
        self._write_batch(txn, items, packer)

    def _write_batch(self, txn, items, packer):
        if items:
            phys, data = self._prepare_batch(items, packer)
            txn.put(phys, data)
            del items[:]

    def _prepare_batch(self, items, packer):
        packer_prefix = self.store._encoder_prefix.get(packer)
        if not packer_prefix:
            packer_prefix = self.store.add_encoder(packer)
        keytups = [key for key, _ in reversed(items)]
        phys = keylib.packs(keytups, self.prefix)
        out = bytearray()

        if len(items) == 1:
            out.extend(packer_prefix)
            out.extend(packer.pack(items[0][1]))
        else:
            keylib.write_int(len(items), out.append, 0)
            for _, data in items:
                keylib.write_int(len(data), out.append, 0)
            out.extend(packer_prefix)
            concat = ''.join(data for _, data in items)
            out.extend(packer.pack(concat))
        return phys, str(out)

    def _split_batch(self, key):
        """Find the batch `key` belongs to and split it, saving all records
        individually except for `key`."""
        key = keylib.Key(key)
        txn = self.store._txn_context.get()
        it = iterators.BatchRangeIterator(txn, self.prefix,
                                          self.store.get_encoder)
        it.set_exact(key)
        assert next(it.forward(), None), 'Physical key missing: %r' % (key,)

        txn.delete(it.phys_key)
        packer = encoders.PLAIN
        packer_prefix = self.store._encoder_prefix.get(encoders.PLAIN)
        for key_, data in it.batch_items():
            if key != key_:
                txn.put(key_.to_raw(self.prefix), packer_prefix + data)

    def put(self, rec, packer=None, key=None, blind=False):
        """Create or overwrite a record.

            `rec`:
                The value to put; must be a value recognised by the
                collection's `encoder`.

            `packer`:
                Encoding to use as compressor, defaults to
                :py:attr:`acid.encoders.PLAIN`.

            `key`:
                If specified, overrides the use of collection's key function
                and forces a specific key. Use with caution.

            `blind`:
                If ``True``, skip checks for any old record assigned the same
                key. Automatically enabled when a collection has no indices, or
                when `blind=` is passed to :py:class:`Collection`'s
                constructor.

                While this significantly improves performance, enabling it for
                a collection with indices and in the presence of old records
                with the same key will lead to inconsistent indices.
                :py:meth:`Index.iteritems` will issue a warning and discard
                obsolete keys when this is detected, however other index
                methods will not.
        """
        txn = self.store._txn_context.get()
        if key is None:
            key = self.key_func(rec)
        key = keylib.Key(key)
        packer = packer or encoders.PLAIN
        packer_prefix = self.store._encoder_prefix.get(packer)
        if not packer_prefix:
            packer_prefix = self.store.add_encoder(packer)

        if self.indices:
            if not (blind or self.info['blind']):
                self.delete(key)
            for index_key in self._index_keys(key, rec):
                txn.put(index_key, '')

        txn.put(key.to_raw(self.prefix),
                packer_prefix + packer.pack(self.encoder.pack(rec)))
        return key

    def delete(self, key):
        """Delete any existing record filed under `key`.
        """
        key = keylib.Key(key)
        it = self._iter(key, None, None, None, None, None, None, None)
        for r in it:
            txn = self.store._txn_context.get()
            if self.indices:
                obj = self.encoder.unpack(r.key, r.data)
                for idx_key in self._index_keys(key, obj):
                    txn.delete(idx_key)
            if len(r.keys) > 1:
                self._split_batch(key)
            else:
                txn.delete(key.to_raw(self.prefix))


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
            raise errors.TxnError('Transaction already active for this thread.')
        self.local.write = write
        return self

    def __enter__(self):
        self.local.txn = self.engine.begin(write=self.local.write)

    def __exit__(self, exc_type, exc_value, traceback):
        handled = True
        if exc_type:
            self.local.txn.abort()
            handled = exc_type is errors.AbortError
        else:
            self.local.txn.commit()
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
        import gevent.local
        self.engine = engine
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
        self.begin = self._txn_context.begin
        self._counter_key_cache = {}
        self._encoder_prefix = dict((e, keylib.pack_int(1 + i))
                                    for i, e in enumerate(encoders._ENCODERS))
        self._prefix_encoder = dict((keylib.pack_int(1 + i), e)
                                    for i, e in enumerate(encoders._ENCODERS))
        # ((kind, name, attr), value)
        self._meta = Collection(self, {'name': '\x00meta', 'idx': 9},
            encoder=encoders.KEY, key_func=lambda t: t[:3])
        self._colls = {}

    def begin(self, write=False):
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
        return self._txn_context.begin()

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
            dct = {'idx': idx, 'index_for': index_for}
            self.set_meta(KIND_INDEX, name, dct)
        return dct

    def rename_collection(self, old, new):
        """Rename the collection named `old` to `new`. Any existing
        :py:class:`Collection` instances will be updated. Raises
        :py:class:`acid.errors.NameInUse` on error."""
        if self.get_meta(KIND_TABLE, name):
            raise errors.NameInUse('collection %r already exists.' % (new,))
        coll = self[old]
        info = self.get_info(KIND_TABLE, name)
        info['name'] = new

    def add_collection(self, name, **kwargs):
        """Shorthand for `acid.Collection(self, **kwargs)`."""
        old = self.get_meta(KIND_TABLE, name)
        encoder = kwargs.get('encoder', encoders.PICKLE)
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
            return self._colls[name]
        except KeyError:
            info = self.get_meta(KIND_TABLE, name)
            if not info:
                raise
            self._colls[name] = Collection(self, info, **kwargs)
            return self._colls[name]

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

if acid._use_speedups:
    try:
        from acid._keylib import decode_offsets
    except ImportError:
        pass
