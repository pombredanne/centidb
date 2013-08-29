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
import functools
import itertools
import operator
import os
import sys
import warnings

from centidb import encoders
from centidb import keycoder
from centidb.encoders import Encoder
from centidb.keycoder import Key

__all__ = ['Store', 'Encoder', 'Collection', 'Index', 'open']

ITEMGETTER_0 = operator.itemgetter(0)
ITEMGETTER_1 = operator.itemgetter(1)
ITEMGETTER_2 = operator.itemgetter(1)

KIND_TABLE = 0
KIND_INDEX = 1
KIND_ENCODER = 2
KIND_COUNTER = 3
KIND_STRUCT = 4


def open(engine, **kwargs):
    """Look up an engine class named by `engine`, instantiate it as
    `engine(**kwargs)` and wrap the result in a :py:class:`Store`. `engine`
    can either be a name from :py:mod:`centidb.engines` or a fully qualified
    name for a class in another module.

    ::

        >>> # Uses centidb.engines.SkiplistEngine
        >>> centidb.open('SkiplistEngine')

        >>> # Uses mymodule.BlarghEngine
        >>> centidb.open('mymodule.BlarghEngine')
    """
    if '.' not in engine:
        engine = 'centidb.engines.' + engine
    modname, _, classname = engine.rpartition('.')
    __import__(modname)
    return Store(getattr(sys.modules[modname], classname)(**kwargs))

def decode_offsets(s):
    getc = itertools.imap(ord, s).next
    ipos = [0]
    def wrapped():
        ipos[0] += 1
        return getc()

    pos = 0
    out = [0]
    for _ in xrange(keycoder.read_int(wrapped)):
        pos += keycoder.read_int(wrapped)
        out.append(pos)
    return out, ipos[0]

def next_greater(s):
    """Given a bytestring `s`, return the most compact bytestring that is
    greater than any value prefixed with `s`, but lower than any other value.

    ::

        >>> assert next_greater('') == '\\x00'
        >>> assert next_greater('\\x00') == '\\x01'
        >>> assert next_greater('\\xff') == '\\xff\\x00'
        >>> assert next_greater('\\x00\\x00') == '\\x00\\x01')
        >>> assert next_greater('\\x00\\xff') == '\\x01')
        >>> assert next_greater('\\xff\\xff') == '\\x01')

    """
    assert s
    # Based on the Plyvel `bytes_increment()` function.
    s2 = s.rstrip('\xff')
    return s2 and (s2[:-1] + chr(ord(s2[-1]) + 1))


def __kcmp(fn, o):
    return fn(o[1])
_kcmp = functools.partial(functools.partial, __kcmp)


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

        `txn`:
            Transaction to use, or ``None`` to indicate the default behaviour
            of the storage engine.

        `max`:
            Maximum number of index records to return.
    """
    def __init__(self, coll, info, func):
        self.coll = coll
        self.store = coll.store
        self.engine = self.store.engine
        self.info = info
        #: The index function.
        self.func = func
        self.prefix = keycoder.pack_int(self.store.prefix, info['idx'])

    def _iter(self, txn, key, lo, hi, reverse, max, include):
        if lo is None:
            lo = self.prefix
        else:
            lo = Key(lo).to_raw(self.prefix)

        if hi is None:
            hi = next_greater(self.prefix)
            if not (key and reverse):
                include = False
        else:
            # This is a broken mess. When doing reverse queries we must account
            # for the key tuple of the index key. next_greater() may fail if
            # the last byte of the index tuple is FF. Needs a better solution.
            hi = next_greater(Key(hi).to_raw(self.prefix)) # TODO WTF
            assert hi

        if key is not None:
            if reverse:
                hi = next_greater(key.to_raw(self.prefix)) # TODO
                assert hi
                include = False
            else:
                lo = Key(key).to_raw(self.prefix)

        if reverse:
            it = (txn or self.engine).iter(hi, True)
            pred = lo.__le__
        else:
            it = (txn or self.engine).iter(lo, False)
            pred = hi.__ge__ if include else hi.__gt__
        it = itertools.takewhile(pred, it)
        if max is not None:
            it = itertools.islice(it, max)
        for key, _ in it:
            if not key.startswith(self.prefix):
                break
            yield Key.from_raw(self.prefix, key)

    def count(self, args=None, lo=None, hi=None, max=None, include=False,
              txn=None):
        """Return a count of index entries matching the parameter
        specification."""
        return sum(1 for _ in self._iter(txn, args, lo, hi, 0, max, include))

    def pairs(self, args=None, lo=None, hi=None, reverse=None, max=None,
            include=False, txn=None):
        """Yield all (tuple, key) pairs in the index, in tuple order. `tuple`
        is the tuple returned by the user's index function, and `key` is the
        key of the matching record.
        
        `Note:` the yielded sequence is a list, not a tuple."""
        return self._iter(txn, args, lo, hi, reverse, max, include)

    def tups(self, args=None, lo=None, hi=None, reverse=None, max=None,
            include=False, txn=None):
        """Yield all index tuples in the index, in tuple order. The index tuple
        is the part of the entry produced by the user's index function, i.e.
        the index's natural "value"."""
        return itertools.imap(ITEMGETTER_0,
            self.pairs(args, lo, hi, reverse, max, include, txn))

    def keys(self, args=None, lo=None, hi=None, reverse=None, max=None,
            include=False, txn=None):
        """Yield all keys in the index, in tuple order."""
        return itertools.imap(ITEMGETTER_1,
            self.pairs(args, lo, hi, reverse, max, include, txn))

    def items(self, args=None, lo=None, hi=None, reverse=None, max=None,
              include=False, txn=None):
        """Yield all `(key, value)` items referred to by the index, in tuple
        order."""
        for _, key in self.pairs(args, lo, hi, reverse, max, include, txn):
            obj = self.coll.get(key, txn=txn)
            if obj:
                yield key, obj
            else:
                warnings.warn('stale entry in %r, requires rebuild' % (self,))

    def values(self, args=None, lo=None, hi=None, reverse=None, max=None,
               include=False, txn=None):
        """Yield all values referred to by the index, in tuple order."""
        it = self.items(args, lo, hi, reverse, max, include, txn, rec)
        return itertools.imap(ITEMGETTER_1, it)

    def find(self, args=None, lo=None, hi=None, reverse=None, include=False,
             txn=None, default=None):
        """Return the first matching record from the index, or None. Like
        ``next(itervalues(), default)``."""
        it = self.values(args, lo, hi, reverse, None, include, txn, rec)
        return next(it, default)

    def has(self, x, txn=None):
        """Return True if an entry with the exact tuple `x` exists in the
        index."""
        x = Key(x)
        tup, _ = next(self.pairs(x, txn=txn), (None, None))
        return tup == x

    def get(self, x, txn=None, default=None):
        """Return the first matching record from the index."""
        for tup in self.items(prefix=x, include=False, txn=txn):
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

        `key_func`, `txn_key_func`:
            Key generator for records about to be saved. `key_func` takes one
            argument, the record's value, and should return a tuple of
            primitive values that will become the record's key.  If the
            function returns a lone primitive value, it will be wrapped in a
            1-tuple.

            Alternatively, `txn_key_func` may be used to access the current
            transaction during key assignment. It is invoked as
            `txn_key_func(txn, value)`, where `txn`  is a reference to the
            active transaction, or :py:class:`Store`'s engine if no transaction
            was supplied.

            If neither function is given, keys are assigned using a
            transactional counter (like auto-increment in SQL). See
            `counter_name`.

        `encoder`:
            :py:class:`Encoder` used to serialize record values to bytestrings;
            defaults to ``PICKLE_ENCODER``.

        `counter_name`:
            Specifies the name of the :py:class:`Store` counter to use when
            generating auto-incremented keys. If unspecified, defaults to
            ``"key:<name>"``. Unused when `key_func` or `txn_key_func`
            are specified.
    """
    def __init__(self, store, info, key_func=None, txn_key_func=None,
                 encoder=None, counter_name=None):
        """Create an instance; see class docstring."""
        self.store = store
        self.engine = store.engine
        self.info = info
        self.prefix = keycoder.pack_int(self.store.prefix, info['idx'])
        if not (key_func or txn_key_func):
            counter_name = counter_name or ('key:%(name)s' % self.info)
            txn_key_func = lambda txn, _: store.count(counter_name, txn=txn)
            info['blind'] = True
        else:
            info.setdefault('blind', False)

        self.key_func = key_func
        self.txn_key_func = txn_key_func

        self.encoder = encoder or encoders.PICKLE_ENCODER
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
        self.store.set_info2(KIND_TABLE, self.info['name'], self.info)

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
        info = self.store.get_index_info(info_name, self.info['name'])
        index = Index(self, info, func)
        self.indices[name] = index
        return index

    def _logical_iter(self, it, reverse, prefix_s, prefix):
        #   * When iterating forward, if first yielded key lacks collection
        #     prefix, result of iteration is empty.
        #   * When iterating reverse, if first yielded key lacks collection
        #     prefix, discard, then behave as forward.
        #   * Records are discarded in the direction of iteration until
        #     startpred() or not self.prefix.
        #   * Records are yielded following startpred() until not endpred() or
        #     not self.prefix.
        prefix = prefix or ()
        tup = next(it, None)
        if tup and tup[0][:len(prefix_s)] == prefix_s:
            it = itertools.chain((tup,), it)
        for key, value in it:
            keys = keycoder.unpacks(prefix_s, key)
            if not keys:
                return

            lenk = len(keys)
            if lenk == 1:
                yield False, Key(*(prefix + keys[0])), self._decompress(value)
            else: # Batch record.
                offsets, dstart = decode_offsets(value)
                data = self._decompress(buffer(value, dstart))
                if reverse:
                    stop = -1
                    step = -1
                    i = lenk
                else:
                    stop = lenk
                    step = 1
                    i = 0
                while i != stop:
                    key = keys[-1 - i]
                    offs = offsets[i]
                    size = offsets[i+1] - offs
                    yield True, Key(*(prefix + key)), buffer(data, offs, size)
                    i += step

    # -----------------------------------------------------------
    # prefix: a
    #                          _iter(key=ad, reverse=True)
    #                         /_iter(hi=ad, reverse=True)
    #                        //
    #       aa     ab     aedc     af     ba
    #       ^             ^               ^
    #       |             |               |
    #       |             |               |
    #  .iter(prefix)      |        .iter(next_greater(prefix))
    #                 .iter(ad)
    # -----------------------------------------------------------
    # _iter(, , , False): lokey=prefix, hikey=ng(prefix)
    #                     startpred=lokey, endpred=
    def _iter(self, txn, key, lo, hi, prefix, reverse, max_, include, max_phys):
        if key is not None:
            key = Key(key)
            if reverse:
                hi = key
                include = True
            else:
                lo = key

        if prefix:
            prefix_s = Key(prefix).to_raw(self.prefix)
        else:
            prefix_s = self.prefix

        if lo is None:
            lokey = prefix_s
        else:
            lo = Key(lo)
            lokey = lo.to_raw(self.prefix)

        if hi is None:
            hikey = next_greater(prefix_s)
            include = False
        else:
            hi = Key(hi)
            hikey = hi.to_raw(self.prefix)

        if reverse:
            startkey = hikey
            startpred = hi and (hi.__lt__ if include else hi.__le__)
            endpred = lo and lo.__ge__
        else:
            startkey = lokey
            startpred = None
            endpred = hi and (hi.__ge__ if include else hi.__gt__)

        it = (txn or self.engine).iter(startkey, reverse)
        if max_phys is not None:
            it = itertools.islice(it, max_phys)

        it = self._logical_iter(it, reverse, prefix_s, prefix)
        if max_ is not None:
            it = itertools.islice(it, max_)
        if startpred:
            it = itertools.dropwhile(_kcmp(startpred), it)
        if endpred:
            it = itertools.takewhile(_kcmp(endpred), it)
        return it

    def _decompress(self, s):
        encoder = self.store.get_encoder(s[0])
        return encoder.unpack(buffer(s, 1))

    def _index_keys(self, key, obj):
        idx_keys = []
        for idx in self.indices.itervalues():
            lst = idx.func(obj)
            if lst:
                if type(lst) is not list:
                    lst = [lst]
                for idx_key in lst:
                    idx_keys.append(keycoder.packs(self.prefix, [idx_key, key]))
        return idx_keys

    def items(self, key=None, lo=None, hi=None, prefix=None, reverse=False,
              max=None, include=False, txn=None, raw=False):
        """Yield all `(key tuple, value)` tuples in key order."""
        it = self._iter(txn, key, lo, hi, prefix, reverse, max, include, None)
        if raw:
            return it
        return ((key, self.encoder.unpack(data)) for _, key, data in it)

    def keys(self, key=None, lo=None, hi=None, prefix=None, reverse=None,
             max=None, include=False, txn=None):
        """Yield key tuples in key order."""
        it = self._iter(txn, key, lo, hi, prefix, reverse, max, include, None)
        return itertools.imap(ITEMGETTER_1, it)

    def values(self, key=None, lo=None, hi=None, prefix=None, reverse=None,
               max=None, include=False, txn=None, raw=False):
        """Yield record values in key order."""
        it = self._iter(txn, key, lo, hi, prefix, reverse, max, include, None)
        if raw:
            return itertools.imap(ITEMGETTER_2, it)
        return (self.encoder.unpack(data) for _, key, data in it)

    def find(self, key=None, lo=None, hi=None, prefix=None, reverse=None,
             include=False, txn=None, raw=None, default=None):
        """Return the first matching record, or None. Like ``next(itervalues(),
        default)``."""
        it = self._iter(txn, key, lo, hi, prefix, reverse, max, include, None)
        for _, _, data in it:
            if raw:
                return data
            return self.encoder.unpack(data)
        return default

    def get(self, key, default=None, txn=None, raw=False):
        """Fetch a record given its key. If `key` is not a tuple, it is wrapped
        in a 1-tuple. If the record does not exist, return ``None`` or if
        `default` is provided, return it instead."""
        it = self._iter(txn, None, key, key, None, False, None, True, None)
        for _, _, data in it:
            if raw:
                return data
            return self.encoder.unpack(data)
        return default

    def batch(self, lo=None, hi=None, prefix=None, max_recs=None,
              max_bytes=None, max_keylen=None, preserve=True, packer=None,
              txn=None, max_phys=None, grouper=None):
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
                Encoding to use as compressor, defaults to PLAIN_PACKER.

            `txn`:
                Transaction to use, or ``None`` to indicate the default
                behaviour of the storage engine.

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
        txn = txn or self.engine
        packer = packer or encoders.PLAIN_PACKER
        it = self._iter(txn, None, lo, hi, prefix, False, None, True, max_phys)
        groupval = None
        items = []

        for batch, key, data in it:
            if preserve and batch:
                self._write_batch(txn, items, packer)
            else:
                txn.delete(key.to_raw(self.prefix))
                items.append((key, data))
                if max_bytes:
                    _, encoded = self._prepare_batch(items, packer)
                    if len(encoded) > max_bytes:
                        items.pop()
                        self._write_batch(txn, items, packer)
                        items.append((key, data))
                done = max_recs and len(items) == max_recs
                if (not done) and grouper:
                    val = grouper(self.encoder.unpack(data))
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
        phys = keycoder.packs(self.prefix, keytups)
        out = bytearray()

        if len(items) == 1:
            out.extend(packer_prefix)
            out.extend(packer.pack(items[0][1]))
        else:
            keycoder.write_int(len(items), out.append)
            for _, data in items:
                keycoder.write_int(len(data), out.append)
            out.extend(packer_prefix)
            concat = ''.join(data for _, data in items)
            out.extend(packer.pack(concat))
        return phys, str(out)

    def _split_batch(self, key, txn):
        """Find the batch `key` belongs to and split it, saving all records
        individually except for `key`."""
        assert False
        it = self._iter(txn, key, None, None, None, None, None, None)
        keys, data = next(it, (None, None))
        assert len(keys) > 1 and key in keys, \
            'Physical key missing: %r' % (key,)

        (txn or self.engine).delete(phys)
        objs = self.encoder.loads_many(self._decompress(data))
        for i, obj in enumerate(objs):
            this_key = keys[-(1 + i)]
            if this_key != key:
                self.put(obj, txn, key=this_key)

    def _assign_key(self, obj, txn):
        if self.txn_key_func:
            return self.txn_key_func(txn, obj)
        return self.key_func(obj)

    def put(self, rec, txn=None, packer=None, key=None, blind=False):
        """Create or overwrite a record.

            `rec`:
                The value to put; must be a value recognised by the
                collection's `encoder`.

            `txn`:
                Transaction to use, or ``None`` to indicate the default
                behaviour of the storage engine.

            `packer`:
                Encoding to use as compressor, defaults to PLAIN_PACKER.

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
        txn = txn or self.engine
        if key is None:
            key = self._assign_key(rec, txn)
        key = Key(key)
        packer = packer or encoders.PLAIN_PACKER
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

    def delete(self, key, txn=None):
        """Delete any existing record filed under `key`.
        """
        it = self._iter(txn, None, key, None, None, None, None, True, None)
        for batch, key_, data in it:
            if key != key_:
                break
            obj = self.encoder.unpack(data)
            if self.indices:
                for key in self._index_keys(key, obj):
                    (txn or self.engine).delete(key)
            if batch:
                self._split_batch(rec, txn)
            else:
                (txn or self.engine).delete(keycoder.packs(self.prefix, key))

class Store(object):
    """Represents access to the underlying storage engine, and manages
    counters.

        `prefix`:
            Prefix for all keys used by any associated object (record, index,
            counter, metadata). This allows the storage engine's key space to
            be shared amongst several users.
    """
    def __init__(self, engine, prefix=''):
        self.engine = engine
        self.prefix = prefix
        self._counter_key_cache = {}
        self._encoder_prefix = dict((e, keycoder.pack_int('', 1 + i))
                                    for i, e in enumerate(encoders._ENCODERS))
        self._prefix_encoder = dict((keycoder.pack_int('', 1 + i), e)
                                    for i, e in enumerate(encoders._ENCODERS))
        # ((kind, name, attr), value)
        self._meta = Collection(self, {'name': '\x00meta', 'idx': 9},
            encoder=encoders.KEY_ENCODER, key_func=lambda t: t[:3])
        self._colls = {}

    def get_info2(self, kind, name):
        items = list(self._meta.items(prefix=(kind, name)))
        return dict((a, v) for (n, k, a,), (v,) in items)

    def set_info2(self, kind, name, dct):
        for key, value in dct.iteritems():
            self._meta.put(value, key=(kind, name, key))

    def check_info2(self, old, new):
        for key, value in old.iteritems():
            if new.setdefault(key, value) != value:
                raise ValueError('attribute %r mismatch: %r vs %r' %\
                                 (key, value, new[key]))

    def add_collection(self, name, **kwargs):
        """Shorthand for `centidb.Collection(self, **kwargs)`."""
        old = self.get_info2(KIND_TABLE, name)
        encoder = kwargs.get('encoder', encoders.PICKLE_ENCODER)
        new = {'name': name, 'encoder': encoder.name}
        if old:
            self.check_info2(old or {}, new)
        else:
            new['idx'] = self.count('\x00collections_idx', init=10)
            self.set_info2(KIND_TABLE, name, new)
        return self[name]

    def __getitem__(self, name):
        try:
            return self._colls[name]
        except KeyError:
            info = self.get_info2(KIND_TABLE, name)
            if not info:
                raise
            self._colls[name] = Collection(self, info)
            return self._colls[name]

    def get_index_info(self, name, index_for):
        dct = self.get_info2(KIND_INDEX, name)
        if not dct:
            idx = self.count('\x00collections_idx', init=10)
            dct = {'idx': idx, 'index_for': index_for}
            self.set_info2(KIND_INDEX, name, dct)
        return dct

    def add_encoder(self, encoder):
        """Register an :py:class:`Encoder` so that :py:class:`Collection` can
        find it during decompression/unpacking."""
        try:
            return self._encoder_prefix[encoder]
        except KeyError:
            dct = self.get_info2(KIND_ENCODER, encoder.name)
            if not dct:
                idx = self.count('\x00encoder_idx', init=10)
                assert idx <= 240
                self.set_info2(KIND_ENCODER, encoder.name, {'idx': idx})
            self._encoder_prefix[encoder] = keycoder.pack_int('', idx)
            self._prefix_encoder[keycoder.pack_int('', idx)] = encoder
            return self._encoder_prefix[encoder]

    def get_encoder(self, prefix):
        """Get a registered :py:class:`Encoder` given its string prefix, or
        raise an error."""
        try:
            return self._prefix_encoder[prefix]
        except KeyError:
            it = self._meta.items(prefix=KIND_ENCODER)
            dct = dict((v, n) for (k, n, a), v in it if a == 'idx')
            idx = keycoder.unpack_int(prefix)
            raise ValueError('Missing encoder: %r / %d' % (dct.get(idx), idx))

    def count(self, name, n=1, init=1, txn=None):
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

            `txn`:
                Transaction to use, or ``None`` to indicate the default
                behaviour of the storage engine.
        """
        try:
            key = self._counter_key_cache[name]
        except KeyError:
            key = Key(KIND_COUNTER, name, None)
            self._counter_key_cache[name] = key

        value, = self._meta.get(key, default=(init,), txn=txn)
        if n:
            self._meta.put(value + n, key=key, txn=txn)
        return 0L + value

# Hack: disable speedups while testing or reading docstrings.
if os.path.basename(sys.argv[0]) not in ('sphinx-build', 'pydoc') and \
        os.getenv('CENTIDB_NO_SPEEDUPS') is None:
    try:
        from centidb._keycoder import decode_offsets
    except ImportError:
        pass
