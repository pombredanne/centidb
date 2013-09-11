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
Iterator implementations.
"""

from __future__ import absolute_import
from acid import keylib


class RangeIterator(object):
    """Provides bidirectional iteration of a range of keys.
    """
    lo = None
    hi = None
    lo_pred = bool
    hi_pred = bool
    remain = -1

    def __init__(self, engine, prefix):
        self.engine = engine
        self.prefix = prefix

    def set_lo(self, key, closed=True):
        self.lo = keylib.Key(key)
        self.lo_pred = getattr(self.lo, ('__lt__', '__le__')[closed])

    def set_hi(self, key, closed=False):
        self.hi = keylib.Key(key)
        self.hi_pred = getattr(self.hi, ('__gt__', '__ge__')[closed])

    def set_max(self, max_):
        self.remain = max_

    def set_exact(self, key):
        key = keylib.Key(key)
        self.lo = key
        self.hi = key
        self.lo_pred = key.__le__
        self.hi_pred = key.__ge__

    def _step(self, n):
        """Step the iterator once, saving the new key and data. Returns True if
        the iterator is still within the bounds of the collection prefix,
        otherwise False."""
        if self.remain or not n:
            self.remain -= n
            self.keys_raw, self.data = next(self.it, ('', ''))
            self.keys = keylib.KeyList.from_raw(self.prefix, self.keys_raw)
            return self.keys is not None

    def forward(self):
        if self.lo is None:
            key = self.prefix
        else:
            key = self.lo.to_raw(self.prefix)
        self.it = self.engine.iter(key, False)
        # Fetch the first key. If _step() returns false, then first key is
        # beyond collection prefix. Cease iteration.
        go = self._step(1)

        # When lo(closed=False), skip the start key.
        if go and not self.lo_pred(self.keys[0]):
            go = self._step(0)
        while go and self.hi_pred(self.keys[0]):
            yield self
            go = self._step(1)

    def reverse(self):
        if self.hi is None:
            key = keylib.next_greater(self.prefix)
        else:
            key = self.hi.to_raw(self.prefix)

        self.it = self.engine.iter(key, True)
        # We may have seeked to first record of next prefix, so skip first
        # returned result.
        go = self._step(1)
        if not go:
            go = self._step(0)

        # We should now be positioned on the first record >= self.hi. When
        # hi(closed=False), skip the first result.
        if go and not self.hi_pred(self.keys[0]):
            go = self._step(0)
        while go and self.lo_pred(self.keys[0]):
            yield self
            go = self._step(1)


class PrefixIterator(RangeIterator):
    def set_prefix(self, key):
        self.set_lo(key, True)
        self.set_hi(key.next_greater(), False)



class Iterator:
    def set_exact():
        pass

    def set_prefix(self, key):
        key = keylib.Key(key)
        self.set_lo(key, True)
        self.set_hi(key.open_prefix())

    def set_max():
        pass

    def set_reverse():
        pass


def from_args(obj, key, lo, hi, prefix, reverse, max_, include):
    txn = obj.store._txn_context.get()
    it = RangeIterator(txn, obj.prefix)
    if lo:
        it.set_lo(lo, include)
    if hi:
        it.set_hi(hi, include)
    if prefix:
        assert 0, 'prefix= not implemented yet.'
    if max_:
        it.set_max(max_)
    #if key:
        #it.set_exact(key)

    if reverse:
        if key:
            it.set_hi(key, closed=True)
        return it.reverse()
    else:
        if key:
            it.set_lo(key, closed=True)
        return it.forward()


class PrefixIterator(RangeIterator):
    """Provides directional iteration of a range of keys with a set prefix.
    """


class BatchRangeIterator(object):
    """Provides bidirectional iteration of a range of keys, treating >1-length
    keys as batch records."""


class BatchPrefixIterator(object):
    """PrefixIterator equivalent for batch records."""

