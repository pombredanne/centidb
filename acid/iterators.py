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

    def __init__(self, engine, prefix):
        self.engine = engine
        self.prefix = prefix

    def set_lo(self, key, closed=True):
        self.lo = keylib.Key(key)
        self.lo_pred = getattr(self.lo, ('__lt__', '__le__')[closed])

    def set_hi(self, key, closed=False):
        self.hi = keylib.Key(key)
        self.hi_pred = getattr(self.hi, ('__gt__', '__ge__')[closed])

    def set_exact(self, key):
        key = keylib.Key(key)
        self.lo = key
        self.hi = key
        self.lo_pred = key.__le__
        self.hi_pred = key.__ge__

    def _step(self):
        """Step the iterator once, saving the new key and data. Returns True if
        the iterator is still within the bounds of the collection prefix,
        otherwise False."""
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
        go = self._step()

        # When lo(closed=False), skip the start key.
        if go and not self.lo_pred(self.keys[0]):
            go = self._step()
        while go and self.hi_pred(self.keys[0]):
            yield self
            go = self._step()

    def reverse(self):
        if self.hi is None:
            key = keylib.next_greater(self.prefix)
        else:
            key = self.hi.to_raw(self.prefix)

        self.it = self.engine.iter(key, True)
        # We may have seeked to first record of next prefix, so skip first
        # returned result.
        go = self._step()
        if not go:
            go = self._step()

        # We should now be positioned on the first record >= self.hi. When
        # hi(closed=False), skip the first result.
        if go and not self.hi_pred(self.keys[0]):
            go = self._step()
        while go and self.lo_pred(self.keys[0]):
            yield self
            go = self._step()


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


def _iter(key, lo, hi, prefix, reverse, max_, include, max_phys):
    if key:
        it.set_exact(key)
    elif prefix is not None:
        it.set_prefix(prefix)
    else:
        if lo:
            it.set_lo(lo)
        if hi:
            it.set_hi(hi)
    if reverse:
        it.set_reverse(reverse)


class PrefixIterator(RangeIterator):
    """Provides directional iteration of a range of keys with a set prefix.
    """



def from_args(engine, coll_prefix, prefix, key, lo, hi, reverse, max, include):
    if key is not None:
        it = PrefixIterator(engine, coll_prefix)
        it.set_prefix(prefix, reverse)


class BatchRangeIterator(object):
    """Provides bidirectional iteration of a range of keys, treating >1-length
    keys as batch records."""


class BatchPrefixIterator(object):
    """PrefixIterator equivalent for batch records."""

