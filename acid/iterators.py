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


class Result(object):
    """Interface for a single element from an iterator's result set. Iterator
    classes do not return :py:class:`Result` instances, they only return
    objects satisfying the same interface."""

    #: :py:class:`acid.keylib.KeyList` instance describing the list of keys
    #: decoded from the physical engine key.
    keys = None

    #: Object satisfying the :py:class:`buffer` interface that represents the
    #: raw record data.
    data = None


class RangeIterator(object):
    """Provides bidirectional iteration of a range of keys.

        `engine`:
            :py:class:`acid.engines.Engine` instance to iterate.
        `prefix`:
            Bytestring prefix for all keys.
    """
    # Various defaults set here to avoid necessity for repeat initialization.
    lo = None
    hi = None
    lo_pred = bool
    hi_pred = bool
    remain = -1

    def __init__(self, engine, prefix):
        self.engine = engine
        self.prefix = prefix

    def set_lo(self, key, closed=True):
        """Set the lower bound to `key`. If `closed` is ``True``, include the
        lower bound in the result set, otherwise exclude it."""
        self.lo = keylib.Key(key)
        self.lo_pred = getattr(self.lo, ('__lt__', '__le__')[closed])

    def set_hi(self, key, closed=False):
        """Set the upper bound to `key`. If `closed` is ``True``, include the
        upper bound in the result set, otherwise exclude it."""
        self.hi = keylib.Key(key)
        self.hi_pred = getattr(self.hi, ('__gt__', '__ge__')[closed])

    def set_prefix(self, key):
        """Provides directional iteration of a range of keys with a set prefix.
        """
        key = keylib.Key(key)
        self.set_lo(key, True)
        self.set_hi(key.next_greater(), False)

    def set_max(self, max_):
        """Set the maximum size of the result set."""
        assert max_ >= 0, 'Result set size must be >= 0'
        self.remain = max_

    def set_exact(self, key):
        """Set the lower and upper bounds such that `key` is the only key
        returned, if it exists."""
        key = keylib.Key(key)
        self.lo = key
        self.hi = key
        self.lo_pred = key.__le__
        self.hi_pred = key.__ge__

    def _step(self):
        """Step the iterator once, saving the new key and data. Returns True if
        the iterator is still within the bounds of the collection prefix,
        otherwise False."""
        keys_raw, self.data = next(self.it, ('', ''))
        keys = keylib.KeyList.from_raw(self.prefix, keys_raw)
        self.keys = keys
        return keys is not None


    def forward(self):
        """Return an iterator yielding the result set from `lo`..`hi`. Each
        iteration returns an object satisfying the :py:class:`Result`
        interface. Note the `Result` object is reused, so references to it
        should not be held."""
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
            go = self._step()

        remain = self._remain
        while go and remain and self.hi_pred(self.keys[0]):
            yield self
            remain -= 1
            go = self._step()

    def reverse(self):
        """Return an iterator yielding the result set from `hi`..`lo`. Each
        iteration returns an object satisfying the :py:class:`Result`
        interface. Note the `Result` object is reused, so references to it
        should not be held."""
        if self.hi is None:
            key = keylib.next_greater(self.prefix)
        else:
            key = self.hi.to_raw(self.prefix)

        self.it = self.engine.iter(key, True)
        # We may have seeked to first record of next prefix, so skip first
        # returned result.
        go = self._step(1)
        if not go:
            go = self._step()

        # We should now be positioned on the first record >= self.hi. When
        # hi(closed=False), skip the first result.
        if go and not self.hi_pred(self.keys[0]):
            go = self._step()

        remain = self._remain
        while go and remain and self.lo_pred(self.keys[0]):
            yield self
            remain -= 1
            go = self._step()


    def set_prefix(self, key):
        key = keylib.Key(key)
        self.set_lo(key, True)
        self.set_hi(key.open_prefix())

    def set_max():
        pass

    def set_reverse():
        pass


def from_args(obj, key, lo, hi, prefix, reverse, max_, include):
    """This function is a stand-in until the core.py API is refurbished."""
    txn = obj.store._txn_context.get()
    it = RangeIterator(txn, obj.prefix)

    if prefix:
        it.set_prefix(prefix)
    elif key:
        if reverse:
            it.set_hi(key, closed=True)
        else:
            it.set_lo(key, closed=True)
    else:
        if lo:
            it.set_lo(lo, include)
        if hi:
            it.set_hi(hi, include)

    if max_:
        it.set_max(max_)
    #if key:
        #it.set_exact(key)

    if reverse:
        return it.reverse()
    else:
        return it.forward()
