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
core.py tests.
"""

import acid
import acid.core
import acid.engines
from acid import keylib

import testlib
from testlib import eq


#@testlib.register()
class IterTest:
    prefix = 'Y'
    KEYS = [[(k,)] for k in 'aa cc d dd de'.split()]
    ITEMS = [(k, '') for k in KEYS]
    REVERSE = ITEMS[::-1]

    def _encode(self, s):
        return keylib.packs(self.prefix, s)

    def setUp(self):
        self.e = acid.engines.ListEngine()
        self.e.put('X', '')
        for key in self.KEYS:
            self.e.put(self._encode(key), '')
        self.e.put('Z', '')
        self.engine = self.e

    def iter(self, key=None, lo=None, hi=None, reverse=None,
            include=None, is_index=False):
        return list(acid.core._iter(self.prefix, self.e, key, lo, hi,
                                    reverse, include, is_index))

    def testForward(self):
        eq(self.ITEMS, self.iter())

    def testForwardSeekFirst(self):
        eq(self.ITEMS, self.iter('aa'))

    def testForwardSeekNoExist(self):
        eq(self.ITEMS[1:], self.iter('b'))

    def testForwardSeekExist(self):
        eq(self.ITEMS[1:], self.iter('cc'))

    def testForwardSkipMostExist(self):
        eq([([('de',),], '')], self.iter('de'))

    def testForwardSeekBeyondNoExist(self):
        eq([], self.iter('df'))

    def riter(self, *args, **kwargs):
        return self.iter(reverse=True, *args, **kwargs)

    def testReverse(self):
        eq(self.REVERSE[1:], self.riter(hi='de'))

    def testReverseAutoInclude(self):
        eq(self.REVERSE, self.riter('de'))

    def testReverseSeekLast(self):
        eq(self.REVERSE[1:], self.riter(hi='de'))

    def testReverseSeekLastInclude(self):
        eq(self.REVERSE, self.riter('de', include=True))

    def testReverseSeekNoExist(self):
        eq(self.REVERSE[1:], self.riter('ddd'))

    def testReverseSeekNoExistInclude(self):
        eq(self.REVERSE[1:], self.riter('ddd'))

    def testReverseSeekExist(self):
        eq(self.REVERSE[2:], self.riter(hi='dd'))

    def testReverseSeekExistInclude(self):
        eq(self.REVERSE[1:], self.riter(hi='dd', include=True))

    def testReverseSkipMostExist(self):
        eq([([('aa',)], '')], self.riter('ab'))

    def testReverseSeekBeyondFirst(self):
        eq([], self.riter('a'))

    def testForwardPrefix(self):
        eq(self.ITEMS, self.iter())

    def testReversePrefix(self):
        eq(self.REVERSE, self.riter())


@testlib.register(native=False)
class SkiplistTest:
    def testFindLess(self):
        sl = acid.engines.SkipList()
        update = sl._update[:]
        assert sl._findLess(update, 'missing') is sl.head

        sl.insert('dave', 'dave')
        assert sl._findLess(update, 'dave') is sl.head

        sl.insert('dave2', 'dave')
        assert sl._findLess(update, 'dave2')[0] == 'dave'

        assert sl._findLess(update, 'dave3')[0] == 'dave2'


@testlib.register()
class OneCollBoundsTest:
    def setUp(self):
        self.store = acid.open('ListEngine')
        self.txn = self.store.begin()
        self.txn.__enter__()
        self.store.add_collection('stuff')
        self.keys = [
            self.store['stuff'].put('a'),
            self.store['stuff'].put('b'),
            self.store['stuff'].put('c')
        ]

    def test1(self):
        eq(self.keys[::-1], list(self.store['stuff'].keys(reverse=True)))

    def test2(self):
        eq(self.keys, list(self.store['stuff'].keys()))


@testlib.register()
class TwoCollBoundsTest(OneCollBoundsTest):
    def setUp(self):
        OneCollBoundsTest.setUp(self)
        self.store.add_collection('stuff2')
        self.store['stuff2'].put('a')
        self.store['stuff2'].put('b')
        self.store['stuff2'].put('c')


@testlib.register()
class ThreeCollBoundsTest(OneCollBoundsTest):
    def setUp(self):
        self.store = acid.open('ListEngine')
        self.txn = self.store.begin()
        self.txn.__enter__()
        self.store.add_collection('stuff')
        self.keys = [
            self.store['stuff'].put('a'),
            self.store['stuff'].put('b'),
            self.store['stuff'].put('c')
        ]
        self.store.add_collection('stuff0')
        self.store['stuff0'].put('a')
        self.store['stuff0'].put('b')
        self.store['stuff0'].put('c')
        OneCollBoundsTest.setUp(self)
        self.store.add_collection('stuff2')
        self.store['stuff2'].put('a')
        self.store['stuff2'].put('b')
        self.store['stuff2'].put('c')


@testlib.register()
class CollBasicTest:
    def setUp(self):
        self.e = acid.engines.ListEngine()
        self.store = acid.Store(self.e)
        self.txn = self.store.begin()
        self.txn.__enter__()
        self.coll = self.store.add_collection('coll1')

    def testGetNoExist(self):
        eq(None, self.coll.get('missing'))

    def testGetNoExistDefault(self):
        eq('dave', self.coll.get('missing', default='dave'))

    def testGetExist(self):
        key = self.coll.put('')
        eq(key, (1,))
        eq('', self.coll.get(1))
        key = self.coll.put('x')
        eq(key, (2,))
        eq('x', self.coll.get(2))

    def testIterItemsExist(self):
        rec = self.coll.put('')
        eq([((1,), '')], list(self.coll.items()))

    def testIterKeysExist(self):
        key = self.coll.put('')
        key2 = self.coll.put('')
        eq([key, key2], list(self.coll.keys()))

    def testIterValuesExist(self):
        rec = self.coll.put('')
        eq([''], list(self.coll.values()))


@testlib.register()
class IndexTest:
    def setUp(self):
        self.store = acid.open('ListEngine')
        self.e = self.store.engine
        self.t = self.store.begin()
        self.t.__enter__()
        self.coll = self.store.add_collection('stuff')
        self.i = self.coll.add_index('idx', lambda obj: (69, obj))

        self.key = self.coll.put('dave')
        self.key2 = self.coll.put('dave2')

        self.expect = [(69, 'dave'), self.key]
        self.expect2 = [(69, 'dave2'), self.key2]
        self.first = [self.expect]
        self.second = [self.expect2]
        self.both = [self.expect, self.expect2]

        # Insert junk in a higher collection to test iter stop conds.
        self.coll2 = self.store.add_collection('stuff2')
        self.i2 = self.coll2.add_index('idx', lambda obj: (69, obj))
        self.coll2.put('XXXX')
        self.coll2.put('YYYY')

    # iterpairs
    def testIterPairs(self):
        eq(self.both, list(self.i.pairs()))
        eq(self.both, list(self.i.pairs(68)))
        eq(self.both, list(self.i.pairs((69, 'dave'))))
        eq(self.second, list(self.i.pairs((69, 'dave2'))))
        eq([], list(self.i.pairs(80)))

        eq(self.both[::-1], list(self.i.pairs(reverse=True)))

        self.coll.delete(self.key)
        eq(self.second, list(self.i.pairs()))
        self.coll.delete(self.key2)
        eq([], list(self.i.pairs()))

    # itertups
    def testIterTups(self):
        eq([(69, 'dave'), (69, 'dave2')], list(self.i.tups()))
        eq([(69, 'dave2'), (69, 'dave')], list(self.i.tups(reverse=True)))

    # iterkeys
    def testIterKeys(self):
        eq([self.key, self.key2], list(self.i.keys()))
        eq([self.key2, self.key], list(self.i.keys(reverse=True)))

    # iteritems
    def testIterItems(self):
        item1 = (self.key, 'dave')
        item2 = (self.key2, 'dave2')
        eq([item1, item2], list(self.i.items()))
        eq([item2, item1], list(self.i.items(reverse=True)))

    # itervalues
    def testIterValues(self):
        eq(['dave', 'dave2'], list(self.i.values()))
        eq(['dave2', 'dave'], list(self.i.values(reverse=True)))

    # find
    def testFind(self):
        eq('dave', self.i.find())
        eq('dave2', self.i.find(reverse=True))
        eq('dave2', self.i.find((69, 'dave2')))
        # open
        eq('dave', self.i.find(hi=(69, 'dave2'), reverse=True))

    # get
    def testGet(self):
        eq(None, self.i.get('missing'))
        eq('dave', self.i.get((69, 'dave')))
        eq('dave2', self.i.get((69, 'dave2')))

    # has
    def testHas(self):
        assert self.i.has((69, 'dave'))
        assert not self.i.has((69, 'dave123'))
        assert self.i.has((69, 'dave2'))


class Bag(object):
    def __init__(self, **kwargs):
        vars(self).update(kwargs)


@testlib.register()
class BatchTest:
    ITEMS = [
        ((1,), 'dave'),
        ((2,), 'jim'),
        ((3,), 'zork')
    ]

    def setUp(self):
        self.e = acid.engines.ListEngine()
        self.store = acid.Store(self.e)
        self.txn = self.store.begin()
        self.txn.__enter__()
        self.coll = self.store.add_collection('people')

    def testBatch(self):
        old_len = len(self.e.items)
        for key, value in self.ITEMS:
            self.coll.put(value, key=key)
        assert len(self.e.items) == (old_len + len(self.ITEMS))
        self.coll.batch(max_recs=len(self.ITEMS))
        assert len(self.e.items) == (old_len + 1)
        assert list(self.coll.items()) == self.ITEMS


@testlib.register()
class CountTest:
    def setUp(self):
        self.e = testlib.CountingEngine(acid.engines.ListEngine())
        self.store = acid.Store(self.e)
        self.txn = self.store.begin()
        self.txn.__enter__()

    def testNoExistNoCount(self):
        eq(10, self.store.count('test', n=0, init=10))
        eq(10, self.store.count('test', n=0, init=10))

    def testExistCount(self):
        eq(10, self.store.count('test', init=10))
        eq(11, self.store.count('test', init=10))
        eq(12, self.store.count('test', init=10))
        assert (self.e.get_count + self.e.iter_count) == 3
        assert self.e.put_count == 3

    def testExistCountSometimes(self):
        eq(10, self.store.count('test', init=10))
        eq(11, self.store.count('test', n=0, init=10))
        eq(11, self.store.count('test', init=10))
        eq(12, self.store.count('test', init=10))
        assert (self.e.get_count + self.e.iter_count) == 4
        assert self.e.put_count == 3


@testlib.register()
class ReopenBugTest:
    """Ensure store metadata survives a round-trip."""
    def test1(self):
        engine = acid.engines.ListEngine()
        st1 = acid.Store(engine)
        st1.begin().__enter__()
        st1.add_collection('dave')
        st2 = acid.Store(engine)
        st2['dave']


@testlib.register()
class DeleteBugTest:
    """Ensure delete deletes everything it should when indices are present."""
    def test1(self):
        store = acid.open('ListEngine')
        with store.begin(write=True):
            stuff = store.add_collection('stuff')
            stuff.add_index('foop', lambda rec: 'foop')
            key = stuff.put('temp')
            assert stuff.indices['foop'].find('foop') == 'temp'
            assert stuff.get(1) == 'temp'
            stuff.delete(1)
            assert stuff.get(1) is None
            i = stuff.indices['foop'].find('foop')
            assert i is None, i


if __name__ == '__main__':
    testlib.main()
