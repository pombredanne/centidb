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
        return keylib.packs(s, self.prefix)

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


@testlib.register()
class OneCollBoundsTest:
    def setUp(self):
        self.store = acid.open('list:/')
        self.txn = self.store.begin()
        self.txn.__enter__()
        self.store.add_collection('stuff')
        self.keys = [
            self.store['stuff'].put(u'a'),
            self.store['stuff'].put(u'b'),
            self.store['stuff'].put(u'c')
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
        self.store['stuff2'].put(u'a')
        self.store['stuff2'].put(u'b')
        self.store['stuff2'].put(u'c')


@testlib.register()
class ThreeCollBoundsTest(OneCollBoundsTest):
    def setUp(self):
        self.store = acid.open('list:/')
        self.txn = self.store.begin()
        self.txn.__enter__()
        self.store.add_collection('stuff')
        self.keys = [
            self.store['stuff'].put(u'a'),
            self.store['stuff'].put(u'b'),
            self.store['stuff'].put(u'c')
        ]
        self.store.add_collection('stuff0')
        self.store['stuff0'].put(u'a')
        self.store['stuff0'].put(u'b')
        self.store['stuff0'].put(u'c')
        OneCollBoundsTest.setUp(self)
        self.store.add_collection('stuff2')
        self.store['stuff2'].put(u'a')
        self.store['stuff2'].put(u'b')
        self.store['stuff2'].put(u'c')


@testlib.register()
class CollBasicTest:
    def setUp(self):
        self.e = acid.engines.ListEngine()
        #self.e = acid.engines.TraceEngine(self.e, '/dev/stdout')
        self.store = acid.Store(self.e)
        self.txn = self.store.begin()
        self.txn.__enter__()
        self.coll = self.store.add_collection('coll1')

    def test_put(self):
        self.store.count('key:coll1') # Ensure counter exists before len()
        old_items = len(self.e.items)
        k = self.coll.put(u'test')
        assert (old_items + 1) == len(self.e.items)

    def testGetNoExist(self):
        eq(None, self.coll.get('missing'))

    def testGetNoExistDefault(self):
        eq(u'dave', self.coll.get('missing', default=u'dave'))

    def testGetExist(self):
        key = self.coll.put(u'')
        eq(key, (1,))
        eq('', self.coll.get(1))
        key = self.coll.put(u'x')
        eq(key, (2,))
        eq(u'x', self.coll.get(2))

    def testIterItemsExist(self):
        rec = self.coll.put(u'')
        eq([((1,), u'')], list(self.coll.items()))

    def testIterKeysExist(self):
        key = self.coll.put(u'')
        key2 = self.coll.put(u'')
        eq([key, key2], list(self.coll.keys()))

    def testIterValuesExist(self):
        rec = self.coll.put(u'')
        eq([u''], list(self.coll.values()))


@testlib.register()
class IndexTest:
    def setUp(self):
        self.store = acid.open('list:/')
        self.e = self.store.engine
        self.t = self.store.begin()
        self.t.__enter__()
        self.coll = self.store.add_collection('stuff')
        self.i = acid.add_index(self.coll, 'idx', lambda obj: (69, obj))

        self.key = self.coll.put(u'dave')
        self.key2 = self.coll.put(u'dave2')

        self.expect = [(69, u'dave'), self.key]
        self.expect2 = [(69, u'dave2'), self.key2]
        self.first = [self.expect]
        self.second = [self.expect2]
        self.both = [self.expect, self.expect2]

        # Insert junk in a higher collection to test iter stop conds.
        self.coll2 = self.store.add_collection('stuff2')
        self.i2 = acid.add_index(self.coll2, 'idx', lambda obj: (69, obj))
        self.coll2.put(u'XXXX')
        self.coll2.put(u'YYYY')

    # iterpairs
    def testIterPairs(self):
        eq(self.both, list(self.i.pairs()))
        eq(self.both, list(self.i.pairs(lo=68)))
        eq(self.both, list(self.i.pairs(lo=(69, u'dave'))))
        eq(self.second, list(self.i.pairs(lo=(69, u'dave2'))))
        eq([], list(self.i.pairs(lo=80)))

        eq(self.both[::-1], list(self.i.pairs(reverse=True)))

        self.coll.delete(self.key)
        eq(self.second, list(self.i.pairs()))
        self.coll.delete(self.key2)
        eq([], list(self.i.pairs()))

    # itertups
    def testIterTups(self):
        eq([(69, u'dave'), (69, u'dave2')], list(self.i.tups()))
        eq([(69, u'dave2'), (69, u'dave')], list(self.i.tups(reverse=True)))

    # iterkeys
    def testIterKeys(self):
        eq([self.key, self.key2], list(self.i.keys()))
        eq([self.key2, self.key], list(self.i.keys(reverse=True)))

    # iteritems
    def testIterItems(self):
        item1 = (self.key, u'dave')
        item2 = (self.key2, u'dave2')
        eq([item1, item2], list(self.i.items()))
        eq([item2, item1], list(self.i.items(reverse=True)))

    # itervalues
    def testIterValues(self):
        eq([u'dave', u'dave2'], list(self.i.values()))
        eq([u'dave2', u'dave'], list(self.i.values(reverse=True)))

    # find
    def testFind(self):
        eq(u'dave', self.i.find())
        eq(u'dave2', self.i.find(reverse=True))
        eq(u'dave2', self.i.find((69, u'dave2')))
        # open
        eq(u'dave', self.i.find(hi=(69, u'dave2'), reverse=True))

    # get
    def testGet(self):
        eq(None, self.i.get('missing'))
        eq(u'dave', self.i.get((69, u'dave')))
        eq(u'dave2', self.i.get((69, u'dave2')))

    # has
    def testHas(self):
        assert self.i.has((69, u'dave'))
        assert not self.i.has((69, u'dave123'))
        assert self.i.has((69, u'dave2'))


class Bag(object):
    def __init__(self, **kwargs):
        vars(self).update(kwargs)


@testlib.register()
class BatchTest:
    ITEMS = [
        ((1,), u'dave'),
        ((3,), u'jim'),
        ((4,), u'zork')
    ]

    def setUp(self):
        self.e = acid.engines.ListEngine()
        self.store = acid.Store(self.e)
        self.txn = self.store.begin()
        self.txn.__enter__()
        self.coll = self.store.add_collection('people')

    def insert_items(self):
        self.old_len = len(self.e.items)
        for key, value in self.ITEMS:
            self.coll.put(value, key=key)
        # Should now contain metadata + individual rows
        assert len(self.e.items) == (self.old_len + len(self.ITEMS))

    def test_batch(self):
        self.insert_items()
        self.coll.strategy.batch(max_recs=len(self.ITEMS))
        # Should now contain metadata + 1 batch
        assert len(self.e.items) == (self.old_len + 1)
        # Forward iteration should succeed.
        assert list(self.coll.items()) == self.ITEMS
        # Reverse iteration should succeed.
        assert list(self.coll.items(reverse=True)) == self.ITEMS[::-1]
        # Individual get should succeed.
        for key, val in self.ITEMS:
            assert self.coll.get(key) == val

    def test_delete(self):
        self.insert_items()
        self.coll.strategy.batch(max_recs=len(self.ITEMS))
        # Should now contain metadata + 1 batch
        assert len(self.e.items) == (self.old_len + 1)
        # Deletion should trigger split of batch.
        self.coll.delete(3)
        # Should now contain metadata + 2 remaining records.
        assert len(self.e.items) == (self.old_len + 2)
        # Values shouldn't have changed.
        assert list(self.coll.items()) == [self.ITEMS[0], self.ITEMS[2]]

    def test_delete_pop(self):
        self.insert_items()
        self.coll.strategy.batch(max_recs=len(self.ITEMS))
        # Pop should trigger split of batch and return of old value.
        data = self.coll.strategy.pop(self.txn.get(), keylib.Key(3))
        eq(data, '"jim"')
        # Should now contain metadata + 2 remaining records.
        assert len(self.e.items) == (self.old_len + 2)
        # Obj should be old value

    def test_put(self):
        """Overwrite key existing as part of a batch."""
        self.insert_items()
        self.coll.strategy.batch(max_recs=len(self.ITEMS))
        # Should now contain metadata + 1 batch
        assert len(self.e.items) == (self.old_len + 1)
        # Put should trigger split of batch and modification of record.
        self.coll.put(u'james', key=3)
        # Should now contain metadata + 3 individual records
        assert len(self.e.items) == (self.old_len + 3)
        # Test for new items.
        assert list(self.coll.items()) ==\
            [self.ITEMS[0], ((3,), 'james'), self.ITEMS[2]]

    def test_put_conflict(self):
        """Insert record covered by a batch, but for which the key does not yet
        exist."""
        self.insert_items()
        self.coll.strategy.batch(max_recs=len(self.ITEMS))
        # Put should trigger split of batch and insert of record.
        self.coll.put(u'john', key=2)
        # Should now contain metadata + 4 individual records
        assert len(self.e.items) == (self.old_len + 4)
        # Test for new items.
        assert list(self.coll.items()) ==\
            [self.ITEMS[0], ((2,), 'john'), ((3,), 'jim'), self.ITEMS[2]]


@testlib.register()
class CountTest:
    def setUp(self):
        eng = acid.engines.ListEngine()
        #eng = acid.engines.TraceEngine(eng, '/dev/stdout')
        self.e = testlib.CountingEngine(eng)
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
        eq(self.e.iter_count, 0)
        eq(self.e.get_count, 3)
        eq(self.e.put_count, 3)

    def testExistCountSometimes(self):
        eq(10, self.store.count('test', init=10))
        eq(11, self.store.count('test', n=0, init=10))
        eq(11, self.store.count('test', init=10))
        eq(12, self.store.count('test', init=10))
        eq(self.e.iter_count, 0)
        eq(self.e.get_count, 4)
        eq(self.e.put_count, 3)


@testlib.register()
class ReopenBugTest:
    """Ensure store metadata survives a round-trip."""
    def test1(self):
        engine = acid.engines.ListEngine()
        st1 = acid.Store(engine)
        st1.begin().__enter__()
        st1.add_collection(u'dave')
        st2 = acid.Store(engine)
        st2[u'dave']


@testlib.register()
class DeleteBugTest:
    """Ensure delete deletes everything it should when indices are present."""
    def test1(self):
        store = acid.open('list:/')
        with store.begin(write=True):
            stuff = store.add_collection('stuff')
            acid.add_index(stuff, 'foop', lambda rec: 'foop')
            key = stuff.put(u'temp')
            assert store['foop'].find('foop') == 'temp'
            assert stuff.get(1) == 'temp'
            stuff.delete(1)
            assert stuff.get(1) is None
            i = store['foop'].find('foop')
            assert i is None, i


@testlib.register()
class TransactionAbortTest:
    def setUp(self):
        self.store = acid.open('list:/')

    def test1(self):
        def crashy():
            with self.store.begin():
                raise Exception()
        self.assertRaises(Exception, crashy)

    def testok(self):
        def crashy():
            with self.store.begin():
                acid.abort()
            return 123
        assert crashy() == 123


if __name__ == '__main__':
    testlib.main()
