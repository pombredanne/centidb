
import cStringIO
import operator
import os
import pdb
import shutil
import time
import unittest

from pprint import pprint
from unittest import TestCase

import centidb
import centidb.centidb
import centidb.engines
from centidb import keycoder


def rm_rf(path):
    if os.path.isfile(path):
        os.unlink(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)


class CountingEngine(object):
    def __init__(self, real_engine):
        self.real_engine = real_engine
        self.get_iter_returned = 0
        self.delete_count = 0
        self.delete_keys = set()
        self.put_count = 0
        self.put_keys = set()
        self.get_count = 0
        self.get_keys = set()
        self.get_returned = 0
        self.iter_keys = set()
        self.iter_count = 0
        self.iter_size = 0

    def put(self, key, value):
        self.put_count += 1
        self.put_keys.add(key)
        self.real_engine.put(key, value)

    def get(self, key):
        self.get_count += 1
        self.get_keys.add(key)
        s = self.real_engine.get(key)
        self.get_returned += s is not None
        self.get_iter_returned += s is not None
        return s

    def delete(self, key):
        self.delete_count += 1
        self.delete_keys.add(key)
        self.real_engine.delete(key)

    def iter(self, key, reverse):
        self.iter_keys.add(key)
        self.iter_count += 1
        it = self.real_engine.iter(key, reverse)
        for x in it:
            yield x
            self.iter_size += 1
            self.get_iter_returned += 1


#
# Module reloads are necessary because KEY_ENCODER & co bind whatever
# packs() & co happens to exist before we get a chance to interfere. It
# also improves the chance of noticing any not-planned-for speedups related
# side effects, rather than relying on explicit test coverage.
# 
# There are nicer approaches to this (e.g. make_key_encoder()), but these would
# optimize for the uncommon case of running tests.
#

class PythonMixin:
    """Reload modules with speedups disabled."""
    @classmethod
    def setUpClass(cls):
        global centidb
        os.environ['CENTIDB_NO_SPEEDUPS'] = '1'
        centidb.keycoder = reload(centidb.keycoder)
        centidb.encoders = reload(centidb.encoders)
        centidb.centidb = reload(centidb.centidb)
        centidb = reload(centidb)
        getattr(cls, '_setUpClass', lambda: None)()

class NativeMixin:
    """Reload modules with speedups enabled."""
    @classmethod
    def setUpClass(cls):
        global centidb
        os.environ.pop('CENTIDB_NO_SPEEDUPS', None)
        centidb.keycoder = reload(centidb.keycoder)
        centidb.encoders = reload(centidb.encoders)
        centidb.centidb = reload(centidb.centidb)
        centidb = reload(centidb)
        getattr(cls, '_setUpClass', lambda: None)()

def register(python=True, native=True):
    def fn(klass):
        if python:
            name = 'Py' + klass.__name__
            globals()[name] = type(name, (klass, PythonMixin, TestCase), {})
        if native:
            name = 'C' + klass.__name__
            globals()[name] = type(name, (klass, NativeMixin, TestCase), {})
        return klass
    return fn


def ddb():
    pprint(list(db))

def copy(it, dst):
    for tup in it:
        dst.put(*tup)


def make_asserter(op, ops):
    def ass(x, y, msg='', *a):
        if msg:
            if a:
                msg %= a
            msg = ' (%s)' % msg

        f = '%r %s %r%s'
        assert op(x, y), f % (x, ops, y, msg)
    return ass

lt = make_asserter(operator.lt, '<')
eq = make_asserter(operator.eq, '==')
le = make_asserter(operator.le, '<=')


#@register()
class IterTest:
    prefix = 'Y'
    KEYS = [[(k,)] for k in 'aa cc d dd de'.split()]
    ITEMS = [(k, '') for k in KEYS]
    REVERSE = ITEMS[::-1]

    def _encode(self, s):
        return keycoder.packs(self.prefix, s)

    def setUp(self):
        self.e = centidb.engines.ListEngine()
        self.e.put('X', '')
        for key in self.KEYS:
            self.e.put(self._encode(key), '')
        self.e.put('Z', '')
        self.engine = self.e

    def iter(self, key=None, lo=None, hi=None, reverse=None,
            include=None, is_index=False):
        return list(centidb.centidb._iter(self.prefix, self.e, key, lo, hi,
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


@register(native=False)
class SkiplistTest:
    def testFindLess(self):
        sl = centidb.engines.SkipList()
        update = sl._update[:]
        assert sl._findLess(update, 'missing') is sl.head

        sl.insert('dave', 'dave')
        assert sl._findLess(update, 'dave') is sl.head

        sl.insert('dave2', 'dave')
        assert sl._findLess(update, 'dave2')[0] == 'dave'

        assert sl._findLess(update, 'dave3')[0] == 'dave2'
        #print sl.reprNode(sl._findLess(update, 'dave3')[3])


@register(native=False)
class SkipListTest:
    def testDeleteDepth(self):
        # Ensure 'shallowing' works correctly.
        sl = centidb.engines.SkipList()
        keys = []
        while sl.level < 4:
            k = time.time()
            keys.append(k)
            sl.insert(k, k)

        while keys:
            assert sl.delete(keys.pop())
        assert sl.level == 0, sl.level


class EngineTestBase:
    def testGetPutOverwrite(self):
        assert self.e.get('dave') is None
        self.e.put('dave', '')
        self.assertEqual(self.e.get('dave'), '')
        self.e.put('dave', '2')
        self.assertEqual(self.e.get('dave'), '2')

    def testDelete(self):
        self.e.delete('dave')
        assert self.e.get('dave') is None
        self.e.put('dave', '')
        self.assertEqual(self.e.get('dave'), '')
        self.e.delete('dave')
        self.assertEqual(self.e.get('dave'), None)

    def testIterForwardEmpty(self):
        assert list(self.e.iter('', False)) == []
        assert list(self.e.iter('x', False)) == []

    def testIterForwardFilled(self):
        self.e.put('dave', '')
        eq(list(self.e.iter('dave', False)), [('dave', '')])
        eq(list(self.e.iter('davee', False)), [])

    def testIterForwardBeyondNoExist(self):
        self.e.put('aa', '')
        self.e.put('bb', '')
        eq([], list(self.e.iter('df', False)))

    def testIterReverseEmpty(self):
        eq(list(self.e.iter('', True)), [])
        eq(list(self.e.iter('x', True)), [])

    def testIterReverseAtEnd(self):
        self.e.put('a', '')
        self.e.put('b', '')
        eq(list(self.e.iter('b', True)), [('b', ''), ('a', '')])

    def testIterReversePastEnd(self):
        self.e.put('a', '')
        self.e.put('b', '')
        eq(list(self.e.iter('c', True)), [('b', ''), ('a', '')])

    def testIterReverseFilled(self):
        self.e.put('dave', '')
        eq(list(self.e.iter('davee', True)), [('dave', '')])

    def testIterForwardMiddle(self):
        self.e.put('a', '')
        self.e.put('c', '')
        self.e.put('d', '')
        assert list(self.e.iter('b', False)) == [('c', ''), ('d', '')]
        assert list(self.e.iter('c', False)) == [('c', ''), ('d', '')]

    def testIterReverseMiddle(self):
        self.e.put('a', '')
        self.e.put('b', '')
        self.e.put('d', '')
        self.e.put('e', '')
        eq(list(self.e.iter('c', True)),
           [('d', ''), ('b', ''), ('a', '')])


@register()
class ListEngineTest(EngineTestBase):
    def setUp(self):
        self.e = centidb.engines.ListEngine()


@register()
class SkiplistEngineTest(EngineTestBase):
    def setUp(self):
        self.e = centidb.engines.SkiplistEngine()


@register()
class PlyvelEngineTest(EngineTestBase):
    @classmethod
    def _setUpClass(cls):
        rm_rf('test.ldb')
        cls.e = centidb.engines.PlyvelEngine(
            name='test.ldb', create_if_missing=True)

    def setUp(self):
        for key, value in self.e.iter('', False):
            self.e.delete(key)

    @classmethod
    def tearDownClass(cls):
        cls.e = None
        rm_rf('test.ldb')


@register()
class KyotoEngineTest(EngineTestBase):
    @classmethod
    def _setUpClass(cls):
        if os.path.exists('test.kct'):
            os.unlink('test.kct')
        cls.e = centidb.engines.KyotoEngine(path='test.kct')

    def setUp(self):
        for key, value in list(self.e.iter('', False)):
            self.e.delete(key)

    @classmethod
    def tearDownClass(cls):
        cls.e = None
        os.unlink('test.kct')


@register()
class LmdbEngineTest(EngineTestBase):
    @classmethod
    def _setUpClass(cls):
        rm_rf('test.lmdb')
        import lmdb
        cls.env = lmdb.open('test.lmdb')
        cls.e = centidb.engines.LmdbEngine(cls.env)

    def setUp(self):
        for key, value in list(self.e.iter('', False)):
            self.e.delete(key)

    @classmethod
    def tearDownClass(cls):
        cls.e = None
        rm_rf('test.lmdb')


@register()
class CollBasicTest:
    def setUp(self):
        self.e = centidb.engines.ListEngine()
        self.store = centidb.Store(self.e)
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


@register()
class IndexTest:
    def setUp(self):
        self.e = centidb.engines.ListEngine()
        self.store = centidb.Store(self.e)
        self.coll = self.store.add_collection('stuff')
        self.i = self.coll.add_index('idx', lambda obj: (69, obj))

        self.key = self.coll.put('dave')
        self.key2 = self.coll.put('dave2')
        self.expect = [(69, 'dave'), self.key]
        self.expect2 = [(69, 'dave2'), self.key2]
        self.first = [self.expect]
        self.second = [self.expect2]
        self.both = [self.expect, self.expect2]

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
        eq('dave2', self.i.find(hi=(69, 'dave2'), reverse=True))

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


@register()
class BatchTest:
    ITEMS = [
        ((1,), 'dave'),
        ((2,), 'jim'),
        ((3,), 'zork')
    ]

    def setUp(self):
        self.e = centidb.engines.ListEngine()
        self.store = centidb.Store(self.e)
        self.coll = self.store.add_collection('people')

    def testBatch(self):
        old_len = len(self.e.items)
        for key, value in self.ITEMS:
            self.coll.put(value, key=key)
        assert len(self.e.items) == (old_len + len(self.ITEMS))
        self.coll.batch(max_recs=len(self.ITEMS))
        assert len(self.e.items) == (old_len + 1)
        assert list(self.coll.items()) == self.ITEMS


@register()
class CountTest:
    def setUp(self):
        self.e = CountingEngine(centidb.engines.ListEngine())
        self.store = centidb.Store(self.e)

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

    def testTxn(self):
        txn = CountingEngine(self.e)
        eq(10, self.store.count('test', init=10, txn=txn))
        assert (txn.get_count + txn.iter_count) == 1
        assert txn.put_count == 1


@register()
class ReopenBugTest:
    def test1(self):
        engine = centidb.engines.ListEngine()
        st1 = centidb.Store(engine)
        st1.add_collection('dave')
        st2 = centidb.Store(engine)
        print st2['dave']


def x():
    db = plyvel.DB('test.ldb', create_if_missing=True)
    store = storelib.Store(db)

    feeds = storelib.Collection(store, 'feeds',
        key_func=lambda _, feed: feed.url,
        encoder=ThriftEncoder(iotypes.Feed))
    feeds.add_index('id', lambda _, feed: [feed.id] if feed.id else [])

    feed = iotypes.Feed(url='http://dave', title='mytitle', id=69)
    feeds.put(feed)


if __name__ == '__main__':
    unittest.main()
