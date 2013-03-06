import cStringIO
import operator
import os
import unittest
import shutil

from itertools import imap
from operator import itemgetter

from pprint import pprint

import centidb
import centidb.centidb
import centidb.support


def ddb():
    pprint(list(db))

def copy(it, dst):
    for tup in it:
        dst.put(*tup)


def make_ass(op, ops):
    def ass(x, y, msg='', *a):
        if msg:
            if a:
                msg %= a
            msg = ' (%s)' % msg

        f = '%r %r %r%s'
        assert op(x, y), f % (x, ops, y, msg)
    return ass

lt = make_ass(operator.lt, '<')
eq = make_ass(operator.eq, '==')
le = make_ass(operator.le, '<=')


class IterTestCase(unittest.TestCase):
    KEYS = ('aa', 'cc', 'd', 'dd', 'de')
    ITEMS = [(k, '') for k in KEYS]
    REVERSE = ITEMS[::-1]

    def setUp(self):
        self.e = centidb.support.ListEngine()
        for key in self.KEYS:
            self.e.put(key, '')

    def iter(self, *args, **kwargs):
        return list(centidb.centidb._iter(self.e, self.e, *args, **kwargs))

    def testForward(self):
        eq(self.ITEMS, self.iter())

    def testForwardSeekFirst(self):
        eq(self.ITEMS, self.iter('aa'))

    def testForwardSeekNoExist(self):
        eq(self.ITEMS[1:], self.iter('b'))

    def testForwardSeekExist(self):
        eq(self.ITEMS[1:], self.iter('cc'))

    def testForwardSkipMostExist(self):
        eq([('de', '')], self.iter('de'))

    def testForwardSeekBeyondNoExist(self):
        eq([], self.iter('df'))

    def riter(self, *args, **kwargs):
        return self.iter(reverse=True, *args, **kwargs)

    def testReverse(self):
        eq(self.REVERSE, self.riter())

    def testReverseSeekLast(self):
        eq(self.REVERSE, self.riter('de'))

    def testReverseSeekNoExist(self):
        eq(self.REVERSE[1:], self.riter('ddd'))

    def testReverseSeekExist(self):
        eq(self.REVERSE[1:], self.riter('dd'))

    def testReverseSkipMostExist(self):
        eq([('aa', '')], self.riter('ab'))

    def testReverseSeekBeyondFirst(self):
        eq([], self.riter('a'))


class EngineTestBase:
    def testGetPutOverwrite(self):
        assert self.e.get('dave') is None
        self.e.put('dave', '1')
        self.assertEqual(self.e.get('dave'), '1')
        self.e.put('dave', '2')
        self.assertEqual(self.e.get('dave'), '2')

    def testDelete(self):
        self.e.delete('dave')
        assert self.e.get('dave') is None
        self.e.put('dave', '1')
        self.assertEqual(self.e.get('dave'), '1')
        self.e.delete('dave')
        self.assertEqual(self.e.get('dave'), None)

    def testIterReverseEmptyNone(self):
        eq(list(self.e.iter(None, reverse=True)), [])

    def testIterReverseFullNone(self):
        self.e.put('a', '')
        self.e.put('b', '')
        eq(list(self.e.iter(None, reverse=True)), [('b', ''), ('a', '')])

    def testIterForwardEmpty(self):
        assert list(self.e.iter('')) == []
        assert list(self.e.iter('x')) == []

    def testIterForwardFilled(self):
        self.e.put('dave', '1')
        assert list(self.e.iter('dave')) == [('dave', '1')]
        assert list(self.e.iter('davee')) == []

    def testIterReverseEmpty(self):
        eq(list(self.e.iter('', reverse=True)), [])
        eq(list(self.e.iter('x', reverse=True)), [])

    def testIterReverseFilled(self):
        self.e.put('dave', '1')
        eq(list(self.e.iter('davee', reverse=True)), [('dave', '1')])

    def testIterForwardMiddle(self):
        self.e.put('a', '1')
        self.e.put('c', '1')
        self.e.put('d', '1')
        assert list(self.e.iter('b')) == [('c', '1'), ('d', '1')]
        assert list(self.e.iter('c')) == [('c', '1'), ('d', '1')]

    def testIterReverseMiddle(self):
        self.e.put('a', '1')
        self.e.put('b', '1')
        self.e.put('d', '1')
        eq(list(self.e.iter('c', reverse=True)), [('b', '1'), ('a', '1')])
        assert list(self.e.iter('b', reverse=True)) == [('b', '1'), ('a', '1')]

class ListEngineTestCase(unittest.TestCase, EngineTestBase):
    def setUp(self):
        self.e = centidb.support.ListEngine()

class PlyvelEngineTestCase(unittest.TestCase, EngineTestBase):
    @classmethod
    def setUpClass(cls):
        if os.path.exists('test.ldb'):
            shutil.rmtree('test.ldb')
        cls.e = centidb.support.PlyvelEngine(
            name='test.ldb', create_if_missing=True)

    def setUp(self):
        for key, value in self.e.iter(''):
            self.e.delete(key)

    @classmethod
    def tearDownClass(cls):
        cls.e = None
        shutil.rmtree('test.ldb')

class KeysTestCase(unittest.TestCase):
    SINGLE_VALS = [
        None,
        1,
        'x',
        u'hehe',
        True,
        False,
        -1
    ]

    def test_single(self):
        for val in self.SINGLE_VALS:
            encoded = centidb.encode_keys((val,))
            decoded = centidb.decode_keys(encoded)
            eq([(val,)], decoded, 'input was %r' % (val,))

    def test_single_sort_lower(self):
        for val in self.SINGLE_VALS:
            e1 = centidb.encode_keys((val,))
            e2 = centidb.encode_keys(((val, val),))
            lt(e1, e2, 'eek %r' % (val,))

class EncodeIntTestCase(unittest.TestCase):
    INTS = [0, 1, 240, 241, 2286, 2287, 2288,
            67823, 67824, 16777215, 16777216,
            4294967295, 4294967296,
            1099511627775, 1099511627776,
            281474976710655, 281474976710656,
            72057594037927935, 72057594037927936]

    def testInts(self):
        for i in self.INTS:
            io = cStringIO.StringIO(centidb.encode_int(i))
            j = centidb.decode_int(lambda: io.read(1), io.read)
            assert j == i, (i, j, io.getvalue())

class TupleTestCase(unittest.TestCase):
    def testTuples(self):
        strs = ['dave', 'dave\x00', 'dave\x01', 'davee\x01']
        prefix = '\x01'
        for s in strs:
            encoded = centidb.encode_keys((s, 69, u'unicod', None), prefix)
            hexed = ' '.join('%02x' % ord(c) for c in encoded[0])
            #print '%-30s %d    %-30s' % (repr(s), len(encoded), hexed)
            #print decode_tuple(encoded)[1]
            #print

        encodeds = [centidb.encode_keys((s,)) for s in strs]
        encodeds.sort()
        decodeds = [centidb.decode_keys(s)[0] for s in encodeds]
        eq(encodeds, decodeds)

def x():
    db = plyvel.DB('test.ldb', create_if_missing=True)
    store = storelib.Store(db)

    feeds = storelib.Collection(store, 'feeds',
        key_func=lambda _, feed: feed.url,
        encoder=ThriftEncoder(iotypes.Feed))
    feeds.add_index('id', lambda _, feed: [feed.id] if feed.id else [])

    feed = iotypes.Feed(url='http://dave', title='mytitle', id=69)
    feeds.put(feed)

    print list(feeds.values())

if __name__ == '__main__':
    unittest.main()
