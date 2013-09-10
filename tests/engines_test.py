
import os

import testlib
from testlib import eq
from testlib import rm_rf

import acid
import acid.engines


try:
    import plyvel
except ImportError:
    plyvel = None

try:
    import lmdb
except ImportError:
    lmdb = None

try:
    import kyotocabinet
except ImportError:
    kyotocabinet = None


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
        # TODO: do we ever need to query from end of engine?
        #eq(list(self.e.iter('', True)), [])
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


@testlib.register()
class ListEngineTest(EngineTestBase):
    def setUp(self):
        self.e = acid.engines.ListEngine()


@testlib.register()
class SkiplistEngineTest(EngineTestBase):
    def setUp(self):
        self.e = acid.engines.SkiplistEngine()


@testlib.register(enable=plyvel is not None)
class PlyvelEngineTest(EngineTestBase):
    @classmethod
    def _setUpClass(cls):
        rm_rf('test.ldb')
        cls.e = acid.engines.PlyvelEngine(
            name='test.ldb', create_if_missing=True)

    def setUp(self):
        for key, value in self.e.iter('', False):
            self.e.delete(key)

    @classmethod
    def tearDownClass(cls):
        cls.e = None
        rm_rf('test.ldb')


@testlib.register(enable=kyotocabinet is not None)
class KyotoEngineTest(EngineTestBase):
    @classmethod
    def _setUpClass(cls):
        if os.path.exists('test.kct'):
            os.unlink('test.kct')
        cls.e = acid.engines.KyotoEngine(path='test.kct')

    def setUp(self):
        for key, value in list(self.e.iter('', False)):
            self.e.delete(key)

    @classmethod
    def tearDownClass(cls):
        cls.e = None
        os.unlink('test.kct')


@testlib.register(enable=lmdb is not None)
class LmdbEngineTest(EngineTestBase):
    @classmethod
    def _setUpClass(cls):
        rm_rf('test.lmdb')
        cls.env = lmdb.open('test.lmdb')
        cls.e = acid.engines.LmdbEngine(cls.env)

    def setUp(self):
        for key, value in list(self.e.iter('', False)):
            self.e.delete(key)

    @classmethod
    def tearDownClass(cls):
        cls.e = None
        rm_rf('test.lmdb')


if __name__ == '__main__':
    testlib.main()
