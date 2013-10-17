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
acid.engines tests.
"""

import os
import time

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


def strize(it):
    return [(str(t[0]), str(t[1])) if isinstance(t, tuple) else str(t)
            for t in it]


class EngineTestBase:
    def testGetPutOverwrite(self):
        assert self.e.get('dave') is None
        self.e.put('dave', '')
        self.assertEqual(str(self.e.get('dave')), '')
        self.e.put('dave', '2')
        self.assertEqual(str(self.e.get('dave')), '2')

    def testReplace(self):
        old = self.e.replace('dave', '')
        assert old is None
        old = self.e.replace('dave', '2')
        assert old == '', [old]

    def testPop(self):
        old = self.e.pop('dave')
        assert old is None

        self.e.put('dave', '1')
        old = self.e.pop('dave')
        assert str(old) == '1'
        old = self.e.pop('dave')
        assert old is None

    def testDelete(self):
        self.e.delete('dave')
        assert self.e.get('dave') is None
        self.e.put('dave', '')
        self.assertEqual(str(self.e.get('dave')), '')
        self.e.delete('dave')
        self.assertEqual(self.e.get('dave'), None)

    def testIterForwardEmpty(self):
        assert list(self.e.iter('', False)) == []
        assert list(self.e.iter('x', False)) == []

    def testIterForwardFilled(self):
        self.e.put('dave', '')
        eq(strize(self.e.iter('dave', False)), [('dave', '')])
        eq(strize(self.e.iter('davee', False)), [])

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
        eq(strize(self.e.iter('b', True)), [('b', ''), ('a', '')])

    def testIterReversePastEnd(self):
        self.e.put('a', '')
        self.e.put('b', '')
        eq(strize(self.e.iter('c', True)), [('b', ''), ('a', '')])

    def testIterReverseFilled(self):
        self.e.put('dave', '')
        eq(strize(self.e.iter('davee', True)), [('dave', '')])

    def testIterForwardMiddle(self):
        self.e.put('a', '')
        self.e.put('c', '')
        self.e.put('d', '')
        eq(strize(self.e.iter('b', False)), [('c', ''), ('d', '')])
        eq(strize(self.e.iter('c', False)), [('c', ''), ('d', '')])

    def testIterReverseMiddle(self):
        self.e.put('a', '')
        self.e.put('b', '')
        self.e.put('d', '')
        self.e.put('e', '')
        eq(strize(self.e.iter('c', True)),
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
        cls.engine = acid.engines.PlyvelEngine(
            name='test.ldb', create_if_missing=True)
        cls.e = cls.engine.begin(write=True)

    def setUp(self):
        for key, value in self.e.iter('', False):
            self.e.delete(key)

    @classmethod
    def tearDownClass(cls):
        cls.e.abort()
        cls.engine.close()
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
        cls.engine = acid.engines.LmdbEngine(cls.env)
        cls.e = cls.engine.begin(write=True)

    def setUp(self):
        keys = [str(k) for k, v in self.e.iter('', False)]
        for key in keys:
            self.e.delete(key)

    @classmethod
    def tearDownClass(cls):
        cls.e = None
        rm_rf('test.lmdb')


@testlib.register(native=False)
class SkipListTest:
    def testDeleteDepth(self):
        # Ensure 'shallowing' works correctly.
        sl = acid.engines.SkipList()
        keys = []
        while sl.level < 4:
            k = time.time()
            keys.append(k)
            sl.insert(k, k)

        while keys:
            assert sl.delete(keys.pop())
        assert sl.level == 0, sl.level

    def testReplace(self):
        sl = acid.engines.SkipList()
        assert sl.insert('dave', '') is None
        assert sl.insert('dave', '') == ''

    def testFindLess(self):
        sl = acid.engines.SkipList()
        update = sl._update[:]
        assert sl._findLess(update, 'missing') is sl.head

        sl.insert('dave', 'dave')
        assert sl._findLess(update, 'dave') is sl.head

        sl.insert('dave2', 'dave')
        assert sl._findLess(update, 'dave2')[0] == 'dave'

        assert sl._findLess(update, 'dave3')[0] == 'dave2'



if __name__ == '__main__':
    testlib.main()
