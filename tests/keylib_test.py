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
Key encoding tests.
"""

import cStringIO
import operator
import os
import uuid

from datetime import datetime
from pprint import pprint

import dateutil.tz
from acid import keylib
try:
    from acid import _keylib
except ImportError:
    _keylib = None

import testlib
from testlib import eq
from testlib import lt


@testlib.register()
class KeysTest:
    SINGLE_VALS = [
        None,
        1,
        'x',
        u'hehe',
        True,
        False,
        -1
    ]

    def _enc(self, *args, **kwargs):
        return keylib.packs('', *args, **kwargs)

    def _dec(self, *args, **kwargs):
        return keylib.unpacks('', *args, **kwargs)

    def test_counter(self):
        s = self._enc(('dave', 1))
        eq([('dave', 1)], self._dec(s))

    def test_single(self):
        for val in self.SINGLE_VALS:
            encoded = keylib.packs('', (val,))
            decoded = keylib.unpacks('', encoded)
            eq([(val,)], decoded, 'input was %r' % (val,))

    def test_single_sort_lower(self):
        for val in self.SINGLE_VALS:
            e1 = keylib.packs('', (val,))
            e2 = keylib.packs('', [(val, val),])
            lt(e1, e2, 'eek %r' % (val,))

    def test_list(self):
        lst = [(1,), (2,)]
        eq(lst, self._dec(self._enc(lst)))



@testlib.register()
class StringEncodingTest:
    def do_test(self, k):
        packed = keylib.packs('', k)
        try:
            unpacked = keylib.unpack('', packed)
            eq(k, keylib.unpack('', keylib.packs('', k)))
        except:
            print 'failing enc was: %r' % (packed,)
            raise

    def test_various_shapes_and_sizes(self):
        for o in xrange(256):
            for i in xrange(64):
                s = chr(255 - o) * i
                self.do_test((s,))

    def test_simple(self):
        self.do_test(('dave',))

    def test_escapes(self):
        self.do_test(('dave\x00\x00',))
        self.do_test(('dave\x00\x01',))
        self.do_test(('dave\x01\x01',))
        self.do_test(('dave\x01\x02',))
        self.do_test(('dave\x01',))


@testlib.register()
class KeyTest:
    def test_already_key(self):
        eq(keylib.Key(), keylib.Key(keylib.Key()))

    def test_not_already_tuple(self):
        eq(keylib.Key(""), keylib.Key(""))


@testlib.register()
class EncodeIntTest:
    INTS = [0, 1, 240, 241, 2286, 2287, 2288,
            67823, 67824, 16777215, 16777216,
            4294967295, 4294967296,
            1099511627775, 1099511627776,
            281474976710655, 281474976710656,
            72057594037927935, 72057594037927936]

    def testInts(self):
        for i in self.INTS:
            s = keylib.pack_int('', i)
            j = keylib.unpack_int(s)
            assert j == i, (i, j, s)


@testlib.register()
class IntKeyTest:
    INTS = [-1, -239, -240, -241, -2285, -2286, -2287, 0, 1, 0xfffff]

    def test1(self):
        for i in self.INTS:
            s = keylib.packs('', i)
            try:
                j, = keylib.unpack('', s)
                eq(j, i)
            except:
                print [i, s]
                raise


@testlib.register(python=True, enable=_keylib is not None)
class SameIntEncodingTest:
    """Compare C extension's int representation with keylib.py's."""
    def test1(self):
        for i in EncodeIntTest.INTS:
            native = _keylib.packs('', i)
            python = keylib.packs('', i)
            try:
                eq(native, python)
            except:
                print 'failing int was ' + str(i)
                raise


@testlib.register()
class TupleTest:
    def assertOrder(self, tups):
        tups = map(keylib.Key, tups)
        encs = map(keylib.packs, tups)
        encs.sort()
        eq(tups, [keylib.unpack(x) for x in encs])

    def testStringSorting(self):
        strs = [(x,) for x in ('dave', 'dave\x00', 'dave\x01', 'davee\x01')]
        encs = map(lambda o: keylib.packs('', o), strs)
        encs.sort()
        eq(strs, [keylib.unpack('', x) for x in encs])

    def testTupleNonTuple(self):
        pass


@testlib.register()
class UuidTest:
    def testUuid(self):
        t = ('a', uuid.uuid4(), 'b')
        s = keylib.packs('', t)
        eq(t, keylib.unpack('', s))


@testlib.register()
class Mod7BugTest:
    def test1(self):
        t = [('', 11, 'item')]
        p = keylib.packs('', t)
        eq(keylib.unpacks('', p), t)

    def test2(self):
        t = [('index:I', 11, 'item')]
        p = keylib.packs('', t)
        eq(keylib.unpacks('', p), t)

    def test3(self):
        t = [('index:Item:first_last', 11, 'item')]
        p = keylib.packs('', t)
        eq(keylib.unpacks('', p), t)


@testlib.register(python=True, enable=_keylib is not None)
class NativeNextGreaterTest:
    """Compare C extension's next_greater() to keylib.py's."""
    KEYS = [
        (1, 2, 3),
        (1, 2, 'dave'),
        (1, 240),
        (1, 239),
        (uuid.UUID(bytes='\xff' * 16),)
    ]

    def test_1(self):
        for key in self.KEYS:
            kp = keylib.Key(key)
            kc = _keylib.Key(key)
            assert kp.next_greater().to_raw('') == kc.next_greater().to_raw('')


@testlib.register(python=True, enable=_keylib is not None)
class NativeTimeTest:
    """Compare C extension's time representation with keylib.py's."""
    def test_utc(self):
        tz = dateutil.tz.gettz('Etc/UTC')
        dt = datetime.now(tz)
        sn = _keylib.packs('', dt)
        sp = keylib.packs('', dt)
        eq(sn, sp)

        dn = _keylib.unpacks('', sn)
        dp = keylib.unpacks('', sp)
        eq(dn, dp)

    def test_naive(self):
        dt = datetime.now()
        sn = _keylib.packs('', dt)
        sp = keylib.packs('', dt)
        eq(sn, sp)

    def test_neg(self):
        tz = dateutil.tz.gettz('Etc/GMT-1')
        dt = datetime.now(tz)
        sn = _keylib.packs('', dt)
        sp = keylib.packs('', dt)
        eq(sn, sp)

        dn = _keylib.unpacks('', sn)
        dp = keylib.unpacks('', sp)
        eq(dn, dp)

    def test_pos(self):
        tz = dateutil.tz.gettz('Etc/GMT+1')
        dt = datetime.now(tz)
        sn = _keylib.packs('', dt)
        sp = keylib.packs('', dt)
        eq(sn, sp)

        dn = _keylib.unpacks('', sn)
        dp = keylib.unpacks('', sp)
        eq(dn, dp)


@testlib.register()
class TimeTest:
    def _now_truncate(self, tz=None):
        dt = datetime.now(tz)
        return dt.replace(microsecond=(dt.microsecond / 1000) * 1000)

    def test_naive(self):
        dt = self._now_truncate()
        s = keylib.packs('', dt)
        dt2, = keylib.unpack('', s)
        eq(dt.utctimetuple(), dt2.utctimetuple())

    def test_utc(self):
        tz = dateutil.tz.gettz('Etc/UTC')
        dt = self._now_truncate(tz)
        s = keylib.packs('', dt)
        dt2, = keylib.unpack('', s)
        eq(dt, dt2)

    def test_plusone(self):
        tz = dateutil.tz.gettz('Etc/GMT+1')
        dt = self._now_truncate(tz)
        s = keylib.packs('', dt)
        dt2, = keylib.unpack('', s)
        eq(dt, dt2)

    def test_minusone(self):
        tz = dateutil.tz.gettz('Etc/GMT-1')
        dt = self._now_truncate(tz)
        s = keylib.packs('', dt)
        dt2, = keylib.unpack('', s)
        eq(dt, dt2)


@testlib.register()
class SortTest:
    """Ensure a bunch of edge cases sort correctly.
    """
    SEQS = [
        [('',), ('a',)],
        [('', 1), ('a',)],
        [('a', 1), ('a', 2)],
        [(-1,), (0,)],
        [(-2,), (-1,)],
        [(-2,), (-1,), (0,), (1,), (2,)],
        [datetime(1970, 1, 1), datetime(1970, 2, 1)],
        [datetime(1969, 1, 1), datetime(1970, 1, 1)]
    ]

    def test1(self):
        for seq in self.SEQS:
            packed = map(lambda s: keylib.packs('', s), seq)
            rnge = range(len(packed))
            done = sorted(rnge, key=packed.__getitem__)
            try:
                eq(done, rnge)
            except:
                print 'failed:', seq
                raise


if __name__ == '__main__':
    testlib.main()
