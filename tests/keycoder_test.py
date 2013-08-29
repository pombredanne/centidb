
import cStringIO
import operator
import os
import unittest
import uuid

from datetime import datetime
from pprint import pprint
from unittest import TestCase

import dateutil.tz
from centidb import keycoder
from centidb import _keycoder


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
        global keycoder
        os.environ['CENTIDB_NO_SPEEDUPS'] = '1'
        keycoder = reload(keycoder)
        keycoder = reload(keycoder)
        getattr(cls, '_setUpClass', lambda: None)()

class NativeMixin:
    """Reload modules with speedups enabled."""
    @classmethod
    def setUpClass(cls):
        global keycoder
        os.environ.pop('CENTIDB_NO_SPEEDUPS', None)
        keycoder = reload(keycoder)
        keycoder = reload(keycoder)
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


@register()
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
        return keycoder.packs('', *args, **kwargs)

    def _dec(self, *args, **kwargs):
        return keycoder.unpacks('', *args, **kwargs)

    def test_counter(self):
        s = self._enc(('dave', 1))
        eq([('dave', 1)], self._dec(s))

    def test_single(self):
        for val in self.SINGLE_VALS:
            encoded = keycoder.packs('', (val,))
            decoded = keycoder.unpacks('', encoded)
            eq([(val,)], decoded, 'input was %r' % (val,))

    def test_single_sort_lower(self):
        for val in self.SINGLE_VALS:
            e1 = keycoder.packs('', (val,))
            e2 = keycoder.packs('', [(val, val),])
            lt(e1, e2, 'eek %r' % (val,))

    def test_list(self):
        lst = [(1,), (2,)]
        eq(lst, self._dec(self._enc(lst)))



@register()
class StringEncodingTest:
    def do_test(self, k):
        packed = keycoder.packs('', k)
        try:
            unpacked = keycoder.unpack('', packed)
            eq(k, keycoder.unpack('', keycoder.packs('', k)))
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


@register()
class KeyTest:
    def test_already_key(self):
        eq(keycoder.Key(), keycoder.Key(keycoder.Key()))

    def test_not_already_tuple(self):
        eq(keycoder.Key(""), keycoder.Key(""))


@register()
class EncodeIntTest:
    INTS = [0, 1, 240, 241, 2286, 2287, 2288,
            67823, 67824, 16777215, 16777216,
            4294967295, 4294967296,
            1099511627775, 1099511627776,
            281474976710655, 281474976710656,
            72057594037927935, 72057594037927936]

    def testInts(self):
        for i in self.INTS:
            s = keycoder.pack_int('', i)
            j = keycoder.unpack_int(s)
            assert j == i, (i, j, s)


@register()
class IntKeyTest:
    INTS = [-1, -239, -240, -241, -2285, -2286, -2287, 0, 1, 0xfffff]

    def test1(self):
        for i in self.INTS:
            s = keycoder.packs('', i)
            try:
                j, = keycoder.unpack('', s)
                eq(j, i)
            except:
                print [i, s]
                raise


@register(python=True)
class SameIntEncodingTest:
    def test1(self):
        for i in EncodeIntTest.INTS:
            native = _keycoder.packs('', i)
            python = keycoder.packs('', i)
            try:
                eq(native, python)
            except:
                print 'failing int was ' + str(i)
                raise


@register()
class TupleTest:
    def assertOrder(self, tups):
        tups = map(keycoder.Key, tups)
        encs = map(keycoder.packs, tups)
        encs.sort()
        eq(tups, [keycoder.unpack(x) for x in encs])

    def testStringSorting(self):
        strs = [(x,) for x in ('dave', 'dave\x00', 'dave\x01', 'davee\x01')]
        encs = map(lambda o: keycoder.packs('', o), strs)
        encs.sort()
        eq(strs, [keycoder.unpack('', x) for x in encs])

    def testTupleNonTuple(self):
        pass


@register()
class UuidTest:
    def testUuid(self):
        t = ('a', uuid.uuid4(), 'b')
        s = keycoder.packs('', t)
        eq(t, keycoder.unpack('', s))


@register()
class Mod7BugTest:
    def test1(self):
        t = [('', 11, 'item')]
        p = keycoder.packs('', t)
        eq(keycoder.unpacks('', p), t)

    def test2(self):
        t = [('index:I', 11, 'item')]
        p = keycoder.packs('', t)
        eq(keycoder.unpacks('', p), t)

    def test3(self):
        t = [('index:Item:first_last', 11, 'item')]
        p = keycoder.packs('', t)
        eq(keycoder.unpacks('', p), t)


@register(python=True)
class NativeTimeTest:
    def test_utc(self):
        tz = dateutil.tz.gettz('Etc/UTC')
        dt = datetime.now(tz)
        sn = _keycoder.packs('', dt)
        sp = keycoder.packs('', dt)
        eq(sn, sp)

        dn = _keycoder.unpacks('', sn)
        dp = keycoder.unpacks('', sp)
        eq(dn, dp)

    def test_naive(self):
        dt = datetime.now()
        sn = _keycoder.packs('', dt)
        sp = keycoder.packs('', dt)
        eq(sn, sp)

    def test_neg(self):
        tz = dateutil.tz.gettz('Etc/GMT-1')
        dt = datetime.now(tz)
        sn = _keycoder.packs('', dt)
        sp = keycoder.packs('', dt)
        eq(sn, sp)

        dn = _keycoder.unpacks('', sn)
        dp = keycoder.unpacks('', sp)
        eq(dn, dp)

    def test_pos(self):
        tz = dateutil.tz.gettz('Etc/GMT+1')
        dt = datetime.now(tz)
        sn = _keycoder.packs('', dt)
        sp = keycoder.packs('', dt)
        eq(sn, sp)

        dn = _keycoder.unpacks('', sn)
        dp = keycoder.unpacks('', sp)
        eq(dn, dp)


@register()
class TimeTest:
    def _now_truncate(self, tz=None):
        dt = datetime.now(tz)
        return dt.replace(microsecond=(dt.microsecond / 1000) * 1000)

    def test_naive(self):
        dt = self._now_truncate()
        s = keycoder.packs('', dt)
        dt2, = keycoder.unpack('', s)
        eq(dt.utctimetuple(), dt2.utctimetuple())

    def test_utc(self):
        tz = dateutil.tz.gettz('Etc/UTC')
        dt = self._now_truncate(tz)
        s = keycoder.packs('', dt)
        dt2, = keycoder.unpack('', s)
        eq(dt, dt2)

    def test_plusone(self):
        tz = dateutil.tz.gettz('Etc/GMT+1')
        dt = self._now_truncate(tz)
        s = keycoder.packs('', dt)
        dt2, = keycoder.unpack('', s)
        eq(dt, dt2)

    def test_minusone(self):
        tz = dateutil.tz.gettz('Etc/GMT-1')
        dt = self._now_truncate(tz)
        s = keycoder.packs('', dt)
        dt2, = keycoder.unpack('', s)
        eq(dt, dt2)


@register()
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
            packed = map(lambda s: keycoder.packs('', s), seq)
            rnge = range(len(packed))
            done = sorted(rnge, key=packed.__getitem__)
            try:
                eq(done, rnge)
            except:
                print 'failed:', seq
                raise


if __name__ == '__main__':
    unittest.main()
