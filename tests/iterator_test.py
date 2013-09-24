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
Iterator implementation tests.
"""

import operator

import acid.engines
import acid.iterators
import acid.keylib

import testlib
from testlib import eq
from testlib import le
from testlib import lt

PREFIX = 'P_'
KEYSETS = [
    ('O_', ['OCCURS_BEFORE_PREFIX']),
    ('P_', 'A B BB C CC D'.split()),
    ('Q_', ['OCCURS_AFTER_PREFIX'])
]

PKEYS = KEYSETS[1][1] # [nomatch: 0] A B BB C CC D [nomatch: a]
RPKEYS = PKEYS[::-1]


BATCH_KEYSETS = [
    ('O_', [
        ['OCCURS_BEFORE_PREFIXA',],
        ['OCCURS_BEFORE_PREFIXB',],
        # And one batch key
        ['OCCURS_BEFORE_PREFIXD', 'OCCURS_BEFORE_PREFIXC',]
    ]),
    ('P_', [
        [('B',), ('A',)],
        [('BB',)],
        [('BD',), ('BC',)],
        [('CC',), ('CB',), ('CA',)],
        [('D',)]
    ])
]

BPKEYS = list(sorted(reduce(operator.add, BATCH_KEYSETS[1][1])))
RBPKEYS = BPKEYS[::-1]

print 'BPKEYS', BPKEYS
print 'RBPKEYS', RBPKEYS

def key0from(genfunc):
    return [g.keys[0][0] for g in genfunc()]

def keyfrom(genfunc):
    return [g.key for g in genfunc()]


@testlib.register()
class RangeIteratorTest:
    def setUp(self):
        self.engine = acid.engines.ListEngine()
        self.rit = acid.iterators.RangeIterator(self.engine, PREFIX)
        self.fill()

    def fill(self):
        for prefix, keys in KEYSETS:
            for key in keys:
                phys = acid.keylib.Key(key).to_raw(prefix)
                self.engine.put(phys, key)

    # Test set_lo() with key <= start of file, >= end of file, some existent
    # key, some nonexistent key, open and closed.

    def test_lo_closed_nomatch_sof(self):
        self.rit.set_lo('0', closed=True)
        eq(PKEYS, key0from(self.rit.forward))
        eq(RPKEYS, key0from(self.rit.reverse))

    def test_lo_closed_nomatch_eof(self):
        self.rit.set_lo('a', closed=True)
        eq([], key0from(self.rit.forward))
        eq([], key0from(self.rit.reverse))

    def test_lo_closed_match(self):
        self.rit.set_lo('B', closed=True)
        eq(PKEYS[1:], key0from(self.rit.forward))
        eq(RPKEYS[:-1], key0from(self.rit.reverse))

    def test_lo_closed_nomatch(self):
        self.rit.set_lo('BA', closed=True)
        eq(PKEYS[2:], key0from(self.rit.forward))
        eq(RPKEYS[:-2], key0from(self.rit.reverse))

    # open

    def test_lo_open_nomatch_sof(self):
        self.rit.set_lo('0', closed=False)
        eq(PKEYS, key0from(self.rit.forward))
        eq(RPKEYS, key0from(self.rit.reverse))

    def test_lo_open_nomatch_eof(self):
        self.rit.set_lo('a', closed=False)
        eq([], key0from(self.rit.forward))
        eq([], key0from(self.rit.reverse))

    def test_lo_open_match(self):
        self.rit.set_lo('B', closed=False)
        eq(PKEYS[2:], key0from(self.rit.forward))
        eq(RPKEYS[:-2], key0from(self.rit.reverse))

    def test_lo_open_nomatch(self):
        self.rit.set_lo('BA', closed=False)
        eq(PKEYS[2:], key0from(self.rit.forward))
        eq(RPKEYS[:-2], key0from(self.rit.reverse))

    # Test set_hi() with key <= start of file, >= end of file, some existent
    # key, some nonexistent key, open and closed.

    def test_hi_closed_nomatch_sof(self):
        self.rit.set_hi('0', closed=True)
        eq([], key0from(self.rit.forward))
        eq([], key0from(self.rit.reverse))

    def test_hi_closed_nomatch_eof(self):
        self.rit.set_hi('a', closed=True)
        eq(PKEYS, key0from(self.rit.forward))
        eq(RPKEYS, key0from(self.rit.reverse))

    def test_hi_closed_match(self):
        self.rit.set_hi('B', closed=True)
        eq(PKEYS[:2], key0from(self.rit.forward))
        eq(RPKEYS[-2:], key0from(self.rit.reverse))

    def test_hi_closed_nomatch(self):
        self.rit.set_hi('BA', closed=True)
        eq(PKEYS[:2], key0from(self.rit.forward))
        eq(RPKEYS[-2:], key0from(self.rit.reverse))

    # Test set_exact() with key <= start of file, >= end of file, some existent
    # key, some nonexistent key, open and closed.

    def test_exact_closed_nomatch_sof(self):
        self.rit.set_exact('0')
        eq([], key0from(self.rit.forward))
        eq([], key0from(self.rit.reverse))

    def test_exact_closed_nomatch_eof(self):
        self.rit.set_exact('a')
        eq([], key0from(self.rit.forward))
        eq([], key0from(self.rit.reverse))

    def test_exact_closed_match(self):
        self.rit.set_exact('B')
        eq([PKEYS[1]], key0from(self.rit.forward))
        eq([RPKEYS[-2]], key0from(self.rit.reverse))

    def test_exact_closed_nomatch(self):
        self.rit.set_exact('BA')
        eq([], key0from(self.rit.forward))
        eq([], key0from(self.rit.reverse))

    # Test max by itself, and with options.

    def test_no_max(self):
        eq(PKEYS, key0from(self.rit.forward))

    def test_no_max_reverse(self):
        eq(RPKEYS, key0from(self.rit.reverse))

    def test_max_1(self):
        self.rit.set_max(1)
        eq([PKEYS[0]], key0from(self.rit.forward))

    def test_max_1_reverse(self):
        self.rit.set_max(1)
        eq([RPKEYS[0]], key0from(self.rit.reverse))

    def test_max_2(self):
        self.rit.set_max(2)
        eq(2, len(list(self.rit.forward())))

    def test_max_1_open_lo(self):
        self.rit.set_max(1)
        self.rit.set_lo('A', closed=False)
        eq([PKEYS[1]], key0from(self.rit.forward))

    def test_max_1_reverse_open_hi(self):
        self.rit.set_max(1)
        self.rit.set_hi('D', closed=False)
        eq([RPKEYS[1]], key0from(self.rit.reverse))


@testlib.register()
class BatchRangeIteratorTest:
    def get_compressor(self, s):
        return acid.encoders.PLAIN

    def setUp(self):
        self.engine = acid.engines.ListEngine()
        self.rit = acid.iterators.BatchRangeIterator(self.engine, PREFIX,
            self.get_compressor)
        self.fill()

    def fill(self):
        val = acid.keylib.pack_int(6)
        for _ in xrange(6):
            val += acid.keylib.pack_int(0)
        val += chr(10)

        for prefix, keys in BATCH_KEYSETS:
            for tups in keys:
                phys = acid.keylib.packs(tups, prefix=prefix)
                self.engine.put(phys, val)

    # Test set_lo() with key <= start of file, >= end of file, some existent
    # key, some nonexistent key, open and closed.

    def test_lo_closed_nomatch_sof(self):
        self.rit.set_lo('0', closed=True)
        eq(BPKEYS, keyfrom(self.rit.forward))
        eq(RBPKEYS, keyfrom(self.rit.reverse))

    def test_lo_closed_nomatch_eof(self):
        self.rit.set_lo('a', closed=True)
        eq([], keyfrom(self.rit.forward))
        eq([], keyfrom(self.rit.reverse))

    def test_lo_closed_match(self):
        self.rit.set_lo('B', closed=True)
        eq(BPKEYS[1:], keyfrom(self.rit.forward))
        eq(RBPKEYS[:-1], keyfrom(self.rit.reverse))

    def test_lo_closed_nomatch(self):
        self.rit.set_lo('BA', closed=True)
        eq(BPKEYS[2:], keyfrom(self.rit.forward))
        eq(RBPKEYS[:-2], keyfrom(self.rit.reverse))

    # open

    def test_lo_open_nomatch_sof(self):
        self.rit.set_lo('0', closed=False)
        eq(BPKEYS, keyfrom(self.rit.forward))
        eq(RBPKEYS, keyfrom(self.rit.reverse))

    def test_lo_open_nomatch_eof(self):
        self.rit.set_lo('a', closed=False)
        eq([], keyfrom(self.rit.forward))
        eq([], keyfrom(self.rit.reverse))

    def test_lo_open_match(self):
        self.rit.set_lo('B', closed=False)
        eq(BPKEYS[2:], keyfrom(self.rit.forward))
        eq(RBPKEYS[:-2], keyfrom(self.rit.reverse))

    def test_lo_open_nomatch(self):
        self.rit.set_lo('BA', closed=False)
        eq(BPKEYS[2:], keyfrom(self.rit.forward))
        eq(RBPKEYS[:-2], keyfrom(self.rit.reverse))

    # Test set_hi() with key <= start of file, >= end of file, some existent
    # key, some nonexistent key, open and closed.

    def test_hi_closed_nomatch_sof(self):
        self.rit.set_hi('0', closed=True)
        eq([], keyfrom(self.rit.forward))
        eq([], keyfrom(self.rit.reverse))

    '''
    def test_hi_closed_nomatch_eof(self):
        self.rit.set_hi('a', closed=True)
        eq(PKEYS, key0from(self.rit.forward))
        eq(RPKEYS, key0from(self.rit.reverse))

    def test_hi_closed_match(self):
        self.rit.set_hi('B', closed=True)
        eq(PKEYS[:2], key0from(self.rit.forward))
        eq(RPKEYS[-2:], key0from(self.rit.reverse))

    def test_hi_closed_nomatch(self):
        self.rit.set_hi('BA', closed=True)
        eq(PKEYS[:2], key0from(self.rit.forward))
        eq(RPKEYS[-2:], key0from(self.rit.reverse))

    # Test set_exact() with key <= start of file, >= end of file, some existent
    # key, some nonexistent key, open and closed.

    def test_exact_closed_nomatch_sof(self):
        self.rit.set_exact('0')
        eq([], key0from(self.rit.forward))
        eq([], key0from(self.rit.reverse))

    def test_exact_closed_nomatch_eof(self):
        self.rit.set_exact('a')
        eq([], key0from(self.rit.forward))
        eq([], key0from(self.rit.reverse))

    def test_exact_closed_match(self):
        self.rit.set_exact('B')
        eq([PKEYS[1]], key0from(self.rit.forward))
        eq([RPKEYS[-2]], key0from(self.rit.reverse))

    def test_exact_closed_nomatch(self):
        self.rit.set_exact('BA')
        eq([], key0from(self.rit.forward))
        eq([], key0from(self.rit.reverse))

    # Test max by itself, and with options.

    def test_no_max(self):
        eq(PKEYS, key0from(self.rit.forward))

    def test_no_max_reverse(self):
        eq(RPKEYS, key0from(self.rit.reverse))

    def test_max_1(self):
        self.rit.set_max(1)
        eq([PKEYS[0]], key0from(self.rit.forward))

    def test_max_1_reverse(self):
        self.rit.set_max(1)
        eq([RPKEYS[0]], key0from(self.rit.reverse))

    def test_max_2(self):
        self.rit.set_max(2)
        eq(2, len(list(self.rit.forward())))

    def test_max_1_open_lo(self):
        self.rit.set_max(1)
        self.rit.set_lo('A', closed=False)
        eq([PKEYS[1]], key0from(self.rit.forward))

    def test_max_1_reverse_open_hi(self):
        self.rit.set_max(1)
        self.rit.set_hi('D', closed=False)
        eq([RPKEYS[1]], key0from(self.rit.reverse))
    '''



if __name__ == '__main__':
    testlib.main()
