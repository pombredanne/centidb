
import bisect
import unittest

import centidb
from centidb import encode_keys
from centidb import decode_keys


class TestDb(object):
    def __init__(self):
        self.pairs = []

    def delete(self, k):
        idx = bisect.bisect_left(self.pairs, (k,))
        if idx < (len(self.pairs) - 1):
            if self.pairs[idx][0] == k:
                self.pairs.pop(idx)

    def put(self, k, v):
        self.delete(k)
        idx = bisect.bisect_left(self.pairs, (k,))
        self.pairs.insert(idx, (k, v))

    def get(self, k):
        idx = bisect.bisect_left(self.pairs, (k,))
        if idx < (len(self.pairs) - 1):
            return self.pairs[idx][1]

    def iterator(self):


class KeysTestCase(unittest.TestCase):
    SINGLE_VALS = [
        None,
        1,
        'x',
        centidb.Key('zerp'),
        u'hehe',
        True,
        False,
        -1
    ]

    def test_single(self):
        for val in self.SINGLE_VALS:
            encoded = encode_keys((val,))
            decoded = decode_keys(encoded)
            assert decoded == [val] and type(val) is type(decoded[0]),\
                'eek %r' % ((decoded, val, type(val), type(decoded[0])),)

    def test_single_sort_lower(self):
        for val in self.SINGLE_VALS:
            e1 = encode_keys((val,))
            e2 = encode_keys(((val,),))
            assert e1 < e2, 'eek %r' % ((e1, e2, val),)


if __name__ == '__main__':
    unittest.main()
