
import random
import time

import centidb
import centidb.support

lines = file('/etc/services').readlines()
header = random.sample(lines, 150)
footer = random.sample(lines, 150)

recs = [header + random.sample(lines, 30) + footer
        for _ in xrange(400)]

def dotest():
    t0 = time.time()
    cnt = 0
    while (time.time() - t0) < 2:
        for x in xrange(100):
            co.get(random.choice(keys))
            cnt += 1
    return cnt / (time.time() - t0)

try:
    import snappy
    SNAPPY_PACKER = centidb.Encoder('snappy',
        snappy.uncompress,
        snappy.compress)
except ImportError:
    SNAPPY_PACKER = None


for packer in centidb.ZLIB_PACKER, SNAPPY_PACKER:
    if not packer:
        continue

    print
    for bsize in 1, 2, 4, 5, 8, 16, 32, 64:
        le = centidb.support.ListEngine()
        st = centidb.Store(le)
        co = centidb.Collection(st, 'people')

        keys = [co.put(rec).key for rec in recs]
        before = le.size

        if bsize == 1:
            print 'Before size %7.2fkb count %4d %28s (%4.2f gets/sec)' %\
                (before / 1024., len(le.pairs), '', dotest())
        co.batch(max_recs=bsize, packer=packer)

        print ' After size %7.2fkb count %4d ratio %5.2f (%7s size %2d, %4.2f gets/sec)' %\
            (le.size / 1024., len(le.pairs), float(before) / le.size, packer.name, bsize, dotest())
