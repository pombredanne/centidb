
import random
import time

import centidb
import centidb.support

lines = file('/etc/services').readlines()
header = random.sample(lines, 150)
footer = random.sample(lines, 150)

def dotest():
    t0 = time.time()
    cnt = 0
    while (time.time() - t0) < 2:
        for x in xrange(100):
            co.get(random.choice(keys))
            cnt += 1
    return cnt / (time.time() - t0)


for bsize in 1, 2, 4, 5, 8, 16, 32, 64:
    le = centidb.support.ListEngine()
    st = centidb.Store(le)
    co = centidb.Collection(st, 'people')

    keys = []
    for x in range(400):
        data = header + random.sample(lines, 30) + footer
        keys.append(co.put(data).key)

    before = le.size

    if bsize == 1:
        print 'Before size %7.2fkb count %4d %26s (%4.2f gets/sec)' %\
            (before / 1024., len(le.pairs), '', dotest())
    co.batch(max_recs=bsize, packer=centidb.ZLIB_PACKER)

    print ' After size %7.2fkb count %4d ratio %5.2f (batch size %2d, %4.2f gets/sec)' %\
        (le.size / 1024., len(le.pairs), float(before) / le.size, bsize, dotest())
