
import json
import os
import random
import time

import centidb


INPUT_PATH = os.path.join(os.path.dirname(__file__), 'hn-comments.json')
recs = json.load(file(INPUT_PATH))


def dotestget():
    t0 = time.time()
    cnt = 0
    while (time.time() - t0) < 2:
        for x in xrange(100):
            co.get(random.choice(keys), raw=True)
            cnt += 1
    return cnt / (time.time() - t0)

def dotestiter():
    t0 = time.time()
    cnt = 0
    recs = 0
    while (time.time() - t0) < 2:
        recs += sum(1 for _ in co.items(random.choice(keys)))
        cnt += 1
    return recs / (time.time() - t0), cnt / (time.time() - t0)

try:
    import lz4
    LZ4_PACKER = centidb.Encoder('lz4', lz4.loads, lz4.dumps)
except ImportError:
    LZ4_PACKER = None

try:
    import snappy
    SNAPPY_PACKER = centidb.Encoder('snappy',
        snappy.uncompress,
        snappy.compress)
except ImportError:
    SNAPPY_PACKER = None


for packer in centidb.ZLIB_PACKER, SNAPPY_PACKER, LZ4_PACKER:
    if not packer:
        continue

    print
    for bsize in 1, 2, 4, 5, 8, 16, 32, 64:
        le = centidb.support.ListEngine()
        st = centidb.Store(le)
        co = centidb.Collection(st, 'people',
            encoder=centidb.support.make_json_encoder(sort_keys=True))

        keys = [co.put(rec).key for rec in recs]
        before = le.size

        if bsize == 1:
            iterrecs, te = dotestiter()
            print 'Before sz %7.2fkb cnt %4d %28s (%4.2f get/s %4.2f iter/s %4.2f iterrecs/s)' %\
                (before / 1024., len(le.items), '', dotestget(), te, iterrecs)
        co.batch(max_recs=bsize, packer=packer)

        iterrecs, te = dotestiter()
        print ' After sz %7.2fkb cnt %4d ratio %5.2f (%7s size %2d, %4.2f get/s %4.2f iter/s %4.2f iterrecs/s)' %\
            (le.size / 1024., len(le.items), float(before) / le.size,
             packer.name, bsize, dotestget(), te, iterrecs)
