
import gzip
import itertools
import json
import os
import random
import shutil
import time

import acid
import acid.encoders
import acid.engines

TMP_PATH = '/ram/tmp.db'
INPUT_PATH = os.path.join(os.path.dirname(__file__), 'laforge.json.gz')
recs = json.load(gzip.open(INPUT_PATH))


def dotestget():
    t0 = time.time()
    cnt = 0
    while (time.time() - t0) < 2:
        for x in xrange(100):
            co.get(nextkey(), raw=True)
            cnt += 1
    return cnt / (time.time() - t0)

def dotestiter():
    t0 = time.time()
    cnt = 0
    recs = 0
    while (time.time() - t0) < 2:
        recs += sum(1 for _ in co.items(nextkey()))
        cnt += 1
    return recs / (time.time() - t0), cnt / (time.time() - t0)

try:
    import lz4
    LZ4_PACKER = acid.Encoder('lz4', lz4.loads, lz4.dumps)
except ImportError:
    LZ4_PACKER = None

try:
    import snappy
    SNAPPY_PACKER = acid.Encoder('snappy',
        snappy.uncompress,
        snappy.compress)
except ImportError:
    SNAPPY_PACKER = None


print '"Packer","Size","Count","Ratio","BatchSz","Gets/sec","Iters/sec","Iterrecs/sec"'

def out(*args):
    print '"%s","%.2fkb","%d","%.2f","%d","%.2f","%.2f","%.2f"' % args

def engine_count(engine):
    return sum(1 for _ in engine.iter('', False))

def engine_size(engine):
    return sum(len(k) + len(v) for k, v in engine.iter('', False))


for packer in acid.encoders.ZLIB_PACKER, SNAPPY_PACKER, LZ4_PACKER:
    if not packer:
        continue

    print
    for bsize in 1, 2, 4, 5, 8, 16, 32, 64:
        if os.path.exists(TMP_PATH):
            shutil.rmtree(TMP_PATH)
        st = acid.open('LmdbEngine', path=TMP_PATH, map_size=1048576*1024)
        co = st.add_collection('people',
            encoder=acid.encoders.make_json_encoder(sort_keys=True))

        keys = [co.put(rec).key for rec in recs]
        random.shuffle(keys)
        nextkey = iter(itertools.cycle(keys)).next

        before = engine_size(st.engine)
        itemcount = engine_count(st.engine)

        if bsize == 1:
            iterrecs, te = dotestiter()
            out('plain', before / 1024., itemcount, 1, 1, dotestget(), te, iterrecs)
        co.batch(max_recs=bsize, packer=packer)

        itemcount = engine_count(st.engine)
        after = engine_size(st.engine)

        iterrecs, te = dotestiter()
        out(packer.name, after / 1024., itemcount, float(before) / after,
            bsize, dotestget(), te, iterrecs)
