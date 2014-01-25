
#
# Compare behaviour when batches are large and record size is tiny.
#

import gzip
import random
import time

import acid
import acid.core
import acid.keylib

from demo_util import store_len
from demo_util import store_size


words = sorted(line.decode().strip()
               for line in gzip.open('words.gz'))
words = words[:1000]

rand = range(1, 1+len(words))
random.shuffle(rand)


def rands(coll, keys):
    t = time.time()
    for k in keys:
        assert coll.get(k, raw=True) is not None, [k]
    t1 = time.time()
    return len(keys) / (t1 - t)


for strat_klass in acid.core.BatchV2Strategy, acid.core.BatchStrategy, :
    compressor = acid.encoders.ZLIB

    store = acid.open('list:/')
    store.begin(write=True).__enter__()
    doink = store.add_collection('doink')
    prefix = acid.keylib.pack_int(doink.info['idx'], store.prefix)
    strat = strat_klass(prefix, store, compressor)
    doink.strategy = strat

    for word in words:
        doink.put(word)

    print 'done', strat, compressor
    print 'before len:', store_len(store)
    print 'before size:', store_size(store)
    print 'avgsz:', store_size(store)/store_len(store)
    print 'look/sec', rands(doink, rand)
    print

    strat.batch(max_bytes=2000)

    print 'done', strat, compressor
    print 'after len:', store_len(store)
    print 'after size:', store_size(store)
    print 'avgsz:', store_size(store)/store_len(store)
    print 'look/sec', rands(doink, rand)
    print

    li = store.engine.items[-1]
    lk = li[0]
    lv = li[1]
