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

TMP_PATH = os.environ.get('ACID_TMPDIR', '/tmp') + '/tmp.db'
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
    LZ4 = acid.encoders.Compressor('lz4', lz4.loads, lz4.dumps)
except ImportError:
    LZ4 = None

try:
    import snappy
    SNAPPY = acid.encoders.Compressor('snappy',
                snappy.uncompress, snappy.compress)
except ImportError:
    SNAPPY = None


print '"Packer","Size","Count","Ratio","BatchSz","Gets/sec","Iters/sec","Iterrecs/sec"'

def out(*args):
    print '"%s","%.2fkb","%d","%.2f","%d","%.2f","%.2f","%.2f"' % args

def store_count(store):
    it = store._txn_context.get().iter('', False)
    return sum(1 for _ in it)

def store_size(store):
    it = store._txn_context.get().iter('', False)
    return sum(len(k) + len(v) for k, v in it)


for packer in acid.encoders.ZLIB, SNAPPY, LZ4:
    if not packer:
        continue

    print
    for bsize in 1, 2, 4, 5, 8, 16, 32, 64:
        if os.path.exists(TMP_PATH):
            shutil.rmtree(TMP_PATH)
        st = acid.open('LmdbEngine', path=TMP_PATH, map_size=1048576*1024)
        with st.begin(write=True):
            co = st.add_collection('people',
                encoder=acid.encoders.make_json_encoder(sort_keys=True))

            keys = [co.put(rec) for rec in recs]
            random.shuffle(keys)
            nextkey = iter(itertools.cycle(keys)).next

            before = store_size(st)
            itemcount = store_count(st)

            if bsize == 1:
                iterrecs, te = dotestiter()
                out('plain', before / 1024., itemcount, 1, 1, dotestget(), te, iterrecs)
            co.batch(max_recs=bsize, packer=packer)

            itemcount = store_count(st)
            after = store_count(st)

            iterrecs, te = dotestiter()
            out(packer.name, after / 1024., itemcount, float(before) / after,
                bsize, dotestget(), te, iterrecs)
