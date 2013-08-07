
from __future__ import absolute_import
import csv
import operator
import os
import random
import shutil
import sys
import time

import centidb
import centidb.support

writer = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
out = lambda *args: writer.writerow(args)

BASE_PATH = '/ram/benchy/'
if not os.path.exists(BASE_PATH):
    os.mkdir(BASE_PATH, 0744)

ENGINES = [
    ('lmdb', BASE_PATH + 'test.lmdb',
     lambda: centidb.open('LmdbEngine', path=BASE_PATH + 'test.lmdb',
                          map_size=1048576*1024)),
    ('skiplist', None,
     lambda: centidb.open('SkiplistEngine', maxsize=int(1e9))),
    ('plyvel', BASE_PATH + 'test.ldb',
     lambda: centidb.open('PlyvelEngine', name=BASE_PATH + 'test.ldb',
                          create_if_missing=True)),
]


def x():
    global store

    words = map(str.strip, file('/usr/share/dict/words'))
    random.shuffle(words)
    upper = map(str.upper, words)
    stub = len('%x' % (random.getrandbits(150*4)))

    encoder = centidb.support.make_msgpack_encoder()
    key_func = operator.itemgetter('name', 'location')

    out('Engine', 'Blind', 'UseIndices', 'Keys', 'Time', 'Ops/s')
    for engine_name, engine_path, engine_factory in ENGINES:
        for blind in True, False:
            for use_indices in True, False:
                if engine_path and os.path.exists(engine_path):
                    shutil.rmtree(engine_path)
                store = engine_factory()

                coll = store.collection('stuff',
                    encoder=encoder, key_func=key_func)
                if use_indices:
                    coll.add_index('rev_name', lambda p: p['name'][::-1])
                    coll.add_index('rev_locn', lambda p: p['location'][::-1])

                t0 = time.time()
                txn = store.engine.begin(write=True)
                for i in xrange(len(words)):
                    doc = {'stub': stub, 'name': words[i], 'location': upper[i]}
                    coll.put(doc, blind=blind, txn=txn)
                txn.commit()
                store.engine.close()
                t = time.time() - t0

                keycnt = len(words) * (1 + len(coll.indices))
                out(engine_name, blind, use_indices, keycnt, '%.2f' % t,
                    '%.2f' % ((keycnt if blind else (keycnt + len(words))) / t)
)
x()
