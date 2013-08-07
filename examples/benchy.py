
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
import pymongo

writer = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
out = lambda *args: writer.writerow(args)

BASE_PATH = '/ram/benchy/'
if not os.path.exists(BASE_PATH):
    os.mkdir(BASE_PATH, 0744)


class CentiEngine(object):
    ENCODER = centidb.support.make_msgpack_encoder()
    KEY_FUNC = operator.itemgetter('name', 'location')

    def create(self):
        if self.PATH and os.path.exists(self.PATH):
            shutil.rmtree(self.PATH)
        self.make_engine()

    def make_coll(self, use_indices):
        self.coll = self.store.collection('stuff',
            encoder=self.ENCODER, key_func=self.KEY_FUNC)
        if use_indices:
            self.coll.add_index('rev_name', lambda p: p['name'][::-1])
            self.coll.add_index('rev_locn', lambda p: p['location'][::-1])

    def close(self):
        self.store.engine.close()
        shutil.rmtree(self.PATH)

    def insert(self, words, upper, stub, blind):
        coll = self.coll
        txn = self.store.engine.begin(write=True)
        for i in xrange(len(words)):
            doc = {'stub': stub, 'name': words[i], 'location': upper[i]}
            coll.put(doc, blind=blind, txn=txn)
        txn.commit()


class LmdbEngine(CentiEngine):
    PATH = BASE_PATH + 'test.lmdb'
    def make_engine(self):
        self.store = centidb.open('LmdbEngine',
            path=self.PATH, map_size=1048576*1024)


class SkiplistEngine(CentiEngine):
    PATH = None
    def make_engine(self):
         self.store = centidb.open('SkiplistEngine', maxsize=int(1e9))


class PlyvelEngine(CentiEngine):
    PATH = BASE_PATH + 'test.ldb'

    def make_engine(self):
        self.store = centidb.open('PlyvelEngine', name=self.PATH,
                                  create_if_missing=True)


class MongoEngine(object):
    def create(self):
        pass

    def make_coll(self, use_indices):
        self.store = pymongo.MongoClient()
        self.store.drop_database('benchy')
        os.system('sudo rm -rf /ram/mongodb/benchy*')
        self.store.benchy.create_collection('stuff')
        self.coll = self.store.benchy.stuff
        if use_indices:
            self.coll.ensure_index('name', 1)
            self.coll.ensure_index('location', 1)

    def insert(self, words, upper, stub, blind):
        coll = self.coll
        for i in xrange(len(words)):
            doc = {'stub': stub, 'name': words[i], 'location': upper[i]}
            coll.insert(doc)


def x():
    global store

    words = map(str.strip, file('/usr/share/dict/words'))
    random.shuffle(words)
    upper = map(str.upper, words)
    stub = len('%x' % (random.getrandbits(150*4)))

    out('Engine', 'Blind', 'UseIndices', 'Keys', 'Time', 'Ops/s')
    for engine in LmdbEngine, SkiplistEngine, PlyvelEngine, MongoEngine:
        for blind in True, False:
            print
            for use_indices in True, False:
                eng = engine()
                eng.create()
                eng.make_coll(use_indices)

                t0 = time.time()
                eng.insert(words, upper, stub, blind)
                eng.close()
                t = time.time() - t0

                keycnt = len(words) * (3 if use_indices else 1)
                engine_name = engine.__name__
                out(engine_name, blind, use_indices, keycnt, '%.2f' % t,
                    '%.2f' % ((keycnt if blind else (keycnt + len(words))) / t))

x()
