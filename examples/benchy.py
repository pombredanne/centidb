
from __future__ import absolute_import
import csv
import gzip
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
            self.coll.add_index('rev_name', lambda p: p['name'])
            self.coll.add_index('rev_locn', lambda p: p['location'])

    def close(self):
        self.store.engine.close()
        if self.PATH:
            shutil.rmtree(self.PATH)

    def insert(self, words, upper, stub, blind):
        coll = self.coll
        txn = self.store.engine.begin(write=True)
        for i in xrange(len(words)):
            doc = {'stub': stub, 'name': words[i], 'location': upper[i]}
            coll.put(doc, blind=blind, txn=txn)
        txn.commit()

    def randget_idx(self, words):
        index = self.coll.indices['rev_name']
        txn = self.store.engine.begin()
        for word in words:
            index.get(word, txn=txn)

    def randget_id(self, words, upper):
        txn = self.store.engine.begin()
        coll = self.coll
        for i in xrange(len(words)):
            coll.get((words[i], upper[i]), txn=txn)


class LmdbEngine(CentiEngine):
    PATH = BASE_PATH + 'test.lmdb'
    def make_engine(self):
        self.store = centidb.open('LmdbEngine',
            path=self.PATH, map_size=1048576*1024,
            writemap=True)


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
            doc = {'stub': stub, 'name': words[i], 'location': upper[i],
                   '_id': '%s-%s' % (words[i], upper[i])}
            coll.insert(doc)

    def randget_idx(self, words):
        coll = self.coll
        for word in words:
            coll.find_one({'name': word})

    def randget_id(self, words, upper):
        coll = self.coll
        for i in xrange(len(words)):
            coll.find_one('%s-%s' % (words[i], upper[i]))

    def close(self):
        pass


def x():
    global store

    words = map(str.strip, gzip.open('words.gz'))
    random.shuffle(words)
    upper = map(str.upper, words)
    stub = len('%x' % (random.getrandbits(150*4)))

    ids = range(len(words))
    random.shuffle(ids)

    out('Engine', 'Blind', 'UseIndices', 'Keys', 'Time', 'Ops/s')
    eng = None
    for engine in LmdbEngine, SkiplistEngine, PlyvelEngine, MongoEngine:
        print
        for blind in True, False:
            for use_indices in False, True:
                if eng:
                    eng.close()
                eng = engine()
                eng.create()
                eng.make_coll(use_indices)

                t0 = time.time()
                eng.insert(words, upper, stub, blind)
                t = time.time() - t0

                keycnt = len(words) * (3 if use_indices else 1)
                engine_name = engine.__name__
                out(engine_name, blind, use_indices, keycnt, '%.2f' % t,
                    int((keycnt if blind else (keycnt + len(words))) / t))

        idxcnt = 0
        t0 = time.time()
        while time.time() < (t0 + 5):
            eng.randget_idx(words)
            idxcnt += len(words)
        idxtime = time.time() - t0
        out(engine_name, '-', 'idx', idxcnt, '%.2f' % t, int(idxcnt/idxtime))

        idcnt = 0
        t0 = time.time()
        while time.time() < (t0 + 5):
            eng.randget_id(words, upper)
            idcnt += len(words)
        idtime = time.time() - t0
        out(engine_name, '-', 'id', idcnt, '%.2f' % t, int(idcnt/idtime))

x()
