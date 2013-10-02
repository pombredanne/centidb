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

from __future__ import absolute_import
import csv
import gzip
import operator
import os
import random
import shutil
import sqlite3
import sys
import time

import acid
import acid.encoders

try:
    import pymongo
except ImportError:
    pymongo = None

try:
    import plyvel
except ImportError:
    plyvel = None

writer = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
out = lambda *args: writer.writerow(args)

USE_SPARSE_FILES = sys.platform != 'darwin'
BASE_PATH = '/ram/benchy/'
if not os.path.exists(BASE_PATH):
    os.mkdir(BASE_PATH, 0744)


class AcidEngine(object):
    ENCODER = acid.encoders.make_msgpack_encoder()
    KEY_FUNC = operator.itemgetter('name', 'location')

    def create(self):
        if self.PATH and os.path.exists(self.PATH):
            shutil.rmtree(self.PATH)
        self.make_engine()

    def make_coll(self, use_indices):
        with self.store.begin(write=True):
            self.coll = self.store.add_collection('stuff',
                encoder=self.ENCODER,
                key_func=self.KEY_FUNC)
            if use_indices:
                self.coll.add_index('rev_name', lambda p: p['name'])
                self.coll.add_index('rev_locn', lambda p: p['location'])

    def close(self):
        self.store.engine.close()
        if self.PATH:
            shutil.rmtree(self.PATH)

    def insert(self, words, upper, stub, blind):
        coll = self.coll
        with self.store.begin(write=True):
            for i in xrange(len(words)):
                doc = {'stub': stub, 'name': words[i], 'location': upper[i]}
                coll.put(doc, blind=blind)

    def randget_idx(self, words):
        index = self.coll.indices['rev_name']
        with self.store.begin():
            for word in words:
                index.get(word)

    def randget_id(self, words, upper):
        coll = self.coll
        with self.store.begin():
            for i in xrange(len(words)):
                coll.get((words[i], upper[i]))


class LmdbEngine(AcidEngine):
    PATH = BASE_PATH + 'test.lmdb'
    def make_engine(self):
        self.store = acid.open('LmdbEngine',
            path=self.PATH, map_size=1048576*1024,
            writemap=USE_SPARSE_FILES)


class SkiplistEngine(AcidEngine):
    PATH = None
    def make_engine(self):
         self.store = acid.open('SkiplistEngine', maxsize=int(1e9))


class PlyvelEngine(AcidEngine):
    PATH = BASE_PATH + 'test.ldb'

    def make_engine(self):
        self.store = acid.open('PlyvelEngine', name=self.PATH,
                                  create_if_missing=True)


class SqliteEngine(object):
    PATH = BASE_PATH + 'test.sqlite3'

    def create(self):
        if self.PATH and os.path.exists(self.PATH):
            os.unlink(self.PATH)
        self.make_engine()

    def make_engine(self):
        self.db = sqlite3.connect(self.PATH)

    def make_coll(self, use_indices):
        self.db.execute('CREATE TABLE stuff(stub, name, location)')
        if use_indices:
            self.db.execute('CREATE INDEX foo ON stuff(name)')
            self.db.execute('CREATE INDEX bar ON stuff(location)')

    def close(self):
        self.db.close()
        if self.PATH:
            os.unlink(self.PATH)

    def insert(self, words, upper, stub, blind):
        c = self.db.cursor()
        for i in xrange(len(words)):
            c.execute('INSERT INTO stuff VALUES(?, ?, ?)',
                      (stub, words[i], upper[i]))
        print 'commit'
        self.db.commit()
        print 'done'

    def randget_idx(self, words):
        c = self.db.cursor()
        for word in words:
            c.execute('SELECT * FROM stuff WHERE name = ?', (word,))
            next(c)

    def randget_id(self, words, upper):
        c = self.db.cursor()
        for i in xrange(len(words)):
            c.execute('SELECT * FROM stuff WHERE oid = ?', (i,))
            next(c)


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


def mode_name(blind, use_indices):
    return 'insert-%sblind-%sindices' %\
        ('' if blind else 'no',
         '' if use_indices else 'no')



import locale

locale.setlocale(locale.LC_ALL, 'en_US')
f = lambda f, n: locale.format(f, n, grouping=True)


def x():
    global store

    words = gzip.open('words.gz').read().split()
    random.shuffle(words)
    upper = map(str.upper, words)
    stub = len('%x' % (random.getrandbits(150*4)))

    ids = range(len(words))
    random.shuffle(ids)

    engines = [LmdbEngine, SkiplistEngine] #, SqliteEngine]
    if plyvel:
        engines += [PlyvelEngine]
    if pymongo:
        engines += [MongoEngine]

    out('Engine', 'Mode', 'Keys', 'Time', 'Ops/s')
    eng = None
    for engine in engines:
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

                out(engine_name, mode_name(blind, use_indices),
                    f('%d', keycnt),
                    f('%.2f', t),
                    f('%d', int((keycnt if blind else (keycnt + len(words))) / t)))

        idxcnt = 0
        t0 = time.time()
        while time.time() < (t0 + 5):
            eng.randget_idx(words)
            idxcnt += len(words)
        idxtime = time.time() - t0
        out(engine_name, 'rand-index',
            f('%d', idxcnt),
            f('%.2f', idxtime),
            f('%d', int(idxcnt/idxtime)))

        idcnt = 0
        t0 = time.time()
        while time.time() < (t0 + 5):
            eng.randget_id(words, upper)
            idcnt += len(words)
        idtime = time.time() - t0
        out(engine_name, 'rand-key',
            f('%d', idcnt),
            f('%.2f', idtime),
            f('%d', int(idcnt/idtime)))

x()
