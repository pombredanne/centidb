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

"""Hodge-podge of Acid specific test functionality.
"""

import operator
import os
import shutil
import sys
import unittest

import acid
import acid.encoders
import acid.engines
import acid.keylib


def ddb():
    pprint(list(db))

def copy(it, dst):
    for tup in it:
        dst.put(*tup)


def make_asserter(op, ops):
    def ass(x, y, msg='', *a):
        if msg:
            if a:
                msg %= a
            msg = ' (%s)' % msg

        f = '%r %s %r%s'
        assert op(x, y), f % (x, ops, y, msg)
    return ass

lt = make_asserter(operator.lt, '<')
eq = make_asserter(operator.eq, '==')
le = make_asserter(operator.le, '<=')


def rm_rf(path):
    if os.path.isfile(path):
        os.unlink(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)


class CountingEngine(acid.engines.Engine):
    def __init__(self, real_engine):
        self.real_engine = real_engine
        self.get_iter_returned = 0
        self.delete_count = 0
        self.delete_keys = set()
        self.put_count = 0
        self.put_keys = set()
        self.get_count = 0
        self.get_keys = set()
        self.get_returned = 0
        self.iter_keys = set()
        self.iter_count = 0
        self.iter_size = 0

    def put(self, key, value):
        self.put_count += 1
        self.put_keys.add(key)
        self.real_engine.put(key, value)

    def get(self, key):
        self.get_count += 1
        self.get_keys.add(key)
        s = self.real_engine.get(key)
        self.get_returned += s is not None
        self.get_iter_returned += s is not None
        return s

    def delete(self, key):
        self.delete_count += 1
        self.delete_keys.add(key)
        self.real_engine.delete(key)

    def iter(self, key, reverse):
        self.iter_keys.add(key)
        self.iter_count += 1
        it = self.real_engine.iter(key, reverse)
        for x in it:
            yield x
            self.iter_size += 1
            self.get_iter_returned += 1


#
# Module reloads are necessary because KEY_ENCODER & co bind whatever
# packs() & co happens to exist before we get a chance to interfere. It
# also improves the chance of noticing any not-planned-for speedups related
# side effects, rather than relying on explicit test coverage.
# 
# There are nicer approaches to this (e.g. make_key_encoder()), but these would
# optimize for the uncommon case of running tests.
#

def _reload_acid():
    global acid
    acid.keylib = reload(acid.keylib)
    acid.engines = reload(acid.engines)
    acid.encoders = reload(acid.encoders)
    acid.core = reload(acid.core)
    acid = reload(acid)


class PythonMixin:
    """Reload modules with speedups disabled."""
    @classmethod
    def setUpClass(cls):
        os.environ['ACID_NO_SPEEDUPS'] = '1'
        _reload_acid()
        getattr(cls, '_setUpClass', lambda: None)()


class NativeMixin:
    """Reload modules with speedups enabled."""
    @classmethod
    def setUpClass(cls):
        os.environ.pop('ACID_NO_SPEEDUPS', None)
        _reload_acid()
        getattr(cls, '_setUpClass', lambda: None)()


def register(enable=True, python=True, native=True):
    def fn(klass):
        if not enable:
            return klass
        globs = vars(sys.modules[klass.__module__])
        if python:
            name = 'Py' + klass.__name__
            cls = type(name, (klass, PythonMixin, unittest.TestCase), {})
            cls.__module__ = klass.__module__
            globs[name] = cls
        if native:
            name = 'C' + klass.__name__
            cls = type(name, (klass, NativeMixin, unittest.TestCase), {})
            cls.__module__ = klass.__module__
            globs[name] = cls
        return klass
    return fn


main = unittest.main
