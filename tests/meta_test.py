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

"""
acid.meta tests
"""


import acid
import acid.meta

import testlib


class Model(acid.meta.Model):
    name = acid.meta.String()

    @acid.meta.index
    def by_name(self):
        return self.name

    @acid.meta.constraint
    def check_name(self):
        return self.name and self.name[0].isupper()


class TestBase:
    def setUp(self):
        self.store = acid.open('ListEngine')
        Model.bind_store(self.store)


@testlib.register()
class TestSave(TestBase):
    def test_save(self):
        mod = Model(name=u'Dave')
        assert not mod.is_saved
        with self.store.begin(write=True):
            mod.save()
        assert mod.is_saved
        assert mod.key == (1,)
        with self.store.begin():
            assert len(list(mod.META_COLLECTION.items())) == 1

    def test_resave(self):
        with self.store.begin(write=True):
            mod = Model(name=u'Dave')
            mod.save()
        with self.store.begin(write=True):
            mod.save()
            assert len(list(mod.META_COLLECTION.items())) == 1
        assert mod.key == (1,)

    def test_index(self):
        with self.store.begin(write=True):
            mod = Model(name=u'Dave')
            mod.save()
        with self.store.begin():
            assert Model.by_name.get(u'Dave') is not None
            assert len(list(Model.by_name.keys('Dave'))) == 1


if __name__ == '__main__':
    testlib.main()
