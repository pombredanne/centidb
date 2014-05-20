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

import time
import acid.events
import acid.meta


class Model(acid.meta.Model):
    """Base class for all cheeseboard models.
    """


class Post(Model):
    name = acid.meta.String()
    text = acid.meta.String()
    created = acid.meta.Time()

    @acid.events.constraint
    def check_name(self):
        return self.name and len(self.name) >= 3

    @acid.events.constraint
    def check_text(self):
        return self.text and len(self.text) > 5

    @acid.events.on_create
    def set_created(self):
        self.created = time.time()


def init_store():
    global store
    url = 'lmdb:store.lmdb;map_size=2040;map_async;nosync;nometasync;writemap'
    store = acid.open(url)
    Model.bind_store(store)
    return store
