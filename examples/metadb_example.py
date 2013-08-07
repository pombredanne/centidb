#!/usr/bin/env python

#
# NOTE: centidb.metadb is incomplete. This exmple does not function.
#

import centidb
from centidb import metadb


class MyModel(metadb.Model):
    pass


class Item(MyModel):
    email = metadb.String()
    password = metadb.String()
    first_name = metadb.String()
    last_name = metadb.String()
    age = metadb.Integer()
    id = metadb.Integer()
    parent_id = metadb.Integer()

    @metadb.key
    def key(self):
        key = [self.id]
        parent_id = self.parent_id
        while parent_id:
            key.append(parent_id)
            parent = self.get(id=parent_id)
            assert parent
            parent_id = parent.id
        return reversed(key)

    @metadb.index
    def first_last(self):
        return self.first, self.last


store = centidb.open('ListEngine')
MyModel.bind_store(store)

i = Item()
print i
i.save()
