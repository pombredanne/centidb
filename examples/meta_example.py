#!/usr/bin/env python

#
# NOTE: acid.meta is incomplete. This exmple does not function.
#

import acid
from acid import meta


class MyModel(meta.Model):
    pass


class Item(MyModel):
    email = meta.String()
    password = meta.String()
    first_name = meta.String()
    last_name = meta.String()
    age = meta.Integer()
    id = meta.Integer()
    parent_id = meta.Integer()

    @meta.key
    def key(self):
        key = [self.id]
        parent_id = self.parent_id
        while parent_id:
            key.append(parent_id)
            parent = self.get(id=parent_id)
            assert parent
            parent_id = parent.id
        return reversed(key)

    @meta.index
    def first_last(self):
        return self.first, self.last


store = acid.open('ListEngine')
MyModel.bind_store(store)

i = Item()
print i
i.save()
