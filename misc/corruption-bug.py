
# Tickle corruption bug

import acid.meta


class Model(acid.meta.Model):
    name = acid.meta.String()

store = acid.open('lmdb:/ram/tdb')
Model.bind_store(store)


with store.begin(write=True):
    for old in iter(Model.find, None):
        old.delete()


with store.begin(write=True):
    keys = [Model(name='wfiowjef'*50).save() for i in range(4096)]

'''
with store.begin(write=True):
    it = Model.iter()
    for expect in keys:
        mod = next(it)
        print 'pos', mod.key
        assert mod.key == expect
        mod.delete()
'''


with store.begin(write=True):
    coll = Model.collection()
    it = coll.keys()
    for expect in keys:
        print expect
        assert expect == next(it)

