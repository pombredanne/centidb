
import unittest

import acid
import acid.meta


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


class TestSave(TestBase, unittest.TestCase):
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
        print Model.by_name.info
        pprint(self.store.engine.items)
        with self.store.begin():
            print len(list(Model.by_name.keys()))
            assert len(list(Model.by_name.keys(u'Dave')))


if __name__ == '__main__':
    unittest.main()
