
import time
import acid.meta


class Model(acid.meta.Model):
    """Base class for all cheeseboard models.
    """


class Post(Model):
    name = acid.meta.String()
    text = acid.meta.String()
    created = acid.meta.Time()

    @acid.meta.constraint
    def check_name(self):
        return self.name and len(self.name) >= 3

    @acid.meta.constraint
    def check_text(self):
        return self.text and len(self.text) > 5

    @acid.meta.on_create
    def set_created(self):
        self.created = time.time()


def init_store():
    global store
    store = acid.open('LmdbEngine',
         path='store.lmdb',
         map_size=(1048576*1024*2) - 1,
         map_async=True,
         sync=False,
         metasync=False,
         writemap=True)
    Model.bind_store(store)
    return store
