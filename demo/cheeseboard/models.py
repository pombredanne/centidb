
import acid.meta


def init_store():
    Model.bind_store(acid.open('LmdbEngine',
        path='/media/scratch/i3.lmdb',
         map_size=1048576*1024*10,
         map_async=True,
         sync=False,
         metasync=False,
         writemap=True))


class Model(acid.meta.Model):
    """Base class for all cheeseboard models.
    """


class User(Model):
    """A user account."""
    META_COLLECTION_NAME = 'users'

    username = acid.meta.String()
    first_seen = acid.meta.Time()
    last_seen = acid.meta.Time()
    comments = acid.meta.Integer()

    @acid.meta.index
    def by_comments(self):
        return self.comments


class Reddit(Model):
    """A subreddit."""
    META_COLLECTION_NAME = 'reddits'

    id = acid.meta.Integer()
    name = acid.meta.String()
    first_seen = acid.meta.Time()
    last_seen = acid.meta.Time()
    links = acid.meta.Integer()
    comments = acid.meta.Integer()

    @acid.meta.key
    def key(self):
        return self.id

    @acid.meta.index
    def by_links(self):
        return self.links

    @acid.meta.index
    def by_comments(self):
        return self.comments


class Link(Model):
    """A link posted to a subreddit."""
    META_COLLECTION_NAME = 'links'

    id = acid.meta.Integer()
    subreddit_id = acid.meta.Integer()
    title = acid.meta.String()
    first_seen = acid.meta.Time()
    last_seen = acid.meta.Time()
    comments = acid.meta.Integer()

    @acid.meta.key
    def key(self):
        return self.id

    @acid.meta.index
    def by_comments(self):
        return self.comments


class Comment(Model):
    """A comment posted to a link on a subreddit by a user."""
    META_COLLECTION_NAME = 'comments'

    id = acid.meta.Integer()
    subreddit_id = acid.meta.Integer()
    link_id = acid.meta.Integer()
    author = acid.meta.String()
    body = acid.meta.String()
    created = acid.meta.Time()
    parent_id = acid.meta.Integer()
    ups = acid.meta.Integer()
    downs = acid.meta.Integer()

    @acid.meta.index
    def by_author_created(self):
        return self.author, self.created

    @acid.meta.index
    def by_subreddit_created(self):
        return self.subreddit_id, self.created
