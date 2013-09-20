
import acid.meta


def init_store():
    global store
    store = acid.open('LmdbEngine',
        trace_path='trace.out',
        path='/media/scratch/t4.lmdb',
        map_size=1048576*1024*3,
        map_async=True,
        sync=False,
        metasync=False,
        writemap=False)
    Model.bind_store(store)
    return store


class Model(acid.meta.Model):
    """Base class for all cheeseboard models.
    """


class Digits(Model):
    """Tracks import state."""
    digits = acid.meta.Integer()

    @acid.meta.key
    def key(self):
        return self.digits


class User(Model):
    """A user account."""
    username = acid.meta.String()
    first_seen = acid.meta.Time()
    last_seen = acid.meta.Time()
    comments = acid.meta.Integer()

    @acid.meta.key
    def key(self):
        return self.username

    @acid.meta.index
    def by_comments(self):
        return self.comments


class Reddit(Model):
    """A subreddit."""
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
    def by_id(self):
        return self.id

    @acid.meta.index
    def by_links(self):
        return self.links

    @acid.meta.index
    def by_comments(self):
        return self.comments


class Link(Model):
    """A link posted to a subreddit."""
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
    id = acid.meta.Integer()
    subreddit_id = acid.meta.Integer()
    link_id = acid.meta.Integer()
    author = acid.meta.String()
    body = acid.meta.String()
    created = acid.meta.Time()
    parent_id = acid.meta.Integer()
    ups = acid.meta.Integer()
    downs = acid.meta.Integer()

    def __repr__(self):
        return '<Comment k=%s id=%d parent=%s>' %\
                (self.key, self.id, self.parent_id)

    def get_parent(self):
        assert self.parent_id
        return self.by_id.get(self.parent_id)

    @acid.meta.key
    def key(self):
        """Return the comment's key, which is a tuple like:

            (link_id, idx1, idx2, ..., idxN)

        `idxN` at each level begins at 1, with the next higher integer assigned
        for that level during insert. This causes the collection to be
        clustered by link_id, then recursively on each level of the comment
        tree. Prefix queries on the link_id, or any comment key, will
        return the comment itself and all its children.

        ::

            # Fetch all comments for link_id 123.
            Comment.iter(prefix=(123,))

            # Fetch link_id 123's first comment and all its child comments.
            Comment.iter(prefix=(123, 1))
        """
        if self.parent_id:
            parent = Comment.by_id.get(self.parent_id)
            assert parent is not None
            eldest = Comment.find(prefix=parent.key, reverse=True)
            if eldest and len(eldest.key) == len(parent.key):
                return parent.key + (1,)
            else:
                t = tuple(parent.key)
                return t[:-1] + (t[-1] + 1,)
        else:
            eldest = Comment.find(prefix=self.link_id, reverse=True)
            if eldest:
                return (self.link_id, eldest.key[1] + 1)
            else:
                return (self.link_id, 1)

    @acid.meta.index
    def by_id(self):
        return self.id

    @acid.meta.index
    def by_author_created(self):
        return self.author, self.created

    @acid.meta.index
    def by_subreddit_created(self):
        return self.subreddit_id, self.created
