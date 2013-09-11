
import acid.meta


def init_store():
    global store
    store = acid.open('LmdbEngine',
        path='/media/scratch/t3.lmdb',
         map_size=1048576*1024*10,
         map_async=True,
         sync=False,
         metasync=False,
         writemap=True)
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

    def get_ancestry(self):
        """Return the comment's ancestry as a list of comment IDs, from least
        direct to most direct. Returns [1, 2] given a tree like:

            Comment(id=1, parent_id=None)
            Comment(id=2, parent_id=1)
            Comment(id=3, parent_id=2)

        Returns ``None`` if the complete ancestry can't be reconstructed (i.e.
        missing data).
        """
        ids = []
        node = self
        while node.parent_id:
            ids.append(node.parent_id)
            node = Comment.by_id.get(ids[-1])
            if node:
                assert node.id == ids[-1], 'got %s want %s' % (node.id, ids[-1])
            else:
                return
            assert node.parent_id not in ids
        ids.reverse()
        return ids

    @acid.meta.key
    def key(self):
        """Return the comment's key, which is a tuple like:

            (link_id, oldestParent, olderParent, ..., parent, comment_id)

        This causes the collection to be clustered by link_id, then recursively
        on each level of the comment tree. Prefix queries on the link_id, or
        any comment key, will return the comment itself and all its children.

        ::

            # Fetch all comments for link_id 123.
            Comment.iter(prefix=(123,))

            # Fetch comment and all its children.
            Comment.iter(prefix=(123, 412, 1521, 12))
        """
        ancestry = self.get_ancestry()
        assert ancestry is not None
        ancestry.insert(0, self.subreddit_id)
        ancestry.append(self.id)
        return tuple(ancestry)

    @acid.meta.index
    def by_id(self):
        return self.id

    @acid.meta.index
    def by_author_created(self):
        return self.author, self.created

    @acid.meta.index
    def by_subreddit_created(self):
        return self.subreddit_id, self.created
