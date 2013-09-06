
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

    def get_ancestry(self):
        """Return the comment's ancestry."""
        parents = []
        comment = self
        while comment.parent_id:
            parents.append(comment.parent_id)
            comment = self.by_id.get(comment.parent_id)
            if comment:
                assert comment.id != comment.parent_id
                assert comment.parent_id != parents[-1]
            if not comment:
                return
        parents.reverse()
        return parents

    @acid.meta.key
    def key(self):
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
