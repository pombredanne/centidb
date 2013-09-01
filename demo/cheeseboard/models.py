
import acid.meta


class Model(acid.meta.Model):
    """Base class for all cheeseboard models.
    """


class User(Model):
    """A user account."""
    username = acid.meta.String()
    first_seen = acid.meta.Time()
    last_seen = acid.meta.Time()
    comment_count = acid.meta.Integer()

    @acid.meta.index
    def by_comment_count(self):
        return self.comment_count


class Reddit(Model):
    """A subreddit."""
    id = acid.meta.Integer()
    name = acid.meta.String()
    first_seen = acid.meta.Time()
    last_seen = acid.meta.Time()
    link_count = acid.meta.Integer()
    comment_count = acid.meta.Integer()

    @acid.meta.key
    def key(self):
        return self.id

    @acid.meta.index
    def by_link_count(self):
        return self.links

    @acid.meta.index
    def by_comment_count(self):
        return self.comments


class Link(Model):
    """A link posted to a subreddit."""
    id = acid.meta.Integer()
    subreddit_id = acid.meta.Integer()
    title = acid.meta.String()
    first_seen = acid.meta.Time()
    last_seen = acid.meta.Time()
    comment_count = acid.meta.Integer()

    @acid.meta.key
    def key(self):
        return self.id

    @acid.meta.index
    def by_comment_count(self):
        return self.comment_count


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

    @acid.meta.index
    def by_author_created(self):
        return self.author, self.created

    @acid.meta.index
    def by_subreddit_created(self):
        return self.subreddit_id, self.created
