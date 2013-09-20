
import pdb

import bz2
import glob
import gzip
import json
import os
import sys

from operator import itemgetter
from pprint import pprint

import models


def db36(s):
    """Convert a Redis base36 ID to an integer, stripping any prefix present
    beforehand."""
    if s[:3] in 't1_t2_t3_t4_t5_':
        s = s[3:]
    return int(s, 36)


def process_one(stats, dct):
    stats['all_comments'] += 1
    created = int(dct['created_utc'])

    comment_id = db36(dct['id'])
    subreddit_id = db36(dct['subreddit_id'])
    link_id = db36(dct['link_id'])

    comment = models.Comment.get(comment_id)
    if not comment:
        stats['comments'] += 1
        user = models.User.get(dct['author'])
        if not user:
            stats['users'] += 1
            user = models.User(username=dct['author'],
                               first_seen=created,
                               last_seen=created,
                               comments=0)
        user.comments += 1
        user.last_seen = max(user.last_seen, created)

        reddit = models.Reddit.get(subreddit_id)
        if not reddit:

            stats['reddits'] += 1
            reddit = models.Reddit(name=dct['subreddit'],
                                   id=subreddit_id,
                                   first_seen=created,
                                   last_seen=created,
                                   links=0,
                                   comments=0)
        reddit.last_seen = created
        reddit.comments += 1

        link = models.Link.get(link_id)
        if not link:
            stats['links'] += 1
            link = models.Link(id=link_id,
                               subreddit_id=subreddit_id,
                               title=dct['link_title'],
                               first_seen=created,
                               last_seen=created,
                               comments=0)
            reddit.links += 1

        link.last_seen = created
        link.comments += 1

        reddit.save()
        user.save()
        link.save()

    if dct['parent_id'].startswith('t3_'):
        parent_id = None
    else:
        parent_id = db36(dct['parent_id'])

    comment = models.Comment(id=comment_id,
                             subreddit_id=subreddit_id,
                             author=dct['author'],
                             body=dct['body'],
                             created=created,
                             link_id=link_id,
                             parent_id=parent_id,
                             ups=dct['ups'],
                             downs=dct['downs'])
    if parent_id and not comment.get_parent():
        stats['orphans'] += 1
        stats['comments'] -= 1
    else:
        comment.save()


def process_set(stats, fp):
    for idx, line in enumerate(iter(fp.readline, None), 1):
        if line == '':
            return False
        process_one(stats, json.loads(line))
        if not (idx % 500):
            break

    if 1 or not (stats['comments'] % 500):
        statinfo = ', '.join('%s=%s' % k for k in sorted(stats.items()))
        print('Commit ' + statinfo)
    return True


fp = None

def main():
    store = models.init_store()
    #fp = bz2.BZ2File('/home/data/dedupped-1-comment-per-line.json.bz2', 'r', 1048576 * 10)
    fp = bz2.BZ2File('/home/data/top4e4.json.bz2', 'r', 1048576 * 10)

    stats = {
        'all_comments': 0,
        'comments': 0,
        'io_error': 0,
        'links': 0,
        'reddits': 0,
        'users': 0,
        'files': 0,
        'orphans': 0,
    }
    more = True
    while more:
        more = store.in_txn(lambda: process_set(stats, fp), write=True)

if __name__ == '__main__':
    main()
