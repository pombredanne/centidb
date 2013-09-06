
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


def get_path_list():
    paths = []
    for dirpath, dirnames, filenames in os.walk('/home/dmw/out'):
        for filename in filenames:
            if filename.endswith('.gz') or filename.endswith('.json'):
                paths.append(os.path.join(dirpath, filename))

    def sort_key(path):
        filename = os.path.basename(path)
        return -int(filename.split('.', 1)[0])
    paths.sort(key=sort_key)
    return paths


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

    comment_id = db36(dct['id'])
    comment = models.Comment(id=comment_id,
                             subreddit_id=subreddit_id,
                             author=dct['author'],
                             body=dct['body'],
                             created=created,
                             parent_id=db36(dct['parent_id']),
                             ups=dct['ups'],
                             downs=dct['downs'])
    comment.save()


def process_set(stats, all_paths):
    while all_paths:
        path = all_paths.pop()
        digit = int(os.path.basename(path).split('.')[0])
        if models.Digits.get(digit) is not None:
            continue

        print 'Loading', path
        if path.endswith('.gz'):
            js = json.loads(gzip.open(path).read())
        else:
            js = json.loads(file(path).read())

        for thing in js['data']['children']:
            process_one(stats, thing['data'])

        models.Digits(digits=digit).save()
        stats['files'] += 1
        if not (stats['files'] % 20):
            break

    statinfo = ', '.join('%s=%s' % k for k in sorted(stats.items()))
    print('Commit ' + statinfo)


def main():
    store = models.init_store()
    all_paths = get_path_list()

    stats = {
        'all_comments': 0,
        'comments': 0,
        'links': 0,
        'reddits': 0,
        'users': 0,
        'files': 0
    }
    while all_paths:
        store.in_txn(lambda: process_set(stats, all_paths), write=True)

if __name__ == '__main__':
    main()
