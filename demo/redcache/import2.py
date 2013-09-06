
import glob
import gzip
import json
import os
import sys

from operator import itemgetter
from pprint import pprint

import cheeselib


store = cheeselib.open_store()


def db36(s):
    if s[:3] in 't1_t2_t3_t4_t5_':
        s = s[3:]
    return int(s, 36)


allpaths = glob.glob('/home/dmw/out/*json*')
allpaths.sort(key=lambda s: int(s.split('/')[-1].split('.')[0]))

unique_cmts = 0
proc = 0

done = 0
txn = None

for path in allpaths:
    digit = int(os.path.basename(path).split('.')[0])
    if store['digits'].get(digit) is not None:
        continue

    if not (done % 20):
        if txn:
            txn.commit()
            print 'commit', 'unique', unique_cmts, 'processed', proc
        txn = store.engine.begin(write=True)
    done += 1

    print 'loading', path
    if path.endswith('.gz'):
        js = json.loads(gzip.open(path).read())
    else:
        js = json.loads(file(path).read())

    for dct in js['data']['children']:
        proc += 1
        dct = dct['data']
        created = int(dct['created_utc'])

        comment = store['comments'].get(db36(dct['id']), txn=txn)
        if not comment:
            unique_cmts += 1
            user = store['users'].get(dct['author'], txn=txn)
            user = user or {
                'username': dct['author'],
                'first_seen': created,
                'last_seen': created,
                'comments': 0,
            }
            user['comments'] += 1
            user['last_seen'] = max(user['last_seen'], created)

            reddit = store['reddits'].get(db36(dct['subreddit_id'])) or {
                'name': dct['subreddit'],
                'id': db36(dct['subreddit_id']),
                'first_seen': created,
                'last_seen': created,
                'links': 0,
                'comments': 0,
            }
            reddit['last_seen'] = created
            reddit['comments'] += 1

            link = store['links'].get(db36(dct['link_id']), txn=txn)
            if not link:
                link = {
                    'id': db36(dct['link_id']),
                    'subreddit_id': db36(dct['subreddit_id']),
                    'title': dct['link_title'],
                    'first_seen': created,
                    'last_seen': created,
                    'comments': 0,
                }
                reddit['links'] += 1

            link['last_seen'] = created
            link['comments'] += 1

            store['reddits'].put(reddit, txn=txn)
            store['users'].put(user, txn=txn)
            store['links'].put(link, txn=txn)

        comment = {
            'id': db36(dct['id']),
            'subreddit_id': db36(dct['subreddit_id']),
            'author': dct['author'],
            'body': dct['body'],
            'created': created,
            'parent_id': db36(dct['parent_id']),
            'ups': dct['ups'],
            'downs': dct['downs']
        }
        store['comments'].put(comment, txn=txn)

    store['digits'].put(digit, key=digit, txn=txn)

txn.commit()
