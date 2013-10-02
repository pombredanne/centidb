#
# Copyright 2013, David Wilson.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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


def get_path_list():
    paths = []
    for dirpath, dirnames, filenames in os.walk('out'):
        for filename in filenames:
            if filename.endswith('.bz2') or filename.endswith('.gz') or filename.endswith('.json'):
                paths.append(os.path.join(dirpath, filename))

    def sort_key(path):
        filename = os.path.basename(path)
        return -int(filename.split('.', 1)[0])
    paths.sort(key=sort_key)
    return paths


fp = os.popen('bzip2 -9c > reformatted.json.bz2', 'w')
seen = set()

def process_one(nah, dct):
    if dct['id'] not in seen:
        fp.write(json.dumps(dct, sort_keys=True, separators=',:') + '\n')
        seen.add(dct['id'])
        nah['comments'] += 1
    else:
        nah['dups'] += 1


def process_set(stats, all_paths):
    while all_paths:
        path = all_paths.pop()
        print 'Loading', path
        try:
            if path.endswith('.gz'):
                js = json.loads(gzip.open(path).read())
            elif path.endswith('.bz2'):
                js = json.loads(bz2.decompress(open(path).read()))
            else:
                js = json.loads(file(path).read())
        except Exception, e:
            print "Can't load %r: %s" % (path, e)
            stats['io_error'] += 1
            continue

        for thing in reversed(js['data']['children']):
            process_one(stats, thing['data'])

        stats['files'] += 1
        if not (stats['files'] % 20):
            break

    statinfo = ', '.join('%s=%s' % k for k in sorted(stats.items()))
    print('Commit ' + statinfo)


def main():
    all_paths = get_path_list()

    stats = {
        'all_comments': 0,
        'comments': 0,
        'io_error': 0,
        'dups': 0,
        'links': 0,
        'reddits': 0,
        'users': 0,
        'files': 0,
        'orphans': 0,
    }
    while all_paths:
        process_set(stats, all_paths)

if __name__ == '__main__':
    main()
