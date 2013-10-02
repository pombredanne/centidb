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

import os
import json
import urllib2
import time


base = 'http://www.reddit.com/r/all/comments.json'

def nexti():
    i = 0
    for path in os.listdir('out'):
        if path[0] == '.':
            continue
        try:
            b, s = path.split('.', 1)
        except ValueError:
            continue
        if b.isdigit():
            i = max(i, int(b))
    return 1+i


while True:
    idx = nexti()
    path = 'out/%d.json' % (idx,)
    url = base + '?limit=200&t=' + str(time.time())
    t0 = time.time()
    req = urllib2.Request(url, headers={
        'User-agent': 'Contact-dw-at-botanicus.net-user-w2m3d'
    })
    js = urllib2.urlopen(req).read()
    file(path, 'w').write(js)
    print url
    try:
        parsed = json.loads(js)
        print idx, 'got', len(parsed['data']['children'])
    except:
        print js
        raise
    t1 = time.time()
    remain = 3 - (t1 - t0)
    if remain > 0:
        time.sleep(remain)

