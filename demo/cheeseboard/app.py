
import datetime
import random
import sys
import time

import bottle
import wheezy.template.engine
import wheezy.template.ext.core
import wheezy.template.loader

import cheeselib

templates = wheezy.template.engine.Engine(
    loader=wheezy.template.loader.FileLoader(['templates']),
    extensions=[wheezy.template.ext.core.CoreExtension()])
templates.global_vars.update({
    'time': lambda t: str(datetime.datetime.fromtimestamp(t))
})

store = cheeselib.open_store()
oldest_key, = next(store['comments'].keys())
newest_key, = next(store['comments'].keys(reverse=True))


def getint(name, default=None):
    try:
        return int(bottle.request.params.get(name))
    except (ValueError, TypeError):
        return default


@bottle.route('/')
def index():
    t0 = time.time()
    if bottle.request.params.get('hi') == 'rand':
        hi = random.randint(oldest_key, newest_key)
    else:
        hi = getint('hi')
    posts = list(store['comments'].items(hi=hi, reverse=True, max=5))
    highest_id = next(store['comments'].keys(reverse=True), None)
    t1 = time.time()

    older = None
    newer = None
    if posts:
        oldest = posts[-1][0][0] - 1
        if oldest > 0:
            older = '?hi=' + str(oldest)
        if posts[0][0] < highest_id:
            newer = '?hi=' + str(posts[0][0][0] + 5)

    # Get reddits.
    srids = set(c['subreddit_id'] for (cid, c) in posts)
    reddits = {rid: store['reddits'].get(rid)['name'] for rid in srids}

    return templates.get_template('index.html').render({
        'posts': posts,
        'reddits': reddits,
        'older': older,
        'newer': newer,
        'msec': int((t1 - t0) * 1000)
    })


@bottle.route('/static/<filename>')
def static(filename):
    return bottle.static_file(filename, root='static')


@bottle.route('/users/<username>')
def user(username):
    posts = list(store['comments']['author'].items(username, max=5,
    reverse=True))
    srids = set(c['subreddit_id'] for (cid, c) in posts)
    reddits = {rid: store['reddits'].get(rid)['name'] for rid in srids}
    older = None
    newer = None
    t1 = t0 = 0
    return templates.get_template('index.html').render({
        'posts': posts,
        'reddits': reddits,
        'older': older,
        'newer': newer,
        'msec': int((t1 - t0) * 1000)
    })


@bottle.post('/newpost')
def newpost():
    post = dict(bottle.request.forms.iteritems())
    post['created'] = time.time()
    store['posts'].put(post)
    return bottle.redirect('.')


if 'debug' in sys.argv:
    bottle.run(host='0.0.0.0', port=8000, debug=True)
else:
    import bjoern
    bjoern.run(bottle.default_app(), '0.0.0.0', 8000)
