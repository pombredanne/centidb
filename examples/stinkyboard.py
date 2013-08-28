
import sys
import time

import bottle
import centidb
import wheezy.template.engine
import wheezy.template.ext.core
import wheezy.template.loader

templates = wheezy.template.engine.Engine(
    loader=wheezy.template.loader.FileLoader(['templates']),
    extensions=[wheezy.template.ext.core.CoreExtension()])

store = centidb.open('LmdbEngine', path='store.lmdb', map_size=512e6)
store.add_collection('posts')


def getint(name, default=None):
    try:
        return int(bottle.request.params.get(name))
    except (ValueError, TypeError):
        return default


@bottle.route('/')
def index():
    t0 = time.time()
    hi = getint('hi')
    posts = list(store['posts'].items(hi=hi, reverse=True, max=5))
    highest_id = next(store['posts'].keys(reverse=True), None)
    t1 = time.time()

    older = None
    newer = None
    if posts:
        oldest = posts[-1][0][0] - 1
        if oldest > 0:
            older = '?hi=' + str(oldest)
        if posts[0][0] < highest_id:
            newer = '?hi=' + str(posts[0][0][0] + 5)

    return templates.get_template('index.html').render({
        'posts': posts,
        'older': older,
        'newer': newer,
        'msec': int((t1 - t0) * 1000)
    })


@bottle.route('/static/<filename>')
def static(filename):
    return bottle.static_file(filename, root='static')


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
