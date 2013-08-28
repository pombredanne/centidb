
import time

import centidb
import flask

import wheezy.template.engine
import wheezy.template.ext.core
import wheezy.template.loader

templates = wheezy.template.engine.Engine(
    loader=wheezy.template.loader.FileLoader(['templates']),
    extensions=[
        wheezy.template.ext.core.CoreExtension()
])

store = centidb.open('LmdbEngine', path='store.lmdb', map_size=512e6)
store.add_collection('posts')

app = flask.Flask(__name__)


def getint(name, default=None):
    try:
        return int(flask.request.args.get(name))
    except (ValueError, TypeError):
        return default


@app.route('/')
def index():
    t0 = time.time()
    lo = getint('lo')
    hi = getint('hi')
    if lo:
        posts = list(store['posts'].items(max=5, lo=lo))[::-1]
    else:
        posts = list(store['posts'].items(hi=hi, reverse=True, max=5))
    t1 = time.time()

    newer = older = None
    if posts:
        lo_id, = posts[-1][0]
        if lo_id != 1:
            older = '?hi=' + str(lo_id)
        if hi:
            newer = '?lo=' + str(hi)

    return templates.get_template('index.html').render({
        'posts': posts,
        'older': older,
        'newer': newer,
        'msec': int((t1 - t0) * 1000)
    })


@app.route('/newpost', methods=['POST'])
def newpost():
    post = dict(flask.request.form.iteritems())
    post['created'] = time.time()
    rec = store['posts'].put(post)
    return flask.redirect('.')

import sys

if 'debug' in sys.argv:
    app.run(host='0.0.0.0', port=8000, debug=True)
else:
    import bjoern
    bjoern.run(app, '0.0.0.0', 8000)
