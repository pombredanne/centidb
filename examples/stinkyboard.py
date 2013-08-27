
import time

import centidb
import flask

store = centidb.open('LmdbEngine', path='store.lmdb', map_size=512e6)
store.add_collection('posts')

app = flask.Flask(__name__, template_folder='templates')


def getint(name, default=None):
    try:
        return int(flask.request.args.get(name))
    except (ValueError, TypeError):
        return default


@app.route('/')
def index():
    lo = getint('lo')
    hi = getint('hi')
    if lo:
        posts = reversed(store['posts'].items(max=5, lo=lo))
    else:
        posts = list(store['posts'].items(hi=hi, reverse=True, max=5))
    if posts:
        lo_id, = posts[0][0]
        hi_id, = posts[-1][0]
        
    return flask.render_template('index.html', posts=posts)


@app.route('/newpost', methods=['POST'])
def newpost():
    post = dict(flask.request.form.iteritems())
    post['created'] = time.time()
    rec = store['posts'].put(post)
    return flask.redirect('.')


app.run(host='0.0.0.0', port=8000, debug=True)
