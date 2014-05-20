
import itertools
from time import time as now
import models

BOOL = [False, True]

models.init_store().begin().__enter__()

benches = []

def benchmark(**params):
    def nothing(func):
        benches.append((list(sorted(params.iteritems())), func))
        return func
    return nothing


def run():
    for params, func in benches:
        configs = ([(name, v) for v in values]
                   for name, values in params)
        for config in itertools.product(*configs):
            name = func.__name__
            if config:
                name += '<%s>' % (
                    ', '.join('%s=%s' % (k, v) for (k, v) in config))
            benchit(name, lambda: func(**dict(config)))
        print


def benchit(s, func):
    times = []
    for i in xrange(5):
        t0 = now()
        count = sum(1 for _ in func())
        t1 = now()
        times.append(t1 - t0)

    best = min(times)
    print '%s x %d: %.2fs (%.2f/sec)' % (s, count, best, count/best)



@benchmark(reverse=BOOL)
def index_keys(reverse):
    return models.Link.by_comments.keys(max=500000, reverse=reverse)

@benchmark(reverse=BOOL, raw=BOOL)
def index_scan(reverse, raw):
    return models.Link.by_comments.values(max=100000, reverse=reverse, raw=raw)

@benchmark(reverse=BOOL, raw=BOOL)
def comment_scan(reverse, raw):
    return models.Comment.iter(max=500000, reverse=reverse, raw=raw)

run()
