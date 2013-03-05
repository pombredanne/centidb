
from __future__ import absolute_import

import bisect
from itertools import imap
from operator import itemgetter

import centidb


class ArrayEngine(object):
    """Storage engine that backs onto a sorted Python array of `(key, value)`
    tuples. Lookup is logarithmic while insertion is worst case linear.

    Primarily useful for unit testing. The constructor receives no arguments.
    """
    txn_id = None

    def __init__(self):
        #: The sorted list of `(key, value)` tuples.
        self.pairs = []
        #: Total size in bytes of all stored pairs, i.e.
        #: ``sum(len(k)+len(v) for k, v in pairs).
        self.size = 0

    def get(self, k):
        idx = bisect.bisect_left(self.pairs, (k,))
        if idx < len(self.pairs):
            return self.pairs[idx][1]

    def put(self, k, v):
        idx = bisect.bisect_left(self.pairs, (k,))
        if idx < len(self.pairs) and self.pairs[idx][0] == k:
            self.size += len(v) - len(self.pairs[idx][1])
            self.pairs[idx] = (k, v)
        else:
            self.pairs.insert(idx, (k, v))
            self.size += len(k) + len(v)

    def delete(self, k):
        idx = bisect.bisect_left(self.pairs, (k,))
        if idx < len(self.pairs) and self.pairs[idx][0] == k:
            self.size -= len(k) + len(self.pairs[idx][1])
            self.pairs.pop(idx)

    def iter(self, k, keys=True, values=True, reverse=False):
        idx = bisect.bisect_left(self.pairs, (k,))
        if reverse:
            idx -= len(self.pairs) == idx
            xr = xrange(idx, -1, -1)
        else:
            xr = xrange(idx, len(self.pairs))
        it = imap(self.pairs.__getitem__, xr)
        return it if keys and values else imap(itemgetter(+(not keys)), it)


class PlyvelEngine(object):
    """Storage engine that uses the Google LevelDB library, by way of the
    `Plyvel <http://plyvel.readthedocs.org/>`_ Python module.

        `db`:
            If specified, should be a `plyvel.DB` instance for an already open
            database. Otherwise, the remaining keyword args are passed to the
            `plyvel.DB` constructor.
    """
    txn_id = None

    def __init__(self, db=None, **kwargs):
        self.db = db
        if not self.db:
            import plyvel
            self.db = plyvel.DB(**kwargs)
        self.get = self.db.get
        self.put = self.db.put
        self.delete = self.db.delete

    def iter(self, k, keys=True, values=True, reverse=False):
        kw = dict(include_key=keys, include_value=values, include_stop=True)
        kw.update(dict(stop=k, reverse=True) if reverse else dict(start=k))
        return self.db.iterator(**kw)


def make_thrift_encoder(klass, factory=None):
    """
    Return an `Encoder` instance that serializes Thrift structs using the given
    protocol factory.

    `klass`:
        Thrift-generated struct class the `Encoder` is for.
    `factory`:
        Thrift protocol factory for the desired protocol, defaults to
        `TCompactProtocolFactory`.
    """
    import thrift.protocol.TCompactProtocol
    import thrift.transport.TTransport
    import thrift.TSerialization

    if not factory:
        factory = thrift.protocol.TCompactProtocol.TCompactProtocolFactory()

    def loads(buf):
        transport = thrift.transport.TTransport.TMemoryBuffer(buf)
        proto = factory(transport)
        value = klass()
        value.read(proto)
        return value

    def dumps(value):
        assert isinstance(value, klass)
        return thrift.TSerialization.serialize(value, factory)

    # Form a name from the Thrift ttypes module and struct name.
    name = 'thrift:%s.%s' % (klass.__module__, klass.__name__)
    return centidb.Encoder(name, loads, dumps)
