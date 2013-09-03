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

from __future__ import absolute_import
import functools
import operator
import cPickle as pickle
import cStringIO as StringIO
import zlib

import acid
import acid.keylib

__all__ = ['RecordEncoder', 'make_json_encoder', 'make_msgpack_encoder',
           'make_thrift_encoder']


class RecordEncoder(object):
    """Instances of this class represent a record encoding, and provides
    accessors for the record's value type. You must instantiate this to support
    new encoders.

        `name`:
            ASCII string uniquely identifying the encoding. A future version
            may use this to verify the encoding matches what was used to create
            the :py:class:`acid.Collection`.

        `unpack`:
            Function invoked as `func(key, data)` to deserialize an encoded
            record. The `data` argument may be **a buffer object**. If your
            encoder does not support :py:func:`buffer` objects (many C
            extensions do), then first convert the buffer using :py:func:`str`.

        `pack`:
            Function invoked as `func(record)` to serialize a record. It may
            return :py:func:`str` or any object supporting the
            :py:func:`buffer` interface.

        `new`
            Function that produces a new, empty instance of the encoder's value
            type. Used by :py:mod:`acid.meta` to manufacture empty Model
            instances. The default is :py:class:`dict`.

        `get`
            Functions invoked as `func(obj, attr)` to return the value of
            attribute `attr` from `obj`. Used by :py:mod:`acid.meta` to
            implement attribute access. The default is
            :py:func:`operator.getitem`.

        `set`
            Function invoked as `func(obj, attr, value)` to set the attribute
            `attr` on `obj` to `value`. Used by :py:mod:`acid.meta` to
            implement attribute access. The default is
            :py:func:`operator.setitem`.

        `delete`
            Function invoked as `func(obj, attr)` to delete the attribute
            `attr` from `obj`. Used by :py:mod:`acid.meta` to implement
            attribute access. The default is :py:func:`operator.delitem`.
    """
    def __init__(self, name, unpack, pack, new=None,
                 get=None, set=None, delete=None):
        self.name = name
        self.unpack = unpack
        self.pack = pack
        self.new = new or dict
        self.get = get or dict.get
        self.set = set or operator.setitem
        self.delete = delete or operator.delitem


class Compressor(object):
    """Represents a compression method. You must instantiate this class and
    pass it to :py:meth:`acid.Store.add_encoder` to register a new compressor.

        `name`:
            ASCII string uniquely identifying the compressor. A future version
            may use this to verify the encoding matches what was used to create
            the :py:class:`Collection`. For encodings used as compressors, this
            name is persisted forever in :py:class:`Store`'s metadata after
            first use.

        `unpack`:
            Function to decompress a bytestring. It may be called with **a
            buffer object containing the encoded bytestring** as its argument,
            and should return the decoded value. If your compressor does not
            support :py:func:`buffer` objects (many C extensions do), then
            convert the buffer using :py:func:`str`.

        `pack`:
            Function to compress a bytestring. It is called with the value as
            its sole argument, and should return the encoded bytestring.
    """
    def __init__(self, name, unpack, pack):
        self.name = name
        self.unpack = unpack
        self.pack = pack


def make_json_encoder(separators=',:', **kwargs):
    """Return an :py:class:`Encoder <acid.Encoder>` that serializes
    dict/list/string/float/int/bool/None objects using the :py:mod:`json`
    module. `separators` and `kwargs` are passed to the JSONEncoder
    constructor."""
    import json
    encoder = json.JSONEncoder(separators=separators, **kwargs)
    decoder = json.JSONDecoder().decode
    decode = lambda s: decoder(str(s))
    return RecordEncoder('json', decode, encoder.encode)


def make_msgpack_encoder():
    """Return an :py:class:`Encoder <acid.Encoder>` that serializes
    dict/list/string/float/int/bool/None objects using `MessagePack
    <http://msgpack.org/>`_ via the `msgpack-python
    <https://pypi.python.org/pypi/msgpack-python/>`_ package."""
    import msgpack
    return RecordEncoder('msgpack', msgpack.loads, msgpack.dumps)


def make_thrift_encoder(klass, factory=None):
    """
    Return an :py:class:`Encoder <acid.Encoder>` instance that serializes
    `Apache Thrift <http://thrift.apache.org/>`_ structs using a compact binary
    representation.

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
        proto = factory.getProtocol(transport)
        value = klass()
        value.read(proto)
        return value

    def dumps(value):
        assert isinstance(value, klass)
        return thrift.TSerialization.serialize(value, factory)

    # Form a name from the Thrift ttypes module and struct name.
    name = 'thrift:%s.%s' % (klass.__module__, klass.__name__)
    return RecordEncoder(name, loads, dumps, factory=klass,
                         get=getattr, set=setattr, delete=delattr)


#: Encode Python tuples using keylib.packs()/keylib.unpacks().
KEY = RecordEncoder('key', lambda key, value: acid.keylib.unpack('', value),
                    functools.partial(acid.keylib.packs, ''))

def _pickle_unpack(key, value):
    """cPickle.loads() can't reading from a buffer directly, however for
    strings it internally constructs a cStringIO.StringI() and invokes load(),
    which has a special case for StringI instances, which do accept buffers. So
    we avoid string copy by constructing the StringI and passing it to load
    instead."""
    return pickle.load(StringIO.StringIO(value))

#: Encode Python objects using the cPickle version 2 protocol.
PICKLE = RecordEncoder('pickle', _pickle_unpack,
                       functools.partial(pickle.dumps, protocol=2))

#: Perform no compression at all.
PLAIN = Compressor('plain', str, lambda o: o)

#: Compress bytestrings using zlib.compress()/zlib.decompress().
ZLIB = Compressor('zlib', zlib.decompress, zlib.compress)

# The order of this tuple is significant. See core.Store source/data format
# documentation for more information.
_ENCODERS = (KEY, PICKLE, PLAIN, ZLIB)
