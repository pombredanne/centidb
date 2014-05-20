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
import zlib

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

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
            record. The `data` argument may be **a buffer**. If your encoder
            does not support the :py:func:`buffer` interface (many C extensions
            do), then first convert it using :py:func:`str`.

        `pack`:
            Function invoked as `func(record)` to serialize a record. The
            function may return :py:func:`str` or any object supporting the
            :py:func:`buffer` interface.

        `new`
            Function that produces a new, empty instance of the encoder's value
            type. Used by :py:mod:`acid.meta` to manufacture empty Model
            instances. The default is :py:class:`dict`.

        `get`
            Functions invoked as `func(obj, attr, default)` to return the value
            of attribute `attr` from `obj` if it is set, otherwise `default`.
            Used by :py:mod:`acid.meta` to implement attribute access. The
            default is :py:func:`operator.getitem`.

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
            Function invoked as `func(data)` to decompress a bytestring. The
            `data` argument may be **a buffer**. If your compressor does not
            support the :py:func:`buffer` interface (many C extensions do),
            then first convert it using :py:func:`str`. The function may return
            :py:func:`str` or any object supporting the :py:func:`buffer`
            interface.

        `pack`:
            Function invoked as `func(data)` to compress a bytestring. The
            `data` argument may be **a buffer**. If your compressor does not
            support the :py:func:`buffer` interface (many C extensions do),
            then first convert it using :py:func:`str`. The function may return
            :py:func:`str` or any object supporting the :py:func:`buffer`
            interface.
    """
    def __init__(self, name, unpack, pack):
        self.name = name
        self.unpack = unpack
        self.pack = pack

    def __repr__(self):
        klass = self.__class__
        return '<%s.%s %r>' % (klass.__module__, klass.__name__, self.name)

def make_json_encoder(separators=',:', **kwargs):
    """Return a :py:class:`RecordEncoder` that serializes
    dict/list/string/float/int/bool/None objects using the :py:mod:`json`
    module. `separators` and `kwargs` are passed to the
    :py:class:`json.JSONEncoder` constructor.

    The `ujson <https://pypi.python.org/pypi/ujson>`_ package will be used for
    decoding if it is available, otherwise :py:func:`json.loads` is used.

    .. warning::

        Strings passed to the encoder **must** be Unicode, since otherwise
        :py:class:`json.JSONEncoder` will silently convert them, causing
        their original and deserialized representations to mismatch, which
        causes index entries to be inconsistent between create/update and
        delete.

        For this reason, the :py:class:`json.JSONEncoder`
        `encoding='undefined'` option is forcibly enabled, causing exceptions
        to be raised when attempting to serialize a bytestring. You must
        explicitly `.decode()` all bytestrings.
    """
    try:
        import json
    except ImportError:
        import simplejson as json

    kwargs['encoding'] = 'undefined'
    encoder = json.JSONEncoder(separators=separators, **kwargs)
    try:
        import ujson
        decoder = ujson.loads
    except ImportError:
        decoder = json.JSONDecoder().decode

    decode = lambda key, data: decoder(bytes(data))
    return RecordEncoder('json', decode, encoder.encode)


def make_msgpack_encoder():
    """Return a :py:class:`RecordEncoder` that serializes
    dict/list/string/float/int/bool/None objects using `MessagePack
    <http://msgpack.org/>`_ via the `msgpack-python
    <https://pypi.python.org/pypi/msgpack-python/>`_ package."""
    import msgpack
    unpack = lambda key, data: msgpack.loads(data)
    return RecordEncoder('msgpack', unpack, msgpack.dumps)


def make_thrift_encoder(klass, factory=None):
    """
    Return a :py:class:`RecordEncoder` instance that serializes `Apache Thrift
    <http://thrift.apache.org/>`_ structs using a compact binary
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

    def loads(key, data):
        transport = thrift.transport.TTransport.TMemoryBuffer(data)
        proto = factory.getProtocol(transport)
        value = klass()
        value.read(proto)
        return value

    def dumps(value):
        assert isinstance(value, klass)
        return thrift.TSerialization.serialize(value, factory)

    # Form a name from the Thrift ttypes module and struct name.
    name = 'thrift:%s.%s' % (klass.__module__, klass.__name__)
    return RecordEncoder(name, loads, dumps, new=klass,
                         get=getattr, set=setattr, delete=delattr)


def make_pickle_encoder(protocol=2):
    """Return a :py:class:`RecordEncoder` that serializes objects using the
    :py:mod:`cPickle` module. `protocol` specifies the protocol version to use.
    """
    import cPickle

    def _pickle_unpack(key, value):
        """cPickle.loads() can't read from a buffer directly, however for
        strings it internally constructs a cStringIO.StringI() and invokes
        load(), which has a special case for StringI instances, which do accept
        buffers. So we avoid string copy by constructing the StringI and
        passing it to load instead."""
        return cPickle.load(StringIO.StringIO(value))

    return RecordEncoder('pickle', _pickle_unpack,
                         functools.partial(cPickle.dumps, protocol=protocol))


#: Encode Python tuples using keylib.packs()/keylib.unpacks().
KEY = RecordEncoder('key', lambda key, value: acid.keylib.unpack(value),
                    acid.keylib.packs)

#: Encode objects using json.dumps()/json.loads().
JSON = make_json_encoder()

#: Perform no compression at all.
PLAIN = Compressor('plain', str, lambda o: o)

#: Compress bytestrings using zlib.compress()/zlib.decompress().
ZLIB = Compressor('zlib', zlib.decompress, zlib.compress)

# The order of this tuple is significant. See core.Store source/data format
# documentation for more information.
_ENCODERS = (KEY, JSON, PLAIN, ZLIB)
