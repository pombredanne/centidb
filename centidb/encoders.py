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

import centidb

__all__ = ['Encoder', 'make_json_encoder', 'make_msgpack_encoder',
           'make_thrift_encoder']


class Encoder(object):
    """Instances of this class represent an encoding.

        `name`:
            ASCII string uniquely identifying the encoding. A future version
            may use this to verify the encoding matches what was used to create
            the :py:class:`Collection`. For encodings used as compressors, this
            name is persisted forever in :py:class:`Store`'s metadata after
            first use.

        `unpack`:
            Function to deserialize an encoded value. It may be called with **a
            buffer object containing the encoded bytestring** as its argument,
            and should return the decoded value. If your encoder does not
            support :py:func:`buffer` objects (many C extensions do), then
            convert the buffer using :py:func:`str`.

        `pack`:
            Function to serialize a value. It is called with the value as its
            sole argument, and should return the encoded bytestring.
    """
    def __init__(self, name, unpack, pack):
        self.name = name
        self.unpack = unpack
        self.pack = pack


def make_json_encoder(separators=',:', **kwargs):
    """Return an :py:class:`Encoder <centidb.Encoder>` that serializes
    dict/list/string/float/int/bool/None objects using the :py:mod:`json`
    module. `separators` and `kwargs` are passed to the JSONEncoder
    constructor."""
    import json
    encoder = json.JSONEncoder(separators=separators, **kwargs)
    decoder = json.JSONDecoder().decode
    decode = lambda s: decoder(str(s))
    return centidb.Encoder('json', decode, encoder.encode)


def make_msgpack_encoder():
    """Return an :py:class:`Encoder <centidb.Encoder>` that serializes
    dict/list/string/float/int/bool/None objects using `MessagePack
    <http://msgpack.org/>`_ via the `msgpack-python
    <https://pypi.python.org/pypi/msgpack-python/>`_ package."""
    import msgpack
    return centidb.Encoder('msgpack', msgpack.loads, msgpack.dumps)


def make_thrift_encoder(klass, factory=None):
    """
    Return an :py:class:`Encoder <centidb.Encoder>` instance that serializes
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
    return centidb.Encoder(name, loads, dumps)
