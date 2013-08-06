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
"""
Minimal declarative ODM for centidb.
"""

from __future__ import absolute_import

import centidb
import centidb.support


class Field(object):
    """Base class for all field types.
    """
    def __get__(self, instance, klass):
        if instance:
            return instance._rec.data.get(self.name)
        return self

    def __set__(self, instance, value):
        instance._rec.data[self.name] = value

    def __delete__(self, instance):
        try:
            del instance._rec.data[self.name]
        except KeyError:
            raise AttributeError(self.name)


class Bool(Field):
    """A boolean field.
    """


class Double(Field):
    """A double field.
    """


class Integer(Field):
    """An integer field.
    """


class String(Field):
    """A string field.
    """


class LazyIndexProperty(object):
    """Property that replaces itself with a centidb.Index when it is first
    accessed."""
    def __init__(self, name):
        self.name = name

    def __get__(self, instance, klass):
        index = klass.collection().indices[self.name]
        setattr(klass, self.name, index)
        return index


class ModelMeta(type):
    def __new__(cls, name, bases, attrs):
        klass = type.__new__(cls, name, bases, attrs)
        cls.setup_type_vars(klass, bases, attrs)
        cls.setup_key_func(klass, bases, attrs)
        cls.setup_index_funcs(klass, bases, attrs)
        cls.setup_index_properties(klass, bases, attrs)
        cls.setup_field_properties(klass, bases, attrs)
        cls.setup_constraints(klass, bases, attrs)
        cls.setup_encoding(klass, bases, attrs)
        return klass

    @classmethod
    def setup_type_vars(cls, klass, bases, attrs):
        if 'METADB_COLLECTION_NAME' not in attrs:
            klass.METADB_COLLECTION_NAME = klass.__name__
        if 'METADB_KIND_NAME' not in attrs:
            qname = '%s.%s' % (klass.__module__, klass.__name__)
            klass.METADB_KIND_NAME = qname
        klass.METADB_COLLECTION = None

    @classmethod
    def setup_key_func(cls, klass, bases, attrs):
        key_func = None
        for key, value in attrs.iteritems():
            if not hasattr(value, 'metadb_derived_key'):
                continue
            if key_func:
                raise TypeError('%r: multiple key functions found: %r and %r'
                                % (klass, key_func, value))
            key_func = value
        if key_func:
            klass.METADB_KEY_FUNC = key_func

    @classmethod
    def setup_index_funcs(cls, klass, bases, attrs):
        base_index_funcs = getattr(klass, 'METADB_INDEX_FUNCS', [])
        index_funcs = []
        if attrs.get('METADB_INHERIT_INDEX_FUNCS', True):
            index_funcs.extend(base_index_funcs)
        for key, value in attrs.iteritems():
            if not hasattr(value, 'metadb_index_func'):
                continue
            if any(f.func_name == value.func_name for f in base_index_funcs):
                raise TypeError('index %r already defined by a base class'
                                % (value.func_name,))
            index_funcs.append(value)
        klass.METADB_INDEX_FUNCS = index_funcs

    @classmethod
    def setup_index_properties(cls, klass, bases, attrs):
        for index_func in klass.METADB_INDEX_FUNCS:
            setattr(klass, index_func.func_name,
                    LazyIndexProperty(index_func.func_name))

    @classmethod
    def setup_constraints(cls, klass, bases, attrs):
        base_constraints = getattr(klass, 'METADB_CONSTRAINTS', [])
        constraints = []
        if attrs.get('METADB_INHERIT_CONSTRAINTS', True):
            constraints.extend(base_constraints)
        for key, value in attrs.iteritems():
            if getattr(value, 'metadb_constraint', False):
                constraints.append(value)
        klass.METADB_CONSTRAINTS = constraints

    @classmethod
    def setup_field_properties(cls, klass, bases, attrs):
        for key, value in attrs.iteritems():
            if isinstance(value, Field):
                value.name = key

    @classmethod
    def setup_encoding(cls, klass, bases, attrs):
        cls.METADB_ENCODING = centidb.support.make_json_encoder()


def key(func):
    """Mark a function as the model's primary key function. If the function
    returns a stable result given the same input model, then
    :py:func:`derived_key` should be used instead.

    ::

        @metadb.key
        def key_func(self):
            return int(time.time() * 1000)
    """
    func.metadb_derived_key = False
    return func


def derived_key(func):
    """Mark a function as the model's primary key function. If the function
    does not return a stable result given the same input model, then use
    :py:func:`key` instead.

    ::

        @metadb.derived_key
        def key_func(self):
            return self.name, self.email
    """
    func.metadb_derived_key = True
    return func


def blind(func):
    """Mark a key function as being compatible with blind writes. This
    indicates the function never generates a duplicate result, therefore the
    database does not need to check for existing keys during save.

    ::

        @metadb.blind
        @metadb.key
        def never_repeating_key_func(self):
            return int(time.time() * 10000000000)
    """
    func.metadb_blind_keys = True
    return func


def index(func):
    """Mark a function as being an index function for the model. The function
    will be called during save to produce secondary indices for each item.

    Once the model class has been constructed, accessing ``Model.func_name``
    will return a :py:class:`centidb.Index` instance representing the index.
    The original function can still be accessed via
    :py:attr:`centidb.Index.func`.

    ::

        class Person(metadb.Model):
            age = metadb.Integer()

            @metadb.index
            def age_index(self):
                return self.age

        # ...
        # Count all people older than 22.
        print('Total people older than 22:', Person.age_index.count(lo=22))

        # Fetch youngest and oldest people.
        youngest = Person.age_index.get()
        oldest = Person.age_index.get(reverse=True)

    """
    func.metadb_index_func = True
    return func


def constraint(func):
    """Mark a function as implementing a collection constraint. Constraints are
    checked during :py:meth:`centidb.metadb.Model.save`.

    ::

        @metadb.constraint
        def age_sane(self):
            return 0 < age < 150

    """
    func.metadb_constraint = True
    return func


class BaseModel(object):
    """Basic model class implementation. This exists separately from
    :py:class:`Model` to allow clean subclassing of the :py:class:`ModelMeta`
    metaclass.
    """
    @classmethod
    def create_collection(cls):
        if not hasattr(cls, 'METADB_STORE'):
            raise TypeError('%s nor any of its bases have been bound to a '
                            'Store. You must call %s.bind_store() with a '
                            'centidb.Store instance.'
                            % (cls.__name__, cls.__name__))

        key_func = getattr(cls, 'METADB_KEY_FUNC', None)
        coll = cls.METADB_STORE.collection(
            cls.METADB_COLLECTION_NAME, key_func=key_func,
            blind=getattr(key_func, 'metadb_blind_keys', False))

        for index_func in cls.METADB_INDEX_FUNCS:
            coll.add_index(index_func.func_name, index_func)
        cls.METADB_COLLECTION = coll

    @classmethod
    def collection(cls):
        """Return the :py:class:`centidb.Collection` used to store instances of
        this model. The collection handles objects understood by the underlying
        encoder, not Model instances.

        :py:meth:`bind_store` must be called before accessing this property.
        """
        coll = cls.METADB_COLLECTION
        if coll:
            return coll
        cls.create_collection()
        return cls.METADB_COLLECTION

    @classmethod
    def bind_store(cls, store):
        """Bind this class and all subclasses to a :py:class:`centidb.Store`.

        ::

            store = centidb.open('ListEngine')
            MyModel.bind_store(store)
        """
        assert isinstance(store, centidb.Store)
        cls.METADB_STORE = store

    @classmethod
    def get(cls, key):
        """Fetch an instance given its key; see
        :py:meth:`centidb.Collection.get`."""
        return cls.collection().get(key)

    @classmethod
    def find(cls, key=None, lo=None, hi=None, reverse=None, include=False):
        """Fetch the first matching instance; see
        :py:meth:`centidb.Collection.find`.
        """
        return cls.collection().find(key, lo, hi, reverse, include)

    @classmethod
    def iter(cls, key=None, lo=None, hi=None, reverse=None, max=None,
             include=False):
        """Yield matching models in key order; see
        :py:meth:`centidb.Store.values`."""
        return cls.collection().values(key, lo, hi, reverse, max, include)

    def __init__(self, _rec=None, **kwargs):
        if _rec is None:
            _rec = centidb.Record(self.collection(), {})
        self._rec = _rec

    @property
    def is_saved(self):
        """``True`` if the model has been saved already.
        """
        return self._rec.key is not None

    def delete(self):
        """Delete the model if it has been saved."""
        if self._rec.key:
            self.collection().delete(self._rec)

    def save(self, check_constraints=True):
        """Create or update the model in the database.

            `check_constraints`:
                If ``False``, then constraint checking is disabled. Useful for
                importing e.g. old data.
        """
        if check_constraints:
            for func in self.METADB_CONSTRAINTS:
                if not func(self):
                    raise ValueError('constraint %r failed for %r'
                                     % (func.func_name, self))
        self.collection().put(self._rec)

    def __repr__(self):
        klass = self.__class__
        qname = '%s.%s' % (klass.__module__, klass.__name__)
        if self.is_saved:
            return '<%s %s>' % (qname, self._rec.key)
        return '<%s unsaved>' % (qname,)


class Model(BaseModel):
    """Inherit from this class to add fields to the basic model.
    """
    __metaclass__ = ModelMeta
