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
The `acid.meta` module provides an ORM-like metaclass that allows
simplified definition of database models using Python code.

This module is a work in progress, and the most interesting aspect of it is
missing. A future version will use the model definition to automatically
produce and maintain a compact encoding. For now this is just a curiosity.
"""

from __future__ import absolute_import
import operator

import acid
import acid.encoders


class Field(object):
    """Base class for all field types.
    """
    def __get__(self, instance, klass):
        if instance:
            return klass.META_BINDING.get(instance._rec, self.name)
        return self

    def __set__(self, instance, value):
        instance.META_BINDING.set(instance._rec, self.name, value)

    def __delete__(self, instance):
        try:
            instance.META_BINDING.delete(instance._rec, self.name)
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
    """Property that replaces itself with a acid.Index when it is first
    accessed."""
    def __init__(self, name):
        self.name = name

    def __get__(self, instance, klass):
        index = klass.collection().indices[self.name]
        setattr(klass, self.name, index)
        return index


class EncoderBinding(object):
    """Wrap implementations for value manipulation on a model's underlying
    encoder. You must instantiate this to support new encoders.

    `encoder`
        The :py:class:`acid.encoders.Encoder` instance itself.

    `new`
        Callable that produces a new, empty instance of the encoder's value
        type.

        The default is :py:class:`dict`.

    `get`
        Callable that when invoked as `func(obj, attr)` returns the value of
        the named attribute `attr` from `obj`.

        The default is :py:func:`operator.getitem`.

    `set`
        Callable that when invoked as `func(obj, attr, value)` sets the value
        of the named attribute `attr` on `obj` to `value`.

        The default is :py:func:`operator.setitem`.

    `delete`
        Callable that when invoked as `func(obj, attr)` deletes the named
        attribute `attr` from `obj`.

        The default is :py:func:`operator.delitem`.
    """
    def __init__(self, encoder, new=None, get=None, set=None, delete=None):
        self.encoder = encoder
        self.new = new or dict
        self.get = get or dict.get
        self.set = set or operator.setitem
        self.delete = delete or operator.delitem


def make_thrift_binding(klass, factory=None):
    """Creates a binding for a Thrift type. Since Thrift exposes user data as
    attributes we must :py:func:`getattr`, :py:func:`setattr` and
    :py:func:`delattr` for value access.
    """
    import acid.encoders
    encoder = encoders.make_thrift_encoder(klass, factory=factory)
    return EncoderBinding(encoder=encoder, factory=klass,
                          get=getattr, set=setattr, delete=delattr)


class ModelMeta(type):
    def __new__(cls, name, bases, attrs):
        klass = type.__new__(cls, name, bases, attrs)
        cls.setup_type_vars(klass, bases, attrs)
        cls.setup_key_func(klass, bases, attrs)
        cls.setup_index_funcs(klass, bases, attrs)
        cls.setup_index_properties(klass, bases, attrs)
        cls.setup_field_properties(klass, bases, attrs)
        cls.setup_constraints(klass, bases, attrs)
        cls.setup_binding(klass, bases, attrs)
        return klass

    @classmethod
    def setup_type_vars(cls, klass, bases, attrs):
        if 'META_COLLECTION_NAME' not in attrs:
            klass.META_COLLECTION_NAME = klass.__name__
        if 'META_KIND_NAME' not in attrs:
            qname = '%s.%s' % (klass.__module__, klass.__name__)
            klass.META_KIND_NAME = qname
        klass.META_COLLECTION = None

    @classmethod
    def setup_key_func(cls, klass, bases, attrs):
        key_func = None
        for key, value in attrs.iteritems():
            if not hasattr(value, 'meta_derived_key'):
                continue
            if key_func:
                raise TypeError('%r: multiple key functions found: %r and %r'
                                % (klass, key_func, value))
            key_func = value
        if key_func:
            klass.META_KEY_FUNC = key_func

    @classmethod
    def setup_index_funcs(cls, klass, bases, attrs):
        index_funcs = list(getattr(klass, 'META_INDEX_FUNCS', []))
        for key, value in attrs.iteritems():
            if not hasattr(value, 'meta_index_func'):
                continue
            if any(f.func_name == value.func_name for f in index_funcs):
                raise TypeError('index %r already defined by a base class'
                                % (value.func_name,))
            index_funcs.append(value)
        klass.META_INDEX_FUNCS = index_funcs

    @classmethod
    def setup_index_properties(cls, klass, bases, attrs):
        for index_func in klass.META_INDEX_FUNCS:
            setattr(klass, index_func.func_name,
                    LazyIndexProperty(index_func.func_name))

    @classmethod
    def setup_constraints(cls, klass, bases, attrs):
        constraints = list(getattr(klass, 'META_CONSTRAINTS', []))
        for key, value in attrs.iteritems():
            if getattr(value, 'meta_constraint', False):
                constraints.append(value)
        klass.META_CONSTRAINTS = constraints

    @classmethod
    def setup_field_properties(cls, klass, bases, attrs):
        for key, value in attrs.iteritems():
            if isinstance(value, Field):
                value.name = key

    @classmethod
    def setup_binding(cls, klass, bases, attrs):
        encoder = acid.encoders.make_json_encoder()
        klass.META_BINDING = EncoderBinding(encoder)


def key(func):
    """Mark a function as the model's primary key function. If the function
    returns a stable result given the same input model, then
    :py:func:`derived_key` should be used instead.

    ::

        @meta.key
        def key_func(self):
            return int(time.time() * 1000)
    """
    func.meta_derived_key = False
    return func


def derived_key(func):
    """Mark a function as the model's primary key function. If the function
    does not return a stable result given the same input model, then use
    :py:func:`key` instead.

    ::

        @meta.derived_key
        def key_func(self):
            return self.name, self.email
    """
    func.meta_derived_key = True
    return func


def blind(func):
    """Mark a key function as being compatible with blind writes. This
    indicates the function never generates a duplicate result, therefore the
    database does not need to check for existing keys during save.

    ::

        @meta.blind
        @meta.key
        def never_repeating_key_func(self):
            return int(time.time() * 10000000000)
    """
    func.meta_blind_keys = True
    return func


def index(func):
    """Mark a function as an index for the model. The function will be called
    during update to produce secondary indices for each item.

    See :py:class:`acid.Index` for more information on the function's return
    value.

    Once the model class has been constructed, accessing ``Model.func_name``
    will return a :py:class:`acid.Index` instance representing the index.
    The original function can still be accessed via
    :py:attr:`acid.Index.func`.

    ::

        class Person(meta.Model):
            age = meta.Integer()

            @meta.index
            def by_age(self):
                return self.age


        # Count all people 22 or older.
        print('Total people older than 22:', Person.by_age.count(lo=22))

        # Fetch youngest and oldest people.
        youngest = Person.by_age.find()
        oldest = Person.by_age.find(reverse=True)
    """
    func.meta_index_func = True
    return func


def constraint(func):
    """Mark a function as implementing a collection constraint. Constraints are
    checked during :py:meth:`acid.meta.Model.save`.

    ::

        @meta.constraint
        def is_age_valid(self):
            return 0 < age < 150
    """
    func.meta_constraint = True
    return func


def on_create(func, klass=None):
    """Mark a function to be called prior to initial save (creation) of a
    model.

    ::

        @meta.on_create
        def set_created(self):
            '''Update the model's creation time.'''
            self.created = datetime.datetime.now()
    """
    assert klass is None, 'external triggers not supported yet.'
    func.meta_on_create = True
    return func


def on_update(func, klass=None):
    """Mark a function to be called prior to create or update of a model.

    ::

        @meta.on_update
        def set_modified(self):
            '''Update the model's modified time.'''
            self.modified = datetime.datetime.utcnow()
    """
    assert klass is None, 'external triggers not supported yet.'
    func.meta_on_update = True
    return func


def on_delete(func, klass=None):
    """Mark a function to be called prior to deletion of a model.

    ::

        @meta.on_delete
        def ensure_can_delete(self):
            '''Prevent deletion if account is active.'''
            if self.state == 'active':
                raise Exception("can't delete while account is active.")
    """
    assert klass is None, 'external triggers not supported yet.'
    func.meta_on_delete = True
    return func


def after_create(func, klass=None):
    """Mark a function to be called after initial save (creation) of a model.

    ::

        @meta.after_create
        def send_welcome_message(self):
            '''Send the user a welcome message.'''
            msg = Message(user_id=self.id, text='Welcome to our service!')
            msg.save()
    """
    assert klass is None, 'external triggers not supported yet.'
    func.meta_after_create = True
    return func


def after_update(func, klass=None):
    """Mark a function to be called after create or update of a model.

    ::

        @meta.after_update
        def notify_update(self):
            '''Push an update event to message queue subscribers.'''
            my_message_queue.send(topic='account-updated', id=self.id)
    """
    assert klass is None, 'external triggers not supported yet.'
    func.meta_after_update = True
    return func


def after_delete(func, klass=None):
    """Mark a function to be called after deletion of a model.

    ::

        @meta.after_delete
        def delete_messages(self):
            '''Delete all the account's messages.'''
            for msg in Message.user_index.find(prefix=self.id):
                msg.delete()
    """
    assert klass is None, 'external triggers not supported yet.'
    func.meta_on_delete = True
    return func


class BaseModel(object):
    """Basic model class implementation. This exists separately from
    :py:class:`Model` to allow clean subclassing of the :py:class:`ModelMeta`
    metaclass.
    """
    @classmethod
    def create_collection(cls):
        if not hasattr(cls, 'META_STORE'):
            raise TypeError('%s nor any of its bases have been bound to a '
                            'Store. You must call %s.bind_store() with a '
                            'acid.Store instance.'
                            % (cls.__name__, cls.__name__))

        key_func = getattr(cls, 'META_KEY_FUNC', None)
        coll = cls.META_STORE.add_collection(
            name=cls.META_COLLECTION_NAME,
            key_func=key_func,
            encoder=cls.META_BINDING.encoder)

        for index_func in cls.META_INDEX_FUNCS:
            coll.add_index(index_func.func_name, index_func)
        cls.META_COLLECTION = coll

    @classmethod
    def collection(cls):
        """Return the :py:class:`acid.Collection` used to store instances of
        this model. The collection handles objects understood by the underlying
        encoder, not Model instances.

        :py:meth:`bind_store` must be called before accessing this property.
        """
        coll = cls.META_COLLECTION
        if coll:
            return coll
        cls.create_collection()
        return cls.META_COLLECTION

    @classmethod
    def bind_store(cls, store):
        """Bind this class and all subclasses to a :py:class:`acid.Store`.

        ::

            store = acid.open('ListEngine')
            MyModel.bind_store(store)
        """
        assert isinstance(store, acid.Store)
        cls.META_STORE = store

    @classmethod
    def get(cls, key):
        """Fetch an instance given its key; see
        :py:meth:`acid.Collection.get`."""
        return cls.collection().get(key)

    @classmethod
    def find(cls, key=None, lo=None, hi=None, reverse=None, include=False):
        """Fetch the first matching instance; see
        :py:meth:`acid.Collection.find`.
        """
        return cls.collection().find(key, lo, hi, reverse, include)

    @classmethod
    def iter(cls, key=None, lo=None, hi=None, reverse=None, max=None,
             include=False):
        """Yield matching models in key order; see
        :py:meth:`acid.Store.values`."""
        return cls.collection().values(key, lo, hi, reverse, max, include)

    def __init__(self, _rec=None, **kwargs):
        self._rec = _rec or self.META_BINDING.new()
        if kwargs:
            for name, value in kwargs.iteritems():
                setattr(self, name, value)

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
            for func in self.META_CONSTRAINTS:
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
