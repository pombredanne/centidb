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
The `acid.events` module defines a set of events that may be fired when some
mutation occurs on a collection.

An event function may be used as a decorator applied to a method of an
:py:mod:`acid.meta` Model, in which case the function will be associated with
the Model's :py:class:`Collection <acid.Collection>` when it is created:

::

    class Account(acid.meta.Model):
        @acid.events.on_create
        def generate_password(self):
            \"\"\"Assign a password to the account during creation.\"\"\"
            self.password = passlib.generate(chars=8)

Alternatively they may be registered for any free-standing function against
any model or :py:class:`Collection <acid.Collection>`:

::

    def generate_password(acct):
        self.password = passlib.generate(chars=8)

    # Works against a Model class:
    acid.events.on_create(generate_password, target=Account)

    # Or a Collection instance:
    store.add_collection('accounts')
    acid.events.on_create(generate_password, target=store['accounts'])

"""


def constraint(func):
    """Mark a function as implementing a collection constraint. The function
    should return ``True`` if the constraint is satisfied, or raise an
    exception or return any falsey value otherwise. Constraints are implemented
    as :py:func:`on_update` handlers that convert falsey return values into
    :py:class:`ConstraintErrors <acid.errors.ConstraintError>`.

    ::

        @acid.events.constraint
        def is_age_valid(self):
            return 0 < self.age < 150
    """
    func.meta_constraint = True
    return func


def on_create(func, target=None):
    """Mark a function to be called prior to initial save (creation) of a
    model.

    ::

        @acid.events.on_create
        def set_created(self):
            '''Update the model's creation time.'''
            self.created = datetime.datetime.now()
    """
    assert target is None, 'external events not supported yet.'
    func.meta_on_create = True
    return func


def on_update(func, target=None):
    """Mark a function to be called prior to create or update of a record.

    ::

        @acid.events.on_update
        def set_modified(self):
            '''Update the model's modified time.'''
            self.modified = datetime.datetime.utcnow()
    """
    assert target is None, 'external events not supported yet.'
    func.meta_on_create = True
    func.meta_on_update = True
    return func


def after_replace(func, target=None):
    """Mark a function to be called after replacement of a record. Unlike
    :py:func:`on_create` or :py:func:`on_update`, `after_replace` only fires if
    the storage engine indicates an existing record value existed prior to
    save. No `on_replace` event is currently provided, since it is not easy to
    make efficient, and due to lack of need.

    ::

        @acid.events.after_replace
        def after_replace(self, old):
            print "Record %s replaced: old ctime %s, new time %s" %\\
                  (self.key, old.ctime, self.ctime)

    .. caution::

        Registering any `after_replace` handler causes :py:meth:`Collection.put
        <acid.Collection.put>` to use :py:meth:`Engine.replace
        <acid.engines.Engine.replace>` instead of :py:meth:`Engine.put
        <acid.engines.Engine.put>`, which cannot be supported efficiently for
        all storage engines. Note that indices are internally implemented as
        `after_replace` handlers, so there is no additional penalty to
        registering handlers if any index is defined on a collection.

    """
    assert target is None, 'external events not supported yet.'
    func.meta_on_create = True
    func.meta_on_update = True
    return func


def on_delete(func, target=None):
    """Mark a function to be called prior to deletion of a record.

    .. caution::

        When applied to a :py:class:`Model <acid.meta.Model>`, fires if
        :py:meth:`delete <acid.meta.Model.delete>` is invoked for any model
        with its key set, which is only possible following a load or save.

        However when applied to a :py:class:`Collection <acid.Collection>`,
        causes :py:meth:`Collection.delete <acid.Collection.delete>` to change
        behaviour, causing a lookup and decode during deletion.

    ::

        @acid.events.on_delete
        def ensure_can_delete(self):
            '''Prevent deletion if account is active.'''
            if self.state == 'active':
                raise Exception("can't delete while account is active.")
    """
    assert target is None, 'external events not supported yet.'
    func.meta_on_delete = True
    return func


def after_create(func, target=None):
    """Mark a function to be called after initial save (creation) of a record.

    ::

        @acid.events.after_create
        def send_welcome_message(self):
            '''Send the user a welcome message.'''
            msg = Message(user_id=self.id, text='Welcome to our service!')
            msg.save()
    """
    assert target is None, 'external events not supported yet.'
    func.meta_after_create = True
    func.meta_after_update = True
    return func


def after_update(func, target=None):
    """Mark a function to be called after create or update of a record.

    ::

        @acid.events.after_update
        def notify_update(self):
            '''Push an update event to message queue subscribers.'''
            my_message_queue.send(topic='account-updated', id=self.id)
    """
    assert target is None, 'external events not supported yet.'
    func.meta_after_update = True
    return func


def after_delete(func, target=None):
    """Mark a function to be called after deletion of a record.

    .. caution::

        When applied to a :py:class:`Model <acid.meta.Model>`, fires if
        :py:meth:`delete <acid.meta.Model.delete>` is invoked for any model
        with its key set, which is only possible following a load or save.

        However when applied to a :py:class:`Collection <acid.Collection>`,
        causes :py:meth:`Collection.delete <acid.Collection.delete>` to change
        behaviour, causing a lookup and decode during deletion.

    ::

        @acid.events.after_delete
        def delete_messages(self):
            '''Delete all the account's messages.'''
            for msg in Message.user_index.find(prefix=self.id):
                msg.delete()
    """
    assert target is None, 'external events not supported yet.'
    func.meta_after_delete = True
    return func


def on_commit(func, target=None):
    """Mark a function to be called prior to commit of any transaction.
    """
    assert target is None, 'external events not supported yet.'
    func.meta_on_commit = True
    return func


def after_commit(func, target=None):
    """Mark a function to be called prior to commit of any transaction.
    """
    assert target is None, 'external events not supported yet.'
    func.meta_after_commit = True
    return func
