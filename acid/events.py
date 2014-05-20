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
Mutation events.

See http://acid.readthedocs.org/en/latest/events.html
"""

from __future__ import absolute_import

import functools
import acid.errors


def _check_constraint(func, model):
    """on_update trigger that checks a constraint is correct. The metaclass
    wraps this in a functools.partial() and adds to to the list of on_update
    triggers for the model."""
    if not func(model):
        raise acid.errors.ConstraintError(name=func.func_name,
            msg='Constraint %r failed' % (func.func_name,))


def constraint(func, target=None):
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
    wrapped = functools.partial(_check_constraint, func)
    return on_update(wrapped, target)


def _listen(name, func, target):
    if target:
        target._listen(name, func)
    else:
        setattr(func, 'meta_' + name, True)
        return func


def on_create(func, target=None):
    """Request `func` be invoked as `func(model)` when a new `model` which has
    no key assigned is about to be saved for the first time. This event can
    only be applied to :py:class:`acid.meta.Model`, it has no meaning when
    applied to a collection.

    ::

        @acid.events.on_create
        def set_created(self):
            '''Update the model's creation time.'''
            self.created = datetime.datetime.now()
    """
    return _listen('on_create', func, target)


def on_update(func, target=None):
    """Request `func` be invoked as `func(model)` when `model` is about to be
    saved for any reason. Alternatively when applied to a collection, request
    `func(key, rec)` be invoked.

    ::

        @acid.events.on_update
        def set_modified(self):
            '''Update the model's modified time.'''
            self.modified = datetime.datetime.utcnow()
    """
    return _listen('on_update', func, target)


def after_replace(func, target=None):
    """Request `func` be invoked as `func(self, old)` when `model` is about to
    replace an older version of itself. Alternatively when applied to a
    collection, request `func(key, old, new)` be invoked.

    ::

        @acid.events.after_replace
        def after_replace(self, old):
            print "Record %s replaced: old ctime %s, new time %s" %\\
                  (self.key, old.ctime, self.ctime)
    """
    return _listen('after_replace', func, target)


def on_delete(func, target=None):
    """Request `func` be invoked as `func(model)` when a `model` that has
    previously been assigned a key is about to be deleted. This event can only
    be applied to :py:class:`acid.meta.Model`, it has no meaning when applied
    to a collection.

    ::

        @acid.events.on_delete
        def ensure_can_delete(self):
            '''Prevent deletion if account is active.'''
            if self.state == 'active':
                raise Exception("can't delete while account is active.")
    """
    return _listen('on_delete', func, target)


def after_create(func, target=None):
    """Request `func` be invoked as `func(model)` when a `model` that had no
    previous key has been saved. Alternatively when applied to a collection,
    request `func(key, rec)` be invoked.

    ::

        @acid.events.after_create
        def send_welcome_message(self):
            '''Send the user a welcome message.'''
            msg = Message(user_id=self.id, text='Welcome to our service!')
            msg.save()
    """
    return _listen('after_create', func, target)


def after_update(func, target=None):
    """Request `func` be invoked as `func(model)` after any change to `model`.
    Alternatively when applied to a collection, request `func(key, rec)` be
    invoked.

    ::

        @acid.events.after_update
        def notify_update(self):
            '''Push an update event to message queue subscribers.'''
            my_message_queue.send(topic='account-updated', id=self.id)
    """
    return _listen('after_update', func, target)


def after_delete(func, target=None):
    """Request `func` be invoked as `func(model)` after any `model` that
    previously had an assigned key is deleted. Alternatively when applied to a
    collection, request `func(key, rec)` be invoked.

    ::

        @acid.events.after_delete
        def delete_messages(self):
            '''Delete all the account's messages.'''
            for msg in Message.user_index.find(prefix=self.id):
                msg.delete()
    """
    return _listen('after_delete', func, target)


def after_abort(func, target=None):
    """Request `func` be invoked as `func()` following abort of any
    transaction."""
    return _listen('after_abort', func, target)


def on_commit(func, target=None):
    """Request `func` be invoked as `func()` prior to commit of any
    transaction."""
    return _listen('on_commit', func, target)


def after_commit(func, target=None):
    """Request `func` be invoked as `func()` following commit of any
    transaction."""
    return _listen('after_commit', func, target)
