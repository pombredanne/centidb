
.. currentmodule:: acid.events

.. _events:

Events
######

.. module:: acid.events

The `acid.events` module defines a set of events that may be fired when some
mutation occurs on a collection.

An event function may be used as a decorator applied to a method of an
:py:mod:`acid.meta` Model, in which case the function will be associated with
the Model's :py:class:`Collection <acid.Collection>` when it is created:

::

    class Account(acid.meta.Model):
        @acid.events.on_create
        def generate_password(self):
            """Assign a password to the account during creation."""
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


Constraints
+++++++++++

.. autofunction:: acid.events.constraint ()


Event Types
+++++++++++

It is possible to register functions to be called when models are modified
somehow. Inheritance is respected, in that triggers registered against a base
class will be invoked prior to any registered against the class of the model
being modified.

.. autofunction:: acid.events.on_create ()
.. autofunction:: acid.events.on_update ()
.. autofunction:: acid.events.on_delete ()

.. autofunction:: acid.events.after_create ()
.. autofunction:: acid.events.after_update ()
.. autofunction:: acid.events.after_delete ()
.. autofunction:: acid.events.after_replace ()

.. autofunction:: acid.events.on_commit ()
.. autofunction:: acid.events.after_commit ()


External triggers
+++++++++++++++++

It is also possible to create a trigger from outside the model definition. This
uses the same `on_*` and `after_*` functions, but includes a second parameter,
which is a reference to the model class to subscribe to.

::

    def log_create(model):
        print 'Model created!', model

    def log_delete(model):
        print 'Model deleted!', model

    def install_debug_helpers():
        if config.DEBUG:
            acid.events.after_create(log_create, models.Base)
            acid.events.after_delete(log_delete, models.Base)
