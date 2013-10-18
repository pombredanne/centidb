
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

.. code-block:: python

    class Account(acid.meta.Model):
        @acid.events.on_create
        def generate_password(self):
            """Assign a password to the account during creation."""
            self.password = passlib.generate(chars=8)

Alternatively they may be registered for any free-standing function against
any model or :py:class:`Collection <acid.Collection>`:

.. code-block:: python

    def generate_password(acct):
        self.password = passlib.generate(chars=8)

    # Works against a Model class:
    acid.events.on_create(generate_password, target=Account)

    # Or a Collection instance:
    store.add_collection('accounts')
    acid.events.on_create(generate_password, target=store['accounts'])


Model vs Collection Events
++++++++++++++++++++++++++

This interface attempts to unify two very different kinds of events: those
occurring logically within the application's data model, and those occurring as
a result of interaction with the storage engine. When applied to
:py:class:`acid.meta.Model`, most event types rely only on in-memory state
tracked by the model class, whereas those applied to
:py:class:`acid.Collection` relate to the state of the storage engine as
reported at the time of the mutation.

The reason for having both event types is that usually Model events are good
enough for application use, but storage engine events are needed to robustly
implement features such as indexing. Instead of burying the storage engine
events within Acid's implementation, they are exposed to user code.


Observing Events
++++++++++++++++

There is an important difference between the events available, since
subscribing to some will cause Acid to adopt a less efficient strategy when
interacting with the storage engine. Engines like :py:class:`LmdbEngine
<acid.engines.LmdbEngine>` support efficient *read-modify-write* operations,
whereas others such as :py:class:`PlyvelEngine <acid.engines.PlyvelEngine>`
have reads that are more expensive than writes.

An *observing event* is one that will cause Acid to ask the storage engine to
read and return the previous record value, if any, during a write operation.
Even for storage engines that support fast *read-modify-write* operations,
emitting the event further requires decoding of the old record value. Therefore
where possible, prefer subscribing to a non-observing event if it fits your use
case.

    .. csv-table:: Observing Event Types (— means unsupported)
        :class: pants
        :header: Event, Used with Model, Used with Collection

        **constraint**, No, No
        **on_create**, No, —
        **on_update**, No, No
        **on_delete**, No, —
        **on_abort**, No, No
        **on_commit**, No, No
        **after_create**, No, Yes
        **after_update**, No, No
        **after_delete**, No, Yes
        **after_replace**, Yes, Yes


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

.. autofunction:: acid.events.on_abort ()
.. autofunction:: acid.events.after_abort ()
.. autofunction:: acid.events.on_commit ()
.. autofunction:: acid.events.after_commit ()
