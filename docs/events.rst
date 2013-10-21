
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

Events registered against against a class will also fire for any modifications
to instances of any subclasses. Note that notification order is most-specific
to least-specific.

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

An *observing event* is one that causes the storage engine to read and return
any previous record value during a write operation. Even for engines that
support fast *read-modify-write* operations, emitting the event further
requires decoding of the old record value. Therefore where possible, prefer
subscribing to a non-observing event if it fits your use case.

    .. csv-table:: Observing Event Types (— means unsupported)
        :class: pants
        :header: Event, Used with Model, Used with Collection

        **constraint**, No, No
        **on_create**, No, —
        **on_update**, No, No
        **on_delete**, No, —
        **on_commit**, No, No
        **after_create**, No, Yes
        **after_update**, No, No
        **after_delete**, No, Yes
        **after_replace**, Yes, Yes
        **after_abort**, No, No
        **after_commit**, No, No

Although it would be possible to support `on_create` and `on_delete` storage
engine events, doing so would necessitate engine operations that require an
extra roundtrip for any engine that relies on the network, unlike the `after_*`
variants which are supported by a single "mutate and return previous" message.
Additionally for networked storage systems that lack transactions, it is very
likely the single message can be supported as an atomic operation.


Debugging
+++++++++

Since it is possible to subscribe to events on a base clase, it is possible to
subscribe to events on :py:class:`acid.meta.Model` itself, thus capturing all
database operations. This may be useful for diagnostics:

::

    def log_create(model):
        print 'Model created!', model

    def log_delete(model):
        print 'Model deleted!', model

    def install_debug_helpers():
        if config.DEBUG:
            acid.events.after_create(log_create, acid.meta.Model)
            acid.events.after_delete(log_delete, acid.meta.Model)


Constraints
+++++++++++

.. autofunction:: acid.events.constraint ()


Event Types
+++++++++++

.. autofunction:: acid.events.on_create ()
.. autofunction:: acid.events.on_update ()
.. autofunction:: acid.events.on_delete ()

.. autofunction:: acid.events.after_create ()
.. autofunction:: acid.events.after_update ()
.. autofunction:: acid.events.after_delete ()
.. autofunction:: acid.events.after_replace ()


Transaction Events
++++++++++++++++++

These may be applied to a :py:class:`Model <acid.meta.Model>`,
:py:class:`Collection <acid.Collection>`, or :py:class:`Store <acid.Store>`. In
the case of Models or collections, the subscription is proxied through to the
associated store.

.. autofunction:: acid.events.after_abort ()
.. autofunction:: acid.events.on_commit ()
.. autofunction:: acid.events.after_commit ()
