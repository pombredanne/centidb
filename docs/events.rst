
.. currentmodule:: acid.events

.. _events:

Events
######

.. automodule:: acid.events


Specifying constraints
++++++++++++++++++++++

.. autofunction:: acid.events.constraint ()


Triggers
++++++++

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
