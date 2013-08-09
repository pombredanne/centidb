
.. currentmodule:: centidb.metadb

Declarative Interface
#####################

.. automodule:: centidb.metadb

::

    import centidb
    from centidb import metadb


    class Base(metadb.Model):
        """Base for models belonging to this program. Can be used to add common
        fields, and to bind all subclasses to a particular centidb.Store with a
        single call."""


    class User(Base):
        email = metadb.String()
        first = metadb.String()
        last = metadb.String()
        age = metadb.Integer()

        @metadb.constraint
        def sane_age(self):
            """Ensure the user's age is 1..149 if they provided it."""
            return age is None or (0 < age < 150)


    class Item(Base):
        user_id = metadb.Integer()
        data = metadb.String()

        @metadb.constraint
        def sane_user_id(self):
            """Ensure a User model exists for user_id."""
            return User.get(self.user_id) is not None


    def main():
        Base.bind_store(centidb.open('ListEngine'))

        user = User(email='dw@botanicus.net', first='David', last='Wilson')
        user.save()

        user = User.by_key(1)


Model class
+++++++++++

.. autoclass:: centidb.metadb.Model (\**kwargs)
    :members:
    :inherited-members:

.. autoclass:: centidb.metadb.BaseModel (\**kwargs)


Field Types
+++++++++++

.. autoclass:: centidb.metadb.Bool
.. autoclass:: centidb.metadb.Double
.. autoclass:: centidb.metadb.Integer
.. autoclass:: centidb.metadb.String


Specifying an index
+++++++++++++++++++

.. autofunction:: centidb.metadb.index ()


Specifying a key function
+++++++++++++++++++++++++

.. autofunction:: centidb.metadb.key ()
.. autofunction:: centidb.metadb.derived_key ()
.. autofunction:: centidb.metadb.blind ()


Specifying constraints
++++++++++++++++++++++

.. autofunction:: centidb.metadb.constraint ()


Triggers
++++++++

It is possible to register functions to be called when models are modified
somehow. Inheritance is respected, in that triggers registered against a base
class will be invoked prior to any registered against the class of the model
being modified.

.. autofunction:: centidb.metadb.on_create ()
.. autofunction:: centidb.metadb.on_update ()
.. autofunction:: centidb.metadb.on_delete ()

.. autofunction:: centidb.metadb.after_create ()
.. autofunction:: centidb.metadb.after_update ()
.. autofunction:: centidb.metadb.after_delete ()


External triggers
-----------------

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
            metadb.after_create(log_create, models.Base)
            metadb.after_delete(log_delete, models.Base)
