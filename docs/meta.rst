
.. warning::

    This is a work in progress! The code you see here does not run yet, itonly
    illustrates how the library will eventually work.

.. currentmodule:: acid.meta

Declarative Interface
#####################

.. automodule:: acid.meta

::

    import acid
    from acid import meta


    class Base(meta.Model):
        """Base for models belonging to this program. Can be used to add common
        fields, and to bind all subclasses to a particular acid.Store with a
        single call."""


    class User(Base):
        email = meta.String()
        first = meta.String()
        last = meta.String()
        age = meta.Integer()

        @meta.constraint
        def sane_age(self):
            """Ensure the user's age is 1..149 if they provided it."""
            return age is None or (0 < age < 150)


    class Item(Base):
        user_id = meta.Integer()
        data = meta.String()

        @meta.constraint
        def sane_user_id(self):
            """Ensure a User model exists for user_id."""
            return User.get(self.user_id) is not None


    def main():
        Base.bind_store(acid.open('ListEngine'))

        user = User(email='dw@botanicus.net', first='David')
        user.save()

        user = User.get(1)


Model class
+++++++++++

.. autoclass:: acid.meta.Model (\**kwargs)
    :members:
    :inherited-members:

.. autoclass:: acid.meta.BaseModel (\**kwargs)


Field Types
+++++++++++

.. autoclass:: acid.meta.Bool
.. autoclass:: acid.meta.Double
.. autoclass:: acid.meta.Integer
.. autoclass:: acid.meta.String


Specifying an index
+++++++++++++++++++

.. autofunction:: acid.meta.index ()


Specifying a key function
+++++++++++++++++++++++++

.. autofunction:: acid.meta.key ()
.. autofunction:: acid.meta.derived_key ()
.. autofunction:: acid.meta.blind ()


Specifying constraints
++++++++++++++++++++++

.. autofunction:: acid.meta.constraint ()


Triggers
++++++++

It is possible to register functions to be called when models are modified
somehow. Inheritance is respected, in that triggers registered against a base
class will be invoked prior to any registered against the class of the model
being modified.

.. autofunction:: acid.meta.on_create ()
.. autofunction:: acid.meta.on_update ()
.. autofunction:: acid.meta.on_delete ()

.. autofunction:: acid.meta.after_create ()
.. autofunction:: acid.meta.after_update ()
.. autofunction:: acid.meta.after_delete ()


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
            meta.after_create(log_create, models.Base)
            meta.after_delete(log_delete, models.Base)
