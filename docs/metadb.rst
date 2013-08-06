
.. currentmodule:: centidb.metadb

Declarative Interface
#####################

The `centidb.metadb` module provides an ORM-like metaclass that allows
simplified definition of database models using Python code.

This module is a work in progress, and the most interesting aspect of it is
missing. A future version will use the model definition to automatically
produce and maintain a compact encoding. For now this is just a curiosity.

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

.. autofunction:: centidb.metadb.index


Specifying a key function
+++++++++++++++++++++++++

.. autofunction:: centidb.metadb.key
.. autofunction:: centidb.metadb.derived_key
.. autofunction:: centidb.metadb.blind


Specifying constraints
++++++++++++++++++++++

.. autofunction:: centidb.metadb.constraint

