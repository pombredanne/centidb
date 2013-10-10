
.. currentmodule:: acid.meta

.. _meta:

Declarative Interface
#####################

.. automodule:: acid.meta

::

    import acid
    import acid.meta


    class Base(acid.meta.Model):
        """Base for models belonging to this program. Can be used to add common
        fields, and to bind all subclasses to a particular acid.Store with a
        single call."""


    class User(Base):
        email = acid.meta.String()
        first = acid.meta.String()
        last = acid.meta.String()
        age = acid.meta.Integer()

        @acid.meta.constraint
        def sane_age(self):
            """Ensure the user's age is 1..149 if they provided it."""
            return self.age is None or (0 < self.age < 150)


    class Item(Base):
        user_id = acid.meta.Integer()
        data = acid.meta.String()

        @acid.meta.constraint
        def sane_user_id(self):
            """Ensure a User model exists for user_id."""
            return User.get(self.user_id) is not None


    def main():
        Base.bind_store(acid.open('ListEngine'))

        user = User(email='john@example.com', first='John')
        user.save()

        user = User.get(1)
        print 'Saved user:', user

    if __name__ == '__main__':
        main()


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

