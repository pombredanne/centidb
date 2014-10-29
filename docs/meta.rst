
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
        email = acid.meta.Field('string')
        first = acid.meta.Field('string')
        last = acid.meta.Field('string')
        age = acid.meta.Field('ivar')

        @acid.meta.constraint
        def sane_age(self):
            """Ensure the user's age is 1..149 if they provided it."""
            return self.age is None or (0 < self.age < 150)


    class Item(Base):
        user_id = acid.meta.Field('ivar')
        data = acid.meta.Field('bytes')

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


Field Classes
+++++++++++++

.. autoclass:: acid.meta.Field
.. autoclass:: acid.meta.List


Primitive Types
+++++++++++++++

    ``bool``
        Fixed boolean, either ``True`` or ``False``. Encodes to 1 byte (+ tag)
        using the default Protocol Buffer encoding, or `n+taglen` bytes when
        used in a list.

    ``double``
        Fixed 64-bit floating point. Encodes to 8 bytes (+ tag) using the
        default Protocol Buffer encoding, or `(n*4)+taglen` bytes when used in
        a list.

    ``float``
        Fixed 32-bit floating point. Encodes to 4 bytes (+ tag) using the
        default Protocol Buffer encoding, or `(n*4)+taglen` when used in a
        list.

    ``fraction``
        pass

    ``decimal``
        pass

    ``i32``
        Fixed 32-bit signed integer. Encodes to 4 bytes (+ tag) using the
        default Protocol Buffer encoding, or `(n*4)+taglen` when used in a
        list.

    ``i64``
        Fixed 64-bit signed integer. Encodes to 4 bytes (+ tag) using the
        default Protocol Buffer encoding, or `(n*4)+taglen` when used in a
        list.

    ``u32``
        Fixed 32-bit unsigned integer. Encodes to 4 bytes (+ tag) using the
        default Protocol Buffer encoding, or `(n*4)+taglen` when used in a
        list.

    ``u64``
        Fixed 64-bit unsigned integer. Encodes to 4 bytes (+ tag) using the
        default Protocol Buffer encoding, or `(n*4)+taglen` when used in a
        list.

    ``ivar``
        Variable-length 64-bit signed integer. Encodes to between 1 and 10
        bytes using the default Protocol Buffer encoding. Negative numbers
        always encode to 10 bytes; use ``svar`` for efficient negative
        representation.

    ``svar``
        Variable-length 64-bit signed, zig-zag integer. Encodes to between 1
        and 10 bytes using the default Protocol Buffer encoding. Negative
        numbers are represented efficiently by interleaving them with the
        positive representation.

    ``uvar``
        Variable-length 64-bit unsigned integer. Encodings to between 1 and 10
        bytes using the default Protocol Buffer encoding.

    ``inet4``
        Fixed 32-bit IPv4 address, represented as a Python string
        `NNN.NNN.NNN.NNN`. Encodes to 4 bytes (+ tag) using the default
        Protocol Buffer encoding, or `(n*4)+taglen` when used in a list.

    ``inet4port``
        pass

    ``inet6``
        pass

    ``inet6port``
        pass

    ``json``
        Any object that can be encoded using the Python JSON encoder. Encodes
        to variable-length string.

    ``pickle``
        Any object that can be encoded using the Python pickle encoder. Encodes
        to variable-length string.

    ``bytes``
        Variable-length bytestring. Encodes to `len+varintlen+taglen` bytes
        using the default Protocol Buffer encoding.

    ``string``
        Variable-length Unicode string. Encodes to `len+varintlen+taglen` bytes
        using the default Protocol Buffer encoding.

    ``uuid``
        Fixed 128-bit UUID, represented as a Python :py:class:`uuid.UUID`.
        Encodes to `len+varintlen+taglen` bytes using the default Protocol
        Buffer encoding.


Specifying an index
+++++++++++++++++++

.. autofunction:: acid.meta.index ()


Specifying a key function
+++++++++++++++++++++++++

.. autofunction:: acid.meta.key ()

