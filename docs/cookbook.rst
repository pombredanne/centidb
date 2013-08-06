Cookbook
########

Index Examples
++++++++++++++

Index Usage
-----------

::

    import itertools
    import centidb
    from pprint import pprint

    import plyvel
    store = centidb.Store(plyvel.DB('test.ldb', create_if_missing=True))
    people = centidb.Collection(store, 'people', key_func=lambda p: p['name'])
    people.add_index('age', lambda p: p['age'])
    people.add_index('name', lambda p: p['age'])
    people.add_index('city_age', lambda p: (p.get('city'), p['age']))

    make_person = lambda name, city, age: dict(locals())

    people.put(make_person(u'Alfred', u'Nairobi', 46))
    people.put(make_person(u'Jemima', u'Madrid', 64))
    people.put(make_person(u'Mildred', u'Paris', 34))
    people.put(make_person(u'Winnifred', u'Paris', 24))

    # Youngest to oldest:
    pprint(list(people.indices['age'].items()))

    # Oldest to youngest:
    pprint(list(people.indices['age'].values(reverse=True)))

    # Youngest to oldest, by city:
    it = people.indices['city_age'].values()
    for city, items in itertools.groupby(it, lambda p: p['city']):
        print '  ', city
        for person in items:
            print '    ', person

    # Fetch youngest person:
    print people.indices['age'].get()

    # Fetch oldest person:
    print people.indices['age'].get(reverse=True)


Reverse Indices
---------------

Built-in support is not yet provided for compound index keys that include
components that are sorted in descending order, however this is easily
emulated:

+-----------+---------------------------------------+
+ *Type*    + *Inversion function*                  |
+-----------+---------------------------------------+
+ Numbers   | ``-i``                                |
+-----------+---------------------------------------+
+ Boolean   + ``not b``                             |
+-----------+---------------------------------------+
+ String    + ``centidb.invert(s)``                 |
+-----------+---------------------------------------+
+ Unicode   + ``centidb.invert(s.encode('utf-8'))`` |
+-----------+---------------------------------------+
+ UUID      + ``centidb.invert(uuid.get_bytes())``  |
+-----------+---------------------------------------+
+ Key       + ``Key(centidb.invert(k))``            |
+-----------+---------------------------------------+

Example:

::

    coll.add_index('name_age_desc',
        lambda person: (person['name'], -person['age']))

Note that if a key contains only a single value, or all the key's components
are in descending order, then transformation is not required as the index
itself may be iterated in reverse:

::

    coll = centidb.Collection(store, 'people',
        key_func=lambda person: person['name'])
    coll.add_index('age', lambda person: person['age'])
    coll.add_index('age_height',
        lambda person: (person['age'], person['height']))

    # Not necessary.
    coll.add_index('name_desc',
        lambda person: centidb.inverse(person['name'].encode('utf-8')))

    # Not necessary.
    coll.add_index('age_desc', lambda person: -person['age'])

    # Not necessary.
    coll.add_index('age_desc_height_desc',
        lambda person: (-person['age'], -person['height']))

    # Equivalent to 'name_desc' index:
    it = coll.items(reverse=True)

    # Equivalent to 'age_desc' index:
    it = coll.index['age'].items(reverse=True)

    # Equivalent to 'age_desc_height_desc' index:
    it = coll.index['age_height'].items(reverse=True)


Covered indices
---------------

No built-in support for covered indices is provided yet, however this can be
emulated by encoding the data to be covered as part of the index key:

::

    coll = centidb.Collection(store, 'people')

    age_height_name = coll.add_index('age_height_name',
        lambda person: (person['age'], person['height'], person['name']))

    age_photo = coll.add_index('age_photo',
        lambda person: (person['age'], file(person['photo']).read()))


    coll.put({'name': u'Bob', 'age': 69, 'height': 113})

    # Query by key but omit covered part:
    tup = next(age_height_name.tups((69, 113)))
    name = tup and tup[-1]

    tup = next(age_photo.tups(69))
    photo = tup and tup[-1]

A future version may allow storing arbitrarily encoded values along with index
entries as part of the API.



Compression Examples
++++++++++++++++++++

Similar records
---------------

Batch compression is useful for storing collections of similar data, such as a
collection of web pages sharing common HTML tags, or perhaps even sharing a
common header and footer. By handing the compressor more data with similar
redundancies, it can do a much better job of producing a smaller bitstream
overall.

Imagine you're building a web scraper, fetching data from a handful of domains
that each has its own distinctive layout. You're not sure about the quality of
your scraper, so you wish to store the source pages in case you need to parse
them again due to a scraper bug.

We're storing our pages in a collection with the record key being the page's
URL. This means pages for the same domain will be physically grouped in the
underlying storage engine, and that contiguous ranges of keys exist where all
keys in the range relate to only a single domain.

::

    >>> pages = centidb.Collection(store, 'pages')
    >>> # ...

    >>> pprint(list(pages.keys(max=5)))
    [("http://bbb.com/page?id=1",),
     ("http://bbb.com/page?id=2",),
     ("http://bbb.com/page?id=3",),
     ("http://ccc.com/page?id=1",),
     ("http://ccc.com/page?id=2")]

    >>> # Print the first record:
    >>> pprint(pages.find())
    {
        "url": "http://bbb.com/page?id=1",
        "html": ... # raw HTML
    }

Here we can use :py:meth:`Collection.batch` with the `grouper=` parameter to
compress 10 pages at a time, while ensuring batches contain only pages relating
to a single domain:

::

    >>> import urlparse

    >>> def domain_grouper(obj):
    ...     return urlparse.urlparse(obj['url']).netloc
    ...

    >>> # Rewrite all records in the collection into batches of 10, ensuring
    >>> # pages from distinct domains don't get batched together:
    >>> coll.batch(max_recs=10, grouper=domain_grouper)
    (1000, 100, None) # Found items, made batches, next key


Archiving Data
--------------



Changing key function
+++++++++++++++++++++

The simplest way to migrate to a new key function is to create a new
collection, and iteratively copy from the old collection:

::

    >>> new_coll.puts(old_coll.values())

