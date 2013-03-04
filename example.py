import plyvel
import centidb

store = centidb.Store(plyvel.DB('test.ldb', create_if_missing=True))
people = centidb.Collection(store, 'people', key_func=lambda p: p['name'])
people.add_index('age', lambda p: p.get('age'))

people.put({
    'name': 'Alfred',
    'age': 46
})

people.put({
    'name': 'Jemima',
    'age': 29
})

people.put({
    'name': 'David',
    'age': 29
})

print list(people.indices['age'].iteritems(29))
