
# Tickle old reverse iteration bug

import acid.keylib
import acid.engines
import acid._iterators

le = acid.engines.ListEngine()
le.put(acid.keylib.packs(('a', 'b'), 'z'), 'b')
le.put(acid.keylib.packs(('b', 'c'), 'z'), 'b')

it = acid._iterators.BasicIterator(le, 'z')

print
print 'it.keys:', it.keys
print 'it.data:', `it.data`
print

it.reverse()

print 'here'
res = next(it)
print 'there'
print 'res:', res
print 'res.keys:', res.keys
print 'res.data:', `res.data`
print


res = next(it)
print 'res:', res
print 'res.keys:', res.keys
print 'res.data:', `res.data`
print

res = next(it)
print 'res:', res
print 'res.keys:', res.keys
print 'res.data:', `res.data`
print
