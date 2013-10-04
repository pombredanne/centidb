
import lmdb


env = lmdb.open(path='/media/scratch/t4.lmdb',
     map_size=1048576*1024*3,
     map_async=True,
     sync=False,
     metasync=False,
     writemap=False)


txn = env.begin(write=True)
it = None

unhex = lambda s: s.decode('hex')

for line in file('/tmp/lmdb.trace'):
    bits = line.rstrip('\n').split(' ')
    if bits[0] == 'put':
        txn.put(unhex(bits[1]), unhex(bits[2]))
    elif bits[0] == 'delete':
        txn.delete(unhex(bits[1]))
    elif bits[0] == 'commit':
        print 'commit'
        txn.commit()
        txn = env.begin(write=True)
    elif bits[0] == 'iter':
        it = txn.cursor()._iter_from(unhex(bits[1]),
                                     unhex(bits[2]) == 'True')
    elif bits[0] == 'fetch':
        key, value = next(it, (None, None))
    elif bits[0] == 'yield':
        assert (key, value) == (unhex(bits[1]), unhex(bits[2]))

