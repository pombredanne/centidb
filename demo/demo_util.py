

def store_len(store):
    it = store._txn_context.get().iter('', False)
    return sum(1 for _ in it)


def store_size(store):
    it = store._txn_context.get().iter('', False)
    return sum(len(k) + len(v) for k, v in it)

