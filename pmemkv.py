import pmemkv_NI

class KVEngine():

    stopped = False

    def __init__(self, engine_name, config):
        retVal = pmemkv_NI.start(engine_name, config)
        if(error):
            raise ValueError(error)

    def stop(self):
        if not self.stopped:
            self.stopped = True
            pmemkv_NI.stop()

    def put(self, key, value):
        returned = pmemkv_NI.put(key, value)
        if returned < 0:
            raise RuntimeError("Unable to put " +str(key))
        return bool(returned)

    def get(self, key):
        return pmemkv_NI.get(key)

    def all(self, func):
        pmemkv_NI.all(func)

    def all_above(self, key, func):
        pmemkv_NI.all_above(key, func)

    def all_below(self, key, func):
        pmemkv_NI.all_below(key, func)

    def all_between(self, key1, key2, func):
        pmemkv_NI.all_between(key1, key2, func)

    def count(self):
        return pmemkv_NI.count()

    def count_above(self, key):
        return pmemkv_NI.count_above(key)

    def count_below(self, key):
        return pmemkv_NI.count_below(key)

    def count_between(self, key1, key2):
        return pmemkv_NI.count_between(key1, key2)

    def each(self, func):
        pmemkv_NI.each(func)

    def each_above(self, key, func):
        pmemkv_NI.each_above(key, func)

    def each_below(self, key, func):
        pmemkv_NI.each_below(key, func)

    def each_between(self, key1, key2, func):
        pmemkv_NI.each_between(key1, key2, func)

    def exists(self, key):
        return bool(pmemkv_NI.exists(key))

    def remove(self, key):
        returned = pmemkv_NI.remove(key)
        if returned < 0:
            raise RuntimeError("Unable to put " +str(key))
        return bool(returned)
