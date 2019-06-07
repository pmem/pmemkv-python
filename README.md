# pmemkv-python
Python bindings for pmemkv

*This is experimental pre-release software and should not be used in
production systems. APIs and file formats may change at any time without
preserving backwards compatibility. All known issues and limitations
are logged as GitHub issues.*

## Dependencies

* Python 3.6 and later
* [PMDK](https://github.com/pmem/pmdk) - native persistent memory libraries
* [pmemkv](https://github.com/pmem/pmemkv) - native key/value library

## Installation

Start by installing [pmemkv](https://github.com/pmem/pmemkv/blob/master/INSTALLING.md) on your system.

```sh
cd ~
git clone https://github.com/pmem/pmemkv-python
cd pmemkv-python
sudo locate libpmemkv.so
sudo PMEMKV_SHARED_LIB=<path to libpmemkv.so> python3.6 setup.py install
```

## Testing

TBD

## Example

We are using `/dev/shm` to
[emulate persistent memory](http://pmem.io/2016/02/22/pm-emulation.html)
in this simple example.

```python
from pmemkv import KVEngine

print ("Starting engine")
kv = KVEngine(r"vsmap", '{"path":"/dev/shm/"}')

print ("Put new key")
kv.put(r"key1", r"value1")
assert kv.count() == 1

print ("Reading key back")
assert kv.get(r"key1") == r"value1"

print ("Iterating existing keys")
kv.put(r"key2", r"value2")
kv.put(r"key3", r"value3")
kv.all_strings(lambda k: print ("  visited: {}".format(k.decode())))

print ("Removing existing key")
kv.remove(r"key1")
assert not kv.exists(r"key1")

print ("Stopping engine")
kv.stop()
```
