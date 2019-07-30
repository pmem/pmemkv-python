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
```
If pmemkv is installed in defualt directory (e.g. /usr):
```sh
sudo python3.6 setup.py install
```
If pmemkv is in some other directory:
```sh
sudo python3.6 setup.py build_ext --library-dirs=path_to_pmemkv_lib_dir --include-dirs=path_to_pmemkv_include_dir
sudo python3.6 setup.py install
```

## Testing

Python bindings includes automated unittest cases.
Use following command to run test cases:
```python
python3.6 -m unittest -v pmemkv_tests.py
```

## Example

We are using `/dev/shm` to
[emulate persistent memory](http://pmem.io/2016/02/22/pm-emulation.html)
in this simple example.

```python
from pmemkv import Database

print ("Starting engine")
db = Database(r"vsmap", '{"path":"/dev/shm","size":1073741824}')

print ("Put new key")
db.put(r"key1", r"value1")
assert db.count_all() == 1

print ("Reading key back")
assert db.get(r"key1") == r"value1"

print ("Iterating existing keys")
db.put(r"key2", r"value2")
db.put(r"key3", r"value3")
db.get_keys_strings(lambda k: print ("  visited: {}".format(k.decode())))

print ("Removing existing key")
db.remove(r"key1")
assert not db.exists(r"key1")

print ("Stopping engine")
db.stop()
```
