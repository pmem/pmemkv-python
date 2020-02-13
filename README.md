# pmemkv-python
Python bindings for pmemkv

*This is experimental pre-release software and should not be used in
production systems. APIs and file formats may change at any time without
preserving backwards compatibility. All known issues and limitations
are logged as GitHub issues.*

## Dependencies

* Python 3.6 or later
	* along with python3-setuptools
* [pmemkv](https://github.com/pmem/pmemkv) - native key/value library

## Installation

Start by installing [pmemkv](https://github.com/pmem/pmemkv/blob/master/INSTALLING.md) on your system.

```sh
git clone https://github.com/pmem/pmemkv-python
cd pmemkv-python
```
If pmemkv is installed in defualt directory (e.g. /usr):
```sh
sudo python3 setup.py install
```
or to rather install it locally (in '/home/user_name/.local/lib/python3.X/site-packages'):
```sh
python3 setup.py install --user
```

If pmemkv is in some other directory:
```sh
python3 setup.py build_ext --library-dirs=<path_to_pmemkv_lib_dir> --include-dirs=<path_to_pmemkv_include_dir>
python3 setup.py install --user
```

## Testing

Python bindings includes automated test cases.
Use following command to run test cases:
```sh
cd tests
python3 -m pytest -v pmemkv_tests.py
```

## Examples

We are using `/dev/shm` to
[emulate persistent memory](http://pmem.io/2016/02/22/pm-emulation.html)
in examples.

They can be found within this repository in [examples directory](https://github.com/pmem/pmemkv-python/tree/master/examples).
To execute examples:
```bash
PMEM_IS_PMEM_FORCE=1 python3 basic_example.py
PMEM_IS_PMEM_FORCE=1 python3 restAPI/run_example.py
```
