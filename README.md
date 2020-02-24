[![Build Status](https://travis-ci.org/pmem/pmemkv-python.svg?branch=master)](https://travis-ci.org/pmem/pmemkv-python)

# pmemkv-python
Python bindings for pmemkv. Currently functionally equal to pmemkv in version 1.0.
Some of the new functionalities (from pmemkv 1.1) are not yet available.

All known issues and limitations are logged as GitHub issues or are described
in pmemkv's man pages.

## Dependencies

* Python 3.6 or later
	* along with python3-setuptools
* python3-dev(el) - header files and a static library for Python
* libpmemkv-dev(el) - at least in version 1.0 - native key/value library

## Installation

Start by installing [pmemkv](https://github.com/pmem/pmemkv/blob/master/INSTALLING.md)
(currently at best in version **1.0.2** or **1.1**) in your system.

```sh
git clone https://github.com/pmem/pmemkv-python
cd pmemkv-python
```
If pmemkv is installed in default directory (e.g. /usr):
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
[emulate persistent memory](https://pmem.io/2016/02/22/pm-emulation.html)
in examples.

They can be found within this repository in [examples directory](https://github.com/pmem/pmemkv-python/tree/master/examples).
To execute examples:
```bash
PMEM_IS_PMEM_FORCE=1 python3 basic_example.py
PMEM_IS_PMEM_FORCE=1 python3 restAPI/run_example.py
```

## Documentation

After installation, docs can be generated using sphinx (to install, run:
`pip3 install sphinx`) by executing commands:
```sh
cd doc
make html
```
