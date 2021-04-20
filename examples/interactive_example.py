#  Copyright 2021, Intel Corporation
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions
#  are met:
#
#      * Redistributions of source code must retain the above copyright
#        notice, this list of conditions and the following disclaimer.
#
#      * Redistributions in binary form must reproduce the above copyright
#        notice, this list of conditions and the following disclaimer in
#        the documentation and/or other materials provided with the
#        distribution.
#
#      * Neither the name of the copyright holder nor the names of its
#        contributors may be used to endorse or promote products derived
#        from this software without specific prior written permission.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
#  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
#  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
#  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
#  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
#  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# Usage:
# insert element: 'interactive_example.py --path path_to_db --insert key value'
# lookup element: 'interactive_example.py --path path_to_db --lookup key'
# print all elements: 'interactive_example.py --path path_to_db --iterate'

import pmemkv
import argparse

def callback_kv(key, value):
    print("key: ", memoryview(key).tobytes().decode(), " value: ", memoryview(value).tobytes().decode())

def callback_v(value):
    print(memoryview(value).tobytes().decode())

parser = argparse.ArgumentParser()
parser.add_argument('--path', help='Path to the database')

group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--lookup', nargs=1, metavar=('key'),
    help='Print element with specified key')
group.add_argument('--insert', nargs=2, metavar=('key', 'value'),
    help='Insert element with specified key and value')
group.add_argument('--iterate', action='store_true',
    help='Print all elements)

args = parser.parse_args()

print("Configuring engine")
config = {}
config["path"] = args.path
config["size"] = 10485760
config["create_if_missing"] = 1

print(f"Starting engine with config: {config}")
db = pmemkv.Database("cmap", config)

if args.lookup:
    db.get(args.lookup[0], callback_v)
elif args.insert:
    db.put(args.insert[0], args.insert[1])
elif args.iterate:
    db.get_all(callback_kv)

print("Stopping engine")
db.stop()
