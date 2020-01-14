#  Copyright 2019-2020, Intel Corporation
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

import pmemkv
import json


def callback(key):
    mem_view = memoryview(key)
    print(mem_view.tobytes().decode())


config = None
with open("vsmap_conf.json") as f:
    config = json.load(f)

print("Starting engine")
db = pmemkv.Database("vsmap", config)
print("Put new key")
db.put("key1", "value1")
assert db.count_all() == 1

print("Reading key back")
assert db.get_string("key1") == "value1"

print("Iterating existing keys")
db.put("key2", "value2")
db.put("key3", "value3")
db.get_keys(lambda k: print(f"visited: {memoryview(k).tobytes().decode()}"))

print("Get single value")
db.get("key1", callback)

print("Get single value and key in lambda expression")
key = "key1"
db.get(
    key,
    lambda v, k=key: print(
        f"key: {k} with value: " f"{memoryview(v).tobytes().decode()}"
    ),
)

print("Removing existing key")
db.remove("key1")
assert not db.exists("key1")

print("Stopping engine")
db.stop()
