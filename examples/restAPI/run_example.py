#!/usr/bin/env python3

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

""" CI integration script for pmemkvREST example. """

import pexpect
import os
import sys
import requests

class RestApiRunner:
    def __init__(self, pool_path):
        self.server = None
        self.pool_path = pool_path
        self.script_dir = os.path.dirname(os.path.realpath(__file__))
        self.python_path = sys.executable

    def run(self):
        self.server = pexpect.spawn(f"{self.python_path} pmemkvREST.py", env={"PMEMKV_POOL_PATH" : self.pool_path}, cwd=self.script_dir)
        self.server.expect("Starting server")

    def stop(self):
        self.server.close()

def create_pool(pool_path, pool_size):
    cmd = f"pmempool create -s {pool_size} -l pmemkv obj {pool_path}"
    if not os.path.exists(pool_path):
        out, exit_status = pexpect.run(cmd, withexitstatus=True)
        if exit_status !=  0:
           raise Exception(f"Cannot create pool: {out}")


if __name__ == "__main__":
    default_pool_size = 1024 * 1024 *1024
    default_pool_path = "/dev/shm/pmemkvREST_pool"
    pool_path = os.environ.get("PMEMKV_POOL_PATH") or default_pool_path
    pool_size = os.environ.get("POOL_SIZE") or default_pool_size

    create_pool(pool_path, pool_size)
    server = RestApiRunner(pool_path)
    print("Run server with pmemkv concurrent hash map as storage")
    server.run()
    print("Put data into database")
    requests.put("http://localhost:8000/db", json={"message": "Hello Data"})
    print("Get list of all elements")
    all_keys = requests.get("http://localhost:8000/db")
    print(f"{'': <8}{all_keys}, {all_keys.content}")
    print("Read back data:")
    response1 = requests.get("http://localhost:8000/db/message")
    print(f"{'': <8}{response1}, {response1.content}")
    print("Shutdown server")
    server.stop()
    print("Rerun server")
    server.run()
    print("Read data written in previous session:")
    response2 = requests.get("http://localhost:8000/db/message")
    print(f"{'': <8}{response2}, {response2.content}")
    print("Remove data")
    response2 = requests.delete("http://localhost:8000/db/message")
    print("Data was removed:")
    response3 = requests.get("http://localhost:8000/db/message")
    print(f"{'': <8}{response3}, {response3.content}")
    print("Shutdown server")
    server.stop()
