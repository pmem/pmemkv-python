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

""" This example shows how to use pmemkv-python binding as data store backend for
simple REST API service based on falcon framework  """

import pmemkv

import falcon

import atexit
import os
import json

from wsgiref import simple_server

class Pmemkv():
    def __init__(self):
        print( f"PATH: {os.environ['PMEMKV_POOL_PATH']}")
        try:
            config = {"path" : os.environ['PMEMKV_POOL_PATH']}
        except KeyError:
            print("To configure path please set PMEMKV_POOL_PATH environment variable")
            exit(1)
        print("Open datastore")
        try:
            """ Open pmemkv datastore with parameters passed by config dictionary,
            and concurrent hash map as storage engine.
            """
            self.db = pmemkv.Database("cmap", config)
        except pmemkv.Error as e:
            print(f"Cannot open datastore: {e}")
            exit(1)

    def teardown(self):
        print("Close datastore")
        """ When access is no more needed, storage engine should be closed."""
        self.db.stop()

class ListResource:
    def __init__(self, storage):
        self.db = storage.db
        self.response = []

    def on_get(self, req, resp):
        print(req)
        """
        Pass callback function, which is run on every key in datastore.
        Function have to accept key parameter.
        Key is Buffer Object. It's possible to directly access it's memory
        through read-only memoryview() interface, or copy to volatile memory through
        bytes().
        To provide data to higher layer of framework, it have to be copied to
        object in volatile memory. It's needed as access to key object outside
        of callback function may cause application crashes
        """
        resp.media = []
        self.db.get_keys(lambda key: resp.media.append(bytes(key).decode()))

    def on_put(self, req, resp):
        print(req)
        doc = json.load(req.bounded_stream)
        """ Save data to datastore using dictionary interface """
        for key in doc:
            self.db[key] = doc[key]

class ElementResource:
    def __init__(self, storage):
        self.db = storage.db

    def on_get(self, req, resp, key):
        print(req)
        """ For use cases, where zero-copy property is not needed,
        access to pmemkv may be done using dictionary interface
        """
        try:
            resp.media = self.db[key]
        except(KeyError):
            resp.status = falcon.HTTP_NOT_FOUND

    def on_delete(self, req, resp, key):
        print(req)
        """ Remove element by key """
        try:
            self.db.remove(key)
        except:
            resp.status = falcon.HTTP_NOT_FOUND

def main():
    pm = Pmemkv()
    atexit.register(pm.teardown)
    app = falcon.API()
    app.add_route('/db', ListResource(pm))
    app.add_route('/db/{key}', ElementResource(pm))

    httpd = simple_server.make_server('0.0.0.0', 8000, app)
    print("Starting server")
    httpd.serve_forever()

if __name__ == '__main__':
    main()

