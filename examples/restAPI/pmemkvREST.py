import pmemkv

import falcon

import atexit
import os
import json

class Pmemkv():
    def __init__(self):
        print( f"PATH: {os.environ['TEST_PATH']}")
        try:
            config = {"path" : os.environ['TEST_PATH']}
        except KeyError:
            print("Please configure path")
            exit(1)
        print("Open database")
        try:
            self.db = pmemkv.Database("cmap", config)
        except pmemkv.Error as e:
            print(f"Cannot open database: {e}")
            exit(1)

    def teardown(self):
        print("Close database")
        self.db.stop()

class ListResource:
    def __init__(self, storage):
        self.db = storage.db
        self.response = []

    def response_callback(self, key):
        self.response.append(memoryview(key).tobytes().decode())

    def on_get(self, req, resp):
        self.db.get_keys(self.response_callback)
        resp.media = self.response
        self.response = []

    def on_put(self, req, resp):
        doc = json.load(req.bounded_stream)
        for key in doc:
            self.db[key] = doc[key]

class ElementResource:
    def __init__(self, storage):
        self.db = storage.db

    def on_get(self, req, resp, key):
        try:
            resp.media = self.db[key]
        except(KeyError):
            resp.status = falcon.HTTP_NOT_FOUND

    def on_delete(self, req, resp, key):
        try:
            self.db.remove(key)
        except:
            resp.status = falcon.HTTP_NOT_FOUND


pm = Pmemkv()
atexit.register(pm.teardown)
app = falcon.API()
app.add_route('/db', ListResource(pm))
app.add_route('/db/{key}', ElementResource(pm))
