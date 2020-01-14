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

""" Python bindings for pmemkv """

import _pmemkv
import json

class Database():

    def __init__(self, engine, config):
        if not isinstance(config, dict):
            raise TypeError("Config should be dictionary")
        self.config = json.dumps(config)
        self.db = _pmemkv.pmemkv_NI()
        self.db.start(engine, self.config)

    def __setitem__(self, key, value):
        self.put(key,value)

    def __getitem__(self, key):
        if key not in self:
            raise KeyError(key)
        return self.db.get_string(key)

    def __len__(self):
        return self.count_all()

    def __contains__(self, key):
        return self.exists(key)

    def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        self.remove(key)

    def stop(self):
        """ Stops the running engine."""
        self.db.stop()

    def put(self, key, value):
        """ Inserts the key/value pair into pmemkv datastore. """
        return self.db.put(key, value)

    def get_keys(self, func):
        """ Executes callback function for every key stored in
        pmemkv datastore.
        """
        return self.db.get_keys(func)

    def get_keys_above(self, key, func):
        """ Executes callback function for every key stored in
        pmemkv datastore, whose keys are greater than the given key.
        """
        return self.db.get_keys_above(key, func)

    def get_keys_below(self, key, func):
        """ Executes callback function for every key stored in
        pmemkv datastore, whose keys are lower than the given key.
        """
        return self.db.get_keys_below(key, func)

    def get_keys_between(self, key1, key2, func):
        """ Executes callback function for every key stored in pmemkv
        datastore, whose keys are greater than the key1 and less than the key2.
        """
        return self.db.get_keys_between(key1, key2, func)

    def count_all(self):
        """ Returns number of currently stored elements in pmemkv datastore."""
        return self.db.count_all()


    def count_above(self, key):
        """ Returns number of currently stored elements in pmemkv datastore,
        whose keys are greater than the given key
        """
        return self.db.count_above(key)


    def count_below(self, key):
        """ Returns number of currently stored elements in pmemkv datastore,
        whose keys are less than the given key.
        """
        return self.db.count_below(key)


    def count_between(self, key1, key2):
        """ Returns number of currently stored elements in pmemkv datastore,
        whose keys are greater than the key1 and less than the key2
        """
        return self.db.count_between(key1, key2)

    def get_all(self, func):
        """ Executes callback function for every record stored in pmemkv
        datastore.
        """
        return self.db.get_all(func)

    def get_above(self, key, func):
        """ Executes callback function for every key/value pair stored in
        pmemkv datastore, whose keys are greater than the given key.
        """
        return self.db.get_above(key, func)


    def get_below(self, key, func):
        """ Executes callback function for every key/value pair stored in
        pmemkv datastore, whose keys are lower than the given key.
        """
        return self.db.get_below(key, func)

    def get_between(self, key1, key2, func):
        """ Executes callback function for every key/value pair stored in
        pmemkv datastore, whose keys are greater than the key1 and less
        than the key2.
        """
        return self.db.get_between(key1, key2, func)

    def exists(self, key):
        """ Verifies the key presence in pmemkv datastore."""
        return self.db.exists(key)

    def get(self, key, func):
        """ Executes callback function for value for given key. """
        return self.db.get(key, func)

    def get_string(self, key):
        """ Gets copy (as a string) of value for given key """
        return self.db.get_string(key)

    def remove(self, key):
        """ Removes key/value pair from pmemkv datastore for given key."""
        return self.db.remove(key)
