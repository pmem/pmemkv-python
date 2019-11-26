'''
 * Copyright 2019, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
'''

import _pmemkv

PMEMKV_STATUS_OK = 0
PMEMKV_STATUS_UNKNOWN_ERROR = 1
PMEMKV_STATUS_NOT_FOUND = 2
PMEMKV_STATUS_NOT_SUPPORTED = 3
PMEMKV_STATUS_INVALID_ARGUMENT = 4
PMEMKV_STATUS_CONFIG_PARSING_ERROR = 5
PMEMKV_STATUS_CONFIG_TYPE_ERROR = 6
PMEMKV_STATUS_STOPPED_BY_CB = 7
PMEMKV_STATUS_OUT_OF_MEMORY = 8
PMEMKV_STATUS_WRONG_ENGINE_NAME = 9
PMEMKV_STATUS_TRANSACTION_SCOPE_ERROR = 10


class Database():

    # Starts the given engine.
    # Takes engine name and configuration from the end user.
    def __init__(self, engine, config):
        self.db = _pmemkv.pmemkv_NI()
        self.db.start(engine, config)

    def __setitem__(self, key, value):
        self.put(key,value)

    def __getitem__(self, key):
        item = self.get(key)
        if not item:
            raise KeyError(key)
        return item

    def __len__(self):
        return self.count_all()

    def __contains__(self, key):
        return self.exists(key)

    def __delitem__(self, key):
        self.remove(key)

    # Stops the running engine.
    def stop(self):
        self.db.stop()

    # Puts the key/value pair into pmemkv datastore.
    # Takes key & value from the end user.
    def put(self, key, value):
        return self.db.put(key, value)

    # Fetches all the keys from pmemkv datastore.
    # Takes callback from the end user and sends the resulted keys through callback.
    def get_keys(self, func):
        return self.db.get_keys(func)

    # Fetches all the keys from the begining of the pmemkv datastore till key matched.
    # Takes key and callback from the end user and sends the resulted keys through callback.
    def get_keys_above(self, key, func):
        return self.db.get_keys_above(key, func)

    # Fetches all the keys from the key matched in pmemkv datastore till end.
    # Takes key and callback from the end user and sends the resulted keys through callback.
    def get_keys_below(self, key, func):
        return self.db.get_keys_below(key, func)

    # Fetches all the keys present, between key1 and key2 from pmemkv datastore.
    # Takes key1, key2 and callback from the end user and sends the resulted keys through callback.
    def get_keys_between(self, key1, key2, func):
        return self.db.get_keys_between(key1, key2, func)

    # Fetches all the keys from the begining of the pmemkv datastore till key matched and encodes them.
    # Takes key, callback and encoding algorithm from the end user and sends the resulted encoded keys through callback.
    def get_keys_strings(self, func, encoding = 'utf-8'):
        return self.db.get_keys(lambda k: func(k.encode(encoding)))

    # Fetches all the keys from the begining of the pmemkv datastore till key matched and encodes them.
    # Takes key, callback and encoding algorithm from the end user and sends the resulted encoded keys through callback.
    def get_keys_strings_above(self, key, func, encoding = 'utf-8'):
        return self.db.get_keys_above(key, lambda k: func(k.encode(encoding)))

    # Fetches all the keys from the key matched in the pmemkv datastore till end and encodes them.
    # Takes key, callback and encoding algorithm from the end user and sends the resulted encoded keys through callback.
    def get_keys_strings_below(self, key, func, encoding = 'utf-8'):
        return self.db.get_keys_below(key, lambda k: func(k.encode(encoding)))

    # Fetches all the keys present, between key1 and key2 from pmemkv datastore and encodes them.
    # Takes key1, key2, callback and encoding algorithm from the end user and sends the resulted encoded keys through callback.
    def get_keys_strings_between(self, key1, key2, func, encoding = 'utf-8'):
        return self.db.get_keys_between(key1, key2, lambda k: func(k.encode(encoding)))

    # Counts the total number of keys in the pmemkv datastore.
    # Returns total number of keys.
    def count_all(self):
        return self.db.count_all()

    # Counts the total number of keys from the begining of the pmemkv datastore till key matched.
    # Takes key from the end user, returns number of resulted keys.
    def count_above(self, key):
        return self.db.count_above(key)

    # Counts the total number of keys from the key matched in the pmemkv datastore till end.
    # Takes key from the end user, returns number of resulted keys.
    def count_below(self, key):
        return self.db.count_below(key)

    # Counts the total number of keys present, between key1 and key2 from pmemkv datastore.
    # Takes key1 and key2 from the end user, returns number of resulted keys.
    def count_between(self, key1, key2):
        return self.db.count_between(key1, key2)

    # Fetches all the key/value pairs from pmemkv datastore.
    # Takes callback from the end user and sends the resulted key/value pairs through callback.
    def get_all(self, func):
        return self.db.get_all(func)

    # Fetches all the key/value pairs from the begining of the pmemkv datastore till key matched.
    # Takes key and callback from the end user and sends the resulted key/value pairs through callback.
    def get_above(self, key, func):
        return self.db.get_above(key, func)


    # Fetches all the key/value pairs from the key matched in the pmemkv datastore till end.
    # Takes key and callback from the end user and sends the resulted key/value pairs through callback.
    def get_below(self, key, func):
        return self.db.get_below(key, func)

    # Fetches all the key/value pairs present, between key1 and key2 from pmemkv datastore.
    # Takes key1, key2 and callback from the end user and sends the resulted key/value pairs through callback.
    def get_between(self, key1, key2, func):
        return self.db.get_between(key1, key2, func)

    # Fetches all the key/value pairs from pmemkv datastore and encodes them.
    # Takes callback and encoding algorithm from the end user and sends the resulted encoded key/value pairs through callback.
    def get_all_string(self, func, encoding = 'utf-8'):
        return self.db.get_all(lambda k, v: func(k.encode(encoding), v.encode(encoding)))

    # Fetches all the key/value pairs from the begining of the pmemkv datastore till key matched and encodes them.
    # Takes key, callback and encoding algorithm from the end user and sends the resulted encoded key/value pairs through callback.
    def get_string_above(self, key, func, encoding = 'utf-8'):
        return self.db.get_above(key, lambda k, v: func(k.encode(encoding), v.encode(encoding)))

    # Fetches all the key/value pairs from the key matched in the pmemkv datastore till end and encodes them.
    # Takes key, callback and encoding algorithm from the end user and sends the resulted encoded key/value pairs through callback.
    def get_string_below(self, key, func, encoding = 'utf-8'):
        return self.db.get_below(key, lambda k, v: func(k.encode(encoding), v.encode(encoding)))

    # Fetches all the key/value pairs present, between key1 and key2 from pmemkv datastore and encodes them.
    # Takes key1, key2, callback and encoding algorithm from the end user and sends the resulted encoded key/value pairs through callback.
    def get_string_between(self, key1, key2, func, encoding = 'utf-8'):
        return self.db.get_between(key1, key2, lambda k, v: func(k.encode(encoding), v.encode(encoding)))

    # Verifies the key presence in pmemkv datastore.
    # Takes key from the end user and returns the key presence.
    def exists(self, key):
        return self.db.exists(key)
    
    # Gets the value for the given key from pmemkv datastore.
    # Takes key from the end user and returns the value.
    def get(self, key):
        return self.db.get(key)

    # Gets the value for the given key from pmemkv datastore.
    # Takes key and encoding algorithm from the end user and returns the encoded value.
    def get_string(self, key, encoding = 'utf-8'):
        value = self.db.get(key)
        return None if (value == None) else value.encode(encoding)

    # Takes key from the end user and returns the key removal status.
    def remove(self, key):
        return self.db.remove(key)
