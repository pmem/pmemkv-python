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

import pmemkv_NI

class KVEngine():

    stopped = False
    # Starts the given engine with the help of pmemkv_NI_Start API 
    # Takes engine name and configuration from the end user 
    def __init__(self, engine, config):
        retVal = pmemkv_NI.start(engine, config)
        if(retVal):
            raise ValueError(retVal)

    # Stops the running engine with the help of pmemkv_NI_Stop API
    def stop(self):
        if not self.stopped:
            self.stopped = True
            pmemkv_NI.stop()

    # Puts the key/value pair into pmemkv datastore with the help of pmemkv_NI_Put API
    # Takes key&value from the end user and returns the Put status (true/false) 
    def put(self, key, value):
        retVal = pmemkv_NI.put(key, value)
        if retVal < 0:
            raise RuntimeError("Unable to put " +str(key))
        return bool(retVal)

    # Gets the value for the given key from pmemkv datastore with the help of pmemkv_NI_Get API
    # Takes key from the end user and returns the value  
    def get(self, key):
        return pmemkv_NI.get(key)

    # Gets the value for the given key from pmemkv datastore with the help of pmemkv_NI_Get API
    # Takes key and encoding algorithm from the end user and returns the encoded value 
    def get_string(self, key, encoding = 'utf-8'):
        result = pmemkv_NI.get(key)
        return None if (result == None) else result.encode(encoding)

    # Fetches all the keys from pmemkv datastore with the help of pmemkv_NI_All API
    # Takes callback from the end user and sends back the resulted keys with the help 
    # of callback function
    def all(self, func):
        pmemkv_NI.all(func)

    # Fetches all the keys from the begining of the pmemkv datastore till key matched with the 
    # help of pmemkv_NI_AllAbove API 
    # Takes key and callback from the end user and sends back the resulted keys with the help 
    # of callback function
    def all_above(self, key, func):
        pmemkv_NI.all_above(key, func)

    # Fetches all the keys from the key matched in pmemkv datastore till end with the help of 
    # pmemkv_NI_AllBelow API 
    # Takes key and callback from the end user and sends back the resulted keys with the help 
    # of callback function
    def all_below(self, key, func):
        pmemkv_NI.all_below(key, func)

    # Fetches all the keys present, between key1 and key2 from pmemkv datastore with the help of 
    # pmemkv_NI_AllBetween API 
    # Takes key1, key2 and callback from the end user and sends back the resulted keys with 
    # the help of callback function
    def all_between(self, key1, key2, func):
        pmemkv_NI.all_between(key1, key2, func)

    # Fetches all the keys from pmemkv datastore with the help of pmemkv_NI_All API and encode them as per 
    # the given encoding algorithm 
    # Takes callback and encoding algorithm from the end user and sends back the resulted encoded keys 
    # with the help of callback function
    def all_strings(self, func, encoding = 'utf-8'):
        pmemkv_NI.all(lambda k: func(k.encode(encoding)))

    # Fetches all the keys from the begining of the pmemkv datastore till key matched with the 
    # help of pmemkv_NI_AllAbove API and encode them as per the given encoding algorithm 
    # Takes key, callback and encoding algorithm from the end user and sends back the resulted encoded keys 
    # with the help of callback function
    def all_strings_above(self, key, func, encoding = 'utf-8'):
        pmemkv_NI.all_above(key, lambda k: func(k.encode(encoding)))

    # Fetches all the keys from the key matched in the pmemkv datastore till end with the help of 
    # pmemkv_NI_AllBelow API and encode them as per the given encoding algorithm 
    # Takes key, callback and encoding algorithm from the end user and sends back the resulted encoded keys 
    # with the help of callback function
    def all_strings_below(self, key, func, encoding = 'utf-8'):
        pmemkv_NI.all_below(key, lambda k: func(k.encode(encoding)))

    # Fetches all the keys present, between key1 and key2 from pmemkv datastore with the help of 
    # pmemkv_NI_AllBetween API and encode them as per the given encoding algorithm 
    # Takes key1,key2,callback and encoding algorithm from the end user and sends back the resulted encoded keys 
    # with the help of callback function
    def all_strings_between(self, key1, key2, func, encoding = 'utf-8'):
        pmemkv_NI.all_between(key1, key2, lambda k: func(k.encode(encoding)))
        
    # Counts the total number of keys in the pmemkv datastore with the help of pmemkv_NI_Count API 
    # Returns the total number of keys to the end user
    def count(self):
        return pmemkv_NI.count()

    # Counts the total number of keys from the begining of the pmemkv datastore till key matched with the
    # help of pmemkv_NI_CountAbove API 
    # Takes key and returns the resulted keys count to the end user
    def count_above(self, key):
        return pmemkv_NI.count_above(key)

    # Counts the total number of keys from the key matched in the pmemkv datastore till end with the
    # help of pmemkv_NI_CountBelow API 
    # Takes key and returns the resulted keys count to the end user
    def count_below(self, key):
        return pmemkv_NI.count_below(key)

    # Counts the total number of keys present, between key1 and key2 from pmemkv datastore with the 
    # help of pmemkv_NI_CountBetween API 
    # Takes key1,key2 and returns the resulted keys count to the end user
    def count_between(self, key1, key2):
        return pmemkv_NI.count_between(key1, key2)

    # Fetches all the key/value pairs from pmemkv datastore with the help of pmemkv_NI_Each API 
    # Takes callback from the end user and sends back the resulted key/value pairs with the help 
    # of callback function
    def each(self, func):
        pmemkv_NI.each(func)

    # Fetches all the key/value pairs from the begining of the pmemkv datastore till key matched with the
    # help of pmemkv_NI_EachAbove API 
    # Takes key and callback from the end user and sends back the resulted key/value pairs with the help 
    # of callback function
    def each_above(self, key, func):
        pmemkv_NI.each_above(key, func)

    # Fetches all the key/value pairs from the key matched in the pmemkv datastore till end with the
    # help of pmemkv_NI_EachBelow API 
    # Takes key and callback from the end user and sends back the resulted key/value pairs with the help 
    # of callback function
    def each_below(self, key, func):
        pmemkv_NI.each_below(key, func)

    # Fetches all the key/value pairs present, between key1 and key2 from pmemkv datastore with the
    # help of pmemkv_NI_EachBetween API 
    # Takes key1,key2 and callback from the end user and sends back the resulted key/value pairs with the help 
    # of callback function
    def each_between(self, key1, key2, func):
        pmemkv_NI.each_between(key1, key2, func)

    # Fetches all the key/value pairs from pmemkv datastore with the help of pmemkv_NI_Each API and encode them as per 
    # the given encoding algorithm 
    # Takes callback and encoding algorithm from the end user and sends back the resulted encoded key/value pairs 
    # with the help of callback function
    def each_string(self, func, encoding = 'utf-8'):
        pmemkv_NI.each(lambda k, v: func(k.encode(encoding), v.encode(encoding)))

    # Fetches all the key/value pairs from the begining of the pmemkv datastore till key matched with the 
    # help of pmemkv_NI_EachAbove API and encode them as per the given encoding algorithm 
    # Takes key,callback and encoding algorithm from the end user and sends back the resulted encoded key/value pairs 
    # with the help of callback function
    def each_string_above(self, key, func, encoding = 'utf-8'):
        pmemkv_NI.each_above(key, lambda k, v: func(k.encode(encoding), v.encode(encoding)))

    # Fetches all the key/value pairs from the key matched in the pmemkv datastore till end with the 
    # help of pmemkv_NI_EachBelow API and encode them as per the given encoding algorithm 
    # Takes key,callback and encoding algorithm from the end user and sends back the resulted encoded key/value pairs 
    # with the help of callback function
    def each_string_below(self, key, func, encoding = 'utf-8'):
        pmemkv_NI.each_below(key, lambda k, v: func(k.encode(encoding), v.encode(encoding)))

    # Fetches all the key/value pairs present, between key1 and key2 from pmemkv datastore with the 
    # help of pmemkv_NI_EachBetween API and encode them as per the given encoding algorithm 
    # Takes key1,key2,callback and encoding algorithm from the end user and sends back the resulted encoded key/value pairs 
    # with the help of callback function
    def each_string_between(self, key1, key2, func, encoding = 'utf-8'):
        pmemkv_NI.each_between(key1, key2, lambda k, v: func(k.encode(encoding), v.encode(encoding)))

    # Verifies the key present in pmemkv datastore with the help of pmemkv_NI_Exists API
    # Takes key from the end user and returns the key existence(true/false)
    def exists(self, key):
        return bool(pmemkv_NI.exists(key))

    # Removes key from the pmemkv datastore with the help of pmemkv_NI_Remove API
    # Takes key from the end user and returns the key removable status(true/false) 
    def remove(self, key):
        returned = pmemkv_NI.remove(key)
        if returned < 0:
            raise RuntimeError("Unable to remove " +str(key))
        return bool(returned)
