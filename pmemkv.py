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

    def __init__(self, engine_name, config):
        retVal = pmemkv_NI.start(engine_name, config)
        if(retVal):
            raise ValueError(retVal)

    def stop(self):
        if not self.stopped:
            self.stopped = True
            pmemkv_NI.stop()

    def put(self, key, value):
        returned = pmemkv_NI.put(key, value)
        if returned < 0:
            raise RuntimeError("Unable to put " +str(key))
        return bool(returned)

    def get(self, key):
        return pmemkv_NI.get(key)

    def get_string(self, key, encoding = 'utf-8'):
        result = pmemkv_NI.get(key)
        return None if (result == None) else result.encode(encoding)
    
    def all(self, func):
        pmemkv_NI.all(func)

    def all_above(self, key, func):
        pmemkv_NI.all_above(key, func)

    def all_below(self, key, func):
        pmemkv_NI.all_below(key, func)

    def all_between(self, key1, key2, func):
        pmemkv_NI.all_between(key1, key2, func)

    def all_strings(self, func, encoding = 'utf-8'):
        pmemkv_NI.all(lambda k: func(k.encode(encoding)))

    def all_strings_above(self, key, func, encoding = 'utf-8'):
        pmemkv_NI.all_above(key, lambda k: func(k.encode(encoding)))

    def all_strings_below(self, key, func, encoding = 'utf-8'):
        pmemkv_NI.all_below(key, lambda k: func(k.encode(encoding)))

    def all_strings_between(self, key1, key2, func, encoding = 'utf-8'):
        pmemkv_NI.all_between(key1, key2, lambda k: func(k.encode(encoding)))
        
    def count(self):
        return pmemkv_NI.count()

    def count_above(self, key):
        return pmemkv_NI.count_above(key)

    def count_below(self, key):
        return pmemkv_NI.count_below(key)

    def count_between(self, key1, key2):
        return pmemkv_NI.count_between(key1, key2)

    def each(self, func):
        pmemkv_NI.each(func)

    def each_above(self, key, func):
        pmemkv_NI.each_above(key, func)

    def each_below(self, key, func):
        pmemkv_NI.each_below(key, func)

    def each_between(self, key1, key2, func):
        pmemkv_NI.each_between(key1, key2, func)

    def each_string(self, func, encoding = 'utf-8'):
        pmemkv_NI.each(lambda k, v: func(k.encode(encoding), v.encode(encoding)))

    def each_string_above(self, key, func, encoding = 'utf-8'):
        pmemkv_NI.each_above(key, lambda k, v: func(k.encode(encoding), v.encode(encoding)))

    def each_string_below(self, key, func, encoding = 'utf-8'):
        pmemkv_NI.each_below(key, lambda k, v: func(k.encode(encoding), v.encode(encoding)))

    def each_string_between(self, key1, key2, func, encoding = 'utf-8'):
        pmemkv_NI.each_between(key1, key2, lambda k, v: func(k.encode(encoding), v.encode(encoding)))

    def exists(self, key):
        return bool(pmemkv_NI.exists(key))

    def remove(self, key):
        returned = pmemkv_NI.remove(key)
        if returned < 0:
            raise RuntimeError("Unable to remove " +str(key))
        return bool(returned)
