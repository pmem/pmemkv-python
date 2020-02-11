'''
 * Copyright 2019-2020, Intel Corporation
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

import unittest

from pmemkv import Database
import pmemkv


class TestKVEngine(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = r"vsmap"
        self.config = {"path":"/dev/shm", "size":1073741824}
        self.key_and_value = r""
        self.formatter = r"{},"


    def all_and_each(self, key = b'', value = b''):
        value_mem_view = memoryview(value)
        key_mem_view = memoryview(key)
        value_text_representation = value_mem_view.tobytes().decode('utf-8')
        key_text_representation = key_mem_view.tobytes().decode('utf-8')
        self.key_and_value += self.formatter.format(key_text_representation,
                              value_text_representation)

    def all_and_each_strings(self, key = b'', value = b''):
        if value != b'':
            self.key_and_value += self.formatter.format(key.decode(), value.decode())
        else:
            self.key += self.formatter.format(key.decode())

    def test_uses_module_to_publish_type(self):
        import pmemkv
        self.assertEqual(Database.__class__, pmemkv.Database.__class__)

    def test_blackhole_engine(self):
        db = Database(r"blackhole", self.config)
        self.assertEqual(db.count_all(), 0)
        self.assertFalse(db.exists(r"key1"))
        with self.assertRaises(KeyError):
            db.get_string(r"key1")
        db.put(r"key1", r"value123")
        self.assertEqual(db.count_all(), 0)
        self.assertFalse(db.exists(r"key1"))
        with self.assertRaises(KeyError):
            db.get_string(r"key1")
        self.assertTrue(db.remove(r"key1"))
        self.assertFalse(db.exists(r"key1"))
        with self.assertRaises(KeyError):
            db.get_string(r"key1")
        db.stop()

    def test_stop_engine_multiple_times(self):
        """ In case of failure, this test cause segmentation fault.
        As there is no way to catch segmentation fault in python, just do not
        assert anything.
        """
        db = Database(self.engine, self.config)
        db.stop()
        db.stop()
        db.stop()

    def test_gets_missing_key(self):
        db = Database(self.engine, self.config)
        self.assertFalse(db.exists(r"key1"))
        with self.assertRaises(KeyError):
            db.get_string(r"key1")
        db.stop()

    def test_puts_basic_values(self):
        db = Database(self.engine, self.config)
        self.assertFalse(db.exists(r"key1"))
        db.put(r"key1", r"value1")
        self.assertTrue(db.exists(r"key1"))
        self.assertEqual(db.get_string(r"key1"), r"value1")
        db.stop()

    def test_puts_binary_keys(self):
        db = Database(self.engine, self.config)
        db.put("A\0B\0\0C", r"value1")
        self.assertTrue(db.exists("A\0B\0\0C"))
        self.assertEqual(db.get_string("A\0B\0\0C"), r"value1")
        db.stop()

    def test_puts_binary_values(self):
        db = Database(self.engine, self.config)
        db.put(r"key1", "A\0B\0\0C")
        self.assertEqual(db.get_string(r"key1"), "A\0B\0\0C")
        db.stop()

    def test_puts_complex_value(self):
        db = Database(self.engine, self.config)
        val = r"one\ttwo or <p>three</p>\n {four}   and ^five"
        db.put(r"key1", val)
        self.assertEqual(db.get_string(r"key1"), val)
        db.stop()

    def test_puts_empty_key(self):
        db = Database(self.engine, self.config)
        db.put(r"", r"empty")
        db.put(r" ", r"single-space")
        db.put(r"\t\t", r"two-tab")
        self.assertTrue(db.exists(r""))
        self.assertEqual(db.get_string(r""), r"empty")
        self.assertTrue(db.exists(r" "))
        self.assertEqual(db.get_string(r" "), r"single-space")
        self.assertTrue(db.exists(r"\t\t"))
        self.assertEqual(db.get_string(r"\t\t"), r"two-tab")
        db.stop()

    def test_puts_empty_values(self):
        db = Database(self.engine, self.config)
        db.put(r"empty", r"")
        db.put(r"single-space", r" ")
        db.put(r"two-tab", r"\t\t")
        self.assertEqual(db.get_string(r"empty"), r"")
        self.assertEqual(db.get_string(r"single-space"), r" ")
        self.assertEqual(db.get_string(r"two-tab"), r"\t\t")
        db.stop()

    def test_puts_multiple_values(self):
        db = Database(self.engine, self.config)
        db.put(r"key1", r"value1")
        db.put(r"key2", r"value2")
        db.put(r"key3", r"value3")
        self.assertTrue(db.exists(r"key1"))
        self.assertEqual(db.get_string(r"key1"), r"value1")
        self.assertTrue(db.exists(r"key2"))
        self.assertEqual(db.get_string(r"key2"), r"value2")
        self.assertTrue(db.exists(r"key3"))
        self.assertEqual(db.get_string(r"key3"), r"value3")
        db.stop()

    def test_puts_overwriting_existing_value(self):
        db = Database(self.engine, self.config)
        db.put(r"key1", r"value1")
        self.assertEqual(db.get_string(r"key1"), r"value1")
        db.put(r"key1", r"value123")
        self.assertEqual(db.get_string(r"key1"), r"value123")
        db.put(r"key1", r"asdf")
        self.assertEqual(db.get_string(r"key1"), r"asdf")
        db.stop()

    def test_puts_utf8_key(self):
        db = Database(self.engine, self.config)
        val = r"to remember, note, record"
        db.put(r"记", val)
        self.assertTrue(db.exists(r"记"))
        self.assertEqual(db.get_string(r"记"), val)
        db.stop()

    def test_puts_utf8_value(self):
        db = Database(self.engine, self.config)
        val = r"记 means to remember, note, record"
        db.put(r"key1", val)
        self.assertEqual(db.get_string(r"key1"), val)
        db.stop()

    def test_removes_key_and_value(self):
        db = Database(self.engine, self.config)
        db.put(r"key1", r"value1")
        self.assertTrue(db.exists(r"key1"))
        self.assertEqual(db.get_string(r"key1"), r"value1")
        self.assertTrue(db.remove(r"key1"))
        self.assertFalse(db.remove(r"key1"))
        self.assertFalse(db.exists(r"key1"))
        with self.assertRaises(KeyError):
            db.get_string(r"key1")
        db.stop()

    def test_exceptions_hierarchy(self):
        exceptions = [pmemkv.Error, pmemkv.UnknownError, pmemkv.NotSupported,
                  pmemkv.InvalidArgument, pmemkv.ConfigParsingError,
                  pmemkv.ConfigTypeError, pmemkv.StoppedByCallback,
                  pmemkv.WrongEngineName, pmemkv.TransactionScopeError]
        with self.assertRaises(Exception):
            raise(pmemkv.Error)
        for ex in exceptions:
            with self.assertRaises(pmemkv.Error):
                raise(ex)

    def test_throws_exception_on_start_when_config_is_empty(self):
        db = None
        with self.assertRaises(pmemkv.Error):
            db = Database(self.engine, {})
        """ InvalidArgument is for consistency with pmemkv interface
        reference in pmemkv test: basic_tests/PmemkvCApiTest.NullConfig
        """
        with self.assertRaises(pmemkv.InvalidArgument):
            db = Database(self.engine, {})


    def test_exception_on_start_when_config_is_wrong_type(self):
        db = None
        with self.assertRaises(TypeError):
            db = Database(self.engine, "{}")
        self.assertEqual(db, None)

    def test_throws_exception_on_start_when_engine_is_invalid(self):
        db = None
        with self.assertRaises(pmemkv.Error):
            db = Database(r"nope.nope", self.config)
        with self.assertRaises(pmemkv.WrongEngineName):
            db = Database(r"nope.nope", self.config)
        self.assertEqual(db, None)

    def test_throws_exception_on_start_when_path_is_invalid(self):
        db = None
        with self.assertRaises(pmemkv.Error):
            db = Database(self.engine, {"path":"/tmp/123/234/345/456/567/678/nope.nope", "size": 1073741824})
        """ This part need to be commented out due to pmemkv issue
            https://github.com/pmem/pmemkv/issues/565
        with self.assertRaises(pmemkv.InvalidArgument):
            db = Database(self.engine, {"path":"/tmp/123/234/345/456/567/678/nope.nope", "size": 1073741824})
        """
        self.assertEqual(db, None)

    def test_throws_exception_on_start_when_path_is_wrong_type(self):
        db = None
        with self.assertRaises(pmemkv.Error):
            db = Database(self.engine, {"path":1234, "size": 1073741824})
        with self.assertRaises(pmemkv.ConfigTypeError):
            db = Database(self.engine, {"path":1234, "size": 1073741824})
        self.assertEqual(db, None)

    def test_uses_get_keys(self):
        db = Database(self.engine, self.config)
        db.put(r"1", r"one")
        db.put(r"2", r"two")

        self.formatter = r"<{}>,"

        self.key = r""
        db.get_keys(self.all_and_each)
        self.assertEqual(self.key_and_value, r"<1>,<2>,")

        db.stop()

    def test_uses_get_keys_above(self):
        db = Database(self.engine, self.config)
        db.put(r"A", r"1")
        db.put(r"AB", r"2")
        db.put(r"AC", r"3")
        db.put(r"B", r"4")
        db.put(r"BB", r"5")
        db.put(r"BC", r"6")

        self.formatter = r"{},"

        self.key = r""
        db.get_keys_above(r"B", self.all_and_each)
        self.assertEqual(self.key_and_value, r"BB,BC,")

        db.stop()

    def test_uses_get_keys_below(self):
        db = Database(self.engine, self.config)
        db.put(r"A", r"1")
        db.put(r"AB", r"2")
        db.put(r"AC", r"3")
        db.put(r"B", r"4")
        db.put(r"BB", r"5")
        db.put(r"BC", r"6")

        self.formatter = r"{},"

        self.key_and_value = r""
        db.get_keys_below(r"B", self.all_and_each)
        self.assertEqual(self.key_and_value, r"A,AB,AC,")

        db.stop()

    def test_uses_get_keys_between(self):
        db = Database(self.engine, self.config)
        db.put(r"A", r"1")
        db.put(r"AB", r"2")
        db.put(r"AC", r"3")
        db.put(r"B", r"4")
        db.put(r"BB", r"5")
        db.put(r"BC", r"6")

        self.formatter = r"{},"

        self.key_and_value = r""
        db.get_keys_between(r"A", r"B", self.all_and_each)
        self.assertEqual(self.key_and_value, r"AB,AC,")

        self.key_and_value = r""
        db.get_keys_between(r"", r"", self.all_and_each)
        db.get_keys_between(r"A", r"A", self.all_and_each)
        db.get_keys_between(r"B", r"A", self.all_and_each)
        self.assertEqual(self.key_and_value, r"")

        db.stop()

    def test_uses_count_all(self):
        db = Database(self.engine, self.config)
        db.put(r"A", r"1")
        db.put(r"AB", r"2")
        db.put(r"AC", r"3")
        db.put(r"B", r"4")
        db.put(r"BB", r"5")
        db.put(r"BC", r"6")
        db.put(r"BD", r"7")
        self.assertEqual(db.count_all(), 7)

        self.assertEqual(db.count_above(r""), 7)
        self.assertEqual(db.count_above(r"A"), 6)
        self.assertEqual(db.count_above(r"B"), 3)
        self.assertEqual(db.count_above(r"BC"), 1)
        self.assertEqual(db.count_above(r"BD"), 0)
        self.assertEqual(db.count_above(r"Z"), 0)

        self.assertEqual(db.count_below(r""), 0)
        self.assertEqual(db.count_below(r"A"), 0)
        self.assertEqual(db.count_below(r"B"), 3)
        self.assertEqual(db.count_below(r"BD"), 6)
        self.assertEqual(db.count_below(r"ZZZZZ"), 7)

        self.assertEqual(db.count_between(r"", r"ZZZZZ"), 7)
        self.assertEqual(db.count_between(r"", r"A"), 0)
        self.assertEqual(db.count_between(r"", r"B"), 3)
        self.assertEqual(db.count_between(r"A", r"B"), 2)
        self.assertEqual(db.count_between(r"B", r"ZZZZZ"), 3)

        self.assertEqual(db.count_between(r"", r""), 0)
        self.assertEqual(db.count_between(r"A", r"A"), 0)
        self.assertEqual(db.count_between(r"AC", r"A"), 0)
        self.assertEqual(db.count_between(r"B", r"A"), 0)
        self.assertEqual(db.count_between(r"BD", r"A"), 0)
        self.assertEqual(db.count_between(r"ZZZ", r"B"), 0)

        db.stop()

    def test_uses_get_all(self):
        db = Database(self.engine, self.config)
        db.put(r"1", r"one")
        db.put(r"2", r"two")

        self.formatter = r"<{}>,<{}>|"

        self.key_and_value = r""
        db.get_all(self.all_and_each)
        self.assertEqual(self.key_and_value, r"<1>,<one>|<2>,<two>|")


        db.stop()

    def test_uses_get_above(self):
        db = Database(self.engine, self.config)
        db.put(r"A", "1")
        db.put(r"AB", r"2")
        db.put(r"AC", r"3")
        db.put(r"B", r"4")
        db.put(r"BB", r"5")
        db.put(r"BC", r"6")

        self.formatter = r"{},{}|"

        self.key_and_value = r""
        db.get_above(r"B", self.all_and_each)
        self.assertEqual(self.key_and_value, r"BB,5|BC,6|")


        db.stop()

    def test_uses_get_below(self):
        db = Database(self.engine, self.config)
        db.put(r"A", r"1")
        db.put(r"AB", r"2")
        db.put(r"AC", r"3")
        db.put(r"B", r"4")
        db.put(r"BB", r"5")
        db.put(r"BC", r"6")

        self.formatter = r"{},{}|"

        self.key_and_value = r""
        db.get_below(r"AC", self.all_and_each)
        self.assertEqual(self.key_and_value, r"A,1|AB,2|")

        db.put(r"记!", r"RR")

        db.stop()

    def test_each_between(self):
        db = Database(self.engine, self.config)
        db.put(r"A", r"1")
        db.put(r"AB", r"2")
        db.put(r"AC", r"3")
        db.put(r"B", r"4")
        db.put(r"BB", r"5")
        db.put(r"BC", r"6")

        self.formatter = r"{},{}|"

        self.key_and_value = r""
        db.get_between(r"A", r"B", self.all_and_each)
        self.assertEqual(self.key_and_value, r"AB,2|AC,3|")



        db.stop()

    def test_dict_set_item(self):
        db = Database(self.engine, self.config)
        db['string_value'] = "test"
        self.assertEqual(db['string_value'], "test")
        db.stop()

    def test_dict_get_item(self):
        db = Database(self.engine, self.config)
        key = "dict_test"
        db[key] = "123"
        temp = db[key]
        self.assertEqual(temp, "123")
        db.stop()

    def test_get_copy_to_class_member(self):
        class Callback:
            def __init__(self):
                self.result = None
            def __call__(self, key):
                self.result = memoryview(key)
        callback = Callback()
        db = Database(self.engine, self.config)
        key = "dict_test"
        val = "123"
        db[key] = val
        db.get(key, callback)
        self.assertEqual(callback.result.tobytes(), "123".encode('utf-8'))
        db.stop()

    def test_get_assert_in_callback(self):
        def callback (key):
            self.assertEqual(memoryview(key).tobytes(), "123".encode('utf-8'))
        key = "dict_test"
        val = "123"
        db = Database(self.engine, self.config)
        db[key] = val
        db.get(key, callback)
        db.stop()

    def test_get_exception_in_callback(self):
        class LocalException(Exception):
            pass
        def callback(key):
            raise LocalException('TestException')
        db = Database(self.engine, self.config)
        key = "dict_test"
        val = "123"
        db[key] = val
        try:
            db.get(key, callback)
        except LocalException as e:
            db.stop()
            self.assertEqual(type(e).__name__ , "LocalException")
        db.stop()

    def test_get_AttributeError_in_callback(self):
        def callback (key):
            self.assertEqual(key.NonexistentMethod(), "123".encode('utf-8'))
        key = "dict_test"
        val = "123"
        db = Database(self.engine, self.config)
        db[key] = val
        try:
            db.get(key, callback)
        except Exception as e:
            assert type(e).__name__ == "AttributeError"
        db.stop()

    def test_get_out_of_bound_access_in_callback(self):
        key = "dict_test"
        val = "123"
        db = Database(self.engine, self.config)
        db[key] = val
        with self.assertRaises(IndexError):
            db.get(key, lambda v: memoryview(v).tobytes()[4])
        with self.assertRaises(IndexError):
            db.get(key, lambda v: memoryview(v)[4])
        db.stop()

    def test_get_lambda_in_callback(self):
        key = "dict_test"
        val = "123"
        db = Database(self.engine, self.config)
        db[key] = val

        db.get(key, lambda v, k=key: self.assertEqual(memoryview(v).tobytes(),
                                             "123".encode('utf-8')))
        db.get(key, lambda v, k=key: self.assertEqual(k, "dict_test"))
        db.stop()

    def test_get_with_value_indexing(self):
        key = "dict_test"
        val = "123"
        db = Database(self.engine, self.config)
        db[key] = val

        db.get(key, lambda v, k=key: self.assertEqual((memoryview(v)[0:2])
                                             .tobytes(),
                                             "12".encode('utf-8')))
        db.stop()

    def test_dict_len(self):
        db = Database(self.engine, self.config)
        db['dict_test'] = "123"
        db['Aa'] = "42"
        self.assertEqual(len(db), 2)
        db.stop()

    def test_dict_item_in(self):
        db = Database(self.engine, self.config)
        db['dict_test'] = "123"
        self.assertIn('dict_test', db)
        self.assertNotIn('Aa', db)
        db.stop()

    def test_dict_item_del(self):
        db = Database(self.engine, self.config)
        db['dict_test'] = "123"
        del db['dict_test']
        with self.assertRaises(KeyError):
            temp = db['dict_test']
        db.stop()

    def test_databases_interference(self):
        db1 = Database(self.engine, self.config)
        db2 = Database(self.engine, self.config)
        db1['1'] = "A"
        db2['2'] = "B"
        with self.assertRaises(KeyError):
            temp = db2['1']
        db1.stop()
        db2.stop()

    def test_get_same_element_two_times(self):
        db = Database(self.engine, self.config)
        db['dict_test'] = "123"
        val1 = db['dict_test']
        val2 = db['dict_test']
        self.assertEqual(val1, val2)
        db.stop()

    def test_delete_same_element_two_times(self):
        db = Database(self.engine, self.config)
        db['dict_test'] = "123"
        del db['dict_test']
        with self.assertRaises(KeyError):
            del db['dict_test']
        db.stop()

    def test_call_del_inside_callback(self):
        def callback(val):
            del(val)
            # check if buffer protocol object was properly removed
            with self.assertRaises(UnboundLocalError):
                del(val)
        key = "dict_test"
        val = "123"
        db = Database(self.engine, self.config)
        db[key] = val
        db.get(key, callback)
        # check if key is accessable
        self.assertEqual(db[key], val)
        db.stop()

    def test_context_manager(self):
        key = "dict_test"
        val = "123"
        with Database(self.engine, self.config) as db:
            db[key] = val
            self.assertEqual(db[key], val)

    def test_throws_exception_in_context_manager(self):
        class TestException(Exception):
            pass
        def callback(val):
            raise TestException()
        key = "dict_test"
        val = "123"
        with Database(self.engine, self.config) as db:
            db[key] = val
            with self.assertRaises(TestException):
                db.get(key, callback)
        with self.assertRaises(TestException):
            with Database(self.engine, self.config) as db:
                db[key] = val
                db.get(key, callback)

if __name__ == '__main__':
    unittest.main()
