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

import unittest
from pmemkv import Database

class TestKVEngine(unittest.TestCase):

    engine = r"vsmap"
    config = "{\"path\":\"/dev/shm\",\"size\":1073741824}"

    key = r""
    key_and_value = r""
    formatter = r"{},"

    def all_and_each(self, key = '', value = ''):
        if value != '':
            self.key_and_value += self.formatter.format(key, value)
        else:
            self.key += self.formatter.format(key)

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
        self.assertEqual(db.get(r"key1"), None)
        db.put(r"key1", r"value123")
        self.assertEqual(db.count_all(), 0)
        self.assertFalse(db.exists(r"key1"))
        self.assertEqual(db.get(r"key1"), None)
        self.assertTrue(db.remove(r"key1"))
        self.assertFalse(db.exists(r"key1"))
        self.assertEqual(db.get(r"key1"), None)
        db.stop()

    def test_start_engine(self):
        db = Database(self.engine, self.config)
        self.assertNotEqual(db, None)
        self.assertFalse(db.stopped)
        db.stop()
        self.assertTrue(db.stopped)

    def test_stops_engine_multiple_times(self):
        db = Database(self.engine, self.config)
        self.assertFalse(db.stopped)
        db.stop()
        self.assertTrue(db.stopped)
        db.stop()
        self.assertTrue(db.stopped)
        db.stop()
        self.assertTrue(db.stopped)

    def test_gets_missing_key(self):
        db = Database(self.engine, self.config)
        self.assertFalse(db.exists(r"key1"))
        self.assertEqual(db.get(r"key1"), None)
        db.stop()

    def test_puts_basic_values(self):
        db = Database(self.engine, self.config)
        self.assertFalse(db.exists(r"key1"))
        db.put(r"key1", r"value1")
        self.assertTrue(db.exists(r"key1"))
        self.assertEqual(db.get(r"key1"), r"value1")
        db.stop()

    def test_puts_binary_keys(self):
        db = Database(self.engine, self.config)
        db.put("A\0B\0\0C", r"value1")
        self.assertTrue(db.exists("A\0B\0\0C"))
        self.assertEqual(db.get("A\0B\0\0C"), r"value1")
        db.stop()

    def test_puts_binary_values(self):
        db = Database(self.engine, self.config)
        db.put(r"key1", "A\0B\0\0C")
        self.assertEqual(db.get(r"key1"), "A\0B\0\0C")
        db.stop()

    def test_puts_complex_value(self):
        db = Database(self.engine, self.config)
        val = r"one\ttwo or <p>three</p>\n {four}   and ^five"
        db.put(r"key1", val)
        self.assertEqual(db.get(r"key1"), val)
        db.stop()

    def test_puts_empty_key(self):
        db = Database(self.engine, self.config)
        db.put(r"", r"empty")
        db.put(r" ", r"single-space")
        db.put(r"\t\t", r"two-tab")
        self.assertTrue(db.exists(r""))
        self.assertEqual(db.get(r""), r"empty")
        self.assertTrue(db.exists(r" "))
        self.assertEqual(db.get(r" "), r"single-space")
        self.assertTrue(db.exists(r"\t\t"))
        self.assertEqual(db.get(r"\t\t"), r"two-tab")
        db.stop()

    def test_puts_empty_values(self):
        db = Database(self.engine, self.config)
        db.put(r"empty", r"")
        db.put(r"single-space", r" ")
        db.put(r"two-tab", r"\t\t")
        self.assertEqual(db.get(r"empty"), r"")
        self.assertEqual(db.get(r"single-space"), r" ")
        self.assertEqual(db.get(r"two-tab"), r"\t\t")
        db.stop()

    def test_puts_multiple_values(self):
        db = Database(self.engine, self.config)
        db.put(r"key1", r"value1")
        db.put(r"key2", r"value2")
        db.put(r"key3", r"value3")
        self.assertTrue(db.exists(r"key1"))
        self.assertEqual(db.get(r"key1"), r"value1")
        self.assertTrue(db.exists(r"key2"))
        self.assertEqual(db.get(r"key2"), r"value2")
        self.assertTrue(db.exists(r"key3"))
        self.assertEqual(db.get(r"key3"), r"value3")
        db.stop()

    def test_puts_overwriting_existing_value(self):
        db = Database(self.engine, self.config)
        db.put(r"key1", r"value1")
        self.assertEqual(db.get(r"key1"), r"value1")
        db.put(r"key1", r"value123")
        self.assertEqual(db.get(r"key1"), r"value123")
        db.put(r"key1", r"asdf")
        self.assertEqual(db.get(r"key1"), r"asdf")
        db.stop()

    def test_puts_utf8_key(self):
        db = Database(self.engine, self.config)
        val = r"to remember, note, record"
        db.put(r"记", val)
        self.assertTrue(db.exists(r"记"))
        self.assertEqual(db.get(r"记"), val)
        db.stop()

    def test_puts_utf8_value(self):
        db = Database(self.engine, self.config)
        val = r"记 means to remember, note, record"
        db.put(r"key1", val)
        self.assertEqual(db.get_string(r"key1").decode(), val)
        db.stop()

    def test_removes_key_and_value(self):
        db = Database(self.engine, self.config)
        db.put(r"key1", r"value1")
        self.assertTrue(db.exists(r"key1"))
        self.assertEqual(db.get(r"key1"), r"value1")
        self.assertTrue(db.remove(r"key1"))
        self.assertFalse(db.remove(r"key1"))
        self.assertFalse(db.exists(r"key1"))
        self.assertEqual(db.get(r"key1"), None)
        db.stop()

    def test_throws_exception_on_start_when_config_is_empty(self):
        db = None
        try:
            db = Database(self.engine, "{}")
            self.assertFalse(True)
        except ValueError as ve:
            self.assertEqual(ve.args[0], "pmemkv_open failed")
        self.assertEqual(db, None)

    def test_exception_on_start_when_config_is_malformed(self):
        db = None
        try:
            db = Database(self.engine, "{")
            self.assertFalse(True)
        except ValueError as ve:
            self.assertEqual(ve.args[0], "Creating a pmemkv config from JSON string failed")
        self.assertEqual(db, None)

    def test_throws_exception_on_start_when_engine_is_invalid(self):
        db = None
        try:
            db = Database(r"nope.nope", self.config)
            self.assertFalse(True)
        except ValueError as ve:
            self.assertEqual(ve.args[0], "pmemkv_open failed")
        self.assertEqual(db, None)

    def test_throws_exception_on_start_when_path_is_invalid(self):
        db = None
        try:
            db = Database(self.engine, "{\"path\":\"/tmp/123/234/345/456/567/678/nope.nope\"}")
            self.assertFalse(True)
        except ValueError as ve:
            self.assertEqual(ve.args[0], "pmemkv_open failed")
        self.assertEqual(db, None)

    def test_throws_exception_on_start_when_path_is_wrong_type(self):
        db = None
        try:
            db = Database(self.engine, '{"path":1234}')
            self.assertFalse(True)
        except ValueError as ve:
            self.assertEqual(ve.args[0], "pmemkv_open failed")
        self.assertEqual(db, None)

    def test_uses_get_keys(self):
        db = Database(self.engine, self.config)
        db.put(r"1", r"one")
        db.put(r"2", r"two")

        self.formatter = r"<{}>,"

        self.key = r""
        db.get_keys(self.all_and_each)
        self.assertEqual(self.key, r"<1>,<2>,")

        db.put(r"记!", r"RR")
        self.key = r""
        db.get_keys_strings(self.all_and_each_strings)
        self.assertEqual(self.key, r"<1>,<2>,<记!>,")

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
        self.assertEqual(self.key, r"BB,BC,")

        db.put(r"记!", r"RR")
        self.key = r""
        db.get_keys_strings_above(r"", self.all_and_each_strings)

        self.assertEqual(self.key, r"A,AB,AC,B,BB,BC,记!,")
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

        self.key = r""
        db.get_keys_below(r"B", self.all_and_each)
        self.assertEqual(self.key, r"A,AB,AC,")

        db.put(r"记!", r"RR")
        self.key = r""
        db.get_keys_strings_below("\uFFFF", self.all_and_each_strings)
        self.assertEqual(self.key, r"A,AB,AC,B,BB,BC,记!,")
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

        self.key = r""
        db.get_keys_between(r"A", r"B", self.all_and_each)
        self.assertEqual(self.key, r"AB,AC,")

        db.put(r"记!", r"RR")
        self.key = r""
        db.get_keys_strings_between(r"B", "\uFFFF", self.all_and_each_strings)
        self.assertEqual(self.key, r"BB,BC,记!,")

        self.key = r""
        db.get_keys_between(r"", r"", self.all_and_each)
        db.get_keys_between(r"A", r"A", self.all_and_each)
        db.get_keys_between(r"B", r"A", self.all_and_each)
        self.assertEqual(self.key, r"")

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

        db.put(r"记!", r"RR")
        self.key_and_value = r""
        db.get_all_string(self.all_and_each_strings)
        self.assertEqual(self.key_and_value, r"<1>,<one>|<2>,<two>|<记!>,<RR>|")

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

        db.put(r"记!", r"RR")
        self.key_and_value = r""
        db.get_string_above(r"", self.all_and_each_strings)
        self.assertEqual(self.key_and_value, r"A,1|AB,2|AC,3|B,4|BB,5|BC,6|记!,RR|")

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
        self.key_and_value = r""
        db.get_string_below("\uFFFD", self.all_and_each_strings)
        self.assertEqual(self.key_and_value, r"A,1|AB,2|AC,3|B,4|BB,5|BC,6|记!,RR|")

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

        db.put(r"记!", r"RR")
        self.key_and_value = r""
        db.get_string_between(r"B", "\uFFFD", self.all_and_each_strings)
        self.assertEqual(self.key_and_value, r"BB,5|BC,6|记!,RR|")

        self.key_and_value = r""
        db.get_between(r"", r"", self.all_and_each)
        db.get_between(r"A", r"A", self.all_and_each)
        db.get_between(r"B", r"A", self.all_and_each)
        self.assertEqual(self.key_and_value, r"")

        db.stop()

if __name__ == '__main__':
    unittest.main()
