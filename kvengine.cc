/*
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
 */

#include <Python.h>
#include <string>
#include <libpmemkv.h>
#include <iostream>

pmemkv_db* db;
PyObject* Python_Callback;

// Turn on/off operations.
static PyObject *
pmemkv_NI_Start(PyObject* self, PyObject* args) {
	Py_buffer engine, path;
	if (!PyArg_ParseTuple(args, "s*s*", &engine, &path)) {
		return NULL;
	}

	pmemkv_config *config = pmemkv_config_new();
	if (config == nullptr) {
		return Py_BuildValue("s", "Allocating a new pmemkv config failed");
	}

	int rv = pmemkv_config_from_json(config, (const char*) path.buf);
	if (rv != PMEMKV_STATUS_OK) {
		pmemkv_config_delete(config);
		return Py_BuildValue("s", "Creating a pmemkv config from JSON string failed");
	}

	rv = pmemkv_open((const char*) engine.buf, config, &db);
	if (rv != PMEMKV_STATUS_OK) {
                return Py_BuildValue("s", "pmemkv_open failed");
        }
	Py_RETURN_NONE;
}

static PyObject *
pmemkv_NI_Stop(PyObject* self) {
	pmemkv_close(db);
	Py_RETURN_NONE;
}

// "All" Methods.
static PyObject *
pmemkv_NI_GetKeys(PyObject* self, PyObject* args) {
	if (!PyArg_ParseTuple(args, "O:set_callback", &Python_Callback)) {
		return NULL;
	}
	auto callback = [](const char* key, size_t keybytes, const char* value, size_t valuebyte, void* context) -> int {
                PyObject* args = Py_BuildValue("(s#)", key, keybytes);
                PyObject_CallObject(Python_Callback, args);
                return 0;
        };
	int result = pmemkv_get_all(db, callback, nullptr);
	return PyLong_FromLong(result);
}

static PyObject *
pmemkv_NI_GetKeysAbove(PyObject* self, PyObject* args) {
	Py_buffer key;
	if (!PyArg_ParseTuple(args, "s*O:set_callback", &key, &Python_Callback)) {
		return NULL;
	}
	auto callback = [](const char* key, size_t keybytes, const char* value, size_t valuebyte, void* context) -> int {
                PyObject* args = Py_BuildValue("(s#)", key, keybytes);
                PyObject_CallObject(Python_Callback, args);
                return 0;
        };
	int result = pmemkv_get_above(db, (const char*) key.buf, key.len, callback, nullptr);
	return PyLong_FromLong(result);
}

static PyObject *
pmemkv_NI_GetKeysBelow(PyObject* self, PyObject* args) {
	Py_buffer key;
	if (!PyArg_ParseTuple(args, "s*O:set_callback", &key, &Python_Callback)) {
		return NULL;
	}
	auto callback = [](const char* key, size_t keybytes, const char* value, size_t valuebyte, void* context) -> int {
                PyObject* args = Py_BuildValue("(s#)", key, keybytes);
                PyObject_CallObject(Python_Callback, args);
                return 0;
        };
        int result = pmemkv_get_below(db, (const char*) key.buf, key.len, callback, nullptr);
        return PyLong_FromLong(result);
}

static PyObject *
pmemkv_NI_GetKeysBetween(PyObject* self, PyObject* args) {
	Py_buffer key1, key2;
	if (!PyArg_ParseTuple(args, "s*s*O:set_callback", &key1, &key2, &Python_Callback)) {
		return NULL;
	}
	auto callback = [](const char* key, size_t keybytes, const char* value, size_t valuebyte, void* context) -> int {
                PyObject* args = Py_BuildValue("(s#)", key, keybytes);
                PyObject_CallObject(Python_Callback, args);
                return 0;
        };
        int result = pmemkv_get_between(db, (const char*) key1.buf, key1.len, (const char*) key2.buf, key2.len, callback, nullptr);
        return PyLong_FromLong(result);
}

// "Count" Methods.
static PyObject *
pmemkv_NI_CountAll(PyObject* self) {
	size_t cnt;
	int result = pmemkv_count_all(db, &cnt);
	if (result == PMEMKV_STATUS_OK) {
		return Py_BuildValue("i", cnt);
	}
	Py_RETURN_NONE;
}

static PyObject *
pmemkv_NI_CountAbove(PyObject* self, PyObject* args) {
	Py_buffer key;
	if (!PyArg_ParseTuple(args, "s*", &key)) {
		return NULL;
	}
	size_t cnt;
	int result = pmemkv_count_above(db, (const char*) key.buf, key.len, &cnt);
        if (result == PMEMKV_STATUS_OK) { 
                return Py_BuildValue("i", cnt);
        }
        Py_RETURN_NONE;
}

static PyObject *
pmemkv_NI_CountBelow(PyObject* self, PyObject* args) {
	Py_buffer key;
	if (!PyArg_ParseTuple(args, "s*", &key)) {
		return NULL;
	}
	size_t cnt;
	int result = pmemkv_count_below(db, (const char*) key.buf, key.len, &cnt);
        if (result == PMEMKV_STATUS_OK) { 
                return Py_BuildValue("i", cnt);
        }
        Py_RETURN_NONE;
}

static PyObject *
pmemkv_NI_CountBetween(PyObject* self, PyObject* args) {
	Py_buffer key1, key2;
	if (!PyArg_ParseTuple(args, "s*s*", &key1, &key2)) {
		return NULL;
	}
	size_t cnt;
	int result = pmemkv_count_between(db, (const char*) key1.buf, key1.len, (const char*) key2.buf, key2.len, &cnt);
        if (result == PMEMKV_STATUS_OK) { 
                return Py_BuildValue("i", cnt);
        }
        Py_RETURN_NONE;
}

// "Each" Methods.
static PyObject *
pmemkv_NI_GetAll(PyObject* self, PyObject* args) {
	if (!PyArg_ParseTuple(args, "O:set_callback", &Python_Callback)) {
		return NULL;
	}
	auto callback = [](const char* key, size_t keybytes, const char* value, size_t valuebyte, void* context) -> int {
                PyObject* args = Py_BuildValue("(s#s#)", key, keybytes, value, valuebyte);
                PyObject_CallObject(Python_Callback, args);
		return 0;
        };
        int result = pmemkv_get_all(db, callback, nullptr);
        return PyLong_FromLong(result);
}

static PyObject *
pmemkv_NI_GetAbove(PyObject* self, PyObject* args) {
	Py_buffer key;
	if (!PyArg_ParseTuple(args, "s*O:set_callback", &key, &Python_Callback)) {
		return NULL;
	}
	auto callback = [](const char* key, size_t keybytes, const char* value, size_t valuebyte, void* context) -> int {
		PyObject* args = Py_BuildValue("(s#s#)", key, keybytes, value, valuebyte);
		PyObject_CallObject(Python_Callback, args);
		return 0;
	};
	int result = pmemkv_get_above(db, (const char*) key.buf, key.len, callback, nullptr);
	return PyLong_FromLong(result);
}

static PyObject *
pmemkv_NI_GetBelow(PyObject* self, PyObject* args) {
	Py_buffer key;
	if (!PyArg_ParseTuple(args, "s*O:set_callback", &key, &Python_Callback)) {
		return NULL;
	}
	auto callback = [](const char* key, size_t keybytes, const char* value, size_t valuebyte, void* context) -> int {
                PyObject* args = Py_BuildValue("(s#s#)", key, keybytes, value, valuebyte);
                PyObject_CallObject(Python_Callback, args);
                return 0;
        };
        int result = pmemkv_get_below(db, (const char*) key.buf, key.len, callback, nullptr);
        return PyLong_FromLong(result);
}

static PyObject *
pmemkv_NI_GetBetween(PyObject* self, PyObject* args) {
	Py_buffer key1, key2;
	if (!PyArg_ParseTuple(args, "s*s*O:set_callback", &key1, &key2, &Python_Callback)) {
		return NULL;
	}
	auto callback = [](const char* key, size_t keybytes, const char* value, size_t valuebyte, void* context) -> int {
                PyObject* args = Py_BuildValue("(s#s#)", key, keybytes, value, valuebyte);
                PyObject_CallObject(Python_Callback, args);
                return 0;
        };
        int result = pmemkv_get_between(db, (const char*) key1.buf, key1.len, (const char*) key2.buf, key2.len, callback, nullptr);
        return PyLong_FromLong(result);
}

// "Exists" Method.
static PyObject *
pmemkv_NI_Exists(PyObject* self, PyObject* args) {
	Py_buffer key;
	if (!PyArg_ParseTuple(args, "s*", &key)) {
		return NULL;
	}
	int result = pmemkv_exists(db, (const char*) key.buf, key.len);
	return PyLong_FromLong(result);
}

// "CRUD" Operations.
static PyObject *
pmemkv_NI_Put(PyObject* self, PyObject* args) {
	Py_buffer key, value;
	if (!PyArg_ParseTuple(args, "s*s*", &key, &value)) {
		return NULL;
	}
	int result = pmemkv_put(db, (const char*) key.buf, key.len, (const char*) value.buf, value.len);
	return PyLong_FromLong(result);
}

static PyObject *
pmemkv_NI_Get(PyObject* self, PyObject* args) {
	Py_buffer key;
	if (!PyArg_ParseTuple(args, "s*", &key)) {
		return NULL;
	}
	struct GetCallbackContext {
		int status;
		std::string value;
	};
	GetCallbackContext cxt = {PMEMKV_STATUS_NOT_FOUND, ""};

	auto callback = [](const char* v, size_t vb, void* context) {
		const auto c = ((GetCallbackContext*) context);
		c->status = PMEMKV_STATUS_OK;
		c->value.append(v, vb);
	};
	int result = pmemkv_get(db, (const char*) key.buf, key.len, callback, &cxt);
	if (result == PMEMKV_STATUS_FAILED) {
		return PyLong_FromLong(result);
	} else if (cxt.status == PMEMKV_STATUS_OK) {
		return Py_BuildValue("s#", cxt.value.data(), cxt.value.size());
	}
	Py_RETURN_NONE;
}

static PyObject *
pmemkv_NI_Remove(PyObject* self, PyObject* args) {
	Py_buffer key;
	if (!PyArg_ParseTuple(args, "s*", &key)) {
		return NULL;
	}
	int result = pmemkv_remove(db, (const char*) key.buf, key.len);
        return PyLong_FromLong(result);
}

// Functions declarations.
static PyMethodDef pmemkv_NI_funcs[] = {
	{"start", (PyCFunction)pmemkv_NI_Start, METH_VARARGS, NULL},
	{"stop", (PyCFunction)pmemkv_NI_Stop, METH_NOARGS, NULL},
	{"put", (PyCFunction)pmemkv_NI_Put, METH_VARARGS, NULL},
	{"get", (PyCFunction)pmemkv_NI_Get, METH_VARARGS, NULL},
	{"get_keys", (PyCFunction)pmemkv_NI_GetKeys, METH_VARARGS, NULL},
	{"get_keys_above", (PyCFunction)pmemkv_NI_GetKeysAbove, METH_VARARGS, NULL},
	{"get_keys_below", (PyCFunction)pmemkv_NI_GetKeysBelow, METH_VARARGS, NULL},
	{"get_keys_between", (PyCFunction)pmemkv_NI_GetKeysBetween, METH_VARARGS, NULL},
	{"count_all", (PyCFunction)pmemkv_NI_CountAll, METH_NOARGS, NULL},
	{"count_above", (PyCFunction)pmemkv_NI_CountAbove, METH_VARARGS, NULL}, 
	{"count_below", (PyCFunction)pmemkv_NI_CountBelow, METH_VARARGS, NULL},
	{"count_between", (PyCFunction)pmemkv_NI_CountBetween, METH_VARARGS, NULL},
	{"get_all", (PyCFunction)pmemkv_NI_GetAll, METH_VARARGS, NULL},
	{"get_above", (PyCFunction)pmemkv_NI_GetAbove, METH_VARARGS, NULL},
	{"get_below", (PyCFunction)pmemkv_NI_GetBelow, METH_VARARGS, NULL},
	{"get_between", (PyCFunction)pmemkv_NI_GetBetween, METH_VARARGS, NULL},
	{"exists", (PyCFunction)pmemkv_NI_Exists, METH_VARARGS, NULL},
	{"remove", (PyCFunction)pmemkv_NI_Remove, METH_VARARGS, NULL},
	{NULL, NULL, 0, NULL}
};

// Module defination.
static struct PyModuleDef initpmemkv_NI = {
	PyModuleDef_HEAD_INIT,
	"pmemkv_NI", /* name of module */
	NULL, /* module documentation, may be NULL */
	-1, /* size of per-interpreter state of the module, or -1 if the module keeps state in global variables. */
	pmemkv_NI_funcs
};

// Creating dynamic module.
PyMODINIT_FUNC PyInit_pmemkv_NI(void) {
	return PyModule_Create(&initpmemkv_NI);
}
