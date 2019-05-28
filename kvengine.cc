#include <Python.h>
#include <string>
#include "kvengine.h"
using namespace pmemkv;

KVEngine* kv;
PyObject* Python_Callback;

// Turn on/off operations.
static PyObject *
pmemkv_NI_Start(PyObject* self, PyObject* args) {
	Py_buffer engine, path;
	if (!PyArg_ParseTuple(args, "s*s*", &engine, &path)) {
                return NULL;
        }

	std::string error_message;
	auto callback = [](void* cxt, const char* engine, const char* config, const char* msg) {
		const auto c = ((std::string*) cxt);
		c->append(msg);
	};

	kv = kvengine_start(&error_message, (const char*) engine.buf, (const char*) path.buf, callback);

	if (error_message.empty()) {
                Py_RETURN_NONE;
        }
	return Py_BuildValue("s", error_message.c_str());
}

static PyObject *
pmemkv_NI_Stop(PyObject* self) {
	kvengine_stop(kv);
	Py_RETURN_NONE;
}

// "All" Methods.
static PyObject *
pmemkv_NI_All(PyObject* self, PyObject* args) {
	if (!PyArg_ParseTuple(args, "O:set_callback", &Python_Callback)) {
		return NULL;
	}

	auto callback = [](void* context, int keybytes, const char* key) {
		PyObject* arg = Py_BuildValue("(s#)", key, keybytes);
		PyObject_CallObject(Python_Callback, arg);
	};
	kvengine_all(kv, NULL, callback);
	Py_RETURN_NONE;
}

static PyObject *
pmemkv_NI_AllAbove(PyObject* self, PyObject* args) {
        Py_buffer key;
        if(!PyArg_ParseTuple(args, "s*O:set_callback", &key, &Python_Callback)) {
                return NULL;
        }

        auto callback = [](void* context, int keybytes, const char* key) {
                PyObject* arg = Py_BuildValue("(s#)", key, keybytes);
                PyObject_CallObject(Python_Callback, arg);
        };

        kvengine_all_above(kv, NULL, key.len, (const char*) key.buf, callback);
	Py_RETURN_NONE;
}

static PyObject *
pmemkv_NI_AllBelow(PyObject* self, PyObject* args) {
        Py_buffer key;
        if(!PyArg_ParseTuple(args, "s*O:set_callback", &key, &Python_Callback)) {
                return NULL;
        }

        auto callback = [](void* context, int keybytes, const char* key) {
                PyObject* arg = Py_BuildValue("(s#)", key, keybytes);
                PyObject_CallObject(Python_Callback, arg);
        };

        kvengine_all_below(kv, NULL, key.len, (const char*) key.buf, callback);
        Py_RETURN_NONE;
}

static PyObject *
pmemkv_NI_AllBetween(PyObject* self, PyObject* args) {
        Py_buffer key1, key2;
        if(!PyArg_ParseTuple(args, "s*s*O:set_callback", &key1, &key2, &Python_Callback)) {
                return NULL;
        }

        auto callback = [](void* context, int keybytes, const char* key) {
                PyObject* arg = Py_BuildValue("(s#)", key, keybytes);
                PyObject_CallObject(Python_Callback, arg);
        };

        kvengine_all_between(kv, NULL, key1.len, (const char*) key1.buf, key2.len, (const char*) key2.buf, callback);
	Py_RETURN_NONE;
}

// "Count" Methods.
static PyObject *
pmemkv_NI_Count(PyObject* self, PyObject* args) {
        return PyLong_FromLong(kvengine_count(kv));
}

static PyObject *
pmemkv_NI_CountAbove(PyObject* self, PyObject* args) {
	Py_buffer key;
	if(!PyArg_ParseTuple(args, "s*", &key)) {
		return NULL;
	}
        return PyLong_FromLong(kvengine_count_above(kv, key.len, (const char*) key.buf));
}

static PyObject *
pmemkv_NI_CountBelow(PyObject* self, PyObject* args) {
        Py_buffer key;
        if(!PyArg_ParseTuple(args, "s*", &key)) {
                return NULL;
        }
        return PyLong_FromLong(kvengine_count_below(kv, key.len, (const char*) key.buf));
}

static PyObject *
pmemkv_NI_CountBetween(PyObject* self, PyObject* args) {
        Py_buffer key1, key2;
        if(!PyArg_ParseTuple(args, "s*s*", &key1, &key2)) {
                return NULL;
        }
        return PyLong_FromLong(kvengine_count_between(kv, key1.len, (const char*) key1.buf, key2.len, (const char*) key2.buf));
}

// "Each" Methods.
static PyObject *
pmemkv_NI_Each(PyObject* self, PyObject* args) {
	if (!PyArg_ParseTuple(args, "O:set_callback", &Python_Callback)) {
                return NULL;
        }

        auto callback = [](void* context, int keybytes, const char* key, int valuebyte, const char* value) {
		PyObject* args = Py_BuildValue("(s#s#)", key, keybytes, value, valuebyte);
                PyObject_CallObject(Python_Callback, args);
        };

        kvengine_each(kv, NULL, callback);
	Py_RETURN_NONE;
}

static PyObject *
pmemkv_NI_EachAbove(PyObject* self, PyObject* args) {
        Py_buffer search_key;
        if(!PyArg_ParseTuple(args, "s*O:set_callback", &search_key, &Python_Callback)) {
                return NULL;
        }

	auto callback = [](void* context, int keybytes, const char* key, int valuebyte, const char* value) {
		PyObject* args = Py_BuildValue("(s#s#)", key, keybytes, value, valuebyte);
                PyObject_CallObject(Python_Callback, args);
        };

	kvengine_each_above(kv, NULL, search_key.len, (const char*) search_key.buf, callback);
	Py_RETURN_NONE;
}

static PyObject *
pmemkv_NI_EachBelow(PyObject* self, PyObject* args) {
	Py_buffer search_key;
        if(!PyArg_ParseTuple(args, "s*O:set_callback", &search_key, &Python_Callback)) {
                return NULL;
        }

        auto callback = [](void* context, int keybytes, const char* key, int valuebyte, const char* value) {
                PyObject* args = Py_BuildValue("(s#s#)", key, keybytes, value, valuebyte);
                PyObject_CallObject(Python_Callback, args);
        };

        kvengine_each_below(kv, NULL, search_key.len, (const char*) search_key.buf, callback);
        Py_RETURN_NONE;
}

static PyObject *
pmemkv_NI_EachBetween(PyObject* self, PyObject* args) {
        Py_buffer key1, key2;
        if(!PyArg_ParseTuple(args, "s*s*O:set_callback", &key1, &key2, &Python_Callback)) {
                return NULL;
        }

        auto callback = [](void* context, int keybytes, const char* key, int valuebyte, const char* value) {
                PyObject* args = Py_BuildValue("(s#s#)", key, keybytes, value, valuebyte);
                PyObject_CallObject(Python_Callback, args);
        };

        kvengine_each_between(kv, NULL, key1.len, (const char*) key1.buf, key2.len, (const char*) key2.buf, callback);
        Py_RETURN_NONE;
}

// "Exists" Method.
static PyObject *
pmemkv_NI_Exists(PyObject* self, PyObject* args) {
        Py_buffer key;
        if(!PyArg_ParseTuple(args, "s*", &key)) {
                return NULL;
        }
        return PyLong_FromLong(kvengine_exists(kv, key.len, (const char*) key.buf));
}

// "CRUD" Operations.
static PyObject *
pmemkv_NI_Put(PyObject* self, PyObject* args) {
	Py_buffer key, value;
        if (!PyArg_ParseTuple(args, "s*s*", &key, &value)) {
                return NULL;
        }
	return PyLong_FromLong(kvengine_put(kv, key.len, (const char*) key.buf, value.len, (const char*) value.buf));
}

static PyObject *
pmemkv_NI_Get(PyObject* self, PyObject* args) {
        Py_buffer key;
        if (!PyArg_ParseTuple(args, "s*", &key)) {
                return NULL;
        }
        struct GetCallbackContext {
                KVStatus status;
                std::string value;
        };

        GetCallbackContext cxt = {NOT_FOUND, ""};

        auto callback = [](void* context, int vb, const char* v) {
                const auto c = ((GetCallbackContext*) context);
                c->status = OK;
                c->value.append(v, vb);
        };

        kvengine_get(kv, &cxt, key.len, (const char*) key.buf, callback);
        if (cxt.status == OK) {
                return Py_BuildValue("s#", cxt.value.c_str(), cxt.value.size());
        }
        Py_RETURN_NONE;
}

static PyObject *
pmemkv_NI_Remove(PyObject* self, PyObject* args) {
        Py_buffer key;
        if(!PyArg_ParseTuple(args, "s*", &key)) {
                return NULL;
        }
	return PyLong_FromLong(kvengine_remove(kv, key.len, (const char*) key.buf));
}

// Functions declarations.
static PyMethodDef pmemkv_NI_funcs[] = {
	{"start", (PyCFunction)pmemkv_NI_Start, METH_VARARGS, NULL},
	{"stop", (PyCFunction)pmemkv_NI_Stop, METH_NOARGS, NULL},
	{"put", (PyCFunction)pmemkv_NI_Put, METH_VARARGS, NULL},
	{"get", (PyCFunction)pmemkv_NI_Get, METH_VARARGS, NULL},
	{"all", (PyCFunction)pmemkv_NI_All, METH_VARARGS, NULL},
	{"all_above", (PyCFunction)pmemkv_NI_AllAbove, METH_VARARGS, NULL},
	{"all_below", (PyCFunction)pmemkv_NI_AllBelow, METH_VARARGS, NULL},
	{"all_between", (PyCFunction)pmemkv_NI_AllBetween, METH_VARARGS, NULL},
	{"count", (PyCFunction)pmemkv_NI_Count, METH_NOARGS, NULL},
	{"count_above", (PyCFunction)pmemkv_NI_CountAbove, METH_VARARGS, NULL},
	{"count_below", (PyCFunction)pmemkv_NI_CountBelow, METH_VARARGS, NULL},
	{"count_between", (PyCFunction)pmemkv_NI_CountBetween, METH_VARARGS, NULL},
	{"each", (PyCFunction)pmemkv_NI_Each, METH_VARARGS, NULL},
	{"each_above", (PyCFunction)pmemkv_NI_EachAbove, METH_VARARGS, NULL},
	{"each_below", (PyCFunction)pmemkv_NI_EachBelow, METH_VARARGS, NULL},
	{"each_between", (PyCFunction)pmemkv_NI_EachBetween, METH_VARARGS, NULL},
	{"exists", (PyCFunction)pmemkv_NI_Exists, METH_VARARGS, NULL},
	{"remove", (PyCFunction)pmemkv_NI_Remove, METH_VARARGS, NULL},
	{NULL, NULL, 0, NULL}
};

// Module defination.
static struct PyModuleDef initpmemkv_NI = {
	PyModuleDef_HEAD_INIT,
	"pmemkv_NI", /* name of module */
	NULL,          /* module documentation, may be NULL */
	-1,          /* size of per-interpreter state of the module, or -1 if the module keeps state in global variables. */
	pmemkv_NI_funcs
};

// Creating dynamic module.
PyMODINIT_FUNC PyInit_pmemkv_NI(void) {
	return PyModule_Create(&initpmemkv_NI);
}
