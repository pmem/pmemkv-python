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
#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include "structmember.h"
#include <string>
#include <libpmemkv.h>
#include <libpmemkv_json_config.h>
#include <iostream>


#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
	PyObject_HEAD
	const char *value;
	Py_ssize_t length;
} PmemkvValueBufferObject;

static PyMemberDef PmemvValueBuffer_members[] = {
	{"value", T_INT, offsetof(PmemkvValueBufferObject, value), 0, "Pointer to data"},
	{"length", T_INT, offsetof(PmemkvValueBufferObject, length), 0,
	 "Length of underlying data"},
	{NULL}};

static int PmemkvValueBufferObject_getbuffer(PyObject *obj, Py_buffer *view, int flags)
{
	if (view == NULL) {
		PyErr_SetString(PyExc_ValueError, "NULL view in getbuffer");
		return -1;
	}
	PmemkvValueBufferObject *self = (PmemkvValueBufferObject *)obj;
	view->obj = obj;
	view->buf = (void *)self->value;
	view->len = self->length;
	view->readonly = 1;
	view->itemsize = 1;
	view->format = 0; // unsigned bytes
	view->ndim = 0;
	view->shape = NULL;
	view->strides = NULL;
	view->suboffsets = NULL;
	view->internal = NULL;
	Py_INCREF(self);
	return 0;
}

static PyBufferProcs PmemkvValueBuffer_as_buffer = {
	// this definition is only compatible with Python 3.3 and above
	(getbufferproc)PmemkvValueBufferObject_getbuffer,
	(releasebufferproc)0, // we do not require any special release function
};

static PyObject *PmemkvValueBuffer_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
	PmemkvValueBufferObject *self =
		(PmemkvValueBufferObject *)type->tp_alloc(type, 0);
	return (PyObject *)self;
}

static int PmemkvValueBuffer_init(PmemkvValueBufferObject *self)
{
	if (self != NULL) {
		self->value = NULL;
		self->length = 0;
	}
	return 0;
}

static void PmemkvValueBuffer_dealloc(PmemkvValueBufferObject *self)
{
	PyObject_Del(self);
}

static PyTypeObject PmemkvValueBufferType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "pmemkv.pmemkv_NI",
	.tp_basicsize = sizeof(PmemkvValueBufferObject),
	.tp_itemsize = 0,
	.tp_dealloc = (destructor)PmemkvValueBuffer_dealloc,
	.tp_print = 0,
	.tp_getattr = 0,
	.tp_setattr = 0,
	.tp_as_async = 0,
	.tp_repr = 0,
	.tp_as_number = 0,
	.tp_as_sequence = 0,
	.tp_as_mapping = 0,
	.tp_hash = 0,
	.tp_call = 0,
	.tp_str = 0,
	.tp_getattro = 0,
	.tp_setattro = 0,
	.tp_as_buffer = &PmemkvValueBuffer_as_buffer,
	.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	.tp_doc = "Pmemkv value type", /* tp_doc */
	.tp_traverse = 0,
	.tp_clear = 0,
	.tp_richcompare = 0,
	.tp_weaklistoffset = 0,
	.tp_iter = 0,
	.tp_iternext = 0,
	.tp_methods = 0,
	.tp_members = PmemvValueBuffer_members,
	.tp_getset = 0,
	.tp_base = 0,
	.tp_dict = NULL,
	.tp_descr_get = 0,
	.tp_descr_set = 0,
	.tp_dictoffset = 0,
	.tp_init = (initproc)PmemkvValueBuffer_init,
	.tp_alloc = 0,
	.tp_new = PmemkvValueBuffer_new,
	.tp_free = 0,
	.tp_is_gc = 0, /* For PyObject_IS_GC */
	.tp_bases = 0,
	.tp_mro = 0,
	.tp_cache = 0,
	.tp_subclasses = 0,
	.tp_weaklist = 0,
	.tp_del = 0,
	.tp_version_tag = 0,
	.tp_finalize = 0,
};

typedef struct {
	PyObject_HEAD
	pmemkv_db *db;
} PmemkvObject;

static PyMemberDef
pmemkv_NI_members[] = {
	{"db", T_INT, offsetof(PmemkvObject, db), 0, "Engine instance"},
	{NULL}
};

static PyObject *
Pmemkv_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
	PmemkvObject *self = (PmemkvObject *) type->tp_alloc(type, 0);
	return (PyObject *) self;
}

static int Pmemkv_init(PmemkvObject *self)
{
	if (self != NULL) {
		self->db = NULL;
	}
	return 0;
}

// Turn on/off operations.
static PyObject *
pmemkv_NI_Start(PmemkvObject *self, PyObject* args) {
	Py_buffer engine, json_config;
	if (!PyArg_ParseTuple(args, "s*s*", &engine, &json_config)) {
		return NULL;
	}

	pmemkv_config *config = pmemkv_config_new();
	if (config == nullptr) {
		// "Allocating a new pmemkv config failed"
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(PMEMKV_STATUS_OUT_OF_MEMORY));
		return NULL;
	}

	int rv = pmemkv_config_from_json(config, (const char*) json_config.buf);
	if (rv != PMEMKV_STATUS_OK) {
		pmemkv_config_delete(config);
		// "Creating a pmemkv config from JSON string failed"
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(rv));
		return NULL;
	}

	rv = pmemkv_open((const char*) engine.buf, config, &self->db);
	if (rv != PMEMKV_STATUS_OK) {
		// "pmemkv_open failed"
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(rv));
		return NULL;
	}
	Py_RETURN_NONE;
}

static PyObject *
pmemkv_NI_Stop(PmemkvObject *self) {
	if( self->db != NULL)
		pmemkv_close(self->db);
	self->db = NULL;
	Py_RETURN_NONE;
}

static void
Pmemkv_dealloc(PmemkvObject *self) {
    pmemkv_NI_Stop(self);
    Py_TYPE(self)->tp_free((PyObject *) self);
}

void value_callback(const char *value, size_t valuebyte, void *context)
{
	PmemkvValueBufferObject *entry =
		PyObject_New(PmemkvValueBufferObject, &PmemkvValueBufferType);
	Py_INCREF(entry);
	if (entry) {
		entry->value = value;
		entry->length = valuebyte;
		PyObject *args = PyTuple_New(1);
		if (args != NULL) {
			if (PyTuple_SetItem(args, 0, (PyObject *)entry) == 0) {
				PyObject *res =
					PyObject_CallObject((PyObject *)context, args);
				Py_XDECREF(res);
			}
		}
		Py_XDECREF(args);
	}
	Py_XDECREF(entry);
}

int key_callback(const char *key, size_t keybytes, const char *value, size_t valuebyte,
		 void *context)
{
	PyObject *args = Py_BuildValue("(s#)", key, keybytes);
	PyObject_CallObject((PyObject *)context, args);
	return 0;
}

int key_value_callback(const char *key, size_t keybytes, const char *value,
		       size_t valuebyte, void *context)
{
	PmemkvValueBufferObject *value_buffer =
		PyObject_New(PmemkvValueBufferObject, &PmemkvValueBufferType);
	PmemkvValueBufferObject *key_buffer =
		PyObject_New(PmemkvValueBufferObject, &PmemkvValueBufferType);
	Py_INCREF(value_buffer);
	Py_INCREF(key_buffer);
	int retval = 0;
	if (value_buffer) {
		value_buffer->value = value;
		value_buffer->length = valuebyte;
	}
	if (key_buffer) {
		key_buffer->value = key;
		key_buffer->length = keybytes;
	}
	PyObject *args = PyTuple_New(2);
	if (PyTuple_SetItem(args, 0, (PyObject *)key_buffer) != 0) {
		retval = -1;
	} else if (PyTuple_SetItem(args, 1, (PyObject *)value_buffer) != 0) {
		retval = -1;
	}
	PyObject *res = PyObject_CallObject((PyObject *)context, args);
	Py_DECREF(value_buffer);
	Py_DECREF(key_buffer);
	Py_DECREF(args);
	Py_XDECREF(res);
	if (PyErr_Occurred() != NULL)
		retval = -1;
	return retval;
}

// "All" Methods.
static PyObject *
pmemkv_NI_GetKeys(PmemkvObject *self, PyObject* args) {
	PyObject* python_callback;
	if (!PyArg_ParseTuple(args, "O:set_callback", &python_callback)) {
		return NULL;
	}
	int result = pmemkv_get_all(self->db, key_callback, python_callback);
	if (PyErr_Occurred() != NULL)
		return NULL;
	if (result != PMEMKV_STATUS_OK) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return PyLong_FromLong(result);
}

static PyObject *
pmemkv_NI_GetKeysAbove(PmemkvObject *self, PyObject* args) {
	Py_buffer key;
	PyObject* python_callback;
	if (!PyArg_ParseTuple(args, "s*O:set_callback", &key, &python_callback)) {
		return NULL;
	}
	int result = pmemkv_get_above(self->db, (const char *)key.buf, key.len,
				      key_callback, python_callback);
	if (PyErr_Occurred() != NULL)
		return NULL;
	if (result != PMEMKV_STATUS_OK) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return PyLong_FromLong(result);
}

static PyObject *
pmemkv_NI_GetKeysBelow(PmemkvObject *self, PyObject* args) {
	Py_buffer key;
	PyObject* python_callback;
	if (!PyArg_ParseTuple(args, "s*O:set_callback", &key, &python_callback)) {
		return NULL;
	}
	int result = pmemkv_get_below(self->db, (const char *)key.buf, key.len,
				      key_callback, python_callback);
	if (PyErr_Occurred() != NULL)
		return NULL;
	if (result != PMEMKV_STATUS_OK) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return PyLong_FromLong(result);
}

static PyObject *
pmemkv_NI_GetKeysBetween(PmemkvObject *self, PyObject* args) {
	Py_buffer key1, key2;
	PyObject* python_callback;
	if (!PyArg_ParseTuple(args, "s*s*O:set_callback", &key1, &key2, &python_callback)) {
		return NULL;
	}
	int result = pmemkv_get_between(self->db, (const char *)key1.buf, key1.len,
					(const char *)key2.buf, key2.len, key_callback,
					python_callback);
	if (PyErr_Occurred() != NULL)
		return NULL;
	if (result != PMEMKV_STATUS_OK) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return PyLong_FromLong(result);
}

// "Count" Methods.
static PyObject *
pmemkv_NI_CountAll(PmemkvObject *self) {
	size_t cnt;
	int result = pmemkv_count_all(self->db, &cnt);
	if (result != PMEMKV_STATUS_OK) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return Py_BuildValue("i", cnt);
}

static PyObject *
pmemkv_NI_CountAbove(PmemkvObject *self, PyObject* args) {
	Py_buffer key;
	if (!PyArg_ParseTuple(args, "s*", &key)) {
		return NULL;
	}
	size_t cnt;
	int result = pmemkv_count_above(self->db, (const char*) key.buf, key.len, &cnt);
	if (result != PMEMKV_STATUS_OK) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return Py_BuildValue("i", cnt);
}

static PyObject *
pmemkv_NI_CountBelow(PmemkvObject *self, PyObject* args) {
	Py_buffer key;
	if (!PyArg_ParseTuple(args, "s*", &key)) {
		return NULL;
	}
	size_t cnt;
	int result = pmemkv_count_below(self->db, (const char*) key.buf, key.len, &cnt);
	if (result != PMEMKV_STATUS_OK) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return Py_BuildValue("i", cnt);
}

static PyObject *
pmemkv_NI_CountBetween(PmemkvObject *self, PyObject* args) {
	Py_buffer key1, key2;
	if (!PyArg_ParseTuple(args, "s*s*", &key1, &key2)) {
		return NULL;
	}
	size_t cnt;
	int result = pmemkv_count_between(self->db, (const char*) key1.buf, key1.len, (const char*) key2.buf, key2.len, &cnt);
	if (result != PMEMKV_STATUS_OK) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return Py_BuildValue("i", cnt);
}

// "Each" Methods.
static PyObject *
pmemkv_NI_GetAll(PmemkvObject *self, PyObject* args) {
	PyObject* python_callback;
	if (!PyArg_ParseTuple(args, "O:set_callback", &python_callback)) {
		return NULL;
	}
	int result = pmemkv_get_all(self->db, key_value_callback, python_callback);
	if (PyErr_Occurred() != NULL)
		return NULL;
	if (result != PMEMKV_STATUS_OK) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return PyLong_FromLong(result);
}

static PyObject *
pmemkv_NI_GetAbove(PmemkvObject *self, PyObject* args) {
	Py_buffer key;
	PyObject* python_callback;
	if (!PyArg_ParseTuple(args, "s*O:set_callback", &key, &python_callback)) {
		return NULL;
	}
	int result = pmemkv_get_above(self->db, (const char *)key.buf, key.len,
				      key_value_callback, python_callback);
	if (PyErr_Occurred() != NULL)
		return NULL;
	if (result != PMEMKV_STATUS_OK) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return PyLong_FromLong(result);
}

static PyObject *
pmemkv_NI_GetBelow(PmemkvObject *self, PyObject* args) {
	Py_buffer key;
	PyObject* python_callback;
	if (!PyArg_ParseTuple(args, "s*O:set_callback", &key, &python_callback)) {
		return NULL;
	}
	int result = pmemkv_get_below(self->db, (const char *)key.buf, key.len,
				      key_value_callback, python_callback);
	if (PyErr_Occurred() != NULL)
		return NULL;
	if (result != PMEMKV_STATUS_OK) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return PyLong_FromLong(result);
}

static PyObject *
pmemkv_NI_GetBetween(PmemkvObject *self, PyObject* args) {
	Py_buffer key1, key2;
	PyObject* python_callback;
	if (!PyArg_ParseTuple(args, "s*s*O:set_callback", &key1, &key2, &python_callback)) {
		return NULL;
	}
	int result = pmemkv_get_between(self->db, (const char *)key1.buf, key1.len,
					(const char *)key2.buf, key2.len,
					key_value_callback, python_callback);
	if (PyErr_Occurred() != NULL)
		return NULL;
	if (result != PMEMKV_STATUS_OK) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return PyLong_FromLong(result);
}

// "Exists" Method.
static PyObject *
pmemkv_NI_Exists(PmemkvObject *self, PyObject* args) {
	Py_buffer key;
	if (!PyArg_ParseTuple(args, "s*", &key)) {
		return NULL;
	}
	int result = pmemkv_exists(self->db, (const char*) key.buf, key.len);
	if (result != PMEMKV_STATUS_OK && result != PMEMKV_STATUS_NOT_FOUND) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return PyLong_FromLong(result == PMEMKV_STATUS_OK);
}

// "CRUD" Operations.
static PyObject *
pmemkv_NI_Put(PmemkvObject *self, PyObject* args) {
	Py_buffer key, value;
	if (!PyArg_ParseTuple(args, "s*s*", &key, &value)) {
		return NULL;
	}
	int result = pmemkv_put(self->db, (const char*) key.buf, key.len, (const char*) value.buf, value.len);
	if (result != PMEMKV_STATUS_OK) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return PyLong_FromLong(result);
}

static PyObject *pmemkv_NI_GetString(PmemkvObject *self, PyObject *args)
{
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
	int result = pmemkv_get(self->db, (const char*) key.buf, key.len, callback, &cxt);
	if (PyErr_Occurred() != NULL)
		return NULL;
	if (result != PMEMKV_STATUS_OK && result != PMEMKV_STATUS_NOT_FOUND) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	} else if (cxt.status == PMEMKV_STATUS_OK) {
		return Py_BuildValue("s#", cxt.value.data(), cxt.value.size());
	}
	Py_RETURN_NONE;
}

static PyObject *pmemkv_NI_Get(PmemkvObject *self, PyObject *args)
{
	Py_buffer key;
	PyObject *python_callback;
	if (!PyArg_ParseTuple(args, "s*O:set_callback", &key, &python_callback)) {
		return NULL;
	}
	int result = pmemkv_get(self->db, (const char *)key.buf, key.len, value_callback,
				python_callback);
	if (PyErr_Occurred() != NULL)
		return NULL;
	if (result != PMEMKV_STATUS_OK) {
		PyErr_SetString(PyExc_Exception, pmemkv_errormsg());
		return NULL;
	}
	return PyLong_FromLong(result);
}

static PyObject *
pmemkv_NI_Remove(PmemkvObject *self, PyObject* args) {
	Py_buffer key;
	if (!PyArg_ParseTuple(args, "s*", &key)) {
		return NULL;
	}
	int result = pmemkv_remove(self->db, (const char*) key.buf, key.len);
	if (result != PMEMKV_STATUS_OK && result != PMEMKV_STATUS_NOT_FOUND) {
		PyErr_SetObject(PyExc_Exception, PyLong_FromLong(result));
		return NULL;
	}
	return PyLong_FromLong(result == PMEMKV_STATUS_OK);
}

// Functions declarations.
static PyMethodDef pmemkv_NI_methods[] = {
	{"start", (PyCFunction)pmemkv_NI_Start, METH_VARARGS, NULL},
	{"stop", (PyCFunction)pmemkv_NI_Stop, METH_NOARGS, NULL},
	{"put", (PyCFunction)pmemkv_NI_Put, METH_VARARGS, NULL},
	{"get_string", (PyCFunction)pmemkv_NI_GetString, METH_VARARGS, NULL},
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
	{NULL, NULL, 0, NULL}};

static PyTypeObject PmemkvType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "pmemkv.pmemkv_NI",
	.tp_basicsize = sizeof(PmemkvObject),
	.tp_itemsize = 0,
	.tp_dealloc = (destructor)Pmemkv_dealloc,
	.tp_print = 0,
	.tp_getattr = 0,
	.tp_setattr = 0,
	.tp_as_async = 0,
	.tp_repr = 0,
	.tp_as_number = 0,
	.tp_as_sequence = 0,
	.tp_as_mapping = 0,
	.tp_hash = 0,
	.tp_call = 0,
	.tp_str = 0,
	.tp_getattro = 0,
	.tp_setattro = 0,
	.tp_as_buffer = 0,
	.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	.tp_doc = "Pmemkv binding", /* tp_doc */
	.tp_traverse = 0,
	.tp_clear = 0,
	.tp_richcompare = 0,
	.tp_weaklistoffset = 0,
	.tp_iter = 0,
	.tp_iternext = 0,
	.tp_methods = pmemkv_NI_methods,
	.tp_members = pmemkv_NI_members,
	.tp_getset = 0,
	.tp_base = 0,
	.tp_dict = NULL,
	.tp_descr_get = 0,
	.tp_descr_set = 0,
	.tp_dictoffset = 0,
	.tp_init = (initproc)Pmemkv_init,
	.tp_alloc = 0,
	.tp_new = Pmemkv_new,
	.tp_free = 0,
	.tp_is_gc = 0, /* For PyObject_IS_GC */
	.tp_bases = 0,
	.tp_mro = 0,
	.tp_cache = 0,
	.tp_subclasses = 0,
	.tp_weaklist = 0,
	.tp_del = 0,
	.tp_version_tag = 0,
	.tp_finalize = 0,
};

// Module defination.
static struct PyModuleDef pmemkv_NI_module = {
	PyModuleDef_HEAD_INIT,
	"_pmemkv", /* name of module */
	NULL, /* module documentation, may be NULL */
	-1, /* size of per-interpreter state of the module, or -1 if the module keeps state in global variables. */
};

// Creating dynamic module.
PyMODINIT_FUNC
PyInit__pmemkv(void) {
	PyObject *m;
	if (PyType_Ready(&PmemkvType) < 0)
		return NULL;

	m = PyModule_Create(&pmemkv_NI_module);
	if (m == NULL)
		return NULL;

	Py_INCREF(&PmemkvType);
	if (PyModule_AddObject(m, "pmemkv_NI", (PyObject *) &PmemkvType) < 0) {
	        Py_DECREF(&PmemkvType);
		Py_DECREF(m);
		return NULL;
	}

	return m;
}

#ifdef __cplusplus
}
#endif
