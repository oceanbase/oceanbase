/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_CPYTHON_HELPER_H_
#define OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_CPYTHON_HELPER_H_

#include <Python.h>
#include <cwchar>

#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/allocator/page_arena.h"
#include "lib/lock/ob_mutex.h"

#if defined(_AIX)
  #define  LIB_OPEN_FLAGS        (RTLD_NOW | RTLD_GLOBAL | RTLD_MEMBER)
#elif defined(__hpux)
  #define  LIB_OPEN_FLAGS        (BIND_DEFERRED |BIND_VERBOSE| DYNAMIC_PATH)
#elif defined(__GNUC__)
  #define  LIB_OPEN_FLAGS        (RTLD_NOW | RTLD_GLOBAL)
#endif

#if defined(_WINDOWS)

  #include <Windows.h>

  #define LIB_HANDLE               HMODULE
  #define LIB_OPEN(l)              LoadLibraryA(l)
  #define LIB_CLOSE                FreeLibrary
  #define LIB_SYMBOL(h, s, p, t)   p = (t) GetProcAddress(h, s)

#elif defined(__hpux)

  #include <dl.h>

  #define LIB_HANDLE               shl_t
  #define LIB_OPEN(l)              shl_load(l, LIB_OPEN_FLAGS, 0L)
  #define LIB_CLOSE                shl_unload
  #define LIB_SYMBOL(h, s, p, t)   shl_findsym(&h, s, (short) TYPE_PROCEDURE, (void *) &p)

#elif defined(__GNUC__)

  #include <dlfcn.h>

  #define LIB_HANDLE               void *
  #define LIB_OPEN(l)              dlopen(l, LIB_OPEN_FLAGS)
  #define LIB_CLOSE                dlclose
  #define LIB_SYMBOL(h, s, p, t)   p = (t) dlsym(h, s)

#else

  #error Unable to compute how to dynamic libraries

#endif

typedef PyStatus (*PyConfig_SetString_Func)(PyConfig *, wchar_t **, const wchar_t *);
typedef void (*PyConfig_InitIsolatedConfig_Func)(PyConfig *);
typedef int (*PyStatus_Exception_Func)(PyStatus);
typedef void (*PyConfig_Clear_Func)(PyConfig *);


typedef PyStatus (*Py_InitializeFromConfig_Func)(PyConfig *);
typedef void (*Py_Initialize_Func)();
typedef void (*Py_IsInitialized_Func)();
typedef void (*Py_Finalize_Func)();
typedef PyObject* (*PyModule_Create2_Func)(PyModuleDef*, int apiver);
typedef PyObject* (*PyModuleDef_Init_Func)(PyModuleDef*);
typedef PyObject* (*PyImport_AddModule_Func)(const char *name);
typedef PyObject* (*PyModule_New_Func)(const char *);
typedef PyObject* (*PyImport_GetModuleDict_Func)();
// Return 0 on success or -1 on failure.
typedef int (*PyDict_SetItemString_Func)(PyObject *dp, const char *key, PyObject *item);
typedef PyObject* (*PyDict_GetItemString_Func)(PyObject *dp, const char *key);
typedef PyObject* (*PyModule_GetDict_Func)(PyObject *);
// Return NULL on failure
typedef PyObject* (*PyRun_String_Func)(const char *str, int s, PyObject *g, PyObject *l);
typedef int (*PyRun_SimpleString_Func)(const char *s);
typedef PyThreadState * (*PyThreadState_Swap_Func)(PyThreadState *);
typedef PyThreadState* (*PyEval_SaveThread_Func)();
typedef void (*PyEval_RestoreThread_Func)(PyThreadState *);
typedef void (*PyEval_AcquireThread_Func)(PyThreadState *);
typedef void (*PyEval_ReleaseThread_Func)(PyThreadState *);
typedef PyThreadState* (*Py_NewInterpreter_Func)();
typedef PyStatus (*Py_NewInterpreterFromConfig_Func)(PyThreadState **tstate_p, const PyInterpreterConfig *config);
typedef void (*Py_EndInterpreter_Func)(PyThreadState *);
typedef PyObject* (*PyUnicode_DecodeFSDefault_Func)(const char *);
typedef PyObject* (*PyImport_Import_Func)(PyObject *);
// 需要手动解引用
typedef PyObject* (*PyImport_ImportModule_Func)(const char *name);

// 需要手动解引用
typedef PyObject* (*PyObject_GetAttrString_Func)(PyObject *, const char *);
typedef int (*PyObject_SetAttrString_Func)(PyObject *o, const char *attr_name, PyObject *v);
// Determine if the object o is callable.
// Return 1 if the object is callable and 0 otherwise.
// This function always succeeds.
typedef int (*PyCallable_Check_Func)(PyObject *);
typedef void (*Py_DecRef_Func)(PyObject *);
typedef PyObject* (*PyTuple_Pack_Func)(Py_ssize_t, ...);
typedef PyObject* (*PyTuple_New_Func)(Py_ssize_t);
typedef int (*PyTuple_SetItem_Func)(PyObject *, Py_ssize_t, PyObject *);
typedef PyObject* (*PyObject_CallObject_Func)(PyObject *, PyObject *);
typedef PyObject* (*PyObject_CallMethod_Func)(PyObject *, const char *name, const char *format, ...);
typedef PyObject* (*PyObject_CallFunction_Func)(PyObject *callable, const char *format, ...);

typedef PyObject* (*PyErr_Occurred_Func)();
typedef void (*PyErr_Fetch_Func)(PyObject **, PyObject **, PyObject **);
typedef PyObject* (*PyObject_Str_Func)(PyObject *);
typedef PyObject* (*PyErr_GetRaisedException_Func)();
typedef void (*PyErr_SetObject_Func)(PyObject *, PyObject *);
typedef PyObject* (*PyCFunction_New_Func)(PyMethodDef *, PyObject *);

// python type
typedef PyObject *(*PyBool_FromLong_Func)(long);
typedef PyObject *(*PyLong_FromLong_Func)(long);
typedef long (*PyLong_AsLong_Func)(PyObject *);
typedef PyObject *(*PyFloat_FromDouble_Fun)(double);
// TODO : decimal
typedef PyObject *(*PyUnicode_FromStringAndSize_Func)(const char *, Py_ssize_t);
typedef PyObject *(*PyBytes_FromStringAndSize_Func)(const char *, Py_ssize_t);
typedef PyObject *(*PyUnicode_FromString_Func)(const char *);
typedef int (*PyObject_IsTrue_Func)(PyObject *);
typedef const char* (*PyUnicode_AsUTF8AndSize_Func)(PyObject *, Py_ssize_t *size);
typedef const char* (*PyUnicode_AsUTF8_Func)(PyObject *unicode);
typedef Py_ssize_t (*PyBytes_Size_Func)(PyObject *);
typedef char* (*PyBytes_AsString_Func)(PyObject *);
typedef double (*PyFloat_AsDouble_Func)(PyObject*);
typedef int (*PyObject_IsInstance_Func)(PyObject *object, PyObject *typeorclass);
// Py_None
typedef PyObject* PyNoneStructPtr;
typedef PyObject** PyExc_RuntimeErrorPtr;
typedef unsigned long * Py_VersionPtr;


extern PyObject* ob_py_builtins_module;
extern PyObject* ob_py_bool_type;
extern PyObject* ob_py_int_type;
extern PyObject* ob_py_str_type;
extern PyObject* ob_py_float_type;
extern PyObject* ob_py_bytes_type;

extern "C" PyModule_Create2_Func ob_py_module_create2;
extern "C" PyModuleDef_Init_Func ob_py_module_def_init;
 // borrowed ref
extern "C" PyImport_AddModule_Func ob_import_add_module;
extern "C" PyModule_New_Func ob_py_module_new;
extern "C" PyImport_GetModuleDict_Func ob_py_import_get_module_dict;
extern "C" PyDict_SetItemString_Func ob_py_dict_set_item_string;
extern "C" PyDict_GetItemString_Func ob_py_dict_get_item_string;
extern "C" PyModule_GetDict_Func ob_py_module_get_dict;
extern "C" PyRun_String_Func ob_py_run_string;
extern "C" PyRun_SimpleString_Func ob_py_run_simple_string;
extern "C" Py_InitializeFromConfig_Func ob_py_initialize_from_config;
extern "C" PyConfig_SetString_Func ob_py_config_set_string;
extern "C" PyConfig_InitIsolatedConfig_Func ob_py_config_init_isolated_config;
extern "C" PyStatus_Exception_Func ob_py_status_exception;
extern "C" PyConfig_Clear_Func ob_py_config_clear;

extern "C" Py_Initialize_Func ob_py_initialize;
extern "C" Py_Finalize_Func ob_py_finilize;
extern "C" Py_IsInitialized_Func ob_py_is_initialized;
extern "C" PyThreadState_Swap_Func ob_py_thread_state_swap;
extern "C" PyEval_SaveThread_Func ob_py_save_thread;
extern "C" PyEval_RestoreThread_Func ob_py_restore_thread;
extern "C" PyEval_AcquireThread_Func ob_py_acquire_thread;
extern "C" PyEval_ReleaseThread_Func ob_py_release_thread;
extern "C" Py_NewInterpreter_Func ob_py_new_interpreter;
extern "C" Py_NewInterpreterFromConfig_Func ob_py_new_interpreter_from_config;
extern "C" Py_EndInterpreter_Func ob_py_end_interpreter;
extern "C" PyUnicode_DecodeFSDefault_Func ob_py_unicode_decode_fs_default;
extern "C" PyImport_Import_Func ob_py_import_import;
extern "C" PyImport_ImportModule_Func ob_py_import_import_module;
extern "C" PyObject_GetAttrString_Func ob_py_object_get_attr_string;
extern "C" PyObject_SetAttrString_Func ob_py_object_set_attr_string;

extern "C" PyCallable_Check_Func ob_py_callable_check;
extern "C" Py_DecRef_Func ob_py_dec_ref;
extern "C" PyTuple_Pack_Func ob_py_tuple_pack;
extern "C" PyTuple_New_Func ob_py_tuple_new;
extern "C" PyTuple_SetItem_Func ob_py_tuple_set_item;
extern "C" PyObject_CallObject_Func ob_py_object_call_object;
extern "C" PyObject_CallMethod_Func ob_py_object_call_method;
extern "C" PyObject_CallFunction_Func ob_py_object_call_function;

extern "C" PyErr_Occurred_Func ob_py_err_occurred;
extern "C" PyErr_Fetch_Func ob_py_err_fetch;
extern "C" PyObject_Str_Func ob_py_object_str;
extern "C" PyErr_GetRaisedException_Func ob_py_err_get_raised_exception;
extern "C" PyErr_SetObject_Func ob_py_err_set_object;
extern "C" PyCFunction_New_Func ob_py_cfunction_new;


// python type
extern "C" PyBool_FromLong_Func ob_py_bool_from_long;
extern "C" PyLong_FromLong_Func ob_py_long_from_long;
extern "C" PyLong_AsLong_Func ob_py_long_as_long;
extern "C" PyFloat_FromDouble_Fun ob_py_float_from_double;
// UTF-8
extern "C" PyUnicode_FromStringAndSize_Func ob_py_unicode_from_string_and_size;
extern "C" PyBytes_FromStringAndSize_Func ob_py_bytes_from_string_and_size;
extern "C" PyUnicode_FromString_Func ob_py_unicode_from_string;
extern "C" PyObject_IsTrue_Func ob_py_object_is_true;
extern "C" PyUnicode_AsUTF8AndSize_Func ob_py_unicode_as_utf8_and_size;
extern "C" PyUnicode_AsUTF8_Func ob_py_unicode_as_utf8;
extern "C" PyBytes_Size_Func ob_py_bytes_size;
extern "C" PyBytes_AsString_Func ob_py_bytes_as_string;
extern "C" PyFloat_AsDouble_Func ob_py_float_as_double;
extern "C" PyObject_IsInstance_Func ob_py_object_is_instance;
extern "C" PyNoneStructPtr ob_py_none_ptr;
extern "C" PyExc_RuntimeErrorPtr ob_py_exection_runtime_error_ptr;
extern "C" Py_VersionPtr ob_py_version_ptr;


namespace oceanbase
{
namespace pl
{

typedef lib::ObLockGuard<lib::ObMutex> LockGuard;

struct ObPyEnvContext
{
public:
  ObPyEnvContext() { reset(); }
  void reset()
  {
    py_loaded_ = false;
    py_mem_bytes_ = 0;
    py_alloc_times_ = 0;
    py_realloc_times_ = 0;
    py_free_times_ = 0;
    py_created_time_ = 0;
    py_referece_times_ = 0;
  }

  bool is_valid()
  {
    return py_loaded_;
  }

public:
  bool py_loaded_;
  uint64_t py_mem_bytes_;
  uint64_t py_alloc_times_;
  uint64_t py_realloc_times_;
  uint64_t py_free_times_;
  int64_t py_created_time_;
  int64_t py_referece_times_;
};

class PyFunctionHelper
{
public:
  static PyFunctionHelper &get_instance();
  PyFunctionHelper(const PyFunctionHelper &) = delete;
  PyFunctionHelper &operator=(const PyFunctionHelper &) = delete;
  bool is_inited() { return is_inited_; }
  int do_init();
  static PyThreadState *get_main_state() { return get_instance().main_state_; }
private:
  PyFunctionHelper()
  {
    int ret = OB_SUCCESS;
    if (is_inited_) {
      // do nothing
    } else if (OB_FAIL(do_init())) {
      is_inited_ = false;
    } else {
      is_inited_ = true;
    }
  }
  int init_py_env();
  int load_py_lib(ObPyEnvContext &ctx);
  int open_py_lib(ObPyEnvContext &py_env_ctx);
  int get_py_path(char *path, uint64_t length);
  int search_dir_file(const char *path, const char *file_name, bool &found);
  void *ob_alloc_py(ObPyEnvContext &ctx, int64_t size);
  void ob_free_py(ObPyEnvContext &ctx, void *ptr);
  int convert_to_wchar(const char *str, wchar_t *&wstr);
  int load_builtins();
  int check_py_version();
private:
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  static PyThreadState *main_state_;
  PyConfig config_;
  ObPyEnvContext py_ctx_;
  void *py_lib_handler_;
  lib::ObMutex lock_;
  obsys::ObRWLock<> load_lib_lock_;
};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_EXTERNAL_ROUTINE_CPYTHON_HELPER_H_
