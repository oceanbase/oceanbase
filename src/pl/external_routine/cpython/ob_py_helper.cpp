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

#define USING_LOG_PREFIX PL

#include <cstdlib>
#include <dirent.h>

#include "ob_py_helper.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/ob_errno.h"
#include "lib/allocator/page_arena.h"
#include "share/config/ob_server_config.h"
#include "lib/oblog/ob_log_module.h"


// Python type objects
PyObject* ob_py_builtins_module = nullptr;
PyObject* ob_py_bool_type = nullptr;
PyObject* ob_py_int_type = nullptr;
PyObject* ob_py_str_type = nullptr;
PyObject* ob_py_float_type = nullptr;
PyObject* ob_py_bytes_type = nullptr;

PyConfig_SetString_Func ob_py_config_set_string = nullptr;
PyConfig_InitIsolatedConfig_Func ob_py_config_init_isolated_config = nullptr;
PyStatus_Exception_Func ob_py_status_exception = nullptr;
Py_InitializeFromConfig_Func ob_py_initialize_from_config = nullptr;
PyConfig_Clear_Func ob_py_config_clear = nullptr;

Py_Initialize_Func ob_py_initialize = nullptr;
Py_IsInitialized_Func ob_py_is_initialized = nullptr;
PyThreadState_Swap_Func ob_py_thread_state_swap = nullptr;
Py_Finalize_Func   ob_py_finilize = nullptr;
PyEval_SaveThread_Func ob_py_save_thread = nullptr;
PyEval_RestoreThread_Func ob_py_restore_thread = nullptr;
PyEval_AcquireThread_Func ob_py_acquire_thread = nullptr;
PyEval_ReleaseThread_Func ob_py_release_thread = nullptr;
Py_NewInterpreter_Func ob_py_new_interpreter = nullptr;
Py_NewInterpreterFromConfig_Func ob_py_new_interpreter_from_config = nullptr;
Py_EndInterpreter_Func ob_py_end_interpreter = nullptr;
PyUnicode_DecodeFSDefault_Func ob_py_unicode_decode_fs_default = nullptr;
PyImport_Import_Func ob_py_import_import = nullptr;
PyImport_ImportModule_Func ob_py_import_import_module = nullptr;
PyObject_GetAttrString_Func ob_py_object_get_attr_string = nullptr;
PyObject_SetAttrString_Func ob_py_object_set_attr_string = nullptr;
PyCallable_Check_Func ob_py_callable_check = nullptr;
Py_DecRef_Func ob_py_dec_ref = nullptr;
PyTuple_Pack_Func ob_py_tuple_pack = nullptr;
PyTuple_New_Func ob_py_tuple_new = nullptr;
PyTuple_SetItem_Func ob_py_tuple_set_item = nullptr;
PyObject_CallObject_Func ob_py_object_call_object = nullptr;
PyObject_CallMethod_Func ob_py_object_call_method = nullptr;
PyObject_CallFunction_Func ob_py_object_call_function = nullptr;

PyErr_Occurred_Func ob_py_err_occurred = nullptr;
PyErr_Fetch_Func ob_py_err_fetch = nullptr;
PyObject_Str_Func ob_py_object_str = nullptr;
PyErr_GetRaisedException_Func ob_py_err_get_raised_exception = nullptr;
PyErr_SetObject_Func ob_py_err_set_object = nullptr;
PyCFunction_New_Func ob_py_cfunction_new = nullptr;
PyModule_Create2_Func ob_py_module_create2 = nullptr;
PyModuleDef_Init_Func ob_py_module_def_init = nullptr;
PyImport_AddModule_Func ob_import_add_module = nullptr;
PyModule_New_Func ob_py_module_new = nullptr;
PyImport_GetModuleDict_Func ob_py_import_get_module_dict = nullptr;
PyDict_SetItemString_Func ob_py_dict_set_item_string = nullptr;
PyDict_GetItemString_Func ob_py_dict_get_item_string = nullptr;
PyModule_GetDict_Func ob_py_module_get_dict = nullptr;
PyRun_String_Func ob_py_run_string = nullptr;
PyRun_SimpleString_Func ob_py_run_simple_string = nullptr;

// pyhton type
PyBool_FromLong_Func ob_py_bool_from_long = nullptr;
PyLong_FromLong_Func ob_py_long_from_long = nullptr;
PyLong_AsLong_Func ob_py_long_as_long = nullptr;
PyFloat_FromDouble_Fun ob_py_float_from_double = nullptr;
PyUnicode_FromStringAndSize_Func ob_py_unicode_from_string_and_size = nullptr;
PyBytes_FromStringAndSize_Func ob_py_bytes_from_string_and_size = nullptr;
PyUnicode_FromString_Func ob_py_unicode_from_string = nullptr;
PyObject_IsTrue_Func ob_py_object_is_true = nullptr;
PyUnicode_AsUTF8AndSize_Func ob_py_unicode_as_utf8_and_size = nullptr;
PyUnicode_AsUTF8_Func ob_py_unicode_as_utf8 = nullptr;
PyBytes_Size_Func ob_py_bytes_size = nullptr;
PyBytes_AsString_Func ob_py_bytes_as_string = nullptr;
PyFloat_AsDouble_Func ob_py_float_as_double = nullptr;
PyObject_IsInstance_Func ob_py_object_is_instance = nullptr;
PyNoneStructPtr ob_py_none_ptr = nullptr;
PyExc_RuntimeErrorPtr ob_py_exection_runtime_error_ptr = nullptr;
Py_VersionPtr ob_py_version_ptr = nullptr;


namespace oceanbase
{

using namespace common;

namespace pl
{

PyThreadState* PyFunctionHelper::main_state_ = nullptr;

PyFunctionHelper &PyFunctionHelper::get_instance()
{
  static PyFunctionHelper helper;
  return helper;
}

int PyFunctionHelper::do_init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    // do nothing
  } else if (OB_FAIL(init_py_env())) {
    LOG_WARN("failed to init python env");
  } else {
    is_inited_ = true;
  }
  return ret;
}

int PyFunctionHelper::init_py_env()
{
  int ret = OB_SUCCESS;
  LockGuard guard(lock_);
  if (OB_FAIL(load_py_lib(py_ctx_))) {
    LOG_WARN("failed to load python lib", K(ret));
  } else if (OB_ISNULL(main_state_)) {
    ob_py_config_init_isolated_config(&config_);
    config_.isolated = 1;
    config_.configure_c_stdio = 0;
    wchar_t *config_home = nullptr;

    if (OB_ISNULL(GCONF.ob_python_home) || GCONF.ob_python_home.get_value_string().empty()) {
      ret = OB_PYTHON_PARAMS_ERROR;
      LOG_USER_ERROR(OB_PYTHON_PARAMS_ERROR, "ob_python_home was not configured");
    } else if (OB_FAIL(convert_to_wchar(GCONF.ob_python_home.get_value_string().ptr(), config_home))) {
      LOG_WARN("failed to convert_to_wchar", K(ret));
    } else {
      PyStatus status;
      status = ob_py_config_set_string(&config_, &config_.home, config_home);
      if (ob_py_status_exception(status)) {
        ob_py_config_clear(&config_);
        ret = OB_PYTHON_PARAMS_ERROR;
        LOG_WARN("failed to set config", K(ret), K(ObString(status.err_msg)), K(GCONF.ob_python_home.get_value_string()));
        LOG_USER_ERROR(OB_PYTHON_PARAMS_ERROR, status.err_msg);
      }
      if (OB_SUCC(ret)) {
        status = ob_py_initialize_from_config(&config_);
        if (ob_py_status_exception(status)) {
          ob_py_config_clear(&config_);
          ret = OB_PYTHON_PARAMS_ERROR;
          LOG_WARN("failed to set config", K(ret), K(GCONF.ob_python_home.get_value_string()), K(ObString(status.err_msg)));
          LOG_USER_ERROR(OB_PYTHON_PARAMS_ERROR, status.err_msg);
        }
        if (FAILEDx(load_builtins())) {
          LOG_WARN("failed to load builtins", K(ret));
        }
        if (OB_SUCC(ret)) {
          main_state_ = ob_py_save_thread();
          if (nullptr == main_state_) {
            ret = OB_PYTHON_ENV_ERROR;
            LOG_WARN("could not get main python state", K(ret), K(lbt()));
          }
        }
      }
    }
  }
  return ret;
}

int PyFunctionHelper::load_py_lib(ObPyEnvContext &ctx)
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard wg(load_lib_lock_);

  if (ctx.is_valid()) {
    // do nothing
    LOG_TRACE("already load python lib", K(ret));
  } else if (ctx.py_loaded_) {
    // do nothing
    LOG_TRACE("already load python lib", K(ret));
  } else if (OB_FAIL(open_py_lib(ctx))) {
    LOG_WARN("failed to open pyhton lib", K(ret));
  } else {
#define LOAD_FUNC_PTR(func_name, func_ptr, func_ptr_type)                           \
  do {                                                                              \
    if (OB_SUCC(ret)) {                                                             \
      LIB_SYMBOL(py_lib_handler_, func_name, func_ptr, func_ptr_type);              \
      if (OB_ISNULL(func_ptr)) {                                                    \
        ret = OB_PYTHON_ENV_ERROR;                                                  \
        LOG_USER_ERROR(OB_PYTHON_PARAMS_ERROR, "Python lib version may be error");  \
      }                                                                             \
    }                                                                               \
  } while (0)

    LOAD_FUNC_PTR("PyConfig_SetString", ob_py_config_set_string, PyConfig_SetString_Func);
    LOAD_FUNC_PTR("PyConfig_InitIsolatedConfig", ob_py_config_init_isolated_config, PyConfig_InitIsolatedConfig_Func);
    LOAD_FUNC_PTR("PyStatus_Exception", ob_py_status_exception, PyStatus_Exception_Func);
    LOAD_FUNC_PTR("PyConfig_Clear", ob_py_config_clear, PyConfig_Clear_Func);
    LOAD_FUNC_PTR("Py_InitializeFromConfig", ob_py_initialize_from_config, Py_InitializeFromConfig_Func);
    LOAD_FUNC_PTR("Py_IsInitialized", ob_py_is_initialized, Py_IsInitialized_Func);
    LOAD_FUNC_PTR("PyThreadState_Swap", ob_py_thread_state_swap, PyThreadState_Swap_Func);
    LOAD_FUNC_PTR("Py_Initialize", ob_py_initialize, Py_Initialize_Func);
    LOAD_FUNC_PTR("Py_Finalize", ob_py_finilize, Py_Finalize_Func);
    LOAD_FUNC_PTR("PyEval_SaveThread", ob_py_save_thread, PyEval_SaveThread_Func);
    LOAD_FUNC_PTR("PyEval_RestoreThread", ob_py_restore_thread, PyEval_RestoreThread_Func);
    LOAD_FUNC_PTR("PyEval_AcquireThread", ob_py_acquire_thread, PyEval_AcquireThread_Func);
    LOAD_FUNC_PTR("PyEval_ReleaseThread", ob_py_release_thread, PyEval_ReleaseThread_Func);
    LOAD_FUNC_PTR("Py_NewInterpreter", ob_py_new_interpreter, Py_NewInterpreter_Func);
    LOAD_FUNC_PTR("Py_NewInterpreterFromConfig", ob_py_new_interpreter_from_config, Py_NewInterpreterFromConfig_Func);
    LOAD_FUNC_PTR("Py_EndInterpreter", ob_py_end_interpreter, Py_EndInterpreter_Func);
    LOAD_FUNC_PTR("PyUnicode_DecodeFSDefault", ob_py_unicode_decode_fs_default, PyUnicode_DecodeFSDefault_Func);
    LOAD_FUNC_PTR("PyImport_Import", ob_py_import_import, PyImport_Import_Func);
    LOAD_FUNC_PTR("PyImport_ImportModule", ob_py_import_import_module, PyImport_ImportModule_Func);
    LOAD_FUNC_PTR("PyObject_GetAttrString", ob_py_object_get_attr_string, PyObject_GetAttrString_Func);
    LOAD_FUNC_PTR("PyObject_SetAttrString", ob_py_object_set_attr_string, PyObject_SetAttrString_Func);
    LOAD_FUNC_PTR("PyCallable_Check", ob_py_callable_check, PyCallable_Check_Func);
    LOAD_FUNC_PTR("Py_DecRef", ob_py_dec_ref, Py_DecRef_Func);
    LOAD_FUNC_PTR("PyTuple_Pack", ob_py_tuple_pack, PyTuple_Pack_Func);
    LOAD_FUNC_PTR("PyTuple_New", ob_py_tuple_new, PyTuple_New_Func);
    LOAD_FUNC_PTR("PyTuple_SetItem", ob_py_tuple_set_item, PyTuple_SetItem_Func);
    LOAD_FUNC_PTR("PyObject_CallObject", ob_py_object_call_object, PyObject_CallObject_Func);
    LOAD_FUNC_PTR("PyObject_CallMethod", ob_py_object_call_method, PyObject_CallMethod_Func);
    LOAD_FUNC_PTR("PyObject_CallFunction", ob_py_object_call_function, PyObject_CallFunction_Func);

    LOAD_FUNC_PTR("PyErr_Occurred", ob_py_err_occurred, PyErr_Occurred_Func);
    LOAD_FUNC_PTR("PyErr_Fetch", ob_py_err_fetch, PyErr_Fetch_Func);
    LOAD_FUNC_PTR("PyObject_Str", ob_py_object_str, PyObject_Str_Func);
    LOAD_FUNC_PTR("PyErr_GetRaisedException", ob_py_err_get_raised_exception, PyErr_GetRaisedException_Func);
    LOAD_FUNC_PTR("PyErr_SetObject", ob_py_err_set_object, PyErr_SetObject_Func);
    LOAD_FUNC_PTR("PyCFunction_New", ob_py_cfunction_new, PyCFunction_New_Func);


    LOAD_FUNC_PTR("PyModule_Create2", ob_py_module_create2, PyModule_Create2_Func);
    LOAD_FUNC_PTR("PyModuleDef_Init", ob_py_module_def_init, PyModuleDef_Init_Func);
    LOAD_FUNC_PTR("PyImport_AddModule", ob_import_add_module, PyImport_AddModule_Func);
    LOAD_FUNC_PTR("PyModule_New", ob_py_module_new, PyModule_New_Func);
    LOAD_FUNC_PTR("PyImport_GetModuleDict", ob_py_import_get_module_dict, PyImport_GetModuleDict_Func);
    LOAD_FUNC_PTR("PyDict_SetItemString", ob_py_dict_set_item_string, PyDict_SetItemString_Func);
    LOAD_FUNC_PTR("PyDict_GetItemString", ob_py_dict_get_item_string, PyDict_GetItemString_Func);
    LOAD_FUNC_PTR("PyModule_GetDict", ob_py_module_get_dict, PyModule_GetDict_Func);
    LOAD_FUNC_PTR("PyRun_String", ob_py_run_string, PyRun_String_Func);
    LOAD_FUNC_PTR("PyRun_SimpleString", ob_py_run_simple_string, PyRun_SimpleString_Func);

    LOAD_FUNC_PTR("PyBool_FromLong", ob_py_bool_from_long, PyBool_FromLong_Func);
    LOAD_FUNC_PTR("PyLong_FromLong", ob_py_long_from_long, PyLong_FromLong_Func);
    LOAD_FUNC_PTR("PyLong_AsLong", ob_py_long_as_long, PyLong_AsLong_Func);
    LOAD_FUNC_PTR("PyFloat_FromDouble", ob_py_float_from_double, PyFloat_FromDouble_Fun);
    LOAD_FUNC_PTR("PyUnicode_FromStringAndSize", ob_py_unicode_from_string_and_size, PyUnicode_FromStringAndSize_Func);
    LOAD_FUNC_PTR("PyBytes_FromStringAndSize", ob_py_bytes_from_string_and_size, PyBytes_FromStringAndSize_Func);
    LOAD_FUNC_PTR("PyUnicode_FromString", ob_py_unicode_from_string, PyUnicode_FromString_Func);
    LOAD_FUNC_PTR("PyObject_IsTrue", ob_py_object_is_true, PyObject_IsTrue_Func);
    LOAD_FUNC_PTR("PyUnicode_AsUTF8AndSize", ob_py_unicode_as_utf8_and_size, PyUnicode_AsUTF8AndSize_Func);
    LOAD_FUNC_PTR("PyUnicode_AsUTF8", ob_py_unicode_as_utf8, PyUnicode_AsUTF8_Func);
    LOAD_FUNC_PTR("PyBytes_Size", ob_py_bytes_size, PyBytes_Size_Func);
    LOAD_FUNC_PTR("PyBytes_AsString", ob_py_bytes_as_string, PyBytes_AsString_Func);
    LOAD_FUNC_PTR("PyFloat_AsDouble", ob_py_float_as_double, PyFloat_AsDouble_Func);
    LOAD_FUNC_PTR("PyObject_IsInstance", ob_py_object_is_instance, PyObject_IsInstance_Func);
    LOAD_FUNC_PTR("_Py_NoneStruct", ob_py_none_ptr, PyNoneStructPtr);
    LOAD_FUNC_PTR("PyExc_RuntimeError", ob_py_exection_runtime_error_ptr, PyExc_RuntimeErrorPtr);
    LOAD_FUNC_PTR("Py_Version", ob_py_version_ptr, Py_VersionPtr);

    LOG_TRACE("succ to open python lib", K(ret));
#undef LOAD_FUNC_PTR
    if (FAILEDx(check_py_version())) {
      LOG_WARN("failed to check python version", K(ret));
    }
    if (OB_SUCC(ret)) {
      ctx.py_loaded_ = true;
    } else {
      LIB_CLOSE(py_lib_handler_);
      py_lib_handler_ = nullptr;
    }
  }
  return ret;
}

int PyFunctionHelper::open_py_lib(ObPyEnvContext &py_env_ctx)
{
  int ret = OB_SUCCESS;
  char *py_lib_buf = nullptr;

  int64_t load_lib_size = 4096; // 4k is enough for library path on `ext4` file system.
  py_lib_handler_ = nullptr;
  if (OB_ISNULL(py_lib_buf = static_cast<char *>(ob_alloc_py(py_env_ctx, load_lib_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate python env for python lib", K(ret));
  } else if (OB_FAIL(get_py_path(py_lib_buf, load_lib_size))) {
    LOG_WARN("failed to get python path", K(ret));
  } else if (OB_ISNULL(py_lib_handler_ = LIB_OPEN(py_lib_buf))) {
    ret = OB_PYTHON_ENV_ERROR;
    const char * dlerror_str = dlerror();
    int dlerror_str_len = STRLEN(dlerror_str);
    LOG_WARN("failed to open python lib from path", K(ret), K(ObString(py_lib_buf)), K(ObString(dlerror_str)));
  } else {
    LOG_TRACE("use python lib from path", K(ret), K(ObString(py_lib_buf)));

    if (OB_FAIL(ret) && OB_NOT_NULL(py_lib_handler_)) {
      if (OB_PYTHON_ENV_ERROR == ret) {
        LOG_WARN("failed to open python lib handle", K(ret));
      }
      LIB_CLOSE(py_lib_handler_);
      py_lib_handler_ = nullptr;
      LOG_WARN("lib_close python lib", K(ret));
    }
  }

  if (OB_NOT_NULL(py_lib_buf)) {
    ob_free_py(py_env_ctx, py_lib_buf);
    py_lib_buf = nullptr;
  }

  return ret;
}

int PyFunctionHelper::get_py_path(char *path, uint64_t length)
{
  int ret = OB_SUCCESS;
  const char *env_var_name = "LD_LIBRARY_PATH";
  const char *lib_name = "libpython3.so";
  char *env_str = getenv(env_var_name);
  LOG_INFO("LD_LIBRARY_PATH: ", K(ObString(env_str)));
  bool found = false;

  if (OB_ISNULL(env_str)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cann't find lib in LD_LIBRARY_PATH", K(ret));
  } else {
    const char *ptr = env_str;
    while (' ' == *ptr || ':' == *ptr) { ++ptr; }
    int64_t env_str_len = STRLEN(ptr);
    int64_t idx = 0;
    while (OB_SUCC(ret) && idx < env_str_len) {
      const char *dir_ptr = ptr + idx;
      int64_t inner_idx = 0;
      while((idx + inner_idx) < env_str_len && ':' != ptr[idx + inner_idx]) {
        ++inner_idx;
      }
      int64_t dir_len = inner_idx;
      while(' ' == *dir_ptr) {
        ++dir_ptr;
        --dir_len;
      }
      if (dir_len > 0 && OB_NOT_NULL(dir_ptr)) {
        ObString dir_name(dir_len, dir_ptr);
        strncpy(path, dir_name.ptr(), dir_name.length());
        path[dir_name.length()] = 0;
        found = false;
        if (OB_FAIL(search_dir_file(path, lib_name, found))) {
          LOG_WARN("failed to searche dir file, continue to find next dir", K(ret), K(path), K(lib_name));
          ret = OB_SUCCESS;
        } else if (found) {
          ObSqlString tmp_path;
          if (OB_FAIL(tmp_path.append(dir_name))) {
          LOG_WARN("failed to append dir path", K(dir_name), K(ret));
          } else if (OB_FAIL(tmp_path.append_fmt("/%s", lib_name))) {
            LOG_WARN("failed to append lib name", K(lib_name), K(ret));
          } else if (tmp_path.length() >= length) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to path length large than expected", K(ret));
          } else {
            strncpy(path, tmp_path.ptr(), tmp_path.length());
            path[tmp_path.length()] = 0;
          }
          break;
        }
      }
      if (idx + inner_idx == env_str_len) {
        idx += inner_idx;
      } else {
        idx += inner_idx + 1; // skip ':'
      }
    }
    if (!found) {
      const char *default_path = "/home/admin/oceanbase/lib";
      const char *default_lib_path = "";
      if (OB_FAIL(search_dir_file(default_path, lib_name, found))) {
        LOG_WARN("failed to searche dir file, continue to fine next dir", K(ret));
        ret = OB_SUCCESS;
      } else if (found) {
        const int64_t default_py_path_len = STRLEN(default_lib_path);
        if (default_py_path_len >= length) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to path length large than expected", K(ret));
        } else {
          strncpy(path, default_lib_path, default_py_path_len);
          path[default_py_path_len] = 0;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to find path of libpython3.so", K(ret), K(found), K(ObString(env_str)));
  } else if (!found) {
    ret = OB_PYTHON_ENV_ERROR;
    const char *user_error_str = "cant not find lib path";
    int user_error_len = STRLEN(user_error_str);
    LOG_WARN("failed to find path", K(ret), K(ObString(env_str)), K(lib_name));
  } else {
    LOG_TRACE("succ to find path", K(ret), K(ObString(env_str)), K(path), K(lib_name));
  }

  return ret;
}

int PyFunctionHelper::search_dir_file(const char *path, const char *file_name, bool &found)
{
  int ret = OB_SUCCESS;
  DIR *dirp = NULL;
  dirent *dp = NULL;
  found = false;
  if (OB_ISNULL(path) || OB_ISNULL(file_name)) {
    found = false;
  } else if (NULL == (dirp = opendir(path))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cann't open dir path", K(ObString(path)), K(ret));
  } else {
    while (OB_NOT_NULL(dp = readdir(dirp))) {
      // symbolic link or regular file
      if (DT_UNKNOWN == dp->d_type || DT_LNK == dp->d_type || DT_REG == dp->d_type) {
        if (0 == strcasecmp(file_name, dp->d_name)) {
          found = true;
          break;
        }
      }
    }
  }
  if (OB_NOT_NULL(dirp)) {
    if (0 != closedir(dirp)) {
      LOG_WARN("failed to close dirp", K(ret), KP(dirp));
    }
  }
  return ret;
}

void *PyFunctionHelper::ob_alloc_py(ObPyEnvContext &ctx, int64_t size)
{
  void *ptr = nullptr;
  if (size != 0) {
    ObMemAttr attr;
    attr.label_ = "ob_alloc_python";
    attr.ctx_id_ = ObCtxIds::DEFAULT_CTX_ID;
    SET_IGNORE_MEM_VERSION(attr);
    {
      ptr = ob_malloc(size, attr);
    }
    if (NULL == ptr) {
      _OB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "ob_tc_malloc failed, size:%lu", size);
    } else {
      ctx.py_mem_bytes_ += size;
      ctx.py_alloc_times_ += 1;
    }
  }
  return ptr;
}

void PyFunctionHelper::ob_free_py(ObPyEnvContext &ctx, void *ptr)
{
  if (nullptr == ptr) {
    // do nothing
  } else {
    ob_free(ptr);
    ctx.py_free_times_ += 1;
  }
}

int PyFunctionHelper::convert_to_wchar(const char *str, wchar_t *&wstr)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(str)) {
    size_t len = STRLEN(str) + 1;
    wstr = nullptr;
    wstr = static_cast<wchar_t *>(allocator_.alloc(len * sizeof(wchar_t)));
    if (OB_ISNULL(wstr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(len));
    } else {
      if (std::mbstowcs(wstr, str, len) == static_cast<size_t>(-1)) {
          allocator_.free(wstr);
          wstr = nullptr;
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to convert to wchar_t", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("str is null", K(ret));
  }
  return ret;
}

int PyFunctionHelper::load_builtins()
{
  int ret = OB_SUCCESS;
#define RET_ERROR(NAME) \
  do {  \
    ret = OB_ERR_UNEXPECTED; \
    LOG_WARN("python object is NULL", K(ret), K(ObString(NAME))); \
  } while (0)

  ob_py_builtins_module = ob_py_import_import_module("builtins");
  if (OB_ISNULL(ob_py_builtins_module)) {
    RET_ERROR("builtins");
  } else if (OB_ISNULL(ob_py_bool_type = ob_py_object_get_attr_string(ob_py_builtins_module, "bool"))) {
    RET_ERROR("bool");
  } else if (OB_ISNULL(ob_py_int_type = ob_py_object_get_attr_string(ob_py_builtins_module, "int"))) {
    RET_ERROR("int");
  } else if (OB_ISNULL(ob_py_str_type = ob_py_object_get_attr_string(ob_py_builtins_module, "str"))) {
    RET_ERROR("str");
  } else if (OB_ISNULL(ob_py_float_type = ob_py_object_get_attr_string(ob_py_builtins_module, "float"))) {
    RET_ERROR("float");
  } else if (OB_ISNULL(ob_py_bytes_type = ob_py_object_get_attr_string(ob_py_builtins_module, "bytes"))) {
    RET_ERROR("bytes");
  }
  return ret;
}

int PyFunctionHelper::check_py_version()
{
  int ret = OB_SUCCESS;
  const unsigned long full_version = *ob_py_version_ptr;
  int major = (int) (full_version >> 24 & 0xFF);
  int minor = (int) (full_version >> 16 & 0xFF);
  int micro = (int) (full_version >> 8 & 0xFF);
  if (major != 3 || minor != 13 || micro != 3) {
    ret = OB_PYTHON_PARAMS_ERROR;
    LOG_USER_ERROR(OB_PYTHON_PARAMS_ERROR, "Python lib version must be 3.13.3, other version");
  }
  LOG_INFO("Python lib version is", K(major), K(minor), K(micro));
  return ret;
}

}
}
