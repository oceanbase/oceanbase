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

#include "ob_py_utils.h"
#include "share/datum/ob_datum.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_sql_string.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{

namespace pl
{

static ObPyObject *forbid_funcs(ObPyObject* self, ObPyObject* args)
{
  ObPyObject* msg = ob_py_unicode_from_string("open() function is disabled.");
  ob_py_err_set_object(*ob_py_exection_runtime_error_ptr, msg);
  ObPyUtils::xdec_ref(msg);
  return nullptr;
}

static struct PyMethodDef MyMethods[] = {
    {"open", forbid_funcs, METH_VARARGS | METH_KEYWORDS, "forbid open function."},
    {NULL, NULL, 0, NULL}  // Sentinel
};

int ObPySubInterContext::create_odps_module()
{
  int ret = OB_SUCCESS;
  const char *code =
        "def annotate(type_signature):\n"
        "    def decorator(cls):\n"
        "        param_str, return_str = type_signature.split('->')\n"
        "        param_types = [t.strip() for t in param_str.split(',')]\n"
        "        return_type = return_str.strip()\n"
        "        for attr_name, attr_value in cls.__dict__.items():\n"
        "            if callable(attr_value) and attr_name == 'evaluate':\n"
        "                attr_value.__annotations__ = {}\n"
        "                for i, param_type in enumerate(param_types):\n"
        "                    attr_value.__annotations__[f'param_{i}'] = param_type\n"
        "                attr_value.__annotations__['return'] = return_type\n"
        "                break\n"
        "        return cls\n"
        "    return decorator\n";
  ObPyObject *odps_module = nullptr;
  ObPyObject *udf_module = nullptr;
  ObPyObject *dict = nullptr;
  ObPyObject *exec_result = nullptr;
  if (OB_ISNULL(odps_module = ob_import_add_module("odps"))) { // borrowed ref
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create odps module failed", K(ret));
  } else if (OB_ISNULL(udf_module = ob_import_add_module("odps.udf"))) { // borrowed ref
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create odps udf module failed", K(ret));
  } else if (FALSE_IT(ob_py_object_set_attr_string(odps_module, "udf", udf_module))) {
  } else if (OB_ISNULL(dict = ob_py_module_get_dict(udf_module))) { // borrowed ref
    ret = OB_PYTHON_PARAMS_ERROR;
    LOG_WARN("get odps dict failed", K(ret));
  } else if (OB_ISNULL(exec_result = ob_py_run_string(code, Py_file_input, dict, dict))) {
    LOG_INFO("failed to run odps string", K(ret));
    if (OB_FAIL(ObPyUtils::exception_check())) {
      LOG_WARN("failed to check exception", K(ret));
    }
  }
  if (nullptr != exec_result) {
     ObPyUtils::xdec_ref(exec_result);
     exec_result = nullptr;
  }
  return ret;
}


ObPySubInterContext::~ObPySubInterContext()
{
  is_inited_ = false;
  acquire_times_ = 0;
  release_times_ = 0;
  for (ModuleMap::iterator it = module_map_.begin(); it != module_map_.end(); ++it) {
    ObPyUtils::xdec_ref(it->second);
    it->second = nullptr;
  }
  module_map_.destroy();
  LOG_INFO("destroy sub tstate", K(sub_tstate_));
  if (sub_tstate_ != nullptr) {
    // ob_py_acquire_thread(sub_tstate_);
    ob_py_thread_state_swap(sub_tstate_);
    ob_py_end_interpreter(sub_tstate_);
    ob_py_thread_state_swap(nullptr);
    sub_tstate_ = nullptr;
  }
}

int ObPySubInterContext::init()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start init sub inter", K(ret));
  if (OB_FAIL(module_map_.create(8, ObMemAttr(MTL_ID(), "PlPythonModule")))) {
    LOG_WARN("failed to create module map", K(ret));
  } else {
    ob_py_thread_state_swap(PyFunctionHelper::get_main_state());
    ObPyThreadState *tstate = ob_py_new_interpreter();
    if (OB_ISNULL(tstate)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new python sub interpreter failed", K(ret));
    } else {
      ob_py_run_simple_string(
        "import sys\n"
        "sys.modules['os'] = None\n"
        "sys.modules['io'] = None\n"
        "sys.modules['socket'] = None\n"
        "sys.modules['subprocess'] = None\n"
      );
      ObPyObject *decimal = nullptr;
      ObPyObject *datetime = nullptr;
      ObPyObject *builtins = nullptr;
      if (OB_FAIL(import_module(ObPyUtils::decimal_module_name, decimal))) {
        LOG_WARN("import module failed", K(ret), K(ObPyUtils::decimal_module_name));
      } else if (OB_FAIL(import_module(ObPyUtils::datetime_module_name, datetime))) {
        LOG_WARN("import module failed", K(ret), K(ObPyUtils::datetime_module_name));
      } else if (OB_FAIL(import_module(ObPyUtils::builtins_module_name, builtins))) {
        LOG_WARN("import module failed", K(ret), K(ObPyUtils::builtins_module_name));
      } else if (OB_ISNULL(builtins)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get builtins", K(ret));
      } else {
        ObPyObject *pCustomOpenFunc = ob_py_cfunction_new(MyMethods, nullptr);
        ob_py_object_set_attr_string(builtins, "open", pCustomOpenFunc);
        ObPyUtils::xdec_ref(pCustomOpenFunc);
        if (OB_FAIL(create_odps_module())) {
          LOG_WARN("create odps module failed", K(ret));
        } else {
          is_inited_ = true;
          inc_acquire_times();
          sub_tstate_ = tstate;
          // ob_py_release_thread(sub_tstate_);
          inc_release_times();
        }
      }
    }
  }
  return ret;
}

int ObPySubInterContext::import_module(const ObString &module_name, ObPyObject *&py_module)
{
  int ret = OB_SUCCESS;
  ObPyObject *tmp = ob_py_import_import_module(module_name.ptr());
  if (tmp == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("import module failed", K(ret), K(module_name));
  } else if (OB_FAIL(module_map_.set_refactored(module_name, tmp))) {
    LOG_WARN("failed to set module map", K(ret), K(module_name));
    ObPyUtils::xdec_ref(tmp);
    tmp = nullptr;
  } else {
    py_module = tmp;
  }
  return ret;
}

ObPyThreadState *ObPySubInterContext::acquire_sub_inter()
{
  // LOG_INFO("before acquire_thread");
  // ob_py_acquire_thread(sub_tstate_);
  // // swap current thread state to sub interpreter
  ob_py_thread_state_swap(sub_tstate_);
  // inc_acquire_times();
  // LOG_INFO("after acquire_thread");
  return sub_tstate_;
}

void ObPySubInterContext::release_sub_inter()
{
  ob_py_release_thread(sub_tstate_);
  ob_py_thread_state_swap(nullptr);
  inc_release_times();
}


int ObPyUtils::load_routine_py(ObPyThreadState *tstate,
                               const ObString &py_source,
                               const int64_t udf_id,
                               const ObString &func_name,
                               ObPyObject *&py_func)
{
  int ret = OB_SUCCESS;
  ObPyObject *py_module = nullptr;
  ObPyObject *sys_module = nullptr;
  ObPyObject *dict = nullptr;
  ObPyObject *exec_result = nullptr;
  ObPyObject *cls_obj = nullptr;

  ObString cls_name = func_name;
  ObString module_name;
  ObSqlString auto_module_name;
  if (func_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "Python UDF symbol");
  } else if (!cls_name.contains(cls_name.find('.'))) {
    if (OB_FAIL(auto_module_name.append_fmt("inner_auto_py_udf_%ld", udf_id))) {
      LOG_WARN("failed to append fmt", K(ret));
    } else {
      module_name = auto_module_name.string();
      LOG_INFO("auto module name", K(ret), K(module_name));
    }
  } else if (FALSE_IT(module_name = cls_name.split_on('.'))) {
  } else if (cls_name.contains(cls_name.find('.'))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("func_name is invalid", K(ret), K(cls_name), K(func_name));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Python UDF symbol must be format as {module_name.func_name} or {func_name}, other format");
  }
  if (OB_FAIL(ret)) {
  } else if (nullptr == tstate) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tstate is null", K(ret));
  } else if (OB_ISNULL(py_module = ob_py_module_new(module_name.ptr()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to new py module", K(ret), K(module_name), K(func_name));
  } else if (OB_ISNULL(sys_module = ob_py_import_get_module_dict())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys_module is null", K(ret), K(module_name), K(func_name));
  } else if (0 != ob_py_dict_set_item_string(sys_module, module_name.ptr(), py_module)) {
    ret = OB_PYTHON_PARAMS_ERROR;
    LOG_WARN("failed to set item", K(ret), K(module_name), K(func_name));
  } else if (OB_ISNULL(dict = ob_py_module_get_dict(py_module))) {
    ret = OB_PYTHON_PARAMS_ERROR;
    LOG_WARN("failed to get dict", K(ret), K(module_name), K(func_name));
  } else if (OB_ISNULL(exec_result = ob_py_run_string(py_source.ptr(), Py_file_input, dict, dict))) {
    LOG_INFO("failed to run string", K(ret), K(module_name), K(func_name));
    if (OB_FAIL(ObPyUtils::exception_check())) {
      LOG_WARN("failed to check exception", K(ret));
    }
  } else if (OB_ISNULL(cls_obj = ob_py_dict_get_item_string(dict, cls_name.ptr()))) {
    LOG_INFO("cls_obj is null", K(ret), K(module_name), K(func_name));
    if (OB_FAIL(ObPyUtils::exception_check())) {
      LOG_WARN("failed to check exception", K(ret));
    }
  } else if (0 == ob_py_callable_check(cls_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cls_obj is not callable", K(ret), K(module_name), K(func_name));
  } else {
    py_func = cls_obj;
  }
  if (nullptr != exec_result) {
     xdec_ref(exec_result);
     exec_result = nullptr;
  }

  return ret;
}

int ObPyUtils::exception_check()
{
  int ret = OB_PYTHON_EXEC_ERROR;
  if (ob_py_err_occurred()) {
    ObPyObject *exc = ob_py_err_get_raised_exception();
    ObPyObject *str_exc = ob_py_object_str(exc);
    // ret = OB_PYTHON_EXEC_ERROR;
    if (str_exc) {
      const char *msg = ob_py_unicode_as_utf8(str_exc);
      LOG_USER_ERROR(OB_PYTHON_EXEC_ERROR, msg);
    } else {
      LOG_WARN("failed to run string", K(ret));
    }
    xdec_ref(str_exc);
    xdec_ref(exc);
  }

  return ret;
}

int ObPyUtils::ob_to_py_type(ObPySubInterContext &ctx,
                             const common::ObObj &obj,
                             ObPyObject *&py_obj)
{
  int ret = OB_SUCCESS;

  switch (obj.get_type()) {
    case ObNullType: {
      py_obj = ob_py_none();
      break;
    }
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      py_obj = ob_py_long_from_long(obj.v_.int64_);
      break;
    }
    case ObUTinyIntType:
    case ObUSmallIntType:
    case ObUMediumIntType:
    case ObUInt32Type:
    case ObUInt64Type: {
      py_obj = ob_py_long_from_long(obj.v_.uint64_);
      break;
    }
    case ObFloatType:
    case ObDoubleType: {
      double d = 0;
      if (obj.get_type() == ObFloatType) {
        d = (double)obj.v_.float_;
      } else {
        d = obj.v_.double_;
      }
      py_obj = ob_py_float_from_double(d);
      break;
    }
    case ObVarcharType:
    case ObCharType: {
      ObString v;
      if (OB_FAIL(obj.get_varchar(v))) {
        LOG_WARN("get varchar failed", K(ret));
      } else {
        if (obj.is_varbinary_or_binary()) {
          // PyBytes_FromStringAndSize
          py_obj = ob_py_bytes_from_string_and_size(v.ptr(), v.length());
        } else {
          py_obj = ob_py_unicode_from_string_and_size(v.ptr(), v.length());
        }
        if (NULL == py_obj) {
          if (OB_FAIL(ObPyUtils::exception_check())) {
            LOG_WARN("failed to check exception", K(ret));
          }
        }
      }
      break;
    }
    case ObNumberType: {
      ObArenaAllocator allocator;
      int64_t buf_len = common::number::ObNumber::MAX_TOTAL_SCALE;
      int64_t pos = 0;
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator.alloc(buf_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), K(buf_len));
      } else if (OB_FAIL(obj.get_number().format_v1((char *)buf, buf_len, pos, obj.get_scale()))) {
        LOG_WARN("failed to format number", K(ret), K(obj));
      } else {
        ((char *)buf)[pos] = 0;
        ObPyObject *py_str = ob_py_unicode_from_string_and_size((char *)buf, pos);
        ObPyObject *module = nullptr;
        ObPyObject *type = nullptr;
        ObPyObject *decimal = nullptr;
        if (OB_FAIL(ctx.get_module_map().get_refactored(decimal_module_name, module))) {
          LOG_WARN("failed to get module", K(ret), K(decimal_module_name));
        } else if (OB_ISNULL(module)) {
          LOG_WARN("module is null", K(ret), K(decimal_module_name));
        } else if (OB_ISNULL(type = ob_py_object_get_attr_string(module, decimal_type_name.ptr()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get decimal type", K(ret), K(decimal_type_name));
        } else {
          ObPyObject *arg = ob_py_tuple_pack(1, py_str);
          decimal = ob_py_object_call_object(type, arg);
          xdec_ref(arg);
        }
        xdec_ref(type);
        if (decimal != nullptr) {
          py_obj = decimal;
        }
      }
      allocator.free(buf);
      break;
    }
    case ObMySQLDateTimeType: {
      ObMySQLDateTime mdt = obj.get_mysql_datetime();
      int32_t y = mdt.year();
      int32_t m = mdt.month();
      int32_t d = (int32_t)mdt.day_;
      int32_t h = (int32_t)mdt.hour_;
      int32_t min = (int32_t)mdt.minute_;
      int32_t s = (int32_t)mdt.second_;
      int32_t us = (int32_t)mdt.microseconds_;
      ObPyObject *module = nullptr;
      ObPyObject *type = nullptr;
      ObPyObject *dt = nullptr;
      if (OB_FAIL(ctx.get_module_map().get_refactored(datetime_module_name, module))) {
        LOG_WARN("failed to get module", K(ret), K(datetime_module_name));
      } else if (OB_ISNULL(module)) {
        LOG_WARN("module is null", K(ret), K(datetime_module_name));
      } else if (OB_ISNULL(type = ob_py_object_get_attr_string(module, datetime_type_name.ptr()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get datetime type", K(ret), K(datetime_type_name));
      } else {
        dt = ob_py_object_call_function(type, "iiiiiii", y, m, d, h, min, s, us);
      }
      xdec_ref(type);
      if (dt != nullptr) {
        py_obj = dt;
      }
      break;
    }
    case ObMySQLDateType: {
      ObMySQLDate md = obj.get_mysql_date();
      int32_t y = (int32_t)md.year_;
      int32_t m = (int32_t)md.month_;
      int32_t d = (int32_t)md.day_;
      ObPyObject *module = nullptr;
      ObPyObject *type = nullptr;
      ObPyObject *dt = nullptr;
      if (OB_FAIL(ctx.get_module_map().get_refactored(datetime_module_name, module))) {
        LOG_WARN("failed to get module", K(ret), K(datetime_module_name));
      } else if (OB_ISNULL(module)) {
        LOG_WARN("module is null", K(ret), K(datetime_module_name));
      } else if (OB_ISNULL(type = ob_py_object_get_attr_string(module, date_type_name.ptr()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get datetime type", K(ret), K(date_type_name));
      } else {
        dt = ob_py_object_call_function(type, "iii", y, m, d);
      }
      xdec_ref(type);
      if (dt != nullptr) {
        py_obj = dt;
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported type", K(ret), K(obj.get_type()));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "type in Python UDF is");
    }
  }
  return ret;
}

int ObPyUtils::ob_from_py_type(ObPySubInterContext &ctx,
                               ObIAllocator &allocator,
                               ObPyObject *py_obj,
                               common::ObObj &obj,
                               const ObObjMeta &meta)
{
  int ret = OB_SUCCESS;
  ObObjType type = meta.get_type();
  if (py_obj == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("py_obj is null", K(ret));
  } else if (py_obj == ob_py_none()) {
    obj.set_null();
  } else {
#define GET_STRING_FROM_PY(STR_OBJ) \
  do {  \
    if (OB_SUCC(ret)) { \
      Py_ssize_t len = 0; \
      const char* c_str = ob_py_unicode_as_utf8_and_size(STR_OBJ, &len);  \
      ObString str; \
      if (OB_FAIL(ob_write_string(allocator, ObString(len, c_str), str))) { \
        LOG_WARN("failed to write string", K(ret), K(STR_OBJ)); \
      } else {  \
        obj.set_varchar(str); \
        obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI); \
      } \
    } \
  } while (0)

    switch (type)
    {
      case ObTinyIntType: {
        if (ob_py_object_is_instance(py_obj, ob_py_bool_type)) {
          obj.set_tinyint(ob_py_object_is_true(py_obj));
        } else if (ob_py_object_is_instance(py_obj, ob_py_int_type)) {
          int64_t v = (int64_t)ob_py_long_as_long(py_obj);
          // check range in `ObSPIService::spi_convert`
          obj.set_int(v);
        } else {
          ret = OB_ERR_EXPRESSION_WRONG_TYPE;
          LOG_WARN("py_obj is not bool type", K(ret), K(py_obj));
        }
        break;
      }
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType: {
        if (ob_py_object_is_instance(py_obj, ob_py_int_type)) {
          ObPyObject* str_obj = ob_py_object_str(py_obj);
          if (OB_ISNULL(str_obj)) {
            if (OB_FAIL(ObPyUtils::exception_check())) {
              LOG_WARN("failed to check exception", K(ret));
            }
          } else {
            GET_STRING_FROM_PY(str_obj);
          }
          xdec_ref(str_obj);
        } else {
          ret = OB_ERR_EXPRESSION_WRONG_TYPE;
          LOG_WARN("py_obj is not int type", K(ret), K(py_obj));
        }
        break;
      }
      case ObVarcharType:
      case ObCharType: {
        if (meta.is_varbinary_or_binary()) {
          if (ob_py_object_is_instance(py_obj, ob_py_bytes_type)) {
            Py_ssize_t len = ob_py_bytes_size(py_obj);
            const char *c_str = ob_py_bytes_as_string(py_obj);
            ObString str;
            if (OB_FAIL(ob_write_string(allocator, ObString(len, c_str), str))) {
              LOG_WARN("failed to write string", K(ret), K(py_obj));
            } else if (meta.is_binary()) {
              obj.set_binary(str);
            } else {
              obj.set_varbinary(str);
            }
          } else {
            ret = OB_ERR_EXPRESSION_WRONG_TYPE;
            LOG_WARN("py_obj is not bytes type", K(ret), K(py_obj));
          }
        } else if (ob_py_object_is_instance(py_obj, ob_py_str_type)) {
          GET_STRING_FROM_PY(py_obj);
        } else {
          ret = OB_ERR_EXPRESSION_WRONG_TYPE;
          LOG_WARN("py_obj is not str type", K(ret), K(py_obj));
        }
        break;
      }
      case ObFloatType:
      case ObDoubleType: {
        if (ob_py_object_is_instance(py_obj, ob_py_float_type)) {
          double d = ob_py_float_as_double(py_obj);
          if (type == ObFloatType) {
            obj.set_float((float)d);
          } else {
            obj.set_double(d);
          }
        } else {
          ret = OB_ERR_EXPRESSION_WRONG_TYPE;
          LOG_WARN("py_obj is not float type", K(ret), K(py_obj));
        }
        break;
      }
      case ObNumberType: {
        bool is_decimal = false;
        if (OB_FAIL(is_dest_type(ctx, py_obj, is_decimal, decimal_module_name, decimal_type_name))) {
          LOG_WARN("failed to check decimal type", K(ret), K(py_obj));
        } else if (is_decimal) {
          if (OB_FAIL(decimal_from_py(allocator, obj, py_obj))) {
            LOG_WARN("failed to from decimal", K(ret), K(py_obj));
          }
        } else {
          ret = OB_ERR_EXPRESSION_WRONG_TYPE;
          LOG_WARN("py_obj is not decimal type", K(ret), K(py_obj));
        }
        break;
      }
      case ObMySQLDateTimeType: {
        bool is_dt = false;
        if (OB_FAIL(is_dest_type(ctx, py_obj, is_dt, datetime_module_name, datetime_type_name))) {
          LOG_WARN("failed to check datetime type", K(ret), K(py_obj));
        } else if (is_dt) {
          if (OB_FAIL(datetime_from_py(allocator, obj, py_obj))) {
            LOG_WARN("failed to from datetime", K(ret), K(py_obj));
          }
        } else {
          ret = OB_ERR_EXPRESSION_WRONG_TYPE;
          LOG_WARN("py_obj is not datetime type", K(ret), K(py_obj));
        }
        break;
      }
      case ObMySQLDateType: {
        bool is_dt = false;
        if (OB_FAIL(is_dest_type(ctx, py_obj, is_dt, datetime_module_name, date_type_name))) {
          LOG_WARN("failed to check date type", K(ret), K(py_obj));
        } else if (is_dt) {
          if (OB_FAIL(date_from_py(allocator, obj, py_obj))) {
            LOG_WARN("failed to from date", K(ret), K(py_obj));
          }
        } else {
          ret = OB_ERR_EXPRESSION_WRONG_TYPE;
          LOG_WARN("py_obj is not date type", K(ret), K(py_obj));
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported type", K(ret), K(obj.get_type()));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "type in Python UDF is");
    }
#undef GET_STRING_FROM_PY
  }
  return ret;
}

void ObPyUtils::ob_py_end_sub_inter(void *py_sub_inter_ctx)
{
  if (nullptr != py_sub_inter_ctx) {
    ObPySubInterContext *ctx = static_cast<ObPySubInterContext *>(py_sub_inter_ctx);
    ctx->~ObPySubInterContext();
  }
}

int ObPyUtils::verify_py_valid()
{
  int ret = OB_SUCCESS;
  PyFunctionHelper &helper = PyFunctionHelper::get_instance();
  if (helper.is_inited()) {
  } else if (OB_FAIL(helper.do_init())) {
    LOG_WARN("failed to setup python env", K(ret));
  }
  return ret;
}

int ObPyUtils::is_dest_type(ObPySubInterContext &ctx,
                            ObPyObject *py_obj,
                            bool &is_dest,
                            const ObString &module_name,
                            const ObString &type_name)
{
  int ret = OB_SUCCESS;
  is_dest = false;
  ObPyObject *module = nullptr;
  ObPyObject *type = nullptr;
  if (OB_FAIL(ctx.get_module_map().get_refactored(module_name, module))) {
    LOG_WARN("failed to get python module", K(ret), K(module_name));
  } else if (OB_ISNULL(module)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("module is null", K(ret));
  } else if (OB_ISNULL(type = ob_py_object_get_attr_string(module, type_name.ptr()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get python type", K(ret), K(type_name));
  } else if (ob_py_object_is_instance(py_obj, type)) {
    is_dest = true;
  }
  xdec_ref(type);
  return ret;
}

int ObPyUtils::decimal_from_py(ObIAllocator &allocator,
                               common::ObObj &obj,
                               ObPyObject *py_obj)
{
  int ret = OB_SUCCESS;
  ObPyObject* py_str = ob_py_object_str(py_obj);
  if (!py_str) {
    if (OB_FAIL(ObPyUtils::exception_check())) {
      LOG_WARN("failed to check exception", K(ret));
    }
  } else {
    const char* c_str = ob_py_unicode_as_utf8(py_str);
    if (!c_str) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get str", K(ret), K(py_str));
    } else {
      number::ObNumber num;
      if (OB_FAIL(num.from_sci_opt(c_str, STRLEN(c_str), allocator))) {
        LOG_WARN("failed to from number", K(ret));
      } else {
        obj.set_number(num);
      }
    }
  }
  xdec_ref(py_str);
  return ret;
}

int ObPyUtils::datetime_from_py(ObIAllocator &allocator,
                                common::ObObj &obj,
                                ObPyObject *py_obj)
{
  int ret = OB_SUCCESS;
  ObPyObject* year_obj = ob_py_object_get_attr_string(py_obj, "year");
  ObPyObject* month_obj = ob_py_object_get_attr_string(py_obj, "month");
  ObPyObject* day_obj = ob_py_object_get_attr_string(py_obj, "day");
  ObPyObject* hour_obj = ob_py_object_get_attr_string(py_obj, "hour");
  ObPyObject* minute_obj = ob_py_object_get_attr_string(py_obj, "minute");
  ObPyObject* second_obj = ob_py_object_get_attr_string(py_obj, "second");
  ObPyObject* microsecond_obj = ob_py_object_get_attr_string(py_obj, "microsecond");

  long year = ob_py_long_as_long(year_obj);
  long month = ob_py_long_as_long(month_obj);
  long day = ob_py_long_as_long(day_obj);
  long hour = ob_py_long_as_long(hour_obj);
  long minute = ob_py_long_as_long(minute_obj);
  long second = ob_py_long_as_long(second_obj);
  long microsecond = ob_py_long_as_long(microsecond_obj);

  ObMySQLDateTime mdt = 0;
  mdt.year_month_ = ObMySQLDateTime::year_month((uint64_t)year, (uint64_t)month);
  mdt.day_ = (uint64_t)day;
  mdt.hour_ = (uint64_t)hour;
  mdt.minute_ = (uint64_t)minute;
  mdt.second_ = (uint64_t)second;
  mdt.microseconds_ = (uint64_t)microsecond;
  obj.set_mysql_datetime(mdt);

  xdec_ref(year_obj);
  xdec_ref(month_obj);
  xdec_ref(day_obj);
  xdec_ref(hour_obj);
  xdec_ref(minute_obj);
  xdec_ref(second_obj);
  xdec_ref(microsecond_obj);

  return ret;
}

int ObPyUtils::date_from_py(ObIAllocator &allocator,
                            common::ObObj &obj,
                            ObPyObject *py_obj)
{
  int ret = OB_SUCCESS;
  ObPyObject* year_obj = ob_py_object_get_attr_string(py_obj, "year");
  ObPyObject* month_obj = ob_py_object_get_attr_string(py_obj, "month");
  ObPyObject* day_obj = ob_py_object_get_attr_string(py_obj, "day");

  long year = ob_py_long_as_long(year_obj);
  long month = ob_py_long_as_long(month_obj);
  long day = ob_py_long_as_long(day_obj);

  ObMySQLDate mdt = 0;
  mdt.year_ = (uint32_t)year;
  mdt.month_ = (uint32_t)month;
  mdt.day_ = (uint32_t)day;
  obj.set_mysql_date(mdt);

  xdec_ref(year_obj);
  xdec_ref(month_obj);
  xdec_ref(day_obj);

  return ret;
}

const ObString ObPyUtils::builtins_module_name = "builtins";
const ObString ObPyUtils::decimal_module_name = "decimal";
const ObString ObPyUtils::datetime_module_name = "datetime";
const ObString ObPyUtils::decimal_type_name = "Decimal";
const ObString ObPyUtils::datetime_type_name = "datetime";
const ObString ObPyUtils::date_type_name = "date";

} // namespace pl

} // namespace oceanbase
