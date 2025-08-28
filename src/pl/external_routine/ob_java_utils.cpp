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

#include "pl/external_routine/ob_java_utils.h"

#include "lib/charset/ob_charset.h"
#include "lib/number/ob_number_v2.h"
#include "sql/ob_spi.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{

using namespace common;

namespace pl
{

int ObJavaUtils::load_routine_jar(const ObString &jar, jobject &class_loader)
{
  int ret = OB_SUCCESS;

  JNIEnv *env = nullptr;

  if (OB_FAIL(ObJniConnector::get_jni_env(env))) {
    LOG_WARN("failed to get_jni_env", K(ret));
  } else {
    jclass loader_clazz = nullptr;
    jmethodID loader_constructor = nullptr;
    jmethodID find_class_method = nullptr;
    jobject loader = nullptr;
    jbyteArray jar_content = nullptr;
    jbyte *jar_content_ptr = nullptr;
    jmethodID load_jar = nullptr;

    if (OB_FAIL(ObJavaUtils::get_udf_loader_class(*env, loader_clazz, loader_constructor, find_class_method))) {
      LOG_WARN("failed to get_udf_loader_class", K(ret));
    } else if (OB_ISNULL(loader_clazz) || OB_ISNULL(loader_constructor)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL loader_clazz or loader_constructor", K(ret), K(loader_clazz), K(loader_constructor));
    } else if (OB_ISNULL(loader = env->NewObject(loader_clazz,
                                                  loader_constructor))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to construct ObPLJavaUDFClassLoader object", K(ret));
    } else if (OB_ISNULL(load_jar = env->GetMethodID(loader_clazz, "loadJar", "([B)V"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find loadJar method in ObPLJavaUDFClassLoader object", K(ret));
    } else if (OB_ISNULL(jar_content = env->NewByteArray(jar.length()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to construct byte array", K(ret));
    } else if (OB_ISNULL(jar_content_ptr = env->GetByteArrayElements(jar_content, nullptr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get jar_content ptr", K(ret));
    } else {
      MEMCPY(jar_content_ptr, jar.ptr(), jar.length());
      env->ReleaseByteArrayElements(jar_content, jar_content_ptr, 0);
      env->CallVoidMethod(loader, load_jar, jar_content);

      if (OB_FAIL(exception_check(env))) {
        LOG_WARN("failed to load jar", K(ret), K(jar));
      } else {
        class_loader = env->NewGlobalRef(loader);
      }
    }

    // always delete local ref
    delete_local_ref(jar_content, env);
    delete_local_ref(loader, env);
  }

  return ret;
}

void ObJavaUtils::delete_local_ref(jobject obj, JNIEnv *env)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(obj)) {
    if (OB_ISNULL(env)) {
      if (OB_FAIL(ObJniConnector::get_jni_env(env))) {
        LOG_WARN("failed to get_jni_env", K(ret));
      }
    }

    if (OB_NOT_NULL(env)) {
      env->DeleteLocalRef(obj);
    }
  }
}

void ObJavaUtils::delete_global_ref(jobject obj, JNIEnv *env)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(obj)) {
    if (OB_ISNULL(env)) {
      if (OB_FAIL(ObJniConnector::get_jni_env(env))) {
        LOG_WARN("failed to get_jni_env", K(ret));
      }
    }

    if (OB_NOT_NULL(env)) {
      env->DeleteLocalRef(obj);
    }
  }
}

int ObJavaUtils::exception_check(JNIEnv *env)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(env)) {
    if (OB_FAIL(ObJniConnector::get_jni_env(env))) {
      LOG_WARN("failed to get_jni_env", K(ret));
    }
  }

  if (OB_NOT_NULL(env)) {
    if (JNI_TRUE == env->ExceptionCheck()) {
      jthrowable exception = nullptr;
      jclass object = nullptr;
      jmethodID toString = nullptr;
      jstring error_string = nullptr;
      jclass timeout = nullptr;

      if (OB_ISNULL(exception = env->ExceptionOccurred())){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL exception", K(ret));
      } else if (FALSE_IT(env->ExceptionDescribe())) {
        // unreachable
      } else if (FALSE_IT(env->ExceptionClear())) {
        // unreachable
      } else if (OB_FAIL(ObJavaUtils::get_cached_class(*env, "java/lang/Object", object))) {
        LOG_WARN("failed to get_cached_class java/lang/Object", K(ret));
      } else if (OB_ISNULL(toString = env->GetMethodID(object, "toString", "()Ljava/lang/String;"))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL toString", K(ret));
      } else if (OB_ISNULL(error_string = static_cast<jstring>(env->CallObjectMethod(exception, toString)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL error_string", K(ret));
      } else if (OB_FAIL(ObJavaUtils::get_cached_class(*env, "java/util/concurrent/TimeoutException", timeout))) {
        LOG_WARN("faield to get_cached_class java/util/concurrent/TimeoutException");
      } else {
        const char *ptr = env->GetStringUTFChars(error_string, nullptr);
        ObString msg(ptr);

        if (env->IsInstanceOf(exception, timeout)) {
          ret = OB_TIMEOUT;
        } else {
          ret = OB_JNI_JAVA_EXCEPTION_ERROR;
          LOG_USER_ERROR(OB_JNI_JAVA_EXCEPTION_ERROR, msg.length(), msg.ptr());
        }

        LOG_WARN("Java exception occurred", K(ret), K(msg));
        env->ReleaseStringUTFChars(error_string, ptr);
      }

      delete_local_ref(error_string, env);
      delete_local_ref(exception, env);
    }
  }

  return ret;
}

void *ObJavaUtils::protobuf_c_allocator_alloc(void *allocator_data, size_t size)
{
  void *ret = nullptr;

  if (OB_NOT_NULL(allocator_data)) {
    ret = static_cast<ObIAllocator*>(allocator_data)->alloc(size);
  }

  return ret;
}

void ObJavaUtils::protobuf_c_allocator_free(void *allocator_data, void *pointer)
{
  if (OB_NOT_NULL(allocator_data)) {
    static_cast<ObIAllocator *>(allocator_data)->free(pointer);
  }
}

int ObToJavaByteTypeMapper::operator()(const common::ObObj &obj, int64_t idx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(type_class_) || 0 >= batch_size_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaByteTypeMapper is not inited", K(ret), K(lbt()), KPC(this));
  } else if (idx >= batch_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx out of range", K(ret), K(idx), K(batch_size_), K(lbt()));
  } else if (obj.is_null()) {
    arg_.null_map.data[idx] = true;
  } else {
    int8_t ob_value = obj.get_tinyint();
    values_.value.data[idx] = ob_value;
  }

  return ret;
}

int ObToJavaShortTypeMapper::operator()(const common::ObObj &obj, int64_t idx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(type_class_) || 0 >= batch_size_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaShortTypeMapper is not inited", K(ret), K(lbt()), KPC(this));
  } else if (idx >= batch_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx out of range", K(ret), K(idx), K(batch_size_), K(lbt()));
  } else if (obj.is_null()) {
    arg_.null_map.data[idx] = true;
  } else {
    int16_t ob_value = obj.get_smallint();
    values_.value[idx] = ob_value;
  }

  return ret;
}

int ObToJavaIntegerTypeMapper::operator()(const ObObj &obj, int64_t idx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(type_class_) || 0 >= batch_size_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaIntegerTypeMapper is not inited", K(ret), K(lbt()), KPC(this));
  } else if (idx >= batch_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx out of range", K(ret), K(idx), K(batch_size_), K(lbt()));
  } else if (obj.is_null()) {
    arg_.null_map.data[idx] = true;
  } else {
    int32_t ob_value = obj.get_int32();
    values_.value[idx] = ob_value;
  }

  return ret;
}

int ObToJavaLongTypeMapper::operator()(const ObObj &obj, int64_t idx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(type_class_) || 0 >= batch_size_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaLongTypeMapper is not inited", K(ret), K(lbt()), KPC(this));
  } else if (idx >= batch_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx out of range", K(ret), K(idx), K(batch_size_), K(lbt()));
  } else if (obj.is_null()) {
    arg_.null_map.data[idx] = true;
  } else {
    int64_t ob_value = obj.get_int();
    values_.value[idx] = ob_value;
  }

  return ret;
}

int ObToJavaFloatTypeMapper::operator()(const ObObj &obj, int64_t idx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(type_class_) || 0 >= batch_size_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaFloatTypeMapper is not inited", K(ret), K(lbt()), KPC(this));
  } else if (idx >= batch_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx out of range", K(ret), K(idx), K(batch_size_), K(lbt()));
  } else if (obj.is_null()) {
    arg_.null_map.data[idx] = true;
  } else {
    float ob_value = obj.get_float();
    values_.value[idx] = ob_value;
  }

  return ret;
}

int ObToJavaDoubleTypeMapper::operator()(const ObObj &obj, int64_t idx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(type_class_) || 0 >= batch_size_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaDoubleTypeMapper is not inited", K(ret), K(lbt()), KPC(this));
  } else if (idx >= batch_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx out of range", K(ret), K(idx), K(batch_size_), K(lbt()));
  } else if (obj.is_null()) {
    arg_.null_map.data[idx] = true;
  } else {
    double ob_value = obj.get_double();
    values_.value[idx] = ob_value;
  }

  return ret;
}

int ObToJavaBigDecimalTypeMapper::operator()(const ObObj &obj, int64_t idx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(type_class_) || 0 >= batch_size_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaBigDecimalTypeMapper is not inited", K(ret), K(lbt()), KPC(this));
  } else if (idx >= batch_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx out of range", K(ret), K(idx), K(batch_size_), K(lbt()));
  } else if (obj.is_null()) {
    arg_.null_map.data[idx] = true;
  } else {
    number::ObNumber ob_value = obj.get_number();
    char buffer[number::ObNumber::MAX_PRECISION * 2 + 4];
    int64_t pos = 0;

    if (OB_FAIL(ob_value.format_v2(buffer, sizeof(buffer) - 1, pos, -1, true))) {
      LOG_WARN("failed to format number to string", K(ret), K(ob_value));
    } else if (OB_ISNULL(values_.value[idx].data = static_cast<uint8_t*>(alloc_.alloc(pos)))){
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for BigDecimal buffer",
               K(ret), K(ObString(pos, buffer)), K(pos), K(obj));
    } else {
      memcpy(values_.value[idx].data, buffer, pos);
      values_.value[idx].len = pos;
    }
  }

  return ret;
}

int ObToJavaStringTypeMapper::operator()(const ObObj &obj, int64_t idx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(type_class_) || 0 >= batch_size_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaStringTypeMapper is not inited", K(ret), K(lbt()), KPC(this));
  } else if (idx >= batch_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx out of range", K(ret), K(idx), K(batch_size_), K(lbt()));
  } else if (obj.is_null()) {
    arg_.null_map.data[idx] = true;
  } else {
    ObArenaAllocator tmp_alloc;
    ObObj value;
    ObString origin_str;
    ObString utf8_str;
    ObCharsetType charset = ObCharset::charset_type_by_coll(obj.get_meta().get_collation_type());

    if (obj.is_lob_storage()) {
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_alloc, obj, origin_str))) {
        LOG_WARN("failed to read_real_string_data", K(ret), K(obj), K(origin_str));
      }
    } else {
      if (OB_FAIL(obj.get_string(origin_str))) {
        LOG_WARN("failed to get_string", K(ret), K(value), K(origin_str));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (CHARSET_INVALID == charset || CHARSET_BINARY == charset) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support charset", K(ret), K(charset), K(origin_str));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "charset");
    } else if (CHARSET_UTF8MB4 == charset) {
      utf8_str = origin_str;
    } else if (OB_FAIL(ObCharset::charset_convert(tmp_alloc,
                                                  origin_str,
                                                  obj.get_meta().get_collation_type(),
                                                  CS_TYPE_UTF8MB4_BIN,
                                                  utf8_str))) {
      LOG_WARN("failed to ObCharset::charset_convert", K(ret), K(origin_str), K(obj));
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (0 == utf8_str.length()) {
      values_.value[idx].data = nullptr;
      values_.value[idx].len = 0;
    } else if (OB_ISNULL(values_.value[idx].data = static_cast<uint8_t*>(alloc_.alloc(utf8_str.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for String buffer", K(ret), K(obj), K(utf8_str));
    } else {
      memcpy(values_.value[idx].data, utf8_str.ptr(), utf8_str.length());
      values_.value[idx].len = utf8_str.length();
    }
  }

  return ret;
}

int ObToJavaByteBufferTypeMapper::operator()(const ObObj &obj, int64_t idx)
{
  int ret = OB_SUCCESS;

  ObString buffer;

  if (OB_ISNULL(type_class_) || 0 >= batch_size_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaByteBufferTypeMapper is not inited", K(ret), K(lbt()), KPC(this));
  } else if (idx >= batch_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx out of range", K(ret), K(idx), K(batch_size_), K(lbt()));
  } else if (obj.is_null()) {
    arg_.null_map.data[idx] = true;
  } else {
    ObArenaAllocator tmp_alloc(ObMemAttr(MTL_ID(), GET_PL_MOD_STRING(OB_PL_ARENA)));

    if (obj.is_lob_storage()) {
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_alloc, obj, buffer))) {
        LOG_WARN("failed to read_real_string_data", K(ret), K(obj), K(buffer));
      }
    } else {
      if (OB_FAIL(obj.get_string(buffer))) {
        LOG_WARN("failed to get_string", K(ret), K(obj), K(buffer));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (0 == buffer.length()) {
      values_.value[idx].data = nullptr;
      values_.value[idx].len = 0;
    } else if (OB_ISNULL(values_.value[idx].data = static_cast<uint8_t*>(alloc_.alloc(buffer.length())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for ByteBuffer buffer", K(ret), K(obj), K(buffer));
    } else {
      memcpy(values_.value[idx].data, buffer.ptr(), buffer.length());
      values_.value[idx].len = buffer.length();
    }
  }

  return ret;
}

int ObFromJavaByteTypeMapper::operator()(const ObPl__JavaUdf__Values &values, ObIArray<ObObj> &result_array)
{
  int ret = OB_SUCCESS;

  if (OB_PL__JAVA_UDF__VALUES__VALUES_BYTE_VALUES != values.values_case) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected values type", K(ret), K(values.values_case));
  } else if (OB_ISNULL(values.byte_values) || OB_ISNULL(values.byte_values->value.data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL values", K(ret), K(values.values_case), K(values.byte_values));
  } else if (values.byte_values->value.len != batch_size_ || values.null_map.len != batch_size_) {
    ret = ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL idx", K(ret), K(values.byte_values->value.len), K(values.null_map.len));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < batch_size_; ++i) {
      ObObj res_obj;
      res_obj.set_null();

      if (!values.null_map.data[i]) {
        ObObj tmp_obj;
        tmp_obj.set_tinyint(values.byte_values->value.data[i]);

        if (OB_FAIL(convert(tmp_obj, res_obj))) {
          LOG_WARN("failed to convert obj", K(ret), K(tmp_obj), K(res_obj));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(result_array.push_back(res_obj))) {
        LOG_WARN("failed to push_back result_array", K(ret), K(result_array), K(i), K(res_obj));
      }
    }
  }


  return ret;
}

int ObFromJavaShortTypeMapper::operator()(const ObPl__JavaUdf__Values &values, ObIArray<ObObj> &result_array)
{
  int ret = OB_SUCCESS;

  if (OB_PL__JAVA_UDF__VALUES__VALUES_SHORT_VALUES != values.values_case) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected values type", K(ret), K(values.values_case));
  } else if (OB_ISNULL(values.short_values)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL values", K(ret), K(values.values_case), K(values.short_values));
  } else if (values.short_values->n_value != batch_size_ || values.null_map.len != batch_size_) {
    ret = ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL idx", K(ret), K(values.short_values->n_value), K(values.null_map.len));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < batch_size_; ++i) {
      ObObj res_obj;
      res_obj.set_null();

      if (!values.null_map.data[i]) {
        ObObj tmp_obj;
        tmp_obj.set_smallint(values.short_values->value[i]);

        if (OB_FAIL(convert(tmp_obj, res_obj))) {
          LOG_WARN("failed to convert obj", K(ret), K(tmp_obj), K(res_obj));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(result_array.push_back(res_obj))) {
        LOG_WARN("failed to push_back result_array", K(ret), K(result_array), K(i), K(res_obj));
      }
    }
  }

  return ret;
}

int ObFromJavaIntegerTypeMapper::operator()(const ObPl__JavaUdf__Values &values, ObIArray<ObObj> &result_array)
{
  int ret = OB_SUCCESS;

  if (OB_PL__JAVA_UDF__VALUES__VALUES_INT_VALUES != values.values_case) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected values type", K(ret), K(values.values_case));
  } else if (OB_ISNULL(values.int_values)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL values", K(ret), K(values.values_case), K(values.int_values));
  } else if (values.int_values->n_value != batch_size_ || values.null_map.len != batch_size_) {
    ret = ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL idx", K(ret), K(values.int_values->n_value), K(values.null_map.len));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < batch_size_; ++i) {
      ObObj res_obj;
      res_obj.set_null();

      if (!values.null_map.data[i]) {
        ObObj tmp_obj;
        tmp_obj.set_int32(values.int_values->value[i]);

        if (OB_FAIL(convert(tmp_obj, res_obj))) {
          LOG_WARN("failed to convert obj", K(ret), K(tmp_obj), K(res_obj));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(result_array.push_back(res_obj))) {
        LOG_WARN("failed to push_back result_array", K(ret), K(result_array), K(i), K(res_obj));
      }
    }
  }

  return ret;
}

int ObFromJavaLongTypeMapper::operator()(const ObPl__JavaUdf__Values &values, ObIArray<ObObj> &result_array)
{
  int ret = OB_SUCCESS;

  if (OB_PL__JAVA_UDF__VALUES__VALUES_LONG_VALUES != values.values_case) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected values type", K(ret), K(values.values_case));
  } else if (OB_ISNULL(values.long_values)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL values", K(ret), K(values.values_case), K(values.long_values));
  } else if (values.long_values->n_value != batch_size_ || values.null_map.len != batch_size_) {
    ret = ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL idx", K(ret), K(values.long_values->n_value), K(values.null_map.len));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < batch_size_; ++i) {
      ObObj res_obj;
      res_obj.set_null();

      if (!values.null_map.data[i]) {
        ObObj tmp_obj;
        tmp_obj.set_int(values.long_values->value[i]);

        if (OB_FAIL(convert(tmp_obj, res_obj))) {
          LOG_WARN("failed to convert obj", K(ret), K(tmp_obj), K(res_obj));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(result_array.push_back(res_obj))) {
        LOG_WARN("failed to push_back result_array", K(ret), K(result_array), K(i), K(res_obj));
      }
    }
  }

  return ret;
}

int ObFromJavaFloatTypeMapper::operator()(const ObPl__JavaUdf__Values &values, ObIArray<ObObj> &result_array)
{
  int ret = OB_SUCCESS;

  if (OB_PL__JAVA_UDF__VALUES__VALUES_FLOAT_VALUES != values.values_case) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected values type", K(ret), K(values.values_case));
  } else if (OB_ISNULL(values.float_values)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL values", K(ret), K(values.values_case), K(values.float_values));
  } else if (values.float_values->n_value != batch_size_ || values.null_map.len != batch_size_) {
    ret = ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL idx", K(ret), K(values.float_values->n_value), K(values.null_map.len));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < batch_size_; ++i) {
      ObObj res_obj;
      res_obj.set_null();

      if (!values.null_map.data[i]) {
        ObObj tmp_obj;
        tmp_obj.set_float(values.float_values->value[i]);

        if (OB_FAIL(convert(tmp_obj, res_obj))) {
          LOG_WARN("failed to convert obj", K(ret), K(tmp_obj), K(res_obj));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(result_array.push_back(res_obj))) {
        LOG_WARN("failed to push_back result_array", K(ret), K(result_array), K(i), K(res_obj));
      }
    }
  }

  return ret;
}

int ObFromJavaDoubleTypeMapper::operator()(const ObPl__JavaUdf__Values &values, ObIArray<ObObj> &result_array)
{
  int ret = OB_SUCCESS;

  if (OB_PL__JAVA_UDF__VALUES__VALUES_DOUBLE_VALUES != values.values_case) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected values type", K(ret), K(values.values_case));
  } else if (OB_ISNULL(values.double_values)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL values", K(ret), K(values.values_case), K(values.double_values));
  } else if (values.double_values->n_value != batch_size_ || values.null_map.len != batch_size_) {
    ret = ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL idx", K(ret), K(values.double_values->n_value), K(values.null_map.len));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < batch_size_; ++i) {
      ObObj res_obj;
      res_obj.set_null();

      if (!values.null_map.data[i]) {
        ObObj tmp_obj;
        tmp_obj.set_double(values.double_values->value[i]);

        if (OB_FAIL(convert(tmp_obj, res_obj))) {
          LOG_WARN("failed to convert obj", K(ret), K(tmp_obj), K(res_obj));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(result_array.push_back(res_obj))) {
        LOG_WARN("failed to push_back result_array", K(ret), K(result_array), K(i), K(res_obj));
      }
    }
  }

  return ret;
}

int ObFromJavaBigDecimalTypeMapper::operator()(const ObPl__JavaUdf__Values &values, ObIArray<ObObj> &result_array)
{
  int ret = OB_SUCCESS;

  if (OB_PL__JAVA_UDF__VALUES__VALUES_BIG_DECIMAL_VALUES != values.values_case) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected values type", K(ret), K(values.values_case));
  } else if (OB_ISNULL(values.big_decimal_values) || OB_ISNULL(values.big_decimal_values->value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL values", K(ret), K(values.values_case), K(values.big_decimal_values));
  } else if (values.big_decimal_values->n_value != batch_size_ || values.null_map.len != batch_size_) {
    ret = ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL idx", K(ret), K(values.big_decimal_values->n_value), K(values.null_map.len));
  } else {
    number::ObNumber nmb;

    for (int i = 0; OB_SUCC(ret) && i < batch_size_; ++i) {
      ObObj res_obj;
      res_obj.set_null();

      if (!values.null_map.data[i]) {
        ObObj tmp_obj;

        int16_t precision = PRECISION_UNKNOWN_YET;
        int16_t scale = SCALE_UNKNOWN_YET;
        ObString str(values.big_decimal_values->value[i].len, reinterpret_cast<char*>(values.big_decimal_values->value[i].data));

        if (OB_FAIL(nmb.from_sci(str.ptr(), str.length(), alloc_, &precision, &scale))) {
          LOG_WARN("failed convert string to ObNumber", K(ret), K(str));
        } else if (FALSE_IT(tmp_obj.set_number(nmb))) {
          // unreachable
        } else if (OB_FAIL(convert(tmp_obj, res_obj))) {
          LOG_WARN("failed to convert obj", K(ret), K(tmp_obj), K(res_obj));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(result_array.push_back(res_obj))) {
        LOG_WARN("failed to push_back result_array", K(ret), K(result_array), K(i), K(res_obj));
      }
    }
  }

  return ret;
}

int ObFromJavaStringTypeMapper::operator()(const ObPl__JavaUdf__Values &values, ObIArray<ObObj> &result_array)
{
  int ret = OB_SUCCESS;

  if (OB_PL__JAVA_UDF__VALUES__VALUES_STRING_VALUES != values.values_case) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected values type", K(ret), K(values.values_case));
  } else if (OB_ISNULL(values.string_values) || OB_ISNULL(values.string_values->value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL values", K(ret), K(values.values_case), K(values.string_values));
  } else if (values.string_values->n_value != batch_size_ || values.null_map.len != batch_size_) {
    ret = ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL idx", K(ret), K(values.string_values->n_value), K(values.null_map.len));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < batch_size_; ++i) {
      ObObj res_obj;
      res_obj.set_null();

      if (!values.null_map.data[i]) {
        ObObj tmp_obj;

        ObString str(values.string_values->value[i].len, reinterpret_cast<char*>(values.string_values->value[i].data));

        tmp_obj.set_varchar(str);
        tmp_obj.set_collation_type(CS_TYPE_UTF8MB4_BIN);

        if (OB_FAIL(convert(tmp_obj, res_obj))) {
          LOG_WARN("failed to convert obj", K(ret), K(tmp_obj), K(res_obj));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(result_array.push_back(res_obj))) {
        LOG_WARN("failed to push_back result_array", K(ret), K(result_array), K(i), K(res_obj));
      }
    }
  }

  return ret;
}

int ObFromJavaByteBufferTypeMapper::operator()(const ObPl__JavaUdf__Values &values, ObIArray<ObObj> &result_array)
{
  int ret = OB_SUCCESS;

  if (OB_PL__JAVA_UDF__VALUES__VALUES_BYTE_BUFFER_VALUES != values.values_case) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected values type", K(ret), K(values.values_case));
  } else if (OB_ISNULL(values.byte_buffer_values) || OB_ISNULL(values.byte_buffer_values->value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL values", K(ret), K(values.values_case), K(values.byte_buffer_values));
  } else if (values.byte_buffer_values->n_value != batch_size_ || values.null_map.len != batch_size_) {
    ret = ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL idx", K(ret), K(values.byte_buffer_values->n_value), K(values.null_map.len));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < batch_size_; ++i) {
      ObObj res_obj;
      res_obj.set_null();

      if (!values.null_map.data[i]) {
        ObObj tmp_obj;
        ObString str(values.byte_buffer_values->value[i].len, reinterpret_cast<char*>(values.byte_buffer_values->value[i].data));

        tmp_obj.set_varbinary(str);

        if (OB_FAIL(convert(tmp_obj, res_obj))) {
          LOG_WARN("failed to convert obj", K(ret), K(tmp_obj), K(res_obj));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(result_array.push_back(res_obj))) {
        LOG_WARN("failed to push_back result_array", K(ret), K(result_array), K(i), K(res_obj));
      }
    }
  }

  return ret;
}

int ObFromJavaTypeMapperBase::convert(ObObj &src, ObObj &dest)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObSPIService::spi_convert(session_,
                                        alloc_,
                                        src,
                                        res_type_,
                                        dest,
                                        false))) {
    LOG_WARN("failed to ObSPIService::spi_convert", K(ret), K(src), K(dest));
  }

  return ret;
}

} // namespace pl
} // namespace oceanbase
