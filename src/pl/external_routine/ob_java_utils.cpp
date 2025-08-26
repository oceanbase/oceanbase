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
    jobject loader = nullptr;
    jbyteArray jar_content = nullptr;
    jbyte *jar_content_ptr = nullptr;
    jmethodID load_jar = nullptr;

    if (OB_ISNULL(loader_clazz = env->FindClass("com/oceanbase/internal/ObJavaUDFClassLoader"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find ObPLJavaUDFClassLoader class", K(ret));
    } else if (OB_ISNULL(loader_constructor = env->GetMethodID(loader_clazz, "<init>", "()V"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find ObPLJavaUDFClassLoader class constructor", K(ret));
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
    delete_local_ref(loader_clazz, env);
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

      if (OB_ISNULL(exception = env->ExceptionOccurred())){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL exception", K(ret));
      } else if (FALSE_IT(env->ExceptionDescribe())) {
        // unreachable
      } else if (FALSE_IT(env->ExceptionClear())) {
        // unreachable
      } else if (OB_ISNULL(object = env->FindClass("java/lang/Object"))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL object", K(ret));
      } else if (OB_ISNULL(toString = env->GetMethodID(object, "toString", "()Ljava/lang/String;"))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL toString", K(ret));
      } else if (OB_ISNULL(error_string = static_cast<jstring>(env->CallObjectMethod(exception, toString)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL error_string", K(ret));
      } else {
        const char *ptr = env->GetStringUTFChars(error_string, nullptr);
        ObString msg(ptr);
        ret = OB_JNI_JAVA_EXCEPTION_ERROR;
        LOG_WARN("Java exception occurred", K(ret), K(msg));
        LOG_USER_ERROR(OB_JNI_JAVA_EXCEPTION_ERROR, msg.length(), msg.ptr());
        env->ReleaseStringUTFChars(error_string, ptr);
      }

      delete_local_ref(error_string, env);
      delete_local_ref(object, env);
      delete_local_ref(exception, env);
    }
  }

  return ret;
}

int ObToJavaByteTypeMapper::operator()(const ObObj &obj, jobject &java_value)
{
  int ret = OB_SUCCESS;

  jobject value = nullptr;
  java_value = nullptr;

  if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaByteTypeMapper is not inited", K(ret), K(lbt()));
  } else if (obj.is_null()) {
    java_value = nullptr;
  } else {
    int8_t ob_value = obj.get_tinyint();

    jmethodID constructor = nullptr;
    jobject value = nullptr;

    if (OB_ISNULL(constructor = env_.GetMethodID(type_class_, "<init>", "(B)V"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL constructor of java.lang.Byte", K(ret));
    } else if (OB_ISNULL(value = env_.NewObject(type_class_, constructor, ob_value))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL value of java.lang.Byte", K(ret), K(ob_value));
    } else {
      java_value = value;
    }
  }

  return ret;
}

int ObToJavaShortTypeMapper::operator()(const ObObj &obj, jobject &java_value)
{
  int ret = OB_SUCCESS;

  jobject value = nullptr;
  java_value = nullptr;

  if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaShortTypeMapper is not inited", K(ret), K(lbt()));
  } else if (obj.is_null()) {
    java_value = nullptr;
  } else {
    int16_t ob_value = obj.get_smallint();

    jmethodID constructor = nullptr;
    jobject value = nullptr;

    if (OB_ISNULL(constructor = env_.GetMethodID(type_class_, "<init>", "(S)V"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL constructor of java.lang.Short", K(ret));
    } else if (OB_ISNULL(value = env_.NewObject(type_class_, constructor, ob_value))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL value of java.lang.Short", K(ret), K(ob_value));
    } else {
      java_value = value;
    }
  }

  return ret;
}

int ObToJavaIntegerTypeMapper::operator()(const ObObj &obj, jobject &java_value)
{
  int ret = OB_SUCCESS;

  jobject value = nullptr;
  java_value = nullptr;

  if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaIntegerTypeMapper is not inited", K(ret), K(lbt()));
  } else if (obj.is_null()) {
    java_value = nullptr;
  } else {
    int32_t ob_value = obj.get_int32();

    jmethodID constructor = nullptr;
    jobject value = nullptr;

    if (OB_ISNULL(constructor = env_.GetMethodID(type_class_, "<init>", "(I)V"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL constructor of java.lang.Integer", K(ret));
    } else if (OB_ISNULL(value = env_.NewObject(type_class_, constructor, ob_value))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL value of java.lang.Integer", K(ret), K(ob_value));
    } else {
      java_value = value;
    }
  }

  return ret;
}

int ObToJavaLongTypeMapper::operator()(const ObObj &obj, jobject &java_value)
{
  int ret = OB_SUCCESS;

  jobject value = nullptr;
  java_value = nullptr;

  if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaLongTypeMapper is not inited", K(ret), K(lbt()));
  } else if (obj.is_null()) {
    java_value = nullptr;
  } else {
    int64_t ob_value = obj.get_int();

    jmethodID constructor = nullptr;
    jobject value = nullptr;

    if (OB_ISNULL(constructor = env_.GetMethodID(type_class_, "<init>", "(J)V"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL constructor of java.lang.Long", K(ret));
    } else if (OB_ISNULL(value = env_.NewObject(type_class_, constructor, ob_value))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL value of java.lang.Long", K(ret), K(ob_value));
    } else {
      java_value = value;
    }
  }

  return ret;
}

int ObToJavaFloatTypeMapper::operator()(const ObObj &obj, jobject &java_value)
{
  int ret = OB_SUCCESS;

  jobject value = nullptr;
  java_value = nullptr;

  if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaFloatTypeMapper is not inited", K(ret), K(lbt()));
  } else if (obj.is_null()) {
    java_value = nullptr;
  } else {
    float ob_value = obj.get_float();

    jmethodID constructor = nullptr;
    jobject value = nullptr;

    if (OB_ISNULL(constructor = env_.GetMethodID(type_class_, "<init>", "(F)V"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL constructor of java.lang.Float", K(ret));
    } else if (OB_ISNULL(value = env_.NewObject(type_class_, constructor, ob_value))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL value of java.lang.Float", K(ret), K(ob_value));
    } else {
      java_value = value;
    }
  }

  return ret;
}

int ObToJavaDoubleTypeMapper::operator()(const ObObj &obj, jobject &java_value)
{
  int ret = OB_SUCCESS;

  jobject value = nullptr;
  java_value = nullptr;

  if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaDoubleTypeMapper is not inited", K(ret), K(lbt()));
  } else if (obj.is_null()) {
    java_value = nullptr;
  } else {
    double ob_value = obj.get_double();

    jmethodID constructor = nullptr;
    jobject value = nullptr;

    if (OB_ISNULL(constructor = env_.GetMethodID(type_class_, "<init>", "(D)V"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL constructor of java.lang.Double", K(ret));
    } else if (OB_ISNULL(value = env_.NewObject(type_class_, constructor, ob_value))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL value of java.lang.Double", K(ret), K(ob_value));
    } else {
      java_value = value;
    }
  }

  return ret;
}

int ObToJavaBigDecimalTypeMapper::operator()(const ObObj &obj, jobject &java_value)
{
  int ret = OB_SUCCESS;

  jobject value = nullptr;
  java_value = nullptr;

  if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaBigDecimalTypeMapper is not inited", K(ret), K(lbt()));
  } else if (obj.is_null()) {
    java_value = nullptr;
  } else {
    number::ObNumber ob_value = obj.get_number();
    char buffer[number::ObNumber::MAX_PRECISION * 2 + 4];
    int64_t pos = 0;
    jstring sci_rep = nullptr;

    jmethodID constructor = nullptr;
    jobject value = nullptr;

    if (OB_FAIL(ob_value.format_v2(buffer, sizeof(buffer) - 1, pos, -1, true))) {
      LOG_WARN("failed to format number to string", K(ret), K(ob_value));
    } else if (FALSE_IT(buffer[pos] = '\0')) {
      // unreachable
    } else if (OB_ISNULL(sci_rep = env_.NewStringUTF(buffer))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL value of java.lang.String", K(ret), K(buffer));
    } else if (OB_ISNULL(constructor = env_.GetMethodID(type_class_, "<init>", "(Ljava/lang/String;)V"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL constructor of java.math.BigDecimal", K(ret));
    } else if (OB_ISNULL(value = env_.NewObject(type_class_, constructor, sci_rep))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL value of java.math.BigDecimal", K(ret), K(ob_value));
    } else {
      java_value = value;
    }

    ObJavaUtils::delete_local_ref(sci_rep, &env_);
  }

  return ret;
}

int ObToJavaStringTypeMapper::operator()(const ObObj &obj, jobject &java_value)
{
  int ret = OB_SUCCESS;

  jobject value = nullptr;
  java_value = nullptr;

  if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaStringTypeMapper is not inited", K(ret), K(lbt()));
  } else if (obj.is_null()) {
    java_value = nullptr;
  } else {
    jmethodID constructor = nullptr;
    jbyteArray byte_array = nullptr;
    jbyte *bytes = nullptr;
    jstring utf8 = nullptr;

    ObArenaAllocator alloc;
    ObObj value;
    ObString origin_str;
    ObString utf8_str;
    ObCharsetType charset = ObCharset::charset_type_by_coll(obj.get_meta().get_collation_type());

    if (OB_FAIL(obj.get_string(origin_str))) {
      LOG_WARN("failed to get_string", K(ret), K(value), K(origin_str));
    } else if (CHARSET_INVALID == charset || CHARSET_BINARY == charset) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support charset", K(ret), K(charset), K(origin_str));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "charset");
    } else if (CHARSET_UTF8MB4 == charset) {
      utf8_str = origin_str;
    } else if (OB_FAIL(ObCharset::charset_convert(alloc,
                                                  origin_str,
                                                  obj.get_meta().get_collation_type(),
                                                  CS_TYPE_UTF8MB4_BIN,
                                                  utf8_str))) {
      LOG_WARN("failed to ObCharset::charset_convert", K(ret), K(origin_str), K(obj));
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(utf8 = env_.NewStringUTF("UTF-8"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL NewStringUTF", K(ret));
    } else if (OB_ISNULL(constructor = env_.GetMethodID(type_class_, "<init>", "([BLjava/lang/String;)V"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL constructor of java.lang.String", K(ret));
    } else if (OB_ISNULL(byte_array = env_.NewByteArray(utf8_str.length()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL NewByteArray", K(ret));
    } else if (OB_ISNULL(bytes = env_.GetByteArrayElements(byte_array, nullptr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL GetByteArrayElements", K(ret));
    } else if (FALSE_IT(MEMCPY(bytes, utf8_str.ptr(), utf8_str.length()))) {
      // unreachable
    } else if (FALSE_IT(env_.ReleaseByteArrayElements(byte_array, bytes, 0))) {
      // unreachable
    } else if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
      LOG_WARN("failed to exception_check", K(ret), K(utf8_str));
    } else {
      jobject java_string = env_.NewObject(type_class_, constructor, byte_array, utf8);
      if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
        LOG_WARN("failed to create java string", K(ret), K(utf8_str));
      } else if (OB_ISNULL(java_string)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL java string", K(ret), K(utf8_str));
      } else {
        java_value = java_string;
      }
    }

    ObJavaUtils::delete_local_ref(byte_array, &env_);
    ObJavaUtils::delete_local_ref(utf8, &env_);
  }

  return ret;
}

int ObToJavaByteBufferTypeMapper::operator()(const ObObj &obj, jobject &java_value)
{
  int ret = OB_SUCCESS;

  ObString buffer;

  java_value = nullptr;

  if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObToJavaByteBufferTypeMapper is not inited", K(ret), K(lbt()));
  } else if (obj.is_null()) {
    java_value = nullptr;
  } else if (OB_FAIL(obj.get_string(buffer))) {
    LOG_WARN("failed to get ObString", K(ret), K(obj));
  } else {
    jobject rw_buffer = env_.NewDirectByteBuffer(buffer.ptr(), buffer.length());

    jmethodID as_read_only = nullptr;

    if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
      LOG_WARN("failed to create java byte buffer", K(ret), K(buffer));
    } else if (OB_ISNULL(rw_buffer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL value of java.nio.ByteBuffer", K(ret), K(buffer));
    } else if (OB_ISNULL(as_read_only = env_.GetMethodID(type_class_, "asReadOnlyBuffer", "()Ljava/nio/ByteBuffer;"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL asReadOnlyBuffer of java.nio.ByteBuffer", K(ret));
    } else {
      jobject value = env_.CallObjectMethod(rw_buffer, as_read_only);

      if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
        LOG_WARN("failed to call asReadOnlyBuffer", K(ret));
      } else if (OB_ISNULL(value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL value of java.nio.ByteBuffer", K(ret));
      } else {
        java_value = value;
      }
    }

    ObJavaUtils::delete_local_ref(rw_buffer, &env_);
  }

  return ret;
}

int ObFromJavaByteTypeMapper::operator()(jobject java_value, ObObj &result)
{
  int ret = OB_SUCCESS;

  jmethodID byte_value = nullptr;

  if (OB_ISNULL(java_value)) {
    result.set_null();
  } else if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFromJavaByteTypeMapper not init", K(ret));
  } else if (!env_.IsInstanceOf(java_value, type_class_)) {
    ret = OB_ERR_EXPRESSION_WRONG_TYPE;
    LOG_WARN("argument is not of expected type, expect java.lang.Byte", K(ret));
  } else if (OB_ISNULL(byte_value = env_.GetMethodID(type_class_, "byteValue", "()B"))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL byteValue of java.lang.Byte", K(ret));
  } else {
    int8_t value = env_.CallByteMethod(java_value, byte_value);

    if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
      LOG_WARN("failed to call byteValue", K(ret), K(value));
    } else {
      result.set_tinyint(value);
    }
  }

  return ret;
}

int ObFromJavaShortTypeMapper::operator()(jobject java_value, ObObj &result)
{
  int ret = OB_SUCCESS;

  jmethodID short_value = nullptr;

  if (OB_ISNULL(java_value)) {
    result.set_null();
  } else if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFromJavaShortTypeMapper not init", K(ret));
  } else if (!env_.IsInstanceOf(java_value, type_class_)) {
    ret = OB_ERR_EXPRESSION_WRONG_TYPE;
    LOG_WARN("argument is not of expected type, expect java.lang.Short", K(ret));
  } else if (OB_ISNULL(short_value = env_.GetMethodID(type_class_, "shortValue", "()S"))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL shortValue of java.lang.Short", K(ret));
  } else {
    int16_t value = env_.CallShortMethod(java_value, short_value);

    if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
      LOG_WARN("failed to call shortValue", K(ret), K(value));
    } else {
      result.set_smallint(value);
    }
  }

  return ret;
}

int ObFromJavaIntegerTypeMapper::operator()(jobject java_value, ObObj &result)
{
  int ret = OB_SUCCESS;

  jmethodID int_value = nullptr;

  if (OB_ISNULL(java_value)) {
    result.set_null();
  } else if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFromJavaIntegerTypeMapper not init", K(ret));
  } else if (!env_.IsInstanceOf(java_value, type_class_)) {
    ret = OB_ERR_EXPRESSION_WRONG_TYPE;
    LOG_WARN("argument is not of expected type, expect java.lang.Integer", K(ret));
  } else if (OB_ISNULL(int_value = env_.GetMethodID(type_class_, "intValue", "()I"))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL intValue of java.lang.Integer", K(ret));
  } else {
    int32_t value = env_.CallIntMethod(java_value, int_value);

    if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
      LOG_WARN("failed to call intValue", K(ret), K(value));
    } else {
      result.set_int32(value);
    }
  }

  return ret;
}

int ObFromJavaLongTypeMapper::operator()(jobject java_value, ObObj &result)
{
  int ret = OB_SUCCESS;

  jmethodID long_value = nullptr;

  if (OB_ISNULL(java_value)) {
    result.set_null();
  } else if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFromJavaLongTypeMapper not init", K(ret));
  } else if (!env_.IsInstanceOf(java_value, type_class_)) {
    ret = OB_ERR_EXPRESSION_WRONG_TYPE;
    LOG_WARN("argument is not of expected type, expect java.lang.Long", K(ret));
  } else if (OB_ISNULL(long_value = env_.GetMethodID(type_class_, "longValue", "()J"))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL longValue of java.lang.Long", K(ret));
  } else {
    int64_t value = env_.CallLongMethod(java_value, long_value);

    if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
      LOG_WARN("failed to call longValue", K(ret), K(value));
    } else {
      result.set_int(value);
    }
  }

  return ret;
}

int ObFromJavaFloatTypeMapper::operator()(jobject java_value, ObObj &result)
{
  int ret = OB_SUCCESS;

  jmethodID float_value = nullptr;

  if (OB_ISNULL(java_value)) {
    result.set_null();
  } else if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFromJavaFloatTypeMapper not init", K(ret));
  } else if (!env_.IsInstanceOf(java_value, type_class_)) {
    ret = OB_ERR_EXPRESSION_WRONG_TYPE;
    LOG_WARN("argument is not of expected type, expect java.lang.Float", K(ret));
  } else if (OB_ISNULL(float_value = env_.GetMethodID(type_class_, "floatValue", "()F"))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL floatValue of java.lang.Float", K(ret));
  } else {
    float value = env_.CallFloatMethod(java_value, float_value);

    if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
      LOG_WARN("failed to call floatValue", K(ret), K(value));
    } else {
      result.set_float(value);
    }
  }

  return ret;
}

int ObFromJavaDoubleTypeMapper::operator()(jobject java_value, ObObj &result)
{
  int ret = OB_SUCCESS;

  jmethodID double_value = nullptr;

  if (OB_ISNULL(java_value)) {
    result.set_null();
  } else if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFromJavaDoubleTypeMapper not init", K(ret));
  } else if (!env_.IsInstanceOf(java_value, type_class_)) {
    ret = OB_ERR_EXPRESSION_WRONG_TYPE;
    LOG_WARN("argument is not of expected type, expect java.lang.Double", K(ret));
  } else if (OB_ISNULL(double_value = env_.GetMethodID(type_class_, "doubleValue", "()D"))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL doubleValue of java.lang.Double", K(ret));
  } else {
    double value = env_.CallDoubleMethod(java_value, double_value);

    if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
      LOG_WARN("failed to call doubleValue", K(ret), K(value));
    } else {
      result.set_double(value);
    }
  }

  return ret;
}

int ObFromJavaBigDecimalTypeMapper::operator()(jobject java_value, ObObj &result)
{
  int ret = OB_SUCCESS;

  jmethodID to_string = nullptr;

  if (OB_ISNULL(java_value)) {
    result.set_null();
  } else if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFromJavaDoubleTypeMapper not init", K(ret));
  } else if (!env_.IsInstanceOf(java_value, type_class_)) {
    ret = OB_ERR_EXPRESSION_WRONG_TYPE;
    LOG_WARN("argument is not of expected type, expect java.math.BigDecimal", K(ret));
  } else if (OB_ISNULL(to_string = env_.GetMethodID(type_class_, "toString", "()Ljava/lang/String;"))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL doubleValue of java.math.BigDecimal", K(ret));
  } else {
    number::ObNumber nmb;
    int16_t precision = PRECISION_UNKNOWN_YET;
    int16_t scale = SCALE_UNKNOWN_YET;
    const char *str = nullptr;

    jstring sci_str = static_cast<jstring>(env_.CallObjectMethod(java_value, to_string));

    if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
      LOG_WARN("failed to call toString", K(ret));
    } else if (OB_ISNULL(sci_str)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL string", K(ret));
    } else if (OB_ISNULL(str = env_.GetStringUTFChars(sci_str, nullptr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL GetStringUTFChars", K(ret));
    } else if (OB_FAIL(nmb.from_sci(str, strlen(str), alloc_, &precision, &scale))) {
      LOG_WARN("failed convert string to ObNumber", K(ret), K(str));
    } else {
      result.set_number(nmb);
    }

    if (OB_NOT_NULL(sci_str) && OB_NOT_NULL(str)) {
      env_.ReleaseStringUTFChars(sci_str, str);
    }

    ObJavaUtils::delete_local_ref(sci_str, &env_);
  }

  return ret;
}

int ObFromJavaStringTypeMapper::operator()(jobject java_value, ObObj &result)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(java_value)) {
    result.set_null();
  } else if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFromJavaStringTypeMapper not init", K(ret));
  } else if (!env_.IsInstanceOf(java_value, type_class_)) {
    ret = OB_ERR_EXPRESSION_WRONG_TYPE;
    LOG_WARN("argument is not of expected type, expect java.lang.String", K(ret));
  } else {
    jmethodID get_bytes = nullptr;
    jstring utf8 = nullptr;
    jstring str = static_cast<jstring>(java_value);
    jobject byte_array = nullptr;
    jbyte *ptr = nullptr;
    int64_t length = OB_INVALID_COUNT;
    ObString res_str;

    if (OB_ISNULL(get_bytes = env_.GetMethodID(type_class_, "getBytes", "(Ljava/lang/String;)[B"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL getBytes of java.lang.String", K(ret));
    } else if (OB_ISNULL(utf8 = env_.NewStringUTF("UTF-8"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL NewStringUTF", K(ret));
    } else if (FALSE_IT(byte_array = env_.CallObjectMethod(str, get_bytes, utf8))) {
      // unreachable
    } else if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
      LOG_WARN("failed to call getBytes of java.lang.String", K(ret));
    } else if (OB_ISNULL(byte_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL getBytes of java.lang.String", K(ret));
    } else if (OB_ISNULL(ptr = env_.GetByteArrayElements(static_cast<jbyteArray>(byte_array), nullptr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL GetByteArrayElements", K(ret));
    } else if (0 > (length = env_.GetArrayLength(static_cast<jbyteArray>(byte_array)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected length of GetArrayLength", K(ret), K(length));
    } else if (OB_FAIL(ob_write_string(alloc_, ObString(length, reinterpret_cast<const char*>(ptr)), res_str))) {
      LOG_WARN("failed to ob_write_string", K(ret), K(ptr));
    } else {
      result.set_varchar(res_str);
      result.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    }

    if (OB_NOT_NULL(byte_array)) {
      if (OB_NOT_NULL(ptr)) {
        env_.ReleaseByteArrayElements(static_cast<jbyteArray>(byte_array), ptr, JNI_ABORT);
        ptr = nullptr;
      }

      env_.DeleteLocalRef(byte_array);
      byte_array = nullptr;
    }

    ObJavaUtils::delete_local_ref(utf8, &env_);
  }

  return ret;
}

int ObFromJavaByteBufferTypeMapper::operator()(jobject java_value, ObObj &result)
{
  int ret = OB_SUCCESS;

  jmethodID remaining = nullptr;
  int32_t need_size = OB_INVALID_SIZE;
  jmethodID get = nullptr;
  jmethodID rewind = nullptr;
  jobject tmp_obj = nullptr;
  jbyteArray byte_array = nullptr;
  jbyte *bytes = nullptr;
  jobject buffer = nullptr;

  ObString binary;

  if (OB_ISNULL(java_value)) {
    result.set_null();
  } else if (OB_ISNULL(type_class_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFromJavaStringTypeMapper not init", K(ret));
  } else if (!env_.IsInstanceOf(java_value, type_class_)) {
    ret = OB_ERR_EXPRESSION_WRONG_TYPE;
    LOG_WARN("argument is not of expected type, expect java.nio.ByteBuffer", K(ret));
  } else if (OB_ISNULL(rewind = env_.GetMethodID(type_class_, "rewind", "()Ljava/nio/Buffer;"))
               && OB_ISNULL(rewind = env_.GetMethodID(type_class_, "rewind", "()Ljava/nio/ByteBuffer;"))) {
    // there is a break change in JDK8 to JDK9, so we have to try twice to find rewind method.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL rewind of java.nio.ByteBuffer", K(ret));
  } else if (OB_FALSE_IT(tmp_obj = env_.CallObjectMethod(java_value, rewind))) {
    // unreachable
  } else if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
    LOG_WARN("failed to call rewind of java.nio.ByteBuffer", K(ret));
  } else if (OB_ISNULL(remaining = env_.GetMethodID(type_class_, "remaining", "()I"))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL remaining of java.nio.ByteBuffer", K(ret));
  } else if (OB_FALSE_IT(need_size = env_.CallIntMethod(java_value, remaining))) {
    // unreachable
  } else if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
    LOG_WARN("failed to call remaining of java.nio.ByteBuffer", K(ret));
  } else if (OB_ISNULL(byte_array = env_.NewByteArray(need_size))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL NewByteArray", K(ret));
  } else if (OB_ISNULL(get = env_.GetMethodID(type_class_, "get", "([B)Ljava/nio/ByteBuffer;"))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL get of java.nio.ByteBuffer", K(ret));
  } else if (OB_FALSE_IT(buffer = env_.CallObjectMethod(java_value, get, byte_array))) {
    // unreachable
  } else if (OB_FAIL(ObJavaUtils::exception_check(&env_))) {
    LOG_WARN("failed to call get of java.nio.ByteBuffer", K(ret));
  } else if (OB_ISNULL(bytes = env_.GetByteArrayElements(byte_array, nullptr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL NewByteArray content", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc_,
                                     ObString(need_size, reinterpret_cast<char*>(bytes)),
                                     binary))) {
    LOG_WARN("failed to deep copy byte array", K(ret));
  } else {
    result.set_varbinary(binary);
  }

  if (OB_NOT_NULL(byte_array) && OB_NOT_NULL(bytes)) {
    env_.ReleaseByteArrayElements(byte_array, bytes, JNI_ABORT);
    bytes = nullptr;
  }

  ObJavaUtils::delete_local_ref(tmp_obj, &env_);
  ObJavaUtils::delete_local_ref(byte_array, &env_);
  ObJavaUtils::delete_local_ref(buffer, &env_);

  return ret;
}

} // namespace pl
} // namespace oceanbase
