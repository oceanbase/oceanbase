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
#define USING_LOG_PREFIX SQL_ENG
#include <jni.h>
#include "ob_odps_jni_connector.h"
#include "lib/jni_env/ob_java_env.h"

namespace oceanbase {

namespace sql {

int ObOdpsJniConnector::get_jni_class(JNIEnv *env, const char *class_name, jclass &class_obj) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(env)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("failed to get jni env", K(ret), K(lbt()));
  } else {
    class_obj = env->FindClass(class_name);
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("jni is with exception", K(ret));
    } else if (OB_ISNULL(class_obj)) {
      ret = OB_JNI_CLASS_NOT_FOUND_ERROR;
      LOG_WARN("failed to found class: ", K(class_name), K(ret));
    }
  }
  return ret;
}

int ObOdpsJniConnector::get_jni_method(JNIEnv *env, jclass class_obj, const char *method_name, const char *method_signature, jmethodID &method_id) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(env)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("failed to get jni env", K(ret), K(lbt()));
  } else {
    method_id = env->GetMethodID(class_obj, method_name, method_signature);
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("jni is with exception", K(ret));
    } else if (OB_ISNULL(method_id)) {
      ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
      LOG_WARN("failed to found method: ", K(method_name), K(ret));
    }
  }
  return ret;
}

int ObOdpsJniConnector::construct_jni_object(JNIEnv *env, jobject &object, jclass clazz, jmethodID constructorMethodID, ...) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(env)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("failed to get jni env", K(ret), K(lbt()));
  } else {
    va_list args;
    va_start(args, constructorMethodID);
    object = env->NewObjectV(clazz, constructorMethodID, args);
    va_end(args);
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("jni is with exception", K(ret));
    } else if (OB_ISNULL(object)) {
      ret = OB_JNI_OBJECT_NOT_FOUND_ERROR;
      LOG_WARN("failed to found object: ", K(clazz), K(ret));
    }
  }
  return ret;
}

int ObOdpsJniConnector::gen_jni_string(JNIEnv *env, const char *str, jstring &j_str) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(env)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("failed to get jni env", K(ret), K(lbt()));
  } else if (OB_ISNULL(str)) {
    j_str = nullptr;
  } else {
    j_str = env->NewStringUTF(str);
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("jni is with exception", K(ret));
    } else if (OB_ISNULL(j_str)) {
      ret = OB_JNI_OBJECT_NOT_FOUND_ERROR;
      LOG_WARN("failed to gen object: ", K(str), K(ret));
    }
  }
  return ret;
}

int ObOdpsJniConnector::check_jni_exception_(JNIEnv *env) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(env)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("failed to get jni env", K(ret), K(lbt()));
  } else {
    jthrowable thr = env->ExceptionOccurred();
    if (!OB_ISNULL(thr)) {
      ret = OB_JNI_JAVA_EXCEPTION_ERROR;
      jclass throwableClass = env->GetObjectClass(thr);

      jmethodID getMessageMethod = env->GetMethodID(
          throwableClass,
          "getMessage",
          "()Ljava/lang/String;"
      );

      if (getMessageMethod != nullptr) {
          jstring jmsg = (jstring)env->CallObjectMethod(thr, getMessageMethod);
          if (env->ExceptionCheck()) {
              env->ExceptionClear(); // 防止调用过程中产生新异常
          }

          if (jmsg != nullptr) {
              const char* cmsg = env->GetStringUTFChars(jmsg, nullptr);
              int64_t len = env->GetStringUTFLength(jmsg);
              LOG_WARN("Exception Message: ", K(cmsg), K(ret));
              LOG_USER_ERROR(OB_JNI_JAVA_EXCEPTION_ERROR, len, cmsg);
              env->ReleaseStringUTFChars(jmsg, cmsg);
              env->DeleteLocalRef(jmsg);
          }
      }

      // 创建StringWriter和PrintWriter
      jclass stringWriterClass = env->FindClass("java/io/StringWriter");
      jmethodID stringWriterCtor = env->GetMethodID(stringWriterClass, "<init>", "()V");
      jobject stringWriter = env->NewObject(stringWriterClass, stringWriterCtor);

      jclass printWriterClass = env->FindClass("java/io/PrintWriter");
      jmethodID printWriterCtor = env->GetMethodID(printWriterClass, "<init>", "(Ljava/io/Writer;)V");
      jobject printWriter = env->NewObject(printWriterClass, printWriterCtor, stringWriter);

      // 调用printStackTrace
      jmethodID printStackTraceMethod = env->GetMethodID(
          throwableClass,
          "printStackTrace",
          "(Ljava/io/PrintWriter;)V"
      );
      env->CallVoidMethod(thr, printStackTraceMethod, printWriter);

      // 获取堆栈字符串
      jmethodID toStringMethod = env->GetMethodID(
          stringWriterClass,
          "toString",
          "()Ljava/lang/String;"
      );
      jstring stackTrace = (jstring)env->CallObjectMethod(stringWriter, toStringMethod);

      // 转换为C字符串
      const char* cStackTrace = env->GetStringUTFChars(stackTrace, nullptr);
      LOG_WARN("Exception Stack Trace: ", K(cStackTrace));

      // 释放资源
      env->ReleaseStringUTFChars(stackTrace, cStackTrace);
      env->DeleteLocalRef(stackTrace);
      env->DeleteLocalRef(printWriter);
      env->DeleteLocalRef(stringWriter);
      env->DeleteLocalRef(throwableClass);
      env->DeleteLocalRef(printWriterClass);
      env->DeleteLocalRef(stringWriterClass);

      env->ExceptionDescribe();
      env->ExceptionClear(); // 清除异常状态
      env->DeleteLocalRef(thr);
    }
  }
  return ret;
}

ObOdpsJniConnector::OdpsType ObOdpsJniConnector::get_odps_type_by_string(ObString type)
{
  OdpsType ret_type = OdpsType::UNKNOWN;
  if (type.prefix_match("BOOLEAN")) {
    ret_type = OdpsType::BOOLEAN;
  } else if (type.prefix_match("TINYINT")) {
    ret_type = OdpsType::TINYINT;
  } else if (type.prefix_match("SMALLINT")) {
    ret_type = OdpsType::SMALLINT;
  } else if (type.prefix_match("INT")) {
    ret_type = OdpsType::INT;
  } else if (type.prefix_match("BIGINT")) {
    ret_type = OdpsType::BIGINT;
  } else if (type.prefix_match("FLOAT")) {
    ret_type = OdpsType::FLOAT;
  } else if (type.prefix_match("DOUBLE")) {
    ret_type = OdpsType::DOUBLE;
  } else if (type.prefix_match("DECIMAL")) {
    ret_type = OdpsType::DECIMAL;
  } else if (type.prefix_match("VARCHAR")) {
    ret_type = OdpsType::VARCHAR;
  } else if (type.prefix_match("CHAR")) {
    ret_type = OdpsType::CHAR;
  } else if (type.prefix_match("STRING")) {
    ret_type = OdpsType::STRING;
  } else if (type.prefix_match("BINARY")) {
    ret_type = OdpsType::BINARY;
  } else if (type.prefix_match("TIMESTAMP_NTZ")) {
    ret_type = OdpsType::TIMESTAMP_NTZ;
  } else if (type.prefix_match("TIMESTAMP")) {
    ret_type = OdpsType::TIMESTAMP;
  } else if (type.prefix_match("DATETIME")) {
    ret_type = OdpsType::DATETIME;
  } else if (type.prefix_match("DATE")) {
    ret_type = OdpsType::DATE;
  } else if (type.prefix_match("MAP")) {
    ret_type = OdpsType::MAP;
  } else if (type.prefix_match("ARRAY")) {
    ret_type = OdpsType::ARRAY;
  } else if (type.prefix_match("STRUCT")) {
    ret_type = OdpsType::STRUCT;
  } else if (type.prefix_match("JSON")) {
    ret_type = OdpsType::JSON;
  } else if (type.prefix_match("ARRAY")) {
    ret_type = OdpsType::ARRAY;
  } else if (type.prefix_match("INTERVAL_YEAR_MONTH")) {
    ret_type = OdpsType::INTERVAL_YEAR_MONTH;
  } else if (type.prefix_match("INTERVAL_DAY_TIME")) {
    ret_type = OdpsType::INTERVAL_DAY_TIME;
  } else if (type.prefix_match("VOID")) {
    ret_type = OdpsType::ARRAY;
  }
  return ret_type;
}

int ObOdpsJniConnector::MirrorOdpsJniColumn::assign(const MirrorOdpsJniColumn &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    name_ = other.name_;
    type_ = other.type_;
    precision_ = other.precision_;
    scale_ = other.scale_;
    length_ = other.length_;
    type_size_ = other.type_size_;
    type_expr_ = other.type_expr_;
    if (OB_FAIL(child_columns_.assign(other.child_columns_))) {
      LOG_WARN("failed to assign array");
    }
  }
  return ret;
}

ObObjType ObOdpsJniConnector::get_ob_type_by_odps_type_default(OdpsType type) {
  ObObjType ret_type = ObObjType::ObMaxType;
  switch (type) {
    case OdpsType::BOOLEAN:
      ret_type = ObObjType::ObTinyIntType;
      break;
    case OdpsType::TINYINT:
      ret_type = ObObjType::ObTinyIntType;
      break;
    case OdpsType::SMALLINT:
      ret_type = ObObjType::ObSmallIntType;
      break;
    case OdpsType::INT:
      ret_type = ObObjType::ObInt32Type;
      break;
    case OdpsType::BIGINT:
      ret_type = ObObjType::ObIntType;
      break;
    case OdpsType::FLOAT:
      ret_type = ObObjType::ObFloatType;
      break;
    case OdpsType::DOUBLE:
      ret_type = ObObjType::ObDoubleType;
      break;
    case OdpsType::DECIMAL:
      ret_type = ObObjType::ObDecimalIntType;
      break;
    case OdpsType::VARCHAR:
      ret_type = ObObjType::ObVarcharType;
      break;
    case OdpsType::CHAR:
      ret_type = ObObjType::ObCharType;
      break;
    case OdpsType::STRING:
      ret_type = ObObjType::ObMediumTextType;
      break;
    case OdpsType::BINARY:
      ret_type = ObObjType::ObTextType;
      break;
    case OdpsType::TIMESTAMP_NTZ:
      ret_type = ObObjType::ObTimestampLTZType;
      break;
    case OdpsType::TIMESTAMP:
      ret_type = ObObjType::ObTimestampType;
      break;
    case OdpsType::DATETIME:
      ret_type = ObObjType::ObDateTimeType;
      break;
    case OdpsType::DATE:
      ret_type = ObObjType::ObDateType;
      break;
    case OdpsType::ARRAY:
      ret_type = ObObjType::ObCollectionSQLType;
      break;
    case OdpsType::JSON:
      ret_type = ObObjType::ObJsonType;
      break;
    default:
      ret_type = ObObjType::ObMaxType;
  }

  return ret_type;
}

}
}
