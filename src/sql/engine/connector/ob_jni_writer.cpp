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

#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"

#include "ob_java_helper.h"
#include "ob_jni_writer.h"
#include <memory>

namespace oceanbase {
namespace sql {


int JniWriter::init_params(const common::hash::ObHashMap<ObString, ObString> &params) {
  int ret = OB_SUCCESS;
  if (is_params_created()) {
    LOG_INFO("jni writer is already inited, skip to re-init", K(ret));
  } else if (OB_FAIL(detect_java_runtime())) {
    ret = OB_JNI_JAVA_HOME_NOT_FOUND_ERROR;
    LOG_WARN("failed to detect java runtime", K(ret));
  } else if (params.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to use empty params for initializing jni scanner", K(ret));
  } else if (OB_FAIL(
    writer_params_.create(MAX_PARAMS_SIZE, "IntoOdps", "IntoOdps", MTL_ID())
  )) {
    LOG_WARN("failed to create scanner params map", K(ret));
  } else if (false == writer_params_.created()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scanner params map is not created", K(ret));
  } else {
    params_created_ = true;
    common::hash::ObHashMap<ObString, ObString>::const_iterator params_iter;
    for (params_iter = params.begin();
         OB_SUCC(ret) && params_iter != params.end(); ++params_iter) {
      LOG_TRACE("scanner params", K(ret), K(params_iter->first), K(params_iter->second));
      // init scanner params
      if (OB_FAIL(writer_params_.set_refactored(params_iter->first,
                                                params_iter->second))) {
        LOG_WARN("failed to set scanner params", K(ret));
      }
    }
  }

  return ret;
}

int JniWriter::do_open() {
  int ret = OB_SUCCESS;
  if (!is_params_created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("writer params is not created", K(ret));
  } else if (is_open()) {
    LOG_INFO("jni scanner is already opend, skip get jni env");
  } else {
    JNIEnv *env = nullptr;
    if (OB_FAIL(get_jni_env(env))) {
        LOG_WARN("failed to get jni env", K(ret));
    } else if (nullptr == env) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get a null jni env", K(ret));
    }
    if (OB_SUCC(ret) &&
      env->EnsureLocalCapacity(writer_params_.size() * 2 + 6) < 0) {
      if (OB_FAIL(check_jni_exception_(env))) {
        LOG_WARN("failed to ensure the local capacity", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(init_jni_table_writer_(env))) {
        LOG_WARN("failed to init table write", K(ret));
      } else if (OB_FAIL(init_jni_method_(env))) {
        LOG_WARN("failed to init jni method", K(ret));
      } else {
        is_opened_ = true;
      }
    }
  }
  
  return ret;
}
int JniWriter::init_jni_table_writer_(JNIEnv *env)
{
  int ret = OB_SUCCESS;
  jclass writer_factory_class = env->FindClass(jni_writer_factory_class_.ptr());
  if (OB_ISNULL(writer_factory_class)) {
    ret = OB_JNI_CLASS_NOT_FOUND_ERROR;
    LOG_WARN("failed to found class: writer", K(ret));
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("jni is with exception", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    jmethodID writer_factory_constructor = env->GetMethodID(writer_factory_class, "<init>", "()V");
    if (OB_ISNULL(writer_factory_constructor)) {
      ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
      LOG_WARN("failed to find writer constructor", K(ret));
    } else {
      jobject writer_factory_obj = env->NewObject(writer_factory_class, writer_factory_constructor);
      jmethodID get_writer_method = env->GetMethodID(writer_factory_class, "getWriterClass", "()Ljava/lang/Class;");
      if (OB_ISNULL(get_writer_method)) {
        ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
        LOG_WARN("failed to get writer method", K(ret));
      } else {
        jni_writer_cls_ = (jclass)env->CallObjectMethod(writer_factory_obj, get_writer_method);
        if (OB_FAIL(check_jni_exception_(env))) {
          ret = OB_JNI_ERROR;
          LOG_WARN("failed to init writer class.", K(ret));
        } else {
          env->DeleteLocalRef(writer_factory_class);
          env->DeleteLocalRef(writer_factory_obj);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    bool is_get_session = writer_params_.get(ObString::make_string("session_id")) == nullptr;
    jmethodID writer_constructor = is_get_session
                                       ? env->GetMethodID(jni_writer_cls_, "<init>", "(Ljava/util/Map;)V")
                                       : env->GetMethodID(jni_writer_cls_, "<init>", "(ILjava/util/Map;)V");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to get a writer class constructor.", K(ret));
    } else {
      jclass hashmap_class = env->FindClass("java/util/HashMap");
      jmethodID hashmap_constructor = env->GetMethodID(hashmap_class, "<init>", "(I)V");
      jobject hashmap_object = env->NewObject(hashmap_class, hashmap_constructor, writer_params_.size());
      jmethodID hashmap_put =
          env->GetMethodID(hashmap_class, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
      if (OB_FAIL(check_jni_exception_(env))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("failed to get the HashMap methods.", K(ret));
      } else {
        common::ObSqlString message;
        message.append("Initialize a scanner with parameters:");

        for (const auto &it : writer_params_) {
          jstring key = env->NewStringUTF(it.first.ptr());
          jstring value = env->NewStringUTF(it.second.ptr());
          message.append(it.first);
          message.append("->");
          message.append(it.second);
          message.append(", ");

          env->CallObjectMethod(hashmap_object, hashmap_put, key, value);
          env->DeleteLocalRef(key);
          env->DeleteLocalRef(value);

        }
        env->DeleteLocalRef(hashmap_class);
        // LOG_INFO("Initialze the writer with parameters:", K(ret), K(message), K(is_get_session));
        if (is_get_session) {
          jni_writer_obj_ = env->NewObject(jni_writer_cls_, writer_constructor, hashmap_object);
          is_get_session = false;
        } else {
          jni_writer_obj_ = env->NewObject(jni_writer_cls_, writer_constructor, 1, hashmap_object);
        }
        if (OB_FAIL(check_jni_exception_(env))) {
          ret = OB_JNI_ERROR;
          LOG_WARN("failed to initialize a writer instance.", K(ret));
        } 
        if (OB_SUCC(ret)) {
          env->DeleteLocalRef(hashmap_object);
          if (OB_FAIL(check_jni_exception_(env))) {
            ret = OB_JNI_ERROR;
            LOG_WARN("failed to initialize a writer instance.", K(ret));
          } else if (nullptr == jni_writer_obj_) {
            ret = OB_JNI_ERROR;
            LOG_WARN("jni scanner obj is null", K(ret));
          }
        }
      }
    }
  }
  return ret;
}


int JniWriter::init_jni_method_(JNIEnv *env) {
  int ret = OB_SUCCESS;
  // init jmethod
  if (OB_SUCC(ret)) {
    jni_writer_open_ = env->GetMethodID(jni_writer_cls_, "open", "(I)V");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `open` jni method", K(ret));
    } else if (OB_UNLIKELY(nullptr == jni_writer_open_)) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `open` jni method", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    jni_writer_get_session_id_ =
      env->GetMethodID(jni_writer_cls_, "getSessionId", "()Ljava/lang/String;");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `get_session id` jni method", K(ret));
    } else if (OB_UNLIKELY(nullptr == jni_writer_get_session_id_)) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `get_session id` jni method", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    jni_writer_write_next_brs_ = 
      env->GetMethodID(jni_writer_cls_, "writeNextBrs", "(JI)V");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `writeNextBrs` jni method", K(ret));
    } else if (OB_UNLIKELY(nullptr == jni_writer_write_next_brs_)) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `writeNextBrs` jni method", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    jni_commit_session_ = env->GetMethodID(jni_writer_cls_, "commitSession", "(J)V");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `commitSession`", K(ret));
    } else if (OB_UNLIKELY(nullptr == jni_commit_session_)) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `commitSession`", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    jni_append_block_id_ = env->GetMethodID(jni_writer_cls_, "appendBlockId", "(J)V");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to `appendBlockId`", K(ret));
    } else if (OB_UNLIKELY(nullptr == jni_append_block_id_)) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `commitSession`", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    jni_get_scheam_address_ = env->GetMethodID(jni_writer_cls_, "getSchemaAddress", "()J");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `open` jni method", K(ret));
    } else if (OB_UNLIKELY(nullptr == jni_get_scheam_address_)) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `open` jni method", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    jni_get_array_address_ = env->GetMethodID(jni_writer_cls_, "getArrayAddress", "()J");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `open` jni method", K(ret));
    } else if (OB_UNLIKELY(nullptr == jni_get_array_address_)) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `open` jni method", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    jni_get_export_schema_address_ = env->GetMethodID(jni_writer_cls_, "getExportToObSchemaAddress", "()J");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `open` jni method", K(ret));
    } else if (OB_UNLIKELY(nullptr == jni_get_export_schema_address_)) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `open` jni method", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    jni_writer_get_odps_schema_ = env->GetMethodID(jni_writer_cls_, "getSchemaFromOdps", "()Ljava/util/List;");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `open` jni method", K(ret));
    } else if (OB_UNLIKELY(nullptr == jni_writer_get_odps_schema_)) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `open` jni method", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    jni_writer_close_ = env->GetMethodID(jni_writer_cls_, "close", "()V");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `open` jni method", K(ret));
    } else if (OB_UNLIKELY(nullptr == jni_writer_close_)) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `open` jni method", K(ret));
    }
  }
  return ret;
}


int JniWriter::get_current_block_addr() {
  int ret = OB_SUCCESS;

  JNIEnv *env = nullptr;
  if (OB_FAIL(get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (nullptr == env) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get a null jni env", K(ret));
  }
  schema_ptr_ = 0;
  array_ptr_ = 0;
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(jni_writer_obj_) || OB_ISNULL(jni_get_scheam_address_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("jni writer is not initialized", K(ret));
    } else {
      jlong value = env->CallLongMethod(jni_writer_obj_, jni_get_scheam_address_);
      if (OB_FAIL(check_jni_exception_(env))) {
        LOG_WARN("failed to get schema address", K(ret));
      }
      schema_ptr_ = value;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(jni_writer_obj_) || OB_ISNULL(jni_get_scheam_address_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("jni writer is not initialized", K(ret));
    } else {
      jlong value = env->CallLongMethod(jni_writer_obj_, jni_get_array_address_);
      if (OB_FAIL(check_jni_exception_(env))) {
        LOG_WARN("failed to open off-heap table writer", K(ret));
      }
      array_ptr_ = value;
    }
  }
  return ret;
}

int JniWriter::do_open_record(int block_id) {
  int ret = OB_SUCCESS;

  JNIEnv *env = nullptr;
  if (OB_FAIL(get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (nullptr == env) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get a null jni env", K(ret));
  }
  if (OB_SUCC(ret)) {
    
    env->CallVoidMethod(jni_writer_obj_, jni_writer_open_, block_id);
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to open record writer", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    jlong value = env->CallLongMethod(jni_writer_obj_, jni_get_export_schema_address_);
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to open off-heap table writer", K(ret));
    }
    export_schema_ptr_ = value;
  }

  if (OB_SUCC(ret)) {
    jclass listClass = nullptr;
    jclass integerClass = nullptr;
    jmethodID sizeMethod = nullptr;
    jmethodID getMethod  = nullptr;
    jmethodID intValueMethod  = nullptr;
    jint size = 0;

    jobject listObject = nullptr;
    if (OB_ISNULL(listObject = env->CallObjectMethod(jni_writer_obj_, jni_writer_get_odps_schema_))) {
      ret = OB_JNI_ENV_ERROR;
      LOG_WARN("failed to get odps schema", K(ret));
    } else if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to get odps schema", K(ret));
    } else if (OB_ISNULL(listClass = env->FindClass("java/util/List"))) {
      ret = OB_JNI_CLASS_NOT_FOUND_ERROR;
      LOG_WARN("failed to find class", K(ret));
    } else if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to get odps schema", K(ret));
    } else if (OB_ISNULL(sizeMethod = env->GetMethodID(listClass, "size", "()I"))) {
      ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
      LOG_WARN("failed to find method", K(ret));
    } else if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to get odps schema", K(ret));
    } else if (OB_ISNULL(getMethod = env->GetMethodID(listClass, "get", "(I)Ljava/lang/Object;"))) {
      ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
      LOG_WARN("failed to find method", K(ret));
    } else if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to get odps schema", K(ret));
    } else if (sizeMethod == nullptr || getMethod == nullptr) {
      ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
      LOG_WARN("failed to find method", K(ret));
    } else if (FALSE_IT(size  = env->CallIntMethod(listObject, sizeMethod))) {
      /* do nothing */
    } else if (OB_FAIL((check_jni_exception_(env)))) {
      LOG_WARN("failed to get odps schema", K(ret));
    } else if (OB_ISNULL(integerClass = env->FindClass("java/lang/Integer"))) {
      ret = OB_JNI_CLASS_NOT_FOUND_ERROR;
      LOG_WARN("failed to find class", K(ret));
    } else if (OB_ISNULL(intValueMethod = env->GetMethodID(integerClass, "intValue", "()I"))) {
      ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
      LOG_WARN("failed to find method", K(ret));
    } else {
      column_types_.set_attr(lib::ObMemAttr(MTL_ID(), "IntoOdps"));
      for (jint i = 0; OB_SUCC(ret) && i < size; ++i) {
        jobject integerObject = env->CallObjectMethod(listObject, getMethod, i);
        if (integerObject != nullptr) {
          jint value = env->CallIntMethod(integerObject, intValueMethod);
          if (OB_FAIL(check_jni_exception_(env))) {
            LOG_WARN("failed to get odps schema", K(ret));
          } else if (OB_FAIL(column_types_.push_back(static_cast<OdpsType>(value)))) {
            LOG_WARN("failed to push back schema ptr", K(ret));
          }
          env->DeleteLocalRef(integerObject);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get integer object");
        }
      }
    }

    // final block we need to delete ref of all obj
    if (OB_NOT_NULL(listObject)) {
      env->DeleteLocalRef(listObject);
    }
    if (OB_NOT_NULL(listClass)) {
      env->DeleteLocalRef(listClass);
    }
  }
  return ret;
}

int JniWriter::do_write_next_brs(void *brs, int batch_size) {
  int ret = OB_SUCCESS;
  JNIEnv *env = nullptr;
  if (OB_FAIL(get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (nullptr == env) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get a null jni env", K(ret));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(jni_writer_obj_) && OB_NOT_NULL(jni_writer_write_next_brs_)) {
    env->CallVoidMethod(jni_writer_obj_, jni_writer_write_next_brs_, brs, batch_size);
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to write heap", K(ret));
    }
  }
  return ret;
}

int JniWriter::get_session_id(ObIAllocator& outstring_alloc_, ObString& sid) {
  int ret = OB_SUCCESS;

  JNIEnv *env = nullptr;
  if (OB_FAIL(get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (nullptr == env) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("unexpected null jni env", K(ret));
  }

  if (OB_FAIL(ret)) {
    /* do nothing */
  } else if (OB_ISNULL(jni_writer_obj_) || OB_ISNULL(jni_writer_get_session_id_)) {
    ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
    LOG_WARN("failed to find method", K(ret));
  } else {
    jstring sid_str = (jstring)env->CallObjectMethod(jni_writer_obj_, jni_writer_get_session_id_);
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to open off-heap table writer", K(ret));
    } else {
      const char *cstr = env->GetStringUTFChars(sid_str, NULL);
      if (cstr != nullptr) {
        if (OB_FAIL(common::ob_write_string(outstring_alloc_, ObString(cstr), sid, false))) {
          LOG_WARN("failed to get output string", K(ret));
        }
      } else {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("unexpected bad null env", K(ret));
      }
    }
  }
  
  return ret;
}

int JniWriter::finish_write()
{
  int ret = OB_SUCCESS;

  JNIEnv *env = nullptr;
  if (OB_FAIL(get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (nullptr == env) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("unexpected null jni env", K(ret));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(jni_writer_obj_) && OB_NOT_NULL(jni_writer_close_)) {
    env->CallVoidMethod(jni_writer_obj_, jni_writer_close_);
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to finish write", K(ret));
    }
  }

  return ret;
}

int JniWriter::append_block_id(int64_t block_id)
{
  int ret = OB_SUCCESS;
 
  JNIEnv *env = nullptr;
  if (OB_FAIL(get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (nullptr == env) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("unexpected null jni env", K(ret));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(jni_writer_obj_) && OB_NOT_NULL(jni_append_block_id_)) {
    env->CallVoidMethod(jni_writer_obj_, jni_append_block_id_, block_id);
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to open off-heap table writer", K(ret));
    }
  }
  return ret;
}

int JniWriter::commit_session(int64_t num_block)
{
  int ret = OB_SUCCESS;
 
  JNIEnv *env = nullptr;
  if (OB_FAIL(get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (nullptr == env) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("unexpected null jni env", K(ret));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(jni_writer_obj_) && OB_NOT_NULL(jni_commit_session_)) {
    env->CallVoidMethod(jni_writer_obj_, jni_commit_session_, num_block);
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to open off-heap table writer", K(ret));
    }
  }
  return ret;
}

int JniWriter::do_close()
{
  int ret = OB_SUCCESS;
  if (!is_params_created()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("jni scanner is not inited, skip to close", K(ret));
  } else if (!is_open()) {
    LOG_WARN("jni scanner is not opened, but inited", K(ret));
    writer_params_.reuse();
    params_created_ = false;
  } else {
    JNIEnv *env = nullptr;
    if (OB_FAIL(get_jni_env(env))) {
      LOG_WARN("failed to get jni env", K(ret));
    } else if (nullptr == env) {
      ret = OB_JNI_ENV_ERROR;
      LOG_WARN("unexpected null jni env", K(ret));
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(jni_writer_obj_)) {
      env->DeleteLocalRef(jni_writer_obj_);
      jni_writer_obj_ = nullptr;
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(jni_writer_cls_)) {
      env->DeleteLocalRef(jni_writer_cls_);
      jni_writer_cls_ = nullptr;
    }
    writer_params_.reuse();
    params_created_ = false;
    is_opened_ = false;
    if (OB_SUCC(ret) && OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to close the jni env", K(ret));
    }
  }
  return ret;
}

JNIWriterPtr create_odps_jni_writer()
{
  const char *writer_factory_class = "com/oceanbase/external/odps/utils/OdpsTunnelConnectorFactory";
  return std::make_shared<JniWriter>(writer_factory_class);
}

}  // namespace sql
}  // namespace oceanbase
