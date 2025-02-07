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

#include <memory>
#include <unordered_set>

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"

#include "ob_java_env.h"
#include "ob_java_helper.h"
#include "ob_jni_scanner.h"

namespace oceanbase {

namespace sql {

int JniScanner::init_jni_method_(JNIEnv *env) {
  int ret = OB_SUCCESS;
  // init jmethod
  jni_scanner_open_ = env->GetMethodID(jni_scanner_cls_, "open", "()V");
  if (OB_FAIL(check_jni_exception_(env))) {
    ret = OB_INVALID_ERROR;
    LOG_WARN("failed to get `open` jni method", K(ret));
  } else { /* do nothing */
  }

  if (OB_SUCC(ret)) {
    jni_scanner_get_next_batch_ =
        env->GetMethodID(jni_scanner_cls_, "getNextOffHeapChunk", "()J");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `open` jni method", K(ret));
    } else { /* do nothing */
    }
  } else { /* do nothing */
  }

  if (OB_SUCC(ret)) {
    jni_scanner_close_ = env->GetMethodID(jni_scanner_cls_, "close", "()V");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `open` jni method", K(ret));
    } else { /* do nothing */
    }
  } else { /* do nothing */
  }

  if (OB_SUCC(ret)) {
    jni_scanner_release_column_ = env->GetMethodID(
        jni_scanner_cls_, "releaseOffHeapColumnVector", "(I)V");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `releaseOffHeapColumnVector` jni method", K(ret));
    } else { /* do nothing */
    }
  } else { /* do nothing */
  }

  if (OB_SUCC(ret)) {
    jni_scanner_release_table_ =
        env->GetMethodID(jni_scanner_cls_, "releaseOffHeapTable", "()V");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ERROR;
      LOG_WARN("failed to get `releaseOffHeapTable` jni method", K(ret));
    } else { /* do nothing */ }
  } else { /* do nothing */ }
  return ret;
}

int JniScanner::init_jni_table_scanner_(JNIEnv *env) {
  int ret = OB_SUCCESS;
  jclass scanner_factory_class =
      env->FindClass(jni_scanner_factory_class_.ptr());
  if (OB_ISNULL(scanner_factory_class)) {
    ret = OB_JNI_CLASS_NOT_FOUND_ERROR;
    LOG_WARN("failed to find class: scanner", K(ret), K(jni_scanner_factory_class_));
    // Because the scanner factory class may be initialized failed too which
    // should log
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("jni is with exception", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    jmethodID scanner_factory_constructor =
        env->GetMethodID(scanner_factory_class, "<init>", "()V");
    if (OB_ISNULL(scanner_factory_constructor)) {
      ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
      LOG_WARN("failed to find scanner constructor", K(ret));
    } else {
      jobject scanner_factory_obj =
          env->NewObject(scanner_factory_class, scanner_factory_constructor);
      if (OB_ISNULL(scanner_factory_obj)) {
        ret = OB_JNI_ERROR;
        LOG_WARN("failed to create scanner factory object", K(ret));
      } else {
        jmethodID get_scanner_method =
            env->GetMethodID(scanner_factory_class, "getScannerClass",
                            "(Ljava/lang/String;)Ljava/lang/Class;");
        if (OB_ISNULL(get_scanner_method)) {
          ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
          LOG_WARN("failed to get scanner class method", K(ret));
        } else {
          jstring scanner_type = env->NewStringUTF(scanner_type_.ptr());
          jni_scanner_cls_ = (jclass)env->CallObjectMethod(
              scanner_factory_obj, get_scanner_method, scanner_type);
          if (OB_FAIL(check_jni_exception_(env))) {
            ret = OB_JNI_ERROR;
            LOG_WARN("failed to init the scanner class.", K(ret));
          }
          env->DeleteLocalRef(scanner_type);
        }
        env->DeleteLocalRef(scanner_factory_obj);
      }
    }
    env->DeleteLocalRef(scanner_factory_class);
  } else { /*do nothing*/
  }

  if (OB_SUCC(ret)) {
    // Note: schema scanner can only transfer the needed params.
    // example: com.oceanbase.odps.reader.OdpsTunnelScanner

    jmethodID scanner_constructor =
        is_schema_scanner_
            ? env->GetMethodID(jni_scanner_cls_, "<init>", "(Ljava/util/Map;)V")
            : env->GetMethodID(jni_scanner_cls_, "<init>", "(ILjava/util/Map;)V");
    if (OB_FAIL(check_jni_exception_(env))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to get a scanner class constructor.", K(ret));
    } else {
      jclass hashmap_class = env->FindClass("java/util/HashMap");
      jmethodID hashmap_constructor =
          env->GetMethodID(hashmap_class, "<init>", "(I)V");
      jobject hashmap_object = env->NewObject(
          hashmap_class, hashmap_constructor, scanner_params_.size());
      jmethodID hashmap_put = env->GetMethodID(
          hashmap_class, "put",
          "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
      if (OB_FAIL(check_jni_exception_(env))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("failed to get the HashMap methods.", K(ret));
      } else {
        common::ObSqlString message;
        message.append("Initialize a scanner with parameters:");

        for (const auto &it : scanner_params_) {
          jstring key = env->NewStringUTF(it.first.ptr());
          jstring value = env->NewStringUTF(it.second.ptr());
          // log but skip encoded object
          if (OB_HASH_EXIST == skipped_log_params_.exist_refactored(it.first)) {
            // DO NOTING
          } else {
            message.append(it.first);
            message.append("->");
            message.append(it.second);
            message.append(", ");
          }

          env->CallObjectMethod(hashmap_object, hashmap_put, key, value);
          env->DeleteLocalRef(key);
          env->DeleteLocalRef(value);
        }
        env->DeleteLocalRef(hashmap_class);
        LOG_TRACE("Initialized with parameters", K(ret), K(message));
        if (is_schema_scanner_) {
          jni_scanner_obj_ =
              env->NewObject(jni_scanner_cls_, scanner_constructor, hashmap_object);
        } else {
          jni_scanner_obj_ =
              env->NewObject(jni_scanner_cls_, scanner_constructor, batch_size_,
                             hashmap_object);
        }
        env->DeleteLocalRef(hashmap_object);
        if (OB_FAIL(check_jni_exception_(env))) {
          LOG_WARN("failed to initialize a scanner instance.", K(ret));
        } else if (nullptr == jni_scanner_obj_) {
          ret = OB_JNI_ERROR;
          LOG_WARN("jni scanner obj is null", K(ret));
        }
      }
    }
  }
  return ret;
}

int JniScanner::do_init(common::hash::ObHashMap<ObString, ObString> &params) {
  int ret = OB_SUCCESS;
  if (inited_) {
    LOG_INFO("jni scanner is already inited, skip to re-init", K(ret));
  } else if (OB_FAIL(detect_java_runtime())) {
    LOG_WARN("failed to detect java runtime", K(ret));
  } else if (params.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to use empty params for initializing jni scanner", K(ret));
  } else if (OB_FAIL(
                 scanner_params_.create(MAX_PARAMS_SIZE, "SCANNER_PARAMS"))) {
    LOG_WARN("failed to create scanner params map", K(ret));
  } else if (false == scanner_params_.created()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scanner params map is not created", K(ret));
  } else {
    common::hash::ObHashMap<ObString, ObString>::iterator params_iter;
    for (params_iter = params.begin();
         OB_SUCC(ret) && params_iter != params.end(); ++params_iter) {
      LOG_TRACE("scanner params", K(ret), K(params_iter->first), K(params_iter->second));
      // init scanner params
      if (OB_FAIL(scanner_params_.set_refactored(params_iter->first,
                                                 params_iter->second))) {
        LOG_WARN("failed to set scanner params", K(ret));
      }
    }
    if (is_debug()) {
      if (OB_FAIL(scanner_params_.set_refactored(
              ObString::make_string("debug_mode"),
              ObString::make_string("true")))) {
        LOG_WARN("failed to add debug flag to scanner params", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(skipped_log_params_.create(MAX_PARAMS_SET_SIZE))) {
      LOG_WARN("failed to create scanner skipped log params set", K(ret));
    } else if (false == skipped_log_params_.created()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("skipped log params set is not created", K(ret));
    } else {
      if (OB_FAIL(skipped_log_params_.set_refactored(
              ObString::make_string("access_id")))) {
        LOG_WARN("failed to add access id to set", K(ret));
      } else if (OB_FAIL(skipped_log_params_.set_refactored(
                     ObString::make_string("access_key")))) {
        LOG_WARN("failed to add access key to set", K(ret));
      } else {
        /* do nothing */
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(skipped_required_params_.create(MAX_PARAMS_SET_SIZE))) {
      LOG_WARN("failed to create scanner skipped required params set", K(ret));
    } else if (false == skipped_required_params_.created()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("skipped lrequiredog params set is not created", K(ret));
    } else {
      if (OB_FAIL(skipped_required_params_.set_refactored(
              ObString::make_string("access_id")))) {
        LOG_WARN("failed to add access id to required set", K(ret));
      } else if (OB_FAIL(skipped_required_params_.set_refactored(
                     ObString::make_string("access_key")))) {
        LOG_WARN("failed to add access key to required set", K(ret));
      } else if (OB_FAIL(skipped_required_params_.set_refactored(
                     ObString::make_string("region")))) {
        LOG_WARN("failed to add region to required set", K(ret));
      } else if (OB_FAIL(skipped_required_params_.set_refactored(
                     ObString::make_string("project")))) {
        LOG_WARN("failed to add project to required set", K(ret));
      } else if (OB_FAIL(skipped_required_params_.set_refactored(
                     ObString::make_string("table_name")))) {
        LOG_WARN("failed to add project to required set", K(ret));
      } else if (OB_FAIL(skipped_required_params_.set_refactored(
                     ObString::make_string("odps_url")))) {
        LOG_WARN("failed to add odps_url to required set", K(ret));
      } else if (OB_FAIL(skipped_required_params_.set_refactored(
                     ObString::make_string("tunnel_url")))) {
        LOG_WARN("failed to add tunnel_url to required set", K(ret));
      } else if (OB_FAIL(skipped_required_params_.set_refactored(
                     ObString::make_string("public_access")))) {
        LOG_WARN("failed to add public_access to required set", K(ret));
      } else if (OB_FAIL(skipped_required_params_.set_refactored(
                     ObString::make_string("debug_mode")))) {
        LOG_WARN("failed to add debug_mode to required set", K(ret));
      } else if (OB_FAIL(skipped_required_params_.set_refactored(
                     ObString::make_string("use_epoch_offset")))) {
        LOG_WARN("failed to add use_epoch_offset to required set", K(ret));
      } else {
        // do nothing
      }
    }
  }

  if (OB_SUCC(ret) && !inited_) {
    inited_ = true;
  }

  return ret;
}

int JniScanner::do_open() {
  int ret = OB_SUCCESS;
  JNIEnv *env = nullptr;
  if (is_opened_) {
    LOG_INFO("jni scanner is already opened, skip to re-open", K(ret));
  } else {
    if (OB_FAIL(get_jni_env(env))) {
      LOG_WARN("failed to get jni env", K(ret));
    } else if (nullptr == env) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get a null jni env", K(ret));
    }
  }

  if (OB_SUCC(ret) && !is_opened_ &&
      env->EnsureLocalCapacity(scanner_params_.size() * 2 + 6) < 0) {
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to ensure the local capacity", K(ret));
    }
  }

  if (OB_SUCC(ret) && !is_opened_) {
    if (OB_FAIL(init_jni_table_scanner_(env))) {
      LOG_WARN("failed init table scanner", K(ret));
    } else if (is_schema_scanner_) {
      // If is schema scanner, then skip executing the init methods
      LOG_INFO("skip to init more methods for schema scanner", K(ret));
    } else if (OB_FAIL(init_jni_method_(env))) {
      LOG_WARN("failed to init jni method", K(ret));
    } else if (OB_ISNULL(jni_scanner_obj_)) {
      ret = OB_JNI_ERROR;
      LOG_WARN("scanner obj is null", K(ret));
    } else {
      env->CallVoidMethod(jni_scanner_obj_, jni_scanner_open_);
      if (OB_FAIL(check_jni_exception_(env))) {
        LOG_WARN("failed to open off-heap table scanner", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !is_opened_) {
    is_opened_ = true;
  }
  return ret;
}

int JniScanner::do_close() {
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    LOG_WARN("jni scanner is not inited, skip to close", K(ret));
  } else if (!is_opened()) {
    LOG_WARN("jni_scanner is not opened, but inited", K(ret));
    scanner_params_.reuse();
    skipped_log_params_.reuse();
    skipped_required_params_.reuse();
    inited_ = false;
  } else {
    JNIEnv *env = nullptr;
    if (OB_FAIL(get_jni_env(env))) {
      LOG_WARN("failed to get jni env", K(ret));
    } else if (nullptr == env) {
      ret = OB_JNI_ENV_ERROR;
      LOG_WARN("unexpected null jni env", K(ret));
    } else { /* do nothing */
    }

    // NOTE!!! schema scanner should not clear this part
    if (OB_SUCC(ret) && nullptr != jni_scanner_obj_ && !is_schema_scanner_) {
      if (nullptr != jni_scanner_release_table_) {
        env->CallVoidMethod(jni_scanner_obj_, jni_scanner_release_table_);
      }
      if (nullptr != jni_scanner_close_) {
        env->CallVoidMethod(jni_scanner_obj_, jni_scanner_close_);
      }
    }
    if (OB_SUCC(ret) && nullptr != jni_scanner_obj_) {
      env->DeleteLocalRef(jni_scanner_obj_);
      jni_scanner_obj_ = nullptr;
    }
    if (OB_SUCC(ret) && nullptr != jni_scanner_cls_) {
      env->DeleteLocalRef(jni_scanner_cls_);
      jni_scanner_cls_ = nullptr;
    }
    if (OB_SUCC(ret)) {
      scanner_params_.reuse();
      skipped_log_params_.reuse();
      skipped_required_params_.reuse();
      inited_ = false;
      is_opened_ = false;
      is_schema_scanner_ = false;
    }
    if (OB_SUCC(ret) && check_jni_exception_(env)) {
      LOG_WARN("close with jni execption", K(ret));
    }
    LOG_TRACE("end of scanner close", K(ret));
  }
  return ret;
}

int JniScanner::do_get_next(int64_t *read_rows, bool *eof) {
  // Call com.oceanbase.jni.connector.ConnectorScanner#getNextOffHeapChunk
  // return the address of meta information
  int ret = OB_SUCCESS;
  JNIEnv *env = nullptr;
  if (OB_FAIL(get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (nullptr == env) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("failed to init jni env", K(ret));
  } else if (!is_opened_) {
    ret = OB_JNI_ERROR;
    LOG_WARN("get next should be after opened", K(ret));
  } else if (OB_ISNULL(jni_scanner_obj_)) {
    ret = OB_JNI_ERROR;
    LOG_WARN("scanner obj is null", K(ret));
  } else if (OB_ISNULL(jni_scanner_get_next_batch_)) {
    ret = OB_JNI_ERROR;
    LOG_WARN("get_next_batch method is null", K(ret));
  }
  long meta_address = 0;
  if (OB_SUCC(ret)) {
    meta_address =
        env->CallLongMethod(jni_scanner_obj_, jni_scanner_get_next_batch_);
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to get next batch data", K(ret));
    }
  }

  long num_rows = 0;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (meta_address == 0) {
    // Address == 0 when there's no data in scanner
    *read_rows = 0;
    *eof = true;
  } else {
    // Which meta address will mapping by
    // com.oceanbase.jni.connector.OffHeapTable#getMetaNativeAddress
    table_meta_.set_meta(meta_address);
    num_rows = table_meta_.next_meta_as_long();
    if (num_rows == 0) {
      *read_rows = 0;
      *eof = true;
    } else {
      *read_rows = num_rows;
      *eof = false;
    }
  }
  return ret;
}

int JniScanner::release_column(int32_t column_index) {
  int ret = OB_SUCCESS;
  JNIEnv *env = nullptr;
  if (OB_FAIL(get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (nullptr == env) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to init jni env", K(ret));
  } else if (OB_ISNULL(jni_scanner_obj_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scanner object is null", K(ret));
  } else if (OB_ISNULL(jni_scanner_release_column_)) {
    ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
    LOG_WARN("failed to find method to release column", K(ret));
  } else {
    // Column is not released when fill_column_ failed. It will be released
    // when releasing table.
    env->CallVoidMethod(jni_scanner_obj_, jni_scanner_release_column_, column_index);
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("check jni with exception", K(ret));
    }
  }
  return ret;
}

int JniScanner::release_table(const int64_t num_rows) {
  int ret = OB_SUCCESS;
  JNIEnv *env = nullptr;
  if (OB_FAIL(get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (nullptr == env) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to init jni env", K(ret));
  } else if (OB_ISNULL(jni_scanner_obj_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scanner object is null", K(ret));
  } else if (OB_ISNULL(jni_scanner_release_table_)) {
    ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
    LOG_WARN("failed to find method to release table", K(ret));
  } else {
    env->CallVoidMethod(jni_scanner_obj_, jni_scanner_release_table_);
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("check jni with exception", K(ret));
    } else {
      LOG_TRACE("release table with num rows", K(ret), K(num_rows));
    }
  }
  return ret;
}

int JniScanner::add_extra_optional_params(
    common::hash::ObHashMap<ObString, ObString> &extra_params) {
  int ret = OB_SUCCESS;
  if (extra_params.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty extra params for initializing jni scanner", K(ret));
  } else if (!scanner_params_.created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scanner params should be initialized before", K(ret));
  } else {
    common::hash::ObHashMap<ObString, ObString>::iterator params_iter;
    for (params_iter = extra_params.begin();
         OB_SUCC(ret) && params_iter != extra_params.end(); params_iter++) {

      if (OB_HASH_EXIST ==
          skipped_required_params_.exist_refactored(params_iter->first)) {
        // do noting
      } else if (OB_FAIL(scanner_params_.set_refactored(
                     params_iter->first, params_iter->second, 1))) {
        // init scanner params
        LOG_WARN("failed to add extra scanner params", K(ret),
                 K(params_iter->first), K(params_iter->second));
      } else { /* do nothing */}
    }
  }
  return ret;
}

// --------------- public method for odps ---------------
int JniScanner::get_odps_partition_specs(
    ObIAllocator &allocator, ObSEArray<ObString, 8> &partition_specs) {
  int ret = OB_SUCCESS;
  partition_specs.reset();
  JNIEnv *env = nullptr;
  if (OB_FAIL(get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (nullptr == env) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("failed to init jni env", K(ret));
  } else if (OB_ISNULL(jni_scanner_cls_)) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to get jni scanner class", K(ret));
  } else if (OB_ISNULL(jni_scanner_obj_)) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to get jni scanner obj", K(ret));
  } else {
    /* do nothing */
  }

  // Get the list class and needed methods
  jmethodID size_mid;
  jmethodID get_mid;
  if (OB_SUCC(ret)) {
    jclass jclazz = env->FindClass("java/util/List");
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to find `List` class", K(ret));
    } else if (nullptr == jclazz) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("List class could not be found", K(ret));
    } else {
      size_mid = env->GetMethodID(jclazz, "size", "()I");
      if (OB_FAIL(check_jni_exception_(env))) {
        LOG_WARN("failed to find `size` method in `List` class", K(ret));
      } else if (nullptr == size_mid) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("size() of List could not be found", K(ret));
      } else { /* do nothing */ }

      if (OB_SUCC(ret)) {
        get_mid = env->GetMethodID(jclazz, "get", "(I)Ljava/lang/Object;");
        if (OB_FAIL(check_jni_exception_(env))) {
          LOG_WARN("failed to find `get` method in `List` class", K(ret));
        } else if (nullptr == get_mid) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("get() of List could not be found", K(ret));
        } else { /* do nothing */
        }
      }
      env->DeleteLocalRef(jclazz);
    }
  }

  // Get the partition specs
  if (OB_SUCC(ret)) {
    jmethodID mid = env->GetMethodID(jni_scanner_cls_, "getPartitionSpecs",
                                     "()Ljava/util/List;");
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("find method with exception", K(ret));
    } else if (nullptr == mid) {
      ret = OB_JNI_ERROR;
      LOG_WARN("faild to get the column names method", K(ret));
    } else {
      jobject partition_specs_list =
          env->CallObjectMethod(jni_scanner_obj_, mid);
      if (OB_FAIL(check_jni_exception_(env))) {
        LOG_WARN("failed to get partition specs list", K(ret));
      } else if (nullptr == partition_specs_list) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get a null partition specs", K(ret));
      } else {
        jint size = env->CallIntMethod(partition_specs_list, size_mid);
        for (int i = 0; OB_SUCC(ret) && i < size; ++i) {
          jstring jstr =
              (jstring)env->CallObjectMethod(partition_specs_list, get_mid, i);
          const char *str = env->GetStringUTFChars(jstr, NULL);
          if (nullptr == str) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("failed to get partition spec by index", K(ret), K(i));
          } else {
            ObString temp_str;
            if (OB_FAIL(ob_write_string(allocator, str, temp_str))) {
              LOG_WARN("failed to write string", K(ret), K(str));
            } else {
              partition_specs.push_back(temp_str);
            }
          }
          // Note: release source
          env->ReleaseStringUTFChars(jstr, str);
        }
        // Delete reference at last
        env->DeleteLocalRef(partition_specs_list);
      }
    }
  }

  return ret;
}

int JniScanner::get_odps_partition_row_count(
    ObIAllocator &allocator, const ObString &partition_spec, int64_t &row_count) {
  int ret = OB_SUCCESS;
  JNIEnv *env = nullptr;
  if (OB_FAIL(get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (nullptr == env) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("failed to init jni env", K(ret));
  } else if (OB_ISNULL(jni_scanner_cls_)) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to get jni scanner class", K(ret));
  } else if (OB_ISNULL(jni_scanner_obj_)) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to get jni scanner obj", K(ret));
  } else {
    /* do nothing */
  }

  // Get the partition specs
  if (OB_FAIL(ret)) {
    /* do nothing */
  } else {
    jmethodID mid = env->GetMethodID(jni_scanner_cls_, "getPartitionRowCount",
                                     "(Ljava/lang/String;)J");
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("find method with exception", K(ret));
    } else if (nullptr == mid) {
      ret = OB_JNI_ERROR;
      LOG_WARN("faild to get the partition row count method", K(ret));
    } else {
      ObString temp_str;
      jstring j_partition_spec;
      // Note: should transfer ObString to c_style
      if (OB_ISNULL(partition_spec) || 0 == partition_spec.length()) {
        // Means current table is with empty
        const char *non_partition = "__NaN__";
        j_partition_spec = env->NewStringUTF(non_partition);
      } else if (OB_FAIL(ob_write_string(allocator, partition_spec, temp_str, true))) {
        LOG_WARN("failed to transfer partition_spec to be c_style", K(ret));
      } else {
        j_partition_spec = env->NewStringUTF(temp_str.ptr());
      }
      if (OB_FAIL(ret)) {
        /* do nothing */
      } else if (nullptr == j_partition_spec) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get partition spec in jstring", K(ret), K(partition_spec));
      } else {
        jlong result = env->CallLongMethod(
            jni_scanner_obj_, mid, j_partition_spec);
        if (OB_FAIL(check_jni_exception_(env))) {
          LOG_WARN("failed to get partition row count", K(ret));
        } else {
          row_count = static_cast<int64_t>(result);
        }
        // Note: release resource
        env->DeleteLocalRef(j_partition_spec);
      }
    }
  }
  return ret;
}

int JniScanner::get_odps_mirror_columns(ObIAllocator &allocator,
                                        ObSEArray<ObString, 8> &mirror_colums) {
  int ret = OB_SUCCESS;
  mirror_colums.reset();
  JNIEnv *env = nullptr;
  if (OB_FAIL(get_jni_env(env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (nullptr == env) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("failed to init jni env", K(ret));
  } else if (!is_inited()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni env is not inited", K(ret));
  } else if (!is_opened()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni env is not opened", K(ret));
  } else if (OB_ISNULL(jni_scanner_cls_)) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to get jni scanner class", K(ret));
  } else {
    /* do nothing */
  }

  // Get the list class and needed methods
  jmethodID size_mid;
  jmethodID get_mid;
  if (OB_SUCC(ret)) {
    jclass jclazz = env->FindClass("java/util/List");
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("failed to find `List` class", K(ret));
    } else if (nullptr == jclazz) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("List class could not be found", K(ret));
    } else {
      size_mid = env->GetMethodID(jclazz, "size", "()I");
      if (OB_FAIL(check_jni_exception_(env))) {
        LOG_WARN("failed to find `size` method in `List` class", K(ret));
      } else if (nullptr == size_mid) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("size() of List could not be found", K(ret));
      } else { /* do nothing */ }

      if (OB_SUCC(ret)) {
        get_mid = env->GetMethodID(jclazz, "get", "(I)Ljava/lang/Object;");
        if (OB_FAIL(check_jni_exception_(env))) {
          LOG_WARN("failed to find `get` method in `List` class", K(ret));
        } else if (nullptr == get_mid) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("get() of List could not be found", K(ret));
        } else { /* do nothing */ }
      }
      env->DeleteLocalRef(jclazz);
    }
  }

  // Get the mirror columns
  if (OB_SUCC(ret)) {
    jmethodID mid = env->GetMethodID(jni_scanner_cls_, "getMirrorColumns",
                                     "()Ljava/util/List;");
    if (OB_FAIL(check_jni_exception_(env))) {
      LOG_WARN("find method with exception", K(ret));
    } else if (nullptr == mid) {
      ret = OB_JNI_ERROR;
      LOG_WARN("faild to get the column names method", K(ret));
    } else {
      jobject mirror_column_list = env->CallObjectMethod(jni_scanner_obj_, mid);
      if (OB_FAIL(check_jni_exception_(env))) {
        LOG_WARN("failed to get partition specs list", K(ret));
      } else if (nullptr == mirror_column_list) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get a null partition specs", K(ret));
      } else {
        jint size = env->CallIntMethod(mirror_column_list, size_mid);
        for (int i = 0; OB_SUCC(ret) && i < size; ++i) {
          jstring jstr =
              (jstring)env->CallObjectMethod(mirror_column_list, get_mid, i);
          const char *str = env->GetStringUTFChars(jstr, NULL);
          if (nullptr == str) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("failed to get partition spec by index", K(ret), K(i));
          } else {
            ObString temp_str;
            if (OB_FAIL(ob_write_string(allocator, str, temp_str))) {
              LOG_WARN("failed to write string", K(ret), K(str));
            } else {
              mirror_colums.push_back(temp_str);
            }
          }
          // Note: release source
          env->ReleaseStringUTFChars(jstr, str);
        }
        // Delete reference at last
        env->DeleteLocalRef(mirror_column_list);
      }
    }
  }
  return ret;
}

int JniScanner::add_extra_optional_part_spec(const ObString &partition_spec) {
  int ret = OB_SUCCESS;
  if (!scanner_params_.created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scanner params should be initialized before", K(ret));
  } else {
    ObString spec = partition_spec;
    if (OB_ISNULL(partition_spec)) {
      spec = ObString::make_string("");
      LOG_WARN("set empty string partition spec for jni scanner", K(ret));
    }

    // init scanner params
    if (OB_FAIL(
            scanner_params_.set_refactored("partition_spec", spec))) {
      LOG_WARN("failed to add parition spec params", K(ret), K(spec));
    }
  }
  return ret;
}

// --------------- create jni scanners ------------------
// TODO:add more jni scanner
// --------------- odps jni scanner ------------------
JNIScannerPtr create_odps_jni_scanner(const bool is_schema_scanner) {
  const char *scanner_factory_class =
      "com/oceanbase/odps/utils/OdpsTunnelConnectorFactory";
  ObString scanner_type;
  return std::shared_ptr<JniScanner>(
      new JniScanner(scanner_factory_class, scanner_type, is_schema_scanner));
}

} // namespace sql
} // namespace oceanbase