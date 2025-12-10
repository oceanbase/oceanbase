/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/string/ob_string.h"
#include "lib/jni_env/ob_java_helper.h"

namespace oceanbase {
namespace plugin {

class ObProperties;

/**
 * @note predefined:
 * - int ret;
 * - JNIEnv *jni_env;
 */
#define OBJNI_RUN(statement)                                                                        \
  do {                                                                                              \
    if (OB_SUCC(ret)) {                                                                             \
      statement;                                                                                    \
      ObJniExceptionResult exception_result;                                                        \
      ObJniTool::instance().check_exception(jni_env, ret, exception_result);                        \
      if (exception_result.exception_occured()) {                                                   \
        LOG_WARN("fail to run jni", "statement", #statement, K(ret), K(exception_result));          \
      } else {                                                                                      \
        LOG_TRACE("success to run jni", "statement", #statement);                                   \
      }                                                                                             \
    }                                                                                               \
  } while (0)

class ObJavaGlobalRef final
{
public:
  ObJavaGlobalRef() = default;
  ObJavaGlobalRef(jobject handle) = delete;
  ~ObJavaGlobalRef();

  ObJavaGlobalRef(const ObJavaGlobalRef &) = delete;
  ObJavaGlobalRef &operator=(const ObJavaGlobalRef &) = delete;

  ObJavaGlobalRef(ObJavaGlobalRef &&other) noexcept {
    handle_ = other.handle_;
    other.handle_ = nullptr;
  }

  ObJavaGlobalRef &operator=(ObJavaGlobalRef &&other) noexcept {
    if (&other != this) {
      clear();
      this->handle_ = other.handle_;
      other.handle_ = nullptr;
    }

    return *this;
  }

  /// @param local_ref a local ref, not global
  int from_local_ref(jobject local_ref, JNIEnv *env);

  jobject handle() const { return handle_; }

  /// a helper function
  jclass class_handle() const { return static_cast<jclass>(handle_); }

  int clear(JNIEnv *jni_env = nullptr);

private:
  jobject handle_ = nullptr;
};

/**
 * Help to print java exceptions.
 */
class ObJniExceptionPrinter final
{
public:
  ObJniExceptionPrinter() = default;
  ~ObJniExceptionPrinter();

  int init(JNIEnv *jni_env);
  void destroy(JNIEnv *jni_env = nullptr);
  int throwable_to_string(JNIEnv *jni_env, jthrowable throwable, bool log_stack,
                          char buf[], int64_t buf_len, int64_t &pos);

private:
  ObJavaGlobalRef    jni_utils_class_;
  jmethodID          get_exception_message_method_ = nullptr;
};

/**
 * Temp record an exception.
 */
class ObJniExceptionResult final
{
public:
  ObJniExceptionResult() = default;
  ~ObJniExceptionResult();

  ObJniExceptionResult(ObJniExceptionResult&) = delete;
  ObJniExceptionResult &operator=(ObJniExceptionResult&) = delete;

  void init_members(JNIEnv *env, jthrowable exception, ObJniExceptionPrinter &exception_printer);
  void set_exception_name(const char *exception_name) { exception_name_ = exception_name; }
  bool exception_occured() const { return exception_ != nullptr; }

  int64_t to_string(char buf[], int64_t buf_len) const;

private:
  JNIEnv *   jni_env_            = nullptr;
  jthrowable exception_          = nullptr;
  const char * exception_name_   = nullptr;
  ObJniExceptionPrinter *exception_printer_ = nullptr;
};

/**
 * A helper class.
 * @details Help for checking exception now.
 * Maybe can help to hold and create some common objects such as String.
 */
class ObJniTool final
{
public:
  ObJniTool();
  ~ObJniTool();

  static ObJniTool &instance();
  static int init_global_instance();
  static void destroy_global_instance();
  /**
    * check if the environment of external table plugin is ready
    * @details check if the configuration of java is right and if the external plugin jar package is in the right path.
   */
  static int is_env_ready(bool &is_ready);

  int init();
  void destroy(JNIEnv *jni_env = nullptr);

  int get_jni_env(JNIEnv *&jni_env);
  JavaVM *get_jvm() const { return jvm_; }

  void check_exception(JNIEnv *env, int &ret, ObJniExceptionResult &result);

  int set_java_log_label();
  int clear_java_log_label();

  int create_java_map_from_properties(JNIEnv *jni_env, const ObProperties &properties, jobject &java_map_ret);
  int create_java_list_from_array_string(JNIEnv *jni_env, const ObIArray<ObString> &oblist, jobject &java_list_ret);

  int create_java_map(JNIEnv *jni_env, const ObString values[], int64_t values_num, jobject &java_map_ret);

  /**
   * construct a java string from C string.
   * return an empty Java string if c_str is null.
   */
  int create_java_string(JNIEnv *jni_env, const char *c_str, jstring &java_str_ret);
  int create_java_string(JNIEnv *jni_env, const ObString &str, jstring &java_str_ret);

  /**
   * from java List<Integer> to oceanbase ObArray<int64_t>
   */
  int int64_list_from_java(JNIEnv *jni_env, jobject java_list, ObIArray<int64_t> &oblist);
  int string_list_from_java(JNIEnv *jni_env, jobject java_list, ObIAllocator &allocator, ObIArray<ObString> &oblist);
  /**
   * Copy string from java to ObString with C style.
   */
  int string_from_java(JNIEnv *jni_env, jstring jstr, ObIAllocator &allocator, ObString &obstr);

  jclass hash_map_class() const { return (jclass)hash_map_class_.handle(); }
  jclass array_list_class() const { return (jclass)array_list_class_.handle(); }
  jclass jni_utils_class() const { return (jclass)jni_utils_class_.handle(); }

  jmethodID hash_map_constructor_method() const { return hash_map_constructor_method_; }
  jmethodID hash_map_put_method() const { return hash_map_put_method_; }
  jmethodID array_list_constructor_method() const { return array_list_constructor_method_; }
  jmethodID array_list_add_method() const { return array_list_add_last_method_; }
  jmethodID jni_utils_parse_sql_filter_from_arrow_method() const { return jni_utils_parse_sql_filter_from_arrow_method_; }
  jmethodID jni_utils_parse_question_mark_values_method() const { return jni_utils_parse_question_mark_values_method_; }
  jmethodID jni_utils_export_arrow_stream_method() const { return jni_utils_export_arrow_stream_method_; }
  jmethodID jni_utils_import_record_batch_method() const { return jni_utils_import_record_batch_method_; }

private:
  /**
   * @brief init java env.
   * @details try best to make sure we can get JNIEnv.
   * We can get JNIEnv by ObJniConnector::get_jni_env, if failed, we can get
   * it by `getJNIEnv`(from HDFS library), or else we create Java VM.
   */
  int init_jni();

  // bootstrap
  int init_exceptions(JNIEnv *jni_env);

  // not bootstrap methods
  int init_global_class_references(JNIEnv *jni_env);
  int find_class(JNIEnv *jni_env, const char *class_name, ObJavaGlobalRef &class_ref);

  int init_log_level(JNIEnv *jni_env);

  int get_jni_env_local(JNIEnv *&jni_env);
  int get_jni_env_hdfs(JNIEnv *&jni_env);
  int get_jni_env_connector(JNIEnv *&jni_env);
private:
  struct JniException
  {
    ObJavaGlobalRef    exception_class_ref;
    const char *       exception_name;
    int                retcode;
  };

private:
  struct JniEnvThreadVar final
  {
    JNIEnv *jni_env = nullptr;
    JavaVM *jvm     = nullptr;

    ~JniEnvThreadVar();
  };

private:
  // JNIEnv can be created by ObJniHelper/ObJniConnector, if so, we doesn't need
  // to hold the jvm_ pointer.
  // ObJniConnector depends on HDFS environment and other Java packages but we don't.
  static thread_local JniEnvThreadVar tls_env_;
  JavaVM *jvm_ = nullptr;

  bool inited_ = false;

  using JNIEnvGetter = int (ObJniTool::*)(JNIEnv *&);
  JNIEnvGetter jni_env_getter_ = nullptr;

  JniException *        exceptions_         = nullptr;
  int64_t               num_exceptions_     = 0;
  ObJniExceptionPrinter exception_printer_;
  ObMemAttr             mem_attr_;

  ObJavaGlobalRef   hash_map_class_;
  ObJavaGlobalRef   array_list_class_;
  ObJavaGlobalRef   list_class_;
  ObJavaGlobalRef   number_class_;
  ObJavaGlobalRef   jni_utils_class_;

  jmethodID hash_map_constructor_method_ = nullptr;
  jmethodID hash_map_put_method_ = nullptr;
  jmethodID array_list_constructor_method_ = nullptr;
  jmethodID array_list_add_last_method_ = nullptr;
  jmethodID list_size_method_ = nullptr;
  jmethodID list_get_method_ = nullptr;
  jmethodID number_int_value_method_ = nullptr;
  jmethodID jni_utils_parse_sql_filter_from_arrow_method_ = nullptr;
  jmethodID jni_utils_parse_question_mark_values_method_ = nullptr;
  jmethodID jni_utils_export_arrow_stream_method_ = nullptr;
  jmethodID jni_utils_import_record_batch_method_ = nullptr;
  jmethodID jni_utils_set_log_label_method_ = nullptr;
  jmethodID jni_utils_clear_log_label_method_ = nullptr;
};

} // namespace plugin
} // namespace oceanbase
