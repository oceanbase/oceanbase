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

#define USING_LOG_PREFIX SHARE

#include <sys/types.h>
#include <dirent.h>

#include "plugin/external_table/ob_external_jni_utils.h"
#include "plugin/share/ob_properties.h"
#include "lib/string/ob_sql_string.h"
#include "lib/file/file_directory_utils.h"
#include "share/config/ob_server_config.h"
#include "lib/jni_env/ob_java_env.h"
#include "lib/jni_env/ob_jni_connector.h"

namespace oceanbase {
using namespace sql;

namespace plugin {

/**
 * A helper macro to execute jni method when bootstraping jni tool
 * @todo move to cpp file
 * @details Execute the `statement` if no error in current context. The exception will be cleaned
 * if jni method throwed.
 * @note The log message wouldn't be printed if the macro executed in the logging context, such as
 * `to_string` routine.
 */
#define OBJNI_BOOTSTRAP_RUN(statement)                                                              \
  do {                                                                                              \
    if (OB_SUCC(ret)) {                                                                             \
      statement;                                                                                    \
      if (jni_env->ExceptionCheck()) {                                                              \
        jni_env->ExceptionDescribe();                                                               \
        jni_env->ExceptionClear();                                                                  \
        ret = OB_JNI_ERROR;                                                                         \
        LOG_WARN("failed to run jni", "statement", #statement);                                     \
      } else {                                                                                      \
        LOG_TRACE("succcess to run jni", "statement", #statement);                                  \
      }                                                                                             \
    }                                                                                               \
  } while (0)

int __find_file(const ObString &directory, const ObString &filename,
                const int max_recursive_level, const int recursive_level,
                ObSqlString &filepath)
{
  int ret = OB_SUCCESS;
  DIR *dir = nullptr;
  int sys_ret = 0;
  ObString directory_str;
  ObArenaAllocator arena_allocator(ob_plugin_mem_attr());
  ObArray<ObString> sub_directories;
  if (OB_FAIL(ob_write_string(arena_allocator, directory, directory_str, true/*c_style*/))) {
    LOG_WARN("failed to create c style string", K(directory), K(ret));
  } else if (OB_ISNULL(dir = opendir(directory_str.ptr()))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("failed to open directory", K(directory_str), K(strerror(errno)));
  } else {
    struct dirent *entry = nullptr;
    while (OB_SUCC(ret) && OB_NOT_NULL(entry = readdir(dir))) {
      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      } else if (entry->d_type == DT_DIR) {
        if (recursive_level >= max_recursive_level) {
          LOG_WARN("recursive_level reach max_recursive_level, ignore the sub directory",
                   K(recursive_level), K(max_recursive_level), K(entry->d_name));
        } else {
          ObString sub_directory;
          ObString strs[] = {directory, ObString("/"), ObString(entry->d_name)};
          if (OB_FAIL(ob_concat_string(arena_allocator, sub_directory, 3, strs, true/*c_style*/))) {
            LOG_WARN("failed to create sub directory", K(ret), K(directory), KCSTRING(entry->d_name));
          } else if (OB_FAIL(sub_directories.push_back(sub_directory))) {
            LOG_WARN("failed to append subdirectory", K(sub_directory), K(ret));
          }
        }
      } else if (0 == filename.compare(ObString(entry->d_name))) {
        LOG_TRACE("got the file", K(directory));
        if (OB_FAIL(filepath.assign(directory))) {
          LOG_WARN("failed to assign string to sql string", K(ret), K(directory));
        } else if (OB_FAIL(filepath.append_fmt("/%s", entry->d_name))) {
          LOG_WARN("failed to append filename to filepath", K(filepath), KCSTRING(entry->d_name), K(ret));
        }
      }
    }
    if (OB_NOT_NULL(dir)) {
      closedir(dir);
      dir = nullptr;
    }
  }

  if (OB_SUCC(ret) && filepath.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && filepath.empty() && i < sub_directories.count(); i++) {
      if (OB_FAIL(__find_file(sub_directories.at(i), filename, max_recursive_level, recursive_level + 1, filepath))) {
        LOG_WARN("find file from sub directory failed", K(ret), K(sub_directories[i]), K(filename));
      }
    }
  }
  return ret;
}

/**
 * Find file under `directory`
 * @param[in] directory The directory to find.
 * @param[in] filename The file to find
 * @param[in] max_recursive_level max recursive to prevent cyclical calling. Also the max sub directory level to find.
 * @param[in] recursive_level current recursive level.
 * @param[out] filepath The path of file to find.
 * @return ret
 *   - OB_SUCCESS no error occures.
 *   - OB_FILE_NOT_EXIST Not found.
 */
int find_file(const ObString &directory, const ObString &filename, const int max_recursive_level, ObSqlString &filepath)
{
  int ret = OB_SUCCESS;
  ret = __find_file(directory, filename, max_recursive_level, 1, filepath);
  if (OB_SUCC(ret) && filepath.empty()) {
    ret = OB_FILE_NOT_EXIST;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObJavaGlobalRef
ObJavaGlobalRef::~ObJavaGlobalRef()
{
  clear();
}

int ObJavaGlobalRef::from_local_ref(jobject local_ref, JNIEnv *jni_env)
{
  int ret = OB_SUCCESS;
  jobject global_ref = nullptr;
  if (OB_NOT_NULL(handle_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("global ref is not null", K(ret), KP(handle_));
  } else if (OB_ISNULL(local_ref) || OB_ISNULL(jni_env)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(local_ref), KP(jni_env));
  } else if (OB_ISNULL(global_ref = jni_env->NewGlobalRef(local_ref))) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to create global ref", K(ret));
  } else {
    handle_ = global_ref;
  }

  return ret;
}

int ObJavaGlobalRef::clear(JNIEnv *jni_env /*=nullptr*/)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(handle_)) {
    if (OB_ISNULL(jni_env) && OB_FAIL(ObJniTool::instance().get_jni_env(jni_env))) {
      LOG_WARN("failed to get jni env, cannot release java global handle", K(ret));
    } else {
      jni_env->DeleteGlobalRef(handle_);
      handle_ = nullptr;
    }
  }
  LOG_DEBUG("clear the global ref", K(ret), KP(handle_));
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObJniExceptionResult
void ObJniExceptionResult::init_members(JNIEnv *jni_env, jthrowable exception, ObJniExceptionPrinter &exception_printer)
{
  jni_env_ = jni_env;
  exception_ = exception;
  exception_printer_ = &exception_printer;
}

ObJniExceptionResult::~ObJniExceptionResult()
{
  if (OB_NOT_NULL(jni_env_) && OB_NOT_NULL(exception_)) {
    jni_env_->DeleteLocalRef(exception_);
    exception_ = nullptr;
    jni_env_->ExceptionClear();
  }
}

int64_t ObJniExceptionResult::to_string(char buf[], int64_t buf_len) const
{
  int ret = 0;
  int64_t pos = 0;
  bool log_stack = IS_LOG_ENABLED(OB_LOG_LEVEL_INFO);
  if (OB_ISNULL(exception_)) {
    databuff_printf(buf, buf_len, pos, "no exception");
  } else if (OB_UNLIKELY(OB_ISNULL(exception_printer_) || OB_ISNULL(jni_env_))) {
    // do nothing
    databuff_printf(buf, buf_len, pos, "(exception printer=%p is null or jni env=%p is null)",
                    exception_printer_, jni_env_);
  } else if (FALSE_IT(databuff_printf(buf, buf_len, pos, "exception name=%s;", exception_name_))) {
  } else if (OB_FAIL(exception_printer_->throwable_to_string(jni_env_, exception_, log_stack, buf, buf_len, pos))) {
    // do nothing
  }

  return pos;
}

////////////////////////////////////////////////////////////////////////////////
// ObJniExceptionPrinter
ObJniExceptionPrinter::~ObJniExceptionPrinter()
{
  destroy();
}

int ObJniExceptionPrinter::init(JNIEnv *jni_env)
{
  int ret = OB_SUCCESS;
  jclass jni_utils_class = nullptr;
  LOCAL_REF_GUARD_ENV(jni_utils_class, jni_env);

  if (get_exception_message_method_ != nullptr) {
    ret = OB_INIT_TWICE;
  }
  OBJNI_BOOTSTRAP_RUN(jni_utils_class = jni_env->FindClass("com/oceanbase/external/internal/JniUtils"));

  if (OB_SUCC(ret) && OB_FAIL(jni_utils_class_.from_local_ref(jni_utils_class, jni_env))) {
    LOG_WARN("failed to create global ref", K(ret));
  }
  OBJNI_BOOTSTRAP_RUN(get_exception_message_method_ = jni_env->GetStaticMethodID(jni_utils_class,
                                                                       "getExceptionMessage",
                                                                       "(Ljava/lang/Throwable;Z)Ljava/lang/String;"));
  return ret;
}

void ObJniExceptionPrinter::destroy(JNIEnv *jni_env/* = nullptr */)
{
  jni_utils_class_.clear(jni_env);
}
int ObJniExceptionPrinter::throwable_to_string(JNIEnv *jni_env, jthrowable throwable, bool log_stack,
                                               char buf[], int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  jstring message = nullptr;
  jsize message_size = 0;
  const char *message_buffer = nullptr;
  LOCAL_REF_GUARD_ENV(message, jni_env);

  if (OB_ISNULL(jni_env) || OB_ISNULL(throwable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(jni_env), KP(throwable));
    databuff_printf(buf, buf_len, pos, "(no exception. env=%p, throwable=%p)", jni_env, throwable);
  } else if (OB_ISNULL(jni_utils_class_.handle()) || OB_ISNULL(get_exception_message_method_)) {
    ret = OB_INVALID_ARGUMENT;
    databuff_printf(buf, buf_len, pos, "(printer not init. class=%p, method=%p)",
                    jni_utils_class_.handle(), get_exception_message_method_);
  }
  OBJNI_BOOTSTRAP_RUN(message = (jstring)jni_env->CallStaticObjectMethod(
      (jclass)jni_utils_class_.handle(), get_exception_message_method_, throwable, log_stack));
  if (OB_FAIL(ret)) {
    databuff_printf(buf, buf_len, pos, "(get exception when call JniUtils::getExceptionMessage)");
  }

  OBJNI_BOOTSTRAP_RUN(message_size = jni_env->GetStringUTFLength(message));
  OBJNI_BOOTSTRAP_RUN(message_buffer = jni_env->GetStringUTFChars(message, nullptr/*isCopy*/));
  if (OB_FAIL(ret) || 0 == message_size) {
    databuff_printf(buf, buf_len, pos, "(failed to get exception message: GetStringUTFChars return null)");
  } else {
    databuff_printf(buf, buf_len, pos, "%.*s", message_size, message_buffer);
    OBJNI_BOOTSTRAP_RUN(jni_env->ReleaseStringUTFChars(message, message_buffer));
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// JvmFunctions
struct JvmFunctions
{
  using JNI_CreateJavaVM_f = jint (*)(JavaVM **pvm, void **penv, void *args);
  using JNI_GetCreatedJavaVMs_f = jint (*)(JavaVM **, jsize, jsize *);

  JNI_CreateJavaVM_f      JNI_CreateJavaVM;
  JNI_GetCreatedJavaVMs_f JNI_GetCreatedJavaVMs;

  int init();

  static int find_libjvm(ObSqlString &libjvm_path);
};

int JvmFunctions::init()
{
  int ret = OB_SUCCESS;
  ObSqlString libjvm_path;
  void *jvm_handle = nullptr;
  if (OB_FAIL(find_libjvm(libjvm_path))) {
    LOG_WARN("failed to find libjvm", K(ret));
  } else if (OB_ISNULL(jvm_handle = dlopen(libjvm_path.ptr(), RTLD_NOW | RTLD_GLOBAL))) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to open jvm lib", K(libjvm_path), KCSTRING(dlerror()));
  } else if (OB_ISNULL(JNI_CreateJavaVM = (JNI_CreateJavaVM_f)dlsym(jvm_handle, "JNI_CreateJavaVM"))) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to get jvm function", KCSTRING("JNI_CreateJavaVM"), KCSTRING(dlerror()));
  } else if (OB_ISNULL(JNI_GetCreatedJavaVMs = (JNI_GetCreatedJavaVMs_f)dlsym(jvm_handle, "JNI_GetCreatedJavaVMs"))) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to get jvm function", KCSTRING("JNI_GetCreatedJavaVMs"), KCSTRING(dlerror()));
  }
  LOG_TRACE("init jvm functions done", K(ret));
  return ret;
}

int JvmFunctions::find_libjvm(ObSqlString &libjvm_path)
{
  int ret = OB_SUCCESS;
  ObString java_home_config(GCONF.ob_java_home.get_value_string());
  if (!java_home_config.empty()) {
    LOG_DEBUG("java home from oceanbase config", K(java_home_config));
  } else if (FALSE_IT(java_home_config = ObString(getenv("JAVA_HOME")))) {
    LOG_DEBUG("java home from env", K(java_home_config));
  }

  const ObString jvm_lib_name("libjvm.so");
  if (java_home_config.empty()) {
    ret = OB_JNI_JAVA_HOME_NOT_FOUND_ERROR;
    LOG_WARN("java env not configured: failed to get from GCONF.ob_java_home and env JAVA_HOME");
  } else if (OB_FAIL(find_file(java_home_config, jvm_lib_name, 5/*max_recursive_level*/, libjvm_path))) {
    LOG_WARN("failed to find libjvm_path", K(ret));
  } else if (-1 == access(libjvm_path.ptr(), F_OK)) {
    ret = OB_JNI_ERROR;
    LOG_WARN("libjvm is not exists in ob_java_home", K(java_home_config), K(libjvm_path), K(strerror(errno)));
  }

  LOG_INFO("got libjvm_path", K(ret), K(libjvm_path));
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObJniTool
static ObJniTool *jni_tool_global_instance = nullptr;
thread_local ObJniTool::JniEnvThreadVar ObJniTool::tls_env_;

ObJniTool::ObJniTool()
{}

ObJniTool::~ObJniTool()
{
  destroy();
}

int ObJniTool::init()
{
  int ret = OB_SUCCESS;
  mem_attr_.label_ = OB_PLUGIN_MEMORY_LABEL;
  if (MTL_ID() == OB_INVALID_TENANT_ID) {
    mem_attr_.tenant_id_ = OB_SERVER_TENANT_ID;
  } else {
    mem_attr_.tenant_id_ = MTL_ID();
  }

  JNIEnv *jni_env = nullptr;

  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(init_jni())) {
    LOG_WARN("failed to create jvm", K(ret));
  } else if (OB_FAIL(get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_FAIL(init_exceptions(jni_env))) {
    LOG_WARN("failed to init exceptions", K(ret));
  } else if (OB_FAIL(init_global_class_references(jni_env))) {
    LOG_WARN("failed to init jni tool global classes", K(ret));
  } else if (OB_FAIL(init_log_level(jni_env))) {
    LOG_WARN("failed to init java log level", K(ret));
  }

  if (OB_FAIL(ret)) {
    destroy(jni_env);
  }
  LOG_INFO("init jni tool done", K(ret));
  inited_ = true;
  return ret;
}

void ObJniTool::destroy(JNIEnv *jni_env/*=nullptr*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jni_env) && OB_FAIL(get_jni_env(jni_env))) {
    LOG_WARN("fail to get jni env", K(ret));
  }
  if (OB_NOT_NULL(jni_env) && OB_NOT_NULL(exceptions_)) {
    for (int64_t i = 0; i < num_exceptions_; i++) {
      JniException &exception_struct = exceptions_[i];
      if (exception_struct.retcode != 0) {
        exception_struct.exception_class_ref.clear(jni_env);
      }
    }
    ob_free(exceptions_);
    exceptions_ = nullptr;
    num_exceptions_ = 0;
  }
  if (OB_NOT_NULL(jni_env)) {
    exception_printer_.destroy(jni_env);
  }
  hash_map_class_.clear();
  array_list_class_.clear();
  inited_ = false;
}

void ObJniTool::check_exception(JNIEnv *env, int &retcode, ObJniExceptionResult &result)
{
  int ret = OB_SUCCESS;
  jthrowable exception = nullptr; // local ref will be released by ExceptionResult

  if (OB_ISNULL(env)) {
    retcode = OB_INVALID_ARGUMENT;
    LOG_WARN("jni env shouldn't be null");
  } else if (!env->ExceptionCheck()) {
    retcode = OB_SUCCESS;
  } else if (OB_ISNULL(exception = env->ExceptionOccurred())) {
    retcode = OB_ERROR;
    LOG_WARN("ExceptionCheck return true but can't get Exception");
  } else if (FALSE_IT(result.init_members(env, exception, exception_printer_))) {
  } else if (OB_ISNULL(exceptions_)) {
    retcode = OB_ERROR;
    LOG_WARN("got exception but can detect the exception class because the exceptions_ is null");
  } else {
    retcode = OB_ERROR;
    env->ExceptionDescribe();
    env->ExceptionClear();
    for (int64_t i = 0; i < num_exceptions_ && exceptions_[i].retcode != 0; i++) {
      JniException &exception_struct = exceptions_[i];
      if (OB_ISNULL(exception_struct.exception_class_ref.handle())) {
      } else if (env->IsInstanceOf(exception, (jclass)exception_struct.exception_class_ref.handle())) {
        retcode = exception_struct.retcode;
        result.set_exception_name(exception_struct.exception_name);
        LOG_DEBUG("get exception", K(i), K(retcode), KCSTRING(exception_struct.exception_name));
        break;
      }
    }
  }
}

/**
 * @brief get jni_env
 * @details We'll try to get JNIEnv from ObJniConnector/ObJniHelper first, and then
 * we'll create it by ourself if failed.
 * ObJniConnector depends on HDFS environment and other Java jar packages that
 * we don't.
 * And if we just want to use external table plugins, we only need to deploy
 * external table jar packages, which can simplify the deployment.
 */
int ObJniTool::get_jni_env(JNIEnv *&jni_env)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(tls_env_.jni_env)) {
    jni_env = tls_env_.jni_env;
    LOG_DEBUG("get jni env from thread cache", KP(jni_env), KP(tls_env_.jvm));
  } else if (OB_ISNULL(jni_env_getter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init. jni_env_getter_ is null");
  } else if (OB_FAIL((this->*jni_env_getter_)(jni_env))) {
    LOG_WARN("failed to get jni_env");
  }

  if (OB_NOT_NULL(jni_env)) {
  } else {
    LOG_WARN("failed to get jni env", K(ret));
  }
  return ret;
}

int ObJniTool::get_jni_env_local(JNIEnv *&jni_env)
{
  int ret = OB_SUCCESS;
  jint jni_ret = JNI_OK;
  if (OB_NOT_NULL(jvm_)) {
    if (JNI_OK != (jni_ret = jvm_->GetEnv((void **)&jni_env, JNI_VERSION_1_8))) {
      if (jni_ret == JNI_EDETACHED) {
        LOG_DEBUG("java env detached, reattach");
        jni_ret = jvm_->AttachCurrentThread((void **)&jni_env, nullptr);
      }

      if (jni_ret != JNI_OK || OB_ISNULL(jni_env)) {
        ret = OB_JNI_ERROR;
        LOG_WARN("failed to get jni env from jvm", K(ret), K(jni_ret), KP(jni_env));
      }
    }
    if (JNI_OK == jni_env && OB_NOT_NULL(jni_env))  {
      tls_env_.jni_env = jni_env;
      tls_env_.jvm = jvm_;
      LOG_DEBUG("get jnienv from jvm", KP(jni_env), KP(jvm_));
    }
  }
  return ret;
}
int ObJniTool::get_jni_env_hdfs(JNIEnv *&jni_env)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(getJNIEnv) && OB_NOT_NULL(jni_env = getJNIEnv())) {
    tls_env_.jni_env = jni_env;
    LOG_DEBUG("get jnienv from hdfs", KP(jni_env));
  } else {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to get jnienv from hdfs", K(ret));
  }
  return ret;
}
int ObJniTool::get_jni_env_connector(JNIEnv *&jni_env)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObJniConnector::get_jni_env(jni_env))) {
    LOG_WARN("failed to get jnienv from ObJniConnector", K(ret));
  } else {
    tls_env_.jni_env = jni_env;
    LOG_DEBUG("get jni env from jni connector", KP(jni_env));
  }
  return ret;
}

struct JavaVMOptionWrapper
{
  JavaVMOption option;

  JavaVMOptionWrapper()
  {
    option.optionString = nullptr;
  }
  JavaVMOptionWrapper(const JavaVMOption &vm_option) : option(vm_option)
  {}

  TO_STRING_KV(KCSTRING(option.optionString));
};

int expand_directory(const ObString &single_classpath, ObSqlString &expanded_classpath)
{
  int ret = OB_SUCCESS;
  DIR *dir = nullptr;
  int sys_ret = 0;
  ObSqlString single_classpath_str;
  if (OB_FAIL(single_classpath_str.assign(single_classpath))) {
    LOG_WARN("failed to assign classpath to sql string", K(single_classpath), K(ret));
  } else if (OB_ISNULL(dir = opendir(single_classpath_str.ptr()))) {
    // ignore error
    LOG_WARN("failed to open directory", K(single_classpath), K(strerror(errno)));
  } else {
    struct dirent *entry = nullptr;
    while (OB_SUCC(ret) && OB_NOT_NULL(entry = readdir(dir))) {
      ObString filename(entry->d_name);
      if (filename.suffix_match_ci(".jar")) {
        LOG_DEBUG("got a jar package", K(filename));
        const char *append_format = "%.*s/%s";
        if (!expanded_classpath.empty()) {
          append_format = ":%.*s/%s";
        }
        if (OB_FAIL(expanded_classpath.append_fmt(append_format,
                                                 single_classpath.length(), single_classpath.ptr(),
                                                 entry->d_name))) {
          LOG_WARN("failed to append classpath to expanded_classpath",
                   K(ret), K(single_classpath), KCSTRING(entry->d_name), K(expanded_classpath));
        }
      }
    }
    if (errno != 0) {
      LOG_WARN("[ignored] failed to expand directory", KCSTRING(strerror(errno)), K(single_classpath));
    }
  }

  if (OB_NOT_NULL(dir) && 0 != (sys_ret = closedir(dir))) {
    LOG_WARN("[ignored] failed to closedir", KCSTRING(strerror(errno)));
  }
  return ret;
}
int expand_classpath(ObArenaAllocator &allocator, const ObString &src_classpath, ObString &expanded_classpath)
{
  int ret = OB_SUCCESS;
  ObString classpath = src_classpath;
  ObSqlString expanded_sql_classpath;
  const char delimiter = ':';
  while (OB_SUCC(ret) && !classpath.empty()) {
    ObString item_classpath = classpath.split_on(delimiter);
    LOG_DEBUG("got a class path", K(item_classpath));
    if (item_classpath.empty() && nullptr == classpath.find(delimiter)) {
      item_classpath = classpath;
      classpath.reset();
    }
    if (!item_classpath.empty()) {
      bool is_directory;
      if (item_classpath.suffix_match("/*")) {
        item_classpath = ObString(item_classpath.length() - 2, item_classpath.ptr());
      }
      ObSqlString path_tmp;
      if (OB_FAIL(path_tmp.assign(item_classpath))) {
        LOG_WARN("failed to copy string to sql string", K(ret), K(item_classpath));
      } else if (OB_FAIL(FileDirectoryUtils::is_directory(path_tmp.ptr(), is_directory))) {
        LOG_WARN("failed to detect directory and ignore the error", K(item_classpath), K(ret));
        ret = OB_SUCCESS;
      } else if (is_directory) {
        if (OB_FAIL(expand_directory(item_classpath, expanded_sql_classpath))) {
          LOG_WARN("failed to expand classpath directory", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        // append this file or directory
        const char *append_format = "%.*s";
        if (!expanded_sql_classpath.empty()) {
          append_format = ":%.*s";
        }
        if (OB_FAIL(expanded_sql_classpath.append_fmt(append_format, item_classpath.length(), item_classpath.ptr()))) {
          LOG_WARN("failed to append classpath to expaned_sql_classpath",
                   K(ret), K(item_classpath), K(expanded_sql_classpath));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!expanded_sql_classpath.empty()) {
    if (OB_FAIL(ob_write_string(allocator, expanded_sql_classpath.string(), expanded_classpath))) {
      LOG_WARN("failed to copy sql string to string", K(expanded_sql_classpath), K(ret));
    }
  }
  return ret;
}
int init_jvm_options(ObArenaAllocator &allocator, ObIArray<JavaVMOptionWrapper> &options)
{
  int ret = OB_SUCCESS;
  const char *classpath_option_name = "-Djava.class.path=";
  ObString connector_path_config;
  ObString java_opts = GCONF.ob_java_opts.get_value_string();
  LOG_INFO("got connector path and java option from config", K(connector_path_config), K(java_opts));
  const char opt_delimiter = ' ';

  if (OB_FAIL(expand_classpath(allocator, GCONF.ob_java_connector_path.get_value_string(), connector_path_config))) {
    LOG_WARN("failed to expand classpath", K(ret), K(GCONF.ob_java_connector_path.get_value_string()));
  }

  while (OB_SUCC(ret) && !java_opts.empty()) {
    ObString option = java_opts.split_on(opt_delimiter);
    LOG_INFO("got a java option", K(option));
    if (option.empty() && nullptr == java_opts.find(opt_delimiter)) {
      option = java_opts;
      java_opts.reset();
    }
    if (!option.empty()) {
      ObString option_copy;
      JavaVMOptionWrapper vm_option;
      if (!connector_path_config.empty() && option.prefix_match(classpath_option_name)) {
        int64_t len = option.length() + connector_path_config.length() + 1;
        char *buf = (char *)allocator.alloc(len);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory to store class.path option", K(ret), K(len));
        } else {
          MEMCPY(buf, option.ptr(), option.length());
          buf[option.length()] = ':';
          MEMCPY(buf + option.length() + 1, connector_path_config.ptr(), connector_path_config.length());
          option.assign_ptr(buf, len);

          // make sure connector path wouldn't be set twice
          connector_path_config.reset();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ob_write_string(allocator, option, option_copy, true/*c_style*/))) {
        LOG_WARN("failed to copy vm option", K(ret));
      } else if (FALSE_IT(vm_option.option.optionString = option_copy.ptr())) {
      } else if (OB_FAIL(options.push_back(vm_option))) {
        LOG_WARN("failed to push vm option into options", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!connector_path_config.empty()) {
    int64_t len = STRLEN(classpath_option_name) + connector_path_config.length() + 1;
    char *buf = (char *)allocator.alloc(len);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for class.path", K(ret), K(len));
    } else {
      MEMCPY(buf, classpath_option_name, STRLEN(classpath_option_name));
      MEMCPY(buf + STRLEN(classpath_option_name), connector_path_config.ptr(), connector_path_config.length());
      buf[len - 1] = 0;

      JavaVMOptionWrapper vm_option;
      vm_option.option.optionString = buf;
      LOG_DEBUG("class path option", KCSTRING(buf));
      if (OB_FAIL(options.push_back(vm_option))) {
        LOG_WARN("failed to push vm option into options", K(ret));
      }
    }
  }

  LOG_INFO("construct java options done", K(ret), K(options));
  return ret;
}

int ObJniTool::init_jni()
{
  int ret = OB_SUCCESS;
  JNIEnv *jni_env = nullptr;
  int num_jvms = 0;
  jint jni_ret = JNI_OK;
  JvmFunctions jvm_functions;
  ObArenaAllocator allocator;
  const int64_t tenant_id = MTL_ID() == OB_INVALID_TENANT_ID ? OB_SERVER_TENANT_ID : MTL_ID();
  allocator.set_tenant_id(tenant_id);
  if (!ObJavaEnv::getInstance().is_env_inited() && OB_FAIL(ObJavaEnv::getInstance().setup_java_env())) {
    LOG_WARN("failed to setup java env", K(ret));
  } else {
    ret = ObJavaEnv::getInstance().setup_java_env_classpath_and_ldlib_path();
    // 优先尝试从ObJniConnector获取jni env
    // 如果失败，尝试从hdfs获取jni env
    if (OB_SUCC(ret)) {
      if (OB_SUCC(ObJniConnector::get_jni_env(jni_env))) {
        // hdfs正确, hdfs classpath正确, ob jar正确
        jni_env_getter_ = &ObJniTool::get_jni_env_connector;
        LOG_INFO("get jni env from ObJniConnector successfully");
      } else {
        // hdfs正确, ob jar不正确
        if (OB_NOT_NULL(getJNIEnv)) {
          jni_env_getter_ = &ObJniTool::get_jni_env_hdfs;
          LOG_INFO("we can get jni env by hdfs");
        } else {
          // libhdfs.so完全没有的时候
        }
      }
    }

    // 如果hdfs获取失败，尝试创建java vm
    if (OB_ISNULL(jni_env_getter_)) {
      // 补救机会
      JavaVM *jvm = nullptr;
      ret = OB_SUCCESS;
      if (OB_FAIL(jvm_functions.init())) {
          LOG_WARN("failed to init jvm functions", K(ret));
      } else if (JNI_OK != (jni_ret = jvm_functions.JNI_GetCreatedJavaVMs(&jvm, 1, &num_jvms))) {
        ret = OB_JNI_ERROR;
        LOG_WARN("failed to get created java vm", K(jni_ret), K(ret));
      } else if (0 == num_jvms) {
        ObArray<JavaVMOptionWrapper> options;
        if (OB_FAIL(init_jvm_options(allocator, options))) {
          LOG_WARN("failed to init jvm options to create jvm", K(ret));
        } else {
          JavaVMInitArgs jvm_args;
          jvm_args.version = JNI_VERSION_1_8;
          jvm_args.nOptions = options.count();
          jvm_args.options = (JavaVMOption *)(&options.at(0));
          jvm_args.ignoreUnrecognized = JNI_TRUE;
          jni_ret = jvm_functions.JNI_CreateJavaVM(&jvm, (void **)&jni_env, &jvm_args);
          if (jni_ret != JNI_OK) {
            ret = OB_JNI_ERROR;
            LOG_WARN("failed to create java vm", K(jni_ret), K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        jni_env_getter_ = &ObJniTool::get_jni_env_local; // 补救机会成功，设置jni env getter
      }
      jvm_ = jvm;
    } else {
      jvm_ = nullptr;
      LOG_INFO("get jni env from external source successfully , jvm is not needed", K(ret));
    }
  }

  LOG_TRACE("create jvm done", KP(jvm_));
  return ret;
}

ObJniTool &ObJniTool::instance()
{
  return *jni_tool_global_instance;
}

int ObJniTool::init_global_instance()
{
  int ret = OB_SUCCESS;
  ObJniTool *instance = nullptr;
  ObMemAttr mem_attr(OB_SERVER_TENANT_ID, OB_PLUGIN_MEMORY_LABEL);
  if (OB_NOT_NULL(jni_tool_global_instance)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(instance = OB_NEW(ObJniTool, mem_attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(sizeof(ObJniTool)));
  } else if (OB_FAIL(instance->init())) {
    LOG_WARN("init jni tool failed", K(ret));
    OB_DELETE(ObJniTool, mem_attr, instance);
  } else {
    jni_tool_global_instance = instance;
  }
  LOG_INFO("init java jni tool done", K(ret));
  return ret;
}

void ObJniTool::destroy_global_instance()
{
  if (OB_NOT_NULL(jni_tool_global_instance)) {
    OB_DELETE(ObJniTool, ObMemAttr(OB_SERVER_TENANT_ID, OB_PLUGIN_MEMORY_LABEL), jni_tool_global_instance);
    jni_tool_global_instance = nullptr;
  }
}

int ObJniTool::set_java_log_label()
{
  int ret = OB_SUCCESS;
  jstring java_log_string = nullptr;
  JNIEnv *jni_env = nullptr;
  LOCAL_REF_GUARD_ENV(java_log_string, jni_env);
  if (OB_ISNULL(jni_utils_set_log_label_method_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jni tool not init", KP(jni_utils_set_log_label_method_));
  } else if (OB_FAIL(get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  } else if (OB_FAIL(create_java_string(jni_env, ObCurTraceId::get_trace_id_str(), java_log_string))) {
    LOG_WARN("failed to create java string for trace info", K(ret));
  }

  OBJNI_RUN(jni_env->CallStaticObjectMethod(
      (jclass)jni_utils_class_.handle(), jni_utils_set_log_label_method_, java_log_string));

  return ret;
}

int ObJniTool::clear_java_log_label()
{
  int ret = OB_SUCCESS;
  JNIEnv *jni_env = nullptr;
  if (OB_ISNULL(jni_utils_clear_log_label_method_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(jni_utils_clear_log_label_method_));
  } else if (OB_FAIL(get_jni_env(jni_env))) {
    LOG_WARN("failed to get jni env", K(ret));
  }

  OBJNI_RUN(jni_env->CallStaticObjectMethod((jclass)jni_utils_class_.handle(), jni_utils_clear_log_label_method_));
  return ret;
}

struct JavaHashMapPutter
{
  JavaHashMapPutter(JNIEnv *jni_env, jobject hash_map, ObJniTool &jni_tool) :
      jni_env_(jni_env), hash_map_(hash_map), jni_tool_(jni_tool)
  {}

  int operator() (const ObString &key, const ObString &value) const
  {
    int ret = OB_SUCCESS;
    JNIEnv *jni_env    = jni_env_;
    jstring java_key   = nullptr;
    jstring java_value = nullptr;
    jobject put_return = nullptr;
    LOCAL_REF_GUARD_ENV(java_key, jni_env);
    LOCAL_REF_GUARD_ENV(java_value, jni_env);
    LOCAL_REF_GUARD_ENV(put_return, jni_env);

    if (OB_FAIL(jni_tool_.create_java_string(jni_env, key, java_key)) ||
        OB_FAIL(jni_tool_.create_java_string(jni_env, value, java_value))) {
      LOG_WARN("failed to create java string", K(ret), K(key.length()), K(value.length()));
    }

    OBJNI_RUN(put_return = jni_env->CallObjectMethod(hash_map_, jni_tool_.hash_map_put_method(), java_key, java_value));
    LOG_DEBUG("put map entry into map", K(ret), K(key), K(value));
    return ret;
  }

private:
  JNIEnv *   jni_env_  = nullptr;
  jobject    hash_map_ = nullptr;
  ObJniTool &jni_tool_;
};

int ObJniTool::create_java_map_from_properties(JNIEnv *jni_env, const ObProperties &properties, jobject &java_map_ret)
{
  int ret = OB_SUCCESS;
  java_map_ret = nullptr;
  jobject hash_map = nullptr;
  LOCAL_REF_GUARD_ENV(hash_map, jni_env);
  if (OB_ISNULL(jni_env)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("jni env is null");
  }

  OBJNI_RUN(hash_map = jni_env->NewObject((jclass)hash_map_class_.handle(), hash_map_constructor_method_));
  if (OB_SUCC(ret) && OB_ISNULL(hash_map)) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to create java hash map", K(ret));
  }

  JavaHashMapPutter hash_map_putter(jni_env, hash_map, *this);

  if (OB_FAIL(properties.foreach(hash_map_putter))) {
    LOG_WARN("failed to construct java hash map", K(ret));
  } else {
    java_map_ret = hash_map;
    hash_map = nullptr; // prevent hash_map to be released
  }
  return ret;
}

int ObJniTool::create_java_map(JNIEnv *jni_env, const ObString values[], int64_t values_num, jobject &java_map_ret)
{
  int ret = OB_SUCCESS;
  java_map_ret = nullptr;
  jobject hash_map = nullptr;
  LOCAL_REF_GUARD_ENV(hash_map, jni_env);
  if (OB_ISNULL(jni_env)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("jni env is null");
  } else if (values_num % 2 != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("string value num is not an event number", K(values_num));
  }

  OBJNI_RUN(hash_map = jni_env->NewObject((jclass)hash_map_class_.handle(), hash_map_constructor_method_));
  if (OB_SUCC(ret) && OB_ISNULL(hash_map)) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to create java hash map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < values_num / 2; i++) {
    const ObString &key = values[i * 2];
    const ObString &value = values[i * 2 + 1];
    jstring java_key = nullptr;
    jstring java_value = nullptr;
    jobject put_return = nullptr;
    LOCAL_REF_GUARD_ENV(java_key, jni_env);
    LOCAL_REF_GUARD_ENV(java_value, jni_env);
    LOCAL_REF_GUARD_ENV(put_return, jni_env);

    if (OB_FAIL(create_java_string(jni_env, key, java_key)) ||
        OB_FAIL(create_java_string(jni_env, value, java_value))) {
      LOG_WARN("failed to create java string", K(ret), K(key.length()), K(value.length()));
    }

    OBJNI_RUN(put_return = jni_env->CallObjectMethod(hash_map, hash_map_put_method_, java_key, java_value));
    LOG_DEBUG("put map entry into map", K(ret), K(key), K(value));
  }

  if (OB_SUCC(ret)) {
    java_map_ret = hash_map;
    hash_map = nullptr; // prevent hash_map to be released
  }
  return ret;
}
int ObJniTool::create_java_list_from_array_string(JNIEnv *jni_env, const ObIArray<ObString> &oblist, jobject &java_list_ret)
{
  int ret = OB_SUCCESS;
  jobject java_list = nullptr;
  LOCAL_REF_GUARD_ENV(java_list, jni_env);

  if (OB_ISNULL(jni_env)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(jni_env));
  }
  OBJNI_RUN(java_list = jni_env->NewObject((jclass)array_list_class_.handle(), array_list_constructor_method_));
  for (int64_t i = 0; OB_SUCC(ret) && i < oblist.count(); i++) {
    const ObString &str = oblist.at(i);
    jstring java_str = nullptr;
    LOCAL_REF_GUARD_ENV(java_str, jni_env);
    if (OB_FAIL(create_java_string(jni_env, str, java_str))) {
      LOG_WARN("failed to create java string", K(ret), K(str));
    }
    OBJNI_RUN(jni_env->CallBooleanMethod(java_list, array_list_add_last_method_, java_str));
    // always be true except exception occures.
  }
  if (OB_SUCC(ret)) {
    java_list_ret = java_list;
    java_list = nullptr;
  }
  LOG_TRACE("create java list done", K(ret), K(oblist.count()));
  return ret;
}

int ObJniTool::create_java_string(JNIEnv *jni_env, const ObString &str, jstring &java_str_ret)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp_str;
  tmp_str.set_attr(ObMemAttr(MTL_ID(), OB_PLUGIN_MEMORY_LABEL));
  if (OB_FAIL(tmp_str.assign(str))) {
    LOG_WARN("failed to assign string to sql string", K(str), K(ret));
  } else {
    ret = create_java_string(jni_env, tmp_str.ptr(), java_str_ret);
  }
  return ret;
}

int ObJniTool::create_java_string(JNIEnv *jni_env, const char *c_str, jstring &java_str_ret)
{
  int ret = OB_SUCCESS;

  jstring java_str = nullptr;
  LOCAL_REF_GUARD_ENV(java_str, jni_env);
  if (OB_ISNULL(jni_env)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(jni_env));
  } else if (OB_ISNULL(c_str)) {
    c_str = "";
  }
  OBJNI_RUN(java_str = jni_env->NewStringUTF(c_str));
  CK (OB_NOT_NULL(java_str));
  if (OB_SUCC(ret)) {
    LOG_DEBUG("create java string", KP(jni_env), KCSTRING(c_str), KP(java_str), K(*(intptr_t*)java_str));
    java_str_ret = java_str;
    java_str = nullptr;
  }
  return ret;
}

int ObJniTool::int64_list_from_java(JNIEnv *jni_env, jobject java_list, ObIArray<int64_t> &oblist)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jni_env) || OB_ISNULL(java_list)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(jni_env), KP(java_list));
  }
  int java_list_size = -1;
  OBJNI_RUN(java_list_size = jni_env->CallIntMethod(java_list, list_size_method_));
  for (int i = 0; OB_SUCC(ret) && i < java_list_size; i++) {
    jobject item = nullptr;
    LOCAL_REF_GUARD_ENV(item, jni_env);
    OBJNI_RUN(item = jni_env->CallObjectMethod(java_list, list_get_method_, i));
    if (OB_FAIL(ret) || OB_ISNULL(item)) {
      LOG_WARN("failed to get java list item", K(i), K(ret));
    }
    int int_value = -1;
    OBJNI_RUN(int_value = jni_env->CallIntMethod(item, number_int_value_method_));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(oblist.push_back(int_value))) {
      LOG_WARN("failed to push item into array", K(ret));
    }
  }
  return ret;
}

int ObJniTool::string_list_from_java(JNIEnv *jni_env,
                                     jobject java_list,
                                     ObIAllocator &allocator,
                                     ObIArray<ObString> &oblist)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jni_env) || OB_ISNULL(java_list)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(jni_env), KP(java_list));
  }
  int java_list_size = -1;
  OBJNI_RUN(java_list_size = jni_env->CallIntMethod(java_list, list_size_method_));
  for (int i = 0; OB_SUCC(ret) && i < java_list_size; i++) {
    jobject item = nullptr;
    LOCAL_REF_GUARD_ENV(item, jni_env);
    OBJNI_RUN(item = jni_env->CallObjectMethod(java_list, list_get_method_, i));
    if (OB_FAIL(ret) || OB_ISNULL(item)) {
    }
    ObString obstr;
    if (OB_FAIL(string_from_java(jni_env, (jstring)item, allocator, obstr))) {
      LOG_WARN("failed to get string from java", K(ret));
    } else if (OB_FAIL(oblist.push_back(obstr))) {
      LOG_WARN("failed to push string to oblist", K(ret));
    }
  }
  return ret;
}

int ObJniTool::string_from_java(JNIEnv *jni_env, jstring jstr, ObIAllocator &allocator, ObString &obstr)
{
  int ret = OB_SUCCESS;
  jsize jstr_len = 0;
  const char *jstr_bytes = nullptr;
  if (OB_ISNULL(jni_env) || OB_ISNULL(jstr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(jni_env), KP(jstr));
  }
  OBJNI_RUN(jstr_len = jni_env->GetStringUTFLength(jstr));
  OBJNI_RUN(jstr_bytes = jni_env->GetStringUTFChars(jstr, nullptr/*isCopy*/));
  if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, ObString(jstr_len, jstr_bytes), obstr, true/*c_style*/))) {
    LOG_WARN("failed to copy string from java", K(ret));
  }
  if (OB_NOT_NULL(jstr_bytes)) {
    jni_env->ReleaseStringUTFChars(jstr, jstr_bytes);
  }
  return ret;
}
int ObJniTool::find_class(JNIEnv *jni_env, const char *class_name, ObJavaGlobalRef &class_ref)
{
  int ret = OB_SUCCESS;
  jclass java_class = nullptr;
  LOCAL_REF_GUARD_ENV(java_class, jni_env);
  if (OB_NOT_NULL(jni_tool_global_instance)) {
    OBJNI_RUN(java_class = jni_env->FindClass(class_name));
  } else {
    OBJNI_BOOTSTRAP_RUN(java_class = jni_env->FindClass(class_name));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(class_ref.from_local_ref(java_class, jni_env))) {
    LOG_WARN("failed to create global object ref of java class");
  }
  LOG_INFO("find java class done", K(ret), KCSTRING(class_name));
  return ret;
}

struct JavaMethodDesc
{
  jmethodID *       java_method;
  bool              is_static;
  ObJavaGlobalRef * java_class;
  const char *      method_name;
  const char *      method_sig;
};

int ObJniTool::init_global_class_references(JNIEnv *jni_env)
{
  int ret = OB_SUCCESS;
  std::pair<const char *, ObJavaGlobalRef *> classes_to_find[] = {
    {"java/util/HashMap",        &hash_map_class_},
    {"java/util/ArrayList",      &array_list_class_},
    {"java/util/List",           &list_class_},
    {"java/lang/Number",         &number_class_},
    {"com/oceanbase/external/internal/JniUtils", &jni_utils_class_}
  };

  for (int64_t i = 0; OB_SUCC(ret) && i < sizeof(classes_to_find) / sizeof(classes_to_find[0]); i++) {
    if (OB_FAIL(find_class(jni_env, classes_to_find[i].first, *classes_to_find[i].second))) {
      LOG_WARN("failed to find java class", K(ret));
    }
  }

  struct JavaMethodDesc methods_to_find[] = {
    {&hash_map_constructor_method_, false, &hash_map_class_, "<init>", "()V"},
    {&hash_map_put_method_, false, &hash_map_class_, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;"},
    {&array_list_constructor_method_, false, &array_list_class_, "<init>", "()V"},
    {&array_list_add_last_method_, false, &array_list_class_, "add", "(Ljava/lang/Object;)Z"},
    {&list_size_method_, false, &list_class_, "size", "()I"},
    {&list_get_method_,  false, &list_class_, "get", "(I)Ljava/lang/Object;"},
    {&number_int_value_method_, false, &number_class_, "intValue", "()I"},
    {&jni_utils_parse_sql_filter_from_arrow_method_, true, &jni_utils_class_,
       "parseSqlFilterFromArrow", "(Lorg/apache/arrow/vector/VectorSchemaRoot;)Ljava/util/List;"},
    {&jni_utils_parse_question_mark_values_method_, true, &jni_utils_class_,
     "parseQuestionMarkValues", "(Lorg/apache/arrow/vector/VectorSchemaRoot;)Ljava/util/List;"},
    {&jni_utils_export_arrow_stream_method_, true, &jni_utils_class_,
     "exportArrowStream", "(Lorg/apache/arrow/vector/ipc/ArrowReader;J)V"},
    {&jni_utils_import_record_batch_method_, true, &jni_utils_class_,
     "importRecordBatch", "(JJ)Lorg/apache/arrow/vector/VectorSchemaRoot;"},
    {&jni_utils_set_log_label_method_, true, &jni_utils_class_,
     "setLogLabel", "(Ljava/lang/String;)V"},
    {&jni_utils_clear_log_label_method_, true, &jni_utils_class_,
     "clearLogLabel", "()V"}
  };
  for (int64_t i = 0; OB_SUCC(ret) && i < sizeof(methods_to_find) / sizeof(methods_to_find[0]); i++) {
    JavaMethodDesc &method = methods_to_find[i];
    if (!method.is_static) {
      OBJNI_BOOTSTRAP_RUN(*method.java_method =
          jni_env->GetMethodID((jclass)method.java_class->handle(), method.method_name, method.method_sig));
    } else {
      OBJNI_BOOTSTRAP_RUN(*method.java_method =
          jni_env->GetStaticMethodID((jclass)method.java_class->handle(), method.method_name, method.method_sig));
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to find java method", K(ret), K(i), KCSTRING(method.method_name), KCSTRING(method.method_sig));
    }
  }
  return ret;
}

int ObJniTool::init_exceptions(JNIEnv *jni_env)
{
  int ret = OB_SUCCESS;
  // TODO remove me: getMessage is enough
  std::pair<const char *, int> exception_class_retcode_pairs[] = {
    // {"java/lang/ArithmeticException", OB_ERR_UNEXPECTED},
    // {"java/lang/ArrayIndexOutOfBoundsException", OB_ARRAY_OUT_OF_RANGE},
    // {"java/lang/IllegalArgumentException", OB_INVALID_ARGUMENT},
    // {"java/lang/IndexOutOfBoundsException", OB_ARRAY_OUT_OF_RANGE},
    // {"java/lang/NegativeArraySizeException", OB_INVALID_ARGUMENT},
    // {"java/lang/NullPointerException", OB_ERR_UNEXPECTED},
    // {"java/lang/StringIndexOutOfBoundsException", OB_ARRAY_OUT_OF_RANGE},
    {"java/lang/ClassNotFoundException", OB_JNI_CLASS_NOT_FOUND_ERROR},
    //{"java/lang/NoSuchFieldException",   OB_ENTRY_NOT_EXIST},
    {"java/lang/NoSuchMethodException",  OB_JNI_METHOD_NOT_FOUND_ERROR},
    // {"java/io/FileNotFoundException", OB_FILE_NOT_EXIST},
    {"java/lang/OutOfMemoryError",       OB_ALLOCATE_MEMORY_FAILED},
    //{"java/lang/StackOverflowError",     OB_JNI_ERROR},
    //{"java/lang/ClassFormatError",       OB_JNI_ERROR},
    {"java/lang/NoClassDefFoundError",   OB_JNI_ERROR},
    //{"java/lang/UnsupportedClassVersionError", OB_JNI_ERROR},
    //{"java/lang/VerifyError", OB_JNI_ERROR},
    //{"java/lang/ExceptionInInitializerError", OB_JNI_ERROR},
    //{"java/lang/IncompatibleClassChangeError", OB_JNI_ERROR},
    //{"java/lang/UnsatisfiedLinkError", OB_JNI_ERROR},
    //{"java/lang/ClassFormatError", OB_JNI_ERROR},
    {"java/io/IOException",              OB_IO_ERROR},
    {"java/lang/Exception",              OB_JNI_ERROR}, // can be ignore
    {"java/lang/Throwable",              OB_JNI_ERROR}
  };

  num_exceptions_ = sizeof(exception_class_retcode_pairs) / sizeof(exception_class_retcode_pairs[0]);
  const size_t num_bytes = num_exceptions_ * sizeof(JniException);

  if (OB_ISNULL(exceptions_ = (JniException *)ob_malloc(num_bytes, mem_attr_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for exceptions", K(num_bytes), K(ret));
  } else if (FALSE_IT(MEMSET(exceptions_, 0, num_bytes))) {
  } else {
    for (size_t i = 0; OB_SUCC(ret) && i < num_exceptions_; i++) {
      jclass exception_class = nullptr;
      JniException &exception_struct = exceptions_[i];
      LOCAL_REF_GUARD_ENV(exception_class, jni_env);
      const char *class_name = exception_class_retcode_pairs[i].first;
      OBJNI_BOOTSTRAP_RUN(exception_class = jni_env->FindClass(class_name));
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to find class", K(ret), KCSTRING(class_name));
      } else if (FALSE_IT(exception_struct.exception_name = exception_class_retcode_pairs[i].first)) {
      } else if (FALSE_IT(exception_struct.retcode = exception_class_retcode_pairs[i].second)) {
      } else if (FALSE_IT(new (&exception_struct.exception_class_ref) ObJavaGlobalRef())) {
      } else if (OB_FAIL(exception_struct.exception_class_ref.from_local_ref(exception_class, jni_env))) {
        LOG_WARN("failed to create global ref", KCSTRING(class_name), K(ret));
      } else {
        LOG_DEBUG("find exception class", KCSTRING(class_name), K(i), K(exception_struct.retcode));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(exception_printer_.init(jni_env))) {
    LOG_WARN("failed to init exception printer");
  }
  return ret;
}

static const char *get_java_log_level_name(int level)
{
  const char *level_name = "DEBUG"; // this is the default java log level
  if (level <= OB_LOG_LEVEL_DBA_ERROR) {
    level_name = "ERROR";
  } else if (level <= OB_LOG_LEVEL_DBA_WARN) {
    level_name = "WARN";
  } else if (level < OB_LOG_LEVEL_TRACE) {
    level_name = "INFO";
  } else if (level == OB_LOG_LEVEL_TRACE) { // in java, DEBUG has high priority than TRACE
    level_name = "TRACE";
  } else if (level == OB_LOG_LEVEL_DEBUG) {
    level_name = "DEBUG";
  } else {
    level_name = "TRACE";
  }
  return level_name;
}

int ObJniTool::init_log_level(JNIEnv *jni_env)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jni_utils_class_.handle())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }

  jmethodID set_log_level_method = nullptr;
  const char *method_name = "setLogLevel";
  const char *method_sig = "(Ljava/lang/String;)V";
  OBJNI_RUN(set_log_level_method = jni_env->GetStaticMethodID(
      (jclass)jni_utils_class_.handle(), method_name, method_sig));
  CK (OB_NOT_NULL(set_log_level_method));

  const char *oblog_level = get_java_log_level_name(OB_LOGGER.get_level());
  jstring jlevel = nullptr;
  LOCAL_REF_GUARD_ENV(jlevel, jni_env);
  if (OB_SUCC(ret) && create_java_string(jni_env, oblog_level, jlevel)) {
    LOG_WARN("failed to create java log level", K(ret));
  }

  CK (OB_NOT_NULL(jlevel));

  OBJNI_RUN(jni_env->CallStaticVoidMethod((jclass)jni_utils_class_.handle(), set_log_level_method, jlevel));
  LOG_INFO("set java log level", K(ret), KCSTRING(oblog_level));
  return ret;
}

static bool is_external_plugin_jar(const ObString &path)
{
  bool exists = false;
  const char *jar_prefix = "oceanbase-external-plugin-";
  const char *jar_suffix = ".jar";
  if (!path.suffix_match_ci(jar_suffix)) {
  } else {
    // Extract the filename from the path (after last '/'), and check if it starts with jar_prefix and ends with jar_suffix
    const char *last_slash = path.reverse_find('/');
    ObString filename;
    if (OB_NOT_NULL(last_slash)) {
      filename.assign_ptr(const_cast<char *>(last_slash + 1), path.ptr() + path.length() - (last_slash + 1));
    } else {
      filename = path;
    }
    if (filename.prefix_match_ci(jar_prefix)) {
      exists = true;
    }
  }
  return exists;
}

int ObJniTool::is_env_ready(bool &is_ready)
{
  int ret = OB_SUCCESS;
  is_ready = false;
  if (!GCONF.ob_enable_java_env ||
      OB_ISNULL(GCONF.ob_java_opts) ||
      OB_ISNULL(GCONF.ob_java_connector_path) ||
      OB_ISNULL(GCONF.ob_java_home)) {
  } else {
    // check if the external plugin jar package is in the right path.
    ObArenaAllocator allocator(ob_plugin_mem_attr());
    ObString connector_path_config;
    if (OB_FAIL(expand_classpath(allocator, GCONF.ob_java_connector_path.get_value_string(), connector_path_config))) {
      LOG_WARN("failed to expand classpath", K(ret), K(GCONF.ob_java_connector_path.get_value_string()));
    } else {
      // Iterate through items in connector_path_config (':' separated), check for '.jar' suffix
      is_ready = false;
      const char delimiter = ':';
      ObString classpath = connector_path_config;
      while (!classpath.empty() && !is_ready) {
        ObString item = classpath.split_on(delimiter);
        if (item.empty()) {
          item = classpath;
          classpath.reset();
        }
        if (is_external_plugin_jar(item)) {
          is_ready = true;
        }
      }
      LOG_INFO("check if the external plugin jar package is in the right path", K(ret), K(is_ready), K(connector_path_config));
    }
  }
  LOG_INFO("check if java env is ready", K(ret), K(is_ready));
  return ret;
}
////////////////////////////////////////////////////////////////////////////////
ObJniTool::JniEnvThreadVar::~JniEnvThreadVar()
{
  // if JVM was created by JniHelper then we won't detach current thread.
  if (OB_NOT_NULL(jni_env) && OB_NOT_NULL(jvm)) {
    jint jni_ret = jvm->DetachCurrentThread();
    LOG_INFO("[JNI] detach current thread", K(jni_ret), KP(jni_env), KP(jvm));
    jni_env = nullptr;
    jvm = nullptr;
  }
}

} // namespace plugin
} // namespace oceanbase
