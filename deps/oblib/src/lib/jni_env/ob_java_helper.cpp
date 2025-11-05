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

#include <cstdlib>
#include <dirent.h>

#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "share/config/ob_server_config.h"

#include "lib/jni_env/ob_java_helper.h"
#include "ob_jar_version_def.h"
#include "share/ob_errno.h"

GETJNIENV getJNIEnv = NULL;
// hdfs detach current thread is a call back function
// but jdbc pulgs use this function to detach current thread
DETACHCURRENTTHREAD detachCurrentThread = NULL;

// hdfs functions
HdfsGetPathInfoFunc obHdfsGetPathInfo = NULL;
HdfsFreeFileInfoFunc obHdfsFreeFileInfo = NULL;
HdfsDeleteFunc obHdfsDelete = NULL;
HdfsGetLastExceptionRootCauseFunc obHdfsGetLastExceptionRootCause = NULL;
HdfsCreateDirectoryFunc obHdfsCreateDirectory = NULL;
HdfsListDirectoryFunc obHdfsListDirectory = NULL;
HdfsCloseFileFunc obHdfsCloseFile = NULL;
HdfsOpenFileFunc obHdfsOpenFile = NULL;
HdfsFileIsOpenForReadFunc obHdfsFileIsOpenForRead = NULL;
HdfsFileIsOpenForWriteFunc obHdfsFileIsOpenForWrite = NULL;
HdfsPreadFunc obHdfsPread = NULL;
HdfsWriteFunc obHdfsWrite = NULL;
HdfsFlushFunc obHdfsFlush = NULL;
HdfsNewBuilderFunc obHdfsNewBuilder = NULL;
HdfsBuilderSetNameNodeFunc obHdfsBuilderSetNameNode = NULL;
HdfsBuilderSetUserNameFunc obHdfsBuilderSetUserName = NULL;
HdfsBuilderSetForceNewInstanceFunc obHdfsBuilderSetForceNewInstance = NULL;
HdfsBuilderConnectFunc obHdfsBuilderConnect = NULL;
HdfsFreeBuilderFunc obHdfsFreeBuilder = NULL;
HdfsDisconnectFunc obHdfsDisconnect = NULL;

HdfsBuilderConfSetStrFunc obHdfsBuilderConfSetStr = NULL;
HdfsBuilderSetPrincipalFunc obHdfsBuilderSetPrincipal = NULL;
HdfsBuilderSetKerb5ConfFunc obHdfsBuilderSetKerb5Conf = NULL;
HdfsBuilderSetKeyTabFileFunc obHdfsBuilderSetKeyTabFile = NULL;
HdfsBuilderSetKerbTicketCachePathFunc obHdfsBuilderSetKerbTicketCachePath = NULL;

namespace oceanbase
{

namespace common
{

thread_local JNIEnv* JVMFunctionHelper::jni_env_ = nullptr;
constexpr const char* CLASS_NATIVE_METHOD_HELPER_NAME = "com/oceanbase/utils/NativeMethodHelper";

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
static JNINativeMethod java_native_methods[] = {
        {"memoryTrackerMalloc", "(J)J", (void*)&JavaNativeMethods::memory_malloc},
        {"memoryTrackerFree", "(J)V", (void*)&JavaNativeMethods::memory_free},
};
#pragma GCC diagnostic pop

// ------------------------- start of JVMFunctionHelper -------------------------
JVMFunctionHelper &JVMFunctionHelper::getInstance() {
  static JVMFunctionHelper helper;
  int ret = OB_SUCCESS;
  if (OB_LIKELY(helper.init_result_ == OB_SUCCESS)) {
    // do nothing
    // if we do not init success we will do symbol link again
  } else if (OB_UNLIKELY(OB_FAIL(helper.do_init_()))) {
    LOG_WARN("failed to init jni env", K(ret));
  } else {
    helper.init_result_ = ret;
  }
  return helper;
}

JVMFunctionHelper::JVMFunctionHelper():load_lib_lock_(common::ObLatchIds::JAVA_HELPER_LOCK), error_msg_(nullptr) {
    int ret = OB_SUCCESS;
    if (init_result_ != OB_NOT_INIT) {
      // do nothing
    } else if (OB_FAIL(do_init_())) {
      init_result_ = ret;
    } else {
      init_result_ = OB_SUCCESS;
    }
}

int JVMFunctionHelper::jni_find_class(const char *clazz, jclass* gen_clazz) {
  int ret = OB_SUCCESS;
  jclass cls = jni_env_->FindClass(clazz);
  if (nullptr == cls) {
    ret = OB_JNI_CLASS_NOT_FOUND_ERROR;
    LOG_WARN("failed to find class", K(ret), K(clazz));
    if (jni_env_->ExceptionCheck()) {
      jni_env_->ExceptionDescribe();
      jni_env_->ExceptionClear();
    }
  } else {
    *gen_clazz = (jclass)jni_env_->NewGlobalRef(cls);
    jni_env_->DeleteLocalRef(cls);
  }
  return ret;
}

int JVMFunctionHelper::get_env(JNIEnv *&env) {
  int ret = OB_SUCCESS;
  if (nullptr == jni_env_) {
    // Because of the jni_env_ is thread local variables, so should handled cross
    // thread scenes.
    if (OB_FAIL(init_jni_env())) {
      LOG_WARN("faild to get cross thread, and init env failed", K(ret));
    } else if (nullptr == jni_env_) {
      ret = OB_JNI_ENV_ERROR;
      LOG_WARN("failed to init jni env in get_env method", K(ret));
    } else {
      env = jni_env_;
    }
  } else {
    env = jni_env_;
  }
  return ret;
}


void *JVMFunctionHelper::ob_alloc_jni(void* ctxp, int64_t size)
{
  void *ptr = nullptr;
  ObJavaEnvContext *ctx = static_cast<ObJavaEnvContext *>(ctxp);
  if (NULL == ctx) {
    // do nothing;
  } else if (size != 0) {
    ObMemAttr attr;
    attr.label_ = "ob_alloc_java";
    attr.ctx_id_ = ObCtxIds::DEFAULT_CTX_ID;
    SET_IGNORE_MEM_VERSION(attr);
    {
      ptr = ob_malloc(size, attr);
    }
    if (NULL == ptr) {
      _OB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "ob_tc_malloc failed, size:%lu", size);
    } else {
      ctx->mem_bytes_jvm_ += size;
      ctx->jvm_alloc_times_ += 1;
    }
  }
  return ptr;
}

void *JVMFunctionHelper::ob_alloc_hdfs(void* ctxp, int64_t size)
{
  void *ptr = nullptr;
  ObHdfsEnvContext *ctx = static_cast<ObHdfsEnvContext *>(ctxp);
  if (NULL == ctx) {
    // do nothing
  } else if (size != 0) {
    ObMemAttr attr;
    attr.label_ = "ob_alloc_hdfs";
    attr.ctx_id_ = ObCtxIds::DEFAULT_CTX_ID;
    SET_IGNORE_MEM_VERSION(attr);
    {
      ptr = ob_malloc(size, attr);
    }
    if (NULL == ptr) {
      _OB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "ob_tc_malloc failed, size:%lu", size);
    } else {
      ctx->mem_bytes_hdfs_ += size;
      ctx->hdfs_alloc_times_ += 1;
    }
  }
  return ptr;
}

int JVMFunctionHelper::search_dir_file(const char *path, const char *file_name, bool &found)
{
  int ret = OB_SUCCESS;
  DIR *dirp = NULL;
  dirent *dp = NULL;
  found = false;
  if (OB_ISNULL(path) || OB_ISNULL(file_name)) {
    found = false;
  } else if (NULL == (dirp = opendir(path))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cann't open dir path", K(ObString(path)), K(file_name), K(ret));
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

int JVMFunctionHelper::get_lib_path(char *path, uint64_t length, const char* lib_name)
{
  int ret = OB_SUCCESS;
  const char *env_var_name = "LD_LIBRARY_PATH";
  const char *env_str = std::getenv(env_var_name);
  LOG_INFO("LD_LIBRARY_PATH: ", K(ObString(env_str)), KCSTRING(lib_name));
  bool found = false;
  ObSqlString lib_paths;
  if (OB_ISNULL(env_str)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cann't find lib in LD_LIBRARY_PATH", K(ret));
  } else {
    if (OB_FAIL(lib_paths.append_fmt("%s:", env_str))) {
      LOG_WARN("failed to append env str", K(ret));
    } else if (0 == STRCMP(lib_name, "libjvm.so")) {
      const char *java_home = std::getenv("JAVA_HOME");
      // libjvm.so relative path options and higher jdk version options are "/lib/server/".
      const char *jdk8_opt = "/jre/lib/amd64/server/";
      const char *jdk_higher_version_opt = "/lib/server/";

      if (OB_ISNULL(java_home)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cann't find java home", K(ret));
      } else if (OB_FAIL(lib_paths.append_fmt("%s%s:", java_home, jdk8_opt))) {
        LOG_WARN("failed to append java 8 path", K(ret));
      } else if (OB_FAIL(lib_paths.append_fmt("%s%s:", java_home, jdk_higher_version_opt))) {
        LOG_WARN("failed to append java higher version path", K(ret));
      }
    }
  }

  LOG_INFO("get final search lib paths: ", K(lib_paths.string()));
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    const char *ptr = lib_paths.ptr();
    while (' ' == *ptr || ':' == *ptr) { ++ptr; }
    int64_t env_str_len = lib_paths.length();
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
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to find path of lib", K(ret), K(found), K(lib_paths.string()), K(lib_name));
  } else if (!found) {
    ret = OB_JNI_ENV_SETUP_ERROR;
    LOG_WARN("failed to find path", K(ret), K(lib_paths.string()), K(lib_name));
  } else {
    LOG_INFO("succ to find path", K(ret), K(lib_paths.string()), K(path), K(lib_name));
  }
  return ret;
}

void JVMFunctionHelper::ob_java_free(void *ctxp, void *ptr)
{
  ObJavaEnvContext *ctx = static_cast<ObJavaEnvContext *>(ctxp);
  if (nullptr == ptr) {
    // do nothig
  } else {
    ob_free(ptr);
    ctx->jvm_free_times_ += 1;
  }
}

void JVMFunctionHelper::ob_hdfs_free(void *ctxp, void *ptr)
{
  ObHdfsEnvContext *ctx = static_cast<ObHdfsEnvContext *>(ctxp);
  if (nullptr == ptr) {
    // do nothig
  } else {
    ob_free(ptr);
    ctx->hdfs_free_times_ += 1;
  }
}

int JVMFunctionHelper::open_java_lib(ObJavaEnvContext &java_env_ctx)
{
  int ret = OB_SUCCESS;
  char * jvm_lib_buf = nullptr;
  const char* jvm_lib_name = "libjvm.so";

  int64_t load_lib_size = 4096; // 4k is enough for library path on `ext4` file system.
  if (OB_ISNULL(jvm_lib_buf = static_cast<char *>(ob_alloc_jni(&java_env_ctx, load_lib_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate jni env for jvm lib", K(ret));
  } else if (OB_FAIL(get_lib_path(jvm_lib_buf, load_lib_size, jvm_lib_name))) {
    // if return OB_SUCCESS, this func will obtain the C-style jvm lib path string.
    LOG_ERROR("failed to get jvm path", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(jvm_lib_handle_ = LIB_OPEN(jvm_lib_buf))) {
    ret = OB_JNI_ENV_SETUP_ERROR;
    const char * dlerror_str = dlerror();
    int dlerror_str_len = STRLEN(dlerror_str);
    LOG_WARN("failed to open jvm lib from path", K(ret), K(ObString(jvm_lib_buf)), K(dlerror_str));
  } else {
    LOG_INFO("use jvm lib from path", K(ret), KP(jvm_lib_handle_), K(ObString(jvm_lib_buf)));

    if (OB_FAIL(ret) && OB_NOT_NULL(jvm_lib_handle_)) {
      if (OB_JNI_ENV_SETUP_ERROR == ret) {
        LOG_WARN("failed to open jvm lib handle", K(ret));
      }
      LIB_CLOSE(jvm_lib_handle_);
      jvm_lib_handle_ = nullptr;
      LOG_WARN("lib_close jni lib", K(ret));
    }
  }

  if (OB_NOT_NULL(jvm_lib_buf)) {
    ob_java_free(&java_env_ctx, jvm_lib_buf);
    jvm_lib_buf = nullptr;
  } else { /* do nothing */ }
  return ret;
}

int JVMFunctionHelper::open_hdfs_lib(ObHdfsEnvContext &hdfs_env_ctx)
{
  int ret = OB_SUCCESS;
  char * hdfs_lib_buf = nullptr;
  const char* hdfs_lib_name = "libhdfs.so";

  int64_t load_lib_size = 4096; // 4k is enough for library path on `ext4` file system.
  if (OB_ISNULL(jvm_lib_handle_)) {
    ret = OB_JNI_ENV_SETUP_ERROR;
    LOG_WARN("invalid previous jvm lib handle which should be not null", K(ret));
  } else if (OB_ISNULL(hdfs_lib_buf = static_cast<char *>(ob_alloc_hdfs(&hdfs_env_ctx, load_lib_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate jni env for hdfs lib", K(ret));
  } else if (OB_FAIL(get_lib_path(hdfs_lib_buf, load_lib_size, hdfs_lib_name))) {
    // if return OB_SUCCESS, this func will obtain the C-style hdfs lib path string.
    LOG_WARN("failed to get hdfs path", K(ret));
  } else if (OB_ISNULL(hdfs_lib_handle_ = LIB_OPEN(hdfs_lib_buf))) {
    ret = OB_JNI_ENV_SETUP_ERROR;
    const char * dlerror_str = dlerror();
    int dlerror_str_len = STRLEN(dlerror_str);
    LOG_WARN("failed to open hdfs lib from path", K(ret), K(ObString(hdfs_lib_buf)), K(dlerror_str));
  } else {
    LOG_TRACE("succ to open jvm and hdfs lib from patch", KP(hdfs_lib_handle_), K(ObString(hdfs_lib_buf)), K(ObString(hdfs_lib_buf)));
    LIB_SYMBOL(hdfs_lib_handle_, "getJNIEnv", getJNIEnv, GETJNIENV);
    LIB_SYMBOL(hdfs_lib_handle_, "detachCurrentThread", detachCurrentThread, DETACHCURRENTTHREAD);
    // link related useful hdfs func
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsGetPathInfo", obHdfsGetPathInfo, HdfsGetPathInfoFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsFreeFileInfo", obHdfsFreeFileInfo, HdfsFreeFileInfoFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsDelete", obHdfsDelete, HdfsDeleteFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsGetLastExceptionRootCause", obHdfsGetLastExceptionRootCause, HdfsGetLastExceptionRootCauseFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsCreateDirectory", obHdfsCreateDirectory, HdfsCreateDirectoryFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsListDirectory", obHdfsListDirectory, HdfsListDirectoryFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsCloseFile", obHdfsCloseFile, HdfsCloseFileFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsOpenFile", obHdfsOpenFile, HdfsOpenFileFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsFileIsOpenForRead", obHdfsFileIsOpenForRead, HdfsFileIsOpenForReadFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsFileIsOpenForWrite", obHdfsFileIsOpenForWrite, HdfsFileIsOpenForWriteFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsPread", obHdfsPread, HdfsPreadFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsWrite", obHdfsWrite, HdfsWriteFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsFlush", obHdfsFlush, HdfsFlushFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsNewBuilder", obHdfsNewBuilder, HdfsNewBuilderFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsBuilderSetNameNode", obHdfsBuilderSetNameNode, HdfsBuilderSetNameNodeFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsBuilderSetUserName", obHdfsBuilderSetUserName, HdfsBuilderSetUserNameFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsBuilderSetForceNewInstance", obHdfsBuilderSetForceNewInstance, HdfsBuilderSetForceNewInstanceFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsBuilderConnect", obHdfsBuilderConnect, HdfsBuilderConnectFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsFreeBuilder", obHdfsFreeBuilder, HdfsFreeBuilderFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsDisconnect", obHdfsDisconnect, HdfsDisconnectFunc);

    // extra added for kerberos auth
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsBuilderConfSetStr", obHdfsBuilderConfSetStr, HdfsBuilderConfSetStrFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsBuilderSetPrincipal", obHdfsBuilderSetPrincipal, HdfsBuilderSetPrincipalFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsBuilderSetKerb5Conf", obHdfsBuilderSetKerb5Conf, HdfsBuilderSetKerb5ConfFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsBuilderSetKeyTabFile", obHdfsBuilderSetKeyTabFile, HdfsBuilderSetKeyTabFileFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsBuilderSetKerbTicketCachePath", obHdfsBuilderSetKerbTicketCachePath, HdfsBuilderSetKerbTicketCachePathFunc);

    int user_error_len = STRLEN(hdfs_lib_buf);
    if (OB_ISNULL(getJNIEnv) || OB_ISNULL(detachCurrentThread) ||
        /* hdfs funcs */
        OB_ISNULL(obHdfsGetPathInfo) ||
        OB_ISNULL(obHdfsFreeFileInfo) || OB_ISNULL(obHdfsDelete) ||
        OB_ISNULL(obHdfsGetLastExceptionRootCause) ||
        OB_ISNULL(obHdfsCreateDirectory) || OB_ISNULL(obHdfsListDirectory) ||
        OB_ISNULL(obHdfsCloseFile) || OB_ISNULL(obHdfsOpenFile) ||
        OB_ISNULL(obHdfsFileIsOpenForRead) || OB_ISNULL(obHdfsFileIsOpenForWrite) ||
        OB_ISNULL(obHdfsPread) || OB_ISNULL(obHdfsWrite) || OB_ISNULL(obHdfsFlush) ||
        OB_ISNULL(obHdfsNewBuilder) ||
        OB_ISNULL(obHdfsBuilderSetNameNode) ||
        OB_ISNULL(obHdfsBuilderSetUserName) ||
        OB_ISNULL(obHdfsBuilderSetForceNewInstance) ||
        OB_ISNULL(obHdfsBuilderConnect) || OB_ISNULL(obHdfsFreeBuilder) ||
        OB_ISNULL(obHdfsDisconnect) || OB_ISNULL(obHdfsBuilderSetPrincipal) ||
        OB_ISNULL(obHdfsBuilderSetKerb5Conf) || OB_ISNULL(obHdfsBuilderSetKeyTabFile)) {
      ret = OB_ERR_UNEXPECTED;
      const char * dlerror_str = dlerror();
      LOG_WARN("expected funcs exist some null, the loaded hdfs lib is not the expected", K(ret), KCSTRING(dlerror_str));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(hdfs_lib_handle_)) {
      if (OB_JNI_ENV_SETUP_ERROR == ret) {
        LOG_WARN("failed to open hdfs lib handle", K(ret));
      }
      getJNIEnv = nullptr;
      detachCurrentThread = nullptr;
      obHdfsGetPathInfo = nullptr;
      obHdfsFreeFileInfo = nullptr;
      obHdfsDelete = nullptr;
      obHdfsGetLastExceptionRootCause = nullptr;
      obHdfsCreateDirectory = nullptr;
      obHdfsListDirectory = nullptr;
      obHdfsCloseFile = nullptr;
      obHdfsOpenFile = nullptr;
      obHdfsFileIsOpenForRead = nullptr;
      obHdfsFileIsOpenForWrite = nullptr;
      obHdfsPread = nullptr;
      obHdfsWrite = nullptr;
      obHdfsFlush = nullptr;
      obHdfsNewBuilder = nullptr;
      obHdfsBuilderSetNameNode = nullptr;
      obHdfsBuilderSetUserName = nullptr;
      obHdfsBuilderSetForceNewInstance = nullptr;
      obHdfsBuilderConnect = nullptr;
      obHdfsFreeBuilder = nullptr;
      obHdfsDisconnect = nullptr;
      obHdfsBuilderSetPrincipal = nullptr;
      obHdfsBuilderSetKerb5Conf = nullptr;
      obHdfsBuilderSetKeyTabFile = nullptr;

      LIB_CLOSE(hdfs_lib_handle_);
      hdfs_lib_handle_ = nullptr;
      // Because hdfs lib depends on jvm lib, and hdfs lis closes then jvm lib close too.
      if (OB_NOT_NULL(jvm_lib_handle_)) {
        LIB_CLOSE(jvm_lib_handle_);
        jvm_lib_handle_ = nullptr;
      }
      LOG_WARN("lib_close hdfs and jni lib", K(ret));
    }
  }

  if (OB_NOT_NULL(hdfs_lib_buf)) {
    ob_hdfs_free(&hdfs_env_ctx, hdfs_lib_buf);
    hdfs_lib_buf = nullptr;
  } else { /* do nothing */ }
  return ret;
}

int JVMFunctionHelper::load_lib(ObJavaEnvContext &java_env_ctx,
                                ObHdfsEnvContext &hdfs_env_ctx) {
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard<> wg(load_lib_lock_);
  if (java_env_ctx.is_valid() && hdfs_env_ctx.is_valid()) {
    // do nothing
    LOG_TRACE("already success to open java and hdfs lib", K(ret));
  } else if (OB_NOT_NULL(jvm_lib_handle_) && OB_NOT_NULL(hdfs_lib_handle_)) {
    // do nothing
    LOG_TRACE("already success to open java and hdfs lib", K(ret));
  } else if (OB_ISNULL(jvm_lib_handle_) && OB_FAIL(open_java_lib(java_env_ctx))) {
    ret = OB_JNI_ENV_SETUP_ERROR;
    error_msg_ = "failed to open java lib, please check JAVA_HOME and LD_LIBRARY_PATH";
    LOG_WARN("failed to open java lib", K(ret));
  } else if (OB_ISNULL(hdfs_lib_handle_) && OB_FAIL(open_hdfs_lib(hdfs_env_ctx))) {
    ret = OB_JNI_ENV_SETUP_ERROR;
    error_msg_ = "failed to open hdfs lib, please check LD_LIBRARY_PATH";
    LOG_WARN("failed to open hdfs lib", K(ret));
  } else {
    LOG_TRACE("success to load most important method: getJNIEnv", K(ret));
    java_env_ctx.jvm_loaded_ = true;
    hdfs_env_ctx.hdfs_loaded_ = true;
  }
  return ret;
}


int JVMFunctionHelper::init_jni_env() {
  int ret = OB_SUCCESS;
  // init_jni_env can be called by multiple thread which it needs to add lock.
  LockGuard guard(lock_);
  if (OB_FAIL(detect_java_runtime())) {
    LOG_WARN("jni env is invalid", K(ret));
  } else if (OB_FAIL(load_lib(java_env_ctx_, hdfs_env_ctx_))) {
    LOG_WARN("failed to load dynamic library", K(ret));
  } else if (nullptr == jni_env_) {
    jni_env_ = getJNIEnv();
    if (nullptr == jni_env_) {
      ret = OB_JNI_ENV_ERROR;
      if (nullptr == error_msg_) {
        error_msg_ = "could not get a JNIEnv please check jvm opts";
      }
      LOG_WARN("could not get a JNIEnv please check jvm opts", K(ret), K(lbt()));
    }
  }
  return ret;
}

int JVMFunctionHelper::init_classes()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(jni_env_)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("already inited jni env, but it is null", K(ret));
  } else {
    jclass native_method_class;
    if (OB_FAIL(jni_find_class(CLASS_NATIVE_METHOD_HELPER_NAME,
                               &native_method_class))) {
      LOG_WARN("unable to find: native_method_class", K(ret));
    } else if (nullptr == native_method_class) {
      ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
      LOG_WARN("unable to find: native_method_class", K(ret));
    } else {
      int res = jni_env_->RegisterNatives(
          native_method_class, java_native_methods,
          sizeof(java_native_methods) / sizeof(java_native_methods[0]));
      if (OB_SUCCESS != res) {
        ret = OB_JNI_ERROR;
        LOG_WARN("failed to register native method for JVMFunctionHelper",
                 K(ret));
      } else {
        jni_env_->DeleteGlobalRef(native_method_class);
      }
    }
  }
  return ret;
}

int JVMFunctionHelper::do_init_() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_jni_env())) {
    LOG_WARN("failed to init jni env", K(ret));
  } else if (OB_ISNULL(jni_env_)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("already inited jni env, but it is null", K(ret));
  } else if (OB_FAIL(init_classes())) {
    LOG_WARN("failed to init useful classes", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr == error_msg_) {
      error_msg_ = "failed to init jni env";
    }
  }

  return ret;
}

int JVMFunctionHelper::detect_java_runtime() {
  int ret = OB_SUCCESS;

  const char *java_home = std::getenv("JAVA_HOME");
  LOG_TRACE("get current java home", K(ret), K(java_home));
  if (nullptr == java_home) {
    ret = OB_JNI_JAVA_HOME_NOT_FOUND_ERROR;
    LOG_WARN("env 'JAVA_HOME' is not set", K(ret), K(java_home));
  }

  if (OB_SUCC(ret)) {
    const char *connector_path = std::getenv("CONNECTOR_PATH");
    if (nullptr == connector_path) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("env 'CONNECTOR_PATH' is not set", K(ret), K(connector_path));
    }
  }

  if (OB_SUCC(ret)) {
    const char *java_opts = std::getenv("JAVA_OPTS");
    if (nullptr == java_opts) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("env 'JAVA_OPTS' is not set", K(ret), K(java_opts));
    } else if (OB_ISNULL(STRSTR(java_opts, "-XX:-CriticalJNINatives"))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN(
          "env 'JAVA_OPTS' without critical config `-XX:-CriticalJNINatives`",
          K(ret), K(java_opts));
      error_msg_ = "run jvm without critical config `-XX:-CriticalJNINatives`";
      LOG_USER_WARN(
          OB_INVALID_ARGUMENT,
          "run jvm without critical config `-XX:-CriticalJNINatives`");
    } else if (OB_ISNULL(STRSTR(java_opts, "-Djdk.lang.processReaperUseDefaultStackSize=true"))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN(
          "env 'JAVA_OPTS' without critical config `-Djdk.lang.processReaperUseDefaultStackSize=true`",
          K(ret), K(java_opts));
      error_msg_ = "run jvm without critical config `-Djdk.lang.processReaperUseDefaultStackSize=true`";
      LOG_USER_WARN(
          OB_INVALID_ARGUMENT,
          "run jvm without critical config `-Djdk.lang.processReaperUseDefaultStackSize=true`");
    } else if (OB_ISNULL(STRSTR(java_opts, "-Xrs"))) {
      ret = OB_INVALID_ARGUMENT;
      error_msg_ = "run jvm without critical config `-Xrs`";
      LOG_WARN("env 'JAVA_OPTS' without critical config `-Xrs`", K(ret), K(java_opts));
      LOG_USER_WARN(OB_INVALID_ARGUMENT, "run jvm without critical config `-Xrs`");
    }
  }

  if (OB_SUCC(ret)) {
    const char *libhdfs_opts = std::getenv("LIBHDFS_OPTS");
    if (nullptr == libhdfs_opts) {
      ret = OB_INVALID_ARGUMENT;
      error_msg_ = "run jvm without critical config `LIBHDFS_OPTS`";
      LOG_WARN("env 'LIBHDFS_OPTS' is not set", K(ret), K(libhdfs_opts));
      LOG_USER_WARN(OB_INVALID_ARGUMENT, "run jvm without critical config `LIBHDFS_OPTS`");
    }
  }

  if (OB_SUCC(ret)) {
    const char *classpath = std::getenv("CLASSPATH");
    if (nullptr == classpath) {
      ret = OB_INVALID_ARGUMENT;
      error_msg_ = "run jvm without critical config `CLASSPATH`";
      LOG_WARN("env 'CLASSPATH' is not set", K(ret), K(classpath));
      LOG_USER_WARN(OB_INVALID_ARGUMENT, "run jvm without critical config `CLASSPATH`");
    }
  }

  return ret;
}

// ------------------------- end of JVMFunctionHelper -------------------------

// ------------------------- start of JavaGlobalRef -------------------------
JavaGlobalRef::~JavaGlobalRef() {
  clear();
}

int JavaGlobalRef::new_global_ref(jobject handle, JNIEnv *env)
{
  int ret = OB_SUCCESS;
  jobject global_ref = nullptr;
  if (OB_NOT_NULL(handle_) || OB_ISNULL(handle) || OB_ISNULL(env)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(handle_), KP(handle), KP(env));
  } else if (OB_ISNULL(global_ref = env->NewGlobalRef(handle))) {
    ret = OB_JNI_ERROR;
    LOG_WARN("failed to create global ref", K(ret));
  } else {
    handle_ = global_ref;
  }

  return ret;
}
void JavaGlobalRef::clear() {
  int ret = OB_SUCCESS;
  if (handle_) {
    JNIEnv *env;
    if (OB_FAIL(JVMFunctionHelper::getInstance().get_env(env))) {
      LOG_WARN("failed to get jni env, cannot release java global handle", K(ret));
    } else {
      env->DeleteGlobalRef(handle_);
    }
    handle_ = nullptr;
  }
  LOG_DEBUG("clear the global ref", K(ret));
}
// ------------------------- end of JavaGlobalRef -------------------------

} // namespace sql
} // namespace oceanbase
