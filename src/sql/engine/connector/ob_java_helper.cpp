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

#include "ob_java_helper.h"
#include "share/ob_errno.h"

GETJNIENV getJNIEnv = NULL;
DETACHCURRENTTHREAD detachCurrentThread = NULL;
DESTROYJNIENV destroyJNIEnv = NULL;

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

using namespace common;
namespace sql
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
  return helper;
}

int JVMFunctionHelper::jni_find_class(const char *clazz, jclass* gen_clazz) {
  int ret = OB_SUCCESS;
  jclass cls = jni_env_->FindClass(clazz);
  if (nullptr == cls) {
    ret = OB_JNI_CLASS_NOT_FOUND_ERROR;
    LOG_WARN("failed to find class", K(ret), K(clazz));
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

int JVMFunctionHelper::inc_ref()
{
  int ret = OB_SUCCESS;
  LockGuard guard(lock_);
  if (java_env_ctx_.referece_times_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid jvm reference times", K(ret), K(java_env_ctx_.referece_times_));
  } else {
    java_env_ctx_.referece_times_ += 1;
  }
  return ret;
}

int JVMFunctionHelper::dec_ref()
{
  int ret = OB_SUCCESS;
  LockGuard guard(lock_);
  if (java_env_ctx_.referece_times_ < 1) {
    LOG_WARN("jvm reference times is less than expected decrease ref", K(ret),
             K(java_env_ctx_.referece_times_));
    java_env_ctx_.referece_times_ = 0;
  } else {
    java_env_ctx_.referece_times_ -= 1;
}
  return ret;
}

int JVMFunctionHelper::cur_ref()
{
  int ret = OB_SUCCESS;
  return java_env_ctx_.referece_times_;
}

int JVMFunctionHelper::reset_ref()
{
  int ret = OB_SUCCESS;
  LockGuard guard(lock_);
  java_env_ctx_.referece_times_ = 0;
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
  char *env_str = getenv(env_var_name);
  LOG_WARN("LD_LIBRARY_PATH: ", K(ObString(env_str)));
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
      if (0 == STRCMP(lib_name, "libjvm.so")) {
        default_lib_path = "/home/admin/oceanbase/lib/libjvm.so";
      } else if (0 == STRCMP(lib_name, "libhdfs.so")) {
        default_lib_path = "/home/admin/oceanbase/lib/libhdfs.so";
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unsupport library to load", K(ret), K(lib_name));
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(search_dir_file(default_path, lib_name, found))) {
          LOG_WARN("failed to searche dir file, continue to fine next dir", K(ret));
          ret = OB_SUCCESS;
      } else if (found) {
        const int64_t default_oci_path_len = STRLEN(default_lib_path);
        if (default_oci_path_len >= length) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to path length large than expected", K(ret));
        } else {
          strncpy(path, default_lib_path, default_oci_path_len);
          path[default_oci_path_len] = 0;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to find path of libjvm.so", K(ret), K(found), K(ObString(env_str)));
  } else if (!found) {
    ret = OB_JNI_ENV_ERROR;
    const char *user_error_str = "cant not find lib path";
    int user_error_len = STRLEN(user_error_str);
    // LOG_USER_ERROR(OB_JNI_ENV_ERROR, user_error_len, user_error_str);
    LOG_WARN("failed to find path", K(ret), K(ObString(env_str)), K(lib_name));
  } else {
    LOG_TRACE("succ to find path", K(ret), K(ObString(env_str)), K(path), K(lib_name));
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
    LOG_WARN("failed to get jvm path", K(ret));
  } else if (OB_ISNULL(jvm_lib_handle_ = LIB_OPEN(jvm_lib_buf))) {
    ret = OB_JNI_ENV_ERROR;
    const char * dlerror_str = dlerror();
    int dlerror_str_len = STRLEN(dlerror_str);
    // LOG_USER_ERROR(OB_JNI_ENV_ERROR, dlerror_str_len, dlerror_str);
    LOG_WARN("failed to open jvm lib from path", K(ret), K(ObString(jvm_lib_buf)), K(dlerror_str));
  } else {
    LOG_TRACE("use jvm lib from path", K(ret), K(ObString(jvm_lib_buf)));

    if (OB_FAIL(ret) && OB_NOT_NULL(jvm_lib_handle_)) {
      if (OB_JNI_ENV_ERROR == ret) {
        // LOG_USER_ERROR(OB_JNI_ENV_ERROR, user_error_len, hdfs_lib_buf);
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
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("invalid previous jvm lib handle which should be not null", K(ret));
  } else if (OB_ISNULL(hdfs_lib_buf = static_cast<char *>(ob_alloc_hdfs(&hdfs_env_ctx, load_lib_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate jni env for hdfs lib", K(ret));
  } else if (OB_FAIL(get_lib_path(hdfs_lib_buf, load_lib_size, hdfs_lib_name))) {
    // if return OB_SUCCESS, this func will obtain the C-style hdfs lib path string.
    LOG_WARN("failed to get hdfs path", K(ret));
  } else if (OB_ISNULL(hdfs_lib_handle_ = LIB_OPEN(hdfs_lib_buf))) {
    ret = OB_JNI_ENV_ERROR;
    const char * dlerror_str = dlerror();
    int dlerror_str_len = STRLEN(dlerror_str);
    // LOG_USER_ERROR(OB_JNI_ENV_ERROR, dlerror_str_len, dlerror_str);
    LOG_WARN("failed to open hdfs lib from path", K(ret), K(ObString(hdfs_lib_buf)), K(dlerror_str));
  } else {
    LOG_TRACE("succ to open jvm and hdfs lib from patch", K(ObString(hdfs_lib_buf)), K(ObString(hdfs_lib_buf)));
    LIB_SYMBOL(hdfs_lib_handle_, "getJNIEnv", getJNIEnv, GETJNIENV);
    LIB_SYMBOL(hdfs_lib_handle_, "detachCurrentThread", detachCurrentThread, DETACHCURRENTTHREAD);
    LIB_SYMBOL(hdfs_lib_handle_, "destroyJNIEnv", destroyJNIEnv, DESTROYJNIENV);
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
        OB_ISNULL(destroyJNIEnv) ||
        /* hdfs funcs */
        OB_ISNULL(obHdfsGetPathInfo) ||
        OB_ISNULL(obHdfsFreeFileInfo) || OB_ISNULL(obHdfsDelete) ||
        OB_ISNULL(obHdfsGetLastExceptionRootCause) ||
        OB_ISNULL(obHdfsCreateDirectory) || OB_ISNULL(obHdfsListDirectory) ||
        OB_ISNULL(obHdfsCloseFile) || OB_ISNULL(obHdfsOpenFile) ||
        OB_ISNULL(obHdfsFileIsOpenForRead) || OB_ISNULL(obHdfsFileIsOpenForWrite) ||
        OB_ISNULL(obHdfsPread) || OB_ISNULL(obHdfsNewBuilder) ||
        OB_ISNULL(obHdfsBuilderSetNameNode) ||
        OB_ISNULL(obHdfsBuilderSetUserName) ||
        OB_ISNULL(obHdfsBuilderSetForceNewInstance) ||
        OB_ISNULL(obHdfsBuilderConnect) || OB_ISNULL(obHdfsFreeBuilder) ||
        OB_ISNULL(obHdfsDisconnect) || OB_ISNULL(obHdfsBuilderSetPrincipal) ||
        OB_ISNULL(obHdfsBuilderSetKerb5Conf) || OB_ISNULL(obHdfsBuilderSetKeyTabFile)) {
      ret = OB_ERR_UNEXPECTED;
      const char * dlerror_str = dlerror();
      int dlerror_str_len = STRLEN(dlerror_str);
      LOG_WARN("expected funcs exist some null, the loaded hdfs lib is not the expected", K(ret), K(dlerror_str));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(hdfs_lib_handle_)) {
      if (OB_JNI_ENV_ERROR == ret) {
        // LOG_USER_ERROR(OB_JNI_ENV_ERROR, user_error_len, hdfs_lib_buf);
        LOG_WARN("failed to open hdfs lib handle", K(ret));
      }
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
  obsys::ObWLockGuard wg(load_lib_lock_);
  if (java_env_ctx.is_valid() && hdfs_env_ctx.is_valid()) {
    // do nothing
    LOG_TRACE("already success to open java and hdfs lib", K(ret));
  } else if (OB_NOT_NULL(jvm_lib_handle_) && OB_NOT_NULL(hdfs_lib_handle_)) {
    // do nothing
    LOG_TRACE("already success to open java and hdfs lib", K(ret));
  } else if (OB_ISNULL(jvm_lib_handle_) && OB_FAIL(open_java_lib(java_env_ctx))) {
    LOG_WARN("failed to open java lib", K(ret));
  } else if (OB_ISNULL(hdfs_lib_handle_) && OB_FAIL(open_hdfs_lib(hdfs_env_ctx))) {
    LOG_WARN("failed to open hdfs lib", K(ret));
  } else {
    LOG_TRACE("succ to open java and hdfs lib", K(ret));
    LOG_TRACE("start to load most important method: getJNIEnv", K(ret));
    LIB_SYMBOL(hdfs_lib_handle_, "getJNIEnv", getJNIEnv, GETJNIENV);

    if (OB_SUCC(ret)) {
      LOG_TRACE("success to load most important method: getJNIEnv", K(ret));
      java_env_ctx.jvm_loaded_ = true;
      hdfs_env_ctx.hdfs_loaded_ = true;
    }
  }
  return ret;
}

int JVMFunctionHelper::detach_current_thread()
{
  int ret = OB_SUCCESS;
  LockGuard guard(lock_);
  if (OB_ISNULL(jni_env_)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("failed to detach thread for null jni env", K(ret));
  } else if (OB_ISNULL(detachCurrentThread)) {
    ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
    LOG_WARN("method detachCurrentThread not exists", K(ret));
  } else { /* do nothing */}
  LOG_TRACE("start to detatch current thread", K(ret));
  if (OB_SUCC(ret)) {
    jint rv = detachCurrentThread();
    if (0 != rv) {
      ret = OB_JNI_ENV_ERROR;
      LOG_WARN("failed to detatch current thread", K(ret), K(rv));
    }
    // Note: jni_env_ should re-get
    jni_env_ = nullptr;
  }
  return ret;
}

int JVMFunctionHelper::destroy_env()
{
  int ret = OB_SUCCESS;
  // TODO(bitao): support destroy env
  // if (OB_ISNULL(destroyJNIEnv)) {
  //   ret = OB_JNI_DESTORY_JVM_ERROR;
  //   LOG_WARN("failed to get critical method point `destoryJNIEnv`", K(ret));
  // } else if (OB_FAIL(destroyJNIEnv())) {
  //   ret = OB_JNI_DESTORY_JVM_ERROR;
  //   LOG_WARN("failed to destory jni env", K(ret));
  // } else {
  //   jni_env_ = nullptr;
  // }
  return ret;
}

int JVMFunctionHelper::check_valid_env() {
  int ret = OB_SUCCESS;
  // TODO(bitao): add a more efficient method to check jni env whether can be used.
  if (OB_FAIL(detect_java_runtime())) {
    LOG_WARN("failed to detect java runtime env", K(ret));
  }
  return ret;
}

int JVMFunctionHelper::init_jni_env() {
  int ret = OB_SUCCESS;
  // init_jni_env can be called by multiple thread which it needs to add lock.
  LockGuard guard(lock_);
  if (OB_FAIL(check_valid_env())) {
    LOG_WARN("jni env is invalid", K(ret));
  } else if (OB_FAIL(load_lib(java_env_ctx_, hdfs_env_ctx_))) {
    LOG_WARN("failed to load dynamic library", K(ret));
  } else if (nullptr == jni_env_) {
    jni_env_ = getJNIEnv();
    if (nullptr == jni_env_) {
      ret = OB_JNI_ENV_ERROR;
      LOG_WARN("could not get a JNIEnv", K(ret), K(lbt()));
    }
  } else {
    const char *jh = std::getenv("JAVA_HOME");
    const char *jo = std::getenv("JAVA_OPTS");
    const char *cp = std::getenv("CLASSPATH");
    const char *ch = std::getenv("CONNECTOR_PATH");
    LOG_TRACE("get env variables in init_jni_env", K(ret), K(jh), K(jo), K(cp), K(ch));
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
  } else { /* do nothing */}
  return ret;
}

ObString JVMFunctionHelper::to_ob_string(jstring str) {
  const char* charflow = jni_env_->GetStringUTFChars((jstring)str, nullptr);
  ObString res(charflow);
  jni_env_->ReleaseStringUTFChars((jstring)str, charflow);
  return res;
}

jmethodID JVMFunctionHelper::getToStringMethod(jclass clazz) {
  return jni_env_->GetMethodID(clazz, "toString", "()Ljava/lang/String;");
}
// ------------------------- end of JVMFunctionHelper -------------------------

// ------------------------- start of JavaGlobalRef -------------------------
JavaGlobalRef::~JavaGlobalRef() {
  clear();
}

void JavaGlobalRef::clear() {
  int ret = OB_SUCCESS;
  if (handle_) {
    JNIEnv *env;
    if (OB_FAIL(JVMFunctionHelper::getInstance().get_env(env))) {
      LOG_WARN("failed to get jni env", K(ret));
    } else {
      env->DeleteGlobalRef(handle_);
    }
    handle_ = nullptr;
  }
  LOG_WARN("clear the global ref", K(ret));
}
// ------------------------- end of JavaGlobalRef -------------------------

int detect_java_runtime() {
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
      LOG_USER_WARN(
          OB_INVALID_ARGUMENT,
          "run jvm without critical config `-XX:-CriticalJNINatives`");
    } else if (OB_ISNULL(STRSTR(java_opts, "-Djdk.lang.processReaperUseDefaultStackSize=true"))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN(
          "env 'JAVA_OPTS' without critical config `-Djdk.lang.processReaperUseDefaultStackSize=true`",
          K(ret), K(java_opts));
      LOG_USER_WARN(
          OB_INVALID_ARGUMENT,
          "run jvm without critical config `-Djdk.lang.processReaperUseDefaultStackSize=true`");
    } else { /* do nothing */}
  }

  if (OB_SUCC(ret)) {
    const char *libhdfs_opts = std::getenv("LIBHDFS_OPTS");
    if (nullptr == libhdfs_opts) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("env 'LIBHDFS_OPTS' is not set", K(ret), K(libhdfs_opts));
    }
  }

  if (OB_SUCC(ret)) {
    const char *classpath = std::getenv("CLASSPATH");
    if (nullptr == classpath) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("env 'CLASSPATH' is not set", K(ret), K(classpath));
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
