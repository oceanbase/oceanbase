/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL_ENG

#include <cstdlib>
#include <dirent.h>
#include <pthread.h>
#include <unistd.h>

#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/file/file_directory_utils.h"
#include "share/config/ob_server_config.h"

#include "lib/jni_env/ob_java_vm_manager.h"
#include "lib/jni_env/ob_java_env.h"
#include "ob_jar_version_def.h"
#include "share/ob_errno.h"

GETJNIENV getJNIEnv = NULL;

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

thread_local JNIEnv* ObJavaVmManager::jni_env_ = nullptr;
constexpr const char* CLASS_NATIVE_METHOD_HELPER_NAME = "com/oceanbase/utils/NativeMethodHelper";

// ---------------------------------------------------------------------------
// Two JNIEnv providers: HDFS-managed vs self-created JVM
// ---------------------------------------------------------------------------
class ObHdfsJniEnvProvider final : public ObJniEnvProvider {
public:
  explicit ObHdfsJniEnvProvider(GETJNIENV get_env_fn) : get_env_fn_(get_env_fn) {}
  int get_env(JNIEnv *&env) override
  {
    int ret = OB_SUCCESS;
    env = get_env_fn_();
    if (OB_ISNULL(env)) {
      ret = OB_JNI_ENV_ERROR;
      LOG_WARN("getJNIEnv returned null", K(ret));
    }
    return ret;
  }
private:
  GETJNIENV get_env_fn_;
};

class ObDirectJvmEnvProvider final : public ObJniEnvProvider {
public:
  explicit ObDirectJvmEnvProvider(JavaVM *jvm) : jvm_(jvm) {}
  int get_env(JNIEnv *&env) override
  {
    int ret = OB_SUCCESS;
    jint jni_ret = jvm_->GetEnv((void **)&env, JNI_VERSION_1_8);
    if (JNI_EDETACHED == jni_ret) {
      jni_ret = jvm_->AttachCurrentThread((void **)&env, nullptr);
    }
    if (JNI_OK != jni_ret || OB_ISNULL(env)) {
      ret = OB_JNI_ERROR;
      LOG_WARN("failed to attach thread to jvm", K(ret), K(jni_ret));
    }
    return ret;
  }
private:
  JavaVM *jvm_;
};

// Minimum remaining stack space required before calling JNI_CreateJavaVM.
//
// Formula:
//   min_remaining = guard_zone + shadow_zone + jvm_init_frames + call_chain_gap + safety_margin
//
// Components:
//   guard_zone       - HotSpot guard pages (red + yellow + reserved) * page_size
//   shadow_zone      - HotSpot shadow pages * page_size
//   jvm_init_frames  - peak stack consumed by boot layer class initialization
//                      (ModuleBootstrap.boot -> <clinit> chains in template interpreter)
//   call_chain_gap   - OB frames between this check and the actual JNI_CreateJavaVM call
//   safety_margin    - buffer for JDK version variance and compiler differences
//
// CRITICAL: HotSpot uses os::vm_page_size() for guard/shadow page calculations.
//   On Kunpeng 920 aarch64 with 64KB pages: guard=(1+1)*64KB=128KB, shadow=1*64KB=64KB
//   On x86_64 with 4KB pages:               guard=(1+2+1)*4KB=16KB, shadow=6*4KB=24KB
//   Using wrong page size leads to threshold being 10x too small!
//
// Empirical measurements (binary-search on test_jvm_stack, Bisheng JDK 11, Kunpeng 920):
//   aarch64 64KB-page: crash boundary at remaining ~397KB
//     breakdown: guard=128KB + shadow=64KB + boot_layer_clinit_frames~200KB ≈ 392KB
//   aarch64 4KB-page:  crash boundary would be ~210KB (estimated)
//   x86_64  4KB-page:  crash boundary ~210KB (measured)
//
// JDK 11 vs JDK 8 difference (~190KB on 64KB-page aarch64):
//   JDK 9+ added Module System (jdk.internal.module.ModuleBootstrap.boot)
//   which recursively initializes ~40 module-related classes during boot layer setup.
//   Each <clinit> call in template interpreter consumes ~1 interpreter frame (variable size).
//   On 64KB-page systems, the shadow zone alone accounts for 64KB of the difference.
static int64_t calc_jvm_min_stack_remaining()
{
  const int64_t page_size = sysconf(_SC_PAGESIZE);  // runtime: 4KB on x86_64, 64KB on Kunpeng aarch64
#if defined(__x86_64__) || defined(__i386__)
  const int64_t stack_red_pages      = 1;
  const int64_t stack_yellow_pages   = 2;
  const int64_t stack_reserved_pages = 1;
  const int64_t stack_shadow_pages   = 6;    // JDK11 x86_64: StackShadowPages=6
  const int64_t jvm_init_frames      = 160L << 10;  // boot layer <clinit> chain peak
#elif defined(__aarch64__)
  const int64_t stack_red_pages      = 1;
  const int64_t stack_yellow_pages   = 1;    // aarch64 JDK 8/11: StackYellowPages=1
  const int64_t stack_reserved_pages = 0;    // aarch64 JDK 8: no StackReservedPages
  const int64_t stack_shadow_pages   = 1;    // aarch64 JDK 8/11: StackShadowPages=1
  const int64_t jvm_init_frames      = 200L << 10;  // boot layer <clinit> chain peak
#else
  const int64_t stack_red_pages      = 1;
  const int64_t stack_yellow_pages   = 2;
  const int64_t stack_reserved_pages = 1;
  const int64_t stack_shadow_pages   = 6;
  const int64_t jvm_init_frames      = 200L << 10;  // conservative
#endif
  const int64_t guard_zone      = (stack_red_pages + stack_yellow_pages + stack_reserved_pages) * page_size;
  const int64_t shadow_zone     = stack_shadow_pages * page_size;
  const int64_t call_chain_gap  = 20L << 10;   // OB frames: check -> setup_env -> open_lib -> init_opts -> create_jvm
  const int64_t safety_margin   = 30L << 10;   // JDK version variance, compiler optimization differences
  return guard_zone + shadow_zone + jvm_init_frames + call_chain_gap + safety_margin;
}

int check_jvm_stack_size()
{
  int ret = OB_SUCCESS;
  const int64_t min_jvm_stack_remaining = calc_jvm_min_stack_remaining();
  pthread_attr_t attr;
  void *stack_base = nullptr;
  size_t stack_size = 0;
  if (0 != pthread_getattr_np(pthread_self(), &attr)) {
    LOG_WARN("failed to get current thread attributes, skip stack size check");
  } else {
    pthread_attr_getstack(&attr, &stack_base, &stack_size);
    pthread_attr_destroy(&attr);
    char local_var;
    const int64_t remaining = (int64_t)((uintptr_t)&local_var - (uintptr_t)stack_base);
    if (OB_UNLIKELY(remaining <= min_jvm_stack_remaining)) {
      ret = OB_JNI_ENV_SETUP_ERROR;
      const char *user_warn_msg =
          "JVM startup requires more remaining stack space; "
          "please set stack_size and _tenant_stack_size to larger values, for example 2M, and restart observer";
      LOG_WARN("remaining thread stack is too small to start jvm",
               K(ret), K(remaining), K(min_jvm_stack_remaining), K(stack_size));
      LOG_USER_WARN(OB_JNI_ENV_SETUP_ERROR, static_cast<int>(STRLEN(user_warn_msg)), user_warn_msg);
    } else {
      LOG_INFO("thread stack check passed for jvm startup",
               K(remaining), K(min_jvm_stack_remaining), K(stack_size));
    }
  }
  return ret;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
static JNINativeMethod java_native_methods[] = {
        {"memoryTrackerMalloc", "(J)J", (void*)&JavaNativeMethods::memory_malloc},
        {"memoryTrackerFree", "(J)V", (void*)&JavaNativeMethods::memory_free},
};
#pragma GCC diagnostic pop

// ------------------------- start of ObJavaVmManager -------------------------
ObJavaVmManager &ObJavaVmManager::getInstance() {
  static ObJavaVmManager helper;
  return helper;
}

ObJavaVmManager::ObJavaVmManager(): init_jni_env_lock_(common::ObLatchIds::JNI_ENV_INIT_LOCK), error_msg_(nullptr) {
}

int ObJavaVmManager::jni_find_class(const char *clazz, jclass* gen_clazz) {
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

int ObJavaVmManager::get_env(JNIEnv *&env) {
  int ret = OB_SUCCESS;
  if (nullptr != jni_env_) {
    env = jni_env_;
  } else if (OB_ISNULL(env_provider_) && OB_FAIL(do_init_())) {
    LOG_WARN("failed to init jni env", K(ret));
  } else if (OB_FAIL(env_provider_->get_env(jni_env_))) {
    LOG_WARN("failed to get jni env from provider", K(ret));
  } else {
    env = jni_env_;
  }
  return ret;
}


// Returns true if file_name exists in dir; false if not found or dir is inaccessible.
bool ObJavaVmManager::search_dir_file(const char *dir, const char *file_name)
{
  bool found = false;
  DIR *dirp = nullptr;
  if (OB_NOT_NULL(dir) && OB_NOT_NULL(file_name) && OB_NOT_NULL(dirp = opendir(dir))) {
    dirent *dp = nullptr;
    while (!found && OB_NOT_NULL(dp = readdir(dirp))) {
      if (DT_UNKNOWN == dp->d_type || DT_LNK == dp->d_type || DT_REG == dp->d_type) {
        found = (0 == strcasecmp(file_name, dp->d_name));
      }
    }
    closedir(dirp);
  }
  return found;
}

// Appends LD_LIBRARY_PATH entries to paths. Reads env only; no filesystem I/O.
int ObJavaVmManager::build_lib_search_paths_(ObSqlString &paths)
{
  int ret = OB_SUCCESS;
  const char *ld_path = std::getenv("LD_LIBRARY_PATH");
  if (OB_ISNULL(ld_path)) {
    ret = OB_JNI_ENV_SETUP_ERROR;
    LOG_WARN("LD_LIBRARY_PATH is not set", K(ret));
  } else if (OB_FAIL(paths.append_fmt("%s:", ld_path))) {
    LOG_WARN("failed to append LD_LIBRARY_PATH to search list", K(ret));
  }
  return ret;
}

// Scans search_paths (colon-delimited) for lib_name; writes the full path into path on success.
int ObJavaVmManager::get_lib_path(const char *lib_name, const ObSqlString &search_paths,
                                    ObSqlString &path)
{
  int ret = OB_SUCCESS;
  bool found = false;
  LOG_INFO("lib search paths", KCSTRING(lib_name), K(search_paths.string()));
  ObString remaining(search_paths.string());
  while (OB_SUCC(ret) && !found && !remaining.empty()) {
    ObString dir = remaining.split_on(':');
    if (dir.empty() && OB_ISNULL(remaining.find(':'))) {
      dir = remaining;
      remaining.reset();
    }
    while (!dir.empty() && ' ' == *dir.ptr()) {
      dir.assign_ptr(dir.ptr() + 1, dir.length() - 1);
    }
    if (!dir.empty()) {
      ObSqlString dir_str;
      if (OB_FAIL(dir_str.append(dir))) {
        LOG_WARN("failed to copy dir to string", K(ret), K(dir));
      } else {
        found = search_dir_file(dir_str.ptr(), lib_name);
        LOG_DEBUG("searched dir for lib", K(dir), KCSTRING(lib_name), K(found));
      }

      if (OB_SUCC(ret) && found) {
        if (OB_FAIL(path.append(dir))) {
          LOG_WARN("failed to build lib path", K(ret), K(dir));
        } else if (OB_FAIL(path.append_fmt("/%s", lib_name))) {
          LOG_WARN("failed to append lib name to path", K(ret), KCSTRING(lib_name));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to resolve lib path", K(ret), KCSTRING(lib_name));
  } else if (!found) {
    ret = OB_JNI_ENV_SETUP_ERROR;
    LOG_WARN("lib not found in any search path", K(ret), KCSTRING(lib_name), K(search_paths.string()));
  } else {
    LOG_INFO("resolved lib path", KCSTRING(lib_name), K(path.string()));
  }
  return ret;
}

int ObJavaVmManager::open_lib_(const char *lib_name, const ObSqlString &search_paths,
                                 void *&lib_handle)
{
  int ret = OB_SUCCESS;
  ObSqlString lib_path;
  if (OB_FAIL(get_lib_path(lib_name, search_paths, lib_path))) {
    LOG_WARN("failed to get lib path", K(ret), KCSTRING(lib_name));
  } else if (OB_ISNULL(lib_handle = LIB_OPEN(lib_path.ptr()))) {
    ret = OB_JNI_ENV_SETUP_ERROR;
    LOG_WARN("failed to open lib", K(ret), K(lib_path.string()), KCSTRING(dlerror()));
  } else {
    LOG_INFO("opened lib", KP(lib_handle), K(lib_path.string()));
  }
  return ret;
}

int ObJavaVmManager::open_java_lib()
{
  int ret = OB_SUCCESS;
  ObSqlString search_paths;
  const char *java_home = std::getenv("JAVA_HOME");
#if defined(__aarch64__)
  const char *jdk8_server = "/jre/lib/aarch64/server/";
#else
  const char *jdk8_server = "/jre/lib/amd64/server/";
#endif
  const char *jdk_higher_version_opt = "/lib/server/";
  if (OB_FAIL(build_lib_search_paths_(search_paths))) {
    error_msg_ = "LD_LIBRARY_PATH is not set, required to locate libjvm.so";
    LOG_WARN("failed to build lib search paths", K(ret));
  } else if (OB_ISNULL(java_home)) {
    ret = OB_JNI_ENV_SETUP_ERROR;
    error_msg_ = "JAVA_HOME is not set, required to locate libjvm.so";
    LOG_WARN("JAVA_HOME is not set, required to locate libjvm.so", K(ret));
  } else if (OB_FAIL(search_paths.append_fmt("%s%s:", java_home, jdk8_server, java_home))) {
    LOG_WARN("failed to append JAVA_HOME paths to search list", K(ret));
  } else if (OB_FAIL(search_paths.append_fmt("%s%s:", java_home, jdk_higher_version_opt))) {
    LOG_WARN("failed to append java higher version path", K(ret));
  } else if (OB_FAIL(open_lib_("libjvm.so", search_paths, jvm_lib_handle_))) {
    error_msg_ = "libjvm.so not found, please check JAVA_HOME and LD_LIBRARY_PATH";
    LOG_ERROR("failed to open jvm lib", K(ret));
  }
  return ret;
}

int ObJavaVmManager::open_hdfs_lib()
{
  int ret = OB_SUCCESS;
  ObSqlString search_paths;
  if (OB_FAIL(build_lib_search_paths_(search_paths))) {
    LOG_WARN("failed to build lib search paths", K(ret));
  } else if (OB_FAIL(open_lib_("libhdfs.so", search_paths, hdfs_lib_handle_))) {
    LOG_WARN("failed to open hdfs lib", K(ret));
  } else {
    LOG_TRACE("opened hdfs lib", KP(hdfs_lib_handle_));
    LIB_SYMBOL(hdfs_lib_handle_, "getJNIEnv", getJNIEnv, GETJNIENV);
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
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsBuilderConfSetStr", obHdfsBuilderConfSetStr, HdfsBuilderConfSetStrFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsBuilderSetPrincipal", obHdfsBuilderSetPrincipal, HdfsBuilderSetPrincipalFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsBuilderSetKerb5Conf", obHdfsBuilderSetKerb5Conf, HdfsBuilderSetKerb5ConfFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsBuilderSetKeyTabFile", obHdfsBuilderSetKeyTabFile, HdfsBuilderSetKeyTabFileFunc);
    LIB_SYMBOL(hdfs_lib_handle_, "hdfsBuilderSetKerbTicketCachePath", obHdfsBuilderSetKerbTicketCachePath, HdfsBuilderSetKerbTicketCachePathFunc);

    if (OB_ISNULL(getJNIEnv) ||
        OB_ISNULL(obHdfsGetPathInfo) ||
        OB_ISNULL(obHdfsFreeFileInfo) || OB_ISNULL(obHdfsDelete) ||
        OB_ISNULL(obHdfsGetLastExceptionRootCause) ||
        OB_ISNULL(obHdfsCreateDirectory) || OB_ISNULL(obHdfsListDirectory) ||
        OB_ISNULL(obHdfsCloseFile) || OB_ISNULL(obHdfsOpenFile) ||
        OB_ISNULL(obHdfsFileIsOpenForRead) || OB_ISNULL(obHdfsFileIsOpenForWrite) ||
        OB_ISNULL(obHdfsPread) || OB_ISNULL(obHdfsWrite) || OB_ISNULL(obHdfsFlush) ||
        OB_ISNULL(obHdfsNewBuilder) || OB_ISNULL(obHdfsBuilderSetNameNode) ||
        OB_ISNULL(obHdfsBuilderSetUserName) || OB_ISNULL(obHdfsBuilderSetForceNewInstance) ||
        OB_ISNULL(obHdfsBuilderConnect) || OB_ISNULL(obHdfsFreeBuilder) ||
        OB_ISNULL(obHdfsDisconnect) || OB_ISNULL(obHdfsBuilderSetPrincipal) ||
        OB_ISNULL(obHdfsBuilderSetKerb5Conf) || OB_ISNULL(obHdfsBuilderSetKeyTabFile)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expected funcs exist some null, the loaded hdfs lib is not the expected",
               K(ret), KCSTRING(dlerror()));
    }

    if (OB_FAIL(ret)) {
      getJNIEnv = nullptr;
      obHdfsGetPathInfo = nullptr; obHdfsFreeFileInfo = nullptr;
      obHdfsDelete = nullptr; obHdfsGetLastExceptionRootCause = nullptr;
      obHdfsCreateDirectory = nullptr; obHdfsListDirectory = nullptr;
      obHdfsCloseFile = nullptr; obHdfsOpenFile = nullptr;
      obHdfsFileIsOpenForRead = nullptr; obHdfsFileIsOpenForWrite = nullptr;
      obHdfsPread = nullptr; obHdfsWrite = nullptr; obHdfsFlush = nullptr;
      obHdfsNewBuilder = nullptr; obHdfsBuilderSetNameNode = nullptr;
      obHdfsBuilderSetUserName = nullptr; obHdfsBuilderSetForceNewInstance = nullptr;
      obHdfsBuilderConnect = nullptr; obHdfsFreeBuilder = nullptr;
      obHdfsDisconnect = nullptr; obHdfsBuilderSetPrincipal = nullptr;
      obHdfsBuilderSetKerb5Conf = nullptr; obHdfsBuilderSetKeyTabFile = nullptr;
      LIB_CLOSE(hdfs_lib_handle_);
      hdfs_lib_handle_ = nullptr;
      LOG_WARN("closed hdfs lib due to symbol load failure", K(ret));
    }
  }
  return ret;
}


int ObJavaVmManager::create_jvm_(JavaVM *&jvm, JavaVMInitArgs &jvm_args)
{
  int ret = OB_SUCCESS;
  using JNI_CreateJavaVM_f = jint (*)(JavaVM **, void **, void *);
  using JNI_GetCreatedJavaVMs_f = jint (*)(JavaVM **, jsize, jsize *);
  JNI_CreateJavaVM_f      create_jvm_fn  = nullptr;
  JNI_GetCreatedJavaVMs_f get_created_fn = nullptr;
  jint jni_ret = JNI_OK;
  int num_jvms = 0;
  jvm = nullptr;

  if (OB_ISNULL(jvm_lib_handle_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("jvm lib handle is null, open_java_lib must be called first", K(ret));
  } else {
    LIB_SYMBOL(jvm_lib_handle_, "JNI_CreateJavaVM",      create_jvm_fn,  JNI_CreateJavaVM_f);
    LIB_SYMBOL(jvm_lib_handle_, "JNI_GetCreatedJavaVMs", get_created_fn, JNI_GetCreatedJavaVMs_f);
    if (OB_ISNULL(create_jvm_fn) || OB_ISNULL(get_created_fn)) {
      ret = OB_JNI_ERROR;
      LOG_WARN("failed to dlsym JNI functions from libjvm.so", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (JNI_OK != (jni_ret = get_created_fn(&jvm, 1, &num_jvms))) {
      ret = OB_JNI_ERROR;
      LOG_WARN("JNI_GetCreatedJavaVMs failed", K(jni_ret), K(ret));
    } else if (0 == num_jvms) {
      if (JNI_OK != (jni_ret = create_jvm_fn(&jvm, (void **)&jni_env_, &jvm_args))) {
        ret = OB_JNI_ERROR;
        LOG_WARN("JNI_CreateJavaVM failed", K(jni_ret), K(ret));
      }
    } else {
      if (JNI_OK != (jni_ret = jvm->GetEnv((void **)&jni_env_, JNI_VERSION_1_8))
          && JNI_EDETACHED != jni_ret) {
        ret = OB_JNI_ERROR;
        LOG_WARN("failed to get jni env from existing jvm", K(jni_ret), K(ret));
      } else if (JNI_EDETACHED == jni_ret) {
        if (JNI_OK != (jni_ret = jvm->AttachCurrentThread((void **)&jni_env_, nullptr))) {
          ret = OB_JNI_ERROR;
          LOG_WARN("failed to attach to existing jvm", K(jni_ret), K(ret));
        }
      }
    }
  }
  LOG_INFO("create_jvm_ done", K(ret), KP(jvm), KP(jni_env_));
  return ret;
}

// ---------- JVM option helpers (moved from ob_external_jni_utils) ----------
namespace {

struct JavaVMOptionWrapper {
  JavaVMOption option;

  JavaVMOptionWrapper()
  {
    option.optionString = nullptr;
  }
  JavaVMOptionWrapper(const JavaVMOption &vm_option) : option(vm_option)
  {}

  TO_STRING_KV(KCSTRING(option.optionString));
};

static int expand_directory_(const ObString &single_classpath, ObSqlString &expanded_classpath)
{
  int ret = OB_SUCCESS;
  DIR *dir = nullptr;
  int sys_ret = 0;
  ObSqlString single_classpath_str;
  if (OB_FAIL(single_classpath_str.assign(single_classpath))) {
    LOG_WARN("failed to assign classpath to sql string", K(single_classpath), K(ret));
  } else if (OB_ISNULL(dir = opendir(single_classpath_str.ptr()))) {
    ret = OB_SUCCESS;
    LOG_INFO("[ignored] failed to open directory", K(ret), K(single_classpath), KCSTRING(strerror(errno)));
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
          LOG_WARN("failed to append classpath", K(ret), K(single_classpath), KCSTRING(entry->d_name));
        }
      }
    }
    if (errno != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ignored] failed to expand directory", K(ret), KCSTRING(strerror(errno)), K(single_classpath));
    }
  }
  if (OB_NOT_NULL(dir) && 0 != (sys_ret = closedir(dir))) {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("[ignored] failed to closedir", K(ret), KCSTRING(strerror(errno)));
  }
  return ret;
}

static int expand_classpath_(ObArenaAllocator &allocator, const ObString &src_classpath,
                             ObString &expanded_classpath)
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
        if (OB_FAIL(expand_directory_(item_classpath, expanded_sql_classpath))) {
          LOG_WARN("failed to expand classpath directory", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        const char *append_format = "%.*s";
        if (!expanded_sql_classpath.empty()) {
          append_format = ":%.*s";
        }
        if (OB_FAIL(expanded_sql_classpath.append_fmt(append_format,
                                                      item_classpath.length(), item_classpath.ptr()))) {
          LOG_WARN("failed to append classpath", K(ret), K(item_classpath), K(expanded_sql_classpath));
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

static int init_jvm_options_(ObArenaAllocator &allocator,
                             ObIArray<JavaVMOptionWrapper> &options,
                             const char *&error_msg)
{
  int ret = OB_SUCCESS;
  const char *classpath_option_name = "-Djava.class.path=";
  ObString connector_path_config;
  ObString java_opts = GCONF.ob_java_opts.get_value_string();
  LOG_INFO("got connector path and java option from config", K(connector_path_config), K(java_opts));
  const char opt_delimiter = ' ';

  if (OB_FAIL(expand_classpath_(allocator, GCONF.ob_java_connector_path.get_value_string(),
                                connector_path_config))) {
    error_msg = "failed to expand ob_java_connector_path, please check the config";
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
      MEMCPY(buf + STRLEN(classpath_option_name), connector_path_config.ptr(),
             connector_path_config.length());
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

} // anonymous namespace

int ObJavaVmManager::init_jni_env() {
  int ret = OB_SUCCESS;
  LockGuard guard(init_jni_env_lock_);
  if (OB_NOT_NULL(env_provider_)) {
    // already initialized by another thread
  } else if (OB_FAIL(check_jvm_stack_size())) {
    error_msg_ = "remaining thread stack is too small to start JVM, please set stack_size and _tenant_stack_size to larger values and restart observer";
    LOG_WARN("failed to check ob stack_size before starting jvm", K(ret));
  } else if (!ObJavaEnv::getInstance().is_env_inited()
             && OB_FAIL(ObJavaEnv::getInstance().setup_java_env())) {
    error_msg_ = "failed to setup java env, please check JAVA_HOME";
    LOG_WARN("failed to setup java env", K(ret));
  } else if (OB_FAIL(ObJavaEnv::getInstance().setup_java_env_classpath_and_ldlib_path())) {
    error_msg_ = "failed to setup java env classpath or LD_LIBRARY_PATH";
    LOG_WARN("failed to setup java env classpath and ldlib path", K(ret));
  } else if (OB_FAIL(ObJavaEnv::getInstance().detect_java_runtime_variables())) {
    error_msg_ = "JVM critical config is missing, please check LIBHDFS_OPTS, CLASSPATH and JVM options";
    LOG_WARN("jni env is invalid", K(ret));
  } else if (OB_ISNULL(jvm_lib_handle_) && OB_FAIL(open_java_lib())) {
    LOG_WARN("failed to open java lib", K(ret));
  } else if (OB_SUCCESS == open_hdfs_lib()) {
    LOG_INFO("libhdfs.so loaded, will use HDFS-managed JVM");
    env_provider_ = OB_NEW(ObHdfsJniEnvProvider, "JniEnvProv", getJNIEnv);
    if (OB_ISNULL(env_provider_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate ObHdfsJniEnvProvider", K(ret));
    }
  } else {
    LOG_INFO("libhdfs.so not available, will create JVM directly");
    JavaVM *jvm = nullptr;
    ObArenaAllocator allocator;
    ObArray<JavaVMOptionWrapper> options;
    if (OB_FAIL(init_jvm_options_(allocator, options, error_msg_))) {
      LOG_WARN("failed to build jvm options", K(ret));
    } else {
      JavaVMInitArgs jvm_args;
      jvm_args.version            = JNI_VERSION_1_8;
      jvm_args.nOptions           = options.count();
      jvm_args.options            = (JavaVMOption *)(&options.at(0));
      jvm_args.ignoreUnrecognized = JNI_TRUE;
      if (OB_FAIL(create_jvm_(jvm, jvm_args))) {
        error_msg_ = "failed to create JVM, please check JVM options and available memory";
        LOG_WARN("failed to create jvm directly", K(ret));
      } else {
        env_provider_ = OB_NEW(ObDirectJvmEnvProvider, "JniEnvProv", jvm);
        if (OB_ISNULL(env_provider_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate ObDirectJvmEnvProvider", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJavaVmManager::init_classes()
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
        LOG_WARN("failed to register native method for ObJavaVmManager",
                 K(ret));
      } else {
        jni_env_->DeleteGlobalRef(native_method_class);
      }
    }
  }
  return ret;
}

int ObJavaVmManager::do_init_() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_jni_env())) {
    LOG_WARN("failed to init jni env", K(ret));
  } else if (OB_FAIL(env_provider_->get_env(jni_env_))) {
    LOG_WARN("failed to get initial jni env from provider", K(ret));
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

// ------------------------- end of ObJavaVmManager -------------------------

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
    if (OB_FAIL(ObJavaVmManager::getInstance().get_env(env))) {
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
