/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JAVA_VM_MANAGER_H_
#define OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JAVA_VM_MANAGER_H_

#include <utility>
#include <string>
#include <vector>

#include <jni.h>
#include <hdfs/hdfs.h>
#include <hdfs/hdfs.h>

#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "common/ob_common_utility.h"
#include "ob_java_native_method.h"
#include "lib/lock/ob_mutex.h"
#include "lib/utility/ob_defer.h"

// implements by libhdfs
// hadoop-hdfs-native-client/src/main/native/libhdfs/jni_helper.c
// Why do we need to use this function?
// 1. a thread can not attach to more than one virtual machine
//    this means the second call to getJNIEnv will return same result as the first call
//    if we do not destory this jni env, all thread will get same env
// 2. libhdfs depends on this function and does some initialization,
// if the JVM has already created it, it won't create it anymore.
// If we skip this function call will cause libhdfs to miss some initialization operations
// extern "C" JNIEnv* getJNIEnv(void);

// Copy from `close_modules/dblink/deps/oblib/src/lib/oracleclient/ob_dlsym_loader.h`
#if defined(_AIX)
  #define  LIB_OPEN_FLAGS        (RTLD_NOW | RTLD_GLOBAL | RTLD_MEMBER)
#elif defined(__hpux)
  #define  LIB_OPEN_FLAGS        (BIND_DEFERRED |BIND_VERBOSE| DYNAMIC_PATH)
#elif defined(__GNUC__)
  #define  LIB_OPEN_FLAGS        (RTLD_NOW | RTLD_GLOBAL)
#endif

#if defined(_WINDOWS)

  #include <Windows.h>

  #define LIB_HANDLE               HMODULE
  #define LIB_OPEN(l)              LoadLibraryA(l)
  #define LIB_CLOSE                FreeLibrary
  #define LIB_SYMBOL(h, s, p, t)   p = (t) GetProcAddress(h, s)

#elif defined(__hpux)

  #include <dl.h>

  #define LIB_HANDLE               shl_t
  #define LIB_OPEN(l)              shl_load(l, LIB_OPEN_FLAGS, 0L)
  #define LIB_CLOSE                shl_unload
  #define LIB_SYMBOL(h, s, p, t)   shl_findsym(&h, s, (short) TYPE_PROCEDURE, (void *) &p)

#elif defined(__GNUC__)

  #include <dlfcn.h>

  #define LIB_HANDLE               void *
  #define LIB_OPEN(l)              dlopen(l, LIB_OPEN_FLAGS)
  #define LIB_CLOSE                dlclose
  #define LIB_SYMBOL(h, s, p, t)   p = (t) dlsym(h, s)

#else

  #error Unable to compute how to dynamic libraries

#endif

/**
 * Length of buffer for retrieving created JVMs.  (We only ever create one.)
 */
#define VM_BUF_LENGTH 1

// define function
typedef JNIEnv* (*GETJNIENV)(void);

// hdfs related symbols
typedef hdfsFileInfo* (*HdfsGetPathInfoFunc)(hdfsFS, const char*);
typedef void (*HdfsFreeFileInfoFunc)(hdfsFileInfo *, int);
typedef int (*HdfsDeleteFunc)(hdfsFS, const char*, int);
typedef char* (*HdfsGetLastExceptionRootCauseFunc)();
typedef int (*HdfsCreateDirectoryFunc)(hdfsFS, const char*);
typedef hdfsFileInfo* (*HdfsListDirectoryFunc)(hdfsFS, const char*, int*);
typedef int (*HdfsCloseFileFunc)(hdfsFS, hdfsFile);
typedef hdfsFile (*HdfsOpenFileFunc)(hdfsFS, const char*, int, int, short, tSize);
typedef int (*HdfsFileIsOpenForReadFunc)(hdfsFile);
typedef int (*HdfsFileIsOpenForWriteFunc)(hdfsFile);
typedef tSize (*HdfsPreadFunc)(hdfsFS, hdfsFile, tOffset, void*, tSize);
typedef tSize (*HdfsWriteFunc)(hdfsFS, hdfsFile, void*, tSize);
typedef tSize (*HdfsFlushFunc)(hdfsFS, hdfsFile);
typedef struct hdfsBuilder* (*HdfsNewBuilderFunc)(void);
typedef void (*HdfsBuilderSetNameNodeFunc)(struct hdfsBuilder *, const char *);
typedef void (*HdfsBuilderSetUserNameFunc)(struct hdfsBuilder *, const char *);
typedef void (*HdfsBuilderSetForceNewInstanceFunc)(struct hdfsBuilder *);
typedef hdfsFS (*HdfsBuilderConnectFunc)(struct hdfsBuilder *);
typedef void (*HdfsFreeBuilderFunc)(struct hdfsBuilder *);
typedef int (*HdfsDisconnectFunc)(hdfsFS);

// extra added for kerberos auth
typedef int (*HdfsBuilderConfSetStrFunc)(struct hdfsBuilder *, const char *, const char *);
typedef void (*HdfsBuilderSetPrincipalFunc)(struct hdfsBuilder *, const char *);
typedef void (*HdfsBuilderSetKerb5ConfFunc)(struct hdfsBuilder *, const char *);
typedef void (*HdfsBuilderSetKeyTabFileFunc)(struct hdfsBuilder *, const char *);
typedef void (*HdfsBuilderSetKerbTicketCachePathFunc)(struct hdfsBuilder *, const char *);

// desclare function
extern "C" GETJNIENV getJNIEnv;

// declare hdfs functions
extern "C" HdfsGetPathInfoFunc obHdfsGetPathInfo;
extern "C" HdfsFreeFileInfoFunc obHdfsFreeFileInfo;
extern "C" HdfsDeleteFunc obHdfsDelete;
extern "C" HdfsGetLastExceptionRootCauseFunc obHdfsGetLastExceptionRootCause;
extern "C" HdfsCreateDirectoryFunc obHdfsCreateDirectory;
extern "C" HdfsListDirectoryFunc obHdfsListDirectory;
extern "C" HdfsCloseFileFunc obHdfsCloseFile;
extern "C" HdfsOpenFileFunc obHdfsOpenFile;
extern "C" HdfsFileIsOpenForReadFunc obHdfsFileIsOpenForRead;
extern "C" HdfsFileIsOpenForWriteFunc obHdfsFileIsOpenForWrite;
extern "C" HdfsPreadFunc obHdfsPread;
extern "C" HdfsWriteFunc obHdfsWrite;
extern "C" HdfsFlushFunc obHdfsFlush;
extern "C" HdfsNewBuilderFunc obHdfsNewBuilder;
extern "C" HdfsBuilderSetNameNodeFunc obHdfsBuilderSetNameNode;
extern "C" HdfsBuilderSetUserNameFunc obHdfsBuilderSetUserName;
extern "C" HdfsBuilderSetForceNewInstanceFunc obHdfsBuilderSetForceNewInstance;
extern "C" HdfsBuilderConnectFunc obHdfsBuilderConnect;
extern "C" HdfsFreeBuilderFunc obHdfsFreeBuilder;
extern "C" HdfsDisconnectFunc obHdfsDisconnect;

// extra added for kerberos auth
extern "C" HdfsBuilderConfSetStrFunc obHdfsBuilderConfSetStr;
extern "C" HdfsBuilderSetPrincipalFunc obHdfsBuilderSetPrincipal;
extern "C" HdfsBuilderSetKerb5ConfFunc obHdfsBuilderSetKerb5Conf;
extern "C" HdfsBuilderSetKeyTabFileFunc obHdfsBuilderSetKeyTabFile;
extern "C" HdfsBuilderSetKerbTicketCachePathFunc obHdfsBuilderSetKerbTicketCachePath;

namespace oceanbase
{

namespace common
{

class JVMClass;

typedef lib::ObLockGuard<lib::ObMutex> LockGuard;


// Restrictions on use:
// can only be used in pthread, not in bthread

// Two strategies for obtaining JNIEnv:
//   - ObHdfsJniEnvProvider:    libhdfs.so present, delegates to getJNIEnv()
//   - ObDirectJvmEnvProvider:  no libhdfs.so, manages JVM via JNI_CreateJavaVM
class ObJniEnvProvider {
public:
  virtual ~ObJniEnvProvider() = default;
  virtual int get_env(JNIEnv *&env) = 0;
};

class ObHdfsJniEnvProvider final : public ObJniEnvProvider {
public:
  ObHdfsJniEnvProvider();
  void init(GETJNIENV fn);
  int get_env(JNIEnv *&env) override;
  // HDFS manages thread lifecycle internally; no detach needed from our side.
private:
  GETJNIENV get_env_fn_;
};

class ObDirectJvmEnvProvider final : public ObJniEnvProvider {
public:
  ObDirectJvmEnvProvider();
  int init(JavaVM *jvm);
  int get_env(JNIEnv *&env) override;
private:
  JavaVM *jvm_;
};

// thread local helper
class ObJavaVmManager {
public:
  static ObJavaVmManager &getInstance();
  ObJavaVmManager(const ObJavaVmManager &) = delete;
  ObJavaVmManager& operator=(const ObJavaVmManager&) = delete;

  int get_env(JNIEnv *&env);

  const char* get_error_msg() { return error_msg_; }

private:
  ObJavaVmManager();

  int do_init_();
  int init_jni_env();
  int check_valid_env();
  int init_classes();
  int jni_find_class(const char *clazz, jclass *gen_clazz);
  int open_lib_(const char *lib_name, const ObSqlString &search_paths, void *&lib_handle);

  static bool search_dir_file(const char *dir, const char *file_name);
  static int build_lib_search_paths_(ObSqlString &paths);
  int get_lib_path(const char *lib_name, const ObSqlString &search_paths, ObSqlString &path);

  int open_java_lib();
  int open_hdfs_lib();

  int create_jvm_(JavaVM *&jvm, JavaVMInitArgs &jvm_args);
private:
  static thread_local JNIEnv *jni_env_;

  // Handle the runtime shared library
  void* jvm_lib_handle_ = nullptr;
  void* hdfs_lib_handle_ = nullptr;

  // Storage for the two possible providers; env_provider_ points to whichever is active.
  ObHdfsJniEnvProvider   hdfs_provider_;
  ObDirectJvmEnvProvider direct_provider_;
  ObJniEnvProvider      *env_provider_ = nullptr;

private:
  lib::ObMutex init_jni_env_lock_;
  const char* error_msg_ = nullptr;
};

#define LOCAL_REF_GUARD_ENV(lref, env)                                         \
  DEFER(                                                                       \
    if (OB_NOT_NULL(lref) && OB_NOT_NULL(env)) {                               \
       env->DeleteLocalRef(lref);                                              \
       lref = nullptr;                                                         \
    }                                                                          \
  )

// A global ref of the guard, handle can be shared across threads
class JavaGlobalRef {
public:
  /// @note handle is a java global ref
  explicit JavaGlobalRef(jobject handle = nullptr) : handle_(handle) {}
  ~JavaGlobalRef();
  JavaGlobalRef(const JavaGlobalRef &) = delete;

  JavaGlobalRef(JavaGlobalRef &&other) noexcept {
    handle_ = other.handle_;
    other.handle_ = nullptr;
  }

  JavaGlobalRef &operator=(JavaGlobalRef &&other) noexcept {
    JavaGlobalRef tmp(std::move(other));
    std::swap(this->handle_, tmp.handle_);
    return *this;
  }

  /// @param handle a local ref, not global
  int new_global_ref(jobject handle, JNIEnv *env);

  jobject handle() const { return handle_; }

  jobject &handle() { return handle_; }

  void clear();

private:
  jobject handle_;
};

// A Class object created from the ClassLoader that can be accessed by multiple threads
class JVMClass
{
public:
  JVMClass(jobject clazz) : clazz_(clazz) {}
  JVMClass(const JVMClass &) = delete;

  JVMClass &operator=(const JVMClass &&) = delete;
  JVMClass &operator=(const JVMClass &other) = delete;

  JVMClass(JVMClass &&other) noexcept : clazz_(nullptr) {
    clazz_ = std::move(other.clazz_);
  }

  JVMClass &operator=(JVMClass &&other) noexcept {
    JVMClass tmp(std::move(other));
    std::swap(this->clazz_, tmp.clazz_);
    return *this;
  }

  jclass clazz() const { return (jclass)clazz_.handle(); }

private:
    JavaGlobalRef clazz_;
};

// Used to get function signatures
class ClassAnalyzer {
public:
  ClassAnalyzer() = default;
  ~ClassAnalyzer() = default;
  int has_method(jclass clazz, const std::string &method, bool *has);
  int get_signature(jclass clazz, const std::string &method, std::string *sign);
  int get_method_object(jclass clazz, const std::string &method_name,
                        jobject &jobj);
};



int check_jvm_stack_size();

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JAVA_VM_MANAGER_H_ */
