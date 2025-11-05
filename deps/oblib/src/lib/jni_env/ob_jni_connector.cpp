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

#include "ob_jni_connector.h"
#include "ob_java_env.h"
#include "lib/jni_env/ob_java_helper.h"
#include "src/share/config/ob_server_config.h"
#include "lib/oblog/ob_log_module.h"
#include "src/observer/ob_service.h"
#include "lib/jni_env/ob_jar_version_def.h"
#include "share/ob_version.h"


namespace oceanbase {
namespace common{
/**
 * @brief 初始化java环境
 *  现在hdfs，odps和udf都依赖于jni connector jar包
 *  统一同这个接口获取java环境
 * @return int
 */
int ObJniConnector::java_env_init() {
  int ret = OB_SUCCESS;
  ObJavaEnv &java_env = ObJavaEnv::getInstance();
  // This entry is first time to setup java env
  if (!GCONF.ob_enable_java_env) {
    ret = OB_JNI_NOT_ENABLE_JAVA_ENV_ERROR;
    LOG_WARN("java env is not enabled", K(ret));
  } else if (!java_env.is_env_inited()) {
    if (OB_FAIL(java_env.setup_java_env())) {
      LOG_WARN("failed to setup java env", K(ret));
    } else if (OB_FAIL(java_env.setup_java_env_for_hdfs())) {
      LOG_WARN("failed to setup java env for hdfs", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    /* do nothing */
  } else if (!java_env.is_env_inited()) {
    // Recheck the java env whether is ready.
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("java env is not ready for scanner", K(ret));
  } else if (OB_FAIL(java_env.check_version_valid())) {
    if (OB_FAIL(ObJniConnector::is_valid_loaded_jars_())) {
      if (OB_JNI_ENV_SETUP_ERROR != ret) {
        java_env.set_version_valid(ObJavaEnv::VersionValid::NOT_VALID);
      }
      LOG_WARN("failed to check valid loaded jars", K(ret));
    } else {
      // fast path for not check version valid again
      java_env.set_version_valid(ObJavaEnv::VersionValid::VALID);
    }
  }
  return ret;
}

// udf odps
const char* ObJniConnector::JAR_VERSION_CLASS = "com/oceanbase/utils/version/JarVersion";

int ObJniConnector::is_valid_loaded_jars_() {
  int ret = OB_SUCCESS;
  bool is_valid = false;
  uint64_t real_jar_version = 0;
  if (OB_SUCC(ret)) {
    JNIEnv *jni_env = nullptr;
    if (OB_FAIL(get_jni_env(jni_env))) {
      LOG_WARN("failed to get jni env", K(ret));
    } else if (OB_ISNULL(jni_env)) {
      ret = OB_JNI_ENV_ERROR;
      LOG_WARN("failed to get null jni env", K(ret));
    } else {
      jclass cls = jni_env->FindClass(JAR_VERSION_CLASS);
      if (OB_FAIL(check_jni_exception_(jni_env))) {
        LOG_WARN("failed to find jar version class", K(ret));
      } else if (OB_ISNULL(cls)) {
        ret = OB_JNI_CLASS_NOT_FOUND_ERROR;
        LOG_WARN("unable to find version class", K(ret));
      } else {
        jmethodID call_version_id =
            jni_env->GetStaticMethodID(cls, "getJarVersion", "()J");
        if (OB_FAIL(check_jni_exception_(jni_env))) {
          LOG_WARN("failed to find call version method", K(ret));
        } else if (OB_ISNULL(call_version_id)) {
          ret = OB_JNI_METHOD_NOT_FOUND_ERROR;
          LOG_WARN("unable to find call version method", K(ret));
        } else {
          jlong jar_version = jni_env->CallStaticLongMethod(cls, call_version_id);
          if (OB_FAIL(check_jni_exception_(jni_env))) {
            LOG_WARN("failed to find get version", K(ret));
          } else {
            real_jar_version = static_cast<uint64_t>(jar_version);
          }
        }
        jni_env->DeleteLocalRef(cls);
      }
    }
  }

  share::ObServerInfoInTable::ObBuildVersion build_version;
  // Note!!!
  // Current jar version should be matched to jar version
  // which is in "tools/upgrade/deps_compat.yml".
  if (OB_FAIL(ret)) {
    /* do nothing */
  } else if (OB_FAIL(observer::ObService::get_build_version(build_version))) {
    LOG_WARN("fail to get build version", KR(ret));
  } else {
    LOG_INFO("build_version", K(build_version));
  }
  int64_t cur_version = 0;
  if (OB_SUCC(ret)) {
    ObString build_version_str = build_version.str();
    ObString version_str = build_version_str.split_on('_');
    ObString major_version_str = version_str.split_on('.');
    ObString minor_version_str = version_str.split_on('.');
    ObString major_patch_version_str = version_str.split_on('.');
    ObString minor_patch_version_str = version_str;
    int major_version = 0;
    int minor_version = 0;
    int major_patch_version = 0;
    int minor_patch_version = 0;
    OX(major_version = ObCharset::strntoll(major_version_str.ptr(), major_version_str.length(), 10, &ret));
    OX(minor_version = ObCharset::strntoll(minor_version_str.ptr(), minor_version_str.length(), 10, &ret));
    OX(major_patch_version = ObCharset::strntoll(major_patch_version_str.ptr(), major_patch_version_str.length(), 10, &ret));
    OX(minor_patch_version = ObCharset::strntoll(minor_patch_version_str.ptr(), minor_patch_version_str.length(), 10, &ret));
    OX(cur_version = cal_version(major_version, minor_version, major_patch_version, minor_patch_version));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_LIKELY(cur_version >= CLUSTER_VERSION_4_4_1_0)) {
    if (OB_LIKELY(JAR_VERSION_102 <= real_jar_version)) {
      is_valid = true;
    } else {
      LOG_WARN("current major jar version is not 1.0.2", K(ret), K(real_jar_version), K(JAR_VERSION_102));
    }
  } else if (OB_LIKELY(cur_version >= CLUSTER_VERSION_4_4_0_0 || cur_version >= MOCK_CLUSTER_VERSION_4_3_5_3)) {
    if (OB_LIKELY(JAR_VERSION_101 <= real_jar_version && real_jar_version < JAR_VERSION_102)) {
      is_valid = true;
    } else {
      LOG_WARN("current major jar version is not 1.0.1", K(ret), K(real_jar_version), K(JAR_VERSION_101));
    }
  } else if (OB_LIKELY(cur_version >= CLUSTER_VERSION_4_3_5_1)) {
    if (OB_LIKELY(JAR_VERSION_100 <= real_jar_version && real_jar_version < JAR_VERSION_101)) {
      is_valid = true;
    } else {
      LOG_WARN("current major jar version is not 1.0.0", K(ret), K(real_jar_version), K(JAR_VERSION_100));
    }
  }
  LOG_TRACE("check jar version in detail", K(ret), K(real_jar_version),
            K(build_version), K(is_valid));
  if (OB_SUCC(ret)) {
    if (is_valid) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_VERSION_NOT_MATCH;
    }
  }
  return ret;
}

int ObJniConnector::get_jni_env(JNIEnv *&env) {
  int ret = OB_SUCCESS;
  // This entry method is the first time to init jni env
  if (!ObJavaEnv::getInstance().is_env_inited()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("failed to init env variables", K(ret));
  } else {
    JVMFunctionHelper &helper = JVMFunctionHelper::getInstance();
    if (OB_FAIL(helper.init_result())) {
      // Means helper is not ready.
      LOG_ERROR("failed to init jni env", K(ret), K(helper.get_error_msg()), K(ObJavaEnv::getInstance().jh_), K(ObJavaEnv::getInstance().ld_));
      if (helper.get_error_msg() != nullptr) {
        LOG_USER_ERROR(OB_JNI_ENV_SETUP_ERROR, STRLEN(helper.get_error_msg()), helper.get_error_msg());
      } else {
        LOG_USER_ERROR(OB_JNI_ENV_SETUP_ERROR, "failed to init jni env please check JAVA_HOME and JAVA_OPTS and LD_LIBRARY_PATH");
      }
    } else if (OB_FAIL(helper.get_env(env))) {
      LOG_WARN("failed to get jni env from helper", K(ret));
    } else if (nullptr == env) {
      ret = OB_JNI_ENV_ERROR;
      LOG_WARN("failed to get null jni env from helper", K(ret));
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObJniConnector::check_jni_exception_(JNIEnv *env) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(env)) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("failed to get jni env", K(ret), K(lbt()));
  } else {
    jthrowable thr = env->ExceptionOccurred();
    if (OB_NOT_NULL(thr)) {
      ret = OB_JNI_JAVA_EXCEPTION_ERROR;
      jclass throwableClass = env->GetObjectClass(thr);

      jmethodID getMessageMethod = env->GetMethodID(
          throwableClass,
          "getMessage",
          "()Ljava/lang/String;"
      );

      if (OB_NOT_NULL(getMessageMethod)) {
          jstring jmsg = (jstring)env->CallObjectMethod(thr, getMessageMethod);
          if (env->ExceptionCheck()) {
              env->ExceptionClear(); // 防止调用过程中产生新异常
          }

          if (OB_NOT_NULL(jmsg)) {
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


}
}
