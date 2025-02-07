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
#include "sql/engine/connector/ob_java_env.h"
#include "sql/engine/connector/ob_java_helper.h"
#include "sql/engine/connector/ob_jni_connector.h"

namespace oceanbase {

namespace sql {


int ObJniConnector::check_jni_exception_(JNIEnv *env) {
  int ret = OB_SUCCESS;
  jthrowable thr = env->ExceptionOccurred();
  if (!OB_ISNULL(thr)) {
    ret = OB_JNI_JAVA_EXCEPTION_ERROR;
    LOG_WARN("java occur exception", K(ret));
    env->ExceptionDescribe();
    env->ExceptionClear();
    env->DeleteLocalRef(thr);
  }
  return ret;
}


bool ObJniConnector::is_java_env_inited()
{
  return ObJavaEnv::getInstance().is_env_inited();
}

int ObJniConnector::get_jni_env(JNIEnv *&env) {
  int ret = OB_SUCCESS;
  // This entry method is the first time to init jni env
  if (!is_java_env_inited()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("failed to init env variables", K(ret));
  } else {
    JVMFunctionHelper &helper = JVMFunctionHelper::getInstance();
    if (OB_FAIL(helper.get_env(env))) {
      LOG_WARN("failed to get jni env from helper", K(ret));
    } else if (nullptr == env) {
      ret = OB_JNI_ENV_ERROR;
      LOG_WARN("failed to get null jni env from helper", K(ret));
    } else { /* do nothing */
    }
    if (OB_SUCC(ret) && !helper.is_inited()) {
      // Means helper is not ready.
      ret = OB_JNI_ENV_ERROR;
      LOG_WARN("failed to get jni env", K(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(helper.get_env(env))) {
      // re-get the env
      LOG_WARN("failed to re-get jni env", K(ret));
    }
  }
  return ret;
}

int ObJniConnector::inc_env_ref()
{
  int ret = OB_SUCCESS;
  JVMFunctionHelper &helper = JVMFunctionHelper::getInstance();
  if (OB_FAIL(helper.inc_ref())) {
    LOG_WARN("failed to inc ref", K(ret));
  }
  int cur_ref = helper.cur_ref();
  LOG_TRACE("current env reference time in increase method", K(ret), K(cur_ref), K(lbt()));
  return ret;
}

int ObJniConnector::dec_env_ref()
{
  int ret = OB_SUCCESS;
  JVMFunctionHelper &helper = JVMFunctionHelper::getInstance();
  if (OB_FAIL(helper.dec_ref())) {
    LOG_WARN("failed to decrease ref", K(ret));
  }
  int cur_ref = helper.cur_ref();
  LOG_TRACE("current env reference time in decrease method", K(ret), K(cur_ref), K(lbt()));
  return ret;
}

int ObJniConnector::detach_jni_env()
{
  int ret = OB_SUCCESS;
  // If using JVMFunctionHelper once, then jvm will allocate a thread.
  // Only detatch it and libhdfs.so will handle all scenes.
  JVMFunctionHelper &helper = JVMFunctionHelper::getInstance();
  if (OB_FAIL(helper.detach_current_thread())) {
    LOG_WARN("failed to detach jni env", K(ret));
  }
  return ret;
}

}
}
