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


namespace oceanbase {
namespace common{
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
    }
  }

  if (OB_FAIL(ret)) {
    /* do nothing */
  } else if (!java_env.is_env_inited()) {
    // Recheck the java env whether is ready.
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("java env is not ready for scanner", K(ret));
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
    if (!helper.is_inited()) {
      // Means helper is not ready.
      ret = OB_JNI_ENV_ERROR;
      LOG_WARN("failed to get jni env", K(ret));
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


}
}
