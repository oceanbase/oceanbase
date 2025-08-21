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

#ifndef OBLIB_SRC_LIB_JNI_ENV_OB_JNI_CONNECTOR_H_
#define OBLIB_SRC_LIB_JNI_ENV_OB_JNI_CONNECTOR_H_

#include <jni.h>

namespace  oceanbase {
namespace common {
class ObJniConnector {
public:
  static int java_env_init();
  static int get_jni_env(JNIEnv *&env);
};
}
}
#endif