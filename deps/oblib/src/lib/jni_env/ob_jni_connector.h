/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  static int is_valid_loaded_jars_();
  static int check_jni_exception_(JNIEnv *env);
  static const char* JAR_VERSION_CLASS;
};
}
}
#endif