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

#ifndef OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JNI_CONNECTOR_H_
#define OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JNI_CONNECTOR_H_

#include <jni.h>

namespace oceanbase {
namespace common {
class ObSqlString;
}

namespace sql {
class ObJniConnector {
public:
  static bool is_java_env_inited();
  static int get_jni_env(JNIEnv *&env);
  static int check_jni_exception_(JNIEnv *env);
  int detach_jni_env();
  int inc_env_ref();
  int dec_env_ref();
};

}
}


#endif