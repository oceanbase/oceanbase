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

#ifndef OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JAVA_NATIVE_METHOD_H_
#define OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JAVA_NATIVE_METHOD_H_

#include <jni.h>
#include "lib/alloc/alloc_struct.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{

namespace common
{

// Without the Java UDF then only support mem malloc and free api
struct JavaNativeMethods {
private:
  static common::ObMemAttr mem_attr_;
  static common::ObMalloc malloc_;

public:
  static jlong memory_malloc(JNIEnv *env, jclass clazz, jlong bytes);
  static void memory_free(JNIEnv *env, jclass clazz, jlong address);
};

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JAVA_NATIVE_METHOD_H_ */