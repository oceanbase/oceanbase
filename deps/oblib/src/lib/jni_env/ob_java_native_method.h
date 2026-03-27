/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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