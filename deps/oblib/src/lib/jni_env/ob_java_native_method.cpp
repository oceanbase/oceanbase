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

#include "ob_java_native_method.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{

namespace common
{

jlong JavaNativeMethods::memory_malloc(JNIEnv *env, jclass clazz, jlong bytes) {
  int ret = OB_SUCCESS;
  int64_t lbytes = reinterpret_cast<int64_t>(bytes);
  oceanbase::lib::ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "JniAllocator"));
  long address = reinterpret_cast<long>(malloc(lbytes));
  LOG_TRACE("allocate bytes of memory address", K(ret), K(lbytes), K(address));
  return address;
}

void JavaNativeMethods::memory_free(JNIEnv *env, jclass clazz, jlong address) {
  int ret = OB_SUCCESS;
  free(reinterpret_cast<void *>(address));
  LOG_TRACE("free memory address", K(ret), K(address));
}

} // namespace sql
} // namespace oceanbase