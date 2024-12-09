/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX COMMON

#include "lib/stat/ob_diagnostic_info_container.h"
#include "lib/stat/ob_diagnostic_info_util.h"
#include "lib/container/ob_vector.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "common/ob_smart_var.h"

namespace oceanbase
{
namespace common
{
void __attribute__((weak, noinline)) lib_release_tenant(void *ptr)
{
  UNUSED(ptr);
}

int64_t __attribute__((weak, noinline)) get_mtl_id()
{
  return OB_INVALID_TENANT_ID;
}
ObDiagnosticInfoContainer *__attribute__((weak, noinline)) get_di_container()
{
  return nullptr;
}

uint64_t __attribute__((weak, noinline)) lib_get_cpu_khz()
{
  return 0;
}

void __attribute__((weak, noinline)) lib_mtl_switch(int64_t tenant_id, std::function<void(int)> fn)
{
  UNUSED(tenant_id);
  fn(OB_NOT_IMPLEMENT);
}

int64_t __attribute__((weak, noinline)) lib_mtl_cpu_count()
{
  return 1;
}

} /* namespace common */
} /* namespace oceanbase */
