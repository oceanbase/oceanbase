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

#define USING_LOG_PREFIX COMMON
#include "ob_errsim_module_interface.h"

#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include "lib/ob_define.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace common {

int __attribute__((weak)) build_tenant_errsim_moulde(
    const uint64_t tenant_id,
    const int64_t config_version,
    const common::ObArray<ObFixedLengthString<ObErrsimModuleTypeHelper::MAX_TYPE_NAME_LENGTH>> &module_array,
    const int64_t percentage)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  UNUSED(config_version);
  UNUSED(module_array);
  UNUSED(percentage);
  return ret;
}

bool __attribute__((weak)) is_errsim_module(
    const uint64_t tenant_id,
    const ObErrsimModuleType::TYPE &type)
{
  bool b_ret = false;
  UNUSED(tenant_id);
  UNUSED(type);
  return b_ret;
}

int __attribute__((weak)) add_tenant_errsim_event(
    const uint64_t tenant_id,
    const ObTenantErrsimEvent &event)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  UNUSED(event);
  return ret;
}

} // common
} // oceanbase
