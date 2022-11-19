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

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_simple_mem_limit_getter.h"

namespace oceanbase
{
namespace common
{
int ObSimpleMemLimitGetter::add_tenant(
    const uint64_t tenant_id,
    const int64_t lower_limit,
    const int64_t upper_limit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) ||
      OB_UNLIKELY(lower_limit < 0) ||
      OB_UNLIKELY(upper_limit < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id),
             K(lower_limit), K(upper_limit));
  } else {
    ObTenantInfo tenant_info(tenant_id,
                             lower_limit,
                             upper_limit);
    SpinWLockGuard guard(lock_);
    if (has_tenant_(tenant_id)) {
      // tenant is exist do nothing
    } else if (OB_FAIL(tenant_infos_.push_back(tenant_info))) {
      LOG_ERROR("push back into tenant_infos failed", K(ret), K(tenant_id));
    }
  }
  return ret;
}

bool ObSimpleMemLimitGetter::has_tenant(const uint64_t tenant_id) const
{
  bool found = false;
  SpinRLockGuard guard(lock_);
  found = has_tenant_(tenant_id);
  return found;
}

bool ObSimpleMemLimitGetter::has_tenant_(const uint64_t tenant_id) const
{
  bool found = false;
  for (int i = 0; i < tenant_infos_.count(); i++) {
    const ObTenantInfo &tenant_info = tenant_infos_[i];
    if (tenant_info.tenant_id_ == tenant_id) {
      found = true;
      break;
    }
  }
  return found;
}

int ObSimpleMemLimitGetter::get_all_tenant_id(ObIArray<uint64_t> &tenant_ids) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  for (int i = 0; OB_SUCC(ret) && i < tenant_infos_.count(); i++) {
    const ObTenantInfo &tenant_info = tenant_infos_[i];
    if (OB_FAIL(tenant_ids.push_back(tenant_info.tenant_id_))) {
      LOG_ERROR("push back tenant id failed", K(ret));
    }
  }
  return ret;
}

int ObSimpleMemLimitGetter::get_tenant_mem_limit(
    const uint64_t tenant_id,
    int64_t &lower_limit,
    int64_t &upper_limit) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  SpinRLockGuard guard(lock_);
  for (int i = 0; i < tenant_infos_.count(); i++) {
    const ObTenantInfo &tenant_info = tenant_infos_[i];
    if (tenant_info.tenant_id_ == tenant_id) {
      found = true;
      lower_limit = tenant_info.mem_lower_limit_;
      upper_limit = tenant_info.mem_upper_limit_;
      break;
    }
  }
  if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("tenant is not exist", K(ret), K(tenant_id));
  }
  return ret;
}

void ObSimpleMemLimitGetter::reset()
{
  tenant_infos_.reset();
}

}
}
