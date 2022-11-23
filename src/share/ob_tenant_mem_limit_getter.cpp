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

#include "share/ob_tenant_mem_limit_getter.h"

#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_tenant_mgr.h"
#include "storage/tx_storage/ob_tenant_freezer.h"

namespace oceanbase
{
using namespace share;
namespace common
{

ObTenantMemLimitGetter &ObTenantMemLimitGetter::get_instance()
{
  static ObTenantMemLimitGetter instance_;
  return instance_;
}

bool ObTenantMemLimitGetter::has_tenant(const uint64_t tenant_id) const
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  omt::ObMultiTenant *omt = GCTX.omt_;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (is_virtual_tenant_id(tenant_id)) {
    bool_ret = ObVirtualTenantManager::get_instance().has_tenant(tenant_id);
  } else {
    if (OB_ISNULL(omt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("omt should not be null", K(ret), K(tenant_id));
    } else if (omt->has_tenant(tenant_id)) {
      bool_ret = true;
    } else {
      // do nothing
    }
  }
  return bool_ret;
}

int ObTenantMemLimitGetter::get_all_tenant_id(ObIArray<uint64_t> &tenant_ids) const
{
  int ret = OB_SUCCESS;
  omt::ObMultiTenant *omt = GCTX.omt_;
  // KVCache's tenant_id_ array cannot be reset or reuse.
  // tenant_ids.reset();
  // get virtual tenant id.
  if (OB_FAIL(ObVirtualTenantManager::get_instance().get_all_tenant_id(tenant_ids))) {
    LOG_WARN("get all virtual tenant id failed", K(ret));
  } else if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("omt should not be null", K(ret));
    // get mtl tenant id.
  } else if (OB_FAIL(omt->get_mtl_tenant_ids(tenant_ids))) {
    LOG_WARN("get no virtual tenant ids failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObTenantMemLimitGetter::get_tenant_mem_limit(
    const uint64_t tenant_id,
    int64_t &lower_limit,
    int64_t &upper_limit) const
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (is_virtual_tenant_id(tenant_id)) {
    if (OB_FAIL(ObVirtualTenantManager::get_instance().get_tenant_mem_limit(tenant_id,
                                                                            lower_limit,
                                                                            upper_limit))) {
      LOG_WARN("get virtual tenant mem limit failed.", K(ret), K(tenant_id));
    }
  } else {
    MTL_SWITCH(tenant_id) {
      storage::ObTenantFreezer *freezer = nullptr;
      freezer = MTL(storage::ObTenantFreezer *);
      if (OB_FAIL(freezer->get_tenant_mem_limit(lower_limit,
                                                upper_limit))) {
        LOG_WARN("get tenant mem limit failed.", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

}
}
