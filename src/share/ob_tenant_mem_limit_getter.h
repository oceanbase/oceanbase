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

#ifndef OCEABASE_SHARE_OB_TENANT_MEM_LIMIT_GETTER_H_
#define OCEABASE_SHARE_OB_TENANT_MEM_LIMIT_GETTER_H_
#include "share/ob_i_tenant_mem_limit_getter.h"

namespace oceanbase
{
namespace common
{

// just used by KVGlobalCache
class ObTenantMemLimitGetter : public ObITenantMemLimitGetter
{
public:
  static ObTenantMemLimitGetter &get_instance();
  // check if the tenant exist.
  // virtual tenant will check ObVirtualTenantManager.
  // mtl tenant will check ObMultiTenant.
  // @param[in] tenant_id, which tenant is need check.
  bool has_tenant(const uint64_t tenant_id) const override;
  // get all the tenant id, include both virtual tenant and mtl tenant.
  // get virtual tenant id from ObVirtualTenantManager.
  // get mtl tenant id from ObMultiTenant.
  // @param[out] tenant_ids, all the tenant id.
  int get_all_tenant_id(ObIArray<uint64_t> &tenant_ids) const override;
  // get the min/max memory limit of a tenant.
  // virtual tenant's result comes from ObVirtualTenantManager.
  // mtl tenant's result comes from ObTenantFreezer.
  // @param[in] tenant_id, which tenant's memory limit need get.
  // @param[out] lower_limit, the min tenant memory limit.
  // @param[out] upper_limit, the max tenant memory limit.
  int get_tenant_mem_limit(const uint64_t tenant_id,
                           int64_t &lower_limit,
                           int64_t &upper_limit) const override;
private:
  ObTenantMemLimitGetter() {}
  virtual ~ObTenantMemLimitGetter() {}
};


}
}
#endif
