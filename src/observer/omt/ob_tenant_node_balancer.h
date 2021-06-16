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

#ifndef _OCEABASE_OBSERVER_OMT_OB_TENANT_NODE_BALANCER_H_
#define _OCEABASE_OBSERVER_OMT_OB_TENANT_NODE_BALANCER_H_

#include "lib/container/ob_vector.h"
#include "lib/net/ob_addr.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "share/ob_unit_getter.h"
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
class ObServerConfig;
}  // namespace common
namespace omt {
class ObMultiTenant;
// monitor tenant units and create/delete/modify local OMT.
class ObTenantNodeBalancer : public share::ObThreadPool {
public:
  struct ServerResource {
  public:
    ServerResource() : max_cpu_(0), min_cpu_(0), max_memory_(0), min_memory_(0)
    {}
    ~ServerResource()
    {}
    void reset()
    {
      max_cpu_ = 0;
      min_cpu_ = 0;
      max_memory_ = 0;
      min_memory_ = 0;
    }
    double max_cpu_;
    double min_cpu_;
    int64_t max_memory_;
    int64_t min_memory_;
  };

public:
  static OB_INLINE ObTenantNodeBalancer& get_instance();

  int init(ObMultiTenant* omt, common::ObMySQLProxy& sql_proxy, const common::ObAddr& myaddr);

  int update_tenant(share::TenantUnits& units, const bool is_local);

  int notify_create_tenant(const obrpc::TenantServerUnitConfig& unit);

  int try_notify_drop_tenant(const int64_t tenant_id);

  int get_server_allocated_resource(ServerResource& server_resource);

  bool is_tenant_exist(const int64_t tenant_id);

  int lock_tenant_balancer();

  int unlock_tenant_balancer();

  int update_tenant_memory(const obrpc::ObTenantMemoryArg& tenant_memory);

  virtual void run1();

private:
  static const int64_t RECYCLE_LATENCY = 1000L * 1000L * 180L;
  ObTenantNodeBalancer();
  ~ObTenantNodeBalancer();

  int check_new_tenant(share::TenantUnits& units);
  int check_new_tenant(const share::ObUnitInfoGetter::ObTenantConfig& config);
  int check_del_tenant(share::TenantUnits& units);
  void periodically_check_tenant();
  int fetch_effective_tenants(const share::TenantUnits& old_tenants, share::TenantUnits& new_tenants);
  int refresh_tenant(share::TenantUnits& units);
  int create_new_tenant(share::ObUnitInfoGetter::ObTenantConfig& unit);
  DISALLOW_COPY_AND_ASSIGN(ObTenantNodeBalancer);

private:
  ObMultiTenant* omt_;
  common::ObAddr myaddr_;
  share::ObUnitInfoGetter unit_getter_;
  mutable common::TCRWLock lock_;
  int64_t refresh_interval_;
};  // end of class ObTenantNodeBalancer

OB_INLINE ObTenantNodeBalancer& ObTenantNodeBalancer::get_instance()
{
  static ObTenantNodeBalancer instance;
  return instance;
}

}  // end of namespace omt
}  // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OMT_OB_TENANT_NODE_BALANCER_H_ */
