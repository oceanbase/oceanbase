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

#ifndef _OB_REPLICA_BALANCE_H
#define _OB_REPLICA_BALANCE_H 1
#include "ob_balance_info.h"
#include "ob_root_utils.h"
namespace oceanbase {
namespace common {
class ObServerConfig;
}
namespace share {
class ObPartitionTableOperator;
}

namespace rootserver {
namespace balancer {
class HashIndexCollection;
}
class ObRebalanceTaskMgr;
class ObZoneManager;
// The algorithm to balance the replicas in units of one tenant.
class ObUnitBalancer {
public:
  ObUnitBalancer();
  virtual ~ObUnitBalancer()
  {}
  int init(common::ObServerConfig& cfg, share::ObPartitionTableOperator& pt_operator, ObRebalanceTaskMgr& task_mgr,
      ObZoneManager& zone_mgr, TenantBalanceStat& tenant_stat, share::ObCheckStopProvider& check_stop_provider);

  // Load balance main entrance, balance according to load
  int unit_load_balance_v2(int64_t& task_cnt, const balancer::HashIndexCollection& hash_index_collection,
      const common::hash::ObHashSet<uint64_t>& processed_tids);

  // for unittest only
  void disable_random_behavior(bool disable = true)
  {
    disable_random_behavior_ = disable;
  }

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObUnitBalancer);
  // function members
  bool can_migrate_pg_by_rule(const PartitionGroup& pg, UnitStat* src, UnitStat* dest);
  bool can_migrate_pg_by_load(ZoneUnit& zu, UnitStat& max_u, UnitStat& min_u, PartitionGroup& pg);
  int migrate_pg(const PartitionGroup& pg, UnitStat* src, UnitStat* dest, int64_t& task_cnt, const char* comment);
  // return OB_CANCELED if stop, else return OB_SUCCESS
  int check_stop() const
  {
    return check_stop_provider_->check_stop();
  }
  // debug
  int print_pg_distribution();

private:
  // data members
  bool inited_;
  bool disable_random_behavior_;
  // Prohibit disturbing tg shuffle to make unit test output stable
  common::ObServerConfig* config_;
  share::ObPartitionTableOperator* pt_operator_;
  ObRebalanceTaskMgr* task_mgr_;
  ObZoneManager* zone_mgr_;
  TenantBalanceStat* tenant_stat_;
  TenantBalanceStat* origin_tenant_stat_;
  share::ObCheckStopProvider* check_stop_provider_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_REPLICA_BALANCE_H */
