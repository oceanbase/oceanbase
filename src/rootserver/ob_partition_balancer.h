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

#ifndef _OB_PARTITION_BALANCER_H
#define _OB_PARTITION_BALANCER_H 1

#include "ob_balance_info.h"
#include "ob_root_utils.h"
#include "rootserver/ob_unit_manager.h"

namespace oceanbase {
namespace common {
class ObServerConfig;
}
namespace share {
class ObPartitionTableOperator;
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share

namespace rootserver {
namespace balancer {
class HashIndexCollection;
}
class ObRebalanceTaskMgr;
class ObZoneManager;
// The algorithm to balance the replicas in units of one tenant.

class ObPartitionUnitProvider : public balancer::IUnitProvider {
public:
  ObPartitionUnitProvider(const ZoneUnit& zone_unit, balancer::ITenantStatFinder& stat_finder)
      : zone_unit_(zone_unit), stat_finder_(stat_finder)
  {}
  virtual ~ObPartitionUnitProvider()
  {}

public:
  virtual int find_unit(int64_t all_tg_idx, int64_t part_idx, uint64_t& unit_id) const;
  virtual int64_t count() const;
  virtual uint64_t get_unit_id(int64_t unit_idx) const;
  virtual int get_units(common::ObIArray<UnitStat*>& unit_stat) const;
  virtual int get_unit_by_id(const uint64_t unit_id, UnitStat*& unit_stat) const;
  virtual int get_avg_load(double& avg_load) const;

private:
  const ZoneUnit& zone_unit_;
  balancer::ITenantStatFinder& stat_finder_;
};

class ObPartitionBalancer {
public:
  ObPartitionBalancer();
  virtual ~ObPartitionBalancer()
  {}
  int init(common::ObServerConfig& cfg, share::schema::ObMultiVersionSchemaService& schema_service,
      share::ObPartitionTableOperator& pt_operator, ObRebalanceTaskMgr& task_mgr, ObZoneManager& zone_mgr,
      TenantBalanceStat& tenant_stat, share::ObCheckStopProvider& check_stop_provider);
  // balance partition groups of tablegroups
  // Let the replicas of the same table group but different partition groups be evenly distributed on all units as far
  // as possible, only considering the number of replicas
  int partition_balance(int64_t& task_cnt, const balancer::HashIndexCollection& hash_index_collection);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObPartitionBalancer);
  // function members
  // balance one table group's partition groups
  bool can_migrate_pg_by_rule(const PartitionGroup& pg, UnitStat* src, UnitStat* dest);
  int migrate_pg(const PartitionGroup& pg, UnitStat* src, UnitStat* dest, int64_t& task_cnt, const char* comment);
  // return OB_CANCELED if stop, else return OB_SUCCESS
  int check_stop() const
  {
    return check_stop_provider_->check_stop();
  }

private:
  // data members
  bool inited_;
  common::ObServerConfig* config_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  share::ObPartitionTableOperator* pt_operator_;
  ObRebalanceTaskMgr* task_mgr_;
  ObZoneManager* zone_mgr_;
  TenantBalanceStat* tenant_stat_;
  TenantBalanceStat* origin_tenant_stat_;
  share::ObCheckStopProvider* check_stop_provider_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_PARTITION_BALANCER_H */
