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

#ifndef _OB_REREPLICATION_H
#define _OB_REREPLICATION_H 1
#include "share/schema/ob_schema_struct.h"
#include "ob_balance_info.h"
#include "ob_root_utils.h"
namespace oceanbase {

namespace share {
class ObPartitionTableOperator;
namespace schema {
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share

namespace rootserver {
class ObRebalanceTaskMgr;
class ObZoneManager;
class ObRereplication {
public:
  ObRereplication();
  virtual ~ObRereplication()
  {}
  int init(share::schema::ObMultiVersionSchemaService& schema_service, ObZoneManager& zone_mgr,
      share::ObPartitionTableOperator& pt_operator, ObRebalanceTaskMgr& task_mgr, TenantBalanceStat& tenant_stat,
      share::ObCheckStopProvider& check_stop_provider, ObUnitManager& unit_mgr);
  int process_only_in_memberlist_replica(int64_t& task_cnt);
  int replicate_enough_replica(const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt);
  int replicate_to_unit(int64_t& task_cnt);
  int remove_permanent_offline_replicas(int64_t& task_cnt);

private:
  int check_inner_stat();
  int check_stop() const
  {
    return check_stop_provider_->check_stop();
  }
  int replicate_enough_paxos_replica(const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt);
  int replicate_enough_specific_logonly_replica(
      const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt);
  int replicate_enough_readonly_replica(const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt);
  int get_locality_info(share::schema::ObSchemaGetterGuard& schema_guard, const Partition& partition,
      const balancer::HashIndexCollection& hash_index_collection, share::schema::ZoneLocalityIArray& zone_locality,
      common::ObIArray<common::ObZone>& zone_list) const;
  int get_readonly_all_server_compensation_mode(share::schema::ObSchemaGetterGuard& schema_guard,
      const Partition& partition, bool& compensate_readonly_all_server) const;

private:
  // data members
  bool inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObZoneManager* zone_mgr_;
  ObRebalanceTaskMgr* task_mgr_;
  share::ObPartitionTableOperator* pt_operator_;
  TenantBalanceStat* tenant_stat_;
  share::ObCheckStopProvider* check_stop_provider_;
  ObUnitManager* unit_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRereplication);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_REREPLICATION_H */
