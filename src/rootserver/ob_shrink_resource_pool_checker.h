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

#ifndef _OCEANBASE_ROOTSERVER_OB_SHRINK_RESOURCE_POOL_CHECKER_H_
#define _OCEANBASE_ROOTSERVER_OB_SHRINK_RESOURCE_POOL_CHECKER_H_ 1
#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "share/schema/ob_schema_struct.h"
#include "ob_balance_info.h"
namespace oceanbase {
namespace common {
class ObAddr;
}
namespace obrpc {
class ObSrvRpcProxy;
}
namespace share {
class ObPartitionTableOperator;
struct ObResourcePool;
class ObPartitionInfo;
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share

namespace rootserver {
class ObLeaderCoordinator;
class ObUnitManager;
struct TenantBalanceStat;
class ObShrinkResourcePoolChecker {
public:
  ObShrinkResourcePoolChecker(volatile bool& is_stop)
      : is_stop_(is_stop),
        leader_coordinator_(NULL),
        schema_service_(NULL),
        srv_rpc_proxy_(NULL),
        unit_mgr_(NULL),
        pt_operator_(NULL),
        self_(),
        tenant_stat_(NULL),
        rebalance_task_mgr_(NULL),
        server_mgr_(NULL),
        is_inited_(false)
  {}
  virtual ~ObShrinkResourcePoolChecker()
  {}

public:
  int init(share::schema::ObMultiVersionSchemaService* schema_service,
      rootserver::ObLeaderCoordinator* leader_coordinator, obrpc::ObSrvRpcProxy* srv_rpc_proxy, common::ObAddr& addr,
      rootserver::ObUnitManager* unit_mgr, share::ObPartitionTableOperator* pt_operator, TenantBalanceStat* tenant_stat,
      ObRebalanceTaskMgr* rebalance_task_mgr, ObServerManager* server_mgr);

public:
  int check_shrink_resource_pool_finished();
  int try_shrink_tenant_resource_pools(int64_t& task_cnt);

private:
  const int64_t NOT_IN_POOL_NON_PAXOS_SAFE_REMOVE_INTERVAL = 120 * 1000000;  // 120s
  struct ZoneReplicas {
    common::ObZone zone_;
    ObSEArray<const Replica*, 8> in_pool_paxos_replicas_;
    ObSEArray<const Replica*, 8> not_in_pool_paxos_replicas_;
    ObSEArray<const Replica*, 8> in_pool_non_paxos_replicas_;
    ObSEArray<const Replica*, 8> not_in_pool_non_paxos_replicas_;
    int assign(const ZoneReplicas& other);
    TO_STRING_KV(K_(zone), K_(in_pool_paxos_replicas), K_(not_in_pool_paxos_replicas), K_(in_pool_non_paxos_replicas),
        K_(not_in_pool_non_paxos_replicas));
    bool is_valid() const
    {
      return !zone_.is_empty();
    }
  };

private:
  int check_stop();
  int check_shrink_resource_pool_finished_by_tenant(
      share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id);
  int check_single_pool_shrinking_finished(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id,
      const uint64_t pool_id, bool& is_finished);
  int check_single_partition_entity_finished(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t schema_id,
      const ObIArray<common::ObAddr>& servers, const ObIArray<uint64_t>& unit_ids, bool& is_finished);
  int commit_shrink_resource_pool(const uint64_t pool_id);
  int extract_units_servers_and_ids(
      const ObIArray<share::ObUnit>& units, ObIArray<common::ObAddr>& servers, ObIArray<uint64_t>& unit_ids);
  int check_partition_replicas_exist_on_servers_and_units(const share::ObPartitionInfo& partition_info,
      const ObIArray<common::ObAddr>& servers, const ObIArray<uint64_t>& unit_ids, bool& has_replica_on_servers);
  int shrink_single_resource_pool(const share::ObResourcePool& pool, int64_t& task_cnt);
  int regulate_single_partition(
      const Partition& partition, const common::ObIArray<common::ObZone>& zone_list, int64_t& task_cnt);
  int regulate_single_partition_by_zone(
      const Partition& partition, const ZoneReplicas& zone_replicas, int64_t& task_cnt);
  int remove_not_in_pool_non_paxos_replicas(
      const Partition& partition, const ObIArray<const Replica*>& not_in_pool_non_paxos_replicas, int64_t& task_cnt);
  int transfer_position_for_not_in_pool_paxos_replicas(const Partition& partition,
      const ObIArray<const Replica*>& not_in_pool_paxos_replicas,
      const ObIArray<const Replica*>& in_pool_non_paxos_replicas, int64_t& task_cnt);
  bool is_non_paxos_replica(const int32_t replica_type);
  bool is_paxos_replica_V2(const int32_t replica_type);

private:
  const volatile bool& is_stop_;
  rootserver::ObLeaderCoordinator* leader_coordinator_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  obrpc::ObSrvRpcProxy* srv_rpc_proxy_;
  rootserver::ObUnitManager* unit_mgr_;
  share::ObPartitionTableOperator* pt_operator_;
  common::ObAddr self_;
  TenantBalanceStat* tenant_stat_;
  ObRebalanceTaskMgr* rebalance_task_mgr_;
  ObServerManager* server_mgr_;
  bool is_inited_;
};
}  // end of namespace rootserver
}  // end of namespace oceanbase
#endif
