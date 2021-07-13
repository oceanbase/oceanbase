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

#include "lib/container/ob_se_array.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_replica_info.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "rootserver/ob_balance_info.h"
#include "ob_root_utils.h"
#ifndef OCEANBASE_ROOTSERVER_OB_LOCALITY_CHECKER_H
#define OCEANBASE_ROOTSERVER_OB_LOCALITY_CHECKER_H
namespace oceanbase {
namespace share {
class ObPartitionInfo;
namespace schema {
class ObTableSchema;
class ObMultiVersionSchemaService;
}  // namespace schema
}  // namespace share
namespace rootserver {
class ObRebalanceTaskMgr;
class ObZoneManager;
class Replica;
class Partition;
class TenantBalanceStat;
class ObFilterLocalityUtility;
class ObDeleteReplicaUtility;
class ObReplicaTypeTransformUtility;
class ObServerManager;
class ObLeaderCoordinator;
class ObLocalityChecker {
public:
  explicit ObLocalityChecker();
  virtual ~ObLocalityChecker();
  int init(share::schema::ObMultiVersionSchemaService& schema_service, TenantBalanceStat& tenant_stat,
      ObRebalanceTaskMgr& task_mgr, ObZoneManager& zone_mgr, share::ObCheckStopProvider& check_stop_provider,
      ObServerManager& server_mgr, ObUnitManager& unit_mgr);
  // use to correct quorum
  int do_modify_quorum(const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt);
  // use to delete redundant replica
  int do_delete_redundant(const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt);
  // use to do type transform for replica
  int do_type_transform(const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt);
  int get_filter_result(
      const balancer::HashIndexCollection& hash_index_collection, common::ObIArray<ObReplicaTask>& results);

private:
  static const int64_t MAX_BATCH_TYPE_TRANSFORM_COUNT = 128;

private:
  void reset();

  int check_partition_locality_for_modify_quorum(const balancer::HashIndexCollection& hash_index_collection,
      const Partition& partition, int64_t& task_cnt,
      common::ObIArray<rootserver::ObModifyQuorumTask>& modify_quorum_tasks);

  int get_previous_locality_paxos_num(
      const uint64_t& tenant_id, const common::ObString& previous_locality, int64_t& pre_paxos_num);

  int check_partition_locality_for_delete(ObDeleteReplicaUtility& checker, const Partition& partition,
      const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt,
      common::ObIArray<ObRemoveMemberTask>& remove_member_tasks,
      common::ObIArray<ObRemoveNonPaxosTask>& remove_non_paxos_replica_tasks, bool& check_pg_leader);

  int generate_designated_zone_locality(const Partition& partition,
      const balancer::HashIndexCollection& hash_index_collection,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& actual_zone_locality,
      common::ObIArray<share::ObZoneReplicaAttrSet>& designated_zone_locality);

  int get_partition_locality(ObFilterLocalityUtility& checker, const Partition& partition,
      const balancer::HashIndexCollection& hash_index_collection);

  int check_can_modify_quorum(const balancer::HashIndexCollection& hash_index_collection, const Partition& Partition,
      int64_t& new_quorum, bool& can_modify_quorum);

  int delete_redundant_replica(const Partition& partition, const Replica& replica,
      common::ObIArray<ObRemoveMemberTask>& remove_member_tasks,
      common::ObIArray<ObRemoveNonPaxosTask>& remove_non_paxos_replica_tasks, bool& check_pg_leader);

  int type_transform(const Partition& partition, const Replica& replica, const common::ObReplicaType& dest_type);

  int modify_quorum(const Partition& partition, const Replica& replica, const int64_t& quorum,
      ObMemberList& member_list, common::ObIArray<rootserver::ObModifyQuorumTask>& modify_quorum_tasks);

  int remove_member(const Partition& partition, const Replica& replica,
      common::ObIArray<ObRemoveMemberTask>& remove_member_tasks, bool& check_pg_leader);

  int remove_replica(const Partition& partition, const Replica& replica,
      common::ObIArray<ObRemoveNonPaxosTask>& remove_non_paxos_replica_tasks);

  int check_stop() const
  {
    return check_stop_provider_->check_stop();
  }

  int check_type_transform_task_valid(
      const Partition& partition, const Replica& replica, const ObReplicaType& dest_type, bool& is_task_valid);

  int check_remove_member_leader(const rootserver::ObRebalanceTaskInfo& task_info, common::ObAddr& leader);

  int try_accumulate_task_info(common::ObIArray<ObTypeTransformTask>& task_array,
      common::ObIArray<common::ObAddr>& leader_addr_array, const ObTypeTransformTaskInfo& task_info,
      const Partition* cur_partition, const Replica& cur_replica, const ObReplicaType dst_type, int64_t& task_cnt);

private:
  static const int64_t DEFAULT_ZONE_CNT = 7;

private:
  bool inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  TenantBalanceStat* tenant_stat_;
  ObRebalanceTaskMgr* task_mgr_;
  ObZoneManager* zone_mgr_;
  share::ObCheckStopProvider* check_stop_provider_;
  int64_t last_table_id_;
  common::ObSEArray<share::ObZoneReplicaAttrSet, DEFAULT_ZONE_CNT> zone_locality_;
  common::ObSEArray<common::ObZone, DEFAULT_ZONE_CNT> zone_list_;
  ObServerManager* server_mgr_;
  common::ObSEArray<common::ObZone, DEFAULT_ZONE_CNT> primary_zone_array_;
  ObUnitManager* unit_mgr_;
};
}  // namespace rootserver
}  // namespace oceanbase
#endif
