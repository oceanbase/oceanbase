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

#ifndef OCEANBASE_ROOTSERVER_OB_REBALANCE_TASK_EXECUTOR_H_
#define OCEANBASE_ROOTSERVER_OB_REBALANCE_TASK_EXECUTOR_H_

#include "rootserver/ob_rebalance_task_mgr.h"
#include "rootserver/ob_unit_manager.h"

namespace oceanbase {
namespace obrpc {
class ObSrvRpcProxy;
}

namespace share {
class ObPartitionTableOperator;
class ObPartitionInfo;
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share
namespace rootserver {
class ObServerManager;
class ObZoneManager;
class ObLeaderCoordinator;
class ObRebalanceTask;

class ObRebalanceTaskExecutor {
public:
  ObRebalanceTaskExecutor()
      : inited_(false),
        pt_operator_(NULL),
        rpc_proxy_(NULL),
        server_mgr_(NULL),
        zone_mgr_(NULL),
        leader_coordinator_(NULL),
        schema_service_(NULL),
        unit_mgr_(NULL)
  {}
  virtual ~ObRebalanceTaskExecutor()
  {}

  int init(share::ObPartitionTableOperator& pt_operator, obrpc::ObSrvRpcProxy& rpc_proxy, ObServerManager& server_mgr,
      const ObZoneManager& zone_mgr, ObLeaderCoordinator& leader_coordinator,
      share::schema::ObMultiVersionSchemaService& schema_service, ObUnitManager& unit_manager);

  bool is_inited() const
  {
    return inited_;
  }
  virtual int execute(const ObRebalanceTask& task, common::ObIArray<int>& rc_array);

private:
  bool inited_;
  share::ObPartitionTableOperator* pt_operator_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  ObServerManager* server_mgr_;
  const ObZoneManager* zone_mgr_;
  ObLeaderCoordinator* leader_coordinator_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  const ObUnitManager* unit_mgr_;

  DISALLOW_COPY_AND_ASSIGN(ObRebalanceTaskExecutor);
};

class ObRebalanceTaskMgr;
class ObRebalanceTaskUtil {
public:
  static const int64_t MAX_BATCH_MODIFY_QUORUM_COUNT = 10000;
  static const int64_t MAX_BATCH_REMOVE_MEMBER_COUNT = 10000;
  static const int64_t MAX_BATCH_REMOVE_NON_PAXOS_REPLICA_COUNT = 100;
  static int build_batch_remove_member_task(ObRebalanceTaskMgr& task_mgr, ObAddr& leader,
      const rootserver::ObRemoveMemberTaskInfo& task_info, const char* comment,
      common::ObIArray<rootserver::ObRemoveMemberTask>& remove_member_tasks, bool& check_pg_leader);
  static int check_can_gather_task(
      const ObRemoveMemberTask& task, const ObRemoveMemberTaskInfo& task_info, const bool is_standby, bool& can_gather);
  static int build_batch_remove_non_paxos_replica_task(ObRebalanceTaskMgr& task_mgr,
      const rootserver::ObRemoveNonPaxosTaskInfo& task_info, const char* comment,
      common::ObIArray<ObRemoveNonPaxosTask>& remove_non_paxos_replica_tasks);
  static int build_batch_modify_quorum_task(ObRebalanceTaskMgr& task_mgr, const ObModifyQuorumTaskInfo& task_info,
      const char* comment, common::ObIArray<rootserver::ObModifyQuorumTask>& modify_quorum_tasks);

private:
  static int get_remove_member_leader(const ObRemoveMemberTaskInfo& task_info, ObAddr& leader);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_REBALANCE_TASK_EXECUTOR_H_
