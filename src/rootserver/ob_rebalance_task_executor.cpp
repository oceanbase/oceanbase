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

#define USING_LOG_PREFIX RS

#include "ob_rebalance_task_executor.h"

#include "share/ob_debug_sync.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_cluster_version.h"
#include "share/partition_table/ob_partition_table_proxy.h"
#include "ob_rs_event_history_table_operator.h"
#include "ob_server_manager.h"
#include "ob_zone_manager.h"
#include "ob_leader_coordinator.h"
#include "ob_rebalance_task_mgr.h"
#include "ob_rebalance_task.h"
#include "observer/ob_server.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/executor/ob_bkgd_dist_task.h"
#include "sql/executor/ob_executor_rpc_proxy.h"
#include "sql/executor/ob_executor_rpc_processor.h"
#include "share/ob_multi_cluster_util.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace rootserver {

int ObRebalanceTaskExecutor::init(share::ObPartitionTableOperator& pt_operator, obrpc::ObSrvRpcProxy& rpc_proxy,
    ObServerManager& server_mgr, const ObZoneManager& zone_mgr, ObLeaderCoordinator& leader_coordinator,
    schema::ObMultiVersionSchemaService& schema_service, ObUnitManager& unit_manager)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRebalanceTaskExecutor inited twice", K(ret));
  } else {
    pt_operator_ = &pt_operator;
    rpc_proxy_ = &rpc_proxy;
    server_mgr_ = &server_mgr;
    zone_mgr_ = &zone_mgr;
    leader_coordinator_ = &leader_coordinator;
    schema_service_ = &schema_service;
    unit_mgr_ = &unit_manager;
    inited_ = true;
  }
  return ret;
}

int ObRebalanceTaskExecutor::execute(const ObRebalanceTask& task, ObIArray<int>& rc_array)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_REBALANCE_TASK_EXECUTE);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(task.get_sub_task_count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", K(ret), "task_info_count", task.get_sub_task_count());
  } else {
    share::ObClusterInfo cluster_info;
    // check dst server avaiable
    const ObAddr& online_server = task.get_dest();
    bool is_alive = false;
    bool in_service = false;
    common::ObAddr leader;
    if (OB_FAIL(server_mgr_->check_server_alive(online_server, is_alive))) {
      LOG_WARN("check server alive failed", K(ret), K(online_server));
    } else if (!is_alive) {
      ret = OB_REBALANCE_TASK_CANT_EXEC;
      LOG_WARN("dst server not alive", K(ret), K(online_server));
    } else if (OB_FAIL(server_mgr_->check_in_service(online_server, in_service))) {
      LOG_WARN("check in service failed", K(ret), K(online_server));
    } else if (!in_service) {
      ret = OB_REBALANCE_TASK_CANT_EXEC;
      LOG_WARN("dst server not in service", K(ret), K(online_server));
    } else if (OB_UNLIKELY(nullptr == pt_operator_ || nullptr == rpc_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null ptr", K(ret), KP(pt_operator_), KP(rpc_proxy_));
    } else if (OB_FAIL(task.check_before_execute(*pt_operator_, leader))) {
      LOG_WARN("fail to check before execute", K(ret));
    } else {
      task.log_execute_start();
      if (OB_FAIL(task.execute(*rpc_proxy_, leader, rc_array))) {
        LOG_WARN("fail to execute task", K(ret));
      }
    }
  }

  LOG_INFO("execute rebalance task", K(ret), K(task));
  return ret;
}

int ObRebalanceTaskUtil::get_remove_member_leader(const ObRemoveMemberTaskInfo& task_info, ObAddr& leader)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo partition;
  partition.set_allocator(&allocator);
  const ObPartitionKey& pkey = task_info.get_partition_key();
  const ObPartitionReplica* replica = NULL;
  ObZone leader_zone;
  if (OB_ISNULL(GCTX.pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt_operator is null", K(ret));
  } else if (OB_FAIL(GCTX.pt_operator_->get(pkey.get_table_id(), pkey.get_partition_id(), partition))) {
    LOG_WARN("fail to get partition info", K(ret), K(pkey));
  } else if (OB_FAIL(partition.find_leader_by_election(replica))) {
    LOG_WARN("find leader failed", K(ret), K(partition));
  } else if (NULL == replica) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL replica returned", K(ret));
  } else {
    leader = replica->server_;
    leader_zone = replica->zone_;
    if (leader != task_info.get_dest().get_server()) {
      // when the remove target is not leader, we can execute using batch mode,
      // if there are leade switch operation after the check, observer is responsible to handle
    } else {
      // need to switch leader to another replica when the remove target is leader
      ObArray<ObAddr> excluded_servers;
      ObArray<ObZone> excluded_zones;
      if (OB_FAIL(excluded_servers.push_back(leader))) {
        LOG_WARN("array push back failed", K(ret));
      } else if (OB_FAIL(excluded_zones.push_back(leader_zone))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_ISNULL(GCTX.root_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("leader_coordinator is null", K(ret));
      } else if (OB_FAIL(GCTX.root_service_->get_leader_coordinator().coordinate_partition_group(
                     pkey.get_table_id(), pkey.get_partition_id(), excluded_servers, excluded_zones))) {
        LOG_WARN("coordinate partition group leader failed", K(ret), "replica", *replica);
      } else {
        partition.reuse();
        replica = NULL;
        if (OB_FAIL(GCTX.pt_operator_->get(pkey.get_table_id(), pkey.get_partition_id(), partition))) {
          LOG_WARN("fail to get partition info", K(ret), K(pkey));
        } else if (OB_FAIL(partition.find_leader_by_election(replica))) {
          LOG_WARN("find leader failed", K(ret), K(partition));
        } else if (NULL == replica) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL replica returned", K(ret));
        } else if (replica->server_ == leader) {  // still leader, switch leader failed
          ret = OB_REBALANCE_TASK_CANT_EXEC;
          LOG_WARN("still leader, can't execute task now", K(task_info), K(leader), K(ret));
        } else {  // switch leader succeed
          leader = replica->server_;
        }
      }
    }
  }
  return ret;
}

int ObRebalanceTaskUtil::build_batch_remove_member_task(ObRebalanceTaskMgr& task_mgr, ObAddr& leader,
    const ObRemoveMemberTaskInfo& task_info, const char* comment, ObIArray<ObRemoveMemberTask>& remove_member_tasks,
    bool& check_pg_leader)
{
  int ret = OB_SUCCESS;
  share::ObClusterInfo cluster_info;
  if (!leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("leader is invalid", K(ret), K(leader), K(task_info));
  } else if (leader == task_info.get_dest().get_server() || check_pg_leader) {
    // need to do execute check_remove_member_leader under the following two situations:
    // 1. the remove target is the leader
    // 2. pg has partitions because of check_remove_member_leader
    if (OB_FAIL(get_remove_member_leader(task_info, leader))) {
      LOG_WARN("fail to get leader", K(ret), K(task_info));
    }
    check_pg_leader = true;
  }
  if (OB_FAIL(ret)) {
    // skip
  } else if (!leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("leader is invalid, wait for next round", K(ret), K(task_info), K(leader));
  } else {
    int64_t index = 0;
    const bool is_standby = common::STANDBY_CLUSTER == cluster_info.cluster_type_;
    common::ObArray<ObRemoveMemberTaskInfo> task_info_array;
    bool check_leader = false;  // no need to check leader when batch_remove_member
    for (/*nop*/; OB_SUCC(ret) && index < remove_member_tasks.count(); index++) {
      ObRemoveMemberTask& task = remove_member_tasks.at(index);
      if (leader == task.get_dest()) {  // not up to the upper limit, go on to accumulate
        bool can_gather = false;
        if (OB_FAIL(check_can_gather_task(task, task_info, is_standby, can_gather))) {
          LOG_WARN("failed to check can gather task", KR(ret), K(task_info), K(task));
        } else if (can_gather) {
          if (OB_FAIL(task.add_remove_member_task_info(task_info))) {
            LOG_WARN("fail to add remove member task info", K(ret));
          }
        } else {  // cannot accumulate any more, execute the previous task the reset
          if (OB_FAIL(task_mgr.add_task(task))) {
            LOG_WARN("fail to add task", K(ret));
          } else if (OB_FAIL(task_info_array.push_back(task_info))) {
            LOG_WARN("fail to add task_info", K(ret), K(task_info));
          } else if (OB_FAIL(task.build(task_info_array, leader, comment, check_leader))) {
            LOG_WARN("fail to build remove member task", K(ret));
          }
        }
        break;
      }
    }
    if (OB_SUCC(ret) && index == remove_member_tasks.count()) {
      // create a new task when no one found
      ObRemoveMemberTask task;
      if (OB_FAIL(task_info_array.push_back(task_info))) {
        LOG_WARN("fail to add task_info", K(ret), K(task_info));
      } else if (OB_FAIL(task.build(task_info_array, leader, comment, check_leader))) {
        LOG_WARN("fail to build remove member task", K(ret));
      } else if (OB_FAIL(remove_member_tasks.push_back(task))) {
        LOG_WARN("fail to add task", K(ret), K(task));
      }
    }
  }
  return ret;
}
int ObRebalanceTaskUtil::check_can_gather_task(
    const ObRemoveMemberTask& task, const ObRemoveMemberTaskInfo& task_info, const bool is_standby, bool& can_gather)
{
  int ret = OB_SUCCESS;
  can_gather = false;
  if (!is_standby) {
    if (MAX_BATCH_REMOVE_MEMBER_COUNT > task.get_sub_task_count()) {
      can_gather = true;
    } else {
      can_gather = false;
    }
  } else if (0 == task.get_sub_task_count()) {
    can_gather = true;
  } else {
    // private partitions and non-private partitons should not be accumulated in the same task
    const ObRebalanceTaskInfo* info = task.get_sub_task(0);
    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task info is null", KR(ret));
    } else {
      can_gather = info->can_accumulate_in_standby(task_info);
    }
  }
  return ret;
}

int ObRebalanceTaskUtil::build_batch_remove_non_paxos_replica_task(ObRebalanceTaskMgr& task_mgr,
    const ObRemoveNonPaxosTaskInfo& task_info, const char* comment,
    ObIArray<ObRemoveNonPaxosTask>& remove_non_paxos_replica_tasks)
{
  int ret = OB_SUCCESS;
  const ObAddr& dest = task_info.get_dest().get_server();
  int64_t index = 0;
  common::ObArray<ObRemoveNonPaxosTaskInfo> task_info_array;
  for (/*nop*/; OB_SUCC(ret) && index < remove_non_paxos_replica_tasks.count(); index++) {
    ObRemoveNonPaxosTask& task = remove_non_paxos_replica_tasks.at(index);
    if (dest == task.get_dest()) {
      if (task.get_sub_task_count() < MAX_BATCH_REMOVE_NON_PAXOS_REPLICA_COUNT) {
        // not up to the upper limit
        if (OB_FAIL(task.add_remove_non_paxos_replica_task_info(task_info))) {
          LOG_WARN("fail to add remove non paxos replica task info", K(ret));
        }
      } else {
        // up to the upper limit execute the previous task then reset
        if (OB_FAIL(task_mgr.add_task(task))) {
          LOG_WARN("fail to add task", K(ret));
        } else if (OB_FAIL(task_info_array.push_back(task_info))) {
          LOG_WARN("fail to add task_info", K(ret), K(task_info));
        } else if (OB_FAIL(task.build(task_info_array, dest, comment))) {
          LOG_WARN("fail to build remove non paxos replica task", K(ret));
        }
      }
      break;
    }
  }
  if (OB_SUCC(ret) && index == remove_non_paxos_replica_tasks.count()) {
    // create a new one when no one found
    ObRemoveNonPaxosTask task;
    if (OB_FAIL(task_info_array.push_back(task_info))) {
      LOG_WARN("fail to add task_info", K(ret), K(task_info));
    } else if (OB_FAIL(task.build(task_info_array, dest, comment))) {
      LOG_WARN("fail to build remove non paxo replica  task", K(ret));
    } else if (OB_FAIL(remove_non_paxos_replica_tasks.push_back(task))) {
      LOG_WARN("fail to add task", K(ret), K(task));
    }
  }
  return ret;
}

int ObRebalanceTaskUtil::build_batch_modify_quorum_task(ObRebalanceTaskMgr& task_mgr,
    const ObModifyQuorumTaskInfo& task_info, const char* comment, ObIArray<ObModifyQuorumTask>& modify_quorum_tasks)
{
  int ret = OB_SUCCESS;
  const ObAddr& leader = task_info.get_dest().get_server();
  int64_t index = 0;
  common::ObArray<ObModifyQuorumTaskInfo> task_info_array;
  for (/*nop*/; OB_SUCC(ret) && index < modify_quorum_tasks.count(); index++) {
    ObModifyQuorumTask& task = modify_quorum_tasks.at(index);
    if (leader == task.get_dest()) {
      if (task.get_sub_task_count() < MAX_BATCH_MODIFY_QUORUM_COUNT) {
        // not up to the upper limit
        if (OB_FAIL(task.add_modify_quorum_task_info(task_info))) {
          LOG_WARN("fail to add remove member task info", K(ret));
        }
      } else {
        // up to the upper limit execute the previous task then reset
        if (OB_FAIL(task_mgr.add_task(task))) {
          LOG_WARN("fail to add task", K(ret));
        } else if (OB_FAIL(task_info_array.push_back(task_info))) {
          LOG_WARN("fail to add task_info", K(ret), K(task_info));
        } else if (OB_FAIL(task.build(task_info_array, leader, comment))) {
          LOG_WARN("fail to build remove member task", K(ret));
        }
      }
      break;
    }
  }
  if (OB_SUCC(ret) && index == modify_quorum_tasks.count()) {
    // create a new task when no one found
    ObModifyQuorumTask task;
    if (OB_FAIL(task_info_array.push_back(task_info))) {
      LOG_WARN("fail to add task_info", K(ret), K(task_info));
    } else if (OB_FAIL(task.build(task_info_array, leader, comment))) {
      LOG_WARN("fail to build remove member task", K(ret));
    } else if (OB_FAIL(modify_quorum_tasks.push_back(task))) {
      LOG_WARN("fail to add task", K(ret), K(task));
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
