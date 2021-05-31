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
#include "ob_locality_checker.h"
#include "ob_rebalance_task_mgr.h"
#include "ob_rebalance_task_executor.h"
#include "ob_balance_info.h"
#include "ob_root_utils.h"
#include "ob_alloc_replica_strategy.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_unit_manager.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_multi_cluster_util.h"
#include "observer/ob_server_struct.h"
namespace oceanbase {
using namespace common;
using namespace share;
using namespace obrpc;
using namespace share::schema;
namespace rootserver {
ObLocalityChecker::ObLocalityChecker()
    : inited_(false),
      schema_service_(NULL),
      tenant_stat_(NULL),
      task_mgr_(NULL),
      zone_mgr_(NULL),
      check_stop_provider_(NULL),
      last_table_id_(0),
      zone_locality_(),
      zone_list_(),
      server_mgr_(NULL),
      primary_zone_array_(),
      unit_mgr_(NULL)
{}

ObLocalityChecker::~ObLocalityChecker()
{}

int ObLocalityChecker::init(ObMultiVersionSchemaService& schema_service, TenantBalanceStat& tenant_stat,
    ObRebalanceTaskMgr& task_mgr, ObZoneManager& zone_mgr, share::ObCheckStopProvider& check_stop_provider,
    ObServerManager& server_mgr, ObUnitManager& unit_mgr)
{
  int ret = OB_SUCCESS;
  schema_service_ = &schema_service;
  tenant_stat_ = &tenant_stat;
  task_mgr_ = &task_mgr;
  zone_mgr_ = &zone_mgr;
  check_stop_provider_ = &check_stop_provider;
  server_mgr_ = &server_mgr;
  unit_mgr_ = &unit_mgr;
  last_table_id_ = 0;
  primary_zone_array_.reset();
  inited_ = true;
  return ret;
}

void ObLocalityChecker::reset()
{
  last_table_id_ = 0;
  zone_locality_.reset();
  primary_zone_array_.reset();
}

int ObLocalityChecker::try_accumulate_task_info(common::ObIArray<ObTypeTransformTask>& task_array,
    common::ObIArray<common::ObAddr>& leader_addr_array, const ObTypeTransformTaskInfo& task_info,
    const Partition* cur_partition, const Replica& cur_replica, const ObReplicaType dst_type, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  ObTypeTransformTask task;
  bool valid_leader = false;
  bool is_standby = false;
  ObClusterType cluster_type = INVALID_CLUSTER_TYPE;
  common::ObAddr cur_leader;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == cur_partition || !ObReplicaTypeCheck::is_replica_type_valid(dst_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(cur_partition), K(dst_type));
  } else if (FALSE_IT(cluster_type = ObClusterInfoGetter::get_cluster_type_v2())) {
  } else if (OB_UNLIKELY(NULL == task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task_mgr_ ptr is null", K(ret));
  } else if (OB_ISNULL(tenant_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_stat is null", K(ret));
  } else {
    is_standby = PRIMARY_CLUSTER != cluster_type;
    if (!cur_partition->in_physical_restore()) {
      if (OB_FAIL(tenant_stat_->get_leader_addr(*cur_partition, cur_leader, valid_leader))) {
        LOG_WARN("fail to get leader addr", K(ret), K(task_info));
      } else if (!valid_leader) {
        LOG_WARN("invalid leader, maybe leader not exist, do it next round", K(*cur_partition));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (task_array.count() != leader_addr_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count not match", K(ret));
  } else {
    int64_t index = 0;
    for (/*nop*/; OB_SUCC(ret) && index < task_array.count(); ++index) {
      common::ObArray<ObTypeTransformTaskInfo> task_info_array;
      ObTypeTransformTask& this_task = task_array.at(index);
      const common::ObAddr& this_leader = leader_addr_array.at(index);
      const ObTypeTransformTaskInfo* this_task_info = nullptr;
      const ObRebalanceTaskInfo* base_info = nullptr;
      if (this_task.get_sub_task_count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this task invalid", K(ret), K(this_task));
      } else if (nullptr == (base_info = this_task.get_sub_task(0 /*the first task*/))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("base task info ptr is null", K(ret));
      } else if (nullptr == (this_task_info = static_cast<const ObTypeTransformTaskInfo*>(base_info))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task info ptr is null", K(ret));
      } else if (cur_replica.server_->server_ == this_task_info->get_dest().get_server() &&
                 cur_replica.replica_type_ == this_task_info->get_src_member().get_replica_type() &&
                 dst_type == this_task_info->get_dest().get_replica_type() && cur_leader == this_leader &&
                 (is_standby && this_task_info->can_accumulate_in_standby(task_info))) {
        if (cur_partition->is_primary() && this_task.get_sub_task_count() >= MAX_BATCH_TYPE_TRANSFORM_COUNT) {
          if (OB_FAIL(task_mgr_->add_task(this_task, task_cnt))) {
            LOG_WARN("fail to add task", K(ret));
          } else if (OB_FAIL(task_info_array.push_back(task_info))) {
            LOG_WARN("fail to push back", K(ret));
          } else if (OB_FAIL(this_task.build(
                         task_info_array, task_info.get_dest().get_server(), balancer::LOCALITY_TYPE_TRANSFORM))) {
            LOG_WARN("fail to build type transform task", K(ret));
          }
        } else {
          if (OB_FAIL(this_task.add_type_transform_task_info(task_info))) {
            LOG_WARN("fail to add type transform taks info", K(ret));
          }
        }
        break;
      } else {
      }  // type not match, go on to check
    }

    if (OB_SUCC(ret) && index >= task_array.count()) {
      common::ObArray<ObTypeTransformTaskInfo> task_info_array;
      ObTypeTransformTask this_task;
      if (OB_FAIL(task_info_array.push_back(task_info))) {
        LOG_WARN("fail to push back", K(ret), K(task_info));
      } else if (OB_FAIL(this_task.build(
                     task_info_array, task_info.get_dest().get_server(), balancer::LOCALITY_TYPE_TRANSFORM))) {
        LOG_WARN("fail to build type transform task", K(ret));
      } else if (OB_FAIL(task_array.push_back(this_task))) {
        LOG_WARN("fail to push back task", K(ret));
      } else if (OB_FAIL(leader_addr_array.push_back(cur_leader))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObLocalityChecker::do_type_transform(const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  reset();
  TenantBalanceStat& ts = *tenant_stat_;
  task_cnt = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ts.is_valid() || NULL == task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant stat or task_mgr_", K(ret), KP(task_mgr_));
  }
  common::ObArray<ObTypeTransformTask> task_array;
  common::ObArray<common::ObAddr> leader_addr_array;
  ObReplicaTypeTransformUtility checker(*zone_mgr_, *tenant_stat_, zone_locality_, zone_list_, *server_mgr_);
  FOREACH_X(partition, ts.sorted_partition_, OB_SUCC(ret))
  {
    ObTypeTransformTaskInfo task_info;
    bool need_change = false;
    bool is_task_valid = false;
    Replica replica;
    common::ObPartitionKey pkey;
    ObReplicaType dest_type = REPLICA_TYPE_MAX;
    int64_t dest_memstore_percent = -1;
    int64_t quorum = 0;
    int64_t transmit_data_size = 0;
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (NULL == *partition) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid partition", K(ret));
    } else if (!(*partition)->is_valid_quorum()) {
      // quorum is introduced from the version 1.4.71, it is updated by partition meta reports,
      // an invalid value of quorum means it is not reported yet, we shall ignore the invalid value and
      // go on to process the next partition
      LOG_WARN("quorum not report yet, just skip", "partition", *(*partition));
    } else if ((*partition)->is_primary() && task_cnt > ObRebalanceTaskMgr::ONCE_ADD_TASK_CNT) {
      break;
    } else if (OB_FAIL(get_partition_locality(checker, *(*partition), hash_index_collection))) {
      LOG_WARN("fail to get partition locality", K(ret));
    } else if (OB_FAIL(
                   checker.get_transform_task(*(*partition), need_change, replica, dest_type, dest_memstore_percent))) {
      LOG_WARN("fail to check task valid", K(ret), "partition", *(*partition));
    } else if (!need_change) {
      // no type transform task found
    } else if (!ObMemstorePercentCheck::is_memstore_percent_valid(dest_memstore_percent)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("memstore percent unexpected", K(ret), K(dest_memstore_percent), K(replica));
    } else if (!ObReplicaTypeCheck::is_replica_type_valid(dest_type) ||
               (dest_type == replica.replica_type_ && dest_memstore_percent == replica.memstore_percent_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica type unexpected", K(ret), K(dest_type), K(replica));
    } else if (OB_FAIL(check_type_transform_task_valid(*(*partition), replica, dest_type, is_task_valid))) {
      LOG_WARN("fail to check type transform task valid", K(ret));
    } else if (!is_task_valid) {
      // task invalid, by pass
    } else if (replica.is_standby_restore()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("replica is in standby restore, can not do type transform", KR(ret), K(replica));
    } else if (!ObBalanceTaskBuilder::can_do_type_transform(
                   *tenant_stat_, *(*partition), replica, dest_type, dest_memstore_percent)) {
      LOG_WARN("this partition cannot do type transform", K(replica), K(dest_type), "partition", *(*partition));
    } else {
      OnlineReplica dst;
      ObReplicaMember data_source;
      ObReplicaMember empty_source;
      ObReplicaMember src_member = ObReplicaMember(
          replica.server_->server_, replica.member_time_us_, replica.replica_type_, replica.memstore_percent_);
      int64_t cluster_id = OB_INVALID_ID;
      dst.member_ =
          ObReplicaMember(replica.server_->server_, ObTimeUtility::current_time(), dest_type, dest_memstore_percent);
      bool is_restore = (*partition)->in_physical_restore();
      dst.unit_id_ = replica.unit_->info_.unit_.unit_id_;
      dst.zone_ = replica.zone_;
      dst.member_.set_region(replica.region_);
      if (OB_FAIL((*tenant_stat_).gen_partition_key(*(*partition), pkey))) {
        LOG_WARN("fail to generate partition key", K(ret));
      } else if (OB_FAIL((*tenant_stat_)
                             .choose_data_source(*(*partition),
                                 dst.member_,
                                 empty_source,
                                 false /*is_rebuild*/,
                                 data_source,
                                 transmit_data_size,
                                 ObRebalanceTaskType::TYPE_TRANSFORM,
                                 cluster_id))) {
        LOG_WARN("fail to choose data source", K(ret), K(replica), "partition", *(*partition));
      } else if (OB_FAIL((*tenant_stat_)
                             .get_transform_quorum_size(
                                 *(*partition), replica.zone_, replica.replica_type_, dest_type, quorum))) {
        LOG_WARN("fail to get transform quorum size", K(ret));
      } else if (FALSE_IT(task_info.set_transmit_data_size(transmit_data_size))) {
        // will never be here
      } else if (OB_FAIL(task_info.build(dst, pkey, src_member, data_source, quorum, is_restore))) {
        LOG_WARN("fail to build type transform task", K(ret));
      } else if (OB_FAIL(try_accumulate_task_info(
                     task_array, leader_addr_array, task_info, *partition, replica, dest_type, task_cnt))) {
        LOG_WARN("fail to accumulate task info", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < task_array.count(); ++i) {
    if (OB_FAIL(task_mgr_->add_task(task_array.at(i), task_cnt))) {
      LOG_WARN("fail to add task", K(ret));
    }
  }
  return ret;
}

// 1. the number of full replicas on primary region shall be more than 1(ignore the single zone cluster)
// 2. the number of paxos replias shall be no less than the majority
int ObLocalityChecker::check_type_transform_task_valid(
    const Partition& partition, const Replica& replica, const ObReplicaType& dest_type, bool& is_task_valid)
{
  int ret = OB_SUCCESS;
  is_task_valid = true;
  // 1. the number of full replicas on primary region shall be more than 1(ignore the single zone cluster)
  int64_t full_in_primary_zone = 0;
  if (REPLICA_TYPE_FULL == replica.replica_type_ &&
      (has_exist_in_array(primary_zone_array_, replica.zone_) || primary_zone_array_.count() == 0)) {
    if (partition.schema_full_replica_cnt_ < 2) {
      // nothing todo
    } else {
      FOR_BEGIN_END_E(r, partition, tenant_stat_->all_replica_, OB_SUCC(ret))
      {
        if (OB_ISNULL(r)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid replica", K(ret), K(ret));
        } else if (primary_zone_array_.count() == 0) {
          full_in_primary_zone++;  // when primary_zone is not specified
        } else if (REPLICA_TYPE_FULL == r->replica_type_ && has_exist_in_array(primary_zone_array_, r->zone_)) {
          full_in_primary_zone++;
        }
      }
      if (OB_FAIL(ret)) {
        // nothing todo
      } else if (full_in_primary_zone > 2) {
        // success
      } else {
        is_task_valid = false;
        LOG_WARN("can't transform, primary zone should have full replica more than 2",
            K(last_table_id_),
            K(full_in_primary_zone));
      }
    }
  }
  // 2. the number of paxos replias shall be no less than the majority
  if (OB_FAIL(ret)) {
    // nothing todo
  } else if (ObReplicaTypeCheck::is_paxos_replica_V2(replica.replica_type_) &&
             !ObReplicaTypeCheck::is_paxos_replica_V2(dest_type)) {
    if ((partition.valid_member_cnt_ - 1) >= majority(partition.schema_replica_cnt_)) {
      // nothing todo
    } else {
      is_task_valid = false;
      LOG_WARN("can't transform, paxos replica should more than majority",
          K(last_table_id_),
          K(partition.valid_member_cnt_),
          K(partition.schema_replica_cnt_));
    }
  }
  return ret;
}

int ObLocalityChecker::do_delete_redundant(
    const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt)
{
  const int64_t start = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  reset();
  TenantBalanceStat& ts = *tenant_stat_;
  task_cnt = 0;
  ObArray<ObRemoveMemberTask> remove_member_tasks;
  ObArray<ObRemoveNonPaxosTask> remove_non_paxos_replica_tasks;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ts.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant stat", K(ret));
  }
  int64_t tmp_cnt = 0;
  ObDeleteReplicaUtility checker(*zone_mgr_, *tenant_stat_, zone_locality_, zone_list_);
  int tmp_ret = OB_SUCCESS;
  bool check_pg_leader = false;
  const Partition* prev = NULL;
  FOREACH_X(p, ts.sorted_partition_, OB_SUCC(ret))
  {
    tmp_cnt = 0;
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (NULL == p || NULL == *p) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid partition", K(ret));
    } else if (!(*p)->is_valid_quorum()) {
      // quorum is introduced from the version 1.4.71, it is updated by partition meta reports,
      // an invalid value of quorum means it is not reported yet, we shall ignore the invalid value and
      // go on to process the next partition
      LOG_WARN("quorum not report yet, just skip", "partition", *(*p));
    } else if (task_cnt > ObRebalanceTaskMgr::ONCE_ADD_TASK_CNT) {
      break;
    } else {
      // a double check leader operation need to be done if a switch leader operation is executed in
      // the same partition group
      if (NULL == prev || !is_same_pg(*prev, *(*p))) {
        check_pg_leader = false;
      }
      prev = *p;
      if (OB_SUCCESS != (tmp_ret = check_partition_locality_for_delete(checker,
                             *(*p),
                             hash_index_collection,
                             tmp_cnt,
                             remove_member_tasks,
                             remove_non_paxos_replica_tasks,
                             check_pg_leader))) {
        // ignore single partition error
        LOG_WARN("fail to check partition locality", K(tmp_ret), "partition", *(*p));
      } else {
        task_cnt += tmp_cnt;
      }
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < remove_member_tasks.count(); i++) {
      if (OB_FAIL(task_mgr_->add_task(remove_member_tasks.at(i)))) {
        LOG_WARN("fail to add task", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < remove_non_paxos_replica_tasks.count(); i++) {
      if (OB_FAIL(task_mgr_->add_task(remove_non_paxos_replica_tasks.at(i)))) {
        LOG_WARN("fail to add task", K(ret));
      }
    }
  }
  LOG_INFO("do delete redudant",
      K(ret),
      "cost",
      ObTimeUtility::current_time() - start,
      "remove_member_task_count",
      remove_member_tasks.count(),
      "remove_non_paxos_replica_count",
      remove_non_paxos_replica_tasks.count());
  return ret;
}

/*
 * the logic of correct quorum is taking effect in the following two situations:
 * 1 no locality modification: the value of quorum doesn't match the quorum, try to increment or
 *   decrement the value of quorum every time until the quorum is equal to the value in locality
 * 2 with locality modification: try to increment or decrement the quorum every time to make quorum
 *   a value between the old locality and new locality, then quorum will be corrected by member change
 */
int ObLocalityChecker::do_modify_quorum(const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt)
{
  const int64_t start = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  reset();
  TenantBalanceStat& ts = *tenant_stat_;
  task_cnt = 0;
  ObArray<ObModifyQuorumTask> modify_quorum_tasks;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ts.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant stat", K(ret));
  } else {
    int64_t tmp_cnt = 0;
    int tmp_ret = OB_SUCCESS;
    FOREACH_X(p, ts.all_partition_, OB_SUCC(ret))
    {
      tmp_cnt = 0;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("balancer stop", K(ret));
      } else if (NULL == p) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid partition", K(ret));
      } else if (!p->is_valid_quorum()) {
        // quorum is introduced from the version 1.4.71, it is updated by partition meta reports,
        // an invalid value of quorum means it is not reported yet, we shall ignore the invalid value and
        // go on to process the next partition
        LOG_WARN("quorum not report yet, just skip", KPC(p));
      } else if (p->in_physical_restore()) {
        // skip
      } else if (task_cnt > ObRebalanceTaskMgr::ONCE_ADD_TASK_CNT) {
        break;
      } else if (OB_SUCCESS != (tmp_ret = check_partition_locality_for_modify_quorum(
                                    hash_index_collection, *p, tmp_cnt, modify_quorum_tasks))) {
        LOG_WARN("fail to check partition locality", K(ret), KPC(p));
      } else {
        task_cnt += tmp_cnt;
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < modify_quorum_tasks.count(); i++) {
      if (OB_FAIL(task_mgr_->add_task(modify_quorum_tasks.at(i)))) {
        LOG_WARN("fail to add task", K(ret));
      }
    }
  }
  LOG_INFO("do modify quorum", K(ret), "cost", ObTimeUtility::current_time() - start);
  return ret;
}

// check if the logonly replicas need to be replicated to any logonly unit
int ObLocalityChecker::check_partition_locality_for_delete(ObDeleteReplicaUtility& checker, const Partition& partition,
    const balancer::HashIndexCollection& hash_index_collection, int64_t& task_cnt,
    ObIArray<ObRemoveMemberTask>& remove_member_tasks, ObIArray<ObRemoveNonPaxosTask>& remove_non_paxos_replica_tasks,
    bool& check_pg_leader)
{
  int ret = OB_SUCCESS;
  ObClusterInfo cluster_info;
  bool need_delete = false;
  task_cnt = 0;
  if (OB_FAIL(get_partition_locality(checker, partition, hash_index_collection))) {
    LOG_WARN("fail to get partition locality", K(ret));
  } else {
    Replica replica;
    if (OB_FAIL(checker.get_delete_task(partition, need_delete, replica))) {
      LOG_WARN("fail to check locality", K(ret), K(partition));
    } else if (!need_delete) {
      // nothing todo
    } else {
      ObRebalanceTaskType task_type = ObReplicaTypeCheck::is_paxos_replica_V2(replica.replica_type_)
                                          ? ObRebalanceTaskType::MEMBER_CHANGE
                                          : ObRebalanceTaskType::REMOVE_NON_PAXOS_REPLICA;
      if (OB_ISNULL(replica.server_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica server is null", K(ret), "replica server", replica.server_);
      } else if (replica.is_in_blacklist(task_type, replica.server_->server_, tenant_stat_)) {
        LOG_INFO("replica member change frequent failed, now in black list", K(replica));
      } else if (OB_FAIL(delete_redundant_replica(
                     partition, replica, remove_member_tasks, remove_non_paxos_replica_tasks, check_pg_leader))) {
        LOG_WARN("fail to delete redundant replica", K(ret));
      } else {
        task_cnt++;
      }
    }
  }
  return ret;
}

int ObLocalityChecker::check_partition_locality_for_modify_quorum(
    const balancer::HashIndexCollection& hash_index_collection, const Partition& partition, int64_t& task_cnt,
    ObIArray<ObModifyQuorumTask>& modify_quorum_tasks)
{
  int ret = OB_SUCCESS;
  int64_t new_quorum = OB_INVALID_COUNT;
  bool can_modify_quorum = false;
  const Replica* leader = NULL;
  ObMemberList member_list;
  task_cnt = 0;
  if (OB_FAIL(check_can_modify_quorum(hash_index_collection, partition, new_quorum, can_modify_quorum))) {
    LOG_WARN("fail to get new quorum", K(ret), K(partition));
  } else if (!can_modify_quorum) {
    // skip
  } else if (new_quorum <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("quorum is invalid", K(ret), K(new_quorum), K(partition));
  } else if (OB_ISNULL(tenant_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_stat is null", K(ret));
  } else if (OB_FAIL(tenant_stat_->get_leader_and_member_list(partition, leader, member_list))) {
    LOG_WARN("get leader and member_list failed", KPC(leader), K(member_list));
  } else if (OB_ISNULL(leader) || member_list.get_member_number() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't find leader", K(ret), K(partition), KPC(leader), K(member_list));
  } else if (OB_FAIL(modify_quorum(partition, *leader, new_quorum, member_list, modify_quorum_tasks))) {
    LOG_WARN("fail to modify quorum", K(ret), K(partition), KPC(leader), K(new_quorum));
  } else {
    task_cnt++;
  }
  return ret;
}

int ObLocalityChecker::get_filter_result(
    const balancer::HashIndexCollection& hash_index_collection, common::ObIArray<ObReplicaTask>& results)
{
  int ret = OB_SUCCESS;
  reset();
  TenantBalanceStat& ts = *tenant_stat_;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ts.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant stat", K(ret));
  } else {
    ObFilterLocalityUtility checker(*zone_mgr_, *tenant_stat_, zone_locality_, zone_list_);
    FOREACH_X(p, ts.all_partition_, OB_SUCC(ret))
    {
      if (NULL == p) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid partition", K(ret));
      } else if (OB_FAIL(get_partition_locality(checker, *p, hash_index_collection))) {
        LOG_WARN("fail to get partition locality", K(ret));
      } else if (OB_FAIL(checker.filter_locality(*p))) {
        LOG_WARN("fail to filter locality", K(ret));
      } else {
        const ObIArray<FilterResult>& tmp_results = checker.get_filter_result();
        ObReplicaTask replica_task;
        for (int i = 0; OB_SUCC(ret) && i < tmp_results.count(); i++) {
          replica_task.reset();
          if (OB_FAIL(tmp_results.at(i).build_task(*p, replica_task))) {
            LOG_WARN("fail to build task", KPC(p), "result", tmp_results.at(i));
          } else if (OB_FAIL(results.push_back(replica_task))) {
            LOG_WARN("results append failed", K(ret), "result", tmp_results.at(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObLocalityChecker::check_can_modify_quorum(const balancer::HashIndexCollection& hash_index_collection,
    const Partition& p, int64_t& new_quorum, bool& can_modify_quorum)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = p.table_id_;
  uint64_t tenant_id = extract_tenant_id(table_id);
  bool locality_changed = true;
  int64_t cur_paxos_num = 0;
  ObSchemaGetterGuard schema_guard;
  can_modify_quorum = true;
  zone_locality_.reset();
  zone_list_.reset();
  common::ObSEArray<share::ObZoneReplicaAttrSet, DEFAULT_ZONE_CNT> actual_zone_locality;
  bool compensate_readonly_all_server = false;
  if (p.in_physical_restore()) {
    can_modify_quorum = false;
  } else if (!p.is_valid_quorum()) {
    // no leader or not reported yet
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid quorum", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema manager failed", K(ret));
  } else if (OB_ISNULL(tenant_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_stat is null", K(ret));
  } else if (OB_FAIL(tenant_stat_->check_table_locality_changed(schema_guard, table_id, locality_changed))) {
    LOG_WARN("fail to check if schema changed", K(ret));
  } else if (locality_changed) {
    ret = OB_EAGAIN;
    LOG_WARN("locality changed, generate task next round", K(ret));
  } else if (!is_tablegroup_id(table_id)) {
    const ObTableSchema* table_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid table schema", K(ret), K(table_id));
    } else if (OB_FAIL(table_schema->get_zone_replica_attr_array_inherit(schema_guard, actual_zone_locality))) {
      LOG_WARN("fail to get zone locality", K(ret), K(table_id));
    } else if (OB_FAIL(table_schema->get_zone_list(schema_guard, zone_list_))) {
      LOG_WARN("fail to get zone list", K(ret), K(table_id));
    } else if (OB_FAIL(table_schema->get_paxos_replica_num(schema_guard, cur_paxos_num))) {
      LOG_WARN("fail to get paxos num", K(ret), K(table_id));
    } else if (OB_FAIL(table_schema->check_is_duplicated(schema_guard, compensate_readonly_all_server))) {
      LOG_WARN("fail to check duplicate scope cluter", K(ret), K(table_id));
    }
  } else {
    compensate_readonly_all_server = false;
    const ObTablegroupSchema* tg_schema = nullptr;
    if (OB_FAIL(schema_guard.get_tablegroup_schema(table_id, tg_schema))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), "tg_id", table_id);
    } else if (OB_ISNULL(tg_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid tg schema", K(ret), "tg_id", table_id);
    } else if (OB_FAIL(tg_schema->get_zone_replica_attr_array_inherit(schema_guard, actual_zone_locality))) {
      LOG_WARN("fail to get zone locality", K(ret), "tg_id", table_id);
    } else if (OB_FAIL(tg_schema->get_zone_list(schema_guard, zone_list_))) {
      LOG_WARN("fail to get zone list", K(ret), "tg_id", table_id);
    } else if (OB_FAIL(tg_schema->get_paxos_replica_num(schema_guard, cur_paxos_num))) {
      LOG_WARN("fail to get paxos num", K(ret), "tg_id", table_id);
    }
  }
  if (OB_SUCC(ret) && can_modify_quorum) {
    ObFilterLocalityUtility checker(*zone_mgr_, *tenant_stat_, zone_locality_, zone_list_);
    if (OB_FAIL(ObLocalityUtil::generate_designated_zone_locality(compensate_readonly_all_server,
            p.tablegroup_id_,
            p.all_pg_idx_,
            hash_index_collection,
            *tenant_stat_,
            actual_zone_locality,
            zone_locality_))) {
      LOG_WARN("fail to generate designated zone locality", K(ret));
    } else if (OB_FAIL(checker.init())) {
      LOG_WARN("fail to init locality checker", K(ret));
    } else if (OB_FAIL(checker.filter_locality(p))) {
      LOG_WARN("fail to filter locality", K(ret), K(p));
    } else if (checker.get_filter_result().count() <= 0 && p.get_quorum() != cur_paxos_num) {
      // locality match and quorum not match
      if (p.get_quorum() > cur_paxos_num) {
        new_quorum = p.get_quorum() - 1;
      } else if (p.get_quorum() < cur_paxos_num) {
        new_quorum = p.get_quorum() + 1;
      }
      can_modify_quorum = true;
    } else {
      // locality not match or quorum match
      can_modify_quorum = false;
    }
  }
  return ret;
}

int ObLocalityChecker::get_previous_locality_paxos_num(
    const uint64_t& tenant_id, const ObString& previous_locality, int64_t& pre_paxos_num)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObZoneReplicaAttrSet> pre_zone_locality;
  common::ObArray<common::ObZone> zones_in_pool;
  ObLocalityDistribution locality_dist;
  pre_paxos_num = OB_INVALID_COUNT;
  if (tenant_id <= 0 || previous_locality.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id or previous_lcoality is invalid", K(ret), K(tenant_id), K(previous_locality));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ is null", K(ret), KP(unit_mgr_));
  } else if (OB_FAIL(unit_mgr_->get_tenant_pool_zone_list(tenant_id, zones_in_pool))) {
    LOG_WARN("fail to get zones of pools", K(ret));
  } else if (OB_FAIL(locality_dist.init())) {
    LOG_WARN("fail to init locality dist", K(ret));
  } else if (OB_FAIL(locality_dist.parse_locality(previous_locality, zones_in_pool))) {
    LOG_WARN("fail to parse locality", K(ret));
  } else if (OB_FAIL(locality_dist.get_zone_replica_attr_array(pre_zone_locality))) {
    LOG_WARN("fail to get zone region replica num array", K(ret));
  } else if (OB_FAIL(ObLocalityCheckHelp::calc_paxos_replica_num(pre_zone_locality, pre_paxos_num))) {
    LOG_WARN("fail to calc paxos replica num", K(ret), K(pre_zone_locality));
  } else if (pre_paxos_num <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pre_paxos_num is invalid", K(ret), K(pre_paxos_num));
  }
  return ret;
}

int ObLocalityChecker::get_partition_locality(ObFilterLocalityUtility& checker, const Partition& partition,
    const balancer::HashIndexCollection& hash_index_collection)
{
  int ret = OB_SUCCESS;
  int64_t table_id = partition.table_id_;
  const uint64_t tenant_id = extract_tenant_id(table_id);
  bool locality_changed = true;
  ObSchemaGetterGuard schema_guard;
  zone_locality_.reset();
  zone_list_.reset();
  checker.reset();
  primary_zone_array_.reset();
  common::PageArena<> alloc;
  ObArray<ObZoneScore> zone_score_array;
  common::ObSEArray<share::ObZoneReplicaAttrSet, DEFAULT_ZONE_CNT> actual_zone_locality;
  bool compensate_readonly_all_server = false;
  if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema manager failed", K(ret));
  } else if (OB_ISNULL(tenant_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_stat is null", K(ret));
  } else if (OB_FAIL(tenant_stat_->check_table_locality_changed(schema_guard, table_id, locality_changed))) {
    LOG_WARN("fail to check if schema changed", K(ret));
  } else if (locality_changed) {
    ret = OB_EAGAIN;
    LOG_WARN("locality changed, generate task next round", K(ret));
  } else if (!is_tablegroup_id(table_id)) {
    const ObTableSchema* table_schema = nullptr;
    if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(table_id));
    } else if (OB_UNLIKELY(nullptr == table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid table schema", K(ret), K(table_id));
    } else if (OB_FAIL(table_schema->get_zone_replica_attr_array_inherit(schema_guard, actual_zone_locality))) {
      LOG_WARN("fail to get zone locality", K(ret), K(table_id));
    } else if (OB_FAIL(table_schema->get_zone_list(schema_guard, zone_list_))) {
      LOG_WARN("fail to get zone list", K(ret));
    } else if (OB_FAIL(ObSchemaUtils::get_primary_zone_array(alloc, *table_schema, schema_guard, zone_score_array))) {
      LOG_WARN("fail to get primary zone array", K(ret), K(table_id));
    } else if (OB_FAIL(table_schema->check_is_duplicated(schema_guard, compensate_readonly_all_server))) {
      LOG_WARN("fail to check duplicate scope cluter", K(ret), K(table_id));
    }
  } else {
    compensate_readonly_all_server = false;
    const ObTablegroupSchema* tg_schema = nullptr;
    if (OB_FAIL(schema_guard.get_tablegroup_schema(table_id, tg_schema))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), "tg_id", table_id);
    } else if (OB_UNLIKELY(nullptr == tg_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid tg schema", K(ret), "tg_id", table_id);
    } else if (OB_FAIL(tg_schema->get_zone_replica_attr_array_inherit(schema_guard, actual_zone_locality))) {
      LOG_WARN("fail to get zone locality", K(ret), "tg_id", table_id);
    } else if (OB_FAIL(tg_schema->get_zone_list(schema_guard, zone_list_))) {
      LOG_WARN("fail to get zone list", K(ret));
    } else if (OB_FAIL(ObSchemaUtils::get_primary_zone_array(alloc, *tg_schema, schema_guard, zone_score_array))) {
      LOG_WARN("fail to get primary zone array", K(ret), "tg_id", table_id);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLocalityUtil::generate_designated_zone_locality(compensate_readonly_all_server,
            partition.tablegroup_id_,
            partition.all_pg_idx_,
            hash_index_collection,
            *tenant_stat_,
            actual_zone_locality,
            zone_locality_))) {
      LOG_WARN("fail to generate designated zone locality", K(ret));
    } else if (OB_FAIL(checker.init())) {
      LOG_WARN("fail to init locality checker", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t first_score = 0;
    for (int64_t i = 0; i < zone_score_array.count() && OB_SUCC(ret); i++) {
      if (0 == first_score) {
        first_score = zone_score_array.at(i).score_;
      }
      if (zone_score_array.at(i).score_ == first_score) {
        if (OB_FAIL(primary_zone_array_.push_back(zone_score_array.at(i).zone_))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }  // end for
    if (OB_SUCC(ret)) {
      ObRegion region;
      ObSEArray<ObZone, DEFAULT_ZONE_CNT> zone_list;
      for (int64_t i = 0; i < primary_zone_array_.count() && OB_SUCC(ret); i++) {
        region.reset();
        zone_list.reset();
        if (OB_FAIL(zone_mgr_->get_region(primary_zone_array_.at(i), region))) {
          LOG_WARN("fail to get region", K(ret), "zone", primary_zone_array_.at(i));
        } else if (OB_FAIL(zone_mgr_->get_zone(region, zone_list))) {
          LOG_WARN("fail to get zone", K(ret), K(region));
        } else {
          for (int64_t j = 0; j < zone_list.count() && OB_SUCC(ret); j++) {
            if (!has_exist_in_array(primary_zone_array_, zone_list.at(j))) {
              if (OB_FAIL(primary_zone_array_.push_back(zone_list.at(j)))) {
                LOG_WARN("fail to push back", K(ret));
              }
            }
          }  // end for
        }    // end else
      }      // end for
    }        // end else
  }
  return ret;
}

int ObLocalityChecker::delete_redundant_replica(const Partition& partition, const Replica& replica,
    ObIArray<ObRemoveMemberTask>& remove_member_tasks, ObIArray<ObRemoveNonPaxosTask>& remove_non_paxos_replica_tasks,
    bool& check_pg_leader)
{
  int ret = OB_SUCCESS;
  // distinguish the paxos member and non-paxos member
  ObClusterInfo cluster_info;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (ObReplicaTypeCheck::is_paxos_replica_V2(replica.replica_type_) && !partition.in_physical_restore()) {
    // need to remove member
    if (OB_FAIL(remove_member(partition, replica, remove_member_tasks, check_pg_leader))) {
      LOG_WARN("fail to remove member", K(ret), K(replica));
    }
  } else if (OB_FAIL(remove_replica(partition, replica, remove_non_paxos_replica_tasks))) {
    LOG_WARN("fail to remove replica", K(ret), K(replica));
  }
  return ret;
}

int ObLocalityChecker::remove_member(const Partition& partition, const Replica& replica,
    ObIArray<ObRemoveMemberTask>& remove_member_tasks, bool& check_pg_leader)
{
  int ret = OB_SUCCESS;
  ObPartitionKey pkey;
  int64_t quorum = 0;
  const char* comment = balancer::LOCALITY_REMOVE_REDUNDANT_MEMBER;
  if (OB_ISNULL(replica.server_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica), K(partition));
  } else if (partition.get_quorum() <= 0) {
    // just skip
    LOG_DEBUG("partition quorum is invalid, just skip", K(partition));
  } else {
    ObAddr leader;
    ObRemoveMemberTaskInfo task_info;
    OnlineReplica remove_member;
    remove_member.member_ = ObReplicaMember(
        replica.server_->server_, replica.member_time_us_, replica.replica_type_, replica.memstore_percent_);
    remove_member.zone_ = replica.zone_;
    bool valid_leader = false;
    if (OB_FAIL(tenant_stat_->gen_partition_key(partition, pkey))) {
      LOG_WARN("fail to gen partition key", K(ret), K(partition), K(replica));
    } else if (OB_FAIL(tenant_stat_->get_remove_replica_quorum_size(
                   partition, replica.zone_, replica.replica_type_, quorum))) {
      LOG_WARN("fail to get quorum size", K(ret), K(partition), K(replica));
    } else if (OB_FAIL(task_info.build(remove_member, pkey, quorum, partition.get_quorum(), false /*check leader*/))) {
      LOG_WARN("fail to build remove member task info", K(ret));
    } else if (OB_ISNULL(tenant_stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant_stat is null", K(ret));
    } else if (OB_FAIL(tenant_stat_->get_leader_addr(partition, leader, valid_leader))) {
      LOG_WARN("fail to get leader addr", K(ret), K(task_info));
    } else if (!valid_leader) {
      LOG_WARN("invalid leader, maybe leader not exist, do it next round", K(partition));
    } else if (!leader.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid leader", K(ret), K(partition), K(leader));
    } else if (OB_ISNULL(task_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task_mgr is null", K(ret));
    } else if (OB_FAIL(ObRebalanceTaskUtil::build_batch_remove_member_task(
                   *task_mgr_, leader, task_info, comment, remove_member_tasks, check_pg_leader))) {
      LOG_WARN("fail to build batch_remove_member_task", K(ret), K(partition), K(task_info));
    }
  }
  return ret;
}

int ObLocalityChecker::modify_quorum(const Partition& partition, const Replica& replica, const int64_t& quorum,
    ObMemberList& member_list, ObIArray<ObModifyQuorumTask>& modify_quorum_tasks)
{
  int ret = OB_SUCCESS;
  ObPartitionKey pkey;
  const char* comment = balancer::LOCALITY_MODIFY_QUORUM;
  if (quorum <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("quorum is invalid", K(ret), K(partition), K(quorum));
  } else if (member_list.get_member_number() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("member_list is empty", K(ret), K(partition));
  } else if (OB_ISNULL(task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task_mgr is null", K(ret));
  } else {
    ObModifyQuorumTaskInfo task_info;
    OnlineReplica leader;
    leader.member_ = ObReplicaMember(
        replica.server_->server_, replica.member_time_us_, replica.replica_type_, replica.memstore_percent_);
    leader.zone_ = replica.zone_;
    if (OB_FAIL(tenant_stat_->gen_partition_key(partition, pkey))) {
      LOG_WARN("fail to gen partition key", K(ret), K(partition));
    } else if (OB_FAIL(task_info.build(leader, pkey, quorum, partition.get_quorum(), member_list))) {
      LOG_WARN("fail to build modify quorum task info", K(ret));
    } else if (OB_FAIL(ObRebalanceTaskUtil::build_batch_modify_quorum_task(
                   *task_mgr_, task_info, comment, modify_quorum_tasks))) {
      LOG_WARN("fail to build batch modify quorum task", K(ret), K(task_info));
    }
  }
  return ret;
}

int ObLocalityChecker::remove_replica(
    const Partition& partition, const Replica& replica, ObIArray<ObRemoveNonPaxosTask>& remove_non_paxos_replica_tasks)
{
  int ret = OB_SUCCESS;
  ObPartitionKey pkey;
  const char* comment = balancer::LOCALITY_REMOVE_REDUNDANT_REPLICA;
  if (OB_ISNULL(replica.server_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica), K(partition));
  } else if (OB_ISNULL(task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task_mgr is null", K(ret));
  } else {
    ObRemoveNonPaxosTaskInfo task_info;
    OnlineReplica remove_member;
    remove_member.member_ = ObReplicaMember(
        replica.server_->server_, replica.member_time_us_, replica.replica_type_, replica.memstore_percent_);
    remove_member.zone_ = replica.zone_;
    if (OB_FAIL(tenant_stat_->gen_partition_key(partition, pkey))) {
      LOG_WARN("fail to gen partition key", K(ret), K(partition), K(replica));
    } else if (OB_FAIL(task_info.build(remove_member, pkey))) {
      LOG_WARN("fail to build remove non paxos replica task info", K(ret));
    } else if (OB_FAIL(ObRebalanceTaskUtil::build_batch_remove_non_paxos_replica_task(
                   *task_mgr_, task_info, comment, remove_non_paxos_replica_tasks))) {
      LOG_WARN("fail to build batch remove non paxos task", K(ret));
    }
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
