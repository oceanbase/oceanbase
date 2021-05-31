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

#include "ob_rebalance_task_mgr.h"

#include "lib/lock/ob_mutex.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "share/ob_debug_sync.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "ob_rebalance_task_executor.h"
#include "rootserver/ob_root_balancer.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/restore/ob_restore_table_operator.h"
#include "rootserver/restore/ob_restore_info.h"
#include "rootserver/ob_balance_info.h"
#include "ob_rs_event_history_table_operator.h"
#include "ob_server_manager.h"
#include "share/ob_rpc_struct.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "ob_global_index_builder.h"
#include "share/ob_server_status.h"
#include "share/ob_multi_cluster_util.h"
#include "share/partition_table/ob_partition_table_proxy.h"
#include "share/partition_table/ob_partition_info.h"
#include "sql/executor/ob_executor_rpc_proxy.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "sql/executor/ob_executor_rpc_processor.h"

namespace oceanbase {
using namespace common;
using namespace lib;
using namespace obrpc;
using namespace share;
using namespace share::schema;
namespace rootserver {

static const char* rebalance_task_type_strs[] = {"MIGRATE_REPLICA",
    "ADD_REPLICA",
    "REBUILD_REPLICA",
    "TYPE_TRANSFORM",
    "MEMBER_CHANGE",
    "REMOVE_NON_PAXOS_REPLICA",
    "RESTORE_REPLICA",
    "MODIFY_QUORUM",
    "COPY_GLOBAL_INDEX_SSTABLE",  // not used
    "COPY_LOCAL_INDEX_SSTABLE",   // not used
    "COPY_SSTABLE",
    "SQL_BACKGROUND_DIST_TASK",
    "BACKUP_REPLICA",
    "PHYSICAL_RESTORE_REPLICA",
    "FAST_RECOVERY_TASK",
    "VALIDATE_BACKUP",
    "STANDBY_CUTDATA",
    "MAX_TYPE"};

bool ObRebalanceTaskKey::is_valid() const
{
  return key_type_ < RebalanceKeyType::REBALANCE_KEY_INVALID && key_type_ >= RebalanceKeyType::FORMAL_BALANCE_KEY;
}

bool ObRebalanceTaskKey::operator==(const ObRebalanceTaskKey& that) const
{
  return key_1_ == that.key_1_ && key_2_ == that.key_2_ && key_3_ == that.key_3_ && key_4_ == that.key_4_ &&
         key_type_ == that.key_type_;
}

ObRebalanceTaskKey& ObRebalanceTaskKey::operator=(const ObRebalanceTaskKey& that)
{
  key_1_ = that.key_1_;
  key_2_ = that.key_2_;
  key_3_ = that.key_3_;
  key_4_ = that.key_4_;
  key_type_ = that.key_type_;
  hash_value_ = that.hash_value_;
  return (*this);
}

uint64_t ObRebalanceTaskKey::hash() const
{
  return hash_value_;
}

int ObRebalanceTaskKey::init(const uint64_t key_1, const uint64_t key_2, const uint64_t key_3, const uint64_t key_4,
    const RebalanceKeyType key_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
          key_type >= RebalanceKeyType::REBALANCE_KEY_INVALID || key_type < RebalanceKeyType::FORMAL_BALANCE_KEY)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key_type));
  } else {
    key_1_ = key_1;
    key_2_ = key_2;
    key_3_ = key_3;
    key_4_ = key_4;
    key_type_ = key_type;
    hash_value_ = inner_hash();
  }
  return ret;
}

int ObRebalanceTaskKey::init(const ObRebalanceTaskKey& that)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!that.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    key_1_ = that.key_1_;
    key_2_ = that.key_2_;
    key_3_ = that.key_3_;
    key_3_ = that.key_4_;
    key_type_ = that.key_type_;
    hash_value_ = inner_hash();
  }
  return ret;
}

uint64_t ObRebalanceTaskKey::inner_hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&key_1_, sizeof(key_1_), hash_val);
  hash_val = murmurhash(&key_2_, sizeof(key_2_), hash_val);
  hash_val = murmurhash(&key_3_, sizeof(key_3_), hash_val);
  hash_val = murmurhash(&key_4_, sizeof(key_4_), hash_val);
  hash_val = murmurhash(&key_type_, sizeof(key_type_), hash_val);
  return hash_val;
}

bool ObRebalanceTaskInfo::is_consistency(
    const uint64_t tenant_id, const obrpc::ObAdminClearBalanceTaskArg::TaskType type) const
{
  return (OB_INVALID_ID == tenant_id || tenant_id == pkey_.get_tenant_id()) &&
         (type == op_type_ || type == obrpc::ObAdminClearBalanceTaskArg::ALL);
}

bool ObRebalanceTaskInfo::is_manual() const
{
  return op_type_ == obrpc::ObAdminClearBalanceTaskArg::MANUAL;
}

int ObRebalanceTaskInfo::set_src_server_stat(ObServerTaskStatMap::Item* src_server_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == src_server_stat)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(src_server_stat));
  } else {
    src_server_stat_ = src_server_stat;
    // src server stat
    src_server_stat_->ref();
    src_server_stat_->v_.total_out_cnt_++;
    src_server_stat_->v_.total_out_data_size_ += get_accumulate_transmit_src_data_size();
  }
  return ret;
}

int ObRebalanceTaskInfo::set_dst_server_stat(ObServerTaskStatMap::Item* dest_server_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == dest_server_stat)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(dest_server_stat));
  } else {
    dest_server_stat_ = dest_server_stat;
    // dest server stat
    dest_server_stat_->ref();
    dest_server_stat_->v_.total_in_cnt_++;
    dest_server_stat_->v_.total_in_data_size_ += get_accumulate_transmit_dst_data_size();
  }
  return ret;
}

int ObRebalanceTaskInfo::set_tenant_stat(ObTenantTaskStatMap::Item* tenant_stat, ObRebalanceTaskPriority task_priority)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == tenant_stat) || OB_UNLIKELY(task_priority < ObRebalanceTaskPriority::HIGH_PRI) ||
      OB_UNLIKELY(task_priority >= ObRebalanceTaskPriority::MAX_PRI)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(tenant_stat), K(task_priority));
  } else {
    tenant_stat_ = tenant_stat;
    // tenant stat
    tenant_stat_->ref();
    if (ObRebalanceTaskPriority::HIGH_PRI == task_priority) {
      tenant_stat_->v_.high_priority_task_cnt_++;
    } else {
      tenant_stat_->v_.low_priority_task_cnt_++;
    }
  }
  return ret;
}

void ObRebalanceTaskInfo::set_schedule()
{
  if (BACKUP_REPLICA == get_rebalance_task_type() || VALIDATE_BACKUP == get_rebalance_task_type()) {
    if (NULL != dest_server_stat_) {
      dest_server_stat_->v_.in_schedule_backup_cnt_++;
    }
  } else {
    if (NULL != src_server_stat_) {
      src_server_stat_->v_.in_schedule_out_cnt_++;
    }
    if (NULL != dest_server_stat_) {
      dest_server_stat_->v_.in_schedule_in_cnt_++;
    }
  }
  LOG_INFO("set task info to schedule", "task_info", *this);
}

int ObRebalanceTaskInfo::set_host(const ObRebalanceTask* host)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == host)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(host));
  } else {
    host_ = host;
  }
  return ret;
}

void ObRebalanceTaskInfo::clean_server_and_tenant_ref(const ObRebalanceTask& task)
{
  if (BACKUP_REPLICA == task.get_rebalance_task_type() || VALIDATE_BACKUP == task.get_rebalance_task_type()) {
    if (NULL != src_server_stat_) {
      src_server_stat_->v_.total_out_cnt_--;
      src_server_stat_->v_.total_out_data_size_ -= get_accumulate_transmit_src_data_size();
      src_server_stat_->unref();
      src_server_stat_ = NULL;
    }

    if (NULL != dest_server_stat_) {
      dest_server_stat_->v_.total_in_cnt_--;
      dest_server_stat_->v_.total_in_data_size_ -= get_accumulate_transmit_dst_data_size();
      if (task.in_schedule()) {
        dest_server_stat_->v_.in_schedule_backup_cnt_--;
      }
      dest_server_stat_->unref();
      dest_server_stat_ = NULL;
    }
  } else {
    if (NULL != src_server_stat_) {
      src_server_stat_->v_.total_out_cnt_--;
      src_server_stat_->v_.total_out_data_size_ -= get_accumulate_transmit_src_data_size();
      if (task.in_schedule()) {
        src_server_stat_->v_.in_schedule_out_cnt_--;
      }
      src_server_stat_->unref();
      src_server_stat_ = NULL;
    }
    if (NULL != dest_server_stat_) {
      dest_server_stat_->v_.total_in_cnt_--;
      dest_server_stat_->v_.total_in_data_size_ -= get_accumulate_transmit_dst_data_size();
      if (task.in_schedule()) {
        dest_server_stat_->v_.in_schedule_in_cnt_--;
      }
      dest_server_stat_->unref();
      dest_server_stat_ = NULL;
    }
  }

  if (NULL != tenant_stat_) {
    if (task.is_high_priority_task()) {
      tenant_stat_->v_.high_priority_task_cnt_--;
    } else {
      tenant_stat_->v_.low_priority_task_cnt_--;
    }
    tenant_stat_->unref();
    tenant_stat_ = NULL;
  }
}

int ObRebalanceTaskInfo::assign(const ObRebalanceTaskInfo& that)
{
  int ret = OB_SUCCESS;
  task_key_ = that.task_key_;
  pkey_ = that.pkey_;
  cluster_id_ = that.cluster_id_;
  transmit_data_size_ = that.transmit_data_size_;
  accumulate_src_transmit_data_size_ = that.accumulate_src_transmit_data_size_;
  accumulate_dst_transmit_data_size_ = that.accumulate_dst_transmit_data_size_;
  src_server_stat_ = that.src_server_stat_;
  dest_server_stat_ = that.dest_server_stat_;
  tenant_stat_ = that.tenant_stat_;
  sibling_in_schedule_ = that.sibling_in_schedule_;
  host_ = that.host_;
  op_type_ = that.op_type_;
  skip_change_member_list_ = that.skip_change_member_list_;
  switchover_epoch_ = that.switchover_epoch_;
  return ret;
}

int ObRebalanceTaskInfo::check_leader(
    const ObRebalanceTask& task, const share::ObPartitionInfo& partition, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  const ObPartitionReplica* replica = NULL;
  const common::ObPartitionKey& pkey = get_partition_key();
  bool has_partition = true;
  bool need_check = !partition.in_physical_restore();
  UNUSED(task);
  if (!need_check) {
    // by pass
  } else if (!partition.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition));
  } else if (OB_FAIL(partition.find_leader_by_election(replica))) {
    LOG_WARN("find leader failed", K(ret), K(partition));
  } else if (NULL == replica) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL replica returned", K(ret));
  } else if (need_switch_leader_before_execute(replica->server_)) {
    ObArray<ObAddr> excluded_servers;
    ObArray<ObZone> excluded_zones;
    if (OB_FAIL(excluded_servers.push_back(replica->server_))) {
      LOG_WARN("array push back failed", K(ret));
    } else if (OB_FAIL(excluded_zones.push_back(replica->zone_))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (nullptr == GCTX.root_service_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("root service ptr is null", K(ret));
    } else if (OB_FAIL(GCTX.root_service_->get_leader_coordinator().coordinate_partition_group(
                   pkey.get_table_id(), pkey.get_partition_id(), excluded_servers, excluded_zones))) {
      LOG_WARN("coordinate partition group leader failed", K(ret), "replica", *replica);
    } else {
      replica = NULL;
      ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
      ObPartitionInfo new_partition;
      new_partition.set_allocator(&allocator);
      if (nullptr == GCTX.pt_operator_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pt operator ptr is null", K(ret));
      } else if (OB_FAIL(GCTX.pt_operator_->get(pkey.get_table_id(), pkey.get_partition_id(), new_partition))) {
        LOG_WARN("get partition failed", K(ret), K(new_partition), "pkey", get_partition_key());
      } else if (OB_FAIL(new_partition.find_leader_by_election(replica))) {
        LOG_WARN("partition find leader failed", K(ret), K(new_partition));
      } else if (NULL == replica) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL replica returned", K(ret));
      } else if (!is_switch_leader_take_effect(replica->server_)) {
        ret = OB_REBALANCE_TASK_CANT_EXEC;
        LOG_WARN("still leader, can't execute task now", K(*this), "leader", replica->server_, K(ret));
      } else {
        leader = replica->server_;
      }
    }
  } else {
    leader = replica->server_;
  }
  return ret;
}

int ObRebalanceTaskInfo::update_with_partition(const common::ObAddr& dst) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rebalance task", K(ret), K(dst));
  } else if (nullptr == GCTX.root_service_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice ptr is null", K(ret));
  } else if (OB_FAIL(GCTX.root_service_->get_server_mgr().set_with_partition(dst))) {
    LOG_WARN("set with partition failed", K(ret), K(dst));
  }
  return ret;
}

int ObRebalanceTaskInfo::try_update_unit_id(
    const share::ObPartitionInfo& partition, const OnlineReplica& online_replica) const
{
  int ret = OB_SUCCESS;
  const ObPartitionReplica* replica = NULL;
  ret = partition.find(online_replica.get_server(), replica);
  if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("find replica failed", K(ret), "dest", online_replica.get_server());
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // replica not exist, insert log replica
    const ObZone& zone = online_replica.zone_;
    ObPartitionReplica replica;
    const int64_t dummy_quorum = -1;  // no need to fill in quorum and memberlist when migration
    if (OB_FAIL(ObIPartitionTable::gen_flag_replica(get_partition_key().get_table_id(),
            get_partition_key().get_partition_id(),
            0 /* partition cnt*/,
            online_replica.get_server(),
            zone,
            online_replica.unit_id_,
            online_replica.member_.get_replica_type(),
            100 /* memstore percent, 100 by default */,
            dummy_quorum,
            replica))) {
      LOG_WARN("gen log replica failed", K(ret), K(replica), K(zone), K(online_replica));
    } else if (nullptr == GCTX.pt_operator_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pt operator ptr is null", K(ret));
    } else if (OB_FAIL(GCTX.pt_operator_->update(replica))) {
      LOG_WARN("update log replica failed", K(ret), K(replica));
    }
  } else {  // OB_SUCCESS == ret
    // replica exist, try update unit id
    if (NULL == replica) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL replica", K(ret));
    } else if (replica->unit_id_ != online_replica.unit_id_) {
      if (nullptr == GCTX.pt_operator_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pt operator ptr is null", K(ret));
      } else if (OB_FAIL(GCTX.pt_operator_->set_unit_id(get_partition_key().get_table_id(),
                     get_partition_key().get_partition_id(),
                     online_replica.get_server(),
                     online_replica.unit_id_))) {
        LOG_WARN("set unit id failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRebalanceTaskInfo::force_check_partition_table(const share::ObPartitionInfo& partition) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("svr_rpc_proxy ptr is null", K(ret));
  } else {
    obrpc::ObReportSingleReplicaArg arg;
    arg.partition_key_ = get_partition_key();
    int64_t replica_count = partition.replica_count();
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < replica_count; ++i) {
      const ObAddr& target_addr = partition.get_replicas_v2().at(i).server_;
      tmp_ret = GCTX.srv_rpc_proxy_->to(target_addr).report_single_replica(arg);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to force leader report replica", K(tmp_ret), K(target_addr), "pkey", arg.partition_key_);
      }
    }
  }
  return ret;
}

bool ObRebalanceTaskInfo::can_accumulate_in_standby(const ObRebalanceTaskInfo& other) const
{
  bool bret = false;
  // the private partitions and non-private partitions shall not be accumulated in on task
  // specify the first task info of the task to be the guard
  const bool is_cluster_private_table =
      ObMultiClusterUtil::is_cluster_private_table(other.get_partition_key().get_table_id());
  const bool task_is_private_table = ObMultiClusterUtil::is_cluster_private_table(pkey_.get_table_id());
  if (is_cluster_private_table == task_is_private_table) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}
int ObRebalanceTaskInfo::set_skip_change_member_list(const ObRebalanceTaskType task_type,
    const common::ObReplicaType& src_type, const common::ObReplicaType& dst_type, const bool is_restore)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = pkey_.get_tenant_id();
  if (MAX_TYPE <= task_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task type is invalid", K(ret), K(task_type));
  } else if (!pkey_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pkey is not valid, may task not init", K(ret), K(pkey_));
  } else if (REBUILD_REPLICA == task_type || REMOVE_NON_PAXOS_REPLICA == task_type || RESTORE_REPLICA == task_type ||
             PHYSICAL_RESTORE_REPLICA == task_type || MODIFY_QUORUM == task_type ||
             COPY_GLOBAL_INDEX_SSTABLE == task_type || COPY_LOCAL_INDEX_SSTABLE == task_type ||
             COPY_SSTABLE == task_type || SQL_BACKGROUND_DIST_TASK == task_type || BACKUP_REPLICA == task_type ||
             VALIDATE_BACKUP == task_type || STANDBY_CUTDATA == task_type) {
    skip_change_member_list_ = true;
  } else if (MEMBER_CHANGE == task_type) {
    skip_change_member_list_ = false;
  } else if (is_restore) {
    skip_change_member_list_ = true;
  } else if (MIGRATE_REPLICA == task_type || ADD_REPLICA == task_type) {
    if (common::REPLICA_TYPE_MAX <= dst_type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("dst member is in valid", K(ret), K(task_type), K(dst_type));
    } else {
      // no need to modify memberlist when the destination is a non-paxos replica
      const bool is_valid_paxos_replica = ObReplicaTypeCheck::is_paxos_replica_V2(dst_type);
      if (is_valid_paxos_replica) {
        skip_change_member_list_ = false;
      } else {
        skip_change_member_list_ = true;
      }
    }
  } else if (TYPE_TRANSFORM == task_type) {
    if (common::REPLICA_TYPE_MAX <= src_type || common::REPLICA_TYPE_MAX <= dst_type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("dst or src is invalid", K(ret), K(src_type), K(dst_type));
    } else {
      if (ObReplicaTypeCheck::is_paxos_replica_V2(dst_type) != ObReplicaTypeCheck::is_paxos_replica_V2(src_type)) {
        // need to modify the member list since the quorum is changed
        skip_change_member_list_ = false;
      } else {
        skip_change_member_list_ = true;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted rebalance task", K(ret), K(task_type));
  }
  return ret;
}

int ObFastRecoveryTaskInfo::build(const common::ObAddr& src, const common::ObAddr& dst,
    const common::ObAddr& rescue_server, const uint64_t tenant_id, const int64_t file_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src.is_valid() || !dst.is_valid() || !rescue_server.is_valid() || OB_INVALID_ID == tenant_id ||
                  file_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src), K(dst), K(rescue_server), K(tenant_id), K(file_id));
  } else if (OB_FAIL(
                 task_key_.init(tenant_id, file_id, 0 /*ignore*/, OB_INVALID_ID, RebalanceKeyType::FAST_RECOVER_KEY))) {
    LOG_WARN("fail to init task key", K(ret));
  } else if (OB_FAIL(pkey_.init(tenant_id, file_id, 0 /*part_cnt*/))) {
    LOG_WARN("fail to init pkey", K(ret), K(tenant_id), K(file_id));
  } else {
    src_ = src;
    dst_ = dst;
    tenant_id_ = tenant_id;
    file_id_ = file_id;
  }
  return ret;
}

int ObFastRecoveryTaskInfo::assign(const ObFastRecoveryTaskInfo& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTaskInfo::assign(that))) {
    LOG_WARN("fail to assign base info", K(ret));
  } else {
    src_ = that.src_;
    dst_ = that.dst_;
    tenant_id_ = that.tenant_id_;
    file_id_ = that.file_id_;
  }
  return ret;
}

int ObFastRecoveryTaskInfo::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = 0;
  return ret;
}

int ObFastRecoveryTaskInfo::check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
    const bool is_admin_force, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(task);
  UNUSED(partition);
  UNUSED(is_admin_force);
  UNUSED(leader);
  if (GCTX.get_switch_epoch2() != switchover_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is change",
        K(ret),
        "switch epoch",
        GCTX.get_switch_epoch2(),
        "task switchover epoch",
        switchover_epoch_,
        "task_info",
        *this);
  }
  return ret;
}

int ObFastRecoveryTaskInfo::get_virtual_rebalance_task_stat_info(
    common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const
{
  int ret = OB_SUCCESS;
  src = src_;
  data_src = src_;
  dest = dst_;
  UNUSED(offline);
  return ret;
}

int ObMigrateTaskInfo::build(const obrpc::MigrateMode migrate_mode, const OnlineReplica& dst,
    const common::ObPartitionKey& pkey, const common::ObReplicaMember& src, const common::ObReplicaMember& data_src,
    const int64_t quorum, const bool is_restore, const bool is_manual)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dst.is_valid() || !pkey.is_valid() || !src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst), K(pkey), K(src));
  } else if (OB_FAIL(task_key_.init(pkey.get_table_id(),
                 pkey.get_partition_id(),
                 0 /* ignore */,
                 OB_INVALID_ID,
                 RebalanceKeyType::FORMAL_BALANCE_KEY))) {
    LOG_WARN("fail to build task key", K(ret), K(pkey));
  } else {
    ObReplicaMember empty_member;
    if (empty_member == data_src || obrpc::MigrateMode::MT_SINGLE_ZONE_MODE == migrate_mode) {
      data_src_ = src;
    } else {
      data_src_ = data_src;
    }
    dst_ = dst;
    pkey_ = pkey;
    src_ = src;
    op_type_ = is_manual ? ObAdminClearBalanceTaskArg::MANUAL : ObAdminClearBalanceTaskArg::AUTO;
    quorum_ = quorum;
    if (OB_FAIL(set_skip_change_member_list(
            MIGRATE_REPLICA, src.get_replica_type(), dst_.member_.get_replica_type(), is_restore))) {
      LOG_WARN("failed to set skip change member list", K(ret), K(src), K(dst_));
    } else {
      LOG_INFO("build migrate task info", "this", *this);
    }
  }
  return ret;
}

int ObMigrateTaskInfo::assign(const ObMigrateTaskInfo& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTaskInfo::assign(that))) {
    LOG_WARN("fail to assign bass info", K(ret));
  } else {
    dst_ = that.dst_;
    src_ = that.src_;
    data_src_ = that.data_src_;
    quorum_ = that.quorum_;
  }
  return ret;
}

int ObTypeTransformTaskInfo::build(const OnlineReplica& dst, const common::ObPartitionKey& pkey,
    const common::ObReplicaMember& src, const common::ObReplicaMember& data_src, const int64_t quorum,
    const bool is_restore, const bool is_manual)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dst.is_valid() || !pkey.is_valid() || !src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst), K(pkey), K(src));
  } else {
    bool is_restore = false;
    const uint64_t tenant_id = pkey.get_tenant_id();
    if (OB_FAIL(GSCHEMASERVICE.check_tenant_is_restore(NULL, tenant_id, is_restore))) {
      LOG_WARN("fail to check tenant is restore", K(ret), K(tenant_id));
    } else if (is_restore) {
      // skip
    } else if (OB_UNLIKELY(quorum <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("quorum num is invalid", K(ret), K(quorum));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(task_key_.init(pkey.get_table_id(),
                 pkey.get_partition_id(),
                 0 /*ignore*/,
                 OB_INVALID_ID,
                 RebalanceKeyType::FORMAL_BALANCE_KEY))) {
    LOG_WARN("fail to build task key", K(ret), K(pkey));
  } else {
    dst_ = dst;
    pkey_ = pkey;
    src_ = src;  // with a replica type in it
    data_src_ = data_src;
    quorum_ = quorum;
    op_type_ = is_manual ? ObAdminClearBalanceTaskArg::MANUAL : ObAdminClearBalanceTaskArg::AUTO;
    if (OB_FAIL(set_skip_change_member_list(
            TYPE_TRANSFORM, src_.get_replica_type(), dst_.member_.get_replica_type(), is_restore))) {
      LOG_WARN("failed to set skip change member list", K(ret), K(src_), K(dst_));
    } else {
      LOG_INFO("build type transform task info", "this", *this);
    }
  }
  return ret;
}

int ObTypeTransformTaskInfo::assign(const ObTypeTransformTaskInfo& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTaskInfo::assign(that))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    dst_ = that.dst_;
    src_ = that.src_;
    data_src_ = that.data_src_;
    quorum_ = that.quorum_;
  }
  return ret;
}

int ObAddTaskInfo::build(const OnlineReplica& dst, const common::ObPartitionKey& pkey,
    const common::ObReplicaMember& data_src, const int64_t quorum, const bool is_restore, const bool is_manual)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dst.is_valid() || !pkey.is_valid() || !data_src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst), K(pkey), K(data_src));
  } else if (OB_FAIL(task_key_.init(pkey.get_table_id(),
                 pkey.get_partition_id(),
                 0 /*ignore*/,
                 OB_INVALID_ID,
                 RebalanceKeyType::FORMAL_BALANCE_KEY))) {
    LOG_WARN("fail to build task key", K(ret), K(pkey));
  } else {
    dst_ = dst;
    pkey_ = pkey;
    data_src_ = data_src;
    quorum_ = quorum;
    op_type_ = is_manual ? ObAdminClearBalanceTaskArg::MANUAL : ObAdminClearBalanceTaskArg::AUTO;
    if (OB_FAIL(set_skip_change_member_list(
            ADD_REPLICA, data_src.get_replica_type(), dst_.member_.get_replica_type(), is_restore))) {
      LOG_WARN("failed to set skip change member list", K(ret), K(data_src), K(dst_));
    } else {
      LOG_INFO("build add replica task info", "this", *this);
    }
  }
  return ret;
}

int ObAddTaskInfo::assign(const ObAddTaskInfo& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTaskInfo::assign(that))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    dst_ = that.dst_;
    data_src_ = that.data_src_;
    quorum_ = that.quorum_;
  }
  return ret;
}

int ObRebuildTaskInfo::build(const OnlineReplica& dst, const common::ObPartitionKey& pkey,
    const common::ObReplicaMember& data_src, const bool is_manual)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dst.is_valid() || !pkey.is_valid() || !data_src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst), K(pkey), K(data_src));
  } else if (OB_FAIL(task_key_.init(pkey.get_table_id(),
                 pkey.get_partition_id(),
                 0 /*ignore*/,
                 OB_INVALID_ID,
                 RebalanceKeyType::FORMAL_BALANCE_KEY))) {
    LOG_WARN("fail to build task key", K(ret), K(pkey));
  } else {
    dst_ = dst;
    pkey_ = pkey;
    data_src_ = data_src;
    op_type_ = is_manual ? ObAdminClearBalanceTaskArg::MANUAL : ObAdminClearBalanceTaskArg::AUTO;
    if (OB_FAIL(set_skip_change_member_list(
            REBUILD_REPLICA, data_src_.get_replica_type(), dst_.member_.get_replica_type()))) {
      LOG_WARN("failed to set skip change member list", K(ret), K(data_src_), K(dst_));
    } else {
      LOG_INFO("build rebuild replica task info", "this", *this);
    }
  }
  return ret;
}

int ObRebuildTaskInfo::assign(const ObRebuildTaskInfo& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTaskInfo::assign(that))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    dst_ = that.dst_;
    data_src_ = that.data_src_;
  }
  return ret;
}

int ObModifyQuorumTaskInfo::build(const OnlineReplica& dst, const common::ObPartitionKey& pkey, const int64_t quorum,
    const int64_t orig_quorum, ObMemberList& member_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid() || quorum <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey), K(quorum));
  } else if (OB_FAIL(task_key_.init(pkey.get_table_id(),
                 pkey.get_partition_id(),
                 0 /*ignore*/,
                 OB_INVALID_ID,
                 RebalanceKeyType::FORMAL_BALANCE_KEY))) {
    LOG_WARN("fail to build task key", K(ret), K(pkey));
  } else {
    dst_ = dst;
    pkey_ = pkey;
    quorum_ = quorum;
    orig_quorum_ = orig_quorum;
    member_list_ = member_list;
    op_type_ = ObAdminClearBalanceTaskArg::AUTO;
    const common::ObReplicaType src_type = common::REPLICA_TYPE_MAX;  // no used
    if (OB_FAIL(set_skip_change_member_list(MODIFY_QUORUM, src_type, dst_.member_.get_replica_type()))) {
      LOG_WARN("failed to set skip change member list", K(ret), K(src_type), K(dst_));
    } else {
      LOG_INFO("build modify quorum task info", "this", *this);
    }
  }
  return ret;
}

int ObModifyQuorumTaskInfo::assign(const ObModifyQuorumTaskInfo& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTaskInfo::assign(that))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    dst_ = that.dst_;
    quorum_ = that.quorum_;
    orig_quorum_ = that.orig_quorum_;
    member_list_ = that.member_list_;
  }
  return ret;
}

int ObRestoreTaskInfo::build(
    const common::ObPartitionKey& pkey, const share::ObRestoreArgs& restore_arg, const OnlineReplica& dst)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid() || !restore_arg.is_valid() || !dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey), K(restore_arg), K(dst));
  } else if (OB_FAIL(task_key_.init(pkey.get_table_id(),
                 pkey.get_partition_id(),
                 0 /*ignore*/,
                 OB_INVALID_ID,
                 RebalanceKeyType::FORMAL_BALANCE_KEY))) {
    LOG_WARN("fail to build task key", K(ret), K(pkey));
  } else {
    pkey_ = pkey;
    dst_ = dst;
    restore_arg_ = restore_arg;
    op_type_ = ObAdminClearBalanceTaskArg::AUTO;
    const common::ObReplicaType src_type = common::REPLICA_TYPE_MAX;  // no used
    if (OB_FAIL(set_skip_change_member_list(RESTORE_REPLICA, src_type, dst_.member_.get_replica_type()))) {
      LOG_WARN("failed to set skip change member list", K(ret), K(src_type), K(dst_));
    } else {
      LOG_INFO("build restore task info", "this", *this);
    }
  }
  return ret;
}

int ObRestoreTaskInfo::assign(const ObRestoreTaskInfo& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTaskInfo::assign(that))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    dst_ = that.dst_;
    restore_arg_ = that.restore_arg_;
  }
  return ret;
}

int ObPhysicalRestoreTaskInfo::build(
    const common::ObPartitionKey& pkey, const share::ObPhysicalRestoreArg& restore_arg, const OnlineReplica& dst)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid() || !restore_arg.is_valid() || !dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey), K(restore_arg), K(dst));
  } else if (OB_FAIL(task_key_.init(pkey.get_table_id(),
                 pkey.get_partition_id(),
                 0 /*ignore*/,
                 OB_INVALID_ID,
                 RebalanceKeyType::FORMAL_BALANCE_KEY))) {
    LOG_WARN("fail to build task key", K(ret), K(pkey));
  } else if (OB_FAIL(restore_arg_.assign(restore_arg))) {
    LOG_WARN("fail to assign restore_arg", K(ret), K(restore_arg));
  } else {
    pkey_ = pkey;
    dst_ = dst;
    op_type_ = ObAdminClearBalanceTaskArg::AUTO;
    const common::ObReplicaType src_type = common::REPLICA_TYPE_MAX;  // no used
    if (OB_FAIL(set_skip_change_member_list(PHYSICAL_RESTORE_REPLICA, src_type, dst_.member_.get_replica_type()))) {
      LOG_WARN("failed to set skip change member list", K(ret), K(src_type), K(dst_));
    } else {
      LOG_INFO("build physical restore task info", "this", *this);
    }
  }
  return ret;
}

int ObPhysicalRestoreTaskInfo::assign(const ObPhysicalRestoreTaskInfo& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTaskInfo::assign(that))) {
    LOG_WARN("fail to assign", K(ret));
  } else if (OB_FAIL(restore_arg_.assign(that.restore_arg_))) {
    LOG_WARN("fail to assign restore_arg", K(ret), K(that));
  } else {
    dst_ = that.dst_;
  }
  return ret;
}

int ObCopySSTableTaskInfo::build(const common::ObPartitionKey& pkey, const OnlineReplica& dst,
    const common::ObReplicaMember& data_src, const int64_t index_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid() || !dst.is_valid() || !data_src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey), K(dst), K(data_src));
  } else if (OB_FAIL(task_key_.init(pkey.get_table_id(),
                 pkey.get_partition_id(),
                 0 /*ignore*/,
                 index_table_id,
                 RebalanceKeyType::FORMAL_BALANCE_KEY))) {
    LOG_WARN("fail to build task key", K(ret), K(pkey));
  } else {
    pkey_ = pkey;
    dst_ = dst;
    data_src_ = data_src;
    index_table_id_ = index_table_id;
    if (OB_FAIL(
            set_skip_change_member_list(COPY_SSTABLE, data_src_.get_replica_type(), dst_.member_.get_replica_type()))) {
      LOG_WARN("failed to set skip change member list", K(ret), K(data_src_), K(dst_));
    } else {
      LOG_INFO("build copy sstable task info", "this", *this);
    }
  }
  return ret;
}

int ObCopySSTableTaskInfo::assign(const ObCopySSTableTaskInfo& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTaskInfo::assign(that))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    dst_ = that.dst_;
    data_src_ = that.data_src_;
    index_table_id_ = that.index_table_id_;
  }
  return ret;
}

int ObRemoveMemberTaskInfo::build(const OnlineReplica& dst, const common::ObPartitionKey& pkey, const int64_t quorum,
    const int64_t orig_quorum, const bool check_leader)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dst.is_valid() || !pkey.is_valid() || quorum <= 0 || orig_quorum <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst), K(pkey), K(quorum), K(orig_quorum));
  } else if (OB_FAIL(task_key_.init(pkey.get_table_id(),
                 pkey.get_partition_id(),
                 0 /*ignore*/,
                 OB_INVALID_ID,
                 RebalanceKeyType::FORMAL_BALANCE_KEY))) {
    LOG_WARN("fail to build task key", K(ret), K(pkey));
  } else {
    dst_ = dst;
    pkey_ = pkey;
    quorum_ = quorum;
    orig_quorum_ = orig_quorum;
    check_leader_ = check_leader;
    const common::ObReplicaType src_type = common::REPLICA_TYPE_MAX;  // no used
    if (OB_FAIL(set_skip_change_member_list(MEMBER_CHANGE, src_type, dst_.member_.get_replica_type()))) {
      LOG_WARN("failed to set skip change member list", K(ret), K(src_type), K(dst_));
    } else {
      LOG_INFO("build remove member task info", "this", *this);
    }
  }
  return ret;
}

int ObRemoveMemberTaskInfo::assign(const ObRemoveMemberTaskInfo& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTaskInfo::assign(that))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    dst_ = that.dst_;
    quorum_ = that.quorum_;
    orig_quorum_ = that.orig_quorum_;
    check_leader_ = that.check_leader_;
  }
  return ret;
}

int ObRemoveNonPaxosTaskInfo::build(const OnlineReplica& dst, const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dst.is_valid() || !pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst), K(pkey));
  } else if (OB_FAIL(task_key_.init(pkey.get_table_id(),
                 pkey.get_partition_id(),
                 0 /*ignore*/,
                 OB_INVALID_ID,
                 RebalanceKeyType::FORMAL_BALANCE_KEY))) {
    LOG_WARN("fail to build task key", K(ret), K(pkey));
  } else {
    pkey_ = pkey;
    dst_ = dst;
    const common::ObReplicaType src_type = common::REPLICA_TYPE_MAX;  // no used
    if (OB_FAIL(set_skip_change_member_list(REMOVE_NON_PAXOS_REPLICA, src_type, dst_.member_.get_replica_type()))) {
      LOG_WARN("failed to set skip change member list", K(ret), K(src_type), K(dst_));
    } else {
      LOG_INFO("build remove non paxos replica task info", "this", *this);
    }
  }
  return ret;
}

int ObRemoveNonPaxosTaskInfo::assign(const ObRemoveNonPaxosTaskInfo& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTaskInfo::assign(that))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    dst_ = that.dst_;
  }
  return ret;
}

int ObSqlBKGDDistTaskInfo::build(const sql::ObSchedBKGDDistTask& bkgd_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!bkgd_task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(bkgd_task));
  } else if (OB_FAIL(task_key_.init(bkgd_task.get_task_id().get_execution_id(),
                 bkgd_task.get_task_id().get_job_id(),
                 bkgd_task.get_task_id().get_task_id(),
                 OB_INVALID_ID,
                 RebalanceKeyType::SQL_BKG_DIST_KEY))) {
    LOG_WARN("fail to build task key", K(ret), K(bkgd_task));
  } else if (OB_FAIL(sql_bkgd_task_.assign(bkgd_task))) {
    LOG_WARN("fail to assign sql backgroup task", K(ret), K(bkgd_task));
  } else {
    pkey_ = bkgd_task.get_partition_key();
    dst_ = bkgd_task.get_dest();
    const common::ObReplicaType src_type = common::REPLICA_TYPE_MAX;  // no used
    const common::ObReplicaType dst_type = common::REPLICA_TYPE_MAX;  // no used
    if (OB_FAIL(set_skip_change_member_list(SQL_BACKGROUND_DIST_TASK, src_type, dst_type))) {
      LOG_WARN("failed to set skip change member list", K(ret), K(src_type), K(dst_type));
    } else {
      LOG_INFO("build sql bkgd dist task info", "this", *this);
    }
  }
  return ret;
}

int ObSqlBKGDDistTaskInfo::assign(const ObSqlBKGDDistTaskInfo& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTaskInfo::assign(that))) {
    LOG_WARN("fail to assign", K(ret));
  } else if (OB_FAIL(sql_bkgd_task_.assign(that.sql_bkgd_task_))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    dst_ = that.dst_;
  }
  return ret;
}

int ObBackupTaskInfo::build(const common::ObPartitionKey& pkey, const OnlineReplica& dst,
    const common::ObReplicaMember& data_src, const share::ObPhysicalBackupArg& physical_backup_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid() || !dst.is_valid() || !data_src.is_valid()) || (!physical_backup_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey), K(dst), K(data_src), K(physical_backup_arg));
  } else if (OB_FAIL(task_key_.init(pkey.get_table_id(),
                 pkey.get_partition_id(),
                 0 /*ignore*/,
                 OB_INVALID_ID,
                 RebalanceKeyType::BACKUP_MANAGER_KEY))) {
    LOG_WARN("fail to build task key", K(ret), K(pkey));
  } else {
    pkey_ = pkey;
    dst_ = dst;
    data_src_ = data_src;
    physical_backup_arg_ = physical_backup_arg;
    if (OB_FAIL(set_skip_change_member_list(
            BACKUP_REPLICA, data_src_.get_replica_type(), dst_.member_.get_replica_type()))) {
      LOG_WARN("failed to set skip change member list", K(ret), K(data_src_), K(dst_));
    } else {
      LOG_INFO("build backup task info", "this", *this);
    }
  }
  return ret;
}

int ObBackupTaskInfo::assign(const ObBackupTaskInfo& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTaskInfo::assign(that))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    dst_ = that.dst_;
    data_src_ = that.data_src_;
    physical_backup_arg_ = that.physical_backup_arg_;
  }
  return ret;
}

int ObValidateTaskInfo::build(const common::ObPartitionKey& pkey, const common::ObReplicaMember& dst,
    const share::ObPhysicalValidateArg& physical_validate_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid() || !physical_validate_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey), K(physical_validate_arg));
  } else if (OB_FAIL(task_key_.init(physical_validate_arg.job_id_,
                 physical_validate_arg.backup_set_id_,
                 pkey.get_table_id(),
                 pkey.get_partition_id(),
                 RebalanceKeyType::FORMAL_BALANCE_KEY))) {
    LOG_WARN("failed to build task key", K(ret), K(pkey));
  } else {
    pkey_ = pkey;
    dst_ = dst;
    physical_validate_arg_ = physical_validate_arg;
  }
  return ret;
}

int ObValidateTaskInfo::assign(const ObValidateTaskInfo& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTaskInfo::assign(that))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    physical_validate_arg_ = that.physical_validate_arg_;
    dst_ = that.dst_;
    addr_ = that.addr_;
  }
  return ret;
}

int ObMigrateTaskInfo::check_quorum(const share::ObPartitionInfo& partition) const
{
  int ret = OB_SUCCESS;
  const ObPartitionReplica* leader = nullptr;
  bool need_check = !partition.in_physical_restore();
  if (!need_check) {
    // by pass
  } else if (nullptr == GCTX.schema_service_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service ptr is null", K(ret));
  } else if (OB_FAIL(partition.find_leader_by_election(leader))) {
    LOG_WARN("fail to get leader", K(ret));
  } else if (OB_ISNULL(leader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader is null", K(ret));
  } else if (leader->quorum_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("quorum is invalid", K(ret), KPC(leader));
  } else if (leader->quorum_ != quorum_) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    LOG_WARN("quorum changed, generate task next round", K(ret), "quorum", leader->quorum_, K(*this));
  }
  return ret;
}

int ObMigrateTaskInfo::check_online_replica(const share::ObPartitionInfo& partition) const
{
  int ret = OB_SUCCESS;
  const ObPartitionReplica* replica = nullptr;
  int tmp_ret = partition.find(dst_.get_server(), replica);
  if (OB_ENTRY_NOT_EXIST == tmp_ret) {
    // good
  } else if (OB_SUCCESS != tmp_ret) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find replica", K(ret));
  } else if (nullptr == replica) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find replica", K(ret));
  } else if (REPLICA_STATUS_NORMAL == replica->replica_status_) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    LOG_WARN("cannot online normal replica already exist", K(ret), "dst", dst_.get_server(), K(partition));
  }
  return ret;
}

int ObMigrateTaskInfo::check_offline_replica(const share::ObPartitionInfo& partition) const
{
  int ret = OB_SUCCESS;
  const ObPartitionReplica* replica = NULL;
  if (OB_FAIL(partition.find(src_.get_server(), replica))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;  // may be replica exists only in memberlist
      LOG_INFO(
          "no replica find, may be replica exists only in memberlist", K(ret), "src", src_.get_server(), K(partition));
    } else {
      LOG_WARN("find replica failed", K(ret), "src", src_.get_server(), K(partition));
    }
  } else if (OB_ISNULL(replica)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL replica returned", K(ret));
  }
  return ret;
}

/* migrate replica check before execute
 * 1 check quorum
 * 2 check online
 * 3 check offline
 * 4 update with partition
 * 5 check leader
 */
int ObMigrateTaskInfo::check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
    const bool is_admin_force, common::ObAddr& dummy) const
{
  int ret = OB_SUCCESS;
  UNUSED(is_admin_force);
  if (GCTX.get_switch_epoch2() != switchover_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is change",
        K(ret),
        "switch epoch",
        GCTX.get_switch_epoch2(),
        "task switchover epoch",
        switchover_epoch_,
        "task_info",
        *this);
  } else if (OB_FAIL(check_quorum(partition))) {
    LOG_WARN("fail to check quorum", K(ret));
  } else if (OB_FAIL(check_online_replica(partition))) {
    LOG_WARN("fail to check online replica", K(ret));
  } else if (OB_FAIL(check_offline_replica(partition))) {
    LOG_WARN("fail to check offline replica", K(ret));
  } else if (OB_FAIL(update_with_partition(dst_.get_server()))) {
    LOG_WARN("fail to update with partition", K(ret));
  } else if (OB_FAIL(check_leader(task, partition, dummy))) {
    LOG_WARN("fail to check leader", K(ret));
  } else if (OB_FAIL(try_update_unit_id(partition, dst_))) {
    LOG_WARN("fail to try update unit_id", K(ret));
  }
  if (OB_REBALANCE_TASK_CANT_EXEC == ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = force_check_partition_table(partition))) {
      LOG_WARN("fail to force check partition table", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObTypeTransformTaskInfo::check_quorum(const share::ObPartitionInfo& partition) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = pkey_.get_tenant_id();
  share::schema::ObSchemaGetterGuard schema_guard;
  bool need_check = !partition.in_physical_restore();
  if (!need_check) {
    // The non-private table of the standby database does not need to check the quorum value
  } else if (nullptr == GCTX.schema_service_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service ptr is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else {
    const ObPartitionReplica* leader = NULL;
    if (OB_FAIL(partition.find_leader_by_election(leader))) {
      LOG_WARN("fail to get leader", K(ret), K(partition));
    } else if (OB_ISNULL(leader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("leader is null", K(ret));
    } else if (leader->quorum_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("quorum is invalid", K(ret), KPC(leader));
    } else {
      int64_t quorum = 0;
      ObQuorumGetter quorum_getter(
          schema_guard, GCTX.root_service_->get_unit_mgr(), GCTX.root_service_->get_zone_mgr());
      if (OB_FAIL(quorum_getter.get_transform_quorum_size(partition.get_table_id(),
              leader->quorum_,
              get_dest().zone_,
              get_src_member().get_replica_type(),
              get_dest().member_.get_replica_type(),
              quorum))) {
        LOG_WARN("fail to calculate quorum", K(ret), K(leader));
      } else if (quorum_ != quorum) {
        ret = OB_REBALANCE_TASK_CANT_EXEC;
        LOG_WARN("quorum changed, generate task next round", K(ret), KPC(leader), K(quorum));
      }
    }
  }
  return ret;
}

int ObTypeTransformTaskInfo::check_online_replica(const share::ObPartitionInfo& partition) const
{
  int ret = OB_SUCCESS;
  const ObPartitionReplica* replica = nullptr;
  int tmp_ret = partition.find(dst_.get_server(), replica);
  if (OB_ENTRY_NOT_EXIST == tmp_ret) {
    // good
  } else if (OB_SUCCESS != tmp_ret) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find replica", K(ret));
  } else if (nullptr == replica) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find replica", K(ret));
  } else if (replica->is_paxos_candidate() && !replica->is_in_service()) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    LOG_WARN("cannot online normal replica already exist", K(ret), "dst", dst_.get_server(), K(partition));
  }
  return ret;
}

int ObTypeTransformTaskInfo::check_paxos_member(const share::ObPartitionInfo& partition) const
{
  int ret = OB_SUCCESS;
  bool need_check = !partition.in_physical_restore();
  if (!need_check) {
    // by pass
  } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(get_dest().member_.get_replica_type())) {
    // nothing todo
  } else {
    const ObZone& dest_zone = dst_.zone_;
    FOREACH_CNT_X(r, partition.get_replicas_v2(), OB_SUCC(ret))
    {
      if (OB_ISNULL(r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid replica", K(ret), K(partition));
      } else if (r->server_ == dst_.get_server()) {
        // nothing todo
      } else if (r->zone_ == dest_zone && r->is_in_service() &&
                 ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_) &&
                 r->replica_type_ == get_dest().member_.get_replica_type()) {
        ret = OB_REBALANCE_TASK_CANT_EXEC;
        LOG_WARN("one zone can only have one same paxos member",
            K(ret),
            K(*r),
            K(*this),
            "dest_zone",
            dest_zone,
            "dest",
            dst_.get_server());
      } else {
      }  // no more to do
    }
  }
  return ret;
}

/* type transform check before execute
 * 1 check quorum
 * 2 check online
 * 3 check paxos member
 * 4 update with partition
 * 5 check leader
 */
int ObTypeTransformTaskInfo::check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
    const bool is_admin_force, common::ObAddr& dummy) const
{
  int ret = OB_SUCCESS;
  UNUSED(is_admin_force);
  if (GCTX.get_switch_epoch2() != switchover_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is change",
        K(ret),
        "switch epoch",
        GCTX.get_switch_epoch2(),
        "task switchover epoch",
        switchover_epoch_,
        "task_info",
        *this);
  } else if (OB_FAIL(check_quorum(partition))) {
    LOG_WARN("fail to check quorum", K(ret));
  } else if (OB_FAIL(check_online_replica(partition))) {
    LOG_WARN("fail to check replica");
  } else if (OB_FAIL(check_paxos_member(partition))) {
    LOG_WARN("fail to check paxos member", K(ret));
  } else if (OB_FAIL(update_with_partition(dst_.get_server()))) {
    LOG_WARN("fail to update with partition", K(ret));
  } else if (OB_FAIL(check_leader(task, partition, dummy))) {
    LOG_WARN("fail to check leader", K(ret));
  } else if (OB_FAIL(try_update_unit_id(partition, dst_))) {
    LOG_WARN("fail to try update unit_id", K(ret));
  }
  if (OB_REBALANCE_TASK_CANT_EXEC == ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = force_check_partition_table(partition))) {
      LOG_WARN("fail to force check partition table", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObAddTaskInfo::check_quorum(const share::ObPartitionInfo& partition, const bool is_admin_force) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = pkey_.get_tenant_id();
  share::schema::ObSchemaGetterGuard schema_guard;
  bool need_check = !partition.in_physical_restore();
  if (!need_check) {
    // by pass
  } else if (nullptr == GCTX.schema_service_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service ptr is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else {
    const ObPartitionReplica* leader = NULL;
    if (OB_FAIL(partition.find_leader_by_election(leader))) {
      LOG_WARN("fail to get leader", K(ret), K(partition));
    } else if (OB_ISNULL(leader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("leader is null", K(ret));
    } else if (leader->quorum_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("quorum is invalid", K(ret), KPC(leader));
    } else {
      int64_t quorum = 0;
      if (is_admin_force) {
        // no need to change quorum for alter system admin command
        quorum = leader->quorum_;
        // particularly, for the situation single replica force recovery,
        // need to 'alter sytem add replica force' command,
        // we need to try to modify quorum at the same time
        int64_t paxos_num = OB_INVALID_COUNT;
        if (!is_tablegroup_id(partition.get_table_id())) {
          const share::schema::ObTableSchema* schema = NULL;
          if (OB_FAIL(schema_guard.get_table_schema(partition.get_table_id(), schema))) {
            LOG_WARN("get table schema failed", K(ret), "table", partition.get_table_id());
          } else if (OB_ISNULL(schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get invalid table schema", K(ret));
          } else if (OB_FAIL(schema->get_paxos_replica_num(schema_guard, paxos_num))) {
            LOG_WARN("fail to get paxos replica num", K(ret), K(schema));
          } else if (paxos_num <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid paxos num", K(ret), K(paxos_num));
          } else {
            quorum = min(leader->quorum_ + 1, paxos_num);
          }
        } else {
          const share::schema::ObTablegroupSchema* tg_schema = nullptr;
          if (OB_FAIL(schema_guard.get_tablegroup_schema(partition.get_table_id(), tg_schema))) {
            LOG_WARN("get tg schema failed", K(ret), "tg_id", partition.get_table_id());
          } else if (OB_UNLIKELY(nullptr == tg_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get invalid tg schema", K(ret));
          } else if (OB_FAIL(tg_schema->get_paxos_replica_num(schema_guard, paxos_num))) {
            LOG_WARN("fail to get paxos replica num", K(ret), KP(tg_schema));
          } else if (paxos_num <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid paxos num", K(ret), K(paxos_num));
          } else {
            quorum = min(leader->quorum_ + 1, paxos_num);
          }
        }
      } else {
        ObQuorumGetter quorum_getter(
            schema_guard, GCTX.root_service_->get_unit_mgr(), GCTX.root_service_->get_zone_mgr());
        if (OB_FAIL(quorum_getter.get_add_replica_quorum_size(partition.get_table_id(),
                leader->quorum_,
                get_dest().zone_,
                get_dest().member_.get_replica_type(),
                quorum))) {
          LOG_WARN("fail to calculate quorum", K(ret), K(leader));
        }
      }
    }
  }
  return ret;
}

int ObAddTaskInfo::check_online_replica(const share::ObPartitionInfo& partition) const
{
  int ret = OB_SUCCESS;
  const ObPartitionReplica* replica = nullptr;
  int tmp_ret = partition.find(dst_.get_server(), replica);
  if (OB_ENTRY_NOT_EXIST == tmp_ret) {
    // good
  } else if (OB_SUCCESS != tmp_ret) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find replica", K(ret));
  } else if (nullptr == replica) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find replica", K(ret));
  } else if (REPLICA_STATUS_NORMAL == replica->replica_status_) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    LOG_WARN("cannot online normal replica already exist", K(ret), "dst", dst_.get_server(), K(partition));
  }
  return ret;
}

int ObAddTaskInfo::check_paxos_member(const share::ObPartitionInfo& partition) const
{
  int ret = OB_SUCCESS;
  bool need_check = !partition.in_physical_restore();
  if (!need_check) {
    // by pass
  } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(get_dest().member_.get_replica_type())) {
    // nothing todo
  } else {
    const ObZone& dest_zone = dst_.zone_;
    FOREACH_CNT_X(r, partition.get_replicas_v2(), OB_SUCC(ret))
    {
      if (OB_ISNULL(r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid replica", K(ret), K(partition));
      } else if (r->server_ == dst_.get_server()) {
        // nothing todo
      } else if (r->zone_ == dest_zone && r->is_in_service() &&
                 ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_) &&
                 r->replica_type_ == get_dest().member_.get_replica_type()) {
        ret = OB_REBALANCE_TASK_CANT_EXEC;
        LOG_WARN("one zone can only have one same paxos member",
            K(ret),
            K(*r),
            K(*this),
            "dest_zone",
            dest_zone,
            "dest",
            dst_.get_server());
      } else {
      }  // no more to do
    }
  }
  return ret;
}

/* Add replica check before execute:
 * 1 check quorum
 * 2 check online
 * 3 check paxos member
 * 4 update with partition
 */
int ObAddTaskInfo::check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
    const bool is_admin_force, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(task);
  UNUSED(is_admin_force);
  UNUSED(leader);
  if (GCTX.get_switch_epoch2() != switchover_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is change",
        K(ret),
        "switch epoch",
        GCTX.get_switch_epoch2(),
        "task switchover epoch",
        switchover_epoch_,
        "task_info",
        *this);
  } else if (OB_FAIL(check_quorum(partition, is_admin_force))) {
    LOG_WARN("fail to check quorum", K(ret));
  } else if (OB_FAIL(check_online_replica(partition))) {
    LOG_WARN("fail to check online replica", K(ret));
  } else if (OB_FAIL(check_paxos_member(partition))) {
    LOG_WARN("fail to check paxos member", K(ret));
  } else if (OB_FAIL(update_with_partition(dst_.get_server()))) {
    LOG_WARN("fail to update with partition", K(ret));
  } else if (OB_FAIL(try_update_unit_id(partition, dst_))) {
    LOG_WARN("fail to try update unit_id", K(ret));
  }
  if (OB_REBALANCE_TASK_CANT_EXEC == ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = force_check_partition_table(partition))) {
      LOG_WARN("fail to force check partition table", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObRebuildTaskInfo::check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
    const bool is_admin_force, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(task);
  UNUSED(partition);
  UNUSED(is_admin_force);
  UNUSED(leader);
  if (GCTX.get_switch_epoch2() != switchover_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is change",
        K(ret),
        "switch epoch",
        GCTX.get_switch_epoch2(),
        "task switchover epoch",
        switchover_epoch_,
        "task_info",
        *this);
  }
  return ret;
}

int ObModifyQuorumTaskInfo::check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
    const bool is_admin_force, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  // check nothing
  UNUSED(task);
  UNUSED(partition);
  UNUSED(is_admin_force);
  UNUSED(leader);
  if (GCTX.get_switch_epoch2() != switchover_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is change",
        K(ret),
        "switch epoch",
        GCTX.get_switch_epoch2(),
        "task switchover epoch",
        switchover_epoch_,
        "task_info",
        *this);
  }
  return ret;
}

/* restore check before execute
 * 1 update with partition
 */
int ObRestoreTaskInfo::check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
    const bool is_admin_force, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(task);
  UNUSED(is_admin_force);
  UNUSED(leader);
  if (GCTX.get_switch_epoch2() != switchover_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is change",
        K(ret),
        "switch epoch",
        GCTX.get_switch_epoch2(),
        "task switchover epoch",
        switchover_epoch_,
        "task_info",
        *this);
  } else if (OB_FAIL(update_with_partition(dst_.get_server()))) {
    LOG_WARN("fail update with partition", K(ret));
  } else if (OB_FAIL(try_update_unit_id(partition, dst_))) {
    LOG_WARN("fail to try update unit_id", K(ret));
  }
  return ret;
}

int ObPhysicalRestoreTaskInfo::check_before_execute(const ObRebalanceTask& task,
    const share::ObPartitionInfo& partition, const bool is_admin_force, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(task);
  UNUSED(is_admin_force);
  UNUSED(leader);
  if (GCTX.get_switch_epoch2() != switchover_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is change",
        K(ret),
        "switch epoch",
        GCTX.get_switch_epoch2(),
        "task switchover epoch",
        switchover_epoch_,
        "task_info",
        *this);
  } else if (OB_FAIL(update_with_partition(dst_.get_server()))) {
    LOG_WARN("fail update with partition", K(ret));
  } else if (OB_FAIL(try_update_unit_id(partition, dst_))) {
    LOG_WARN("fail to try update unit_id", K(ret));
  }
  return ret;
}

int ObCopySSTableTaskInfo::check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
    const bool is_admin_force, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(task);
  UNUSED(partition);
  UNUSED(is_admin_force);
  UNUSED(leader);
  if (GCTX.get_switch_epoch2() != switchover_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is change",
        K(ret),
        "switch epoch",
        GCTX.get_switch_epoch2(),
        "task switchover epoch",
        switchover_epoch_,
        "task_info",
        *this);
  }
  return ret;
}

/* remove member check before execute:
 * 1 check leader
 */
int ObRemoveMemberTaskInfo::check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
    const bool is_admin_force, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(is_admin_force);
  if (GCTX.get_switch_epoch2() != switchover_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is change",
        K(ret),
        "switch epoch",
        GCTX.get_switch_epoch2(),
        "task switchover epoch",
        switchover_epoch_,
        "task_info",
        *this);
  } else if (OB_FAIL(check_leader(task, partition, leader))) {
    LOG_WARN("fail to check leader", K(ret));
  }
  return ret;
}

int ObRemoveNonPaxosTaskInfo::check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
    const bool is_admin_force, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(task);
  UNUSED(partition);
  UNUSED(is_admin_force);
  UNUSED(leader);
  if (GCTX.get_switch_epoch2() != switchover_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is change",
        K(ret),
        "switch epoch",
        GCTX.get_switch_epoch2(),
        "task switchover epoch",
        switchover_epoch_,
        "task_info",
        *this);
  }
  return ret;
}

int ObSqlBKGDDistTaskInfo::check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
    const bool is_admin_force, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(task);
  UNUSED(partition);
  UNUSED(is_admin_force);
  UNUSED(leader);
  if (GCTX.get_switch_epoch2() != switchover_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is change",
        K(ret),
        "switch epoch",
        GCTX.get_switch_epoch2(),
        "task switchover epoch",
        switchover_epoch_,
        "task_info",
        *this);
  }
  return ret;
}

int ObBackupTaskInfo::check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
    const bool is_admin_force, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(task);
  UNUSED(partition);
  UNUSED(is_admin_force);
  UNUSED(leader);
  if (GCTX.get_switch_epoch2() != switchover_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is change",
        K(ret),
        "switch epoch",
        GCTX.get_switch_epoch2(),
        "task switchover epoch",
        switchover_epoch_,
        "task_info",
        *this);
  }
  return ret;
}

int ObValidateTaskInfo::check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
    const bool is_admin_force, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(task);
  UNUSED(partition);
  UNUSED(is_admin_force);
  UNUSED(leader);
  if (GCTX.get_switch_epoch2() != switchover_epoch_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task status is change",
        K(ret),
        "switch epoch",
        GCTX.get_switch_epoch2(),
        "task switchover epoch",
        switchover_epoch_,
        "task_info",
        *this);
  }
  return ret;
}

int ObMigrateTaskInfo::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  if (REPLICA_TYPE_FULL == dst_.member_.get_replica_type() ||
      REPLICA_TYPE_READONLY == dst_.member_.get_replica_type()) {
    execute_transmit_size = transmit_data_size_;
  } else if (REPLICA_TYPE_LOGONLY == dst_.member_.get_replica_type()) {
    execute_transmit_size = 0;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid type", K(ret), K(*this));
  }
  return ret;
}

int ObTypeTransformTaskInfo::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  if (REPLICA_TYPE_LOGONLY == dst_.member_.get_replica_type()) {
    execute_transmit_size = 0;
  } else if (REPLICA_TYPE_FULL == dst_.member_.get_replica_type()) {
    if (REPLICA_TYPE_READONLY == src_.get_replica_type()) {
      execute_transmit_size = 0;
    } else if (REPLICA_TYPE_LOGONLY == src_.get_replica_type()) {
      execute_transmit_size = transmit_data_size_;
    } else if (REPLICA_TYPE_FULL == src_.get_replica_type()) {
      if (dst_.member_.get_memstore_percent() == src_.get_memstore_percent()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Memstore_percent conversion between D and F must be changed", K(ret), K(*this));
      } else {
        execute_transmit_size = 0;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type", K(ret), K(*this));
    }
  } else if (REPLICA_TYPE_READONLY == dst_.member_.get_replica_type()) {
    if (REPLICA_TYPE_FULL == src_.get_replica_type()) {
      execute_transmit_size = 0;
    } else if (REPLICA_TYPE_LOGONLY == src_.get_replica_type()) {
      execute_transmit_size = transmit_data_size_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid type", K(ret), K(*this));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid type", K(ret), K(*this));
  }
  return ret;
}

int ObAddTaskInfo::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  if (REPLICA_TYPE_FULL == dst_.member_.get_replica_type() ||
      REPLICA_TYPE_READONLY == dst_.member_.get_replica_type()) {
    execute_transmit_size = transmit_data_size_;
  } else if (REPLICA_TYPE_LOGONLY == dst_.member_.get_replica_type()) {
    execute_transmit_size = 0;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid type", K(ret), K(*this));
  }
  return ret;
}

/* rebuild set execute_transmit_size to 0 deliberately
 */
int ObRebuildTaskInfo::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = 0;
  return ret;
}

int ObModifyQuorumTaskInfo::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = 0;
  return ret;
}

int ObRestoreTaskInfo::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = transmit_data_size_;
  return ret;
}

int ObPhysicalRestoreTaskInfo::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = transmit_data_size_;
  return ret;
}

int ObCopySSTableTaskInfo::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = transmit_data_size_;
  return ret;
}

int ObRemoveMemberTaskInfo::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = transmit_data_size_;
  return ret;
}

int ObRemoveNonPaxosTaskInfo::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = transmit_data_size_;
  return ret;
}

int ObSqlBKGDDistTaskInfo::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = transmit_data_size_;
  return ret;
}

int ObBackupTaskInfo::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = transmit_data_size_;
  return ret;
}

int ObValidateTaskInfo::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = transmit_data_size_;
  return ret;
}

int ObMigrateTaskInfo::get_virtual_rebalance_task_stat_info(
    common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const
{
  int ret = OB_SUCCESS;
  src = src_.get_server();
  data_src = data_src_.get_server();
  dest = dst_.get_server();
  offline = src_.get_server();
  return ret;
}

int ObTypeTransformTaskInfo::get_virtual_rebalance_task_stat_info(
    common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const
{
  int ret = OB_SUCCESS;
  src = src_.get_server();
  data_src = data_src_.get_server();
  dest = dst_.get_server();
  UNUSED(offline);
  return ret;
}

int ObAddTaskInfo::get_virtual_rebalance_task_stat_info(
    common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const
{
  int ret = OB_SUCCESS;
  src = data_src_.get_server();
  data_src = data_src_.get_server();
  dest = dst_.get_server();
  UNUSED(offline);
  return ret;
}

int ObRebuildTaskInfo::get_virtual_rebalance_task_stat_info(
    common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const
{
  int ret = OB_SUCCESS;
  src = data_src_.get_server();
  data_src = data_src_.get_server();
  dest = dst_.get_server();
  UNUSED(offline);
  return ret;
}

int ObModifyQuorumTaskInfo::get_virtual_rebalance_task_stat_info(
    common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const
{
  int ret = OB_SUCCESS;
  UNUSED(src);
  UNUSED(data_src);
  dest = dst_.get_server();
  UNUSED(offline);
  return ret;
}

int ObRestoreTaskInfo::get_virtual_rebalance_task_stat_info(
    common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const
{
  int ret = OB_SUCCESS;
  UNUSED(src);
  UNUSED(data_src);
  dest = dst_.get_server();
  UNUSED(offline);
  return ret;
}

int ObPhysicalRestoreTaskInfo::get_virtual_rebalance_task_stat_info(
    common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const
{
  int ret = OB_SUCCESS;
  UNUSED(src);
  UNUSED(data_src);
  dest = dst_.get_server();
  UNUSED(offline);
  return ret;
}

int ObCopySSTableTaskInfo::get_virtual_rebalance_task_stat_info(
    common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const
{
  int ret = OB_SUCCESS;
  src = data_src_.get_server();
  data_src = data_src_.get_server();
  dest = dst_.get_server();
  UNUSED(offline);
  return ret;
}

int ObRemoveMemberTaskInfo::get_virtual_rebalance_task_stat_info(
    common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const
{
  int ret = OB_SUCCESS;
  UNUSED(src);
  UNUSED(data_src);
  dest = dst_.get_server();
  UNUSED(offline);
  return ret;
}

int ObRemoveNonPaxosTaskInfo::get_virtual_rebalance_task_stat_info(
    common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const
{
  int ret = OB_SUCCESS;
  UNUSED(src);
  UNUSED(data_src);
  dest = dst_.get_server();
  UNUSED(offline);
  return ret;
}

int ObSqlBKGDDistTaskInfo::get_virtual_rebalance_task_stat_info(
    common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const
{
  int ret = OB_SUCCESS;
  UNUSED(src);
  UNUSED(data_src);
  dest = dst_;
  UNUSED(offline);
  return ret;
}

int ObBackupTaskInfo::get_virtual_rebalance_task_stat_info(
    common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const
{
  int ret = OB_SUCCESS;
  src = data_src_.get_server();
  data_src = data_src_.get_server();
  dest = dst_.get_server();
  UNUSED(offline);
  return ret;
}

int ObValidateTaskInfo::get_virtual_rebalance_task_stat_info(
    common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const
{
  int ret = OB_SUCCESS;
  UNUSED(src);
  UNUSED(data_src);
  UNUSED(dest);
  UNUSED(offline);
  return ret;
}

bool ObRebalanceTask::is_admin_cmd() const
{
  return 0 == STRCMP(comment_, balancer::ADMIN_MIGRATE_REPLICA) || 0 == STRCMP(comment_, balancer::ADMIN_ADD_REPLICA) ||
         0 == STRCMP(comment_, balancer::ADMIN_REMOVE_MEMBER) ||
         0 == STRCMP(comment_, balancer::ADMIN_REMOVE_REPLICA) || 0 == STRCMP(comment_, balancer::ADMIN_TYPE_TRANSFORM);
}

int ObRebalanceTask::generate_sub_task_err_code(const int err, common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  rc_array.reset();
  int64_t sub_task_cnt = get_sub_task_count();
  for (int64_t i = 0; OB_SUCC(ret) && i < sub_task_cnt; ++i) {
    if (OB_FAIL(rc_array.push_back(err))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int ObRebalanceTask::set_server_and_tenant_stat(
    ObServerTaskStatMap& server_stat_map, ObTenantTaskStatMap& tenant_stat_map)
{
  int ret = OB_SUCCESS;
  AddrTaskMap src_server_map;
  AddrTaskMap dst_server_map;
  const int64_t bucket_num = 128;

  if (OB_UNLIKELY(!server_stat_map.is_inited() || !tenant_stat_map.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        "server map inited",
        server_stat_map.is_inited(),
        "tenant map inited",
        tenant_stat_map.is_inited());
  } else if (OB_FAIL(src_server_map.create(bucket_num, ObModIds::OB_REBALANCE_TASK_MGR))) {
    LOG_WARN("fail to create src server map", K(ret));
  } else if (OB_FAIL(dst_server_map.create(bucket_num, ObModIds::OB_REBALANCE_TASK_MGR))) {
    LOG_WARN("fail to create des server map", K(ret));
  } else if (OB_FAIL(set_tenant_stat(src_server_map, dst_server_map, tenant_stat_map))) {
    LOG_WARN("fail to set tenant stat", K(ret));
  } else if (OB_FAIL(set_server_stat(src_server_map, dst_server_map, server_stat_map))) {
    LOG_WARN("fail to set server stat", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObRebalanceTask::set_tenant_stat(
    AddrTaskMap& src_server_map, AddrTaskMap& dst_server_map, ObTenantTaskStatMap& tenant_stat_map)
{
  int ret = OB_SUCCESS;
  // the task_infos in one task are executed serialy on observer,
  // so each task occupys one thread even if there multiple task infos in one task.
  // however, the data source for each task info in the task may differs
  // we accumulate the data_size of all task_infos in map
  for (int64_t i = 0; OB_SUCC(ret) && i < get_sub_task_count(); ++i) {
    ObTenantTaskStatMap::Item* tenant_stat = NULL;
    ObRebalanceTaskInfo* task_info = get_sub_task(i);
    // this branch will update the tenant_stat_map, and record the task_info in both
    // src_server_map and dst_server_map.
    // src_server_map and dst_server_map will update server_stat_map afterwards
    if (OB_UNLIKELY(nullptr == task_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task info ptr is null", K(ret));
    } else {
      if (task_info->need_check_specific_src_data_volumn()) {
        const common::ObAddr& data_src = task_info->get_specific_server_data_src();
        if (!data_src.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret));
        } else if (OB_FAIL(accumulate_src_addr_server_map(src_server_map, data_src, *task_info))) {
          LOG_WARN("fail to try update src server map", K(ret), K(data_src), "task_info", *task_info);
        }
      }
      if (OB_SUCC(ret) && task_info->need_check_specific_dst_data_volumn()) {
        const common::ObAddr& data_dst = task_info->get_specific_server_data_dst();
        if (!data_dst.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret));
        } else if (OB_FAIL(accumulate_dst_addr_server_map(dst_server_map, data_dst, *task_info))) {
          LOG_WARN("fail to try update dst server map", K(ret), K(data_dst), "task_info", *task_info);
        }
      }
      if (OB_SUCC(ret)) {
        const uint64_t tenant_id = task_info->get_tenant_id();
        if (OB_INVALID_ID == tenant_id) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret));
        } else if (OB_FAIL(tenant_stat_map.locate(tenant_id, tenant_stat))) {
          LOG_WARN("fail to get tenant stat item", K(ret));
        } else if (OB_UNLIKELY(nullptr == tenant_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null tenant stat ptr", K(ret), KP(tenant_stat));
        } else if (OB_FAIL(task_info->set_tenant_stat(tenant_stat, priority_))) {
          LOG_WARN("fail to set tenant stat", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRebalanceTask::set_server_stat(
    const AddrTaskMap& src_server_map, const AddrTaskMap& dst_server_map, ObServerTaskStatMap& server_stat_map)
{
  int ret = OB_SUCCESS;
  for (AddrTaskMap::const_iterator iter = src_server_map.begin(); OB_SUCC(ret) && iter != src_server_map.end();
       ++iter) {
    ObServerTaskStatMap::Item* src_server_stat = NULL;
    const common::ObAddr& addr = iter->first;
    ObRebalanceTaskInfo* task_info = iter->second;
    if (OB_UNLIKELY(NULL == task_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task info is null", K(ret), KP(task_info));
    } else if (OB_FAIL(server_stat_map.locate(addr, src_server_stat))) {
      LOG_WARN("fail to locate src server stat", K(ret), K(addr));
    } else if (OB_UNLIKELY(NULL == src_server_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src server stat is null", K(ret), KP(src_server_stat));
    } else if (OB_FAIL(task_info->set_src_server_stat(src_server_stat))) {
      LOG_WARN("fail to set src server stat", K(ret));
    } else {
    }  // no more to do
  }
  for (AddrTaskMap::const_iterator iter = dst_server_map.begin(); OB_SUCC(ret) && iter != dst_server_map.end();
       ++iter) {
    ObServerTaskStatMap::Item* dst_server_stat = NULL;
    const common::ObAddr& addr = iter->first;
    ObRebalanceTaskInfo* task_info = iter->second;
    if (OB_UNLIKELY(NULL == task_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task info is null", K(ret), KP(task_info));
    } else if (OB_FAIL(server_stat_map.locate(addr, dst_server_stat))) {
      LOG_WARN("fail to locate dst server stat", K(ret), K(addr));
    } else if (OB_UNLIKELY(NULL == dst_server_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dst server stat is null", K(ret), KP(dst_server_stat));
    } else if (OB_FAIL(task_info->set_dst_server_stat(dst_server_stat))) {
      LOG_WARN("fail to set dst server stat", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObRebalanceTask::accumulate_src_addr_server_map(
    AddrTaskMap& addr_server_map, const common::ObAddr& src_addr, ObRebalanceTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_addr));
  } else {
    // accumualte the data size for each task info with the same source
    ObRebalanceTaskInfo* task_info_in_map = NULL;
    if (OB_FAIL(addr_server_map.get_refactored(src_addr, task_info_in_map))) {
      if (OB_HASH_NOT_EXIST == ret) {
        const int overwrite = 0;
        // the first appearance of this server, set accumulated size to zero
        task_info.accumulate_transmit_src_data_size(0);
        if (OB_FAIL(addr_server_map.set_refactored(src_addr, &task_info, overwrite))) {
          LOG_WARN("fail to set to map", K(ret), K(src_addr));
        } else {
        }  // no more to do
      } else {
        LOG_WARN("fail to get from map", K(ret));
      }
    } else if (OB_UNLIKELY(NULL == task_info_in_map)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task info ptr NULL", K(ret), KP(task_info_in_map));
    } else {
      const int overwrite = 1;
      task_info.accumulate_transmit_src_data_size(task_info_in_map->get_accumulate_transmit_src_data_size());
      if (OB_FAIL(addr_server_map.set_refactored(src_addr, &task_info, overwrite))) {
        LOG_WARN("fail to set to map", K(ret), K(src_addr));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObRebalanceTask::accumulate_dst_addr_server_map(
    AddrTaskMap& addr_server_map, const common::ObAddr& dst_addr, ObRebalanceTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dst_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst_addr));
  } else {
    // accumualte the data size for each task info with the same destination
    ObRebalanceTaskInfo* task_info_in_map = NULL;
    if (OB_FAIL(addr_server_map.get_refactored(dst_addr, task_info_in_map))) {
      if (OB_HASH_NOT_EXIST == ret) {
        const int overwrite = 0;
        // the first appearance of this server, set accumulated size to zero
        task_info.accumulate_transmit_dst_data_size(0);
        if (OB_FAIL(addr_server_map.set_refactored(dst_addr, &task_info, overwrite))) {
          LOG_WARN("fail to set to map", K(ret), K(dst_addr));
        } else {
        }  // no more to do
      } else {
        LOG_WARN("fail to get from map", K(ret));
      }
    } else if (OB_UNLIKELY(NULL == task_info_in_map)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task info ptr NULL", K(ret), KP(task_info_in_map));
    } else {
      const int overwrite = 1;
      task_info.accumulate_transmit_dst_data_size(task_info_in_map->get_accumulate_transmit_dst_data_size());
      if (OB_FAIL(addr_server_map.set_refactored(dst_addr, &task_info, overwrite))) {
        LOG_WARN("fail to set to map", K(ret), K(dst_addr));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObRebalanceTask::set_task_id(const common::ObAddr& addr)
{
  int ret = OB_SUCCESS;
  task_id_.init(addr);
  return ret;
}

int ObRebalanceTask::deep_copy(const ObRebalanceTask& that)
{
  int ret = OB_SUCCESS;
  // generate_time_ will not be copied, the generate_time
  // is automatically set in the constructor func
  priority_ = that.priority_;
  comment_ = that.comment_;
  schedule_time_ = that.schedule_time_;
  executor_time_ = that.executor_time_;
  task_id_ = that.task_id_;
  dst_ = that.dst_;
  return ret;
}

void ObRebalanceTask::clean_server_and_tenant_ref(TaskInfoMap& task_info_map)
{
  UNUSED(task_info_map);
  for (int64_t i = 0; i < get_sub_task_count(); ++i) {
    ObRebalanceTaskInfo* this_task_info = get_sub_task(i);
    if (nullptr != this_task_info) {
      this_task_info->clean_server_and_tenant_ref(*this);
    }
  }
}

void ObRebalanceTask::clean_task_info_map(TaskInfoMap& task_info_map)
{
  for (int64_t i = 0; i < get_sub_task_count(); ++i) {
    int tmp_ret = OB_SUCCESS;
    ObRebalanceTaskInfo* task_info = NULL;
    const ObRebalanceTaskInfo* this_task_info = get_sub_task(i);
    if (nullptr == this_task_info) {
      // bypass
    } else {
      const ObRebalanceTaskKey& task_key = this_task_info->get_task_key();
      if (OB_SUCCESS != (tmp_ret = task_info_map.get_refactored(task_key, task_info))) {
        // do not exist in task info map, ignore
      } else if (NULL == task_info) {
        // null task info, ignore
      } else if (this != task_info->get_host()) {
        // do not belong to this task, ignore
      } else if (OB_SUCCESS != (tmp_ret = task_info_map.erase_refactored(task_key))) {
        LOG_WARN("fail to remove task info from map", K(tmp_ret));
      }
    }
  }
}

const char* ObRebalanceTask::get_task_type_str(const ObRebalanceTaskType task_type)
{
  static_assert(ObRebalanceTaskType::MAX_TYPE == ARRAYSIZEOF(rebalance_task_type_strs) - 1,
      "type str array size mismatch with type count");

  const char* task_type_str = NULL;
  if (task_type >= ObRebalanceTaskType::MIGRATE_REPLICA && task_type < ObRebalanceTaskType::MAX_TYPE) {
    task_type_str = rebalance_task_type_strs[static_cast<int64_t>(task_type)];
  }
  return task_type_str;
}

bool ObRebalanceTask::can_batch_get_partition_info() const
{
  bool can_batch = true;
  const ObRebalanceTaskInfo* sample_task_info = nullptr;
  if (get_sub_task_count() <= 1) {
    can_batch = false;
  } else if (nullptr == (sample_task_info = get_sub_task(0))) {
    can_batch = false;
  } else {
    uint64_t sample_tenant_id = sample_task_info->get_tenant_id();
    for (int64_t i = 0; can_batch && i < get_sub_task_count(); ++i) {
      const ObRebalanceTaskInfo* task_info = get_sub_task(i);
      if (nullptr == task_info) {
        can_batch = false;
      } else {
        const ObPartitionKey& pkey = task_info->get_partition_key();
        if (pkey.get_tenant_id() == common::OB_SYS_TENANT_ID) {
          can_batch = false;
        } else if (sample_tenant_id != pkey.get_tenant_id()) {
          can_batch = false;
        } else if (is_sys_table(pkey.get_table_id())) {
          can_batch = false;
        }
      }
    }
  }
  return can_batch;
}

int ObRebalanceTask::extract_task_pkeys(common::ObIArray<common::ObPartitionKey>& pkeys) const
{
  int ret = OB_SUCCESS;
  pkeys.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < get_sub_task_count(); ++i) {
    const ObRebalanceTaskInfo* task_info = get_sub_task(i);
    if (OB_UNLIKELY(nullptr == task_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task info ptr is null", K(ret));
    } else {
      const ObPartitionKey& pkey = task_info->get_partition_key();
      if (OB_FAIL(pkeys.push_back(pkey))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObRebalanceTask::check_table_schema() const
{
  int ret = OB_SUCCESS;
  bool is_partition_exist = false;
  share::schema::ObSchemaGetterGuard schema_guard;
  share::schema::ObMultiVersionSchemaService* schema_service = GCTX.schema_service_;
  if (OB_UNLIKELY(nullptr == schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service ptr is null", K(ret));
  } else if (OB_FAIL(schema_service->get_schema_guard(schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_sub_task_count(); ++i) {
      const ObRebalanceTaskInfo* task_info = get_sub_task(i);
      if (OB_UNLIKELY(nullptr == task_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task info ptr is null", K(ret));
      } else {
        const ObPartitionKey& pkey = task_info->get_partition_key();
        bool check_partition_exist = true;
        if (OB_FAIL(schema_guard.check_partition_exist(
                pkey.get_table_id(), pkey.get_partition_id(), check_partition_exist, is_partition_exist))) {
          LOG_WARN("check partition exist failed", K(ret), K(pkey));
        } else if (!is_partition_exist) {
          ret = OB_REBALANCE_TASK_CANT_EXEC;
          LOG_WARN("partition not exist while rebalance task executing", K(ret), K(pkey));
        } else {
        }  // go on next
      }
    }
  }
  return ret;
}

int ObRebalanceTask::build_task_partition_infos(const uint64_t tenant_id,
    common::ObIArray<share::ObPartitionInfo>& partition_infos, PartitionInfoMap& partition_info_map) const
{
  int ret = OB_SUCCESS;
  ObRootService* root_service = NULL;
  common::ObArray<common::ObPartitionKey> pkeys;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(NULL == (root_service = GCTX.root_service_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice ptr is null", K(ret));
  } else if (OB_FAIL(extract_task_pkeys(pkeys))) {
    LOG_WARN("fail to extract task pkeys", K(ret));
  } else {
    common::ObMySQLProxy& sql_proxy = root_service->get_sql_proxy();
    ObNormalPartitionTableProxy pt_proxy(sql_proxy);
    const bool filter_flag_replica = true;
    if (OB_FAIL(pt_proxy.multi_fetch_partition_infos(tenant_id, pkeys, filter_flag_replica, partition_infos))) {
      LOG_WARN("fail to multi fetch partition infos", K(ret));
    } else if (partition_infos.count() != get_sub_task_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part info count and task infos count not match",
          K(ret),
          "left_count",
          partition_infos.count(),
          "right_count",
          get_sub_task_count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_infos.count(); ++i) {
        ObPartitionInfo& partition_info = partition_infos.at(i);
        ObPartitionKey part_key;
        const int overwrite = 0;
        if (OB_FAIL(part_key.init(
                partition_info.get_table_id(), partition_info.get_partition_id(), 0 /*ignore part cnt*/))) {
          LOG_WARN("fail to init part key", K(ret));
        } else if (OB_FAIL(partition_info_map.set_refactored(part_key, &partition_info, overwrite))) {
          LOG_WARN("fail to set refactored", K(ret));
        } else {
        }  //
      }
    }
  }
  return ret;
}

int ObRebalanceTask::batch_check_task_partition_condition(const bool is_admin_force) const
{
  int ret = OB_SUCCESS;
  const ObRebalanceTaskInfo* first_task_info = nullptr;
  if (OB_UNLIKELY(get_sub_task_count() <= 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sub task count invalid", "sub_task_cnt", get_sub_task_count());
  } else if (nullptr == (first_task_info = get_sub_task(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first task info null");
  } else {
    const uint64_t tenant_id = first_task_info->get_tenant_id();
    common::ObArray<share::ObPartitionInfo> partition_infos;
    PartitionInfoMap part_info_map;
    const int64_t bucket_num = get_sub_task_count() * 2;
    if (OB_FAIL(part_info_map.create(bucket_num, ObModIds::OB_RS_PARTITION_TABLE_TEMP))) {
      LOG_WARN("fail to create partition info map", K(ret));
    } else if (OB_FAIL(build_task_partition_infos(tenant_id, partition_infos, part_info_map))) {
      LOG_WARN("fail to build task partition infos", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < get_sub_task_count(); ++i) {
        common::ObAddr dummy;
        const ObRebalanceTaskInfo* task_info = get_sub_task(i);
        if (OB_UNLIKELY(nullptr == task_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task info ptr is null", K(ret));
        } else {
          const common::ObPartitionKey& pkey = task_info->get_partition_key();
          share::ObPartitionInfo* partition = nullptr;
          common::ObPartitionKey part_key;
          if (OB_FAIL(part_key.init(pkey.get_table_id(), pkey.get_partition_id(), 0 /*ignore part cnt*/))) {
            LOG_WARN("fail to init", K(ret), K(pkey));
          } else if (OB_FAIL(part_info_map.get_refactored(part_key, partition))) {
            LOG_WARN("fail to get refactored", K(ret));
          } else if (OB_UNLIKELY(nullptr == partition)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("part info ptr is null", K(ret));
          } else if (OB_FAIL(task_info->check_before_execute(*this, *partition, is_admin_force, dummy))) {
            LOG_WARN("fail to before execute");
          }
        }
      }
    }
  }
  return ret;
}

int ObRebalanceTask::check_task_partition_condition(
    share::ObPartitionTableOperator& pt_operator, const bool is_admin_force) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_sub_task_count(); ++i) {
    ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
    ObPartitionInfo partition;
    partition.set_allocator(&allocator);
    const ObRebalanceTaskInfo* task_info = get_sub_task(i);
    if (OB_UNLIKELY(nullptr == task_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task info ptr is null", K(ret));
    } else {
      const ObPartitionKey& pkey = task_info->get_partition_key();
      common::ObAddr dummy;
      if (OB_FAIL(pt_operator.get(pkey.get_table_id(), pkey.get_partition_id(), partition))) {
        LOG_WARN("fail to get partition info", K(ret));
      } else if (OB_FAIL(task_info->check_before_execute(*this, partition, is_admin_force, dummy))) {
        LOG_WARN("fail to check before execute", K(ret));
      }
    }
  }
  return ret;
}

int ObRebalanceTask::get_execute_transmit_size(int64_t& execute_transmit_size) const
{
  int ret = OB_SUCCESS;
  execute_transmit_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_sub_task_count(); ++i) {
    int64_t this_size = 0;
    const ObRebalanceTaskInfo* task_info = get_sub_task(i);
    if (OB_UNLIKELY(nullptr == task_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task info ptr is null", K(ret));
    } else if (OB_FAIL(task_info->get_execute_transmit_size(this_size))) {
      LOG_WARN("fail to get execute transmit size", K(ret));
    } else {
      execute_transmit_size += this_size;
    }
  }
  return ret;
}

int ObMigrateReplicaTask::build(const obrpc::MigrateMode migrate_mode,
    const common::ObIArray<ObMigrateTaskInfo>& task_infos, const common::ObAddr& dst,
    const ObRebalanceTaskPriority priority, const char* comment, bool admin_force)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos.count() <= 0 || !dst.is_valid() || !is_valid_priority(priority))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_infos), K(dst), K(priority));
  } else if (OB_FAIL(set_task_id(GCTX.self_addr_))) {
    LOG_WARN("fail to set task id", K(ret));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      if (OB_FAIL(task_infos_.push_back(task_infos.at(i)))) {
        LOG_WARN("fail to push task", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      dst_ = dst;
      priority_ = priority;
      comment_ = comment;
      admin_force_ = admin_force;
      migrate_mode_ = migrate_mode;
    }
  }
  return ret;
}

int ObMigrateReplicaTask::get_data_size(int64_t& data_size) const
{
  int ret = OB_SUCCESS;
  data_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
    data_size += task_infos_.at(i).get_transmit_data_size();
  }
  return ret;
}

int ObMigrateReplicaTask::get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
    const int64_t min_task_timeout_us, int64_t& timeout_us) const
{
  int ret = OB_SUCCESS;
  int64_t data_size = 0;
  if (OB_UNLIKELY(network_bandwidth <= 0 || server_concurrency_limit <= 0 || min_task_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(network_bandwidth), K(server_concurrency_limit), K(min_task_timeout_us));
  } else if (OB_FAIL(get_data_size(data_size))) {
    LOG_WARN("fail to get data size", K(ret));
  } else if (data_size < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data size unexpected", K(ret), K(data_size));
  } else {
    int64_t timeout_us_by_part_cnt = TIMEOUT_US_PER_PARTITION * task_infos_.count();
    timeout_us = data_size / network_bandwidth * 1000000 * server_concurrency_limit;
    timeout_us = std::max(min_task_timeout_us, timeout_us);
    timeout_us = std::max(timeout_us_by_part_cnt, timeout_us);
  }
  return ret;
}

int ObMigrateReplicaTask::build_migrate_replica_rpc_arg(obrpc::ObMigrateReplicaBatchArg& arg) const
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else if (OB_FAIL(get_timeout(ObRebalanceTaskMgr::NETWORK_BANDWIDTH,
                 std::max(GCONF.server_data_copy_in_concurrency.get(), GCONF.server_data_copy_out_concurrency.get()),
                 GCONF.balancer_task_timeout,
                 timeout))) {
    LOG_WARN("fail to get timeout", K(ret));
  } else if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timeout value unexpected", K(ret), K(timeout));
  } else {
    arg.task_id_ = task_id_;
    arg.timeout_ts_ = timeout + ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
      obrpc::ObMigrateReplicaArg element;
      element.key_ = task_infos_.at(i).get_partition_key();
      element.src_ = task_infos_.at(i).get_src_member();
      element.data_source_ = task_infos_.at(i).get_data_src_member();
      element.dst_ = task_infos_.at(i).get_dest().member_;
      element.quorum_ = task_infos_.at(i).get_quorum();
      element.skip_change_member_list_ = task_infos_.at(i).skip_change_member_list_;
      element.switch_epoch_ = task_infos_.at(i).switchover_epoch_;
      element.migrate_mode_ = migrate_mode_;
      if (OB_FAIL(arg.arg_array_.push_back(element))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObMigrateReplicaTask::build_by_task_result(const obrpc::ObMigrateReplicaRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObMigrateTaskInfo task_info;
    if (OB_FAIL(task_info.init_task_key(arg.key_.get_table_id(),
            arg.key_.get_partition_id(),
            0 /* ignore */,
            OB_INVALID_ID,
            RebalanceKeyType::FORMAL_BALANCE_KEY))) {
      LOG_WARN("fail to init task key", K(ret));
    } else {
      task_info.pkey_ = arg.key_;
      task_info.src_ = arg.src_;
      task_info.data_src_ = arg.data_src_;
      task_info.dst_.member_ = arg.dst_;
      task_infos_.reset();
      if (OB_FAIL(task_infos_.push_back(task_info))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
        dst_ = arg.dst_.get_server();
      }
    }
  }
  return ret;
}

int ObMigrateReplicaTask::build_by_task_result(const obrpc::ObMigrateReplicaBatchRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(arg.res_array_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.res_array_.count(); ++i) {
      const ObMigrateReplicaRes& res = arg.res_array_.at(i);
      ObMigrateTaskInfo task_info;
      if (!res.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(res));
      } else if (OB_FAIL(task_info.init_task_key(res.key_.get_table_id(),
                     res.key_.get_partition_id(),
                     0 /**/,
                     OB_INVALID_ID,
                     RebalanceKeyType::FORMAL_BALANCE_KEY))) {
        LOG_WARN("fail to init task key", K(ret));
      } else {
        task_info.pkey_ = res.key_;
        task_info.src_ = res.src_;
        task_info.data_src_ = res.data_src_;
        task_info.dst_.member_ = res.dst_;
        if (OB_FAIL(task_infos_.push_back(task_info))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
        }  // no more
      }
    }
    if (OB_SUCC(ret)) {
      dst_ = task_infos_.at(0).dst_.member_.get_server();
    }
  }
  return ret;
}

int ObTypeTransformTask::build(const common::ObIArray<ObTypeTransformTaskInfo>& task_infos, const common::ObAddr& dst,
    const char* comment, bool admin_force)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos.count() <= 0 || !dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst), "task_info_count", task_infos.count());
  } else if (OB_FAIL(set_task_id(GCTX.self_addr_))) {
    LOG_WARN("fail to set task id", K(ret));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      if (OB_FAIL(task_infos_.push_back(task_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      dst_ = dst;
      comment_ = comment;
      admin_force_ = admin_force;
    }
  }
  return ret;
}

int ObTypeTransformTask::get_data_size(int64_t& data_size) const
{
  int ret = OB_SUCCESS;
  data_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
    data_size += task_infos_.at(i).get_transmit_data_size();
  }
  return ret;
}

int ObTypeTransformTask::get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
    const int64_t min_task_timeout_us, int64_t& timeout_us) const
{
  int ret = OB_SUCCESS;
  int64_t data_size = 0;
  if (OB_UNLIKELY(network_bandwidth <= 0 || server_concurrency_limit <= 0 || min_task_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(network_bandwidth), K(server_concurrency_limit), K(min_task_timeout_us));
  } else if (OB_FAIL(get_data_size(data_size))) {
    LOG_WARN("fail to get data size", K(ret));
  } else if (data_size < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data size unexpected", K(ret), K(data_size));
  } else {
    int64_t timeout_us_by_part_cnt = TIMEOUT_US_PER_PARTITION * task_infos_.count();
    timeout_us = data_size / network_bandwidth * 1000000 * server_concurrency_limit;
    timeout_us = std::max(min_task_timeout_us, timeout_us);
    timeout_us = std::max(timeout_us_by_part_cnt, timeout_us);
  }
  return ret;
}

int ObTypeTransformTask::build_type_transform_rpc_arg(obrpc::ObChangeReplicaBatchArg& arg) const
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else if (OB_FAIL(get_timeout(ObRebalanceTaskMgr::NETWORK_BANDWIDTH,
                 std::max(GCONF.server_data_copy_in_concurrency.get(), GCONF.server_data_copy_out_concurrency.get()),
                 GCONF.balancer_task_timeout,
                 timeout))) {
    LOG_WARN("fail to get timeout", K(ret));
  } else if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timeout value unexpected", K(ret), K(timeout));
  } else {
    arg.task_id_ = task_id_;
    arg.timeout_ts_ = timeout + ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
      obrpc::ObChangeReplicaArg element;
      element.key_ = task_infos_.at(i).get_partition_key();
      element.src_ = task_infos_.at(i).get_data_src_member();
      element.dst_ = task_infos_.at(i).get_dest().member_;
      element.quorum_ = task_infos_.at(i).get_quorum();
      element.skip_change_member_list_ = task_infos_.at(i).skip_change_member_list_;
      element.switch_epoch_ = task_infos_.at(i).switchover_epoch_;
      if (OB_FAIL(arg.arg_array_.push_back(element))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObTypeTransformTask::build_by_task_result(const obrpc::ObChangeReplicaRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    ObTypeTransformTaskInfo task_info;
    if (OB_FAIL(task_info.init_task_key(arg.key_.get_table_id(),
            arg.key_.get_partition_id(),
            0 /* ignore */,
            OB_INVALID_ID,
            RebalanceKeyType::FORMAL_BALANCE_KEY))) {
      LOG_WARN("fail to init task key", K(ret));
    } else {
      task_info.pkey_ = arg.key_;
      task_info.src_ = arg.src_;
      task_info.data_src_ = arg.data_src_;
      task_info.dst_.member_ = arg.dst_;
      task_info.quorum_ = arg.quorum_;
      task_infos_.reset();
      if (OB_FAIL(task_infos_.push_back(task_info))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
        dst_ = arg.dst_.get_server();
      }
    }
  }
  return ret;
}

int ObTypeTransformTask::build_by_task_result(const obrpc::ObChangeReplicaBatchRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(arg.res_array_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.res_array_.count(); ++i) {
      const ObChangeReplicaRes& res = arg.res_array_.at(i);
      ObTypeTransformTaskInfo task_info;
      if (!res.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(res));
      } else if (OB_FAIL(task_info.init_task_key(res.key_.get_table_id(),
                     res.key_.get_partition_id(),
                     0 /* ignore */,
                     OB_INVALID_ID,
                     RebalanceKeyType::FORMAL_BALANCE_KEY))) {
        LOG_WARN("fail to init task key", K(ret));
      } else {
        task_info.pkey_ = res.key_;
        task_info.src_ = res.src_;
        task_info.data_src_ = res.data_src_;
        task_info.dst_.member_ = res.dst_;
        task_info.quorum_ = res.quorum_;
        if (OB_FAIL(task_infos_.push_back(task_info))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
        }  // no more
      }
    }
    if (OB_SUCC(ret)) {
      dst_ = task_infos_.at(0).dst_.member_.get_server();
    }
  }
  return ret;
}

int ObAddReplicaTask::build(
    const common::ObIArray<ObAddTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment, bool admin_force)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos.count() <= 0 || !dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (1 != task_infos.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't do batch rereplicate now", K(ret), K(task_infos));
  } else if (OB_FAIL(set_task_id(GCTX.self_addr_))) {
    LOG_WARN("fail to set task id", K(ret));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      if (OB_FAIL(task_infos_.push_back(task_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (is_inner_table(task_infos_.at(0).get_partition_key().get_table_id())) {
        priority_ = HIGH_PRI;
      } else {
        priority_ = LOW_PRI;
      }
      dst_ = dst;
      comment_ = comment;
      admin_force_ = admin_force;
    }
  }
  return ret;
}

int ObAddReplicaTask::get_data_size(int64_t& data_size) const
{
  int ret = OB_SUCCESS;
  data_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
    data_size += task_infos_.at(i).get_transmit_data_size();
  }
  return ret;
}

int ObAddReplicaTask::get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
    const int64_t min_task_timeout_us, int64_t& timeout_us) const
{
  int ret = OB_SUCCESS;
  int64_t data_size = 0;
  if (OB_UNLIKELY(network_bandwidth <= 0 || server_concurrency_limit <= 0 || min_task_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(network_bandwidth), K(server_concurrency_limit), K(min_task_timeout_us));
  } else if (OB_FAIL(get_data_size(data_size))) {
    LOG_WARN("fail to get data size", K(ret));
  } else if (data_size < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data size unexpected", K(ret), K(data_size));
  } else {
    int64_t timeout_us_by_part_cnt = TIMEOUT_US_PER_PARTITION * task_infos_.count();
    timeout_us = data_size / network_bandwidth * 1000000 * server_concurrency_limit;
    timeout_us = std::max(min_task_timeout_us, timeout_us);
    timeout_us = std::max(timeout_us_by_part_cnt, timeout_us);
  }
  return ret;
}

int ObAddReplicaTask::build_add_replica_rpc_arg(obrpc::ObAddReplicaBatchArg& arg) const
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else if (OB_FAIL(get_timeout(ObRebalanceTaskMgr::NETWORK_BANDWIDTH,
                 std::max(GCONF.server_data_copy_in_concurrency.get(), GCONF.server_data_copy_out_concurrency.get()),
                 GCONF.balancer_task_timeout,
                 timeout))) {
    LOG_WARN("fail to get timeout", K(ret));
  } else if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timeout value unexpected", K(ret), K(timeout));
  } else {
    arg.task_id_ = task_id_;
    arg.timeout_ts_ = timeout + ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
      obrpc::ObAddReplicaArg element;
      element.key_ = task_infos_.at(i).get_partition_key();
      element.src_ = task_infos_.at(i).get_data_src_member();
      element.dst_ = task_infos_.at(i).get_dest().member_;
      element.quorum_ = task_infos_.at(i).get_quorum();
      element.cluster_id_ = task_infos_.at(i).get_cluster_id();
      element.skip_change_member_list_ = task_infos_.at(i).skip_change_member_list_;
      element.switch_epoch_ = task_infos_.at(i).switchover_epoch_;
      if (OB_FAIL(arg.arg_array_.push_back(element))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObAddReplicaTask::build_by_task_result(const obrpc::ObAddReplicaRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    ObAddTaskInfo task_info;
    if (OB_FAIL(task_info.init_task_key(arg.key_.get_table_id(),
            arg.key_.get_partition_id(),
            0 /* ignore */,
            OB_INVALID_ID,
            RebalanceKeyType::FORMAL_BALANCE_KEY))) {
      LOG_WARN("fail to init task key", K(ret));
    } else {
      task_info.pkey_ = arg.key_;
      task_info.data_src_ = arg.data_src_;
      task_info.dst_.member_ = arg.dst_;
      task_info.quorum_ = arg.quorum_;
      task_infos_.reset();
      if (OB_FAIL(task_infos_.push_back(task_info))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
        dst_ = arg.dst_.get_server();
      }
    }
  }
  return ret;
}

int ObAddReplicaTask::build_by_task_result(const obrpc::ObAddReplicaBatchRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(arg.res_array_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.res_array_.count(); ++i) {
      const ObAddReplicaRes& res = arg.res_array_.at(i);
      ObAddTaskInfo task_info;
      if (!res.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(res));
      } else if (OB_FAIL(task_info.init_task_key(res.key_.get_table_id(),
                     res.key_.get_partition_id(),
                     0 /* ignore */,
                     OB_INVALID_ID,
                     RebalanceKeyType::FORMAL_BALANCE_KEY))) {
        LOG_WARN("fail to init task key", K(ret));
      } else {
        task_info.pkey_ = res.key_;
        task_info.data_src_ = res.data_src_;
        task_info.dst_.member_ = res.dst_;
        task_info.quorum_ = res.quorum_;
        if (OB_FAIL(task_infos_.push_back(task_info))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
        }  // no more
      }
    }
    if (OB_SUCC(ret)) {
      dst_ = task_infos_.at(0).dst_.member_.get_server();
    }
  }
  return ret;
}

int ObRebuildReplicaTask::build(
    const common::ObIArray<ObRebuildTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos.count() <= 0 || !dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(set_task_id(GCTX.self_addr_))) {
    LOG_WARN("fail to set task id", K(ret));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      if (OB_FAIL(task_infos_.push_back(task_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      dst_ = dst;
      comment = comment;
      priority_ = ObRebalanceTaskPriority::HIGH_PRI;
    }
  }
  return ret;
}

int ObRebuildReplicaTask::get_data_size(int64_t& data_size) const
{
  int ret = OB_SUCCESS;
  data_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
    data_size += task_infos_.at(i).get_transmit_data_size();
  }
  return ret;
}

int ObRebuildReplicaTask::get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
    const int64_t min_task_timeout_us, int64_t& timeout_us) const
{
  int ret = OB_SUCCESS;
  int64_t data_size = 0;
  if (OB_UNLIKELY(network_bandwidth <= 0 || server_concurrency_limit <= 0 || min_task_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(network_bandwidth), K(server_concurrency_limit), K(min_task_timeout_us));
  } else if (OB_FAIL(get_data_size(data_size))) {
    LOG_WARN("fail to get data size", K(ret));
  } else if (data_size < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data size unexpected", K(ret), K(data_size));
  } else {
    int64_t timeout_us_by_part_cnt = TIMEOUT_US_PER_PARTITION * task_infos_.count();
    timeout_us = data_size / network_bandwidth * 1000000 * server_concurrency_limit;
    timeout_us = std::max(min_task_timeout_us, timeout_us);
    timeout_us = std::max(timeout_us_by_part_cnt, timeout_us);
  }
  return ret;
}

int ObRebuildReplicaTask::build_rebuild_replica_rpc_arg(obrpc::ObRebuildReplicaBatchArg& arg) const
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else if (OB_FAIL(get_timeout(ObRebalanceTaskMgr::NETWORK_BANDWIDTH,
                 std::max(GCONF.server_data_copy_in_concurrency.get(), GCONF.server_data_copy_out_concurrency.get()),
                 GCONF.balancer_task_timeout,
                 timeout))) {
    LOG_WARN("fail to get timeout", K(ret));
  } else if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timeout value unexpected", K(ret), K(timeout));
  } else {
    arg.task_id_ = task_id_;
    arg.timeout_ts_ = timeout + ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
      obrpc::ObRebuildReplicaArg element;
      element.key_ = task_infos_.at(i).get_partition_key();
      element.src_ = task_infos_.at(i).get_data_src_member();
      element.dst_ = task_infos_.at(i).get_dest().member_;
      element.skip_change_member_list_ = task_infos_.at(i).skip_change_member_list_;
      element.switch_epoch_ = task_infos_.at(i).switchover_epoch_;
      if (OB_FAIL(arg.arg_array_.push_back(element))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObRebuildReplicaTask::build_by_task_result(const obrpc::ObRebuildReplicaRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    ObRebuildTaskInfo task_info;
    if (OB_FAIL(task_info.init_task_key(arg.key_.get_table_id(),
            arg.key_.get_partition_id(),
            0 /* ignore */,
            OB_INVALID_ID,
            RebalanceKeyType::FORMAL_BALANCE_KEY))) {
      LOG_WARN("fail to init task key", K(ret));
    } else {
      task_info.pkey_ = arg.key_;
      task_info.data_src_ = arg.data_src_;
      task_info.dst_.member_ = arg.dst_;
      task_infos_.reset();
      if (OB_FAIL(task_infos_.push_back(task_info))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
        dst_ = arg.dst_.get_server();
      }
    }
  }
  return ret;
}

int ObRebuildReplicaTask::build_by_task_result(const obrpc::ObRebuildReplicaBatchRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(arg.res_array_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.res_array_.count(); ++i) {
      const ObRebuildReplicaRes& res = arg.res_array_.at(i);
      ObRebuildTaskInfo task_info;
      if (!res.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(res));
      } else if (OB_FAIL(task_info.init_task_key(res.key_.get_table_id(),
                     res.key_.get_partition_id(),
                     0 /* ignore */,
                     OB_INVALID_ID,
                     RebalanceKeyType::FORMAL_BALANCE_KEY))) {
        LOG_WARN("fail to init task key", K(ret));
      } else {
        task_info.pkey_ = res.key_;
        task_info.data_src_ = res.data_src_;
        task_info.dst_.member_ = res.dst_;
        if (OB_FAIL(task_infos_.push_back(task_info))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
        }  // no more
      }
    }
    if (OB_SUCC(ret)) {
      dst_ = task_infos_.at(0).dst_.member_.get_server();
    }
  }
  return ret;
}

int ObModifyQuorumTask::build(
    const common::ObIArray<ObModifyQuorumTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos.count() <= 0 || !dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst));
  } else if (OB_FAIL(set_task_id(GCTX.self_addr_))) {
    LOG_WARN("fail to set task id", K(ret));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      if (OB_FAIL(task_infos_.push_back(task_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      dst_ = dst;
      comment_ = comment;
    }
  }
  return ret;
}

int ObModifyQuorumTask::get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
    const int64_t min_task_timeout_us, int64_t& timeout_us) const
{
  int ret = OB_SUCCESS;
  UNUSED(network_bandwidth);
  UNUSED(server_concurrency_limit);
  UNUSED(min_task_timeout_us);
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    timeout_us = TIMEOUT_US_PER_PARTITION * task_infos_.count();
    timeout_us = std::max(min_task_timeout_us, timeout_us);
  }
  return ret;
}

int ObModifyQuorumTask::build_modify_quorum_rpc_arg(obrpc::ObModifyQuorumBatchArg& arg) const
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else if (OB_FAIL(get_timeout(ObRebalanceTaskMgr::NETWORK_BANDWIDTH,
                 std::max(GCONF.server_data_copy_in_concurrency.get(), GCONF.server_data_copy_out_concurrency.get()),
                 GCONF.balancer_task_timeout,
                 timeout))) {
    LOG_WARN("fail to get timeout", K(ret));
  } else if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timeout value unexpected", K(ret), K(timeout));
  } else {
    arg.task_id_ = task_id_;
    arg.timeout_ts_ = timeout + ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
      obrpc::ObModifyQuorumArg element;
      element.key_ = task_infos_.at(i).get_partition_key();
      element.quorum_ = task_infos_.at(i).get_quorum();
      element.orig_quorum_ = task_infos_.at(i).get_orig_quorum();
      element.member_list_ = task_infos_.at(i).get_member_list();
      if (OB_FAIL(arg.arg_array_.push_back(element))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObRestoreTask::build(
    const common::ObIArray<ObRestoreTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos.count() <= 0 || !dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst));
  } else if (OB_FAIL(set_task_id(GCTX.self_addr_))) {
    LOG_WARN("fail to set task id", K(ret));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      if (OB_FAIL(task_infos_.push_back(task_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      dst_ = dst;
      comment_ = comment;
    }
  }
  return ret;
}

/* timeout of restore task can be calculated more precisely
 */
int ObRestoreTask::get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
    const int64_t min_task_timeout_us, int64_t& timeout_us) const
{
  int ret = OB_SUCCESS;
  UNUSED(network_bandwidth);
  UNUSED(server_concurrency_limit);
  UNUSED(min_task_timeout_us);
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    timeout_us = TIMEOUT_US_PER_PARTITION * task_infos_.count();
    timeout_us = std::max(min_task_timeout_us, timeout_us);
  }
  return ret;
}

int ObRestoreTask::build_restore_rpc_arg(obrpc::ObRestoreReplicaArg& arg) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos_.count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else {
    arg.key_ = task_infos_.at(0).get_partition_key();
    arg.src_ = task_infos_.at(0).get_restore_arg();
    arg.dst_ = task_infos_.at(0).get_dest().member_;
    arg.skip_change_member_list_ = task_infos_.at(0).skip_change_member_list_;
    arg.switch_epoch_ = task_infos_.at(0).switchover_epoch_;
    arg.task_id_ = task_id_;
  }
  return ret;
}

int ObRestoreTask::build_by_task_result(const obrpc::ObRestoreReplicaRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    ObRestoreTaskInfo task_info;
    if (OB_FAIL(task_info.init_task_key(arg.key_.get_table_id(),
            arg.key_.get_partition_id(),
            0 /* ignore */,
            OB_INVALID_ID,
            RebalanceKeyType::FORMAL_BALANCE_KEY))) {
      LOG_WARN("fail to init task key", K(ret));
    } else {
      task_info.pkey_ = arg.key_;
      task_info.restore_arg_ = arg.src_;
      task_info.dst_.member_ = arg.dst_;
      task_infos_.reset();
      if (OB_FAIL(task_infos_.push_back(task_info))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
        dst_ = arg.dst_.get_server();
      }
    }
  }
  return ret;
}

int ObPhysicalRestoreTask::build(
    const common::ObIArray<ObPhysicalRestoreTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos.count() <= 0 || !dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst));
  } else if (OB_FAIL(set_task_id(GCTX.self_addr_))) {
    LOG_WARN("fail to set task id", K(ret));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      if (OB_FAIL(task_infos_.push_back(task_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      dst_ = dst;
      comment_ = comment;
    }
  }
  return ret;
}

/* timeout of restore task can be calculated more precisely
 */
int ObPhysicalRestoreTask::get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
    const int64_t min_task_timeout_us, int64_t& timeout_us) const
{
  int ret = OB_SUCCESS;
  UNUSED(network_bandwidth);
  UNUSED(server_concurrency_limit);
  UNUSED(min_task_timeout_us);
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    timeout_us = TIMEOUT_US_PER_PARTITION * task_infos_.count();
    timeout_us = std::max(min_task_timeout_us, timeout_us);
  }
  return ret;
}

int ObPhysicalRestoreTask::build_restore_rpc_arg(obrpc::ObPhyRestoreReplicaArg& arg) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos_.count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else if (OB_FAIL(arg.src_.assign(task_infos_.at(0).get_restore_arg()))) {
    LOG_WARN("fail to assign restore arg", K(ret), "arg", task_infos_.at(0).get_restore_arg());
  } else {
    arg.key_ = task_infos_.at(0).get_partition_key();
    arg.dst_ = task_infos_.at(0).get_dest().member_;
    arg.task_id_ = task_id_;
  }
  return ret;
}

int ObPhysicalRestoreTask::build_by_task_result(const obrpc::ObPhyRestoreReplicaRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    ObPhysicalRestoreTaskInfo task_info;
    if (OB_FAIL(task_info.init_task_key(arg.key_.get_table_id(),
            arg.key_.get_partition_id(),
            0 /* ignore */,
            OB_INVALID_ID,
            RebalanceKeyType::FORMAL_BALANCE_KEY))) {
      LOG_WARN("fail to init task key", K(ret));
    } else if (OB_FAIL(task_info.restore_arg_.assign(arg.src_))) {
      LOG_WARN("fail to assign restore arg", K(ret), "arg", arg.src_);
    } else {
      task_info.pkey_ = arg.key_;
      task_info.dst_.member_ = arg.dst_;
      task_infos_.reset();
      if (OB_FAIL(task_infos_.push_back(task_info))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
        dst_ = arg.dst_.get_server();
      }
    }
  }
  return ret;
}

int ObCopySSTableTask::build(const common::ObIArray<ObCopySSTableTaskInfo>& task_infos,
    const common::ObCopySSTableType copy_sstable_type, const common::ObAddr& dst, const char* comment)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos.count() <= 0 || !dst.is_valid() || OB_COPY_SSTABLE_TYPE_INVALID == copy_sstable_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst), K(copy_sstable_type));
  } else if (OB_FAIL(set_task_id(GCTX.self_addr_))) {
    LOG_WARN("fail to set task id", K(ret));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      if (OB_FAIL(task_infos_.push_back(task_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      copy_sstable_type_ = copy_sstable_type;
      dst_ = dst;
      priority_ = ObRebalanceTaskPriority::HIGH_PRI;
      comment_ = comment;
    }
  }
  return ret;
}

int ObCopySSTableTask::get_data_size(int64_t& data_size) const
{
  int ret = OB_SUCCESS;
  data_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
    data_size += task_infos_.at(i).get_transmit_data_size();
  }
  return ret;
}

int ObCopySSTableTask::get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
    const int64_t min_task_timeout_us, int64_t& timeout_us) const
{
  int ret = OB_SUCCESS;
  int64_t data_size = 0;
  if (OB_UNLIKELY(network_bandwidth <= 0 || server_concurrency_limit <= 0 || min_task_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(network_bandwidth), K(server_concurrency_limit), K(min_task_timeout_us));
  } else if (OB_FAIL(get_data_size(data_size))) {
    LOG_WARN("fail to get data size", K(ret));
  } else if (data_size < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data size unexpected", K(ret), K(data_size));
  } else {
    int64_t timeout_us_by_part_cnt = TIMEOUT_US_PER_PARTITION * task_infos_.count();
    timeout_us = data_size / network_bandwidth * 1000000 * server_concurrency_limit;
    timeout_us = std::max(min_task_timeout_us, timeout_us);
    timeout_us = std::max(timeout_us_by_part_cnt, timeout_us);
  }
  return ret;
}

int ObCopySSTableTask::build_copy_sstable_rpc_arg(obrpc::ObCopySSTableBatchArg& arg) const
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else if (OB_FAIL(get_timeout(ObRebalanceTaskMgr::NETWORK_BANDWIDTH,
                 std::max(GCONF.server_data_copy_in_concurrency.get(), GCONF.server_data_copy_out_concurrency.get()),
                 GCONF.balancer_task_timeout,
                 timeout))) {
    LOG_WARN("fail to get timeout", K(ret));
  } else if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timeout value unexpected", K(ret), K(timeout));
  } else {
    arg.task_id_ = task_id_;
    arg.timeout_ts_ = timeout + ObTimeUtility::current_time();
    arg.type_ = copy_sstable_type_;
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
      obrpc::ObCopySSTableArg element;
      element.type_ = copy_sstable_type_;
      element.task_id_ = task_id_;
      element.key_ = task_infos_.at(i).get_partition_key();
      element.src_ = task_infos_.at(i).get_data_src_member();
      element.dst_ = task_infos_.at(i).get_dest().member_;
      element.index_table_id_ = task_infos_.at(i).get_index_table_id();
      element.cluster_id_ = task_infos_.at(i).get_cluster_id();
      element.skip_change_member_list_ = task_infos_.at(i).skip_change_member_list_;
      element.switch_epoch_ = task_infos_.at(i).switchover_epoch_;
      if (OB_COPY_SSTABLE_TYPE_LOCAL_INDEX == element.type_ && element.index_table_id_ <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid index_table_id", K(ret), K(element));
      } else if (OB_FAIL(arg.arg_array_.push_back(element))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObCopySSTableTask::build_by_task_result(const obrpc::ObCopySSTableBatchRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(arg.res_array_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_COPY_SSTABLE_TYPE_INVALID == arg.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid copy sstable type", K(ret));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.res_array_.count(); ++i) {
      const ObCopySSTableRes& res = arg.res_array_.at(i);
      ObCopySSTableTaskInfo task_info;
      if (!res.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(res));
      } else if (OB_FAIL(task_info.init_task_key(res.key_.get_table_id(),
                     res.key_.get_partition_id(),
                     0 /* ignore */,
                     res.index_table_id_,
                     RebalanceKeyType::FORMAL_BALANCE_KEY))) {
        LOG_WARN("fail to init task key", K(ret));
      } else {
        task_info.pkey_ = res.key_;
        task_info.data_src_ = res.data_src_;
        task_info.dst_.member_ = res.dst_;
        task_info.index_table_id_ = res.index_table_id_;
        if (OB_FAIL(task_infos_.push_back(task_info))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
        }  // no more
      }
    }
    if (OB_SUCC(ret)) {
      copy_sstable_type_ = arg.type_;
      dst_ = task_infos_.at(0).dst_.member_.get_server();
    }
  }
  return ret;
}

int ObRemoveMemberTask::build(const common::ObIArray<ObRemoveMemberTaskInfo>& task_infos, const common::ObAddr& dst,
    const char* comment, const bool admin_force)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos.count() <= 0 || !dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst), "task_info_count", task_infos.count());
  } else if (OB_FAIL(set_task_id(GCTX.self_addr_))) {
    LOG_WARN("fail to set task id", K(ret));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      if (OB_FAIL(task_infos_.push_back(task_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      dst_ = dst;
      comment_ = comment;
      admin_force_ = admin_force;
    }
  }
  return ret;
}

int ObRemoveMemberTask::get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
    const int64_t min_task_timeout_us, int64_t& timeout_us) const
{
  int ret = OB_SUCCESS;
  UNUSED(network_bandwidth);
  UNUSED(server_concurrency_limit);
  UNUSED(min_task_timeout_us);
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    timeout_us = TIMEOUT_US_PER_PARTITION * task_infos_.count();
    timeout_us = std::max(min_task_timeout_us, timeout_us);
  }
  return ret;
}

int ObRemoveMemberTask::build_remove_member_rpc_arg(obrpc::ObMemberChangeBatchArg& arg) const
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else if (OB_FAIL(get_timeout(ObRebalanceTaskMgr::NETWORK_BANDWIDTH,
                 std::max(GCONF.server_data_copy_in_concurrency.get(), GCONF.server_data_copy_out_concurrency.get()),
                 GCONF.balancer_task_timeout,
                 timeout))) {
    LOG_WARN("fail to get timeout", K(ret));
  } else if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timeout value unexpected", K(ret), K(timeout));
  } else {
    arg.task_id_ = task_id_;
    arg.timeout_ts_ = timeout + ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
      obrpc::ObMemberChangeArg element;
      element.key_ = task_infos_.at(i).get_partition_key();
      element.member_ = task_infos_.at(i).get_dest().member_;
      element.quorum_ = task_infos_.at(i).get_quorum();
      element.orig_quorum_ = task_infos_.at(i).get_orig_quorum();
      if (OB_FAIL(arg.arg_array_.push_back(element))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObRemoveNonPaxosTask::build(const common::ObIArray<ObRemoveNonPaxosTaskInfo>& task_infos, const common::ObAddr& dst,
    const char* comment, const bool admin_force)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos.count() <= 0 || !dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst), "task_info_count", task_infos.count());
  } else if (OB_FAIL(set_task_id(GCTX.self_addr_))) {
    LOG_WARN("fail to set task id", K(ret));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      if (OB_FAIL(task_infos_.push_back(task_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      dst_ = dst;
      comment_ = comment;
      admin_force_ = admin_force;
    }
  }
  return ret;
}

int ObRemoveNonPaxosTask::get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
    const int64_t min_task_timeout_us, int64_t& timeout_us) const
{
  int ret = OB_SUCCESS;
  UNUSED(network_bandwidth);
  UNUSED(server_concurrency_limit);
  UNUSED(min_task_timeout_us);
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    timeout_us = TIMEOUT_US_PER_PARTITION * task_infos_.count();
    timeout_us = std::max(min_task_timeout_us, timeout_us);
  }
  return ret;
}

int ObRemoveNonPaxosTask::build_remove_non_paxos_rpc_arg(obrpc::ObRemoveNonPaxosReplicaBatchArg& arg) const
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else if (OB_FAIL(get_timeout(ObRebalanceTaskMgr::NETWORK_BANDWIDTH,
                 std::max(GCONF.server_data_copy_in_concurrency.get(), GCONF.server_data_copy_out_concurrency.get()),
                 GCONF.balancer_task_timeout,
                 timeout))) {
    LOG_WARN("fail to get timeout", K(ret));
  } else if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timeout value unexpected", K(ret), K(timeout));
  } else {
    arg.task_id_ = task_id_;
    arg.timeout_ts_ = timeout + ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
      ObRemoveNonPaxosReplicaArg element;
      element.key_ = task_infos_.at(i).get_partition_key();
      element.dst_ = task_infos_.at(i).get_dest().member_;
      element.skip_change_member_list_ = task_infos_.at(i).skip_change_member_list_;
      element.switch_epoch_ = task_infos_.at(i).switchover_epoch_;
      if (OB_FAIL(arg.arg_array_.push_back(element))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObRebalanceSqlBKGTask::build(const ObSqlBKGDDistTaskInfo& task_info, const char* comment)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!task_info.get_sql_bkgd_task().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(task_infos_.push_back(task_info))) {
    LOG_WARN("fail to push back", K(ret));
  } else {
    dst_ = task_info.get_sql_bkgd_task().get_dest();
    comment_ = comment;
    if (nullptr != ObCurTraceId::get_trace_id()) {
      task_id_ = *ObCurTraceId::get_trace_id();
    }
  }
  return ret;
}

int ObRebalanceSqlBKGTask::get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
    const int64_t min_task_timeout_us, int64_t& timeout_us) const
{
  int ret = OB_SUCCESS;
  UNUSED(network_bandwidth);
  UNUSED(server_concurrency_limit);
  UNUSED(min_task_timeout_us);
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    const int64_t now = common::ObTimeUtility::current_time();
    timeout_us = task_infos_.at(0).get_sql_bkgd_task().get_abs_timeout_us() - now;
  }
  return ret;
}

int ObRebalanceSqlBKGTask::build_rebalance_sql_bkg_rpc_arg(sql::ObBKGDDistExecuteArg& arg) const
{
  int ret = OB_SUCCESS;
  if (task_infos_.count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "task_info_count", task_infos_.count());
  } else {
    const sql::ObSchedBKGDDistTask& bkgd_task = task_infos_.at(0).get_sql_bkgd_task();
    if (!bkgd_task.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(bkgd_task));
    } else if (bkgd_task.get_abs_timeout_us() < ObTimeUtility::current_time()) {
      ret = OB_TIMEOUT;
      LOG_WARN("task already timeouted", K(ret), K(bkgd_task));
    } else {
      arg.tenant_id_ = bkgd_task.get_tenant_id();
      arg.task_id_ = bkgd_task.get_task_id();
      arg.scheduler_id_ = bkgd_task.get_scheduler_id();
      arg.return_addr_ = GCONF.self_addr_;
      arg.serialized_task_ = bkgd_task.get_serialized_task();
    }
  }
  return ret;
}

int ObRebalanceSqlBKGTask::build_by_task_result(const sql::ObBKGDTaskCompleteArg& arg)
{
  int ret = OB_SUCCESS;
  if (!arg.task_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObSqlBKGDDistTaskInfo task_info;
    if (OB_FAIL(task_info.init_task_key(arg.task_id_.get_execution_id(),
            arg.task_id_.get_job_id(),
            arg.task_id_.get_task_id(),
            OB_INVALID_ID,
            RebalanceKeyType::SQL_BKG_DIST_KEY))) {
      LOG_WARN("fail to build task key", K(ret));
    } else {
      if (OB_FAIL(task_info.sql_bkgd_task_.init_execute_over_task(arg.task_id_))) {
        LOG_INFO("init execute task failed", K(ret));
      } else if (OB_FAIL(task_infos_.push_back(task_info))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRebalanceSqlBKGTask::check_task_exist(bool& exist)
{
  int ret = OB_SUCCESS;
  exist = true;
  ObCheckBuildIndexTaskExistArg arg;
  if (task_infos_.count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "task_info_count", task_infos_.count());
  } else {
    const sql::ObSchedBKGDDistTask& bkgd_task = task_infos_.at(0).get_sql_bkgd_task();
    if (!bkgd_task.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(bkgd_task));
    } else {
      arg.tenant_id_ = bkgd_task.get_tenant_id();
      arg.task_id_ = bkgd_task.get_task_id();
      arg.scheduler_id_ = bkgd_task.get_scheduler_id();
      Bool rpc_res = true;
      if (OB_FAIL(GCTX.executor_rpc_->get_proxy()->to(dst_).check_build_index_task_exist(arg, rpc_res))) {
        LOG_WARN("check build index task exist failed", K(ret));
      } else {
        exist = rpc_res;
      }
    }
  }
  return ret;
}

int ObBackupTask::build(
    const common::ObIArray<ObBackupTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos.count() <= 0 || !dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst));
  } else if (OB_FAIL(set_task_id(GCTX.self_addr_))) {
    LOG_WARN("fail to set task id", K(ret));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      if (OB_FAIL(task_infos_.push_back(task_infos.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      dst_ = dst;
      priority_ = ObRebalanceTaskPriority::HIGH_PRI;
      comment_ = comment;
    }
  }
  return ret;
}

int ObBackupTask::get_data_size(int64_t& data_size) const
{
  int ret = OB_SUCCESS;
  data_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
    data_size += task_infos_.at(i).get_transmit_data_size();
  }
  return ret;
}

int ObBackupTask::get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
    const int64_t min_task_timeout_us, int64_t& timeout_us) const
{
  int ret = OB_SUCCESS;
  int64_t data_size = 0;
  if (OB_UNLIKELY(network_bandwidth <= 0 || server_concurrency_limit <= 0 || min_task_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(network_bandwidth), K(server_concurrency_limit), K(min_task_timeout_us));
  } else if (OB_FAIL(get_data_size(data_size))) {
    LOG_WARN("fail to get data size", K(ret));
  } else if (data_size < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data size unexpected", K(ret), K(data_size));
  } else {
    int64_t timeout_us_by_part_cnt = TIMEOUT_US_PER_PARTITION * task_infos_.count();
    timeout_us = data_size / network_bandwidth * 1000000 * server_concurrency_limit;
    timeout_us = std::max(min_task_timeout_us, timeout_us);
    timeout_us = std::max(timeout_us_by_part_cnt, timeout_us);
  }
  return ret;
}

int ObBackupTask::build_backup_rpc_arg(obrpc::ObBackupBatchArg& arg) const
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else if (OB_FAIL(get_timeout(ObRebalanceTaskMgr::NETWORK_BANDWIDTH,
                 std::max(GCONF.server_data_copy_in_concurrency.get(), GCONF.server_data_copy_out_concurrency.get()),
                 GCONF.balancer_task_timeout,
                 timeout))) {
    LOG_WARN("fail to get timeout", K(ret));
  } else if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timeout value unexpected", K(ret), K(timeout));
  } else {
    arg.task_id_ = task_id_;
    arg.timeout_ts_ = timeout + ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
      obrpc::ObBackupArg element;
      element.task_id_ = task_id_;
      element.key_ = task_infos_.at(i).get_partition_key();
      element.src_ = task_infos_.at(i).get_data_src_member();
      element.dst_ = task_infos_.at(i).get_dest().member_;
      element.cluster_id_ = task_infos_.at(i).get_cluster_id();
      element.skip_change_member_list_ = task_infos_.at(i).skip_change_member_list_;
      element.switch_epoch_ = task_infos_.at(i).switchover_epoch_;
      element.physical_backup_arg_ = task_infos_.at(i).physical_backup_arg_;
      if (OB_FAIL(arg.arg_array_.push_back(element))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

int ObBackupTask::build_by_task_result(const obrpc::ObBackupBatchRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(arg.res_array_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.res_array_.count(); ++i) {
      const ObBackupRes& res = arg.res_array_.at(i);
      ObBackupTaskInfo task_info;
      if (!res.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(res));
      } else if (OB_FAIL(task_info.init_task_key(res.key_.get_table_id(),
                     res.key_.get_partition_id(),
                     0 /* ignore */,
                     OB_INVALID_ID,
                     RebalanceKeyType::BACKUP_MANAGER_KEY))) {
        LOG_WARN("fail to init task key", K(ret));
      } else {
        task_info.pkey_ = res.key_;
        task_info.data_src_ = res.data_src_;
        task_info.dst_.member_ = res.dst_;
        task_info.physical_backup_arg_ = res.physical_backup_arg_;
        if (OB_FAIL(task_infos_.push_back(task_info))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
        }  // no more
      }
    }
    if (OB_SUCC(ret)) {
      dst_ = task_infos_.at(0).dst_.member_.get_server();
    }
  }
  return ret;
}

int ObValidateTask::build(
    const ObIArray<ObValidateTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(set_task_id(GCTX.self_addr_))) {
    LOG_WARN("failed to set task id", K(ret));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      if (OB_FAIL(task_infos_.push_back(task_infos.at(i)))) {
        LOG_WARN("failed to push task", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      priority_ = ObRebalanceTaskPriority::HIGH_PRI;
      comment_ = comment;
      dst_ = dst;
    }
  }
  return ret;
}

int ObValidateTask::get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
    const int64_t min_task_timeout_us, int64_t& timeout_us) const
{
  int ret = OB_SUCCESS;
  int64_t data_size = 0;
  if (OB_UNLIKELY(network_bandwidth <= 0 || server_concurrency_limit <= 0 || min_task_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(network_bandwidth), K(server_concurrency_limit), K(min_task_timeout_us));
  } else if (OB_FAIL(get_data_size(data_size))) {
    LOG_WARN("failed to get data size", K(ret), K(data_size));
  } else if (data_size < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data size unexpected", K(ret), K(data_size));
  } else {
    int64_t timeout_us_by_part_cnt = TIMEOUT_US_PER_PARTITION * task_infos_.count();
    timeout_us = data_size / network_bandwidth * 1000000 * server_concurrency_limit;
    timeout_us = std::max(min_task_timeout_us, timeout_us);
    timeout_us = std::max(timeout_us_by_part_cnt, timeout_us);
  }
  return ret;
}

int ObValidateTask::get_data_size(int64_t& data_size) const
{
  int ret = OB_SUCCESS;
  data_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
    data_size += task_infos_.at(i).get_transmit_data_size();
  }
  return ret;
}

int ObValidateTask::check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(pt_operator);
  UNUSED(leader);
  return ret;
}

int ObValidateTask::execute(
    obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader, common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  UNUSED(dummy_leader);
  UNUSED(rc_array);
  obrpc::ObValidateBatchArg arg;
  arg.task_id_.init(MYADDR);
  ret = E(EventTable::EN_BALANCE_TASK_EXE_ERR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    // bypass
  } else if (OB_FAIL(build_validate_rpc_arg(arg))) {
    LOG_WARN("failed to build validate batch arg", K(ret));
  } else if (OB_FAIL(rpc_proxy.to(dst_).validate_backup_batch(arg))) {
    LOG_WARN("failed to validate batch", K(ret), K(dst_), K(arg));
  } else {
    LOG_INFO("start to validate batch", K(arg));
  }
  return ret;
}

int ObValidateTask::build_validate_rpc_arg(obrpc::ObValidateBatchArg& arg) const
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  if (OB_UNLIKELY(task_infos_.count() < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else if (OB_FAIL(get_timeout(ObRebalanceTaskMgr::NETWORK_BANDWIDTH,
                 std::max(GCONF.server_data_copy_in_concurrency.get(), GCONF.server_data_copy_out_concurrency.get()),
                 GCONF.balancer_task_timeout,
                 timeout))) {
    LOG_WARN("failed to get timeout", K(ret));
  } else if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timeout value unexpected", K(ret), K(timeout));
  } else {
    arg.task_id_ = task_id_;
    arg.timeout_ts_ = timeout + ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos_.count(); ++i) {
      const ObValidateTaskInfo& task_info = task_infos_.at(i);
      obrpc::ObValidateArg element;
      element.trace_id_ = task_info.get_physical_validate_arg().trace_id_;
      element.dst_ = task_info.get_replica_member();
      element.physical_validate_arg_ = task_info.get_physical_validate_arg();
      element.priority_ = ObReplicaOpPriority::PRIO_LOW;
      if (OB_FAIL(arg.arg_array_.push_back(element))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
      }
    }
  }
  return ret;
}

int ObValidateTask::build_by_task_result(const obrpc::ObValidateRes& res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!res.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObValidateTaskInfo task_info;
    if (OB_FAIL(task_info.init_task_key(res.validate_arg_.job_id_,
            res.validate_arg_.backup_set_id_,
            res.key_.get_table_id(),
            res.key_.get_partition_id(),
            RebalanceKeyType::FORMAL_BALANCE_KEY))) {
      LOG_WARN("failed to init task key", K(ret));
    } else {
      task_info.pkey_ = res.key_;
      task_info.dst_ = res.dst_;
      task_info.physical_validate_arg_ = res.validate_arg_;
      if (OB_FAIL(task_infos_.push_back(task_info))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObValidateTask::build_by_task_result(const obrpc::ObValidateBatchRes& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(arg.res_array_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else {
    task_infos_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.res_array_.count(); ++i) {
      const ObValidateRes& res = arg.res_array_.at(i);
      ObValidateTaskInfo task_info;
      if (!res.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(res));
      } else if (OB_FAIL(task_info.init_task_key(res.validate_arg_.job_id_,
                     res.validate_arg_.backup_set_id_,
                     res.key_.get_table_id(),
                     res.key_.get_partition_id(),
                     RebalanceKeyType::FORMAL_BALANCE_KEY))) {
        LOG_WARN("failed to init task key", K(ret));
      } else {
        task_info.pkey_ = res.key_;
        task_info.dst_ = res.dst_;
        if (OB_FAIL(task_infos_.push_back(task_info))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
        }  // no more
      }
    }
  }
  return ret;
}

int ObMigrateReplicaTask::check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& is_available) const
{
  int ret = OB_SUCCESS;
  is_available = true;
  if (OB_FAIL(task_mgr.check_dest_server_migrate_in_blocked(*this, is_available))) {
    LOG_WARN("check dest server migrate in blocked failed", K(ret));
  } else if (!is_available) {
    // no need to check any more
  } else if (OB_FAIL(task_mgr.check_dest_server_out_of_disk(*this, is_available))) {
    LOG_WARN("check dest server outof disk failed", K(ret));
  } else if (!is_available) {
    // no need to check any more
  } else if (OB_FAIL(task_mgr.check_dest_server_has_too_many_partitions(*this, is_available))) {
    LOG_WARN("check dest server has too many partitions failed", K(ret));
  }
  return ret;
}

int ObTypeTransformTask::check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& is_available) const
{
  int ret = OB_SUCCESS;
  is_available = true;
  if (OB_FAIL(task_mgr.check_dest_server_migrate_in_blocked(*this, is_available))) {
    LOG_WARN("check dest server migrate in blocked failed", K(ret));
  } else if (!is_available) {
    // no need to check any more
  } else if (OB_FAIL(task_mgr.check_dest_server_out_of_disk(*this, is_available))) {
    LOG_WARN("check dest server outof disk failed", K(ret));
  }
  return ret;
}

int ObAddReplicaTask::check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& is_available) const
{
  int ret = OB_SUCCESS;
  is_available = true;
  if (OB_FAIL(task_mgr.check_dest_server_migrate_in_blocked(*this, is_available))) {
    LOG_WARN("check dest server migrate in blocked failed", K(ret));
  } else if (!is_available) {
    // no need to check any more
  } else if (OB_FAIL(task_mgr.check_dest_server_out_of_disk(*this, is_available))) {
    LOG_WARN("check dest server outof disk failed", K(ret));
  } else if (!is_available) {
    // no need to check any more
  } else if (OB_FAIL(task_mgr.check_dest_server_has_too_many_partitions(*this, is_available))) {
    LOG_WARN("check dest server has too many partitions failed", K(ret));
  }
  return ret;
}

int ObRebuildReplicaTask::check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& is_available) const
{
  int ret = OB_SUCCESS;
  is_available = true;
  if (OB_FAIL(task_mgr.check_dest_server_migrate_in_blocked(*this, is_available))) {
    LOG_WARN("check dest server migrate in blocked failed", K(ret));
  } else if (!is_available) {
    // no need to check any more
  } else if (OB_FAIL(task_mgr.check_dest_server_out_of_disk(*this, is_available))) {
    LOG_WARN("check dest server outof disk failed", K(ret));
  }
  return ret;
}

int ObModifyQuorumTask::check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& is_available) const
{
  int ret = OB_SUCCESS;
  UNUSED(task_mgr);
  is_available = true;
  return ret;
}

int ObRestoreTask::check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& is_available) const
{
  int ret = OB_SUCCESS;
  is_available = true;
  if (OB_FAIL(task_mgr.check_dest_server_migrate_in_blocked(*this, is_available))) {
    LOG_WARN("check dest server migrate in blocked failed", K(ret));
  } else if (!is_available) {
    // no need to check any more
  } else if (OB_FAIL(task_mgr.check_dest_server_out_of_disk(*this, is_available))) {
    LOG_WARN("check dest server outof disk failed", K(ret));
  }
  return ret;
}

int ObPhysicalRestoreTask::check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& is_available) const
{
  int ret = OB_SUCCESS;
  is_available = true;
  if (OB_FAIL(task_mgr.check_dest_server_migrate_in_blocked(*this, is_available))) {
    LOG_WARN("check dest server migrate in blocked failed", K(ret));
  } else if (!is_available) {
    // no need to check any more
  } else if (OB_FAIL(task_mgr.check_dest_server_out_of_disk(*this, is_available))) {
    LOG_WARN("check dest server outof disk failed", K(ret));
  }
  return ret;
}

int ObCopySSTableTask::check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& is_available) const
{
  int ret = OB_SUCCESS;
  is_available = true;
  if (OB_FAIL(task_mgr.check_dest_server_migrate_in_blocked(*this, is_available))) {
    LOG_WARN("check dest server migrate in blocked failed", K(ret));
  } else if (!is_available) {
    // no need to check any more
  } else if (OB_FAIL(task_mgr.check_dest_server_out_of_disk(*this, is_available))) {
    LOG_WARN("check dest server outof disk failed", K(ret));
  }
  return ret;
}

int ObRemoveMemberTask::check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& is_available) const
{
  int ret = OB_SUCCESS;
  UNUSED(task_mgr);
  is_available = true;
  return ret;
}

int ObRemoveNonPaxosTask::check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& is_available) const
{
  int ret = OB_SUCCESS;
  UNUSED(task_mgr);
  is_available = true;
  return ret;
}

int ObRebalanceSqlBKGTask::check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& is_available) const
{
  int ret = OB_SUCCESS;
  UNUSED(task_mgr);
  is_available = true;
  return ret;
}

int ObBackupTask::check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& is_available) const
{
  int ret = OB_SUCCESS;
  UNUSED(task_mgr);
  is_available = true;
  return ret;
}

int ObValidateTask::check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& is_available) const
{
  int ret = OB_SUCCESS;
  UNUSED(task_mgr);
  is_available = true;
  return ret;
}

int ObMigrateReplicaTask::log_execute_result(const common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (get_executor_time() > 0) ? (now - get_executor_time()) : (now - get_schedule_time());
  if (OB_UNLIKELY(task_infos_.count() != rc_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info count and ret code count not match",
        K(ret),
        "task_info_count",
        task_infos_.count(),
        "ret_code_count",
        rc_array.count());
  } else {
    if (task_infos_.count() > 1) {
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_batch_migrate_replica",
          "tenant_id",
          task_infos_.at(0).get_tenant_id(),
          "destination",
          dst_,
          "first_partitionkey",
          task_infos_.at(0).get_partition_key(),
          "task_info_count",
          task_infos_.count(),
          "comment",
          get_comment(),
          K(elapsed));
    }
    for (int64_t i = 0; i < task_infos_.count(); ++i) {
      const ObMigrateTaskInfo& info = task_infos_.at(i);
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_migrate_replica",
          "partition",
          ReplicaInfo(info.get_partition_key(), info.get_transmit_data_size()),
          "source",
          info.get_src_member(),
          "destination",
          info.get_dest(),
          "result",
          rc_array.at(i),
          "data_src",
          info.get_data_src_member(),
          K(elapsed));
    }
    EVENT_ADD(RS_PARTITION_MIGRATE_COUNT, task_infos_.count());
    EVENT_ADD(RS_PARTITION_MIGRATE_TIME, elapsed);
  }
  return ret;
}

int ObTypeTransformTask::log_execute_result(const common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (get_executor_time() > 0) ? (now - get_executor_time()) : (now - get_schedule_time());
  if (OB_UNLIKELY(task_infos_.count() != rc_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info count and ret code count not match",
        K(ret),
        "task_info_count",
        task_infos_.count(),
        "ret_code_count",
        rc_array.count());
  } else {
    for (int64_t i = 0; i < task_infos_.count(); ++i) {
      const ObTypeTransformTaskInfo& info = task_infos_.at(i);
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_type_tranform",
          "partition",
          info.get_partition_key(),
          "destination",
          info.get_dest(),
          "result",
          rc_array.at(i),
          "src",
          info.get_src_member(),
          "data_src",
          info.get_data_src_member(),
          K(elapsed));
    }
    EVENT_ADD(RS_PARTITION_TRANSFORM_COUNT, task_infos_.count());
    EVENT_ADD(RS_PARTITION_TRANSFORM_TIME, elapsed);
  }
  return ret;
}

int ObAddReplicaTask::log_execute_result(const common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (get_executor_time() > 0) ? (now - get_executor_time()) : (now - get_schedule_time());
  if (OB_UNLIKELY(task_infos_.count() != rc_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info count and ret code count not match",
        K(ret),
        "task_info_count",
        task_infos_.count(),
        "ret_code_count",
        rc_array.count());
  } else {
    for (int64_t i = 0; i < task_infos_.count(); ++i) {
      const ObAddTaskInfo& info = task_infos_.at(i);
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_add_replica",
          "partition",
          ReplicaInfo(info.get_partition_key(), info.get_transmit_data_size()),
          "destination",
          info.get_dest(),
          "result",
          rc_array.at(i),
          "data_src",
          info.get_data_src_member(),
          K(elapsed));
    }
    EVENT_ADD(RS_PARTITION_ADD_COUNT, task_infos_.count());
    EVENT_ADD(RS_PARTITION_ADD_TIME, elapsed);
  }
  return ret;
}

int ObRebuildReplicaTask::log_execute_result(const common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (get_executor_time() > 0) ? (now - get_executor_time()) : (now - get_schedule_time());
  if (OB_UNLIKELY(task_infos_.count() != rc_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info count and ret code count not match",
        K(ret),
        "task_info_count",
        task_infos_.count(),
        "ret_code_count",
        rc_array.count());
  } else {
    for (int64_t i = 0; i < task_infos_.count(); ++i) {
      const ObRebuildTaskInfo& info = task_infos_.at(i);
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_rebuild_replica",
          "partition",
          ReplicaInfo(info.get_partition_key(), info.get_transmit_data_size()),
          "destination",
          info.get_dest(),
          "result",
          rc_array.at(i),
          "data_src",
          info.get_data_src_member(),
          K(elapsed));
    }
    EVENT_ADD(RS_PARTITION_REBUILD_COUNT, task_infos_.count());
    EVENT_ADD(RS_PARTITION_REBUILD_TIME, elapsed);
  }
  return ret;
}

int ObModifyQuorumTask::log_execute_result(const common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (get_executor_time() > 0) ? (now - get_executor_time()) : (now - get_schedule_time());
  if (OB_UNLIKELY(task_infos_.count() != rc_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info count and ret code count not match",
        K(ret),
        "task_info_count",
        task_infos_.count(),
        "ret_code_count",
        rc_array.count());
  } else {
    if (task_infos_.count() > 1) {
      // if a batch process, record in event history table for this batch information
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_batch_modify_quorum",
          "leader",
          dst_,
          "task_info_count",
          task_infos_.count(),
          "comment",
          get_comment(),
          K(elapsed));
    }
    for (int64_t i = 0; i < task_infos_.count(); ++i) {
      const ObModifyQuorumTaskInfo& info = task_infos_.at(i);
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_modify_quorum",
          "partition",
          info.get_partition_key(),
          "quorum",
          info.get_quorum(),
          "orig_quorum",
          info.get_orig_quorum(),
          "leader",
          info.get_dest().get_server(),
          "result",
          rc_array.at(i));
    }
    EVENT_ADD(RS_PARTITION_MODIFY_QUORUM_COUNT, task_infos_.count());
    EVENT_ADD(RS_PARTITION_MODIFY_QUORUM_TIME, elapsed);
  }
  return ret;
}

int ObRestoreTask::log_execute_result(const common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (get_executor_time() > 0) ? (now - get_executor_time()) : (now - get_schedule_time());
  if (OB_UNLIKELY(task_infos_.count() != rc_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info count and ret code count not match",
        K(ret),
        "task_info_count",
        task_infos_.count(),
        "ret_code_count",
        rc_array.count());
  } else {
    for (int64_t i = 0; i < task_infos_.count(); ++i) {
      const ObRestoreTaskInfo& info = task_infos_.at(i);
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_restore_replica",
          "partition",
          info.get_partition_key(),
          "source",
          info.get_restore_arg(),
          "destination",
          info.get_dest(),
          "result",
          rc_array.at(i),
          "comment",
          get_comment(),
          K(elapsed));
    }
  }
  return ret;
}

int ObPhysicalRestoreTask::log_execute_result(const common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (get_executor_time() > 0) ? (now - get_executor_time()) : (now - get_schedule_time());
  if (OB_UNLIKELY(task_infos_.count() != rc_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info count and ret code count not match",
        K(ret),
        "task_info_count",
        task_infos_.count(),
        "ret_code_count",
        rc_array.count());
  } else {
    ObCurTraceId::TraceId task_id = *ObCurTraceId::get_trace_id();
    for (int64_t i = 0; i < task_infos_.count(); ++i) {
      const ObPhysicalRestoreTaskInfo& info = task_infos_.at(i);
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_physical_restore_replica",
          "partition",
          info.get_partition_key(),
          "destination",
          info.get_dest(),
          "result",
          rc_array.at(i),
          "comment",
          get_comment(),
          K(elapsed),
          K(task_id));
    }
  }
  return ret;
}

int ObCopySSTableTask::log_execute_result(const common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (get_executor_time() > 0) ? (now - get_executor_time()) : (now - get_schedule_time());
  if (OB_UNLIKELY(task_infos_.count() != rc_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info count and ret code count not match",
        K(ret),
        "task_info_count",
        task_infos_.count(),
        "ret_code_count",
        rc_array.count());
  } else {
    const char* event = (OB_COPY_SSTABLE_TYPE_RESTORE_FOLLOWER == copy_sstable_type_)
                            ? "finish_restore_follower_replica"
                            : "finish_copy_sstable";
    for (int64_t i = 0; i < task_infos_.count(); ++i) {
      const ObCopySSTableTaskInfo& info = task_infos_.at(i);
      ROOTSERVICE_EVENT_ADD("balancer",
          event,
          "partition",
          ReplicaInfo(info.get_partition_key(), info.get_transmit_data_size()),
          "source",
          info.get_data_src_member(),
          "destination",
          info.get_dest(),
          "result",
          rc_array.at(i),
          "comment",
          get_comment(),
          K(elapsed));
    }
  }
  return ret;
}

int ObRemoveMemberTask::log_execute_result(const common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (get_executor_time() > 0) ? (now - get_executor_time()) : (now - get_schedule_time());
  if (OB_UNLIKELY(task_infos_.count() != rc_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info count and ret code count not match",
        K(ret),
        "task_info_count",
        task_infos_.count(),
        "ret_code_count",
        rc_array.count());
  } else {
    if (task_infos_.count() > 1) {
      // if a batch process, record in event history table for this batch information
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_batch_remove_member",
          "destination",
          dst_,
          "task_info_count",
          task_infos_.count(),
          "comment",
          get_comment(),
          K(elapsed));
    }
    for (int64_t i = 0; i < task_infos_.count(); ++i) {
      const ObRemoveMemberTaskInfo& info = task_infos_.at(i);
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_remove_member",
          "partition",
          info.get_partition_key(),
          "member",
          info.get_dest(),
          "quorum",
          info.get_quorum(),
          "orig_quorum",
          info.get_orig_quorum(),
          "result",
          rc_array.at(i),
          "comment",
          get_comment());
    }
    EVENT_ADD(RS_PARTITION_CHANGE_MEMBER_COUNT, task_infos_.count());
    EVENT_ADD(RS_PARTITION_CHANGE_MEMBER_TIME, elapsed);
  }
  return ret;
}

int ObRemoveNonPaxosTask::log_execute_result(const common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (get_executor_time() > 0) ? (now - get_executor_time()) : (now - get_schedule_time());
  if (OB_UNLIKELY(task_infos_.count() != rc_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info count and ret code count not match",
        K(ret),
        "task_info_count",
        task_infos_.count(),
        "ret_code_count",
        rc_array.count());
  } else {
    if (task_infos_.count() > 1) {
      // if a batch process, record in event history table for this batch information
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_batch_remove_non_paxos_replica",
          "destination",
          dst_,
          "task_info_count",
          task_infos_.count(),
          "comment",
          get_comment());
    }
    for (int64_t i = 0; i < task_infos_.count(); ++i) {
      const ObRemoveNonPaxosTaskInfo& info = task_infos_.at(i);
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_remove_non_paxos_replica",
          "partition",
          info.get_partition_key(),
          "dst",
          info.get_dest(),
          "result",
          rc_array.at(i),
          "comment",
          get_comment(),
          K(elapsed));
    }
    EVENT_ADD(RS_PARTITION_REMOVE_COUNT, task_infos_.count());
    EVENT_ADD(RS_PARTITION_REMOVE_TIME, elapsed);
  }
  return ret;
}

int ObRebalanceSqlBKGTask::log_execute_result(const common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (get_executor_time() > 0) ? (now - get_executor_time()) : (now - get_schedule_time());
  if (OB_UNLIKELY(rc_array.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ret code count not match", K(ret), "ret_code_count", rc_array.count());
  } else {
    for (int64_t i = 0; i < task_infos_.count(); i++) {
      const sql::ObSchedBKGDDistTask& info = task_infos_.at(i).get_sql_bkgd_task();
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_sql_bkgd_task",
          "partition",
          info.get_partition_key(),
          "task_id",
          info.get_task_id(),
          "destination",
          info.get_dest(),
          "result",
          rc_array.at(i),
          "comment",
          get_comment(),
          K(elapsed));
    }
  }
  return ret;
}

int ObBackupTask::log_execute_result(const common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (get_executor_time() > 0) ? (now - get_executor_time()) : (now - get_schedule_time());
  if (OB_UNLIKELY(task_infos_.count() != rc_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info count and ret code count not match",
        K(ret),
        "task_info_count",
        task_infos_.count(),
        "ret_code_count",
        rc_array.count());
  } else {
    for (int64_t i = 0; i < task_infos_.count(); ++i) {
      const ObBackupTaskInfo& info = task_infos_.at(i);
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_backup",
          "partition",
          ReplicaInfo(info.get_partition_key(), info.get_transmit_data_size()),
          "source",
          info.get_data_src_member(),
          "destination",
          info.get_dest(),
          "result",
          rc_array.at(i),
          "comment",
          get_comment(),
          K(elapsed));
    }
  }
  return ret;
}

int ObValidateTask::log_execute_result(const ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (get_executor_time() > 0) ? (now - get_executor_time()) : (now - get_schedule_time());
  if (OB_UNLIKELY(task_infos_.count() != rc_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info count and ret code count not match",
        K(ret),
        "task_info_count",
        task_infos_.count(),
        "ret_code_count",
        rc_array.count());
  } else {
    for (int64_t i = 0; i < task_infos_.count(); ++i) {
      const ObValidateTaskInfo& info = task_infos_.at(i);
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_validate",
          "job_id",
          info.get_physical_validate_arg().job_id_,
          "backup_set_id",
          info.get_physical_validate_arg().backup_set_id_,
          "tenant_id",
          info.get_tenant_id(),
          "table_id",
          info.get_partition_key().get_table_id(),
          "partition",
          info.get_partition_key().get_partition_id(),
          K(elapsed));
    }
  }
  return ret;
}

int ObMigrateReplicaTask::clone(void* input_ptr, ObRebalanceTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(input_ptr));
  } else {
    ObMigrateReplicaTask* my_task = new (input_ptr) ObMigrateReplicaTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObMigrateReplicaTask construct failed", K(ret));
    } else if (OB_FAIL(my_task->ObRebalanceTask::deep_copy(*this))) {
      LOG_WARN("fail to deep copy base task", K(ret));
    } else if (OB_FAIL(my_task->task_infos_.assign(task_infos_))) {
      LOG_WARN("fail to assign task infos", K(ret));
    } else {
      my_task->admin_force_ = admin_force_;
      my_task->migrate_mode_ = migrate_mode_;
    }
    if (OB_SUCC(ret)) {
      output_task = my_task;
    }
  }
  return ret;
}

int ObTypeTransformTask::clone(void* input_ptr, ObRebalanceTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(input_ptr));
  } else {
    ObTypeTransformTask* my_task = new (input_ptr) ObTypeTransformTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObMigrateReplicaTask construct failed", K(ret));
    } else if (OB_FAIL(my_task->ObRebalanceTask::deep_copy(*this))) {
      LOG_WARN("fail to deep copy base task", K(ret));
    } else if (OB_FAIL(my_task->task_infos_.assign(task_infos_))) {
      LOG_WARN("fail to assign task infos", K(ret));
    } else {
      my_task->admin_force_ = admin_force_;
    }
    if (OB_SUCC(ret)) {
      output_task = my_task;
    }
  }
  return ret;
}

int ObAddReplicaTask::clone(void* input_ptr, ObRebalanceTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(input_ptr));
  } else {
    ObAddReplicaTask* my_task = new (input_ptr) ObAddReplicaTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObMigrateReplicaTask construct failed", K(ret));
    } else if (OB_FAIL(my_task->ObRebalanceTask::deep_copy(*this))) {
      LOG_WARN("fail to deep copy base task", K(ret));
    } else if (OB_FAIL(my_task->task_infos_.assign(task_infos_))) {
      LOG_WARN("fail to assign task infos", K(ret));
    } else {
      my_task->admin_force_ = admin_force_;
    }
    if (OB_SUCC(ret)) {
      output_task = my_task;
    }
  }
  return ret;
}

int ObRebuildReplicaTask::clone(void* input_ptr, ObRebalanceTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(input_ptr));
  } else {
    ObRebuildReplicaTask* my_task = new (input_ptr) ObRebuildReplicaTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObMigrateReplicaTask construct failed", K(ret));
    } else if (OB_FAIL(my_task->ObRebalanceTask::deep_copy(*this))) {
      LOG_WARN("fail to deep copy base task", K(ret));
    } else if (OB_FAIL(my_task->task_infos_.assign(task_infos_))) {
      LOG_WARN("fail to assign task infos", K(ret));
    }
    if (OB_SUCC(ret)) {
      output_task = my_task;
    }
  }
  return ret;
}

int ObModifyQuorumTask::clone(void* input_ptr, ObRebalanceTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(input_ptr));
  } else {
    ObModifyQuorumTask* my_task = new (input_ptr) ObModifyQuorumTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObMigrateReplicaTask construct failed", K(ret));
    } else if (OB_FAIL(my_task->ObRebalanceTask::deep_copy(*this))) {
      LOG_WARN("fail to deep copy base task", K(ret));
    } else if (OB_FAIL(my_task->task_infos_.assign(task_infos_))) {
      LOG_WARN("fail to assign task infos", K(ret));
    }
    if (OB_SUCC(ret)) {
      output_task = my_task;
    }
  }
  return ret;
}

int ObRestoreTask::clone(void* input_ptr, ObRebalanceTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(input_ptr));
  } else {
    ObRestoreTask* my_task = new (input_ptr) ObRestoreTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObMigrateReplicaTask construct failed", K(ret));
    } else if (OB_FAIL(my_task->ObRebalanceTask::deep_copy(*this))) {
      LOG_WARN("fail to deep copy base task", K(ret));
    } else if (OB_FAIL(my_task->task_infos_.assign(task_infos_))) {
      LOG_WARN("fail to assign task infos", K(ret));
    }
    if (OB_SUCC(ret)) {
      output_task = my_task;
    }
  }
  return ret;
}

int ObPhysicalRestoreTask::clone(void* input_ptr, ObRebalanceTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(input_ptr));
  } else {
    ObPhysicalRestoreTask* my_task = new (input_ptr) ObPhysicalRestoreTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObMigrateReplicaTask construct failed", K(ret));
    } else if (OB_FAIL(my_task->ObRebalanceTask::deep_copy(*this))) {
      LOG_WARN("fail to deep copy base task", K(ret));
    } else if (OB_FAIL(my_task->task_infos_.assign(task_infos_))) {
      LOG_WARN("fail to assign task infos", K(ret));
    }
    if (OB_SUCC(ret)) {
      output_task = my_task;
    }
  }
  return ret;
}

int ObCopySSTableTask::clone(void* input_ptr, ObRebalanceTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(input_ptr));
  } else {
    ObCopySSTableTask* my_task = new (input_ptr) ObCopySSTableTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObMigrateReplicaTask construct failed", K(ret));
    } else if (OB_FAIL(my_task->ObRebalanceTask::deep_copy(*this))) {
      LOG_WARN("fail to deep copy base task", K(ret));
    } else if (OB_FAIL(my_task->task_infos_.assign(task_infos_))) {
      LOG_WARN("fail to assign task infos", K(ret));
    } else {
      my_task->copy_sstable_type_ = copy_sstable_type_;
    }
    if (OB_SUCC(ret)) {
      output_task = my_task;
    }
  }
  return ret;
}

int ObRemoveMemberTask::clone(void* input_ptr, ObRebalanceTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(input_ptr));
  } else {
    ObRemoveMemberTask* my_task = new (input_ptr) ObRemoveMemberTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObMigrateReplicaTask construct failed", K(ret));
    } else if (OB_FAIL(my_task->ObRebalanceTask::deep_copy(*this))) {
      LOG_WARN("fail to deep copy base task", K(ret));
    } else if (OB_FAIL(my_task->task_infos_.assign(task_infos_))) {
      LOG_WARN("fail to assign task infos", K(ret));
    } else {
      my_task->admin_force_ = admin_force_;
    }
    if (OB_SUCC(ret)) {
      output_task = my_task;
    }
  }
  return ret;
}

int ObRemoveNonPaxosTask::clone(void* input_ptr, ObRebalanceTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(input_ptr));
  } else {
    ObRemoveNonPaxosTask* my_task = new (input_ptr) ObRemoveNonPaxosTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObMigrateReplicaTask construct failed", K(ret));
    } else if (OB_FAIL(my_task->ObRebalanceTask::deep_copy(*this))) {
      LOG_WARN("fail to deep copy base task", K(ret));
    } else if (OB_FAIL(my_task->task_infos_.assign(task_infos_))) {
      LOG_WARN("fail to assign task infos", K(ret));
    } else {
      my_task->admin_force_ = admin_force_;
    }
    if (OB_SUCC(ret)) {
      output_task = my_task;
    }
  }
  return ret;
}

int ObRebalanceSqlBKGTask::clone(void* input_ptr, ObRebalanceTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(input_ptr));
  } else {
    ObRebalanceSqlBKGTask* my_task = new (input_ptr) ObRebalanceSqlBKGTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObMigrateReplicaTask construct failed", K(ret));
    } else if (OB_FAIL(my_task->ObRebalanceTask::deep_copy(*this))) {
      LOG_WARN("fail to deep copy base task", K(ret));
    } else if (OB_FAIL(my_task->task_infos_.assign(task_infos_))) {
      LOG_WARN("fail to assign task infos", K(ret));
    }
    if (OB_SUCC(ret)) {
      output_task = my_task;
    }
  }
  return ret;
}

int ObBackupTask::clone(void* input_ptr, ObRebalanceTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(input_ptr));
  } else {
    ObBackupTask* my_task = new (input_ptr) ObBackupTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObMigrateReplicaTask construct failed", K(ret));
    } else if (OB_FAIL(my_task->ObRebalanceTask::deep_copy(*this))) {
      LOG_WARN("fail to deep copy base task", K(ret));
    } else if (OB_FAIL(my_task->task_infos_.assign(task_infos_))) {
      LOG_WARN("fail to assign task infos", K(ret));
    }
    if (OB_SUCC(ret)) {
      output_task = my_task;
    }
  }
  return ret;
}

int ObValidateTask::clone(void* input_ptr, ObRebalanceTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(input_ptr));
  } else {
    ObValidateTask* my_task = new (input_ptr) ObValidateTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObValidateTsk construct failed", K(ret));
    } else if (OB_FAIL(my_task->ObRebalanceTask::deep_copy(*this))) {
      LOG_WARN("failed to deep copy base task", K(ret));
    } else if (OB_FAIL(my_task->task_infos_.assign(task_infos_))) {
      LOG_WARN("failed to assign task infos", K(ret));
    }
    if (OB_SUCC(ret)) {
      output_task = my_task;
    }
  }
  return ret;
}

int ObMigrateReplicaTask::assign(const ObMigrateReplicaTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTask::assign(that))) {
    LOG_WARN("fail to deep copy base task", K(ret));
  } else if (OB_FAIL(task_infos_.assign(that.task_infos_))) {
    LOG_WARN("fail to assign task infos", K(ret));
  } else {
    admin_force_ = that.admin_force_;
    migrate_mode_ = that.migrate_mode_;
  }
  return ret;
}

int ObTypeTransformTask::assign(const ObTypeTransformTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTask::assign(that))) {
    LOG_WARN("fail to deep copy base task", K(ret));
  } else if (OB_FAIL(task_infos_.assign(that.task_infos_))) {
    LOG_WARN("fail to assign task infos", K(ret));
  } else {
    admin_force_ = that.admin_force_;
  }
  return ret;
}

int ObAddReplicaTask::assign(const ObAddReplicaTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTask::assign(that))) {
    LOG_WARN("fail to deep copy base task", K(ret));
  } else if (OB_FAIL(task_infos_.assign(that.task_infos_))) {
    LOG_WARN("fail to assign task infos", K(ret));
  } else {
    admin_force_ = that.admin_force_;
  }
  return ret;
}

int ObRebuildReplicaTask::assign(const ObRebuildReplicaTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTask::assign(that))) {
    LOG_WARN("fail to deep copy base task", K(ret));
  } else if (OB_FAIL(task_infos_.assign(that.task_infos_))) {
    LOG_WARN("fail to assign task infos", K(ret));
  }
  return ret;
}

int ObModifyQuorumTask::assign(const ObModifyQuorumTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTask::assign(that))) {
    LOG_WARN("fail to deep copy base task", K(ret));
  } else if (OB_FAIL(task_infos_.assign(that.task_infos_))) {
    LOG_WARN("fail to assign task infos", K(ret));
  }
  return ret;
}

int ObRestoreTask::assign(const ObRestoreTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTask::assign(that))) {
    LOG_WARN("fail to deep copy base task", K(ret));
  } else if (OB_FAIL(task_infos_.assign(that.task_infos_))) {
    LOG_WARN("fail to assign task infos", K(ret));
  }
  return ret;
}

int ObPhysicalRestoreTask::assign(const ObPhysicalRestoreTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTask::assign(that))) {
    LOG_WARN("fail to deep copy base task", K(ret));
  } else if (OB_FAIL(task_infos_.assign(that.task_infos_))) {
    LOG_WARN("fail to assign task infos", K(ret));
  }
  return ret;
}

int ObCopySSTableTask::assign(const ObCopySSTableTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTask::assign(that))) {
    LOG_WARN("fail to deep copy base task", K(ret));
  } else if (OB_FAIL(task_infos_.assign(that.task_infos_))) {
    LOG_WARN("fail to assign task infos", K(ret));
  } else {
    copy_sstable_type_ = that.copy_sstable_type_;
  }
  return ret;
}

int ObRemoveMemberTask::assign(const ObRemoveMemberTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTask::assign(that))) {
    LOG_WARN("fail to deep copy base task", K(ret));
  } else if (OB_FAIL(task_infos_.assign(that.task_infos_))) {
    LOG_WARN("fail to assign task infos", K(ret));
  } else {
    admin_force_ = that.admin_force_;
  }
  return ret;
}

int ObRemoveNonPaxosTask::assign(const ObRemoveNonPaxosTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTask::assign(that))) {
    LOG_WARN("fail to deep copy base task", K(ret));
  } else if (OB_FAIL(task_infos_.assign(that.task_infos_))) {
    LOG_WARN("fail to assign task infos", K(ret));
  } else {
    admin_force_ = that.admin_force_;
  }
  return ret;
}

int ObRebalanceSqlBKGTask::assign(const ObRebalanceSqlBKGTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTask::assign(that))) {
    LOG_WARN("fail to deep copy base task", K(ret));
  } else if (OB_FAIL(task_infos_.assign(that.task_infos_))) {
    LOG_WARN("fail to assign task infos", K(ret));
  }
  return ret;
}

int ObBackupTask::assign(const ObBackupTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTask::assign(that))) {
    LOG_WARN("fail to deep copy base task", K(ret));
  } else if (OB_FAIL(task_infos_.assign(that.task_infos_))) {
    LOG_WARN("fail to assign task infos", K(ret));
  }
  return ret;
}

int ObValidateTask::assign(const ObValidateTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTask::assign(that))) {
    LOG_WARN("fail to deep copy base task", K(ret));
  } else if (OB_FAIL(task_infos_.assign(that.task_infos_))) {
    LOG_WARN("fail to assign task infos", K(ret));
  }
  return ret;
}

void ObRebalanceSqlBKGTask::notify_cancel() const
{
  int tmp_ret = OB_SUCCESS;
  if (1 != task_infos_.count()) {
    tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be one task info", K(tmp_ret), K(task_infos_.count()));
  } else if (OB_SUCCESS !=
             (tmp_ret = sql::ObRpcBKGDTaskCompleteP::notify_error(task_infos_.at(0).get_sql_bkgd_task().get_task_id(),
                  task_infos_.at(0).get_sql_bkgd_task().get_scheduler_id(),
                  OB_CANCELED))) {
    LOG_WARN("notify task canceled filed", K(tmp_ret));
  }
}

/* migrate replica check before execute
 * 1 check quorum
 * 2 check online
 * 3 check offline
 * 4 update with partition
 * 5 set unit id
 */
int ObMigrateReplicaTask::check_before_execute(
    share::ObPartitionTableOperator& pt_operator, common::ObAddr& dummy) const
{
  int ret = OB_SUCCESS;
  UNUSED(dummy);
  if (OB_FAIL(check_table_schema())) {
    LOG_WARN("fail to check table schema", K(ret));
  } else if (can_batch_get_partition_info()) {
    if (OB_FAIL(batch_check_task_partition_condition(is_admin_force()))) {
      LOG_WARN("fail to batch check task partition condition", K(ret));
    }
  } else {
    if (OB_FAIL(check_task_partition_condition(pt_operator, is_admin_force()))) {
      LOG_WARN("fail to check partition condition", K(ret));
    }
  }
  return ret;
}

int ObMigrateReplicaTask::execute(
    obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader, common::ObIArray<int>& dummy_rc_array) const
{
  int ret = OB_SUCCESS;
  UNUSED(dummy_leader);
  UNUSED(dummy_rc_array);
  // rpc timeout for batch migration is self-adaption.
  // the timeout is at least 60s, if the timeout is calculated beyond 60s,
  // specify 1ms for each partition
  const int64_t rpc_timeout = std::max(60 * 1000000L, task_infos_.count() * 1000);
  obrpc::ObMigrateReplicaBatchArg arg;
  arg.task_id_.init(MYADDR);
  ret = E(EventTable::EN_BALANCE_TASK_EXE_ERR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    // bypass
  } else if (OB_FAIL(build_migrate_replica_rpc_arg(arg))) {
    LOG_WARN("fail to build migrate batch arg", K(ret));
  } else if (OB_FAIL(rpc_proxy.to(dst_).timeout(rpc_timeout).migrate_replica_batch(arg))) {
    LOG_WARN("fail to execute migrate batch rpc", K(ret), K(arg));
  } else {
    LOG_INFO("start to migrate batch", K(arg));
  }
  return ret;
}

/* type transform check before execute
 * 1 check quorum
 * 2 check online
 * 3 check paxos member
 * 4 update with partition
 * 5 set unit id
 */
int ObTypeTransformTask::check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& dummy) const
{
  int ret = OB_SUCCESS;
  UNUSED(dummy);
  if (OB_FAIL(check_table_schema())) {
    LOG_WARN("fail to check table schema", K(ret));
  } else if (can_batch_get_partition_info()) {
    if (OB_FAIL(batch_check_task_partition_condition(is_admin_force()))) {
      LOG_WARN("fail to batch check task partition condition", K(ret));
    }
  } else {
    if (OB_FAIL(check_task_partition_condition(pt_operator, is_admin_force()))) {
      LOG_WARN("fail to check partition condition", K(ret));
    }
  }
  return ret;
}

int ObTypeTransformTask::execute(
    obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader, common::ObIArray<int>& dummy_rc_array) const
{
  int ret = OB_SUCCESS;
  obrpc::ObChangeReplicaBatchArg arg;
  arg.task_id_.init(MYADDR);
  UNUSED(dummy_leader);
  UNUSED(dummy_rc_array);
  ret = E(EventTable::EN_BALANCE_TASK_EXE_ERR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    // bypass
  } else if (OB_FAIL(build_type_transform_rpc_arg(arg))) {
    LOG_WARN("fail to build type transform batch rpc", K(ret));
  } else if (OB_FAIL(rpc_proxy.to(dst_).change_replica_batch(arg))) {
    LOG_WARN("fail to execute transform type batch", K(ret), K(arg));
  } else {
    LOG_INFO("start to transform type batch", K(ret), K(arg));
  }
  return ret;
}

/* Add replica check before execute:
 * 1 check quorum
 * 2 check online
 * 3 check paxos member
 * 4 update with partition
 * 5 set unit id
 */
int ObAddReplicaTask::check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& dummy) const
{
  int ret = OB_SUCCESS;
  UNUSED(dummy);
  if (OB_FAIL(check_table_schema())) {
    LOG_WARN("fail to check table schema", K(ret));
  } else if (can_batch_get_partition_info()) {
    if (OB_FAIL(batch_check_task_partition_condition(is_admin_force()))) {
      LOG_WARN("fail to batch check task partition condition", K(ret));
    }
  } else {
    if (OB_FAIL(check_task_partition_condition(pt_operator, is_admin_force()))) {
      LOG_WARN("fail to check partition condition", K(ret));
    }
  }
  return ret;
}

int ObAddReplicaTask::execute(
    obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader, common::ObIArray<int>& dummy_rc_leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(dummy_leader);
  UNUSED(dummy_rc_leader);
  obrpc::ObAddReplicaBatchArg arg;
  arg.task_id_.init(MYADDR);
  ret = E(EventTable::EN_BALANCE_TASK_EXE_ERR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    // bypass
  } else if (OB_FAIL(build_add_replica_rpc_arg(arg))) {
    LOG_WARN("fail to build add replica batch arg", K(ret));
  } else if (OB_FAIL(rpc_proxy.to(dst_).add_replica_batch(arg))) {
    LOG_WARN("fail to add replica batch", K(ret), K(arg));
  } else {
    LOG_INFO("start to add replica batch", K(arg));
  }
  return ret;
}

int ObRebuildReplicaTask::check_before_execute(
    share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  // check nothing
  UNUSED(pt_operator);
  UNUSED(leader);
  if (OB_FAIL(check_table_schema())) {
    LOG_WARN("fail to check table schema", K(ret));
  }
  return ret;
}

int ObRebuildReplicaTask::execute(
    obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader, common::ObIArray<int>& dummy_rc_array) const
{
  int ret = OB_SUCCESS;
  obrpc::ObRebuildReplicaBatchArg arg;
  arg.task_id_.init(MYADDR);
  UNUSED(dummy_leader);
  UNUSED(dummy_rc_array);
  ret = E(EventTable::EN_BALANCE_TASK_EXE_ERR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    // bypass
  } else if (OB_FAIL(build_rebuild_replica_rpc_arg(arg))) {
    LOG_WARN("fail to build rebuild replica batch arg", K(ret));
  } else if (OB_FAIL(rpc_proxy.to(dst_).rebuild_replica_batch(arg))) {
    LOG_WARN("fail to rebuild replica batch", K(ret), K(arg));
  } else {
    LOG_INFO("start to rebuild replica batch", K(arg));
  }
  return ret;
}

int ObModifyQuorumTask::check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  // check nothing
  UNUSED(pt_operator);
  UNUSED(leader);
  if (OB_FAIL(check_table_schema())) {
    LOG_WARN("fail to check table schema", K(ret));
  }
  return ret;
}

int ObModifyQuorumTask::execute(
    obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader, common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  obrpc::ObModifyQuorumBatchArg arg;
  obrpc::ObModifyQuorumBatchResult result;
  arg.task_id_.init(MYADDR);
  UNUSED(dummy_leader);
  // suppose modify_quorum of one partitions takes 1ms, at least 10s
  const int64_t rpc_timeout = max(10 * 1000 * 1000L, task_infos_.count() * 1000);
  rc_array.reset();
  ret = E(EventTable::EN_BALANCE_TASK_EXE_ERR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(tmp_ret) && i < task_infos_.count(); ++i) {
      if (OB_SUCCESS != (tmp_ret = rc_array.push_back(ret))) {
        LOG_WARN("failed to push back", K(ret), K(tmp_ret));
      }
    }
  } else if (OB_FAIL(build_modify_quorum_rpc_arg(arg))) {
    LOG_WARN("fail to build modify quorum batch rpc arg", K(ret));
  } else if (OB_FAIL(rpc_proxy.to(dst_).timeout(rpc_timeout).modify_quorum_batch(arg, result))) {
    LOG_WARN("fail to execute modify quorum rpc", K(ret), K(arg));
  } else {
    LOG_INFO("modify quorum batch success", "arg_cnt", arg.arg_array_.count());
  }

  int tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(tmp_ret) && i < result.return_array_.count(); i++) {
    int result_ret = result.return_array_.at(i);
    if (OB_SUCCESS != (tmp_ret = rc_array.push_back(result_ret))) {
      LOG_WARN("fail to push back", K(ret), K(i), K(result_ret));
    }
  }
  return ret;
}

/* restore check before execute
 * 1 update with partition
 * 2 try update unit id
 */
int ObRestoreTask::check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(leader);
  if (OB_FAIL(check_table_schema())) {
    LOG_WARN("fail to check table schema", K(ret));
  } else if (OB_FAIL(check_task_partition_condition(pt_operator, false /*is_admin_force*/))) {
    LOG_WARN("fail to check task partition condition", K(ret));
  }
  return ret;
}

int ObRestoreTask::execute(
    obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader, common::ObIArray<int>& dummy_rc_array) const
{
  int ret = OB_SUCCESS;
  obrpc::ObRestoreReplicaArg arg;
  arg.task_id_.init(MYADDR);
  UNUSED(dummy_leader);
  UNUSED(dummy_rc_array);
  ret = E(EventTable::EN_BALANCE_TASK_EXE_ERR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    // bypass
  } else if (OB_FAIL(build_restore_rpc_arg(arg))) {
    LOG_WARN("fail to build restore replica arg", K(ret));
  } else if (OB_FAIL(rpc_proxy.to(dst_).restore_replica(arg))) {
    LOG_WARN("fail to restore replica", K(ret), K(arg));
  } else {
    LOG_INFO("start to restore replica", K(arg));
  }
  return ret;
}

int ObPhysicalRestoreTask::check_before_execute(
    share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(leader);
  if (OB_FAIL(check_task_partition_condition(pt_operator, false /*is_admin_force*/))) {
    LOG_WARN("fail to check task partition condition", K(ret));
  }
  return ret;
}

int ObPhysicalRestoreTask::execute(
    obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader, common::ObIArray<int>& dummy_rc_array) const
{
  int ret = OB_SUCCESS;
  obrpc::ObPhyRestoreReplicaArg arg;
  arg.task_id_.init(MYADDR);
  UNUSED(dummy_leader);
  UNUSED(dummy_rc_array);
  ret = E(EventTable::EN_BALANCE_TASK_EXE_ERR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    // bypass
  } else if (OB_FAIL(build_restore_rpc_arg(arg))) {
    LOG_WARN("fail to build restore replica arg", K(ret));
  } else if (OB_FAIL(rpc_proxy.to(dst_).physical_restore_replica(arg))) {
    LOG_WARN("fail to physical restore replica", K(ret), K(arg));
  } else {
    LOG_INFO("start to physical restore replica", K(arg));
  }
  return ret;
}

int ObCopySSTableTask::check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(pt_operator);
  UNUSED(leader);
  if (OB_FAIL(check_table_schema())) {
    LOG_WARN("fail to check table schema", K(ret));
  }
  return ret;
}

int ObCopySSTableTask::execute(
    obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader, common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  UNUSED(dummy_leader);
  UNUSED(rc_array);
  obrpc::ObCopySSTableBatchArg arg;
  arg.task_id_.init(MYADDR);
  if (OB_FAIL(build_copy_sstable_rpc_arg(arg))) {
    LOG_WARN("fail to build copy sstable batch arg", K(ret));
  } else if (OB_FAIL(rpc_proxy.to(dst_).copy_sstable_batch(arg))) {
    LOG_WARN("fail to copy sstable batch", K(ret), K(arg));
  } else {
    LOG_INFO("start to copy sstable batch", K(arg));
  }
  return ret;
}

/* remove member check before execute:
 * 1 check leader
 */
int ObRemoveMemberTask::check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_infos_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(check_table_schema())) {
    LOG_WARN("fail to check table schema", K(ret));
  } else if (task_infos_.count() > 1) {
    leader = dst_;
  } else {
    ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
    ObPartitionInfo partition;
    partition.set_allocator(&allocator);
    const ObRemoveMemberTaskInfo& task_info = task_infos_.at(0);
    const ObPartitionKey& pkey = task_info.get_partition_key();
    if (OB_FAIL(pt_operator.get(pkey.get_table_id(), pkey.get_partition_id(), partition))) {
      LOG_WARN("fail to get partition info", K(ret), K(pkey));
    } else if (OB_FAIL(task_info.check_before_execute(*this, partition, is_admin_force(), leader))) {
      LOG_WARN("fail to check leader", K(ret), K(partition), K(*this));
    }
  }
  return ret;
}

int ObRemoveMemberTask::execute(
    obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& leader, common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  obrpc::ObMemberChangeBatchArg arg;
  obrpc::ObMemberChangeBatchResult result;
  arg.task_id_.init(MYADDR);
  // specify remove member 1ms for each partition, at least 10s for total
  rc_array.reset();
  const int64_t rpc_timeout = max(10 * 1000 * 1000L, task_infos_.count() * 1000);
  ret = E(EventTable::EN_BALANCE_TASK_EXE_ERR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(tmp_ret) && i < task_infos_.count(); ++i) {
      if (OB_SUCCESS != (tmp_ret = rc_array.push_back(ret))) {
        LOG_WARN("failed to push back array", K(ret), K(tmp_ret));
      }
    }
  } else if (!leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(leader));
  } else if (OB_FAIL(build_remove_member_rpc_arg(arg))) {
    LOG_WARN("fail to build remove member batch rpc arg", K(ret));
  } else if (OB_FAIL(rpc_proxy.to(leader).timeout(rpc_timeout).remove_member_batch(arg, result))) {
    LOG_WARN("fail to execute remove member rpc", K(ret), K(leader), K(arg));
  } else {
    LOG_INFO("remove member batch success", "arg_cnt", arg.arg_array_.count());
  }

  int tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; (OB_SUCCESS == tmp_ret) && i < result.return_array_.count(); i++) {
    int result_ret = result.return_array_.at(i);
    if (OB_SUCCESS != (tmp_ret = rc_array.push_back(result_ret))) {
      LOG_WARN("fail to push back", K(ret), K(i), K(result_ret));
    }
  }
  return ret;
}

int ObRemoveNonPaxosTask::check_before_execute(
    share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  // check nothing
  UNUSED(pt_operator);
  UNUSED(leader);
  ObPartitionInfo partition;
  common::ObAddr dummy;
  if (OB_FAIL(check_table_schema())) {
    LOG_WARN("fail to check table schema", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_sub_task_count(); ++i) {
      const ObRebalanceTaskInfo* task_info = get_sub_task(i);
      if (OB_UNLIKELY(nullptr == task_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task info ptr is null", K(ret));
      } else if (OB_FAIL(task_info->check_before_execute(*this, partition, is_admin_force(), dummy))) {
        LOG_WARN("fail to check before execute", K(ret));
      }
    }
  }
  return ret;
}

int ObRemoveNonPaxosTask::execute(
    obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader, common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  obrpc::ObRemoveNonPaxosReplicaBatchArg arg;
  obrpc::ObRemoveNonPaxosReplicaBatchResult result;
  arg.task_id_.init(MYADDR);
  UNUSED(dummy_leader);
  rc_array.reset();
  // suppose the timeout for remove paxos replica is at most 200ms
  const int64_t rpc_timeout = max(10 * 1000 * 1000L, task_infos_.count() * 200 * 1000);
  ret = E(EventTable::EN_BALANCE_TASK_EXE_ERR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < task_infos_.count(); i++) {
      if (OB_SUCCESS != (tmp_ret = rc_array.push_back(ret))) {
        LOG_WARN("failed to push back", K(ret), K(tmp_ret));
      }
    }
  } else if (OB_FAIL(build_remove_non_paxos_rpc_arg(arg))) {
    LOG_WARN("fail to build remove non paxos replica batch arg", K(ret));
  } else if (OB_FAIL(rpc_proxy.to(dst_).timeout(rpc_timeout).remove_non_paxos_replica_batch(arg, result))) {
    LOG_WARN("fail to execute remove non paxos replica batch", K(ret), K(arg), K(result));
  } else {
    LOG_INFO("finish remove non paxos replica batch", "arg_cnt", arg.arg_array_.count());
  }
  int tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < result.return_array_.count(); i++) {
    int result_ret = result.return_array_.at(i);
    if (OB_SUCCESS != (tmp_ret = rc_array.push_back(result_ret))) {
      LOG_WARN("fail to push back", K(ret), K(i), K(result_ret));
    }
  }
  return ret;
}

int ObRebalanceSqlBKGTask::check_before_execute(
    share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  // check nothing
  UNUSED(pt_operator);
  UNUSED(leader);
  ObPartitionInfo partition;
  common::ObAddr dummy;
  bool is_admin_force = false;
  if (OB_FAIL(check_table_schema())) {
    LOG_WARN("fail to check table schema", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_sub_task_count(); ++i) {
      const ObRebalanceTaskInfo* task_info = get_sub_task(i);
      if (OB_UNLIKELY(nullptr == task_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task info ptr is null", K(ret));
      } else if (OB_FAIL(task_info->check_before_execute(*this, partition, is_admin_force, dummy))) {
        LOG_WARN("fail to check before execute", K(ret));
      }
    }
  }
  return ret;
}

int ObRebalanceSqlBKGTask::execute(
    obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader, common::ObIArray<int>& dummy_rc_array) const
{
  int ret = OB_SUCCESS;
  UNUSED(rpc_proxy);
  UNUSED(dummy_leader);
  UNUSED(dummy_rc_array);
  if (OB_ISNULL(GCTX.executor_rpc_) || OB_ISNULL(GCTX.executor_rpc_->get_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy is NULL", K(ret));
  } else if (task_infos_.count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "task_info_count", task_infos_.count());
  } else {
    const sql::ObSchedBKGDDistTask& bkgd_task = task_infos_.at(0).get_sql_bkgd_task();
    sql::ObBKGDDistExecuteArg arg;
    if (bkgd_task.get_abs_timeout_us() < ObTimeUtility::current_time()) {
      ret = OB_TIMEOUT;
      LOG_WARN("task already timeouted", K(ret), K(bkgd_task));
    } else if (!bkgd_task.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(bkgd_task));
    } else if (OB_FAIL(build_rebalance_sql_bkg_rpc_arg(arg))) {
      LOG_WARN("fail to build rebalance sql bkg rpc arg", K(ret));
    } else if (OB_FAIL(GCTX.executor_rpc_->get_proxy()
                           ->to(bkgd_task.get_dest())
                           .by(OB_SYS_TENANT_ID)
                           .timeout(bkgd_task.get_abs_timeout_us())
                           .bkgd_task_submit(arg))) {
      LOG_WARN("send background task failed", K(ret), K(arg));
    } else {
      LOG_INFO("send background task success", K(arg));
    }
  }
  return ret;
}

int ObBackupTask::check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(pt_operator);
  UNUSED(leader);
  if (OB_FAIL(check_table_schema())) {
    LOG_WARN("fail to check table schema", K(ret));
  }
  return ret;
}

int ObBackupTask::execute(
    obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader, common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  UNUSED(dummy_leader);
  UNUSED(rc_array);
  obrpc::ObBackupBatchArg arg;
  arg.task_id_.init(MYADDR);
  if (OB_FAIL(build_backup_rpc_arg(arg))) {
    LOG_WARN("fail to build backup batch arg", K(ret));
  } else if (OB_FAIL(rpc_proxy.to(dst_).backup_replica_batch(arg))) {
    LOG_WARN("fail to backup batch", K(ret), K(arg));
  } else {
    LOG_INFO("start to backup batch", K(arg));
  }
  return ret;
}

int ObMigrateReplicaTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  if (task_infos_.count() > 1) {
    // record in the event history table for the batch task if it is a batch migration task
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_batch_migrate_replica",
        "tenant_id",
        task_infos_.at(0).get_tenant_id(),
        "destiontion",
        dst_,
        /* record the first partition key for this batch task */
        "first_partitionkey",
        task_infos_.at(0).get_partition_key(),
        "task_info_count",
        task_infos_.count(),
        "comment",
        get_comment(),
        "migrate_mode",
        migrate_mode_);
  }
  for (int64_t i = 0; i < task_infos_.count(); ++i) {
    const ObMigrateTaskInfo& info = task_infos_.at(i);
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_migrate_replica",
        "partition",
        ReplicaInfo(info.get_partition_key(), info.get_transmit_data_size()),
        "source",
        info.get_src_member(),
        "destination",
        info.get_dest(),
        "data_source",
        info.get_data_src_member(),
        "comment",
        get_comment(),
        "switchover_epoch_",
        info.switchover_epoch_);
  }
  return ret;
}

int ObTypeTransformTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  if (task_infos_.count() > 1) {
    // record in the event history table for the batch task if it is a batch migration task
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_batch_type_transform",
        "leader",
        dst_,
        "task_info_count",
        task_infos_.count(),
        "comment",
        get_comment());
  }
  for (int64_t i = 0; i < task_infos_.count(); ++i) {
    const ObTypeTransformTaskInfo& info = task_infos_.at(i);
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_type_transform",
        "partition",
        info.get_partition_key(),
        "quorum",
        info.get_quorum(),
        "destination",
        info.get_dest(),
        "src",
        info.get_src_member(),
        "data_src",
        info.get_data_src_member(),
        "comment",
        get_comment());
  }
  return ret;
}

int ObAddReplicaTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < task_infos_.count(); ++i) {
    const ObAddTaskInfo& info = task_infos_.at(i);
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_add_replica",
        "partition",
        ReplicaInfo(info.get_partition_key(), info.get_transmit_data_size()),
        "destination",
        info.get_dest(),
        "data_source",
        info.get_data_src_member(),
        "quorum",
        info.get_quorum(),
        "comment",
        get_comment(),
        "switchover_epoch_",
        info.switchover_epoch_);
  }
  return ret;
}

int ObRebuildReplicaTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < task_infos_.count(); ++i) {
    const ObRebuildTaskInfo& info = task_infos_.at(i);
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_rebuild_replica",
        "partition",
        ReplicaInfo(info.get_partition_key(), info.get_transmit_data_size()),
        "destination",
        info.get_dest(),
        "data_source",
        info.get_data_src_member(),
        "comment",
        get_comment(),
        "switchover_epoch_",
        info.switchover_epoch_);
  }
  return ret;
}

int ObModifyQuorumTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < task_infos_.count(); ++i) {
    const ObModifyQuorumTaskInfo& info = task_infos_.at(i);
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_modify_quorum",
        "partition",
        info.get_partition_key(),
        "quorum",
        info.get_quorum(),
        "orig_quorum",
        info.get_orig_quorum(),
        "destination",
        info.get_dest(),
        "comment",
        get_comment(),
        "switchover_epoch_",
        info.switchover_epoch_);
  }
  return ret;
}

int ObRestoreTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < task_infos_.count(); ++i) {
    const ObRestoreTaskInfo& info = task_infos_.at(i);
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_restore_replica",
        "partition",
        info.get_partition_key(),
        "destination",
        info.get_dest(),
        "comment",
        get_comment(),
        "switchover_epoch_",
        info.switchover_epoch_);
  }
  return ret;
}

int ObPhysicalRestoreTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < task_infos_.count(); ++i) {
    const ObPhysicalRestoreTaskInfo& info = task_infos_.at(i);
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_physical_restore_replica",
        "partition",
        info.get_partition_key(),
        "destination",
        info.get_dest(),
        "comment",
        get_comment(),
        "switchover_epoch_",
        info.switchover_epoch_);
  }
  return ret;
}

int ObCopySSTableTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  const char* event = (OB_COPY_SSTABLE_TYPE_RESTORE_FOLLOWER == copy_sstable_type_) ? "start_restore_follower_replica"
                                                                                    : "start_copy_sstable";
  for (int64_t i = 0; i < task_infos_.count(); ++i) {
    const ObCopySSTableTaskInfo& info = task_infos_.at(i);
    ROOTSERVICE_EVENT_ADD("balancer",
        event,
        "partition",
        ReplicaInfo(info.get_partition_key(), info.get_transmit_data_size()),
        "destination",
        info.get_dest(),
        "data_source",
        info.get_data_src_member(),
        "index_table_id",
        info.get_index_table_id(),
        "comment",
        get_comment());
  }
  return ret;
}

int ObRemoveMemberTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  if (task_infos_.count() > 1) {
    // record in the event history table for the batch task if it is a batch migration task
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_batch_remove_member",
        "destiontion",
        dst_,
        "task_info_count",
        task_infos_.count(),
        "comment",
        get_comment());
  }
  for (int64_t i = 0; i < task_infos_.count(); ++i) {
    const ObRemoveMemberTaskInfo& info = task_infos_.at(i);
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_remove_member",
        "partition",
        info.get_partition_key(),
        "member",
        info.get_dest(),
        "quorum",
        info.get_quorum(),
        "orig_quorum",
        info.get_orig_quorum(),
        "comment",
        get_comment(),
        "switchover_epoch_",
        info.switchover_epoch_);
  }
  return ret;
}

int ObRemoveNonPaxosTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  if (task_infos_.count() > 1) {
    // record in the event history table for the batch task if it is a batch migration task
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_batch_remove_non_paxos_repliac",
        "destiontion",
        dst_,
        "task_info_count",
        task_infos_.count(),
        "comment",
        get_comment());
  }
  for (int64_t i = 0; i < task_infos_.count(); ++i) {
    const ObRemoveNonPaxosTaskInfo& info = task_infos_.at(i);
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_remove_non_paxos_replica",
        "partition",
        info.get_partition_key(),
        "dst",
        info.get_dest(),
        "comment",
        get_comment(),
        "switchover_epoch_",
        info.switchover_epoch_);
  }
  return ret;
}

int ObRebalanceSqlBKGTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < task_infos_.count(); ++i) {
    auto& info = task_infos_.at(i).get_sql_bkgd_task();
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_sql_bkgd_task",
        "partition",
        info.get_partition_key(),
        "task_id",
        info.get_task_id(),
        "destination",
        info.get_dest(),
        "comment",
        get_comment());
  }
  return ret;
}

int ObBackupTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < task_infos_.count(); ++i) {
    const ObBackupTaskInfo& info = task_infos_.at(i);
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_backup",
        "partition",
        ReplicaInfo(info.get_partition_key(), info.get_transmit_data_size()),
        "destination",
        info.get_dest(),
        "data_source",
        info.get_data_src_member(),
        "comment",
        get_comment(),
        "switchover_epoch_",
        info.switchover_epoch_);
  }
  return ret;
}

int ObValidateTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < task_infos_.count(); ++i) {
    const ObValidateTaskInfo& info = task_infos_.at(i);
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_validate",
        "job_id",
        info.get_physical_validate_arg().job_id_,
        "backup_set_id",
        info.get_physical_validate_arg().backup_set_id_,
        "tenant_id",
        info.get_tenant_id(),
        "table_id",
        info.get_partition_key().get_table_id(),
        "partition",
        info.get_partition_key().get_partition_id());
  }
  return ret;
}

int ObTypeTransformTask::add_type_transform_task_info(const ObTypeTransformTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_infos_.push_back(task_info))) {
    LOG_WARN("fail to push back", K(ret), K(task_info));
  }
  return ret;
}

// the code try to invoke this func has the responsbility to guarantee
// there is only one task info for each partition
int ObRemoveMemberTask::add_remove_member_task_info(const ObRemoveMemberTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_infos_.push_back(task_info))) {
    LOG_WARN("fail to push back", K(ret), K(task_info));
  }
  return ret;
}

// the code try to invoke this func has the responsbility to guarantee
// there is only one task info for each partition
int ObModifyQuorumTask::add_modify_quorum_task_info(const ObModifyQuorumTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_infos_.push_back(task_info))) {
    LOG_WARN("fail to push back", K(ret), K(task_info));
  }
  return ret;
}

// the code try to invoke this func has the responsbility to guarantee
// there is only one task info for each partition
int ObRemoveNonPaxosTask::add_remove_non_paxos_replica_task_info(const ObRemoveNonPaxosTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_infos_.push_back(task_info))) {
    LOG_WARN("fail to push back", K(ret), K(task_info));
  }
  return ret;
}

int ObFastRecoveryTask::build(const ObFastRecoveryTaskInfo& task_info, const uint64_t dest_unit_id, const char* comment)
{
  int ret = OB_SUCCESS;
  task_infos_.reset();
  if (OB_FAIL(set_task_id(GCTX.self_addr_))) {
    LOG_WARN("fail to set task id", K(ret));
  } else if (OB_FAIL(task_infos_.push_back(task_info))) {
    LOG_WARN("fail to push back", K(ret));
  } else {
    dst_ = task_info.dst_;
    dest_unit_id_ = dest_unit_id;
    comment_ = comment;
  }
  return ret;
}

int ObFastRecoveryTask::get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
    const int64_t min_task_timeout_us, int64_t& timeout_us) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(network_bandwidth <= 0 || server_concurrency_limit <= 0 || min_task_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(network_bandwidth), K(server_concurrency_limit), K(min_task_timeout_us));
  } else {
    timeout_us = INT64_MAX;
  }
  return ret;
}

int ObFastRecoveryTask::check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& is_available) const
{
  int ret = OB_SUCCESS;
  is_available = true;
  if (OB_FAIL(task_mgr.check_dest_server_migrate_in_blocked(*this, is_available))) {
    LOG_WARN("check dest server migrate in blocked failed", K(ret));
  } else if (!is_available) {
    // no need to check any more
  } else if (OB_FAIL(task_mgr.check_dest_server_out_of_disk(*this, is_available))) {
    LOG_WARN("check dest server outof disk failed", K(ret));
  } else if (!is_available) {
    // no need to check any more
  } else if (OB_FAIL(task_mgr.check_dest_server_has_too_many_partitions(*this, is_available))) {
    LOG_WARN("check dest server has too many partitions failed", K(ret));
  }
  return ret;
}

int ObFastRecoveryTask::log_execute_result(const common::ObIArray<int>& rc_array) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (get_executor_time() > 0) ? (now - get_executor_time()) : (now - get_schedule_time());
  if (OB_UNLIKELY(rc_array.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ret code count not match", K(ret), "ret_code_count", rc_array.count());
  } else {
    for (int64_t i = 0; i < task_infos_.count(); i++) {
      const ObFastRecoveryTaskInfo& info = task_infos_.at(i);
      ROOTSERVICE_EVENT_ADD("balancer",
          "finish_fast_recovery_task",
          "tenant_id",
          info.get_tenant_id(),
          "file_id",
          info.get_file_id(),
          "source",
          info.get_src(),
          "destination",
          info.get_dst(),
          "result",
          rc_array.at(i),
          K(elapsed));
    }
  }
  return ret;
}

int ObFastRecoveryTask::clone(void* input_ptr, ObRebalanceTask*& output_task) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == input_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(input_ptr));
  } else {
    ObFastRecoveryTask* my_task = new (input_ptr) ObFastRecoveryTask();
    if (OB_UNLIKELY(nullptr == my_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObFastRecoveryTask construct failed", K(ret));
    } else if (OB_FAIL(my_task->ObRebalanceTask::deep_copy(*this))) {
      LOG_WARN("fail to deep copy base task", K(ret));
    } else if (OB_FAIL(my_task->task_infos_.assign(task_infos_))) {
      LOG_WARN("fail to assign task infos", K(ret));
    } else {
      my_task->dest_unit_id_ = dest_unit_id_;
    }
    if (OB_SUCC(ret)) {
      output_task = my_task;
    }
  }
  return ret;
}

int ObFastRecoveryTask::check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  UNUSED(pt_operator);
  UNUSED(leader);
  return ret;
}

int ObFastRecoveryTask::execute(
    obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader, common::ObIArray<int>& dummy_rc_array) const
{
  int ret = OB_SUCCESS;
  UNUSED(rpc_proxy);
  UNUSED(dummy_leader);
  UNUSED(dummy_rc_array);
  return ret;
}

int ObFastRecoveryTask::log_execute_start() const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < task_infos_.count(); ++i) {
    auto& info = task_infos_.at(i);
    ROOTSERVICE_EVENT_ADD("balancer",
        "start_fast_recovery_task",
        "tenant_id",
        info.get_tenant_id(),
        "file_id",
        info.get_file_id(),
        "source",
        info.get_src(),
        "destination",
        info.get_dst(),
        "comment",
        get_comment());
  }
  return ret;
}

int ObFastRecoveryTask::assign(const ObFastRecoveryTask& that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRebalanceTask::assign(that))) {
    LOG_WARN("fail to deep copy base task", K(ret));
  } else if (OB_FAIL(task_infos_.assign(that.task_infos_))) {
    LOG_WARN("fail to assign task infos", K(ret));
  } else {
    dest_unit_id_ = that.dest_unit_id_;
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
