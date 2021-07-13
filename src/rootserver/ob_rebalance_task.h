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

#ifndef OCEANBASE_ROOTSERVER_OB_REBALANCE_TASK_H_
#define OCEANBASE_ROOTSERVER_OB_REBALANCE_TASK_H_

#include "lib/hash/ob_hashtable.h"
#include "lib/hash/ob_refered_map.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/net/ob_addr.h"
#include "share/ob_rpc_struct.h"
#include "share/restore/ob_restore_args.h"
#include "storage/ob_partition_service_rpc.h"
#include "sql/executor/ob_bkgd_dist_task.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace share {
class ObPartitionTableOperator;
}
namespace obrpc {
class ObSrvRpcProxy;
}
namespace sql {
class ObBKGDTaskCompleteArg;
class ObBKGDDistExecuteArg;
}  // namespace sql

namespace rootserver {
class ObRebalanceTaskMgr;
class ObRebalanceTaskQueue;
struct ObServerTaskStat {
  ObServerTaskStat()
      : addr_(),
        in_schedule_in_cnt_(0),
        in_schedule_out_cnt_(0),
        total_in_cnt_(0),
        total_out_cnt_(0),
        total_in_data_size_(0),
        total_out_data_size_(0),
        data_in_limit_ts_(0),
        in_schedule_backup_cnt_(0)
  {}

  const common::ObAddr& get_key() const
  {
    return addr_;
  }
  void set_key(const common::ObAddr& key)
  {
    addr_ = key;
  }

  bool is_valid() const  // %addr_ can be invalid
  {
    return in_schedule_in_cnt_ >= 0 && in_schedule_out_cnt_ >= 0 && total_in_cnt_ >= 0 && total_out_cnt_ >= 0 &&
           total_in_data_size_ >= 0 && total_out_data_size_ >= 0 && in_schedule_backup_cnt_ >= 0;
  }

  TO_STRING_KV(K_(addr), K_(in_schedule_in_cnt), K_(in_schedule_out_cnt), K_(total_in_cnt), K_(total_out_cnt),
      K_(total_in_data_size), K_(total_out_data_size), K_(data_in_limit_ts), K_(in_schedule_backup_cnt));

  common::ObAddr addr_;
  // after batch migration is introduced, rs migration task sent to observer
  // may contain more than one partition. all sub tasks are executed serially
  // on the destination observer, so only one thread is occupied during one task is executed

  // copy data in task count executed on observer parallelly
  int64_t in_schedule_in_cnt_;
  // copy data out task count executed on observer parallelly
  int64_t in_schedule_out_cnt_;
  // copy data in task count executed on observer parallelly
  int64_t total_in_cnt_;
  // copy data out task count executed on observer parallelly
  int64_t total_out_cnt_;
  // copy data in task count executed on observer parallelly
  int64_t total_in_data_size_;
  // copy data out task count executed on observer parallelly
  int64_t total_out_data_size_;
  // used to specify if the observer is up to limit,
  // and it records the timestamp
  int64_t data_in_limit_ts_;
  // backup and validate task count executed on observer parallelly
  int64_t in_schedule_backup_cnt_;
};

struct ObTenantTaskStat {
  ObTenantTaskStat() : tenant_id_(common::OB_INVALID_ID), high_priority_task_cnt_(0), low_priority_task_cnt_(0)
  {}

  const uint64_t& get_key() const
  {
    return tenant_id_;
  }
  void set_key(const uint64_t key)
  {
    tenant_id_ = key;
  }

  bool is_valid()
  {
    return common::OB_INVALID_ID != tenant_id_ && high_priority_task_cnt_ >= 0 && low_priority_task_cnt_ >= 0;
  }

  TO_STRING_KV(K_(tenant_id), K_(high_priority_task_cnt), K_(low_priority_task_cnt));

  uint64_t tenant_id_;
  int64_t high_priority_task_cnt_;
  int64_t low_priority_task_cnt_;
};

typedef common::hash::ObReferedMap<common::ObAddr, ObServerTaskStat> ObServerTaskStatMap;

typedef common::hash::ObReferedMap<uint64_t, ObTenantTaskStat> ObTenantTaskStatMap;

struct OnlineReplica {
  OnlineReplica() : unit_id_(common::OB_INVALID_ID), zone_(), member_()
  {}
  TO_STRING_KV(K_(zone), K_(unit_id), K_(member));
  bool is_valid() const
  {
    return member_.is_valid();
  }
  const ObAddr& get_server() const
  {
    return member_.get_server();
  }
  common::ObReplicaType get_replica_type() const
  {
    return member_.get_replica_type();
  }
  void reset()
  {
    unit_id_ = common::OB_INVALID_ID;
    zone_.reset();
    member_.reset();
  }

  uint64_t unit_id_;
  // zone_ member in this struct is used to record information in event history table
  common::ObZone zone_;
  common::ObReplicaMember member_;
};

// ReplicaInfo: this struct is used only in event history record
struct ReplicaInfo {
  ReplicaInfo(const ObPartitionKey& pkey, int64_t data_size) : pkey_(pkey), data_size_(data_size)
  {}

  TO_STRING_KV(K_(pkey), K_(data_size));

  const ObPartitionKey& pkey_;
  int64_t data_size_;
};

enum class RebalanceKeyType : int64_t {
  FORMAL_BALANCE_KEY = 0,
  SQL_BKG_DIST_KEY,
  BACKUP_MANAGER_KEY,
  FAST_RECOVER_KEY,
  REBALANCE_KEY_INVALID,
};

/* 1 in the previous implementation, the key of rebalance task queue map is the partition key, after the refactor
 *   of rebalance task manager, A single partition key cannot satisfy the requirement of the current implementation,
 *   two reasons as follows:
 *   1.1 when copy baseline data for building local index, the task key is the partition key of its data table.
 *       only one baseline data copy task can be executed for a data table even when more than one local index
 *       building task exists.
 *   1.2 when building global index, we have a new task type called single replica baseline data generation task,
 *       the key of single replica baseline data generation task is not any partition key.
 * 2 after the refactor of rebalance task manager implementation, the new task key of the rebalance task map queue is
 *   a combination of the following fields:
 *   2.1 four general uint64_t values
 *   2.2 a task_type which is used to specify the rebalance task type.
 */
class ObRebalanceTaskKey {
public:
  ObRebalanceTaskKey()
      : key_1_(-1),
        key_2_(-1),
        key_3_(-1),
        key_4_(-1),
        key_type_(RebalanceKeyType::REBALANCE_KEY_INVALID),
        hash_value_(0)
  {}
  virtual ~ObRebalanceTaskKey()
  {}

public:
  bool is_valid() const;
  bool operator==(const ObRebalanceTaskKey& that) const;
  ObRebalanceTaskKey& operator=(const ObRebalanceTaskKey& that);
  uint64_t hash() const;
  int init(const uint64_t key_1, const uint64_t key_2, const uint64_t key_3, const uint64_t key_4,
      const RebalanceKeyType key_type);
  int init(const ObRebalanceTaskKey& that);
  TO_STRING_KV(K_(key_1), K_(key_2), K_(key_3), K_(key_4), K_(key_type));

private:
  uint64_t inner_hash() const;

private:
  uint64_t key_1_;
  uint64_t key_2_;
  uint64_t key_3_;
  uint64_t key_4_;
  RebalanceKeyType key_type_;
  uint64_t hash_value_;
};

enum ObRebalanceTaskType : int64_t {
  MIGRATE_REPLICA = 0,
  ADD_REPLICA,
  REBUILD_REPLICA,
  TYPE_TRANSFORM,
  MEMBER_CHANGE,
  REMOVE_NON_PAXOS_REPLICA,
  RESTORE_REPLICA,
  MODIFY_QUORUM,
  COPY_GLOBAL_INDEX_SSTABLE,  // no used
  // copy local index sstable task is used by standby cluster, when copy
  // the local index sstable from primary cluster, it is a inter-cluster task
  COPY_LOCAL_INDEX_SSTABLE,  // no used
  COPY_SSTABLE,
  SQL_BACKGROUND_DIST_TASK,  // background executing sql distributed task
  BACKUP_REPLICA,
  PHYSICAL_RESTORE_REPLICA,
  FAST_RECOVERY_TASK,
  VALIDATE_BACKUP,
  STANDBY_CUTDATA,
  MAX_TYPE,
};

enum ObRebalanceTaskPriority {
  HIGH_PRI = 0,
  LOW_PRI,
  MAX_PRI,
};

class ObRebalanceTask;
class ObMigrateReplicaTask;
class ObTypeTransformTask;
class ObAddReplicaTask;
class ObRebuildReplicaTask;
class ObModifyQuorumTask;
class ObRestoreTask;
class ObPhysicalRestoreTask;
class ObCopySSTableTask;
class ObRemoveMemberTask;
class ObRemoveNonPaxosTask;
class ObRebalanceSqlBKGTask;
class ObRebalanceTask;

/* this RebalanceTaskInfo is the base class,
 * derived classes include Migrate/Add/Type transform/Remove and so on
 */
class ObRebalanceTaskInfo {
public:
  friend class ObRebalanceTask;
  friend class ObMigrateReplicaTask;
  friend class ObTypeTransformTask;
  friend class ObAddReplicaTask;
  friend class ObRebuildReplicaTask;
  friend class ObModifyQuorumTask;
  friend class ObRestoreTask;
  friend class ObPhysicalRestoreTask;
  friend class ObCopySSTableTask;
  friend class ObRemoveMemberTask;
  friend class ObRemoveNonPaxosTask;
  friend class ObRebalanceSqlBKGTask;
  friend class ObBackupTask;
  friend class ObValidateTask;

public:
  ObRebalanceTaskInfo()
      : task_key_(),
        pkey_(),
        cluster_id_(-1),
        transmit_data_size_(0),
        accumulate_src_transmit_data_size_(0),
        accumulate_dst_transmit_data_size_(0),
        src_server_stat_(nullptr),
        dest_server_stat_(nullptr),
        tenant_stat_(nullptr),
        sibling_in_schedule_(false),
        host_(nullptr),
        op_type_(obrpc::ObAdminClearBalanceTaskArg::AUTO),
        skip_change_member_list_(false),
        switchover_epoch_(GCTX.get_switch_epoch2())
  {}
  virtual ~ObRebalanceTaskInfo()
  {}

public:
  virtual ObRebalanceTaskType get_rebalance_task_type() const = 0;
  virtual bool need_process_failed_task() const
  {
    return true;
  }
  /* rebalance task mgr related virtual interface
   */
  virtual const common::ObAddr& get_faillist_anchor() const = 0;
  virtual bool need_check_specific_src_data_volumn() const = 0;
  virtual bool need_check_specific_dst_data_volumn() const = 0;
  virtual common::ObAddr get_specific_server_data_src() const = 0;
  virtual common::ObAddr get_specific_server_data_dst() const = 0;
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const = 0;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const = 0;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const = 0;
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const = 0;
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const = 0;

public:
  uint64_t get_tenant_id() const
  {
    return pkey_.get_tenant_id();
  }
  const ObRebalanceTaskKey& get_rebalance_task_key() const
  {
    return task_key_;
  }
  bool is_consistency(const uint64_t tenant_id, const obrpc::ObAdminClearBalanceTaskArg::TaskType type) const;
  bool is_manual() const;
  void set_cluster_id(const int64_t cluster_id)
  {
    cluster_id_ = cluster_id;
  }
  int64_t get_cluster_id() const
  {
    return cluster_id_;
  }
  void set_transmit_data_size(const int64_t size)
  {
    transmit_data_size_ = size;
  }
  void accumulate_transmit_src_data_size(const int64_t accumulate_size)
  {
    accumulate_src_transmit_data_size_ += accumulate_size;
  }
  void accumulate_transmit_dst_data_size(const int64_t accumulate_size)
  {
    accumulate_dst_transmit_data_size_ += accumulate_size;
  }
  int64_t get_accumulate_transmit_src_data_size() const
  {
    return accumulate_src_transmit_data_size_;
  }
  int64_t get_accumulate_transmit_dst_data_size() const
  {
    return accumulate_dst_transmit_data_size_;
  }
  int64_t get_transmit_data_size() const
  {
    return transmit_data_size_;
  }
  int set_src_server_stat(ObServerTaskStatMap::Item* src_server_stat);
  int set_dst_server_stat(ObServerTaskStatMap::Item* dest_server_stat);
  int set_tenant_stat(ObTenantTaskStatMap::Item* tenant_stat, ObRebalanceTaskPriority task_priority);
  const ObServerTaskStatMap::Item* get_src_server_stat() const
  {
    return src_server_stat_;
  }
  const ObServerTaskStatMap::Item* get_dest_server_stat() const
  {
    return dest_server_stat_;
  }
  void set_schedule();
  bool is_sibling_in_schedule() const
  {
    return sibling_in_schedule_;
  }
  void set_sibling_in_schedule(bool is_schedule)
  {
    sibling_in_schedule_ = is_schedule;
  }
  int set_host(const ObRebalanceTask* host);
  const ObRebalanceTask* get_host() const
  {
    return host_;
  }
  void clean_server_and_tenant_ref(const ObRebalanceTask& task);
  virtual TO_STRING_KV(K_(task_key), K_(pkey), K_(cluster_id), K_(transmit_data_size),
      K_(accumulate_src_transmit_data_size), K_(accumulate_dst_transmit_data_size), K_(op_type),
      K_(skip_change_member_list));
  int set_skip_change_member_list(const ObRebalanceTaskType task_type, const common::ObReplicaType& src_type,
      const common::ObReplicaType& dst_type, const bool is_restore = false);
  bool can_accumulate_in_standby(const ObRebalanceTaskInfo& other) const;

public:
  int assign(const ObRebalanceTaskInfo& task_info);
  int init_task_key(const uint64_t key_1, const uint64_t key_2, const uint64_t key_3, const uint64_t key_4,
      const RebalanceKeyType key_type)
  {
    return task_key_.init(key_1, key_2, key_3, key_4, key_type);
  }
  int init_task_key(const ObRebalanceTaskKey& that)
  {
    return task_key_.init(that);
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const ObRebalanceTaskKey& get_task_key() const
  {
    return task_key_;
  }

public:
  /* disallow copy constructor and operator= */
  ObRebalanceTaskInfo(const ObRebalanceTaskInfo&) = delete;
  ObRebalanceTaskInfo& operator=(const ObRebalanceTaskInfo&) = delete;

public:
  int check_leader(const ObRebalanceTask& task, const share::ObPartitionInfo& partition, common::ObAddr& leader) const;
  int update_with_partition(const common::ObAddr& dst) const;
  int try_update_unit_id(const share::ObPartitionInfo& partition, const OnlineReplica& online_replica) const;
  int force_check_partition_table(const share::ObPartitionInfo& partition) const;

protected:
  ObRebalanceTaskKey task_key_;
  common::ObPartitionKey pkey_;
  int64_t cluster_id_;
  /* transmit_data_size_ is the data transmission volumn when this task info is executed,
   * when a migrate/add task is executed, transmit_data_size_ is the data size of the replica,
   * when a quorum modification/replica type transform task is executed, no data needs to be
   * transmitted, so the tranmit_data_size_ is set to zero.
   */
  int64_t transmit_data_size_;
  int64_t accumulate_src_transmit_data_size_;
  int64_t accumulate_dst_transmit_data_size_;
  ObServerTaskStatMap::Item* src_server_stat_;
  ObServerTaskStatMap::Item* dest_server_stat_;
  ObTenantTaskStatMap::Item* tenant_stat_;
  bool sibling_in_schedule_;
  const ObRebalanceTask* host_;
  obrpc::ObAdminClearBalanceTaskArg::TaskType op_type_;
  bool skip_change_member_list_;
  int64_t switchover_epoch_;
};

class ObFastRecoveryTask;
class ObFastRecoveryTaskInfo : public ObRebalanceTaskInfo {
public:
  friend class ObFastRecoveryTask;

public:
  ObFastRecoveryTaskInfo() : ObRebalanceTaskInfo(), src_(), dst_(), tenant_id_(OB_INVALID_ID), file_id_(-1)
  {}
  virtual ~ObFastRecoveryTaskInfo()
  {}

public:
  int build(const common::ObAddr& src, const common::ObAddr& dst, const common::ObAddr& rescue_server,
      const uint64_t tenant_id, const int64_t file_id);
  int assign(const ObFastRecoveryTaskInfo& task_info);
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_file_id() const
  {
    return file_id_;
  }
  const common::ObAddr& get_src() const
  {
    return src_;
  }
  const common::ObAddr& get_dst() const
  {
    return dst_;
  }

public:
  /* virtual method implement
   */
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::FAST_RECOVERY_TASK;
  }
  virtual const common::ObAddr& get_faillist_anchor() const override
  {
    return src_;
  }
  virtual bool need_check_specific_src_data_volumn() const override
  {
    return false;
  }
  virtual bool need_check_specific_dst_data_volumn() const override
  {
    return true;
  }
  virtual common::ObAddr get_specific_server_data_src() const override
  {
    return src_;
  }
  virtual common::ObAddr get_specific_server_data_dst() const override
  {
    return dst_;
  }
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const override;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const override;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return false;
  }
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return true;
  }
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const override;

private:
  common::ObAddr src_;
  common::ObAddr dst_;
  uint64_t tenant_id_;
  int64_t file_id_;
};

class ObMigrateReplicaTask;
class ObMigrateTaskInfo : public ObRebalanceTaskInfo {
public:
  friend class ObMigrateReplicaTask;

public:
  ObMigrateTaskInfo() : ObRebalanceTaskInfo(), dst_(), src_(), data_src_(), quorum_(0)
  {}
  virtual ~ObMigrateTaskInfo()
  {}

public:
  int build(const obrpc::MigrateMode migrate_mode, const OnlineReplica& dst, const common::ObPartitionKey& pkey,
      const common::ObReplicaMember& src, const common::ObReplicaMember& data_src, const int64_t quorum,
      const bool is_restore, const bool is_manual = false);
  int assign(const ObMigrateTaskInfo& task_info);

public:
  /* virtual method implement
   */
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::MIGRATE_REPLICA;
  }
  virtual const common::ObAddr& get_faillist_anchor() const override
  {
    return src_.get_server();
  }
  virtual bool need_check_specific_src_data_volumn() const override
  {
    return true;
  }
  virtual bool need_check_specific_dst_data_volumn() const override
  {
    return true;
  }
  virtual common::ObAddr get_specific_server_data_src() const override
  {
    return data_src_.get_server();
  }
  virtual common::ObAddr get_specific_server_data_dst() const override
  {
    return dst_.get_server();
  }
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const override;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const override;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const override
  {
    return curr_leader == src_.get_server();
  }
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const override
  {
    return curr_leader != src_.get_server();
  }
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const override;

public:
  virtual INHERIT_TO_STRING_KV("ObRebalanceTaskInfo", ObRebalanceTaskInfo, K_(dst), K_(src), K_(data_src), K_(quorum),
      "task_type", get_rebalance_task_type());
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const OnlineReplica& get_dest() const
  {
    return dst_;
  }
  const common::ObReplicaMember& get_src_member() const
  {
    return src_;
  }
  const common::ObReplicaMember& get_data_src_member() const
  {
    return data_src_;
  }
  int64_t get_quorum() const
  {
    return quorum_;
  }

private:
  int check_quorum(const share::ObPartitionInfo& partition_info) const;
  int check_online_replica(const share::ObPartitionInfo& partition_info) const;
  int check_offline_replica(const share::ObPartitionInfo& partition_info) const;

private:
  OnlineReplica dst_;
  common::ObReplicaMember src_;
  common::ObReplicaMember data_src_;
  int64_t quorum_;
};

class ObTypeTransformTask;

class ObTypeTransformTaskInfo : public ObRebalanceTaskInfo {
public:
  friend class ObTypeTransformTask;

public:
  ObTypeTransformTaskInfo() : ObRebalanceTaskInfo(), dst_(), src_(), data_src_(), quorum_(0)
  {}
  virtual ~ObTypeTransformTaskInfo()
  {}

public:
  int build(const OnlineReplica& dst, const common::ObPartitionKey& pkey,
      const common::ObReplicaMember& src,       // replica before this transform
      const common::ObReplicaMember& data_src,  // data source of this task
      const int64_t quorum, const bool is_restore, const bool is_manual = false);
  int assign(const ObTypeTransformTaskInfo& task_info);

public:
  /* virtual methods implement
   */
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::TYPE_TRANSFORM;
  }
  virtual const common::ObAddr& get_faillist_anchor() const override
  {
    return data_src_.get_server();
  }
  virtual bool need_check_specific_src_data_volumn() const override
  {
    return true;
  }
  virtual bool need_check_specific_dst_data_volumn() const override
  {
    return true;
  }
  virtual common::ObAddr get_specific_server_data_src() const override
  {
    return data_src_.get_server();
  }
  virtual common::ObAddr get_specific_server_data_dst() const override
  {
    return dst_.get_server();
  }
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const override;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const override;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const override
  {
    return curr_leader == dst_.get_server();
  }
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const override
  {
    return curr_leader != dst_.get_server();
  }
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const override;

public:
  virtual INHERIT_TO_STRING_KV("ObRebalanceTaskInfo", ObRebalanceTaskInfo, K_(dst), K_(src), K_(data_src), K_(quorum),
      "task_type", get_rebalance_task_type());
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const OnlineReplica& get_dest() const
  {
    return dst_;
  }
  const common::ObReplicaMember& get_src_member() const
  {
    return src_;
  }
  const common::ObReplicaMember& get_data_src_member() const
  {
    return data_src_;
  }
  int64_t get_quorum() const
  {
    return quorum_;
  }

protected:
  int check_quorum(const share::ObPartitionInfo& partition) const;
  int check_online_replica(const share::ObPartitionInfo& partition) const;
  int check_paxos_member(const share::ObPartitionInfo& partition) const;

private:
  OnlineReplica dst_;
  common::ObReplicaMember src_;
  common::ObReplicaMember data_src_;
  int64_t quorum_;
};

class ObAddReplicaTask;

class ObAddTaskInfo : public ObRebalanceTaskInfo {
public:
  friend class ObAddReplicaTask;

public:
  ObAddTaskInfo() : ObRebalanceTaskInfo(), dst_(), data_src_(), quorum_(0)
  {}
  virtual ~ObAddTaskInfo()
  {}

public:
  int build(const OnlineReplica& dst, const common::ObPartitionKey& pkey, const common::ObReplicaMember& data_src,
      const int64_t quorum, const bool is_restore, const bool is_manual = false);
  int assign(const ObAddTaskInfo& task_info);

public:
  /* virtual method implement
   */
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::ADD_REPLICA;
  }
  virtual const common::ObAddr& get_faillist_anchor() const override
  {
    return data_src_.get_server();
  }
  virtual bool need_check_specific_src_data_volumn() const override
  {
    return true;
  }
  virtual bool need_check_specific_dst_data_volumn() const override
  {
    return true;
  }
  virtual common::ObAddr get_specific_server_data_src() const override
  {
    return data_src_.get_server();
  }
  virtual common::ObAddr get_specific_server_data_dst() const override
  {
    return dst_.get_server();
  }
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const override;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const override;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return false;
  }
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return true;
  }
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const override;

public:
  virtual INHERIT_TO_STRING_KV("ObRebalanceTaskInfo", ObRebalanceTaskInfo, K_(dst), K_(data_src), K_(quorum),
      "task_type", get_rebalance_task_type());
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const common::ObReplicaMember& get_data_src_member() const
  {
    return data_src_;
  }
  const OnlineReplica& get_dest() const
  {
    return dst_;
  }
  int64_t get_quorum() const
  {
    return quorum_;
  }

protected:
  int check_quorum(const share::ObPartitionInfo& partition, const bool is_admin_force) const;
  int check_online_replica(const share::ObPartitionInfo& partition) const;
  int check_paxos_member(const share::ObPartitionInfo& partition) const;

private:
  OnlineReplica dst_;
  common::ObReplicaMember data_src_;
  int64_t quorum_;
};

class ObRebuildReplicaTask;

class ObRebuildTaskInfo : public ObRebalanceTaskInfo {
public:
  friend class ObRebuildReplicaTask;

public:
  ObRebuildTaskInfo() : ObRebalanceTaskInfo(), dst_(), data_src_()
  {
    skip_change_member_list_ = true;
  }
  virtual ~ObRebuildTaskInfo()
  {}

public:
  int build(const OnlineReplica& dst, const common::ObPartitionKey& pkey, const common::ObReplicaMember& data_src,
      const bool is_manual = false);
  int assign(const ObRebuildTaskInfo& task_info);

public:
  /* virtual method implment
   */
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::REBUILD_REPLICA;
  }
  virtual const common::ObAddr& get_faillist_anchor() const override
  {
    return data_src_.get_server();
  }
  virtual bool need_check_specific_src_data_volumn() const override
  {
    return true;
  }
  virtual bool need_check_specific_dst_data_volumn() const override
  {
    return true;
  }
  virtual common::ObAddr get_specific_server_data_src() const override
  {
    return data_src_.get_server();
  }
  virtual common::ObAddr get_specific_server_data_dst() const override
  {
    return dst_.get_server();
  }
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const override;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const override;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return false;
  }
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return true;
  }
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTaskInfo", ObRebalanceTaskInfo, K_(dst), K_(data_src), "task_type", get_rebalance_task_type());
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const common::ObReplicaMember& get_data_src_member() const
  {
    return data_src_;
  }
  const OnlineReplica& get_dest() const
  {
    return dst_;
  }

private:
  OnlineReplica dst_;
  common::ObReplicaMember data_src_;
};

class ObModifyQuorumTask;

class ObModifyQuorumTaskInfo : public ObRebalanceTaskInfo {
public:
  friend class ObModifiQuorumTask;

public:
  ObModifyQuorumTaskInfo() : ObRebalanceTaskInfo(), dst_(), quorum_(0), orig_quorum_(0), member_list_()
  {
    skip_change_member_list_ = true;
  }
  virtual ~ObModifyQuorumTaskInfo()
  {}

public:
  int build(const OnlineReplica& dst, const common::ObPartitionKey& pkey, const int64_t quorum,
      const int64_t orig_quorum, ObMemberList& member_list);
  int assign(const ObModifyQuorumTaskInfo& task_info);

public:
  /* virtual methods implement
   */
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::MODIFY_QUORUM;
  }
  virtual const common::ObAddr& get_faillist_anchor() const override
  {
    return dst_.get_server();
  }
  virtual bool need_check_specific_src_data_volumn() const override
  {
    return false;
  }
  virtual bool need_check_specific_dst_data_volumn() const override
  {
    return false;
  }
  virtual common::ObAddr get_specific_server_data_src() const override
  {
    return common::ObAddr();  // return an invalid value
  }
  virtual common::ObAddr get_specific_server_data_dst() const override
  {
    return common::ObAddr();  // return an invalid value
  }
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const override;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const override;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return false;
  }
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return true;
  }
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const override;

public:
  virtual INHERIT_TO_STRING_KV("ObRebalanceTaskInfo", ObRebalanceTaskInfo, K_(dst), K_(quorum), K_(orig_quorum),
      K_(member_list), "task_type", get_rebalance_task_type());
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const OnlineReplica& get_dest() const
  {
    return dst_;
  }
  int64_t get_quorum() const
  {
    return quorum_;
  }
  int64_t get_orig_quorum() const
  {
    return orig_quorum_;
  }
  const common::ObMemberList& get_member_list() const
  {
    return member_list_;
  }

private:
  OnlineReplica dst_;
  int64_t quorum_;
  int64_t orig_quorum_;
  common::ObMemberList member_list_;
};

class ObRestoreTask;

class ObRestoreTaskInfo : public ObRebalanceTaskInfo {
public:
  friend class ObRestoreTask;

public:
  ObRestoreTaskInfo() : ObRebalanceTaskInfo(), dst_(), restore_arg_()
  {
    skip_change_member_list_ = true;
  }
  virtual ~ObRestoreTaskInfo()
  {}

public:
  int build(const common::ObPartitionKey& pkey, const share::ObRestoreArgs& restore_arg, const OnlineReplica& dst);
  int assign(const ObRestoreTaskInfo& task_info);

public:
  /* virtual methods implement
   */
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::RESTORE_REPLICA;
  }
  virtual const common::ObAddr& get_faillist_anchor() const override
  {
    return dst_.get_server();
  }
  virtual bool need_check_specific_src_data_volumn() const override
  {
    return false;
  }
  virtual bool need_check_specific_dst_data_volumn() const override
  {
    return true;
  }
  virtual common::ObAddr get_specific_server_data_src() const override
  {
    return common::ObAddr();  // return an invalid value
  }
  virtual common::ObAddr get_specific_server_data_dst() const override
  {
    return dst_.get_server();
  }
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const override;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const override;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return false;
  }
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return true;
  }
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTaskInfo", ObRebalanceTaskInfo, K_(dst), K_(restore_arg), "task_type", get_rebalance_task_type());
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const OnlineReplica& get_dest() const
  {
    return dst_;
  }
  const share::ObRestoreArgs& get_restore_arg() const
  {
    return restore_arg_;
  }

private:
  OnlineReplica dst_;
  share::ObRestoreArgs restore_arg_;
};

class ObPhysicalRestoreTask;
class ObPhysicalRestoreTaskInfo : public ObRebalanceTaskInfo {
public:
  friend class ObPhysicalRestoreTask;

public:
  ObPhysicalRestoreTaskInfo() : ObRebalanceTaskInfo(), dst_(), restore_arg_()
  {
    skip_change_member_list_ = true;
  }
  virtual ~ObPhysicalRestoreTaskInfo()
  {}

public:
  int build(
      const common::ObPartitionKey& pkey, const share::ObPhysicalRestoreArg& restore_arg, const OnlineReplica& dst);
  int assign(const ObPhysicalRestoreTaskInfo& task_info);

public:
  /* virtual methods implement
   */
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::PHYSICAL_RESTORE_REPLICA;
  }
  virtual const common::ObAddr& get_faillist_anchor() const override
  {
    return dst_.get_server();
  }
  virtual bool need_check_specific_src_data_volumn() const override
  {
    return false;
  }
  virtual bool need_check_specific_dst_data_volumn() const override
  {
    return true;
  }
  virtual common::ObAddr get_specific_server_data_src() const override
  {
    return common::ObAddr();  // return an invalid value
  }
  virtual common::ObAddr get_specific_server_data_dst() const override
  {
    return dst_.get_server();
  }
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const override;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const override;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return false;
  }
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return true;
  }
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTaskInfo", ObRebalanceTaskInfo, K_(dst), K_(restore_arg), "task_type", get_rebalance_task_type());
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const OnlineReplica& get_dest() const
  {
    return dst_;
  }
  const share::ObPhysicalRestoreArg& get_restore_arg() const
  {
    return restore_arg_;
  }

private:
  OnlineReplica dst_;
  share::ObPhysicalRestoreArg restore_arg_;
};

class ObCopySSTableTask;

class ObCopySSTableTaskInfo : public ObRebalanceTaskInfo {
public:
  friend class ObCopySSTableTask;

public:
  ObCopySSTableTaskInfo() : ObRebalanceTaskInfo(), dst_(), data_src_(), index_table_id_(common::OB_INVALID_ID)
  {
    skip_change_member_list_ = true;
  }
  virtual ~ObCopySSTableTaskInfo()
  {}

public:
  int build(const common::ObPartitionKey& pkey, const OnlineReplica& dst, const common::ObReplicaMember& data_src,
      const int64_t index_table_id = common::OB_INVALID_ID);
  int assign(const ObCopySSTableTaskInfo& task_info);

public:
  /* virtual methods implement
   */
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::COPY_SSTABLE;
  }
  virtual const common::ObAddr& get_faillist_anchor() const override
  {
    return data_src_.get_server();
  }
  virtual bool need_check_specific_src_data_volumn() const override
  {
    return true;
  }
  virtual bool need_check_specific_dst_data_volumn() const override
  {
    return true;
  }
  virtual common::ObAddr get_specific_server_data_src() const override
  {
    return data_src_.get_server();
  }
  virtual common::ObAddr get_specific_server_data_dst() const override
  {
    return dst_.get_server();
  }
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const override;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const override;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return false;
  }
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return true;
  }
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const override;

public:
  virtual INHERIT_TO_STRING_KV("ObRebalanceTaskInfo", ObRebalanceTaskInfo, K_(dst), K_(data_src), K_(index_table_id),
      "task_type", get_rebalance_task_type());
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const OnlineReplica& get_dest() const
  {
    return dst_;
  }
  const common::ObReplicaMember& get_data_src_member() const
  {
    return data_src_;
  }
  uint64_t get_index_table_id() const
  {
    return index_table_id_;
  }

private:
  OnlineReplica dst_;
  common::ObReplicaMember data_src_;
  uint64_t index_table_id_;
};

class ObRemoveMemberTask;

class ObRemoveMemberTaskInfo : public ObRebalanceTaskInfo {
public:
  friend class ObRemoveMemberTask;

public:
  ObRemoveMemberTaskInfo() : ObRebalanceTaskInfo(), dst_(), quorum_(0), orig_quorum_(0), check_leader_(true)
  {}
  virtual ~ObRemoveMemberTaskInfo()
  {}

public:
  int build(const OnlineReplica& dst, const common::ObPartitionKey& pkey, const int64_t quorum,
      const int64_t orig_quorum, const bool check_leader = true);
  int assign(const ObRemoveMemberTaskInfo& task_info);

public:
  /* virtual methods implement
   */
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::MEMBER_CHANGE;
  }
  virtual const common::ObAddr& get_faillist_anchor() const override
  {
    return dst_.get_server();
  }
  virtual bool need_check_specific_src_data_volumn() const override
  {
    return false;
  }
  virtual bool need_check_specific_dst_data_volumn() const override
  {
    return false;
  }
  virtual common::ObAddr get_specific_server_data_src() const override
  {
    return common::ObAddr();  // return an invalid value
  }
  virtual common::ObAddr get_specific_server_data_dst() const override
  {
    return common::ObAddr();  // return an invalid value
  }
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const override;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const override;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const override
  {
    return curr_leader == dst_.get_server();
  }
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const override
  {
    return curr_leader != dst_.get_server();
  }
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const override;

public:
  virtual INHERIT_TO_STRING_KV("ObRebalanceTaskInfo", ObRebalanceTaskInfo, K_(dst), K_(quorum), K_(orig_quorum),
      K_(check_leader), "task_type", get_rebalance_task_type());
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const OnlineReplica& get_dest() const
  {
    return dst_;
  }
  int64_t get_quorum() const
  {
    return quorum_;
  }
  int64_t get_orig_quorum() const
  {
    return orig_quorum_;
  }
  bool need_check_leader() const
  {
    return check_leader_;
  }

private:
  OnlineReplica dst_;
  int64_t quorum_;
  int64_t orig_quorum_;
  bool check_leader_;
};

class ObRemoveNonPaxosTask;

class ObRemoveNonPaxosTaskInfo : public ObRebalanceTaskInfo {
public:
  friend class ObRemoveNonPaxosTask;

public:
  ObRemoveNonPaxosTaskInfo() : ObRebalanceTaskInfo(), dst_()
  {
    skip_change_member_list_ = true;
  }
  virtual ~ObRemoveNonPaxosTaskInfo()
  {}

public:
  int build(const OnlineReplica& dst, const common::ObPartitionKey& pkey);
  int assign(const ObRemoveNonPaxosTaskInfo& task_info);

public:
  /* virtual methods implement
   */
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::REMOVE_NON_PAXOS_REPLICA;
  }
  virtual const common::ObAddr& get_faillist_anchor() const override
  {
    return dst_.get_server();
  }
  virtual bool need_check_specific_src_data_volumn() const override
  {
    return false;
  }
  virtual bool need_check_specific_dst_data_volumn() const override
  {
    return false;
  }
  virtual common::ObAddr get_specific_server_data_src() const override
  {
    return common::ObAddr();  // return an invalid value
  }
  virtual common::ObAddr get_specific_server_data_dst() const override
  {
    return common::ObAddr();  // return an invalid value
  }
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const override;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const override;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return false;
  }
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return true;
  }
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTaskInfo", ObRebalanceTaskInfo, K_(dst), "task_type", get_rebalance_task_type());
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const OnlineReplica& get_dest() const
  {
    return dst_;
  }

private:
  OnlineReplica dst_;
};

class ObRebalanceSqlBKGTask;

class ObSqlBKGDDistTaskInfo : public ObRebalanceTaskInfo {
public:
  friend class ObRebalanceSqlBKGTask;

public:
  ObSqlBKGDDistTaskInfo() : ObRebalanceTaskInfo(), sql_bkgd_task_(), dst_()
  {
    skip_change_member_list_ = true;
  }
  virtual ~ObSqlBKGDDistTaskInfo()
  {}

public:
  int build(const sql::ObSchedBKGDDistTask& bkgd_task);
  int assign(const ObSqlBKGDDistTaskInfo& that);

public:
  /* virtual methods implement
   */
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::SQL_BACKGROUND_DIST_TASK;
  }
  virtual const common::ObAddr& get_faillist_anchor() const override
  {
    return dst_;
  }
  virtual bool need_check_specific_src_data_volumn() const override
  {
    return false;
  }
  virtual bool need_check_specific_dst_data_volumn() const override
  {
    return false;
  }
  virtual common::ObAddr get_specific_server_data_src() const override
  {
    return common::ObAddr();  // return an invalid value
  }
  virtual common::ObAddr get_specific_server_data_dst() const override
  {
    return common::ObAddr();  // return an invalid value
  }
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const override;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const override;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return false;
  }
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return true;
  }
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTaskInfo", ObRebalanceTaskInfo, K_(dst), K_(sql_bkgd_task), "task_type", get_rebalance_task_type());
  const sql::ObSchedBKGDDistTask& get_sql_bkgd_task() const
  {
    return sql_bkgd_task_;
  }
  sql::ObSchedBKGDDistTask& get_sql_bkgd_task()
  {
    return sql_bkgd_task_;
  }

private:
  sql::ObSchedBKGDDistTask sql_bkgd_task_;
  common::ObAddr dst_;
};

class ObBackupTask;
class ObBackupTaskInfo : public ObRebalanceTaskInfo {
public:
  friend class ObBackupTask;

public:
  ObBackupTaskInfo() : ObRebalanceTaskInfo(), dst_(), data_src_(), physical_backup_arg_()
  {
    skip_change_member_list_ = true;
  }
  virtual ~ObBackupTaskInfo()
  {}

public:
  int build(const common::ObPartitionKey& pkey, const OnlineReplica& dst, const common::ObReplicaMember& data_src,
      const share::ObPhysicalBackupArg& physical_backup_arg);
  int assign(const ObBackupTaskInfo& task_info);

public:
  /* virtual method implement
   */
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::BACKUP_REPLICA;
  }
  virtual const common::ObAddr& get_faillist_anchor() const override
  {
    return dst_.get_server();
  }
  virtual bool need_check_specific_src_data_volumn() const override
  {
    return true;
  }
  virtual bool need_check_specific_dst_data_volumn() const override
  {
    return true;
  }
  virtual common::ObAddr get_specific_server_data_src() const override
  {
    return data_src_.get_server();
  }
  virtual common::ObAddr get_specific_server_data_dst() const override
  {
    return dst_.get_server();
  }
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const override;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const override;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return false;
  }
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return true;
  }
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const override;

public:
  virtual INHERIT_TO_STRING_KV("ObRebalanceTaskInfo", ObRebalanceTaskInfo, K_(dst), K_(data_src),
      K_(physical_backup_arg), "task_type", get_rebalance_task_type());
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const OnlineReplica& get_dest() const
  {
    return dst_;
  }
  const common::ObReplicaMember& get_data_src_member() const
  {
    return data_src_;
  }
  const share::ObPhysicalBackupArg& get_physical_backup_arg() const
  {
    return physical_backup_arg_;
  }

private:
  OnlineReplica dst_;
  common::ObReplicaMember data_src_;
  share::ObPhysicalBackupArg physical_backup_arg_;
};

class ObValidateTaskInfo : public ObRebalanceTaskInfo {
public:
  friend class ObValidateTask;

public:
  ObValidateTaskInfo() : ObRebalanceTaskInfo(), physical_validate_arg_()
  {
    skip_change_member_list_ = true;
  }
  virtual ~ObValidateTaskInfo()
  {}

public:
  int build(const common::ObPartitionKey& pkey, const common::ObReplicaMember& dst,
      const share::ObPhysicalValidateArg& physical_validate_arg);
  int assign(const ObValidateTaskInfo& task_info);

public:
  /* virtual method implement
   */
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::VALIDATE_BACKUP;
  }
  virtual const common::ObAddr& get_faillist_anchor() const override
  {
    return dst_.get_server();
  }
  virtual bool need_check_specific_src_data_volumn() const override
  {
    return false;
  }
  virtual bool need_check_specific_dst_data_volumn() const override
  {
    return true;
  }
  virtual common::ObAddr get_specific_server_data_src() const override
  {
    return dst_.get_server();
  }
  virtual common::ObAddr get_specific_server_data_dst() const override
  {
    return dst_.get_server();
  }
  virtual int get_execute_transmit_size(int64_t& execute_transmit_size) const override;
  /* rebalance task executor related virtual interface
   */
  virtual int check_before_execute(const ObRebalanceTask& task, const share::ObPartitionInfo& partition,
      const bool is_admin_force, common::ObAddr& leader) const override;
  virtual bool need_switch_leader_before_execute(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return false;
  }
  virtual bool is_switch_leader_take_effect(const common::ObAddr& curr_leader) const override
  {
    UNUSED(curr_leader);
    return true;
  }
  /* all_virtual_rebalance_task_stat pure interface
   */
  virtual int get_virtual_rebalance_task_stat_info(
      common::ObAddr& src, common::ObAddr& data_src, common::ObAddr& dest, common::ObAddr& offline) const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTaskInfo", ObRebalanceTaskInfo, K_(physical_validate_arg), "task_type", get_rebalance_task_type());
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const common::ObReplicaMember& get_replica_member() const
  {
    return dst_;
  }
  const share::ObPhysicalValidateArg& get_physical_validate_arg() const
  {
    return physical_validate_arg_;
  }

private:
  share::ObPhysicalValidateArg physical_validate_arg_;
  common::ObReplicaMember dst_;
  common::ObAddr addr_;
};

class ObRebalanceTask : public common::ObDLinkBase<ObRebalanceTask> {
public:
  friend class ObRebalanceTaskMgr;
  friend class ObRebalanceTaskQueue;

public:
  typedef common::hash::ObHashMap<ObRebalanceTaskKey, ObRebalanceTaskInfo*, common::hash::NoPthreadDefendMode>
      TaskInfoMap;
  typedef common::hash::ObHashMap<common::ObAddr, ObRebalanceTaskInfo*, common::hash::NoPthreadDefendMode> AddrTaskMap;

public:
  ObRebalanceTask()
      : generate_time_(common::ObTimeUtility::current_time()),
        priority_(ObRebalanceTaskPriority::LOW_PRI),
        comment_(""),
        schedule_time_(0),
        executor_time_(0),
        task_id_(),
        dst_()
  {}
  virtual ~ObRebalanceTask()
  {}

public:
  /* general pure interfaces
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const = 0;
  virtual ObRebalanceTaskType get_rebalance_task_type() const = 0;
  virtual int64_t get_sub_task_count() const = 0;
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const = 0;
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) = 0;
  /* pure interfaces related to rebalance task mgr
   */
  virtual bool skip_put_to_queue() const = 0;
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const = 0;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const = 0;
  virtual int64_t get_deep_copy_size() const = 0;
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const = 0;
  virtual void notify_cancel() const = 0;
  /* pure interfaces related to rebalance task executor
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const = 0;
  virtual int execute(
      obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& leader, common::ObIArray<int>& rc_array) const = 0;
  virtual int log_execute_start() const = 0;

public:
  virtual void set_schedule()
  {
    for (int64_t i = 0; i < get_sub_task_count(); ++i) {
      if (nullptr != get_sub_task(i)) {
        get_sub_task(i)->set_schedule();
      }
    }
    schedule_time_ = ObTimeUtility::current_time();
  }
  int generate_sub_task_err_code(const int err, common::ObIArray<int>& rc_array) const;
  bool is_high_priority_task() const
  {
    return ObRebalanceTaskPriority::HIGH_PRI == priority_;
  }
  bool is_low_priority_task() const
  {
    return ObRebalanceTaskPriority::LOW_PRI == priority_;
  }
  bool is_valid_priority(const ObRebalanceTaskPriority priority) const
  {
    return ObRebalanceTaskPriority::HIGH_PRI == priority || ObRebalanceTaskPriority::LOW_PRI == priority;
  }
  int set_server_and_tenant_stat(ObServerTaskStatMap& server_map, ObTenantTaskStatMap& tenant_map);
  void reset()
  {
    generate_time_ = common::ObTimeUtility::current_time();
    priority_ = ObRebalanceTaskPriority::LOW_PRI;
    comment_ = "";
    schedule_time_ = 0;
    executor_time_ = 0;
    task_id_.reset();
  }
  bool is_admin_cmd() const;
  const char* get_comment() const
  {
    return comment_;
  }
  bool in_schedule() const
  {
    return schedule_time_ > 0;
  }
  const common::ObAddr& get_dest() const
  {
    return dst_;
  }
  int64_t get_executor_time() const
  {
    return executor_time_;
  }
  void set_executor_time(const int64_t executor_time)
  {
    executor_time_ = executor_time;
  }
  int64_t get_generate_time() const
  {
    return generate_time_;
  }
  void set_generate_time(const int64_t generate_time)
  {
    generate_time_ = generate_time;
  }
  int64_t get_schedule_time() const
  {
    return schedule_time_;
  }
  void set_schedule_time(const int64_t schedule_time)
  {
    schedule_time_ = schedule_time;
  }
  int set_task_id(const common::ObAddr& addr);
  int deep_copy(const ObRebalanceTask& that);
  int assign(const ObRebalanceTask& that)
  {
    return deep_copy(that);
  }
  void clean_task_info_map(TaskInfoMap& task_info_map);
  void clean_server_and_tenant_ref(TaskInfoMap& task_info_map);
  int get_execute_transmit_size(int64_t& execute_transmit_size) const;
  static const char* get_task_type_str(const ObRebalanceTaskType task_type);
  virtual TO_STRING_KV(
      K_(generate_time), K_(priority), K_(comment), K_(schedule_time), K_(executor_time), K_(task_id), K_(dst));

protected:
  typedef common::hash::ObHashMap<common::ObPartitionKey, share::ObPartitionInfo*, common::hash::NoPthreadDefendMode>
      PartitionInfoMap;
  bool can_batch_get_partition_info() const;
  int batch_check_task_partition_condition(const bool is_admin_force) const;
  int check_task_partition_condition(share::ObPartitionTableOperator& pt_operator, const bool is_admin_force) const;
  int extract_task_pkeys(common::ObIArray<common::ObPartitionKey>& pkeys) const;
  int build_task_partition_infos(const uint64_t tenant_id, common::ObIArray<share::ObPartitionInfo>& partition_infos,
      PartitionInfoMap& partition_info_map) const;
  int check_table_schema() const;

protected:
  static const int64_t TIMEOUT_US_PER_PARTITION = 5 * 1000000;  // 5s
private:
  int set_tenant_stat(AddrTaskMap& src_server_map, AddrTaskMap& dst_server_map, ObTenantTaskStatMap& tenant_stat_map);
  int set_server_stat(
      const AddrTaskMap& src_server_map, const AddrTaskMap& dst_server_map, ObServerTaskStatMap& server_stat_map);
  int accumulate_src_addr_server_map(
      AddrTaskMap& add_task_map, const common::ObAddr& src_addr, ObRebalanceTaskInfo& task_info);
  int accumulate_dst_addr_server_map(
      AddrTaskMap& add_task_map, const common::ObAddr& dst_addr, ObRebalanceTaskInfo& task_info);

protected:
  int64_t generate_time_;
  ObRebalanceTaskPriority priority_;
  const char* comment_;
  int64_t schedule_time_;
  int64_t executor_time_;
  share::ObTaskId task_id_;
  common::ObAddr dst_;
};

class ObMigrateReplicaTask : public ObRebalanceTask {
public:
  ObMigrateReplicaTask()
      : ObRebalanceTask(), task_infos_(), admin_force_(false), migrate_mode_(obrpc::MigrateMode::MT_MAX)
  {}
  virtual ~ObMigrateReplicaTask()
  {}

public:
  int build(const obrpc::MigrateMode migrate_mode, const common::ObIArray<ObMigrateTaskInfo>& task_infos,
      const common::ObAddr& dst, const ObRebalanceTaskPriority priority, const char* comment, bool admin_force = false);

public:
  /* general pure interfaces override
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const override;
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::MIGRATE_REPLICA;
  }
  virtual int64_t get_sub_task_count() const override
  {
    return task_infos_.count();
  }
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const override
  {
    const ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) override
  {
    ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  /* rebalance task mgr pure interfaces override
   */
  virtual bool skip_put_to_queue() const override
  {
    return false;
  }
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const override;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(ObMigrateReplicaTask);
  }
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const override;
  virtual void notify_cancel() const override
  {}
  /* rebalance task executor pure interfaces implementation
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const override;
  virtual int execute(obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader,
      common::ObIArray<int>& dummy_rc_array) const override;
  virtual int log_execute_start() const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTask", ObRebalanceTask, K_(task_infos), K_(admin_force), "task_type", get_rebalance_task_type());
  bool is_admin_force() const
  {
    return admin_force_;
  }
  int build_migrate_replica_rpc_arg(obrpc::ObMigrateReplicaBatchArg& arg) const;
  int build_by_task_result(const obrpc::ObMigrateReplicaRes& res);
  int build_by_task_result(const obrpc::ObMigrateReplicaBatchRes& res);
  int assign(const ObMigrateReplicaTask& that);

private:
  int get_data_size(int64_t& data_size) const;

private:
  common::ObArray<ObMigrateTaskInfo> task_infos_;
  bool admin_force_;
  obrpc::MigrateMode migrate_mode_;
};

class ObTypeTransformTask : public ObRebalanceTask {
public:
  ObTypeTransformTask() : ObRebalanceTask(), task_infos_(), admin_force_(false)
  {}
  virtual ~ObTypeTransformTask()
  {}

public:
  int build(const common::ObIArray<ObTypeTransformTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment,
      bool admin_force = false);

public:
  /* general pure interfaces
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const override;
  virtual int64_t get_sub_task_count() const override
  {
    return task_infos_.count();
  }
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::TYPE_TRANSFORM;
  }
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const override
  {
    const ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) override
  {
    ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  /* rebalance task mgr pure interfaces override
   */
  virtual bool skip_put_to_queue() const override
  {
    return false;
  }
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const override;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(ObTypeTransformTask);
  }
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const override;
  virtual void notify_cancel() const override
  {}
  /* rebalance task executor pure interfaces implementation
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const override;
  virtual int execute(obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader,
      common::ObIArray<int>& dummy_rc_array) const override;
  virtual int log_execute_start() const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTask", ObRebalanceTask, K_(task_infos), K_(admin_force), "task_type", get_rebalance_task_type());
  bool is_admin_force() const
  {
    return admin_force_;
  }
  int build_type_transform_rpc_arg(obrpc::ObChangeReplicaBatchArg& arg) const;
  int build_by_task_result(const obrpc::ObChangeReplicaRes& res);
  int build_by_task_result(const obrpc::ObChangeReplicaBatchRes& res);
  int assign(const ObTypeTransformTask& that);
  int add_type_transform_task_info(const ObTypeTransformTaskInfo& task_info);

private:
  int get_data_size(int64_t& data_size) const;

private:
  common::ObArray<ObTypeTransformTaskInfo> task_infos_;
  bool admin_force_;
};

class ObAddReplicaTask : public ObRebalanceTask {
public:
  ObAddReplicaTask() : ObRebalanceTask(), task_infos_(), admin_force_(false)
  {
    priority_ = HIGH_PRI;
  }
  virtual ~ObAddReplicaTask()
  {}

public:
  int build(const common::ObIArray<ObAddTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment,
      bool admin_force = false);

public:
  /* general pure interfaces override
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const override;
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::ADD_REPLICA;
  }
  virtual int64_t get_sub_task_count() const override
  {
    return task_infos_.count();
  }
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const override
  {
    const ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) override
  {
    ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  /* rebalance task mgr pure interfaces override
   */
  virtual bool skip_put_to_queue() const override
  {
    return false;
  }
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const override;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(ObAddReplicaTask);
  }
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const override;
  virtual void notify_cancel() const override
  {}
  /* rebalance task executor pure interfaces implementation
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const override;
  virtual int execute(obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader,
      common::ObIArray<int>& dummy_rc_array) const override;
  virtual int log_execute_start() const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTask", ObRebalanceTask, K_(task_infos), K_(admin_force), "task_type", get_rebalance_task_type());
  bool is_admin_force() const
  {
    return admin_force_;
  }
  int build_add_replica_rpc_arg(obrpc::ObAddReplicaBatchArg& arg) const;
  int build_by_task_result(const obrpc::ObAddReplicaRes& res);
  int build_by_task_result(const obrpc::ObAddReplicaBatchRes& res);
  int assign(const ObAddReplicaTask& that);

private:
  int get_data_size(int64_t& data_size) const;

private:
  common::ObArray<ObAddTaskInfo> task_infos_;
  bool admin_force_;
};

class ObRebuildReplicaTask : public ObRebalanceTask {
public:
  ObRebuildReplicaTask() : ObRebalanceTask(), task_infos_()
  {}
  virtual ~ObRebuildReplicaTask()
  {}

public:
  int build(const common::ObIArray<ObRebuildTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment);

public:
  /* general pure interfaces override
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const override;
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::REBUILD_REPLICA;
  }
  virtual int64_t get_sub_task_count() const override
  {
    return task_infos_.count();
  }
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const override
  {
    const ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) override
  {
    ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  /* rebalance task mgr pure interfaces override
   */
  virtual bool skip_put_to_queue() const override
  {
    return false;
  }
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const override;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(ObRebuildReplicaTask);
  }
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const override;
  virtual void notify_cancel() const override
  {}
  /* rebalance task executor pure interfaces implementation
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const override;
  virtual int execute(obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader,
      common::ObIArray<int>& dummy_rc_array) const override;
  virtual int log_execute_start() const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTask", ObRebalanceTask, K_(task_infos), "task_type", get_rebalance_task_type());
  int build_rebuild_replica_rpc_arg(obrpc::ObRebuildReplicaBatchArg& arg) const;
  int build_by_task_result(const obrpc::ObRebuildReplicaRes& res);
  int build_by_task_result(const obrpc::ObRebuildReplicaBatchRes& res);
  int assign(const ObRebuildReplicaTask& that);

private:
  int get_data_size(int64_t& data_size) const;

private:
  common::ObArray<ObRebuildTaskInfo> task_infos_;
};

class ObModifyQuorumTask : public ObRebalanceTask {
public:
  ObModifyQuorumTask() : ObRebalanceTask(), task_infos_()
  {}
  virtual ~ObModifyQuorumTask()
  {}

public:
  int build(const common::ObIArray<ObModifyQuorumTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment);

public:
  /* general pure interfaces override
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const override;
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::MODIFY_QUORUM;
  }
  virtual int64_t get_sub_task_count() const override
  {
    return task_infos_.count();
  }
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const override
  {
    const ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) override
  {
    ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  /* rebalance task mgr pure interfaces override
   */
  virtual bool skip_put_to_queue() const override
  {
    return true;
  }
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const override;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(ObModifyQuorumTask);
  }
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const override;
  virtual void notify_cancel() const override
  {}
  /* rebalance task executor pure interfaces implementation
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const override;
  virtual int execute(obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader,
      common::ObIArray<int>& rc_array) const override;
  virtual int log_execute_start() const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTask", ObRebalanceTask, K_(task_infos), "task_type", get_rebalance_task_type());
  int build_modify_quorum_rpc_arg(obrpc::ObModifyQuorumBatchArg& arg) const;
  int add_modify_quorum_task_info(const ObModifyQuorumTaskInfo& task_info);
  int assign(const ObModifyQuorumTask& that);

private:
  common::ObArray<ObModifyQuorumTaskInfo> task_infos_;
};

class ObRestoreTask : public ObRebalanceTask {
public:
  ObRestoreTask() : ObRebalanceTask(), task_infos_()
  {}
  virtual ~ObRestoreTask()
  {}

public:
  int build(const common::ObIArray<ObRestoreTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment);

public:
  /* general pure interfaces override
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const override;
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::RESTORE_REPLICA;
  }
  virtual int64_t get_sub_task_count() const override
  {
    return task_infos_.count();
  }
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const override
  {
    const ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) override
  {
    ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  /* rebalance task mgr pure interfaces override
   */
  virtual bool skip_put_to_queue() const override
  {
    return false;
  }
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const override;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(ObRestoreTask);
  }
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const override;
  virtual void notify_cancel() const override
  {}
  /* rebalance task executor pure interfaces implementation
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const override;
  virtual int execute(obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader,
      common::ObIArray<int>& dummy_rc_array) const override;
  virtual int log_execute_start() const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTask", ObRebalanceTask, K_(task_infos), "task_type", get_rebalance_task_type());
  int build_restore_rpc_arg(obrpc::ObRestoreReplicaArg& arg) const;
  int build_by_task_result(const obrpc::ObRestoreReplicaRes& res);
  int assign(const ObRestoreTask& that);

private:
  common::ObArray<ObRestoreTaskInfo> task_infos_;
};

class ObPhysicalRestoreTask : public ObRebalanceTask {
public:
  ObPhysicalRestoreTask() : ObRebalanceTask(), task_infos_()
  {}
  virtual ~ObPhysicalRestoreTask()
  {}

public:
  int build(
      const common::ObIArray<ObPhysicalRestoreTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment);

public:
  /* general pure interfaces override
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const override;
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::PHYSICAL_RESTORE_REPLICA;
  }
  virtual int64_t get_sub_task_count() const override
  {
    return task_infos_.count();
  }
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const override
  {
    const ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) override
  {
    ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  /* rebalance task mgr pure interfaces override
   */
  virtual bool skip_put_to_queue() const override
  {
    return false;
  }
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const override;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(ObPhysicalRestoreTask);
  }
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const override;
  virtual void notify_cancel() const override
  {}
  /* rebalance task executor pure interfaces implementation
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const override;
  virtual int execute(obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader,
      common::ObIArray<int>& dummy_rc_array) const override;
  virtual int log_execute_start() const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTask", ObRebalanceTask, K_(task_infos), "task_type", get_rebalance_task_type());
  int build_restore_rpc_arg(obrpc::ObPhyRestoreReplicaArg& arg) const;
  int build_by_task_result(const obrpc::ObPhyRestoreReplicaRes& res);
  int assign(const ObPhysicalRestoreTask& that);

private:
  common::ObArray<ObPhysicalRestoreTaskInfo> task_infos_;
};

class ObCopySSTableTask : public ObRebalanceTask {
public:
  ObCopySSTableTask() : ObRebalanceTask(), task_infos_(), copy_sstable_type_(common::OB_COPY_SSTABLE_TYPE_INVALID)
  {
    priority_ = HIGH_PRI;
  }
  virtual ~ObCopySSTableTask()
  {}

public:
  int build(const common::ObIArray<ObCopySSTableTaskInfo>& task_infos,
      const common::ObCopySSTableType copy_sstable_type, const common::ObAddr& dst, const char* comment);

public:
  /* general pure interfaces override
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const override;
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::COPY_SSTABLE;
  }
  virtual int64_t get_sub_task_count() const override
  {
    return task_infos_.count();
  }
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const override
  {
    const ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) override
  {
    ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  /* rebalance task mgr pure interfaces override
   */
  virtual bool skip_put_to_queue() const override
  {
    return false;
  }
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const override;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(ObCopySSTableTask);
  }
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const override;
  virtual void notify_cancel() const override
  {}
  /* rebalance task executor pure interfaces implementation
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const override;
  virtual int execute(obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader,
      common::ObIArray<int>& dummy_rc_array) const override;
  virtual int log_execute_start() const override;

public:
  virtual INHERIT_TO_STRING_KV("ObRebalanceTask", ObRebalanceTask, K_(task_infos), K_(copy_sstable_type), "task_type",
      get_rebalance_task_type());
  int build_copy_sstable_rpc_arg(obrpc::ObCopySSTableBatchArg& arg) const;
  int build_by_task_result(const obrpc::ObCopySSTableBatchRes& res);
  common::ObCopySSTableType get_copy_sstable_type() const
  {
    return copy_sstable_type_;
  }
  int assign(const ObCopySSTableTask& that);

private:
  int get_data_size(int64_t& data_size) const;

private:
  common::ObArray<ObCopySSTableTaskInfo> task_infos_;
  common::ObCopySSTableType copy_sstable_type_;
};

class ObRemoveMemberTask : public ObRebalanceTask {
public:
  ObRemoveMemberTask() : ObRebalanceTask(), task_infos_(), admin_force_(false)
  {}
  virtual ~ObRemoveMemberTask()
  {}

public:
  int build(const common::ObIArray<ObRemoveMemberTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment,
      const bool admin_force = false);

public:
  /* general pure interfaces override
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const override;
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::MEMBER_CHANGE;
  }
  virtual int64_t get_sub_task_count() const override
  {
    return task_infos_.count();
  }
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const override
  {
    const ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) override
  {
    ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  /* rebalance task mgr pure interfaces override
   */
  virtual bool skip_put_to_queue() const override
  {
    return true;
  }
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const override;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(ObRemoveMemberTask);
  }
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const override;
  virtual void notify_cancel() const override
  {}
  /* rebalance task executor pure interfaces implementation
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const override;
  virtual int execute(
      obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& leader, common::ObIArray<int>& rc_array) const override;
  virtual int log_execute_start() const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTask", ObRebalanceTask, K_(task_infos), K_(admin_force), "task_type", get_rebalance_task_type());
  bool is_admin_force() const
  {
    return admin_force_;
  }
  int build_remove_member_rpc_arg(obrpc::ObMemberChangeBatchArg& arg) const;
  int add_remove_member_task_info(const ObRemoveMemberTaskInfo& task_info);
  int assign(const ObRemoveMemberTask& that);

private:
  common::ObArray<ObRemoveMemberTaskInfo> task_infos_;
  bool admin_force_;
};

class ObRemoveNonPaxosTask : public ObRebalanceTask {
public:
  ObRemoveNonPaxosTask() : ObRebalanceTask(), task_infos_(), admin_force_(false)
  {}
  virtual ~ObRemoveNonPaxosTask()
  {}

public:
  int build(const common::ObIArray<ObRemoveNonPaxosTaskInfo>& task_infos, const common::ObAddr& dst,
      const char* comment, const bool admin_force = false);

public:
  /* general pure interfaces override
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const override;
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::REMOVE_NON_PAXOS_REPLICA;
  }
  virtual int64_t get_sub_task_count() const override
  {
    return task_infos_.count();
  }
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const override
  {
    const ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) override
  {
    ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  /* rebalance task mgr pure interfaces override
   */
  virtual bool skip_put_to_queue() const override
  {
    return true;
  }
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const override;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(ObRemoveNonPaxosTask);
  }
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const override;
  virtual void notify_cancel() const override
  {}
  /* rebalance task executor pure interfaces implementation
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const override;
  virtual int execute(obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader,
      common::ObIArray<int>& rc_array) const override;
  virtual int log_execute_start() const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTask", ObRebalanceTask, K_(task_infos), K_(admin_force), "task_type", get_rebalance_task_type());
  bool is_admin_force() const
  {
    return admin_force_;
  }
  int build_remove_non_paxos_rpc_arg(obrpc::ObRemoveNonPaxosReplicaBatchArg& arg) const;
  int add_remove_non_paxos_replica_task_info(const ObRemoveNonPaxosTaskInfo& task_info);
  int assign(const ObRemoveNonPaxosTask& that);

private:
  common::ObArray<ObRemoveNonPaxosTaskInfo> task_infos_;
  bool admin_force_;
};

class ObRebalanceSqlBKGTask : public ObRebalanceTask {
public:
  ObRebalanceSqlBKGTask() : ObRebalanceTask(), task_infos_()
  {}
  virtual ~ObRebalanceSqlBKGTask()
  {}

public:
  int build(const ObSqlBKGDDistTaskInfo& task_info, const char* comment);

public:
  /* general pure interfaces override
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const override;
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::SQL_BACKGROUND_DIST_TASK;
  }
  virtual int64_t get_sub_task_count() const override
  {
    return task_infos_.count();
  }
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const override
  {
    const ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) override
  {
    ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  /* rebalance task mgr pure interfaces override
   */
  virtual bool skip_put_to_queue() const override
  {
    return false;
  }
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const override;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(ObRebalanceSqlBKGTask);
  }
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const override;
  virtual void notify_cancel() const override;
  /* rebalance task executor pure interfaces implementation
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const override;
  virtual int execute(obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader,
      common::ObIArray<int>& dummy_rc_array) const override;
  virtual int log_execute_start() const override;

  int check_task_exist(bool& exist);

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTask", ObRebalanceTask, K_(task_infos), "task_type", get_rebalance_task_type());
  int build_rebalance_sql_bkg_rpc_arg(sql::ObBKGDDistExecuteArg& arg) const;
  int build_by_task_result(const sql::ObBKGDTaskCompleteArg& arg);
  int assign(const ObRebalanceSqlBKGTask& that);

private:
  common::ObArray<ObSqlBKGDDistTaskInfo> task_infos_;
};

class ObBackupTask : public ObRebalanceTask {
public:
  ObBackupTask() : ObRebalanceTask(), task_infos_()
  {
    priority_ = HIGH_PRI;
  }
  virtual ~ObBackupTask()
  {}

public:
  int build(const common::ObIArray<ObBackupTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment);

public:
  /* general pure interfaces override
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const override;
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::BACKUP_REPLICA;
  }
  virtual int64_t get_sub_task_count() const override
  {
    return task_infos_.count();
  }
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const override
  {
    const ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) override
  {
    ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  /* rebalance task mgr pure interfaces override
   */
  virtual bool skip_put_to_queue() const override
  {
    return false;
  }
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const override;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(ObBackupTask);
  }
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const override;
  virtual void notify_cancel() const override
  {}
  /* rebalance task executor pure interfaces implementation
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const override;
  virtual int execute(obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader,
      common::ObIArray<int>& dummy_rc_array) const override;
  virtual int log_execute_start() const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTask", ObRebalanceTask, K_(task_infos), "task_type", get_rebalance_task_type());
  int build_backup_rpc_arg(obrpc::ObBackupBatchArg& arg) const;
  int build_by_task_result(const obrpc::ObBackupBatchRes& res);
  int assign(const ObBackupTask& that);
  share::ObTaskId& get_task_id()
  {
    return task_id_;
  }

private:
  int get_data_size(int64_t& data_size) const;

private:
  common::ObArray<ObBackupTaskInfo> task_infos_;
};

class ObFastRecoveryTask : public ObRebalanceTask {
public:
  ObFastRecoveryTask() : ObRebalanceTask(), task_infos_(), dest_unit_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObFastRecoveryTask()
  {}

public:
  int build(const ObFastRecoveryTaskInfo& task_info, const uint64_t dest_unit_id, const char* comment);

public:
  /* general pure interfaces override
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const override;
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::FAST_RECOVERY_TASK;
  }
  virtual int64_t get_sub_task_count() const override
  {
    return task_infos_.count();
  }
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const override
  {
    const ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) override
  {
    ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  /* rebalance task mgr pure interfaces override
   */
  virtual bool skip_put_to_queue() const override
  {
    return false;
  }
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const override;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(ObFastRecoveryTask);
  }
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const override;
  virtual void notify_cancel() const override
  {}
  /* rebalance task executor pure interfaces implementation
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const override;
  virtual int execute(obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader,
      common::ObIArray<int>& dummy_rc_array) const override;
  virtual int log_execute_start() const override;

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTask", ObRebalanceTask, K_(task_infos), K_(dest_unit_id), "task_type", get_rebalance_task_type());
  int assign(const ObFastRecoveryTask& that);

private:
  common::ObArray<ObFastRecoveryTaskInfo> task_infos_;
  uint64_t dest_unit_id_;
};

class ObValidateTask : public ObRebalanceTask {
public:
  ObValidateTask() : ObRebalanceTask(), task_infos_()
  {}
  virtual ~ObValidateTask()
  {}

public:
  int build(const common::ObIArray<ObValidateTaskInfo>& task_infos, const common::ObAddr& dst, const char* comment);

public:
  /* general pure interfaces override
   */
  virtual int get_timeout(const int64_t network_bandwidth, const int64_t server_concurrency_limit,
      const int64_t min_task_timeout_us, int64_t& timeout_us) const override;
  virtual ObRebalanceTaskType get_rebalance_task_type() const override
  {
    return ObRebalanceTaskType::VALIDATE_BACKUP;
  }
  virtual int64_t get_sub_task_count() const override
  {
    return task_infos_.count();
  }
  virtual const ObRebalanceTaskInfo* get_sub_task(const int64_t idx) const override
  {
    const ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  virtual ObRebalanceTaskInfo* get_sub_task(const int64_t idx) override
  {
    ObRebalanceTaskInfo* ptr = nullptr;
    if (idx >= 0 && idx < task_infos_.count()) {
      ptr = &task_infos_.at(idx);
    }
    return ptr;
  }
  /* rebalance task mgr pure interfaces override
   */
  virtual bool skip_put_to_queue() const override
  {
    return false;
  }
  virtual int check_can_execute_on_dest(ObRebalanceTaskMgr& task_mgr, bool& can) const override;
  virtual int log_execute_result(const common::ObIArray<int>& rc_array) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(ObValidateTask);
  }
  virtual int clone(void* input_ptr, ObRebalanceTask*& out_task) const override;
  /* rebalance task executor pure interfaces implementation
   */
  virtual int check_before_execute(share::ObPartitionTableOperator& pt_operator, common::ObAddr& leader) const override;
  virtual int execute(obrpc::ObSrvRpcProxy& rpc_proxy, const common::ObAddr& dummy_leader,
      common::ObIArray<int>& dummy_rc_array) const override;
  virtual int log_execute_start() const override;
  virtual void notify_cancel() const override
  {}

public:
  virtual INHERIT_TO_STRING_KV(
      "ObRebalanceTask", ObRebalanceTask, K_(task_infos), "task_type", get_rebalance_task_type());
  int build_validate_rpc_arg(obrpc::ObValidateBatchArg& arg) const;
  int build_by_task_result(const obrpc::ObValidateRes& res);
  int build_by_task_result(const obrpc::ObValidateBatchRes& res);
  int assign(const ObValidateTask& that);
  const share::ObTaskId& get_task_id() const
  {
    return task_id_;
  }

private:
  int get_data_size(int64_t& data_size) const;

private:
  common::ObArray<ObValidateTaskInfo> task_infos_;
};

}  // end namespace rootserver
}  // end namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_REBALANCE_TASK_H_
