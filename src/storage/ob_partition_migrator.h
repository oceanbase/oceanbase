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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_MIGRATOR_H_
#define OCEANBASE_STORAGE_OB_PARTITION_MIGRATOR_H_

#include "lib/thread/ob_work_queue.h"
#include "lib/thread/ob_dynamic_thread_pool.h"
#include "share/ob_common_rpc_proxy.h"  // ObCommonRpcProxy
#include "share/ob_srv_rpc_proxy.h"     // ObPartitionServiceRpcProxy
#include "share/scheduler/ob_sys_task_stat.h"
#include "storage/ob_i_partition_storage.h"
#include "observer/ob_rpc_processor_simple.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_i_partition_base_data_reader.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "ob_partition_service_rpc.h"
#include "blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_partition_base_data_backup.h"
#include "storage/ob_partition_base_data_physical_restore.h"
#include "storage/ob_partition_base_data_validate.h"
#include "storage/backup/ob_partition_base_data_physical_restore_v2.h"

namespace oceanbase {
namespace storage {
class ObPartitionLocalReader;
class ObPartitionService;
class ObMigrateFinishTask;
class ObMigrateLogicSSTableCtx;
class ObMigratePhysicalSSTableCtx;
class ObPartitionMigrateCtx;
class ObIMigrateMacroBlockWriter;
class ObBackupPhysicalPGCtx;
class ObPartGroupTask;

class ObMacroBlockReuseMgr final {
public:
  ObMacroBlockReuseMgr();
  ~ObMacroBlockReuseMgr();
  int build_reuse_macro_map(ObMigrateCtx& ctx, const ObITable::TableKey& table_key,
      const common::ObIArray<blocksstable::ObSSTablePair>& macro_block_list);
  int get_reuse_macro_meta(
      const blocksstable::ObMajorMacroBlockKey& block_key, blocksstable::ObMacroBlockInfoPair& info);
  void reset();

private:
  class RemoveFunctor {
  public:
    RemoveFunctor() = default;
    ~RemoveFunctor() = default;
    bool operator()(const blocksstable::ObMajorMacroBlockKey& block_key, blocksstable::ObMacroBlockInfoPair& info);
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockReuseMgr);
  typedef common::ObLinearHashMap<blocksstable::ObMajorMacroBlockKey, blocksstable::ObMacroBlockInfoPair> MacroMetaMap;
  MacroMetaMap full_map_;
  MacroMetaMap reuse_map_;
  common::ObArenaAllocator allocator_;
};

struct ObPartitionGroupInfoResult {
  ObPartitionGroupInfoResult();
  void reset();
  int assign(const ObPartitionGroupInfoResult& result);
  bool is_valid() const
  {
    return choose_src_info_.is_valid();
  }
  TO_STRING_KV(K_(result), K_(choose_src_info));

  obrpc::ObFetchPGInfoResult result_;
  ObMigrateSrcInfo choose_src_info_;
};

class ObBaseMigrateDag;
struct ObMigrateCtx {
public:
  static const int64_t MIN_CHECK_REPLAY_TS = 100 * 1000;  // 100ms
  static const int64_t MAX_CONTINUES_FAIL_COUNT = 10;
  // Only in the SCHEDULE_AND_COPY_BASE_DATA state, there will be multiple tasks holding migrate ctx, other times are
  // single-threaded operations
  enum MigrateAction {
    INIT,
    PREPARE,
    COPY_MINOR,
    COPY_MAJOR,
    END,  // Only the state of END can deconstruct ObPartitionMigrateContext
    UNKNOWN,
  };

  static const char* trans_action_to_str(const MigrateAction& action);

  ObMigrateCtx();
  virtual ~ObMigrateCtx();
  int64_t get_doing_task_count();
  bool is_valid() const;
  int notice_start_part_task();
  int notice_finish_part_task();
  ObIPartitionGroup* get_partition()
  {
    return partition_guard_.get_partition_group();
  }
  void set_result_code(const int32_t result);  // TODO(): illustrate rebuild error code later
  int update_partition_migration_status() const;
  bool is_only_copy_sstable() const;
  int fill_comment(char* buf, const int64_t buf_len) const;
  int add_major_block_id(const blocksstable::MacroBlockId& marco_block_id);
  void free_old_macro_block();
  void calc_need_retry();
  void rebuild_migrate_ctx();
  int generate_and_schedule_migrate_dag();
  int generate_and_schedule_backup_dag(const share::ObBackupDataType& backup_type);
  bool is_need_retry(const int result) const;
  int change_replica_with_data(bool& is_replica_with_data);
  int get_restore_replay_log_id(uint64_t& log_id) const;
  int build_current_macro_map();
  int add_reuse_major_block(
      const blocksstable::MacroBlockId& macro_block_id, const blocksstable::ObMacroBlockMeta& macro_meta);
  int get_restore_clog_info(uint64_t& log_id, int64_t& acc_checksum) const;
  bool is_leader_restore_archive_data() const;
  OB_INLINE bool is_migrate_compat_version() const;
  int set_is_restore_for_add_replica(const int16_t src_is_restore);
  bool is_copy_index() const;
  int alloc_migrate_dag(ObBaseMigrateDag*& base_migrate_dag);
  TO_STRING_KV(K_(replica_op_arg), KP_(macro_indexs), "action", trans_action_to_str(action_), K_(result),
      K_(replica_state), K_(doing_task_cnt), K_(total_task_cnt), K_(need_rebuild), K(create_ts_), K(task_id_),
      K(copy_size_), K(continue_fail_count_), K(rebuild_count_), K(finish_ts_), K(clog_parent_), K(migrate_src_info_),
      K(wait_replay_start_ts_), K(wait_minor_merge_start_ts_), K(last_confirmed_log_id_), K(last_confirmed_log_ts_),
      K(group_task_id_), KP(group_task_), K_(during_migrating), K_(need_online_for_rebuild), K_(trace_id_array),
      K_(need_offline), K_(is_restore), K_(use_slave_safe_read_ts), K_(is_copy_cover_minor), K(mig_src_file_id_),
      K(mig_dest_file_id_), K(src_suspend_ts_), K(is_takeover_finished_), K(is_member_change_finished_),
      K_(local_last_replay_log_ts), K_(trans_table_handle), K_(create_new_pg), K_(fetch_pg_info_compat_version));

  common::SpinRWLock lock_;
  ObReplicaOpArg replica_op_arg_;
  ObIPhyRestoreMacroIndexStore* macro_indexs_;
  ObBackupMacroIndexStore macro_index_store_;
  MigrateAction action_;    // Ctx will be destructed only when END, and will be changed only when doing_task_cnt_ is 1.
  int32_t result_;          // Only log fatal errors
  int64_t doing_task_cnt_;  // The action can only be changed when the count is 1.
  int64_t total_task_cnt_;
  bool need_rebuild_;
  ObIPartitionGroupGuard partition_guard_;
  ObPartitionReplicaState replica_state_;
  int64_t local_publish_version_;  // The largest snapshot_version of the local sstable
  int64_t local_last_replay_log_id_;
  int64_t last_check_replay_ts_;
  int64_t create_ts_;
  share::ObTaskId task_id_;
  int64_t copy_size_;
  int64_t continue_fail_count_;
  int64_t rebuild_count_;

  ObAddr clog_parent_;
  ObMigrateSrcInfo migrate_src_info_;
  int64_t finish_ts_;
  int64_t wait_replay_start_ts_;
  int64_t wait_minor_merge_start_ts_;
  uint64_t last_confirmed_log_id_;
  int64_t last_confirmed_log_ts_;
  ObPartGroupTask* group_task_;
  ObRestoreInfo restore_info_;
  share::ObTaskId group_task_id_;
  bool during_migrating_;
  bool need_online_for_rebuild_;
  common::ObArray<common::ObCurTraceId::TraceId> trace_id_array_;
  common::ObArray<blocksstable::MacroBlockId> major_block_id_array_[2];
  common::ObArray<blocksstable::MacroBlockId>* major_block_id_array_ptr_;
  int64_t curr_major_block_array_index_;
  bool can_rebuild_;
  ObPartitionGroupMeta pg_meta_;
  ObArray<ObPartitionMigrateCtx> part_ctx_array_;
  bool need_offline_;
  int16_t is_restore_;
  bool use_slave_safe_read_ts_;
  bool need_report_checksum_;
  // for partition_migration_status
  ObPartitionMigrationDataStatics data_statics_;
  bool is_copy_cover_minor_;  // Indicates whether to overwrite the local minor sstable as much as possible

  // for fast migrate
  int64_t mig_src_file_id_;
  int64_t mig_dest_file_id_;
  int64_t src_suspend_ts_;
  bool is_takeover_finished_;
  bool is_member_change_finished_;
  ObMacroBlockReuseMgr reuse_block_mgr_;
  int64_t local_last_replay_log_ts_;
  ObTableHandle trans_table_handle_;
  int64_t old_trans_table_seq_;
  bool create_new_pg_;
  ObIPartitionGroupMetaRestoreReader* restore_meta_reader_;
  ObPartitionGroupMetaBackupReader* backup_meta_reader_;
  int64_t fetch_pg_info_compat_version_;
  ObBackupPhysicalPGCtx physical_backup_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMigrateCtx);
};

class ObMigrateCtxGuard {
public:
  explicit ObMigrateCtxGuard(const bool is_write_lock, ObMigrateCtx& ctx);
  virtual ~ObMigrateCtxGuard();

private:
  ObMigrateCtx& ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMigrateCtxGuard);
};

struct ObPartitionMigrateCtx final {
  ObPartitionMigrateCtx();
  bool is_valid() const;
  void reset();
  int add_sstable(ObSSTable& handle);
  int assign(const ObPartitionMigrateCtx& part_migrate_ctx);

  ObMigrateCtx* ctx_;
  ObMigratePartitionInfo copy_info_;
  ObTablesHandle handle_;
  bool is_partition_exist_;
  common::SpinRWLock lock_;
  // When the log_id is not continuous, we directly migrate all the dumps from the source end to replace the local one
  // to prevent discontinuity after the migration
  bool need_reuse_local_minor_;

  TO_STRING_KV(KP_(ctx), K_(copy_info), K_(is_partition_exist), K_(handle), K_(need_reuse_local_minor));

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionMigrateCtx);
};

struct ObIPartMigrationTask {
  enum TaskStatus { INIT = 0, DOING = 1, FINISH = 2, MAX };
  ObIPartMigrationTask() : arg_(), status_(MAX), result_(OB_SUCCESS)
  {}

  TO_STRING_KV(K_(status), K_(result), K_(arg));
  ObReplicaOpArg arg_;
  TaskStatus status_;
  int32_t result_;
};

struct ObPartMigrationTask : ObIPartMigrationTask {
  ObPartMigrationTask() : ObIPartMigrationTask(), ctx_(), need_reset_migrate_status_(false), during_migrating_(false)
  {}
  int assign(const ObPartMigrationTask& task);
  TO_STRING_KV(K_(status), K_(result), K_(arg), K_(need_reset_migrate_status), K_(during_migrating), K_(ctx));
  ObMigrateCtx ctx_;
  bool need_reset_migrate_status_;
  bool during_migrating_;
  DISALLOW_COPY_AND_ASSIGN(ObPartMigrationTask);
};

struct ObReportPartMigrationTask : ObIPartMigrationTask {
  ObReportPartMigrationTask()
      : ObIPartMigrationTask(), need_report_checksum_(false), partitions_(), ctx_(nullptr), data_statics_()
  {}

  int assign(const ObReportPartMigrationTask& report_task)
  {
    int ret = common::OB_SUCCESS;
    arg_ = report_task.arg_;
    status_ = report_task.status_;
    result_ = report_task.result_;
    need_report_checksum_ = report_task.need_report_checksum_;
    data_statics_ = report_task.data_statics_;
    if (OB_FAIL(partitions_.assign(report_task.partitions_))) {
      STORAGE_LOG(WARN, "failed to assign task partitions", K(ret), K(report_task));
    }
    ctx_ = report_task.ctx_;
    return ret;
  }

  TO_STRING_KV(K_(need_report_checksum), K_(partitions), KP_(ctx), K_(data_statics));

  bool need_report_checksum_;
  ObPartitionArray partitions_;
  ObMigrateCtx* ctx_;
  ObPartitionMigrationDataStatics data_statics_;
  DISALLOW_COPY_AND_ASSIGN(ObReportPartMigrationTask);
};

class ObPartitionMigrator {
public:
  const static int64_t OB_MIGRATE_SCHEDULE_SLEEP_INTERVAL_S = 100 * 1000;  // 100ms

  ObPartitionMigrator();
  virtual ~ObPartitionMigrator();

  static ObPartitionMigrator& get_instance();
  common::ObDynamicThreadPool& get_task_pool()
  {
    return restore_macro_block_task_pool_;
  }

  int init(obrpc::ObPartitionServiceRpcProxy& srv_rcp_proxy, ObPartitionServiceRpc& pts_rpc,
      ObIPartitionComponentFactory* cp_fty, ObPartitionService* partition_service,
      common::ObInOutBandwidthThrottle& bandwidth_throttle, share::ObIPartitionLocationCache* location_cache,
      share::schema::ObMultiVersionSchemaService* schema_service);

  int notify_migrate_result_callback(const ObReplicaOpArg& migrate_arg, int result);
  void destroy();
  void stop();
  void wait();
  bool is_inited() const
  {
    return is_inited_;
  }
  bool is_stop() const
  {
    return is_stop_;
  }

  storage::ObPartitionService* get_partition_service()
  {
    return partition_service_;
  }
  ObPartitionServiceRpc* get_pts_rpc()
  {
    return pts_rpc_;
  }
  obrpc::ObPartitionServiceRpcProxy* get_svr_rpc_proxy()
  {
    return svr_rpc_proxy_;
  }
  share::ObIPartitionLocationCache* get_location_cache()
  {
    return location_cache_;
  }
  common::ObInOutBandwidthThrottle* get_bandwidth_throttle()
  {
    return bandwidth_throttle_;
  }
  share::schema::ObMultiVersionSchemaService* get_schema_service()
  {
    return schema_service_;
  }
  ObIPartitionComponentFactory* get_cp_fty()
  {
    return cp_fty_;
  }
  static int fill_comment(const ObMigrateCtx& ctx, char* buf, const int64_t buf_len);

private:
  bool is_inited_;
  bool is_stop_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  storage::ObPartitionService* partition_service_;
  obrpc::ObPartitionServiceRpcProxy* svr_rpc_proxy_;
  ObPartitionServiceRpc* pts_rpc_;
  ObIPartitionComponentFactory* cp_fty_;
  share::ObIPartitionLocationCache* location_cache_;
  common::SpinRWLock lock_;
  common::ObDynamicThreadPool restore_macro_block_task_pool_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionMigrator);
};

class ObPartGroupTask {
public:
  enum Type {
    PART_GROUP_MIGATION_TASK,
    PART_GROUP_BACKUP_TASK,
  };

public:
  ObPartGroupTask();
  virtual ~ObPartGroupTask();

  bool is_finish() const;
  int get_pkey_list(ObIArray<ObPartitionKey>& pkey_list) const;
  int check_dup_task(const common::ObIArray<ObPartitionKey>& pkey_list) const;
  int check_self_dup_task() const;

  int set_part_task_start(const ObPartitionKey& pkey, const int64_t backup_set_id);
  int set_part_task_finish(const ObPartitionKey& pkey, const int32_t result, const share::ObTaskId& part_task_id,
      const bool during_migrating, const int64_t backup_set_id);
  bool has_error() const;
  const share::ObTaskId& get_task_id() const
  {
    return task_id_;
  }
  const common::ObIArray<ObPartMigrationTask>& get_task_list() const
  {
    return task_list_;
  }
  ObReplicaOpType get_type() const
  {
    return type_;
  }
  int64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int fill_report_list(bool& is_batch_mode, ObIArray<ObReportPartMigrationTask>& report_list);
  void wakeup();

  virtual int do_task() = 0;
  virtual Type get_task_type() const = 0;

  TO_STRING_KV(K_(task_id), K_(is_inited), K_(is_finished), K_(is_batch_mode), KP_(partition_service),
      K_(first_error_code), K_(type), "sub_task_count", task_list_.count(), K_(task_list));

protected:
  int build_migrate_ctx(const ObReplicaOpArg& arg, ObMigrateCtx& migrate_ctx);
  void dump_task_status();
  int check_is_task_cancel();
  int check_partition_checksum(const common::ObPartitionKey& pkey);
  int check_is_tenant_exist(bool& is_exist);
  int check_is_partition_exist(const common::ObPartitionKey& pkey, bool& is_exist);
  int check_can_as_data_source(
      const ObReplicaOpType& op, const ObReplicaType& src_type, const ObReplicaType& dst_type, bool& as_data_source);
  static int build_failed_report_list(const int32_t first_fail_code, ObIArray<ObReportPartMigrationTask>& report_list);
  int set_report_list_result(
      ObIArray<ObReportPartMigrationTask>& report_list, const ObPartitionKey& pkey, const int32_t result);

protected:
  bool is_inited_;
  bool is_batch_mode_;  // Used to be compatible with the old version of rs, when it is false, all rpc related to rs
                        // will use the old interface
  common::SpinRWLock lock_;
  ObArray<ObPartMigrationTask> task_list_;  // pkey is only used by schedule thread, otherwise has concurrent issue
  storage::ObPartitionService* partition_service_;
  int32_t first_error_code_;  // Have concurrency issues
  ObReplicaOpType type_;      // Only the scheduling thread will change and use
  share::ObTaskId task_id_;
  bool is_finished_;
  int64_t retry_job_start_ts_;
  int64_t tenant_id_;
  ObThreadCond cond_;
  bool need_idle_;
  ObArray<ObReportPartMigrationTask> report_list_;
  ObChangeMemberOption change_member_option_;
  DISALLOW_COPY_AND_ASSIGN(ObPartGroupTask);
};

class ObPartGroupMigrationTask : public ObPartGroupTask {
public:
  // Only need to retry if the migration exceeds 2 hours
  static const int64_t LARGE_MIGRATION_NEED_RETRY_COST_TIME = 2 * 3600 * 1000 * 1000LL;
  static const int64_t PART_GROUP_TASK_IDLE_TIME_MS = 10 * 1000LL;  // 10s

  ObPartGroupMigrationTask();
  virtual ~ObPartGroupMigrationTask();
  int init(const common::ObIArray<ObReplicaOpArg>& task_list, const bool is_batch_mode,
      storage::ObPartitionService* partition_service, const share::ObTaskId& task_id);
  int check_before_do_task();  // Called only before starting execution, do not consider concurrency
  int set_migrate_in(ObPGStorage& pg_storage);
  int set_create_new_pg(const ObPartitionKey& pg_key);
  const ObPhyRestoreMetaIndexStore& get_meta_indexs()
  {
    return meta_indexs_;
  }
  ObBackupMetaIndexStore& get_meta_index_store()
  {
    return meta_index_store_;
  }
  int64_t get_schedule_ts() const
  {
    return schedule_ts_;
  }

  virtual int do_task();
  virtual Type get_task_type() const
  {
    return PART_GROUP_MIGATION_TASK;
  }

  TO_STRING_KV(K_(task_id), K_(is_inited), K_(is_finished), K_(is_batch_mode), K_(schedule_ts),
      K_(start_change_member_ts), KP_(partition_service), K_(first_error_code), K_(type), "sub_task_count",
      task_list_.count(), K_(task_list), K_(restore_version));

private:
  int check_partition_validation();  // Called only before starting execution, do not consider concurrency
  int check_disk_space();            // Called only before starting execution, do not consider concurrency
  static int get_partition_required_size(const common::ObPartitionKey& pkey, int64_t& required_size);
  int try_schedule_new_partition_migration();
  int try_finish_group_migration();
  int finish_group_migration(common::ObIArray<ObReportPartMigrationTask>& report_list);
  int report_restore_fatal_error_();
  int try_change_member_list(ObIArray<ObReportPartMigrationTask>& report_list, bool& is_all_finish);
  int try_not_batch_change_member_list(ObIArray<ObReportPartMigrationTask>& report_list, bool& is_all_finish);
  bool need_retry_change_member_list(const int32_t result_code);
  int check_need_batch_change_member_list(
      const common::ObAddr& leader_addr, common::ObIArray<ObReportPartMigrationTask>& report_list, bool& need_batch);
  int try_batch_change_member_list(
      common::ObIArray<ObReportPartMigrationTask>& report_list, const ObAddr& leader_addr, bool& is_all_finish);
  int try_batch_remove_member(common::ObIArray<ObReportPartMigrationTask>& report_list, const ObAddr& leader_addr,
      hash::ObHashSet<ObPartitionKey>& removed_pkeys);
  int fast_migrate_add_member_list_one_by_one(
      common::ObIArray<ObReportPartMigrationTask>& report_list, bool& is_all_finish);
  int try_batch_add_member(common::ObIArray<ObReportPartMigrationTask>& report_list,
      const hash::ObHashSet<ObPartitionKey>& removed_pkeys, const ObAddr& leader_addr,
      hash::ObHashSet<ObPartitionKey>& added_keys, bool& is_all_finish);
  int try_batch_add_member(
      ObIArray<ObReportPartMigrationTask>& report_list, const ObAddr& leader_addr, bool& is_all_finish);
  int build_add_member_ctx(const common::ObIArray<ObReportPartMigrationTask>& report_list,
      const hash::ObHashSet<ObPartitionKey>& removed_pkeys, obrpc::ObChangeMemberArgs& ctx);
  int remove_src_replica(const common::ObIArray<ObReportPartMigrationTask>& report_list);
  int batch_remove_src_replica(const common::ObIArray<ObReportPartMigrationTask>& report_list);
  int wait_batch_member_change_done(const ObAddr& leader_addr, obrpc::ObChangeMemberCtxs& change_member_info);
  int report_meta_table(const common::ObIArray<ObReportPartMigrationTask>& report_list,
      const hash::ObHashSet<ObPartitionKey>& member_removed_pkeys);
  int check_report_done(const common::ObIArray<ObReportPartMigrationTask>& report_list, common::ObIArray<bool>& result);
  int get_tables_with_major_sstable(const ObPartitionKey& pkey, ObIArray<uint64_t>& table_ids);
  int check_member_major_sstable_enough(common::ObIArray<ObReportPartMigrationTask>& report_list);
  int check_member_pg_major_sstable_enough(ObReportPartMigrationTask& task);
  int schedule_migrate_dag(ObMigrateCtx& migrate_ctx);
  int remove_member_list_if_need();
  int check_need_batch_remove_member(ObAddr& leader_addr, bool& need_remove, bool& need_batch);
  int try_single_remove_member();
  int inner_schedule_partition(ObPartMigrationTask*& task, bool& need_schedule);
  int try_schedule_partition_validate();
  int try_schedule_partition_migration();
  int get_migrate_task(const ObPartitionKey& pg_key, ObPartMigrationTask*& task);
  int set_migrate_task_flags_(const ObMigrateStatus& status, const bool is_restore, ObPartMigrationTask& task);
  int check_before_backup();

private:
  static const int64_t RETRY_JOB_MAX_WAIT_TIMEOUT = 600 * 1000 * 1000;  // 10m
  int64_t schedule_ts_;                     // Only the scheduling thread will change and use
  int64_t start_change_member_ts_;          // Only the scheduling thread will change and use
  ObPhyRestoreMetaIndexStore meta_indexs_;  // backup file meta index
  ObBackupMetaIndexStore meta_index_store_;
  int64_t restore_version_;
  bool skip_change_member_list_;
  DISALLOW_COPY_AND_ASSIGN(ObPartGroupMigrationTask);
};

class ObPartGroupMigrator {
public:
  ObPartGroupMigrator();
  virtual ~ObPartGroupMigrator();
  void destroy();
  void stop();
  void wait();
  bool is_stop() const;

  static ObPartGroupMigrator& get_instance();
  int init(storage::ObPartitionService* partition_service, ObIPartitionComponentFactory* cp_fty);
  int schedule(const ObReplicaOpArg& arg, const share::ObTaskId& task_id);
  int schedule(const common::ObIArray<ObReplicaOpArg>& arg_list, const share::ObTaskId& tsk_id);
  // mark means partition couldn't be migrated. So it won't be scheduled from arg and now only used by remove
  // replication
  int mark(const ObReplicaOpArg& arg, const share::ObTaskId& task_id, ObPartGroupTask*& group_task);
  int has_task(const ObPartitionKey& pkey, bool& has_task);
  int task_exist(const share::ObTaskId& arg, bool& res);
  int remove_finish_task(ObPartGroupTask* group_task);
  void wakeup();

private:
  int inner_schedule(const common::ObIArray<ObReplicaOpArg>& arg_list, const bool is_batch_mode,
      const share::ObTaskId& task_id, const bool is_normal_migrate, ObPartGroupTask*& group_task);
  int check_copy_limit_(const ObIArray<ObReplicaOpArg>& arg_list);
  int get_not_finish_task_count(
      int64_t& rebuild_count, int64_t& backup_count, int64_t& validate_count, int64_t& is_normal_migrate);
  static int check_arg_list(const common::ObIArray<ObReplicaOpArg>& arg_list);
  int check_dup_task(const ObPartGroupTask& task);
  int check_dup_task(const ObIArray<ObPartitionKey>& pkey_list);
  int schedule_group_migrate_dag(ObPartGroupTask*& group_task);
  int get_group_task(const ObIArray<ObReplicaOpArg>& arg_list, const bool is_batch_mode,
      const share::ObTaskId& in_task_id, ObPartGroupTask*& group_task);

private:
  bool is_inited_;
  common::SpinRWLock update_task_list_lock_;  // protect new_task_list_ and count of task_list_
  ObArray<ObPartGroupTask*> task_list_;
  storage::ObPartitionService* partition_service_;
  ObIPartitionComponentFactory* cp_fty_;
  bool is_stop_;
  DISALLOW_COPY_AND_ASSIGN(ObPartGroupMigrator);
};

class ObFastMigrateDag : public share::ObIDag {
public:
  enum TaskType { INVALID = 0, SUSPEND_SRC = 1, HANDOVER_PG = 2, CLEAN_UP = 3, REPORT_META_TABLE = 4, MAX_TYPE };
  static bool is_valid_task_type(TaskType type);

public:
  ObFastMigrateDag();
  virtual ~ObFastMigrateDag();
  virtual bool operator==(const ObIDag& other) const override;
  virtual int64_t hash() const override;
  virtual int64_t get_tenant_id() const override;
  virtual int fill_comment(char* buf, const int64_t buf_len) const override;
  virtual int64_t get_compat_mode() const override;
  int init(ObPartGroupMigrationTask* group_task, TaskType sub_type);

public:
  ObPartGroupMigrationTask* group_task_;
  TaskType sub_type_;
};

class ObBaseMigrateDag : public share::ObIDag {
public:
  ObBaseMigrateDag(const ObIDagType type, const ObIDagPriority priority);
  virtual ~ObBaseMigrateDag();
  virtual bool operator==(const ObIDag& other) const override;
  virtual int64_t hash() const override;
  ObMigrateCtx* get_ctx()
  {
    return ctx_;
  }
  virtual int64_t get_tenant_id() const override
  {
    return tenant_id_;
  }
  virtual int fill_comment(char* buf, const int64_t buf_len) const override;
  virtual int64_t get_compat_mode() const override
  {
    return static_cast<int64_t>(compat_mode_);
  }
  void clear();

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, KP(this), K(*ctx_));

protected:
  bool is_inited_;
  ObMigrateCtx* ctx_;
  int64_t tenant_id_;
  share::ObWorker::CompatMode compat_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObBaseMigrateDag);
};

class ObMigrateDag : public ObBaseMigrateDag {
public:
  ObMigrateDag();
  virtual ~ObMigrateDag();
  int init(ObMigrateCtx& migrate_ctx);

protected:
  int init_for_restore_(ObMigrateCtx& ctx);
  int update_partition_meta_for_restore();
  int online_for_rebuild();
  int online_for_restore();

protected:
  DISALLOW_COPY_AND_ASSIGN(ObMigrateDag);
};

class ObBackupDag : public ObBaseMigrateDag {
public:
  ObBackupDag();
  virtual ~ObBackupDag();
  int init(const share::ObBackupDataType& backup_type, ObMigrateCtx& migrate_ctx);
  const share::ObBackupDataType& get_backup_data_type() const
  {
    return backup_data_type_;
  }

private:
  share::ObBackupDataType backup_data_type_;

protected:
  DISALLOW_COPY_AND_ASSIGN(ObBackupDag);
};

class ObValidateDag : public ObBaseMigrateDag {
public:
  ObValidateDag();
  virtual ~ObValidateDag();
  virtual bool operator==(const ObIDag& other) const override;
  virtual int64_t hash() const override;
  int init(ObMigrateCtx& migrate_ctx);

protected:
  DISALLOW_COPY_AND_ASSIGN(ObValidateDag);
};

class ObMigrateUtil {
public:
  static const int64_t RETRY_TASK_SLEEP_INTERVAL_S = 1000 * 1000L;  // 1s
  static const int64_t OB_MIGRATE_ONLINE_RETRY_COUNT = 3;
  static int enable_replay_with_old_partition(ObMigrateCtx& ctx);
  static int push_reference_tables_if_need(
      ObMigrateCtx& ctx, const ObPartitionSplitInfo& split_info, const int64_t last_replay_log_id);
  static int merge_trans_table(ObMigrateCtx& ctx);
  static int enable_replay_with_new_partition(ObMigrateCtx& ctx);
  static int create_empty_trans_sstable_for_compat(ObMigrateCtx& ctx);

private:
  static int wait_trans_table_merge_finish(ObMigrateCtx& ctx);
  static int add_trans_sstable_to_part_ctx(
      const ObPartitionKey& trans_table_pkey, ObSSTable& sstable, ObMigrateCtx& ctx);
};

class ObMigratePrepareTask : public share::ObITask {
public:
  static const int64_t MAX_LOGIC_TASK_COUNT_PER_SSTABLE = 64;
  static const int64_t MAX_MACRO_BLOCK_COUNT_PER_TASK = 128;
  static const int64_t OB_GET_SNAPSHOT_SCHEMA_VERSION_TIMEOUT = 30 * 1000 * 1000;  // 30s
  static const int64_t OB_GET_SNAPSHOT_SCHEMA_INTERVAL = 1 * 1000 * 1000;          // 1s
  static const int64_t OB_FETCH_TABLE_INFO_TIMEOUT = 30 * 1000 * 1000;             // 30s
  static const int64_t OB_FETCH_TABLE_INFO_RETRY_INTERVAL = 1 * 1000 * 1000L;      // 1s
  typedef common::hash::ObHashMap<blocksstable::ObSSTablePair, blocksstable::MacroBlockId> MacroPairMap;
  typedef int (*IsValidSrcFunc)(const obrpc::ObFetchPGInfoResult& result, ObMigrateCtx& ctx, bool& is_valid);
  ObMigratePrepareTask();
  virtual ~ObMigratePrepareTask();
  int init();
  virtual int process() override;

protected:
  int add_migrate_status(ObMigrateCtx* ctx);
  int add_partition_migration_status(const ObMigrateCtx& ctx);

  int prepare_migrate();
  int try_hold_local_partition();
  int choose_migrate_src();
  int choose_ob_migrate_src(const ObReplicaOpArg& arg, ObPartitionGroupMeta& pg_meta, ObMigrateSrcInfo& addr);
  int choose_restore_migrate_src(const ObReplicaOpArg& arg, ObPartitionGroupMeta& meta, ObMigrateSrcInfo& src_info);
  int choose_phy_restore_follower_src(
      const ObReplicaOpArg& arg, ObPartitionGroupMeta& pg_meta, ObMigrateSrcInfo& src_info);
  int choose_backup_migrate_src(const ObReplicaOpArg& arg, ObPartitionGroupMeta& meta, ObMigrateSrcInfo& src_info);
  int choose_ob_rebuild_src(const ObReplicaOpArg& arg, ObPartitionGroupMeta& meta, ObMigrateSrcInfo& src_info);
  int choose_rebuild_src(const ObReplicaOpArg& arg, ObPartitionGroupMeta& meta, ObMigrateSrcInfo& src_info);
  int choose_standby_restore_src(const ObReplicaOpArg& arg, ObPartitionGroupMeta& meta, ObMigrateSrcInfo& src_info);
  int choose_recommendable_src(const ObReplicaOpArg& arg, ObPartitionGroupMeta& meta, ObMigrateSrcInfo& src_info);

  int choose_ob_src(
      IsValidSrcFunc is_valid_src, const ObReplicaOpArg& arg, ObPartitionGroupMeta& meta, ObMigrateSrcInfo& src_info);

  int copy_needed_table_info(const ObReplicaOpArg& arg, const obrpc::ObPGPartitionMetaInfo& data_src_result,
      common::ObIArray<obrpc::ObFetchTableInfoResult>& table_info_res, common::ObIArray<uint64_t>& table_id_list,
      bool& found);
  int fetch_partition_group_info(
      const ObReplicaOpArg& arg, const ObMigrateSrcInfo& src_info, ObPartitionGroupInfoResult& result);

  int choose_rebuild_candidate(const ObReplicaOpArg& arg, const common::ObIArray<ObMigrateSrcInfo>& src_info_array,
      ObPartitionGroupMeta& meta, ObMigrateSrcInfo& src_info);
  int split_candidate_with_region(const common::ObIArray<ObMigrateSrcInfo>& src_info_array,
      common::ObIArray<ObMigrateSrcInfo>& same_region_array, common::ObIArray<ObMigrateSrcInfo>& diff_region_array);
  int fetch_suitable_rebuild_src(const ObReplicaOpArg& arg, const common::ObIArray<ObMigrateSrcInfo>& src_array,
      ObPartitionGroupInfoResult& out_result);

  int build_migrate_pg_partition_info();
  int build_migrate_partition_info(const obrpc::ObPGPartitionMetaInfo& partition_meta_info,
      const common::ObIArray<obrpc::ObFetchTableInfoResult>& table_info_res,
      const common::ObIArray<uint64_t>& table_id_list, ObPartitionMigrateCtx& part_migrate_ctx);
  int build_migrate_partition_info(ObMigratePartitionInfo& info);

  int get_local_table_info(
      const uint64_t table_id, const ObPartitionKey& pkey, common::ObIArray<ObITable::TableKey>& local_tables_info);
  int build_migrate_table_info(const uint64_t table_id, const ObPartitionKey& pkey,
      common::ObIArray<ObITable::TableKey>& local_tables_info, const obrpc::ObFetchTableInfoResult& result,
      ObMigrateTableInfo& info, ObPartitionMigrateCtx& part_ctx);
  int check_remote_sstables(const uint64_t table_id, common::ObIArray<ObITable::TableKey>& remote_major_sstables,
      common::ObIArray<ObITable::TableKey>& remote_inc_tbales);
  int check_remote_inc_sstables_continuity(common::ObIArray<ObITable::TableKey>& remote_inc_tables);
  int build_migrate_major_sstable(const bool need_reuse_local_minor,
      common::ObIArray<ObITable::TableKey>& local_major_tables, common::ObIArray<ObITable::TableKey>& local_inc_tables,
      common::ObIArray<ObITable::TableKey>& remote_major_tables,
      common::ObIArray<ObITable::TableKey>& remote_inc_tables,
      common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables);
  int build_migrate_major_sstable_(common::ObIArray<ObITable::TableKey>& local_major_tables,
      common::ObIArray<ObITable::TableKey>& local_inc_tables, common::ObIArray<ObITable::TableKey>& remote_major_tables,
      common::ObIArray<ObITable::TableKey>& remote_inc_tables,
      common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables);
  int build_migrate_major_sstable_v2_(const bool need_reuse_local_minor,
      common::ObIArray<ObITable::TableKey>& local_major_tables, common::ObIArray<ObITable::TableKey>& local_inc_tables,
      common::ObIArray<ObITable::TableKey>& remote_major_tables,
      common::ObIArray<ObITable::TableKey>& remote_inc_tables,
      common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables);
  int build_migrate_minor_sstable(const bool need_reuse_local_minor, common::ObIArray<ObITable::TableKey>& local_tables,
      common::ObIArray<ObITable::TableKey>& remote_inc_tables, ObIArray<ObITable::TableKey>& remote_gc_inc_sstables,
      common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables);
  int set_replica_type(ObMigrateCtx* ctx);
  int create_new_partition(const common::ObAddr& src_server, ObReplicaOpArg& replica_op_arg,
      ObIPartitionGroupGuard& partition_guard, blocksstable::ObStorageFileHandle& file_handle);

  int generate_physic_minor_sstable_copy_task(const ObMigrateSrcInfo& src_info, ObPartitionMigrateCtx& part_migrate_ctx,
      const ObMigrateTableInfo::SSTableInfo& minor_sstable_info, ObITask* parent_task, ObITask* child_task);
  int generate_logic_minor_sstable_copy_task(share::ObITask* parent_task, share::ObITask* child_task,
      const ObMigrateSrcInfo& src, const ObMigrateTableInfo::SSTableInfo& minor_sstable_info,
      ObPartitionMigrateCtx& part_migrate_ctx);
  int generate_minor_sstable_copy_task(share::ObITask* parent_task, share::ObITask* child_task,
      const ObMigrateSrcInfo& src_info, const ObMigrateTableInfo::SSTableInfo& minor_sstable_info,
      ObPartitionMigrateCtx& part_migrate_ctx);
  int build_logic_sstable_ctx(const ObMigrateSrcInfo& src_info,
      const ObMigrateTableInfo::SSTableInfo& minor_sstable_info, ObMigrateLogicSSTableCtx& ctx);
  int generate_major_sstable_copy_task(const ObMigrateSrcInfo& src_info, ObPartitionMigrateCtx& part_migrate_ctx,
      const ObMigrateTableInfo::SSTableInfo& major_sstable_info, share::ObITask* parent_task,
      share::ObITask* child_task);
  int generate_physic_sstable_copy_task(const ObMigrateSrcInfo& src_info, ObPartitionMigrateCtx& part_migrate_ctx,
      const ObMigrateTableInfo::SSTableInfo& sstable_info, share::ObITask* parent_task, share::ObITask* child_task);
  int build_physical_sstable_ctx(const ObMigrateSrcInfo& src_info, const ObMigrateTableInfo::SSTableInfo& sstable_info,
      ObMigratePhysicalSSTableCtx& ctx);
  int build_sub_physical_task(
      ObMigratePhysicalSSTableCtx& ctx, common::ObIArray<blocksstable::ObSSTablePair>& macro_block_list);

  int generate_pg_backup_tasks(common::ObIArray<share::ObITask*>& last_task_array);
  int generate_backup_tasks(ObITask*& last_task);
  int generate_backup_tasks(share::ObFakeTask& wait_migrate_finish_task);
  int generate_backup_major_tasks(share::ObITask*& parent_task);
  int generate_backup_major_copy_task(ObITask* parent_task, ObITask* child_task);
  int build_backup_physical_ctx(ObBackupPhysicalPGCtx& ctx);
  int fetch_backup_sstables(ObIArray<ObITable::TableKey>& table_keys);
  int build_backup_sub_task(ObBackupPhysicalPGCtx& ctx);

  int generate_pg_validate_tasks(common::ObIArray<share::ObITask*>& last_task_array);
  int generate_validate_tasks(share::ObITask*& last_task);
  int generate_validate_tasks(share::ObFakeTask& wait_finish_task);
  int generate_validate_backup_tasks(share::ObITask*& parent_task);
  int generate_validate_backup_task(share::ObITask* parent_task, share::ObITask* child_task);
  int build_validate_backup_ctx(ObValidateBackupPGCtx& ctx);
  int fetch_pg_macro_index_list(const common::ObPartitionKey& pg_key, ObValidateBackupPGCtx& ctx,
      common::ObIArray<ObBackupMacroIndex>& macro_index_list);
  int build_validate_sub_task(const common::ObIArray<ObBackupMacroIndex>& macro_index_list, ObValidateBackupPGCtx& ctx);

  int generate_pg_migrate_tasks(common::ObIArray<share::ObITask*>& last_task_array);
  int generate_pg_rebuild_tasks(common::ObIArray<share::ObITask*>& last_task_array);
  int generate_rebuild_tasks(ObPartitionMigrateCtx& part_migrate_ctx, share::ObITask*& last_task);
  int generate_wait_migrate_finish_task(share::ObFakeTask*& wait_migrate_finish_task);
  int generate_migrate_tasks(ObPartitionMigrateCtx& part_migrate_ctx, share::ObITask*& last_task);
  int generate_migrate_tasks(const ObMigrateSrcInfo& src_info, ObPartitionMigrateCtx& part_migrate_ctx,
      const ObMigrateTableInfo& table_info, share::ObFakeTask& wait_migrate_finish_task);
  int generate_rebuild_tasks(const ObMigrateSrcInfo& src_info, ObPartitionMigrateCtx& part_migrate_ctx,
      const ObMigrateTableInfo& table_info, share::ObFakeTask& wait_rebuild_finish_task);
  int generate_major_tasks(const ObMigrateSrcInfo& src_info, ObPartitionMigrateCtx& part_migrate_ctx,
      const ObMigrateTableInfo& table_info, share::ObITask*& parent_task);
  int generate_minor_tasks(const ObMigrateSrcInfo& src, ObPartitionMigrateCtx& part_migrate_ctx,
      const ObMigrateTableInfo& table_info, share::ObITask*& parent_task);

  int generate_finish_migrate_task(const common::ObIArray<ObITask*>& last_task_array, share::ObITask*& last_task);
  int try_get_snapshot_schema_version(
      const uint64_t table_id, const common::ObVersionRange& version_range, int64_t& schema_version);
  int get_snapshot_schema_version(const common::ObVersionRange& version_range, const uint64_t table_id,
      const uint64_t data_table_id, int64_t& schema_version);
  static int is_valid_migrate_src(const obrpc::ObFetchPGInfoResult& result, ObMigrateCtx& ctx, bool& is_valid);
  static int is_valid_standby_restore_src(const obrpc::ObFetchPGInfoResult& result, ObMigrateCtx& ctx, bool& is_valid);
  static int is_valid_rebuild_src(const obrpc::ObFetchPGInfoResult& result, ObMigrateCtx& ctx, bool& is_valid);
  static bool can_migrate_src_skip_log_sync(const obrpc::ObFetchPGInfoResult& result, ObMigrateCtx& ctx);
  int need_generate_sstable_migrate_tasks(bool& need_schedule);
  int get_base_meta_reader(const ObMigrateSrcInfo& src_info, const obrpc::ObFetchPhysicalBaseMetaArg& arg,
      ObIPhysicalBaseMetaReader*& reader);
  int get_base_meta_restore_reader_v1(const ObITable::TableKey& table_key, ObIPhysicalBaseMetaReader*& reader);
  int get_base_meta_restore_reader_v2(const ObITable::TableKey& table_key, ObIPhysicalBaseMetaReader*& reader);
  int get_base_meta_backup_reader(const ObITable::TableKey& table_key, ObIPhysicalBaseMetaReader*& reader);
  int get_base_meta_ob_reader(const ObMigrateSrcInfo& src_info, const obrpc::ObFetchPhysicalBaseMetaArg& arg,
      ObIPhysicalBaseMetaReader*& reader);
  int get_base_meta_rpc_reader(const ObMigrateSrcInfo& src_info, const obrpc::ObFetchPhysicalBaseMetaArg& arg,
      ObIPhysicalBaseMetaReader*& reader);
  int calc_macro_block_reuse(
      const ObITable::TableKey& table_key, common::ObIArray<blocksstable::ObSSTablePair>& macro_block_list);
  int check_can_reuse_sstable(
      const ObPartitionKey& pkey, ObMigrateTableInfo& table_info, ObPartitionMigrateCtx& part_ctx);
  int check_and_reuse_sstable(
      const ObPartitionKey& pkey, const ObITable::TableKey& table_key, bool& is_reuse, ObPartitionMigrateCtx& part_ctx);
  int prepare_restore_reader_if_needed();
  int prepare_backup_reader_if_needed();
  int create_partition_if_needed(  // for physical follower restore
      ObIPartitionGroupGuard& pg_guard, const ObPGPartitionStoreMeta& partition_meta);
  int update_multi_version_start();
  int build_index_partition_info(const ObReplicaOpArg& arg, ObIPGPartitionBaseDataMetaObReader* reader);
  int build_table_partition_info(const ObReplicaOpArg& arg, ObIPGPartitionBaseDataMetaObReader* reader);
  int remove_uncontinue_local_tables(const ObPartitionKey& pkey, const uint64_t table_id);
  int copy_rebuild_partition_info_result(
      ObPartitionGroupInfoResult& in_result, ObPartitionGroupInfoResult& out_result, bool& find_src);
  int check_partition_integrity();
  int create_pg_partition(const ObPGPartitionStoreMeta& meta, ObIPartitionGroup* partition, const bool in_slog_trans);
  int get_partition_table_info_reader(const ObMigrateSrcInfo& src_info, ObIPGPartitionBaseDataMetaObReader*& reader);
  int inner_get_partition_table_info_reader(
      const ObMigrateSrcInfo& src_info, ObIPGPartitionBaseDataMetaObReader*& reader);
  int inner_get_partition_table_info_restore_reader_v1(
      const ObMigrateSrcInfo& src_info, ObIPGPartitionBaseDataMetaObReader*& reader);
  int inner_get_partition_table_info_restore_reader_v2(
      const ObMigrateSrcInfo& src_info, ObIPGPartitionBaseDataMetaObReader*& reader);
  int inner_get_partition_table_info_backup_reader(
      const ObMigrateSrcInfo& src_info, ObIPGPartitionBaseDataMetaObReader*& reader);
  int build_remote_minor_sstables(const common::ObIArray<ObITable::TableKey>& local_minor_sstables,
      const common::ObIArray<ObITable::TableKey>& tmp_remote_minor_sstables,
      const common::ObIArray<ObITable::TableKey>& tmp_remote_gc_minor_sstables,
      common::ObIArray<ObITable::TableKey>& remote_minor_sstables,
      common::ObIArray<ObITable::TableKey>& remote_gc_minor_sstables, bool& need_reuse_local_minor);
  int generate_trans_table_migrate_task(ObITask*& last_task);
  int generate_post_prepare_task(ObITask& parent_task);
  int prepare_new_partition();
  int check_backup_data_continues();
  int prepare_restore_reader();
  int fill_log_ts_for_compat(ObMigrateTableInfo& info);
  int get_migrate_suitable_src(const common::ObIArray<ObMigrateSrcInfo>& src_info_array, const ObReplicaOpArg& arg,
      IsValidSrcFunc is_valid_src, bool& find_suitable_src, ObPartitionGroupMeta& pg_meta, ObMigrateSrcInfo& src_info);
  int get_minor_src_candidate_with_region(
      const ObReplicaOpArg& arg, common::ObIArray<ObMigrateSrcInfo>& src_info_array);
  int get_minor_src_candidate_without_region(
      const ObReplicaOpArg& arg, common::ObIArray<ObMigrateSrcInfo>& src_info_array);

  int generate_tasks_by_type();
  int generate_migrate_prepare_task();
  int generate_restore_cut_prepare_task();

protected:
  static const int64_t SRC_SET_BUCKET_NUM = 128;
  bool is_inited_;
  ObMigrateCtx* ctx_;
  ObIPartitionComponentFactory* cp_fty_;
  ObPartitionServiceRpc* rpc_;
  obrpc::ObPartitionServiceRpcProxy* srv_rpc_proxy_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  ObPartitionService* partition_service_;
  ObIPartitionGroupMetaRestoreReader* validate_meta_reader_;
  ObIPartitionGroupMetaRestoreReader* restore_meta_reader_;
  ObPartitionGroupMetaBackupReader* backup_meta_reader_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMigratePrepareTask);
};

struct ObMigrateLogicSSTableCtx {
  struct SubLogicTask {
    ObStoreRowkey end_key_;
    blocksstable::ObMacroBlocksWriteCtx block_write_ctx_;
    blocksstable::ObMacroBlocksWriteCtx lob_block_write_ctx_;

    SubLogicTask();
    virtual ~SubLogicTask();
    TO_STRING_KV(K_(end_key), K_(block_write_ctx), K_(lob_block_write_ctx));
  };
  SubLogicTask* tasks_;
  int64_t task_count_;
  ObLogicTableMeta meta_;
  common::ObArenaAllocator allocator_;
  ObMigrateSrcInfo src_info_;
  ObITable::TableKey table_key_;
  int64_t dest_base_version_;
  int64_t data_checksum_;

  ObMigrateLogicSSTableCtx();
  virtual ~ObMigrateLogicSSTableCtx();
  void reset();
  bool is_valid();
  int build_sub_task(const common::ObIArray<common::ObStoreRowkey>& end_key_list);
  int get_sub_task(const int64_t idx, SubLogicTask*& sub_task);
  int get_dest_table_key(ObITable::TableKey& dest_table_key);
  TO_STRING_KV(K_(meta), K_(task_count));

private:
  DISALLOW_COPY_AND_ASSIGN(ObMigrateLogicSSTableCtx);
};

class ObMigrateCopyLogicTask : public share::ObITask {
public:
  ObMigrateCopyLogicTask();
  virtual ~ObMigrateCopyLogicTask();
  int init(const int64_t task_idx, ObMigrateLogicSSTableCtx& sstable_ctx, ObMigrateCtx& ctx);
  int generate_next_task(ObITask*& next_task) override;
  virtual int process() override;

private:
  bool is_inited_;
  ObMigrateCtx* ctx_;
  int64_t task_idx_;
  ObMigrateLogicSSTableCtx* sstable_ctx_;
  ObMigrateLogicSSTableCtx::SubLogicTask* sub_task_;
  DISALLOW_COPY_AND_ASSIGN(ObMigrateCopyLogicTask);
};

class ObMigrateFinishLogicTask : public share::ObITask {
public:
  ObMigrateFinishLogicTask();
  virtual ~ObMigrateFinishLogicTask();
  int init(ObPartitionMigrateCtx& part_migrate_ctx);
  virtual int process() override;
  ObMigrateLogicSSTableCtx& get_sstable_ctx()
  {
    return sstable_ctx_;
  }

private:
  bool is_inited_;
  ObPartitionMigrateCtx* part_migrate_ctx_;
  ObMigrateLogicSSTableCtx sstable_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObMigrateFinishLogicTask);
};

struct ObMigratePhysicalSSTableCtx {
  struct SubTask {
    ObMigrateMacroBlockInfo* block_info_;
    int64_t block_count_;
    common::ObPartitionKey pkey_;
    common::ObArenaAllocator allocator_;

    SubTask();
    virtual ~SubTask();
    DECLARE_VIRTUAL_TO_STRING;
  };
  SubTask* tasks_;
  int64_t task_count_;
  common::ObArenaAllocator allocator_;
  blocksstable::ObSSTableBaseMeta meta_;
  ObMigrateSrcInfo src_info_;
  ObMigrateTableInfo::SSTableInfo sstable_info_;
  bool is_leader_restore_;
  int64_t restore_version_;

  ObMigratePhysicalSSTableCtx();
  virtual ~ObMigratePhysicalSSTableCtx();
  bool is_valid();
  void reset();
  int get_dest_table_key(ObITable::TableKey& dest_table_key);
  DECLARE_VIRTUAL_TO_STRING;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMigratePhysicalSSTableCtx);
};

class ObMigrateCopyPhysicalTask : public share::ObITask {
public:
  ObMigrateCopyPhysicalTask();
  virtual ~ObMigrateCopyPhysicalTask();
  int generate_next_task(ObITask*& next_task);
  int init(const int64_t task_idx, ObMigratePhysicalSSTableCtx& sstable_ctx, ObMigrateCtx& ctx);
  virtual int process() override;

private:
  int get_macro_block_reader(obrpc::ObPartitionServiceRpcProxy* srv_rpc_proxy,
      common::ObInOutBandwidthThrottle* bandwidth_throttle, const ObMigrateSrcInfo& src_info,
      common::ObIArray<ObMigrateArgMacroBlockInfo>& list, ObITable::TableKey& table_key,
      const ObRestoreInfo* restore_info, ObIPartitionMacroBlockReader*& reader);
  int get_macro_block_restore_reader(common::ObInOutBandwidthThrottle& bandwidth_throttle,
      common::ObIArray<ObMigrateArgMacroBlockInfo>& list, const ObRestoreInfo& restore_info,
      const ObITable::TableKey& table_key, ObIPartitionMacroBlockReader*& reader);
  int get_macro_block_restore_reader_v1(common::ObInOutBandwidthThrottle& bandwidth_throttle,
      common::ObIArray<ObMigrateArgMacroBlockInfo>& list, const share::ObPhysicalRestoreArg& restore_info,
      const ObITable::TableKey& table_key, ObIPartitionMacroBlockReader*& reader);
  int get_macro_block_restore_reader_v2(common::ObInOutBandwidthThrottle& bandwidth_throttle,
      common::ObIArray<ObMigrateArgMacroBlockInfo>& list, const share::ObPhysicalRestoreArg& restore_info,
      const ObITable::TableKey& table_key, ObIPartitionMacroBlockReader*& reader);
  int get_macro_block_ob_reader(obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy,
      common::ObInOutBandwidthThrottle& bandwidth_throttle, const ObMigrateSrcInfo& src_info,
      ObITable::TableKey& table_key, common::ObIArray<ObMigrateArgMacroBlockInfo>& list,
      ObIPartitionMacroBlockReader*& reader);
  int fetch_major_block_with_retry(
      common::ObIArray<ObMigrateArgMacroBlockInfo>& list, blocksstable::ObMacroBlocksWriteCtx& copied_ctx);
  int fetch_major_block(
      common::ObIArray<ObMigrateArgMacroBlockInfo>& list, blocksstable::ObMacroBlocksWriteCtx& copied_ctx);
  int calc_migrate_data_statics(const int64_t copy_count, const int64_t reuse_count);
  int alloc_migrate_writer(ObIPartitionMacroBlockReader* reader, ObIMigrateMacroBlockWriter*& writer);
  void free_migrate_writer(ObIMigrateMacroBlockWriter*& writer);

private:
  static const int64_t MAX_RETRY_TIMES = 3;
  static const int64_t OB_FETCH_MAJOR_BLOCK_RETRY_INTERVAL = 1 * 1000 * 1000L;  // 1s
  bool is_inited_;
  ObMigrateCtx* ctx_;
  int64_t task_idx_;
  ObMigratePhysicalSSTableCtx* sstable_ctx_;
  ObMigratePhysicalSSTableCtx::SubTask* sub_task_;
  ObIPartitionComponentFactory* cp_fty_;
  DISALLOW_COPY_AND_ASSIGN(ObMigrateCopyPhysicalTask);
};

class ObMigrateFinishPhysicalTask : public share::ObITask {
public:
  ObMigrateFinishPhysicalTask();
  virtual ~ObMigrateFinishPhysicalTask();
  int init(ObPartitionMigrateCtx& part_finish_ctx);
  virtual int process() override;
  ObMigratePhysicalSSTableCtx& get_sstable_ctx()
  {
    return sstable_ctx_;
  }

private:
  int check_sstable_meta(
      const blocksstable::ObSSTableBaseMeta& src_meta, const blocksstable::ObSSTableBaseMeta& write_meta);
  int acquire_sstable(const ObITable::TableKey& dest_table_key, ObTableHandle& handle);

private:
  bool is_inited_;
  ObPartitionMigrateCtx* part_migrate_ctx_;
  ObMigratePhysicalSSTableCtx sstable_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObMigrateFinishPhysicalTask);
};

class ObMigrateFinishTask : public share::ObITask {
public:
  static const int64_t OB_CHECK_LOG_SYNC_INTERVAL = 200 * 1000;                 // 200ms
  static const int64_t OB_WAIT_LOG_SYNC_TIMEOUT = 30 * 1000 * 1000;             // 30 s
  static const int64_t OB_CHECK_MINOR_MERGE_FINISH_INTERVAL = 100 * 1000;       // 100ms
  static const int64_t OB_WAIT_MINOR_MERGE_FINISH_TIMEOUT = 120 * 1000 * 1000;  // 2min

  ObMigrateFinishTask();
  virtual ~ObMigrateFinishTask();
  int init(ObMigrateCtx& ctx);
  virtual int process() override;

private:
  int check_pg_available_index_all_exist();
  int check_available_index_all_exist(const ObPartitionKey& pkey, ObPartitionStorage* storage);
  int wait_log_sync(uint64_t max_clog_id = OB_INVALID_TIMESTAMP);
  int check_pg_partition_ready_for_read();
  int check_partition_ready_for_read(const ObMigratePartitionInfo& copy_info, ObIPartitionStorage* storage);
  int check_partition_ready_for_read(ObIPartitionStorage* storage);
  int enable_replay_for_rebuild();
  int update_pg_partition_report_status();
  int create_pg_partition_if_need();
  int check_partition_ready_for_read_in_remote();
  int check_partition_ready_for_read_out_remote();
  int update_split_state();
  int lock_pg_owner(common::ObMySQLTransaction& trans, ObIPartitionGroup& pg, const common::ObAddr& data_src_server,
      const int64_t epoch_number);
  int enable_replay();

private:
  bool is_inited_;
  ObMigrateCtx* ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObMigrateFinishTask);
};

class ObGroupMigrateDag : public share::ObIDag {
public:
  ObGroupMigrateDag();
  virtual ~ObGroupMigrateDag();
  virtual bool operator==(const ObIDag& other) const override;
  virtual int64_t hash() const override;
  virtual int init(ObPartGroupTask* group_task, storage::ObPartitionService* partition_service);
  virtual int64_t get_tenant_id() const override
  {
    return tenant_id_;
  }
  virtual int fill_comment(char* buf, const int64_t buf_len) const override;
  virtual int64_t get_compat_mode() const override
  {
    return static_cast<int64_t>(share::ObWorker::CompatMode::MYSQL);
  }
  void clear();

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, KP(this), K(group_task_));

private:
  int report_result(const bool is_batch_mode, const ObIArray<ObReportPartMigrationTask>& report_list);
  int single_report_result(const ObIArray<ObReportPartMigrationTask>& report_list);
  int batch_report_result(const ObIArray<ObReportPartMigrationTask>& report_list);

private:
  bool is_inited_;
  ObPartGroupTask* group_task_;
  int64_t tenant_id_;
  storage::ObPartitionService* partition_service_;
  DISALLOW_COPY_AND_ASSIGN(ObGroupMigrateDag);
};

class ObGroupMigrateExecuteTask : public share::ObITask {
public:
  ObGroupMigrateExecuteTask();
  virtual ~ObGroupMigrateExecuteTask();
  int init(ObPartGroupTask* group_task);
  virtual int process() override;

private:
  bool is_inited_;
  ObPartGroupTask* group_task_;
  DISALLOW_COPY_AND_ASSIGN(ObGroupMigrateExecuteTask);
};

class ObMigrateGetLeaderUtil {
public:
  static int get_leader(
      const common::ObPartitionKey& pkey, ObMigrateSrcInfo& leader_info, const bool force_renew = false);
  static int get_clog_parent(clog::ObIPartitionLogService& log_service, ObMigrateSrcInfo& parent_info);
};

class ObMigratePostPrepareTask : public ObMigratePrepareTask {
public:
  ObMigratePostPrepareTask();
  virtual ~ObMigratePostPrepareTask();
  virtual int process() override;

private:
  int schedule_migrate_tasks();
  int enable_replay_with_new_partition();
  int deal_with_rebuild_partition();
  int update_multi_version_start();
  int deal_with_old_partition();
  int deal_with_new_partition();
  int deal_with_standby_restore_partition();
  int generate_restore_tailored_task(share::ObITask* last_task);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMigratePostPrepareTask);
};

OB_INLINE bool ObMigrateCtx::is_migrate_compat_version() const
{
  return fetch_pg_info_compat_version_ < ObFetchPGInfoArg::FETCH_PG_INFO_ARG_COMPAT_VERSION_V2;
}

class ObRestoreTailoredFinishTask;
class ObRestoreTailoredPrepareTask : public share::ObITask {
public:
  ObRestoreTailoredPrepareTask();
  virtual ~ObRestoreTailoredPrepareTask();
  int init();
  virtual int process() override;

private:
  int schedule_restore_tailored_task(ObRestoreTailoredFinishTask& finish_task);
  int schedule_restore_tailored_finish_task();
  int filter_tailored_tables(common::ObIArray<uint64_t>& index_ids, ObRecoveryPointSchemaFilter& schema_filter);
  int update_restore_flag_cut_data();
  int check_need_generate_task(bool& need_generate);

private:
  int schedule_restore_table_tailored_task_(const common::ObIArray<uint64_t>& index_ids,
      const ObPartitionKey& partition_key, ObPartitionStorage& storage, ObRestoreTailoredFinishTask& finish_task);

private:
  bool is_inited_;
  ObMigrateCtx* ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRestoreTailoredPrepareTask);
};

class ObRestoreTailoredTask : public share::ObITask {
public:
  ObRestoreTailoredTask();
  virtual ~ObRestoreTailoredTask();
  int init(const uint64_t index_id, const ObTablesHandle& minor_tables_handle, const ObTableHandle& major_table_handle,
      const ObPGKey& pg_key, const ObPartitionKey& partition_key, ObRestoreTailoredFinishTask& finish_task);
  virtual int process() override;

private:
  int get_tailored_table_key(ObITable::TableKey& table_key);
  int generate_new_minor_sstable(const ObITable::TableKey& table_key,
      blocksstable::ObMacroBlocksWriteCtx& block_write_ctx, blocksstable::ObMacroBlocksWriteCtx& lob_block_write_ctx);
  int generate_major_sstable();

private:
  bool is_inited_;
  ObMigrateCtx* ctx_;
  uint64_t index_id_;
  ObTablesHandle minor_tables_handle_;
  ObTableHandle major_table_handle_;
  ObPGKey pg_key_;
  ObPartitionKey partition_key_;
  ObRestoreTailoredFinishTask* finish_task_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRestoreTailoredTask);
};

class ObRestoreTailoredFinishTask : public share::ObITask {
public:
  ObRestoreTailoredFinishTask();
  virtual ~ObRestoreTailoredFinishTask();
  int init();
  virtual int process() override;
  int add_sstable_handle(const ObPartitionKey& pkey, ObTableHandle& handle);
  int set_schema_version(const int64_t schema_version);

private:
  bool is_inited_;
  ObMigrateCtx* ctx_;
  common::SpinRWLock lock_;
  ObArray<ObPartitionMigrateCtx> part_ctx_array_;
  int64_t schema_version_;
};

}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_PARTITION_MIGRATOR_H_
