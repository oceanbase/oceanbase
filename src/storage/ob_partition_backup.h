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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_BACKUP_H_
#define OCEANBASE_STORAGE_OB_PARTITION_BACKUP_H_

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
#include "storage/ob_partition_migrator.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObPartitionMigrateCtx;
class ObBackupPhysicalPGCtx;
class ObPartGroupBackupMajorTask;

class ObPartGroupBackupTask : public ObPartGroupTask {
public:
  static const int64_t PART_GROUP_TASK_IDLE_TIME_MS = 10 * 1000LL;   // 10s
  static const int64_t PART_GROUP_BACKUP_POINT_CHECK_TIME_MS = 100;  // 100ms
  typedef hash::ObHashMap<ObITable::TableKey, int64_t> BackupTableIndexMap;
  ObPartGroupBackupTask();
  virtual ~ObPartGroupBackupTask();
  int init(const common::ObIArray<ObReplicaOpArg>& task_list, const bool is_batch_mode,
      storage::ObPartitionService* partition_service, const share::ObTaskId& task_id);
  int check_before_do_task();  // only invoked before executing, so without concurrency invoke
  virtual int do_task();
  virtual Type get_task_type() const
  {
    return PART_GROUP_BACKUP_TASK;
  }

  TO_STRING_KV(K_(task_id), K_(is_inited), K_(is_finished), K_(is_batch_mode), KP_(partition_service),
      K_(first_error_code), K_(type), "sub_task_count", task_list_.count(), K_(task_list));

private:
  int check_partition_validation();  // only invoked before executing, so without concurrency invoke
  int do_part_group_backup_minor_task();
  int do_part_group_backup_major_task();
  int do_backup_pg_metas();
  int do_backup_task(const share::ObBackupDataType& backup_data_type);

  int set_task_list_result(const int32_t first_error_code, const common::ObIArray<ObPartMigrationTask>& task_list);
  int finish_group_backup_task();
  int init_backup_minor_task(
      const bool is_batch_mode, storage::ObPartitionService* partition_service, const share::ObTaskId& task_id);
  int try_schedule_new_partition_backup(const share::ObBackupDataType& backup_data_type);
  int try_finish_group_backup(bool& is_finished);
  int schedule_backup_dag(const share::ObBackupDataType& backup_data_type, ObMigrateCtx& migrate_ctx);
  int inner_schedule_partition(ObPartMigrationTask*& task, bool& need_schedule);
  int try_schedule_partition_backup(const share::ObBackupDataType& backup_data_type);
  void reset_tasks_status();
  int check_all_pg_backup_point_created();
  int check_pg_backup_point_created(const ObPartitionKey& pg_key, const int64_t backup_snapshot_version);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartGroupBackupTask);
};

class ObBackupPrepareTask : public share::ObITask {
public:
  typedef common::hash::ObHashMap<blocksstable::ObSSTablePair, blocksstable::MacroBlockId> MacroPairMap;
  ObBackupPrepareTask();
  virtual ~ObBackupPrepareTask();
  int init(ObIPartitionComponentFactory& cp_fty, common::ObInOutBandwidthThrottle& bandwidth_throttle,
      ObPartitionService& partition_service);
  virtual int process() override;

protected:
  int add_backup_status(ObMigrateCtx* ctx);
  int add_partition_backup_status(const ObMigrateCtx& ctx);
  int prepare_backup();
  int build_backup_prepare_context();
  int schedule_backup_tasks();
  int fetch_partition_group_info(
      const ObReplicaOpArg& arg, const ObMigrateSrcInfo& src_info, ObPartitionGroupInfoResult& result);
  int build_backup_pg_partition_info();
  int build_backup_partition_info(const obrpc::ObPGPartitionMetaInfo& partition_meta_info,
      const common::ObIArray<obrpc::ObFetchTableInfoResult>& table_info_res,
      const common::ObIArray<uint64_t>& table_id_list, ObPartitionMigrateCtx& part_migrate_ctx);

  int build_backup_table_info(const uint64_t table_id, const ObPartitionKey& pkey,
      const obrpc::ObFetchTableInfoResult& result, ObMigrateTableInfo& info);
  int check_remote_sstables(const uint64_t table_id, common::ObIArray<ObITable::TableKey>& remote_major_sstables,
      common::ObIArray<ObITable::TableKey>& remote_inc_tbales);
  int check_remote_inc_sstables_continuity(
      const int64_t last_major_snapshot_version, common::ObIArray<ObITable::TableKey>& remote_inc_tables);
  int build_backup_major_sstable(const common::ObIArray<ObITable::TableKey>& local_tables,
      common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables);
  int build_backup_minor_sstable(const common::ObIArray<ObITable::TableKey>& local_tables,
      common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables);
  int generate_wait_backup_finish_task(share::ObFakeTask*& wait_backup_finish_task);
  int generate_pg_backup_tasks(common::ObIArray<share::ObITask*>& last_task_array);
  int generate_backup_tasks(ObITask*& last_task);
  int generate_backup_tasks(share::ObFakeTask& wait_migrate_finish_task);
  int generate_backup_sstable_tasks(share::ObITask*& parent_task);
  int generate_backup_sstable_copy_task(ObITask* parent_task, ObITask* child_task);
  int build_backup_physical_ctx(ObBackupPhysicalPGCtx& ctx);
  int fetch_backup_sstables(const share::ObBackupDataType& backup_data_type, ObIArray<ObITable::TableKey>& table_keys);
  int fetch_backup_major_sstables(ObIArray<ObITable::TableKey>& table_keys);
  int fetch_backup_minor_sstables(ObIArray<ObITable::TableKey>& table_keys);

  int build_backup_sub_task(ObBackupPhysicalPGCtx& ctx);
  int prepare_backup_reader();
  int build_table_partition_info(const ObReplicaOpArg& arg, ObIPGPartitionBaseDataMetaObReader* reader);
  int get_partition_table_info_reader(const ObMigrateSrcInfo& src_info, ObIPGPartitionBaseDataMetaObReader*& reader);
  int inner_get_partition_table_info_reader(
      const ObMigrateSrcInfo& src_info, ObIPGPartitionBaseDataMetaObReader*& reader);
  int get_partition_table_info_backup_reader(
      const ObMigrateSrcInfo& src_info, ObIPGPartitionBaseDataMetaObReader*& reader);
  int check_backup_data_continues();

protected:
  bool is_inited_;
  ObMigrateCtx* ctx_;
  ObIPartitionComponentFactory* cp_fty_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  ObPartitionService* partition_service_;
  ObPartitionGroupMetaBackupReader* backup_meta_reader_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupPrepareTask);
};

}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_PARTITION_BACKUP_H_
