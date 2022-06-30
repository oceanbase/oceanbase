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

#define USING_LOG_PREFIX STORAGE
#include "lib/utility/ob_tracepoint.h"
#include "lib/hash/ob_hashset.h"
#include "share/ob_force_print_log.h"
#include "storage/ob_partition_migrator.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_sstable.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_log.h"
#include "storage/ob_partition_base_data_ob_reader.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "clog/ob_clog_history_reporter.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "share/partition_table/ob_united_pt_operator.h"
#include "storage/ob_partition_base_data_restore_reader.h"
#include "storage/ob_partition_base_data_backup.h"
#include "storage/ob_partition_base_data_validate.h"
#include "share/ob_task_define.h"
#include "share/backup/ob_backup_struct.h"
#include "ob_migrate_logic_row_writer.h"
#include "ob_table_mgr.h"
#include "ob_migrate_macro_block_writer.h"
#include "share/ob_debug_sync_point.h"
#include "ob_storage_struct.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "share/ob_force_print_log.h"
#include "storage/ob_partition_migrator_table_key_mgr.h"
#include "storage/ob_partition_migration_status.h"
#include "storage/ob_pg_all_meta_checkpoint_reader.h"
#include "storage/blocksstable/ob_store_file_system.h"
#include "storage/ob_data_macro_id_iterator.h"
#include "share/partition_table/ob_united_pt_operator.h"
#include "share/backup/ob_backup_path.h"
#include "storage/ob_partition_base_data_physical_restore.h"
#include "storage/ob_file_system_util.h"
#include "storage/ob_partition_backup.h"
#include "storage/ob_pg_storage.h"
#include "storage/ob_partition_scheduler.h"

namespace oceanbase {
using namespace common;
using namespace common::hash;
using namespace share;
using namespace obrpc;
using namespace blocksstable;
using namespace oceanbase::share::schema;
using namespace omt;
using namespace transaction;

namespace storage {
#define MIGRATOR (::oceanbase::storage::ObPartitionMigrator::get_instance())
typedef common::hash::ObHashMap<uint64_t, ObSSTable *> SSTableMap;

int ObPartMigrationTask::assign(const ObPartMigrationTask &task)
{
  int ret = common::OB_SUCCESS;

  if (task.ctx_.is_valid()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("cannot assign valid ctx", K(ret), K(task));
  } else {
    arg_ = task.arg_;
    status_ = task.status_;
    result_ = task.result_;
    need_reset_migrate_status_ = task.need_reset_migrate_status_;
    during_migrating_ = task.during_migrating_;
  }
  return ret;
}

ObMacroBlockReuseMgr::ObMacroBlockReuseMgr() : full_map_(), reuse_map_(), allocator_(ObModIds::OB_PARTITION_MIGRATOR)
{}

ObMacroBlockReuseMgr::~ObMacroBlockReuseMgr()
{
  reset();
}

// TODO(): split reuse macro block not support yet
int ObMacroBlockReuseMgr::build_reuse_macro_map(
    ObMigrateCtx &ctx, const ObITable::TableKey &table_key, const common::ObIArray<ObSSTablePair> &macro_block_list)
{
  int ret = OB_SUCCESS;
  ObTablesHandle tables_handle;
  ObDataMacroIdIterator data_macro_iter;
  ObIPartitionGroup *pg = ctx.get_partition();
  if (OB_ISNULL(pg)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, pg must not be null", K(ret));
  } else if (ObITable::MAJOR_SSTABLE != table_key.table_type_) {
    // only major sstable can reuse macro block
  } else if (OB_FAIL(data_macro_iter.init(ObPartitionService::get_instance(), pg))) {
    LOG_WARN("fail to init pg iterator", K(ret));
  } else if (OB_FAIL(full_map_.init(ObModIds::OB_PARTITION_MIGRATOR, pg->get_partition_key().get_tenant_id()))) {
    LOG_WARN("fail to init hash map", K(ret));
  } else if (!reuse_map_.is_inited() &&
             OB_FAIL(reuse_map_.init(ObModIds::OB_PARTITION_MIGRATOR, pg->get_partition_key().get_tenant_id()))) {
    LOG_WARN("fail to init hash map", K(ret));
  } else {
    ObMacroBlockInfoPair info;
    ObTenantFileKey file_key;
    const ObMacroBlockMetaV2 *meta = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(data_macro_iter.get_next_macro_info(info, file_key))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        ObMajorMacroBlockKey key;
        meta = info.meta_.meta_;
        key.table_id_ = meta->table_id_;
        key.partition_id_ = meta->partition_id_;
        key.data_version_ = meta->data_version_;
        key.data_seq_ = meta->data_seq_;
        if (OB_FAIL(full_map_.insert_or_update(key, info))) {
          LOG_WARN("fail to set macro block map", K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_list.count(); ++i) {
      ObMacroBlockMetaV2 *dst_meta = nullptr;
      ObMacroBlockSchemaInfo *dst_schema = nullptr;
      ObMacroBlockInfoPair info;
      ObMajorMacroBlockKey key;
      key.table_id_ = table_key.table_id_;
      key.partition_id_ = table_key.pkey_.get_partition_id();
      key.data_version_ = macro_block_list.at(i).data_version_;
      key.data_seq_ = macro_block_list.at(i).data_seq_;
      if (OB_FAIL(full_map_.get(key, info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get from hash map", K(ret), K(key));
        }
      } else {
        FLOG_INFO("reuse a macro block", K(key), K(*info.meta_.meta_));
        if (OB_FAIL(ctx.add_major_block_id(info.block_id_))) {
          LOG_WARN("fail to add major block id", K(ret));
        } else if (OB_FAIL(info.meta_.meta_->deep_copy(dst_meta, allocator_))) {
          LOG_WARN("fail to deep copy meta", K(ret));
        } else if (OB_FAIL(info.meta_.schema_->deep_copy(dst_schema, allocator_))) {
          LOG_WARN("fail to deep copy macro schema", K(ret));
        } else {
          ObFullMacroBlockMeta full_meta;
          full_meta.schema_ = dst_schema;
          full_meta.meta_ = dst_meta;
          if (OB_FAIL(reuse_map_.insert_or_update(key, ObMacroBlockInfoPair(info.block_id_, full_meta)))) {
            LOG_WARN("fail to set reuse map", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
          if (nullptr != dst_meta) {
            dst_meta->~ObMacroBlockMetaV2();
            dst_meta = nullptr;
          }
          if (nullptr != dst_schema) {
            dst_schema->~ObMacroBlockSchemaInfo();
            dst_schema = nullptr;
          }
        }
      }
    }
  }
  full_map_.destroy();
  return ret;
}

void ObMacroBlockReuseMgr::reset()
{
  full_map_.destroy();
  RemoveFunctor functor;
  reuse_map_.remove_if(functor);
  reuse_map_.destroy();
  allocator_.reset();
}

bool ObMacroBlockReuseMgr::RemoveFunctor::operator()(
    const blocksstable::ObMajorMacroBlockKey &block_key, blocksstable::ObMacroBlockInfoPair &info)
{
  UNUSED(block_key);
  if (nullptr != info.meta_.meta_) {
    info.meta_.meta_->~ObMacroBlockMetaV2();
  }
  if (nullptr != info.meta_.schema_) {
    info.meta_.schema_->~ObMacroBlockSchemaInfo();
  }
  return true;
}

int ObMacroBlockReuseMgr::get_reuse_macro_meta(
    const blocksstable::ObMajorMacroBlockKey &block_key, blocksstable::ObMacroBlockInfoPair &info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reuse_map_.get(block_key, info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get from reuse map", K(ret));
    }
  }
  return ret;
}

ObPartitionGroupInfoResult::ObPartitionGroupInfoResult() : result_(), choose_src_info_()
{}

void ObPartitionGroupInfoResult::reset()
{
  result_.reset();
  choose_src_info_.reset();
}

int ObPartitionGroupInfoResult::assign(const ObPartitionGroupInfoResult &result)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(result_.assign(result.result_))) {
    STORAGE_LOG(WARN, "fail to assign result", K(ret), K(result));
  } else {
    choose_src_info_ = result.choose_src_info_;
  }
  return ret;
}

ObMigrateCtx::ObMigrateCtx()
    : lock_(),
      replica_op_arg_(),
      macro_indexs_(NULL),
      action_(UNKNOWN),
      result_(OB_SUCCESS),
      doing_task_cnt_(0),
      total_task_cnt_(0),
      need_rebuild_(false),
      partition_guard_(),
      replica_state_(OB_UNKNOWN_REPLICA),
      local_publish_version_(0),
      local_last_replay_log_id_(0),
      last_check_replay_ts_(0),
      create_ts_(0),
      task_id_(),
      copy_size_(0),
      continue_fail_count_(0),
      rebuild_count_(-1),
      finish_ts_(0),
      wait_replay_start_ts_(0),
      wait_minor_merge_start_ts_(0),
      last_confirmed_log_id_(OB_INVALID_ID),
      last_confirmed_log_ts_(OB_INVALID_TIMESTAMP),
      group_task_(NULL),
      restore_info_(),
      during_migrating_(false),
      need_online_for_rebuild_(false),
      trace_id_array_(),
      major_block_id_array_ptr_(NULL),
      curr_major_block_array_index_(0),
      can_rebuild_(true),
      pg_meta_(),
      part_ctx_array_(),
      need_offline_(false),
      is_restore_(share::REPLICA_NOT_RESTORE),
      use_slave_safe_read_ts_(false),
      need_report_checksum_(false),
      data_statics_(),
      is_copy_cover_minor_(true),
      mig_src_file_id_(OB_INVALID_DATA_FILE_ID),
      mig_dest_file_id_(OB_INVALID_DATA_FILE_ID),
      src_suspend_ts_(0),
      is_takeover_finished_(false),
      is_member_change_finished_(false),
      local_last_replay_log_ts_(0),
      old_trans_table_seq_(-1),
      create_new_pg_(false),
      restore_meta_reader_(nullptr),
      fetch_pg_info_compat_version_(ObFetchPGInfoArg::FETCH_PG_INFO_ARG_COMPAT_VERSION_V2),
      physical_backup_ctx_(),
      recovery_point_ctx_()
{}

ObMigrateCtx::~ObMigrateCtx()
{
  for (int64_t i = 0; i < 2; ++i) {
    for (int64_t j = 0; j < major_block_id_array_[i].count(); ++j) {
      if (major_block_id_array_[i].at(j).is_valid()) {
        get_partition()->get_storage_file()->dec_ref(major_block_id_array_[i].at(j));
        major_block_id_array_[i].at(j).reset();
      }
    }
  }
  if (NULL != MIGRATOR.get_cp_fty()) {
    if (NULL != restore_meta_reader_) {
      MIGRATOR.get_cp_fty()->free(restore_meta_reader_);
    }
    if (NULL != macro_indexs_) {
      MIGRATOR.get_cp_fty()->free(macro_indexs_);
    }
  }
}

bool ObMigrateCtx::is_valid() const
{
  return replica_op_arg_.is_valid() && UNKNOWN != action_ && doing_task_cnt_ >= 0 && total_task_cnt_ >= 0 &&
         NULL != group_task_
      // replica_state_ maybe OB_ERROR_REPLICA in init
      // partition maybe null
      // migrate_src mybe invalid
      ;
}

int ObMigrateCtx::notice_start_part_task()
{
  int ret = OB_SUCCESS;
  if (NULL != group_task_) {
    int64_t backup_set_id = -1;
    if (VALIDATE_BACKUP_OP == replica_op_arg_.type_) {
      backup_set_id = replica_op_arg_.validate_arg_.backup_set_id_;
    } else if (BACKUP_BACKUPSET_OP == replica_op_arg_.type_) {
      backup_set_id = replica_op_arg_.backup_backupset_arg_.backup_set_id_;
    }
    if (OB_SUCCESS != (ret = group_task_->set_part_task_start(replica_op_arg_.key_, backup_set_id))) {
      STORAGE_LOG(ERROR, "set_part_task_start failed", K(ret), K(*this));
    } else {
      STORAGE_LOG(INFO, "set_part_task_start", K(ret), K(*this));
    }
  }

  return ret;
}

int ObMigrateCtx::notice_finish_part_task()
{
  int ret = OB_SUCCESS;
  if (NULL != group_task_) {
    SMART_VAR(ObReplicaOpArg, tmp_replica_op_arg)
    {
      tmp_replica_op_arg = replica_op_arg_;
      int64_t backup_set_id = -1;
      if (VALIDATE_BACKUP_OP == replica_op_arg_.type_) {
        backup_set_id = replica_op_arg_.validate_arg_.backup_set_id_;
      } else if (BACKUP_BACKUPSET_OP == replica_op_arg_.type_) {
        backup_set_id = replica_op_arg_.backup_backupset_arg_.backup_set_id_;
      }
      if (OB_SUCCESS != (ret = group_task_->set_part_task_finish(
                             replica_op_arg_.key_, result_, task_id_, during_migrating_, backup_set_id))) {
        STORAGE_LOG(ERROR, "set_part_task_finish failed", K(ret), K(tmp_replica_op_arg));
      } else {
        STORAGE_LOG(INFO, "set_part_task_finish", K(ret), K(tmp_replica_op_arg));
      }
    }
  }

  return ret;
}

void ObMigrateCtx::set_result_code(const int32_t result)
{
  const bool write_lock = true;
  ObMigrateCtxGuard guard(write_lock, *this);
  if (OB_SUCCESS == result_) {
    result_ = result;
    LOG_WARN("record result code", K(result), K(lbt()));
  }
}

ObMigrateCtxGuard::ObMigrateCtxGuard(const bool is_write_lock, ObMigrateCtx &ctx) : ctx_(ctx)
{
  int tmp_ret = OB_SUCCESS;

  if (is_write_lock) {
    if (OB_SUCCESS != (tmp_ret = ctx_.lock_.wrlock())) {
      STORAGE_LOG(ERROR, "failed to wrlock", K(tmp_ret));
    }
  } else {
    if (OB_SUCCESS != (tmp_ret = ctx_.lock_.rdlock())) {
      STORAGE_LOG(ERROR, "failed to rdlock", K(tmp_ret));
    }
  }
}

ObMigrateCtxGuard::~ObMigrateCtxGuard()
{
  ctx_.lock_.unlock();
}

int ObMigrateCtx::update_partition_migration_status() const
{
  int ret = OB_SUCCESS;
  const ObMigrateCtx &ctx = *this;

  if (!ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(ctx));
  } else {
    ObPartitionMigrationStatusGuard status_guard;
    ObPartitionMigrationStatus *status = NULL;
    if (OB_FAIL(ObPartitionMigrationStatusMgr::get_instance().get_status(ctx.task_id_, status_guard))) {
      STORAGE_LOG(WARN, "failed to get partition status", K(ret), K(ctx));
    } else if (NULL == (status = status_guard.getStatus())) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "status not found", K(ret), K(ctx));
    } else {
      // status->task_id_; should not change
      // status->pkey_; should not change
      status->src_ = ctx.replica_op_arg_.data_src_.get_server();
      // status->dest_; should not change
      status->clog_parent_ = ctx.clog_parent_;
      status->result_ = ctx.result_;
      status->start_time_ = ctx.create_ts_;
      status->action_ = ctx.action_;
      status->replica_state_ = ctx.replica_state_;
      status->doing_task_count_ = ctx.doing_task_cnt_;
      status->total_task_count_ = ctx.total_task_cnt_;
      status->rebuild_count_ = ctx.rebuild_count_;
      status->continue_fail_count_ = ctx.continue_fail_count_;
      status->finish_time_ = ctx.finish_ts_;

      status->data_statics_.total_macro_block_ = ATOMIC_LOAD(&ctx.data_statics_.total_macro_block_);
      status->data_statics_.ready_macro_block_ = ATOMIC_LOAD(&ctx.data_statics_.ready_macro_block_);
      status->data_statics_.major_count_ = ATOMIC_LOAD(&ctx.data_statics_.major_count_);
      status->data_statics_.mini_minor_count_ = ATOMIC_LOAD(&ctx.data_statics_.mini_minor_count_);
      status->data_statics_.normal_minor_count_ = ATOMIC_LOAD(&ctx.data_statics_.normal_minor_count_);
      status->data_statics_.buf_minor_count_ = ATOMIC_LOAD(&ctx.data_statics_.buf_minor_count_);
      // allow comment truncation, no need to set ret
      (void)ctx.fill_comment(status->comment_, sizeof(status->comment_));

      STORAGE_LOG(INFO, "update migration status", K(*status));
    }
  }

  return ret;
}

bool ObMigrateCtx::is_only_copy_sstable() const
{
  return COPY_GLOBAL_INDEX_OP == replica_op_arg_.type_ || COPY_LOCAL_INDEX_OP == replica_op_arg_.type_ ||
         RESTORE_REPLICA_OP == replica_op_arg_.type_ || RESTORE_FOLLOWER_REPLICA_OP == replica_op_arg_.type_ ||
         BACKUP_REPLICA_OP == replica_op_arg_.type_ || RESTORE_STANDBY_OP == replica_op_arg_.type_ ||
         VALIDATE_BACKUP_OP == replica_op_arg_.type_ || LINK_SHARE_MAJOR_OP == replica_op_arg_.type_ ||
         BACKUP_BACKUPSET_OP == replica_op_arg_.type_;
}

bool ObMigrateCtx::is_copy_index() const
{
  return COPY_GLOBAL_INDEX_OP == replica_op_arg_.type_ || COPY_LOCAL_INDEX_OP == replica_op_arg_.type_;
}

int ObMigrateCtx::add_major_block_id(const MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(major_block_id_array_ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "major block id array ptr should not be NULL", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(major_block_id_array_ptr_->push_back(macro_block_id))) {
      STORAGE_LOG(WARN, "fail to add major block key into array", K(ret));
    } else if (OB_FAIL(get_partition()->get_storage_file()->inc_ref(macro_block_id))) {
      major_block_id_array_ptr_->pop_back();
      STORAGE_LOG(WARN, "fail to inc ref, ", K(ret), K(macro_block_id));
    }
  }
  return ret;
}

void ObMigrateCtx::free_old_macro_block()
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  int64_t index = (curr_major_block_array_index_ + 1) % 2;
  for (int64_t i = 0; i < major_block_id_array_[index].count(); ++i) {
    if (OB_FAIL(get_partition()->get_storage_file()->dec_ref(major_block_id_array_[index].at(i)))) {
      STORAGE_LOG(WARN, "fail to dec ref, ", K(ret), K(major_block_id_array_[index].at(i)));
    }
  }
  major_block_id_array_[index].reuse();
}

void ObMigrateCtx::calc_need_retry()
{
  common::SpinWLockGuard guard(lock_);
  if (!need_rebuild_) {
    LOG_INFO("ObParGroupMigrationTask do not need rebuild", K(*this));
  } else if (rebuild_count_ < GCONF.sys_bkgd_migration_retry_num) {
    rebuild_count_++;
  } else {
    need_rebuild_ = false;
  }
}

void ObMigrateCtx::rebuild_migrate_ctx()
{
  common::SpinWLockGuard guard(lock_);
  action_ = ObMigrateCtx::INIT;
  result_ = OB_SUCCESS;
  doing_task_cnt_ = 1;
  total_task_cnt_++;
  need_rebuild_ = false;
  need_online_for_rebuild_ = false;
  part_ctx_array_.reset();
  need_report_checksum_ = false;
  data_statics_.reset();
  trans_table_handle_.reset();
  old_trans_table_seq_ = -1;
  create_new_pg_ = false;
  if (BACKUP_REPLICA_OP != replica_op_arg_.type_) {
    pg_meta_.reset();
  }
  local_publish_version_ = 0;
  local_last_replay_log_ts_ = 0;
  fetch_pg_info_compat_version_ = ObFetchPGInfoArg::FETCH_PG_INFO_ARG_COMPAT_VERSION_V2;
  local_last_replay_log_id_ = 0;
  physical_backup_ctx_.reset();
  recovery_point_ctx_.reset();
}

int ObMigrateCtx::generate_and_schedule_migrate_dag()
{
  int ret = OB_SUCCESS;
  ObBaseMigrateDag *dag = NULL;
  ObMigrateTaskSchedulerTask *scheduler_task = NULL;
  if (BACKUP_REPLICA_OP == replica_op_arg_.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("generate and schedule migrate dag get unexpected replica op", K(ret), K(*this));
  } else {
    {
      common::SpinWLockGuard guard(lock_);
      if (OB_FAIL(alloc_migrate_dag(dag))) {
        LOG_WARN("failed to alloc migrate dag", K(ret));
      } else if (OB_FAIL(dag->alloc_task(scheduler_task))) {
        STORAGE_LOG(WARN, "Fail to alloc task, ", K(ret));
      } else if (OB_FAIL(scheduler_task->init())) {
        STORAGE_LOG(WARN, "failed to init prepare_task", K(ret));
      } else if (OB_FAIL(dag->add_task(*scheduler_task))) {
        STORAGE_LOG(WARN, "Fail to add task", K(ret), K(replica_op_arg_));
      } else if (OB_FAIL(ObDagScheduler::get_instance().add_dag(dag))) {
        STORAGE_LOG(WARN, "failed to add dag", K(ret), K(*dag));
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          LOG_WARN("Fail to add task", K(ret));
          ret = OB_EAGAIN;
        }
      } else {
        LOG_INFO("succeed to schedule migrate dag", K(*dag));
      }
    }
  }

  if (OB_FAIL(ret)) {
    result_ = ret;
    if (NULL != dag) {
      dag->clear();  // release dag ctx_
      ObDagScheduler::get_instance().free_dag(*dag);
    }
  }
  return ret;
}

int ObMigrateCtx::generate_and_schedule_backup_dag(const ObBackupDataType &backup_data_type)
{
  int ret = OB_SUCCESS;
  ObBackupDag *backup_dag = NULL;
  ObBackupPrepareTask *prepare_task = NULL;

  if (!backup_data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate and schedule backup dag get invalid argument", K(ret), K(backup_data_type));
  } else if (BACKUP_REPLICA_OP != replica_op_arg_.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("generate and schedule backup dag get unexpected replica op", K(ret), K(*this));
  } else {
    {
      common::SpinWLockGuard guard(lock_);
      if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(backup_dag))) {
        LOG_WARN("failed to alloc backup dag", K(ret));
      } else if (OB_FAIL(backup_dag->init(backup_data_type, *this))) {
        LOG_WARN("failed to init migrate dag", K(ret));
      } else if (OB_FAIL(backup_dag->alloc_task(prepare_task))) {
        STORAGE_LOG(WARN, "Fail to alloc task, ", K(ret));
      } else if (OB_FAIL(prepare_task->init(
                     *MIGRATOR.get_cp_fty(), *MIGRATOR.get_bandwidth_throttle(), *MIGRATOR.get_partition_service()))) {
        STORAGE_LOG(WARN, "failed to init prepare_task", K(ret));
      } else if (OB_FAIL(backup_dag->add_task(*prepare_task))) {
        STORAGE_LOG(WARN, "Fail to add task", K(ret), K(replica_op_arg_));
      } else if (OB_FAIL(ObDagScheduler::get_instance().add_dag(backup_dag))) {
        STORAGE_LOG(WARN, "failed to add dag", K(ret), K(*backup_dag));
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          LOG_WARN("Fail to add task", K(ret));
          ret = OB_EAGAIN;
        }
      } else {
        LOG_INFO("succeed to schedule backup dag", K(*backup_dag));
      }
    }
  }

  if (OB_FAIL(ret)) {
    result_ = ret;
    if (NULL != backup_dag) {
      backup_dag->clear();  // release dag ctx_
      ObDagScheduler::get_instance().free_dag(*backup_dag);
    }
  }
  return ret;
}

bool ObMigrateCtx::is_need_retry(const int result) const
{
  bool ret = false;
  if (OB_NOT_INIT != result && OB_INVALID_ARGUMENT != result && OB_ERR_SYS != result && OB_INIT_TWICE != result &&
      OB_ERR_UNEXPECTED != result && OB_LOG_NOT_SYNC != result && OB_NO_NEED_REBUILD != result &&
      OB_SRC_DO_NOT_ALLOWED_MIGRATE != result && OB_NOT_SUPPORTED != result && OB_PG_IS_REMOVED != result &&
      can_rebuild_) {
    ret = true;
  }
  return ret;
}

// if local has partition full data, no need copy, return true
int ObMigrateCtx::change_replica_with_data(bool &is_replica_with_data)
{
  int ret = OB_SUCCESS;
  is_replica_with_data = false;

  if (CHANGE_REPLICA_OP != replica_op_arg_.type_) {
    is_replica_with_data = false;
    LOG_INFO("not change replica op");
  } else if (OB_ISNULL(partition_guard_.get_partition_group())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition should not be NULL", K(ret));
  } else {
    const ObReplicaType &replica_type = partition_guard_.get_partition_group()->get_replica_type();
    is_replica_with_data = ObReplicaTypeCheck::is_replica_with_ssstore(replica_type);
    LOG_INFO("get local replica type", K(is_replica_with_data), K(replica_type));
  }
  return ret;
}

int ObMigrateCtx::get_restore_clog_info(uint64_t &log_id, int64_t &acc_checksum) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!replica_op_arg_.is_physical_restore_leader())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(replica_op_arg_));
  } else if (OB_UNLIKELY(pg_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "restore pg meta is invalid", K(ret), K(pg_meta_));
  } else {
    log_id = pg_meta_.storage_info_.get_clog_info().get_last_replay_log_id();
    acc_checksum = pg_meta_.storage_info_.get_clog_info().get_accumulate_checksum();
  }
  return ret;
}

bool ObMigrateCtx::is_leader_restore_archive_data() const
{
  return replica_op_arg_.is_physical_restore_leader() && REPLICA_RESTORE_ARCHIVE_DATA == is_restore_;
}

// reset is_restore in add_replica when src is in restore process
int ObMigrateCtx::set_is_restore_for_add_replica(const int16_t src_is_restore)
{
  int ret = OB_SUCCESS;
  if (ADD_REPLICA_OP != replica_op_arg_.type_) {
    // do nothing
  } else {
    switch (src_is_restore) {
      case REPLICA_NOT_RESTORE:
      case REPLICA_LOGICAL_RESTORE_DATA:
      case REPLICA_RESTORE_DATA:
      case REPLICA_RESTORE_STANDBY:
      case REPLICA_RESTORE_CUT_DATA:
      case REPLICA_RESTORE_STANDBY_CUT:
        is_restore_ = src_is_restore;
        break;
      case REPLICA_RESTORE_ARCHIVE_DATA:
      case REPLICA_RESTORE_LOG:
      case REPLICA_RESTORE_DUMP_MEMTABLE:
        is_restore_ = REPLICA_RESTORE_ARCHIVE_DATA;
        break;
      case REPLICA_RESTORE_WAIT_ALL_DUMPED:
        is_restore_ = REPLICA_RESTORE_WAIT_ALL_DUMPED;
        break;
      case REPLICA_RESTORE_MEMBER_LIST: {
        ret = OB_CANNOT_ADD_REPLICA_DURING_SET_MEMBER_LIST;
        STORAGE_LOG(WARN, "cannot add replica during set member list", K(ret), K(src_is_restore));
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unknown restore type", K(ret), K(src_is_restore));
        break;
      }
    }
  }
  return ret;
}

int ObMigrateCtx::alloc_migrate_dag(ObBaseMigrateDag *&base_migrate_dag)
{
  int ret = OB_SUCCESS;
  base_migrate_dag = NULL;
  if (!replica_op_arg_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "replica op arg is invlaid", K(ret), K(*this));
  } else if (VALIDATE_BACKUP_OP == replica_op_arg_.type_) {
    ObValidateDag *dag = NULL;
    if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(dag))) {
      LOG_WARN("failed to alloc validate dag", K(ret));
    } else if (FALSE_IT(base_migrate_dag = dag)) {
    } else if (OB_FAIL(dag->init(*this))) {
      LOG_WARN("failed to init validate dag", K(ret));
    }
  } else if (BACKUP_BACKUPSET_OP == replica_op_arg_.type_) {
    ObBackupBackupsetDag *dag = NULL;
    if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(dag))) {
      LOG_WARN("failed to alloc backup backupset dag", K(ret));
    } else if (FALSE_IT(base_migrate_dag = dag)) {
    } else if (OB_FAIL(dag->init(*this))) {
      LOG_WARN("failed to init validate dag", K(ret));
    }
  } else if (BACKUP_ARCHIVELOG_OP == replica_op_arg_.type_) {
    ObBackupArchiveLogDag *dag = NULL;
    if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(dag))) {
      LOG_WARN("failed to alloc backup archivelog dag", K(ret));
    } else if (FALSE_IT(base_migrate_dag = dag)) {
    } else if (OB_FAIL(dag->init(*this))) {
      LOG_WARN("failed to init validate dag", K(ret));
    }
  } else {
    ObMigrateDag *dag = NULL;
    if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(dag))) {
      LOG_WARN("failed to alloc migrate dag", K(ret));
    } else if (FALSE_IT(base_migrate_dag = dag)) {
    } else if (OB_FAIL(dag->init(*this))) {
      LOG_WARN("failed to init migrate dag", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != base_migrate_dag) {
      base_migrate_dag->clear();  // release dag ctx_
      ObDagScheduler::get_instance().free_dag(*base_migrate_dag);
      base_migrate_dag = nullptr;
    }
  }
  return ret;
}

ObPartitionMigrateCtx::ObPartitionMigrateCtx()
    : ObIPartitionMigrateCtx(),
      copy_info_(),
      handle_(),
      is_partition_exist_(true),
      lock_(),
      need_reuse_local_minor_(true)
{}

bool ObPartitionMigrateCtx::is_valid() const
{
  bool bret = NULL != ctx_ && ctx_->is_valid() && copy_info_.is_valid();
  if (bret) {
    if (ctx_->is_copy_index() && !handle_.empty()) {
      bret = false;
    }
  }
  return bret;
}

void ObPartitionMigrateCtx::reset()
{
  ctx_ = NULL;
  copy_info_.reset();
  handle_.reset();
  is_partition_exist_ = true;
  need_reuse_local_minor_ = true;
}

int ObPartitionMigrateCtx::assign(const ObPartitionMigrateCtx &part_migrate_ctx)
{
  int ret = OB_SUCCESS;
  if (!part_migrate_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign partition migrate ctx get invalid argument", K(ret), K(part_migrate_ctx));
  } else if (OB_FAIL(copy_info_.assign(part_migrate_ctx.copy_info_))) {
    LOG_WARN("failed to assign copy info", K(ret), K(part_migrate_ctx));
  } else if (OB_FAIL(handle_.assign(part_migrate_ctx.handle_))) {
    LOG_WARN("failed to assign handle", K(ret));
  } else {
    ctx_ = part_migrate_ctx.ctx_;
    is_partition_exist_ = part_migrate_ctx.is_partition_exist_;
    need_reuse_local_minor_ = part_migrate_ctx.need_reuse_local_minor_;
  }
  return ret;
}

int ObPartitionMigrateCtx::add_sstable(ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *pg = NULL;
  const int64_t max_kept_major_version_number = 0;
  const bool in_slog_trans = false;

  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("ctx must not null", K(ret), K(*this));
  } else if (!ctx_->is_copy_index()) {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(handle_.add_table(&sstable))) {
      LOG_WARN("failed to add table", K(ret));
    } else {
      FLOG_INFO("hold sstable in ctx", "pkey", copy_info_.meta_.pkey_, "table_key", sstable.get_key());
    }
  } else {
    if (OB_ISNULL(pg = ctx_->partition_guard_.get_partition_group())) {
      ret = OB_ERR_SYS;
      LOG_ERROR("partition must not null", K(ret));
    } else if (OB_FAIL(pg->get_pg_storage().add_sstable(
                   copy_info_.meta_.pkey_, &sstable, max_kept_major_version_number, in_slog_trans))) {
      LOG_WARN("failed to add sstable", K(ret));
    } else {
      FLOG_INFO("add sstable to pg", "pkey", copy_info_.meta_.pkey_, "table_key", sstable.get_key());
    }
  }
  return ret;
}

ObMigrateRecoveryPointCtx::ObMigrateRecoveryPointCtx()
    : ObIPartitionMigrateCtx(), recovery_point_index_(0), recovery_point_key_array_(), tables_handle_map_(), lock_()
{}

ObMigrateRecoveryPointCtx::~ObMigrateRecoveryPointCtx()
{
  reset();
}

int ObMigrateRecoveryPointCtx::init(ObMigrateCtx &migrate_ctx)
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(tables_handle_map_.create(BUCKET_NUM, ObModIds::OB_PARTITION_MIGRATOR))) {
    LOG_WARN("failed to create tables handle map", K(ret));
  } else {
    ctx_ = &migrate_ctx;
  }
  return ret;
}

void ObMigrateRecoveryPointCtx::reset()
{
  reuse();
  tables_handle_map_.reuse();
}

// for inner retry
// just reuse recovery_point_index_ and recovery_point_key_array_
void ObMigrateRecoveryPointCtx::reuse()
{
  recovery_point_index_ = 0;
  recovery_point_key_array_.reset();
}

bool ObMigrateRecoveryPointCtx::is_valid() const
{
  return tables_handle_map_.created() && recovery_point_index_ >= 0 && recovery_point_key_array_.count() >= 0;
}

int ObMigrateRecoveryPointCtx::add_sstable(ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObTableHandle table_handle;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!sstable.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add sstable get invalid argument", K(ret), K(sstable));
  } else if (OB_FAIL(table_handle.set_table(&sstable))) {
    LOG_WARN("failed to set table", K(ret), K(sstable));
  } else if (OB_FAIL(add_sstable(table_handle))) {
    LOG_WARN("failed to add sstable", K(ret), K(table_handle));
  }
  return ret;
}

int ObMigrateRecoveryPointCtx::get_recovery_point_info(
    const int64_t recovery_point_index, ObRecoveryPointKey &recovery_point_key)
{
  int ret = OB_SUCCESS;
  recovery_point_key.reset();
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (recovery_point_index < 0 || recovery_point_index >= recovery_point_key_array_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "get recovery point info get invalid argument", K(ret), K(recovery_point_index), K(recovery_point_key_array_));
  } else {
    recovery_point_key = recovery_point_key_array_.at(recovery_point_index);
  }
  return ret;
}

int ObMigrateRecoveryPointCtx::get_current_recovery_point_info(ObRecoveryPointKey &recovery_point_key)
{
  int ret = OB_SUCCESS;
  recovery_point_key.reset();
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (recovery_point_index_ >= recovery_point_key_array_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("get current recovery point info failed", K(ret), K(recovery_point_index_), K(recovery_point_key_array_));
  } else {
    recovery_point_key = recovery_point_key_array_.at(recovery_point_index_);
  }
  return ret;
}

int ObMigrateRecoveryPointCtx::add_sstable(ObTableHandle &table_handle)
{
  int ret = OB_SUCCESS;
  ObITable *table = NULL;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(table = table_handle.get_table())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is NULL", K(ret), K(table_handle));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(tables_handle_map_.set_refactored(table->get_key(), table_handle))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to set table handle into map", K(ret), K(table_handle));
      }
    }
  }
  return ret;
}

bool ObMigrateRecoveryPointCtx::is_recovery_point_iter_finish() const
{
  return recovery_point_index_ >= recovery_point_key_array_.count();
}

bool ObMigrateRecoveryPointCtx::is_recovery_point_index_valid() const
{
  return recovery_point_index_ >= 0 && recovery_point_index_ <= recovery_point_key_array_.count();
}

int ObMigrateRecoveryPointCtx::get_sstable(const ObITable::TableKey &table_key, ObTableHandle &table_handle)
{
  int ret = OB_SUCCESS;
  table_handle.reset();
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get sstable get invalid argument", K(ret), K(table_key));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (OB_FAIL(tables_handle_map_.get_refactored(table_key, table_handle))) {
      LOG_WARN("failed to get table handle from map", K(ret), K(table_key));
    }
  }
  return ret;
}

bool ObMigrateRecoveryPointCtx::is_tables_handle_empty() const
{
  return 0 == tables_handle_map_.size();
}

int ObMigrateRecoveryPointCtx::get_all_recovery_point_key(ObIArray<ObRecoveryPointKey> &recovery_point_key_array)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("recovery point ctx do not init", K(ret));
  } else if (OB_FAIL(recovery_point_key_array.assign(recovery_point_key_array_))) {
    LOG_WARN("failed to assign recovery point key array", K(ret));
  }
  return ret;
}

int ObMigrateRecoveryPointCtx::remove_unneed_table_handle(const ObHashSet<ObITable::TableKey> &table_key_set)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  hash::ObHashMap<ObITable::TableKey, ObTableHandle>::const_iterator iter;
  ObArray<ObITable::TableKey> need_removed_table_keys;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("recovery point ctx do not init", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    for (iter = tables_handle_map_.begin(); OB_SUCC(ret) && iter != tables_handle_map_.end(); ++iter) {
      const ObITable::TableKey &table_key = iter->first;
      hash_ret = table_key_set.exist_refactored(table_key);
      if (OB_HASH_EXIST == hash_ret) {
      } else if (OB_HASH_NOT_EXIST == hash_ret) {
        if (OB_FAIL(need_removed_table_keys.push_back(table_key))) {
          LOG_WARN("failed to push table key into array", K(ret), K(table_key));
        }
      } else {
        ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < need_removed_table_keys.count(); ++i) {
      const ObITable::TableKey &table_key = need_removed_table_keys.at(i);
      if (OB_FAIL(tables_handle_map_.erase_refactored(table_key))) {
        LOG_WARN("failed to erase table key from map", K(ret), K(table_key));
      }
    }
  }
  return ret;
}

int64_t ObMigrateRecoveryPointCtx::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV(KP(this), K_(recovery_point_index), K_(recovery_point_key_array));
    J_COMMA();
    J_OBJ_END();
  }
  return pos;
}

int ObMigrateFinishPhysicalTask::check_sstable_meta(
    const blocksstable::ObSSTableBaseMeta &src_meta, const blocksstable::ObSSTableBaseMeta &write_meta, const bool is_check_in_advance)
{
  int ret = OB_SUCCESS;
  const int32_t ERROR_CODE = is_check_in_advance ? OB_ENTRY_EXIST : OB_INVALID_DATA;

  if (src_meta.index_id_ != write_meta.index_id_) {
    ret = ERROR_CODE;
    STORAGE_LOG(WARN, "index_id_ not match", K(ret));
  } else if (src_meta.row_count_ != write_meta.row_count_) {
    ret = ERROR_CODE;
    STORAGE_LOG(WARN, "row_count_ not match", K(ret));
  } else if (src_meta.occupy_size_ != write_meta.occupy_size_) {
    ret = ERROR_CODE;
    STORAGE_LOG(WARN, "occupy_size_ not match", K(ret));
  } else if (src_meta.data_checksum_ != write_meta.data_checksum_) {
    ret = ERROR_CODE;
    STORAGE_LOG(WARN, "data checksum not match", K(ret));
  } else if (src_meta.row_checksum_ != write_meta.row_checksum_) {
    ret = ERROR_CODE;
    STORAGE_LOG(WARN, "row_checksum_ not match", K(ret));
  } else if (src_meta.data_version_ != write_meta.data_version_) {
    ret = ERROR_CODE;
    STORAGE_LOG(WARN, "data_version_ not match", K(ret));
  } else if (src_meta.rowkey_column_count_ != write_meta.rowkey_column_count_) {
    ret = ERROR_CODE;
    STORAGE_LOG(WARN, "rowkey_column_count_ not match", K(ret));
  } else if (src_meta.table_type_ != write_meta.table_type_) {
    ret = ERROR_CODE;
    STORAGE_LOG(WARN, "table_type_ not match", K(ret));
  } else if (src_meta.index_type_ != write_meta.index_type_) {
    ret = ERROR_CODE;
    STORAGE_LOG(WARN, "index_type_ not match", K(ret));
  } else if (src_meta.macro_block_count_ != write_meta.macro_block_count_) {
    ret = ERROR_CODE;
    STORAGE_LOG(WARN, "macro_block_count_ not match", K(ret));
  } else if (src_meta.lob_macro_block_count_ != write_meta.lob_macro_block_count_) {
    ret = ERROR_CODE;
    STORAGE_LOG(WARN, "lob_macro_block_count_ not match", K(ret));
  } else if (src_meta.column_cnt_ != write_meta.column_cnt_) {
    ret = ERROR_CODE;
    STORAGE_LOG(WARN, "column_cnt_ not match", K(ret));
  } else if (src_meta.total_sstable_count_ != write_meta.total_sstable_count_) {
    ret = ERROR_CODE;
    STORAGE_LOG(WARN, "total_sstable_count_ not match", K(ret));
  } else if (src_meta.max_logic_block_index_ != write_meta.max_logic_block_index_) {
    ret = ERROR_CODE;
    STORAGE_LOG(WARN, "max_logic_block_index_ not match", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < src_meta.column_cnt_; ++i) {
      const ObSSTableColumnMeta &src_col = src_meta.column_metas_.at(i);
      const ObSSTableColumnMeta &write_col = write_meta.column_metas_.at(i);
      if (src_col.column_id_ != write_col.column_id_ ||
          src_col.column_default_checksum_ != write_col.column_default_checksum_ ||
          src_col.column_checksum_ != write_col.column_checksum_) {
        ret = ERROR_CODE;
        STORAGE_LOG(WARN, "column_metas_ not match", K(ret), K(i), K(src_meta), K(write_meta));
      }
    }
  }
  return ret;
}

int ObMigrateFinishPhysicalTask::check_sstable_meta(
    const bool is_check_in_advance, ObTableHandle &handle)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate finish physical task do not init", K(ret));
  } else if (!handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check sstable meta get invalid argument", K(ret), K(handle));
  } else if (OB_FAIL(handle.get_sstable(sstable))) {
    LOG_WARN("fail to get sstable", K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_SYS;
    LOG_WARN("sstable should not be null", K(ret));
  } else if (OB_FAIL(check_sstable_meta(sstable_ctx_.meta_, sstable->get_meta(), is_check_in_advance))) {
    LOG_WARN("failed to check sstable meta",
        K(ret), K(sstable_ctx_.meta_), "new_meta", sstable->get_meta());
  }
  return ret;
}


int ObMigratePrepareTask::create_new_partition(const ObAddr &src_server, ObReplicaOpArg &replica_op_arg,
    ObIPartitionGroupGuard &partition_guard, ObStorageFileHandle &file_handle)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  clog::ObIPartitionLogService *pls = NULL;
  const int64_t is_restore = ctx_->is_restore_;
  ObTablesHandle sstables_handle;                                        // empty
  ctx_->pg_meta_.storage_info_.set_pg_file_id(OB_INVALID_DATA_FILE_ID);  // for create a new pg_file in ofs mode
  const ObSavedStorageInfoV2 &saved_storage_info = ctx_->pg_meta_.storage_info_;
  const ObPartitionSplitInfo &split_info = ctx_->pg_meta_.split_info_;
  int64_t split_state = ctx_->pg_meta_.saved_split_state_;
  // 221 and 222 observer may set split_state = -1, upgrade to 224 may has problem, now set split_state = 1 directly
  if (-1 == split_state) {
    split_state = 1;
    LOG_WARN("split state was -1, should be rewritten to 1", "pkey", ctx_->pg_meta_.pg_key_);
  }
  const ObBaseStorageInfo &clog_info = saved_storage_info.get_clog_info();
  common::ObReplicaType replica_type = replica_op_arg.dst_.get_replica_type();
  const int64_t memstore_percent = replica_op_arg.dst_.get_memstore_percent();
  ObCreatePGParam param;
  common::ObReplicaProperty replica_property;  // TODO: need using RS appoint replica property
  ObMigrateStatus migrate_status;
  bool need_create_memtable = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "task has not initialized.", K(ret));
  } else if (OB_FAIL(partition_service_->create_new_partition(replica_op_arg.key_, partition))) {
    STORAGE_LOG(WARN, "Fail to create new partition, ", K(replica_op_arg.key_), K(ret));
  } else if (NULL == partition) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The partition is NULL, ", K(ret), K(replica_op_arg.key_));
  } else if (OB_FAIL(ObMigrateStatusHelper::trans_replica_op(replica_op_arg.type_, migrate_status))) {
    LOG_WARN("failed to trans migrate status", K(ret), K(replica_op_arg));
  } else {
    partition_guard.set_partition_group(partition_service_->get_pg_mgr(), *partition);
    ObChangePartitionLogEntry entry;
    const int64_t subcmd = ObIRedoModule::gen_subcmd(OB_REDO_LOG_PARTITION, REDO_LOG_ADD_PARTITION_GROUP);
    const bool write_slog = true;
    const int64_t data_version = 0;
    const bool in_slog_trans = true;

    entry.partition_key_ = ctx_->replica_op_arg_.key_;
    entry.replica_type_ = replica_type;
    // pg and partition witch contains pg_key and partition_key are same
    entry.pg_key_ = ctx_->replica_op_arg_.key_;

    ObStorageLogAttribute log_attr;

    param.create_frozen_version_ = ctx_->pg_meta_.create_frozen_version_;
    param.last_restore_log_id_ = ctx_->pg_meta_.last_restore_log_id_;
    param.last_restore_log_ts_ = ctx_->pg_meta_.last_restore_log_ts_;
    param.restore_snapshot_version_ = ctx_->pg_meta_.restore_snapshot_version_;
    param.restore_schema_version_ = ctx_->pg_meta_.restore_schema_version_;
    if (NULL == (pls = partition->get_log_service())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error, pls is NULL, ", K(ret));
    } else if (OB_FAIL(replica_property.set_memstore_percent(memstore_percent))) {
      STORAGE_LOG(WARN, "failed to set memstore percent", K(ret), K(replica_op_arg), K(memstore_percent));
    } else if (FALSE_IT(param.create_timestamp_ = ctx_->pg_meta_.create_timestamp_)) {
    } else if (FALSE_IT(param.replica_property_ = replica_property)) {
    } else if (FALSE_IT(param.replica_type_ = replica_type)) {
    } else if (FALSE_IT(param.is_restore_ = is_restore)) {
    } else if (FALSE_IT(param.write_slog_ = write_slog)) {
    } else if (FALSE_IT(param.data_version_ = data_version)) {
    } else if (FALSE_IT(param.split_state_ = split_state)) {
    } else if (FALSE_IT(param.file_handle_ = &file_handle)) {
    } else if (FALSE_IT(param.file_mgr_ = &OB_SERVER_FILE_MGR)) {
    } else if (FALSE_IT(param.migrate_status_ = migrate_status)) {
    } else if (OB_FAIL(param.set_storage_info(saved_storage_info))) {
      LOG_WARN("failed to set storage info", K(ret), K(saved_storage_info));
    } else if (OB_FAIL(param.set_split_info(split_info))) {
      LOG_WARN("failed to set split info", K(ret), K(split_info));
    } else if (OB_FAIL(partition_service_->get_clog_mgr()->add_partition(replica_op_arg.key_,
                   replica_type,
                   replica_property,
                   clog_info,
                   ObVersion(0, 0) /*freeze version*/,
                   src_server,
                   is_restore,
                   pls))) {
      STORAGE_LOG(WARN, "fail to initiate partition log service", K(ret), K(replica_op_arg));
      // migrate partition will not refresh schema, need set region
    } else if (OB_FAIL(partition_service_->set_region(replica_op_arg.key_, pls))) {
      STORAGE_LOG(WARN, "fail to set region", K(ret), K(replica_op_arg));
    } else {
      log_attr.tenant_id_ = entry.pg_key_.get_tenant_id();
      log_attr.data_file_id_ = file_handle.get_storage_file()->get_file_id();
      param.migrate_status_ = migrate_status;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(SLOGGER.write_log(subcmd, log_attr, entry))) {
      STORAGE_LOG(WARN, "fail to write add partition (group) log.", K(ret), K(subcmd), K(log_attr));
    } else if (OB_FAIL(partition->create_partition_group(param))) {
      STORAGE_LOG(WARN, "fail to create partition group", K(ret));
    } else if (FALSE_IT(need_create_memtable = !(partition->get_pg_storage().is_restoring_base_data() ||
                                                 partition->get_pg_storage().is_restoring_standby()))) {
    } else if (need_create_memtable && OB_FAIL(partition->create_memtable(in_slog_trans))) {
      STORAGE_LOG(WARN, "fail to create memtable", K(ret));
      // pause it to make sure the sstable upper trans version is update after migration finished.
    } else if (need_create_memtable && OB_FALSE_IT(partition->get_pg_storage().pause())) {
    } else {
      file_handle.reset();
      // create new partition need set schema_version in pg meta in order to prevent gc
      for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->part_ctx_array_.count(); ++i) {
        const ObMigratePartitionInfo &copy_info = ctx_->part_ctx_array_.at(i).copy_info_;
        const ObPGPartitionStoreMeta &meta = copy_info.meta_;
        if (!meta.pkey_.is_trans_table() && OB_FAIL(create_pg_partition(meta, partition, in_slog_trans))) {
          LOG_WARN("failed to create pg partition", K(ret), K(meta));
        }
      }
    }
  }

  return ret;
}

////////////////////////////////////////////////////////////////////
// ObPartitionMigrator
//
ObPartitionMigrator::ObPartitionMigrator()
    : is_inited_(false),
      is_stop_(false),
      bandwidth_throttle_(NULL),
      partition_service_(NULL),
      svr_rpc_proxy_(NULL),
      pts_rpc_(NULL),
      cp_fty_(NULL),
      location_cache_(NULL),
      lock_(ObLatchIds::MIGRATE_LOCK),
      schema_service_(NULL)
{}

ObPartitionMigrator::~ObPartitionMigrator()
{
  if (is_inited_) {
    destroy();
  }
}

void ObPartitionMigrator::destroy()
{
  stop();
  wait();
}

void ObPartitionMigrator::stop()
{
  common::SpinWLockGuard guard(lock_);
  is_stop_ = true;
}

void ObPartitionMigrator::wait()
{
  STORAGE_LOG(INFO, "wait restore_macro_block_task_pool_ stop");
  restore_macro_block_task_pool_.destroy();

  STORAGE_LOG(INFO, "wait migrate task stop");
  if (is_inited_) {
    bool has_running_task = true;
    while (has_running_task) {
      int64_t migrate_dag_count = ObDagScheduler::get_instance().get_dag_count(ObIDag::DAG_TYPE_MIGRATE) +
                                  ObDagScheduler::get_instance().get_dag_count(ObIDag::DAG_TYPE_BACKUP) +
                                  ObDagScheduler::get_instance().get_dag_count(ObIDag::DAG_TYPE_VALIDATE) +
                                  ObDagScheduler::get_instance().get_dag_count(ObIDag::DAG_TYPE_BACKUP_BACKUPSET) +
                                  ObDagScheduler::get_instance().get_dag_count(ObIDag::DAG_TYPE_BACKUP_ARCHIVELOG);
      has_running_task = migrate_dag_count > 0;

      if (has_running_task) {
        usleep(OB_MIGRATE_SCHEDULE_SLEEP_INTERVAL_S);
      }
    }
  }
  is_inited_ = false;

  STORAGE_LOG(INFO, "ObPartitionMigrator stopped");
}

int ObMigrateTaskSchedulerTask::add_migrate_status(ObMigrateCtx *ctx)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (NULL == ctx) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arg", K(ret));
  } else if (!ctx->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(*ctx));
  } else if (OB_FAIL(add_partition_migration_status(*ctx))) {
    STORAGE_LOG(WARN, "failed to add partition_migration_status", K(ret), K(*ctx));
  } else {
    STORAGE_LOG(INFO, "succeed to add migrate ctx", KP(ctx), K(*ctx));
  }
  return ret;
}

int ObMigratePrepareTask::add_partition_migration_status(const ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(ctx));
  } else {
    ObPartitionMigrationStatus task;
    task.task_id_ = ctx.task_id_;
    task.migrate_type_ = ctx.replica_op_arg_.get_replica_op_type_str();
    task.pkey_ = ctx.replica_op_arg_.key_;
    task.clog_parent_.reset();  // we don't know it now
    task.src_.reset();          // we don't know it now
    task.dest_ = OBSERVER.get_self();
    task.result_ = ctx.result_;
    task.start_time_ = ctx.create_ts_;
    task.action_ = ctx.action_;
    task.replica_state_ = OB_UNKNOWN_REPLICA;  // we don't know it now
    task.doing_task_count_ = 0;
    task.total_task_count_ = 0;
    task.rebuild_count_ = 0;
    task.continue_fail_count_ = 0;
    task.data_statics_ = ctx.data_statics_;

    // allow comment truncation, no need to set ret
    (void)ctx.fill_comment(task.comment_, sizeof(task.comment_));

    if (OB_SUCCESS != (tmp_ret = ObPartitionMigrationStatusMgr::get_instance().add_status(task))) {
      STORAGE_LOG(WARN, "failed to add partition migration status", K(tmp_ret), K(task));
    }
  }

  return ret;
}

int ObMigrateCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  // static function, no need check is_inited_
  int ret = OB_SUCCESS;

  if (NULL == buf || NULL == group_task_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(group_task_), K(buf));
  } else {
    int n = snprintf(buf,
        buf_len,
        "single partition migration: group_task_id=%s key=%s, op_type=%s, src=%s, dest=%s,"
        "dest_memstore_percent=%s",
        to_cstring(group_task_->get_task_id()),
        to_cstring(replica_op_arg_.key_),
        replica_op_arg_.get_replica_op_type_str(),
        to_cstring(replica_op_arg_.data_src_.get_server()),
        to_cstring(replica_op_arg_.dst_.get_server()),
        to_cstring(replica_op_arg_.dst_.get_memstore_percent()));
    if (n < 0 || n >= buf_len) {
      ret = OB_BUF_NOT_ENOUGH;
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {  // 10s
        STORAGE_LOG(WARN, "buf not enough, comment is truncated", K(ret), K(n), K(buf));
      }
    }
  }
  return ret;
}

ObPartitionMigrator &ObPartitionMigrator::get_instance()
{
  static ObPartitionMigrator migrator_instance_;
  return migrator_instance_;
}

int ObPartitionMigrator::init(obrpc::ObPartitionServiceRpcProxy &srv_rpc_proxy, ObPartitionServiceRpc &pts_rpc,
    ObIPartitionComponentFactory *cp_fty, ObPartitionService *partition_service,
    common::ObInOutBandwidthThrottle &bandwidth_throttle, share::ObIPartitionLocationCache *location_cache,
    share::schema::ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObPartitionMigrator has inited.", K(ret));
  } else if (!srv_rpc_proxy.is_inited() || NULL == cp_fty || NULL == partition_service || NULL == location_cache ||
             NULL == schema_service) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid arguments.",
        "proxy valid",
        srv_rpc_proxy.is_inited(),
        KP(cp_fty),
        KP(partition_service),
        KP(location_cache),
        KP(schema_service),
        K(ret));
  } else if (OB_FAIL(restore_macro_block_task_pool_.init("RstMacroTask"))) {
    STORAGE_LOG(WARN, "failed to init restore_macro_block_task_pool_", K(ret));
  } else {
    bandwidth_throttle_ = &bandwidth_throttle;
    partition_service_ = partition_service;
    svr_rpc_proxy_ = &srv_rpc_proxy;
    pts_rpc_ = &pts_rpc;
    cp_fty_ = cp_fty;
    location_cache_ = location_cache;
    schema_service_ = schema_service;
    is_inited_ = true;
  }

  return ret;
}

const char *ObMigrateCtx::trans_action_to_str(const MigrateAction &action)
{
  const char *str = "UNKNOWN";

  switch (action) {
    case INIT:
      str = "INIT";
      break;
    case PREPARE:
      str = "PREPARE";
      break;
    case COPY_MINOR:
      str = "COPY_MINOR";
      break;
    case COPY_MAJOR:
      str = "COPY_MAJOR";
      break;
    case END:
      str = "END";
      break;
    default:
      str = "UNKOWN";
      STORAGE_LOG(WARN, "unknown action", K(action));
  }

  return str;
}

ObPartGroupMigrator::DoingTaskStat::DoingTaskStat()
    : rebuild_count_(0),
      backup_count_(0),
      validate_count_(0),
      backup_backupset_count_(0),
      backup_archivelog_count_(0),
      normal_migrate_count_(0)
{}

void ObPartGroupMigrator::DoingTaskStat::reset()
{
  rebuild_count_ = 0;
  backup_count_ = 0;
  validate_count_ = 0;
  backup_backupset_count_ = 0;
  backup_archivelog_count_ = 0;
  normal_migrate_count_ = 0;
}

ObPartGroupMigrator::ObPartGroupMigrator()
    : is_inited_(false),
      update_task_list_lock_(ObLatchIds::GROUP_MIGRATE_LOCK),
      task_list_(),
      partition_service_(NULL),
      cp_fty_(NULL),
      is_stop_(false)
{}

ObPartGroupMigrator::~ObPartGroupMigrator()
{
  if (is_inited_) {
    destroy();
  }
}

void ObPartGroupMigrator::destroy()
{
  stop();
  wait();
}

void ObPartGroupMigrator::stop()
{
  common::SpinWLockGuard guard(update_task_list_lock_);
  is_stop_ = true;
}

void ObPartGroupMigrator::wait()
{
  bool need_wait = true;
  while (need_wait) {
    common::SpinRLockGuard guard(update_task_list_lock_);
    if (task_list_.count() > 0) {
      STORAGE_LOG(INFO, "not finish part group task, waiting", "total_count", task_list_.count());
      usleep(1000 * 1000);  // sleep 1s
    } else {
      need_wait = false;
    }
  }
  is_inited_ = false;
}

bool ObPartGroupMigrator::is_stop() const
{
  return is_stop_;
}

ObPartGroupMigrator &ObPartGroupMigrator::get_instance()
{
  static ObPartGroupMigrator group_migrator;
  return group_migrator;
}

int ObPartGroupMigrator::init(storage::ObPartitionService *partition_service, ObIPartitionComponentFactory *cp_fty)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(ERROR, "cannot init twice", K(ret));
  } else if (NULL == partition_service || NULL == cp_fty) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(partition_service), KP(cp_fty));
  } else {
    is_inited_ = true;
    partition_service_ = partition_service;
    cp_fty_ = cp_fty;
  }
  return ret;
}

int ObPartGroupMigrator::schedule(const ObReplicaOpArg &arg, const share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  // task will be executed directly when it is scheduled and will be release when it execute over,
  // so this task point address will not keep effective
  ObPartGroupTask *group_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(task_id), K(arg));
  } else {
    ObSEArray<ObReplicaOpArg, 1> task_list;
    const bool is_batch_mode = false;
    const bool is_normal_migrate = true;

    if (OB_FAIL(task_list.push_back(arg))) {
      STORAGE_LOG(WARN, "failed to add task list", K(ret), K(arg));
    } else if (OB_FAIL(inner_schedule(task_list, is_batch_mode, task_id, is_normal_migrate, group_task))) {
      STORAGE_LOG(WARN, "failed to inner schedule task", K(ret), K(arg), K(task_id));
    }
  }
  return ret;
}

int ObPartGroupMigrator::schedule(const ObIArray<ObReplicaOpArg> &arg_list, const share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  const bool is_batch_mode = true;
  // task will be executed directly when it is scheduled and will be release when it execute over,
  // so this task point address will not keep effective
  ObPartGroupTask *group_task = NULL;
  bool is_normal_migrate = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (arg_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(task_id), K(arg_list));
  } else if (OB_FAIL(inner_schedule(arg_list, is_batch_mode, task_id, is_normal_migrate, group_task))) {
    STORAGE_LOG(WARN, "failed to inner schedule task", K(ret), K(arg_list), K(task_id));
  }
  return ret;
}

int ObPartGroupMigrator::mark(const ObReplicaOpArg &arg, const share::ObTaskId &task_id, ObPartGroupTask *&group_task)
{
  // only remove replica task finsih is control outside, so allo outside call group_task to finish
  int ret = OB_SUCCESS;
  group_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!arg.is_valid() || task_id.is_invalid() || REMOVE_REPLICA_OP != arg.type_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(task_id), K(arg));
  } else {
    SMART_VAR(ObArray<ObReplicaOpArg>, task_list)
    {
      const bool is_batch_mode = false;
      const bool is_normal_migrate = false;

      if (OB_FAIL(task_list.push_back(arg))) {
        STORAGE_LOG(WARN, "failed to add task list", K(ret), K(arg));
      } else if (OB_FAIL(inner_schedule(task_list, is_batch_mode, task_id, is_normal_migrate, group_task))) {
        STORAGE_LOG(WARN, "failed to inner schedule task", K(ret), K(arg), K(task_id));
      }

      DEBUG_SYNC(AFTER_MIGRATION_MARK);
    }
  }
  return ret;
}

int ObPartGroupMigrator::get_not_finish_task_count(DoingTaskStat &stat)
{  // caller must hold lock
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
      if (!task_list_[i]->is_finish() && REMOVE_REPLICA_OP != task_list_[i]->get_type()) {
        if (REBUILD_REPLICA_OP == task_list_[i]->get_type()) {
          ++stat.rebuild_count_;
        } else if (BACKUP_REPLICA_OP == task_list_[i]->get_type()) {
          ++stat.backup_count_;
        } else if (BACKUP_BACKUPSET_OP == task_list_[i]->get_type()) {
          ++stat.backup_backupset_count_;
        } else if (BACKUP_ARCHIVELOG_OP == task_list_[i]->get_type()) {
          ++stat.backup_archivelog_count_;
        } else if (VALIDATE_BACKUP_OP == task_list_[i]->get_type()) {
          ++stat.validate_count_;
        } else {
          ++stat.normal_migrate_count_;
        }
        STORAGE_LOG(DEBUG, "dump task list", K(i), K(*task_list_[i]));
      }
    }
  }

  return ret;
}

static inline bool task_need_data_copy(const ObIArray<ObReplicaOpArg> &task_list)
{
  bool bool_ret = false;
  for (int64_t i = 0; i < task_list.count() && !bool_ret; ++i) {
    const ObReplicaOpArg &task = task_list.at(i);
    if (REMOVE_REPLICA_OP != task.type_) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int ObPartGroupMigrator::has_task(const ObPartitionKey &pkey, bool &has_task)
{
  int ret = OB_SUCCESS;
  ObArray<ObPartitionKey> pkey_list;
  common::SpinRLockGuard guard(update_task_list_lock_);

  has_task = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(pkey_list.push_back(pkey))) {
    LOG_WARN("failed to add pkey", K(ret), K(pkey));
  } else if (OB_FAIL(check_dup_task(pkey_list))) {
    if (OB_TASK_EXIST != ret) {
      LOG_WARN("failed to check dup task", K(ret), K(pkey));
    } else {
      ret = OB_SUCCESS;
      has_task = true;
    }
  }
  return ret;
}

int ObPartGroupMigrator::task_exist(const share::ObTaskId &task_id, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;

  common::SpinRLockGuard guard(update_task_list_lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count() && !exist; ++i) {
    if (NULL != task_list_[i] && task_id.equals(task_list_[i]->get_task_id())) {
      exist = true;
    }
  }
  return ret;
}

int ObPartGroupMigrator::check_copy_limit_(const ObIArray<ObReplicaOpArg> &arg_list)
{
  int ret = OB_SUCCESS;
  int64_t data_copy_in_limit = GCONF.server_data_copy_in_concurrency;
  DoingTaskStat stat;
  ObReplicaOpType type = UNKNOWN_REPLICA_OP;

  if (GCONF.migrate_concurrency == 0) {
    data_copy_in_limit = 0;
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (arg_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(arg_list));
  } else if (FALSE_IT(type = arg_list.at(0).type_)) {
  } else if (OB_FAIL(get_not_finish_task_count(stat))) {
    STORAGE_LOG(WARN, "failed to get_not_finish_task_count", K(ret));
  } else if (!task_need_data_copy(arg_list)) {
    // pass
  } else if (REBUILD_REPLICA_OP == type) {
    if (stat.rebuild_count_ >= data_copy_in_limit) {
      ret = OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT;
      LOG_WARN("rebuild count exceed the limit", K(ret), K(stat), K(data_copy_in_limit));
    }
  } else if (BACKUP_REPLICA_OP == type) {
    data_copy_in_limit = OB_GROUP_BACKUP_CONCURRENCY;
    if (stat.backup_count_ >= data_copy_in_limit) {
      ret = OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT;
      LOG_WARN("backup count exceed the limit", K(ret), K(stat), K(data_copy_in_limit));
    }
  } else if (VALIDATE_BACKUP_OP == type) {
    data_copy_in_limit = OB_GROUP_VALIDATE_CONCURRENCY;
    if (stat.validate_count_ >= data_copy_in_limit) {
      ret = OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT;
      LOG_WARN("validate count exceed the limit", K(ret), K(stat), K(data_copy_in_limit));
    }
  } else if (BACKUP_BACKUPSET_OP == type) {
    data_copy_in_limit = OB_GROUP_BACKUP_CONCURRENCY;
    if (stat.backup_backupset_count_ >= data_copy_in_limit) {
      ret = OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT;
      LOG_WARN(
          "backup backupset count exceed the limit", K(ret), K(stat.backup_backupset_count_), K(data_copy_in_limit));
    }
  } else if (BACKUP_ARCHIVELOG_OP == type) {
    data_copy_in_limit = OB_GROUP_BACKUP_CONCURRENCY;
    if (stat.backup_archivelog_count_ >= data_copy_in_limit) {
      ret = OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT;
      LOG_WARN(
          "backup backupset count exceed the limit", K(ret), K(stat.backup_archivelog_count_), K(data_copy_in_limit));
    }
  } else {
    if (stat.normal_migrate_count_ >= data_copy_in_limit) {
      ret = OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT;
      LOG_WARN("migrator count exceed the limit", K(ret), K(stat), K(data_copy_in_limit));
    }
  }
  return ret;
}

int ObPartGroupMigrator::inner_schedule(const ObIArray<ObReplicaOpArg> &arg_list, const bool is_batch_mode,
    const share::ObTaskId &in_task_id, const bool is_normal_migrate, ObPartGroupTask *&out_group_task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPartGroupTask *group_task = NULL;
  share::ObTaskId task_id = in_task_id;  // copy it
  DEBUG_SYNC(BEFORE_OBSERVER_SCHEDULE_MIGRATE);

  common::SpinWLockGuard guard(update_task_list_lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (is_stop()) {
    ret = OB_SERVER_IS_STOPPING;
    STORAGE_LOG(WARN, "observer is stopping", K(ret));
  } else if (OB_FAIL(check_arg_list(arg_list))) {
    STORAGE_LOG(WARN, "arg_list is empty", K(ret), K(arg_list));
  } else if (OB_FAIL(check_copy_limit_(arg_list))) {
    LOG_WARN("failed to check_copy_limit", K(ret));
  } else if (OB_FAIL(get_group_task(arg_list, is_batch_mode, task_id, group_task))) {
    STORAGE_LOG(WARN, "failed to get group task", K(ret), K(arg_list));
  } else if (OB_FAIL(check_dup_task(*group_task))) {
    STORAGE_LOG(WARN, "failed to check new task", K(ret), K(group_task));
  } else if (OB_FAIL(task_list_.push_back(group_task))) {
    STORAGE_LOG(WARN, "failed to add task", K(ret), K(group_task));
  } else if (is_normal_migrate && OB_FAIL(schedule_group_migrate_dag(group_task))) {
    LOG_WARN("failed to schedule_group_migrate_dag", K(ret));
    if (OB_SUCCESS != (tmp_ret = task_list_.remove(task_list_.count() - 1))) {
      LOG_ERROR("failed to rollback failed group task, fatal error", K(ret));
      ob_abort();
    }
  } else {
    STORAGE_LOG(INFO,
        "succeed to schedule partition group migration task",
        "task_count",
        task_list_.count(),
        KP(group_task),
        K(*group_task));
    out_group_task = group_task;
    group_task = NULL;
  }

  if (NULL != group_task && NULL != cp_fty_) {
    cp_fty_->free(group_task);
  }
  return ret;
}

// static func
int ObPartGroupMigrator::check_arg_list(const ObIArray<ObReplicaOpArg> &arg_list)
{
  int ret = OB_SUCCESS;
  ObReplicaOpType type = UNKNOWN_REPLICA_OP;
  const int64_t restore_concurrency = GCONF.restore_concurrency;

  if (arg_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "arg list must not empty", K(ret), K(arg_list));
  } else {
    type = arg_list.at(0).type_;
    if (!is_replica_op_valid(type)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid type", K(ret), K(type), K(arg_list));
    } else if (RESTORE_REPLICA_OP == type && restore_concurrency <= 0) {
      ret = OB_OP_NOT_ALLOW;
      STORAGE_LOG(WARN, "restore is disabled, cannot restore tenant", K(ret), K(restore_concurrency));
    }
    for (int64_t i = 1; OB_SUCC(ret) && i < arg_list.count(); ++i) {
      if (type != arg_list.at(i).type_) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(ERROR, "op type must be same", K(ret), K(type), K(i), K(arg_list));
      }
    }
  }
  return ret;
}

int ObPartGroupMigrator::check_dup_task(const ObPartGroupTask &task)
{
  // caller must hold update_task_list_lock_
  int ret = OB_SUCCESS;
  ObArray<ObPartitionKey> pkey_list;
  ObReplicaOpType type = task.get_type();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(task.check_self_dup_task())) {
    STORAGE_LOG(WARN, "failed to check_self_dup_task", K(ret));
  } else if (VALIDATE_BACKUP_OP == type || BACKUP_BACKUPSET_OP == type || BACKUP_ARCHIVELOG_OP == type) {
    // skip
  } else if (OB_FAIL(task.get_pkey_list(pkey_list))) {
    STORAGE_LOG(WARN, "failed to get pkey list", K(ret));
  } else if (OB_FAIL(check_dup_task(pkey_list))) {
    LOG_WARN("failed to check dup task", K(ret));
  }
  return ret;
}

int ObPartGroupMigrator::check_dup_task(const ObIArray<ObPartitionKey> &pkey_list)
{
  // caller must hold update_task_list_lock_
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
    if (OB_FAIL(task_list_[i]->check_dup_task(pkey_list))) {
      STORAGE_LOG(WARN, "failed to check_dup_task in task_list", K(ret), K(i));
    }
  }

  return ret;
}

int ObPartGroupMigrator::schedule_group_migrate_dag(ObPartGroupTask *&group_task)
{
  int ret = OB_SUCCESS;
  ObGroupMigrateDag *dag = NULL;
  ObGroupMigrateExecuteTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(group_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("group_task must not null", K(ret), KP(group_task));
  } else if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(dag))) {
    LOG_WARN("failed to alloc group migrate dag", K(ret));
  } else if (OB_FAIL(dag->init(group_task, partition_service_))) {
    LOG_WARN("failed to init dag", K(ret));
  } else if (OB_FAIL(dag->alloc_task(task))) {
    LOG_WARN("failed to alloc task, ", K(ret));
  } else if (OB_FAIL(task->init(group_task))) {
    LOG_WARN("failed to init task", K(ret));
  } else if (OB_FAIL(dag->add_task(*task))) {
    LOG_WARN("Failed to add task", K(ret));
  } else if (OB_FAIL(ObDagScheduler::get_instance().add_dag(dag))) {
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add group migrate dag", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != dag) {
      dag->clear();
      ObDagScheduler::get_instance().free_dag(*dag);
    }
  }

  return ret;
}

int ObPartGroupMigrator::remove_finish_task(ObPartGroupTask *group_task)
{
  int ret = OB_SUCCESS;
  bool found = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(group_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("group_task must not null", K(ret), KP(group_task));
  } else {
    {
      common::SpinWLockGuard guard(update_task_list_lock_);
      for (int64_t i = task_list_.count() - 1; OB_SUCC(ret) && !found && i >= 0; --i) {
        ObPartGroupTask *task = task_list_[i];
        ;
        if (task == group_task) {
          found = true;
          if (OB_FAIL(task_list_.remove(i))) {
            STORAGE_LOG(ERROR, "failed to remove finished part group migration task", K(ret), K(i), K(task_list_));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!found) {
      ret = OB_ERR_SYS;
      LOG_ERROR("cannot find need remove finish task", K(ret), KP(group_task), K(*group_task));
    } else {
      STORAGE_LOG(
          INFO, "remove finished part group migration task", K(task_list_.count()), KP(group_task), K(*group_task));
      cp_fty_->free(group_task);
    }
  }
  return ret;
}

void ObPartGroupMigrator::wakeup()
{
  common::SpinRLockGuard guard(update_task_list_lock_);
  for (int64_t i = 0; i < task_list_.count(); ++i) {
    ObPartGroupTask *task = task_list_[i];
    if (NULL != task) {
      task->wakeup();
    }
  }
}

int ObPartGroupMigrator::get_group_task(const ObIArray<ObReplicaOpArg> &arg_list, const bool is_batch_mode,
    const share::ObTaskId &in_task_id, ObPartGroupTask *&group_task)
{
  int ret = OB_SUCCESS;
  group_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (arg_list.empty()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "replica op arg list should not be empty", K(ret), K(in_task_id), K(arg_list));
  } else {
    const ObReplicaOpType type = arg_list.at(0).type_;
    if (BACKUP_REPLICA_OP == type) {
      ObPartGroupBackupTask *backup_group_task = NULL;
      if (NULL == (backup_group_task = cp_fty_->get_part_group_backup_task())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc group task", K(ret));
      } else if (FALSE_IT(group_task = backup_group_task)) {
      } else if (OB_FAIL(backup_group_task->init(arg_list, is_batch_mode, partition_service_, in_task_id))) {
        STORAGE_LOG(WARN, "failed to init group task", K(ret));
      }
    } else {
      ObPartGroupMigrationTask *migration_group_task = NULL;
      if (NULL == (migration_group_task = cp_fty_->get_part_group_migration_task())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc group task", K(ret));
      } else if (FALSE_IT(group_task = migration_group_task)) {
      } else if (OB_FAIL(migration_group_task->init(arg_list, is_batch_mode, partition_service_, in_task_id))) {
        STORAGE_LOG(WARN, "failed to init group task", K(ret));
      }
    }
  }
  return ret;
}

ObPartGroupTask::ObPartGroupTask()
    : is_inited_(false),
      is_batch_mode_(true),
      lock_(ObLatchIds::GROUP_MIGRATE_TASK_LOCK),
      task_list_(),
      partition_service_(NULL),
      first_error_code_(OB_SUCCESS),
      type_(UNKNOWN_REPLICA_OP),
      task_id_(),
      is_finished_(false),
      retry_job_start_ts_(0),
      tenant_id_(0),
      cond_(),
      need_idle_(true),
      report_list_(),
      change_member_option_(NORMAL_CHANGE_MEMBER_LIST)
{}

ObPartGroupTask::~ObPartGroupTask()
{}

bool ObPartGroupTask::is_finish() const
{
  common::SpinRLockGuard guard(lock_);
  return is_finished_;
}

int ObPartGroupTask::get_pkey_list(ObIArray<ObPartitionKey> &pkey_list) const
{
  int ret = OB_SUCCESS;

  common::SpinRLockGuard guard(lock_);
  pkey_list.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
      if (OB_FAIL(pkey_list.push_back(task_list_.at(i).arg_.key_))) {
        STORAGE_LOG(WARN, "failed to add pkey", K(ret));
      }
    }
  }
  return ret;
}

int ObPartGroupTask::check_dup_task(const ObIArray<ObPartitionKey> &pkey_list) const
{
  // Check duplicate task just use pkey, so it do not has concurrency. No need lock
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (ObReplicaOpType::VALIDATE_BACKUP_OP == get_type() || ObReplicaOpType::BACKUP_BACKUPSET_OP == get_type() ||
             ObReplicaOpType::BACKUP_ARCHIVELOG_OP == get_type()) {
    // skip
  } else {
    for (int64_t task_idx = 0; OB_SUCC(ret) && task_idx < task_list_.count(); ++task_idx) {
      for (int64_t pkey_idx = 0; OB_SUCC(ret) && pkey_idx < pkey_list.count(); ++pkey_idx) {
        if (task_list_[task_idx].arg_.key_ == pkey_list.at(pkey_idx)) {
          ret = OB_TASK_EXIST;
          ObTaskController::get().allow_next_syslog();
          STORAGE_LOG(WARN,
              "migration task is dup",
              K(ret),
              K_(task_id),
              K_(type),
              "pkey",
              pkey_list.at(pkey_idx),
              K(task_list_[task_idx]));
        }
      }
    }
  }

  return ret;
}

int ObPartGroupTask::check_self_dup_task() const
{
  int ret = OB_SUCCESS;
  // Check duplicate task just use pkey, so it do not has concurrency. No need lock

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
      for (int64_t j = i + 1; OB_SUCC(ret) && j < task_list_.count(); ++j) {
        if (task_list_[i].arg_.type_ == VALIDATE_BACKUP_OP && task_list_[j].arg_.type_ == VALIDATE_BACKUP_OP) {
          if (task_list_[i].arg_.validate_arg_.pg_key_ == task_list_[j].arg_.validate_arg_.pg_key_ &&
              task_list_[i].arg_.validate_arg_.backup_set_id_ == task_list_[j].arg_.validate_arg_.backup_set_id_) {
            ret = OB_INVALID_ARGUMENT;
            STORAGE_LOG(WARN, "task is dup", K(ret), K(i), K(j), K(task_list_[i]), K(task_list_[j]));
          }
        } else if (task_list_[i].arg_.type_ == BACKUP_BACKUPSET_OP && task_list_[j].arg_.type_ == BACKUP_BACKUPSET_OP) {
          if (task_list_[i].arg_.backup_backupset_arg_.pg_key_ == task_list_[j].arg_.backup_backupset_arg_.pg_key_ &&
              task_list_[i].arg_.backup_backupset_arg_.backup_set_id_ ==
                  task_list_[j].arg_.backup_backupset_arg_.backup_set_id_) {
            ret = OB_INVALID_ARGUMENT;
            STORAGE_LOG(WARN,
                "task is dup",
                K(ret),
                K(i),
                K(j),
                K(task_list_[i].arg_.backup_backupset_arg_),
                K(task_list_[j].arg_.backup_backupset_arg_));
          }
        } else if (task_list_[i].arg_.type_ == BACKUP_ARCHIVELOG_OP &&
                   task_list_[j].arg_.type_ == BACKUP_ARCHIVELOG_OP) {
          if (task_list_[i].arg_.backup_archive_log_arg_.pg_key_ ==
                  task_list_[j].arg_.backup_archive_log_arg_.pg_key_ &&
              task_list_[i].arg_.backup_archive_log_arg_.piece_id_ ==
                  task_list_[j].arg_.backup_archive_log_arg_.piece_id_) {
            ret = OB_INVALID_ARGUMENT;
            STORAGE_LOG(WARN,
                "task is dup",
                K(ret),
                K(i),
                K(j),
                K(task_list_[i].arg_.backup_archive_log_arg_),
                K(task_list_[j].arg_.backup_archive_log_arg_));
          }
        } else if (task_list_[i].arg_.key_ == task_list_[j].arg_.key_) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "task is dup", K(ret), K(i), K(j), K(task_list_[i]), K(task_list_[j]));
        }
      }
    }
  }
  return ret;
}

int ObPartGroupTask::set_part_task_start(const ObPartitionKey &pkey, const int64_t backup_set_id)
{
  int ret = OB_SUCCESS;
  ObPartMigrationTask *task = NULL;

  {
    common::SpinWLockGuard guard(lock_);

    if (!is_inited_) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "not inited", K(ret));
    } else {
      for (int64_t i = 0; NULL == task && i < task_list_.count(); ++i) {
        if (VALIDATE_BACKUP_OP == get_type()) {
          if (task_list_[i].arg_.validate_arg_.pg_key_ == pkey &&
              task_list_[i].arg_.validate_arg_.backup_set_id_ == backup_set_id) {
            task = &task_list_[i];
          }
        } else if (BACKUP_BACKUPSET_OP == get_type()) {
          if (task_list_[i].arg_.backup_backupset_arg_.pg_key_ == pkey &&
              task_list_[i].arg_.backup_backupset_arg_.backup_set_id_ == backup_set_id) {
            task = &task_list_[i];
          }
        } else {
          if (task_list_[i].arg_.key_ == pkey) {
            task = &task_list_[i];
          }
        }
      }
      if (NULL == task) {
        ret = OB_ENTRY_NOT_EXIST;
        STORAGE_LOG(ERROR, "cannot found pkey in task", K(ret), K(pkey), KP(this), K(*this));
      } else if (ObPartMigrationTask::INIT == task->status_) {
        task->status_ = ObPartMigrationTask::DOING;
        STORAGE_LOG(INFO, "set_part_task_start", K(pkey), K(ret), K(*this));
        dump_task_status();
      }
    }
  }
  return ret;
}

int ObPartGroupTask::set_part_task_finish(const ObPartitionKey &pkey, const int32_t result,
    const share::ObTaskId &part_task_id, const bool during_migrating, const int64_t backup_set_id)
{
  int ret = OB_SUCCESS;
  ObPartMigrationTask *task = NULL;

  {
    common::SpinWLockGuard guard(lock_);

    if (!is_inited_) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "not inited", K(ret));
    } else {
      for (int64_t i = 0; NULL == task && i < task_list_.count(); ++i) {
        if (VALIDATE_BACKUP_OP == get_type()) {
          if (task_list_[i].arg_.validate_arg_.pg_key_ == pkey &&
              task_list_[i].arg_.validate_arg_.backup_set_id_ == backup_set_id) {
            task = &task_list_[i];
          }
        } else if (BACKUP_BACKUPSET_OP == get_type()) {
          if (task_list_[i].arg_.backup_backupset_arg_.pg_key_ == pkey &&
              task_list_[i].arg_.backup_backupset_arg_.backup_set_id_ == backup_set_id) {
            task = &task_list_[i];
          }
        } else {
          if (task_list_[i].arg_.key_ == pkey) {
            task = &task_list_[i];
          }
        }
      }
      if (NULL == task) {
        ret = OB_ENTRY_NOT_EXIST;
        STORAGE_LOG(ERROR, "cannot found pkey in task", K(ret), K(pkey), K(result), KP(this), K(*this));
      } else {
        task->status_ = ObPartMigrationTask::FINISH;
        task->result_ = result;
        task->during_migrating_ = during_migrating;
      }
    }

    if (OB_SUCCESS == first_error_code_) {
      if (OB_SUCCESS != result) {
        if (BACKUP_REPLICA_OP == get_type() && OB_PG_IS_REMOVED == result) {
          // backup while partition may migrate out, skip set first error code
          // do nothing
        } else {
          first_error_code_ = result;
        }
        STORAGE_LOG(WARN, "record first_error_code_ with partition task", K(result), K(pkey), K(ret));
      } else if (OB_SUCCESS != ret) {
        first_error_code_ = ret;
        STORAGE_LOG(WARN, "record first_error_code_", K(result), K(pkey), K(ret));
      }
    }
    STORAGE_LOG(INFO,
        "set_part_task_finish",
        K(pkey),
        K(result),
        K(during_migrating),
        K(ret),
        K(part_task_id),
        K(*this),
        K(backup_set_id));
    dump_task_status();
  }
  ObPartGroupMigrator::get_instance().wakeup();

  return ret;
}

int ObPartGroupTask::build_migrate_ctx(const ObReplicaOpArg &arg, ObMigrateCtx &migrate_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartGroupMigrationTask has not been inited", K(ret));
  } else {
    migrate_ctx.replica_op_arg_ = arg;
    migrate_ctx.action_ = ObMigrateCtx::INIT;
    migrate_ctx.result_ = OB_SUCCESS;
    migrate_ctx.doing_task_cnt_ = 1;
    migrate_ctx.total_task_cnt_ = 1;
    migrate_ctx.need_rebuild_ = false;
    if (0 == migrate_ctx.create_ts_) {
      migrate_ctx.create_ts_ = ObTimeUtility::current_time();
    }
    migrate_ctx.task_id_.reset();  // filled in add_migrate_ctx_without_lock
    migrate_ctx.continue_fail_count_ = 0;
    migrate_ctx.rebuild_count_ = migrate_ctx.rebuild_count_ < 0 ? 0 : migrate_ctx.rebuild_count_;
    migrate_ctx.group_task_ = this;
    migrate_ctx.group_task_id_ = get_task_id();
    migrate_ctx.part_ctx_array_.reset();
    migrate_ctx.curr_major_block_array_index_ = (migrate_ctx.curr_major_block_array_index_ + 1) % 2;
    migrate_ctx.major_block_id_array_ptr_ =
        &migrate_ctx.major_block_id_array_[migrate_ctx.curr_major_block_array_index_];
    migrate_ctx.is_copy_cover_minor_ = false;

    if (migrate_ctx.is_only_copy_sstable()) {
      // do nothing
    } else if (OB_FAIL(migrate_ctx.recovery_point_ctx_.init(migrate_ctx))) {
      STORAGE_LOG(WARN, "failed to init reocovery point ctx", K(ret), K(arg));
    }
  }
  return ret;
}

void ObPartGroupTask::dump_task_status()
{
  // caller must hold lock
  int tmp_ret = OB_SUCCESS;
  int64_t count[ObPartMigrationTask::MAX];
  memset(count, 0, sizeof(count));

  if (!is_inited_) {
    tmp_ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not inited", K(tmp_ret));
  } else {
    for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < task_list_.count(); ++i) {
      ObPartMigrationTask &task = task_list_[i];
      if (task.status_ < 0 || task.status_ >= ObPartMigrationTask::MAX) {
        tmp_ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "invalid task stauts", K(tmp_ret), K(i), K(task));
      } else {
        count[task.status_]++;
      }
    }

    STORAGE_LOG(INFO,
        "dump group migration task status",
        "total_count",
        task_list_.count(),
        "INIT",
        count[ObPartMigrationTask::INIT],
        "DOING",
        count[ObPartMigrationTask::DOING],
        "FINISH",
        count[ObPartMigrationTask::FINISH]);
  }
}

int ObPartGroupTask::check_is_task_cancel()
{
  int ret = OB_SUCCESS;
  bool is_cancel = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not inited", K(ret));
  } else if (OB_SUCCESS == first_error_code_) {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(SYS_TASK_STATUS_MGR.is_task_cancel(task_id_, is_cancel))) {
      STORAGE_LOG(ERROR, "failed to check is group task cancel", K(ret), K(*this));
    } else if (is_cancel) {
      first_error_code_ = OB_CANCELED;
      STORAGE_LOG(WARN, "group part migration is cancelled", K(ret), K(*this));
    } else if (ObPartitionMigrator::get_instance().is_stop()) {
      first_error_code_ = OB_CANCELED;
      STORAGE_LOG(WARN, "mirgator is stop, set group task is cancelled", K(ret), K(*this));
    } else if (ObReplicaOpType::VALIDATE_BACKUP_OP == get_type() ||
               ObReplicaOpType::BACKUP_BACKUPSET_OP == get_type() ||
               ObReplicaOpType::BACKUP_ARCHIVELOG_OP == get_type()) {
      // do nothing
    } else if (!GCTX.omt_->has_tenant(tenant_id_)) {
      first_error_code_ = OB_TENANT_NOT_EXIST;
      STORAGE_LOG(WARN, "tenant not exists, set migration failed", K(ret), K(*this));
    }
  }

  if (OB_SUCC(ret) && OB_SUCCESS == first_error_code_) {
    // TODO(jiage): remove it after fix GCTX.omt_->has_tenant
    bool is_tenant_exist = false;
    if (ObReplicaOpType::VALIDATE_BACKUP_OP == get_type() || ObReplicaOpType::BACKUP_BACKUPSET_OP == get_type() ||
        ObReplicaOpType::BACKUP_ARCHIVELOG_OP == get_type()) {
      // do nothing
    } else if (OB_FAIL(check_is_tenant_exist(is_tenant_exist))) {
      LOG_WARN("failed to check is tenant exist", K(ret));
    } else if (!is_tenant_exist) {
      common::SpinWLockGuard guard(lock_);
      first_error_code_ = OB_TENANT_NOT_EXIST;
      STORAGE_LOG(WARN, "tenant schema not exists, set migration failed", K(ret), K(*this));
    }
  }
  return ret;
}

int ObPartGroupTask::check_partition_checksum(const common::ObPartitionKey &pkey)
{
  int ret = OB_SUCCESS;
  ModulePageAllocator allocator(ObNewModIds::OB_PARTITION_MIGRATE);
  share::ObPartitionInfo partition_info;
  partition_info.set_allocator(&allocator);
  share::ObUnitedPtOperator united_operator;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The migrator has not inited or stopped.", K(ret));
  } else if (OB_ISNULL(GCTX.pt_operator_) || OB_ISNULL(GCTX.remote_pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pt_operator must not NULL.", K(ret));
  } else if (OB_FAIL(united_operator.init(GCTX.pt_operator_, GCTX.remote_pt_operator_))) {
    LOG_WARN("fail to init united_operator", KR(ret));
  } else if (OB_SUCCESS !=
             (ret = united_operator.united_get(pkey.get_table_id(), pkey.get_partition_id(), partition_info))) {
    STORAGE_LOG(WARN, "fail to get partition info.", K(ret), K(pkey));
  } else if (partition_info.replica_count() > 1) {
    const common::ObIArray<share::ObPartitionReplica> &replicas = partition_info.get_replicas_v2();
    int64_t lastIdx = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < replicas.count(); ++i) {
      if (replicas.at(i).replica_type_ == REPLICA_TYPE_LOGONLY || replicas.at(i).is_restore_ != 0 ||
          (lastIdx >= 0 && replicas.at(lastIdx).is_restore_ != 0)) {
        continue;
      } else if (lastIdx == -1) {
        lastIdx = i;
      } else if (replicas.at(i).data_version_ > replicas.at(lastIdx).data_version_) {
        lastIdx = i;
      } else if (replicas.at(i).data_version_ == replicas.at(lastIdx).data_version_) {
        if (replicas.at(i).data_checksum_ != replicas.at(lastIdx).data_checksum_) {
          ret = OB_CHECKSUM_ERROR;
          STORAGE_LOG(ERROR,
              "replicas check sum is not match",
              K(pkey),
              "replica1",
              replicas.at(i),
              "replica2",
              replicas.at(lastIdx));
        }
      }
    }
  }

  if (OB_SUCCESS != ret && OB_CHECKSUM_ERROR != ret) {
    STORAGE_LOG(WARN, "failed to check checksum for partition, overwrite ret", K(pkey), K(ret));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObPartGroupTask::check_is_tenant_exist(bool &is_exist)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  share::schema::ObMultiVersionSchemaService &schema_service =
      share::schema::ObMultiVersionSchemaService::get_instance();

  is_exist = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not inited", K(ret));
  } else if (OB_FAIL(schema_service.get_tenant_full_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.check_tenant_exist(tenant_id_, is_exist))) {
    LOG_WARN("failed to check tenant exist", K(ret), K_(tenant_id));
  }
  return ret;
}

int ObPartGroupTask::check_is_partition_exist(const common::ObPartitionKey &pkey, bool &is_exist)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  share::schema::ObMultiVersionSchemaService &schema_service =
      share::schema::ObMultiVersionSchemaService::get_instance();
  const uint64_t fetch_tenant_id = is_inner_table(pkey.get_table_id()) ? OB_SYS_TENANT_ID : pkey.get_tenant_id();
  bool check_dropped_partition = true;
  is_exist = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not inited", K(ret));
  } else if (OB_FAIL(schema_service.get_tenant_full_schema_guard(fetch_tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.check_partition_exist(
                 pkey.get_table_id(), pkey.get_partition_id(), check_dropped_partition, is_exist))) {
    LOG_WARN("failed to check partition exist", K(ret), K(pkey));
  }

  return ret;
}

int ObPartGroupTask::check_can_as_data_source(
    const ObReplicaOpType &op, const ObReplicaType &src_type, const ObReplicaType &dst_type, bool &as_data_source)
{
  int ret = OB_SUCCESS;
  as_data_source = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartGroupMigrationTask has not been inited", K(ret));
  } else {
    if (COPY_GLOBAL_INDEX_OP == op || COPY_LOCAL_INDEX_OP == op) {
      as_data_source = ObReplicaTypeCheck::is_replica_with_ssstore(src_type) &&
                       ObReplicaTypeCheck::is_replica_with_ssstore(dst_type);
    } else {
      as_data_source = ObReplicaTypeCheck::can_as_data_source(dst_type, src_type);
    }
  }
  return ret;
}

int ObPartGroupTask::build_failed_report_list(
    const int32_t first_fail_code, ObIArray<ObReportPartMigrationTask> &report_list)
{
  // static func
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == first_fail_code) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(first_fail_code));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
      report_list.at(i).result_ = first_fail_code;
    }
  }
  return ret;
}

int ObPartGroupTask::set_report_list_result(
    ObIArray<ObReportPartMigrationTask> &report_list, const ObPartitionKey &pkey, const int32_t result)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_SUCCESS == result) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "only failed result need set", K(ret), K(pkey), K(result));
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < report_list.count(); ++i) {
      if (report_list.at(i).arg_.key_ == pkey) {
        report_list.at(i).result_ = result;
        STORAGE_LOG(INFO, "set report list result", K(i), K(pkey), K(result));
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObPartGroupTask::fill_report_list(bool &is_batch_mode, ObIArray<ObReportPartMigrationTask> &report_list)
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard guard(lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!is_finished_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("part group migration task not finished, cannot fill report list", K(ret), K(*this));
  } else if (OB_FAIL(report_list.assign(report_list_))) {
    LOG_WARN("failed to assign report list", K(ret));
  } else {
    is_batch_mode = is_batch_mode_;
  }
  return ret;
}

bool ObPartGroupTask::has_error() const
{
  return OB_SUCCESS != first_error_code_;
}

void ObPartGroupTask::wakeup()
{
  ObThreadCondGuard guard(cond_);
  need_idle_ = false;
  cond_.broadcast();
}

ObPartGroupMigrationTask::ObPartGroupMigrationTask()
    : ObPartGroupTask(),
      schedule_ts_(0),
      start_change_member_ts_(0),
      meta_indexs_(),
      meta_index_store_(),
      restore_version_(INT64_MAX),
      skip_change_member_list_(false)
{}

ObPartGroupMigrationTask::~ObPartGroupMigrationTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (is_inited_) {
    int64_t timestamp = ObTimeUtility::current_time();
    common::SpinWLockGuard guard(lock_);

    bool is_all_finish = false;
    while (!is_all_finish) {
      if (ObPartGroupMigrator::get_instance().is_stop()) {
        STORAGE_LOG(WARN, "mirgator is stop, set group task is cancelled", K(ret), K(*this));
        break;
      } else {
        // After the dfault loop is over, it should all be over
        is_all_finish = true;
        for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
          if (task_list_[i].need_reset_migrate_status_) {
            DEBUG_SYNC(BEFORE_CLEAR_MIGRATE_STATUS);
            bool need_retry = false;
            ObIPartitionGroup *partition = NULL;
            ObPGStorage *pg_storage = NULL;
            ObMigrateStatus cur_migrate_status = OB_MIGRATE_STATUS_NONE;
            ObMigrateStatus new_migrate_status = OB_MIGRATE_STATUS_NONE;

            if (OB_ISNULL(partition = task_list_[i].ctx_.get_partition())) {
              tmp_ret = OB_ERR_SYS;
              LOG_ERROR("partition/partition storage must not null", K(tmp_ret));
            } else if (OB_ISNULL(pg_storage = &(partition->get_pg_storage()))) {
              tmp_ret = OB_ERR_SYS;
              LOG_ERROR("pg_storage must not null", K(tmp_ret), KP(pg_storage));
            } else if (OB_SUCCESS != (tmp_ret = pg_storage->get_pg_migrate_status(cur_migrate_status))) {
              LOG_WARN("failed to get migrate status", K(tmp_ret), "pkey", task_list_[i].arg_.key_);
            } else if (OB_SUCCESS == task_list_[i].result_) {
              new_migrate_status = OB_MIGRATE_STATUS_NONE;
              LOG_INFO(
                  "migrate success, set migrate status none", "pkey", task_list_[i].arg_.key_, K(cur_migrate_status));
            } else if (!task_list_[i].during_migrating_) {
              new_migrate_status = OB_MIGRATE_STATUS_NONE;
              LOG_INFO("migrate fail not during migrting, set migrate status none",
                  "pkey",
                  task_list_[i].arg_.key_,
                  K(cur_migrate_status));
            } else if (FAST_MIGRATE_REPLICA_OP == task_list_[i].arg_.type_ &&
                       task_list_[i].ctx_.is_takeover_finished_) {
              new_migrate_status = OB_MIGRATE_STATUS_NONE;
              LOG_INFO("fast migrate: set status normal when takeover finished",
                  K(ret),
                  K(cur_migrate_status),
                  K(task_list_[i].arg_.key_));
            } else {
              if (OB_SUCCESS !=
                  (tmp_ret = ObMigrateStatusHelper::trans_fail_status(cur_migrate_status, new_migrate_status))) {
                LOG_WARN("failed to trans_migrate_fail_status",
                    K(tmp_ret),
                    "pkey",
                    task_list_[i].arg_.key_,
                    K(cur_migrate_status));
              }
            }

            if (OB_SUCCESS == tmp_ret) {
              if (OB_SUCCESS != (tmp_ret = pg_storage->set_pg_migrate_status(new_migrate_status, timestamp))) {
                if (OB_PARTITION_IS_REMOVED == tmp_ret) {
                  LOG_WARN("partition is removed, no need to set migrate status",
                      "pkey",
                      task_list_[i].arg_.key_,
                      K(tmp_ret));
                } else {
                  LOG_ERROR("failed to set migrate status, will retry next loop",
                      "pkey",
                      task_list_[i].arg_.key_,
                      K(tmp_ret));
                  need_retry = true;
                }
              } else {
                LOG_INFO(
                    "succ to reset migrate status", "pkey", task_list_[i].arg_.key_, K(new_migrate_status), K(tmp_ret));
              }
            }

            if (!need_retry) {
              task_list_[i].need_reset_migrate_status_ = false;
            } else {
              // If there are tasks that need to be retried, not all tasks are considered to has been completed
              is_all_finish = false;
            }
          }  // end of if (task_list_[i].need_reset_migrate_status_)
        }    // end of for loop
      }
    }  // end of while loop
    is_inited_ = false;
  }
}

int ObPartGroupMigrationTask::init(const ObIArray<ObReplicaOpArg> &task_list, const bool is_batch_mode,
    storage::ObPartitionService *partition_service, const share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObPartMigrationTask, tmp_task)
  {
    ObChangeMemberOption change_member_option = NORMAL_CHANGE_MEMBER_LIST;
    common::SpinWLockGuard guard(lock_);

    if (is_inited_) {
      ret = OB_INIT_TWICE;
      STORAGE_LOG(WARN, "cannot init twice", K(ret));
    } else if (NULL == partition_service || task_list.empty() || task_id.is_invalid()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid args", K(ret), K(is_batch_mode), KP(partition_service), K(task_id), K(task_list));
    } else if (OB_FAIL(cond_.init(ObWaitEventIds::GROUP_MIGRATE_TASK_IDLE_WAIT))) {
      STORAGE_LOG(WARN, "failed to init cond", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < task_list.count(); ++i) {
        tmp_task.arg_ = task_list.at(i);
        tmp_task.arg_.cluster_id_ =
            (tmp_task.arg_.cluster_id_ == OB_INVALID_CLUSTER_ID) ? GCONF.cluster_id : tmp_task.arg_.cluster_id_;
        tmp_task.status_ = ObPartMigrationTask::INIT;
        tmp_task.result_ = OB_SUCCESS;

        if (OB_FAIL(task_list_.push_back(tmp_task))) {
          STORAGE_LOG(WARN, "failed to add task list", K(ret));
        }

        if (OB_SUCC(ret)) {
          if (0 == i) {
            type_ = task_list.at(i).type_;
            tenant_id_ = task_list.at(i).key_.get_tenant_id();
            change_member_option = task_list.at(i).change_member_option_;
            restore_version_ = task_list.at(i).restore_version_;
            if (!is_replica_op_valid(type_)) {
              ret = OB_ERR_SYS;
              STORAGE_LOG(ERROR, "invalid type", K(ret), K(i), K(type_), K(task_list));
            } else if (SKIP_CHANGE_MEMBER_LIST == change_member_option && ADD_REPLICA_OP != type_ &&
                       REBUILD_REPLICA_OP != type_ && MIGRATE_REPLICA_OP != type_ && FAST_MIGRATE_REPLICA_OP != type_ &&
                       CHANGE_REPLICA_OP != type_ && LINK_SHARE_MAJOR_OP != type_ && RESTORE_STANDBY_OP != type_) {
              ret = OB_ERR_SYS;
              STORAGE_LOG(ERROR,
                  "only add or migrate or change replica can skip change member list",
                  K(ret),
                  K(type_),
                  K(task_list));
            }
          } else if (type_ != task_list.at(i).type_ || tenant_id_ != task_list.at(i).key_.get_tenant_id() ||
                     change_member_option != task_list.at(i).change_member_option_) {
            ret = OB_ERR_SYS;
            STORAGE_LOG(ERROR,
                "migration type or tenant_id or skip_change_member_list not same in task list",
                K(ret),
                K(i),
                K(task_list));
          }
        }
      }

      if (OB_SUCC(ret)) {
        is_inited_ = true;
        is_batch_mode_ = is_batch_mode;
        schedule_ts_ = ObTimeUtility::current_time();
        start_change_member_ts_ = 0;
        partition_service_ = partition_service;
        first_error_code_ = OB_SUCCESS;
        task_id_ = task_id;
        is_finished_ = false;
        need_idle_ = true;
        change_member_option_ = change_member_option;
        STORAGE_LOG(INFO, "succeed to init group migration task", K(*this));
      }
    }
  }
  return ret;
}

int ObPartGroupMigrationTask::do_task()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  ObCurTraceId::set(task_id_);

  ObTaskController::get().allow_next_syslog();
  LOG_INFO("start group migration task", K(*this));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    if (OB_FAIL(check_before_do_task())) {
      LOG_WARN("Failed to check_before_do_task", K(ret));
    } else if (OB_FAIL(remove_member_list_if_need())) {
      LOG_WARN("failed to remove_member_list_if_need", K(ret));
    } else if (RESTORE_REPLICA_OP == type_) {
      const bool need_check_compeleted = true;
      if (OB_UNLIKELY(0 == task_list_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("restore task_list_ is empty", K(ret), K(type_), K(task_list_.count()));
      } else if (task_list_[0].arg_.is_physical_restore_leader()) {
        const share::ObPhysicalRestoreArg &restore_arg = task_list_[0].arg_.phy_restore_arg_;
        if (OB_FAIL(init_restore_meta_index_(restore_arg))) {
          LOG_WARN("failed to init meta index", K(ret), K(restore_arg));
        }
      }
    } else if (VALIDATE_BACKUP_OP == type_) {
      const share::ObPhysicalValidateArg &validate_arg = task_list_[0].arg_.validate_arg_;
      ObBackupBaseDataPathInfo path_info;
      if (!validate_arg.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("validate arg is invalid, please check", K(ret), K(validate_arg));
      } else if (OB_FAIL(validate_arg.get_backup_base_data_info(path_info))) {
        LOG_WARN("failed to get backup base data info", K(ret), K(validate_arg));
      } else if (OB_FAIL(meta_index_store_.init(path_info))) {
        LOG_WARN("init validate meta_index_store failed", K(ret), K(path_info));
      }
    } else {
      // do nothing
    }

    if (OB_FAIL(ret)) {
      common::SpinWLockGuard guard(lock_);
      if (OB_SUCCESS == first_error_code_) {
        first_error_code_ = ret;
        STORAGE_LOG(WARN, "set first_error_code_", K(ret));
      }
    }

    while (!is_finished_) {
      if (OB_SUCCESS != (tmp_ret = check_is_task_cancel())) {
        STORAGE_LOG(WARN, "failed to check is task cancel", K(tmp_ret));
      }

      if (OB_SUCCESS != (tmp_ret = try_schedule_new_partition_migration())) {
        STORAGE_LOG(WARN, "failed to try_schedule_new_partition_migration", K(tmp_ret));
      }

      if (OB_SUCCESS != (tmp_ret = try_finish_group_migration())) {
        STORAGE_LOG(WARN, "failed to try_finish_group_migration", K(tmp_ret));
      }

      if (!is_finished_) {
        share::dag_yield();
        ObThreadCondGuard guard(cond_);
        if (need_idle_) {
          if (OB_SUCCESS != (tmp_ret = cond_.wait(PART_GROUP_TASK_IDLE_TIME_MS))) {
            if (OB_TIMEOUT != tmp_ret) {
              STORAGE_LOG(WARN, "failed to idle", K(tmp_ret));
            }
          }
        }
        need_idle_ = true;
      }
    }
  }

  ObTaskController::get().allow_next_syslog();
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("group migrate task finish", K(cost_ts), K(*this));

  return ret;
}

int ObPartGroupMigrationTask::remove_member_list_if_need()
{
  int ret = OB_SUCCESS;
  ObAddr leader_addr;
  bool need_remove = false;
  bool need_batch = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (CHANGE_REPLICA_OP != type_) {
    LOG_INFO("no need remove member list");
  } else if (OB_FAIL(check_need_batch_remove_member(leader_addr, need_remove, need_batch))) {
    LOG_WARN("failed to check need batch remove member", K(ret));
  } else if (!need_remove) {
    LOG_INFO("no need remove member");
  } else if (need_batch) {
    hash::ObHashSet<ObPartitionKey> removed_pkeys;
    ObArray<ObReportPartMigrationTask> report_list;
    SMART_VAR(ObReportPartMigrationTask, tmp_task)
    {
      for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
        ObPartMigrationTask &sub_task = task_list_[i];
        tmp_task.arg_ = sub_task.arg_;
        tmp_task.status_ = sub_task.status_;
        tmp_task.result_ = sub_task.result_;
        tmp_task.need_report_checksum_ = sub_task.ctx_.need_report_checksum_;
        tmp_task.ctx_ = &sub_task.ctx_;
        if (OB_FAIL(report_list.push_back(tmp_task))) {
          // report_list is reserved before, should not fail here
          STORAGE_LOG(ERROR, "failed to add report list", K(ret));
        }
      }

      if (OB_FAIL(try_batch_remove_member(report_list, leader_addr, removed_pkeys))) {
        LOG_WARN("failed to batch remove member list", K(ret));
      } else if ((removed_pkeys.size() != report_list.count()) || (report_list.count() != task_list_.count())) {
        ret = OB_PARTIAL_FAILED;
        LOG_WARN("failed to remove member list",
            K(ret),
            K(leader_addr),
            "removed_pkeys",
            removed_pkeys.size(),
            "task_count",
            report_list.count(),
            K(task_list_.count()));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
          ObPartMigrationTask &sub_task = task_list_[i];
          sub_task.result_ = report_list.at(i).result_;
        }
      }
    }
  } else {
    if (OB_FAIL(try_single_remove_member())) {
      LOG_WARN("failed to single remove member list", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    first_error_code_ = ret;
  }
  return ret;
}

int ObPartGroupMigrationTask::check_need_batch_remove_member(ObAddr &leader_addr, bool &need_remove, bool &need_batch)
{
  int ret = OB_SUCCESS;
  ObMigrateSrcInfo tmp_src_info;
  const int64_t start_ts = ObTimeUtility::current_time();
  leader_addr.reset();
  need_remove = false;
  need_batch = true;
  const bool is_primary_cluster = GCTX.is_primary_cluster();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (task_list_.count() <= 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("no task need migrate", K(ret), K(task_list_));
  } else if (NORMAL_CHANGE_MEMBER_LIST != task_list_.at(0).arg_.change_member_option_) {
    LOG_INFO("task skip change member list, skip it", K(task_list_.at(0)));
  } else {
    ObReplicaType src_type = task_list_.at(0).arg_.src_.get_replica_type();
    ObReplicaType dest_type = task_list_.at(0).arg_.dst_.get_replica_type();
    const ObAddr &self_addr = partition_service_->get_self_addr();

    // For batch operations of this type, rs guarantees src_type and dest_type must be consistent with the local,
    // or an error will be report directly
    for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
      ObIPartitionGroupGuard guard;
      ObPartMigrationTask &task = task_list_.at(i);
      if (OB_FAIL(ObPartitionService::get_instance().get_partition(task.arg_.key_, guard))) {
        LOG_WARN("failed to get partition guard", K(ret), "arg", task.arg_);
      } else if (OB_ISNULL(guard.get_partition_group())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("partition must not null", K(ret), "arg", task.arg_);
      } else if (guard.get_partition_group()->get_replica_type() != src_type ||
                 src_type != task.arg_.src_.get_replica_type() || dest_type != task.arg_.dst_.get_replica_type() ||
                 self_addr != task.arg_.src_.get_server() || self_addr != task.arg_.dst_.get_server()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("replica type not match",
            K(ret),
            K(self_addr),
            K(src_type),
            K(dest_type),
            "local_replica_type",
            guard.get_partition_group()->get_replica_type(),
            "arg",
            task.arg_);
      }
    }

    if (OB_SUCC(ret)) {
      if (src_type == dest_type) {
        ret = OB_ALREADY_DONE;
        STORAGE_LOG(WARN, "no change of replica type", K(ret), K(src_type), K(dest_type));
      } else if (!ObReplicaTypeCheck::change_replica_op_allow(src_type, dest_type)) {
        ret = OB_OP_NOT_ALLOW;
        STORAGE_LOG(WARN, "change replica op not allow", K(src_type), K(dest_type), K(ret));
      } else if (ObReplicaTypeCheck::is_paxos_replica(src_type) && !ObReplicaTypeCheck::is_paxos_replica(dest_type)) {
        LOG_INFO("need remove member list first");
        need_remove = true;
        for (int64_t i = 0; OB_SUCC(ret) && need_batch && i < task_list_.count(); ++i) {
          const ObReplicaOpArg &arg = task_list_.at(i).arg_;
          if (OB_FAIL(ObMigrateGetLeaderUtil::get_leader(arg.key_, tmp_src_info, true /*force_update*/))) {
            STORAGE_LOG(WARN, "failed to get leader address", K(ret), K(arg.key_));
          } else if (!tmp_src_info.is_valid()) {
            need_batch = false;
            STORAGE_LOG(WARN, "some partition has no leader, cannot batch change member", K(arg), K(tmp_src_info));
          } else if (0 == i) {
            leader_addr = tmp_src_info.src_addr_;
          } else if (leader_addr != tmp_src_info.src_addr_) {
            need_batch = false;
            STORAGE_LOG(WARN,
                "partitions have diff leader, cannot batch change member",
                K(tmp_src_info),
                K(leader_addr),
                K(arg));
          }
        }
      }
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret) && need_batch) {
    need_batch = GCONF.allow_batch_remove_member_during_change_replica;
    if (!need_batch) {
      LOG_ERROR("fake not need batch remove member list");
    }
  }
#endif

  ObTaskController::get().allow_next_syslog();
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  STORAGE_LOG(INFO,
      "check_need_batch_remove_member",
      K(ret),
      "count",
      task_list_.count(),
      K(need_remove),
      K(need_batch),
      K(cost_ts));
  return ret;
}

int ObPartGroupMigrationTask::try_single_remove_member()
{
  int ret = OB_SUCCESS;
  share::ObTaskId dummy_id;
  const int64_t start_ts = ObTimeUtility::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    dummy_id.init(partition_service_->get_self_addr());
    for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
      const ObReplicaOpArg &arg = task_list_.at(i).arg_;
      int64_t dummy_orig_quorum = OB_INVALID_COUNT;  // only used in ObPartitionService::batch_remove_replica_mc
      obrpc::ObMemberChangeArg remove_arg = {
          arg.key_, arg.src_, false, arg.quorum_, dummy_orig_quorum, WITHOUT_MODIFY_QUORUM, dummy_id};  // mock task id
      if (OB_FAIL(partition_service_->try_remove_from_member_list(remove_arg))) {
        STORAGE_LOG(WARN, "remove replica from member list failed", K(remove_arg), K(ret));
      } else {
        STORAGE_LOG(INFO, "remove replica from member list successfully", K(i), K(arg));
      }
    }
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  ObTaskController::get().allow_next_syslog();
  LOG_INFO("finish try_single_remove_member", K(ret), "count", task_list_.count(), K(cost_ts));
  return ret;
}

int ObPartGroupMigrationTask::set_create_new_pg(const ObPartitionKey &pg_key)
{
  int ret = OB_SUCCESS;
  ObPartMigrationTask *task = NULL;
  common::SpinWLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    for (int64_t i = 0; NULL == task && i < task_list_.count(); ++i) {
      if (task_list_[i].arg_.key_ == pg_key) {
        task = &task_list_[i];
      }
    }
    if (OB_ISNULL(task)) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(ERROR, "cannot found pg in task", K(ret), K(pg_key), KP(this), K(*this));
    } else {
      task->need_reset_migrate_status_ = true;
    }
  }
  return ret;
}

int ObPartGroupMigrationTask::set_migrate_in(ObPGStorage &pg_storage)
{
  int ret = OB_SUCCESS;
  ObPartMigrationTask *task = NULL;
  const ObPartitionKey &pkey = pg_storage.get_partition_key();  // TODO () change it to pg key
  int64_t current_ts = ObTimeUtility::current_time();
  ObMigrateStatus old_status = OB_MIGRATE_STATUS_MAX;
  ObMigrateStatus new_status = OB_MIGRATE_STATUS_MAX;
  common::SpinWLockGuard guard(lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    for (int64_t i = 0; NULL == task && i < task_list_.count(); ++i) {
      if (task_list_[i].arg_.key_ == pkey) {
        task = &task_list_[i];
      }
    }
    if (NULL == task) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(ERROR, "cannot found pkey in task", K(ret), K(pkey), KP(this), K(*this));
    } else if (OB_FAIL(pg_storage.get_pg_migrate_status(old_status))) {
      LOG_WARN("failed to get old status", K(ret), K(pkey));
    } else if (BACKUP_REPLICA_OP == task->arg_.type_ || VALIDATE_BACKUP_OP == task->arg_.type_ ||
               BACKUP_BACKUPSET_OP == task->arg_.type_ || BACKUP_ARCHIVELOG_OP == task->arg_.type_) {
      LOG_INFO("no need set migrate_status", K(task->arg_.type_), K(pkey));
    } else if (OB_FAIL(ObMigrateStatusHelper::trans_replica_op(task->arg_.type_, new_status))) {
      LOG_WARN("failed to trans_replica_op_to_migrate_status", K(ret), "arg", task->arg_);
    } else if (OB_MIGRATE_STATUS_NONE != old_status) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("old migrate is not finish, cannot set new migrate status",
          K(ret),
          K(pkey),
          K(old_status),
          "arg",
          task->arg_);
    } else {
      if (OB_FAIL(pg_storage.set_pg_migrate_status(new_status, current_ts))) {
        STORAGE_LOG(WARN, "failed to set migrate status", K(ret), K(pkey), K(new_status));
      } else {
        task->need_reset_migrate_status_ = true;
        if (old_status == OB_MIGRATE_STATUS_ADD_FAIL || old_status == OB_MIGRATE_STATUS_MIGRATE_FAIL) {
          task->during_migrating_ = true;
        }
        if (ADD_REPLICA_OP == task->arg_.type_ && pg_storage.is_restore()) {
          change_member_option_ = SKIP_CHANGE_MEMBER_LIST;
          STORAGE_LOG(INFO, "add replica op when restore skip set member list", K(pkey));
        }
      }
      STORAGE_LOG(INFO,
          "finish set migrate status",
          K(ret),
          K(pkey),
          K(old_status),
          K(new_status),
          "during_migrating_flag",
          task->during_migrating_);
    }
  }
  return ret;
}

int ObPartGroupMigrationTask::check_before_do_task()
{
  // Scheduling is only necessary before starting execution, without concurrent consideration
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not inited", K(ret));
  } else if (REMOVE_REPLICA_OP == type_) {
    STORAGE_LOG(INFO, "no need to check remove replica task", K(*this));
  } else if (BACKUP_REPLICA_OP == type_) {
    if (OB_FAIL(check_before_backup())) {
      STORAGE_LOG(WARN, "failed to check before backup", K(ret), K(*this));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_partition_validation())) {
    STORAGE_LOG(WARN, "failed to check_partition_validation", K(ret));
  } else if (OB_FAIL(check_disk_space())) {
    STORAGE_LOG(WARN, "failed to check_disk_space", K(ret));
  }

  if (OB_FAIL(ret)) {
    ObTaskController::get().allow_next_syslog();
    first_error_code_ = ret;
    STORAGE_LOG(WARN, "failed to check task, mark fail", K(ret), K(*this));
  }
  return ret;
}

int ObPartGroupMigrationTask::check_partition_validation()
{
  // Scheduling is only necessary before starting execution, without concurrent consideration
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
      ObPartMigrationTask &sub_task = task_list_[i];
      ObPartitionKey &pkey = sub_task.arg_.key_;
      bool in_member_list = false;
      bool is_working_partition = partition_service_->is_working_partition(pkey);

      if (ObPartGroupMigrator::get_instance().is_stop()) {
        ret = OB_SERVER_IS_STOPPING;
        STORAGE_LOG(WARN, "server is stopping", K(ret));
      } else if (REMOVE_REPLICA_OP == sub_task.arg_.type_) {
        ret = OB_ERR_SYS;
        LOG_ERROR("remove replica cannot schedule migrate", K(ret), "arg", sub_task.arg_);
      } else if (ADD_REPLICA_OP == type_ || MIGRATE_REPLICA_OP == type_ || FAST_MIGRATE_REPLICA_OP == type_) {
        if (partition_service_->is_partition_exist(pkey)) {
          ret = OB_ENTRY_EXIST;
          ObTaskController::get().allow_next_syslog();
          STORAGE_LOG(WARN, "can not add replica which is already exsit", K(ret), K(pkey), K(sub_task));
        }
      } else if (REBUILD_REPLICA_OP == type_) {
        if (!is_working_partition) {
          if (OB_FAIL(partition_service_->check_self_in_member_list(pkey, in_member_list))) {
            STORAGE_LOG(WARN, "failed to check_self_in_member_list", K(ret), K(pkey));
          } else if (!in_member_list) {
            ret = OB_WORKING_PARTITION_NOT_EXIST;
            ObTaskController::get().allow_next_syslog();
            STORAGE_LOG(WARN, "can not rebuild, it is not normal replica", K(ret), K(pkey), K(sub_task));
          } else {
            // Allow partition reconstruction in the member list except for non normal partition
            ObTaskController::get().allow_next_syslog();
            STORAGE_LOG(WARN, "it is not normal partition", K(ret), K(pkey), K(sub_task));
          }
        }
      } else if (COPY_GLOBAL_INDEX_OP == type_ || COPY_LOCAL_INDEX_OP == type_ || LINK_SHARE_MAJOR_OP == type_) {
        if (!is_working_partition) {
          ret = OB_STATE_NOT_MATCH;
          ObTaskController::get().allow_next_syslog();
          LOG_WARN("cannot copy index to not working partition", K(ret));
        }
      } else {
        // do nothing
      }
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_CHECK_SUB_MIGRATION_TASK) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_CHECK_SUB_MIGRATION_TASK", K(ret));
      }
    }
#endif
  }

  return ret;
}

int ObPartGroupMigrationTask::check_disk_space()
{  // Scheduling is only necessary before starting execution, without concurrent consideration
  int ret = OB_SUCCESS;
  int64_t required_size = 0;
  int64_t partition_required_size = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not inited", K(ret));
  } else if (MIGRATE_REPLICA_OP != type_ && ADD_REPLICA_OP != type_) {
    STORAGE_LOG(INFO, "only migrate or add replica task need check disk space, others skip it", K(type_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
      ObPartitionKey &pkey = task_list_[i].arg_.key_;
      if (ObPartGroupMigrator::get_instance().is_stop()) {
        ret = OB_SERVER_IS_STOPPING;
        STORAGE_LOG(WARN, "server is stopping", K(ret));
      } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(task_list_[i].arg_.dst_.get_replica_type())) {
        STORAGE_LOG(INFO, "dst has no ssstore, no need check disk space", "arg", task_list_[i].arg_);
      } else if (OB_FAIL(get_partition_required_size(pkey, partition_required_size))) {
        STORAGE_LOG(WARN, "failed to partition_required_size", K(ret), K(pkey));
      } else {
        required_size += partition_required_size;
      }
    }

    if (OB_SUCC(ret) && required_size > 0) {
      if (OB_FAIL(OB_STORE_FILE.check_disk_full(required_size))) {
        if (OB_CS_OUTOF_DISK_SPACE == ret) {
          ret = OB_SERVER_MIGRATE_IN_DENIED;
        }
        ObTaskController::get().allow_next_syslog();
        STORAGE_LOG(WARN, "failed to check_is_disk_full, cannot migrate in", K(ret), K(required_size));
      }
    }
  }

  return ret;
}

int ObPartGroupMigrationTask::check_before_backup()
{
  int ret = OB_SUCCESS;
  ObExternBackupInfoMgr extern_backup_info_mgr;
  ObClusterBackupDest cluster_backup_dest;
  ObBackupDest backup_dest;
  ObBackupPath path;
  ObStorageUtil util(false /*need retry*/);
  bool is_exist = false;

  // For nfs 4.2 may has bug, which makes open wrong file handle
  // There just check file exist
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (BACKUP_REPLICA_OP != type_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "replica op is not backup, no need check", K(ret));
  } else {
    const ObPhysicalBackupArg &backup_arg = task_list_[0].arg_.backup_arg_;
    const uint64_t tenant_id = task_list_[0].arg_.key_.get_tenant_id();
    if (OB_FAIL(backup_dest.set(backup_arg.uri_header_, backup_arg.storage_info_))) {
      STORAGE_LOG(WARN, "failed to set backup dest", K(ret), K(backup_arg));
    } else if (OB_FAIL(cluster_backup_dest.set(backup_dest, backup_arg.incarnation_))) {
      STORAGE_LOG(WARN, "failed to set cluster backup dest", K(ret), K(backup_dest));
    } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_backup_info_path(cluster_backup_dest, tenant_id, path))) {
      LOG_WARN("failed to get tenant data backup info path", K(ret), K(backup_dest));
    } else if (OB_FAIL(util.is_exist(path.get_ptr(), backup_arg.storage_info_, is_exist))) {
      LOG_WARN("failed to check extern backup file info exist", K(ret), K(path), K(backup_dest));
    } else if (!is_exist) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extern backup info is not exist", K(ret), K(backup_arg), K(path));
    }
  }
  return ret;
}

int ObPartGroupMigrationTask::get_partition_required_size(const common::ObPartitionKey &pkey, int64_t &required_size)
{
  // statuc func
  int ret = OB_SUCCESS;
  share::ObPartitionTableOperator *pt_operator = NULL;
  ModulePageAllocator allocator(ObNewModIds::OB_PARTITION_MIGRATE);
  share::ObPartitionInfo partition_info;
  partition_info.set_allocator(&allocator);
  required_size = 0;

  if (NULL == (pt_operator = GCTX.pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pt_operator must not NULL.", K(ret));
  } else if (OB_SUCCESS != (ret = pt_operator->get(pkey.get_table_id(), pkey.get_partition_id(), partition_info))) {
    STORAGE_LOG(WARN, "fail to get partition info.", K(ret), K(pkey));
  } else {
    const common::ObIArray<share::ObPartitionReplica> &replicas = partition_info.get_replicas_v2();
    for (int64_t i = 0; OB_SUCC(ret) && i < replicas.count(); ++i) {
      if (replicas.at(i).required_size_ > required_size) {
        required_size = replicas.at(i).required_size_;
      }
    }
  }

  return ret;
}

int ObPartGroupMigrationTask::try_schedule_new_partition_migration()
{
  int ret = OB_SUCCESS;

  {
    common::SpinRLockGuard guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(ERROR, "not inited", K(ret));
    } else if (OB_SUCCESS != first_error_code_) {
      ret = first_error_code_;
      STORAGE_LOG(WARN, "first_error_code_ is set, skip schedule new partition migration", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (VALIDATE_BACKUP_OP == type_) {
      if (OB_FAIL(try_schedule_partition_validate())) {
        STORAGE_LOG(WARN, "failed to try schedule partition validate", K(ret));
      }
    } else if (BACKUP_BACKUPSET_OP == type_) {
      if (OB_FAIL(try_schedule_partition_backup_backupset())) {
        STORAGE_LOG(WARN, "failed to try schedule partition backup backupset", K(ret));
      }
    } else if (BACKUP_ARCHIVELOG_OP == type_) {
      if (OB_FAIL(try_schedule_partition_backup_archivelog())) {
        STORAGE_LOG(WARN, "failed to try schedule partition backup archivelog", K(ret));
      }
    } else {
      if (OB_FAIL(try_schedule_partition_migration())) {
        STORAGE_LOG(WARN, "failed to try scheudule partition migration", K(ret));
      }
    }
  }
  return ret;
}

// caller must not hold wlock
int ObPartGroupMigrationTask::schedule_migrate_dag(ObMigrateCtx &migrate_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(check_partition_checksum(migrate_ctx.replica_op_arg_.key_))) {
    STORAGE_LOG(WARN,
        "failed to check partition checksum, cannot schedule migration",
        K(ret),
        K(migrate_ctx.replica_op_arg_.key_));
  } else if (OB_FAIL(migrate_ctx.generate_and_schedule_migrate_dag())) {
    STORAGE_LOG(WARN, "failed to generate migrate dag", K(ret));
  }

  if (OB_FAIL(ret)) {
    share::ObTaskId fake_task_id;
    const bool during_migrating = false;
    int64_t backup_set_id = -1;
    if (VALIDATE_BACKUP_OP == migrate_ctx.replica_op_arg_.type_) {
      backup_set_id = migrate_ctx.replica_op_arg_.validate_arg_.backup_set_id_;
    } else if (BACKUP_BACKUPSET_OP == migrate_ctx.replica_op_arg_.type_) {
      backup_set_id = migrate_ctx.replica_op_arg_.backup_backupset_arg_.backup_set_id_;
    }
    if (OB_SUCCESS != (tmp_ret = set_part_task_finish(
                           migrate_ctx.replica_op_arg_.key_, ret, fake_task_id, during_migrating, backup_set_id))) {
      LOG_WARN("failed to finish part task", K(ret));
    }
  }

  return ret;
}

int ObPartGroupMigrationTask::try_finish_group_migration()
{
  int ret = OB_SUCCESS;
  bool is_sub_task_finish = true;
  report_list_.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not inited", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);

    for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
      ObPartMigrationTask &task = task_list_[i];
      if (ObPartMigrationTask::FINISH != task.status_) {
        if (OB_SUCCESS != first_error_code_ && ObPartMigrationTask::INIT == task.status_) {
          task.status_ = ObPartMigrationTask::FINISH;
          task.result_ = first_error_code_;
        } else {
          is_sub_task_finish = false;
        }
      } else if (OB_SUCCESS != task.result_) {
        if (BACKUP_REPLICA_OP == get_type() && OB_PG_IS_REMOVED == task.result_) {
          // do nothing
        } else if (OB_SUCCESS == first_error_code_) {
          first_error_code_ = OB_ERR_SYS;
          STORAGE_LOG(ERROR, "sub task has error , but first_error_code_ is succ, use OB_ERR_SYS", K(*this));
        }
      }
    }

    if (OB_SUCC(ret) && is_sub_task_finish) {
      SMART_VAR(ObReportPartMigrationTask, tmp_task)
      {

        if (OB_FAIL(report_list_.reserve(task_list_.count()))) {
          STORAGE_LOG(WARN, "failed to reserve report list", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
          ObPartMigrationTask &sub_task = task_list_[i];
          tmp_task.arg_ = sub_task.arg_;
          tmp_task.status_ = ObIPartMigrationTask::FINISH;
          tmp_task.result_ = sub_task.result_;
          tmp_task.need_report_checksum_ = sub_task.ctx_.need_report_checksum_;
          tmp_task.ctx_ = &sub_task.ctx_;
          tmp_task.data_statics_ = sub_task.ctx_.data_statics_;
          if (OB_FAIL(tmp_task.partitions_.assign(sub_task.ctx_.pg_meta_.partitions_))) {
            STORAGE_LOG(WARN, "failed to assign partitions", K(ret), K(sub_task.ctx_));
          } else if (OB_FAIL(report_list_.push_back(tmp_task))) {
            // report_list is reserved before, should not fail here
            STORAGE_LOG(ERROR, "failed to add report list", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && is_sub_task_finish) {
    if (OB_FAIL(finish_group_migration(report_list_))) {
      STORAGE_LOG(WARN, "failed to do finish_group_migration", K(ret));
    }
  }

  return ret;
}

int ObPartGroupMigrationTask::finish_group_migration(ObIArray<ObReportPartMigrationTask> &report_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not inited", K(ret));
  } else if (REMOVE_REPLICA_OP == type_) {
    // The deleting op of the replica does not change the operation of the member list and reporting operation
    common::SpinWLockGuard guard(lock_);
    is_finished_ = true;
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "group migration all finish", K(ret), K(first_error_code_), K(report_list), K(*this));
  } else if (BACKUP_REPLICA_OP == type_ || VALIDATE_BACKUP_OP == type_ || BACKUP_BACKUPSET_OP == type_ ||
             BACKUP_ARCHIVELOG_OP == type_) {
    // backup base sstable only need change task status
    if (BACKUP_REPLICA_OP == type_) {
      ObArray<ObPartMigrationRes> report_res_list;
      if (OB_SUCCESS != (tmp_ret = ObMigrateUtil::get_report_result(report_list, report_res_list))) {
        LOG_WARN("failed to get report result", K(tmp_ret), K(report_list));
      } else if (OB_SUCCESS != (tmp_ret = partition_service_->report_pg_backup_task(report_res_list))) {
        LOG_WARN("failed to report pg backup task", K(tmp_ret), K(report_res_list));
      }
    }
    common::SpinWLockGuard guard(lock_);
    is_finished_ = true;
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO, "group migration-backup all finish", K(ret), K(first_error_code_), K(report_list), K(*this));
  } else if (OB_UNLIKELY(report_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(report_list.count()));
  } else {
    bool is_all_finish = false;
    bool is_report_succ = false;
    hash::ObHashSet<ObPartitionKey> removed_pkeys;

    if (0 == retry_job_start_ts_) {
      retry_job_start_ts_ = ObTimeUtility::current_time();
    }
    if (OB_SUCCESS != (tmp_ret = report_meta_table(report_list, removed_pkeys))) {
      STORAGE_LOG(WARN, "fail to report meta table", K(ret));
    } else {
      is_report_succ = true;
    }
    if (has_error()) {
      is_all_finish = true;
      if (OB_FAIL(build_failed_report_list(first_error_code_, report_list))) {
        STORAGE_LOG(WARN, "failed to build_failed_report_list", K(ret));
      }
    } else if (is_report_succ) {
      // TODO () consider more detail about it
      if (RESTORE_REPLICA_OP == type_ || COPY_LOCAL_INDEX_OP == type_ || RESTORE_FOLLOWER_REPLICA_OP == type_ ||
          RESTORE_STANDBY_OP == type_ || LINK_SHARE_MAJOR_OP == type_) {
        is_all_finish = true;
        STORAGE_LOG(INFO, "no need change member list", K(type_));
      } else {
        if (OB_SUCCESS != (tmp_ret = try_change_member_list(report_list, is_all_finish))) {
          STORAGE_LOG(WARN, "failed to try_change_member_list", K(tmp_ret), K(first_error_code_));
        } else {
          is_all_finish = true;
        }
      }

      if (first_error_code_ != OB_SUCCESS) {
        is_all_finish = true;
        if (OB_FAIL(build_failed_report_list(first_error_code_, report_list))) {
          STORAGE_LOG(WARN, "failed to build_failed_report_list", K(ret));
        }
      }
    }

    if (is_all_finish) {
      {
        common::SpinWLockGuard guard(lock_);
        is_finished_ = true;
        ObTaskController::get().allow_next_syslog();
        STORAGE_LOG(INFO, "group migration all finish", K(ret), K(first_error_code_), K(report_list), K(*this));
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; i < report_list.count(); ++i) {
          for (int64_t j = 0; j < report_list.at(i).partitions_.count(); ++j) {
            const ObPartitionKey &pkey = report_list.at(i).partitions_.at(j);
            if (OB_SUCCESS != (tmp_ret = partition_service_->report_migrate_in_indexes(pkey))) {
              STORAGE_LOG(WARN, "failed to handle_report_meta_table_callback", K(tmp_ret), K(i), K(report_list.at(i)));
            }
          }
        }
      }

      if (RESTORE_REPLICA_OP == type_ && OB_SUCCESS != first_error_code_) {
        if (OB_SUCCESS != (tmp_ret = report_restore_fatal_error_())) {
          LOG_WARN("failed to report restore failed", K(tmp_ret));
        }
      }

      ObRebuildReplicaTaskScheduler &rebuild_scheduler =
          partition_service_->get_rebuild_replica_service().get_scheduler();
      if (OB_SUCC(ret) && REBUILD_REPLICA_OP == type_) {
        ObRebuildReplicaResult results;
        for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
          if (OB_FAIL(results.set_result(report_list.at(i).arg_.key_, report_list.at(i).result_))) {
            STORAGE_LOG(WARN, "failed to set rebuild result", K(ret), K(report_list.at(i)));
          }
        }
      }
      rebuild_scheduler.notify();
    }
  }
  return ret;
}

int ObPartGroupMigrationTask::report_restore_fatal_error_()
{
  int ret = OB_SUCCESS;
  int64_t job_id = 0;

  if (OB_FAIL(ObBackupInfoMgr::get_instance().get_restore_job_id(tenant_id_, job_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get restore info", K(ret), K(tenant_id_));
    } else {
      ret = OB_SUCCESS;
      LOG_INFO("physical restore info not exist", K(tenant_id_), K(first_error_code_));
    }
  } else if (OB_INVALID_ARGUMENT == first_error_code_ || OB_ERR_SYS == first_error_code_ ||
             OB_INIT_TWICE == first_error_code_ || OB_ERR_UNEXPECTED == first_error_code_ ||
             OB_INVALID_BACKUP_DEST == first_error_code_ || OB_INVALID_DATA == first_error_code_ ||
             OB_CHECKSUM_ERROR == first_error_code_) {
    if (OB_FAIL(ObRestoreFatalErrorReporter::get_instance().add_restore_error_task(
            tenant_id_, PHYSICAL_RESTORE_MOD_STORAGE, first_error_code_, job_id, MYADDR))) {
      LOG_WARN("failed to report restore error", K(ret), K(tenant_id_), K(first_error_code_));
    }
  } else {
    LOG_INFO("skip not fatal report restore error", K(tenant_id_), K(first_error_code_));
  }

  return ret;
}

int ObPartGroupMigrationTask::try_change_member_list(
    ObIArray<ObReportPartMigrationTask> &report_list, bool &is_all_finish)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t change_member_list_timeout = GCONF.sys_bkgd_migration_change_member_list_timeout;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObMigrateSrcInfo leader_info;
  bool need_batch = false;

  DEBUG_SYNC(BEFORE_CHANGE_MEMBER_LIST);

  if (0 == start_change_member_ts_) {
    start_change_member_ts_ = ObTimeUtility::current_time();
    STORAGE_LOG(INFO, "start change member list", K(report_list));
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (report_list.count() <= 0) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "task list is empty", K(ret));
  } else if (NORMAL_CHANGE_MEMBER_LIST != change_member_option_) {
    if (MIGRATE_REPLICA_OP == type_) {
      if (OB_SUCCESS != (tmp_ret = batch_remove_src_replica(report_list))) {
        STORAGE_LOG(WARN, "failed to batch remove src replica", K(ret));
      }
    }
    STORAGE_LOG(INFO, "skip change member list is set, no need try_change_memer_list", K(ret), K(*this));
  } else if (start_ts - start_change_member_ts_ > change_member_list_timeout) {
    is_all_finish = true;
    ret = OB_TIMEOUT;
    first_error_code_ = OB_SUCCESS == first_error_code_ ? ret : first_error_code_;
    STORAGE_LOG(WARN,
        "max retry change member time exceeds",
        K(ret),
        K(start_change_member_ts_),
        K(change_member_list_timeout));
  } else if (OB_FAIL(
                 ObMigrateGetLeaderUtil::get_leader(report_list.at(0).arg_.key_, leader_info, true /*force_update*/))) {
    bool is_partition_exist = true;
    if (OB_SUCCESS != (tmp_ret = check_is_partition_exist(report_list.at(0).arg_.key_, is_partition_exist))) {
      LOG_WARN("failed to check is partition exist", K(tmp_ret));
    } else if (!is_partition_exist) {
      first_error_code_ = OB_SUCCESS == first_error_code_ ? ret : first_error_code_;
      LOG_WARN("partition not exist, set migration failed", K(ret), "pkey", report_list.at(0).arg_.key_);
    } else {
      STORAGE_LOG(WARN, "fail to get leader", K(ret));
    }
  } else if (OB_FAIL(check_need_batch_change_member_list(leader_info.src_addr_, report_list, need_batch))) {
    STORAGE_LOG(WARN, "failed to check_need_batch_change_member_list", K(ret));
  } else if (need_batch) {
    if (MIGRATE_REPLICA_OP == type_ || FAST_MIGRATE_REPLICA_OP == type_) {
      if (OB_FAIL(try_batch_change_member_list(report_list, leader_info.src_addr_, is_all_finish))) {
        STORAGE_LOG(WARN, "failed to try_batch_change_member_list", K(ret), K(leader_info), K(report_list));
      }
    } else if (CHANGE_REPLICA_OP == type_) {
      if (OB_FAIL(try_batch_add_member(report_list, leader_info.src_addr_, is_all_finish))) {
        LOG_WARN("failed to try_batch_add_member", K(ret), K(leader_info), K(report_list));
      }
    } else {
      ret = OB_ERR_SYS;
      LOG_ERROR("not support batch type", K(ret), K_(type));
    }
  } else {
    if (OB_FAIL(try_not_batch_change_member_list(report_list, is_all_finish))) {
      STORAGE_LOG(WARN, "failed to try_not_batch_change_member_list", K(ret));
    }
  }
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  STORAGE_LOG(INFO, "try_change_member_list", K(cost_ts), K(ret), K(is_all_finish), "count", report_list.count());

  return ret;
}

int ObPartGroupMigrationTask::check_need_batch_change_member_list(
    const ObAddr &leader_addr, ObIArray<ObReportPartMigrationTask> &report_list, bool &need_batch)
{
  int ret = OB_SUCCESS;
  ObMigrateSrcInfo tmp_leader_info;
  need_batch = false;
  const int64_t start_ts = ObTimeUtility::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (report_list.count() <= 0 || !leader_addr.is_valid()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "task list is empty", K(ret), K(leader_addr));
  } else {
    need_batch = true;
    for (int64_t i = 0; OB_SUCC(ret) && need_batch && i < report_list.count(); ++i) {
      const ObReplicaOpArg &arg = report_list.at(i).arg_;
      if (MIGRATE_REPLICA_OP == arg.type_ || FAST_MIGRATE_REPLICA_OP == arg.type_) {
        if (!ObReplicaTypeCheck::is_paxos_replica(arg.dst_.get_replica_type())) {
          need_batch = false;
        }
      } else if (CHANGE_REPLICA_OP == arg.type_) {
        if (ObReplicaTypeCheck::is_paxos_replica(arg.src_.get_replica_type()) ||
            !ObReplicaTypeCheck::is_paxos_replica(arg.dst_.get_replica_type())) {
          need_batch = false;
        }
      } else {
        need_batch = false;
      }

      if (OB_SUCC(ret) && need_batch) {
        if (OB_FAIL(ObMigrateGetLeaderUtil::get_leader(arg.key_, tmp_leader_info, true /*force_update*/))) {
          STORAGE_LOG(WARN, "failed to get leader address", K(ret), K(arg.key_));
          int tmp_ret = OB_SUCCESS;
          bool is_schema_exist = true;
          if (OB_SUCCESS != (tmp_ret = check_is_partition_exist(arg.key_, is_schema_exist))) {
            LOG_WARN("check is partition exist failed", K(tmp_ret), K(arg.key_));
          } else if (!is_schema_exist) {
            first_error_code_ = ret;
            LOG_WARN("partition schema not exist, set first_error_code_", K(first_error_code_));
          }
        } else if (!tmp_leader_info.is_valid()) {
          need_batch = false;
          STORAGE_LOG(WARN, "some partition has no leader, cannot batch change member", K(arg), K(tmp_leader_info));
        } else if (leader_addr != tmp_leader_info.src_addr_) {
          need_batch = false;
          STORAGE_LOG(WARN,
              "partitions have diff leader, cannot batch change member",
              K(tmp_leader_info),
              K(leader_addr),
              K(arg));
        }
      }
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret) && need_batch) {
    need_batch = GCONF.allow_batch_change_member;
    if (!need_batch) {
      LOG_ERROR("fake not need batch change member list");
    }
  }
#endif

  ObTaskController::get().allow_next_syslog();
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  STORAGE_LOG(
      INFO, "check_need_batch_change_member_list", K(cost_ts), K(ret), K(need_batch), "count", report_list.count());
  return ret;
}

int ObPartGroupMigrationTask::fast_migrate_add_member_list_one_by_one(
    common::ObIArray<ObReportPartMigrationTask> &report_list, bool &is_all_finish)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  int64_t succ_count = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (report_list.count() <= 0) {
    ret = OB_ERR_SYS;
    LOG_WARN("task list is empty", K(ret));
  } else if (FAST_MIGRATE_REPLICA_OP != type_) {
    ret = OB_ERR_SYS;
    LOG_WARN("wrong task type", K(ret), K(type_));
  }
  int64_t switch_epoch = GCTX.get_switch_epoch2();
  for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
    ObReportPartMigrationTask &cur_task = report_list.at(i);
    const ObPGKey &pg_key = cur_task.arg_.key_;
    ObMigrateCtx *cur_ctx = cur_task.ctx_;
    ObIPartitionGroup *pg = nullptr;
    int64_t quorum = 0;
    ObMigrateSrcInfo leader_info;
    ObMember new_member(GCTX.self_addr_, ObTimeUtility::current_time());
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(cur_ctx) || OB_ISNULL(pg = cur_ctx->partition_guard_.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("migrate ctx is null", K(ret), K(pg_key), KP(cur_ctx), KP(pg));
    } else if (OB_SUCCESS != cur_task.result_) {
      LOG_INFO("fast migrate: skip add self to member list", K(cur_task.result_), K(pg_key));
    } else if (OB_SUCCESS != (tmp_ret = pg->get_log_service()->get_replica_num(quorum))) {
      LOG_WARN("fail to get quorum", K(tmp_ret), K(pg_key));
    } else if (OB_SUCCESS !=
               (tmp_ret = ObMigrateGetLeaderUtil::get_leader(pg_key, leader_info, true /*force_update*/))) {
      LOG_WARN("fail to get pg leader", K(tmp_ret), K(pg_key));
    } else {
      cur_ctx->is_member_change_finished_ = true;
      LOG_INFO("fast_migrate: succ to change member list", K(pg_key));
      ++succ_count;
    }
  }
  if (OB_FAIL(ret)) {
    first_error_code_ = OB_SUCCESS == first_error_code_ ? ret : first_error_code_;
    STORAGE_LOG(WARN, "set first_error_code_", K(ret), K(first_error_code_));
  }
  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  is_all_finish = true;
  LOG_INFO("fast migrate: finish change member list in turn",
      K(ret),
      K(cost_ts),
      "task_count",
      report_list.count(),
      K(succ_count));
  return ret;
}

int ObPartGroupMigrationTask::try_batch_change_member_list(
    ObIArray<ObReportPartMigrationTask> &report_list, const ObAddr &leader_addr, bool &is_all_finish)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  hash::ObHashSet<ObPartitionKey> removed_pkeys;
  hash::ObHashSet<ObPartitionKey> added_pkeys;
  const int64_t start_ts = ObTimeUtility::current_time();
  is_all_finish = false;

  STORAGE_LOG(INFO, "start try_batch_change_member_list");

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (report_list.count() <= 0) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "task list is empty", K(ret));
  } else {
    if (MIGRATE_REPLICA_OP == type_) {
      if (OB_FAIL(try_batch_remove_member(report_list, leader_addr, removed_pkeys))) {
        STORAGE_LOG(WARN, "failed to try_batch_remove_member", K(ret), K(leader_addr), K(report_list));
      }
    } else {
      ret = OB_ERR_SYS;
      LOG_WARN("only migrate or fast migrate allow batch change member list", K(ret), K(type_));
    }
  }
  if (OB_SUCC(ret)) {
    // For migartion, if no need to retry, then add the deleted partition, otherwise no need to care
    is_all_finish = true;
    if (removed_pkeys.size() > 0) {
      if (OB_SUCCESS !=
          (tmp_ret = try_batch_add_member(report_list, removed_pkeys, leader_addr, added_pkeys, is_all_finish))) {
        STORAGE_LOG(WARN,
            "failed to try batch add member",
            K(ret),
            K(tmp_ret),
            K(leader_addr),
            K(removed_pkeys),
            K(report_list));
      } else if (FAST_MIGRATE_REPLICA_OP == type_) {
        for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
          ObReportPartMigrationTask &report_task = report_list.at(i);
          if (OB_NOT_NULL(report_task.ctx_) && OB_HASH_EXIST == added_pkeys.exist_refactored(report_task.arg_.key_)) {
            report_task.ctx_->is_member_change_finished_ = true;
            LOG_INFO("fast_migrate: succ to change member list", K(report_task.arg_.key_));
          }
        }
      }
    }

    // only migrate need remove src
    if (added_pkeys.size() > 0 && FAST_MIGRATE_REPLICA_OP != type_) {
      if (OB_SUCCESS != (tmp_ret = batch_remove_src_replica(report_list))) {
        STORAGE_LOG(WARN, "failed to batch remove src replica", K(ret), K(added_pkeys));
      }
    }
  }
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  STORAGE_LOG(INFO,
      "finish try_batch_change_member_list",
      K(cost_ts),
      K(ret),
      K(is_all_finish),
      "count",
      report_list.count(),
      "removed_count",
      removed_pkeys.size(),
      "added_count",
      added_pkeys.size());

  return ret;
}

int ObPartGroupMigrationTask::try_batch_remove_member(ObIArray<ObReportPartMigrationTask> &report_list,
    const ObAddr &leader_addr, hash::ObHashSet<ObPartitionKey> &removed_pkeys)
{
  int ret = OB_SUCCESS;
  ObChangeMemberArgs args;
  ObChangeMemberArg single_arg;
  ObChangeMemberCtxs change_member_info;
  const int64_t start_ts = ObTimeUtility::current_time();

  STORAGE_LOG(INFO, "start try_batch_remove_member");
  DEBUG_SYNC(BEFORE_BATCH_REMOVE_MEMBER);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(removed_pkeys.create(report_list.count()))) {
    STORAGE_LOG(WARN, "failed to create removed_pkeys", K(ret));
  } else if (OB_FAIL(args.reserve(report_list.count()))) {
    STORAGE_LOG(WARN, "failed to reserve change member ctx", K(ret), K(report_list.count()));
  } else {
    ObIPartitionServiceRpc &rpc = partition_service_->get_pts_rpc();
    for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
      single_arg.partition_key_ = report_list.at(i).arg_.key_;
      single_arg.member_ = report_list.at(i).arg_.src_;
      single_arg.quorum_ = report_list.at(i).arg_.quorum_;
      single_arg.switch_epoch_ = report_list.at(0).arg_.switch_epoch_;
      if (OB_FAIL(args.push_back(single_arg))) {
        STORAGE_LOG(WARN, "failed to add single ctx", K(ret), K(single_arg));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(rpc.batch_post_remove_replica_mc_msg(leader_addr, args, change_member_info))) {
        STORAGE_LOG(
            WARN, "failed to batch_post_remove_replica_mc_msg", K(ret), K(leader_addr), K(args), K(change_member_info));
      }
    }

    if (OB_SUCC(ret) || OB_PARTIAL_FAILED == ret) {
      ret = OB_SUCCESS;  // ovewrwrite retry
      if (OB_FAIL(wait_batch_member_change_done(leader_addr, change_member_info))) {
        STORAGE_LOG(
            WARN, "failed to wait_change_member_done for remove", K(ret), K(leader_addr), K(change_member_info));
      }

      if (OB_SUCC(ret) || OB_PARTIAL_FAILED == ret) {
        ret = OB_SUCCESS;  // overwrite ret
        if (change_member_info.count() != report_list.count()) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(ERROR,
              "change_member_info count not match report_list count",
              K(ret),
              K(change_member_info.count()),
              K(report_list.count()),
              K(change_member_info),
              K(report_list));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < change_member_info.count(); ++i) {
            const ObChangeMemberCtx &ret_ctx = change_member_info.at(i);
            if (OB_SUCCESS != ret_ctx.ret_value_) {
              STORAGE_LOG(WARN, "skip add removed pkey", K(ret_ctx));
              if (OB_FAIL(set_report_list_result(report_list, ret_ctx.partition_key_, ret_ctx.ret_value_))) {
                STORAGE_LOG(WARN, "failed to set report list result", K(ret));
              }
            } else {
              if (OB_FAIL(removed_pkeys.set_refactored(ret_ctx.partition_key_))) {
                removed_pkeys.reuse();
                STORAGE_LOG(WARN, "failed to set removed_pkeys", K(ret));
              }
            }
          }
        }
      }

      if (OB_FAIL(ret)) {
        first_error_code_ = OB_SUCCESS == first_error_code_ ? ret : first_error_code_;
        STORAGE_LOG(WARN, "set first_error_code_", K(ret), K(first_error_code_));
        ret = OB_SUCCESS;  // overwrite ret
      }
    }
  }
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  ObTaskController::get().allow_next_syslog();
  STORAGE_LOG(
      INFO, "finish try_batch_remove_member", K(cost_ts), K(ret), K(first_error_code_), "count", report_list.count());
  return ret;
}

int ObPartGroupMigrationTask::try_batch_add_member(ObIArray<ObReportPartMigrationTask> &report_list,
    const hash::ObHashSet<ObPartitionKey> &removed_pkeys, const ObAddr &leader_addr,
    hash::ObHashSet<ObPartitionKey> &added_keys, bool &is_all_finish)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  UNUSEDx(report_list, removed_pkeys, leader_addr, added_keys, is_all_finish);
  ret = E(EventTable::EN_FAST_MIGRATE_ADD_MEMBER_FAIL) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    LOG_INFO("errsim add member failure", K(ret));
    return ret;
  }
#endif
  ObChangeMemberArgs args;
  ObChangeMemberCtxs add_member_info;
  const int64_t start_ts = ObTimeUtility::current_time();
  STORAGE_LOG(INFO, "start try_batch_add_member");
  DEBUG_SYNC(BEFORE_BATCH_ADD_MEMBER);
  // is_all_finish is not initialized here. It may have been assigned true outside

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(added_keys.create(removed_pkeys.size()))) {
    STORAGE_LOG(WARN, "failed to create added_keys", K(ret));
  } else if (OB_FAIL(build_add_member_ctx(report_list, removed_pkeys, args))) {
    STORAGE_LOG(WARN, "failed to build_add_member_ctx", K(ret), K(removed_pkeys), K(report_list));
  } else {
    ObIPartitionServiceRpc &rpc = partition_service_->get_pts_rpc();

    if (OB_SUCC(ret)) {
      if (OB_FAIL(rpc.batch_post_add_replica_mc_msg(leader_addr, args, add_member_info))) {
        STORAGE_LOG(
            WARN, "failed to batch_post_add_replica_mc_msg", K(ret), K(leader_addr), K(args), K(add_member_info));
      }
    }

    if (OB_SUCC(ret) || OB_PARTIAL_FAILED == ret) {
      is_all_finish = true;
      ret = OB_SUCCESS;  // overwrite ret
      if (OB_FAIL(wait_batch_member_change_done(leader_addr, add_member_info))) {
        STORAGE_LOG(WARN, "failed to wait_change_member_done for add", K(ret), K(leader_addr), K(add_member_info));
      }

      if (OB_SUCC(ret) || OB_PARTIAL_FAILED == ret) {
        ret = OB_SUCCESS;  // overwrite ret
        if (add_member_info.count() != removed_pkeys.size()) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(ERROR,
              "removed_pkeys count not match add_member_info count",
              K(ret),
              K(removed_pkeys.size()),
              K(add_member_info.count()),
              K(removed_pkeys),
              K(add_member_info));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < add_member_info.count(); ++i) {
            const ObChangeMemberCtx &ret_ctx = add_member_info.at(i);
            if (OB_SUCCESS != ret_ctx.ret_value_) {
              STORAGE_LOG(WARN, "skip add added pkey", K(ret_ctx));
              if (OB_FAIL(set_report_list_result(report_list, ret_ctx.partition_key_, ret_ctx.ret_value_))) {
                STORAGE_LOG(WARN, "failed to set report list result", K(ret));
              }
            } else if (OB_FAIL(added_keys.set_refactored(add_member_info.at(i).partition_key_))) {
              STORAGE_LOG(WARN, "failed to set added keys", K(ret));
              added_keys.reuse();
            }
          }
        }
      }
    }
  }
  if (OB_FAIL(ret) && is_all_finish) {
    first_error_code_ = OB_SUCCESS == first_error_code_ ? ret : first_error_code_;
    STORAGE_LOG(WARN, "set first_error_code_", K(ret), K(first_error_code_));
  }
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  ObTaskController::get().allow_next_syslog();
  STORAGE_LOG(INFO,
      "finish try_batch_add_member",
      K(cost_ts),
      K(ret),
      K(first_error_code_),
      K(is_all_finish),
      "count",
      removed_pkeys.size());
  return ret;
}

int ObPartGroupMigrationTask::try_batch_add_member(
    ObIArray<ObReportPartMigrationTask> &report_list, const ObAddr &leader_addr, bool &is_all_finish)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<ObPartitionKey> removed_pkeys;
  hash::ObHashSet<ObPartitionKey> added_keys;
  const int64_t start_ts = ObTimeUtility::current_time();

  is_all_finish = false;
  STORAGE_LOG(INFO, "start try_batch_add_member");
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (report_list.count() <= 0) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "task list is empty", K(ret));
  } else if (CHANGE_REPLICA_OP != type_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("only migrate or change replica allow batch change member list", K(ret), K(type_));
  } else if (OB_FAIL(removed_pkeys.create(report_list.count()))) {
    STORAGE_LOG(WARN, "failed to create removed_pkeys", K(ret));
  } else {
    // chang replica also needs to set removed_pkeys. The logic behind it is than only pkeys than have been successfully
    // removed will be added
    for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
      if (OB_FAIL(removed_pkeys.set_refactored(report_list.at(i).arg_.key_))) {
        LOG_WARN("failed to add change replica pkey", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_batch_add_member(report_list, removed_pkeys, leader_addr, added_keys, is_all_finish))) {
      LOG_WARN("failed to try_batch_add_member", K(ret));
    } else if (CHANGE_REPLICA_OP == type_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
        ObReportPartMigrationTask &report_task = report_list.at(i);
        if (OB_NOT_NULL(report_task.ctx_) && OB_HASH_EXIST == added_keys.exist_refactored(report_task.arg_.key_)) {
          report_task.ctx_->is_member_change_finished_ = true;
          LOG_INFO("change replica: succ to add member list", K(report_task.arg_.key_));
        }
      }
    }
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  STORAGE_LOG(INFO,
      "finish try_batch_add_member",
      K(cost_ts),
      K(ret),
      K(is_all_finish),
      "count",
      report_list.count(),
      "added_count",
      added_keys.size());
  return ret;
}

int ObPartGroupMigrationTask::build_add_member_ctx(const ObIArray<ObReportPartMigrationTask> &report_list,
    const hash::ObHashSet<ObPartitionKey> &removed_pkeys, ObChangeMemberArgs &ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObChangeMemberArg single_ctx;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(ctx.reserve(removed_pkeys.size()))) {
    STORAGE_LOG(WARN, "failed to reserve change member ctx", K(ret), K(removed_pkeys.size()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
    const ObReplicaOpArg &arg = report_list.at(i).arg_;
    if (OB_HASH_EXIST == (tmp_ret = removed_pkeys.exist_refactored(arg.key_))) {
      single_ctx.partition_key_ = arg.key_;
      single_ctx.member_ = arg.dst_;
      single_ctx.quorum_ = arg.quorum_;
      single_ctx.switch_epoch_ = report_list.at(0).arg_.switch_epoch_;
      if (OB_FAIL(ctx.push_back(single_ctx))) {
        STORAGE_LOG(WARN, "failed to add single ctx", K(ret), K(single_ctx));
      }
    } else if (OB_HASH_NOT_EXIST != tmp_ret) {
      ret = tmp_ret;
      STORAGE_LOG(WARN, "failed to check removed pkey exist", K(ret));
    }
  }

  return ret;
}

int ObPartGroupMigrationTask::remove_src_replica(const ObIArray<ObReportPartMigrationTask> &report_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  share::ObTaskId dummy_id;
  dummy_id.init(OBSERVER.get_self());

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
    const ObReplicaOpArg &arg = report_list.at(i).arg_;
    if (OB_SUCCESS != report_list.at(i).result_) {
      STORAGE_LOG(INFO, "skip remove failed partition", K(report_list.at(i)));
    } else {
      obrpc::ObRemoveReplicaArg remove_replica_arg = {arg.key_, arg.src_};
      if (OB_SUCCESS != (tmp_ret = partition_service_->get_pts_rpc().post_remove_replica(
                             arg.src_.get_server(), remove_replica_arg))) {
        STORAGE_LOG(WARN, "remove replica from source observer failed", K(tmp_ret), K(remove_replica_arg));
      } else {
        STORAGE_LOG(INFO, "remove source replica successfully", K(arg.src_), K(remove_replica_arg));
      }
    }
  }

  return ret;
}

int ObPartGroupMigrationTask::batch_remove_src_replica(const ObIArray<ObReportPartMigrationTask> &report_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  share::ObTaskId dummy_id;
  dummy_id.init(OBSERVER.get_self());
  ObArenaAllocator allocator;
  typedef hash::ObHashMap<ObAddr, obrpc::ObRemoveReplicaArgs *, common::hash::NoPthreadDefendMode> JoinMap;
  JoinMap join_map;  // Map aggregated by serve
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(join_map.create(report_list.count(), ObModIds::OB_PARTITION_MIGRATOR))) {
    STORAGE_LOG(WARN, "failed to create hash table for join", K(ret));
  } else {
    obrpc::ObRemoveReplicaArgs *remove_replica_args = NULL;
    void *buf = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
      const ObReplicaOpArg &arg = report_list.at(i).arg_;
      buf = NULL;
      remove_replica_args = NULL;
      if (OB_SUCCESS != report_list.at(i).result_) {
        STORAGE_LOG(INFO, "skip remove failed partition", "pkey", arg.key_);
      } else {
        if (OB_FAIL(join_map.get_refactored(arg.src_.get_server(), remove_replica_args))) {
          if (OB_HASH_NOT_EXIST != ret) {
            STORAGE_LOG(WARN, "failed to get remove replica args from map", K(ret));
          } else if (NULL == (buf = allocator.alloc(sizeof(obrpc::ObRemoveReplicaArgs)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "failed to alloc buf", K(ret));
          } else if (NULL == (remove_replica_args = new (buf) obrpc::ObRemoveReplicaArgs())) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "failed to new remove replica args");
          } else if (OB_FAIL(join_map.set_refactored(arg.src_.get_server(), remove_replica_args))) {
            STORAGE_LOG(WARN, "failed to set join map", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          obrpc::ObRemoveReplicaArg remove_replica_arg = {arg.key_, arg.src_};
          if (OB_FAIL(remove_replica_args->arg_array_.push_back(remove_replica_arg))) {
            STORAGE_LOG(WARN, "failed to push remove replica arg into array", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (JoinMap::iterator iter = join_map.begin(); iter != join_map.end(); ++iter) {
        const ObAddr server = iter->first;
        remove_replica_args = iter->second;
        if (OB_SUCCESS !=
            (tmp_ret = partition_service_->get_pts_rpc().batch_post_remove_replica(server, *remove_replica_args))) {
          STORAGE_LOG(WARN, "remove replica from source observer failed", K(tmp_ret), K(*remove_replica_args));
        } else {
          STORAGE_LOG(INFO, "remove source replica successfully", K(*remove_replica_args));
        }
      }
    }
    // demonstrate destruct remove_replica_args
    for (JoinMap::iterator iter = join_map.begin(); iter != join_map.end(); ++iter) {
      remove_replica_args = iter->second;
      if (NULL != remove_replica_args) {
        remove_replica_args->~ObRemoveReplicaArgs();
      }
    }
    join_map.clear();
  }
  return ret;
}

int ObPartGroupMigrationTask::check_member_major_sstable_enough(ObIArray<ObReportPartMigrationTask> &report_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartGroupMigrationTask has not been inited", K(ret));
  } else if (OB_UNLIKELY(report_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(report_list.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
      if (ObReplicaTypeCheck::is_replica_with_ssstore(report_list.at(i).arg_.dst_.get_replica_type()) &&
          (ADD_REPLICA_OP == report_list.at(i).arg_.type_ ||
              MIGRATE_REPLICA_OP == report_list.at(i).arg_.type_
              //          || CHANGE_REPLICA_OP == report_list.at(i).arg_.type_ // TODO(): open it after add batch check
              || REBUILD_REPLICA_OP == report_list.at(i).arg_.type_)) {

        if (OB_FAIL(check_member_pg_major_sstable_enough(report_list.at(i)))) {
          STORAGE_LOG(WARN, "fail to check member pg major sstable enough", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_MAJOR_SSTABLE_NOT_EXIST == ret || OB_NOT_SUPPORTED == ret) {
      first_error_code_ = OB_SUCCESS == first_error_code_ ? ret : first_error_code_;
      STORAGE_LOG(WARN, "set first_error_code_", K(ret), K(first_error_code_), K(report_list));
    } else {
      const int64_t now = ObTimeUtility::current_time();
      if (now > retry_job_start_ts_ + RETRY_JOB_MAX_WAIT_TIMEOUT) {
        ret = OB_TIMEOUT;
        first_error_code_ = OB_SUCCESS == first_error_code_ ? ret : first_error_code_;
        STORAGE_LOG(WARN, "set first_error_code_", K(ret), K(first_error_code_), K(report_list));
      } else {
        STORAGE_LOG(WARN, "fail to check member major sstable enough, retry it", K(ret));
      }
    }
  }
  return ret;
}

int ObPartGroupMigrationTask::check_member_pg_major_sstable_enough(ObReportPartMigrationTask &task)
{
  int ret = OB_SUCCESS;
  ObMigrateSrcInfo leader_info;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup *partition = NULL;
  ObPGStorage *pg_storage = NULL;
  ObTablesHandle tables_handle;
  ObArray<uint64_t> table_ids;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartGroupMigrationTask has not been inited", K(ret));
  } else if (ObReplicaTypeCheck::is_replica_with_ssstore(task.arg_.dst_.get_replica_type()) &&
             (ADD_REPLICA_OP != task.arg_.type_ &&
                 MIGRATE_REPLICA_OP != task.arg_.type_
                 //          || CHANGE_REPLICA_OP != task.arg_.arg_.type_ // TODO(): open it after add batch check
                 && REBUILD_REPLICA_OP != task.arg_.type_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "failed to check member pg major sstable enough", K(ret), K(task));
  } else if (OB_FAIL(ObMigrateGetLeaderUtil::get_leader(task.arg_.key_, leader_info, true /*force_update*/))) {
    STORAGE_LOG(WARN, "failed to get leader address", K(ret), K(task.arg_.key_));
  } else if (OB_FAIL(partition_service_->get_partition(task.arg_.key_, guard))) {
    STORAGE_LOG(WARN, "failed to get partition group", K(ret), K(task.arg_));
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "partition group should not be NULL", K(ret), KP(partition));
  } else if (OB_ISNULL(pg_storage = &(partition->get_pg_storage()))) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "pg_storage should not be NULL", K(ret), KP(pg_storage));
  } else if (OB_FAIL(pg_storage->get_last_all_major_sstable(tables_handle))) {
    STORAGE_LOG(WARN, "failed to get last all major sstable", K(ret), K(task));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
      ObITable *table = tables_handle.get_tables().at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "error sys, table must not be NULL", K(ret));
      } else if (OB_FAIL(table_ids.push_back(table->get_key().table_id_))) {
        STORAGE_LOG(WARN, "failed to push table id into array", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      obrpc::ObMemberMajorSSTableCheckArg arg;
      arg.pkey_ = task.arg_.key_;
      if (OB_FAIL(arg.table_ids_.assign(table_ids))) {
        STORAGE_LOG(WARN, "fail to assign table ids", K(ret));
      } else if (OB_FAIL(partition_service_->get_pts_rpc().check_member_pg_major_sstable_enough(
                     leader_info.src_addr_, arg))) {
        STORAGE_LOG(WARN, "fail to check member pg major sstable enough", K(ret), K(arg));
        if (OB_NOT_SUPPORTED == ret) {
          if (REBUILD_REPLICA_OP == task.arg_.type_) {

            // not supported may rpc send to old server, for compat
            STORAGE_LOG(WARN, "result is not supported, may src server not support the rpc", K(ret));
            ret = OB_SUCCESS;
          } else if (OB_FAIL(partition_service_->get_pts_rpc().check_member_major_sstable_enough(
                         leader_info.src_addr_, arg))) {
            STORAGE_LOG(WARN, "fail to check member major sstable enough", K(ret), K(leader_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartGroupMigrationTask::report_meta_table(
    const ObIArray<ObReportPartMigrationTask> &report_list, const hash::ObHashSet<ObPartitionKey> &member_removed_pkeys)
{
  int ret = OB_SUCCESS;
  const int64_t ONE_ROUND_WAIT_TIMEOUT = 60 * 1000 * 1000;  // 1m
  const int64_t RETRY_INTERVAL = 100 * 1000;                // 100ms
  const int64_t start_ts = ObTimeUtility::current_time();
  ObArray<bool> check_result;
  FLOG_INFO("start report meta table", K(report_list));

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartGroupMigrationTask has not been inited", K(ret));
  } else if (OB_UNLIKELY(report_list.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(report_list.count()));
  } else if (OB_FAIL(check_result.reserve(report_list.count()))) {
    STORAGE_LOG(WARN, "fail to reserve check result", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
      const ObReportPartMigrationTask &report_task = report_list.at(i);
      if (OB_FAIL(partition_service_->handle_report_meta_table_callback(
              report_task.arg_.key_, report_task.result_, report_task.need_report_checksum_))) {
        STORAGE_LOG(WARN, "fail to report meta table", K(ret));
      }
    }
    if (OB_SUCC(ret) && !has_error()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
        const ObReportPartMigrationTask &report_task = report_list.at(i);
        bool need_check = ObReplicaTypeCheck::is_replica_with_ssstore(report_task.arg_.dst_.get_replica_type());
        if (FAST_MIGRATE_REPLICA_OP == type_) {
          int exist_ret = member_removed_pkeys.exist_refactored(report_list.at(i).arg_.key_);
          if (OB_HASH_EXIST != exist_ret && OB_HASH_NOT_EXIST != exist_ret) {
            ret = exist_ret;
            LOG_WARN("check is partition member list removed fail", K(ret), K(i), "report_task", report_list.at(i));
          } else if (OB_HASH_NOT_EXIST == exist_ret) {
            need_check = false;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(check_result.push_back(!need_check))) {
          STORAGE_LOG(WARN, "fail to push back check result", K(ret));
        }
      }
    }
    // If the previous migration fails, there is no need to wait
    while (OB_SUCC(ret) && !has_error()) {
      if (OB_FAIL(check_report_done(report_list, check_result))) {
        STORAGE_LOG(WARN, "fail to check report done", K(ret));
      } else {
        bool is_all_done = true;
        for (int64_t i = 0; OB_SUCC(ret) && i < check_result.count() && is_all_done; ++i) {
          is_all_done = check_result.at(i);
        }
        if (is_all_done) {
          break;
        } else {
          const int64_t now = ObTimeUtility::current_time();
          if (now > retry_job_start_ts_ + RETRY_JOB_MAX_WAIT_TIMEOUT) {
            ret = OB_TIMEOUT;
            first_error_code_ = OB_SUCCESS == first_error_code_ ? ret : first_error_code_;
            STORAGE_LOG(WARN, "wait report meta table done timeout, migration failed", K(ret));
          } else if (now > start_ts + ONE_ROUND_WAIT_TIMEOUT) {
            ret = OB_TIMEOUT;
            STORAGE_LOG(WARN, "wait report meta table done timeout, retry later", K(ret));
          } else {
            usleep(RETRY_INTERVAL);
          }
        }
      }
    }
  }
  const int64_t end_ts = ObTimeUtility::current_time();
  FLOG_INFO("wait report meta table done", "cost_ts", end_ts - start_ts, K(ret));
  return ret;
}

int ObPartGroupMigrationTask::check_report_done(
    const ObIArray<ObReportPartMigrationTask> &report_list, ObIArray<bool> &result)
{
  int ret = OB_SUCCESS;
  share::ObPartitionTableOperator *pt_operator = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartGroupMigrationTask has not been inited", K(ret));
  } else if (report_list.count() <= 0 || (report_list.count() != result.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(report_list.count()), K(result.count()));
  } else if (OB_ISNULL(pt_operator = GCTX.pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, pt operator must not be NULL", K(ret));
  } else {
    share::ObPartitionInfo partition_info;
    share::ObReplicaStatus replica_status;
    ObArenaAllocator allocator(ObModIds::OB_PARTITION_MIGRATOR);
    partition_info.set_allocator(&allocator);
    for (int64_t i = 0; OB_SUCC(ret) && i < result.count(); ++i) {
      const ObPartitionReplica *replica = NULL;
      partition_info.reuse();
      if (OB_FAIL(partition_service_->get_replica_status(report_list.at(i).arg_.key_, replica_status))) {
        STORAGE_LOG(WARN, "fail to get replica status", K(ret));
      } else if (REPLICA_STATUS_NORMAL != replica_status) {
        ret = OB_STATE_NOT_MATCH;
        first_error_code_ = OB_SUCCESS == first_error_code_ ? ret : first_error_code_;
        STORAGE_LOG(
            WARN, "partition status not match, migration failed", K(report_list.at(i).arg_.key_), K(replica_status));
      } else if (!result.at(i)) {
        if (OB_FAIL(pt_operator->get(report_list.at(i).arg_.key_.get_table_id(),
                report_list.at(i).arg_.key_.get_partition_id(),
                partition_info))) {
          STORAGE_LOG(WARN, "fail to get partition info.", K(ret), "pkey", report_list.at(i).arg_.key_);
        } else if (OB_FAIL(partition_info.find(GCTX.self_addr_, replica))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            STORAGE_LOG(WARN, "fail to find replica", K(ret), K(GCTX.self_addr_));
          } else {
            ret = OB_SUCCESS;
          }
        } else if (OB_ISNULL(replica)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "replica must not be NULL", K(ret));
        } else {
          result.at(i) = REPLICA_STATUS_NORMAL == replica->status_;
        }
      }
    }
  }
  return ret;
}

int ObPartGroupMigrationTask::wait_batch_member_change_done(
    const ObAddr &leader_addr, ObChangeMemberCtxs &change_member_info)
{
  int ret = OB_SUCCESS;
  int64_t max_wait_batch_member_change_done_us = 300 * 1000 * 1000;  // 5m
  const int64_t start_ts = ObTimeUtility::current_time();

#ifdef ERRSIM
  int64_t tmp = GCONF.max_wait_batch_member_change_done_us;
  if (tmp != max_wait_batch_member_change_done_us) {
    max_wait_batch_member_change_done_us = tmp;
    STORAGE_LOG(INFO, "fake max_wait_batch_member_change_done_us", K(max_wait_batch_member_change_done_us));
  }
#endif
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      const int64_t now = ObTimeUtility::current_time();
      if (now > start_ts + max_wait_batch_member_change_done_us) {
        ret = OB_TIMEOUT;
        STORAGE_LOG(WARN, "wait batch member change done retry timeout", K(ret));
        break;
      } else if (OB_FAIL(
                     partition_service_->get_pts_rpc().is_batch_member_change_done(leader_addr, change_member_info))) {
        if (OB_PARTIAL_FAILED != ret) {
          STORAGE_LOG(WARN, "failed to is_batch_member_change_done", K(ret), K(leader_addr), K(change_member_info));
        } else {
          STORAGE_LOG(INFO, "failed to is_batch_member_change_done", K(ret), K(leader_addr), K(change_member_info));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < change_member_info.count(); ++i) {
          if (OB_SUCCESS != change_member_info.at(i).ret_value_) {
            ret = OB_ERR_SYS;
            STORAGE_LOG(ERROR,
                "is_batch_member_change_done return OB_SUCCESS, butn has failed sub part",
                K(ret),
                K(i),
                K(change_member_info.at(i)));
          }
        }
        break;
      }

      if (OB_PARTIAL_FAILED == ret) {
        for (int64_t i = 0; i < change_member_info.count(); ++i) {
          if (OB_EAGAIN == change_member_info.at(i).ret_value_) {
            STORAGE_LOG(
                INFO, "some partition change member is not finish, need retry", K(i), K(change_member_info.at(i)));
            ret = OB_SUCCESS;
            break;
          }
        }
      } else if (OB_TIMEOUT == ret) {
        STORAGE_LOG(INFO, "send is_batch_member_change_done fail, need retry", K(ret));
        ret = OB_SUCCESS;
      }

      if (OB_SUCC(ret)) {
        usleep(100 * 1000);  // 100ms
      }
    }
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("finish wait_batch_member_change_done", K(ret), K(cost_ts));
  return ret;
}

int ObPartGroupMigrationTask::try_not_batch_change_member_list(
    ObIArray<ObReportPartMigrationTask> &report_list, bool &is_all_finish)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool could_retry = false;
  SMART_VAR(ObPartMigrationTask, tmp_task)
  {
    const int64_t start_ts = ObTimeUtility::current_time();
    is_all_finish = false;

    if (!is_inited_) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "not inited", K(ret));
    } else if (report_list.count() <= 0) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "task list is empty", K(ret));
    } else {
      ObReportPartMigrationTask &first_task = report_list.at(0);

      if (OB_SUCCESS != (tmp_ret = partition_service_->handle_member_change_callback(
                             first_task.arg_, first_task.result_, could_retry))) {
        if (MIGRATE_REPLICA_OP == type_ && could_retry && need_retry_change_member_list(tmp_ret)) {
          STORAGE_LOG(WARN, "failed to handle_replica_callback, retry later", K(tmp_ret), K(first_task));
        } else {
          is_all_finish = true;
          STORAGE_LOG(WARN,
              "failed to handle_replica_callback, failed all sub tasks",
              K(tmp_ret),
              K(could_retry),
              K(first_task));
          if (OB_FAIL(build_failed_report_list(tmp_ret, report_list))) {
            STORAGE_LOG(WARN, "failed to build failed report list", K(ret));
          }
        }
      } else {
        first_task.ctx_->is_member_change_finished_ = true;
        is_all_finish = true;

        for (int64_t i = 1; OB_SUCC(ret) && i < report_list.count(); ++i) {
          ObReportPartMigrationTask &sub_task = report_list.at(i);
          if (OB_SUCCESS != (tmp_ret = partition_service_->handle_member_change_callback(
                                 sub_task.arg_, sub_task.result_, could_retry))) {
            sub_task.result_ = tmp_ret;
            STORAGE_LOG(WARN, "failed to handle_replica_callback,", K(tmp_ret), K(sub_task));
          } else {
            sub_task.ctx_->is_member_change_finished_ = true;
          }
        }
      }

      const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
      STORAGE_LOG(INFO,
          "finish try_not_batch_change_member_list",
          "count",
          report_list.count(),
          K(start_ts),
          K(cost_ts),
          "avg_cost",
          cost_ts / report_list.count());
    }
  }
  return ret;
}

bool ObPartGroupMigrationTask::need_retry_change_member_list(const int32_t result_code)
{
  bool need_retry = false;
  const int64_t change_member_list_timeout = GCONF.sys_bkgd_migration_change_member_list_timeout;
  const int64_t now = ObTimeUtility::current_time();
  int64_t large_migration_need_retry_cost_time = LARGE_MIGRATION_NEED_RETRY_COST_TIME;

#ifdef ERRSIM
  large_migration_need_retry_cost_time = GCONF.sys_bkgd_migration_large_task_threshold;
  STORAGE_LOG(INFO, "use GCONF.sys_bkgd_migration_large_task_threshold", K(large_migration_need_retry_cost_time));
#endif

  if (!is_inited_) {
    STORAGE_LOG(ERROR, "not inited");
  } else {
    // Retrial condition
    // 1. It tasks more than 2 hours to copy data
    // 2. The default retry times has not been exceeded
    // 3. Match specific error code
    need_retry = true;
    if (change_member_list_timeout <= 0) {
      need_retry = false;
      STORAGE_LOG(INFO, "change member list time out is 0, no need retry", K(change_member_list_timeout));
    } else if (start_change_member_ts_ - schedule_ts_ < large_migration_need_retry_cost_time) {
      need_retry = false;
      STORAGE_LOG(INFO,
          "no large migration, no need retry",
          K(schedule_ts_),
          K(start_change_member_ts_),
          K(large_migration_need_retry_cost_time));
    } else if (now > start_change_member_ts_ + change_member_list_timeout) {
      need_retry = false;
      STORAGE_LOG(INFO,
          "retry timeout expired, no need retry",
          K(now),
          K(start_change_member_ts_),
          K(change_member_list_timeout));
    } else if (result_code != OB_STATE_NOT_MATCH && result_code != OB_LOG_NOT_SYNC) {
      need_retry = false;
      STORAGE_LOG(INFO, "no need retry for these error", K(result_code));
    }
  }
  return need_retry;
}

int ObPartGroupMigrationTask::inner_schedule_partition(ObPartMigrationTask *&task, bool &need_schedule)
{
  int ret = OB_SUCCESS;
  need_schedule = false;
  task = NULL;
  common::SpinRLockGuard guard(lock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
    task = &task_list_[i];
    bool as_data_source = false;
    if (type_ != task->arg_.type_ || !is_replica_op_valid(task->arg_.type_)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "invalid type, cannot handle this task", K(ret), K(i), K(*task));
    } else if (REMOVE_REPLICA_OP == type_) {
      // do nothing
    } else if (OB_FAIL(check_can_as_data_source(type_,
                   task->arg_.data_src_.get_replica_type(),
                   task->arg_.dst_.get_replica_type(),
                   as_data_source))) {
      STORAGE_LOG(WARN, "fail to check can as data source", K(ret), K(task->arg_));
    } else if (!as_data_source) {
      ret = OB_NOT_SUPPORTED;
      first_error_code_ = ret;
      LOG_ERROR("invalid data src", K(ret), K(task->arg_));
    } else if (ObPartMigrationTask::INIT == task->status_) {
      if (OB_FAIL(build_migrate_ctx(task->arg_, task->ctx_))) {
        STORAGE_LOG(WARN, "fail to build migrate ctx", K(ret));
      } else {
        need_schedule = true;
        break;
      }
    }
  }
  return ret;
}

int ObPartGroupMigrationTask::try_schedule_partition_migration()
{
  int ret = OB_SUCCESS;
  bool need_schedule = true;
  ObPartMigrationTask *task = NULL;
  const int64_t data_copy_in_concurrency = GCONF.server_data_copy_in_concurrency;

  while (OB_SUCC(ret) && need_schedule &&
         ObDagScheduler::get_instance().get_dag_count(ObIDag::DAG_TYPE_MIGRATE) < data_copy_in_concurrency) {
    if (OB_FAIL(inner_schedule_partition(task, need_schedule))) {
      STORAGE_LOG(WARN, "failed to inner schedule partition", K(ret));
    } else if (!need_schedule) {
      // do nothing
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "task need schedule but is NULL", K(ret), KP(task));
    } else if (OB_FAIL(schedule_migrate_dag(task->ctx_))) {
      STORAGE_LOG(WARN, "failed to schedule migrate dag", K(ret), K(task->arg_));
    }
  }
  return ret;
}

int ObPartGroupMigrationTask::try_schedule_partition_validate()
{
  int ret = OB_SUCCESS;
  bool need_schedule = true;
  ObPartMigrationTask *task = NULL;
  int64_t data_validate_concurrency = GCONF.backup_concurrency;
  int32_t up_limit = 0;
  if (OB_FAIL(ObDagScheduler::get_instance().get_up_limit(ObIDag::DAG_ULT_BACKUP, up_limit))) {
    STORAGE_LOG(WARN, "failed to get up limit", K(ret));
  } else {
    data_validate_concurrency = data_validate_concurrency == 0 ? up_limit : data_validate_concurrency;
    FLOG_INFO("validate concurrency", K(data_validate_concurrency), K(up_limit));
  }

  while (OB_SUCC(ret) && need_schedule &&
         ObDagScheduler::get_instance().get_dag_count(ObIDag::DAG_TYPE_VALIDATE) < data_validate_concurrency) {
    if (OB_FAIL(inner_schedule_partition(task, need_schedule))) {
      STORAGE_LOG(WARN, "failed to inner schedule partiion", K(ret));
    } else if (!need_schedule) {
      // do nothing
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "task need schedule but is NULL", K(ret), KP(task));
    } else if (OB_FAIL(schedule_migrate_dag(task->ctx_))) {
      STORAGE_LOG(WARN, "failed to schedule migrate dag", K(ret), K(task->arg_));
    }
  }
  return ret;
}

int ObPartGroupMigrationTask::try_schedule_partition_backup_backupset()
{
  int ret = OB_SUCCESS;
  bool need_schedule = true;
  ObPartMigrationTask *task = NULL;
  int64_t data_backup_concurrency = GCONF.backup_concurrency;
  int32_t up_limit = 0;
  if (OB_FAIL(ObDagScheduler::get_instance().get_up_limit(ObIDag::DAG_ULT_BACKUP, up_limit))) {
    STORAGE_LOG(WARN, "failed to get up limit", K(ret));
  } else {
    data_backup_concurrency = data_backup_concurrency == 0 ? up_limit : data_backup_concurrency;
    FLOG_INFO("backup concurrency", K(data_backup_concurrency), K(up_limit));
  }

  while (OB_SUCC(ret) && need_schedule &&
         ObDagScheduler::get_instance().get_dag_count(ObIDag::DAG_TYPE_BACKUP_BACKUPSET) < data_backup_concurrency) {
    if (OB_FAIL(inner_schedule_partition(task, need_schedule))) {
      STORAGE_LOG(WARN, "failed to inner schedule partition", K(ret));
    } else if (!need_schedule) {
      STORAGE_LOG(INFO, "no need schedule");
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "task need schedule but is NULL", K(ret), KP(task));
    } else if (OB_FAIL(schedule_migrate_dag(task->ctx_))) {
      STORAGE_LOG(WARN, "failed to schedule migrate dag", K(ret), K(task->arg_));
    }
  }
  return ret;
}

int ObPartGroupMigrationTask::try_schedule_partition_backup_archivelog()
{
  int ret = OB_SUCCESS;
  bool need_schedule = true;
  ObPartMigrationTask *task = NULL;
  int64_t data_backup_concurrency = GCONF.backup_concurrency;
  int32_t up_limit = 0;
  if (OB_FAIL(ObDagScheduler::get_instance().get_up_limit(ObIDag::DAG_ULT_BACKUP, up_limit))) {
    STORAGE_LOG(WARN, "failed to get up limit", K(ret));
  } else {
    data_backup_concurrency = data_backup_concurrency == 0 ? up_limit : data_backup_concurrency;
    FLOG_INFO("backup concurrency", K(data_backup_concurrency), K(up_limit));
  }

  while (OB_SUCC(ret) && need_schedule &&
         ObDagScheduler::get_instance().get_dag_count(ObIDag::DAG_TYPE_BACKUP_ARCHIVELOG) < data_backup_concurrency) {
    if (OB_FAIL(inner_schedule_partition(task, need_schedule))) {
      STORAGE_LOG(WARN, "failed to inner schedule partition", K(ret));
    } else if (!need_schedule) {
      STORAGE_LOG(INFO, "no need schedule");
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "task need schedule but is NULL", K(ret), KP(task));
    } else if (OB_FAIL(schedule_migrate_dag(task->ctx_))) {
      STORAGE_LOG(WARN, "failed to schedule migrate dag", K(ret), K(task->arg_));
    }
  }
  return ret;
}

// caller hold task_list_ lock
int ObPartGroupMigrationTask::get_migrate_task(const ObPartitionKey &pg_key, ObPartMigrationTask *&task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get migrate task get invalid argument", K(ret), K(pg_key));
  } else {
    for (int64_t i = 0; NULL == task && i < task_list_.count(); ++i) {
      if (task_list_[i].arg_.key_ == pg_key) {
        task = &task_list_[i];
      }
    }
    if (OB_ISNULL(task)) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(ERROR, "cannot found pkey in task", K(ret), K(pg_key), KP(this), K(*this));
    }
  }
  return ret;
}
// TODO(muwei): fix it
// int ObPartGroupMigrationTask::set_migrate_task_flags(
//    const ObPartitionKey &pg_key,
//    const ObMigrateStatus &status,
//    const bool is_restore)
//{
//  int ret = OB_SUCCESS;
//  ObPartMigrationTask *task = NULL;
//  if (!is_inited_) {
//    ret = OB_NOT_INIT;
//    STORAGE_LOG(WARN, "not inited", K(ret));
//  } else if (!pg_key.is_valid() || status >= ObMigrateStatus::OB_MIGRATE_STATUS_MAX) {
//    ret = OB_INVALID_ARGUMENT;
//    STORAGE_LOG(WARN, "get migrate task get invalid argument", K(ret),
//        K(pg_key), K(status), K(is_restore));
//  } else {
//    common::SpinWLockGuard guard(lock_);
//    if (OB_FAIL(get_migrate_task(pg_key, task))) {
//      STORAGE_LOG(WARN, "failed to get migrate task", K(ret), K(pg_key));
//    } else if (OB_FAIL(set_migrate_task_flags_(status, is_restore, *task))) {
//      STORAGE_LOG(WARN, "failed to set migrate task flags", K(ret), K(pg_key), K(status));
//    }
//  }
//  return ret;
//}

int ObPartGroupMigrationTask::init_restore_meta_index_(const share::ObPhysicalRestoreArg &restore_arg)
{
  int ret = OB_SUCCESS;
  const bool is_compat_backup_path = !restore_arg.restore_info_.is_compat_backup_path();
  const int64_t compatible = restore_arg.restore_info_.compatible_;
  bool need_check_compeleted = true;
  const bool need_check_all_meta_files = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!restore_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("restore arg is invlaid, pelase check", K(ret), K(restore_arg));
  } else if (FALSE_IT(need_check_compeleted = (restore_arg.restore_info_.cluster_version_ > CLUSTER_VERSION_2271))) {
  } else {
    if (is_compat_backup_path) {
      share::ObSimpleBackupSetPath simple_path;
      if (OB_FAIL(restore_arg.get_largest_backup_set_path(simple_path))) {
        LOG_WARN("failed to get largest backup set path", K(restore_arg));
        ;
      } else if (OB_FAIL(
                     meta_indexs_.init(simple_path, compatible, need_check_all_meta_files, need_check_compeleted))) {
        LOG_WARN("init restore meta index map fail", K(ret), K(restore_arg));
      }
    } else {
      ObBackupBaseDataPathInfo path_info;
      if (OB_FAIL(restore_arg.get_backup_base_data_info(path_info))) {
        LOG_WARN("failed to get backup base data info", K(ret), K(restore_arg));
      } else if (OB_FAIL(meta_indexs_.init(path_info, compatible, need_check_all_meta_files, need_check_compeleted))) {
        LOG_WARN("init restore meta index map fail", K(ret), K(restore_arg));
      }
    }
  }
  return ret;
}

// caller hold task_list_ lock
int ObPartGroupMigrationTask::set_migrate_task_flags_(
    const ObMigrateStatus &status, const bool is_restore, ObPartMigrationTask &task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    task.need_reset_migrate_status_ = true;
    if (status == OB_MIGRATE_STATUS_ADD_FAIL || status == OB_MIGRATE_STATUS_MIGRATE_FAIL) {
      task.during_migrating_ = true;
    }
    if (ADD_REPLICA_OP == task.arg_.type_ && is_restore) {
      skip_change_member_list_ = true;
      STORAGE_LOG(INFO, "add replica op when restore skip set member list", "pkey", task.ctx_.replica_op_arg_.key_);
    }
  }
  return ret;
}

int ObGroupMigrateDag::report_result(const bool is_batch_mode, const ObIArray<ObReportPartMigrationTask> &report_list)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!is_batch_mode) {
    if (OB_FAIL(single_report_result(report_list))) {
      STORAGE_LOG(WARN, "failed to single_report_result", K(ret));
    }
  } else {
    if (OB_FAIL(batch_report_result(report_list))) {
      STORAGE_LOG(WARN, "failed to batch_report_result", K(ret));
    }
  }
  return ret;
}

int ObGroupMigrateDag::single_report_result(const ObIArray<ObReportPartMigrationTask> &report_list)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (report_list.count() != 1) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "cannot single report result more than 1", K(ret), K(report_list));
  } else {
    const ObReportPartMigrationTask &task = report_list.at(0);
    if (OB_FAIL(partition_service_->retry_post_operate_replica_res(task.arg_, task.result_))) {
      STORAGE_LOG(WARN, "failed to retry_post_operate_replica_res", K(ret), K(task));
    }
  }
  return ret;
}

int ObGroupMigrateDag::batch_report_result(const ObIArray<ObReportPartMigrationTask> &report_list)
{
  int ret = OB_SUCCESS;
  ObArray<ObPartMigrationRes> report_res_list;
  ObPartMigrationRes tmp_res;
  ObReplicaOpType type;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (report_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "report list must not empty", K(ret));
  } else if (FALSE_IT(type = report_list.at(0).arg_.type_)) {
  } else if (OB_FAIL(ObMigrateUtil::get_report_result(report_list, report_res_list))) {
    LOG_WARN("failed to get report result", K(ret), K(report_list));
  } else if (OB_FAIL(partition_service_->retry_post_batch_migrate_replica_res(type, report_res_list))) {
    STORAGE_LOG(WARN, "failed to retry_post_operate_replica_res", K(ret), K(type), K(report_res_list));
  }
  return ret;
}

int ObPartGroupMigrationTask::get_tables_with_major_sstable(const ObPartitionKey &pkey, ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard part_guard;
  ObIPartitionGroup *partition = NULL;
  ObPartitionStorage *storage = NULL;
  ObPGPartitionGuard pg_partition_guard;
  ObTablesHandle tables_handle;
  table_ids.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartGroupMigrationTask has not been inited", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(pkey));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pkey, part_guard))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_ISNULL(partition = part_guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition must not be NULL", K(ret), K(pkey));
  } else if (OB_FAIL(partition->get_pg_partition(pkey, pg_partition_guard))) {
    STORAGE_LOG(WARN, "get pg partition error", K(ret), K(pkey));
  } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get pg partition error", K(ret), KP(pg_partition_guard.get_pg_partition()));
  } else if (OB_ISNULL(
                 storage = static_cast<ObPartitionStorage *>(pg_partition_guard.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition storage must not be NULL", K(ret));
  } else if (OB_FAIL(storage->get_partition_store().get_last_all_major_sstable(tables_handle))) {
    STORAGE_LOG(WARN, "fail to get last all major sstables", K(ret), K(pkey));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
      ObITable *table = tables_handle.get_tables().at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "error sys, table must not be NULL", K(ret));
      } else if (OB_FAIL(table_ids.push_back(table->get_key().table_id_))) {
        STORAGE_LOG(WARN, "fail to push back table id", K(ret));
      }
    }
  }
  return ret;
}

ObFastMigrateDag::ObFastMigrateDag()
    : ObIDag(ObIDagType::DAG_TYPE_MIGRATE, ObIDagPriority::DAG_PRIO_MIGRATE_MID),
      group_task_(nullptr),
      sub_type_(TaskType::INVALID)
{}

ObFastMigrateDag::~ObFastMigrateDag()
{
  group_task_ = nullptr;
}

bool ObFastMigrateDag::operator==(const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObFastMigrateDag &other_dag = static_cast<const ObFastMigrateDag &>(other);
    is_same = (group_task_ == other_dag.group_task_ && sub_type_ == other_dag.sub_type_);
  }
  return is_same;
}

int64_t ObFastMigrateDag::hash() const
{
  int64_t hash_value = 0;
  if (nullptr != group_task_) {
    hash_value = common::murmurhash(&group_task_, sizeof(group_task_), hash_value);
  }
  if (is_valid_task_type(sub_type_)) {
    hash_value = common::murmurhash(&sub_type_, sizeof(sub_type_), hash_value);
  }
  return hash_value;
}

int64_t ObFastMigrateDag::get_tenant_id() const
{
  int64_t tenant_id = OB_INVALID_TENANT_ID;
  if (nullptr != group_task_) {
    tenant_id = group_task_->get_tenant_id();
  }
  return tenant_id;
}

int ObFastMigrateDag::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    const char *type_str = "UNKOWN";
    static const char *TYPE_STRING[] = {
        "INVALID",
        "SUSPEND_SRC",
        "HANDOVER_PG",
        "CLEAN_UP",
        "REPORT_META_TABLE",
    };
    STATIC_ASSERT(ARRAYSIZEOF(TYPE_STRING) == TaskType::MAX_TYPE, "TYPE_STRING count mismatch with task type count");
    if (is_valid_task_type(sub_type_)) {
      type_str = TYPE_STRING[sub_type_];
    }
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "fast migrate batch exec: "))) {
      LOG_WARN("generate comment failed", K(ret));
    } else if (OB_FAIL(databuff_print_json_kv(buf, buf_len, pos, "type", type_str))) {
      LOG_WARN("generate sub_type failed", K(ret));
    } else if (OB_FAIL(databuff_print_json_kv_comma(buf, buf_len, pos, K(group_task_)))) {
      LOG_WARN("generate group task addr failed", K(ret));
    } else if (nullptr != group_task_ &&
               OB_FAIL(databuff_print_json_kv_comma(buf, buf_len, pos, "trace_id", group_task_->get_task_id()))) {
      LOG_WARN("generate trace id failed", K(ret));
    } else if (OB_FAIL(
                   databuff_print_json_kv_comma(buf, buf_len, pos, "running_task_count", get_running_task_count()))) {
      LOG_WARN("generate group task addr failed", K(ret));
    }
  }
  return ret;
}

int64_t ObFastMigrateDag::get_compat_mode() const
{
  return static_cast<int64_t>(share::ObWorker::CompatMode::MYSQL);
}

bool ObFastMigrateDag::is_valid_task_type(TaskType type)
{
  return type > TaskType::INVALID && type < TaskType::MAX_TYPE;
}

int ObFastMigrateDag::init(ObPartGroupMigrationTask *group_task, TaskType sub_type)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(group_task_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(group_task_));
  } else if (OB_ISNULL(group_task) || !is_valid_task_type(sub_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(group_task), K(sub_type));
  } else {
    group_task_ = group_task;
    sub_type_ = sub_type;
  }
  return ret;
}

ObBaseMigrateDag::ObBaseMigrateDag(const ObIDagType type, const ObIDagPriority priority)
    : ObIDag(type, priority), is_inited_(false), ctx_(NULL), tenant_id_(0), compat_mode_()
{}

ObBaseMigrateDag::~ObBaseMigrateDag()
{}

bool ObBaseMigrateDag::operator==(const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObMigrateDag &other_dag = static_cast<const ObMigrateDag &>(other);
    if (NULL != ctx_ && NULL != other_dag.ctx_) {
      if (ctx_->replica_op_arg_.key_ != other_dag.ctx_->replica_op_arg_.key_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

int64_t ObBaseMigrateDag::hash() const
{
  int64_t hash_value = 0;

  if (NULL != ctx_) {
    hash_value = common::murmurhash(&ctx_->replica_op_arg_.key_, sizeof(ctx_->replica_op_arg_.key_), hash_value);
  }
  return hash_value;
}

void ObBaseMigrateDag::clear()
{
  ctx_ = NULL;
  is_inited_ = false;
  tenant_id_ = 0;
}

int ObBaseMigrateDag::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(ctx_->fill_comment(buf, buf_len))) {
    LOG_WARN("failed to fill comment", K(ret), K(*ctx_));
  }
  return ret;
}

ObMigrateDag::ObMigrateDag() : ObBaseMigrateDag(ObIDag::DAG_TYPE_MIGRATE, ObIDag::DAG_PRIO_MIGRATE_HIGH)
{}

ObMigrateDag::~ObMigrateDag()
{
  int tmp_ret = OB_SUCCESS;
  ObPartitionService *partition_service = MIGRATOR.get_partition_service();
  ObMigrateStatus migrate_status = OB_MIGRATE_STATUS_NONE;
  bool need_offline_partition = false;

  if (NULL != ctx_) {
    ctx_->calc_need_retry();
    ObReplicaOpArg tmp_relica_op_arg = ctx_->replica_op_arg_;
    ObIPartitionGroup *partition = ctx_->get_partition();
    if (OB_SUCCESS == ctx_->result_ && OB_SUCCESS != this->get_dag_ret()) {
      ctx_->result_ = this->get_dag_ret();
      LOG_WARN("migrate dag is failed", "result", this->get_dag_ret(), K(*ctx_));
    }

    if (OB_SUCCESS == ctx_->result_ && ctx_->need_rebuild_) {  // A state that should not exist
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("result success no need rebuild", K(tmp_ret), K(*ctx_));
      ctx_->result_ = tmp_ret;
      ctx_->need_rebuild_ = false;
    }

    if (OB_SUCCESS != ctx_->result_ && NULL != partition_service && NULL != partition) {
      if (OB_SUCCESS != (tmp_ret = ctx_->get_partition()->get_pg_storage().get_pg_migrate_status(migrate_status))) {
        LOG_WARN("failed to get_migrate_status", K(tmp_ret), K(*ctx_));
      } else if (OB_MIGRATE_STATUS_ADD == migrate_status || OB_MIGRATE_STATUS_MIGRATE == migrate_status) {
        if (OB_NOT_EXIST_REPLICA == ctx_->replica_state_ &&
            !partition_service->is_partition_exist(ctx_->replica_op_arg_.key_)) {
          if (OB_SUCCESS !=
              (tmp_ret = partition_service->get_election_mgr()->stop_partition(ctx_->replica_op_arg_.key_))) {
            STORAGE_LOG(WARN, "failed to stop partition in election mgr", K(tmp_ret), K(*ctx_));
          }
        } else if (OB_NORMAL_REPLICA != ctx_->replica_state_) {
          need_offline_partition = true;
          if (ctx_->need_rebuild_) {
            STORAGE_LOG(INFO, "rebuild offline partition without state switch", K(*ctx_));
            if (OB_SUCCESS != (tmp_ret = partition->pause())) {
              STORAGE_LOG(WARN, "fail to pause partition", K(tmp_ret), K(*ctx_));
            }
          } else if (FAST_MIGRATE_REPLICA_OP == ctx_->replica_op_arg_.type_ && ctx_->is_takeover_finished_) {
            // do nothing, fast_migrate_mgr will retry
            LOG_INFO("fast migrate: keep pg when takeover finished", K(ctx_->replica_op_arg_.key_));
          } else {
            // If the migration and replication of partition fail,
            // offline opertion is performed on the partition and the status is rolled backup to offline
            STORAGE_LOG(INFO, "rollback partition when migrate fail", K(*ctx_));
            ObPartitionState pstate = partition->get_partition_state();
            if (ObPartitionState::INIT == pstate) {
              if (OB_SUCCESS != (tmp_ret = partition->set_valid())) {
                STORAGE_LOG(WARN, "failed to set partition to valid", K(tmp_ret), KPC(ctx_));
              }
            } else if (OB_SUCCESS != (tmp_ret = partition->offline())) {
              STORAGE_LOG(WARN, "fail to offline partition", K(tmp_ret), K(*ctx_));
            }
          }
        } else {
          // For the rebuild replica opertion, if it fails, it will not be handled temporarily.
          // The reasion is that there may be trasactions in the rebuild replica state that have partitipated in the
          // voting. If it is handled, the majority will not be established.
        }
      } else if (OB_MIGRATE_STATUS_REBUILD == migrate_status) {
        if (OB_SUCCESS != (tmp_ret = online_for_rebuild())) {
          LOG_WARN("failed to online_for_rebuild", K(tmp_ret), K(*ctx_));
        }
      } else if (OB_MIGRATE_STATUS_RESTORE == migrate_status) {
        if (OB_RESTORE_PARTITION_IS_COMPELETE == ctx_->result_) {
          LOG_INFO("restore partition compelete", K(*ctx_));
          ctx_->need_rebuild_ = false;
          ctx_->result_ = OB_SUCCESS;
        }
      }
    }

    ctx_->finish_ts_ = ObTimeUtility::current_time();

    if (!ctx_->need_rebuild_) {
      ctx_->action_ = ObMigrateCtx::END;
      if (OB_SUCCESS == ctx_->result_ && VALIDATE_BACKUP_OP != ctx_->replica_op_arg_.type_) {
        ctx_->result_ = update_partition_meta_for_restore();
      } else if (RESTORE_STANDBY_OP == ctx_->replica_op_arg_.type_) {
        // this is for restore standby, cause standby need receive log to retry
        if (OB_SUCCESS != (tmp_ret = online_for_restore())) {
          STORAGE_LOG(WARN, "failed to online for restore", K(tmp_ret), K(ctx_->replica_op_arg_));
        }
      }

      if (OB_SUCCESS == ctx_->result_) {
        ctx_->during_migrating_ = false;
      }

      if (OB_SUCCESS != (tmp_ret = ctx_->update_partition_migration_status())) {
        STORAGE_LOG(WARN, "failed to update_partition_migration_status", K(tmp_ret));
      }

      if (OB_SUCCESS != (tmp_ret = ctx_->notice_finish_part_task())) {
        STORAGE_LOG(WARN, "failed to notice_finish_part_task", K(tmp_ret), K(tmp_relica_op_arg));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = ctx_->update_partition_migration_status())) {
        STORAGE_LOG(WARN, "failed to update_partition_migration_status", K(tmp_ret));
      }

      LOG_INFO("migrate retry", "replica_op_arg", ctx_->replica_op_arg_, "rebuild_count", ctx_->rebuild_count_);

      int tmp_result = ctx_->result_;
      ctx_->rebuild_migrate_ctx();
      // gereate_and_schedule_migrate_dag is successful, you should not access ctx content,
      // and there will be concurrency problems

      if (OB_SUCCESS != (tmp_ret = ctx_->generate_and_schedule_migrate_dag())) {
        LOG_WARN("failed to schedule migrate dag", K(tmp_ret), K(*ctx_));
        ctx_->result_ = tmp_result;  // set error state before rebuild
        ctx_->need_rebuild_ = false;
        ctx_->action_ = ObMigrateCtx::END;

        if (need_offline_partition) {
          STORAGE_LOG(INFO, "rollback partition when migrate fail", K(*ctx_));
          if (OB_SUCCESS != (tmp_ret = partition->offline())) {
            STORAGE_LOG(WARN, "fail to offline partition", K(tmp_ret), K(*ctx_));
          }
        }

        if (OB_SUCCESS != (tmp_ret = ctx_->update_partition_migration_status())) {
          STORAGE_LOG(WARN, "failed to update_partition_migration_status", K(tmp_ret));
        }

        if (OB_SUCCESS != (tmp_ret = ctx_->notice_finish_part_task())) {
          STORAGE_LOG(WARN, "failed to notice_finish_part_task", K(tmp_ret), K(tmp_relica_op_arg));
        }
      }
    }  // need rebuild

  }  // ctx != NULL
}

int ObMigrateDag::init(ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (!ctx.replica_op_arg_.is_valid() || OB_ISNULL(ctx.group_task_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(ctx.group_task_), K(ctx.replica_op_arg_));
  } else {
    ObReplicaRestoreStatus restore_status = ObReplicaRestoreStatus::REPLICA_NOT_RESTORE;
    tenant_id_ = ctx.replica_op_arg_.key_.get_tenant_id();
    if (ctx.replica_op_arg_.is_physical_restore_leader()) {
      if (OB_FAIL(init_for_restore_(ctx))) {
        LOG_WARN("failed to init for restore", K(ret));
      }
    } else if (RESTORE_REPLICA_OP == ctx.replica_op_arg_.type_) {
      if (OB_FAIL(ctx.restore_info_.init(ctx.replica_op_arg_.restore_arg_))) {
        STORAGE_LOG(WARN, "failed to init restore info", K(ret));
      }
    } else if (VALIDATE_BACKUP_OP == ctx.replica_op_arg_.type_) {
      ObBackupBaseDataPathInfo path_info;
      if (OB_FAIL(ctx.replica_op_arg_.validate_arg_.get_backup_base_data_info(path_info))) {
        STORAGE_LOG(WARN, "failed to get backup base data info", K(ret));
      } else if (!ctx.macro_index_store_.is_inited() && OB_FAIL(ctx.macro_index_store_.init(path_info))) {
        STORAGE_LOG(WARN, "failed to init macro index store", K(ret), K(path_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (ObReplicaOpPriority::PRIO_LOW == ctx.replica_op_arg_.priority_) {
        set_priority(ObIDag::DAG_PRIO_MIGRATE_LOW);
      } else if (ObReplicaOpPriority::PRIO_MID == ctx.replica_op_arg_.priority_) {
        set_priority(ObIDag::DAG_PRIO_MIGRATE_MID);
      } else {
        set_priority(ObIDag::DAG_PRIO_MIGRATE_HIGH);
      }

      if (OB_FAIL(ctx.notice_start_part_task())) {
        LOG_WARN("failed to notice_start_part_task", K(ret));
        ctx.result_ = ret;
      } else if (OB_FAIL(get_compat_mode_with_table_id(ctx.replica_op_arg_.key_.table_id_, compat_mode_))) {
        LOG_WARN("failed to get table compat mode", K(ret), K(ctx));
      } else {
        ctx_ = &ctx;
        is_inited_ = true;
        STORAGE_LOG(INFO, "succeed init migrate dag");
      }
    }
  }
  return ret;
}

int ObMigrateDag::init_for_restore_(ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObReplicaRestoreStatus restore_status = ObReplicaRestoreStatus::REPLICA_NOT_RESTORE;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup *partition = NULL;
  ObPartGroupMigrationTask *group_task = NULL;
  ObPGKey backup_pg_key;
  ObBackupMetaIndex meta_index;
  const int64_t compatible = ctx.replica_op_arg_.phy_restore_arg_.restore_info_.compatible_;

  if (OB_FAIL(ObPartitionService::get_instance().get_partition(ctx.replica_op_arg_.key_, guard))) {
    LOG_WARN("failed to get physical restore partition", K(ret), "pkey", ctx.replica_op_arg_.key_);
  } else if (OB_ISNULL(partition = guard.get_partition_group())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "partition must not null", K(ret), K(ctx.replica_op_arg_));
  } else if (FALSE_IT(restore_status =
                          static_cast<ObReplicaRestoreStatus>(partition->get_pg_storage().get_restore_state()))) {
  } else if (ObReplicaRestoreStatus::REPLICA_RESTORE_ARCHIVE_DATA == restore_status) {
    STORAGE_LOG(INFO,
        "restore status is replica restore archive data, no need init for restore",
        "pkey",
        ctx.replica_op_arg_.key_);
  } else if (OB_FAIL(ctx.replica_op_arg_.phy_restore_arg_.get_backup_pgkey(backup_pg_key))) {
    STORAGE_LOG(WARN, "failed to get backup pgkey", K(ret), K(ctx.replica_op_arg_.phy_restore_arg_));
  } else if (FALSE_IT(group_task = reinterpret_cast<ObPartGroupMigrationTask *>(ctx.group_task_))) {
  } else if (OB_ISNULL(ctx.macro_indexs_)) {
    if (OB_BACKUP_COMPATIBLE_VERSION_V3 == compatible || OB_BACKUP_COMPATIBLE_VERSION_V4 == compatible) {
      const ObBackupMetaType meta_type = ObBackupMetaType::PARTITION_GROUP_META_INFO;
      ObPhyRestoreMacroIndexStoreV2 *phy_restore_macro_index_v2 = NULL;
      if (OB_ISNULL(phy_restore_macro_index_v2 = MIGRATOR.get_cp_fty()->get_phy_restore_macro_index_v2())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy restore macro index should not be NULL", K(ret), KP(phy_restore_macro_index_v2));
      } else if (OB_FAIL(group_task->get_meta_indexs().get_meta_index(backup_pg_key, meta_type, meta_index))) {
        STORAGE_LOG(WARN, "failed to get meta index", K(ret), K(backup_pg_key), K(meta_index));
      } else if (OB_FAIL(phy_restore_macro_index_v2->init(
                     meta_index.task_id_, ctx.replica_op_arg_.phy_restore_arg_, restore_status))) {
        LOG_WARN("failed to int physcial restore macro index", K(ret));
      } else {
        ctx.macro_indexs_ = phy_restore_macro_index_v2;
        phy_restore_macro_index_v2 = NULL;
      }
      if (OB_NOT_NULL(phy_restore_macro_index_v2)) {
        MIGRATOR.get_cp_fty()->free(phy_restore_macro_index_v2);
        ctx.macro_indexs_ = NULL;
      }
    } else if (OB_BACKUP_COMPATIBLE_VERSION_V1 == compatible || OB_BACKUP_COMPATIBLE_VERSION_V2 == compatible) {
      ObPhyRestoreMacroIndexStore *phy_restore_macro_index = NULL;
      if (OB_ISNULL(phy_restore_macro_index = MIGRATOR.get_cp_fty()->get_phy_restore_macro_index())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy restore macro index should not be NULL", K(ret), KP(phy_restore_macro_index));
      } else if (OB_FAIL(phy_restore_macro_index->init(ctx.replica_op_arg_.phy_restore_arg_, restore_status))) {
        LOG_WARN("failed to int physcial restore macro index", K(ret));
      } else {
        ctx.macro_indexs_ = phy_restore_macro_index;
        phy_restore_macro_index = NULL;
      }
      if (OB_NOT_NULL(phy_restore_macro_index)) {
        MIGRATOR.get_cp_fty()->free(phy_restore_macro_index);
        ctx.macro_indexs_ = NULL;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("restore get invalid compatible version", K(ret), K(compatible));
    }
  } else if (!ctx.macro_indexs_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro index is not NULL but not init", K(ret), KP(ctx.macro_indexs_));
  }

  return ret;
}

int ObMigrateDag::update_partition_meta_for_restore()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  bool need_set_restore = true;
  // If it is a phsical restore, is_restore will set REPLICA_RESTORE_LOG, if not, directly chagne to
  // REPLICA_NOT_RESTORE.
  int16_t is_restore = ObReplicaRestoreStatus::REPLICA_NOT_RESTORE;
  int16_t restore_state = ObReplicaRestoreStatus::REPLICA_NOT_RESTORE;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "migrate dag do not init", K(ret));
  } else if (OB_ISNULL(partition = ctx_->get_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition must not null", K(ret), K(*ctx_));
  } else if ((RESTORE_REPLICA_OP == ctx_->replica_op_arg_.type_ ||
                 RESTORE_FOLLOWER_REPLICA_OP == ctx_->replica_op_arg_.type_ ||
                 RESTORE_STANDBY_OP == ctx_->replica_op_arg_.type_) &&
             OB_SUCC(ctx_->result_)) {
    const ObPartitionKey &pgkey = ctx_->replica_op_arg_.key_;
    if (ctx_->replica_op_arg_.is_physical_restore() || ctx_->replica_op_arg_.is_standby_restore()) {
      is_restore = ctx_->replica_op_arg_.is_physical_restore() ? ObReplicaRestoreStatus::REPLICA_RESTORE_LOG
                                                               : ObReplicaRestoreStatus::REPLICA_NOT_RESTORE;
      ObSavedStorageInfoV2 info;
      ObBaseStorageInfo clog_info;
      bool has_memtable = partition->get_pg_storage().has_memstore();
      bool is_offline = true;
      restore_state = partition->get_pg_storage().get_restore_state();

      if (REPLICA_RESTORE_DATA != restore_state && REPLICA_RESTORE_ARCHIVE_DATA != restore_state &&
          REPLICA_RESTORE_STANDBY != restore_state && REPLICA_RESTORE_CUT_DATA != restore_state &&
          REPLICA_RESTORE_STANDBY_CUT != restore_state) {
        need_set_restore = false;
        STORAGE_LOG(ERROR, "physical restore state is unexpected", K(pgkey), K(restore_state));
      }

      if (OB_SUCC(ret) && OB_FAIL(partition->get_log_service()->is_offline(is_offline))) {
        STORAGE_LOG(WARN, "get log service offline stat fail", K(pgkey));
      }
      // check status, restore finish, no memtable or offline
      if (OB_SUCC(ret) && !need_set_restore && (!has_memtable || is_offline)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "pg has restore finish, but memtable or log service is not ready",
            K(ret),
            K(need_set_restore),
            K(restore_state),
            K(has_memtable),
            K(is_offline));
      }

#ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = E(EventTable::EN_RESTORE_UPDATE_PARTITION_META_FAILED) OB_SUCCESS;
        if (-9001 == ret) {
          STORAGE_LOG(INFO, "inject restore fail before create_memtable", K(ret), K(pgkey));
        } else {
          ret = OB_SUCCESS;
        }
      }
#endif
      if (OB_SUCC(ret) && ctx_->pg_meta_.storage_info_.get_data_info().get_schema_version() == 0) {
        ObDataStorageInfo cur_data_info;
        if (OB_FAIL(partition->get_saved_data_info(cur_data_info))) {
          LOG_WARN("failed to get saved data info", K(ret));
        } else {
          ctx_->pg_meta_.storage_info_.get_data_info().set_schema_version(cur_data_info.get_schema_version());
        }
      }

      if (OB_SUCC(ret) && !has_memtable) {
        if (REPLICA_RESTORE_CUT_DATA == restore_state || REPLICA_RESTORE_STANDBY_CUT == restore_state) {
          // do nothing
        } else if (OB_FAIL(partition->set_storage_info(ctx_->pg_meta_.storage_info_))) {
          STORAGE_LOG(WARN, "failed to set storage info", K(ret), K(pgkey));
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(partition->create_memtable())) {
          STORAGE_LOG(WARN, "failed to create memtable", K(ret), K(pgkey));
        }
      }

#ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = E(EventTable::EN_RESTORE_UPDATE_PARTITION_META_FAILED) OB_SUCCESS;
        if (-9002 == ret) {
          STORAGE_LOG(INFO, "inject restore fail before log online", K(ret), K(pgkey));
        } else {
          ret = OB_SUCCESS;
        }
      }
#endif

      if (OB_SUCC(ret) && is_offline) {
        if (OB_FAIL(online_for_restore())) {
          LOG_WARN("failed to online for restore", K(ret));
        }
      }
      STORAGE_LOG(INFO,
          "physical restore update partition meta finish",
          K(pgkey),
          K(restore_state),
          K(has_memtable),
          K(is_offline),
          K(info));
    }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_RESTORE_UPDATE_PARTITION_META_FAILED) OB_SUCCESS;
      if (-9003 == ret) {
        STORAGE_LOG(INFO, "inject restore fail before set_restore_flag", K(ret), K(pgkey));
      } else {
        ret = OB_SUCCESS;
      }
    }
#endif

    if (OB_FAIL(ret) || !need_set_restore) {
    } else if (!partition->get_pg_storage().is_restore()) {
      ret = OB_RESTORE_PARTITION_TWICE;
      STORAGE_LOG(WARN, "partition restore twice", K(ret), K(pgkey));
    } else if (REPLICA_RESTORE_STANDBY_CUT == restore_state) {
      const int16_t new_restore_state = ObReplicaRestoreStatus::REPLICA_NOT_RESTORE;
      if (OB_FAIL(partition->get_pg_storage().set_restore_flag(new_restore_state,
              OB_INVALID_TIMESTAMP /*not update restore_snapshot_version */,
              OB_INVALID_TIMESTAMP /*not update restore_schema_version*/))) {
        LOG_WARN("failed to set restore flag", K(ret), K(*ctx_));
      }
    } else if (OB_FAIL(ObPartitionService::get_instance().set_restore_flag(pgkey, is_restore))) {
      STORAGE_LOG(WARN, "fail to set partition store meta", K(ret));
    }
    // If it is physical restore, set_archive_restoring will be setted in set_restore_flag function
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = partition->get_pg_storage().clear_all_memtables())) {
        LOG_WARN("failed to clear all memtables", K(tmp_ret));
      }
    }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_RESTORE_UPDATE_PARTITION_META_FAILED) OB_SUCCESS;
      if (-9004 == ret) {
        STORAGE_LOG(INFO, "inject restore fail before report to rs", K(ret), K(pgkey));
      } else {
        ret = OB_SUCCESS;
      }
    }
#endif
  }
  return ret;
}

int ObMigrateDag::online_for_rebuild()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSavedStorageInfoV2 info;
  ObBaseStorageInfo &clog_info = info.get_clog_info();
  ObDataStorageInfo &data_info = info.get_data_info();
  if (NULL != ctx_ && ctx_->need_online_for_rebuild_) {
    ObIPartitionGroup *partition = ctx_->get_partition();
    int64_t restore_snapshot_version = OB_INVALID_TIMESTAMP;
    uint64_t last_restore_log_id = OB_INVALID_ID;
    int64_t last_restore_log_ts = OB_INVALID_TIMESTAMP;
    if (NULL == partition) {
      ret = OB_ERR_SYS;
      LOG_ERROR("partition must not null", K(ret), K(*ctx_));
    } else if (partition->is_replica_using_remote_memstore()) {
      // do nothing, cause data replica no need offline
    } else if (OB_FAIL(partition->get_all_saved_info(info))) {
      STORAGE_LOG(WARN, "failed to get saved info", K(ret));
    } else if (OB_FAIL(partition->create_memtable())) {
      LOG_WARN("failed to create memtable", K(ret), "pkey", partition->get_partition_key());
    } else if (OB_FAIL(partition->get_log_service()->migrate_set_base_storage_info(clog_info))) {
      STORAGE_LOG(WARN, "reset clog start point fail", K(ret), K(info));
    } else if (OB_FAIL(partition->get_pg_storage().get_restore_replay_info(
                   last_restore_log_id, last_restore_log_ts, restore_snapshot_version))) {
      LOG_WARN("failed to get_restore_replay_info", KR(ret), "pkey", partition->get_partition_key());
    } else if (OB_FAIL(MIGRATOR.get_partition_service()->online_partition(ctx_->replica_op_arg_.key_,
                   data_info.get_publish_version(),
                   restore_snapshot_version,
                   last_restore_log_id,
                   last_restore_log_ts))) {
      STORAGE_LOG(WARN,
          "online partition failed",
          K(ret),
          K(info),
          K(restore_snapshot_version),
          K(last_restore_log_id),
          K(last_restore_log_ts),
          K(*ctx_));
    } else if (OB_FAIL(ctx_->get_partition()->get_log_service()->restore_replayed_log(clog_info))) {
      STORAGE_LOG(WARN, "restore and replay log failed.", K(ret), K_(ctx_->replica_op_arg_.key), K(info));
    } else if (OB_FAIL(ctx_->get_partition()->get_log_service()->set_online(
                   clog_info, ObVersion(0, 0) /*freeze version*/))) {
      STORAGE_LOG(WARN, "reset log temporary status failed.", K(ret), K_(ctx_->replica_op_arg_.key), K(info));
    } else if (OB_FAIL(ctx_->get_partition()->report_clog_history_online())) {
      LOG_WARN("failed to report clog history online", K(ret));
    } else {
      STORAGE_LOG(INFO, "succeed online_for_rebuild", K(ret), "pkey", ctx_->replica_op_arg_.key_, K(info));
      if (OB_SUCCESS != (tmp_ret = MIGRATOR.get_partition_service()->turn_off_rebuild_flag(ctx_->replica_op_arg_))) {
        LOG_WARN("Failed to report_rebuild_replica off", K(tmp_ret), "arg", ctx_->replica_op_arg_);
      }
    }
  }
  return ret;
}

int ObMigrateDag::online_for_restore()
{
  int ret = OB_SUCCESS;
  bool is_offline = true;
  ObSavedStorageInfoV2 info;
  ObBaseStorageInfo clog_info;
  ObIPartitionGroup *partition = NULL;
  if (OB_ISNULL(ctx_)) {
    // do nothing
  } else if (RESTORE_STANDBY_OP != ctx_->replica_op_arg_.type_ && RESTORE_REPLICA_OP != ctx_->replica_op_arg_.type_ &&
             RESTORE_FOLLOWER_REPLICA_OP != ctx_->replica_op_arg_.type_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "online for restore replica op type is unexpected", K(ret), K(ctx_->replica_op_arg_));
  } else if (OB_ISNULL(partition = ctx_->get_partition())) {
    // do nothing
  } else if (OB_FAIL(partition->get_log_service()->is_offline(is_offline))) {
    STORAGE_LOG(WARN, "get log service offline stat fail");
  } else if (!is_offline) {
    STORAGE_LOG(INFO, "partition standby restore is online", K(partition->get_partition_key()));
  } else if (OB_FAIL(partition->reset_for_replay())) {
    LOG_WARN("failed to reset partition for replay", K(ret), K(partition->get_partition_key()));
  } else if (OB_FAIL(partition->get_all_saved_info(info))) {
    STORAGE_LOG(WARN, "failed to get saved info", K(ret));
  } else if (OB_FAIL(partition->get_saved_clog_info(clog_info))) {
    LOG_WARN("failed to get saved clog info", K(ret));
  } else if (OB_FAIL(partition->get_log_service()->migrate_set_base_storage_info(clog_info))) {
    STORAGE_LOG(WARN, "migrate_set_base_storage_info fail", K(ret), K(clog_info));
  } else if (OB_FAIL(partition->get_log_service()->set_online(clog_info, ObVersion(0, 0) /*freeze version*/))) {
    STORAGE_LOG(WARN, "set_online fail", K(ret), K(clog_info));
  } else if (OB_FAIL(partition->report_clog_history_online())) {
    STORAGE_LOG(WARN, "failed to report clog history online", K(ret), K(partition->get_partition_key()));
  } else {
    STORAGE_LOG(
        INFO, "partition restore online succ", K(partition->get_partition_key()), K(ctx_->replica_op_arg_.type_));
  }
  return ret;
}

ObBackupDag::ObBackupDag() : ObBaseMigrateDag(ObIDag::DAG_TYPE_BACKUP, ObIDag::DAG_PRIO_BACKUP), backup_data_type_()
{}

ObBackupDag::~ObBackupDag()
{
  int tmp_ret = OB_SUCCESS;

  if (NULL != ctx_) {
    ctx_->calc_need_retry();
    ObReplicaOpArg tmp_relica_op_arg = ctx_->replica_op_arg_;
    ObIPartitionGroup *partition = ctx_->get_partition();
    if (OB_SUCCESS == ctx_->result_ && OB_SUCCESS != this->get_dag_ret()) {
      ctx_->result_ = this->get_dag_ret();
      LOG_WARN("migrate dag is failed", "result", this->get_dag_ret(), K(*ctx_));
    }

    if (OB_SUCCESS == ctx_->result_ && ctx_->need_rebuild_) {  // Status should not exist status
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("result success no need rebuild", K(tmp_ret), K(*ctx_));
      ctx_->result_ = tmp_ret;
      ctx_->need_rebuild_ = false;
    }

    ctx_->finish_ts_ = ObTimeUtility::current_time();

    if (!ctx_->need_rebuild_) {
      ctx_->action_ = ObMigrateCtx::END;

      if (OB_SUCCESS == ctx_->result_) {
        ctx_->during_migrating_ = false;
      }

      if (OB_SUCCESS != (tmp_ret = ctx_->update_partition_migration_status())) {
        STORAGE_LOG(WARN, "failed to update_partition_migration_status", K(tmp_ret));
      }

      if (OB_SUCCESS != (tmp_ret = ctx_->notice_finish_part_task())) {
        STORAGE_LOG(WARN, "failed to notice_finish_part_task", K(tmp_ret), K(tmp_relica_op_arg));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = ctx_->update_partition_migration_status())) {
        STORAGE_LOG(WARN, "failed to update_partition_migration_status", K(tmp_ret));
      }
      LOG_INFO("backup retry", "replica_op_arg", ctx_->replica_op_arg_, "rebuild_count", ctx_->rebuild_count_);
      int tmp_result = ctx_->result_;
      ctx_->rebuild_migrate_ctx();
      // It has concurrency problems when you use ctx after generate_and_scheduler_migrate_dag successful
      if (OB_SUCCESS != (tmp_ret = ctx_->generate_and_schedule_backup_dag(backup_data_type_))) {
        LOG_WARN("failed to schedule migrate dag", K(tmp_ret), K(*ctx_));
        ctx_->result_ = tmp_result;  // set error code before rebuild
        ctx_->need_rebuild_ = false;
        ctx_->action_ = ObMigrateCtx::END;
        if (OB_SUCCESS != (tmp_ret = ctx_->update_partition_migration_status())) {
          STORAGE_LOG(WARN, "failed to update_partition_migration_status", K(tmp_ret));
        }

        if (OB_SUCCESS != (tmp_ret = ctx_->notice_finish_part_task())) {
          STORAGE_LOG(WARN, "failed to notice_finish_part_task", K(tmp_ret), K(tmp_relica_op_arg));
        }
      }
    }  // need rebuild
  }    // ctx != NULL
}

int ObBackupDag::init(const ObBackupDataType &backup_data_type, ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (!ctx.replica_op_arg_.is_valid() || OB_ISNULL(ctx.group_task_) || !backup_data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(ctx.group_task_), K(ctx.replica_op_arg_), K(backup_data_type));
  } else if (BACKUP_REPLICA_OP != ctx.replica_op_arg_.type_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "replica op type is not backup", K(ret), K(ctx.replica_op_arg_));
  } else {
    tenant_id_ = ctx.replica_op_arg_.key_.get_tenant_id();
    if (OB_FAIL(ctx.physical_backup_ctx_.init(*ObPartitionMigrator::get_instance().get_bandwidth_throttle(),
            ctx.replica_op_arg_.backup_arg_,
            ctx.replica_op_arg_.key_))) {
      LOG_WARN("failed to init physical backup ctx", K(ret), K(ctx.replica_op_arg_.backup_arg_));
    } else if (ctx.replica_op_arg_.backup_arg_.is_incremental_backup() && OB_ISNULL(ctx.macro_indexs_) &&
               backup_data_type.is_major_backup()) {
      ObPhyRestoreMacroIndexStoreV2 *phy_restore_macro_index_v2 = NULL;
      if (OB_ISNULL(phy_restore_macro_index_v2 = MIGRATOR.get_cp_fty()->get_phy_restore_macro_index_v2())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy restore macro index should not be NULL", K(ret), KP(phy_restore_macro_index_v2));
      } else if (OB_FAIL(phy_restore_macro_index_v2->init(ctx.replica_op_arg_.backup_arg_.task_id_,
                     ctx.replica_op_arg_.key_,
                     ctx.replica_op_arg_.backup_arg_,
                     backup_data_type))) {
        LOG_WARN("failed to int physcial restore macro index", K(ret));
      } else {
        ctx.macro_indexs_ = phy_restore_macro_index_v2;
        phy_restore_macro_index_v2 = NULL;
      }
      if (OB_NOT_NULL(phy_restore_macro_index_v2)) {
        MIGRATOR.get_cp_fty()->free(phy_restore_macro_index_v2);
        ctx.macro_indexs_ = NULL;
      }
    }

    if (OB_SUCC(ret)) {
      set_priority(ObIDag::DAG_PRIO_BACKUP);
      if (OB_FAIL(ctx.notice_start_part_task())) {
        LOG_WARN("failed to notice_start_part_task", K(ret));
        ctx.result_ = ret;
      } else if (OB_FAIL(get_compat_mode_with_table_id(ctx.replica_op_arg_.key_.table_id_, compat_mode_))) {
        LOG_WARN("failed to get table compat mode", K(ret), K(ctx));
      } else {
        ctx_ = &ctx;
        backup_data_type_ = backup_data_type;
        is_inited_ = true;
        STORAGE_LOG(INFO, "succeed init backup dag");
      }
    }
  }
  return ret;
}

ObValidateDag::ObValidateDag() : ObBaseMigrateDag(ObIDag::DAG_TYPE_VALIDATE, ObIDag::DAG_PRIO_VALIDATE)
{}

ObValidateDag::~ObValidateDag()
{
  int tmp_ret = OB_SUCCESS;
  int ret = OB_SUCCESS;
  if (NULL != ctx_) {
    ctx_->calc_need_retry();
    SMART_VAR(ObReplicaOpArg, tmp_relica_op_arg)
    {
      tmp_relica_op_arg = ctx_->replica_op_arg_;
      if (OB_SUCCESS == ctx_->result_ && OB_SUCCESS != this->get_dag_ret()) {
        ctx_->result_ = this->get_dag_ret();
        LOG_WARN("migrate dag is failed", "result", this->get_dag_ret(), K(*ctx_));
      }

      ctx_->action_ = ObMigrateCtx::END;
      ctx_->during_migrating_ = false;
      if (OB_SUCCESS != (tmp_ret = ctx_->update_partition_migration_status())) {
        STORAGE_LOG(WARN, "failed to update_partition_migration_status", K(tmp_ret));
      }

      ctx_->finish_ts_ = ObTimeUtility::current_time();
      if (OB_SUCCESS != (tmp_ret = ctx_->notice_finish_part_task())) {
        STORAGE_LOG(WARN, "failed to notice_finish_part_task", K(tmp_ret), K(tmp_relica_op_arg));
      }
    }
  }  // ctx != NULL
}

bool ObValidateDag::operator==(const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObValidateDag &other_dag = static_cast<const ObValidateDag &>(other);
    if (NULL != ctx_ && NULL != other_dag.ctx_) {
      const ObPhysicalValidateArg &other_arg = other_dag.ctx_->replica_op_arg_.validate_arg_;
      const ObPhysicalValidateArg &arg = ctx_->replica_op_arg_.validate_arg_;
      is_same = arg.backup_set_id_ == other_arg.backup_set_id_ && arg.pg_key_ == other_arg.pg_key_;
    } else {
      is_same = false;
    }
  }
  return is_same;
}

int64_t ObValidateDag::hash() const
{
  int64_t hash_value = 0;
  if (NULL != ctx_) {
    const ObPhysicalValidateArg &arg = ctx_->replica_op_arg_.validate_arg_;
    hash_value = common::murmurhash(&(arg.backup_set_id_), sizeof(arg.backup_set_id_), hash_value);
    hash_value = common::murmurhash(&(arg.pg_key_), sizeof(arg.pg_key_), hash_value);
  }
  return hash_value;
}

int ObValidateDag::init(ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (!ctx.replica_op_arg_.is_valid() || OB_ISNULL(ctx.group_task_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(ctx.group_task_), K(ctx.replica_op_arg_));
  } else if (VALIDATE_BACKUP_OP != ctx.replica_op_arg_.type_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "replica op type is not validate", K(ret), K(ctx.replica_op_arg_));
  } else {
    const ObPartitionKey &pkey = ctx.replica_op_arg_.key_;
    tenant_id_ = pkey.get_tenant_id();
    set_priority(ObIDag::DAG_PRIO_VALIDATE);
    if (OB_FAIL(ctx.notice_start_part_task())) {
      LOG_WARN("failed to notice_start_part_task", K(ret));
      ctx.result_ = ret;
    } else {
      ctx_ = &ctx;
      is_inited_ = true;
      STORAGE_LOG(INFO, "succeed init validate dag", K(pkey));
    }
  }
  return ret;
}

ObBackupBackupsetDag::ObBackupBackupsetDag()
    : ObBaseMigrateDag(ObIDag::DAG_TYPE_BACKUP_BACKUPSET, ObIDag::DAG_PRIO_BACKUP)
{}

ObBackupBackupsetDag::~ObBackupBackupsetDag()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(ctx_)) {
    ctx_->calc_need_retry();
    SMART_VAR(ObReplicaOpArg, tmp_relica_op_arg)
    {
      tmp_relica_op_arg = ctx_->replica_op_arg_;
      if (OB_SUCCESS == ctx_->result_ && OB_SUCCESS != this->get_dag_ret()) {
        ctx_->result_ = this->get_dag_ret();
        LOG_WARN("backup backupset dag is failed", "result", this->get_dag_ret(), K(*ctx_));
      }

      ctx_->action_ = ObMigrateCtx::END;
      ctx_->during_migrating_ = false;
      if (OB_SUCCESS != (tmp_ret = ctx_->update_partition_migration_status())) {
        STORAGE_LOG(WARN, "failed to update_partition_migration_status", K(tmp_ret));
      }

      ctx_->finish_ts_ = ObTimeUtility::current_time();
      if (OB_SUCCESS != (tmp_ret = ctx_->notice_finish_part_task())) {
        STORAGE_LOG(WARN, "failed to notice_finish_part_task", K(tmp_ret), K(tmp_relica_op_arg));
      }
    }
  }
}

bool ObBackupBackupsetDag::operator==(const ObIDag &other) const
{
  bool equal = false;
  if (OB_UNLIKELY(this == &other)) {
    equal = true;
  } else {
    if (get_type() == other.get_type()) {
      const ObBackupBackupsetDag &other_dag = static_cast<const ObBackupBackupsetDag &>(other);
      if (OB_NOT_NULL(ctx_) && OB_NOT_NULL(other_dag.ctx_)) {
        const share::ObBackupBackupsetArg &arg = ctx_->replica_op_arg_.backup_backupset_arg_;
        const share::ObBackupBackupsetArg &other_arg = other_dag.ctx_->replica_op_arg_.backup_backupset_arg_;
        if (arg.backup_set_id_ == other_arg.backup_set_id_ && arg.pg_key_ == other_arg.pg_key_) {
          equal = true;
          STORAGE_LOG(
              WARN, "two backup backupset dag is same", K(arg), K(other_arg), KP(this), KP(&other), K(*this), K(other));
        }
      }
    }
  }
  return equal;
}

int64_t ObBackupBackupsetDag::hash() const
{
  int64_t hash_value = 0;
  if (OB_NOT_NULL(ctx_)) {
    const share::ObBackupBackupsetArg &arg = ctx_->replica_op_arg_.backup_backupset_arg_;
    hash_value = common::murmurhash(&(arg.backup_set_id_), sizeof(arg.backup_set_id_), hash_value);
    hash_value = common::murmurhash(&(arg.pg_key_), sizeof(arg.pg_key_), hash_value);
  }
  return hash_value;
}

int ObBackupBackupsetDag::init(ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(ERROR, "cannot init twice", KR(ret));
  } else if (!ctx.replica_op_arg_.is_valid() || OB_ISNULL(ctx.group_task_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", KR(ret), KP(ctx.group_task_), K(ctx.replica_op_arg_));
  } else if (BACKUP_BACKUPSET_OP != ctx.replica_op_arg_.type_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "replica op type is not backup backupset", KR(ret), K(ctx.replica_op_arg_));
  } else {
    const ObPartitionKey &pkey = ctx.replica_op_arg_.key_;
    tenant_id_ = pkey.get_tenant_id();
    const share::ObBackupBackupsetArg &arg = ctx.replica_op_arg_.backup_backupset_arg_;

    if (OB_SUCC(ret)) {
      set_priority(ObIDag::DAG_PRIO_BACKUP);
      if (OB_FAIL(ctx.notice_start_part_task())) {
        STORAGE_LOG(WARN, "failed to notice_start_part_task", KR(ret));
        ctx.result_ = ret;
      } else {
        ctx_ = &ctx;
        is_inited_ = true;
        STORAGE_LOG(INFO, "succeed init backup backupset dag", K(pkey));
      }
    }
  }
  return ret;
}

ObBackupArchiveLogDag::ObBackupArchiveLogDag()
    : ObBaseMigrateDag(ObIDag::DAG_TYPE_BACKUP_ARCHIVELOG, ObIDag::DAG_PRIO_BACKUP)
{}

ObBackupArchiveLogDag::~ObBackupArchiveLogDag()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(ctx_)) {
    ctx_->calc_need_retry();
    SMART_VAR(ObReplicaOpArg, tmp_relica_op_arg)
    {
      tmp_relica_op_arg = ctx_->replica_op_arg_;
      if (OB_SUCCESS == ctx_->result_ && OB_SUCCESS != this->get_dag_ret()) {
        ctx_->result_ = this->get_dag_ret();
        LOG_WARN("backup archivelog dag is failed", "result", this->get_dag_ret(), K(*ctx_));
      }

      ctx_->action_ = ObMigrateCtx::END;
      ctx_->during_migrating_ = false;
      if (OB_SUCCESS != (tmp_ret = ctx_->update_partition_migration_status())) {
        STORAGE_LOG(WARN, "failed to update_partition_migration_status", K(tmp_ret));
      }

      ctx_->finish_ts_ = ObTimeUtility::current_time();
      if (OB_SUCCESS != (tmp_ret = ctx_->notice_finish_part_task())) {
        STORAGE_LOG(WARN, "failed to notice_finish_part_task", K(tmp_ret), K(tmp_relica_op_arg));
      }
    }
  }
}

int ObBackupArchiveLogDag::init(ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(ERROR, "cannot init twice", KR(ret));
  } else if (!ctx.replica_op_arg_.is_valid() || OB_ISNULL(ctx.group_task_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", KR(ret), KP(ctx.group_task_), K(ctx.replica_op_arg_));
  } else if (BACKUP_ARCHIVELOG_OP != ctx.replica_op_arg_.type_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "replica op type is not backup archivelog", KR(ret), K(ctx.replica_op_arg_));
  } else {
    const ObPartitionKey &pkey = ctx.replica_op_arg_.key_;
    tenant_id_ = pkey.get_tenant_id();

    if (OB_SUCC(ret)) {
      set_priority(ObIDag::DAG_PRIO_BACKUP);
      if (OB_FAIL(ctx.notice_start_part_task())) {
        STORAGE_LOG(WARN, "failed to notice_start_part_task", KR(ret));
        ctx.result_ = ret;
      } else {
        ctx_ = &ctx;
        is_inited_ = true;
        STORAGE_LOG(INFO, "succeed init backup archivelog dag", K(pkey));
      }
    }
  }
  return ret;
}

ObMigratePrepareTask::ObMigratePrepareTask()
    : ObITask(TASK_TYPE_MIGRATE_PREPARE),
      is_inited_(false),
      ctx_(NULL),
      cp_fty_(NULL),
      rpc_(NULL),
      srv_rpc_proxy_(NULL),
      bandwidth_throttle_(NULL),
      partition_service_(NULL)
{}

ObMigratePrepareTask::~ObMigratePrepareTask()
{}

int ObMigratePrepareTask::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (ObIDag::DAG_TYPE_MIGRATE != dag_->get_type() && ObIDag::DAG_TYPE_BACKUP != dag_->get_type() &&
             ObIDag::DAG_TYPE_VALIDATE != dag_->get_type() && ObIDag::DAG_TYPE_BACKUP_BACKUPSET != dag_->get_type() &&
             ObIDag::DAG_TYPE_BACKUP_ARCHIVELOG != dag_->get_type()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), K(*dag_));
  } else {
    ctx_ = static_cast<ObMigrateDag *>(dag_)->get_ctx();
    is_inited_ = true;
    bandwidth_throttle_ = MIGRATOR.get_bandwidth_throttle();
    partition_service_ = MIGRATOR.get_partition_service();
    srv_rpc_proxy_ = MIGRATOR.get_svr_rpc_proxy();
    rpc_ = MIGRATOR.get_pts_rpc();
    cp_fty_ = MIGRATOR.get_cp_fty();
  }
  return ret;
}

int ObMigratePrepareTask::prepare_restore_reader_if_needed()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  ObPartGroupMigrationTask *group_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (RESTORE_REPLICA_OP == ctx_->replica_op_arg_.type_ ||
             RESTORE_FOLLOWER_REPLICA_OP == ctx_->replica_op_arg_.type_) {
    if (OB_ISNULL(partition = ctx_->get_partition())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error, the partition is NULL ", K(ret));
    } else if (!partition->get_pg_storage().is_restore()) {
      ret = OB_RESTORE_PARTITION_TWICE;
      STORAGE_LOG(WARN, "partition restore twice", K(ret), K(ctx_->replica_op_arg_.key_));
    } else if (ctx_->replica_op_arg_.is_physical_restore_leader()) {
      if (OB_FAIL(prepare_restore_reader())) {
        LOG_WARN("failed to prepare restore reader", K(ret), "op_arg", ctx_->replica_op_arg_);
      }
    } else if (ctx_->replica_op_arg_.is_physical_restore_follower()) {
      ctx_->use_slave_safe_read_ts_ = false;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only support physical restore", K(ret), "op", ctx_->replica_op_arg_);
    }
  }
  return ret;
}

int ObMigratePrepareTask::prepare_restore_reader()
{
  int ret = OB_SUCCESS;
  ObPartGroupMigrationTask *group_task = NULL;
  const int64_t compatible = ctx_->replica_op_arg_.phy_restore_arg_.restore_info_.compatible_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_BACKUP_COMPATIBLE_VERSION_V3 == compatible || OB_BACKUP_COMPATIBLE_VERSION_V4 == compatible) {
    ObPartitionGroupMetaRestoreReaderV2 *restore_meta_reader_v2 = NULL;
    ObPhyRestoreMacroIndexStoreV2 *macro_index = NULL;

    if (OB_ISNULL(ctx_->macro_indexs_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro indexs should not be NULL", K(ret), KP(ctx_->macro_indexs_));
    } else if (ObIPhyRestoreMacroIndexStore::PHY_RESTORE_MACRO_INDEX_STORE_V2 != ctx_->macro_indexs_->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro indexs type is unexpected", K(ret), K(ctx_->macro_indexs_->get_type()));
    } else if (FALSE_IT(macro_index = reinterpret_cast<ObPhyRestoreMacroIndexStoreV2 *>(ctx_->macro_indexs_))) {
    } else if (OB_ISNULL(restore_meta_reader_v2 = cp_fty_->get_partition_group_meta_restore_reader_v2())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta reader should not be NULL", K(ret), KP(restore_meta_reader_v2));
    } else if (FALSE_IT(group_task = reinterpret_cast<ObPartGroupMigrationTask *>(ctx_->group_task_))) {
    } else if (OB_FAIL(restore_meta_reader_v2->init(*bandwidth_throttle_,
                   ctx_->replica_op_arg_.phy_restore_arg_,
                   group_task->get_meta_indexs(),
                   *macro_index))) {
      LOG_WARN("fail to init meta reader", K(ret), KP(restore_meta_reader_v2));
    } else {
      ctx_->restore_meta_reader_ = restore_meta_reader_v2;
      restore_meta_reader_v2 = NULL;
    }
    if (NULL != cp_fty_) {
      if (NULL != restore_meta_reader_v2) {
        cp_fty_->free(restore_meta_reader_v2);
      }
    }
  } else if (OB_BACKUP_COMPATIBLE_VERSION_V1 == compatible || OB_BACKUP_COMPATIBLE_VERSION_V2 == compatible) {
    ObPartitionGroupMetaRestoreReaderV1 *restore_meta_reader_v1 = NULL;
    ObPhyRestoreMacroIndexStore *macro_index = NULL;
    if (OB_ISNULL(ctx_->macro_indexs_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro indexs should not be NULL", K(ret), KP(ctx_->macro_indexs_));
    } else if (ObIPhyRestoreMacroIndexStore::PHY_RESTORE_MACRO_INDEX_STORE_V1 != ctx_->macro_indexs_->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "macro indexs type is unexpected", K(ret), K(ctx_->macro_indexs_->get_type()));
    } else if (FALSE_IT(macro_index = reinterpret_cast<ObPhyRestoreMacroIndexStore *>(ctx_->macro_indexs_))) {
    } else if (OB_ISNULL(restore_meta_reader_v1 = cp_fty_->get_partition_group_meta_restore_reader_v1())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta reader should not be NULL", K(ret), KP(restore_meta_reader_v1));
    } else if (FALSE_IT(group_task = reinterpret_cast<ObPartGroupMigrationTask *>(ctx_->group_task_))) {
    } else if (OB_FAIL(restore_meta_reader_v1->init(*bandwidth_throttle_,
                   ctx_->replica_op_arg_.phy_restore_arg_,
                   group_task->get_meta_indexs(),
                   *macro_index))) {
      LOG_WARN("fail to init meta reader", K(ret), KP(restore_meta_reader_v1));
    } else {
      ctx_->restore_meta_reader_ = restore_meta_reader_v1;
      restore_meta_reader_v1 = NULL;
    }
    if (NULL != cp_fty_) {
      if (NULL != restore_meta_reader_v1) {
        cp_fty_->free(restore_meta_reader_v1);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compatible version is unexpected", K(ret), K(compatible));
  }
  return ret;
}

// TODO() interface optimization for restore and rebuild
int ObMigratePrepareTask::create_partition_if_needed(
    ObIPartitionGroupGuard &pg_guard, const ObPGPartitionStoreMeta &partition_meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not inited", K(ret));
  } else if (OB_UNLIKELY(!partition_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "partition_meta is not valid", K(ret), K(partition_meta));
  } else {
    const ObPartitionKey &pg_key = ctx_->replica_op_arg_.key_;
    const ObPartitionKey &pkey = partition_meta.pkey_;
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup *pg = NULL;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS == (tmp_ret = ObPartitionService::get_instance().get_partition(pkey, guard))) {
      STORAGE_LOG(INFO, "partition exist, not need to create", K(ret), K(pkey));
    } else if (OB_PARTITION_NOT_EXIST != tmp_ret) {
      ret = tmp_ret;
      STORAGE_LOG(WARN, "restore get partition fail", K(ret), K(pkey));
    } else {
      obrpc::ObCreatePartitionArg arg;
      arg.schema_version_ = partition_meta.create_schema_version_;
      arg.lease_start_ = partition_meta.create_timestamp_;
      arg.restore_ = REPLICA_RESTORE_DATA;
      const bool is_replay = false;
      const uint64_t unused = 0;
      const bool in_slog_trans = false;
      ObTablesHandle sstables_handle;

      if (OB_ISNULL(pg = pg_guard.get_partition_group())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition group should not be null", K(ret), K(pg_key));
      } else if (OB_FAIL(pg->create_pg_partition(pkey,
                     partition_meta.multi_version_start_,
                     partition_meta.data_table_id_,
                     arg,
                     in_slog_trans,
                     is_replay,
                     unused,
                     sstables_handle))) {
        STORAGE_LOG(WARN, "fail to create pg partition", K(ret), K(partition_meta));
      } else {
        sstables_handle.reset();
        STORAGE_LOG(
            INFO, "succeed to create follower pg partition during physical restore", K(pkey), K(partition_meta));
      }
    }
  }
  return ret;
}

int ObMigratePrepareTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(prepare_migrate())) {
    LOG_WARN("failed to prepare migrate", K(ret));
  } else if (OB_FAIL(generate_and_schedule_tasks())) {
    LOG_WARN("failed to generate and schedule tasks", K(ret));
  }

  if (OB_FAIL(ret) && NULL != ctx_) {
    if (ctx_->is_need_retry(ret)) {
      ctx_->need_rebuild_ = true;
      LOG_INFO("migrate prepare task need retry", K(ret), K(*ctx_));
    }
    ctx_->set_result_code(ret);
  }

  LOG_INFO("end ObMigratePrepareTask process", K(ret));
  return ret;
}

int ObMigratePrepareTask::generate_and_schedule_tasks()
{
  int ret = OB_SUCCESS;
  ObMigrateTransTableTaskGeneratorTask *migrate_trans_table_task_generator_task = NULL;
  ObMigrateTaskGeneratorTask *migrate_task_generator_task = NULL;
  ObMigrateRecoveryPointTaskGeneratorTask *recovery_point_task_generator_task = NULL;
  ObFakeTask *wait_trans_table_finish_task = NULL;
  ObFakeTask *wait_recovery_point_finish_task = NULL;
  // migrate_trans_table_task_generator_task -> wait_trans_table_finish_task
  // wait_trans_table_finish_task -> recovery_point_task_generator_task
  // wait_trans_table_finish_task -> wait_recovery_point_finish_task
  // recovery_point_task_generator_task -> wait_recovery_point_finish_task
  // wait_recovery_point_finish_task -> migrate_task_generator_task

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(wait_trans_table_finish_task))) {
    LOG_WARN("failed to alloc wait finish task", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(migrate_trans_table_task_generator_task))) {
    LOG_WARN("failed to alloc migrate trans generator task", K(ret));
  } else if (OB_FAIL(migrate_trans_table_task_generator_task->init(*wait_trans_table_finish_task))) {
    LOG_WARN("failed to init migrate trans table task generator task", K(ret));
  } else if (OB_FAIL(add_child(*migrate_trans_table_task_generator_task))) {
    LOG_WARN("failed to add migrate trans table task generator task", K(ret));
  } else if (OB_FAIL(dag_->add_task(*migrate_trans_table_task_generator_task))) {
    LOG_WARN("failed to add migrate trans table task generator task into dag", K(ret));
  } else if (OB_FAIL(migrate_trans_table_task_generator_task->add_child(*wait_trans_table_finish_task))) {
    LOG_WARN("failed to add wait rans table finish task", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(wait_recovery_point_finish_task))) {
    LOG_WARN("failed to alloc wait recovery point finish task", K(ret));
  } else if (OB_FAIL(wait_trans_table_finish_task->add_child(*wait_recovery_point_finish_task))) {
    LOG_WARN("failed to add wait recovery point finish task", K(ret));
  } else if (OB_FAIL(dag_->add_task(*wait_trans_table_finish_task))) {
    LOG_WARN("failed to add wait trans table finish task into dag", K(ret));
  } else if (!ctx_->recovery_point_ctx_.recovery_point_key_array_.empty()) {
    if (OB_FAIL(dag_->alloc_task(recovery_point_task_generator_task))) {
      LOG_WARN("failed to alloc migrate task generator task", K(ret));
    } else if (OB_FAIL(recovery_point_task_generator_task->init(*wait_recovery_point_finish_task))) {
      LOG_WARN("failed to init migrate task generator task", K(ret));
    } else if (OB_FAIL(wait_trans_table_finish_task->add_child(*recovery_point_task_generator_task))) {
      LOG_WARN("failed to add migrate task generator task", K(ret));
    } else if (OB_FAIL(dag_->add_task(*recovery_point_task_generator_task))) {
      LOG_WARN("failed to add migrate task generator task into dag", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dag_->alloc_task(migrate_task_generator_task))) {
    LOG_WARN("failed to alloc migrate task generator task", K(ret));
  } else if (OB_FAIL(migrate_task_generator_task->init())) {
    LOG_WARN("failed to init migrate task generator task", K(ret));
  } else if (OB_FAIL(wait_recovery_point_finish_task->add_child(*migrate_task_generator_task))) {
    LOG_WARN("failed to add migrate task generator task", K(ret));
  } else if (OB_FAIL(dag_->add_task(*wait_recovery_point_finish_task))) {
    LOG_WARN("failed to add wait recovery point finish task", K(ret));
  } else if (OB_FAIL(dag_->add_task(*migrate_task_generator_task))) {
    LOG_WARN("failed to add wait recovery point finish task", K(ret));
  }
  return ret;
}

int ObITableTaskGeneratorTask::need_generate_sstable_migrate_tasks(bool &need_schedule)
{
  int ret = OB_SUCCESS;
  need_schedule = true;
  bool is_replica_with_data = true;

  if (NULL != ctx_) {
    if (OB_FAIL(ctx_->change_replica_with_data(is_replica_with_data))) {
      LOG_WARN("failed to check replica", K(ret), K(*ctx_));
    } else if (is_replica_with_data) {
      need_schedule = false;
      LOG_INFO("change replica op with data, no need migrate task", K(*ctx_));
    } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(ctx_->replica_op_arg_.dst_.get_replica_type())) {
      need_schedule = false;
      LOG_INFO("dst replica has no ssstore, no need migrate task", K(*ctx_));
    } else if (ctx_->is_leader_restore_archive_data()) {
      need_schedule = false;
      LOG_INFO("no need restore archive data", "arg", ctx_->replica_op_arg_);
    } else if (ADD_REPLICA_OP == ctx_->replica_op_arg_.type_ &&
               (REPLICA_LOGICAL_RESTORE_DATA == ctx_->is_restore_ || REPLICA_RESTORE_DATA == ctx_->is_restore_ ||
                   REPLICA_RESTORE_STANDBY == ctx_->is_restore_)) {
      need_schedule = false;
      LOG_INFO("no need migrate task for add replica when restoring", "arg", ctx_->replica_op_arg_);
    }
  }
  return ret;
}

int ObMigratePrepareTask::prepare_migrate()
{
  int ret = OB_SUCCESS;
  const bool is_write_lock = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    ObMigrateCtxGuard guard(is_write_lock, *ctx_);

    if (ctx_->is_leader_restore_archive_data()) {
      LOG_INFO("no need restore archive data", "arg", ctx_->replica_op_arg_);
    } else if (OB_FAIL(prepare_restore_reader_if_needed())) {
      LOG_WARN("failed to prepare restore reader", K(ret));
    } else if (OB_FAIL(choose_migrate_src())) {
      LOG_WARN("fail to choose migrate src", K(ret));
    } else if (OB_FAIL(check_backup_data_continues())) {
      LOG_WARN("failed to check backup data continues", K(ret));
    } else if (OB_FAIL(build_migrate_pg_partition_info())) {
      LOG_WARN("failed to build migrate pg partition info", K(ret), K(*ctx_));
    } else if (BACKUP_REPLICA_OP == ctx_->replica_op_arg_.type_) {
      LOG_INFO("no need replay for BACKUP_REPLICA_OP", "arg", ctx_->replica_op_arg_);
    } else if (VALIDATE_BACKUP_OP == ctx_->replica_op_arg_.type_) {
      LOG_INFO("no need replay for VALIDATE_BACKUP_OP", "arg", ctx_->replica_op_arg_);
    } else if (BACKUP_BACKUPSET_OP == ctx_->replica_op_arg_.type_) {
      LOG_INFO("no need replay for BACKUP_BACKUPSET_OP", "arg", ctx_->replica_op_arg_);
    } else if (BACKUP_ARCHIVELOG_OP == ctx_->replica_op_arg_.type_) {
      LOG_INFO("no need replay for BACKUP_ARCHIVELOG_OP", "arg", ctx_->replica_op_arg_);
    } else if (OB_ISNULL(ctx_->get_partition())) {
      if (OB_FAIL(prepare_new_partition())) {
        LOG_WARN("failed to prepare new partition", K(ret), "arg", ctx_->replica_op_arg_);
      } else {
        ctx_->create_new_pg_ = true;
      }
    }

    if (OB_SUCC(ret)) {
      ATOMIC_AAF(&ctx_->data_statics_.partition_count_, ctx_->pg_meta_.partitions_.count());
      ctx_->need_rebuild_ = false;
      ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "finish prepare migrate", "pkey", ctx_->replica_op_arg_.key_, K(*ctx_));
    } else if (NULL != ctx_) {
      ctx_->result_ = ret;
    }
  }
  return ret;
}

int ObMigrateTaskSchedulerTask::try_hold_local_partition()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  ObPGStorage *pg_storage = NULL;
  common::ObAddr leader;
  ObReplicaProperty replica_property;
  int16_t restore_state = 0;
  int64_t trans_table_end_log_ts = 0;
  int64_t trans_table_timestamp = 0;
  ObRole role;
  ObPartitionGroupMeta local_pg_meta;
  ObPartGroupMigrationTask *group_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (VALIDATE_BACKUP_OP == ctx_->replica_op_arg_.type_ || BACKUP_BACKUPSET_OP == ctx_->replica_op_arg_.type_ ||
             BACKUP_ARCHIVELOG_OP == ctx_->replica_op_arg_.type_) {
    LOG_INFO("no need to hold local partition", "type", ctx_->replica_op_arg_.type_);
  } else if (OB_NOT_NULL(partition = ctx_->get_partition())) {
    // do nothing
    pg_storage = &(partition->get_pg_storage());
    LOG_INFO("partition already hold, do not hold again");
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(
                 ctx_->replica_op_arg_.key_, ctx_->partition_guard_))) {
    if (OB_PARTITION_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "get partition failed.", K(ret), K(*ctx_));
    } else if (ctx_->is_only_copy_sstable()) {
      LOG_WARN("failed to get partition for copy index", K(ret), K(*ctx_));
    } else if (CHANGE_REPLICA_OP == ctx_->replica_op_arg_.type_) {
      LOG_WARN("partition not exist, cannot change replica", K(ret), K(*ctx_));
    } else if (REBUILD_REPLICA_OP == ctx_->replica_op_arg_.type_) {
      LOG_WARN("partition not exist, cannot rebuild replica", K(ret), K(*ctx_));
    } else if (LINK_SHARE_MAJOR_OP == ctx_->replica_op_arg_.type_) {
      LOG_WARN("partition not exist, cannot link share major", K(ret), K(*ctx_));
    } else {
      ret = OB_SUCCESS;
      ctx_->replica_state_ = OB_NOT_EXIST_REPLICA;
    }
  } else if (OB_ISNULL(partition = ctx_->partition_guard_.get_partition_group()) ||
             OB_ISNULL(pg_storage = &(partition->get_pg_storage()))) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "partition must not null", K(ret));
  } else if (FALSE_IT(ctx_->is_restore_ = partition->get_pg_storage().get_restore_state())) {
  } else if (ctx_->replica_op_arg_.is_physical_restore() && REPLICA_RESTORE_DATA != ctx_->is_restore_ &&
             REPLICA_RESTORE_ARCHIVE_DATA != ctx_->is_restore_) {
    ObMigrateStatus migrate_status;
    if (OB_FAIL(partition->get_pg_storage().get_pg_migrate_status(migrate_status))) {
      STORAGE_LOG(WARN, "get pg migrate status fail", K(ret), K(ctx_->replica_op_arg_));
    } else if (OB_MIGRATE_STATUS_NONE != migrate_status) {
      ret = OB_STATE_NOT_MATCH;
      STORAGE_LOG(
          WARN, "physical restore failed", K(ret), K(ctx_->is_restore_), K(migrate_status), K(ctx_->replica_op_arg_));
    } else {
      // restore = 0,1,3,4,5 skip physical restore and ret will be set succ before dag finish
      ret = OB_RESTORE_PARTITION_IS_COMPELETE;
      STORAGE_LOG(INFO,
          "physical restore base data is already finished",
          K(ret),
          K(ctx_->is_restore_),
          K(ctx_->replica_op_arg_));
    }
  } else if (ctx_->replica_op_arg_.is_standby_restore() && REPLICA_RESTORE_STANDBY != ctx_->is_restore_ &&
             REPLICA_RESTORE_STANDBY_CUT != ctx_->is_restore_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("standby replica restore but restore status is wrong", K(*ctx_));
  } else if (OB_FAIL(partition->get_leader(leader))) {
    LOG_WARN("failed to get leader", K(ret), "arg", ctx_->replica_op_arg_);
  } else if (OB_FAIL(partition->get_role(role))) {
    LOG_WARN("failed to get partition role", K(ret), "arg", ctx_->replica_op_arg_);
  } else if (leader.is_valid() &&
             leader == MYADDR
             // support rebuild in leader reconfirm
             && partition->get_log_service()->is_leader_active()
             // TODO(wait yanmu)
             //&& is_strong_leader(role)
             && (ADD_REPLICA_OP == ctx_->replica_op_arg_.type_ || MIGRATE_REPLICA_OP == ctx_->replica_op_arg_.type_ ||
                    FAST_MIGRATE_REPLICA_OP == ctx_->replica_op_arg_.type_ ||
                    REBUILD_REPLICA_OP == ctx_->replica_op_arg_.type_ ||
                    CHANGE_REPLICA_OP == ctx_->replica_op_arg_.type_ ||
                    LINK_SHARE_MAJOR_OP == ctx_->replica_op_arg_.type_)) {
    if (REBUILD_REPLICA_OP == ctx_->replica_op_arg_.type_) {
      if (OB_FAIL(MIGRATOR.get_partition_service()->turn_off_rebuild_flag(ctx_->replica_op_arg_))) {
        LOG_WARN("Failed to report_rebuild_replica off", K(ret), "arg", ctx_->replica_op_arg_);
      } else {
        ret = OB_NO_NEED_REBUILD;
        LOG_WARN("leader can not as rebuild dst", K(ret), K(leader), "myaddr", MYADDR, "arg", ctx_->replica_op_arg_);
      }
    } else {
      ret = OB_ERR_SYS;
      LOG_WARN("leader cannot as add_replica,migrate,change_replica,share_major dst",
          K(ret),
          K(leader),
          "myaddr",
          MYADDR,
          "arg",
          ctx_->replica_op_arg_);
    }
  } else if (OB_ISNULL(ctx_->group_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("group task should not be null here", K(ret));
  } else if (FALSE_IT(group_task = reinterpret_cast<ObPartGroupMigrationTask *>(ctx_->group_task_))) {
  } else if (OB_FAIL(group_task->set_migrate_in(partition->get_pg_storage()))) {
    LOG_WARN("failed to set migrate int", "pkey", ctx_->replica_op_arg_.key_, K(ret));
  }

  if (OB_FAIL(ret) || OB_ISNULL(partition) || OB_ISNULL(pg_storage)) {
    // do nothing
  } else if (OB_FAIL(partition->get_replica_state(ctx_->replica_state_))) {
    LOG_WARN("failed to get replica state", K(ret), K(*ctx_));
  } else if (OB_FAIL(pg_storage->get_replica_property(replica_property))) {
    LOG_WARN("failed to get replica property", K(ret), "arg", ctx_->replica_op_arg_);
  } else if (OB_FAIL(pg_storage->get_pg_meta(local_pg_meta))) {
    LOG_WARN("failed to get pg meta", K(ret));
  } else {
    if (OB_FAIL(pg_storage->get_trans_table_end_log_ts_and_timestamp(trans_table_end_log_ts, trans_table_timestamp))) {
      if (OB_ENTRY_NOT_EXIST != ret && OB_PARTITION_NOT_EXIST != ret) {
        LOG_WARN("failed to get trans table end_log_ts and timestamp", K(ret), K(ctx_->replica_op_arg_.key_));
      } else if (ADD_REPLICA_OP == ctx_->replica_op_arg_.type_ || MIGRATE_REPLICA_OP == ctx_->replica_op_arg_.type_ ||
                 FAST_MIGRATE_REPLICA_OP == ctx_->replica_op_arg_.type_ ||
                 RESTORE_REPLICA_OP == ctx_->replica_op_arg_.type_ ||
                 RESTORE_FOLLOWER_REPLICA_OP == ctx_->replica_op_arg_.type_ ||
                 RESTORE_STANDBY_OP == ctx_->replica_op_arg_.type_) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get trans table end_log_ts and timestamp", K(ret), K(ctx_->replica_op_arg_));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      ctx_->local_publish_version_ = local_pg_meta.storage_info_.get_data_info().get_publish_version();
      ctx_->local_last_replay_log_id_ = local_pg_meta.storage_info_.get_clog_info().get_last_replay_log_id();
      ctx_->local_last_replay_log_ts_ =
          std::max(trans_table_end_log_ts, local_pg_meta.storage_info_.get_data_info().get_last_replay_log_ts());
      common::ObReplicaType replica_type = ctx_->replica_op_arg_.dst_.get_replica_type();
      common::ObReplicaType local_replica_type = partition->get_replica_type();
      if (local_replica_type != replica_type &&
          !ObReplicaTypeCheck::change_replica_op_allow(local_replica_type, replica_type)) {
        ret = OB_OP_NOT_ALLOW;
        STORAGE_LOG(WARN, "change replica op not allow", K(ret), K(local_replica_type), K(replica_type), K(*ctx_));
      }
    }

    if (OB_SUCC(ret)) {
      if (REBUILD_REPLICA_OP == ctx_->replica_op_arg_.type_) {
        if (pg_storage->is_replica_with_remote_memstore()) {
          ctx_->use_slave_safe_read_ts_ = false;
          LOG_INFO("rebuild replica op which memstore percent is 0 do not use slave safe read ts", K(replica_property));
        }
      } else if (ctx_->is_leader_restore_archive_data()) {
        if (OB_FAIL(partition->get_pg_storage().get_pg_meta(ctx_->pg_meta_))) {
          ctx_->result_ = ret;
          STORAGE_LOG(WARN, "get local pg meta fail", K(ret), K(ctx_->replica_op_arg_.key_));
        }
      }
    }
  }

  if (OB_FAIL(ret) && NULL != ctx_) {
    ctx_->can_rebuild_ = false;
    LOG_WARN("failed during try_hold_local_partition, set cannot rebuild", K(ret), K(*ctx_));
  }
  return ret;
}

int ObMigratePrepareTask::choose_migrate_src()
{
  int ret = OB_SUCCESS;
  const int64_t local_cluster_id = GCONF.cluster_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    const ObReplicaOpType op_type = ctx_->replica_op_arg_.type_;
    switch (op_type) {
      case VALIDATE_BACKUP_OP:
      case BACKUP_BACKUPSET_OP:
      case BACKUP_ARCHIVELOG_OP: {
        LOG_INFO("no need to choose migrate src", K(ret));
        break;
      }
      case RESTORE_REPLICA_OP: {
        if (OB_FAIL(choose_restore_migrate_src(ctx_->replica_op_arg_, ctx_->pg_meta_, ctx_->migrate_src_info_))) {
          LOG_WARN("failed to choose restore migrate src", K(ret));
        }
        break;
      }
      case BACKUP_REPLICA_OP: {
        if (OB_FAIL(choose_backup_migrate_src(ctx_->replica_op_arg_, ctx_->pg_meta_, ctx_->migrate_src_info_))) {
          LOG_WARN("failed to choose restore migrate src", K(ret));
        }
        break;
      }
      case REBUILD_REPLICA_OP: {
        if (OB_FAIL(choose_ob_rebuild_src(ctx_->replica_op_arg_, ctx_->pg_meta_, ctx_->migrate_src_info_))) {
          LOG_WARN("failed to choose rebuild src", K(ret));
        }
        break;
      }
      case CHANGE_REPLICA_OP:
      case ADD_REPLICA_OP:
      case MIGRATE_REPLICA_OP: {
        if (ctx_->replica_op_arg_.cluster_id_ != local_cluster_id) {
          ret = OB_ERR_SYS;
          LOG_WARN("can not migrate from diff cluster", K(*ctx_), K(local_cluster_id));
        } else if (OB_FAIL(choose_ob_migrate_src(ctx_->replica_op_arg_, ctx_->pg_meta_, ctx_->migrate_src_info_))) {
          LOG_WARN("fail to choose ob master migrate src", K(ret));
        }
        LOG_INFO("migrate choose src", K(*ctx_));
        break;
      }
      case RESTORE_STANDBY_OP: {
        if (OB_FAIL(choose_standby_restore_src(ctx_->replica_op_arg_, ctx_->pg_meta_, ctx_->migrate_src_info_))) {
          LOG_WARN("failed to choose standby restore src", K(ret));
        }
        break;
      }
      case COPY_LOCAL_INDEX_OP:
      case COPY_GLOBAL_INDEX_OP:
      case RESTORE_FOLLOWER_REPLICA_OP:
      case FAST_MIGRATE_REPLICA_OP: {
        // these operation type no need choose src, use rs provided
        if (OB_FAIL(choose_recommendable_src(ctx_->replica_op_arg_, ctx_->pg_meta_, ctx_->migrate_src_info_))) {
          LOG_WARN("failed to choose recommendable src", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected replica op type", K(ret), K(*ctx_));
        break;
      }
    }  // end-switch
  }
  return ret;
}

int ObMigratePrepareTask::choose_ob_migrate_src(
    const ObReplicaOpArg &arg, ObPartitionGroupMeta &pg_meta, ObMigrateSrcInfo &src_info)
{
  int ret = OB_SUCCESS;
  const int64_t local_cluster_id = GCONF.cluster_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (ctx_->replica_op_arg_.cluster_id_ != local_cluster_id &&
             MIGRATE_REPLICA_OP == ctx_->replica_op_arg_.type_) {
    ret = OB_ERR_SYS;
    LOG_WARN("can not migrate from diff cluster", K(*ctx_), K(local_cluster_id));
  } else if (OB_FAIL(choose_ob_src(is_valid_migrate_src, arg, pg_meta, src_info))) {
    LOG_WARN("failed to choose ob src", K(ret), K(arg));
  } else if (ctx_->is_copy_index()) {
    ctx_->recovery_point_ctx_.recovery_point_key_array_.reset();
  }
  return ret;
}

int ObMigratePrepareTask::choose_ob_rebuild_src(
    const ObReplicaOpArg &arg, ObPartitionGroupMeta &meta, ObMigrateSrcInfo &src_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(choose_rebuild_src(arg, meta, src_info))) {
    LOG_WARN("failed to choose local cluster rebuild src, try choose remote cluster rebuild src", K(ret), K(arg));
  }
  return ret;
}

int ObMigratePrepareTask::choose_rebuild_src(
    const ObReplicaOpArg &arg, ObPartitionGroupMeta &meta, ObMigrateSrcInfo &src_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObMigrateSrcInfo> src_info_array;
  meta.reset();
  src_info_array.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(partition_service_->get_migrate_leader_and_parent(arg.key_, src_info_array))) {
    LOG_WARN("failed to get migrat src", K(ret), K(arg));
  } else if (OB_FAIL(choose_rebuild_candidate(arg, src_info_array, meta, src_info))) {
    LOG_WARN("fail to choose rebuild candidate", K(ret), K(arg));
  }

  if (OB_FAIL(ret)) {
    // overwrite ret
    src_info_array.reset();
    if (OB_FAIL(partition_service_->get_rebuild_src(arg.key_, arg.dst_.get_replica_type(), src_info_array))) {
      LOG_WARN("failed to get migrat src", K(ret), K(arg));
    } else if (OB_FAIL(choose_rebuild_candidate(arg, src_info_array, meta, src_info))) {
      LOG_WARN("fail to choose rebuild candidate", K(ret), K(arg));
    }
  }
  return ret;
}

int ObMigratePrepareTask::copy_needed_table_info(const ObReplicaOpArg &arg,
    const obrpc::ObPGPartitionMetaInfo &data_src_result, ObIArray<obrpc::ObFetchTableInfoResult> &table_info_res,
    ObIArray<uint64_t> &table_id_list, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    if (COPY_LOCAL_INDEX_OP != arg.type_) {
      if (OB_FAIL(table_id_list.assign(data_src_result.table_id_list_))) {
        LOG_WARN("failed to assign table id list", K(ret));
      } else if (OB_FAIL(table_info_res.assign(data_src_result.table_info_))) {
        LOG_WARN("failed to copy needed table info", K(ret), K(data_src_result));
      } else {
        found = true;
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < data_src_result.table_id_list_.count() && !found; ++i) {
        found = arg.index_id_ == data_src_result.table_id_list_.at(i);
        if (found) {
          if (OB_FAIL(table_id_list.push_back(data_src_result.table_id_list_.at(i)))) {
            LOG_WARN("failed to push table id into array", K(ret), K(arg));
          } else if (OB_FAIL(table_info_res.push_back(data_src_result.table_info_.at(i)))) {
            LOG_WARN("failed to push table info into array", K(ret), K(data_src_result), K(i));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (found) {
        if (table_id_list.count() != table_info_res.count()) {
          ret = OB_ERR_SYS;
          LOG_WARN("table id list count not equal to table info count",
              K(ret),
              "table_id_list count",
              table_id_list.count(),
              "table_info count",
              table_info_res.count());
        }
      }
    }
  }
  return ret;
}

int ObMigratePrepareTask::fetch_partition_group_info(
    const ObReplicaOpArg &arg, const ObMigrateSrcInfo &src_info, ObPartitionGroupInfoResult &result)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!src_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fetch rebuild partition info get invalid argument", K(ret), K(src_info));
  } else if (OB_FAIL(rpc_->post_fetch_partition_group_info_request(src_info.src_addr_,
                 arg.key_,
                 arg.dst_.get_replica_type(),
                 src_info.cluster_id_,
                 ctx_->use_slave_safe_read_ts_,
                 result.result_))) {
    LOG_WARN("fail to post fetch partition info request", K(ret), K(src_info), K(arg));
  } else {
    result.choose_src_info_ = src_info;
  }

  return ret;
}

int ObMigratePrepareTask::choose_rebuild_candidate(const ObReplicaOpArg &arg,
    const common::ObIArray<ObMigrateSrcInfo> &src_info_array, ObPartitionGroupMeta &meta, ObMigrateSrcInfo &src_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObMigrateSrcInfo> same_region_array;
  ObArray<ObMigrateSrcInfo> diff_region_array;
  bool find_src = false;
  const int64_t local_cluster_id = GCONF.cluster_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (src_info_array.empty() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("choose fetch candidate get invalid argument", K(ret), K(src_info_array.count()), K(arg));
  } else if (OB_FAIL(split_candidate_with_region(src_info_array, same_region_array, diff_region_array))) {
    LOG_WARN("fail to split candidate with region", K(ret));
  } else if (src_info_array.count() != (same_region_array.count() + diff_region_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("split candidate error ",
        K(ret),
        K(src_info_array.count()),
        K(same_region_array.count()),
        K(diff_region_array.count()));
  } else {
    // first choose same region
    ObPartitionGroupInfoResult result;
    ObPartitionGroupInfoResult tmp_result;
    if (OB_SUCCESS != (tmp_ret = fetch_suitable_rebuild_src(arg, same_region_array, result))) {
      LOG_WARN("fail to fetch same region suitable src and result", K(tmp_ret), K(arg));
    } else {
      find_src = result.is_valid() && result.result_.is_log_sync_;
      if (find_src && OB_FAIL(ctx_->recovery_point_ctx_.recovery_point_key_array_.assign(
                          result.result_.recovery_point_key_array_))) {
        LOG_WARN("failed to assign recovery point key array", K(ret), K(arg));
      }
    }
    // if not find same region src, then try choose from all region
    if (OB_SUCC(ret) && !find_src) {
      if (OB_FAIL(fetch_suitable_rebuild_src(arg, diff_region_array, tmp_result))) {
        LOG_WARN("fail to fetch diff region suitable src and result", K(ret), K(arg));
      } else if (OB_FAIL(copy_rebuild_partition_info_result(tmp_result, result, find_src))) {
        LOG_WARN("fail to copy rebuild partition info", K(ret), K(tmp_result), K(result));
      } else if (!find_src) {
      } else if (OB_FAIL(ctx_->recovery_point_ctx_.recovery_point_key_array_.assign(
                     result.result_.recovery_point_key_array_))) {
        LOG_WARN("failed to assgin recovery point key array", K(ret), K(result));
      }
    }

    if (OB_SUCC(ret) && !result.is_valid()) {
      ret = OB_DATA_SOURCE_NOT_EXIST;
      LOG_WARN("cannot choose_ob_rebuild_src", K(ret), K(arg), K(src_info_array), K(result), K(find_src));
    } else {
      LOG_INFO("choose_ob_rebuild_src", "src", result.choose_src_info_);
      if (OB_FAIL(meta.deep_copy(result.result_.pg_meta_))) {
        LOG_WARN("failed to deep copy meta", K(ret));
      } else {
        ctx_->mig_src_file_id_ = result.result_.pg_file_id_;
        ctx_->fetch_pg_info_compat_version_ = result.result_.compat_version_;
        src_info = result.choose_src_info_;
        if (src_info.cluster_id_ != local_cluster_id) {
          // restore point can not create to standby cluster
          ctx_->recovery_point_ctx_.recovery_point_key_array_.reset();
        }
      }
    }
  }
  return ret;
}

int ObMigratePrepareTask::copy_rebuild_partition_info_result(
    ObPartitionGroupInfoResult &tmp_result, ObPartitionGroupInfoResult &target_result, bool &find_src)
{
  int ret = OB_SUCCESS;
  find_src = false;
  if (tmp_result.is_valid()) {
    bool need_assign = false;
    if (target_result.is_valid()) {
      if (tmp_result.result_.pg_meta_.storage_info_.get_data_info().get_publish_version() >
          target_result.result_.pg_meta_.storage_info_.get_data_info().get_publish_version()) {
        need_assign = true;
      }
    } else {
      need_assign = true;
    }

    if (need_assign) {
      target_result.reset();
      if (OB_FAIL(target_result.assign(tmp_result))) {
        LOG_WARN("fail to assign out result", K(ret), K(tmp_result));
      } else {
        find_src = target_result.is_valid() && target_result.result_.is_log_sync_;
      }
    }
  }
  return ret;
}

int ObMigratePrepareTask::split_candidate_with_region(const ObIArray<ObMigrateSrcInfo> &src_info_array,
    ObIArray<ObMigrateSrcInfo> &same_region_array, ObIArray<ObMigrateSrcInfo> &diff_region_array)
{
  int ret = OB_SUCCESS;
  ObRegion local_addr_region;
  const ObAddr &self_addr = OBSERVER.get_self();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (src_info_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("split candidate with region get invaid argument", K(ret), K(src_info_array.count()));
  } else if (OB_FAIL(partition_service_->get_server_region(self_addr, local_addr_region))) {
    LOG_WARN("fail to get local src region", K(ret), K(self_addr));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < src_info_array.count(); ++i) {
      ObRegion tmp_region;
      const ObMigrateSrcInfo &src = src_info_array.at(i);
      if (OB_FAIL(partition_service_->get_server_region_across_cluster(src.src_addr_, tmp_region))) {
        LOG_WARN("fail to get server region", K(ret), K(src));
      } else if (local_addr_region == tmp_region) {
        if (OB_FAIL(same_region_array.push_back(src))) {
          LOG_WARN("fail to push src into same region array", K(ret), K(src));
        }
      } else {
        if (OB_FAIL(diff_region_array.push_back(src))) {
          LOG_WARN("fail to push src into diff region array", K(ret), K(src));
        }
      }
    }
  }
  return ret;
}

int ObMigratePrepareTask::fetch_suitable_rebuild_src(const ObReplicaOpArg &arg,
    const common::ObIArray<ObMigrateSrcInfo> &src_array, ObPartitionGroupInfoResult &out_result)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  ObPGStorage *pg_storage = NULL;
  ObPartitionGroupMeta local_pg_meta;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fetch suitable src get invalid argument", K(ret), K(arg));
  } else if (OB_ISNULL(partition = ctx_->partition_guard_.get_partition_group()) ||
             OB_ISNULL(pg_storage = &(partition->get_pg_storage()))) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "partition must not null", K(ret));
  } else if (OB_FAIL(pg_storage->get_pg_meta(local_pg_meta))) {
    LOG_WARN("failed to get pg meta", K(ret));
  } else {
    ObPartitionGroupInfoResult tmp_result;
    int64_t max_major_snapshot_version = local_pg_meta.report_status_.snapshot_version_;
    int64_t max_minor_snapshot_version = ctx_->local_publish_version_;
    // TODO() handle 22x compatible
    uint64_t max_migrate_replay_log_ts =
        std::max(local_pg_meta.get_migrate_replay_log_ts(), ctx_->local_last_replay_log_ts_);
    int64_t max_last_replay_log_id = ctx_->local_last_replay_log_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < src_array.count(); ++i) {
      tmp_result.reset();
      const ObMigrateSrcInfo &src_info = src_array.at(i);
      bool is_valid_src = false;
      bool is_suitable = false;
      if (!src_info.is_valid()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("invalid src addr", K(src_info));
      } else if (OB_SUCCESS != (tmp_ret = fetch_partition_group_info(arg, src_info, tmp_result))) {
        LOG_WARN("fail to fetch rebuild partition info", K(tmp_ret), K(src_info));
      } else if (OB_FAIL(is_valid_rebuild_src(tmp_result.result_, *ctx_, is_valid_src))) {
        LOG_WARN("failed to check rebuild src valid", K(ret), K(tmp_result));
      } else if (!is_valid_src) {
        // do nothing
      } else {
        const ObDataStorageInfo &data_info = tmp_result.result_.pg_meta_.storage_info_.get_data_info();
        const int64_t remote_minor_snapshot_version = data_info.get_publish_version();
        const int64_t remote_major_snapshot_version = tmp_result.result_.pg_meta_.report_status_.snapshot_version_;
        const uint64_t remote_last_replay_log_id =
            tmp_result.result_.pg_meta_.storage_info_.get_clog_info().get_last_replay_log_id();
        const uint64_t remote_migrate_replay_log_ts = tmp_result.result_.pg_meta_.get_migrate_replay_log_ts();
        bool is_suitable = false;
        const bool is_empty_pg = tmp_result.result_.pg_meta_.partitions_.empty();
        // new freeze might generate empty log_ts_range, still need use version compare
        // pg upgrade from 22x will always has replay_log_ts with 4096
        if (remote_migrate_replay_log_ts > max_migrate_replay_log_ts) {
          is_suitable = true;
        } else if (!ctx_->is_migrate_compat_version() && remote_migrate_replay_log_ts < max_migrate_replay_log_ts) {
          // from 3.1, replay_log_ts is an accurate metric
          is_suitable = false;
        } else if (is_empty_pg) {
          if (remote_last_replay_log_id > max_last_replay_log_id) {
            is_suitable = true;
          }
        } else if (remote_minor_snapshot_version > max_minor_snapshot_version) {
          is_suitable = true;
        } else if (remote_minor_snapshot_version < max_minor_snapshot_version) {
          is_suitable = false;
        } else if (remote_major_snapshot_version > max_major_snapshot_version) {
          is_suitable = true;
        } else if (remote_major_snapshot_version < max_major_snapshot_version) {
          is_suitable = false;
        } else if (remote_last_replay_log_id > max_last_replay_log_id) {
          is_suitable = true;
        } else {  // remote_last_replay_log_id <= max_last_replay_log_id
          is_suitable = false;
        }

#ifdef ERRSIM
        if (OB_SUCC(ret) && !is_suitable) {
          is_suitable = ObServerConfig::get_instance()._ob_enable_rebuild_on_purpose;
        }
#endif
        if (is_suitable) {
          out_result.reset();
          if (OB_FAIL(out_result.assign(tmp_result))) {
            LOG_WARN("fail to assign out result", K(ret), K(tmp_result));
          } else {
            max_minor_snapshot_version = std::max(remote_minor_snapshot_version, max_minor_snapshot_version);
            max_major_snapshot_version = remote_major_snapshot_version;
            max_last_replay_log_id = remote_last_replay_log_id;
            max_migrate_replay_log_ts = remote_migrate_replay_log_ts;
            LOG_INFO("find one candidate src",
                K(src_info),
                K(remote_minor_snapshot_version),
                K(max_minor_snapshot_version),
                K(remote_major_snapshot_version),
                K(remote_last_replay_log_id),
                K(remote_migrate_replay_log_ts));
          }
        } else {
          LOG_INFO("src snapshot version cannot use as src",
              K(src_info),
              K(remote_minor_snapshot_version),
              K(max_minor_snapshot_version),
              K(remote_major_snapshot_version),
              K(max_major_snapshot_version),
              K(remote_last_replay_log_id),
              K(max_last_replay_log_id),
              K(remote_migrate_replay_log_ts),
              K(max_migrate_replay_log_ts),
              K_(ctx_->fetch_pg_info_compat_version));
        }
      }
    }
  }
  return ret;
}

int ObMigratePrepareTask::choose_restore_migrate_src(
    const ObReplicaOpArg &arg, ObPartitionGroupMeta &pg_meta, ObMigrateSrcInfo &src_info)
{
  int ret = OB_SUCCESS;
  const int64_t compatible = ctx_->replica_op_arg_.phy_restore_arg_.restore_info_.compatible_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(ctx_->restore_meta_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore meta read is NULL", K(ret), KP(ctx_->restore_meta_reader_));
  } else {
    src_info.src_addr_ = arg.data_src_.get_server();
    src_info.cluster_id_ = arg.cluster_id_;
    if (OB_FAIL(ctx_->restore_meta_reader_->fetch_partition_group_meta(pg_meta))) {
      LOG_WARN("fail to fetch partition store meta", K(ret), K(arg));
    } else {
      if (OB_BACKUP_COMPATIBLE_VERSION_V1 == compatible || OB_BACKUP_COMPATIBLE_VERSION_V2 == compatible) {
        ctx_->fetch_pg_info_compat_version_ = ObFetchPGInfoArg::FETCH_PG_INFO_ARG_COMPAT_VERSION_V1;
      } else if (OB_BACKUP_COMPATIBLE_VERSION_V3 == compatible || OB_BACKUP_COMPATIBLE_VERSION_V4 == compatible) {
        ctx_->fetch_pg_info_compat_version_ = ObFetchPGInfoArg::FETCH_PG_INFO_ARG_COMPAT_VERSION_V2;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("restore compatible version is unexpected", K(ret), K(compatible), "arg", arg);
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succeed to choose_restore_migrate_src", K(ret), K(arg), K(pg_meta));
  }
  return ret;
}

int ObMigratePrepareTask::choose_phy_restore_follower_src(
    const ObReplicaOpArg &arg, ObPartitionGroupMeta &pg_meta, ObMigrateSrcInfo &src_info)
{
  int ret = OB_SUCCESS;
  ObPartitionGroupInfoResult src_result;
  ObMigrateSrcInfo tmp_src_info;
  tmp_src_info.src_addr_ = arg.data_src_.get_server();
  tmp_src_info.cluster_id_ = arg.cluster_id_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(fetch_partition_group_info(arg, tmp_src_info, src_result))) {
    LOG_WARN("failed to fetch partition info", K(ret), K(arg));
  } else if (OB_FAIL(pg_meta.deep_copy(src_result.result_.pg_meta_))) {
    LOG_WARN("Failed to copy pg meta", K(ret));
  } else {
    src_info = tmp_src_info;
    LOG_INFO("succeed to choose_phy_restore_follower src", K(ret), K(arg), K(pg_meta));
  }
  return ret;
}

// TODO(muwei.ym) delete it later
int ObMigratePrepareTask::choose_backup_migrate_src(
    const ObReplicaOpArg &arg, ObPartitionGroupMeta &pg_meta, ObMigrateSrcInfo &src_info)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!pg_meta.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup pg meta is invalid", K(ret), K(pg_meta));
  } else {
    src_info.src_addr_ = arg.data_src_.get_server();
    src_info.cluster_id_ = arg.cluster_id_;
  }
  DEBUG_SYNC(BACKUP_BEFROE_CHOOSE_SRC);
  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "succeed to choose_backup_migrate_src", K(ret), K(arg), K(pg_meta));
  }
  return ret;
}

int ObMigratePrepareTask::choose_standby_restore_src(
    const ObReplicaOpArg &arg, ObPartitionGroupMeta &pg_meta, ObMigrateSrcInfo &src_info)
{
  int ret = OB_SUCCESS;
  const int64_t local_cluster_id = GCONF.cluster_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (ctx_->replica_op_arg_.cluster_id_ != local_cluster_id &&
             RESTORE_STANDBY_OP == ctx_->replica_op_arg_.type_) {
    ret = OB_ERR_SYS;
    LOG_WARN("can not migrate from diff cluster", K(*ctx_), K(local_cluster_id));
  } else if (OB_FAIL(choose_ob_src(is_valid_standby_restore_src, arg, pg_meta, src_info))) {
    LOG_WARN("failed to choose ob src", K(ret), K(arg));
  } else {
    ctx_->recovery_point_ctx_.recovery_point_key_array_.reset();
  }
  return ret;
}

int ObMigratePrepareTask::choose_ob_src(
    IsValidSrcFunc is_valid_src, const ObReplicaOpArg &arg, ObPartitionGroupMeta &pg_meta, ObMigrateSrcInfo &src_info)
{
  int ret = OB_SUCCESS;
  ObPartitionGroupInfoResult data_src_result;
  ObArray<ObMigrateSrcInfo> src_info_array;
  bool find_suitable_src = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_minor_src_candidate_with_region(arg, src_info_array))) {
    LOG_WARN("failed to get minor src candidate with region", K(ret), K(arg));
  } else if (!src_info_array.empty()) {
    if (OB_FAIL(get_migrate_suitable_src(src_info_array, arg, is_valid_src, find_suitable_src, pg_meta, src_info))) {
      LOG_WARN("failed to get migrate suitable src", K(ret));
    }
  }

  // rewrite ret
  if (!find_suitable_src) {
    src_info_array.reset();
    if (OB_FAIL(get_minor_src_candidate_without_region(arg, src_info_array))) {
      LOG_WARN("failed to get minor src candidate without region", K(ret), K(arg));
    } else if (src_info_array.empty()) {
      ret = OB_DATA_SOURCE_NOT_EXIST;
      STORAGE_LOG(WARN, "no available src, migrate failed", K(ret), K(arg), K(src_info_array));
    } else if (OB_FAIL(
                   get_migrate_suitable_src(src_info_array, arg, is_valid_src, find_suitable_src, pg_meta, src_info))) {
      LOG_WARN("failed to get migrate suitable src", K(ret));
    }
  }

  if (OB_SUCC(ret) && !find_suitable_src) {
    ret = OB_DATA_SOURCE_NOT_EXIST;
    STORAGE_LOG(WARN, "no available src, migrate failed", K(ret), K(arg), K(src_info_array));
  }
  return ret;
}

int ObMigratePrepareTask::is_valid_migrate_src(
    const obrpc::ObFetchPGInfoResult &result, ObMigrateCtx &ctx, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  const ObDataStorageInfo &data_info = result.pg_meta_.storage_info_.get_data_info();
  const ObBaseStorageInfo &clog_info = result.pg_meta_.storage_info_.get_clog_info();
  const ObReplicaType replica_type = ctx.replica_op_arg_.dst_.get_replica_type();
  const int64_t remote_last_replay_log_id = result.pg_meta_.storage_info_.get_clog_info().get_last_replay_log_id();
  const int64_t remote_publish_version = data_info.get_publish_version();
  const bool is_empty_pg = result.pg_meta_.partitions_.empty();

  if (!ObReplicaTypeCheck::is_replica_with_memstore(replica_type) &&
      !ObReplicaTypeCheck::is_replica_with_ssstore(replica_type)) {
    is_valid = true;
    LOG_INFO("data is not needed, skip check src valid");
  } else if (data_info.is_created_by_new_minor_freeze() &&
             data_info.get_last_replay_log_ts() < ctx.local_last_replay_log_ts_ &&
             CHANGE_REPLICA_OP != ctx.replica_op_arg_.type_) {
    LOG_INFO("replica with smaller last_replay_log_id is not a valid migrate src",
        "remote last_replay_log_ts",
        data_info.get_last_replay_log_ts(),
        "local last_replay_log_ts",
        ctx.local_last_replay_log_ts_);
  } else if (!can_migrate_src_skip_log_sync(result, ctx) && !result.is_log_sync_) {
    LOG_INFO("src log not sync", K(result));
  } else if (CHANGE_REPLICA_OP == ctx.replica_op_arg_.type_) {
    // no need check last replay log id
    is_valid = true;
  } else if (remote_last_replay_log_id < ctx.local_last_replay_log_id_) {
    LOG_INFO("src last replay log id  must not less than local",
        "remote_last_replay_log_id",
        remote_last_replay_log_id,
        "local_last_replay_log_id",
        ctx.local_last_replay_log_id_);
  } else {
    is_valid = true;
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_VALID_MIGRATE_SRC) OB_SUCCESS;
  }
#endif
  return ret;
}

int ObMigratePrepareTask::is_valid_standby_restore_src(
    const obrpc::ObFetchPGInfoResult &result, ObMigrateCtx &ctx, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  const ObDataStorageInfo &data_info = result.pg_meta_.storage_info_.get_data_info();
  const ObBaseStorageInfo &clog_info = result.pg_meta_.storage_info_.get_clog_info();
  const ObReplicaType replica_type = ctx.replica_op_arg_.dst_.get_replica_type();
  const int64_t remote_last_replay_log_id = result.pg_meta_.storage_info_.get_clog_info().get_last_replay_log_id();

  // TODO() standby cluster need choose data version >= self data version as src
  if (!ObReplicaTypeCheck::is_replica_with_ssstore(result.pg_meta_.replica_type_)) {
    LOG_WARN("src not has ssstore, cannot copy sstable", K(result));
  } else if (REPLICA_NOT_RESTORE != result.pg_meta_.is_restore_) {
    LOG_INFO("data src is doing restore, cannot copy sstable");
  } else if (!ObReplicaTypeCheck::is_replica_with_memstore(replica_type) &&
             !ObReplicaTypeCheck::is_replica_with_ssstore(replica_type)) {
    is_valid = true;
    LOG_INFO("data is not needed, skip check src valid");
  } else if (remote_last_replay_log_id < ctx.local_last_replay_log_id_) {
    LOG_INFO("src last replay log id  must not less than local",
        "remote_last_replay_log_id",
        remote_last_replay_log_id,
        "local_last_replay_log_id",
        ctx.local_last_replay_log_id_);
  } else {
    is_valid = true;
  }

  return ret;
}

int ObMigratePrepareTask::is_valid_rebuild_src(
    const obrpc::ObFetchPGInfoResult &result, ObMigrateCtx &ctx, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  const ObReplicaType replica_type = ctx.replica_op_arg_.dst_.get_replica_type();
  const int64_t remote_last_replay_log_id = result.pg_meta_.storage_info_.get_clog_info().get_last_replay_log_id();

  if (REBUILD_REPLICA_OP != ctx.replica_op_arg_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is valid rebuild src get invalid argument", K(ret), K(ctx));
  } else if (ObReplicaTypeCheck::is_replica_with_ssstore(replica_type) &&
             !ObReplicaTypeCheck::is_replica_with_ssstore(result.pg_meta_.replica_type_)) {
    LOG_WARN("src not has ssstore, cannot copy sstable", K(result));
  } else if (remote_last_replay_log_id <= ctx.local_last_replay_log_id_) {
    LOG_INFO("src last replay log id  must not less than local",
        "remote_last_replay_log_id",
        remote_last_replay_log_id,
        "local_last_replay_log_id",
        ctx.local_last_replay_log_id_);
#ifdef ERRSIM
    is_valid = ObServerConfig::get_instance()._ob_enable_rebuild_on_purpose;
#endif
  } else {
    is_valid = true;
  }
  return ret;
}

bool ObMigratePrepareTask::can_migrate_src_skip_log_sync(const obrpc::ObFetchPGInfoResult &result, ObMigrateCtx &ctx)
{
  bool b_ret = false;

  if (REPLICA_NOT_RESTORE == result.pg_meta_.is_restore_) {
    b_ret = false;
  } else if (ADD_REPLICA_OP == ctx.replica_op_arg_.type_ || MIGRATE_REPLICA_OP == ctx.replica_op_arg_.type_) {
    if (result.pg_meta_.is_restore_ > REPLICA_NOT_RESTORE &&
        result.pg_meta_.is_restore_ <= REPLICA_RESTORE_MEMBER_LIST) {
      b_ret = true;
    }
  }
  return b_ret;
}

int ObMigratePrepareTask::build_migrate_pg_partition_info()
{
  int ret = OB_SUCCESS;
  ObIPGPartitionBaseDataMetaObReader *reader = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (VALIDATE_BACKUP_OP == ctx_->replica_op_arg_.type_ || BACKUP_BACKUPSET_OP == ctx_->replica_op_arg_.type_ ||
             BACKUP_ARCHIVELOG_OP == ctx_->replica_op_arg_.type_) {
    // do nothing
  } else if (OB_FAIL(get_partition_table_info_reader(ctx_->migrate_src_info_, reader))) {
    LOG_WARN("failed to get partition info reader", K(ret), K(ctx_));
  } else {
    if (COPY_LOCAL_INDEX_OP == ctx_->replica_op_arg_.type_) {
      if (OB_FAIL(build_index_partition_info(ctx_->replica_op_arg_, reader))) {
        LOG_WARN("failed to build index partition info", K(ret), K(ctx_->replica_op_arg_));
      }
    } else {
      if (OB_FAIL(build_table_partition_info(ctx_->replica_op_arg_, reader))) {
        LOG_WARN("failed to build table partition info", K(ret), K(ctx_->replica_op_arg_));
      }
    }
  }

  if (NULL != reader) {
    cp_fty_->free(reader);
  }
  return ret;
}

int ObMigratePrepareTask::build_migrate_partition_info(const ObPGPartitionMetaInfo &partition_meta_info,
    const common::ObIArray<obrpc::ObFetchTableInfoResult> &table_info_res,
    const common::ObIArray<uint64_t> &table_id_list, ObPartitionMigrateCtx &part_migrate_ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable::TableKey> local_tables_info;
  ObMigrateTableInfo table_info;
  int64_t cost_ts = ObTimeUtility::current_time();
  const ObPartitionKey &pkey = partition_meta_info.meta_.pkey_;
  ObMigratePartitionInfo &info = part_migrate_ctx.copy_info_;
  part_migrate_ctx.ctx_ = ctx_;
  DEBUG_SYNC(BEFORE_BUILD_MIGRATE_PARTITION_INFO);

  if (!is_inner_table(ctx_->replica_op_arg_.key_.get_table_id())) {
    DEBUG_SYNC(BEFORE_BUILD_MIGRATE_PARTITION_INFO_USER_TABLE);
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!partition_meta_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build migrate partition info get invalid argument", K(ret), K(partition_meta_info));
  } else if (OB_FAIL(info.meta_.deep_copy(partition_meta_info.meta_))) {
    LOG_WARN("fail to deep copy partition store meta", K(ret), K(partition_meta_info.meta_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_info_res.count(); ++i) {
      table_info.reuse();
      local_tables_info.reuse();
      const obrpc::ObFetchTableInfoResult &table_res = table_info_res.at(i);
      const uint64_t table_id = table_id_list.at(i);
      LOG_INFO("build_migrate_partition_info for table", "table_id", table_id);

      if (OB_SUCC(ret)) {
        if (OB_FAIL(remove_uncontinue_local_tables(pkey, table_id))) {
          LOG_WARN("failed to remove uncontinue local tables", K(ret), K(pkey), K(table_id));
        } else if (OB_FAIL(get_local_table_info(table_id, pkey, local_tables_info))) {
          LOG_WARN("failed to get local table info", K(ret));
        } else if (OB_FAIL(build_migrate_table_info(
                       table_id, pkey, local_tables_info, table_res, table_info, part_migrate_ctx))) {
          LOG_WARN("failed to build migrate table info", K(ret));
        } else if (OB_FAIL(info.table_id_list_.push_back(table_id))) {
          LOG_WARN("failed to push table id into array", K(ret));
        } else if (OB_FAIL(info.table_infos_.push_back(table_info))) {
          LOG_WARN("failed to add migrate table info", K(ret));
        } else {
          LOG_INFO("add table info", K(table_info));
          if (table_res.multi_version_start_ > info.meta_.multi_version_start_) {
            info.meta_.multi_version_start_ = table_res.multi_version_start_;
            LOG_INFO("update migrate multi version start", K(table_res.multi_version_start_));
          }
        }
      }
    }
  }

  cost_ts = ObTimeUtility::current_time() - cost_ts;
  LOG_INFO("build_migrate_partition_info",
      K(cost_ts),
      "pkey",
      info.meta_.pkey_,
      "src",
      info.src_,
      "table_count",
      info.table_id_list_.count());
  return ret;
}

int ObMigratePrepareTask::get_local_table_info(
    const uint64_t table_id, const ObPartitionKey &pkey, ObIArray<ObITable::TableKey> &local_tables_info)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  ObPGPartitionGuard guard;
  ObPGPartition *pg_partition = NULL;
  ObIPartitionStorage *storage = NULL;
  ObTablesHandle handle;
  local_tables_info.reuse();
  bool is_ready_for_read = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (NULL == (partition = ctx_->partition_guard_.get_partition_group())) {
    LOG_INFO("local partition not exist, no thing reuse", K(table_id));
  } else if (OB_FAIL(partition->get_pg_partition(pkey, guard))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get pg partition", K(ret), K(pkey));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("pg partition should not be NULL", K(ret), KP(pg_partition), K(pkey));
  } else if (OB_ISNULL(storage = pg_partition->get_storage())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("storage must not null", K(ret), KP(storage));
  } else if (OB_FAIL(static_cast<ObPartitionStorage *>(storage)->get_partition_store().get_migrate_tables(
                 table_id, handle, is_ready_for_read))) {
    LOG_WARN("failed to get effective tables", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < handle.get_count(); ++i) {
      const ObITable *table = handle.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must not null", K(ret));
      } else if (table->is_memtable()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("Unexpected local memtable", K(ret), K(i), KPC(table), K(handle));
      } else if (OB_FAIL(local_tables_info.push_back(table->get_key()))) {
        LOG_WARN("failed to add local table info into array", K(ret), K(table->get_key()));
      }
    }
  }

  return ret;
}

int ObMigratePrepareTask::build_migrate_table_info(const uint64_t table_id, const ObPartitionKey &pkey,
    ObIArray<ObITable::TableKey> &local_tables_info, const obrpc::ObFetchTableInfoResult &result,
    ObMigrateTableInfo &info, ObPartitionMigrateCtx &part_ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable::TableKey> remote_major_sstables;
  ObArray<ObITable::TableKey> remote_inc_sstables;
  ObArray<ObITable::TableKey> remote_gc_major_sstables;
  ObArray<ObITable::TableKey> remote_gc_inc_sstables;
  ObArray<ObITable::TableKey> local_major_sstables;
  ObArray<ObITable::TableKey> local_inc_sstables;
  bool &need_reuse_local_minor = part_ctx.need_reuse_local_minor_;
  info.reuse();
  info.table_id_ = table_id;
  // TODO  only use source minor sstable now, consider performance later
  need_reuse_local_minor = false;

  FLOG_INFO("start to build_migrate_table_info",
      K(result),
      K(info),
      K(local_major_sstables),
      K(local_inc_sstables),
      K(remote_gc_major_sstables),
      K(pkey),
      K(table_id),
      K(need_reuse_local_minor));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(ObTableKeyMgrUtil::classify_mgirate_tables(
                 table_id, local_tables_info, local_major_sstables, local_inc_sstables))) {
    LOG_WARN("failed to classify local migrate tables", K(ret));
  } else if (OB_FAIL(ObTableKeyMgrUtil::classify_mgirate_tables(
                 table_id, result.table_keys_, remote_major_sstables, remote_inc_sstables))) {
    LOG_WARN("failed to classify remote migrate tables", K(ret));
  } else if (OB_FAIL(ObTableKeyMgrUtil::classify_mgirate_tables(
                 table_id, result.gc_table_keys_, remote_gc_major_sstables, remote_gc_inc_sstables))) {
    LOG_WARN("failed to classify remote gc sstables", K(ret));
  } else if (OB_FAIL(check_remote_sstables(table_id, remote_major_sstables, remote_inc_sstables))) {
    LOG_WARN("failed to check remote sstables", K(ret), K(table_id));
  } else if (OB_FAIL(build_migrate_major_sstable(need_reuse_local_minor,
                 local_major_sstables,
                 local_inc_sstables,
                 remote_major_sstables,
                 remote_inc_sstables,
                 info.major_sstables_,
                 part_ctx))) {
    LOG_WARN("failed to build migrate major sstable", K(ret));
  } else if (OB_FAIL(build_migrate_minor_sstable(need_reuse_local_minor,
                 local_inc_sstables,
                 remote_inc_sstables,
                 remote_gc_inc_sstables,
                 info.minor_sstables_))) {
    LOG_WARN("failed to build migrate minor sstable", K(ret));
  } else if (OB_FAIL(fill_log_ts_for_compat(info))) {
    LOG_WARN("Failed to fill new log ts fro compat", K(info));
  } else if (OB_FAIL(check_can_reuse_sstable(pkey, info, part_ctx))) {
    LOG_WARN("failed to check can reuse sstable", K(ret));
  }

  if (OB_SUCC(ret)) {
    info.multi_version_start_ = result.multi_version_start_;
    info.ready_for_read_ = result.is_ready_for_read_;
    FLOG_INFO("succeed build_migrate_table_info",
        K(result),
        K(info),
        K(local_major_sstables),
        K(local_inc_sstables),
        K(remote_gc_major_sstables),
        K(pkey),
        K(table_id),
        K(need_reuse_local_minor));
  }
  return ret;
}

int ObMigratePrepareTask::fill_log_ts_for_compat(ObMigrateTableInfo &info)
{
  int ret = OB_SUCCESS;

  if (!ctx_->is_migrate_compat_version()) {
  } else {
    ObTableCompater table_compater;
    if (OB_FAIL(table_compater.add_tables(info.major_sstables_))) {
      STORAGE_LOG(WARN, "Failed to add major sstables for compat", K(ret), K(info.major_sstables_));
    } else if (OB_FAIL(table_compater.add_tables(info.minor_sstables_))) {
      STORAGE_LOG(WARN, "Failed to add minor sstables for compat", K(ret), K(info.minor_sstables_));
    } else if (OB_FAIL(table_compater.fill_log_ts())) {
      STORAGE_LOG(WARN, "Failed to set new log ts for compat", K(ret), K(table_compater));
    } else {
      ctx_->pg_meta_.storage_info_.get_data_info().set_last_replay_log_ts(ObTableCompater::OB_MAX_COMPAT_LOG_TS);
    }
  }

  return ret;
}

int ObMigratePrepareTask::check_remote_sstables(const uint64_t table_id,
    common::ObIArray<ObITable::TableKey> &remote_major_sstables,
    common::ObIArray<ObITable::TableKey> &remote_inc_tables)
{
  int ret = OB_SUCCESS;
  bool need_check_major_sstable = true;
  const share::schema::ObTableSchema *table_schema = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  share::schema::ObMultiVersionSchemaService &schema_service =
      share::schema::ObMultiVersionSchemaService::get_instance();
  const uint64_t fetch_tenant_id = is_inner_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (is_trans_table_id(table_id)) {
    need_check_major_sstable = false;
  } else if (OB_FAIL(schema_service.get_tenant_full_schema_guard(fetch_tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
    LOG_WARN("Failed to get table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    need_check_major_sstable = false;
    LOG_INFO("table schema is null, skip check major", K(table_id));
  } else if (table_schema->is_index_table() && INDEX_STATUS_AVAILABLE != table_schema->get_index_status()) {
    need_check_major_sstable = false;
    LOG_INFO("index table schema is not available, skip check major", K(table_id));
  } else if (REPLICA_NOT_RESTORE != ctx_->pg_meta_.is_restore_) {
    need_check_major_sstable = false;
    LOG_INFO("table is in restore status, skip check major", K(table_id));
  }

  if (OB_SUCC(ret)) {
    if (remote_major_sstables.empty()) {
      if (need_check_major_sstable) {
        ret = OB_DATA_SOURCE_NOT_VALID;
        LOG_WARN("major remote table not found", K(ret), K(table_id));
      } else if (remote_inc_tables.count() > 0) {
      } else {
        // no logic migrate, allow inc table be empty
      }
    }
  }

  if (OB_SUCC(ret) && remote_inc_tables.count() > 0) {
    if (OB_FAIL(check_remote_inc_sstables_continuity(remote_inc_tables))) {
      LOG_WARN(
          "failed to check remote inc sstables continuity", K(ret), K(remote_major_sstables), K(remote_inc_tables));
    }
  }

  return ret;
}

int ObMigratePrepareTask::check_remote_inc_sstables_continuity(common::ObIArray<ObITable::TableKey> &remote_inc_tables)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (OB_UNLIKELY(remote_inc_tables.empty())) {
    // skip empty inc tables
  } else {
    int64_t last_end_log_ts = remote_inc_tables.at(0).get_start_log_ts();
    for (int64_t i = 0; OB_SUCC(ret) && i < remote_inc_tables.count(); ++i) {
      const ObITable::TableKey &remote_inc_table = remote_inc_tables.at(i);
      if (remote_inc_table.is_complement_minor_sstable()) {
        // skip buffer minor sstable and complement sstable
      } else if (remote_inc_table.get_start_log_ts() != last_end_log_ts) {
        ret = OB_DATA_SOURCE_NOT_VALID;
        LOG_WARN(
            "minor sstables not continue", K(ret), K(i), K(remote_inc_table), K(last_end_log_ts), K(remote_inc_tables));
      } else {
        last_end_log_ts = remote_inc_table.get_end_log_ts();
      }
    }
  }
  return ret;
}

// TODO  only use source minor sstable now, consider performance later
int ObMigratePrepareTask::build_remote_minor_sstables(const common::ObIArray<ObITable::TableKey> &local_minor_sstables,
    const common::ObIArray<ObITable::TableKey> &tmp_remote_minor_sstables,
    const common::ObIArray<ObITable::TableKey> &tmp_remote_gc_minor_sstables,
    common::ObIArray<ObITable::TableKey> &remote_minor_sstables,
    common::ObIArray<ObITable::TableKey> &remote_gc_minor_sstables, bool &need_reuse_local_minor)
{
  return OB_NOT_SUPPORTED;
  int ret = OB_SUCCESS;
  remote_minor_sstables.reuse();
  remote_gc_minor_sstables.reuse();
  need_reuse_local_minor = !ctx_->is_migrate_compat_version();
  if (local_minor_sstables.empty()) {
    if (OB_FAIL(remote_minor_sstables.assign(tmp_remote_minor_sstables))) {
      LOG_WARN("failed to assign tables", K(ret));
    } else if (OB_FAIL(remote_gc_minor_sstables.assign(tmp_remote_gc_minor_sstables))) {
      LOG_WARN("failed to assign tables", K(ret));
    }
  } else if (need_reuse_local_minor) {
    const ObLogTsRange local_last_log_ts_range =
        local_minor_sstables.at(local_minor_sstables.count() - 1).log_ts_range_;
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_remote_minor_sstables.count(); ++i) {
      const ObITable::TableKey &remote_sstable = tmp_remote_minor_sstables.at(i);
      if (remote_minor_sstables.empty() &&
          remote_sstable.log_ts_range_.start_log_ts_ > local_last_log_ts_range.end_log_ts_) {
        need_reuse_local_minor = false;
        break;
      } else if (remote_sstable.log_ts_range_.end_log_ts_ > local_last_log_ts_range.end_log_ts_) {
        if (OB_FAIL(remote_minor_sstables.push_back(remote_sstable))) {
          LOG_WARN("failed to push back remote sstable", K(ret), K(remote_sstable));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!need_reuse_local_minor) {
        if (OB_FAIL(remote_minor_sstables.assign(tmp_remote_minor_sstables))) {
          LOG_WARN("failed to assign remote minor sstable", K(ret), K(tmp_remote_minor_sstables));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_remote_gc_minor_sstables.count(); ++i) {
          const ObITable::TableKey &remote_sstable = tmp_remote_gc_minor_sstables.at(i);
          if (remote_sstable.log_ts_range_.end_log_ts_ > local_last_log_ts_range.end_log_ts_) {
            if (OB_FAIL(remote_gc_minor_sstables.push_back(remote_sstable))) {
              LOG_WARN("failed to push back remote sstable", K(ret), K(remote_sstable));
            }
          }
        }
      }
    }
  } else {
    // migrate from compat version, only use remote sstable
    if (OB_FAIL(remote_minor_sstables.assign(tmp_remote_minor_sstables))) {
      LOG_WARN("failed to assign remote minor sstable", K(ret), K(tmp_remote_minor_sstables));
    }
  }
  return ret;
}

int ObMigratePrepareTask::build_migrate_major_sstable(const bool need_reuse_local_minor,
    ObIArray<ObITable::TableKey> &local_major_tables, ObIArray<ObITable::TableKey> &local_inc_tables,
    ObIArray<ObITable::TableKey> &remote_major_tables, ObIArray<ObITable::TableKey> &remote_inc_tables,
    ObIArray<ObMigrateTableInfo::SSTableInfo> &copy_sstables, ObPartitionMigrateCtx &part_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(build_migrate_major_sstable_v2_(need_reuse_local_minor,
          local_major_tables,
          local_inc_tables,
          remote_major_tables,
          remote_inc_tables,
          copy_sstables,
          part_ctx))) {
    LOG_WARN("failed to build_migrate_major_sstable", K(ret));
  }
  return ret;
}

int ObMigratePrepareTask::build_migrate_major_sstable_(ObIArray<ObITable::TableKey> &local_major_tables,
    ObIArray<ObITable::TableKey> &local_inc_tables, ObIArray<ObITable::TableKey> &remote_major_tables,
    ObIArray<ObITable::TableKey> &remote_inc_tables, ObIArray<ObMigrateTableInfo::SSTableInfo> &copy_sstables)
{
  int ret = OB_SUCCESS;
  // static func, skip check is_inited_
  int64_t local_major_max_snapshot_version = 0;
  int64_t local_inc_max_snapshot_version = 0;
  int64_t local_max_snapshot_version = 0;
  int64_t max_snapshot_version = 0;
  int64_t remote_min_base_version = 0;
  int64_t remote_max_snapshot_version = 0;
  bool local_has_major = false;
  // D type replica major need to consider compaction_interval. Do local compaction when greater than 0, and meanwhile
  // need local major
  const int64_t follower_replica_merge_level = GCONF._follower_replica_merge_level;

  for (int64_t i = 0; i < local_major_tables.count(); ++i) {
    const ObITable::TableKey &local_major_table = local_major_tables.at(i);
    if (ObITable::is_major_sstable(local_major_table.table_type_)) {
      local_has_major = true;
      local_major_max_snapshot_version = local_major_table.trans_version_range_.snapshot_version_;
    }
  }
  if (local_inc_tables.count() > 0) {
    local_inc_max_snapshot_version =
        local_inc_tables.at(local_inc_tables.count() - 1).trans_version_range_.snapshot_version_;
  }
  local_max_snapshot_version = std::max(local_inc_max_snapshot_version, local_major_max_snapshot_version);

  if (remote_inc_tables.count() > 0) {
    remote_min_base_version = remote_inc_tables.at(0).trans_version_range_.base_version_;
    remote_max_snapshot_version =
        remote_inc_tables.at(remote_inc_tables.count() - 1).trans_version_range_.snapshot_version_;
  }

  if (local_max_snapshot_version >= remote_min_base_version) {
    max_snapshot_version = std::max(local_max_snapshot_version, remote_max_snapshot_version);
  } else {
    max_snapshot_version = local_max_snapshot_version;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < remote_major_tables.count(); ++i) {
    bool use_local = false;
    const ObITable::TableKey &remote_major_table = remote_major_tables.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < local_major_tables.count() && !use_local; ++j) {
      if (local_major_tables.at(j) == remote_major_table) {
        use_local = true;
        LOG_INFO("use local major sstable", "table_key", remote_major_table);
      }
    }

    if (OB_SUCC(ret) && !use_local) {
      // check major tables can use minor sstable to merge
      if (local_has_major && max_snapshot_version >= remote_major_table.trans_version_range_.snapshot_version_ &&
          is_follower_d_major_merge(follower_replica_merge_level)) {
        use_local = true;
        LOG_INFO("local table can merge become need major table, no need copy",
            K(local_inc_tables),
            K(remote_inc_tables),
            K(local_major_tables),
            "need table_key",
            remote_major_table);
      }
    }

    if (BACKUP_REPLICA_OP == ctx_->replica_op_arg_.type_) {
      use_local = false;
    }
    if (OB_SUCC(ret) && !use_local) {
      ObMigrateTableInfo::SSTableInfo info;
      info.src_table_key_ = remote_major_table;
      info.dest_base_version_ = remote_major_table.trans_version_range_.base_version_;
      info.dest_log_ts_range_ = remote_major_table.log_ts_range_;
      if (OB_FAIL(copy_sstables.push_back(info))) {
        LOG_WARN("failed to add major sstables", K(ret));
      } else {
        ctx_->need_report_checksum_ = true;
        LOG_INFO("use remote major sstable", "table_key", remote_major_table);
      }
    }
  }

  if (OB_SUCC(ret)) {
    FLOG_INFO("build_migrate_major_sstable", K(local_major_tables), K(remote_major_tables), K(copy_sstables));
  }
  return ret;
}

int ObMigratePrepareTask::build_migrate_major_sstable_v2_(const bool need_reuse_local_minor,
    ObIArray<ObITable::TableKey> &local_major_tables, ObIArray<ObITable::TableKey> &local_inc_tables,
    ObIArray<ObITable::TableKey> &remote_major_tables, ObIArray<ObITable::TableKey> &remote_inc_tables,
    ObIArray<ObMigrateTableInfo::SSTableInfo> &copy_sstables, ObPartitionMigrateCtx &part_ctx)
{
  int ret = OB_SUCCESS;
  int64_t max_snapshot_version = 0;
  UNUSED(local_inc_tables);
  UNUSED(remote_inc_tables);
  UNUSED(need_reuse_local_minor);
  bool need_add_local_major = false;

  // D type replica major need to consider compaction_interval. Do local compaction when greater than 0, and meanwhile
  // need local major const int64_t follower_replica_merge_level = GCONF._follower_replica_merge_level;

  for (int64_t i = 0; i < local_major_tables.count(); ++i) {
    const ObITable::TableKey &local_major_table = local_major_tables.at(i);
    if (ObITable::is_major_sstable(local_major_table.table_type_)) {
      max_snapshot_version = local_major_table.get_snapshot_version();
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < remote_major_tables.count(); ++i) {
    const ObITable::TableKey &remote_major_table = remote_major_tables.at(i);
    if (remote_major_table.get_snapshot_version() <= max_snapshot_version && !remote_major_table.is_trans_sstable()) {
      need_add_local_major = true;
      continue;
    } else {
      ObMigrateTableInfo::SSTableInfo info;
      info.src_table_key_ = remote_major_table;
      info.dest_base_version_ = remote_major_table.get_base_version();
      info.dest_log_ts_range_ = remote_major_table.log_ts_range_;
      if (OB_FAIL(copy_sstables.push_back(info))) {
        LOG_WARN("failed to push back sstable info", K(ret), K(info));
      } else {
        ctx_->need_report_checksum_ = true;
        LOG_INFO("use remote major sstable", "table_key", remote_major_table);
      }
    }
  }

  if (OB_SUCC(ret) && need_add_local_major) {
    ObTableHandle table_handle;
    for (int64_t i = 0; OB_SUCC(ret) && i < local_major_tables.count(); ++i) {
      const ObITable::TableKey &local_major_table = local_major_tables.at(i);
      ObITable *table = NULL;
      table_handle.reset();
      if (OB_FAIL(ObPartitionService::get_instance().acquire_sstable(local_major_table, table_handle))) {
        LOG_WARN("failed to get complete sstable by key", K(ret), K(local_major_table));
      } else if (NULL == (table = table_handle.get_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), KP(table));
      } else if (OB_FAIL(part_ctx.add_sstable(*reinterpret_cast<ObSSTable *>(table)))) {
        LOG_WARN("failed to add sstable", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    FLOG_INFO("build_migrate_major_sstable",
        K(local_major_tables),
        K(remote_major_tables),
        K(copy_sstables),
        K(need_reuse_local_minor));
  }

  return ret;
}

int ObMigratePrepareTask::get_migrate_suitable_src(const common::ObIArray<ObMigrateSrcInfo> &src_info_array,
    const ObReplicaOpArg &arg, IsValidSrcFunc is_valid_src, bool &find_suitable_src, ObPartitionGroupMeta &pg_meta,
    ObMigrateSrcInfo &src_info)
{
  int ret = OB_SUCCESS;
  find_suitable_src = false;
  pg_meta.reset();
  src_info.reset();
  ObPartitionGroupInfoResult data_src_result;
  ObPartitionGroupInfoResult tmp_data_src_result;
  int64_t max_last_replay_log_id = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (src_info_array.empty() || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get migrate suitable src get invalid argument", K(ret), K(src_info_array), K(arg));
  } else {
    // skip ret, try all servers
    for (int64_t i = 0; i < src_info_array.count(); ++i) {
      const ObMigrateSrcInfo &tmp_src_info = src_info_array.at(i);
      bool is_src_valid = true;
      int64_t remote_last_replay_log_id = 0;
      tmp_data_src_result.reset();
      if (MYADDR == tmp_src_info.src_addr_) {
        // do nothing
      } else if (OB_FAIL(fetch_partition_group_info(arg, tmp_src_info, tmp_data_src_result))) {
        LOG_WARN("failed to fetch data src result", K(ret), K(arg), K(tmp_src_info));
        ret = OB_SUCCESS;
      } else if (OB_FAIL(is_valid_src(tmp_data_src_result.result_, *ctx_, is_src_valid))) {
        LOG_WARN("Failed to check is valid migrate src", K(ret), K(arg.data_src_), K(tmp_data_src_result));
      } else if (!is_src_valid) {
        STORAGE_LOG(INFO, "migrate src is not suitable, skip it", K(arg), K(tmp_src_info));
      } else if (FALSE_IT(
                     remote_last_replay_log_id =
                         tmp_data_src_result.result_.pg_meta_.storage_info_.get_clog_info().get_last_replay_log_id())) {
      } else if (max_last_replay_log_id <= remote_last_replay_log_id) {
        data_src_result.reset();
        if (OB_FAIL(data_src_result.assign(tmp_data_src_result))) {
          LOG_WARN("failed to assign partition info result", K(ret), K(tmp_data_src_result));
        } else {
          max_last_replay_log_id = remote_last_replay_log_id;
          find_suitable_src = true;
        }
      }
    }

    if (OB_SUCC(ret) && find_suitable_src) {
      if (OB_FAIL(pg_meta.deep_copy(data_src_result.result_.pg_meta_))) {
        LOG_WARN("Failed to copy pg meta", K(ret));
      } else if (OB_FAIL(ctx_->recovery_point_ctx_.recovery_point_key_array_.assign(
                     data_src_result.result_.recovery_point_key_array_))) {
        LOG_WARN("failed to assgin recovery point key array", K(ret), K(data_src_result));
      } else if (OB_FAIL(ctx_->set_is_restore_for_add_replica(pg_meta.is_restore_))) {
        LOG_WARN("Failed to set is restore", K(ret), K(pg_meta));
      } else {
        ctx_->mig_src_file_id_ = data_src_result.result_.pg_file_id_;
        ctx_->fetch_pg_info_compat_version_ = data_src_result.result_.compat_version_;
        src_info = data_src_result.choose_src_info_;
        LOG_INFO("succeed to choose_ob_migrate_src", K(ret), K(arg), K(pg_meta));
      }
    }
  }
  return ret;
}

int ObMigratePrepareTask::get_minor_src_candidate_with_region(
    const ObReplicaOpArg &arg, ObIArray<ObMigrateSrcInfo> &src_info_array)
{
  int ret = OB_SUCCESS;
  ObArray<ObMigrateSrcInfo> tmp_info_array;
  ObArray<ObMigrateSrcInfo> same_region_array;
  ObArray<ObMigrateSrcInfo> diff_region_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (OB_FAIL(partition_service_->get_migrate_member_list_src(arg.key_, tmp_info_array))) {
    LOG_WARN("failed to get migrate src", K(ret), "pkey", arg.key_);
  } else if (tmp_info_array.empty()) {
    LOG_INFO("migrate meta table src is empty");
  } else if (OB_FAIL(split_candidate_with_region(tmp_info_array, same_region_array, diff_region_array))) {
    LOG_WARN("failed to split candidate with region", K(ret), K(arg));
  } else if (OB_FAIL(src_info_array.assign(same_region_array))) {
    LOG_WARN("failed to assign src info array", K(ret));
  } else {
    LOG_INFO("get minor src with region", K(tmp_info_array), K(same_region_array));
  }
  return ret;
}

int ObMigratePrepareTask::get_minor_src_candidate_without_region(
    const ObReplicaOpArg &arg, common::ObIArray<ObMigrateSrcInfo> &src_info_array)
{
  int ret = OB_SUCCESS;

  src_info_array.reset();
  bool found_recommendable_src = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (OB_FAIL(partition_service_->get_migrate_leader_and_parent(arg.key_, src_info_array))) {
    LOG_WARN("failed to get migrate src", K(ret), "pkey", arg.key_);
  }

  // skip get migrate leader and parent ret, overwrite it
  {
    ret = OB_SUCCESS;
    for (int64_t i = 0; i < src_info_array.count() && !found_recommendable_src; ++i) {
      const ObMigrateSrcInfo &src_info = src_info_array.at(i);
      if (arg.data_src_.get_server() == src_info.src_addr_) {
        found_recommendable_src = true;
      }
    }

    if (!found_recommendable_src) {
      ObMigrateSrcInfo recommendable_src_info;
      recommendable_src_info.src_addr_ = arg.data_src_.get_server();
      recommendable_src_info.cluster_id_ = arg.cluster_id_;
      if (OB_FAIL(src_info_array.push_back(recommendable_src_info))) {
        LOG_WARN("failed to push migrate src into array", K(ret), K(recommendable_src_info));
      }
    }
  }

  LOG_INFO("get minor src candidate without region", K(ret), K(src_info_array));
  return ret;
}

int ObMigratePrepareTask::choose_recommendable_src(
    const ObReplicaOpArg &arg, ObPartitionGroupMeta &pg_meta, ObMigrateSrcInfo &src_info)
{
  int ret = OB_SUCCESS;
  ObPartitionGroupInfoResult src_result;
  ObMigrateSrcInfo tmp_src_info;
  tmp_src_info.src_addr_ = arg.data_src_.get_server();
  tmp_src_info.cluster_id_ = arg.cluster_id_;
  bool need_recovery_point = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(fetch_partition_group_info(arg, tmp_src_info, src_result))) {
    LOG_WARN("failed to fetch partition info", K(ret), K(arg));
  } else if (ctx_->is_only_copy_sstable()) {
    if (!ObReplicaTypeCheck::is_replica_with_ssstore(src_result.result_.pg_meta_.replica_type_)) {
      ret = OB_DATA_SOURCE_NOT_EXIST;
      LOG_WARN("src not has ssstore, cannot copy sstable", K(src_result));
    } else {
      need_recovery_point = false;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(pg_meta.deep_copy(src_result.result_.pg_meta_))) {
    LOG_WARN("Failed to copy pg meta", K(ret));
  } else {
    ctx_->mig_src_file_id_ = src_result.result_.pg_file_id_;
    ctx_->fetch_pg_info_compat_version_ = src_result.result_.compat_version_;
    src_info = tmp_src_info;
    LOG_INFO("succeed to choose_recommendable src", K(ret), K(arg), K(pg_meta));
  }
  return ret;
}

int ObMigratePrepareTask::build_migrate_minor_sstable(const bool need_reuse_local_minor,
    ObIArray<ObITable::TableKey> &local_inc_tables, ObIArray<ObITable::TableKey> &remote_inc_tables,
    ObIArray<ObITable::TableKey> &remote_gc_inc_sstables, ObIArray<ObMigrateTableInfo::SSTableInfo> &copy_sstables)
{
  int ret = OB_SUCCESS;
  // ObITable::Tablekey's table_key.version is mark major version flag which is smaller than copy sstables need to
  // remove remote'version is monotonically increasing continuously, and there is only one minor sstable for each
  // version there are some minor sstables for on version in local
  // TODO() errsim case may not pass
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (ctx_->is_copy_index() || LINK_SHARE_MAJOR_OP == ctx_->replica_op_arg_.type_) {
    // copy index only need major sstable
  } else if (remote_inc_tables.empty()) {
    LOG_INFO("remote inc table is empty, skip it",
        K(copy_sstables),
        K(local_inc_tables),
        K(remote_inc_tables),
        K(remote_gc_inc_sstables));
  } else if (!need_reuse_local_minor) {
    ObMigrateTableInfo::SSTableInfo info;
    for (int64_t i = 0; OB_SUCC(ret) && i < remote_inc_tables.count(); ++i) {
      info.reset();
      const ObITable::TableKey &remote_table = remote_inc_tables.at(i);
      info.src_table_key_ = remote_table;
      info.dest_base_version_ = remote_table.trans_version_range_.base_version_;
      info.dest_log_ts_range_ = remote_table.log_ts_range_;
      if (OB_FAIL(copy_sstables.push_back(info))) {
        LOG_WARN("failed to push back sstable info", K(ret), K(info));
      }
    }
  } else {
    // TODO  reuse local minor with continuous log ts range
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Reusing local minor sstable within migration is not supported", K(ret));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("build_migrate_minor_sstable",
        K(copy_sstables),
        K(local_inc_tables),
        K(remote_inc_tables),
        K(remote_gc_inc_sstables));
  }
  return ret;
}

int ObMigratePrepareTask::check_can_reuse_sstable(
    const ObPartitionKey &pkey, ObMigrateTableInfo &table_info, ObPartitionMigrateCtx &part_ctx)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (BACKUP_REPLICA_OP == ctx_->replica_op_arg_.type_) {
  } else if (NULL == (ctx_->partition_guard_.get_partition_group())) {
    LOG_INFO("local partition not exist, no thing reuse", K(table_info));
    // skip backup
  } else {
    // check major sstable
    bool is_reuse = false;
    for (int64_t i = table_info.major_sstables_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      is_reuse = false;
      if (OB_FAIL(check_and_reuse_sstable(pkey, table_info.major_sstables_.at(i).src_table_key_, is_reuse, part_ctx))) {
        LOG_WARN("failed to check can reuse sstable", K(ret), "table_key", table_info.major_sstables_.at(i));
      } else if (!is_reuse) {
        // do nothing
      } else if (OB_FAIL(table_info.major_sstables_.remove(i))) {
        LOG_WARN("failed to remove reused table", K(ret), "table_key", table_info.major_sstables_.at(i));
      }
    }

    if (OB_SUCC(ret)) {
      // check minor sstable
      for (int64_t i = table_info.minor_sstables_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
        is_reuse = false;
        ObITable::TableKey table_key;
        if (OB_FAIL(ObDestTableKeyManager::convert_sstable_info_to_table_key(
                table_info.minor_sstables_.at(i), table_key))) {
          LOG_WARN("failed to convert sstable info to table key", K(ret), K(table_info));
        } else if (OB_FAIL(check_and_reuse_sstable(pkey, table_key, is_reuse, part_ctx))) {
          LOG_WARN("failed to check can reuse sstable", K(ret), "table_key", table_key);
        } else if (is_reuse) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("minor sstable is reuse, unexpected !", K(ret), "src table info",
              table_info.minor_sstables_.at(i), "convert table key", table_key);
        }
        //Because trans sstable can not reuse, so minor sstables cannot be reuse too.
        //else if (OB_FAIL(check_and_reuse_sstable(pkey, table_key, is_reuse, part_ctx))) {
        //  LOG_WARN("failed to check can reuse sstable", K(ret), "table_key", table_key);
        //} else if (!is_reuse) {
        //  //do nothing
        //} else if (OB_FAIL(table_info.minor_sstables_.remove(i))) {
        //  LOG_WARN("failed to remove reused table", K(ret), "table_key", table_key);
        //}
      }
    }
  }
  return ret;
}

int ObMigrateUtil::get_report_result(const common::ObIArray<ObReportPartMigrationTask> &report_list,
    common::ObIArray<ObPartMigrationRes> &report_res_list)
{
  int ret = OB_SUCCESS;
  ObPartMigrationRes tmp_res;

  if (report_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "report list must not empty", K(ret));
  } else if (OB_FAIL(report_res_list.reserve(report_list.count()))) {
    STORAGE_LOG(WARN, "failed to reserve report_res_list", K(ret), K(report_list));
  } else {
    ObReplicaOpType type = report_list.at(0).arg_.type_;
    for (int64_t i = 0; OB_SUCC(ret) && i < report_list.count(); ++i) {
      const ObReplicaOpArg &arg = report_list.at(i).arg_;
      if (type != arg.type_) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(ERROR, "op type not same", K(ret), K(type), K(i), K(arg), K(report_list));
      } else {
        tmp_res.key_ = arg.key_;
        tmp_res.src_ = arg.src_;
        tmp_res.dst_ = arg.dst_;
        tmp_res.data_src_ = arg.data_src_;
        tmp_res.quorum_ = arg.quorum_;
        tmp_res.backup_arg_ = arg.backup_arg_;
        tmp_res.validate_arg_ = arg.validate_arg_;
        tmp_res.backup_backupset_arg_ = arg.backup_backupset_arg_;
        tmp_res.backup_archivelog_arg_ = arg.backup_archive_log_arg_;
        tmp_res.data_statics_ = report_list.at(i).data_statics_;
        tmp_res.result_ = report_list.at(i).result_;
        if (OB_FAIL(report_res_list.push_back(tmp_res))) {
          STORAGE_LOG(WARN, "failed to add report res list", K(ret));
        }
      }
    }
  }
  return ret;
}

// for split reuse, because dest pkey may not eqaul to table key's pkey
int ObMigratePrepareTask::check_and_reuse_sstable(
    const ObPartitionKey &pkey, const ObITable::TableKey &table_key, bool &is_reuse, ObPartitionMigrateCtx &part_ctx)
{
  int ret = OB_SUCCESS;
  ObTableHandle table_handle;
  const bool in_slog_trans = false;
  ObITable *table = NULL;
  is_reuse = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!pkey.is_valid() || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table key is invalid", K(ret), K(pkey), K(table_key));
  } else if (table_key.is_memtable()) {
    LOG_INFO("memtable no need check and reuse, skip it", K(table_key));
  } else if (table_key.is_trans_sstable()) {
    LOG_INFO("trans sstable can not reuse, skip it", K(table_key));
  } else if (!table_key.is_table_log_ts_comparable()) {
    // old minor sstable always need to migrate to fill log ts
    LOG_INFO("old minor sstable can not reuse, skip it", K(table_key));
  } else if (table_key.is_minor_sstable()) {
    LOG_INFO("minor sstable do not reuse", K(table_key));
  } else {
    if (OB_FAIL(ObPartitionService::get_instance().acquire_sstable(table_key, table_handle))) {
      if (OB_ENTRY_NOT_EXIST != ret && OB_PARTITION_NOT_EXIST != ret) {
        LOG_WARN("failed to get complete sstable by key", K(ret), K(table_key));
      } else {
        is_reuse = false;
        ret = OB_SUCCESS;
      }
    } else if (NULL == (table = table_handle.get_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table should not be NULL", K(ret), KP(table));
    } else if (OB_FAIL(part_ctx.add_sstable(*reinterpret_cast<ObSSTable *>(table)))) {
      LOG_WARN("failed to add sstable", K(ret));
    } else {
      is_reuse = true;
      LOG_INFO("table has already been in table mgr", K(table_key));
    }
  }
  return ret;
}

int ObMigratePrepareTask::prepare_new_partition()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageFileHandle file_handle;
  ObPartGroupMigrationTask *group_task = NULL;
  bool has_mark_creating = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (NULL != ctx_->get_partition()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot prepare new partition for exist partition", K(ret), K(*ctx_), K(ctx_->get_partition()));
  } else if (REBUILD_REPLICA_OP == ctx_->replica_op_arg_.type_) {
    ret = OB_PARTITION_NOT_EXIST;
    LOG_ERROR("cannot create new partition for rebuild", K(ret), K(*ctx_));
  } else if (ctx_->is_only_copy_sstable()) {
    ret = OB_PARTITION_NOT_EXIST;
    LOG_WARN("partition not exist, cannot copy only sstable", K(ret));
  } else if (OB_FAIL(OB_SERVER_FILE_MGR.alloc_file(ctx_->replica_op_arg_.key_.get_tenant_id(),
                 common::is_sys_table(ctx_->replica_op_arg_.key_.get_table_id()),
                 file_handle))) {
    STORAGE_LOG(WARN, "fail to alloc file", K(ret));
  } else if (OB_ISNULL(file_handle.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage file is null", K(ret), K(file_handle));
  } else if (OB_UNLIKELY(0 >= (ctx_->mig_dest_file_id_ = file_handle.get_storage_file()->get_file_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid file id", K(ret), K(ctx_->mig_dest_file_id_));
  } else if (OB_FAIL(SLOGGER.begin(OB_LOG_CS_MIGRATE_PARTITION))) {
    STORAGE_LOG(WARN, "fail to begin commit log.", K(ret));
  } else if (OB_FAIL(partition_service_->mark_pg_creating(ctx_->replica_op_arg_.key_))) {
    STORAGE_LOG(WARN, "fail to mark pg creating.", K(ret));
  } else if (FALSE_IT(has_mark_creating = true)) {
  } else if (OB_FAIL(create_new_partition(
                 ctx_->migrate_src_info_.src_addr_, ctx_->replica_op_arg_, ctx_->partition_guard_, file_handle))) {
    STORAGE_LOG(WARN, "failed to create new partition", K(ret));
  } else if (OB_ISNULL(ctx_->get_partition()) || OB_ISNULL(&(ctx_->get_partition()->get_pg_storage()))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("partition or storage must not null", K(ctx_->get_partition()), K(ret));
  } else if (FALSE_IT(group_task = reinterpret_cast<ObPartGroupMigrationTask *>(ctx_->group_task_))) {
  } else if (OB_FAIL(group_task->set_create_new_pg(ctx_->get_partition()->get_partition_key()))) {
    LOG_WARN("failed to set create new pg", K(ret));
  } else {
    ctx_->need_offline_ = true;
    ctx_->during_migrating_ = true;
    LOG_INFO("create new partition for migrate, record migrate start", K(ctx_->replica_op_arg_));
  }

  if (OB_FAIL(ret)) {
    // partition_service_->add_new_parititoon must have commit log or abort log
    if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
      STORAGE_LOG(WARN, "fail to abort log.", K(tmp_ret));
    }
  } else if (OB_FAIL(partition_service_->add_new_partition(ctx_->partition_guard_))) {
    STORAGE_LOG(WARN, "add partition to manager failed", K(ctx_->replica_op_arg_), K(ret));
  }
  if (has_mark_creating) {
    partition_service_->mark_pg_created(ctx_->replica_op_arg_.key_);
  }
  if (OB_FAIL(ret) && NULL != ctx_) {
    ctx_->can_rebuild_ = false;
    LOG_WARN("failed during prepare_new_partition, set cannot rebuild", K(ret), K(*ctx_));
  }
  return ret;
}

int ObMigrateUtil::enable_replay_with_new_partition(ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;

  LOG_INFO("enable_replay_with_new_partition");
  if (OB_ISNULL(partition = ctx.get_partition())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition should not be null", K(ret));
  } else if (OB_FAIL(partition->create_memtable())) {
    LOG_WARN("failed to create memtable", K(ret), "pkey", partition->get_partition_key());
  } else if (OB_FAIL(ObPartitionService::get_instance().online_partition(partition->get_partition_key(),
                 ctx.pg_meta_.storage_info_.get_data_info().get_publish_version(),
                 ctx.pg_meta_.restore_snapshot_version_,
                 ctx.pg_meta_.last_restore_log_id_,
                 ctx.pg_meta_.last_restore_log_ts_))) {
    STORAGE_LOG(WARN, "add partition to manager failed", K(ctx.replica_op_arg_), K(ret));
  }

  if (OB_SUCC(ret)) {
    if (ADD_REPLICA_OP == ctx.replica_op_arg_.type_ &&
        (REPLICA_RESTORE_DATA == ctx.is_restore_ || REPLICA_RESTORE_ARCHIVE_DATA == ctx.is_restore_ ||
            REPLICA_RESTORE_STANDBY == ctx.is_restore_)) {
      ctx.need_offline_ = false;
      LOG_INFO("no need set clog online for add replica during restore",
          "pkey",
          ctx.replica_op_arg_.key_,
          "restore_state",
          ctx.is_restore_);
    } else if (OB_FAIL(partition->get_log_service()->set_online(
                   ctx.pg_meta_.storage_info_.get_clog_info(), ObVersion(0, 0) /*freeze version*/))) {
      STORAGE_LOG(WARN, "failed to set clog online", K(ret), K(ctx));
    } else if (OB_FAIL(partition->report_clog_history_online())) {
      LOG_WARN("failed to report clog history", K(ret));
    } else {
      STORAGE_LOG(INFO, "succeed to enable replay for new partition", K(ctx.pg_meta_.storage_info_));
    }
  }

  if (OB_FAIL(ret)) {
    ctx.can_rebuild_ = false;
    LOG_WARN("failed during enable_replay_with_new_partition, set cannot rebuild", K(ret), K(ctx));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::deal_with_old_partition()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  clog::ObIPartitionLogService *pls = NULL;
  const bool write_slog = true;
  bool is_log_sync = false;

  LOG_INFO("start enable replay");
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_ISNULL(partition = ctx_->get_partition())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot use old partition", K(ret));
  } else if (NULL == (pls = partition->get_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected error, pls is NULL, ", K(ret), KP(pls));
  } else if (OB_FAIL(pls->is_log_sync_with_leader(is_log_sync))) {
    LOG_WARN("failed to get log is in sync", K(ret));
  } else if (REBUILD_REPLICA_OP == ctx_->replica_op_arg_.type_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot enable_replay_with_old_partition for rebuild", K(ret), K(*ctx_));
  } else {
    bool is_replica_with_data = true;
    common::ObReplicaType replica_type = ctx_->replica_op_arg_.dst_.get_replica_type();
    common::ObReplicaType local_replica_type = partition->get_replica_type();
    const ObReplicaProperty &local_replica_property = partition->get_replica_property();
    ctx_->need_offline_ = true;
    const ObPartitionSplitInfo &split_info = ctx_->pg_meta_.split_info_;
    if (split_info.is_valid() && OB_FAIL(partition->save_split_info(split_info))) {
      LOG_WARN("failed to save split info", K(ret), K(split_info), K(*ctx_));
    } else if (OB_FAIL(update_multi_version_start())) {
      LOG_WARN("failed to update multi version start", K(ret), K(*ctx_));
    } else if (local_replica_type != replica_type &&
               !ObReplicaTypeCheck::change_replica_op_allow(local_replica_type, replica_type)) {
      ret = OB_OP_NOT_ALLOW;
      STORAGE_LOG(WARN, "change replica op not allow", K(ret), K(local_replica_type), K(replica_type), K(*ctx_));
    } else if (ctx_->replica_op_arg_.is_physical_restore()) {
      if (RESTORE_REPLICA_OP != ctx_->replica_op_arg_.type_ &&
          RESTORE_FOLLOWER_REPLICA_OP != ctx_->replica_op_arg_.type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN(
            "replica op type is not restore or follower restore or standby restore", K(ret), K(ctx_->replica_op_arg_));
      } else {
        ctx_->need_offline_ = false;
        if (OB_FAIL(partition->set_replica_type(replica_type, write_slog))) {
          LOG_WARN("Failed to set replica type", K(ret), K(ctx_->replica_op_arg_));
        } else {
          ctx_->during_migrating_ = true;
          LOG_INFO("record migrate start", K(ctx_->replica_op_arg_));
        }
      }
    } else if (is_log_sync &&
               (0 != local_replica_property.get_memstore_percent() ||
                   (0 == local_replica_property.get_memstore_percent() &&
                       ObReplicaTypeCheck::is_replica_with_memstore(local_replica_type))) &&
               (OB_NORMAL_REPLICA == ctx_->replica_state_ || OB_RESTORE_REPLICA == ctx_->replica_state_) &&
               ADD_REPLICA_OP != ctx_->replica_op_arg_.type_ && MIGRATE_REPLICA_OP != ctx_->replica_op_arg_.type_ &&
               FAST_MIGRATE_REPLICA_OP != ctx_->replica_op_arg_.type_) {
      ctx_->need_offline_ = false;
      LOG_INFO("reuse memtable", K(ctx_->replica_op_arg_), K(is_log_sync));
      if (OB_FAIL(partition->set_replica_type(replica_type, write_slog))) {
        LOG_WARN("Failed to set replica type", K(ret), K(ctx_->replica_op_arg_));
      } else {
        ctx_->during_migrating_ = true;
        LOG_INFO("record migrate start", K(ctx_->replica_op_arg_));
      }
    } else if (ctx_->is_only_copy_sstable()) {
      ret = OB_LOG_NOT_SYNC;
      LOG_WARN("log is not sync, cannot copy only sstable now", K(ret), K(is_log_sync), K(*ctx_));
    } else if (OB_FAIL(ctx_->change_replica_with_data(is_replica_with_data))) {
      LOG_WARN("failed to check replica", K(ret), K(*ctx_));
    } else if (is_replica_with_data) {
      ret = OB_LOG_NOT_SYNC;
      LOG_WARN("log is not sync, cannot change replica now", K(is_log_sync), K(*ctx_));
    } else if (OB_FAIL(partition->set_replica_type(replica_type, write_slog))) {
      LOG_WARN("Failed to set replica type", K(ret), K(ctx_->replica_op_arg_));
    } else {
      ctx_->during_migrating_ = true;
      LOG_INFO("record migrate start", K(ctx_->replica_op_arg_));

      if (OB_FAIL(partition->pause())) {
        LOG_WARN("failed to pause partition", K(ret));
      }
    }
  }
  return ret;
}

int ObMigrateUtil::enable_replay_with_old_partition(ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  ObReplicaRestoreStatus restore_status = ObReplicaRestoreStatus::REPLICA_RESTORE_MAX;

  LOG_INFO("start enable replay");
  if (!ctx.is_valid()) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ctx not valid", K(ret), K(ctx));
  } else if (OB_ISNULL(partition = ctx.get_partition())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot use old partition", K(ret));
  } else if (FALSE_IT(restore_status = partition->get_pg_storage().get_restore_status())) {
  } else {
    int64_t retry_times = 0;
    int64_t restore_snapshot_version = OB_INVALID_VERSION;
    uint64_t last_restore_log_id = OB_INVALID_ID;
    int64_t last_restore_log_ts = OB_INVALID_TIMESTAMP;

    ObPartitionGroupMeta &meta = ctx.pg_meta_;
    ObBaseStorageInfo &clog_info = meta.storage_info_.get_clog_info();
    ObDataStorageInfo &data_info = meta.storage_info_.get_data_info();
    if (ObReplicaRestoreStatus::REPLICA_NOT_RESTORE == restore_status) {
      restore_snapshot_version = meta.restore_snapshot_version_;
      last_restore_log_id = meta.last_restore_log_id_;
      last_restore_log_ts = meta.last_restore_log_ts_;
    } else {
      if (OB_FAIL(partition->get_pg_storage().get_restore_replay_info(
              last_restore_log_id, last_restore_log_ts, restore_snapshot_version))) {
        LOG_WARN("failed to get restore replay info", K(ret));
      }
    }

    const int64_t local_last_replay_log_id = partition->get_log_service()->get_next_index_log_id() - 1;
    const int64_t src_last_replay_log_id =
        std::max(clog_info.get_last_replay_log_id(), data_info.get_last_replay_log_id());
    if (src_last_replay_log_id <= local_last_replay_log_id && REBUILD_REPLICA_OP == ctx.replica_op_arg_.type_ &&
        partition->get_pg_storage().is_replica_with_remote_memstore()) {
      if (OB_FAIL(partition->set_storage_info(meta.storage_info_))) {
        LOG_WARN("failed to set storage info for migration", K(ret), "pkey", partition->get_partition_key());
      }
      LOG_INFO("local last replay log id is new enough, no need offline and online pg",
          K(local_last_replay_log_id),
          K(src_last_replay_log_id));
    } else {
      while (OB_SUCC(ret) && retry_times < OB_MIGRATE_ONLINE_RETRY_COUNT) {
        if (OB_FAIL(partition->pause())) {
          LOG_WARN("failed to pause partition", K(ret), "pkey", partition->get_partition_key());
        } else if (OB_FAIL(partition->set_storage_info(meta.storage_info_))) {
          LOG_WARN("failed to set storage info", K(ret), "pkey", partition->get_partition_key());
        } else if (OB_FAIL(partition->create_memtable())) {
          LOG_WARN("failed to create memtable", K(ret), "pkey", partition->get_partition_key());
        } else if (OB_FAIL(ctx.get_partition()->get_log_service()->migrate_set_base_storage_info(clog_info))) {
          STORAGE_LOG(WARN, "reset clog start point fail", K(ret), K(meta.storage_info_));
        } else if (OB_FAIL(MIGRATOR.get_partition_service()->online_partition(ctx.replica_op_arg_.key_,
                       data_info.get_publish_version(),
                       restore_snapshot_version,
                       last_restore_log_id,
                       last_restore_log_ts))) {
          STORAGE_LOG(WARN, "online partition failed", K(ctx.replica_op_arg_), K(meta), K(ret));
        } else if (OB_PERMANENT_OFFLINE_REPLICA == ctx.replica_state_ &&
                   F_WORKING != ctx.get_partition()->get_partition_state() &&
                   OB_FAIL(ctx.get_partition()->switch_partition_state(F_WORKING))) {
          STORAGE_LOG(WARN, "partition set partition state F_WORKING failed", K(ctx.replica_op_arg_.key_), K(ret));
        }
        ++retry_times;
        if (OB_FAIL(ret) && retry_times < OB_MIGRATE_ONLINE_RETRY_COUNT) {
          ret = OB_SUCCESS;
          usleep(RETRY_TASK_SLEEP_INTERVAL_S);
        } else {
          STORAGE_LOG(INFO, "online partition finish", K(ret), K(ctx.replica_op_arg_.key_), K(meta.storage_info_));
          break;
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ctx.get_partition()->get_log_service()->restore_replayed_log(clog_info))) {
          STORAGE_LOG(WARN, "restore and replay log failed.", K(ret), K_(ctx.replica_op_arg_.key), K(meta));
        } else if (OB_FAIL(ctx.get_partition()->get_log_service()->set_online(
                       clog_info, ObVersion(0, 0) /*freeze version*/))) {
          STORAGE_LOG(WARN, "reset log temporary status failed.", K(ret), K_(ctx.replica_op_arg_.key), K(meta));
        } else if (OB_FAIL(ctx.get_partition()->report_clog_history_online())) {
          LOG_WARN("failed to report clog history online", K(ret));
        } else {
          STORAGE_LOG(INFO, "succeed to enable replay for exist partition");
        }
      }
    }
  }

  return ret;
}

int ObMigrateUtil::push_reference_tables_if_need(
    ObMigrateCtx &ctx, const ObPartitionSplitInfo &split_info, const int64_t last_replay_log_id)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  if (!ctx.is_valid()) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ctx not valid", K(ret), K(ctx));
  } else if (OB_ISNULL(partition = ctx.get_partition())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot use old partition", K(ret));
  } else {
    ObPartitionGroupMeta &meta = ctx.pg_meta_;
    if (split_info.get_src_partition() == meta.pg_key_) {
      const int64_t source_log_id = split_info.get_source_log_id();
      if (last_replay_log_id >= source_log_id && OB_INVALID_ID != source_log_id) {
        if (OB_FAIL(
                partition->push_reference_tables(split_info.get_dest_partitions(), split_info.get_split_version()))) {
          LOG_WARN("failed to push reference tables", K(split_info));
        }
      }
    }
  }

  return ret;
}

int ObMigrateUtil::merge_trans_table(ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *pg = nullptr;
  ObSSTable *trans_sstable = nullptr;
  ObPartitionKey pg_key;
  bool is_replica_with_data = false;

  if (OB_ISNULL(trans_sstable = static_cast<ObSSTable *>(ctx.trans_table_handle_.get_table()))) {
    FLOG_INFO("trans sstable is empty", K(ret), K(ctx));
    if (need_migrate_trans_table(ctx.replica_op_arg_.type_) && ctx.is_migrate_compat_version()) {
      if (OB_FAIL(ObMigrateUtil::create_empty_trans_sstable_for_compat(ctx))) {
        LOG_WARN("failed to create empty trans sstable for compat", K(ret), K(ctx));
      }
    } else if (LINK_SHARE_MAJOR_OP == ctx.replica_op_arg_.type_) {
      ctx.old_trans_table_seq_ = ctx.get_partition()->get_pg_storage().get_trans_table_seq();
    }
  } else if (OB_FAIL(wait_trans_table_merge_finish(ctx))) {
    LOG_WARN("failed to wait trans table merge finish", K(ret), K(ctx));
  } else if (OB_ISNULL(pg = ctx.get_partition())) {
    ret = OB_ERR_SYS;
    LOG_WARN("partition group must not null", K(ret));
  } else if (FALSE_IT(pg_key = pg->get_partition_key())) {
  } else if (FALSE_IT(ctx.old_trans_table_seq_ = pg->get_pg_storage().get_trans_table_seq())) {
  } else if (OB_FAIL(
                 ObMigrateUtil::add_trans_sstable_to_part_ctx(trans_sstable->get_key().pkey_, *trans_sstable, ctx))) {
    LOG_WARN("failed to add trans sstable to part migrate ctx", K(ret), KPC(trans_sstable));
  }
  // TODO() need reconsider sstable reuse, now skip trans sstable reuse
  /*
  else {
    ObTransTableMergeTask merge_task;
    ObMacroBlockWriter writer;
    ObTableHandle new_trans_sstable;
    ObSSTable* new_sstable = nullptr;
    ctx.old_trans_table_seq_ = pg->get_pg_storage().get_trans_table_seq();
    if (OB_FAIL(merge_task.init(pg_key, trans_sstable))) {
      LOG_WARN("failed to init merge task", K(ret), K(pg_key));
    } else if (OB_FAIL(merge_task.init_schema_and_writer(writer))) {
      LOG_WARN("failed to init schema and writer", K(ret), K(pg_key));
    } else if (OB_FAIL(merge_task.merge_trans_table(writer))) {
      LOG_WARN("failed to merge trans table", K(ret), K(pg_key));
    } else if (OB_FAIL(merge_task.get_merged_trans_sstable(new_trans_sstable, writer))) {
      LOG_WARN("failed to get merged trans sstable", K(ret), K(pg_key));
    } else if (OB_FAIL(new_trans_sstable.get_sstable(new_sstable))) {
      LOG_WARN("failed to get new trans sstable", K(ret), K(pg_key));
    } else if (OB_FAIL(ObMigrateUtil::add_trans_sstable_to_part_ctx(new_sstable->get_key().pkey_, *new_sstable, ctx))) {
      LOG_WARN("failed to add trans sstable to part migrate ctx", K(ret), KPC(new_sstable));
    }
  }
  */
  return ret;
}

int ObMigrateUtil::create_empty_trans_sstable_for_compat(ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  ObPartitionKey trans_table_pkey;
  const int64_t SCHEMA_VERSION = 1;
  if (OB_FAIL(trans_table_pkey.generate_trans_table_pkey(ctx.pg_meta_.pg_key_))) {
    LOG_WARN("failed to generate trans table pkey", K(ret), K(ctx));
  } else if (OB_FAIL(
                 table_schema.generate_kv_schema(trans_table_pkey.get_tenant_id(), trans_table_pkey.get_table_id()))) {
    LOG_WARN("failed to generate trans table schema", K(ret), K(trans_table_pkey));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(ctx.replica_op_arg_.dst_.get_replica_type())) {
    LOG_INFO("no need create_empty_trans_sstable_for_compat without ssstore",
        "pg_key",
        ctx.pg_meta_.pg_key_,
        "type",
        ctx.replica_op_arg_.dst_.get_replica_type());
  } else {
    ObCreateSSTableParamWithTable param;
    ObITable::TableKey table_key;
    ObSSTable *sstable = nullptr;
    ObTableHandle table_handle;

    table_key.table_type_ = ObITable::TableType::TRANS_SSTABLE;
    table_key.table_id_ = trans_table_pkey.get_table_id();
    table_key.trans_version_range_.multi_version_start_ = 0;
    table_key.trans_version_range_.base_version_ = 0;
    table_key.trans_version_range_.snapshot_version_ = ObTimeUtility::current_time();
    table_key.log_ts_range_.start_log_ts_ = 0;
    table_key.log_ts_range_.end_log_ts_ = ctx.pg_meta_.storage_info_.get_data_info().get_last_replay_log_ts();
    table_key.log_ts_range_.max_log_ts_ = table_key.log_ts_range_.end_log_ts_;
    table_key.pkey_ = trans_table_pkey;

    param.table_key_ = table_key;
    param.schema_ = &table_schema;
    param.schema_version_ = SCHEMA_VERSION;
    param.logical_data_version_ = table_key.version_ + 1;
    ObPGCreateSSTableParam pg_create_sstable_param;
    pg_create_sstable_param.with_table_param_ = &param;
    if (OB_FAIL(ctx.get_partition()->create_sstable(pg_create_sstable_param, table_handle))) {
      LOG_WARN("fail to create sstable", K(ret));
    } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
      LOG_WARN("failed to get sstable", K(ret), K(table_handle));
    } else if (OB_ISNULL(sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable should not be null", K(ret));
    } else if (OB_FAIL(ObMigrateUtil::add_trans_sstable_to_part_ctx(trans_table_pkey, *sstable, ctx))) {
      LOG_WARN("failed to add trans sstable to part ctx", K(ret), K(trans_table_pkey));
    }
  }

  if (OB_SUCC(ret)) {
    ctx.old_trans_table_seq_ = ctx.get_partition()->get_pg_storage().get_trans_table_seq();
  }
  return ret;
}

int ObMigrateUtil::add_trans_sstable_to_part_ctx(
    const ObPartitionKey &trans_table_pkey, ObSSTable &sstable, ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.part_ctx_array_.count(); ++i) {
    ObPartitionMigrateCtx &part_ctx = ctx.part_ctx_array_.at(i);
    if (part_ctx.copy_info_.meta_.pkey_.is_trans_table()) {
      if (OB_FAIL(part_ctx.add_sstable(sstable))) {
        LOG_WARN("failed to add new trans sstable to partition migrate ctx", K(ret), K(trans_table_pkey));
      } else {
        FLOG_INFO("succeed to add trans sstable to partition migrate ctx", K(ret), K(sstable));
      }
      found = true;
      break;
    }
  }
  if (OB_SUCC(ret) && !found) {
    ObPartitionMigrateCtx part_ctx;
    part_ctx.copy_info_.meta_.pkey_ = trans_table_pkey;
    part_ctx.copy_info_.meta_.multi_version_start_ = 1;
    part_ctx.copy_info_.meta_.create_timestamp_ = ObTimeUtility::current_time();
    part_ctx.is_partition_exist_ = false;
    part_ctx.need_reuse_local_minor_ = false;
    part_ctx.ctx_ = &ctx;
    if (OB_FAIL(part_ctx.add_sstable(sstable))) {
      LOG_WARN("failed to add sstable to partition ctx", K(ret), K(sstable));
    } else if (OB_FAIL(ctx.part_ctx_array_.push_back(part_ctx))) {
      LOG_WARN("failed to push back migrate partition ctx", K(ret), K(trans_table_pkey));
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::deal_with_rebuild_partition()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  common::ObAddr leader;
  ctx_->need_offline_ = true;
  ObRole role;
  LOG_INFO("start deal_with_rebuild_partition");

  if (NULL != ctx_) {
    FLOG_INFO("rebuild is start", "pkey", ctx_->replica_op_arg_.key_);
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (REBUILD_REPLICA_OP != ctx_->replica_op_arg_.type_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("replica op is  not rebuild op", K(ret), "arg", ctx_->replica_op_arg_);
  } else if (OB_ISNULL(partition = ctx_->get_partition())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot use old partition", K(ret));
  } else if (OB_FAIL(partition->get_leader(leader))) {
    LOG_WARN("failed to get leader", K(ret), "arg", ctx_->replica_op_arg_);
  } else if (OB_FAIL(partition->get_role(role))) {
    LOG_WARN("failed to get real role", K(ret), "arg", ctx_->replica_op_arg_);
  } else if (leader.is_valid() &&
             leader == MYADDR
             // support rebuild in leader reconfirm
             &&
             partition->get_log_service()->is_leader_active()) {  // TODO(wait for yanmu) //&& is_strong_leader(role)) {
    if (OB_FAIL(MIGRATOR.get_partition_service()->turn_off_rebuild_flag(ctx_->replica_op_arg_))) {
      LOG_WARN("Failed to report_rebuild_replica off", K(ret), "arg", ctx_->replica_op_arg_);
    } else {
      ret = OB_NO_NEED_REBUILD;
      LOG_WARN("leader can not as rebuild dst", K(ret), K(leader), "myaddr", MYADDR, "arg", ctx_->replica_op_arg_);
    }
  } else {
    ObBaseStorageInfo &remote_clog_info = ctx_->pg_meta_.storage_info_.get_clog_info();
    const ObPartitionSplitInfo &split_info = ctx_->pg_meta_.split_info_;
    const int64_t local_max_confirm_log_id = partition->get_log_service()->get_next_index_log_id() - 1;
    const int64_t src_last_replay_log_id = remote_clog_info.get_last_replay_log_id();
    const bool is_replica_with_remote_memstore = partition->get_pg_storage().is_replica_with_remote_memstore();
    bool skip_log_id_check = false;

#ifdef ERRSIM
    skip_log_id_check = ObServerConfig::get_instance()._ob_enable_rebuild_on_purpose;
#endif

    if (OB_FAIL(ret)) {
    } else if (src_last_replay_log_id <= local_max_confirm_log_id && !is_replica_with_remote_memstore &&
               !skip_log_id_check) {
      ret = OB_NO_NEED_REBUILD;
      LOG_WARN("local log id is new enough , no need to rebuild, will turn off rebuild flag",
          K(ret),
          K(src_last_replay_log_id),
          K(local_max_confirm_log_id),
          K(remote_clog_info));
      if (OB_SUCCESS != (tmp_ret = partition_service_->turn_off_rebuild_flag(ctx_->replica_op_arg_))) {
        LOG_WARN("Failed to report_rebuild_replica off", K(tmp_ret), "arg", ctx_->replica_op_arg_);
      }
    } else if (OB_FAIL(update_multi_version_start())) {
      LOG_WARN("failed to update multi version start", K(ret), K(*ctx_));
    } else if (split_info.is_valid() && OB_FAIL(partition->save_split_info(split_info))) {
      LOG_WARN("failed to save split info", K(ret), K(split_info), K(*ctx_));
    } else {
      ctx_->during_migrating_ = true;
      if (!is_replica_with_remote_memstore) {
        ctx_->need_online_for_rebuild_ = true;
        if (OB_FAIL(partition->pause())) {
          LOG_WARN("failed to pause partition", K(ret), "pkey", partition->get_partition_key());
        } else if (is_standby_leader(role)) {
          uint64_t unused_log_id = OB_INVALID_ID;
          ObBaseStorageInfo clog_info;
          if (OB_FAIL(partition->get_log_service()->get_base_storage_info(clog_info, unused_log_id))) {
            LOG_WARN("failed to get base storage info", K(ret));
          } else if (OB_FAIL(remote_clog_info.standby_force_update_member_list(clog_info.get_membership_log_id(),
                         clog_info.get_membership_timestamp(),
                         clog_info.get_replica_num(),
                         clog_info.get_curr_member_list(),
                         clog_info.get_ms_proposal_id()))) {
            LOG_WARN("failed to update restore standby member list", K(ret), K(clog_info), K(remote_clog_info));
          }
        }

        if (OB_SUCC(ret)) {
          LOG_INFO("succeed to offline_rebuild_partition",
              K(leader),
              "arg",
              ctx_->replica_op_arg_,
              K(src_last_replay_log_id),
              K(local_max_confirm_log_id));
        }
      } else {
        LOG_INFO("succeed to deal_with_replica using remote memestore replica",
            K(leader),
            "arg",
            ctx_->replica_op_arg_,
            K(src_last_replay_log_id),
            K(local_max_confirm_log_id));
      }
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::deal_with_standby_restore_partition()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;

  LOG_INFO("start deal_with_standby restore_partition");

  if (NULL != ctx_) {
    FLOG_INFO("standby restore is start", "pkey", ctx_->replica_op_arg_.key_);
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (RESTORE_STANDBY_OP != ctx_->replica_op_arg_.type_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("replica op is  not standby restore op", K(ret), "arg", ctx_->replica_op_arg_);
  } else if (OB_ISNULL(partition = ctx_->get_partition())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot use old partition", K(ret));
  } else {
    ObBaseStorageInfo &remote_clog_info = ctx_->pg_meta_.storage_info_.get_clog_info();
    uint64_t unused_log_id = OB_INVALID_ID;
    ObBaseStorageInfo clog_info;
    if (OB_FAIL(partition->get_log_service()->get_base_storage_info(clog_info, unused_log_id))) {
      LOG_WARN("failed to get base storage info", K(ret));
    } else if (OB_FAIL(update_multi_version_start())) {
      LOG_WARN("failed to update multi version start", K(ret), K(*ctx_));
    } else if (OB_FAIL(partition->get_log_service()->set_offline())) {
      LOG_WARN("failed to offline log service", K(ret));
    } else if (OB_FAIL(remote_clog_info.standby_force_update_member_list(clog_info.get_membership_log_id(),
                   clog_info.get_membership_timestamp(),
                   clog_info.get_replica_num(),
                   clog_info.get_curr_member_list(),
                   clog_info.get_ms_proposal_id()))) {
      LOG_WARN("failed to update restore standby member list", K(ret), K(clog_info), K(remote_clog_info));
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_pg_validate_tasks(ObIArray<ObITask *> &last_task_array)
{
  int ret = OB_SUCCESS;
  ObITask *last_task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (VALIDATE_BACKUP_OP != ctx_->replica_op_arg_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected op type, ", K(ret), K(ctx_->replica_op_arg_.type_));
  } else if (OB_FAIL(generate_validate_tasks(last_task))) {
    LOG_WARN("failed to generate validate tasks", K(ret));
  } else if (OB_FAIL(last_task_array.push_back(last_task))) {
    LOG_WARN("failed to push last task into last task array", K(ret));
    ;
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_validate_tasks(ObITask *&last_task)
{
  int ret = OB_SUCCESS;
  ObFakeTask *wait_validate_finish_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(generate_wait_migrate_finish_task(wait_validate_finish_task))) {
    LOG_WARN("failed to generate_wait_migrate_finish_task", K(ret));
  } else if (OB_ISNULL(wait_validate_finish_task)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("wait_validate_finsh_task must not be NULL", K(ret));
  } else if (OB_FAIL(generate_validate_tasks(*wait_validate_finish_task))) {
    LOG_WARN("failed to generate_validate_tasks", K(ret));
  }

  if (OB_SUCC(ret)) {
    last_task = wait_validate_finish_task;
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_validate_tasks(ObFakeTask &wait_validate_finish_task)
{
  int ret = OB_SUCCESS;
  ObITask *parent_task = this;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(generate_validate_backup_tasks(parent_task))) {
    LOG_WARN("failed to generate validate backup tasks", K(ret));
  } else if (OB_FAIL(parent_task->add_child(wait_validate_finish_task))) {
    LOG_WARN("failed to add wait_validate_finish_task", K(ret));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_validate_backup_tasks(share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;
  ObFakeTask *wait_finish_task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(parent_task));
  } else if (OB_FAIL(dag_->alloc_task(wait_finish_task))) {
    LOG_WARN("failed to alloc wait finish task", K(ret));
  } else if (OB_FAIL(generate_validate_backup_task(parent_task, wait_finish_task))) {
    LOG_WARN("failed to generate validate backup task", K(ret));
  } else if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
    LOG_WARN("failed to add wait finish task", K(ret));
  } else {
    parent_task = wait_finish_task;
    LOG_DEBUG("succeed to generate_validate_backup_tasks");
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_validate_backup_task(share::ObITask *parent_task, share::ObITask *child_task)
{
  int ret = OB_SUCCESS;
  const int64_t task_idx = 0;
  const int64_t clog_file_id = 1;
  ObValidatePrepareTask *prepare_task = NULL;
  ObValidateClogDataTask *clog_task = NULL;
  ObValidateBaseDataTask *base_task = NULL;
  ObValidateFinishTask *finish_task = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(parent_task) || OB_ISNULL(child_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(parent_task), KP(child_task));
  } else if (OB_FAIL(dag_->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc finish task", K(ret));
  } else if (OB_FAIL(build_validate_backup_ctx(finish_task->get_validate_ctx()))) {
    LOG_WARN("failed to build validate backup ctx", K(ret));
  } else if (OB_FAIL(finish_task->init(*ctx_))) {
    LOG_WARN("failed to init finish task", K(ret));
  } else if (OB_FAIL(finish_task->add_child(*child_task))) {
    LOG_WARN("failed to add child", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (finish_task->get_validate_ctx().get_sub_task_count() > 0) {
      if (OB_FAIL(dag_->alloc_task(prepare_task))) {
        LOG_WARN("failed to alloc prepare task", K(ret));
      } else if (OB_FAIL(dag_->alloc_task(clog_task))) {
        LOG_WARN("failed to alloc clog task", K(ret));
      } else if (OB_FAIL(dag_->alloc_task(base_task))) {
        LOG_WARN("failed to alloc base task", K(ret));
      } else if (OB_FAIL(prepare_task->init(*ctx_, finish_task->get_validate_ctx()))) {
        LOG_WARN("failed to init prepare task", K(ret));
      } else if (OB_FAIL(clog_task->init(clog_file_id, *ctx_, finish_task->get_validate_ctx()))) {
        LOG_WARN("failed to init clog task", K(ret));
      } else if (OB_FAIL(base_task->init(task_idx, *ctx_, finish_task->get_validate_ctx()))) {
        LOG_WARN("failed to init base task", K(ret));
      } else if (OB_FAIL(parent_task->add_child(*prepare_task))) {
        LOG_WARN("failed to add child prepare task", K(ret));
      } else if (OB_FAIL(prepare_task->add_child(*clog_task))) {
        LOG_WARN("failed to add child clog task", K(ret));
      } else if (OB_FAIL(clog_task->add_child(*base_task))) {
        LOG_WARN("failed to add child base task", K(ret));
      } else if (OB_FAIL(base_task->add_child(*finish_task))) {
        LOG_WARN("failed to add child finish task", K(ret));
      } else if (OB_FAIL(dag_->add_task(*prepare_task))) {
        LOG_WARN("failed to add prepare task to dag", K(ret));
      } else if (OB_FAIL(dag_->add_task(*clog_task))) {
        LOG_WARN("failed to add clog task to dag", K(ret));
      } else if (OB_FAIL(dag_->add_task(*base_task))) {
        LOG_WARN("failed to add base task to dag", K(ret));
      }
    } else {
      if (OB_FAIL(parent_task->add_child(*finish_task))) {
        LOG_WARN("failed to add child finish_task for parent");
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(dag_->add_task(*finish_task))) {
      LOG_WARN("failed tp add finish task to dag", K(ret));
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::build_validate_backup_ctx(ObValidateBackupPGCtx &pg_ctx)
{
  int ret = OB_SUCCESS;
  pg_ctx.reset();
  ObBackupBaseDataPathInfo path_info;
  ObArray<ObBackupMacroIndex> macro_index_list;
  const ObPartitionKey &pg_key = ctx_->replica_op_arg_.validate_arg_.pg_key_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(ctx_->replica_op_arg_.validate_arg_.get_backup_base_data_info(path_info))) {
    STORAGE_LOG(WARN, "failed to get backup base data info", K(ret));
  } else if (!ctx_->macro_index_store_.is_inited() && OB_FAIL(ctx_->macro_index_store_.init(path_info))) {
    STORAGE_LOG(WARN, "failed to init macro index store", K(ret), K(path_info));
  } else if (OB_FAIL(pg_ctx.init(*ctx_, *bandwidth_throttle_))) {
    LOG_WARN("ctx init failed", K(ret));
  } else if (OB_FAIL(fetch_pg_macro_index_list(pg_key, pg_ctx, macro_index_list))) {
    LOG_WARN("failed to fetch pg macro block list", K(ret));
  } else if (OB_FAIL(build_validate_sub_task(macro_index_list, pg_ctx))) {
    LOG_WARN("failed to build validate sub task", K(ret));
  } else {
    LOG_INFO("succeed to build validate backup pg ctx", K(pg_ctx));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::fetch_pg_macro_index_list(
    const ObPartitionKey &pg_key, ObValidateBackupPGCtx &pg_ctx, ObIArray<ObBackupMacroIndex> &macro_index_list)
{
  int ret = OB_SUCCESS;
  share::ObBackupMetaIndex meta_index;
  ObArray<ObBackupMacroIndex> *index_list = NULL;
  ObBackupMetaIndexStore *meta_index_store = NULL;
  ObBackupMacroIndexStore &macro_index_store = ctx_->macro_index_store_;
  ObPartGroupMigrationTask *group_task = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (FALSE_IT(group_task = reinterpret_cast<ObPartGroupMigrationTask *>(ctx_->group_task_))) {
  } else if (FALSE_IT(meta_index_store = &group_task->get_meta_index_store())) {
  } else if (OB_ISNULL(meta_index_store)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta index store should not be NULL", K(ret), KP(meta_index_store));
  } else if (OB_FAIL(meta_index_store->get_meta_index(pg_key, ObBackupMetaType::PARTITION_GROUP_META, meta_index))) {
    if (OB_HASH_NOT_EXIST == ret) {
      pg_ctx.only_in_clog_ = true;
      ret = OB_SUCCESS;
      LOG_INFO("pkey only exists in clog", K(ret), K(pg_key));
    }
  } else if (OB_UNLIKELY(!macro_index_store.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro index store do not init", K(ret));
  } else if (OB_FAIL(macro_index_store.get_macro_block_index(pg_key, index_list))) {
    LOG_WARN("failed to get macro block index", K(ret), K(pg_key));
  } else if (OB_ISNULL(index_list)) {
    LOG_INFO("the pg is empty, skip", K(ret), K(pg_key));
  } else if (OB_FAIL(macro_index_list.assign(*index_list))) {
    LOG_WARN("failed to assign to macro_index_list", K(ret));
  } else if (FALSE_IT(pg_ctx.total_macro_block_count_ = macro_index_list.count())) {
    // do nothing
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::build_validate_sub_task(
    const ObIArray<ObBackupMacroIndex> &macro_index_list, ObValidateBackupPGCtx &ctx)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  int64_t max_macro_block_count_per_task = 1024 /*MAX_MACRO_BLOCK_COUNT_PER_TASK*/;
  LOG_INFO("start to build validate sub task");
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (macro_index_list.empty()) {
    ctx.sub_task_cnt_ = 0;
    LOG_INFO("macro index list is empty, skip validating", K(ret));
  } else {
    ctx.sub_task_cnt_ = macro_index_list.count() / max_macro_block_count_per_task;
    if (macro_index_list.count() % max_macro_block_count_per_task > 0) {
      ++ctx.sub_task_cnt_;
    }
    if (OB_ISNULL(buf = ctx.allocator_.alloc(sizeof(ObValidateBackupPGCtx::SubTask) * ctx.sub_task_cnt_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate memory", K(ret));
    } else {
      ctx.sub_tasks_ = new (buf) ObValidateBackupPGCtx::SubTask[ctx.sub_task_cnt_];
      buf = NULL;
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.sub_task_cnt_; ++i) {
      ObValidateBackupPGCtx::SubTask &sub_task = ctx.sub_tasks_[i];
      sub_task.macro_block_count_ =
          std::min(max_macro_block_count_per_task, macro_index_list.count() - i * max_macro_block_count_per_task);
      sub_task.pkey_ = ctx.pg_key_;
      if (OB_ISNULL(buf = ctx.allocator_.alloc(sizeof(ObBackupMacroIndex) * sub_task.macro_block_count_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret));
      } else {
        sub_task.macro_block_infos_ = new (buf) ObBackupMacroIndex[sub_task.macro_block_count_];
        buf = NULL;
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < sub_task.macro_block_count_; ++j) {
        const int64_t macro_idx = i * max_macro_block_count_per_task + j;
        if (macro_idx >= macro_index_list.count()) {
          break;
        }
        const ObBackupMacroIndex &macro_index = macro_index_list.at(macro_idx);
        ObBackupMacroIndex &index = ctx.sub_tasks_[i].macro_block_infos_[j];
        index = macro_index;
      }
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_pg_backup_backupset_tasks(common::ObIArray<share::ObITask *> &last_task_array)
{
  int ret = OB_SUCCESS;
  ObITask *last_task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", KR(ret));
  } else if (BACKUP_BACKUPSET_OP != ctx_->replica_op_arg_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexcepted op type, ", KR(ret), K(ctx_->replica_op_arg_));
  } else if (OB_FAIL(generate_backup_backupset_tasks(last_task))) {
    LOG_WARN("fail to generate backup backupset tasks", KR(ret));
  } else if (OB_FAIL(last_task_array.push_back(last_task))) {
    LOG_WARN("fail to push last task into last task array", KR(ret));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_backup_backupset_tasks(share::ObITask *&last_task)
{
  int ret = OB_SUCCESS;
  ObFakeTask *wait_migrate_finish_task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", KR(ret));
  } else if (OB_FAIL(generate_wait_migrate_finish_task(wait_migrate_finish_task))) {
    LOG_WARN("failed to generate_wait_migrate_finish_task", KR(ret));
  } else if (OB_ISNULL(wait_migrate_finish_task)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("wait_migrate_finish_task must not null", KR(ret));
  } else if (OB_FAIL(generate_backup_backupset_tasks(*wait_migrate_finish_task))) {
    LOG_WARN("failed to generate_backup_backupset_tasks", K(ret));
  }

  if (OB_SUCC(ret)) {
    last_task = wait_migrate_finish_task;
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_backup_backupset_tasks(share::ObFakeTask &wait_finish_task)
{
  int ret = OB_SUCCESS;
  ObITask *parent_task = this;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", KR(ret));
  } else if (OB_FAIL(generate_backup_backupset_pg_tasks(parent_task))) {
    LOG_WARN("failed to generate backup major tasks", K(ret));
  } else if (OB_FAIL(parent_task->add_child(wait_finish_task))) {
    LOG_WARN("failed to add wait_migrate_finish_task", K(ret));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_backup_backupset_pg_tasks(share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;
  ObFakeTask *wait_finish_task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", KR(ret));
  } else if (OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(parent_task));
  } else if (OB_FAIL(dag_->alloc_task(wait_finish_task))) {
    LOG_WARN("failed to alloc wait finish task", K(ret));
  } else if (OB_FAIL(generate_backup_backupset_pg_tasks(parent_task, wait_finish_task))) {
    LOG_WARN("failed to generate backup backupset pg task", KR(ret));
  } else if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
    LOG_WARN("failed to add wait finish task", K(ret));
  } else {
    parent_task = wait_finish_task;
    LOG_INFO("succeed to generate_backup_backupset_pg_tasks");
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_backup_backupset_pg_tasks(
    share::ObITask *&parent_task, share::ObITask *child_task)
{
  int ret = OB_SUCCESS;
  int64_t cur_idx = 0;
  ObBackupBackupsetFileTask *copy_task = NULL;
  ObBackupBackupsetFinishTask *finish_task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_ISNULL(parent_task) || OB_ISNULL(child_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(parent_task), KP(child_task));
  } else if (OB_FAIL(dag_->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc finish task", KR(ret));
  } else if (OB_FAIL(build_backup_backupset_ctx_v2(finish_task->get_backup_backupset_file_ctx()))) {
    LOG_WARN("failed to build backup backupset ctx", KR(ret));
  } else if (OB_FAIL(finish_task->init(*ctx_))) {
    LOG_WARN("failed to init finish task", KR(ret));
  } else if (OB_FAIL(finish_task->add_child(*child_task))) {
    LOG_WARN("failed to add child", KR(ret));
  }

  if (OB_SUCC(ret)) {
    // TODO() FIX total file count
    const int64_t total_file_count = 0;
    if (total_file_count > 0) {
      if (OB_FAIL(dag_->alloc_task(copy_task))) {
        LOG_WARN("failed to alloc copy task", KR(ret));
      } else if (OB_FAIL(copy_task->init(cur_idx, *ctx_, finish_task->get_backup_backupset_file_ctx()))) {
        LOG_WARN("failed to init copy task", KR(ret));
      } else if (OB_FAIL(parent_task->add_child(*copy_task))) {
        LOG_WARN("failed to add child copy task", KR(ret));
      } else if (OB_FAIL(copy_task->add_child(*finish_task))) {
        LOG_WARN("failed to add child finish task", KR(ret));
      } else if (OB_FAIL(dag_->add_task(*copy_task))) {
        LOG_WARN("failed to add copy task to dag", KR(ret));
      }
    } else {
      if (OB_FAIL(parent_task->add_child(*finish_task))) {
        LOG_WARN("failed to add child finish_task for parent", KR(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dag_->add_task(*finish_task))) {
      LOG_WARN("failed to add finish taksk to dag", KR(ret));
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::build_backup_backupset_ctx_v2(ObBackupBackupsetPGFileCtx &pg_ctx)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util(true /*need retry*/);
  const share::ObBackupBackupsetArg &bb_arg = ctx_->replica_op_arg_.backup_backupset_arg_;
  const common::ObPGKey &pg_key = bb_arg.pg_key_;
  const uint64_t table_id = pg_key.get_table_id();
  const int64_t partition_id = pg_key.get_partition_id();
  const int64_t compatible = bb_arg.compatible_;
  ObBackupBaseDataPathInfo path_info;
  ObBackupPath major_pg_path, minor_pg_path;
  int64_t minor_task_id = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (compatible <= OB_BACKUP_COMPATIBLE_VERSION_V1 || compatible >= OB_BACKUP_COMPATIBLE_VERSION_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compatible version is not valid", KR(ret), K(compatible));
  } else if (OB_FAIL(pg_ctx.open(bb_arg, pg_key))) {
    LOG_WARN("failed to open pg ctx", KR(ret), K(bb_arg), K(pg_key));
  } else if (OB_FAIL(bb_arg.get_src_backup_base_data_info(path_info))) {
    LOG_WARN("failed to get src backup base data info", KR(ret), K(bb_arg));
  } else if (OB_FAIL(
                 ObBackupPathUtil::get_tenant_pg_major_data_path(path_info, table_id, partition_id, major_pg_path))) {
    LOG_WARN("failed to get tenant pg data path", KR(ret), K(path_info), K(pg_key));
    // TODO()Backup backup fix it
    //} else if (OB_FAIL(util.list_files(major_pg_path.get_obstr(),
    //               path_info.dest_.get_storage_info(),
    //               pg_ctx.allocator_,
    //               pg_ctx.major_files_))) {
    //  LOG_WARN("failed to list files", KR(ret), K(major_pg_path), K(path_info));
    //  }
  } else {
    if (compatible >= ObBackupCompatibleVersion::OB_BACKUP_COMPATIBLE_VERSION_V3) {
      if (OB_FAIL(get_backup_backup_minor_task_id(path_info, pg_key, minor_task_id))) {
        LOG_WARN("failed to get backup backup minor task id", KR(ret), K(path_info), K(pg_key));
      } else if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_minor_data_path(
                     path_info, table_id, partition_id, minor_task_id, minor_pg_path))) {
        LOG_WARN("failed to get tenant pg minor data path", KR(ret), K(path_info), K(pg_key));
        // TODO()Backup backup Fix it
        //} else if (OB_FAIL(util.list_files(minor_pg_path.get_obstr(),
        //               path_info.dest_.get_storage_info(),
        //               pg_ctx.allocator_,
        //               pg_ctx.minor_files_))) {
        //  LOG_WARN("failed to list files", KR(ret), K(minor_pg_path), K(path_info));
        //} else {
        //  pg_ctx.minor_task_id_ = minor_task_id;
        //  }
      }
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::get_backup_backup_minor_task_id(
    const share::ObBackupBaseDataPathInfo &path_info, const common::ObPGKey &pg_key, int64_t &task_id)
{
  int ret = OB_SUCCESS;
  task_id = 0;
  ObArenaAllocator allocator;
  ObStorageUtil util(true /*need retry*/);
  const uint64_t table_id = pg_key.get_table_id();
  const int64_t partition_id = pg_key.get_partition_id();
  const share::ObBackupBackupsetArg &bb_arg = ctx_->replica_op_arg_.backup_backupset_arg_;
  ObBackupPath pg_path;
  ObArray<ObString> dir_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(pg_key));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_pg_minor_dir_path(path_info, table_id, partition_id, pg_path))) {
    LOG_WARN("failed to get tenant pg minor data path", KR(ret), K(path_info), K(pg_key));
  } else if (OB_FAIL(
                 util.list_directories(pg_path.get_obstr(), path_info.dest_.get_storage_info(), allocator, dir_list))) {
    LOG_WARN("failed to list files", KR(ret), K(pg_path), K(path_info));
  } else if (dir_list.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("minor directory is empty", KR(ret), K(path_info), K(pg_key));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dir_list.count(); ++i) {
      int64_t tmp_task_id = 0;
      const ObString &dir = dir_list.at(i);
      const char *str = dir.ptr();
      for (int64_t j = 0; OB_SUCC(ret) && j < dir.length(); ++j) {
        const char end_flag = '/';
        if (end_flag == str[j]) {
          break;
        } else if (!isdigit(str[j])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not digit number", KR(ret), K(j), K(str[j]));
        } else {
          if (tmp_task_id > INT64_MAX / 10 || (tmp_task_id == INT64_MAX / 10 && (str[j] - '0') > INT64_MAX % 10)) {
            ret = OB_DECIMAL_OVERFLOW_WARN;
            LOG_WARN("task id is not valid", KR(ret), K(tmp_task_id));
          } else {
            tmp_task_id = tmp_task_id * 10 + (str[j] - '0');
          }
        }
      }
      if (OB_SUCC(ret) && task_id < tmp_task_id) {
        task_id = tmp_task_id;
      }
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_pg_backup_archive_log_tasks(
    common::ObIArray<share::ObITask *> &last_task_array)
{
  int ret = OB_SUCCESS;
  ObITask *last_task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", KR(ret));
  } else if (BACKUP_ARCHIVELOG_OP != ctx_->replica_op_arg_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexcepted op type, ", KR(ret), K(ctx_->replica_op_arg_));
  } else if (OB_FAIL(generate_backup_archive_log_tasks(last_task))) {
    LOG_WARN("fail to generate backup backupset tasks", KR(ret));
  } else if (OB_FAIL(last_task_array.push_back(last_task))) {
    LOG_WARN("fail to push last task into last task array", KR(ret));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_backup_archive_log_tasks(share::ObITask *&last_task)
{
  int ret = OB_SUCCESS;
  ObFakeTask *wait_migrate_finish_task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", KR(ret));
  } else if (OB_FAIL(generate_wait_migrate_finish_task(wait_migrate_finish_task))) {
    LOG_WARN("failed to generate_wait_migrate_finish_task", KR(ret));
  } else if (OB_ISNULL(wait_migrate_finish_task)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("wait_migrate_finish_task must not null", KR(ret));
  } else if (OB_FAIL(generate_backup_archive_log_tasks(*wait_migrate_finish_task))) {
    LOG_WARN("failed to generate_backup_backupset_tasks", K(ret));
  }

  if (OB_SUCC(ret)) {
    last_task = wait_migrate_finish_task;
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_backup_archive_log_tasks(share::ObFakeTask &wait_finish_task)
{
  int ret = OB_SUCCESS;
  ObITask *parent_task = this;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", KR(ret));
  } else if (OB_FAIL(generate_backup_archive_log_pg_tasks(parent_task))) {
    LOG_WARN("failed to generate backup major tasks", K(ret));
  } else if (OB_FAIL(parent_task->add_child(wait_finish_task))) {
    LOG_WARN("failed to add wait_migrate_finish_task", K(ret));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_backup_archive_log_pg_tasks(share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;
  ObFakeTask *wait_finish_task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", KR(ret));
  } else if (OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(parent_task));
  } else if (OB_FAIL(dag_->alloc_task(wait_finish_task))) {
    LOG_WARN("failed to alloc wait finish task", K(ret));
  } else if (OB_FAIL(generate_backup_archive_log_pg_tasks(parent_task, wait_finish_task))) {
    LOG_WARN("failed to generate backup backupset pg task", KR(ret));
  } else if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
    LOG_WARN("failed to add wait finish task", K(ret));
  } else {
    parent_task = wait_finish_task;
    LOG_INFO("succeed to generate_backup_backupset_pg_tasks");
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_backup_archive_log_pg_tasks(
    share::ObITask *&parent_task, share::ObITask *child_task)
{
  int ret = OB_SUCCESS;
  ObBackupArchiveLogPGTask *copy_task = NULL;
  ObBackupArchiveLogFinishTask *finish_task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_ISNULL(parent_task) || OB_ISNULL(child_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(parent_task), KP(child_task));
  } else if (OB_FAIL(dag_->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc finish task", KR(ret));
  } else if (OB_FAIL(build_backup_archive_log_ctx(finish_task->get_backup_archivelog_ctx()))) {
    LOG_WARN("failed to build backup archive log ctx", KR(ret));
  } else if (OB_FAIL(finish_task->init(*ctx_))) {
    LOG_WARN("failed to init finish task", KR(ret));
  } else if (OB_FAIL(finish_task->add_child(*child_task))) {
    LOG_WARN("failed to add child", KR(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dag_->alloc_task(copy_task))) {
      LOG_WARN("failed to alloc copy task", KR(ret));
    } else if (OB_FAIL(copy_task->init(*ctx_, finish_task->get_backup_archivelog_ctx()))) {
      LOG_WARN("failed to init copy task", KR(ret));
    } else if (OB_FAIL(parent_task->add_child(*copy_task))) {
      LOG_WARN("failed to add child copy task", KR(ret));
    } else if (OB_FAIL(copy_task->add_child(*finish_task))) {
      LOG_WARN("failed to add child finish task", KR(ret));
    } else if (OB_FAIL(dag_->add_task(*copy_task))) {
      LOG_WARN("failed to add copy task to dag", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dag_->add_task(*finish_task))) {
      LOG_WARN("failed to add finish taksk to dag", KR(ret));
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::build_backup_archive_log_ctx(ObBackupArchiveLogPGCtx &ctx)
{
  int ret = OB_SUCCESS;
  const common::ObPGKey &pg_key = ctx_->replica_op_arg_.backup_archive_log_arg_.pg_key_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ctx.open(*ctx_, pg_key, *bandwidth_throttle_))) {
    LOG_WARN("failed to init ctx", KR(ret), K(pg_key));
  } else {
    LOG_INFO("build backup archive log ctx success", KR(ret));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_pg_backup_tasks(ObIArray<ObITask *> &last_task_array)
{
  int ret = OB_SUCCESS;
  ObITask *last_task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (BACKUP_REPLICA_OP != ctx_->replica_op_arg_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexcepted op type, ", K(ret), K(ctx_->replica_op_arg_));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(ctx_->replica_op_arg_.src_.get_replica_type())) {
    ret = OB_REPLICA_CANNOT_BACKUP;
    LOG_WARN("replica has no sstore.", K(ret), K(ctx_->replica_op_arg_));
  } else if (ctx_->partition_guard_.get_partition_group()->is_removed()) {
    ret = OB_PG_IS_REMOVED;
    LOG_WARN("partition is already removed, can not backup", K(ret), K(ctx_->replica_op_arg_.key_));
  } else if (OB_FAIL(generate_backup_tasks(last_task))) {
    LOG_WARN("fail to generate backup tasks", K(ret));
  } else if (OB_FAIL(last_task_array.push_back(last_task))) {
    LOG_WARN("fail to push last task into last task array", K(ret));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_backup_tasks(ObITask *&last_task)
{
  int ret = OB_SUCCESS;
  ObFakeTask *wait_migrate_finish_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(generate_wait_migrate_finish_task(wait_migrate_finish_task))) {
    LOG_WARN("failed to generate_wait_migrate_finish_task", K(ret));
  } else if (OB_ISNULL(wait_migrate_finish_task)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("wait_rebuild_finish_task must not null", K(ret));
  } else if (OB_FAIL(generate_backup_tasks(*wait_migrate_finish_task))) {
    LOG_WARN("failed to generate_backup_tasks", K(ret));
  }

  if (OB_SUCC(ret)) {
    last_task = wait_migrate_finish_task;
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_backup_tasks(ObFakeTask &wait_migrate_finish_task)
{
  int ret = OB_SUCCESS;
  ObITask *parent_task = this;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(generate_backup_major_tasks(parent_task))) {
    LOG_WARN("failed to generate backup major tasks", K(ret));
  } else if (OB_FAIL(parent_task->add_child(wait_migrate_finish_task))) {
    LOG_WARN("failed to add wait_migrate_finish_task", K(ret));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_backup_major_tasks(share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;
  ObFakeTask *wait_finish_task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(parent_task));
  } else if (OB_FAIL(dag_->alloc_task(wait_finish_task))) {
    LOG_WARN("failed to alloc wait finish task", K(ret));
  } else if (OB_FAIL(generate_backup_major_copy_task(parent_task, wait_finish_task))) {
    LOG_WARN("failed to generate major sstable copy task", K(ret));
  } else if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
    LOG_WARN("failed to add wait finish task", K(ret));
  } else {
    parent_task = wait_finish_task;
    LOG_DEBUG("succeed to generate_major_sstable_copy_task");
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_backup_major_copy_task(ObITask *parent_task, ObITask *child_task)
{
  int ret = OB_SUCCESS;
  ObBackupCopyPhysicalTask *copy_task = NULL;
  ObBackupFinishTask *finish_task = NULL;
  const int64_t task_idx = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(parent_task) || OB_ISNULL(child_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(parent_task), KP(child_task));
  } else if (OB_FAIL(dag_->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc finish task", K(ret));
  } else if (OB_FAIL(build_backup_physical_ctx(ctx_->physical_backup_ctx_))) {
    LOG_WARN("failed to build physical sstable ctx", K(ret));
  } else if (OB_FAIL(finish_task->init(*ctx_))) {
    LOG_WARN("failed to init finish task", K(ret));
  } else if (OB_FAIL(finish_task->add_child(*child_task))) {
    LOG_WARN("failed to add child", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (ctx_->physical_backup_ctx_.get_task_count() > 0) {
      // parent->copy->finish->child
      if (OB_FAIL(dag_->alloc_task(copy_task))) {
        LOG_WARN("failed to alloc copy task", K(ret));
      } else if (OB_FAIL(copy_task->init(task_idx, *ctx_))) {
        LOG_WARN("failed to init copy task", K(ret));
      } else if (OB_FAIL(parent_task->add_child(*copy_task))) {
        LOG_WARN("failed to add child copy task", K(ret));
      } else if (OB_FAIL(copy_task->add_child(*finish_task))) {
        LOG_WARN("failed to add child finish task", K(ret));
      } else if (OB_FAIL(dag_->add_task(*copy_task))) {
        LOG_WARN("failed to add copy task to dag", K(ret));
      }
    } else {
      // parent->finish->child
      if (OB_FAIL(parent_task->add_child(*finish_task))) {
        LOG_WARN("failed to add child finish_task for parent", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dag_->add_task(*finish_task))) {
      LOG_WARN("failed to add finish task to dag", K(ret));
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::build_backup_physical_ctx(ObBackupPhysicalPGCtx &physical_backup_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(physical_backup_ctx.open())) {
    LOG_WARN("failed to open physical backup ctx", K(ret), K(ctx_->pg_meta_));
  } else if (OB_FAIL(fetch_backup_sstables(physical_backup_ctx.table_keys_))) {
    LOG_WARN("failed to build sub task", K(ret));
  } else if (0 == physical_backup_ctx.table_keys_.count()) {
    LOG_INFO("pg don't have major sstable, ", K(ret), K(ctx_->pg_meta_));
  } else if (OB_FAIL(build_backup_sub_task(physical_backup_ctx))) {
    LOG_WARN("failed to build sub task", K(ret));
  } else {
    ATOMIC_AAF(&ctx_->data_statics_.total_macro_block_, physical_backup_ctx.macro_block_count_);
    LOG_INFO("succeed to build physical sstable ctx", K(physical_backup_ctx));
  }

  return ret;
}

int ObMigrateTaskGeneratorTask::fetch_backup_sstables(ObIArray<ObITable::TableKey> &table_keys)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else {
    // partition
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->part_ctx_array_.count(); ++i) {
      ObPartitionMigrateCtx &part_migrate_ctx = ctx_->part_ctx_array_.at(i);
      if (OB_LIKELY(!part_migrate_ctx.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("part_migrate_ctx is invalid", K(ret), K(part_migrate_ctx));
      } else {
        // tables
        const ObArray<ObMigrateTableInfo> &table_infos = part_migrate_ctx.copy_info_.table_infos_;
        for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
          const ObMigrateTableInfo &table_info = table_infos.at(i);
          // major sstables
          for (int64_t sstable_idx = 0; OB_SUCC(ret) && sstable_idx < table_info.major_sstables_.count();
               ++sstable_idx) {
            const ObITable::TableKey &major_table_key = table_info.major_sstables_.at(sstable_idx).src_table_key_;
            if (OB_UNLIKELY(!major_table_key.is_valid())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid major table key", K(ret), K(major_table_key));
            } else if (OB_UNLIKELY(!ObITable::is_major_sstable(major_table_key.table_type_))) {
              ret = OB_ERR_SYS;
              LOG_ERROR("table type is not major sstable", K(ret), K(major_table_key), K(table_info));
            } else {
              table_keys.push_back(major_table_key);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::build_backup_sub_task(ObBackupPhysicalPGCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < ctx.table_keys_.count(); ++i) {
      ObTableHandle tmp_handle;
      ObSSTable *sstable = NULL;
      int64_t sstable_macro_count = 0;
      ObITable::TableKey &major_table_key = ctx.table_keys_.at(i);
      if (OB_FAIL(ObPartitionService::get_instance().acquire_sstable(major_table_key, tmp_handle))) {
        STORAGE_LOG(WARN, "failed to get table", K(major_table_key), K(ret));
      } else if (OB_FAIL(tmp_handle.get_sstable(sstable))) {
        STORAGE_LOG(WARN, "failed to get table", K(major_table_key), K(ret));
      } else if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "sstable should not be null", K(ret));
      } else if (0 == (sstable_macro_count = sstable->get_meta().get_total_macro_block_count())) {
        // empty sstable
      } else {
        ObBackupMacroBlockInfo block_info;
        block_info.table_key_ = major_table_key;
        block_info.start_index_ = 0;
        block_info.cur_block_count_ = sstable_macro_count;
        block_info.total_block_count_ = sstable_macro_count;
        if (OB_FAIL(ctx.add_backup_macro_block_info(block_info))) {
          STORAGE_LOG(WARN, "add backup block info failed", K(ret), K(block_info));
        }
      }
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_pg_migrate_tasks(ObIArray<ObITask *> &last_task_array)
{
  int ret = OB_SUCCESS;
  ObITask *last_task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(ctx_->replica_op_arg_.dst_.get_replica_type())) {
    ret = OB_ERR_SYS;
    LOG_WARN("no need to generate migrate tasks", K(ret), K(ctx_->replica_op_arg_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->part_ctx_array_.count(); ++i) {
      ObPartitionMigrateCtx &part_migrate_ctx = ctx_->part_ctx_array_.at(i);
      if (part_migrate_ctx.copy_info_.meta_.pkey_.is_trans_table()) {
        continue;
      } else if (OB_FAIL(generate_migrate_tasks(part_migrate_ctx, last_task))) {
        LOG_WARN("fail to generate migrate tasks", K(ret), K(part_migrate_ctx));
      } else if (OB_FAIL(last_task_array.push_back(last_task))) {
        LOG_WARN("fail to push last task into last task array", K(ret));
      }
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_pg_rebuild_tasks(ObIArray<ObITask *> &last_task_array)
{
  int ret = OB_SUCCESS;
  ObITask *last_task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(ctx_->replica_op_arg_.dst_.get_replica_type())) {
    ret = OB_ERR_SYS;
    LOG_WARN("no need to generate rebuild tasks", K(ret), K(ctx_->replica_op_arg_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->part_ctx_array_.count(); ++i) {
      ObPartitionMigrateCtx &part_migrate_ctx = ctx_->part_ctx_array_.at(i);
      if (part_migrate_ctx.copy_info_.meta_.pkey_.is_trans_table()) {
        continue;
      } else if (OB_FAIL(generate_rebuild_tasks(part_migrate_ctx, last_task))) {
        LOG_WARN("fail to generate rebuild tasks", K(ret), K(part_migrate_ctx));
      } else if (OB_FAIL(last_task_array.push_back(last_task))) {
        LOG_WARN("fail to push last task into last task array", K(ret));
      }
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_migrate_tasks(ObPartitionMigrateCtx &part_migrate_ctx, ObITask *&last_task)
{
  int ret = OB_SUCCESS;
  ObFakeTask *wait_migrate_finish_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (!part_migrate_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy info is invalid", K(ret), K(part_migrate_ctx));
  } else if (OB_FAIL(generate_wait_migrate_finish_task(wait_migrate_finish_task))) {
    LOG_WARN("failed to generate_wait_migrate_finish_task", K(ret));
  } else if (OB_ISNULL(wait_migrate_finish_task)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("wait_rebuild_finish_task must not null", K(ret));
  } else {
    const ObMigratePartitionInfo &copy_info = part_migrate_ctx.copy_info_;
    const ObArray<ObMigrateTableInfo> &table_infos = copy_info.table_infos_;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
      const ObMigrateTableInfo &table_info = table_infos.at(i);
      if (OB_FAIL(generate_migrate_tasks(
              ctx_->migrate_src_info_, part_migrate_ctx, table_info, *wait_migrate_finish_task))) {
        LOG_WARN("failed to generate_migrate_tasks", K(ret), K(i), K(table_info), K(part_migrate_ctx));
      }
    }
  }

  if (OB_SUCC(ret)) {
    last_task = wait_migrate_finish_task;
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_GEN_REBUILD_TASK) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake EN_GEN_REBUILD_TASK", K(ret));
    }
  }
#endif
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_rebuild_tasks(ObPartitionMigrateCtx &part_migrate_ctx, ObITask *&last_task)
{
  int ret = OB_SUCCESS;
  ObFakeTask *wait_rebuild_finish_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (!part_migrate_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy info is invalid", K(ret), K(part_migrate_ctx));
  } else if (OB_FAIL(generate_wait_migrate_finish_task(wait_rebuild_finish_task))) {
    LOG_WARN("failed to generate_wait_rebuild_finish_task", K(ret));
  } else if (OB_ISNULL(wait_rebuild_finish_task)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("wait_rebuild_finish_task must not null", K(ret));
  } else {
    const ObMigratePartitionInfo &copy_info = part_migrate_ctx.copy_info_;
    const ObArray<ObMigrateTableInfo> &table_infos = copy_info.table_infos_;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
      const ObMigrateTableInfo &table_info = table_infos.at(i);
      if (OB_FAIL(generate_rebuild_tasks(
              ctx_->migrate_src_info_, part_migrate_ctx, table_info, *wait_rebuild_finish_task))) {
        LOG_WARN("failed to generate_rebuild_tasks", K(ret), K(i), K(table_info), K(part_migrate_ctx));
      }
    }
  }

  if (OB_SUCC(ret)) {
    last_task = wait_rebuild_finish_task;
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_GEN_REBUILD_TASK) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake EN_GEN_REBUILD_TASK", K(ret));
    }
  }
#endif
  return ret;
}

int ObITableTaskGeneratorTask::generate_wait_migrate_finish_task(ObFakeTask *&wait_migrate_finish_task)
{
  int ret = OB_SUCCESS;
  wait_migrate_finish_task = NULL;

  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret), KP(ctx_));
  } else if (OB_FAIL(dag_->alloc_task(wait_migrate_finish_task))) {
    LOG_WARN("failed to alloc wait_rebuild_finish_task", K(ret));
  } else if (OB_FAIL(add_child(*wait_migrate_finish_task))) {
    LOG_WARN("failed to add child wait_rebuild_finish_task", K(ret));
  } else if (OB_FAIL(dag_->add_task(*wait_migrate_finish_task))) {
    LOG_WARN("failed to add wait_rebuild_finish_task", K(ret));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_rebuild_tasks(const ObMigrateSrcInfo &src_info,
    ObPartitionMigrateCtx &part_migrate_ctx, const ObMigrateTableInfo &table_info, ObFakeTask &wait_rebuild_finish_task)
{
  int ret = OB_SUCCESS;
  ObITask *parent_task = this;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if ((!src_info.is_valid() && RESTORE_REPLICA_OP != ctx_->replica_op_arg_.type_) ||
             !part_migrate_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate migrate tasks get invalid argument", K(ret), K(src_info), K(part_migrate_ctx));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(ctx_->replica_op_arg_.dst_.get_replica_type())) {
    ret = OB_ERR_SYS;
    LOG_WARN("no need to generate minor task", K(ret), K(ctx_->replica_op_arg_));
  } else if (OB_FAIL(generate_major_tasks(src_info, part_migrate_ctx, table_info, parent_task))) {
    LOG_WARN("failed to generate rebuild major tasks", K(ret), K(table_info), K(part_migrate_ctx));
  } else if (OB_FAIL(generate_minor_tasks(src_info, part_migrate_ctx, table_info, parent_task))) {
    LOG_WARN("failed to generate rebuild minor tasks", K(ret), K(table_info), K(part_migrate_ctx));
  } else if (OB_FAIL(parent_task->add_child(wait_rebuild_finish_task))) {
    LOG_WARN("failed to add wait_rebuild_finish_task", K(ret));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_migrate_tasks(const ObMigrateSrcInfo &src_info,
    ObPartitionMigrateCtx &part_migrate_ctx, const ObMigrateTableInfo &table_info, ObFakeTask &wait_migrate_finish_task)
{
  int ret = OB_SUCCESS;
  ObITask *parent_task = this;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if ((RESTORE_REPLICA_OP != ctx_->replica_op_arg_.type_ && !src_info.is_valid()) ||
             !part_migrate_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate tasks get invalid argument", K(ret), K(src_info), K(part_migrate_ctx));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(ctx_->replica_op_arg_.dst_.get_replica_type())) {
    ret = OB_ERR_SYS;
    LOG_WARN("no need to generate minor task", K(ret), K(ctx_->replica_op_arg_));
  } else if (OB_FAIL(generate_minor_tasks(src_info, part_migrate_ctx, table_info, parent_task))) {
    LOG_WARN("failed to generate migrate minor tasks", K(ret), K(table_info), K(part_migrate_ctx));
  } else if (OB_FAIL(generate_major_tasks(src_info, part_migrate_ctx, table_info, parent_task))) {
    LOG_WARN("failed to generate migrate major tasks", K(ret), K(table_info), K(part_migrate_ctx));
  } else if (OB_FAIL(parent_task->add_child(wait_migrate_finish_task))) {
    LOG_WARN("failed to add wait_migrate_finish_task", K(ret));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_major_tasks(const ObMigrateSrcInfo &src_info,
    ObPartitionMigrateCtx &part_migrate_ctx, const ObMigrateTableInfo &table_info, share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if ((!src_info.is_valid() && RESTORE_REPLICA_OP != ctx_->replica_op_arg_.type_) || OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate migrate major task get invalid argument", K(ret), K(src_info), KP(parent_task));
  } else {
    // add major sstables
    for (int64_t sstable_idx = 0; OB_SUCC(ret) && sstable_idx < table_info.major_sstables_.count(); ++sstable_idx) {
      const ObMigrateTableInfo::SSTableInfo &major_table_info = table_info.major_sstables_.at(sstable_idx);
      ObFakeTask *wait_finish_task = NULL;
      if (!ObITable::is_major_sstable(major_table_info.src_table_key_.table_type_)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table type not match major sstable", K(ret), K(major_table_info), K(table_info));
      } else {
        if (OB_FAIL(dag_->alloc_task(wait_finish_task))) {
          LOG_WARN("failed to alloc wait finish task", K(ret));
        } else if (OB_FAIL(generate_major_sstable_copy_task(
                       src_info, part_migrate_ctx, major_table_info, parent_task, wait_finish_task))) {
          LOG_WARN("failed to generate major sstable copy task", K(ret), K(major_table_info));
        } else if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
          LOG_WARN("failed to add wait finish task", K(ret));
        } else {
          parent_task = wait_finish_task;
          LOG_INFO("succeed to generate_major_sstable_copy_task", K(src_info), K(major_table_info));
        }
      }
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_minor_tasks(const ObMigrateSrcInfo &src_info,
    ObPartitionMigrateCtx &part_migrate_ctx, const ObMigrateTableInfo &table_info, share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if ((!src_info.is_valid() && RESTORE_REPLICA_OP != ctx_->replica_op_arg_.type_) || OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate migrate minor task get invalid argument", K(ret), K(src_info), KP(parent_task));
  } else {
    for (int64_t sstable_idx = 0; OB_SUCC(ret) && sstable_idx < table_info.minor_sstables_.count(); ++sstable_idx) {
      const ObMigrateTableInfo::SSTableInfo &minor_sstable_info = table_info.minor_sstables_.at(sstable_idx);
      ObFakeTask *wait_finish_task = NULL;
      if (!ObITable::is_minor_sstable(minor_sstable_info.src_table_key_.table_type_) &&
          !ObITable::is_memtable(minor_sstable_info.src_table_key_.table_type_)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table type not match minor sstable", K(ret), K(minor_sstable_info), K(table_info));
      } else {
        if (OB_FAIL(dag_->alloc_task(wait_finish_task))) {
          LOG_WARN("failed to alloc wait finish task", K(ret));
        } else if (OB_FAIL(generate_minor_sstable_copy_task(
                       parent_task, wait_finish_task, src_info, minor_sstable_info, part_migrate_ctx))) {
          LOG_WARN("failed to generate minor sstable copy task", K(ret), K(minor_sstable_info));
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
            LOG_WARN("failed to add wait finish task", K(ret));
          } else {
            parent_task = wait_finish_task;
            LOG_INFO("succeed to generate_minor_sstable_copy_task", K(src_info), K(minor_sstable_info));
          }
        }
      }
    }
  }
  return ret;
}

// now only support rebuild from old server,
// if want support more condition, modified it
int ObMigrateTaskGeneratorTask::generate_physic_minor_sstable_copy_task(const ObMigrateSrcInfo &src_info,
    ObPartitionMigrateCtx &part_migrate_ctx, const ObMigrateTableInfo::SSTableInfo &minor_sstable_info,
    ObITask *parent_task, ObITask *child_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(parent_task) || OB_ISNULL(child_task) ||
             (RESTORE_REPLICA_OP != ctx_->replica_op_arg_.type_ && !src_info.is_valid()) ||
             !minor_sstable_info.is_valid() || !part_migrate_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args",
        K(ret),
        KP(parent_task),
        KP(child_task),
        K(src_info),
        K(minor_sstable_info),
        K(ctx_->replica_op_arg_),
        K(part_migrate_ctx));
  } else if (OB_FAIL(generate_physical_sstable_copy_task(
                 src_info, part_migrate_ctx, minor_sstable_info, parent_task, child_task))) {
    LOG_WARN("fail to generate physic sstable copy task", K(ret), K(src_info), K(minor_sstable_info));
  }
  return ret;
}

int ObITableTaskGeneratorTask::generate_logic_minor_sstable_copy_task(ObITask *parent_task, ObITask *child_task,
    const ObMigrateSrcInfo &src_info, const ObMigrateTableInfo::SSTableInfo &minor_sstable_info,
    ObPartitionMigrateCtx &part_migrate_ctx)
{
  int ret = OB_SUCCESS;
  ObMigrateCopyLogicTask *copy_task = NULL;
  ObMigrateFinishLogicTask *finish_task = NULL;
  const int64_t task_idx = 0;
  const ObITable::TableKey &table_key = minor_sstable_info.src_table_key_;

  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(parent_task) || OB_ISNULL(child_task) || !src_info.is_valid() ||
             !minor_sstable_info.is_valid() || !part_migrate_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args",
        K(ret),
        KP(parent_task),
        KP(child_task),
        K(src_info),
        K(minor_sstable_info),
        K(part_migrate_ctx));
  } else if (table_key.pkey_.get_partition_id() != part_migrate_ctx.copy_info_.meta_.pkey_.get_partition_id()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("cannot migrate partition with different partition id minor sstable",
        K(ret),
        K(table_key),
        "pkey",
        part_migrate_ctx.copy_info_.meta_.pkey_,
        "pg_key",
        ctx_->replica_op_arg_.key_);
  } else if (OB_FAIL(dag_->alloc_task(copy_task))) {
    LOG_WARN("failed to alloc copy task", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc finish task", K(ret));
  } else if (OB_FAIL(build_logic_sstable_ctx(src_info, minor_sstable_info, finish_task->get_sstable_ctx()))) {
    LOG_WARN("failed to build logic sstable ctx", K(ret), K(minor_sstable_info));
  } else if (OB_FAIL(copy_task->init(task_idx, finish_task->get_sstable_ctx(), *ctx_))) {
    LOG_WARN("failed to init copy task", K(ret));
  } else if (OB_FAIL(finish_task->init(part_migrate_ctx))) {
    LOG_WARN("failed to init finish task", K(ret), K(part_migrate_ctx));
  } else if (OB_FAIL(parent_task->add_child(*copy_task))) {
    LOG_WARN("failed to add child copy task", K(ret));
  } else if (OB_FAIL(copy_task->add_child(*finish_task))) {
    LOG_WARN("failed to add child finish task", K(ret));
  } else if (OB_FAIL(finish_task->add_child(*child_task))) {
    LOG_WARN("failed to add child", K(ret));
  } else if (OB_FAIL(dag_->add_task(*copy_task))) {
    LOG_WARN("failed to add copy task to dag", K(ret));
  } else if (OB_FAIL(dag_->add_task(*finish_task))) {
    LOG_WARN("failed to add finish task to dag", K(ret));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_minor_sstable_copy_task(share::ObITask *parent_task,
    share::ObITask *child_task, const ObMigrateSrcInfo &src_info,
    const ObMigrateTableInfo::SSTableInfo &minor_sstable_info, ObPartitionMigrateCtx &part_migrate_ctx)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(parent_task) || OB_ISNULL(child_task) ||
             (RESTORE_REPLICA_OP != ctx_->replica_op_arg_.type_ && !src_info.is_valid()) ||
             !minor_sstable_info.is_valid() || !part_migrate_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args",
        K(ret),
        KP(parent_task),
        KP(child_task),
        K(src_info),
        K(minor_sstable_info),
        K(part_migrate_ctx));
  } else {
    if (ObITable::is_minor_sstable(minor_sstable_info.src_table_key_.table_type_)) {
      if (OB_FAIL(generate_physic_minor_sstable_copy_task(
              src_info, part_migrate_ctx, minor_sstable_info, parent_task, child_task))) {
        LOG_WARN("failed to generate physic minor sstable copy task", K(ret), K(src_info), K(minor_sstable_info));
      }
    } else {
      if (OB_FAIL(generate_logic_minor_sstable_copy_task(
              parent_task, child_task, src_info, minor_sstable_info, part_migrate_ctx))) {
        LOG_WARN("fail to generate logic minor sstable copy task", K(ret), K(src_info), K(minor_sstable_info));
      }
    }
  }
  return ret;
}

int ObITableTaskGeneratorTask::build_logic_sstable_ctx(const ObMigrateSrcInfo &src_info,
    const ObMigrateTableInfo::SSTableInfo &minor_sstable_info, ObMigrateLogicSSTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  obrpc::ObFetchLogicBaseMetaArg arg;
  ObLogicBaseMetaReader reader;
  obrpc::ObFetchLogicRowArg logic_row_arg;
  ObLogicDataChecksumReader logic_data_checksum_reader;
  ObSchemaGetterGuard schema_guard;

  common::ObArray<common::ObStoreRowkey> end_key_list;
  ctx.reset();

  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), KP(ctx_));
  } else {
    arg.table_key_ = minor_sstable_info.src_table_key_;
    arg.task_count_ = ObMigratePrepareTask::MAX_LOGIC_TASK_COUNT_PER_SSTABLE;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(
                 reader.init(*srv_rpc_proxy_, *bandwidth_throttle_, src_info.src_addr_, arg, src_info.cluster_id_))) {
    LOG_WARN("failed to init logic meta reader", K(ret), K(src_info), K(arg));
  } else if (OB_FAIL(reader.fetch_end_key_list(end_key_list))) {
    LOG_WARN("failed to fetch_end_key_list", K(ret), K(src_info), K(arg));
  } else if (OB_FAIL(ctx.build_sub_task(end_key_list))) {
    LOG_WARN("failed to build sub task", K(ret));
  } else if (FALSE_IT(ctx.meta_.schema_version_ = ctx_->pg_meta_.storage_info_.get_data_info().get_schema_version())) {
    // get remote memtable's max table_schema_version
  } else if (OB_FAIL(MIGRATOR.get_schema_service()->retry_get_schema_guard(ctx.meta_.schema_version_,
                 minor_sstable_info.src_table_key_.table_id_,
                 schema_guard,
                 ctx.meta_.schema_version_))) {
    LOG_WARN("failed to get schema guard", K(minor_sstable_info), K(ret), K(ctx));
  } else {
    ctx.src_info_ = src_info;
    ctx.table_key_ = minor_sstable_info.src_table_key_;
    ctx.dest_base_version_ = minor_sstable_info.dest_base_version_;
    LOG_INFO("succ to get logic migrate context", K(ctx));
  }

  // fetch logic data checksum
  if (OB_FAIL(ret)) {
  } else {
    logic_row_arg.table_key_ = minor_sstable_info.src_table_key_;
    logic_row_arg.key_range_.set_whole_range();
    logic_row_arg.schema_version_ = ctx.meta_.schema_version_;
    if (OB_FAIL(logic_data_checksum_reader.init(
            *srv_rpc_proxy_, *bandwidth_throttle_, src_info.src_addr_, src_info.cluster_id_, logic_row_arg))) {
      LOG_WARN("failed to init logic data checksum reader", K(ret));
    } else if (OB_FAIL(logic_data_checksum_reader.get_data_checksum(ctx.data_checksum_))) {
      LOG_WARN("fail to get logic data checksum", K(ret));
    } else {
      LOG_INFO("succ to get logic data checksum", K(ctx.data_checksum_));
    }
  }

  return ret;
}

int ObITableTaskGeneratorTask::generate_physical_sstable_copy_task(const ObMigrateSrcInfo &src_info,
    ObIPartitionMigrateCtx &part_migrate_ctx, const ObMigrateTableInfo::SSTableInfo &sstable_info, ObITask *parent_task,
    ObITask *child_task)
{
  int ret = OB_SUCCESS;
  ObMigrateCopyPhysicalTask *copy_task = NULL;
  ObMigrateFinishPhysicalTask *finish_task = NULL;
  const int64_t task_idx = 0;

  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), KP(ctx_));
  } else if (OB_ISNULL(parent_task) || OB_ISNULL(child_task) ||
             (RESTORE_REPLICA_OP != ctx_->replica_op_arg_.type_ && !src_info.is_valid()) || !sstable_info.is_valid() ||
             !part_migrate_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid args", K(ret), KP(parent_task), KP(child_task), K(src_info), K(sstable_info), K(part_migrate_ctx));
  } else if (OB_FAIL(dag_->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc finish task", K(ret));
  } else if (OB_FAIL(build_physical_sstable_ctx(src_info, sstable_info, finish_task->get_sstable_ctx()))) {
    LOG_WARN("failed to build physical sstable ctx", K(ret), K(src_info), K(sstable_info));
  } else if (OB_FAIL(finish_task->init(part_migrate_ctx))) {
    LOG_WARN("failed to init finish task", K(ret), K(part_migrate_ctx));
  } else if (OB_FAIL(finish_task->add_child(*child_task))) {
    LOG_WARN("failed to add child", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (finish_task->get_sstable_ctx().task_count_ > 0) {
      // parent->copy->finish->child
      if (OB_FAIL(dag_->alloc_task(copy_task))) {
        LOG_WARN("failed to alloc copy task", K(ret));
      } else if (OB_FAIL(copy_task->init(task_idx, finish_task->get_sstable_ctx(), *ctx_))) {
        LOG_WARN("failed to init copy task", K(ret));
      } else if (OB_FAIL(parent_task->add_child(*copy_task))) {
        LOG_WARN("failed to add child copy task", K(ret));
      } else if (OB_FAIL(copy_task->add_child(*finish_task))) {
        LOG_WARN("failed to add child finish task", K(ret));
      } else if (OB_FAIL(dag_->add_task(*copy_task))) {
        LOG_WARN("failed to add copy task to dag", K(ret));
      }
    } else {
      // parent->finish->child
      if (OB_FAIL(parent_task->add_child(*finish_task))) {
        LOG_WARN("failed to add child finish_task for parent", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dag_->add_task(*finish_task))) {
      LOG_WARN("failed to add finish task to dag", K(ret));
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_major_sstable_copy_task(const ObMigrateSrcInfo &src_info,
    ObPartitionMigrateCtx &part_migrate_ctx, const ObMigrateTableInfo::SSTableInfo &major_sstable_info,
    ObITask *parent_task, ObITask *child_task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(parent_task) || OB_ISNULL(child_task) ||
             (RESTORE_REPLICA_OP != ctx_->replica_op_arg_.type_ && !src_info.is_valid()) ||
             !major_sstable_info.is_valid() || !part_migrate_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args",
        K(ret),
        KP(parent_task),
        KP(child_task),
        K(src_info),
        K(major_sstable_info),
        K(part_migrate_ctx));
  } else if (OB_FAIL(generate_physical_sstable_copy_task(
                 src_info, part_migrate_ctx, major_sstable_info, parent_task, child_task))) {
    LOG_WARN("fail to generate physic sstable copy task", K(ret), K(major_sstable_info), K(src_info));
  }

  return ret;
}

int ObMigrateTaskGeneratorTask::generate_finish_migrate_task(
    const ObIArray<ObITask *> &last_task_array, share::ObITask *&last_task)
{
  int ret = OB_SUCCESS;
  ObMigrateFinishTask *finish_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(finish_task->init(*ctx_))) {
    LOG_WARN("failed to init finish task", K(ret));
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < last_task_array.count(); ++i) {
      ObITask *last_task = last_task_array.at(i);
      if (NULL != last_task) {
        if (OB_FAIL(last_task->add_child(*finish_task))) {
          LOG_WARN("failed to add child of last task", K(ret));
        }
      } else {
        if (OB_FAIL(add_child(*finish_task))) {
          LOG_WARN("failed to add child of prepare task", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dag_->add_task(*finish_task))) {
      LOG_WARN("failed to add finish task", K(ret));
    } else {
      last_task = finish_task;
    }
  }
  return ret;
}

int ObITableTaskGeneratorTask::get_base_meta_reader(
    const ObMigrateSrcInfo &src_info, const obrpc::ObFetchPhysicalBaseMetaArg &arg, ObIPhysicalBaseMetaReader *&reader)
{
  int ret = OB_SUCCESS;
  const int64_t compatible = ctx_->replica_op_arg_.phy_restore_arg_.restore_info_.compatible_;

  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), KP(ctx_));
  } else if (ctx_->replica_op_arg_.is_physical_restore_leader()) {
    if (OB_BACKUP_COMPATIBLE_VERSION_V1 == compatible || OB_BACKUP_COMPATIBLE_VERSION_V2 == compatible) {
      if (OB_FAIL(get_base_meta_restore_reader_v1(arg.table_key_, reader))) {
        STORAGE_LOG(WARN, "fail to get_base_meta_restore_reader_v1", K(ret));
      }
    } else if (OB_BACKUP_COMPATIBLE_VERSION_V3 == compatible || OB_BACKUP_COMPATIBLE_VERSION_V4 == compatible) {
      if (OB_FAIL(get_base_meta_restore_reader_v2(arg.table_key_, reader))) {
        STORAGE_LOG(WARN, "fail to get_base_meta_restore_reader_v2", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("restore compatible version is expected", K(ret), K(compatible), K(arg));
    }
  } else if (RESTORE_REPLICA_OP == ctx_->replica_op_arg_.type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("not support logical restore", K(ret), "replica_op_arg", ctx_->replica_op_arg_);
  } else if (BACKUP_REPLICA_OP == ctx_->replica_op_arg_.type_) {
    if (OB_FAIL(get_base_meta_backup_reader(arg.table_key_, reader))) {
      STORAGE_LOG(WARN, "fail to get_base_meta_backup_reader", K(ret));
    }
  } else {
    if (OB_FAIL(get_base_meta_ob_reader(src_info, arg, reader))) {
      STORAGE_LOG(WARN, "failed to get_macro_block_ob_reader", K(ret));
    }
  }
  return ret;
}

int ObITableTaskGeneratorTask::get_base_meta_restore_reader_v1(
    const ObITable::TableKey &table_key, ObIPhysicalBaseMetaReader *&reader)
{
  int ret = OB_SUCCESS;
  ObPhysicalBaseMetaRestoreReaderV1 *tmp_reader = NULL;

  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), KP(ctx_));
  } else if (OB_ISNULL(ctx_->restore_meta_reader_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("restore meta reader must not null", K(ret));
  } else if (NULL == (tmp_reader = cp_fty_->get_base_data_meta_restore_reader_v1())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to get macro block ob reader", K(ret));
  } else if (OB_FAIL(tmp_reader->init(*bandwidth_throttle_,
                 ctx_->replica_op_arg_.phy_restore_arg_,
                 table_key,
                 *ctx_->restore_meta_reader_))) {
    STORAGE_LOG(WARN, "failed to init ob reader", K(ret));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
  }

  if (NULL != cp_fty_) {
    if (OB_FAIL(ret)) {
      if (NULL != reader) {
        cp_fty_->free(reader);
        reader = NULL;
      }
    }
    if (NULL != tmp_reader) {
      cp_fty_->free(tmp_reader);
      tmp_reader = NULL;
    }
  }
  return ret;
}

int ObITableTaskGeneratorTask::get_base_meta_restore_reader_v2(
    const ObITable::TableKey &table_key, ObIPhysicalBaseMetaReader *&reader)
{
  int ret = OB_SUCCESS;
  ObPhysicalBaseMetaRestoreReaderV2 *tmp_reader = NULL;

  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), KP(ctx_));
  } else if (OB_ISNULL(ctx_->restore_meta_reader_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("restore meta reader must not null", K(ret));
  } else if (NULL == (tmp_reader = cp_fty_->get_base_data_meta_restore_reader_v2())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to get macro block ob reader", K(ret));
  } else if (OB_FAIL(tmp_reader->init(*bandwidth_throttle_,
                 ctx_->replica_op_arg_.phy_restore_arg_,
                 table_key,
                 *ctx_->restore_meta_reader_))) {
    STORAGE_LOG(WARN, "failed to init ob reader", K(ret));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
  }

  if (NULL != cp_fty_) {
    if (OB_FAIL(ret)) {
      if (NULL != reader) {
        cp_fty_->free(reader);
        reader = NULL;
      }
    }
    if (NULL != tmp_reader) {
      cp_fty_->free(tmp_reader);
      tmp_reader = NULL;
    }
  }
  return ret;
}

// TODO() delete it later
int ObITableTaskGeneratorTask::get_base_meta_backup_reader(
    const ObITable::TableKey &table_key, ObIPhysicalBaseMetaReader *&reader)
{
  int ret = OB_SUCCESS;
  ObPhysicalBaseMetaBackupReader *tmp_reader = NULL;
  UNUSED(table_key);

  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), KP(ctx_));
  } else if (OB_ISNULL(tmp_reader = cp_fty_->get_base_data_meta_backup_reader())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to get base data meta backup reader", K(ret));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
  }

  if (NULL != cp_fty_) {
    if (OB_FAIL(ret)) {
      if (NULL != reader) {
        cp_fty_->free(reader);
        reader = NULL;
      }
    }
    if (NULL != tmp_reader) {
      cp_fty_->free(tmp_reader);
      tmp_reader = NULL;
    }
  }
  return ret;
}

int ObITableTaskGeneratorTask::get_base_meta_ob_reader(
    const ObMigrateSrcInfo &src_info, const obrpc::ObFetchPhysicalBaseMetaArg &arg, ObIPhysicalBaseMetaReader *&reader)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), KP(ctx_));
  } else if (OB_FAIL(get_base_meta_rpc_reader(src_info, arg, reader))) {
    STORAGE_LOG(WARN, "fail to get base meta rpc reader", K(ret), K(src_info), K(arg));
  }

  if (NULL != cp_fty_) {
    if (OB_FAIL(ret)) {
      if (NULL != reader) {
        cp_fty_->free(reader);
        reader = NULL;
      }
    }
  }
  return ret;
}

int ObITableTaskGeneratorTask::get_base_meta_rpc_reader(
    const ObMigrateSrcInfo &src_info, const obrpc::ObFetchPhysicalBaseMetaArg &arg, ObIPhysicalBaseMetaReader *&reader)
{
  int ret = OB_SUCCESS;
  ObPhysicalBaseMetaReader *tmp_reader = NULL;

  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), KP(ctx_));
  } else if (NULL == (tmp_reader = cp_fty_->get_base_data_meta_ob_reader())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to get macro block ob reader", K(ret));
  } else if (OB_FAIL(tmp_reader->init(
                 *srv_rpc_proxy_, *bandwidth_throttle_, src_info.src_addr_, arg, src_info.cluster_id_))) {
    STORAGE_LOG(WARN, "failed to init ob reader", K(ret));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
  }

  if (NULL != cp_fty_) {
    if (OB_FAIL(ret)) {
      if (NULL != tmp_reader) {
        cp_fty_->free(tmp_reader);
        tmp_reader = NULL;
      }
    }
  }

  return ret;
}

int ObITableTaskGeneratorTask::build_physical_sstable_ctx(const ObMigrateSrcInfo &src_info,
    const ObMigrateTableInfo::SSTableInfo &sstable_info, ObMigratePhysicalSSTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  obrpc::ObFetchPhysicalBaseMetaArg arg;
  common::ObArray<blocksstable::ObSSTablePair> macro_block_list;
  ObTablesHandle handle;
  ctx.reset();
  ObIPhysicalBaseMetaReader *reader = NULL;
  arg.table_key_ = sstable_info.src_table_key_;
  ctx.sstable_info_ = sstable_info;
  ctx.src_info_ = src_info;
  ctx.is_leader_restore_ = ctx_->replica_op_arg_.is_physical_restore_leader();
  ctx.restore_version_ = ctx_->replica_op_arg_.phy_restore_arg_.restore_data_version_;

  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), KP(ctx_));
  } else if (OB_FAIL(get_base_meta_reader(src_info, arg, reader))) {
    LOG_WARN("fail to get base meta reader", K(ret));
  } else if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reader should not be NULL", K(ret), KP(reader));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(reader->fetch_sstable_meta(ctx.meta_))) {
    LOG_WARN("failed to fetch physic table meta", K(ret), K(src_info), K(arg));
  } else if (OB_FAIL(reader->fetch_macro_block_list(macro_block_list))) {
    LOG_WARN("failed to fetch_macro_block_list", K(ret), K(src_info), K(arg));
  } else if (OB_FAIL(calc_macro_block_reuse(sstable_info.src_table_key_, macro_block_list))) {
    LOG_WARN("fail to calc macro block reuse", K(ret));
  } else if (OB_FAIL(build_sub_physical_task(ctx, macro_block_list))) {
    LOG_WARN("failed to build sub task", K(ret));
  } else {
    ATOMIC_AAF(&ctx_->data_statics_.total_macro_block_, macro_block_list.count());
    LOG_INFO("succeed to build physical sstable ctx", K(ctx));
  }
  if (NULL != reader) {
    cp_fty_->free(reader);
  }

  return ret;
}

int ObITableTaskGeneratorTask::build_sub_physical_task(
    ObMigratePhysicalSSTableCtx &ctx, common::ObIArray<blocksstable::ObSSTablePair> &macro_block_list)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;

  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), KP(ctx_));
  } else if (0 == macro_block_list.count()) {
    ctx.task_count_ = 0;
    LOG_INFO("no macro block need copy", K(ret));
  } else {
    const int64_t max_macro_block_count_per_task = ObMigratePrepareTask::MAX_MACRO_BLOCK_COUNT_PER_TASK;
    ctx.task_count_ = macro_block_list.count() / max_macro_block_count_per_task;
    if (macro_block_list.count() % max_macro_block_count_per_task > 0) {
      ++ctx.task_count_;
    }

    if (OB_ISNULL(buf = ctx.allocator_.alloc(sizeof(ObMigratePhysicalSSTableCtx::SubTask) * ctx.task_count_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buf", K(ret));
    } else {
      ctx.tasks_ = new (buf) ObMigratePhysicalSSTableCtx::SubTask[ctx.task_count_];
      buf = NULL;
    }
    for (int64_t task_idx = 0; OB_SUCC(ret) && task_idx < ctx.task_count_; ++task_idx) {
      ObMigratePhysicalSSTableCtx::SubTask &sub_task = ctx.tasks_[task_idx];
      sub_task.pkey_ = ctx.sstable_info_.src_table_key_.pkey_;
      sub_task.block_count_ = std::min(
          max_macro_block_count_per_task, macro_block_list.count() - task_idx * max_macro_block_count_per_task);
      if (OB_ISNULL(buf = ctx.allocator_.alloc(sizeof(ObMigrateMacroBlockInfo) * sub_task.block_count_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc buf", K(ret));
      } else {
        sub_task.block_info_ = new (buf) ObMigrateMacroBlockInfo[sub_task.block_count_];
        buf = NULL;
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < sub_task.block_count_; ++i) {
        const int64_t macro_idx = task_idx * max_macro_block_count_per_task + i;

        if (macro_idx >= macro_block_list.count()) {
          break;
        }

        const blocksstable::ObSSTablePair &pair = macro_block_list.at(macro_idx);
        ObMigrateMacroBlockInfo &info = ctx.tasks_[task_idx].block_info_[i];
        info.pair_ = pair;
        info.need_copy_ = true;
      }
    }
  }
  return ret;
}

int ObITableTaskGeneratorTask::calc_macro_block_reuse(
    const ObITable::TableKey &table_key, ObIArray<ObSSTablePair> &macro_block_list)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), KP(ctx_));
  } else if (0 == macro_block_list.count()) {
    // do nothing
  } else if (OB_FAIL(ctx_->reuse_block_mgr_.build_reuse_macro_map(*ctx_, table_key, macro_block_list))) {
    LOG_WARN("fail to build reuse macro map", K(ret));
  }

  return ret;
}

int ObMigratePrepareTask::build_index_partition_info(
    const ObReplicaOpArg &arg, ObIPGPartitionBaseDataMetaObReader *reader)
{
  int ret = OB_SUCCESS;
  ObPartitionMigrateCtx part_migrate_ctx;
  ObArray<obrpc::ObFetchTableInfoResult> table_info_res;
  ObArray<uint64_t> table_id_list;
  bool found = false;
  ObPGPartitionMetaInfo partition_meta_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate preprae task do not init", K(ret));
  } else if (!arg.is_valid() || OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build index partition info get invalid argument", K(ret), K(arg), KP(reader));
  } else if (COPY_LOCAL_INDEX_OP != arg.type_) {
    ret = OB_ERR_SYS;
    LOG_WARN("replica op type do not match", K(ret), K(arg));
  } else {
    while (OB_SUCC(ret)) {
      partition_meta_info.reset();
      table_info_res.reuse();
      table_id_list.reuse();
      part_migrate_ctx.reset();
      found = false;
      if (OB_FAIL(reader->fetch_pg_partition_meta_info(partition_meta_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to fetch pg partition meta info", K(ret), K(ctx_->replica_op_arg_));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_meta_info.table_id_list_.count() && !found; ++i) {
          found = arg.index_id_ == partition_meta_info.table_id_list_.at(i);
          if (found) {
            if (OB_FAIL(table_id_list.push_back(partition_meta_info.table_id_list_.at(i)))) {
              LOG_WARN("failed to push table id into array", K(ret), K(arg));
            } else if (OB_FAIL(table_info_res.push_back(partition_meta_info.table_info_.at(i)))) {
              LOG_WARN("failed to push table info into array", K(ret), K(partition_meta_info), K(i));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (found) {
          if (OB_FAIL(
                  build_migrate_partition_info(partition_meta_info, table_info_res, table_id_list, part_migrate_ctx))) {
            LOG_WARN("fail to build migrate partition info", K(ret), K(partition_meta_info));
          } else if (OB_FAIL(ctx_->part_ctx_array_.push_back(part_migrate_ctx))) {
            LOG_WARN("fail to push part migrate ctx into array", K(ret), K(part_migrate_ctx));
          } else {
            break;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (0 == ctx_->part_ctx_array_.count()) {
        ret = OB_ERR_SYS;
        FLOG_WARN("local index can not find part ctx", K(ret), K(*ctx_));
      }
    }
  }
  return ret;
}

int ObMigratePrepareTask::build_table_partition_info(
    const ObReplicaOpArg &arg, ObIPGPartitionBaseDataMetaObReader *reader)
{
  int ret = OB_SUCCESS;
  ObPartitionMigrateCtx part_migrate_ctx;
  ObArray<obrpc::ObFetchTableInfoResult> table_info_res;
  ObArray<uint64_t> table_id_list;
  ObPGPartitionMetaInfo partition_meta_info;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (!arg.is_valid() || OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build table partition info get invalid argument", K(ret), K(arg), KP(reader));
  } else if (OB_UNLIKELY(COPY_LOCAL_INDEX_OP == arg.type_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("replica op type do not match", K(ret), K(arg));
  } else {
    while (OB_SUCC(ret)) {
      partition_meta_info.reset();
      table_info_res.reuse();
      table_id_list.reuse();
      part_migrate_ctx.reset();
      if (OB_FAIL(reader->fetch_pg_partition_meta_info(partition_meta_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to fetch pg partition meta info", K(ret), K(ctx_->replica_op_arg_));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(table_id_list.assign(partition_meta_info.table_id_list_))) {
        LOG_WARN("failed to assign table id list", K(ret));
      } else if (OB_FAIL(table_info_res.assign(partition_meta_info.table_info_))) {
        LOG_WARN("failed to copy needed table info", K(ret), K(partition_meta_info));
      } else if (OB_FAIL(build_migrate_partition_info(
                     partition_meta_info, table_info_res, table_id_list, part_migrate_ctx))) {
        LOG_WARN("fail to build migrate partition info", K(ret), K(partition_meta_info));
      } else if (OB_FAIL(ctx_->part_ctx_array_.push_back(part_migrate_ctx))) {
        LOG_WARN("fail to push part migrate ctx into array", K(ret), K(part_migrate_ctx));
      }

      if (OB_SUCC(ret) && ctx_->replica_op_arg_.is_physical_restore_follower()) {
        if (OB_FAIL(create_partition_if_needed(ctx_->partition_guard_, partition_meta_info.meta_))) {
          LOG_WARN("fail to create partition for follower restore", K(ret), K(arg), K(partition_meta_info.meta_));
        }
      }
    }
  }
  return ret;
}

int ObMigratePrepareTask::remove_uncontinue_local_tables(const ObPartitionKey &pkey, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObPGPartitionGuard guard;
  ObIPartitionGroup *partition = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (!pkey.is_valid() || 0 == table_id || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove uncontinue local tables get invalid argument", K(ret), K(pkey), K(table_id));
  } else if (OB_ISNULL(partition = ctx_->partition_guard_.get_partition_group())) {
    // do nothing
    LOG_DEBUG("partition not exist, skip remove uncontinue local tables", K(pkey), K(table_id));
  } else if (OB_FAIL(partition->get_pg_storage().remove_uncontinues_inc_tables(pkey, table_id))) {
    LOG_WARN("failed to remove uncontinue local tables", K(ret));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::check_partition_integrity()
{
  int ret = OB_SUCCESS;
  ObPGPartitionGuard guard;
  ObIPartitionGroup *partition = NULL;
  ObPartitionArray local_pkeys;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (COPY_LOCAL_INDEX_OP == ctx_->replica_op_arg_.type_ || BACKUP_REPLICA_OP == ctx_->replica_op_arg_.type_ ||
             VALIDATE_BACKUP_OP == ctx_->replica_op_arg_.type_ || BACKUP_BACKUPSET_OP == ctx_->replica_op_arg_.type_ ||
             BACKUP_ARCHIVELOG_OP == ctx_->replica_op_arg_.type_) {
    LOG_INFO("no need check partition integrity", "arg", ctx_->replica_op_arg_);
  } else if (OB_ISNULL(partition = ctx_->partition_guard_.get_partition_group())) {
    ret = OB_ERR_SYS;
    LOG_WARN("partition should not be NULL", K(ret), KP(partition));
  } else if (OB_FAIL(partition->get_all_pg_partition_keys(local_pkeys, true /*include_trans_table*/))) {
    LOG_WARN("failed to get pg partition keys", K(ret), K(*ctx_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->part_ctx_array_.count(); ++i) {
      ObPartitionMigrateCtx &pctx = ctx_->part_ctx_array_.at(i);
      const ObPartitionKey &remote_pkey = pctx.copy_info_.meta_.pkey_;
      pctx.is_partition_exist_ = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < local_pkeys.count() && !pctx.is_partition_exist_; ++j) {
        const ObPartitionKey &local_pkey = local_pkeys.at(j);
        if (local_pkey == remote_pkey) {
          pctx.is_partition_exist_ = true;
        }
      }

      LOG_INFO("check partition exist", K(remote_pkey), "is_exist", pctx.is_partition_exist_);
    }
  }
  return ret;
}

int ObMigratePrepareTask::create_pg_partition(
    const ObPGPartitionStoreMeta &meta, ObIPartitionGroup *partition, const bool in_slog_trans)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (!meta.is_valid() || OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create pg partition get invalid argument", K(ret), K(meta), KP(partition));
  } else {
    ObTablesHandle sstables_handle;  // empty
    obrpc::ObCreatePartitionArg arg;
    arg.schema_version_ = meta.create_schema_version_;
    arg.lease_start_ = meta.create_timestamp_;
    arg.restore_ = ctx_->is_restore_;
    const int64_t multi_version_start = meta.multi_version_start_;
    const int64_t data_table_id = meta.data_table_id_;
    const uint64_t unused = 0;
    const bool is_replay = false;

    if (OB_FAIL(partition->create_pg_partition(
            meta.pkey_, multi_version_start, data_table_id, arg, in_slog_trans, is_replay, unused, sstables_handle))) {
      LOG_WARN("fail to create pg partition", K(ret), K(meta));
    }
  }
  return ret;
}

int ObMigratePrepareTask::inner_get_partition_table_info_reader(
    const ObMigrateSrcInfo &src_info, ObIPGPartitionBaseDataMetaObReader *&reader)
{
  int ret = OB_SUCCESS;
  UNUSED(src_info);
  const bool is_copy_index = COPY_LOCAL_INDEX_OP == ctx_->replica_op_arg_.type_ ||
                             COPY_GLOBAL_INDEX_OP == ctx_->replica_op_arg_.type_ ||
                             LINK_SHARE_MAJOR_OP == ctx_->replica_op_arg_.type_;

  ObPGPartitionBaseDataMetaObReader *tmp_reader = NULL;
  obrpc::ObFetchPGPartitionInfoArg rpc_arg;
  rpc_arg.pg_key_ = ctx_->replica_op_arg_.key_;
  rpc_arg.snapshot_version_ = ctx_->pg_meta_.storage_info_.get_data_info().get_publish_version();
  rpc_arg.log_ts_ = ctx_->pg_meta_.storage_info_.get_data_info().get_last_replay_log_ts();
  rpc_arg.is_only_major_sstable_ = is_copy_index;

  if (OB_ISNULL(cp_fty_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cp fty should not be NULL", K(ret), KP(cp_fty_));
  } else if (NULL == (tmp_reader = cp_fty_->get_pg_info_reader())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to get macro block ob reader", K(ret));
  } else if (OB_FAIL(tmp_reader->init(
                 *srv_rpc_proxy_, *bandwidth_throttle_, src_info.src_addr_, rpc_arg, src_info.cluster_id_))) {
    LOG_WARN("fail to init partition meta info reader", K(ret), K(src_info));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
  }

  if (NULL != cp_fty_) {
    if (NULL != tmp_reader) {
      cp_fty_->free(tmp_reader);
      tmp_reader = NULL;
    }
  }
  return ret;
}

int ObMigratePrepareTask::inner_get_partition_table_info_restore_reader_v1(
    const ObMigrateSrcInfo &src_info, ObIPGPartitionBaseDataMetaObReader *&reader)
{
  int ret = OB_SUCCESS;
  UNUSED(src_info);
  ObPGPartitionBaseDataMetaRestoreReaderV1 *tmp_reader = NULL;
  ObPartitionGroupMetaRestoreReaderV1 *restore_meta_reader = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(ctx_->restore_meta_reader_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("restore_meta_reader_ must not null", K(ret));
  } else if (OB_ISNULL(cp_fty_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cp fty should not be NULL", K(ret), KP(cp_fty_));
  } else if (OB_ISNULL(tmp_reader = cp_fty_->get_pg_info_restore_reader_v1())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to get macro block ob reader", K(ret));
  } else if (OB_ISNULL(ctx_->restore_meta_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "phaysical restore macro index should not be NULL", K(ret), KP(ctx_->restore_meta_reader_));
  } else if (ObIPartitionGroupMetaRestoreReader::PG_META_RESTORE_READER_V1 != ctx_->restore_meta_reader_->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "restore meta reader type is unexpected", K(ret), K(ctx_->restore_meta_reader_->get_type()));
  } else if (FALSE_IT(restore_meta_reader =
                          reinterpret_cast<ObPartitionGroupMetaRestoreReaderV1 *>(ctx_->restore_meta_reader_))) {
  } else if (OB_FAIL(tmp_reader->init(ctx_->pg_meta_.partitions_, restore_meta_reader))) {
    LOG_WARN("fail to init partition table info restore reader", K(ret));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
  }

  if (NULL != cp_fty_) {
    if (NULL != tmp_reader) {
      cp_fty_->free(tmp_reader);
      tmp_reader = NULL;
    }
  }
  return ret;
}

int ObMigratePrepareTask::inner_get_partition_table_info_restore_reader_v2(
    const ObMigrateSrcInfo &src_info, ObIPGPartitionBaseDataMetaObReader *&reader)
{
  int ret = OB_SUCCESS;
  UNUSED(src_info);
  ObPGPartitionBaseDataMetaRestoreReaderV2 *tmp_reader = NULL;
  ObPartitionGroupMetaRestoreReaderV2 *restore_meta_reader = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(ctx_->restore_meta_reader_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("restore_meta_reader_ must not null", K(ret));
  } else if (OB_ISNULL(cp_fty_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cp fty should not be NULL", K(ret), KP(cp_fty_));
  } else if (OB_ISNULL(tmp_reader = cp_fty_->get_pg_info_restore_reader_v2())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to get macro block ob reader", K(ret));
  } else if (OB_ISNULL(ctx_->restore_meta_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "phaysical restore macro index should not be NULL", K(ret), KP(restore_meta_reader));
  } else if (ObIPartitionGroupMetaRestoreReader::PG_META_RESTORE_READER_V2 != ctx_->restore_meta_reader_->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "restore meta reader type is unexpected", K(ret), K(ctx_->restore_meta_reader_->get_type()));
  } else if (FALSE_IT(restore_meta_reader =
                          reinterpret_cast<ObPartitionGroupMetaRestoreReaderV2 *>(ctx_->restore_meta_reader_))) {
  } else if (OB_FAIL(tmp_reader->init(ctx_->pg_meta_.partitions_, restore_meta_reader))) {
    LOG_WARN("fail to init partition table info restore reader", K(ret));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
  }

  if (NULL != cp_fty_) {
    if (NULL != tmp_reader) {
      cp_fty_->free(tmp_reader);
      tmp_reader = NULL;
    }
  }
  return ret;
}

// TODO(muwei.ym) delete it later
int ObMigratePrepareTask::inner_get_partition_table_info_backup_reader(
    const ObMigrateSrcInfo &src_info, ObIPGPartitionBaseDataMetaObReader *&reader)
{
  int ret = OB_SUCCESS;
  UNUSED(src_info);
  ObPGPartitionBaseDataMetaBackupReader *tmp_reader = NULL;

  if (OB_ISNULL(cp_fty_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cp fty should not be NULL", K(ret), KP(cp_fty_));
  } else if (OB_ISNULL(tmp_reader = cp_fty_->get_pg_info_backup_reader())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to get macro block ob reader", K(ret));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
  }

  if (NULL != cp_fty_) {
    if (NULL != tmp_reader) {
      cp_fty_->free(tmp_reader);
      tmp_reader = NULL;
    }
  }
  return ret;
}

int ObMigratePrepareTask::get_partition_table_info_reader(
    const ObMigrateSrcInfo &src_info, ObIPGPartitionBaseDataMetaObReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = NULL;
  const int64_t compatible = ctx_->replica_op_arg_.phy_restore_arg_.restore_info_.compatible_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (!src_info.is_valid() && RESTORE_REPLICA_OP != ctx_->replica_op_arg_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "get partition table info reader get invalid argument", K(ret), K(src_info), K(ctx_->replica_op_arg_.type_));
  } else if (ctx_->replica_op_arg_.is_physical_restore_leader() && share::REPLICA_RESTORE_DATA == ctx_->is_restore_) {
    if (OB_BACKUP_COMPATIBLE_VERSION_V1 == compatible || OB_BACKUP_COMPATIBLE_VERSION_V2 == compatible) {
      if (OB_FAIL(inner_get_partition_table_info_restore_reader_v1(src_info, reader))) {
        LOG_WARN("failed to get partition table info restore reader", K(ret));
      }
    } else if (OB_BACKUP_COMPATIBLE_VERSION_V3 == compatible || OB_BACKUP_COMPATIBLE_VERSION_V4 == compatible) {
      if (OB_FAIL(inner_get_partition_table_info_restore_reader_v2(src_info, reader))) {
        LOG_WARN("failed to get partition table info restore reader", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("restore compatible version is expected", K(ret), K(compatible));
    }
  } else if (RESTORE_REPLICA_OP == ctx_->replica_op_arg_.type_ &&
             share::REPLICA_LOGICAL_RESTORE_DATA == ctx_->is_restore_) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("not support logical restore", K(ret));
  } else if (BACKUP_REPLICA_OP == ctx_->replica_op_arg_.type_) {
    if (OB_FAIL(inner_get_partition_table_info_backup_reader(src_info, reader))) {
      LOG_WARN("failed to get partition table info backup reader", K(ret));
    }
  } else {
    if (OB_FAIL(inner_get_partition_table_info_reader(src_info, reader))) {
      LOG_WARN("failed to get partition table info reader", K(ret));
    }
  }
  return ret;
}

int ObMigratePrepareTask::check_backup_data_continues()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition_group = NULL;
  clog::ObPGLogArchiveStatus pg_log_archive_status;
  ObLogArchiveBackupInfo archive_backup_info;
  const int64_t start_ts = ObTimeUtility::current_time();
  static const int64_t OB_MAX_RETRY_TIME = 60 * 1000 * 1000;  // 60s
  static const int64_t OB_SINGLE_SLEEP_US = 1 * 1000 * 1000;  // 1s
  const int64_t last_replay_log_id = ctx_->pg_meta_.storage_info_.get_clog_info().get_last_replay_log_id();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (BACKUP_REPLICA_OP != ctx_->replica_op_arg_.type_) {
    // do nothing
  } else if (OB_ISNULL(partition_group = ctx_->get_partition())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup local partition should not be NULL", K(ret), KP(partition_group), K(*ctx_));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(partition_group->get_log_archive_status(pg_log_archive_status))) {
        LOG_WARN("failed to get log archive status", K(ret), K(*ctx_));
      } else if (OB_FAIL(ObBackupInfoMgr::get_instance().get_log_archive_backup_info(archive_backup_info))) {
        LOG_WARN("failed to get log archive backup info", K(ret));
      } else if (ObLogArchiveStatus::INVALID == pg_log_archive_status.status_) {
        if (ObLogArchiveStatus::DOING == archive_backup_info.status_.status_) {
          LOG_WARN("partition group log archive status is invalid, need retry",
              K(pg_log_archive_status),
              K(archive_backup_info));
          ret = OB_EAGAIN;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN(
              "server do not archive log, can not backup", K(ret), K(pg_log_archive_status), K(archive_backup_info));
        }
      } else if (ObLogArchiveStatus::DOING == pg_log_archive_status.status_) {
        if (pg_log_archive_status.archive_incarnation_ != archive_backup_info.status_.incarnation_ ||
            pg_log_archive_status.log_archive_round_ != archive_backup_info.status_.round_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pg archive round or incarnation not equal to server archive info",
              K(ret),
              K(pg_log_archive_status),
              K(archive_backup_info));
        } else if (pg_log_archive_status.round_start_log_id_ < 1) {
          ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
          LOG_WARN("log archive status is not match", K(ret), K(pg_log_archive_status));
        } else if (last_replay_log_id + 1 < pg_log_archive_status.round_start_log_id_) {
          ret = OB_ARCHIVE_LOG_NOT_CONTINUES_WITH_DATA;
          LOG_WARN("base data and log archive data do not continues",
              K(ret),
              K(pg_log_archive_status),
              K(ctx_->pg_meta_),
              K(last_replay_log_id));
        } else {
          break;
        }
      } else {
        ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
        LOG_WARN("log archive status is unexpected", K(ret), K(pg_log_archive_status));
      }

      if (OB_EAGAIN == ret) {
        const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
        if (cost_ts < OB_MAX_RETRY_TIME) {
          usleep(OB_SINGLE_SLEEP_US);
          ret = OB_SUCCESS;
        } else {
          ret = OB_PG_LOG_ARCHIVE_STATUS_NOT_INIT;
          LOG_WARN("server start do archive log, but partition retry many times status still wrong",
              K(ret),
              K(pg_log_archive_status),
              K(archive_backup_info),
              K(*ctx_));
        }
      }
    }
  }
  return ret;
}

ObMigrateLogicSSTableCtx::SubLogicTask::SubLogicTask() : end_key_(), block_write_ctx_(), lob_block_write_ctx_()
{}

ObMigrateLogicSSTableCtx::SubLogicTask::~SubLogicTask()
{}

ObMigrateLogicSSTableCtx::ObMigrateLogicSSTableCtx()
    : tasks_(NULL),
      task_count_(0),
      meta_(),
      allocator_(ObModIds::OB_PARTITION_MIGRATOR),
      src_info_(),
      table_key_(),
      dest_base_version_(-1),
      data_checksum_(0)
{}

ObMigrateLogicSSTableCtx::~ObMigrateLogicSSTableCtx()
{
  reset();
}

void ObMigrateLogicSSTableCtx::reset()
{
  for (int64_t i = 0; i < task_count_; ++i) {
    tasks_[i].~SubLogicTask();
  }
  tasks_ = NULL;
  task_count_ = 0;
  meta_.reset();
  allocator_.reset();
  src_info_.reset();
  table_key_.reset();
  dest_base_version_ = -1;
  data_checksum_ = 0;
}

bool ObMigrateLogicSSTableCtx::is_valid()
{
  bool valid = true;

  if (NULL == tasks_ || task_count_ <= 0 || !meta_.is_valid() || !src_info_.is_valid() || !table_key_.is_valid() ||
      dest_base_version_ < table_key_.trans_version_range_.base_version_ ||
      dest_base_version_ >= table_key_.trans_version_range_.snapshot_version_) {
    valid = false;
  } else {
    for (int64_t i = 0; valid && i < task_count_; ++i) {
      if (!tasks_[i].end_key_.is_valid()) {
        valid = false;
      }
    }
  }
  return valid;
}

int ObMigrateLogicSSTableCtx::build_sub_task(const common::ObIArray<common::ObStoreRowkey> &end_key_list)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;

  if (NULL != tasks_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (end_key_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(end_key_list));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(SubLogicTask) * end_key_list.count()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc sub logic task buf", K(ret));
  } else {
    task_count_ = end_key_list.count();
    tasks_ = new (buf) SubLogicTask[task_count_];
    for (int64_t i = 0; OB_SUCC(ret) && i < task_count_; ++i) {
      if (OB_FAIL(end_key_list.at(i).deep_copy(tasks_[i].end_key_, allocator_))) {
        LOG_WARN("failed to deep copy end key", K(ret));
      }

      if (OB_SUCC(ret) && i == task_count_ - 1) {
        if (!tasks_[i].end_key_.is_max()) {
          ret = OB_ERR_SYS;
          LOG_ERROR("the last end key must be max row", K(ret), K(tasks_[i].end_key_), K(end_key_list));
        }
      }
    }
  }
  return ret;
}

int ObMigrateLogicSSTableCtx::get_sub_task(const int64_t idx, SubLogicTask *&sub_task)
{
  int ret = OB_SUCCESS;
  sub_task = NULL;

  if (OB_ISNULL(tasks_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(*this));
  } else if (idx < 0 || idx >= task_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(idx), K(*this));
  } else {
    sub_task = tasks_ + idx;
  }
  return ret;
}

int ObMigrateLogicSSTableCtx::get_dest_table_key(ObITable::TableKey &dest_table_key)
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid ctx", K(ret), K(*this));
  } else if (table_key_.trans_version_range_.base_version_ == dest_base_version_) {
    dest_table_key = table_key_;
    dest_table_key.table_type_ = ObITable::MINI_MINOR_SSTABLE;
  } else {
    dest_table_key = table_key_;
    dest_table_key.table_type_ = ObITable::MINI_MINOR_SSTABLE;
    dest_table_key.trans_version_range_.base_version_ = dest_base_version_;
    dest_table_key.version_ = 0;
    if (dest_base_version_ > dest_table_key.trans_version_range_.multi_version_start_) {
      dest_table_key.trans_version_range_.multi_version_start_ = dest_base_version_;
    }
    LOG_INFO(
        "base version not match, rewrite trans version range", K(dest_base_version_), K_(table_key), K(dest_table_key));
  }
  return ret;
}

ObMigrateCopyLogicTask::ObMigrateCopyLogicTask()
    : ObITask(TASK_TYPE_MIGRATE_COPY_LOGIC),
      is_inited_(false),
      ctx_(NULL),
      task_idx_(-1),
      sstable_ctx_(NULL),
      sub_task_(NULL)
{}

ObMigrateCopyLogicTask::~ObMigrateCopyLogicTask()
{}

int ObMigrateCopyLogicTask::init(const int64_t task_idx, ObMigrateLogicSSTableCtx &sstable_ctx, ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (task_idx < 0 || task_idx >= sstable_ctx.task_count_ || !sstable_ctx.is_valid() || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(task_idx), K(sstable_ctx), K(ctx));
  } else {
    ctx_ = &ctx;
    task_idx_ = task_idx;
    sstable_ctx_ = &sstable_ctx;
    sub_task_ = sstable_ctx.tasks_ + task_idx_;
    is_inited_ = true;
  }
  return ret;
}

int ObMigrateCopyLogicTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  const int64_t next_task_idx = task_idx_ + 1;
  ObMigrateCopyLogicTask *tmp_next_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not init", K(ret));
  } else if (task_idx_ + 1 == sstable_ctx_->task_count_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(dag_->alloc_task(tmp_next_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(tmp_next_task->init(next_task_idx, *sstable_ctx_, *ctx_))) {
    LOG_WARN("failed o init next task", K(ret));
  } else {
    next_task = tmp_next_task;
  }
  return ret;
}

int ObMigrateCopyLogicTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogicRowFetcher fetcher;
  obrpc::ObFetchLogicRowArg reader_arg;
  ObMigrateLogicRowWriter writer;
  obrpc::ObPartitionServiceRpcProxy *srv_rpc_proxy = MIGRATOR.get_svr_rpc_proxy();
  common::ObInOutBandwidthThrottle *bandwidth_throttle = MIGRATOR.get_bandwidth_throttle();
  DEBUG_SYNC(BEFORE_MIGRATE_COPY_LOGIC_DATA);

  if (NULL != sstable_ctx_) {
    LOG_INFO("start ObMigrateCopyLogicTask process",
        "table_key",
        sstable_ctx_->table_key_,
        "dest_base_version",
        sstable_ctx_->dest_base_version_);
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_ISNULL(srv_rpc_proxy) || OB_ISNULL(bandwidth_throttle)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("srv_rpc_proxy or bandwidth_throttle must not null", K(ret), KP(srv_rpc_proxy), KP(bandwidth_throttle));
  } else {
    ctx_->action_ = ObMigrateCtx::COPY_MINOR;
    if (OB_SUCCESS != (tmp_ret = ctx_->update_partition_migration_status())) {
      LOG_WARN("failed to update_partition_migration_status", K(tmp_ret));
    }

    reader_arg.table_key_ = sstable_ctx_->table_key_;
    reader_arg.key_range_.set_left_open();
    reader_arg.schema_version_ = sstable_ctx_->meta_.schema_version_;
    if (task_idx_ == 0) {
      reader_arg.key_range_.set_start_key(ObStoreRowkey::MIN_STORE_ROWKEY);
    } else {
      reader_arg.key_range_.set_start_key(sstable_ctx_->tasks_[task_idx_ - 1].end_key_);
    }

    if (task_idx_ == sstable_ctx_->task_count_ - 1) {
      reader_arg.key_range_.set_end_key(ObStoreRowkey::MAX_STORE_ROWKEY);
      reader_arg.key_range_.set_right_open();
    } else {
      reader_arg.key_range_.set_end_key(sub_task_->end_key_);
      reader_arg.key_range_.set_right_closed();
    }
    reader_arg.data_checksum_ = sstable_ctx_->data_checksum_;
    if (OB_FAIL(fetcher.init(*srv_rpc_proxy,
            *bandwidth_throttle,
            sstable_ctx_->src_info_.src_addr_,
            reader_arg,
            ctx_->replica_op_arg_.cluster_id_))) {
      LOG_WARN("failed to init reader", K(ret));
    } else if (OB_FAIL(writer.init(
                   &fetcher, ctx_->replica_op_arg_.key_, ctx_->get_partition()->get_storage_file_handle()))) {
      LOG_WARN("failed to init writer", K(ret));
    } else if (OB_FAIL(writer.process(sub_task_->block_write_ctx_, sub_task_->lob_block_write_ctx_))) {
      LOG_WARN("failed to process writer", K(ret));
    } else {
      const int64_t data_checksum = writer.get_data_checksum();
      if (data_checksum != sstable_ctx_->data_checksum_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("logic migrate data checksum is not equal",
            K(ret),
            "dst data_checksum",
            data_checksum,
            "src data_checksum",
            sstable_ctx_->data_checksum_);
      }
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_MIGRATE_LOGIC_TASK) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_ERROR("fake logic migrate fail", K(ret));
    }
  }
#endif

  if (OB_FAIL(ret) && NULL != ctx_) {
    if (ctx_->is_need_retry(ret)) {
      ctx_->need_rebuild_ = true;
      LOG_INFO("migreate copy logic task need retry", K(ret), K(*ctx_));
    }
    ctx_->set_result_code(ret);
  }

  if (NULL != sstable_ctx_) {
    LOG_INFO("end ObMigrateCopyLogicTask process",
        "table_key",
        sstable_ctx_->table_key_,
        "dest_base_version",
        sstable_ctx_->dest_base_version_);
  }
  return ret;
}

ObMigrateFinishLogicTask::ObMigrateFinishLogicTask()
    : ObITask(TASK_TYPE_MIGRATE_FINISH_LOGIC), is_inited_(false), part_migrate_ctx_(NULL), sstable_ctx_()
{}

ObMigrateFinishLogicTask::~ObMigrateFinishLogicTask()
{}

int ObMigrateFinishLogicTask::init(ObPartitionMigrateCtx &part_migrate_ctx)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (!part_migrate_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(part_migrate_ctx));
  } else {
    is_inited_ = true;
    part_migrate_ctx_ = &part_migrate_ctx;
  }
  return ret;
}

int ObMigrateFinishLogicTask::process()
{
  int ret = OB_SUCCESS;
  storage::ObCreateSSTableParamWithTable create_sstable_param;
  ObSSTable *sstable = NULL;
  ObSchemaGetterGuard schema_guard;
  ObTableHandle handle;
  ObMigrateCtx *ctx = part_migrate_ctx_->ctx_;

  bool has_lob_column = false;
  const uint64_t table_id = sstable_ctx_.table_key_.table_id_;
  const uint64_t tenant_id = is_inner_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);
  const bool check_formal =
      extract_pure_id(table_id) > OB_MAX_CORE_TABLE_ID;  // Avoid the problem of circular dependency
  LOG_INFO("start ObMigrateFinishLogicTask process",
      "table_key",
      sstable_ctx_.table_key_,
      "dest_base_version",
      sstable_ctx_.dest_base_version_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(sstable_ctx_.get_dest_table_key(create_sstable_param.table_key_))) {
    LOG_WARN("failed to get dest table key", K(ret), K(sstable_ctx_));
  } else {
    create_sstable_param.schema_version_ = sstable_ctx_.meta_.schema_version_;
    create_sstable_param.logical_data_version_ = create_sstable_param.table_key_.trans_version_range_.snapshot_version_;

    if (OB_FAIL(MIGRATOR.get_schema_service()->get_tenant_schema_guard(
            tenant_id, schema_guard, sstable_ctx_.meta_.schema_version_, OB_INVALID_VERSION))) {
      LOG_WARN("failed to get schema guard", K(ret), K(sstable_ctx_.meta_));
    } else if (check_formal && OB_FAIL(schema_guard.check_formal_guard())) {
      LOG_WARN("schema_guard is not formal", K(ret), K(tenant_id));
    } else if (OB_FAIL(
                   schema_guard.get_table_schema(sstable_ctx_.table_key_.table_id_, create_sstable_param.schema_))) {
      LOG_WARN("Fail to get table schema, ", K(ret), K(sstable_ctx_.table_key_));
    } else if (OB_ISNULL(create_sstable_param.schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null table schema", K(ret), K(sstable_ctx_.table_key_));
    } else if (OB_FAIL(create_sstable_param.schema_->has_lob_column(has_lob_column, true))) {
      LOG_WARN("Failed to check lob column in table schema", K(ret));
    } else {
      ObPGCreateSSTableParam param;
      param.with_table_param_ = &create_sstable_param;
      for (int64_t task_idx = 0; OB_SUCC(ret) && task_idx < sstable_ctx_.task_count_; ++task_idx) {
        ObMigrateLogicSSTableCtx::SubLogicTask &sub_task = sstable_ctx_.tasks_[task_idx];
        if (!sub_task.block_write_ctx_.file_handle_.is_valid() &&
            OB_FAIL(sub_task.block_write_ctx_.file_handle_.assign(
                part_migrate_ctx_->ctx_->partition_guard_.get_partition_group()->get_storage_file_handle()))) {
          LOG_WARN("fail to assign file handle", K(ret));
        } else if (!sub_task.lob_block_write_ctx_.file_handle_.is_valid() &&
                   OB_FAIL(sub_task.lob_block_write_ctx_.file_handle_.assign(
                       part_migrate_ctx_->ctx_->partition_guard_.get_partition_group()->get_storage_file_handle()))) {
          LOG_WARN("fail to assign file handle", K(ret));
        } else if (OB_FAIL(param.data_blocks_.push_back(&sub_task.block_write_ctx_))) {
          LOG_WARN("fail to push back data block write ctx", K(ret));
        } else if (has_lob_column && sub_task.lob_block_write_ctx_.is_valid() &&
                   !sub_task.lob_block_write_ctx_.is_empty()) {
          if (OB_FAIL(param.lob_blocks_.push_back(&sub_task.lob_block_write_ctx_))) {
            LOG_WARN("fail to push back lob block write ctx", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObIPartitionGroup *pg = nullptr;
        if (OB_ISNULL(pg = part_migrate_ctx_->ctx_->partition_guard_.get_partition_group())) {
          ret = OB_ERR_SYS;
          LOG_WARN("partition must not null", K(ret));
        } else if (OB_FAIL(pg->create_sstable(param, handle))) {
          LOG_WARN("fail to create sstable", K(ret));
        } else if (OB_FAIL(handle.get_sstable(sstable))) {
          LOG_WARN("fail to get sstable", K(ret));
        } else if (OB_FAIL(part_migrate_ctx_->add_sstable(*sstable))) {
          LOG_WARN("failed to add sstable", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret) && NULL != ctx) {
    ctx->set_result_code(ret);
  }

  LOG_INFO("end ObMigrateFinishLogicTask process",
      "table_key",
      sstable_ctx_.table_key_,
      "dest_base_version",
      sstable_ctx_.dest_base_version_);
  return ret;
}

ObMigratePhysicalSSTableCtx::SubTask::SubTask()
    : block_info_(NULL), block_count_(0), pkey_(), allocator_(ObModIds::OB_PARTITION_MIGRATOR)
{}

ObMigratePhysicalSSTableCtx::SubTask::~SubTask()
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard pg_guard;
  blocksstable::ObStorageFile *pg_file = NULL;
  if (OB_FAIL(ObFileSystemUtil::get_pg_file_with_guard(pkey_, pg_guard, pg_file))) {
    LOG_ERROR("meta must not null", K(ret), K_(pkey));
  } else {
    if (NULL != block_info_ && block_count_ > 0) {
      for (int64_t i = 0; i < block_count_; ++i) {
        if (nullptr != block_info_[i].full_meta_.meta_) {
          block_info_[i].full_meta_.meta_->~ObMacroBlockMetaV2();
          block_info_[i].full_meta_.meta_ = nullptr;
        }
        if (nullptr != block_info_[i].full_meta_.schema_) {
          block_info_[i].full_meta_.schema_->~ObMacroBlockSchemaInfo();
          block_info_[i].full_meta_.schema_ = nullptr;
        }
        if (block_info_[i].macro_block_id_.is_valid()) {
          if (OB_FAIL(pg_file->dec_ref(block_info_[i].macro_block_id_))) {
            LOG_WARN("fail to dec ref, ", K(ret));
          }
          block_info_[i].need_copy_ = false;
          block_info_[i].macro_block_id_.reset();
        }
      }
    }
  }
  allocator_.clear();
}

ObMigratePhysicalSSTableCtx::ObMigratePhysicalSSTableCtx()
    : tasks_(NULL),
      task_count_(0),
      allocator_(ObModIds::OB_PARTITION_MIGRATOR),
      meta_(allocator_),
      src_info_(),
      sstable_info_(),
      is_leader_restore_(false),
      restore_version_(0)
{}

ObMigratePhysicalSSTableCtx::~ObMigratePhysicalSSTableCtx()
{
  reset();
}

bool ObMigratePhysicalSSTableCtx::is_valid()
{
  bool valid = true;
  if (NULL == tasks_ || task_count_ <= 0 || !meta_.is_valid() || (!is_leader_restore_ && !src_info_.is_valid()) ||
      !sstable_info_.is_valid()) {
    valid = false;
    LOG_WARN("ctx not valid",
        K(task_count_),
        KP(tasks_),
        K(meta_.is_valid()),
        K(src_info_.is_valid()),
        K(sstable_info_.is_valid()));
  } else {
    for (int64_t i = 0; valid && i < task_count_; ++i) {
      if (tasks_[i].block_count_ <= 0 || NULL == tasks_[i].block_info_) {
        valid = false;
        LOG_WARN("sub task not valid", K(i), K(task_count_), K(tasks_[i]));
      }
    }
  }
  return valid;
}

void ObMigratePhysicalSSTableCtx::reset()
{
  if (NULL != tasks_ && task_count_ > 0) {
    for (int64_t task_idx = 0; task_idx < task_count_; ++task_idx) {
      ObMigratePhysicalSSTableCtx::SubTask &task = tasks_[task_idx];
      task.~SubTask();
    }
  }
  tasks_ = NULL;
  task_count_ = 0;
  meta_.reset();
  allocator_.reset();
  src_info_.reset();
  sstable_info_.reset();
  is_leader_restore_ = false;
  restore_version_ = 0;
}

int ObMigratePhysicalSSTableCtx::get_dest_table_key(ObITable::TableKey &dest_table_key)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObDestTableKeyManager::convert_sstable_info_to_table_key(sstable_info_, dest_table_key))) {
    LOG_WARN("failed to conver sstable info to table key", K(ret), K(sstable_info_));
  } else if (ObITable::is_major_sstable(sstable_info_.src_table_key_.table_type_)) {
    dest_table_key = sstable_info_.src_table_key_;
    if (is_leader_restore_) {
      dest_table_key.version_ = restore_version_;
    }
  }

  return ret;
}

ObMigrateCopyPhysicalTask::ObMigrateCopyPhysicalTask()
    : ObITask(TASK_TYPE_MIGRATE_COPY_PHYSICAL),
      is_inited_(false),
      ctx_(NULL),
      task_idx_(-1),
      sstable_ctx_(NULL),
      sub_task_(NULL),
      cp_fty_(NULL)
{}

ObMigrateCopyPhysicalTask::~ObMigrateCopyPhysicalTask()
{}

int ObMigrateCopyPhysicalTask::init(const int64_t task_idx, ObMigratePhysicalSSTableCtx &sstable_ctx, ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret), K(is_inited_));
  } else if (task_idx < 0 || task_idx >= sstable_ctx.task_count_ || !sstable_ctx.is_valid() || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(task_idx), K(sstable_ctx.is_valid()), K(ctx.is_valid()), K(sstable_ctx), K(ctx));
  } else {
    is_inited_ = true;
    ctx_ = &ctx;
    task_idx_ = task_idx;
    sstable_ctx_ = &sstable_ctx;
    sub_task_ = sstable_ctx.tasks_ + task_idx_;
    cp_fty_ = MIGRATOR.get_cp_fty();
  }
  return ret;
}

int ObMigrateCopyPhysicalTask::get_macro_block_reader(obrpc::ObPartitionServiceRpcProxy *srv_rpc_proxy,
    common::ObInOutBandwidthThrottle *bandwidth_throttle, const ObMigrateSrcInfo &src_info,
    common::ObIArray<ObMigrateArgMacroBlockInfo> &list, ObITable::TableKey &table_key,
    const ObRestoreInfo *restore_info, ObIPartitionMacroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  UNUSED(restore_info);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (ctx_->replica_op_arg_.is_physical_restore_leader() && share::REPLICA_RESTORE_DATA == ctx_->is_restore_) {
    const int64_t compatible = ctx_->replica_op_arg_.phy_restore_arg_.restore_info_.compatible_;
    if (OB_BACKUP_COMPATIBLE_VERSION_V1 == compatible || OB_BACKUP_COMPATIBLE_VERSION_V2 == compatible) {
      if (OB_FAIL(get_macro_block_restore_reader_v1(
              *bandwidth_throttle, list, ctx_->replica_op_arg_.phy_restore_arg_, table_key, reader))) {
        STORAGE_LOG(WARN, "get_macro_block_restore_reader_v1 fail", K(ret), K(table_key));
      }
    } else if (OB_FAIL(get_macro_block_restore_reader_v2(
                   *bandwidth_throttle, list, ctx_->replica_op_arg_.phy_restore_arg_, table_key, reader))) {
      STORAGE_LOG(WARN, "get_macro_block_restore_reader_v2 fail", K(ret), K(table_key));
    }
  } else if (RESTORE_REPLICA_OP == ctx_->replica_op_arg_.type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("cannot support logical backup data anymore", K(ret), "ARG", ctx_->replica_op_arg_);
  } else {
    if (OB_FAIL(get_macro_block_ob_reader(*srv_rpc_proxy, *bandwidth_throttle, src_info, table_key, list, reader))) {
      STORAGE_LOG(WARN, "failed to get_macro_block_ob_reader", K(ret));
    }
  }
  return ret;
}

int ObMigrateCopyPhysicalTask::get_macro_block_restore_reader_v1(common::ObInOutBandwidthThrottle &bandwidth_throttle,
    common::ObIArray<ObMigrateArgMacroBlockInfo> &list, const share::ObPhysicalRestoreArg &restore_info,
    const ObITable::TableKey &table_key, ObIPartitionMacroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  ObPartitionMacroBlockRestoreReaderV1 *tmp_reader = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (ctx_->macro_indexs_->get_type() != ObIPhyRestoreMacroIndexStore::PHY_RESTORE_MACRO_INDEX_STORE_V1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invlaid macro index type", K(ret), "type", ctx_->macro_indexs_->get_type());
  } else if (OB_ISNULL(tmp_reader = cp_fty_->get_macro_block_restore_reader_v1())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to get macro block ob reader", K(ret));
  } else if (OB_FAIL(tmp_reader->init(bandwidth_throttle,
                 list,
                 restore_info,
                 *static_cast<const ObPhyRestoreMacroIndexStore *>(ctx_->macro_indexs_),
                 table_key))) {
    STORAGE_LOG(WARN, "failed to init restore reader", K(ret), K(table_key));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
  }

  if (NULL != cp_fty_) {
    if (OB_FAIL(ret)) {
      if (NULL != reader) {
        cp_fty_->free(reader);
        reader = NULL;
      }
    }
    if (NULL != tmp_reader) {
      cp_fty_->free(tmp_reader);
      tmp_reader = NULL;
    }
  }
  return ret;
}

int ObMigrateCopyPhysicalTask::get_macro_block_restore_reader_v2(common::ObInOutBandwidthThrottle &bandwidth_throttle,
    common::ObIArray<ObMigrateArgMacroBlockInfo> &list, const share::ObPhysicalRestoreArg &restore_info,
    const ObITable::TableKey &table_key, ObIPartitionMacroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  ObPartitionMacroBlockRestoreReaderV2 *tmp_reader = NULL;
  ObPhyRestoreMacroIndexStoreV2 *macro_index = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(tmp_reader = cp_fty_->get_macro_block_restore_reader_v2())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to get macro block ob reader", K(ret));
  } else if (OB_ISNULL(ctx_->macro_indexs_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "macro index should not be NULL", K(ret), KP(ctx_->macro_indexs_));
  } else if (ObIPhyRestoreMacroIndexStore::PHY_RESTORE_MACRO_INDEX_STORE_V2 != ctx_->macro_indexs_->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "macro index type is unexpected", K(ret), "type", ctx_->macro_indexs_->get_type());
  } else if (FALSE_IT(macro_index = reinterpret_cast<ObPhyRestoreMacroIndexStoreV2 *>(ctx_->macro_indexs_))) {
  } else if (OB_FAIL(tmp_reader->init(bandwidth_throttle, list, restore_info, *macro_index, table_key))) {
    STORAGE_LOG(WARN, "failed to init restore reader", K(ret), K(table_key));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
  }

  if (NULL != cp_fty_) {
    if (OB_FAIL(ret)) {
      if (NULL != reader) {
        cp_fty_->free(reader);
        reader = NULL;
      }
    }
    if (NULL != tmp_reader) {
      cp_fty_->free(tmp_reader);
      tmp_reader = NULL;
    }
  }
  return ret;
}

int ObMigrateCopyPhysicalTask::get_macro_block_ob_reader(obrpc::ObPartitionServiceRpcProxy &srv_rpc_proxy,
    common::ObInOutBandwidthThrottle &bandwidth_throttle, const ObMigrateSrcInfo &src_info,
    ObITable::TableKey &table_key, common::ObIArray<ObMigrateArgMacroBlockInfo> &list,
    ObIPartitionMacroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  ObPartitionMacroBlockObReader *tmp_reader = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!is_replica_op_valid(ctx_->replica_op_arg_.type_) || RESTORE_REPLICA_OP == ctx_->replica_op_arg_.type_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unexpected type", K(ret), K(*this), K(*ctx_));
  } else if (NULL == (tmp_reader = cp_fty_->get_macro_block_ob_reader())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to get macro block ob reader", K(ret));
  } else if (OB_FAIL(tmp_reader->init(
                 srv_rpc_proxy, bandwidth_throttle, src_info.src_addr_, table_key, list, src_info.cluster_id_))) {
    STORAGE_LOG(WARN, "failed to init ob reader", K(ret));
  }

  if (OB_SUCC(ret)) {
    reader = tmp_reader;
    tmp_reader = NULL;
  }

  if (NULL != cp_fty_) {
    if (OB_FAIL(ret)) {
      if (NULL != reader) {
        cp_fty_->free(reader);
        reader = NULL;
      }
    }
    if (NULL != tmp_reader) {
      cp_fty_->free(tmp_reader);
      tmp_reader = NULL;
    }
  }
  return ret;
}

int ObMigrateCopyPhysicalTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  const int64_t next_task_idx = task_idx_ + 1;
  ObMigrateCopyPhysicalTask *tmp_next_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (task_idx_ + 1 == sstable_ctx_->task_count_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(dag_->alloc_task(tmp_next_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(tmp_next_task->init(next_task_idx, *sstable_ctx_, *ctx_))) {
    LOG_WARN("failed o init next task", K(ret));
  } else {
    next_task = tmp_next_task;
  }
  return ret;
}

int ObMigrateCopyPhysicalTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  common::ObArray<ObMigrateArgMacroBlockInfo> list;
  ObMigrateArgMacroBlockInfo arg_info;
  ObMacroBlockInfoPair info;
  ObMacroBlocksWriteCtx copied_ctx;
  ObMajorMacroBlockKey key;
  int64_t copy_count = 0;
  int64_t reuse_count = 0;
  storage::ObIPartitionGroupGuard pg_guard;
  blocksstable::ObStorageFile *pg_file = NULL;
  DEBUG_SYNC(BEFORE_MIGRATE_COPY_BASE_DATA);

  if (NULL != sstable_ctx_) {
    LOG_INFO("start ObMigrateCopyPhysicalTask process", "table_key", sstable_ctx_->sstable_info_.src_table_key_);
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(ObFileSystemUtil::get_pg_file_with_guard(
                 sstable_ctx_->sstable_info_.src_table_key_.pkey_, pg_guard, pg_file))) {
    LOG_ERROR("fail to get pg file", K(ret), K(sstable_ctx_->sstable_info_.src_table_key_.pkey_));
  } else if (!copied_ctx.file_handle_.is_valid() &&
             OB_FAIL(copied_ctx.file_handle_.assign(pg_guard.get_partition_group()->get_storage_file_handle()))) {
    LOG_WARN("fail to assign file handle", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < sub_task_->block_count_; ++i) {
    arg_info.reset();
    info.reset();
    key.reset();

    if (!sub_task_->block_info_[i].need_copy_) {
      ++reuse_count;
    } else {
      key.table_id_ = sstable_ctx_->sstable_info_.src_table_key_.table_id_;
      key.partition_id_ = sstable_ctx_->sstable_info_.src_table_key_.pkey_.get_partition_id();
      key.data_version_ = sub_task_->block_info_[i].pair_.data_version_;
      key.data_seq_ = sub_task_->block_info_[i].pair_.data_seq_;
      if (sub_task_->block_info_[i].macro_block_id_.is_valid()) {
        ret = OB_ERR_SYS;
        LOG_ERROR(
            "need copy block info's macro block id must be 0", K(ret), K(i), "block_info", sub_task_->block_info_[i]);
      } else if (key.is_valid() && BACKUP_REPLICA_OP != ctx_->replica_op_arg_.type_ &&
                 OB_SUCCESS == ctx_->reuse_block_mgr_.get_reuse_macro_meta(key, info)) {
        sub_task_->block_info_[i].need_copy_ = false;
        sub_task_->block_info_[i].macro_block_id_ = info.block_id_;
        sub_task_->block_info_[i].full_meta_ = info.meta_;
        if (OB_FAIL(pg_file->inc_ref(info.block_id_))) {
          LOG_ERROR("fail to inc ref, ", K(ret), K(info.block_id_));
        } else {
          ObTaskController::get().allow_next_syslog();
          LOG_INFO("reuse local macro id", K(info.block_id_), K(key), "meta", info.meta_);
          ++reuse_count;
        }
        if (OB_FAIL(ret)) {
          sub_task_->block_info_[i].macro_block_id_.reset();
        }
        // todo ():backup macro reuse
      } else {
        const int64_t max_block_count_per_task = ObMigratePrepareTask::MAX_MACRO_BLOCK_COUNT_PER_TASK;
        arg_info.fetch_arg_.macro_block_index_ = max_block_count_per_task * task_idx_ + i;
        arg_info.fetch_arg_.data_version_ = sub_task_->block_info_[i].pair_.data_version_;
        arg_info.fetch_arg_.data_seq_ = sub_task_->block_info_[i].pair_.data_seq_;
        if (OB_FAIL(list.push_back(arg_info))) {
          LOG_WARN("failed to add list", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && list.count() > 0) {
    if (OB_FAIL(fetch_major_block_with_retry(list, copied_ctx))) {
      LOG_WARN("failed to fetch major block", K(ret), K(list));
    } else if (BACKUP_REPLICA_OP == ctx_->replica_op_arg_.type_) {
      copy_count += list.count();
    } else if (list.count() != copied_ctx.get_macro_block_count()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("list count not match",
          K(ret),
          K(list.count()),
          K(copied_ctx.get_macro_block_count()),
          K(list),
          K(copied_ctx));
    } else if (copied_ctx.macro_block_list_.count() != copied_ctx.macro_block_meta_list_.count()) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, macro block count do not match", K(ret), K(copied_ctx));
    } else {
      int64_t sub_task_idx = 0;
      bool found = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < copied_ctx.macro_block_list_.count(); ++i) {
        blocksstable::ObSSTablePair pair;
        pair.data_version_ = list.at(i).fetch_arg_.data_version_;
        pair.data_seq_ = list.at(i).fetch_arg_.data_seq_;
        found = false;

        while (OB_SUCC(ret) && !found && sub_task_idx < sub_task_->block_count_) {
          if (sub_task_->block_info_[sub_task_idx].pair_ == pair) {
            found = true;
            sub_task_->block_info_[sub_task_idx].macro_block_id_ = copied_ctx.macro_block_list_.at(i);
            if (OB_FAIL(copied_ctx.macro_block_meta_list_.at(i).deep_copy(
                    sub_task_->block_info_[sub_task_idx].full_meta_, sub_task_->allocator_))) {
              LOG_WARN("fail to deep copy macro block meta, ", K(ret));
            } else if (OB_FAIL(pg_file->inc_ref(copied_ctx.macro_block_list_.at(i)))) {
              sub_task_->block_info_[sub_task_idx].macro_block_id_.reset();
              LOG_WARN("fail to inc ref", K(ret));
            } else if (OB_FAIL(ctx_->add_major_block_id(copied_ctx.macro_block_list_.at(i)))) {
              LOG_WARN("fail to add major block info", K(ret));
            }
          }
          ++sub_task_idx;
          ++copy_count;
        }

        if (!found && OB_SUCC(ret)) {
          ret = OB_ERR_SYS;
          LOG_ERROR("no macro block task found", K(ret), K(i), K(pair), K(*sub_task_), K(copied_ctx));
        }
      }
    }
  }

  if (OB_SUCCESS != (tmp_ret = calc_migrate_data_statics(copy_count, reuse_count))) {
    LOG_WARN("failed to calc migrate data statics", K(ret));
  }
  if (OB_FAIL(ret) && NULL != ctx_) {
    if (ctx_->is_need_retry(ret)) {
      ctx_->need_rebuild_ = true;
      LOG_INFO("migreate copy physical task need retry", K(ret), K(*ctx_));
    }
    ctx_->set_result_code(ret);
  }

  if (NULL != sstable_ctx_) {
    LOG_INFO("end ObMigrateCopyPhysicalTask process", "table_key", sstable_ctx_->sstable_info_.src_table_key_);
  }
  return ret;
}

int ObMigrateCopyPhysicalTask::fetch_major_block_with_retry(
    ObIArray<ObMigrateArgMacroBlockInfo> &list, ObMacroBlocksWriteCtx &copied_ctx)
{
  int ret = OB_SUCCESS;
  int64_t retry_times = 0;
  obrpc::ObPartitionServiceRpcProxy *srv_rpc_proxy = MIGRATOR.get_svr_rpc_proxy();
  common::ObInOutBandwidthThrottle *bandwidth_throttle = MIGRATOR.get_bandwidth_throttle();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate copy physical task do not init", K(ret));
  } else if (OB_ISNULL(srv_rpc_proxy) || OB_ISNULL(bandwidth_throttle)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("srv_rpc_proxy or bandwidth_throttle must not null", K(ret), KP(srv_rpc_proxy), KP(bandwidth_throttle));
  } else if (0 == list.count()) {
    LOG_INFO("no macro block need fetch", K(list.count()));
  } else {
    while (retry_times < MAX_RETRY_TIMES) {
      if (retry_times > 0) {
        LOG_INFO("retry get major block", K(retry_times));
      }

      if (OB_FAIL(fetch_major_block(list, copied_ctx))) {
        STORAGE_LOG(WARN, "failed to fetch major block", K(ret), K(retry_times));
      }

      if (OB_SUCC(ret)) {
        break;
      }

      if (OB_FAIL(ret)) {
        copied_ctx.clear();
        retry_times++;
        usleep(OB_FETCH_MAJOR_BLOCK_RETRY_INTERVAL);
      }
    }
  }

  return ret;
}

int ObMigrateCopyPhysicalTask::alloc_migrate_writer(
    ObIPartitionMacroBlockReader *reader, ObIMigrateMacroBlockWriter *&writer)
{
  int ret = OB_SUCCESS;
  writer = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(reader));
  } else if (OB_ISNULL(sstable_ctx_) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("invalid inner state", K(ret), KP(sstable_ctx_), KP(ctx_));
  } else {
    void *buf = nullptr;
    const uint64_t tenant_id = sstable_ctx_->sstable_info_.src_table_key_.get_tenant_id();
    ObStorageFile *dest_pg_file = ctx_->get_partition()->get_pg_storage().get_storage_file();
    if (OB_ISNULL(buf = ob_malloc(sizeof(ObMigrateMacroBlockWriter), ObModIds::OB_PARTITION_MIGRATE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc migrate macro block writer", K(ret));
    } else {
      ObMigrateMacroBlockWriter *tmp_writer = new (buf) ObMigrateMacroBlockWriter;
      if (OB_FAIL(tmp_writer->init(reader, tenant_id, dest_pg_file))) {
        LOG_WARN("failed to init writer", K(ret), KP(reader), K(tenant_id), KP(dest_pg_file));
        tmp_writer->~ObMigrateMacroBlockWriter();
      } else {
        writer = tmp_writer;
      }
    }
    if (OB_FAIL(ret) && NULL != buf) {
      ob_free(buf);
    }
  }
  return ret;
}

void ObMigrateCopyPhysicalTask::free_migrate_writer(ObIMigrateMacroBlockWriter *&writer)
{
  if (NULL != writer) {
    writer->~ObIMigrateMacroBlockWriter();
    ob_free(writer);
    writer = nullptr;
  }
}

int ObMigrateCopyPhysicalTask::fetch_major_block(
    ObIArray<ObMigrateArgMacroBlockInfo> &list, ObMacroBlocksWriteCtx &copied_ctx)
{
  int ret = OB_SUCCESS;
  obrpc::ObPartitionServiceRpcProxy *srv_rpc_proxy = MIGRATOR.get_svr_rpc_proxy();
  common::ObInOutBandwidthThrottle *bandwidth_throttle = MIGRATOR.get_bandwidth_throttle();
  ObIMigrateMacroBlockWriter *writer = NULL;
  ObIPartitionMacroBlockReader *reader = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate copy physical task do not init", K(ret));
  } else if (OB_ISNULL(srv_rpc_proxy) || OB_ISNULL(bandwidth_throttle)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("srv_rpc_proxy or bandwidth_throttle must not null", K(ret), KP(srv_rpc_proxy), KP(bandwidth_throttle));
  } else if (0 == list.count()) {
    LOG_INFO("no macro block need fetch", K(list.count()));
  } else {
    LOG_INFO("init reader", K(list));
    if (OB_FAIL(get_macro_block_reader(srv_rpc_proxy,
            bandwidth_throttle,
            sstable_ctx_->src_info_,
            list,
            sstable_ctx_->sstable_info_.src_table_key_,
            &ctx_->restore_info_,
            reader))) {
      LOG_WARN("fail to get macro block reader", K(ret));
    } else if (OB_FAIL(alloc_migrate_writer(reader, writer))) {
      LOG_WARN("fail to alloc migrate macro block writer", K(ret), KP(reader));
    } else if (OB_FAIL(writer->process(copied_ctx))) {
      LOG_WARN("failed to process writer", K(ret));
    } else if (list.count() != copied_ctx.get_macro_block_count()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("list count not match",
          K(ret),
          K(list.count()),
          K(copied_ctx.get_macro_block_count()),
          K(list),
          K(copied_ctx));
    }

    if (NULL != reader) {
      cp_fty_->free(reader);
    }
    if (NULL != writer) {
      free_migrate_writer(writer);
    }
  }
  return ret;
}

int ObMigrateCopyPhysicalTask::calc_migrate_data_statics(const int64_t copy_count, const int64_t reuse_count)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate copy physic task do not init", K(ret));
  } else {
    const ObITable::TableKey &src_table_key = sstable_ctx_->sstable_info_.src_table_key_;
    ctx_->action_ = src_table_key.is_major_sstable() ? ObMigrateCtx::COPY_MAJOR : ObMigrateCtx::COPY_MINOR;
    ATOMIC_AAF(&ctx_->data_statics_.ready_macro_block_, sub_task_->block_count_);
    if (src_table_key.is_major_sstable()) {
      ATOMIC_AAF(&ctx_->data_statics_.major_count_, copy_count);
    } else if (src_table_key.is_mini_minor_sstable()) {
      ATOMIC_AAF(&ctx_->data_statics_.mini_minor_count_, copy_count);
    } else if (src_table_key.is_normal_minor_sstable()) {
      ATOMIC_AAF(&ctx_->data_statics_.normal_minor_count_, copy_count);
    }

    ATOMIC_AAF(&ctx_->data_statics_.reuse_count_, reuse_count);
    if (OB_SUCCESS != (tmp_ret = ctx_->update_partition_migration_status())) {
      LOG_WARN("failed to update_partition_migration_status", K(tmp_ret));
    }
  }
  return ret;
}

ObMigrateFinishPhysicalTask::ObMigrateFinishPhysicalTask()
    : ObITask(TASK_TYPE_MIGRATE_FINISH_PHYSICAL), is_inited_(false), part_migrate_ctx_(NULL), sstable_ctx_()
{}

ObMigrateFinishPhysicalTask::~ObMigrateFinishPhysicalTask()
{}

int ObMigrateFinishPhysicalTask::init(ObIPartitionMigrateCtx &part_migrate_ctx)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (!part_migrate_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(part_migrate_ctx));
  } else {
    is_inited_ = true;
    part_migrate_ctx_ = &part_migrate_ctx;
  }
  return ret;
}

int ObMigrateFinishPhysicalTask::process()
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = NULL;
  ObTableHandle handle;
  ObIPartitionGroup *partition = NULL;
  ObMigrateCtx *ctx = part_migrate_ctx_->ctx_;
  ObITable::TableKey dest_table_key;
  blocksstable::ObMacroBlocksWriteCtx write_ctx, lob_write_ctx;  // TODO(): fix it for ofs
  ObPGCreateSSTableParam param;
  ObTimeGuard tg("ObMigrateFinishPhysicalTask", 1000L * 1000L);

  LOG_INFO("start ObMigrateFinishPhysicalTask process", "table_key", sstable_ctx_.sstable_info_.src_table_key_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(sstable_ctx_.get_dest_table_key(dest_table_key))) {
    LOG_WARN("failed to get dest table key", K(ret), K(dest_table_key));
  } else if (!dest_table_key.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_WARN("table key is invalid", K(ret), K(dest_table_key));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, write_ctx.file_ctx_))) {
    LOG_WARN("Failed to init file ctx", K(ret));
  } else if (sstable_ctx_.meta_.lob_macro_block_count_ > 0) {
    if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, lob_write_ctx.file_ctx_))) {
      LOG_WARN("Failed to init lob file ctx", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (!write_ctx.file_handle_.is_valid() &&
        OB_FAIL(
            write_ctx.file_handle_.assign(ctx->partition_guard_.get_partition_group()->get_storage_file_handle()))) {
      LOG_WARN("fail to assign file handle", K(ret));
    } else if (!lob_write_ctx.file_handle_.is_valid() &&
               OB_FAIL(lob_write_ctx.file_handle_.assign(
                   ctx->partition_guard_.get_partition_group()->get_storage_file_handle()))) {
      LOG_WARN("fail to assign file handle", K(ret));
    }
    tg.click("init");
    for (int64_t task_idx = 0; OB_SUCC(ret) && task_idx < sstable_ctx_.task_count_; ++task_idx) {
      ObMigratePhysicalSSTableCtx::SubTask &sub_task = sstable_ctx_.tasks_[task_idx];
      for (int64_t i = 0; OB_SUCC(ret) && i < sub_task.block_count_; ++i) {
        const MacroBlockId macro_block_id = sub_task.block_info_[i].macro_block_id_;
        const ObFullMacroBlockMeta &macro_meta = sub_task.block_info_[i].full_meta_;
        if (sstable_ctx_.meta_.lob_macro_block_count_ > 0) {
          if (OB_UNLIKELY(!macro_meta.is_valid())) {
            ret = OB_ERR_SYS;
            LOG_WARN("Invalid macro meta", "macro_block_id", macro_block_id, K(macro_meta), K(ret));
          } else if (macro_meta.meta_->is_lob_data_block()) {
            if (OB_FAIL(lob_write_ctx.add_macro_block(macro_block_id, macro_meta))) {
              LOG_WARN("Failed to add lob macro block to write ctx", K(ret));
            }
          } else if (OB_FAIL(write_ctx.add_macro_block(macro_block_id, macro_meta))) {
            LOG_WARN("Failed to add macro block to write ctx", K(ret));
          }
        } else if (OB_FAIL(write_ctx.add_macro_block(macro_block_id, macro_meta))) {
          LOG_WARN("Failed to add macro block to write ctx", K(ret));
        }
      }
    }
    tg.click("for_loop");
    if (OB_SUCC(ret)) {
      if (OB_FAIL(param.data_blocks_.push_back(&write_ctx))) {
        LOG_WARN("fail to push back data block", K(ret));
      } else if (sstable_ctx_.meta_.lob_macro_block_count_ > 0) {
        if (OB_FAIL(param.lob_blocks_.push_back(&lob_write_ctx))) {
          LOG_WARN("fail to push back lob macro block", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    param.table_key_ = &dest_table_key;
    param.meta_ = &sstable_ctx_.meta_;
    bool is_check_in_advance = false;
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(partition = ctx->partition_guard_.get_partition_group())) {
      ret = OB_ERR_SYS;
      LOG_ERROR("partition must not null", K(ret));
    } else if (OB_SUCCESS != (tmp_ret = acquire_sstable(dest_table_key, handle))) {
      LOG_WARN("failed to acquire sstbale", K(ret), K(dest_table_key));
      is_check_in_advance = false;
    } else {
      is_check_in_advance = true;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(partition->create_sstable(param, handle))) {
      LOG_WARN("fail to create sstable", K(ret), K(param));
    } else if (OB_FAIL(check_sstable_meta(is_check_in_advance, handle))) {
      LOG_WARN("failed to check sstable meta",
          K(ret), K(sstable_ctx_.meta_), K(handle));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(handle.get_sstable(sstable))) {
      LOG_WARN("fail to get sstable", K(ret));
    } else if (OB_ISNULL(sstable)) {
      ret = OB_ERR_SYS;
      LOG_WARN("sstable should not be null", K(ret));
    }
    tg.click("create_sstable");

    if (OB_SUCC(ret)) {
      if (ObIPartitionMigrateCtx::RECOVERY_POINT_CTX == part_migrate_ctx_->get_type()) {
        if (OB_FAIL(part_migrate_ctx_->add_sstable(*sstable))) {
          LOG_WARN("failed to add sstable", K(ret));
        }
      } else {
        if (dest_table_key.is_trans_sstable()) {
          if (OB_FAIL(ctx->trans_table_handle_.set_table(sstable))) {
            LOG_WARN("failed to set trans table", K(ret), K(dest_table_key));
          }
        } else if (OB_FAIL(part_migrate_ctx_->add_sstable(*sstable))) {
          LOG_WARN("failed to add sstable", K(ret));
        }
      }
      tg.click("add_sstable");
    }
  }

  if (OB_FAIL(ret) && NULL != ctx) {
    ctx->set_result_code(ret);
  }
  tg.click("set_result_code");
  LOG_INFO("end ObMigrateFinishPhysicalTask process",
      "table_key",
      dest_table_key,
      "sub_task_cnt",
      sstable_ctx_.task_count_,
      K(tg));
  return ret;
}

int ObMigrateFinishPhysicalTask::acquire_sstable(const ObITable::TableKey &dest_table_key, ObTableHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  ObSSTable *sstable = NULL;
  const bool is_check_in_advance = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish physical task do not init", K(ret));
  } else if (!dest_table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("acquire sstable get invalid argument", K(ret), K(dest_table_key));
  } else if (OB_FAIL(ObPartitionService::get_instance().acquire_sstable(dest_table_key, handle))) {
    LOG_WARN("failed to acquire sstable", K(ret), K(dest_table_key));
  }
  return ret;
}

ObMigrateFinishTask::ObMigrateFinishTask() : ObITask(TASK_TYPE_MIGRATE_FINISH), is_inited_(false), ctx_(NULL)
{}

ObMigrateFinishTask::~ObMigrateFinishTask()
{}

int ObMigrateFinishTask::init(ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else {
    is_inited_ = true;
    ctx_ = &ctx;
  }
  return ret;
}

int ObMigrateFinishTask::process()
{
  int ret = OB_SUCCESS;
  bool is_change_replica_with_data = true;
  bool is_add_replica_during_restore = false;
  ObIPartitionGroup *partition = NULL;
  ObPGStorage *pg_storage = NULL;
  DEBUG_SYNC(BEFORE_FINISH_MIGRATE_TASK);

  if (NULL != ctx_) {
    LOG_INFO("start ObMigrateFinishTask process", "pkey", ctx_->replica_op_arg_.key_, K(ctx_->replica_op_arg_.type_));
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(BACKUP_REPLICA_OP == ctx_->replica_op_arg_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("migrate op type is unexpected", K(ctx_->replica_op_arg_));
  } else if (OB_ISNULL(partition = ctx_->partition_guard_.get_partition_group())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("partition must not null", K(ret));
  } else if (OB_ISNULL(pg_storage = &(partition->get_pg_storage()))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("storage must not null", K(ret), KP(pg_storage));
  } else if (ctx_->is_leader_restore_archive_data()) {
    LOG_INFO("no need check read for leader_restore_archive_data",
        "pkey",
        ctx_->replica_op_arg_.key_,
        "type",
        ctx_->replica_op_arg_.type_);
  } else if (OB_FAIL(ctx_->change_replica_with_data(is_change_replica_with_data))) {
    LOG_WARN("failed to check replica", K(ret), K(*ctx_));
  } else if (is_change_replica_with_data) {
    LOG_INFO("no need check read for change replica", K(*ctx_));
  } else if (OB_FAIL(ObMigrateUtil::merge_trans_table(*ctx_))) {
    LOG_WARN("failed to merge trans table", K(ret));
  } else if (OB_FAIL(create_pg_partition_if_need())) {
    LOG_WARN("Failed to create pg partition if need", K(ret));
  } else if (ADD_REPLICA_OP == ctx_->replica_op_arg_.type_ &&
             ((ctx_->is_restore_ > REPLICA_NOT_RESTORE && ctx_->is_restore_ < REPLICA_RESTORE_MAX) ||
                 REPLICA_RESTORE_STANDBY == ctx_->is_restore_)) {
    is_add_replica_during_restore = true;
    LOG_INFO("no need check read add replica during restore", K(ctx_->is_restore_), "pkey", ctx_->replica_op_arg_.key_);
  } else {
    if (ObReplicaTypeCheck::is_replica_with_ssstore(ctx_->replica_op_arg_.dst_.get_replica_type())) {
      if (OB_FAIL(check_pg_partition_ready_for_read())) {
        LOG_WARN("Failed to check partition ready for read", K(ret));
      } else if (OB_FAIL(check_pg_available_index_all_exist())) {
        if (ctx_->replica_op_arg_.is_physical_restore()) {
          ret = OB_SUCCESS;
          // TODO: fix restore schema, some index status should be set unavailable
        }
        LOG_WARN("failed to check_available_index_all_exist", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(enable_replay())) {
      LOG_WARN("failed to enable replay", K(ret), K(ctx_->replica_op_arg_));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t retry_times = 0;
    while (OB_SUCC(ret) && retry_times < ObMigrateUtil::OB_MIGRATE_ONLINE_RETRY_COUNT) {
      if (OB_FAIL(update_split_state())) {
        LOG_WARN("failed to update split state", K(ret));
      }
      retry_times++;
      if (OB_FAIL(ret) && retry_times < ObMigrateUtil::OB_MIGRATE_ONLINE_RETRY_COUNT) {
        ret = OB_SUCCESS;
        usleep(ObMigrateUtil::RETRY_TASK_SLEEP_INTERVAL_S);
      } else {
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (is_change_replica_with_data || is_add_replica_during_restore ||
        REBUILD_REPLICA_OP == ctx_->replica_op_arg_.type_ || ctx_->is_only_copy_sstable()) {
      LOG_INFO("no need to wait log sync", K(is_change_replica_with_data), "arg", ctx_->replica_op_arg_);
    } else if (OB_FAIL(wait_log_sync())) {
      LOG_WARN("failed to check is log sync", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_pg_partition_report_status())) {
      LOG_WARN("fail to update pg partition report status", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t cur_ts = ObTimeUtility::current_time();
    if (OB_FAIL(pg_storage->set_pg_migrate_status(OB_MIGRATE_STATUS_HOLD, cur_ts))) {
      LOG_WARN("failed to set migrate status to OB_MIGRATE_STATUS_HOLD", K(ret), K(*ctx_));
    } else {
      LOG_INFO("succeed to set migrate status to OB_MIGRATE_STATUS_HOLD", K(*ctx_));
    }
  }

  if (OB_SUCC(ret) && RESTORE_REPLICA_OP == ctx_->replica_op_arg_.type_ && REPLICA_RESTORE_DATA == ctx_->is_restore_) {
    if (OB_FAIL(partition->set_storage_info(ctx_->pg_meta_.storage_info_))) {
      STORAGE_LOG(WARN,
          "failed to set storage info",
          K(ret),
          "pg_key",
          ctx_->replica_op_arg_.key_,
          "storage_info",
          ctx_->pg_meta_.storage_info_);
    }
  }

  if (OB_SUCC(ret) &&
      ((RESTORE_REPLICA_OP == ctx_->replica_op_arg_.type_ &&
           (REPLICA_RESTORE_DATA == ctx_->is_restore_ || REPLICA_RESTORE_CUT_DATA == ctx_->is_restore_)) ||
          (RESTORE_STANDBY_OP == ctx_->replica_op_arg_.type_ && REPLICA_RESTORE_STANDBY == ctx_->is_restore_) ||
          RESTORE_FOLLOWER_REPLICA_OP == ctx_->replica_op_arg_.type_)) {
    if (ObReplicaTypeCheck::is_replica_with_memstore(partition->get_replica_type()) &&
        OB_FAIL(partition->get_pg_storage().restore_mem_trans_table())) {
      STORAGE_LOG(WARN, "failed to restore mem trans table", K(ret));
    }
  }

  if (OB_FAIL(ret) && NULL != ctx_) {
    if (OB_LOG_NOT_SYNC == ret) {
      if (ADD_REPLICA_OP == ctx_->replica_op_arg_.type_ || MIGRATE_REPLICA_OP == ctx_->replica_op_arg_.type_ ||
          FAST_MIGRATE_REPLICA_OP == ctx_->replica_op_arg_.type_ || REBUILD_REPLICA_OP == ctx_->replica_op_arg_.type_) {
        ctx_->need_rebuild_ = true;
      }
    }
    ctx_->set_result_code(ret);
  }

  if (NULL != ctx_) {
    LOG_INFO("end ObMigrateFinishTask process", "pkey", ctx_->replica_op_arg_.key_, K(ctx_->replica_op_arg_.type_));
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::AFTER_MIGRATE_FINISH_TASK) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake AFTER_MIGRATE_FINISH_TASK", K(ret));
    }
  }
#endif
  return ret;
}

int ObMigrateFinishTask::lock_pg_owner(common::ObMySQLTransaction &trans, ObIPartitionGroup &pg,
    const common::ObAddr &data_src_addr, const int64_t epoch_number)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pg.is_valid() || !data_src_addr.is_valid() || OB_INVALID_VERSION == epoch_number)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg), K(data_src_addr), K(epoch_number));
  } else {
    const ObPGKey &pg_key = pg.get_partition_key();
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, result)
    {
      sqlclient::ObMySQLResult *mysql_result = nullptr;
      char ip_str[32] = {0};
      const char *sql_template = " SELECT is_owner, epoch_num "
                                 " FROM __all_tenant_meta_table "
                                 " WHERE tenant_id=%lu AND table_id=%lu AND partition_id=%ld "
                                 " AND svr_ip='%s' AND svr_port=%d AND epoch_num=%ld and is_owner=%d"
                                 " FOR UPDATE; ";
      if (OB_UNLIKELY(!data_src_addr.ip_to_string(ip_str, sizeof(ip_str)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get ip string", K(ret), K(data_src_addr));
      } else if (OB_FAIL(sql.append_fmt(sql_template,
                     pg_key.get_tenant_id(),
                     pg_key.get_table_id(),
                     pg_key.get_partition_id(),
                     ip_str,
                     data_src_addr.get_port(),
                     epoch_number,
                     true /*is_owner*/))) {
        LOG_WARN("fail to generate sql string", K(ret), K(pg_key));
      } else if (OB_FAIL(trans.read(result, pg_key.get_tenant_id(), sql.ptr()))) {
        LOG_WARN("fail to execute read sql", K(ret), K(sql), K(pg_key));
      } else if (OB_ISNULL(mysql_result = result.get_result())) {
        // empty result, owner maybe changed or src server reboot
        ret = OB_EMPTY_RESULT;
        LOG_WARN("query get empty result", K(ret), K(data_src_addr), K(pg_key), K(epoch_number));
      }
    }
  }
  return ret;
}

int ObMigrateFinishTask::enable_replay()
{
  int ret = OB_SUCCESS;
  if (REBUILD_REPLICA_OP == ctx_->replica_op_arg_.type_) {
    if (OB_FAIL(enable_replay_for_rebuild())) {
      LOG_WARN("failed to enable_replay_for_rebuild", K(ret), "arg", ctx_->replica_op_arg_);
    }
  } else {
    if (ctx_->create_new_pg_) {
      if (OB_FAIL(ObMigrateUtil::enable_replay_with_new_partition(*ctx_))) {
        LOG_WARN("failed to enable replay with new partition", K(ret), KPC(ctx_));
      }
    } else if (ctx_->get_partition()->get_pg_storage().is_paused()) {
      if (OB_FAIL(ObMigrateUtil::enable_replay_with_old_partition(*ctx_))) {
        LOG_WARN("failed to enable replay with old partition", K(ret), KPC(ctx_));
      }
    }
  }
  return ret;
}

int ObMigrateFinishTask::create_pg_partition_if_need()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *pg = NULL;
  const bool in_slog_trans = false;
  const int64_t max_kept_major_version_number = 0;

  LOG_INFO("start create_pg_partition_if_need");

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(pg = ctx_->partition_guard_.get_partition_group())) {
    ret = OB_ERR_SYS;
    LOG_WARN("pg should not be NULL", K(ret), KP(pg));
  } else if (ctx_->is_copy_index()) {
    // no need to replace store map, just skip it.
    LOG_INFO("replica op tyoe is no need batch replace sstable, skip it");
  } else {
    const ObSavedStorageInfoV2 &saved_storage_info = ctx_->pg_meta_.storage_info_;
    if (OB_FAIL(pg->get_pg_storage().batch_replace_store_map(ctx_->part_ctx_array_,
            saved_storage_info.get_data_info().get_schema_version(),
            ctx_->is_restore_,
            ctx_->old_trans_table_seq_))) {
      LOG_WARN("failed to batch replace store map", K(ret));
    }
  }
  return ret;
}

int ObMigrateFinishTask::check_pg_partition_ready_for_read()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_partition_ready_for_read_in_remote())) {
    LOG_WARN("failed to check partition ready for read in remote", K(ret));
  } else if (OB_FAIL(check_partition_ready_for_read_out_remote())) {
    LOG_WARN("failed to check partition ready for read out remote", K(ret));
  }
  return ret;
}

int ObMigrateFinishTask::check_partition_ready_for_read_in_remote()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  ObPGPartition *pg_partition = NULL;
  ObIPartitionStorage *storage = NULL;
  ObPartitionArray pkeys;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(partition = ctx_->partition_guard_.get_partition_group())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("partition must not null", K(ret));
  } else if (OB_FAIL(partition->get_all_pg_partition_keys(pkeys, true /*include_trans_table*/))) {
    LOG_WARN("fail to get all pg partition keys", K(ret));
  } else if (pkeys.count() < ctx_->part_ctx_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not remove partition during migration", K(ret), K(pkeys.count()), K(ctx_->part_ctx_array_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->part_ctx_array_.count(); ++i) {
      ObPGPartitionGuard guard;
      storage = NULL;
      const ObMigratePartitionInfo &copy_info = ctx_->part_ctx_array_.at(i).copy_info_;
      const ObPartitionKey &pkey = copy_info.meta_.pkey_;
      if (pkey.is_trans_table()) {
        continue;
      } else if (OB_FAIL(partition->get_pg_partition(pkey, guard))) {
        LOG_ERROR("failed to get pg partition", K(ret), K(pkey));
      } else if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("pg partition should not be NULL", K(ret), KP(pg_partition));
      } else if (OB_ISNULL(storage = pg_partition->get_storage())) {
        ret = OB_ERR_SYS;
        LOG_ERROR("storage must not null", K(ret));
      } else if (OB_FAIL(check_partition_ready_for_read(copy_info, storage))) {
        LOG_WARN("failed to check partition ready for read", K(ret), K(copy_info));
      }
    }
  }
  return ret;
}

int ObMigrateFinishTask::check_partition_ready_for_read(
    const ObMigratePartitionInfo &copy_info, ObIPartitionStorage *storage)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  bool is_ready_for_read = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!copy_info.is_valid() || OB_ISNULL(storage)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check partition ready for read get invalid argument", K(ret), K(copy_info), KP(storage));
  } else {
    ObPartitionStore &store = static_cast<ObPartitionStorage *>(storage)->get_partition_store();
    for (int64_t i = 0; OB_SUCC(ret) && i < copy_info.table_infos_.count(); ++i) {
      const ObMigrateTableInfo &info = copy_info.table_infos_[i];
      handle.reset();
      if (OB_FAIL(store.get_effective_tables(info.table_id_, handle, is_ready_for_read))) {
        LOG_WARN("failed to check ready for read", K(ret), K(info.table_id_));
      } else if (info.ready_for_read_ && !is_ready_for_read) {
        const uint64_t tenant_id = extract_tenant_id(info.table_id_);
        share::schema::ObSchemaGetterGuard schema_guard;
        share::schema::ObMultiVersionSchemaService &schema_service =
            share::schema::ObMultiVersionSchemaService::get_instance();
        bool is_exist = true;
        if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
          LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
        } else if (OB_FAIL(schema_guard.check_table_exist(info.table_id_, is_exist))) {
          LOG_WARN("failed to check table exist", K(ret));
        } else if (!is_exist) {
          ret = OB_TABLE_IS_DELETED;
          LOG_WARN("table is not exist, can not check is ready for read", K(ret), K(info));
        } else {
          ret = OB_ERR_SYS;
          LOG_ERROR("src is ready for read, but dest is not, migrate failed", K(ret), K(info), K(handle));
        }
      }
    }
  }
  return ret;
}

int ObMigrateFinishTask::check_partition_ready_for_read_out_remote()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  ObPGPartition *pg_partition = NULL;
  ObIPartitionStorage *storage = NULL;
  ObTablesHandle handle;
  ObPartitionArray pkeys;
  ObHashSet<ObPartitionKey> remote_pkeys;
  int hash_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(partition = ctx_->partition_guard_.get_partition_group())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("partition must not null", K(ret));
  } else if (OB_FAIL(partition->get_pg_storage().get_all_pg_partition_keys_with_lock(
                 pkeys, true /*include_trans_table*/))) {
    LOG_WARN("fail to get all pg partition keys", K(ret));
  } else {
    const int64_t OB_MAX_BUCKET = std::max(pkeys.count(), ctx_->part_ctx_array_.count());
    if (0 == OB_MAX_BUCKET) {
      // do nothing
    } else if (pkeys.count() < ctx_->part_ctx_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not remove partition during migration", K(ret), K(pkeys.count()), K(ctx_->part_ctx_array_.count()));
    } else if (OB_FAIL(remote_pkeys.create(OB_MAX_BUCKET))) {
      LOG_WARN("failed to create remote pkeys set", K(ret), K(ctx_->part_ctx_array_.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->part_ctx_array_.count(); ++i) {
        const ObMigratePartitionInfo &copy_info = ctx_->part_ctx_array_.at(i).copy_info_;
        const ObPartitionKey &pkey = copy_info.meta_.pkey_;
        if (OB_FAIL(remote_pkeys.set_refactored(pkey))) {
          LOG_WARN("failed to set pkey into remote pkey set", K(ret), K(pkey));
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
        const ObPartitionKey &pkey = pkeys.at(i);
        hash_ret = remote_pkeys.exist_refactored(pkey);
        if (OB_HASH_EXIST == hash_ret) {
          // do nothing
        } else if (OB_HASH_NOT_EXIST == hash_ret) {
          ObPGPartitionGuard guard;
          storage = NULL;
          if (pkey.is_trans_table()) {
            continue;
          } else if (OB_FAIL(partition->get_pg_partition(pkey, guard))) {
            LOG_ERROR("failed to get pg partition", K(ret), K(pkey));
          } else if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
            ret = OB_ERR_SYS;
            LOG_ERROR("pg partition should not be NULL", K(ret), KP(pg_partition));
          } else if (OB_ISNULL(storage = pg_partition->get_storage())) {
            ret = OB_ERR_SYS;
            LOG_ERROR("storage must not null", K(ret));
          } else if (OB_FAIL(check_partition_ready_for_read(storage))) {
            LOG_WARN("failed to check partition out remote ready for read", K(ret), K(pkey));
          }
        } else {
          ret = hash_ret;
          LOG_WARN("failed to check pkey into set", K(ret), K(pkey));
        }
      }
    }
  }
  return ret;
}

int ObMigrateFinishTask::check_partition_ready_for_read(ObIPartitionStorage *storage)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  bool is_ready_for_read = false;
  ObArray<uint64_t> table_ids;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(storage)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check partition ready for read get invalid argument", K(ret), KP(storage));
  } else {
    ObPartitionStore &store = static_cast<ObPartitionStorage *>(storage)->get_partition_store();
    if (OB_FAIL(store.get_all_table_ids(table_ids))) {
      LOG_WARN("failed to get all table ids", K(ret), K(storage->get_partition_key()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
        const uint64_t table_id = table_ids.at(i);
        handle.reset();
        if (OB_FAIL(store.get_effective_tables(table_id, handle, is_ready_for_read))) {
          LOG_WARN("failed to check ready for read", K(ret), K(table_id));
        } else if (!is_ready_for_read) {
          ret = OB_VERSION_RANGE_NOT_CONTINUES;
          LOG_WARN("partition which not in remote has uncontinues tables",
              K(ret),
              K(table_id),
              K(handle),
              K(storage->get_partition_key()));
        }
      }
    }
  }
  return ret;
}

int ObMigrateFinishTask::check_pg_available_index_all_exist()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  ObPGPartition *pg_partition = NULL;
  ObPartitionStorage *storage = NULL;
  ObPartitionArray pkeys;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(partition = ctx_->partition_guard_.get_partition_group())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("partition must not null", K(ret));
  } else if (OB_FAIL(partition->get_all_pg_partition_keys(pkeys))) {
    LOG_WARN("failed to get all pg partition keys", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      ObPGPartitionGuard guard;
      storage = NULL;
      const ObPartitionKey &pkey = pkeys.at(i);
      if (OB_FAIL(partition->get_pg_partition(pkey, guard))) {
        LOG_ERROR("failed to get pg partition", K(ret), K(pkey));
      } else if (OB_ISNULL(pg_partition = guard.get_pg_partition())) {
        ret = OB_ERR_SYS;
        LOG_WARN("pg partition should not be NULL", K(ret), KP(pg_partition));
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage *>(pg_partition->get_storage()))) {
        ret = OB_ERR_SYS;
        LOG_WARN("fail to get partition storage", K(ret), KP(storage));
      } else if (OB_FAIL(check_available_index_all_exist(pkey, storage))) {
        LOG_WARN("fail to check available index all exist", K(ret));
      }
    }
  }
  return ret;
}

int ObMigrateFinishTask::check_available_index_all_exist(const ObPartitionKey &pkey, ObPartitionStorage *storage)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<ObIndexTableStat> index_stats;
  ObTableHandle handle;
  const uint64_t tenant_id = is_inner_table(pkey.get_table_id()) ? OB_SYS_TENANT_ID : pkey.get_tenant_id();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(storage)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check available index get invalid argument", K(ret), KP(storage));
  } else if (OB_FAIL(MIGRATOR.get_schema_service()->get_tenant_full_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_index_status(pkey.get_table_id(), false /*with global index*/, index_stats))) {
    LOG_WARN("fail to get index status", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_stats.count(); ++i) {
      if (is_available_index_status(index_stats.at(i).index_status_)) {
        if (OB_FAIL(storage->get_partition_store().get_last_major_sstable(index_stats.at(i).index_id_, handle))) {
          LOG_WARN("failed to get last major sstable for index", K(ret), "index_id", index_stats.at(i).index_id_);
        } else if (NULL == handle.get_table()) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("has no major sstable for index", K(ret), "index_id", index_stats.at(i).index_id_);
        }
      }
    }
    // TODO(): should rebuild migrate
  }
  return ret;
}

int ObMigrateFinishTask::update_pg_partition_report_status()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  const int64_t version = ctx_->pg_meta_.report_status_.data_version_ > 0
                              ? ctx_->pg_meta_.report_status_.data_version_
                              : ObPartitionScheduler::get_instance().get_frozen_version();
  const common::ObVersion report_version(version);
  bool need_report = false;
  bool check_finished = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(partition = ctx_->partition_guard_.get_partition_group())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("partition must not null", K(ret));
  } else if (OB_ISNULL(GCTX.ob_service_)) {
    // skip it
  } else if (OB_SUCCESS != (tmp_ret = partition->get_pg_storage().try_update_report_status(
                                *GCTX.ob_service_, report_version, check_finished, need_report))) {
    LOG_WARN("failed to try_update_report_status", K(ret), K(partition->get_partition_key()));
  }
  return ret;
}

int64_t ObMigratePhysicalSSTableCtx::SubTask::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV(KP(this), K_(block_count));
    J_COMMA();
    J_ARRAY_START();
    for (int64_t i = 0; i < block_count_; ++i) {
      ObMigrateMacroBlockInfo &info = block_info_[i];
      J_OBJ_START();
      J_KV(K(i), K(info));
      J_OBJ_END();
      J_COMMA();
    }
    J_ARRAY_END();
    J_OBJ_END();
  }
  return pos;
}

int64_t ObMigratePhysicalSSTableCtx::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV(KP(this), K_(src_info), K_(sstable_info), K_(task_count), KP(tasks_), K_(meta));
    J_COMMA();
    J_ARRAY_START();
    for (int64_t i = 0; i < task_count_; ++i) {
      SubTask &task = tasks_[i];
      J_OBJ_START();
      J_KV(K(i), K(task));
      J_OBJ_END();
      J_COMMA();
    }
    J_ARRAY_END();
    J_OBJ_END();
  }
  return pos;
}

int ObMigrateFinishTask::wait_log_sync(uint64_t max_clog_id /* = OB_INVALID_TIMESTAMP*/)
{
  int ret = OB_SUCCESS;
  bool is_log_sync = false;
  uint64_t last_confirmed_log_id = 0;
  bool is_cancel = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), "pkey", ctx_->replica_op_arg_.key_);
  } else if (0 == ctx_->wait_replay_start_ts_) {
    ctx_->wait_replay_start_ts_ = ObTimeUtility::current_time();
    LOG_INFO("start wait_log_sync", "pkey", ctx_->replica_op_arg_.key_);
  }

  while (OB_SUCC(ret) && !is_log_sync) {
    if (ctx_->group_task_->has_error()) {
      ret = OB_CANCELED;
      STORAGE_LOG(WARN, "group task has error, cancel subtask", K(ret));
    } else if (OB_FAIL(SYS_TASK_STATUS_MGR.is_task_cancel(get_dag()->get_dag_id(), is_cancel))) {
      STORAGE_LOG(ERROR, "failed to check is task canceled", K(ret), K(*this));
    } else if (is_cancel) {
      ret = OB_CANCELED;
      STORAGE_LOG(WARN, "task is cancelled", K(ret), K(*this));
    } else if (OB_FAIL(MIGRATOR.get_partition_service()->is_log_sync(
                   ctx_->replica_op_arg_.key_, is_log_sync, last_confirmed_log_id))) {
      LOG_WARN("failed to check is log sync", K(ret));
    } else if (OB_INVALID_TIMESTAMP != max_clog_id) {
      is_log_sync = last_confirmed_log_id >= max_clog_id;
    }
    if (OB_FAIL(ret)) {
    } else if (is_log_sync) {
      const int64_t cost_ts = ObTimeUtility::current_time() - ctx_->wait_replay_start_ts_;
      LOG_INFO(
          "log is sync, stop wait_log_sync", "pkey", ctx_->replica_op_arg_.key_, K(cost_ts), K(last_confirmed_log_id));
    } else {
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        LOG_INFO("log is not sync, retry next loop", "pkey", ctx_->replica_op_arg_.key_, K(last_confirmed_log_id));
      }
      usleep(OB_CHECK_LOG_SYNC_INTERVAL);
      // If a log is not played backup for more than 30 minutes, it will be timeout during in sync
      const int64_t CLOG_IN_SYNC_DELAY_TIMEOUT = 30 * 60 * 1000 * 1000;
      if ((ctx_->last_confirmed_log_id_ == last_confirmed_log_id) &&
          (OB_INVALID_TIMESTAMP != ctx_->last_confirmed_log_ts_)) {
        if (ObTimeUtility::current_time() - ctx_->last_confirmed_log_ts_ >= CLOG_IN_SYNC_DELAY_TIMEOUT) {
          ctx_->result_ = OB_TIMEOUT;
          ret = OB_TIMEOUT;
          STORAGE_LOG(
              WARN, "failed to check is partition log replay sync. timeout, stop migrate task", K(ret), K(*ctx_));
        }
      } else {
        ctx_->last_confirmed_log_id_ = last_confirmed_log_id;
        ctx_->last_confirmed_log_ts_ = ObTimeUtility::current_time();
      }
    }
  }

  if (OB_SUCC(ret) && !is_log_sync) {
    const int64_t cost_ts = ObTimeUtility::current_time() - ctx_->wait_replay_start_ts_;
    ret = OB_LOG_NOT_SYNC;
    LOG_WARN(
        "log is not sync", "pkey", ctx_->replica_op_arg_.key_, "pkey", ctx_->replica_op_arg_.key_, K(cost_ts), K(ret));
  }
  return ret;
}

int ObMigrateFinishTask::enable_replay_for_rebuild()
{
  int ret = OB_SUCCESS;

  LOG_INFO("start enable_replay_for_rebuild");

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), "pkey", ctx_->replica_op_arg_.key_);
  } else if (REBUILD_REPLICA_OP != ctx_->replica_op_arg_.type_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot enable_replay_for_rebuild for other type", K(ret), K(*ctx_));
  } else {
    ctx_->need_online_for_rebuild_ = false;
    if (OB_FAIL(ObMigrateUtil::enable_replay_with_old_partition(*ctx_))) {
      LOG_WARN("failed to enable_replay_with_old_partition", K(ret), K(*ctx_));
    }
  }

  return ret;
}

int ObMigrateFinishTask::update_split_state()
{
  int ret = OB_SUCCESS;
  ObPartitionGroupMeta &meta = ctx_->pg_meta_;
  ObBaseStorageInfo &clog_info = meta.storage_info_.get_clog_info();
  ObDataStorageInfo &data_info = meta.storage_info_.get_data_info();
  const ObPartitionSplitInfo &split_info = meta.split_info_;
  int64_t split_state = meta.saved_split_state_;
  // 221,222 may set spit_status = -1, and it has bug during upgrade to 224. Now set split_status = 1.
  if (-1 == split_state) {
    split_state = 1;
    LOG_WARN("split state was -1, should be rewritten to 1", K(meta.pg_key_));
  }
  ObIPartitionGroup *partition = NULL;

  if (OB_ISNULL(partition = ctx_->get_partition())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot use old partition", K(ret));
  } else if (need_copy_split_state(ctx_->replica_op_arg_.type_)) {
    if (split_info.get_src_partition() == meta.pg_key_) {
      const int64_t source_log_id = split_info.get_source_log_id();
      const int64_t last_replay_log_id =
          std::min(clog_info.get_last_replay_log_id(), data_info.get_last_replay_log_id());

      if (last_replay_log_id >= source_log_id && OB_INVALID_ID != source_log_id) {
        if (OB_FAIL(
                partition->shutdown(split_info.get_split_version(), source_log_id, split_info.get_schema_version()))) {
          LOG_WARN("failed to shutdown", K(ret), K(split_info));
        } else if (OB_FAIL(partition->push_reference_tables(
                       split_info.get_dest_partitions(), split_info.get_split_version()))) {
          LOG_WARN("failed to push reference tables", K(split_info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition->save_split_state(split_state))) {
        LOG_WARN("failed to save split info and state", K(ret), K(split_state));
      }
    }
  }

  return ret;
}

ObGroupMigrateDag::ObGroupMigrateDag()
    : ObIDag(ObIDag::DAG_TYPE_GROUP_MIGRATE, ObIDag::DAG_PRIO_GROUP_MIGRATE),
      is_inited_(false),
      group_task_(NULL),
      tenant_id_(0),
      partition_service_(NULL)
{}

ObGroupMigrateDag::~ObGroupMigrateDag()
{
  int tmp_ret = OB_SUCCESS;

  if (NULL != group_task_) {
    ObArray<ObReportPartMigrationTask> report_list;
    bool is_batch_mode = false;
    bool could_report = false;
    if (OB_SUCCESS != (tmp_ret = group_task_->fill_report_list(is_batch_mode, report_list))) {
      LOG_WARN("failed to fill report list", K(tmp_ret), K_(tenant_id));
    } else {
      could_report = true;
    }

    if (OB_SUCCESS != (tmp_ret = ObPartGroupMigrator::get_instance().remove_finish_task(group_task_))) {
      LOG_WARN("failed to remove finish task", K(tmp_ret), K_(tenant_id));
    }
    group_task_ = NULL;

    if (could_report) {
      if (OB_SUCCESS != (tmp_ret = report_result(is_batch_mode, report_list))) {
        STORAGE_LOG(WARN, "failed to report result", K(tmp_ret), K_(tenant_id), K(is_batch_mode));
      }
    }
  }
}

bool ObGroupMigrateDag::operator==(const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObGroupMigrateDag &other_dag = static_cast<const ObGroupMigrateDag &>(other);
    is_same = group_task_ == other_dag.group_task_;
  }
  return is_same;
}

int64_t ObGroupMigrateDag::hash() const
{
  int64_t ptr = reinterpret_cast<int64_t>(group_task_);
  return common::murmurhash(&ptr, sizeof(ptr), 0);
}

int ObGroupMigrateDag::init(ObPartGroupTask *group_task, storage::ObPartitionService *partition_service)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (OB_ISNULL(group_task) || OB_ISNULL(partition_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(group_task), KP(partition_service));
  } else if (OB_FAIL(set_dag_id(group_task->get_task_id()))) {
    LOG_WARN("failed to set dag id", K(ret));
  } else {
    tenant_id_ = group_task->get_tenant_id();
    group_task_ = group_task;
    partition_service_ = partition_service;
    is_inited_ = true;
  }
  return ret;
}

void ObGroupMigrateDag::clear()
{
  is_inited_ = false;
  group_task_ = NULL;
  tenant_id_ = 0;
}

int ObGroupMigrateDag::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (NULL == buf || NULL == group_task_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(group_task_), K(buf));
  } else if (group_task_->get_task_list().count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(*group_task_));
  } else {
    const ObReplicaOpArg &arg = group_task_->get_task_list().at(0).arg_;
    int n = snprintf(buf,
        buf_len,
        "group partition migration executor: group_task_id=%s partition_count=%ld partition_id=%ld "
        "key=%s, op_type=%s, src=%s, dest=%s,",
        to_cstring(group_task_->get_task_id()),
        group_task_->get_task_list().count(),
        arg.key_.get_partition_id(),
        to_cstring(arg.key_),
        arg.get_replica_op_type_str(),
        to_cstring(arg.data_src_.get_server()),
        to_cstring(arg.dst_.get_server()));
    if (n < 0 || n >= buf_len) {
      tmp_ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buf not enough, comment is truncated", K(tmp_ret), K(n));
    }
  }
  return ret;
}

ObGroupMigrateExecuteTask::ObGroupMigrateExecuteTask()
    : ObITask(TASK_TYPE_GROUP_MIGRATE), is_inited_(false), group_task_(NULL)
{}

ObGroupMigrateExecuteTask::~ObGroupMigrateExecuteTask()
{}

int ObGroupMigrateExecuteTask::init(ObPartGroupTask *group_task)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (OB_ISNULL(group_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(group_task));
  } else {
    is_inited_ = true;
    group_task_ = group_task;
  }
  return ret;
}

int ObGroupMigrateExecuteTask::process()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(group_task_->do_task())) {
    LOG_WARN("failed to do task", K(ret));
  }
  return ret;
}

int ObMigrateGetLeaderUtil::get_leader(
    const common::ObPartitionKey &pkey, ObMigrateSrcInfo &leader_info, const bool force_renew)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  leader_info.reset();
  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "pkey is invalid", K(ret), K(pkey));
  } else {
    share::ObIPartitionLocationCache *location_cache = NULL;
    if (NULL == (location_cache = ObPartitionService::get_instance().get_location_cache())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "location cache must not null", K(ret));
    } else if (OB_FAIL(location_cache->get_leader_by_election(pkey, leader_info.src_addr_, force_renew))) {
      STORAGE_LOG(WARN, "get leader address failed", K(ret), K(pkey));
    } else {
      leader_info.cluster_id_ = cluster_id;
    }
  }
  return ret;
}

int ObMigrateGetLeaderUtil::get_clog_parent(clog::ObIPartitionLogService &log_service, ObMigrateSrcInfo &parent_info)
{
  int ret = OB_SUCCESS;
  parent_info.reset();
  ObAddr parent_src;
  int64_t cluster_id = -1;

  if (OB_FAIL(log_service.get_clog_parent_for_migration(parent_src, cluster_id))) {
    STORAGE_LOG(WARN, "get parent addr failed", K(ret));
  } else {
    parent_info.src_addr_ = parent_src;
    parent_info.cluster_id_ = cluster_id;
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::schedule_migrate_tasks()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_schedule = false;
  ObArray<ObITask *> last_task_array;
  ObITask *last_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(need_generate_sstable_migrate_tasks(need_schedule))) {
    LOG_WARN("failed to check generate sstable migrate task", K(ret));
  } else if (!need_schedule) {
    LOG_INFO("no need_generate_sstable_migrate_tasks");
  } else if (REBUILD_REPLICA_OP == ctx_->replica_op_arg_.type_) {
    if (OB_FAIL(generate_pg_rebuild_tasks(last_task_array))) {
      LOG_WARN("failed to generate rebuild task", K(ret));
    }
  } else if (BACKUP_REPLICA_OP == ctx_->replica_op_arg_.type_) {
    if (OB_FAIL(generate_pg_backup_tasks(last_task_array))) {
      LOG_WARN("failed to generate backup task", K(ret));
    }
  } else if (VALIDATE_BACKUP_OP == ctx_->replica_op_arg_.type_) {
    if (OB_FAIL(generate_pg_validate_tasks(last_task_array))) {
      LOG_WARN("failed to generate validate task", K(ret));
    }
  } else if (BACKUP_BACKUPSET_OP == ctx_->replica_op_arg_.type_) {
    if (OB_FAIL(generate_pg_backup_backupset_tasks(last_task_array))) {
      LOG_WARN("failed to generate backup backupset task", K(ret));
    }
  } else if (BACKUP_ARCHIVELOG_OP == ctx_->replica_op_arg_.type_) {
    if (OB_FAIL(generate_pg_backup_archive_log_tasks(last_task_array))) {
      LOG_WARN("failed to generate backup backupset task", K(ret));
    }
  } else if (OB_FAIL(generate_pg_migrate_tasks(last_task_array))) {
    LOG_WARN("failed to generate_pg_migrate_tasks", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (BACKUP_REPLICA_OP == ctx_->replica_op_arg_.type_) {
      LOG_INFO("no need finish task for backup");
    } else if (VALIDATE_BACKUP_OP == ctx_->replica_op_arg_.type_) {
      LOG_INFO("no need finish task for validate");
    } else if (BACKUP_BACKUPSET_OP == ctx_->replica_op_arg_.type_) {
      LOG_INFO("no need finish task for backup backupset");
    } else if (BACKUP_ARCHIVELOG_OP == ctx_->replica_op_arg_.type_) {
      LOG_INFO("no need finish task for backup archivelog");
    } else if (OB_FAIL(generate_finish_migrate_task(last_task_array, last_task))) {
      LOG_WARN("failed to generate prepare migrate task", K(ret));
    } else if (RESTORE_REPLICA_OP == ctx_->replica_op_arg_.type_) {
      if (OB_FAIL(generate_restore_tailored_task(last_task))) {
        LOG_WARN("failed to generate restore tailored task", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (tmp_ret = ctx_->update_partition_migration_status())) {
      LOG_WARN("failed to update partition migration status", K(tmp_ret));
    }
  }

  // Whether success or failure, we need to clean up the reference count of macro block to prevent leakage
  if (NULL != ctx_) {
    ctx_->free_old_macro_block();
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::update_multi_version_start()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  ObPGStorage *pg_storage = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (OB_ISNULL(partition = ctx_->get_partition())) {
    ret = OB_ERR_SYS;
    LOG_WARN("can not update multi version when pg is null", K(ret), KP(partition));
  } else if (OB_ISNULL(pg_storage = &(partition->get_pg_storage()))) {
    ret = OB_ERR_SYS;
    LOG_WARN("pg storage should not be NULL", K(ret), KP(pg_storage));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->part_ctx_array_.count(); ++i) {
      const ObPartitionMigrateCtx &pctx = ctx_->part_ctx_array_.at(i);
      const ObMigratePartitionInfo &copy_info = ctx_->part_ctx_array_.at(i).copy_info_;
      const ObPartitionKey &pkey = copy_info.meta_.pkey_;
      if (pctx.is_partition_exist_) {
        if (OB_FAIL(pg_storage->update_multi_version_start(pkey, copy_info.meta_.multi_version_start_))) {
          LOG_WARN("fail to update multi version start", K(ret), K(pkey), K(copy_info));
        }
      }
    }
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::deal_with_new_partition()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_->group_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("group task should not be null here", K(ret));
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::generate_restore_tailored_task(share::ObITask *last_task)
{
  int ret = OB_SUCCESS;
  ObRestoreTailoredPrepareTask *prepare_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate post prepare task do not init", K(ret));
  } else if (OB_ISNULL(last_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate restore tailored task get invalid argument", K(ret), KP(last_task));
  } else if (RESTORE_REPLICA_OP != ctx_->replica_op_arg_.type_ && RESTORE_STANDBY_OP != ctx_->replica_op_arg_.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to generate restore tailored task", K(ret), K(*ctx_));
  } else if (OB_FAIL(dag_->alloc_task(prepare_task))) {
    LOG_WARN("failed to allock prepare task", K(ret));
  } else if (OB_FAIL(prepare_task->init())) {
    LOG_WARN("failed to init prepare task", K(ret));
  } else if (OB_FAIL(last_task->add_child(*prepare_task))) {
    LOG_WARN("failed to add prepare task to child task", K(ret));
  } else if (OB_FAIL(dag_->add_task(*prepare_task))) {
    LOG_WARN("failed to add prepare task to dag", K(ret));
  }
  return ret;
}

int ObMigrateUtil::wait_trans_table_merge_finish(ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;
  const int64_t WAIT_TIMEOUT = 300L * 1000L * 1000L;  // 5min
  const int64_t SLEEP_INTERVAL = 100L * 1000L;        // 100ms
  ObTransTableMergeDag fake_dag;
  if (OB_FAIL(fake_dag.init(ctx.pg_meta_.pg_key_))) {
    LOG_WARN("failed to init dag", K(ret), K(ctx.pg_meta_.pg_key_));
  } else if (OB_FAIL(ObDagScheduler::get_instance().cancel_dag(&fake_dag))) {
    LOG_WARN("failed to cancel dag", K(ret), K(fake_dag));
  } else {
    bool exist = true;
    const int64_t start_ts = ObTimeUtility::current_time();
    do {
      if (OB_FAIL(ObDagScheduler::get_instance().check_dag_exist(&fake_dag, exist))) {
        LOG_WARN("failed to check dag exist", K(ret), K(fake_dag));
      } else if (exist) {
        if (ObTimeUtility::current_time() - start_ts > WAIT_TIMEOUT) {
          ret = OB_WAIT_TRANS_TABLE_MERGE_TIMEOUT;
          LOG_WARN("wait trans table merge finish timeout", K(ret), K(fake_dag));
        } else {
          usleep(SLEEP_INTERVAL);
        }
      }
    } while (OB_SUCC(ret) && exist);
  }
  return ret;
}

ObRestoreTailoredPrepareTask::ObRestoreTailoredPrepareTask()
    : ObITask(TASK_TYPE_RESTORE_TAILORED_PREPARE), is_inited_(false), ctx_(NULL)
{}

ObRestoreTailoredPrepareTask::~ObRestoreTailoredPrepareTask()
{}

int ObRestoreTailoredPrepareTask::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("restore tailored prepare task init twice", K(ret));
  } else if (ObIDag::DAG_TYPE_MIGRATE != dag_->get_type()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), K(*dag_));
  } else {
    ctx_ = static_cast<ObMigrateDag *>(dag_)->get_ctx();
    is_inited_ = true;
  }
  return ret;
}

int ObRestoreTailoredPrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObRestoreTailoredFinishTask *finish_task = NULL;
  ObIPartitionGroup *partition_group = NULL;
  bool need_generate = false;
  bool cleared_memstore = false;
  bool has_memstore = false;
  int16_t restore_state = ObReplicaRestoreStatus::REPLICA_NOT_RESTORE;

  LOG_INFO("start ObRestoreTailoredPrepareTask process", "pg_key", ctx_->replica_op_arg_.key_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore tailored prepare task do not init", K(ret));
  } else if (OB_ISNULL(partition_group = ctx_->partition_guard_.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition should not be NULL", K(ret), KP(partition_group));
  } else if (FALSE_IT(restore_state = partition_group->get_pg_storage().get_restore_state())) {
  } else if (REPLICA_RESTORE_DATA != restore_state && REPLICA_RESTORE_CUT_DATA != restore_state &&
             REPLICA_RESTORE_ARCHIVE_DATA != restore_state && REPLICA_RESTORE_STANDBY_CUT != restore_state) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected restore state during tailored task", K(ret), K(restore_state));
  } else if (REPLICA_RESTORE_ARCHIVE_DATA == restore_state) {
    LOG_INFO("replica archive data not need cut data, skip", K(restore_state));
  } else if (FALSE_IT(has_memstore = partition_group->get_pg_storage().has_memstore())) {
  } else if (has_memstore) {
    ObTablesHandle tables_handle;
    if (OB_FAIL(partition_group->get_pg_storage().get_reference_memtables(tables_handle))) {
      LOG_WARN("failed to get memtables", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
        memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(tables_handle.get_table(i));
        if (OB_ISNULL(memtable)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("memtable should not be NULL", KP(memtable));
        } else if (memtable->not_empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("memtable should be empty", K(ret), "pg_key", ctx_->replica_op_arg_.key_);
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition_group->get_pg_storage().clear_non_reused_stores(
              ctx_->replica_op_arg_.key_, cleared_memstore))) {
        LOG_WARN("failed to clean non reuse stores", K(ret), K(*ctx_));
      } else if (!cleared_memstore) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("has memstore but do do cleared memstore", K(ret), K(has_memstore), K(cleared_memstore));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_need_generate_task(need_generate))) {
    LOG_WARN("failed to check need generate task", K(ret));
  } else if (!need_generate) {
    // do nothing
  } else if (OB_FAIL(dag_->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc finish task", K(ret));
  } else if (OB_FAIL(finish_task->init())) {
    LOG_WARN("failed to init finish task", K(ret));
  } else if (OB_FAIL(update_restore_flag_cut_data())) {
    LOG_WARN("failed to update restore flag cut data", K(ret));
  } else if (OB_FAIL(schedule_restore_tailored_task(*finish_task))) {
    LOG_WARN("failed to schedule restore tailored task", K(ret));
  } else if (OB_FAIL(dag_->add_task(*finish_task))) {
    LOG_WARN("failed to add finish task", K(ret));
  }

  if (OB_FAIL(ret) && NULL != ctx_) {
    ctx_->set_result_code(ret);
  }

  LOG_INFO("end ObRestoreTailoredPrepareTask process", "pg_key", ctx_->replica_op_arg_.key_);
  return ret;
}

int ObRestoreTailoredPrepareTask::schedule_restore_tailored_task(ObRestoreTailoredFinishTask &finish_task)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition_group = NULL;
  ObPartitionArray pkeys;
  const bool include_trans_table = false;
  const bool is_restore_point = false;
  const int64_t restore_schema_version = OB_INVALID_VERSION;
  const int64_t current_schema_version = OB_INVALID_VERSION;
  ObRecoveryPointSchemaFilter schema_filter;
  uint64_t tenant_id = OB_INVALID_ID;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore tailored prepare task do not init", K(ret));
  } else if (FALSE_IT(tenant_id = is_inner_table(ctx_->replica_op_arg_.key_.get_table_id())
                                      ? OB_SYS_TENANT_ID
                                      : ctx_->replica_op_arg_.key_.get_tenant_id())) {
  } else if (OB_ISNULL(partition_group = ctx_->partition_guard_.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition group should not be NULL", K(ret), KP(partition_group));
  } else if (OB_FAIL(partition_group->get_all_pg_partition_keys(pkeys, include_trans_table))) {
    LOG_WARN("failed to get all pg partition keys", K(ret), "pg key", ctx_->replica_op_arg_.key_);
  } else {
    ObSchemaGetterGuard schema_guard;
    int64_t schema_version = 0;
    if (OB_FAIL(MIGRATOR.get_schema_service()->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret), K(partition_group->get_partition_key()));
    } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, schema_version))) {
      LOG_WARN("failed to get table schema version", K(ret), K(partition_group->get_partition_key()));
    } else if (OB_FAIL(finish_task.set_schema_version(schema_version))) {
      LOG_WARN("failed to set schema version", K(ret), K(schema_version), K(partition_group->get_partition_key()));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_filter.init(tenant_id, is_restore_point, restore_schema_version, current_schema_version))) {
    LOG_WARN("failed to init schema filter", K(ret), K(tenant_id), K(restore_schema_version));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      const ObPartitionKey &pkey = pkeys.at(i);
      ObPGPartitionGuard pg_partition_guard;
      ObTablesHandle tables_handle;
      ObPartitionStorage *storage = NULL;
      ObArray<uint64_t> index_ids;
      bool is_exist = false;
      if (schema_filter.check_partition_exist(pkey, is_exist)) {
        LOG_WARN("failed to check partition exist", K(ret), K(pkey));
      } else if (!is_exist) {
        // do nothing
      } else if (OB_FAIL(partition_group->get_pg_partition(pkey, pg_partition_guard))) {
        STORAGE_LOG(WARN, "get pg partition error", K(ret), K(pkey));
      } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get pg partition error", K(ret), KP(pg_partition_guard.get_pg_partition()));
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage *>(
                               pg_partition_guard.get_pg_partition()->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition storage must not be NULL", K(ret));
      } else if (OB_FAIL(storage->get_partition_store().get_all_table_ids(index_ids))) {
        STORAGE_LOG(WARN, "failed to get all table ids", K(ret));
      } else if (OB_FAIL(filter_tailored_tables(index_ids, schema_filter))) {
        LOG_WARN("failed to filter tailored tables", K(ret));
      } else if (OB_FAIL(schedule_restore_table_tailored_task_(index_ids, pkey, *storage, finish_task))) {
        STORAGE_LOG(WARN, "failed to schedule restore table tailored task", K(ret));
      }
    }
  }
  return ret;
}

int ObRestoreTailoredPrepareTask::schedule_restore_table_tailored_task_(const common::ObIArray<uint64_t> &index_ids,
    const ObPartitionKey &partition_key, ObPartitionStorage &storage, ObRestoreTailoredFinishTask &finish_task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore tailored prepare task do not init", K(ret));
  } else {
    const ObPGKey &pg_key = ctx_->replica_op_arg_.key_;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_ids.count(); ++i) {
      const uint64_t index_id = index_ids.at(i);
      ObTablesHandle minor_tables_handle;
      ObTableHandle major_handle;
      ObRestoreTailoredTask *task = NULL;
      if (OB_FAIL(storage.get_partition_store().get_latest_minor_sstables(index_id, minor_tables_handle))) {
        LOG_WARN("failed to get latest minor sstables", K(ret), K(index_id));
      } else if (OB_FAIL(storage.get_partition_store().get_last_major_sstable(index_id, major_handle))) {
        LOG_WARN("failed to get lastest major sstable", K(ret), K(index_id));
      } else if (OB_FAIL(dag_->alloc_task(task))) {
        LOG_WARN("failed to alloc task", K(ret));
      } else if (OB_FAIL(task->init(index_id, minor_tables_handle, major_handle, pg_key, partition_key, finish_task))) {
        LOG_WARN("failed to init restore tailored prepare task", K(ret), K(partition_key));
      } else if (OB_FAIL(this->add_child(*task))) {
        LOG_WARN("failed to add child of task", K(ret));
      } else if (OB_FAIL(dag_->add_task(*task))) {
        LOG_WARN("failed to add task to dag", K(ret));
      } else if (OB_FAIL(task->add_child(finish_task))) {
        LOG_WARN("failed to add finish task", K(ret));
      }
    }
  }
  return ret;
}

int ObRestoreTailoredPrepareTask::filter_tailored_tables(
    common::ObIArray<uint64_t> &index_ids, ObRecoveryPointSchemaFilter &schema_filter)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tmp_index_ids;

  for (int64_t i = 0; OB_SUCC(ret) && i < index_ids.count(); ++i) {
    const uint64_t index_id = index_ids.at(i);
    bool is_exist = false;
    if (OB_FAIL(schema_filter.check_table_exist(index_id, is_exist))) {
      LOG_WARN("failed to check table exist", K(ret), K(index_id));
    } else if (!is_exist) {
      // do nothing
    } else if (OB_FAIL(tmp_index_ids.push_back(index_id))) {
      LOG_WARN("failed to push index id into array", K(ret), K(index_id));
    }
  }

  if (OB_SUCC(ret)) {
    index_ids.reset();
    if (OB_FAIL(index_ids.assign(tmp_index_ids))) {
      LOG_WARN("failed to assign index ids", K(ret));
    }
  }
  return ret;
}

int ObRestoreTailoredPrepareTask::update_restore_flag_cut_data()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition_group = NULL;
  int16_t restore_status = ObReplicaRestoreStatus::REPLICA_NOT_RESTORE;
  const int16_t flag = REPLICA_RESTORE_CUT_DATA;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore failored prepare task do not init", K(ret));
  } else if (OB_ISNULL((partition_group = ctx_->partition_guard_.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition group should not be NULL", K(ret), KP(partition_group));
  } else if (FALSE_IT(restore_status = partition_group->get_pg_storage().get_restore_state())) {
  } else if (RESTORE_STANDBY_OP == ctx_->replica_op_arg_.type_) {
    if (REPLICA_RESTORE_STANDBY_CUT != restore_status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("restore standby op get invlaid restore status", K(ret), K(*ctx_), K(restore_status));
    } else {
      // do nothing
    }
  } else if (REPLICA_RESTORE_CUT_DATA == restore_status) {
    // do nothing
  } else {
    const ObPGKey &pg_key = ctx_->replica_op_arg_.key_;
    if (OB_FAIL(MIGRATOR.get_partition_service()->set_restore_flag(pg_key, flag))) {
      LOG_WARN("failed to set restore flag", K(ret), K(pg_key));
    }
  }
  return ret;
}

int ObRestoreTailoredPrepareTask::check_need_generate_task(bool &need_generate)
{
  int ret = OB_SUCCESS;
  need_generate = false;
  ObIPartitionGroup *partition_group = NULL;
  ObTablesHandle tables_handle;
  int64_t max_upper_trans_version = 0;
  transaction::ObTransService *trans_service = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore failored prepare task do not init", K(ret));
  } else if (OB_ISNULL(partition_group = ctx_->partition_guard_.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition group should not be NULL", K(ret), KP(partition_group));
  } else if (OB_FAIL(partition_group->get_all_tables(tables_handle))) {
    LOG_WARN("failed to get all tables", K(ret), KP(partition_group));
  } else if (OB_ISNULL(trans_service = GCTX.par_ser_->get_trans_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans service should not be NULL", K(ret), KP(trans_service));
  } else if (RESTORE_STANDBY_OP == ctx_->replica_op_arg_.type_ &&
             REPLICA_RESTORE_STANDBY_CUT == partition_group->get_pg_storage().get_restore_status()) {
    need_generate = true;
    LOG_INFO("standby restore cut data");
  } else {
    const int64_t restore_snapshot_version =
        ctx_->replica_op_arg_.phy_restore_arg_.restore_info_.restore_snapshot_version_;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
      ObITable *table = tables_handle.get_table(i);
      ObSSTable *sstable = NULL;
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), KP(table));
      } else if (table->is_major_sstable() || table->is_trans_sstable()) {
        // do nothing
      } else if (OB_ISNULL(sstable = reinterpret_cast<ObSSTable *>(table))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable should not be NULL", K(ret), KP(sstable));
      } else {
        int64_t max_trans_version = sstable->get_upper_trans_version();
        if (INT64_MAX == sstable->get_upper_trans_version()) {
          bool is_all_rollback_trans = false;
          // get_all_latest_minor_sstables return all minor except complement
          // all normal minor sstable consist of data between [start_log_ts, end_log_ts]
          if (OB_FAIL(trans_service->get_max_trans_version_before_given_log_ts(
                  sstable->get_partition_key(), sstable->get_end_log_ts(), max_trans_version, is_all_rollback_trans))) {
            LOG_WARN("failed to get_max_trans_version_before_given_log_id", K(ret), KPC(sstable));
          } else if (0 == max_trans_version && !is_all_rollback_trans) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("max trans version should not be 0",
                K(sstable->get_partition_key()),
                "end_log_ts",
                sstable->get_end_log_ts());
          }
        }

        if (OB_SUCC(ret)) {
          max_upper_trans_version = std::max(max_trans_version, max_upper_trans_version);
        }
      }
    }

    if (OB_SUCC(ret)) {
      need_generate = restore_snapshot_version < max_upper_trans_version;
      // do need generate task
      if (!need_generate) {
        LOG_INFO("restore snapshot version is larger than max upper trans version, no need cut",
            K(restore_snapshot_version),
            K(max_upper_trans_version),
            K(tables_handle));
      }
    }
  }
  return ret;
}

ObRestoreTailoredTask::ObRestoreTailoredTask()
    : ObITask(TASK_TYPE_RESTORE_TAILORED_PROCESS),
      is_inited_(false),
      ctx_(NULL),
      index_id_(OB_INVALID_ID),
      minor_tables_handle_(),
      major_table_handle_(),
      pg_key_(),
      finish_task_(NULL)
{}

ObRestoreTailoredTask::~ObRestoreTailoredTask()
{}

int ObRestoreTailoredTask::init(const uint64_t index_id, const ObTablesHandle &minor_tables_handle,
    const ObTableHandle &major_table_handle, const ObPGKey &pg_key, const ObPartitionKey &partition_key,
    ObRestoreTailoredFinishTask &finish_task)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("restore tailored task has already inited", K(ret));
  } else if (OB_INVALID_ID == index_id || !pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init restore tailored task get invalid argument", K(ret), K(index_id), K(pg_key));
  } else if (OB_FAIL(minor_tables_handle_.add_tables(minor_tables_handle))) {
    LOG_WARN("failed to add table handle", K(ret), K(index_id));
  } else if (OB_FAIL(major_table_handle_.assign(major_table_handle))) {
    LOG_WARN("failed to add major tables handle", K(ret), K(index_id));
  } else {
    index_id_ = index_id;
    pg_key_ = pg_key;
    partition_key_ = partition_key;
    ctx_ = static_cast<ObMigrateDag *>(dag_)->get_ctx();
    finish_task_ = &finish_task;
    is_inited_ = true;
  }
  return ret;
}

int ObRestoreTailoredTask::process()
{
  int ret = OB_SUCCESS;
  ObITable::TableKey table_key;
  ObTailoredRowIterator row_iter;
  ObMigrateLogicRowWriter logic_row_writer;
  ObMacroBlocksWriteCtx block_write_ctx;
  ObMacroBlocksWriteCtx lob_block_write_ctx;
  const int64_t schema_version = OB_INVALID_VERSION;
  int64_t restore_snapshot = 0;

  LOG_INFO("start ObRestoreTailoredTask process", "table_id", index_id_, "partition_key", partition_key_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore tailored task do not init", K(ret));
  } else if (minor_tables_handle_.empty()) {
    // do nothing
    LOG_INFO("minor tables handle is empty, no need cut", K(partition_key_), K(index_id_));
  } else if (FALSE_IT(
                 restore_snapshot = ctx_->replica_op_arg_.phy_restore_arg_.restore_info_.restore_snapshot_version_)) {
  } else if (OB_FAIL(get_tailored_table_key(table_key))) {
    LOG_WARN("failed to get tailored table key", K(ret), K(index_id_), K(pg_key_));
  } else if (OB_FAIL(row_iter.init(
                 index_id_, pg_key_, schema_version, table_key, restore_snapshot, minor_tables_handle_))) {
    LOG_WARN("failed to init tailored row iter", K(ret), K(index_id_), K(pg_key_), K(partition_key_));
  } else if (OB_FAIL(logic_row_writer.init(&row_iter, pg_key_, ctx_->get_partition()->get_storage_file_handle()))) {
    LOG_WARN("failed to init logic row writer", K(ret), K(pg_key_), K(partition_key_));
  } else if (OB_FAIL(logic_row_writer.process(block_write_ctx, lob_block_write_ctx))) {
    LOG_WARN("failed to process writer", K(ret));
  } else if (OB_FAIL(generate_new_minor_sstable(table_key, block_write_ctx, lob_block_write_ctx))) {
    LOG_WARN("failed to generate new sstable", K(ret), K(table_key));
  } else if (OB_FAIL(generate_major_sstable())) {
    LOG_WARN("failed to generate major sstable", K(ret), K(partition_key_), K(index_id_));
  }

  if (OB_FAIL(ret) && NULL != ctx_) {
    ctx_->set_result_code(ret);
  }

  LOG_INFO("end ObRestoreTailoredTask process", "table_id", index_id_, "partition key", partition_key_);

  return ret;
}

int ObRestoreTailoredTask::get_tailored_table_key(ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore tailored task do not init", K(ret));
  } else {
    int64_t base_version = INT64_MAX;
    int64_t multi_version_start = 0;
    int64_t snapshot_version = 0;
    int64_t start_log_ts = INT64_MAX;
    int64_t end_log_ts = 0;
    int64_t max_log_ts = 0;
    int64_t pre_log_ts = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables_handle_.get_count(); ++i) {
      const ObITable *table = minor_tables_handle_.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), KP(table), K(index_id_), K(partition_key_), K(pg_key_));
      } else if (!table->is_minor_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table type is unexpected", K(ret), KPC(table));
      } else if (0 == i) {
        pre_log_ts = table->get_end_log_ts();
        multi_version_start = table->get_multi_version_start();
      } else if (pre_log_ts > table->get_start_log_ts()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("minor table is not continue", K(ret), K(*table));
      } else {
        if (table->get_base_version() != table->get_multi_version_start()) {
          multi_version_start = table->get_multi_version_start();
        }
        pre_log_ts = table->get_end_log_ts();
      }

      if (OB_SUCC(ret)) {
        base_version = std::min(base_version, table->get_base_version());
        start_log_ts = std::min(start_log_ts, table->get_start_log_ts());
        end_log_ts = std::max(end_log_ts, table->get_end_log_ts());
        max_log_ts = std::max(max_log_ts, table->get_max_log_ts());
        snapshot_version = std::max(snapshot_version, table->get_snapshot_version());
      }
    }

    if (OB_SUCC(ret)) {
      table_key.pkey_ = partition_key_;
      table_key.table_id_ = index_id_;
      table_key.table_type_ = ObITable::MULTI_VERSION_MINOR_SSTABLE;
      table_key.trans_version_range_.base_version_ = base_version;
      table_key.trans_version_range_.multi_version_start_ = multi_version_start;
      table_key.trans_version_range_.snapshot_version_ =
          std::min(ctx_->replica_op_arg_.phy_restore_arg_.restore_info_.restore_snapshot_version_, snapshot_version);
      table_key.version_.version_ = ctx_->replica_op_arg_.phy_restore_arg_.restore_data_version_ + 1;
      // table_key.upper_trans_version
      // table_key.max_merged_version
      // log ts
      table_key.log_ts_range_.start_log_ts_ = start_log_ts;
      table_key.log_ts_range_.end_log_ts_ = end_log_ts;
      table_key.log_ts_range_.max_log_ts_ = max_log_ts;
      if (!table_key.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tailored table key is invalid", K(ret), K(table_key));
      } else if (1 == minor_tables_handle_.get_count()) {
        const ObITable *table = minor_tables_handle_.get_table(0);
        if (table->get_key() == table_key) {
          // same table key not allowed because can not add in table mgr
          table_key.table_type_ = ObITable::MINI_MINOR_SSTABLE;
          if (!table_key.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tailored table key is invalid", K(ret), K(table_key));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else {
        LOG_INFO("new cut table key", K(table_key));
      }
    }
  }
  return ret;
}

int ObRestoreTailoredTask::generate_new_minor_sstable(const ObITable::TableKey &table_key,
    ObMacroBlocksWriteCtx &block_write_ctx, ObMacroBlocksWriteCtx &lob_block_write_ctx)
{
  int ret = OB_SUCCESS;
  storage::ObCreateSSTableParamWithTable create_sstable_param;
  ObSSTable *sstable = NULL;
  ObSchemaGetterGuard schema_guard;
  ObTableHandle handle;
  bool has_lob_column = false;
  const uint64_t table_id = index_id_;
  const uint64_t tenant_id = is_inner_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);
  const bool check_formal =
      extract_pure_id(table_id) > OB_MAX_CORE_TABLE_ID;  // Avoid the problem of circular dependency

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore tailored task do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate new sstable get invalid argument", K(ret), K(table_key));
  } else {
    create_sstable_param.table_key_ = table_key;
    create_sstable_param.logical_data_version_ = table_key.trans_version_range_.snapshot_version_;
    if (OB_FAIL(MIGRATOR.get_schema_service()->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret), K(tenant_id), K(table_key));
    } else if (check_formal && OB_FAIL(schema_guard.check_formal_guard())) {
      LOG_WARN("schema_guard is not formal", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(table_key.table_id_, create_sstable_param.schema_))) {
      LOG_WARN("Fail to get table schema, ", K(ret), K(partition_key_));
    } else if (OB_ISNULL(create_sstable_param.schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null table schema", K(ret), K(partition_key_));
    } else if (OB_FAIL(
                   schema_guard.get_table_schema_version(table_key.table_id_, create_sstable_param.schema_version_))) {
      LOG_WARN("failed to get table schema version", K(ret), K(table_key));
    } else if (OB_FAIL(create_sstable_param.schema_->has_lob_column(has_lob_column, true))) {
      LOG_WARN("Failed to check lob column in table schema", K(ret));
    } else {
      ObPGCreateSSTableParam param;
      param.with_table_param_ = &create_sstable_param;
      if (!block_write_ctx.file_handle_.is_valid() &&
          OB_FAIL(block_write_ctx.file_handle_.assign(
              ctx_->partition_guard_.get_partition_group()->get_storage_file_handle()))) {
        LOG_WARN("fail to assign file handle", K(ret));
      } else if (!lob_block_write_ctx.file_handle_.is_valid() &&
                 OB_FAIL(lob_block_write_ctx.file_handle_.assign(
                     ctx_->partition_guard_.get_partition_group()->get_storage_file_handle()))) {
        LOG_WARN("fail to assign file handle", K(ret));
      } else if (OB_FAIL(param.data_blocks_.push_back(&block_write_ctx))) {
        LOG_WARN("fail to push back data block write ctx", K(ret));
      } else if (has_lob_column && lob_block_write_ctx.is_valid() && !lob_block_write_ctx.is_empty()) {
        if (OB_FAIL(param.lob_blocks_.push_back(&lob_block_write_ctx))) {
          LOG_WARN("fail to push back lob block write ctx", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        ObIPartitionGroup *pg = nullptr;
        if (OB_ISNULL(pg = ctx_->partition_guard_.get_partition_group())) {
          ret = OB_ERR_SYS;
          LOG_WARN("partition must not null", K(ret));
        } else if (OB_FAIL(pg->create_sstable(param, handle))) {
          LOG_WARN("fail to create sstable", K(ret));
        } else if (OB_FAIL(handle.get_sstable(sstable))) {
          LOG_WARN("fail to get sstable", K(ret));
        } else if (OB_FAIL(finish_task_->add_sstable_handle(partition_key_, handle))) {
          LOG_WARN("failed to add sstable handle", K(ret));
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(finish_task_->set_schema_version(create_sstable_param.schema_version_))) {
            LOG_WARN("failed to set schema version", K(ret), K(create_sstable_param));
          }
        }
      }
    }
  }
  return ret;
}

int ObRestoreTailoredTask::generate_major_sstable()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore tailored task do not init", K(ret));
  } else if (OB_FAIL(finish_task_->add_sstable_handle(partition_key_, major_table_handle_))) {
    LOG_WARN("failed to add sstable handle", K(ret), K(partition_key_));
  }
  return ret;
}

ObRestoreTailoredFinishTask::ObRestoreTailoredFinishTask()
    : ObITask(TASK_TYPE_RESTORE_TAILORED_FINISH),
      is_inited_(false),
      ctx_(NULL),
      lock_(),
      part_ctx_array_(),
      schema_version_(OB_INVALID_VERSION)
{}

ObRestoreTailoredFinishTask::~ObRestoreTailoredFinishTask()
{}

int ObRestoreTailoredFinishTask::add_sstable_handle(const ObPartitionKey &pkey, ObTableHandle &handle)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore tailored finish task do not init", K(ret));
  } else if (!pkey.is_valid() || !handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add sstable handle get invalid argument", K(ret), K(handle));
  } else {
    common::SpinWLockGuard guard(lock_);
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_ctx_array_.count() && !found; ++i) {
      ObPartitionMigrateCtx &part_ctx = part_ctx_array_.at(i);
      if (part_ctx.copy_info_.meta_.pkey_ == pkey) {
        if (OB_FAIL(part_ctx.handle_.add_table(handle))) {
          LOG_WARN("failed to add table handle", K(ret), K(handle));
        } else {
          found = true;
        }
      }
    }

    if (OB_SUCC(ret) && !found) {
      ObPartitionMigrateCtx part_ctx;
      part_ctx.is_partition_exist_ = true;
      part_ctx.need_reuse_local_minor_ = false;
      part_ctx.ctx_ = ctx_;
      if (OB_FAIL(ctx_->partition_guard_.get_partition_group()->get_pg_storage().get_pg_partition_store_meta(
              pkey, part_ctx.copy_info_.meta_))) {
        LOG_WARN("failed to get pg partition store meta", K(ret), K(pkey));
      } else if (OB_FAIL(part_ctx.handle_.add_table(handle))) {
        LOG_WARN("failed to add table handle", K(ret), K(handle));
      } else if (OB_FAIL(part_ctx_array_.push_back(part_ctx))) {
        LOG_WARN("failed to push part ctx into array", K(ret));
      }
    }
  }
  return ret;
}

int ObRestoreTailoredFinishTask::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("restore tailored finish task init twice", K(ret));
  } else {
    ctx_ = static_cast<ObMigrateDag *>(dag_)->get_ctx();
    is_inited_ = true;
  }
  return ret;
}

int ObRestoreTailoredFinishTask::process()
{
  int ret = OB_SUCCESS;
  const int64_t schema_version = OB_INVALID_VERSION;
  ObIPartitionGroup *partition_group = NULL;
  ObSavedStorageInfoV2 save_info;
  ObPGKey pg_key;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore tailored finish task do not init", K(ret));
  } else {
    pg_key = ctx_->replica_op_arg_.key_;
    LOG_INFO("start ObRestoreTailoredFinishTask process", "pg key", pg_key);
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(partition_group = ctx_->partition_guard_.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition group should not be NULL", K(ret), KP(partition_group));
  } else {
    int64_t trans_table_seq = ctx_->partition_guard_.get_partition_group()->get_pg_storage().get_trans_table_seq();
    const int64_t restore_snapshot_version =
        ctx_->replica_op_arg_.phy_restore_arg_.restore_info_.restore_snapshot_version_;

    if (OB_FAIL(partition_group->get_pg_storage().batch_replace_store_map(
            part_ctx_array_, schema_version, ctx_->is_restore_, trans_table_seq))) {
      LOG_WARN("failed to batch replace store map", K(ret));
    } else if (OB_FAIL(partition_group->get_all_saved_info(save_info))) {
      LOG_WARN("failed to get all saved info", K(ret));
    } else {
      ObDataStorageInfo &storage_info = save_info.get_data_info();
      ObBaseStorageInfo &clog_info = save_info.get_clog_info();
      const int64_t publish_version = std::min(storage_info.get_publish_version(), restore_snapshot_version);
      storage_info.set_publish_version(publish_version);
      storage_info.set_schema_version(schema_version_);
      int16_t restore_flag = REPLICA_NOT_RESTORE;
      restore_flag = partition_group->get_pg_storage().get_restore_state();
      const int64_t clog_info_log_id = clog_info.get_last_replay_log_id();
      const int64_t data_info_log_id = storage_info.get_last_replay_log_id();

      if (REPLICA_RESTORE_CUT_DATA == restore_flag) {
        int64_t backup_snapshot_version = 0;
        bool is_snapshot_restore = false;
        if (OB_FAIL(ObBackupInfoMgr::get_instance().get_restore_backup_snapshot_version(
                pg_key.get_tenant_id(), backup_snapshot_version))) {
          LOG_WARN("failed to get backup snapshot version", K(ret));
        } else if (OB_FAIL(ObRestoreBackupInfoUtil::check_is_snapshot_restore(backup_snapshot_version,
                       ctx_->replica_op_arg_.phy_restore_arg_.restore_info_.restore_snapshot_version_,
                       ctx_->replica_op_arg_.phy_restore_arg_.restore_info_.cluster_version_,
                       is_snapshot_restore))) {
          LOG_WARN("failed to check is snapshot restore", K(ret), KPC(ctx_));
        } else if (is_snapshot_restore && clog_info_log_id < data_info_log_id) {
          clog_info.set_last_replay_log_id(data_info_log_id);
          FLOG_INFO("push clog info log id",
              "orginal clog info log id",
              clog_info_log_id,
              "data info log id",
              data_info_log_id);
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(partition_group->set_storage_info(save_info))) {
        LOG_WARN("failed to set storage info", K(ret), K(save_info));
      }
    }
  }

  if (OB_FAIL(ret) && NULL != ctx_) {
    ctx_->set_result_code(ret);
  }

  LOG_INFO("end ObRestoreTailoredFinishTask process", "pg key", pg_key);
  return ret;
}

int ObRestoreTailoredFinishTask::set_schema_version(const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore tailored finish task do not init", K(ret));
  } else if (schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set schema version get invalid argument", K(ret), K(schema_version));
  } else {
    schema_version_ = std::max(schema_version_, schema_version);
  }
  return ret;
}

ObMigrateTaskGeneratorTask::ObMigrateTaskGeneratorTask() : ObITableTaskGeneratorTask(), is_inited_(false)
{}

ObMigrateTaskGeneratorTask::~ObMigrateTaskGeneratorTask()
{}

int ObMigrateTaskGeneratorTask::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (ObIDag::DAG_TYPE_MIGRATE != dag_->get_type() && ObIDag::DAG_TYPE_BACKUP != dag_->get_type() &&
             ObIDag::DAG_TYPE_VALIDATE != dag_->get_type() && ObIDag::DAG_TYPE_BACKUP_BACKUPSET != dag_->get_type() &&
             ObIDag::DAG_TYPE_BACKUP_ARCHIVELOG != dag_->get_type()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), K(*dag_));
  } else {
    ctx_ = static_cast<ObMigrateDag *>(dag_)->get_ctx();
    is_inited_ = true;
    bandwidth_throttle_ = MIGRATOR.get_bandwidth_throttle();
    partition_service_ = MIGRATOR.get_partition_service();
    srv_rpc_proxy_ = MIGRATOR.get_svr_rpc_proxy();
    rpc_ = MIGRATOR.get_pts_rpc();
    cp_fty_ = MIGRATOR.get_cp_fty();
  }
  return ret;
}

int ObMigrateTaskGeneratorTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret));
  } else {
    if (BACKUP_REPLICA_OP == ctx_->replica_op_arg_.type_) {
      LOG_INFO("no need replay for BACKUP_REPLICA_OP", "arg", ctx_->replica_op_arg_);
    } else if (ctx_->create_new_pg_) {
      if (OB_FAIL(deal_with_new_partition())) {
        LOG_WARN("failed to enable_replay_with_new_partition", K(ret), "arg", ctx_->replica_op_arg_);
      }
    } else if (VALIDATE_BACKUP_OP == ctx_->replica_op_arg_.type_ ||
               BACKUP_BACKUPSET_OP == ctx_->replica_op_arg_.type_ ||
               BACKUP_ARCHIVELOG_OP == ctx_->replica_op_arg_.type_) {
      // do nothing
    } else {
      if (REBUILD_REPLICA_OP == ctx_->replica_op_arg_.type_) {
        if (OB_FAIL(deal_with_rebuild_partition())) {
          LOG_WARN("failed to offline_rebuild_partition", K(ret), "arg", ctx_->replica_op_arg_);
        }
      } else if (RESTORE_STANDBY_OP == ctx_->replica_op_arg_.type_) {
        if (OB_FAIL(deal_with_standby_restore_partition())) {
          LOG_WARN("failed to deal with standby restore partition", K(ret), "arg", ctx_->replica_op_arg_);
        }
      } else {
        if (OB_FAIL(deal_with_old_partition())) {
          LOG_WARN("failed to deal_with_old_partition", K(ret), "arg", ctx_->replica_op_arg_);
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_partition_integrity())) {
        LOG_WARN("failed to check partition intergrity", K(ret), "arg", ctx_->replica_op_arg_);
      } else if (OB_FAIL(schedule_migrate_tasks())) {
        LOG_WARN("failed to schedule migrate tasks", K(ret), KPC(ctx_));
      }
    }

    if (OB_FAIL(ret) && NULL != ctx_) {
      if (ctx_->is_need_retry(ret)) {
        ctx_->need_rebuild_ = true;
        LOG_INFO("migrate post prepare task need retry", K(ret), K(*ctx_));
      }
      ctx_->set_result_code(ret);
    }
    LOG_INFO("migrate post prepare task finish", K(ret));
  }
  return ret;
}

ObMigrateRecoveryPointTaskGeneratorTask::ObMigrateRecoveryPointTaskGeneratorTask()
    : ObITableTaskGeneratorTask(),
      is_inited_(false),
      recovery_point_meta_info_(),
      is_recovery_point_exist_(false),
      wait_recovery_point_task_(NULL)
{}

ObMigrateRecoveryPointTaskGeneratorTask::~ObMigrateRecoveryPointTaskGeneratorTask()
{}

int ObMigrateRecoveryPointTaskGeneratorTask::init(share::ObITask &wait_recovery_point_task)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (ObIDag::DAG_TYPE_MIGRATE != dag_->get_type() && ObIDag::DAG_TYPE_BACKUP != dag_->get_type() &&
             ObIDag::DAG_TYPE_VALIDATE != dag_->get_type()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), K(*dag_));
  } else if (FALSE_IT(ctx_ = static_cast<ObMigrateDag *>(dag_)->get_ctx())) {
  } else if (ADD_REPLICA_OP != ctx_->replica_op_arg_.type_ && MIGRATE_REPLICA_OP != ctx_->replica_op_arg_.type_ &&
             REBUILD_REPLICA_OP != ctx_->replica_op_arg_.type_ && CHANGE_REPLICA_OP != ctx_->replica_op_arg_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init migrate recovery point failed", K(ret));
  } else if (OB_FAIL(this->add_child(wait_recovery_point_task))) {
    LOG_WARN("failed to add recovery point finish task", K(ret));
  } else {
    ctx_ = static_cast<ObMigrateDag *>(dag_)->get_ctx();
    bandwidth_throttle_ = MIGRATOR.get_bandwidth_throttle();
    partition_service_ = MIGRATOR.get_partition_service();
    srv_rpc_proxy_ = MIGRATOR.get_svr_rpc_proxy();
    rpc_ = MIGRATOR.get_pts_rpc();
    cp_fty_ = MIGRATOR.get_cp_fty();
    wait_recovery_point_task_ = &wait_recovery_point_task;
    is_inited_ = true;
  }
  return ret;
}

int ObMigrateRecoveryPointTaskGeneratorTask::process()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_ISNULL(ctx_->get_partition())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx do not hold partition", K(ret));
  } else if (ctx_->recovery_point_ctx_.is_recovery_point_iter_finish()) {
    // do nothing
  } else if (OB_FAIL(build_migrate_recovery_point_task())) {
    LOG_WARN("failed to prepare migrate", K(ret));
  } else if (OB_FAIL(schedule_migrate_recovery_point_tasks())) {
    LOG_WARN("failed to schedule migrate recovery point task", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("finish migrate recovery point task generator task", K(*ctx_));
  }

  if (OB_FAIL(ret) && NULL != ctx_) {
    if (ctx_->is_need_retry(ret)) {
      ctx_->need_rebuild_ = true;
      LOG_INFO("migrate prepare task need retry", K(ret), K(*ctx_));
    }
    ctx_->set_result_code(ret);
  }

  LOG_INFO("end ObMigrateRecoveryPointTaskGeneratorTask process", K(ret));
  return ret;
}

int ObMigrateRecoveryPointTaskGeneratorTask::build_migrate_recovery_point_task()
{
  int ret = OB_SUCCESS;
  const bool is_write_lock = true;
  bool is_exist = false;
  ObRecoveryPointKey recovery_point_key;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!ctx_->is_valid()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "invalid migrate ctx", K(ret), K(*this), K(*ctx_));
  } else if (1 != ctx_->doing_task_cnt_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "doing task must be 1 during prepare action", K(ret), K(*this), K(*ctx_));
  } else if (REMOVE_REPLICA_OP == ctx_->replica_op_arg_.type_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "should not prepare migrate for remove replica", K(ret), K(ctx_->replica_op_arg_));
  } else if (!ctx_->recovery_point_ctx_.is_recovery_point_index_valid()) {
    ret = OB_ERR_SYS;
    LOG_WARN("recovery point index is invalid", K(ret), K(ctx_->recovery_point_ctx_));
  } else if (ctx_->recovery_point_ctx_.is_recovery_point_iter_finish()) {
    // do nothing
  } else if (OB_FAIL(ctx_->recovery_point_ctx_.get_current_recovery_point_info(recovery_point_key))) {
    LOG_WARN("failed to get current recovery point info", K(ret), K(ctx_->recovery_point_ctx_));
  } else {
    ObMigrateCtxGuard guard(is_write_lock, *ctx_);
    if (OB_FAIL(check_local_recovery_point_exist(recovery_point_key, is_exist))) {
      LOG_WARN("failed to check local recovery point exist", K(ret), K(recovery_point_key));
    } else if (is_exist) {
      is_recovery_point_exist_ = true;
      LOG_INFO("local recovery point has exist ,no need generate it", K(ret), K(recovery_point_key));
    } else if (OB_FAIL(build_migrate_recovery_point_info(recovery_point_key))) {
      LOG_WARN("failed to build migrate pg partition info", K(ret), K(*ctx_), K(ctx_->recovery_point_ctx_));
    }
  }

  if (OB_SUCC(ret)) {
    ctx_->need_rebuild_ = false;
    ObTaskController::get().allow_next_syslog();
    STORAGE_LOG(INFO,
        "finish build migrate recovery point task",
        "pkey",
        ctx_->replica_op_arg_.key_,
        K(*ctx_),
        K(ctx_->recovery_point_ctx_));
  } else if (NULL != ctx_) {
    ctx_->result_ = ret;
  }

  return ret;
}

int ObMigrateRecoveryPointTaskGeneratorTask::build_migrate_recovery_point_info(
    const ObRecoveryPointKey &recovery_point_key)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(get_recovery_point_meta_info(ctx_->migrate_src_info_, recovery_point_key))) {
    STORAGE_LOG(WARN, "failed to get recovery point meta info reader", K(ret), K(*ctx_));
  } else if (!recovery_point_meta_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("recovery point meta info is invalid", K(ret), K(recovery_point_meta_info_));
  } else if (OB_FAIL(inner_build_migrate_recovery_point_info(recovery_point_meta_info_))) {
    LOG_WARN("failed to do inner build migrate recovery point info", K(ret), K(recovery_point_meta_info_));
  }
  return ret;
}

int ObMigrateRecoveryPointTaskGeneratorTask::get_recovery_point_meta_info(
    const ObMigrateSrcInfo &src_info, const ObRecoveryPointKey &recovery_point_key)
{
  int ret = OB_SUCCESS;
  ObRecoveryPointMetaInfoReader reader;
  ObFetchPGRecoveryPointMetaInfoArg arg;
  ObRecoveryPointMetaInfo tmp_recovery_point_meta_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (!src_info.is_valid() || !recovery_point_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get recovery point meta info reader get invalid argument", K(ret), K(src_info), K(recovery_point_key));
  } else if (FALSE_IT(arg.pg_key_ = ctx_->replica_op_arg_.key_)) {
  } else if (OB_FAIL(arg.recovery_point_key_array_.push_back(recovery_point_key))) {
    LOG_WARN("failed to push recovery point key into array", K(ret), K(recovery_point_key));
  } else if (OB_FAIL(
                 reader.init(*srv_rpc_proxy_, *bandwidth_throttle_, src_info.src_addr_, arg, src_info.cluster_id_))) {
    LOG_WARN("failed to init recovery point meta info reader", K(ret), K(src_info));
  } else if (OB_FAIL(reader.fetch_recovery_point_meta_info(recovery_point_meta_info_))) {
    LOG_WARN("failed to fetch recovery point meta info", K(ret), K(arg));
  } else if (OB_ITER_END != reader.fetch_recovery_point_meta_info(tmp_recovery_point_meta_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("has more recovery point meta info", K(ret), K(tmp_recovery_point_meta_info), K(arg));
  }
  return ret;
}

int ObMigrateRecoveryPointTaskGeneratorTask::inner_build_migrate_recovery_point_info(
    const ObRecoveryPointMetaInfo &recovery_point_meta_info)
{
  int ret = OB_SUCCESS;
  int64_t cost_ts = ObTimeUtility::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!recovery_point_meta_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner build migrate recovery point info get invalid argument", K(ret), K(recovery_point_meta_info));
  } else if (OB_FAIL(build_migrate_recovery_point_table_info(recovery_point_meta_info))) {
    LOG_WARN("failed to build migrate recovery point table info", K(ret));
  }

  cost_ts = ObTimeUtility::current_time() - cost_ts;
  LOG_INFO("inner_build_migrate_recovery_point_info", K(cost_ts), K(recovery_point_meta_info));
  return ret;
}

int ObMigrateRecoveryPointTaskGeneratorTask::check_local_recovery_point_exist(
    const ObRecoveryPointKey &recovery_point_key, bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  is_exist = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (NULL == (partition = ctx_->partition_guard_.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local partition do not exist", K(ret), K(*ctx_));
  } else if (OB_FAIL(partition->get_pg_storage().get_recovery_data_mgr().check_recovery_point_exist(
                 recovery_point_key, is_exist))) {
    LOG_WARN("failed to check recovery point exist", K(ret), K(recovery_point_key));
  }
  return ret;
}

int ObMigrateRecoveryPointTaskGeneratorTask::build_migrate_recovery_point_table_info(
    const ObRecoveryPointMetaInfo &recovery_point_meta_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (!recovery_point_meta_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build migrate recovery point table info get invalid argument", K(ret), K(recovery_point_meta_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < recovery_point_meta_info.table_keys_.count(); ++i) {
      const ObITable::TableKey &table_key = recovery_point_meta_info.table_keys_.at(i);
      ObTableHandle table_handle;
      ObITable *table = NULL;
      if (!table_key.is_valid() || table_key.is_memtable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table key is unexpected", K(ret), K(table_key));
      } else if (OB_FAIL(partition_service_->acquire_sstable(table_key, table_handle))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("failed to get complete sstable by key", K(ret), K(table_key));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (NULL == (table = table_handle.get_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), KP(table));
      } else if (OB_FAIL(ctx_->recovery_point_ctx_.add_sstable(table_handle))) {
        LOG_WARN("failed to add table into tables handle", K(ret));
      }
    }
  }
  return ret;
}

int ObMigrateRecoveryPointTaskGeneratorTask::schedule_migrate_recovery_point_tasks()
{
  int ret = OB_SUCCESS;
  ObITask *last_task = NULL;
  ObMigrateRecoveryPointFinishTask *finish_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(finish_task->init(
                 is_recovery_point_exist_, recovery_point_meta_info_, *ctx_, *wait_recovery_point_task_))) {
    LOG_WARN("failed to init finish task", K(ret));
  } else if (is_recovery_point_exist_) {
    LOG_INFO("no need_generate_recovery point tasks");
  } else if (OB_FAIL(generate_migrate_recovery_point_tasks(last_task))) {
    LOG_WARN("failed to generate migrate recovery point tasks", K(ret));
  }

  if (OB_SUCC(ret)) {
    // generate finish task
    if (NULL != last_task) {
      if (OB_FAIL(last_task->add_child(*finish_task))) {
        LOG_WARN("failed to add child of last task", K(ret));
      }
    } else {
      if (OB_FAIL(add_child(*finish_task))) {
        LOG_WARN("failed to add child of prepare task", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(finish_task->add_child(*wait_recovery_point_task_))) {
      LOG_WARN("failed to add wait recovery point task", K(ret));
    } else if (OB_FAIL(dag_->add_task(*finish_task))) {
      LOG_WARN("failed to add finish task", K(ret));
    }
  }
  return ret;
}

int ObMigrateRecoveryPointTaskGeneratorTask::generate_migrate_recovery_point_tasks(ObITask *&last_task)
{
  int ret = OB_SUCCESS;
  last_task = NULL;
  ObFakeTask *wait_migrate_finish_task = NULL;
  ObITask *parent_task = this;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (!recovery_point_meta_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reocvery point meta info is invalid", K(ret), K(recovery_point_meta_info_));
  } else if (OB_FAIL(generate_wait_migrate_finish_task(wait_migrate_finish_task))) {
    LOG_WARN("failed to generate_wait_migrate_finish_task", K(ret));
  } else if (OB_ISNULL(wait_migrate_finish_task)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("wait_rebuild_finish_task must not null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < recovery_point_meta_info_.table_keys_.count(); ++i) {
      const ObITable::TableKey &table_key = recovery_point_meta_info_.table_keys_.at(i);
      ObFakeTask *wait_finish_task = NULL;
      ObMigrateTableInfo::SSTableInfo sstable_info;
      sstable_info.src_table_key_ = table_key;
      sstable_info.dest_base_version_ = table_key.trans_version_range_.base_version_;
      sstable_info.dest_log_ts_range_ = table_key.log_ts_range_;

      if (OB_FAIL(dag_->alloc_task(wait_finish_task))) {
        LOG_WARN("failed to alloc wait finish task", K(ret));
      } else if (OB_FAIL(generate_physical_sstable_copy_task(ctx_->migrate_src_info_,
                     ctx_->recovery_point_ctx_,
                     sstable_info,
                     parent_task,
                     wait_finish_task))) {
        LOG_WARN("fail to generate physic sstable copy task", K(ret), K(ctx_->migrate_src_info_), K(sstable_info));
      } else if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
        LOG_WARN("failed to add wait finish task", K(ret));
      } else {
        parent_task = wait_finish_task;
        LOG_INFO("succeed to generate sstable copy task", K(ctx_->migrate_src_info_), K(sstable_info));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(parent_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parent task should not be NULL", K(ret), KP(parent_task));
    } else if (OB_FAIL(parent_task->add_child(*wait_migrate_finish_task))) {
      LOG_WARN("failed to add wait migrate finish task ", K(ret));
    } else {
      last_task = wait_migrate_finish_task;
    }
  }
  return ret;
}

ObMigrateRecoveryPointFinishTask::ObMigrateRecoveryPointFinishTask()
    : ObITask(TASK_TYPE_MIGRATE_FINISH),
      is_inited_(false),
      is_recovery_point_exist_(false),
      recovery_point_meta_info_(),
      ctx_(NULL),
      wait_recovery_point_finish_task_(NULL)
{}

ObMigrateRecoveryPointFinishTask::~ObMigrateRecoveryPointFinishTask()
{}

int ObMigrateRecoveryPointFinishTask::init(const bool is_recovery_point_exist,
    const ObRecoveryPointMetaInfo &recovery_point_meta_info, ObMigrateCtx &ctx,
    share::ObITask &wait_recovery_point_finish_task)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (!is_recovery_point_exist && !recovery_point_meta_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init migrate recovery point finish task failed",
        K(ret),
        K(is_recovery_point_exist),
        K(recovery_point_meta_info));
  } else if (!is_recovery_point_exist && OB_FAIL(recovery_point_meta_info_.assign(recovery_point_meta_info))) {
    LOG_WARN("failed to copy recovery point meta info", K(ret), K(recovery_point_meta_info));
  } else {
    is_recovery_point_exist_ = is_recovery_point_exist;
    ctx_ = &ctx;
    wait_recovery_point_finish_task_ = &wait_recovery_point_finish_task;
    is_inited_ = true;
  }
  return ret;
}

int ObMigrateRecoveryPointFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObRecoveryPointKey recovery_point_key;

  if (NULL != ctx_) {
    if (ctx_->recovery_point_ctx_.is_recovery_point_iter_finish()) {
      LOG_INFO("start ObMigrateRecoveryPointFinishTask process",
          "recovery_point index",
          ctx_->recovery_point_ctx_,
          K(ctx_->replica_op_arg_.type_));
    } else if (OB_FAIL(ctx_->recovery_point_ctx_.get_current_recovery_point_info(recovery_point_key))) {
      LOG_WARN("failed to get recovery point info", K(ret));
    } else {
      LOG_INFO("start ObMigrateRecoveryPointFinishTask process",
          "recovery_point index",
          ctx_->recovery_point_ctx_,
          "recovery point key",
          recovery_point_key,
          K(ctx_->replica_op_arg_.type_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (ctx_->recovery_point_ctx_.is_recovery_point_iter_finish()) {
    // do nothing
  } else if (OB_FAIL(create_recovery_point())) {
    LOG_WARN("failed to create recovery point", K(ret));
  } else if (OB_FAIL(generate_next_task())) {
    LOG_WARN("failed to generate next task", K(ret));
  }

  if (OB_FAIL(ret) && NULL != ctx_) {
    ctx_->need_rebuild_ = true;
    ctx_->set_result_code(ret);
  }
  if (NULL != ctx_) {
    LOG_INFO("end ObMigrateRecoveryPointFinishTask process",
        "pkey",
        ctx_->replica_op_arg_.key_,
        K(ctx_->replica_op_arg_.type_));
  }
  return ret;
}

int ObMigrateRecoveryPointFinishTask::create_recovery_point()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition = NULL;
  ObRecoveryPointKey recovery_point_key;
  ObTablesHandle tables_handle;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_recovery_point_exist_) {
    // do nothing
  } else if (!recovery_point_meta_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("recovery point meta info should be valid", K(ret), K(recovery_point_meta_info_));
  } else if (OB_FAIL(ctx_->recovery_point_ctx_.get_current_recovery_point_info(recovery_point_key))) {
    LOG_WARN("failed to get recovery point info", K(ret));
  } else if (NULL == (partition = ctx_->partition_guard_.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local partition do not exist", K(ret), K(*ctx_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < recovery_point_meta_info_.table_keys_.count(); ++i) {
      const ObITable::TableKey &table_key = recovery_point_meta_info_.table_keys_.at(i);
      ObTableHandle table_handle;
      if (OB_FAIL(partition->acquire_sstable(table_key, table_handle))) {
        LOG_WARN("failed to acquire sstable", K(ret), K(table_key));
      } else if (OB_FAIL(tables_handle.add_table(table_handle))) {
        LOG_WARN("failed ot add table handle into tables handle", K(ret), K(table_handle));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition->get_pg_storage().get_recovery_data_mgr().add_recovery_point(
              recovery_point_key, recovery_point_meta_info_, tables_handle))) {
        if (OB_ENTRY_EXIST == ret) {
          LOG_INFO("recovery point has already exist", K(recovery_point_key));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to add recovery point info",
              K(ret),
              K(recovery_point_key),
              K(recovery_point_meta_info_),
              K(tables_handle));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ctx_->recovery_point_ctx_.reuse_tables_handle();
    }
  }
  return ret;
}

int ObMigrateRecoveryPointFinishTask::generate_next_task()
{
  int ret = OB_SUCCESS;
  ObITask *task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(inner_generate_next_task(task))) {
    LOG_WARN("failed to do inner generate next task", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task should not be NULL", K(ret), KP(task));
  } else if (OB_FAIL(dag_->add_task(*task))) {
    LOG_WARN("failed to add task into dag", K(ret), K(task));
  }
  return ret;
}

int ObMigrateRecoveryPointFinishTask::inner_generate_next_task(ObITask *&task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  ObMigrateRecoveryPointTaskGeneratorTask *recovert_point_task_generator_task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    // has next recovery point
    if (OB_FAIL(dag_->alloc_task(recovert_point_task_generator_task))) {
      LOG_WARN("failed to alloc copy task", K(ret));
    } else if (OB_FAIL(recovert_point_task_generator_task->init(*wait_recovery_point_finish_task_))) {
      LOG_WARN("failed to init copy task", K(ret));
    } else if (OB_FAIL(add_child(*recovert_point_task_generator_task))) {
      LOG_WARN("failed to add child copy task", K(ret));
    } else {
      task = recovert_point_task_generator_task;
      ctx_->recovery_point_ctx_.inc_recovery_point_index();
    }
  }
  return ret;
}

ObMigrateTransTableTaskGeneratorTask::ObMigrateTransTableTaskGeneratorTask()
    : ObITableTaskGeneratorTask(), is_inited_(false), wait_finish_task_(NULL)
{}

ObMigrateTransTableTaskGeneratorTask::~ObMigrateTransTableTaskGeneratorTask()
{}

int ObMigrateTransTableTaskGeneratorTask::init(share::ObITask &wait_finish_task)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (ObIDag::DAG_TYPE_MIGRATE != dag_->get_type() && ObIDag::DAG_TYPE_BACKUP != dag_->get_type() &&
             ObIDag::DAG_TYPE_VALIDATE != dag_->get_type() && ObIDag::DAG_TYPE_BACKUP_BACKUPSET != dag_->get_type() &&
             ObIDag::DAG_TYPE_BACKUP_ARCHIVELOG != dag_->get_type()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), K(*dag_));
  } else {
    ctx_ = static_cast<ObMigrateDag *>(dag_)->get_ctx();
    bandwidth_throttle_ = MIGRATOR.get_bandwidth_throttle();
    partition_service_ = MIGRATOR.get_partition_service();
    srv_rpc_proxy_ = MIGRATOR.get_svr_rpc_proxy();
    rpc_ = MIGRATOR.get_pts_rpc();
    cp_fty_ = MIGRATOR.get_cp_fty();
    wait_finish_task_ = &wait_finish_task;
    is_inited_ = true;
  }
  return ret;
}

int ObMigrateTransTableTaskGeneratorTask::process()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (BACKUP_REPLICA_OP == ctx_->replica_op_arg_.type_ || BACKUP_BACKUPSET_OP == ctx_->replica_op_arg_.type_ ||
             VALIDATE_BACKUP_OP == ctx_->replica_op_arg_.type_ || BACKUP_ARCHIVELOG_OP == ctx_->replica_op_arg_.type_) {
    // do nothing
  } else if (OB_ISNULL(ctx_->get_partition())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx do not hold partition", K(ret));
  } else if (OB_FAIL(build_migrate_trans_table_task())) {
    LOG_WARN("failed to build migrate trans table task", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("finish migrate trans table generator task", K(*ctx_));
  }

  if (OB_FAIL(ret) && NULL != ctx_) {
    if (ctx_->is_need_retry(ret)) {
      ctx_->need_rebuild_ = true;
      LOG_INFO("migrate prepare task need retry", K(ret), K(*ctx_));
    }
    ctx_->set_result_code(ret);
  }

  LOG_INFO("end ObMigrateTransTableTaskGeneratorTask process", K(ret));
  return ret;
}

int ObMigrateTransTableTaskGeneratorTask::build_migrate_trans_table_task()
{
  int ret = OB_SUCCESS;
  bool need_schedule = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate trans table task generator do not init", K(ret));
  } else if (ObFetchPGInfoResult::FETCH_PG_INFO_RES_COMPAT_VERSION_V1 >= ctx_->fetch_pg_info_compat_version_) {
    LOG_INFO("compat version no need create trans table", K(ret), K(ctx_->fetch_pg_info_compat_version_));
  } else if (OB_FAIL(need_generate_sstable_migrate_tasks(need_schedule))) {
    LOG_WARN("failed to check need generate sstable migrate tasks", K(ret));
  } else if (!need_schedule) {
    // do nothing
  } else if (!need_migrate_trans_table(ctx_->replica_op_arg_.type_)) {
    LOG_INFO("no need to migrate trans table", K(ret), K(ctx_->replica_op_arg_.type_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->part_ctx_array_.count(); ++i) {
      ObPartitionMigrateCtx &part_migrate_ctx = ctx_->part_ctx_array_.at(i);
      if (!part_migrate_ctx.copy_info_.meta_.pkey_.is_trans_table()) {
        // do nothing
      } else if (OB_FAIL(inner_build_migrate_trans_table_task(part_migrate_ctx))) {
        LOG_WARN("failed to inner build migrate trans table task", K(ret), K(part_migrate_ctx));
      }
    }
  }
  return ret;
}

int ObMigrateTransTableTaskGeneratorTask::inner_build_migrate_trans_table_task(ObPartitionMigrateCtx &part_migrate_ctx)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate trans table task generator do not init", K(ret));
  } else if (!part_migrate_ctx.copy_info_.meta_.pkey_.is_trans_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner build migrate trans table task get invalid argument", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_migrate_ctx.copy_info_.table_infos_.count(); ++i) {
      const ObMigratePartitionInfo &copy_info = part_migrate_ctx.copy_info_;
      const ObArray<ObMigrateTableInfo> &table_infos = copy_info.table_infos_;
      for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
        const ObMigrateTableInfo &table_info = table_infos.at(i);
        if (OB_FAIL(build_trans_sstable_tasks(ctx_->migrate_src_info_, table_info, part_migrate_ctx))) {
          LOG_WARN("failed to build trans sstable tasks", K(ret), K(i), K(table_info), K(part_migrate_ctx));
        }
      }
    }
  }
  return ret;
}

int ObMigrateTransTableTaskGeneratorTask::build_trans_sstable_tasks(
    const ObMigrateSrcInfo &src_info, const ObMigrateTableInfo &table_info, ObPartitionMigrateCtx &part_migrate_ctx)
{
  int ret = OB_SUCCESS;
  ObITask *parent_task = this;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate trans table task generator do not init", K(ret));
  } else if (!table_info.minor_sstables_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans sstable has minor sstables which is unexpected", K(ret), K(table_info));
  } else {
    // add trans sstables
    for (int64_t sstable_idx = 0; OB_SUCC(ret) && sstable_idx < table_info.major_sstables_.count(); ++sstable_idx) {
      const ObMigrateTableInfo::SSTableInfo &trans_table_info = table_info.major_sstables_.at(sstable_idx);
      ObFakeTask *wait_finish_task = NULL;
      if (!ObITable::is_trans_sstable(trans_table_info.src_table_key_.table_type_)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table type not match major sstable", K(ret), K(trans_table_info), K(table_info));
      } else {
        if (OB_FAIL(dag_->alloc_task(wait_finish_task))) {
          LOG_WARN("failed to alloc wait finish task", K(ret));
        } else if (OB_FAIL(generate_physical_sstable_copy_task(
                       src_info, part_migrate_ctx, trans_table_info, parent_task, wait_finish_task))) {
          LOG_WARN("fail to generate physical sstable copy task", K(ret), K(trans_table_info), K(src_info));
        } else if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
          LOG_WARN("failed to add wait finish task", K(ret));
        } else {
          parent_task = wait_finish_task;
          LOG_INFO("succeed to build trans sstable tasks", K(src_info), K(trans_table_info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(parent_task->add_child(*wait_finish_task_))) {
        LOG_WARN("failed to add wait_rebuild_finish_task", K(ret));
      }
    }
  }
  return ret;
}

ObMigrateTaskSchedulerTask::ObMigrateTaskSchedulerTask()
    : ObITask(TASK_TYPE_MIGRATE_PREPARE),
      is_inited_(false),
      ctx_(NULL),
      cp_fty_(NULL),
      rpc_(NULL),
      srv_rpc_proxy_(NULL),
      bandwidth_throttle_(NULL),
      partition_service_(NULL)
{}

ObMigrateTaskSchedulerTask::~ObMigrateTaskSchedulerTask()
{}

int ObMigrateTaskSchedulerTask::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("migrate task scheduler task init twice", K(ret));
  } else if (ObIDag::DAG_TYPE_MIGRATE != dag_->get_type() && ObIDag::DAG_TYPE_BACKUP != dag_->get_type() &&
             ObIDag::DAG_TYPE_VALIDATE != dag_->get_type() && ObIDag::DAG_TYPE_BACKUP_BACKUPSET != dag_->get_type() &&
             ObIDag::DAG_TYPE_BACKUP_ARCHIVELOG != dag_->get_type()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), K(*dag_));
  } else {
    ctx_ = static_cast<ObMigrateDag *>(dag_)->get_ctx();
    is_inited_ = true;
    bandwidth_throttle_ = MIGRATOR.get_bandwidth_throttle();
    partition_service_ = MIGRATOR.get_partition_service();
    srv_rpc_proxy_ = MIGRATOR.get_svr_rpc_proxy();
    rpc_ = MIGRATOR.get_pts_rpc();
    cp_fty_ = MIGRATOR.get_cp_fty();
  }
  return ret;
}

int ObMigrateTaskSchedulerTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (NULL != ctx_) {
    ctx_->action_ = ObMigrateCtx::PREPARE;
    if (NULL != dag_) {
      ctx_->task_id_ = dag_->get_dag_id();
    }

    if (OB_SUCCESS != (tmp_ret = (ctx_->trace_id_array_.push_back(*ObCurTraceId::get_trace_id())))) {
      STORAGE_LOG(WARN, "failed to push back trace id to array", K(tmp_ret));
    }
    LOG_INFO("start ObMigrateTaskSchedulerTask process", "arg", ctx_->replica_op_arg_);
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(add_migrate_status(ctx_))) {
    LOG_WARN("failed to add migrate status", K(ret));
  } else {
    {
      const bool is_write_lock = true;
      ObMigrateCtxGuard guard(is_write_lock, *ctx_);
      if (!ctx_->is_valid()) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "invalid migrate ctx", K(ret), K(*this), K(*ctx_));
      } else if (1 != ctx_->doing_task_cnt_) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "doing task must be 1 during prepare action", K(ret), K(*this), K(*ctx_));
      } else if (REMOVE_REPLICA_OP == ctx_->replica_op_arg_.type_) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "should not prepare migrate for remove replica", K(ret), K(ctx_->replica_op_arg_));
      } else if (OB_FAIL(try_hold_local_partition())) {
        LOG_WARN("failed to hold local partition", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(schedule_task_by_type())) {
        LOG_WARN("failed to schedule task by type", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("finish scheduler task", K(*ctx_));
  }

  if (OB_FAIL(ret) && NULL != ctx_) {
    if (ctx_->is_need_retry(ret)) {
      ctx_->need_rebuild_ = true;
      LOG_INFO("migrate scheduler task need retry", K(ret), K(*ctx_));
    }
    ctx_->set_result_code(ret);
  }

  LOG_INFO("end ObMigrateTaskSchedulerTask process", K(ret));
  return ret;
}

int ObMigrateTaskSchedulerTask::add_partition_migration_status(const ObMigrateCtx &ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(ctx));
  } else {
    ObPartitionMigrationStatus task;
    task.task_id_ = ctx.task_id_;
    task.migrate_type_ = ctx.replica_op_arg_.get_replica_op_type_str();
    task.pkey_ = ctx.replica_op_arg_.key_;
    task.clog_parent_.reset();  // we don't know it now
    task.src_.reset();          // we don't know it now
    task.dest_ = OBSERVER.get_self();
    task.result_ = ctx.result_;
    task.start_time_ = ctx.create_ts_;
    task.action_ = ctx.action_;
    task.replica_state_ = OB_UNKNOWN_REPLICA;  // we don't know it now
    task.doing_task_count_ = 0;
    task.total_task_count_ = 0;
    task.rebuild_count_ = 0;
    task.continue_fail_count_ = 0;
    task.data_statics_ = ctx.data_statics_;

    // allow comment truncation, no need to set ret
    (void)ctx.fill_comment(task.comment_, sizeof(task.comment_));

    if (OB_SUCCESS != (tmp_ret = ObPartitionMigrationStatusMgr::get_instance().add_status(task))) {
      STORAGE_LOG(WARN, "failed to add partition migration status", K(tmp_ret), K(task));
    }
  }
  return ret;
}

int ObMigrateTaskSchedulerTask::schedule_task_by_type()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup *partition_group = NULL;
  int16_t restore_flag = REPLICA_NOT_RESTORE;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (FALSE_IT(partition_group = ctx_->partition_guard_.get_partition_group())) {
  } else if (OB_NOT_NULL(partition_group) &&
             FALSE_IT(restore_flag = partition_group->get_pg_storage().get_restore_state())) {
  } else if ((RESTORE_REPLICA_OP == ctx_->replica_op_arg_.type_ || RESTORE_STANDBY_OP == ctx_->replica_op_arg_.type_) &&
             (REPLICA_RESTORE_CUT_DATA == restore_flag || REPLICA_RESTORE_STANDBY_CUT == restore_flag)) {
    if (OB_FAIL(generate_restore_cut_prepare_task())) {
      LOG_WARN("failed to generate restore cut prepare task", K(ret), K(*ctx_));
    }
  } else {
    if (OB_FAIL(generate_migrate_prepare_task())) {
      LOG_WARN("failed to generate migrate prepare task", K(ret), K(*ctx_));
    }
  }
  return ret;
}

int ObMigrateTaskSchedulerTask::generate_restore_cut_prepare_task()
{
  int ret = OB_SUCCESS;
  ObRestoreTailoredPrepareTask *task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(task))) {
    LOG_WARN("failed to alloc dag task", K(ret), K(*ctx_));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init restore tailored pepare task", K(ret), K(*ctx_));
  } else if (OB_FAIL(this->add_child(*task))) {
    LOG_WARN("failed to add child task", K(ret), K(*ctx_));
  } else if (OB_FAIL(dag_->add_task(*task))) {
    LOG_WARN("failed to add task to dag", K(ret), K(*ctx_));
  }
  return ret;
}

int ObMigrateTaskSchedulerTask::generate_migrate_prepare_task()
{
  int ret = OB_SUCCESS;
  ObMigratePrepareTask *task = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(task))) {
    LOG_WARN("failed to allock migrate prepare task", K(ret), K(*ctx_));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init migrate prepare task", K(ret), K(*ctx_));
  } else if (OB_FAIL(this->add_child(*task))) {
    LOG_WARN("failed to add child task", K(ret), K(*ctx_));
  } else if (OB_FAIL(dag_->add_task(*task))) {
    LOG_WARN("failed to add task to dag", K(ret), K(*ctx_));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
