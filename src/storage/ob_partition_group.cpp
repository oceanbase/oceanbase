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

#include "clog/ob_clog_history_reporter.h"
#include "storage/ob_partition_group.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "common/ob_partition_key.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_partition_service_rpc.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_log.h"
#include "storage/ob_saved_storage_info_v2.h"
#include "common/storage/ob_freeze_define.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/ob_partition_scheduler.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_partition_split_worker.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "storage/ob_partition_log.h"
#include "storage/ob_partition_checkpoint.h"
#include "share/ob_partition_modify.h"
#include "storage/ob_partition_migrator.h"
#include "storage/ob_pg_log.h"
#include "share/schema/ob_part_mgr_util.h"
#include "storage/ob_file_system_util.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "storage/transaction/ob_trans_split_adapter.h"
#include "share/ob_cluster_version.h"

namespace oceanbase {
using namespace common;
using namespace rootserver;
using namespace blocksstable;
using namespace memtable;
using namespace transaction;
using namespace clog;
using namespace share::schema;
using namespace share;
using namespace obrpc;
namespace storage {

const char* OB_PARTITION_STATE_STR[INVALID_STATE + 1] = {"INIT",
    "F_WORKING",
    "F_MINOR",
    "L_TAKEOVER",
    "L_TAKEOVERED",
    "L_CANCELED",
    "L_WORKING",
    "L_FROZEN",
    "L_MINOR",
    "L_REVOKE",
    "OFFLINING",
    "OFFLINE",
    "REMOVE",
    "INVALID_STATE"};

ObPartitionGroup::ObPartitionGroup()
    : is_inited_(false),
      pkey_(),
      replay_status_(NULL),
      cp_fty_(NULL),
      pls_(NULL),
      txs_(NULL),
      rp_eg_(NULL),
      ps_(NULL),
      pg_index_(nullptr),
      pg_partition_map_(nullptr),
      lock_(),
      partition_state_lock_(),
      partition_state_(INIT),
      schema_version_container_(),
      schema_service_(NULL),
      split_state_(),
      split_info_(),
      split_trans_clear_ts_(0),
      is_split_blocked_by_mc_(false),
      max_passed_trans_version_(0),
      freeze_record_(),
      trans_version_lock_(),
      gc_schema_drop_ts_(OB_INVALID_TIMESTAMP),
      gc_seq_check_valid_member_(-1),
      offline_log_id_(OB_INVALID_ID),
      migrate_retry_flag_(NO_NEED_RETRY),
      need_gc_(false),
      restore_task_cnt_(0),
      restore_task_ts_(0),
      has_clear_trans_after_restore_(false)
{
  REF_KEEPER.reg(safe_ref_, (void*)this);
}

ObPartitionGroup::~ObPartitionGroup()
{
  destroy();
}

int ObPartitionGroup::check_init_(void* cp, const char* cp_name) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || NULL == cp || NULL == cp_name) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "component does not exist", "component name", cp_name);
  }
  return ret;
}

const ObPartitionKey& ObPartitionGroup::get_partition_key() const
{
  return pkey_;
}

int ObPartitionGroup::get_pg_partition(const common::ObPartitionKey& pkey, ObPGPartitionGuard& guard)
{
  return pg_storage_.get_pg_partition(pkey, guard);
}

int ObPartitionGroup::set_valid()
{
  return try_switch_partition_state(F_WORKING);
}

void ObPartitionGroup::set_invalid()
{
  STORAGE_LOG(ERROR, "set partition to INVALID_STATE state");
  ATOMIC_STORE(&partition_state_, INVALID_STATE);
  while (1) {
    ;
  }
}

int ObPartitionGroup::table_scan(ObTableScanParam& param, ObNewRowIterator*& result)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_split_state_())) {
    LOG_WARN("check split state failed", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_storage_.table_scan(param, result))) {
    if (NULL != param.trans_desc_) {
      LOG_WARN("failed to do table scan", K(ret), "this", *this, "trans_id", param.trans_desc_->get_trans_id());
    } else {
      LOG_WARN("failed to do table scan", K(ret), "this", *this);
    }
  } else {
    // do nothing
  }

  return ret;
}
/*
 The description of access replica_type and splite_state without lock at DML interfaces such as table_scan and
 insert/update.
 1. TableScan
   a) There is no guarantee that read does not report errors if we check replica type without lock at PG layer, for
 example read and replica type transforming occur concurrently. This situation should be dealt with at
 ObPartitionStore::get_read_tables. b) PG split The origin partition can be choosed as the read partition, if table scan
 starts before PG split; The read operation at dest partition needs to retry (transmit it maybe better), if the
 partition is at splitting sate which means that the logical split is not completed.
 2. Insert/Update/Delete/Lock
   a) The replica type change is made on the follower. Even if there is no defense here, the transaction layer will
 still report an error. b) During the operation of DML, it is necessary to ensure that the memstore that has been frozen
 can no longer be written. This is guaranteed at check_split_state.

 In conclusion, the above interfaces do not need to be modified to access replica_type and split_state with lock.
 */
int ObPartitionGroup::table_scan(ObTableScanParam& param, ObNewIterIterator*& result)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_split_state_())) {
    LOG_WARN("check split state failed", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_storage_.table_scan(param, result))) {
    if (NULL != param.trans_desc_) {
      LOG_WARN("failed to do table scan", K(ret), "this", *this, "trans_id", param.trans_desc_->get_trans_id());
    } else {
      LOG_WARN("failed to do table scan", K(ret), "this", *this);
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionGroup::join_mv_scan(ObTableScanParam& left_param, ObTableScanParam& right_param,
    ObIPartitionGroup& right_partition, common::ObNewRowIterator*& result)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_split_state_())) {
    LOG_WARN("check split state failed", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_storage_.join_mv_scan(left_param, right_param, right_partition, result))) {
    LOG_WARN("join mv scan failed", K(ret), K(left_param), K(right_param));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionGroup::delete_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_split_state_())) {
    LOG_WARN("check split state failed", K(ret), K_(pkey));
    // } else if (OB_FAIL(schema_recorder_.try_update_table_schema(dml_param.schema_version_))) {
    //   LOG_WARN("fail to record table schema", K(ret), K(pkey_), K(dml_param.schema_version_));
  } else if (OB_FAIL(pg_storage_.delete_rows(ctx, dml_param, column_ids, row_iter, affected_rows))) {
    LOG_WARN("failed to delete row", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionGroup::delete_row(
    const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids, const ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_split_state_())) {
    LOG_WARN("check split state failed", K(ret), K_(pkey));
    // } else if (OB_FAIL(schema_recorder_.try_update_table_schema(dml_param.schema_version_))) {
    //   LOG_WARN("fail to record table schema", K(ret), K(pkey_), K(dml_param.schema_version_));
  } else if (OB_FAIL(pg_storage_.delete_row(ctx, dml_param, column_ids, row))) {
    LOG_WARN("failed to delete row", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionGroup::put_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, ObNewRowIterator* row_iter, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_split_state_())) {
    LOG_WARN("check split state failed", K(ret), K(pkey_));
    // } else if (OB_FAIL(schema_recorder_.try_update_table_schema(dml_param.schema_version_))) {
    //   LOG_WARN("fail to record table schema", K(ret), K(pkey_), K(dml_param.schema_version_));
  } else if (OB_FAIL(pg_storage_.put_rows(ctx, dml_param, column_ids, row_iter, affected_rows))) {
    LOG_WARN("failed to put rows", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionGroup::insert_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, ObNewRowIterator* row_iter, int64_t& affected_rows)
//  ObPartitionKey &pkey
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_split_state_())) {
    LOG_WARN("check split state failed", K(ret), K_(pkey));
    // } else if (OB_FAIL(schema_recorder_.try_update_table_schema(dml_param.schema_version_))) {
    //   LOG_WARN("fail to record table schema", K(ret), K(pkey_), K(dml_param.schema_version_));
  } else if (OB_FAIL(pg_storage_.insert_rows(ctx, dml_param, column_ids, row_iter, affected_rows))) {
    LOG_WARN("failed to insert rows", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionGroup::insert_row(
    const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids, const ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_split_state_())) {
    LOG_WARN("check split state failed", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_storage_.insert_row(ctx, dml_param, column_ids, row))) {
    LOG_WARN("failed to insert rows", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionGroup::insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& duplicated_column_ids,
    const common::ObNewRow& row, const ObInsertFlag flag, int64_t& affected_rows,
    common::ObNewRowIterator*& duplicated_rows)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_split_state_())) {
    LOG_WARN("check split state failed", K(ret), K_(pkey));
    // } else if (OB_FAIL(schema_recorder_.try_update_table_schema(dml_param.schema_version_))) {
    //   LOG_WARN("fail to record table schema", K(ret), K(pkey_), K(dml_param.schema_version_));
  } else if (OB_FAIL(pg_storage_.insert_row(
                 ctx, dml_param, column_ids, duplicated_column_ids, row, flag, affected_rows, duplicated_rows))) {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE != ret) {
      LOG_WARN("failed to insert rows", K(ret), "this", *this, K(ctx));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionGroup::fetch_conflict_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& in_column_ids, const ObIArray<uint64_t>& out_column_ids, ObNewRowIterator& check_row_iter,
    ObIArray<ObNewRowIterator*>& dup_row_iters)
{
  int ret = OB_SUCCESS;
  // For DML, it should check if the replica is writable and be able to read the data in memtable
  // even if there is no sstable.
  if (OB_FAIL(check_split_state_())) {
    LOG_WARN("check split state failed", K(ret), K_(pkey));
    // } else if (OB_FAIL(schema_recorder_.try_update_table_schema(dml_param.schema_version_))) {
    //   LOG_WARN("fail to record table schema", K(ret), K(pkey_), K(dml_param.schema_version_));
  } else if (OB_FAIL(pg_storage_.fetch_conflict_rows(
                 ctx, dml_param, in_column_ids, out_column_ids, check_row_iter, dup_row_iters))) {
    LOG_WARN("failed to fetch conflict rows", K(ret), KPC(this), K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionGroup::revert_insert_iter(const common::ObPartitionKey& pkey, ObNewRowIterator* iter)
{
  return pg_storage_.revert_insert_iter(pkey, iter);
}

int ObPartitionGroup::update_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, const ObIArray<uint64_t>& updated_column_ids, ObNewRowIterator* row_iter,
    int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_split_state_())) {
    LOG_WARN("check split state failed", K(ret), K_(pkey));
    // } else if (OB_FAIL(schema_recorder_.try_update_table_schema(dml_param.schema_version_))) {
    //   LOG_WARN("fail to record table schema", K(ret), K(pkey_), K(dml_param.schema_version_));
  } else if (OB_FAIL(
                 pg_storage_.update_rows(ctx, dml_param, column_ids, updated_column_ids, row_iter, affected_rows))) {
    LOG_WARN("failed to update rows", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionGroup::update_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
    const ObIArray<uint64_t>& column_ids, const ObIArray<uint64_t>& updated_column_ids, const ObNewRow& old_row,
    const ObNewRow& new_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_split_state_())) {
    LOG_WARN("check split state failed", K(ret), K_(pkey));
    // } else if (OB_FAIL(schema_recorder_.try_update_table_schema(dml_param.schema_version_))) {
    //   LOG_WARN("fail to record table schema", K(ret), K(pkey_), K(dml_param.schema_version_));
  } else if (OB_FAIL(pg_storage_.update_row(ctx, dml_param, column_ids, updated_column_ids, old_row, new_row))) {
    LOG_WARN("failed to update row", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionGroup::lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
    ObNewRowIterator* row_iter, const ObLockFlag lock_flag, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_split_state_())) {
    LOG_WARN("check split state failed", K(ret), K_(pkey));
    // } else if (OB_FAIL(schema_recorder_.try_update_table_schema(dml_param.schema_version_))) {
    //   LOG_WARN("fail to record table schema", K(ret), K(pkey_), K(dml_param.schema_version_));
  } else if (OB_FAIL(pg_storage_.lock_rows(ctx, dml_param, abs_lock_timeout, row_iter, lock_flag, affected_rows))) {
    LOG_WARN("failed to lock rows", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionGroup::lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
    const ObNewRow& row, const ObLockFlag lock_flag)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_split_state_())) {
    LOG_WARN("check split state failed", K(ret), K_(pkey));
    // } else if (OB_FAIL(schema_recorder_.try_update_table_schema(dml_param.schema_version_))) {
    //   LOG_WARN("fail to record table schema", K(ret), K(pkey_), K(dml_param.schema_version_));
  } else if (OB_FAIL(pg_storage_.lock_rows(ctx, dml_param, abs_lock_timeout, row, lock_flag))) {
    LOG_WARN("failed to lock rows", K(ret), "this", *this, K(ctx));
  } else {
    // do nothing
  }
  return ret;
}

const clog::ObIPartitionLogService* ObPartitionGroup::get_log_service() const
{
  return pls_;
}

clog::ObIPartitionLogService* ObPartitionGroup::get_log_service()
{
  return pls_;
}

ObPartitionService* ObPartitionGroup::get_partition_service()
{
  return ps_;
}

ObTransService* ObPartitionGroup::get_trans_service()
{
  return txs_;
}

int ObPartitionGroup::get_role(common::ObRole& role) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = check_init_(pls_, "partition log service"))) {
    ret = pls_->get_role(role);
  }
  return ret;
}

int ObPartitionGroup::get_role_for_partition_table(common::ObRole& role) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = check_init_(pls_, "partition log service"))) {
    ret = pls_->get_role_for_partition_table(role);
  }
  return ret;
}

int ObPartitionGroup::get_role_unsafe(common::ObRole& role) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = check_init_(pls_, "partition log service"))) {
    ret = pls_->get_role(role);
  }
  return ret;
}

int ObPartitionGroup::get_leader(common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = check_init_(pls_, "partition log service"))) {
    ret = pls_->get_leader(leader);
  }
  return ret;
}

int ObPartitionGroup::get_leader_curr_member_list(ObMemberList& member_list) const
{
  return get_leader_curr_member_list_(member_list);
}

int ObPartitionGroup::get_leader_curr_member_list_(ObMemberList& member_list) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = check_init_(pls_, "partition log service"))) {
    ret = pls_->get_leader_curr_member_list(member_list);
  }
  return ret;
}

int ObPartitionGroup::get_curr_member_list(ObMemberList& member_list) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = check_init_(pls_, "partition log service"))) {
    ret = pls_->get_curr_member_list(member_list);
  }
  return ret;
}

int ObPartitionGroup::get_curr_member_list_for_report(ObMemberList& member_list) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = check_init_(pls_, "partition log service"))) {
    ret = pls_->get_curr_member_list_for_report(member_list);
  }
  return ret;
}

int ObPartitionGroup::get_curr_leader_and_memberlist(common::ObAddr& leader, common::ObRole& role,
    common::ObMemberList& curr_member_list, common::ObChildReplicaList& children_list) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = check_init_(pls_, "partition log service"))) {
    ret = pls_->get_curr_leader_and_memberlist(leader, role, curr_member_list, children_list);
  }
  return ret;
}

int ObPartitionGroup::get_dst_leader_candidate(ObMemberList& member_list) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = check_init_(pls_, "partition log service"))) {
    ret = pls_->get_dst_leader_candidate(member_list);
  }
  return ret;
}

int ObPartitionGroup::get_log_archive_status(ObPGLogArchiveStatus& status) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = check_init_(pls_, "partition log service"))) {
    ret = pls_->get_log_archive_status(status);
  }
  return ret;
}

int ObPartitionGroup::change_leader(const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  ObTsWindows changing_leader_windows;
  if (OB_SUCCESS == (ret = check_init_(pls_, "partition log service"))) {
    if (OB_FAIL(pls_->change_leader(server, changing_leader_windows))) {
      STORAGE_LOG(WARN, "change leader failed", K(ret), K(server));
    } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2100 || !GCONF.enable_smooth_leader_switch) {
      STORAGE_LOG(INFO, "do not register prepare changing leader task", K_(pkey));
    } else if (changing_leader_windows.get_start() == changing_leader_windows.get_end()) {
      STORAGE_LOG(WARN, "invalid change leader windows", K_(pkey), K(changing_leader_windows));
    } else if (OB_FAIL(register_txs_change_leader(server, changing_leader_windows))) {
      STORAGE_LOG(WARN, "register txs prepare change leader task failed", K(ret), K_(pkey), K(changing_leader_windows));
    }
  }
  return ret;
}

int ObPartitionGroup::register_txs_change_leader(const common::ObAddr& server, ObTsWindows& changing_leader_windows)
{
  int ret = OB_SUCCESS;

  const int64_t ts = changing_leader_windows.get_start() + changing_leader_windows.get_left_size() / 2;

  if (OB_FAIL(txs_->register_prepare_changing_leader_task(pkey_, server, ts))) {
    STORAGE_LOG(
        WARN, "register prepare changing leader task failed", K(ret), K_(pkey), K(ts), K(changing_leader_windows));
  } else {
    STORAGE_LOG(INFO, "register prepare changing leader task success", K_(pkey), K(ts), K(changing_leader_windows));
  }

  return ret;
}

int ObPartitionGroup::check_physical_split(bool& finished)
{
  int ret = OB_SUCCESS;
  ret = check_physical_split_(finished);
  return ret;
}

int ObPartitionGroup::wait_replay_()
{
  int ret = OB_SUCCESS;

  restore_task_cnt_ = 0;

  while (true) {
    int64_t task_cnt = replay_status_->get_pending_task_count();
    int64_t curr = ObTimeUtility::current_time();

    if (restore_task_cnt_ && curr - restore_task_ts_ > 1000L * 1000L) {
      STORAGE_LOG(INFO, "wait replay", K(pkey_), K(task_cnt));
    }

    if (0 == task_cnt) {
      break;
    } else if (task_cnt != restore_task_cnt_) {
      restore_task_cnt_ = task_cnt;
      restore_task_ts_ = curr;
    } else if (curr - restore_task_ts_ > 180L * 1000L * 1000L) {
      // It is regarded as timeout if there is no replay action for 3 minutes.
      ret = OB_TIMEOUT;
      break;
    }

    usleep(20 * 1000);  // 20ms
  }

  return ret;
}

int ObPartitionGroup::leader_takeover()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(pls_) || OB_ISNULL(txs_) || OB_ISNULL(replay_status_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else {
    const bool in_slog_trans = false;
    const bool is_replay = false;
    ObPartitionGroupLockGuard guard(lock_, PGLOCKREPLAY | PGLOCKCLOG | PGLOCKTRANS, PGLOCKSTORAGE);
    SpinRLockGuard split_guard(split_lock_);
    ObBaseStorageInfo clog_info;

    if (0 == get_replica_property().get_memstore_percent()) {
      if (OB_FAIL(pg_storage_.get_saved_clog_info(clog_info))) {
        STORAGE_LOG(WARN, "fail to get saved info", K(ret), K(pkey_));
      } else if (OB_FAIL(clear_non_reused_stores_(pkey_))) {
        STORAGE_LOG(WARN, "fail to clear non-reused stores", K(ret), K(pkey_));
        // D replica will create memtable at leader takeover process.
      } else if (OB_FAIL(create_memtable_(in_slog_trans, is_replay, true))) {
        STORAGE_LOG(WARN, "fail to create memtable", K(ret), K(pkey_));
      } else if (FALSE_IT(guard.unlock(PGLOCKSTORAGE))) {
      } else if (OB_FAIL(pls_->restore_replayed_log(clog_info))) {
        STORAGE_LOG(WARN, "fail to restore replayed log", K(ret), K(pkey_), K(clog_info));
      } else if (OB_FAIL(wait_replay_())) {
        STORAGE_LOG(WARN, "fail to wait replay", K(ret), K(pkey_));
      }
    }
  }

  if (OB_FAIL(ret)) {
    // rollback
    ObPartitionGroupLockGuard guard(lock_, 0, PGLOCKTRANS | PGLOCKREPLAY | PGLOCKSTORAGE);
    if (0 == get_replica_property().get_memstore_percent()) {
      int64_t tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = replay_status_->set_need_filter_trans_log(pkey_, true /*need filter*/))) {
        STORAGE_LOG(ERROR, "fail to set_need_filter_trans_log", K(tmp_ret), K_(pkey));
      } else if (OB_SUCCESS != (tmp_ret = txs_->clear_all_ctx(pkey_))) {
        STORAGE_LOG(ERROR, "fail to clear all trans ctx", K(tmp_ret), K_(pkey));
        // PG or stand alone partition
      } else if (OB_SUCCESS != (tmp_ret = clear_non_reused_stores_(pkey_))) {
        STORAGE_LOG(WARN, "fail to clear non reused store", K(ret), K(pkey_));
      }
    }
  }

  return ret;
}

int ObPartitionGroup::leader_revoke()
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg(__func__, 1000000);
  // TODO: comebine the two locks
  ObPartitionGroupLockGuard pg_guard(lock_, 0, PGLOCKTRANS | PGLOCKREPLAY | PGLOCKSTORAGE);
  SpinWLockGuard guard(split_lock_);
  tg.click();
  if (OB_FAIL(split_state_.switch_state(LEADER_REVOKE))) {
    STORAGE_LOG(WARN, "switch state failed", K(ret));
  }
  is_split_blocked_by_mc_ = false;
  replica_split_progress_array_.reset();
  // Checkpoint can be written continuously if the partition is selected as leader again.
  partition_loop_worker_.reset_last_checkpoint();
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "leader revoke failed", K(ret), K_(pkey));
  } else {
    STORAGE_LOG(INFO, "leader revoke success", K_(pkey));
  }

  if (0 == get_replica_property().get_memstore_percent()) {
    int64_t tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = replay_status_->set_need_filter_trans_log(pkey_, true /*need filter*/))) {
      STORAGE_LOG(ERROR, "fail to set_need_filter_trans_log", K(tmp_ret), K_(pkey));
    } else if (OB_SUCCESS != (tmp_ret = txs_->clear_all_ctx(pkey_))) {
      STORAGE_LOG(ERROR, "fail to clear all trans ctx", K(tmp_ret), K_(pkey));
      // PG or stand alone partition
    } else if (OB_SUCCESS != (tmp_ret = clear_non_reused_stores_(pkey_))) {
      STORAGE_LOG(ERROR, "fail to clear non reused store", K(ret), K(pkey_));
    }
  }

  return ret;
}

int ObPartitionGroup::leader_active()
{
  int ret = OB_SUCCESS;
  bool is_all_trans_clear = false;
  int64_t cur_checkpoint = 0;

  if (OB_FAIL(pg_storage_.get_weak_read_timestamp(cur_checkpoint))) {
    STORAGE_LOG(WARN, "get readable ts error", K(ret), K_(pkey));
  } else {
    partition_loop_worker_.update_last_checkpoint(cur_checkpoint);
  }
  if (OB_SUCC(ret)) {
    ObTimeGuard tg(__func__, 1000000);
    SpinWLockGuard guard(split_lock_);
    tg.click();
    if (OB_FAIL(split_state_.switch_state(LEADER_TAKEOVER))) {
      STORAGE_LOG(WARN, "switch state failed", K(ret));
    } else {
      if (in_source_splitting(split_state_.get_state())) {
        if (OB_FAIL(txs_->block_partition(pkey_, is_all_trans_clear))) {
          STORAGE_LOG(WARN, "block partition failed", K(ret), K_(pkey));
        } else if (OB_FAIL(push_split_task_(split_info_.get_schema_version(), split_info_.get_spp()))) {
          STORAGE_LOG(WARN, "push split task failed", K(ret), K_(split_state));
        } else {
          STORAGE_LOG(INFO, "push split task success", K_(split_state));
        }
      }
      if (OB_SUCCESS != ret) {
        (void)split_state_.restore_state();
      }
    }
  }

  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "leader active failed", K(ret), K_(pkey));
  } else {
    STORAGE_LOG(INFO, "leader active success", K_(pkey));
  }

  return ret;
}

ObReplayStatus* ObPartitionGroup::get_replay_status()
{
  return replay_status_;
}

int ObPartitionGroup::serialize(ObArenaAllocator& allocator, char*& new_buf, int64_t& serialize_size)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(pg_storage_.serialize(allocator, new_buf, serialize_size))) {
    STORAGE_LOG(WARN, "Fail to serialize storage, ", K(ret));
  }

  return ret;
}

int ObPartitionGroup::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t state = static_cast<ObPartitionSplitStateEnum>(UNKNOWN_SPLIT_STATE);
  ObPartitionSplitInfo split_info;

  if (NULL == buf || buf_len <= 0 || pos < 0 || pos >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(buf), K(buf_len), K(pos), K(ret));
  } else {
    if (OB_FAIL(pg_storage_.init(pkey_, cp_fty_, schema_service_, txs_, pls_, this))) {
      STORAGE_LOG(
          WARN, "partition info init error", K(ret), K_(pkey), KP_(cp_fty), KP_(schema_service), KP_(pls), KP_(txs));
    } else if (OB_FAIL(pg_storage_.deserialize(buf, buf_len, pos, state, split_info))) {
      STORAGE_LOG(WARN, "Fail to deserialize pg partition info, ", K(ret));
    } else if (OB_FAIL(split_state_.set_partition_key(pg_storage_.get_partition_key()))) {
      STORAGE_LOG(WARN, "Fail to set partition key", K(ret));
    } else if (UNKNOWN_SPLIT_STATE != state &&
               OB_FAIL(split_state_.set_state(static_cast<ObPartitionSplitStateEnum>(state)))) {
      STORAGE_LOG(WARN, "set split state failed", K(ret), K(state));
    } else if (OB_FAIL(split_info_.assign(split_info))) {
      STORAGE_LOG(WARN, "failed to assign split info", K(ret), K(split_info));
    } else {
      // The update of pkey_ should be before the partition_loop_worker init.
      pkey_ = pg_storage_.get_partition_key();
      if (nullptr != ps_ && OB_FAIL(partition_loop_worker_.init(this))) {
        STORAGE_LOG(WARN, "partition worker init error", K(ret), K_(pkey));
      } else if (OB_ISNULL(replay_status_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "replay status is NULL", K(ret), K(pkey_));
      } else if (OB_FAIL(replay_status_->init(pkey_.get_tenant_id(), rp_eg_, safe_ref_))) {
        STORAGE_LOG(WARN, "failed to init replay_status", K(ret), K(pkey_));
      }
    }
  }
  STORAGE_LOG(INFO, "deserialize partition", K(ret), K_(pkey));

  return ret;
}

bool ObPartitionGroup::is_valid() const
{
  ObPartitionState state = get_partition_state();
  return state > INIT && state < INVALID_STATE;
}

int ObPartitionGroup::init(const ObPartitionKey& key, ObIPartitionComponentFactory* cp_fty,
    share::schema::ObMultiVersionSchemaService* schema_service, transaction::ObTransService* txs,
    replayengine::ObILogReplayEngine* rp_eg, ObPartitionService* ps,
    ObPartitionGroupIndex* pg_index /*only assign in replay process*/,
    ObPGPartitionMap* pg_partition_map /*only assign in replay process*/)
{
  // WARNING: the parameter of pkey is fake, when the server restarts.
  int ret = OB_SUCCESS;
  if (!key.is_valid() || NULL == cp_fty || NULL == schema_service || NULL == txs || NULL == rp_eg ||
      (NULL == ps && (NULL == pg_index || NULL == pg_partition_map))) {
    STORAGE_LOG(WARN,
        "invalid arguments.",
        K(key),
        KP(cp_fty),
        KP(schema_service),
        KP(txs),
        KP(rp_eg),
        KP(ps),
        KP(pg_index),
        KP(pg_partition_map));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "partition is already initialized", K(key), K(ret));
    // FIXME
  } else {
    // The assignment of cp_fty_ must be first, otherwise there will be a memory leak in the case of init failure.
    cp_fty_ = cp_fty;
    uint64_t tenant_id = key.get_tenant_id();
    if (tenant_id <= 0) {
      tenant_id = OB_SERVER_TENANT_ID;
    }
    if (NULL == (pls_ = cp_fty->get_log_service(tenant_id))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "create partition log service failed, ", K(ret));
    } else if (NULL == (replay_status_ = cp_fty->get_replay_status(tenant_id))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "create replay status object failed.", K(ret));
    } else if (OB_FAIL(split_state_.init(key))) {
      STORAGE_LOG(WARN, "init split state failed", K(ret));
    } else if (OB_FAIL(split_state_.set_state(FOLLOWER_INIT))) {
      STORAGE_LOG(WARN, "set split state failed", K(ret));
    } else {
      is_inited_ = true;
      pkey_ = key;
      next_ = NULL;
      txs_ = txs;
      rp_eg_ = rp_eg;
      ps_ = ps;
      pg_index_ = (nullptr == ps) ? pg_index : &ps->get_pg_index();
      pg_partition_map_ = (nullptr == ps) ? pg_partition_map : &ps->get_partition_map();
      // create_timestamp_ = ObTimeUtility::current_time();
      schema_service_ = schema_service;
      if (key.get_table_id() != OB_MIN_USER_TABLE_ID) {
        // The key may be invalid at this time when the machine is down and restarted
        if (OB_FAIL(pg_storage_.init(key, cp_fty_, schema_service, txs, pls_, this))) {
          STORAGE_LOG(WARN, "pg storage init error", K(ret), K(key));
        } else if (nullptr != ps_ && OB_FAIL(partition_loop_worker_.init(this))) {
          STORAGE_LOG(WARN, "partition worker init error", K(ret), K_(pkey));
        } else if (OB_FAIL(replay_status_->init(tenant_id, rp_eg, safe_ref_))) {
          STORAGE_LOG(WARN, "replay status init failed", K(ret), K(key));
        } else {
          // do nothing
        }
      }
    }
    if (NULL != replay_status_) {
      // You need to add the reference count whether init succeeds or fails to make sure that
      // destroy will not make mistakes in case of init failure.
      replay_status_->inc_ref();
    }
    if (!is_inited_) {
      destroy();
    }
  }
  STORAGE_LOG(INFO, "partition init", KP(replay_status_), K(ret), K(key), KP(this));
  return ret;
}

// Write meta slog and create PG memstore while creating partition group.
int ObPartitionGroup::create_partition_group(const ObCreatePGParam& param)
{
  int ret = OB_SUCCESS;
  ObPartitionGroupLockGuard guard(lock_, 0, PGLOCKSTORAGE);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(pkey_), K(param));
  } else if (OB_FAIL(pg_storage_.create_partition_group(param))) {
    STORAGE_LOG(WARN, "pg storage update pg meta error", K(ret), K(param));
  } else if (OB_FAIL(split_info_.assign(param.split_info_))) {
    STORAGE_LOG(WARN, "failed to assign split info", K(ret), K(param));
  } else if (OB_FAIL(split_state_.set_state(static_cast<ObPartitionSplitStateEnum>(param.split_state_)))) {
    STORAGE_LOG(WARN, "failed to set split state", K(ret), K(param));
  } else {
    STORAGE_LOG(INFO, "create partition group success", K(param));
  }
  return ret;
}

int ObPartitionGroup::replay_pg_partition(const common::ObPartitionKey& pkey, const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(pkey), K(log_id));
  } else {
    ObPartitionGroupLockGuard guard(lock_, 0, PGLOCKSTORAGE, true /* trylock */);

    if (!guard.locked()) {
      ret = OB_EAGAIN;
    } else if (OB_FAIL(pg_storage_.replay_pg_partition(pkey, log_id))) {
      STORAGE_LOG(WARN, "create pg partition error", K(ret), "pkey", pkey_, K(pkey), K(log_id));
    } else {
      STORAGE_LOG(INFO, "create pg partition success", "pkey", pkey_, K(pkey), K(log_id));
    }
  }

  return ret;
}

int ObPartitionGroup::create_pg_partition(const common::ObPartitionKey& pkey, const int64_t multi_version_start,
    const uint64_t data_table_id, const obrpc::ObCreatePartitionArg& arg, const bool in_slog_trans,
    const bool is_replay, const uint64_t log_id, ObTablesHandle& sstables_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else {
    ObCreatePartitionParam param;
    if (OB_FAIL(param.extract_from(arg))) {
      STORAGE_LOG(WARN, "failed to extract create_param", KR(ret), K(pkey), K(arg));
    } else {
      ret = create_pg_partition(
          pkey, multi_version_start, data_table_id, param, in_slog_trans, is_replay, log_id, sstables_handle);
    }
  }
  return ret;
}

int ObPartitionGroup::create_pg_partition(const common::ObPartitionKey& pkey, const int64_t multi_version_start,
    const uint64_t data_table_id, const ObCreatePartitionParam& arg, const bool in_slog_trans, const bool is_replay,
    const uint64_t log_id, ObTablesHandle& sstables_handle)
{
  ObTimeGuard tg(__func__, 100L * 1000L);
  int ret = OB_SUCCESS;
  ObPartitionGroupLockGuard guard(lock_, 0, PGLOCKSTORAGE, is_replay /* trylock */);
  if (!guard.locked()) {
    ret = OB_EAGAIN;
  }

  tg.click();

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (OB_INVALID_ID == data_table_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(data_table_id));
    // Add Partition To PG
  } else if (OB_FAIL(pg_storage_.create_pg_partition(
                 pkey, multi_version_start, data_table_id, arg, in_slog_trans, is_replay, log_id, sstables_handle))) {
    STORAGE_LOG(WARN, "failed to create partition store", K(ret));
  } else {
    // do nothing
  }
  tg.click();

  return ret;
}

void ObPartitionGroup::destroy()
{
  ObTimeGuard timeguard("destroy partition", 3 * 1000 * 1000);
  STORAGE_LOG(INFO, "destroy partition", K(*this), KP(this), K(lbt()));
  // It must be placed in the header to ensure that all members of the partition are valid during the reference process
  REF_KEEPER.unreg(safe_ref_);

  if (NULL != cp_fty_) {
    timeguard.click();
    if (NULL != pls_) {
      cp_fty_->free(pls_);
      pls_ = NULL;
    }
    timeguard.click();
    if (NULL != replay_status_) {
      if (0 == replay_status_->dec_ref()) {
        cp_fty_->free(replay_status_);
      }
      replay_status_ = nullptr;
    }
  }

  timeguard.click();
  cp_fty_ = NULL;
  partition_state_ = INIT;

  migrate_retry_flag_ = NO_NEED_RETRY;
  need_gc_ = false;
  is_inited_ = false;
}

void ObPartitionGroup::clear()
{
  destroy();
  pg_storage_.clear();
}

int ObPartitionGroup::get_replica_state(ObPartitionReplicaState& state)
{
  int ret = OB_SUCCESS;
  state = OB_UNKNOWN_REPLICA;
  bool disable_replay_log = false;

  if (!is_inited_ || NULL == pls_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized.", K(ret), K_(pls));
  } else if (OB_SUCCESS != (ret = pls_->is_offline(disable_replay_log))) {
    STORAGE_LOG(WARN, "get temporary state from clog error.", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_storage_.get_replica_state(disable_replay_log, state))) {
    STORAGE_LOG(WARN, "get replica state error", K(ret), K_(pkey), K(state));
  } else {
    // do nothing
  }

  return ret;
}

void ObPartitionGroup::replay_status_revoke()
{
  // replay_status_.leader_revoke(pkey_);
}

int ObPartitionGroup::report_clog_history_online()
{
  int ret = OB_SUCCESS;

  uint64_t start_log_id = 0;
  int64_t start_tstamp = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret));
  } else if (OB_FAIL(get_saved_last_log_info_(start_log_id, start_tstamp))) {
    STORAGE_LOG(WARN, "fail to get last log info", K(ret), K(pkey_), K(start_log_id), K(start_tstamp));
  } else if (ObClogHistoryReporter::is_related_table(pkey_.get_tenant_id(), pkey_.get_table_id())) {
    STORAGE_LOG(INFO, "update clog history info", K(pkey_));
    (void)ObClogHistoryReporter::get_instance().online(pkey_, start_log_id, start_tstamp);
  }

  return ret;
}

int ObPartitionGroup::report_clog_history_offline()
{
  int ret = OB_SUCCESS;

  uint64_t start_log_id = OB_INVALID_ID;
  int64_t start_log_timestamp = OB_INVALID_TIMESTAMP;
  uint64_t end_log_id = OB_INVALID_ID;
  int64_t end_log_timestamp = OB_INVALID_TIMESTAMP;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret));
  } else if (ObClogHistoryReporter::is_related_table(pkey_.get_tenant_id(), pkey_.get_table_id())) {
    if (OB_FAIL(get_clog_service_range_for_clog_history_info_(
            start_log_id, start_log_timestamp, end_log_id, end_log_timestamp))) {
      STORAGE_LOG(WARN, "fail to get clog service range", K(ret), K(pkey_));
    } else {
      STORAGE_LOG(INFO,
          "update clog history info on offline",
          K(pkey_),
          K(start_log_id),
          K(start_log_timestamp),
          K(end_log_id),
          K(end_log_timestamp));
      (void)clog::ObClogHistoryReporter::get_instance().offline(
          pkey_, start_log_id, start_log_timestamp, end_log_id, end_log_timestamp);
    }
  }
  return ret;
}

int ObPartitionGroup::get_clog_service_range_for_clog_history_info_(
    uint64_t& start_log_id, int64_t& start_log_timestamp, uint64_t& end_log_id, int64_t& end_log_timestamp)
{
  int ret = OB_SUCCESS;
  clog::ObIPartitionLogService* pls = NULL;
  uint64_t max_log_id = OB_INVALID_ID;
  int64_t max_log_ts = OB_INVALID_TIMESTAMP;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret));
  } else if (NULL == (pls = get_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pls should not be NULL", K(ret));
  } else if (OB_SUCCESS !=
             (ret = pls->get_log_id_range(start_log_id, start_log_timestamp, end_log_id, end_log_timestamp))) {
    STORAGE_LOG(WARN, "fail to get log_id_range", K(ret));
  }
  // For clog history info, the maximum log ID can be larger, but not smaller.
  // The maximum value returned by get_log_id_range() is the maximum log ID
  // that has been replayed. Some logs may not be replayed in the sliding window.
  // These logs should also be the logs of the server service and be recorded at
  // clog history. For this purpose, the right boundary of the sliding window is
  // taken as the maximum log of the service
  else if (OB_FAIL(pls->get_sw_max_log_id_info(max_log_id, max_log_ts))) {
    STORAGE_LOG(WARN, "get_sw_max_log_id_info fail", K(ret), K(pkey_));
  } else {
    end_log_id = max_log_id;
    end_log_timestamp = max_log_ts;

    STORAGE_LOG(INFO,
        "get clog service range",
        K_(pkey),
        K(start_log_id),
        K(start_log_timestamp),
        K(end_log_id),
        K(start_log_timestamp));
  }
  return ret;
}

void ObPartitionGroup::erase_pg_from_clog()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(pls_)) {
  } else if (OB_FAIL(pls_->set_offline())) {
    STORAGE_LOG(WARN, "partition log service set offline failed", K(ret), K(pkey_));
  }
}

int ObPartitionGroup::remove_election_from_mgr()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(pls_)) {
    // do nothing
  } else if (OB_FAIL(pls_->remove_election())) {
    STORAGE_LOG(ERROR, "remove election failed", K(ret), K(pkey_));
  } else {
    STORAGE_LOG(INFO, "remove election success", K(ret), K(pkey_));
  }

  return ret;
}

void ObPartitionGroup::erase_pg_from_election()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(pls_)) {
    // do nothing
  } else if (OB_FAIL(pls_->stop_election())) {
    STORAGE_LOG(ERROR, "stop election failed", K(ret), K(pkey_));
  } else {
    // do nothing
  }
}

void ObPartitionGroup::erase_pg_from_replay_engine()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(rp_eg_)) {
  } else if (OB_FAIL(rp_eg_->remove_partition(pkey_, this))) {
    // The partition may be not exist after offline.
    if (OB_ENTRY_NOT_EXIST != ret && OB_PARTITION_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "replay engine remove partition failed", K(ret), K(pkey_));
    }
  }
}

void ObPartitionGroup::erase_pg_from_trans()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(txs_)) {
  } else if (OB_FAIL(txs_->remove_partition(pkey_, false))) {
    if (OB_ENTRY_NOT_EXIST != ret && OB_PARTITION_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to remove partition from transaction service", K(ret), K(pkey_));
    }
  }
}

void ObPartitionGroup::erase_pg_from_stat_cache()
{
  int ret = OB_SUCCESS;

  if (!is_pg()) {
    ObPGPartitionGuard pg_partition_guard;
    ObPGPartition* pg_partition = nullptr;
    ObPartitionStorage* storage = nullptr;
    if (OB_FAIL(get_pg_storage().get_pg_partition(pkey_, pg_partition_guard))) {
      STORAGE_LOG(WARN, "failed to get pg partition", K(ret), K(pkey_));
    } else if (OB_ISNULL(pg_partition = pg_partition_guard.get_pg_partition())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg partition is null", K(ret), K(pkey_));
    } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(pg_partition->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "partition storage is null", K(ret), K(pkey_));
    } else if (OB_FAIL(storage->erase_stat_cache())) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "failed to erase stat cache", K(ret), K(pkey_));
      }
    }
  }
}

int ObPartitionGroup::pause()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(replay_status_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "replay_status is NULL", K(ret), K(pkey_));
  } else {
    ObPGLockWithPendingReplayGuard guard(
        lock_, *replay_status_, pkey_, 0, PGLOCKREPLAY | PGLOCKCLOG | PGLOCKTRANS | PGLOCKSTORAGE);
    ret = pause_();
  }

  return ret;
}

int ObPartitionGroup::pause_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret));
  } else {
    pg_storage_.pause();
    erase_pg_from_clog();
    erase_pg_from_replay_engine();
    erase_pg_from_trans();

    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = report_clog_history_offline()))) {
      STORAGE_LOG(WARN, "fail to report offline to clog hisory", K(ret), K(pkey_));
    }

    if (OB_FAIL(clear_non_reused_stores_(pkey_))) {
      STORAGE_LOG(WARN, "failed to clear pg stores", K(ret), K(pkey_));
    }
  }

  return ret;
}

int ObPartitionGroup::stop()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(replay_status_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "replay_status is NULL", K(ret), K(pkey_));
  } else {
    ObPGLockWithPendingReplayGuard guard(
        lock_, *replay_status_, pkey_, 0, PGLOCKREPLAY | PGLOCKCLOG | PGLOCKTRANS | PGLOCKSTORAGE);
    ret = stop_();
  }

  return ret;
}

int ObPartitionGroup::stop_()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(SYNC_PG_AND_REPLAY_ENGINE_DEADLOCK);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret));
  } else if (OB_FAIL(pg_storage_.set_pg_removed())) {
    STORAGE_LOG(WARN, "failed to set removed", K(ret), K(pkey_));
  } else if (OB_FAIL(try_switch_partition_state(REMOVE))) {
    STORAGE_LOG(WARN, "switch partition state to REMOVE failed", K(ret), K(pkey_));
  } else if (OB_FAIL(pause_())) {
    STORAGE_LOG(WARN, "fail to pause partition", K(ret), K(pkey_));
  } else {
    ATOMIC_SET(&has_clear_trans_after_restore_, false);
    erase_pg_from_election();
    erase_pg_from_stat_cache();
  }

  return ret;
}

// TODO: refactor is needed later.
int ObPartitionGroup::offline_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret));
  } else if (OB_FAIL(try_switch_partition_state(OFFLINING))) {
    STORAGE_LOG(WARN, "partition is doing other task, sleep and retry", K(ret), K(pkey_));
  } else if (OB_FAIL(pause_())) {
    STORAGE_LOG(WARN, "fail to pause partition", K(ret), K(pkey_));
  } else {
    erase_pg_from_election();
    erase_pg_from_stat_cache();

    if (OB_FAIL(try_switch_partition_state(OFFLINE))) {
      STORAGE_LOG(WARN, "switch partition state to OFFLINE failed", K(ret), K(pkey_));
    }
  }

  return ret;
}

int ObPartitionGroup::offline()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(replay_status_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "replay_status is NULL", K(ret), K(pkey_));
  } else {
    ObPGLockWithPendingReplayGuard guard(
        lock_, *replay_status_, pkey_, 0, PGLOCKREPLAY | PGLOCKCLOG | PGLOCKTRANS | PGLOCKSTORAGE);
    ret = offline_();
  }

  return ret;
}

int ObPartitionGroup::schema_drop(const bool for_replay, const uint64_t log_id, const bool is_physical_drop)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret));
  } else {
    ObPartitionGroupLockGuard guard(lock_, 0, PGLOCKTRANS | PGLOCKSTORAGE, for_replay /* trylock */);

    if (!guard.locked()) {
      ret = OB_EAGAIN;
    } else if (for_replay) {
      // Make a mark at ReplayStatus when the task of OFFLINE_PARTITION is submitted to
      // temporarily solve the deadlock of asynchronous OFFLINE_PARTITION task and migration task.
      replay_status_->offline_partition_task_submitted(log_id);
    }

    if (OB_SUCC(ret)) {
      const KillTransArg arg(true, false);
      bool is_all_trans_clear = false;
      if (OB_FAIL(set_offline_log_id(log_id))) {
        STORAGE_LOG(WARN, "fail to set_offline_log_id", K(ret), K(pkey_));
      } else if (pg_storage_.has_memstore()) {
        // The reason for calling the block partition interface of transaction when the offline partition
        // log is replayed at follower are as follows:
        // 1. to avoid read at follower.
        // 2. both the leader and follower need to call block partition.
        // 3. bugfix (12293135).
        if (OB_FAIL(txs_->block_partition(pkey_, is_all_trans_clear))) {
          STORAGE_LOG(ERROR, "block partition error in follower state", K(ret), K(pkey_));
        } else if (!is_all_trans_clear && OB_FAIL(txs_->kill_all_trans(pkey_, arg, is_all_trans_clear))) {
          STORAGE_LOG(WARN, "kill all trans failed", K(ret), K(pkey_));
        } else if (!is_all_trans_clear && OB_FAIL(txs_->wait_all_trans_clear(pkey_))) {
          if (OB_EAGAIN != ret) {
            STORAGE_LOG(WARN, "wait all trans clear failed", K(ret), K(pkey_));
          }
        } else if (is_physical_drop && OB_FAIL(clear_non_reused_stores_(pkey_))) {
          STORAGE_LOG(WARN, "failed to clear pg stores", K(ret), K(pkey_));
        } else if (OB_FAIL(report_clog_history_offline())) {
          STORAGE_LOG(WARN, "fail to report offline to clog hisory", K(ret), K(pkey_));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // Override the error code and let the upper layer try again
    ret = OB_EAGAIN;
  }

  return ret;
}

int ObPartitionGroup::get_create_ts(int64_t& create_ts)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(pg_storage_.get_pg_create_ts(create_ts))) {
    STORAGE_LOG(WARN, "get pg create ts fail", K(ret), K(pkey_));
  }

  return ret;
}

int ObPartitionGroup::submit_add_partition_to_pg_log(const obrpc::ObCreatePartitionArg& arg, ObPartitionService* ps,
    uint64_t& log_id, int64_t& log_ts, ObAddPartitionToPGLogCb*& out_cb)
{
  int ret = OB_SUCCESS;
  ObStorageLogType log_type = OB_LOG_ADD_PARTITION_TO_PG;
  ObAddPartitionToPGLogCb* cb = NULL;
  ObPartitionState partition_state;
  ObAddPartitionToPGLog log;
  char* buf = NULL;
  // 64K
  const int64_t MAX_LOG_SIZE = (common::OB_MAX_LOG_ALLOWED_SIZE >> 5);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret));
  } else if (!ObStorageLogTypeChecker::is_add_partition_to_pg_log(log_type) || !arg.is_valid() || OB_ISNULL(ps)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(log_type), K(arg), KP(ps));
  } else if (OFFLINE == (partition_state = get_partition_state()) || OFFLINING == partition_state ||
             REMOVE == partition_state) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "state not match", K(ret), K(partition_state));
  } else if (!pkey_.is_valid() || !pkey_.is_pg()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected pkey", K(ret), K_(pkey));
  } else if (!share::ObMultiClusterUtil::is_cluster_allow_submit_log(pkey_.get_table_id())) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "is_cluster_allow_submit_log return false", K(ret), K(pkey_));
  } else if (OB_ISNULL(cb = op_alloc(ObAddPartitionToPGLogCb))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "alloc memory fail", K(ret), KP(cb));
  } else if (NULL == (buf = reinterpret_cast<char*>(ob_malloc(MAX_LOG_SIZE, ObModIds::OB_PARTITION_LOG_BUF)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "allocate memory failed", K(ret));
  } else {
    if (OB_FAIL(cb->init(log_type, pkey_, arg.partition_key_))) {
      STORAGE_LOG(WARN, "ObAddPartitionToPGLogCb init error", K(ret), K(log_type), K(arg));
    } else if (OB_FAIL(log.init(log_type, arg))) {
      STORAGE_LOG(WARN, "ObPGPartitionChangeLog init error", K(ret), K(log_type), K(arg));
    } else {
      int64_t pos = 0;
      int64_t log_timestamp = 0;
      const int64_t base_timestamp = 0;
      const bool is_trans_log = false;
      if (OB_SUCCESS != (ret = serialization::encode_i64(buf, MAX_LOG_SIZE, pos, log_type))) {
        STORAGE_LOG(WARN, "serialize log_type error", K(ret), K(log_type));
      } else if (OB_FAIL(log.serialize(buf, MAX_LOG_SIZE, pos))) {
        STORAGE_LOG(WARN, "serialize log failed", K(ret));
      } else if (OB_FAIL(pls_->submit_log(buf, pos, base_timestamp, cb, is_trans_log, log_id, log_timestamp))) {
        STORAGE_LOG(WARN, "submit add partition to pg log error", K(ret), K(log_type), K_(pkey));
      } else {
        out_cb = cb;
        log_ts = log_timestamp;
        STORAGE_LOG(INFO, "submit add partition to pg log success", K(log_type), K(log_ts), K(arg));
      }
    }
    if (NULL != buf) {
      ob_free(buf);
      buf = NULL;
    }
  }
  if (OB_FAIL(ret) && NULL != cb) {
    op_free(cb);
    cb = NULL;
  }

  return ret;
}

int ObPartitionGroup::submit_partition_schema_change_log(const common::ObPartitionKey& pkey,
    const int64_t schema_version, const uint64_t index_id, ObPartitionService* ps, uint64_t& log_id, int64_t& log_ts,
    ObSchemaChangeClogCb*& out_cb)
{
  int ret = OB_SUCCESS;
  ObStorageLogType log_type = OB_PARTITION_SCHEMA_VERSION_CHANGE_LOG;
  ObSchemaChangeClogCb* cb = NULL;
  ObPartitionState partition_state;
  ObPGSchemaChangeLog log;
  char* buf = NULL;
  // 64K
  const int64_t MAX_LOG_SIZE = (common::OB_MAX_LOG_ALLOWED_SIZE >> 5);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret));
  } else if (!ObStorageLogTypeChecker::is_schema_version_change_log(log_type) || !pkey.is_valid() ||
             schema_version < 0 || OB_ISNULL(ps)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(log_type), K(pkey_), K(pkey), K(schema_version), KP(ps));
  } else if (OFFLINE == (partition_state = get_partition_state()) || OFFLINING == partition_state ||
             REMOVE == partition_state) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "state not match", K(ret), K(partition_state));
  } else if (!share::ObMultiClusterUtil::is_cluster_allow_submit_log(pkey_.get_table_id())) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "is_cluster_allow_submit_log return false", K(ret), K(pkey_));
  } else if (OB_ISNULL(cb = op_alloc(ObSchemaChangeClogCb))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "alloc memory fail", K(ret), KP(cb));
  } else if (NULL == (buf = reinterpret_cast<char*>(ob_malloc(MAX_LOG_SIZE, ObModIds::OB_PARTITION_LOG_BUF)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "allocate memory failed", K(ret));
  } else {
    if (OB_FAIL(cb->init(log_type, pkey_, pkey))) {
      STORAGE_LOG(WARN, "ObSchemaChangeClogCb init error", K(ret), K(log_type), K(pkey_), K(pkey));
    } else if (OB_FAIL(log.init(log_type, pkey_, pkey, schema_version, index_id))) {
      STORAGE_LOG(WARN,
          "ObPGPartitionChangeLog init error",
          K(ret),
          K(log_type),
          K(pkey_),
          K(pkey),
          K(schema_version),
          K(index_id));
    } else {
      int64_t pos = 0;
      const int64_t base_timestamp = 0;
      const bool is_trans_log = false;
      if (OB_FAIL(serialization::encode_i64(buf, MAX_LOG_SIZE, pos, log_type))) {
        STORAGE_LOG(WARN, "serialize log_type error", K(ret), K(log_type));
      } else if (OB_FAIL(log.serialize(buf, MAX_LOG_SIZE, pos))) {
        STORAGE_LOG(WARN, "serialize log failed", K(ret));
      } else if (OB_FAIL(pls_->submit_log(buf, pos, base_timestamp, cb, is_trans_log, log_id, log_ts))) {
        STORAGE_LOG(
            WARN, "submit partition schema version change log error", K(ret), K(log_type), K_(pkey), K(schema_version));
      } else {
        out_cb = cb;
        STORAGE_LOG(
            WARN, "submit partition schema version change log success", K(log_type), K_(pkey), K(schema_version));
      }
    }
    if (NULL != buf) {
      ob_free(buf);
      buf = NULL;
    }
  }
  if (OB_FAIL(ret) && NULL != cb) {
    op_free(cb);
    cb = NULL;
  }

  return ret;
}

int ObPartitionGroup::remove_partition_from_pg(
    const bool for_replay, const ObPartitionKey& pkey, const bool write_slog_trans, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  ObPartitionState partition_state;
  ObPGPartitionGuard partition_guard;
  ObPartitionKey trans_table_pkey;
  ObPGPartitionGuard trans_partition_guard;
  ObPartitionGroupLockGuard guard(lock_, 0, PGLOCKSTORAGE, for_replay /* trylock */);

  if (!guard.locked()) {
    ret = OB_EAGAIN;
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret));
  } else if (!pkey.is_valid() || pkey.is_pg()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OFFLINE == (partition_state = get_partition_state()) || OFFLINING == partition_state ||
             REMOVE == partition_state) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "state not match", K(ret), K(partition_state));
    // pg partition
  } else if (OB_FAIL(get_pg_partition(pkey, partition_guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("pg partition is already removed, no need to remove again", K(ret), K(pkey), "pg_key", pkey_);
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get_pg_partition", K(ret), K(pkey), "pg key", pkey_);
    }
  } else if (OB_FAIL(clear_non_reused_stores_(pkey))) {
    STORAGE_LOG(WARN, "clear pg partition non reused stores error", K(ret), K(pkey), K(pkey_));
  } else if (OB_FAIL(pg_storage_.remove_pg_partition_from_pg(pkey, write_slog_trans, log_id))) {
    STORAGE_LOG(WARN, "remove pg partition from pg error", K(ret), K(pkey), K(pkey_));
  } else {
    STORAGE_LOG(INFO, "remove pg partition from pg success", K(pkey), K(pkey_), K(log_id));
  }

  return ret;
}

int ObPartitionGroup::submit_remove_partition_from_pg_log(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  ObStorageLogType log_type = OB_LOG_REMOVE_PARTITION_FROM_PG;
  const int64_t REMOVE_PARTITION_FROM_PG_BUF_LEN = 256;
  ObRemovePartitionFromPGLogCb* cb = NULL;
  ObPartitionState partition_state;
  ObRemovePartitionFromPGLog log;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (!share::ObMultiClusterUtil::is_cluster_allow_submit_log(pkey_.get_table_id())) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "is_cluster_allow_submit_log return false", K(ret), K(pkey_));
  } else if (OFFLINE == (partition_state = get_partition_state()) || OFFLINING == partition_state ||
             REMOVE == partition_state) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "state not match", K(ret), K(partition_state));
  } else if (OB_ISNULL(cb = op_alloc(ObRemovePartitionFromPGLogCb))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "alloc memory fail", K(ret), KP(cb));
  } else if (OB_FAIL(cb->init(log_type, pkey_, pkey, &(ps_->get_callback_async_worker())))) {
    STORAGE_LOG(WARN, "ObRemovePartitionFromPGLogCb init error", K(ret), K(log_type), K(pkey));
  } else if (OB_FAIL(log.init(log_type, pkey_, pkey))) {
    STORAGE_LOG(WARN, "ObRemovePartitionFromPGLog init error", K(ret), K(log_type), K(pkey), K(pkey_));
  } else {
    char buf[REMOVE_PARTITION_FROM_PG_BUF_LEN];
    int64_t pos = 0;
    uint64_t log_id = 0;
    int64_t log_timestamp = 0;
    const int64_t base_timestamp = 0;
    const bool is_trans_log = false;
    if (OB_FAIL(serialization::encode_i64(buf, REMOVE_PARTITION_FROM_PG_BUF_LEN, pos, log_type))) {
      STORAGE_LOG(WARN, "serialize log_type error", K(ret), K(log_type));
    } else if (OB_FAIL(log.serialize(buf, REMOVE_PARTITION_FROM_PG_BUF_LEN, pos))) {
      STORAGE_LOG(WARN, "serialize log failed", K(ret));
    } else if (OB_SUCCESS !=
               (ret = pls_->submit_log(buf, pos, base_timestamp, cb, is_trans_log, log_id, log_timestamp))) {
      STORAGE_LOG(WARN, "submit remove partition from pg error", K(ret), K(log_type), K_(pkey));
    } else {
      // do nothing
    }
  }
  STORAGE_LOG(INFO, "submit remove partition from pg log", K(ret), K(pkey), K(pkey_));
  if (OB_FAIL(ret) && NULL != cb) {
    op_free(cb);
    cb = NULL;
  }

  return ret;
}

bool ObPartitionGroup::is_removed() const
{
  return (REMOVE == get_partition_state());
}

int ObPartitionGroup::check_is_in_member_list(bool& is_in_member_list) const
{
  int ret = OB_SUCCESS;
  is_in_member_list = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret));
  } else if (OB_ISNULL(pls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition log service is empty", K_(pkey), K(ret));
  } else {
    is_in_member_list = pls_->is_svr_in_member_list(GCTX.self_addr_);
  }
  return ret;
}

int ObPartitionGroup::offline_itself(const bool is_physical_drop)
{
  int ret = OB_SUCCESS;
  const int64_t OFFLINE_PARTITION_LOG_BUF_LEN = 256;
  char buf[OFFLINE_PARTITION_LOG_BUF_LEN] = "";
  int64_t pos = 0;
  uint64_t log_id = 0;
  int64_t log_timestamp = 0;
  const int64_t base_timestamp = 0;
  const bool is_trans_log = false;
  ObPartitionState partition_state;
  ObOfflinePartitionCb* cb = NULL;
  // New log types will be written after 226 version
  ObStorageLogType log_type =
      GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2260 ? OB_LOG_OFFLINE_PARTITION : OB_LOG_OFFLINE_PARTITION_V2;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret));
  } else if (!share::ObMultiClusterUtil::is_cluster_allow_submit_log(pkey_.get_table_id())) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "is_cluster_allow_submit_log return false", K(ret), K(pkey_));
  } else if (OFFLINE == (partition_state = get_partition_state()) || OFFLINING == partition_state ||
             REMOVE == partition_state) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "state not match", K(ret), K(partition_state));
  } else if (OB_ISNULL(cb = op_alloc(ObOfflinePartitionCb))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "alloc memory fail", K(ret), KP(cb));
  } else if (cb->init(&(ps_->get_callback_async_worker()), is_physical_drop)) {
    STORAGE_LOG(WARN, "ObOfflinePartitionCb init error", K(ret), K(pkey_));
  } else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, OFFLINE_PARTITION_LOG_BUF_LEN, pos, log_type))) {
    STORAGE_LOG(WARN, "serialize log_type error", K(ret), K(log_type));
  } else {
    if (OB_LOG_OFFLINE_PARTITION == log_type) {
      // do nothing
    } else {
      ObOfflinePartitionLog log;
      if (OB_FAIL(log.init(log_type, is_physical_drop))) {
        STORAGE_LOG(WARN, "failed to init offline log", KR(ret), K(log_type), K(is_physical_drop));
      } else if (OB_FAIL(log.serialize(buf, OFFLINE_PARTITION_LOG_BUF_LEN, pos))) {
        STORAGE_LOG(WARN, "serialize offline log error", KR(ret), K(log_type), K(is_physical_drop));
      } else { /*do nothing*/
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(pls_->submit_log(buf, pos, base_timestamp, cb, is_trans_log, log_id, log_timestamp))) {
        STORAGE_LOG(WARN, "submit offline log error", K(ret), K(log_type), K_(pkey));
      }
    }
  }

  if (OB_FAIL(ret) && NULL != cb) {
    op_free(cb);
    cb = NULL;
  }

  STORAGE_LOG(INFO, "offline_itself", K(ret), K_(pkey), K(log_type), K(is_physical_drop));

  return ret;
}

int ObPartitionGroup::replay_partition_meta_log(
    const ObStorageLogType log_type, const int64_t log_id, const char* buf, const int64_t size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not inited", K(ret), K(pkey_));
  } else if (OB_LOG_PARTITION_SCHEMA == log_type) {
    if (OB_FAIL(pg_storage_.replay_schema_log(buf, size, log_id))) {
      STORAGE_LOG(WARN, "fail to replay schema log", K(ret), K(pkey_));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "unknown meta log type", K(ret), K(pkey_), K(log_type));
  }

  return ret;
}

int ObPartitionGroup::set_wait_split()
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg(__func__, 1000000);
  SpinWLockGuard guard(split_lock_);
  tg.click();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K_(pkey), K(ret));
  } else if (OB_FAIL(split_state_.switch_state(WAIT_SPLIT))) {
    STORAGE_LOG(WARN, "switch split state failed", K(ret), K_(pkey));
  } else {
    // do nothing
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "set wait split failed", K(ret), K_(pkey));
  } else {
    STORAGE_LOG(INFO, "set wait split success", K_(pkey));
  }
  return ret;
}

int ObPartitionGroup::save_split_state(const bool write_slog)
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg(__func__, 1000000);
  SpinRLockGuard guard(split_lock_);
  tg.click();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K_(pkey), K(ret));
  } else {
    ret = save_split_state_(split_state_.get_persistent_state(), write_slog);
  }
  return ret;
}

int ObPartitionGroup::restore_split_state(const int state)
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg(__func__, 1000000);
  SpinRLockGuard guard(split_lock_);
  tg.click();
  if (OB_FAIL(split_state_.set_state(static_cast<ObPartitionSplitStateEnum>(state)))) {
    STORAGE_LOG(WARN,
        "restore split state failed",
        K(ret),
        K_(pkey),
        "state",
        to_state_str(static_cast<ObPartitionSplitStateEnum>(state)));
  } else {
    STORAGE_LOG(INFO,
        "restore split state success",
        K_(pkey),
        "state",
        to_state_str(static_cast<ObPartitionSplitStateEnum>(state)));
  }
  return ret;
}

int ObPartitionGroup::restore_split_info(const ObPartitionSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg(__func__, 1000000);
  SpinRLockGuard guard(split_lock_);
  tg.click();
  if (OB_FAIL(split_info_.assign(split_info))) {
    STORAGE_LOG(WARN, "restore split info failed", K(ret), K_(pkey), K(split_info));
  } else {
    STORAGE_LOG(INFO, "restore split info success", K_(pkey), K(split_info));
  }
  return ret;
}

int ObPartitionGroup::replay_split_source_log(
    const ObPartitionSplitSourceLog& log, const uint64_t log_id, const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  bool write_slog = false;
  const int64_t start_ts = ObTimeUtility::current_time();
  bool is_dest_partition_ready = false;
  ObTimeGuard tg(__func__, 1000000);
  ObPartitionGroupLockGuard guard(lock_, 0, PGLOCKSTORAGE, true /* trylock */);
  SpinWLockGuard split_guard(split_lock_);
  tg.click();
  if (!guard.locked()) {
    ret = OB_EAGAIN;
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K_(pkey), K(ret));
  } else if (!log.is_valid() || !is_valid_log_id(log_id) || 0 >= log_ts) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(log), K(log_id), K(log_ts));
  } else if (OB_FAIL(check_if_dest_pg_ready_(log.get_spp().get_dest_array(), is_dest_partition_ready))) {
    STORAGE_LOG(WARN, "check if dest partition group ready failed", K(ret), K(log));
  } else if (!is_dest_partition_ready) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "dest partition group is not ready, need retry", K(ret), K(log));
  } else if (OB_FAIL(txs_->checkpoint(pkey_, log.get_slave_read_ts(), NULL))) {
    STORAGE_LOG(WARN, "split srouce log replay to checkpoint error", K(ret), K(log), K_(pkey), K(log_id), K(log_ts));
  } else if (is_dest_split(split_state_.get_state())) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "dest partition is splitting", K(ret), K_(split_state));
    int tmp_ret = OB_SUCCESS;
    int unused_progress = UNKNOWN_SPLIT_PROGRESS;
    // drive dest partition split state
    if (OB_SUCCESS != (tmp_ret = get_dest_split_progress_(unused_progress))) {
      STORAGE_LOG(WARN, "get dest split progress failed", K(tmp_ret), K_(pkey));
    }
  } else if (!partition_split_progress_array_.is_inited() &&
             OB_FAIL(partition_split_progress_array_.init(log.get_spp().get_dest_array()))) {
    STORAGE_LOG(WARN, "init partition split progress array failed", K(ret));
  } else {
    int64_t split_version = log_ts + SPLIT_FREEZE_WAIT_TS;
    bool log_success = is_split_source_log_success(split_state_.get_state());
    // check if need write slog before switch state
    write_slog = (log_success ? false : true);
    if (OB_FAIL(split_state_.switch_state(REPLAY_SOURCE_SPLIT_LOG))) {
      STORAGE_LOG(WARN, "switch split state failed", K(ret), K_(pkey));
    } else {
      split_info_.set_split_version(split_version);
      split_info_.set_source_log_id(log_id);
      split_info_.set_source_log_ts(log_ts);
      if (OB_FAIL(
              split_info_.set(log.get_schema_version(), log.get_spp(), ObPartitionSplitInfo::SPLIT_SOURCE_PARTITION))) {
      } else if (OB_FAIL(shutdown_(split_version, log_id, log.get_schema_version()))) {
        STORAGE_LOG(WARN, "shutdown source partition failed", K(ret));
      } else if (OB_FAIL(E(EventTable::EN_REPLAY_SOURCE_SPLIT_LOG_FAILED) OB_SUCCESS)) {
        STORAGE_LOG(WARN, "ERRSIM: EN_REPLAY_SOURCE_SPLIT_LOG_FAILED", K(ret), K_(pkey));
      } else if (OB_FAIL(push_reference_tables_(log.get_spp().get_dest_array(), split_version))) {
        STORAGE_LOG(WARN, "push reference tables failed", K(ret));
      } else if (OB_FAIL(save_split_info_(split_info_, write_slog))) {
        STORAGE_LOG(WARN, "save split info failed", K(ret), K_(pkey));
      } else if (OB_FAIL(save_split_state_(split_state_.get_persistent_state(), write_slog))) {
        STORAGE_LOG(WARN, "save split state failed", K(ret), K_(pkey));
      } else {
        // do nothing
      }
      if (OB_SUCCESS != ret) {
        (void)split_state_.restore_state();
      }
    }
  }

  const int64_t end_ts = ObTimeUtility::current_time();
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN,
        "replay split source log failed",
        K(ret),
        K_(pkey),
        K(log),
        K(log_id),
        K(log_ts),
        "used_time",
        end_ts - start_ts);
  } else {
    STORAGE_LOG(INFO,
        "replay split source log success",
        K_(pkey),
        K(log),
        K(log_id),
        K(log_ts),
        K(write_slog),
        "used_time",
        end_ts - start_ts);
  }
  return ret;
}

int ObPartitionGroup::replay_split_dest_log(const ObPartitionSplitDestLog& log)
{
  int ret = OB_SUCCESS;
  bool write_slog = false;
  bool is_empty_pg = pg_storage_.is_empty_pg();
  const int64_t start_ts = ObTimeUtility::current_time();
  ObTimeGuard tg(__func__, 1000000);
  SpinWLockGuard guard(split_lock_);
  tg.click();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K_(pkey), K(ret));
  } else if (OB_FAIL(E(EventTable::EN_REPLAY_SPLIT_DEST_LOG_FAILED) OB_SUCCESS)) {
    STORAGE_LOG(WARN, "ERRSIM: EN_REPLAY_SPLIT_DEST_LOG_FAILED", K(ret), K_(pkey));
  } else if (OB_UNLIKELY(!log.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(log));
  } else if (log.get_schema_version() < split_info_.get_schema_version()) {
    STORAGE_LOG(INFO, "ignore replay split dest log", K(log), K_(split_info));
  } else {
    write_slog = (is_split_dest_log_success(split_state_.get_state()) ? false : true);
    if (OB_FAIL(split_state_.switch_state(REPLAY_DEST_SPLIT_LOG))) {
      STORAGE_LOG(WARN, "switch split state failed", K(ret), K_(pkey));
    } else {
      int64_t split_state = split_state_.get_persistent_state();
      if (is_empty_pg) {
        split_state = FOLLOWER_INIT;
      }
      split_info_.set_split_version(log.get_split_version());
      split_info_.set_source_log_id(log.get_source_log_id());
      split_info_.set_source_log_ts(log.get_source_log_ts());
      if (OB_FAIL(
              split_info_.set(log.get_schema_version(), log.get_spp(), ObPartitionSplitInfo::SPLIT_DEST_PARTITION))) {
      } else if (OB_FAIL(save_split_info_(split_info_, write_slog))) {
        STORAGE_LOG(WARN, "save split info failed", K(ret), K_(pkey));
      } else if (OB_FAIL(save_split_state_(split_state, write_slog))) {
        STORAGE_LOG(WARN, "save split state failed", K(ret), K_(pkey));
      } else {
        // do nothing
      }
      if (OB_SUCCESS != ret) {
        (void)split_state_.restore_state();
      }
    }
  }
  const int64_t end_ts = ObTimeUtility::current_time();
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "replay split dest log failed", K(ret), K_(pkey), K(log), "used_time", end_ts - start_ts);
  } else {
    STORAGE_LOG(INFO, "replay split dest log success", K_(pkey), K(log), K(write_slog), "used_time", end_ts - start_ts);
  }
  return ret;
}

int ObPartitionGroup::sync_split_source_log_success(const int64_t log_id, const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  const bool write_slog = true;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObTimeGuard tg(__func__, 1000000);
  ObPartitionGroupLockGuard guard(lock_, 0, PGLOCKSTORAGE);
  SpinWLockGuard split_guard(split_lock_);
  tg.click();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K_(pkey), K(ret));
  } else if (!is_valid_log_id(log_id) || 0 >= log_ts) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(log_id), K(log_ts));
  } else if (OB_FAIL(split_state_.switch_state(SOURCE_SPLIT_LOG_REACH_MAJORITY))) {
    STORAGE_LOG(WARN, "switch split state failed", K(ret));
  } else {
    split_info_.set_split_version(log_ts + SPLIT_FREEZE_WAIT_TS);
    split_info_.set_source_log_id(log_id);
    split_info_.set_source_log_ts(log_ts);

    do {
      if (OB_FAIL(E(EventTable::EN_SAVE_SPLIT_STATE_FAILED) OB_SUCCESS)) {
        STORAGE_LOG(WARN, "ERRSIM: EN_PREPARE_SPLIT_FAILED", K(ret), K_(pkey));
      } else if (OB_FAIL(save_split_info_(split_info_, write_slog))) {
        STORAGE_LOG(WARN, "save split info failed", K(ret), K_(pkey));
      }
    } while (OB_CS_OUTOF_DISK_SPACE == ret || OB_SLOG_REACH_MAX_CONCURRENCY == ret || OB_ALLOCATE_MEMORY_FAILED == ret);

    do {
      if (OB_FAIL(save_split_state_(split_state_.get_persistent_state(), write_slog))) {
        STORAGE_LOG(WARN, "save split state failed", K(ret), K_(pkey));
      }
    } while (OB_CS_OUTOF_DISK_SPACE == ret || OB_SLOG_REACH_MAX_CONCURRENCY == ret || OB_ALLOCATE_MEMORY_FAILED == ret);

    if (OB_SUCCESS != ret) {
      (void)split_state_.restore_state();
    }
  }

  const int64_t end_ts = ObTimeUtility::current_time();
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN,
        "sync split source log failed",
        K(ret),
        K_(pkey),
        K_(split_info),
        K(log_id),
        K(log_ts),
        "used_time",
        end_ts - start_ts);
  } else {
    STORAGE_LOG(INFO,
        "sync split source log success",
        K_(pkey),
        K_(split_info),
        K(log_id),
        K(log_ts),
        "used_time",
        end_ts - start_ts);
  }
  return ret;
}

int ObPartitionGroup::sync_split_dest_log_success()
{
  int ret = OB_SUCCESS;
  const bool write_slog = true;
  const int64_t start_ts = ObTimeUtility::current_time();
  bool is_empty_pg = pg_storage_.is_empty_pg();
  ObTimeGuard tg(__func__, 1000000);
  SpinWLockGuard guard(split_lock_);
  tg.click();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K_(pkey), K(ret));
  } else if (OB_FAIL(split_state_.switch_state(DEST_SPLIT_LOG_REACH_MAJORITY))) {
    STORAGE_LOG(WARN, "switch split state failed", K(ret));
  } else {
    int64_t split_state = split_state_.get_persistent_state();
    if (is_empty_pg) {
      split_state = FOLLOWER_INIT;
    }
    do {
      if (OB_FAIL(E(EventTable::EN_SAVE_SPLIT_STATE_FAILED) OB_SUCCESS)) {
        STORAGE_LOG(WARN, "ERRSIM: EN_PREPARE_SPLIT_FAILED", K(ret), K_(pkey));
      } else if (OB_FAIL(save_split_info_(split_info_, write_slog))) {
        STORAGE_LOG(WARN, "save split info failed", K(ret), K_(pkey));
      }
    } while (OB_CS_OUTOF_DISK_SPACE == ret || OB_SLOG_REACH_MAX_CONCURRENCY == ret || OB_ALLOCATE_MEMORY_FAILED == ret);

    do {
      if (OB_FAIL(save_split_state_(split_state, write_slog))) {
        STORAGE_LOG(WARN, "save split state failed", K(ret), K_(pkey));
      }
    } while (OB_CS_OUTOF_DISK_SPACE == ret || OB_SLOG_REACH_MAX_CONCURRENCY == ret || OB_ALLOCATE_MEMORY_FAILED == ret);

    if (OB_SUCCESS != ret) {
      (void)split_state_.restore_state();
    }
  }
  const int64_t end_ts = ObTimeUtility::current_time();
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "sync split dest log failed", K(ret), K_(pkey), "used_time", end_ts - start_ts);
  } else {
    STORAGE_LOG(INFO, "sync split dest log success", K_(pkey), "used_time", end_ts - start_ts);
  }
  return ret;
}

int ObPartitionGroup::prepare_splitting(
    const ObPartitionSplitInfo& split_info, const ObMemberList& mlist, const ObAddr& leader)
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg(__func__, 1000000);
  SpinWLockGuard guard(split_lock_);
  tg.click();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K_(pkey), K(ret));
  } else if (OB_UNLIKELY(!split_info.is_valid()) || OB_UNLIKELY(!mlist.is_valid()) || OB_UNLIKELY(!leader.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(split_info), K(mlist), K(leader));
  } else if (is_split_blocked_by_mc_) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "partition is changing member list", K(ret), K_(pkey));
  } else if (ObReplicaTypeCheck::is_readonly_replica(get_replica_type())) {
    STORAGE_LOG(INFO, "readonly replica do nothing", K_(pkey));
  } else if (!is_physical_split_finished_()) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "source partition physical split not finished", K(ret), K_(pkey));
  } else if (ps_->get_self_addr() == leader) {
    ObMemberList leader_mlist;
    if (!is_leader_state(get_partition_state())) {
      ret = OB_NOT_MASTER;
      STORAGE_LOG(WARN, "not master", K(ret), K_(pkey));
    } else if (OB_FAIL(get_leader_curr_member_list_(leader_mlist))) {
      STORAGE_LOG(WARN, "get leader current member list failed", K(ret), K_(pkey));
    } else if (!mlist.member_addr_equal(leader_mlist)) {
      ret = OB_STATE_NOT_MATCH;
      STORAGE_LOG(WARN, "member list not match", K(ret), K_(pkey), K(leader_mlist), K(mlist));
    } else {
      // do nothing
    }
  } else {
    // do nothing
  }
  // Avoid writing multiple slogs
  if (OB_SUCC(ret) && split_info.get_schema_version() > split_info_.get_schema_version()) {
    const bool write_slog = true;
    split_info_.reset();
    if (OB_FAIL(split_info_.assign(split_info))) {
      STORAGE_LOG(WARN, "assign split info failed", K(ret), K_(pkey));
    } else if (OB_FAIL(save_split_info_(split_info_, write_slog))) {
      STORAGE_LOG(WARN, "save split info failed", K(ret), K_(pkey));
    } else {
      STORAGE_LOG(INFO, "save split info success", K_(pkey), K_(split_info));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(E(EventTable::EN_PREPARE_SPLIT_FAILED) OB_SUCCESS)) {
      STORAGE_LOG(WARN, "ERRSIM: EN_PREPARE_SPLIT_FAILED", K(ret), K_(pkey));
    }
  }

  STORAGE_LOG(INFO, "prepare splitting", K(ret), K(split_info), K(mlist), K(leader));

  return ret;
}

/**
 Description:
   1. The partition will trigger splitting after setting split_info. The transaction layer judges whether current
   partition is splitting according to the value of split_info.

 Concurrent situation:
   1. split_info should be set first and then check transactions before logical split;
   2. It means the partition will be split as long as the split_info is valid in the process of transaction and
   there will be a list of target partitions.
   3. The transaction layer needs to check the split_info with split_lock.

 WARNING:
   1. The transaction layer may have add the target partition into the participant list after the split_info set.
   The transaction commit process will be affected if the creating of split target partition is failed finally.
 */
int ObPartitionGroup::check_cur_partition_split(bool& is_split_partition)
{
  int ret = OB_SUCCESS;
  //  SpinWLockGuard guard(split_lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K_(pkey), K(ret));
  } else if (split_info_.is_valid()) {
    is_split_partition = true;
  } else {
    is_split_partition = false;
  }
  if (!(GET_MIN_CLUSTER_VERSION() > CLUSTER_VERSION_3100)) {
    is_split_partition = false;
  }

  return ret;
}

int ObPartitionGroup::split_source_partition(
    const int64_t schema_version, const ObSplitPartitionPair& spp, enum ObSplitProgress& partition_progress)
{
  int ret = OB_SUCCESS;
  ObMemberList member_list;
  ObSArray<ObAddr> member_array;
  ObTimeGuard tg(__func__, 1000000);
  bool is_all_dest_finished = false;
  SpinWLockGuard guard(split_lock_);
  tg.click();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K_(pkey), K(ret));
  } else if (OB_UNLIKELY(0 >= schema_version) || OB_UNLIKELY(!spp.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(schema_version), K(spp));
  } else if (!spp.is_source_partition(pkey_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "self is not source partition", K(ret), K_(pkey));
  } else if (spp.is_dest_partition(pkey_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "source partition is in dest partition array", K(ret), K_(pkey));
  } else if (schema_version < split_info_.get_schema_version()) {
    ret = OB_OLD_SCHEMA_VERSION;
    STORAGE_LOG(WARN, "obsolete partition-splitting request", K(ret), K(schema_version), K_(split_info));
  } else {
    if (!partition_split_progress_array_.is_inited()) {
      if (OB_FAIL(partition_split_progress_array_.init(spp.get_dest_array()))) {
        STORAGE_LOG(WARN, "init partition split progress array failed", K(ret));
      } else {
        partition_progress = IN_SPLITTING;
      }
    } else {
      int tmp_progress = UNKNOWN_SPLIT_PROGRESS;
      if (OB_FAIL(partition_split_progress_array_.get_min_progress(tmp_progress))) {
        STORAGE_LOG(WARN, "get min split progress failed", K(ret));
      } else {
        partition_progress = static_cast<enum ObSplitProgress>(tmp_progress);
      }
    }
  }
  tg.click();
  if (OB_SUCCESS == ret) {
    bool log_finished = false;
    bool is_1pc_trx_end = false;
    switch (split_state_.get_state()) {
      case LEADER_INIT: {
        if (OB_FAIL(split_state_.switch_state(GET_RS_SPLIT_REQUEST))) {
          STORAGE_LOG(WARN, "switch split state failed", K(ret));
        } else {
          if (OB_FAIL(txs_->block_partition(pkey_, is_1pc_trx_end))) {
            STORAGE_LOG(WARN, "block partition failed", K(ret));
          } else if (OB_FAIL(E(EventTable::EN_PUSH_TASK_FAILED) OB_SUCCESS)) {
            STORAGE_LOG(WARN, "ERRSIM: EN_PUSH_TASK_FAILED", K(ret), K_(pkey));
          } else if (OB_FAIL(push_split_task_(schema_version, spp))) {
            STORAGE_LOG(WARN, "push split task failed", K(ret), K(schema_version), K(spp));
          } else {
            // do nothing
          }
          if (OB_SUCCESS != ret) {
            (void)split_state_.restore_state();
          }
        }
        if (OB_SUCCESS != ret) {
          break;
        } else {
          // go through
        }
      }
      case SPLIT_START: {
        if (!is_1pc_trx_end) {
          const bool split_kill_trans = true;
          if (!split_kill_trans && GET_MIN_CLUSTER_VERSION() > CLUSTER_VERSION_3100) {
            if (OB_FAIL(txs_->submit_log_for_split(pkey_, log_finished))) {
              STORAGE_LOG(WARN, "submit_log_for_split failed", K(ret));
            } else if (log_finished) {
              if (OB_FAIL(txs_->wait_1pc_trx_end(pkey_))) {
                if (OB_EAGAIN != ret) {
                  STORAGE_LOG(WARN, "wait 1pc trx end failed", K(ret));
                } else {
                  if (EXECUTE_COUNT_PER_SEC(1)) {
                    STORAGE_LOG(WARN, "wait 1pc trx end failed", K(ret));
                  }
                }
              } else {
                is_1pc_trx_end = true;
              }
            }
          } else {
            const KillTransArg arg(true, true, false);
            if (OB_FAIL(txs_->kill_all_trans(pkey_, arg, is_1pc_trx_end))) {
              STORAGE_LOG(WARN, "kill all trans failed", K(ret));
            }
          }
        }
        if (OB_SUCCESS == ret && is_1pc_trx_end) {
          split_trans_clear_ts_ = MonotonicTs::current_time();
          if (OB_FAIL(split_state_.switch_state(ALL_TRANS_CLEAR))) {
            STORAGE_LOG(WARN, "switch split state failed", K(ret));
          }
        }
        if (OB_SUCCESS != ret || !is_1pc_trx_end) {
          break;
        } else {
          // go through
        }
      }
      case SPLIT_TRANS_CLEAR: {
        ObPartitionSplitSourceLog log;
        int64_t gts = 0;
        int64_t base_ts = 0;
        MonotonicTs unused_ts = MonotonicTs::current_time();
        const transaction::MonotonicTs split_trans_clear_ts =
            split_trans_clear_ts_ + transaction::MonotonicTs(SPLIT_FREEZE_WAIT_TS);
        if (OB_FAIL(OB_TS_MGR.get_gts(pkey_.get_tenant_id(), split_trans_clear_ts, NULL, gts, unused_ts))) {
          if (OB_EAGAIN != ret) {
            STORAGE_LOG(WARN, "get gts failed", K(ret));
          } else {
            // rewrite ret
            ret = OB_SUCCESS;
          }
        } else if (OB_FAIL(decide_split_version_(gts, base_ts))) {
          STORAGE_LOG(WARN, "fail to decide split version", K(ret), K_(pkey));
        } else if (OB_FAIL(split_state_.switch_state(SUBMIT_SOURCE_SPLIT_LOG_SUCCESS))) {
          STORAGE_LOG(WARN, "switch split state failed", K(ret));
        } else {
          int64_t timestamp = 0;
          if (OB_FAIL(E(EventTable::EN_BLOCK_SUBMIT_SPLIT_SOURCE_LOG) OB_SUCCESS)) {
            STORAGE_LOG(WARN, "ERRSIM: EN_BLOCK_SUBMIT_SPLIT_SOURCE_LOG", K(ret), K_(pkey));
          } else if (OB_FAIL(pg_storage_.get_weak_read_timestamp(timestamp))) {
            STORAGE_LOG(WARN, "pg storage get weak read timestamp error", KR(ret), K_(pkey));
          } else if (OB_FAIL(log.init(schema_version, spp, timestamp))) {
            STORAGE_LOG(WARN, "split source log init failed", K(ret));
          } else if (OB_FAIL(submit_split_source_log_(log, base_ts))) {
            STORAGE_LOG(WARN, "submit split source log failed", K(ret));
          } else {
            STORAGE_LOG(INFO, "submit split source log success", K_(pkey));
          }
          if (OB_SUCCESS != ret) {
            (void)split_state_.restore_state();
          }
        }
        break;
      }
      case SPLIT_SOURCE_LOGGING: {
        STORAGE_LOG(INFO, "split source partition is logging");
        break;
      }
      case LEADER_SPLIT_SOURCE_LOG:
        // go through
      case FOLLOWER_SPLIT_SOURCE_LOG: {
        if (OB_FAIL(split_state_.switch_state(SOURCE_SHUTDOWN_SUCCESS))) {
          STORAGE_LOG(WARN, "switch split state failed", K(ret));
        } else {
          const int64_t split_version = split_info_.get_split_version();
          const int64_t log_id = split_info_.get_source_log_id();
          const int64_t schema_version = split_info_.get_schema_version();
          if (OB_FAIL(E(EventTable::EN_BLOCK_SHUTDOWN_PARTITION) OB_SUCCESS)) {
            STORAGE_LOG(WARN, "ERRSIM: EN_PUSH_REFERENCE_TABLE_FAIL", K(ret), K_(pkey));
          } else if (OB_FAIL(shutdown_(split_version, log_id, schema_version))) {
            STORAGE_LOG(WARN, "shutdown source partition failed", K(ret));
          }

          if (OB_SUCCESS != ret) {
            (void)split_state_.restore_state();
          }
        }
        break;
      }
      case SHUTDOWN_SUCCESS: {
        if (OB_FAIL(split_state_.switch_state(SET_REFERENCE_TABLE_SUCCESS))) {
          STORAGE_LOG(WARN, "switch split state failed", K(ret));
        } else {
          const int64_t split_version = split_info_.get_split_version();
          bool is_dest_partition_ready = false;
          if (OB_FAIL(E(EventTable::EN_PUSH_REFERENCE_TABLE_FAIL) OB_SUCCESS)) {
            STORAGE_LOG(WARN, "ERRSIM: EN_PUSH_REFERENCE_TABLE_FAIL", K(ret), K_(pkey));
          } else if (OB_FAIL(check_if_dest_pg_ready_(spp.get_dest_array(), is_dest_partition_ready))) {
            STORAGE_LOG(WARN, "check if dest partition group ready failed", K(ret), K_(pkey));
          } else if (!is_dest_partition_ready) {
            ret = OB_EAGAIN;
            STORAGE_LOG(WARN, "dest partition group is not ready, need retry", K(ret), K_(pkey));
          } else if (OB_FAIL(push_reference_tables_(split_info_.get_dest_partitions(), split_version))) {
            STORAGE_LOG(WARN, "push reference tables failed", K(ret), K(split_version));
          }

          if (OB_SUCCESS != ret) {
            (void)split_state_.restore_state();
          }
        }
        break;
      }
      case TABLE_REFERENCE_SUCCESS: {
        if (!is_all_dest_finished) {
          if (OB_FAIL(E(EventTable::EN_BLOCK_SPLIT_DEST_PARTITION) OB_SUCCESS)) {
            STORAGE_LOG(WARN, "ERRSIM: EN_BLOCK_SPLIT_DEST_PARTITION", K(ret), K_(pkey));
          } else if (OB_FAIL(split_dest_partitions_(is_all_dest_finished))) {
            STORAGE_LOG(WARN, "split dest partitions failed", K(ret));
          } else {
            // do nothing
          }
        }
        break;
      }
      case FOLLOWER_INIT:
        // go through
        break;
      default: {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected split state", K(ret), K_(split_state));
      }
    }
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN,
        "split source partition failed",
        K(ret),
        K_(pkey),
        K(schema_version),
        K(partition_progress),
        K(tg),
        K_(partition_split_progress_array));
  } else if (PHYSICAL_SPLIT_FINISH != partition_progress) {
    STORAGE_LOG(INFO,
        "receive split source partition request",
        K_(pkey),
        K(schema_version),
        K(partition_progress),
        K(tg),
        K_(partition_split_progress_array));
  } else {
    STORAGE_LOG(INFO,
        "split source partition success",
        K_(pkey),
        K(schema_version),
        K(partition_progress),
        K(tg),
        K_(partition_split_progress_array));
  }
  return ret;
}

int ObPartitionGroup::split_dest_partition(const ObPartitionSplitInfo& split_info, enum ObSplitProgress& progress)
{
  int ret = OB_SUCCESS;
  int tmp_progress = UNKNOWN_SPLIT_PROGRESS;
  ObTimeGuard tg(__func__, 1000000);
  ObMemberList mlist;
  ObSArray<ObAddr> marray;
  SpinWLockGuard guard(split_lock_);
  tg.click();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K_(pkey), K(ret));
  } else if (0 >= split_info.get_split_version() || 0 > split_info.get_schema_version() ||
             !is_valid_log_id(split_info.get_source_log_id()) || !split_info.get_spp().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(split_info));
  } else if (OB_FAIL(get_leader_curr_member_list_(mlist))) {
    STORAGE_LOG(WARN, "get leader curr member list failed", K(ret));
  } else if (split_info.get_schema_version() < split_info_.get_schema_version()) {
    progress = PHYSICAL_SPLIT_FINISH;
    STORAGE_LOG(WARN, "receive obsolete split dest request", K_(pkey), K(split_info), K(split_info));
  } else if (!replica_split_progress_array_.is_inited() || !mlist.member_addr_equal(saved_member_list_)) {
    replica_split_progress_array_.reset();
    if (OB_FAIL(mlist.get_addr_array(marray))) {
      STORAGE_LOG(WARN, "get addr array failed", K(ret), K(mlist));
    } else if (OB_FAIL(replica_split_progress_array_.init(marray))) {
      STORAGE_LOG(WARN, "init replica split progress array failed", K(ret), K(marray));
    } else {
      progress = IN_SPLITTING;
      saved_member_list_ = mlist;
    }
  } else if (OB_FAIL(replica_split_progress_array_.get_min_progress(tmp_progress))) {
    STORAGE_LOG(WARN, "get min progress failed", K(ret), K_(replica_split_progress_array));
  } else {
    progress = static_cast<ObSplitProgress>(tmp_progress);
  }
  if (OB_SUCCESS == ret && PHYSICAL_SPLIT_FINISH > progress) {
    if (OB_FAIL(query_replica_split_progress_(split_info.get_schema_version()))) {
      STORAGE_LOG(WARN, "query replica split progress failed", K(ret));
    }
  }
  bool is_complete = false;
  const int64_t split_version = split_info.get_split_version();
  if (OB_SUCCESS == ret && PHYSICAL_SPLIT_FINISH > progress) {
    switch (split_state_.get_state()) {
      case LEADER_INIT: {
        break;
      }
      case LEADER_WAIT_SPLIT: {
        if (OB_FAIL(check_complete(is_complete))) {
          STORAGE_LOG(WARN, "failed to check complete", K(ret), K(pkey_));
        } else if (!is_complete) {
          ret = OB_EAGAIN;
          if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
            REPLAY_LOG(WARN, "dest partition not complete, should retry", K(ret), K(pkey_));
          }
        } else if (OB_FAIL(OB_TS_MGR.wait_gts_elapse(pkey_.get_tenant_id(), split_version))) {
          STORAGE_LOG(WARN, "failed to wait gts elapse", K(ret), K(split_info));
        } else if (OB_FAIL(split_state_.switch_state(GET_SOURCE_SPLIT_REQUEST))) {
          STORAGE_LOG(WARN, "switch split state failed", K(ret));
        } else {
          ObPartitionSplitDestLog log;
          if (OB_FAIL(split_info_.init(
                  split_info.get_schema_version(), split_info.get_spp(), ObPartitionSplitInfo::SPLIT_DEST_PARTITION))) {
            STORAGE_LOG(WARN, "partition split info init failed", K(ret));
          } else {
            split_info_.set_split_version(split_version);
            split_info_.set_source_log_id(split_info.get_source_log_id());
            split_info_.set_source_log_ts(split_info.get_source_log_ts());
            if (OB_FAIL(log.init(split_version,
                    split_info.get_schema_version(),
                    split_info.get_source_log_id(),
                    split_info.get_source_log_ts(),
                    split_info.get_spp()))) {
              STORAGE_LOG(WARN, "split dest log init failed", K(ret));
            } else if (OB_FAIL(submit_split_dest_log_(log))) {
              STORAGE_LOG(WARN, "submit split dest log failed", K(ret), K(log));
            } else {
              // do nothing
            }
          }
          if (OB_SUCCESS != ret) {
            split_info_.reset();
            (void)split_state_.restore_state();
          }
        }
        break;
      }
      case SPLIT_DEST_LOGGING: {
        STORAGE_LOG(INFO, "split dest partition is logging");
        break;
      }
      case LEADER_LOGICAL_SPLIT_SUCCESS: {
        bool is_physical_split_finished = false;
        if (OB_FAIL(check_physical_split_(is_physical_split_finished))) {
          STORAGE_LOG(WARN, "check physical split failed", K(ret));
        } else if (is_physical_split_finished) {
          if (OB_FAIL(split_state_.switch_state(PHYSICAL_SPLIT_SUCCESS))) {
            STORAGE_LOG(WARN, "switch split state failed", K(ret));
          }
        } else {
          // do nothing
        }
        break;
      }
      case FOLLOWER_INIT:
        // go through
      case FOLLOWER_WAIT_SPLIT:
        // go through
      case FOLLOWER_LOGICAL_SPLIT_SUCCESS: {
        ret = OB_NOT_MASTER;
        STORAGE_LOG(WARN, "dest partition is not master", K(ret), K_(split_state));
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected split state", K(ret), K_(split_state));
      }
    }
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN,
        "split dest partition failed",
        K(ret),
        K_(pkey),
        K(tg),
        K(split_info),
        K(progress),
        K_(replica_split_progress_array));
  } else if (PHYSICAL_SPLIT_FINISH != progress) {
    STORAGE_LOG(INFO,
        "receive split dest partition request",
        K_(pkey),
        K(tg),
        K(split_info),
        K(progress),
        K_(replica_split_progress_array));
  } else {
    STORAGE_LOG(INFO, "split dest partition success", K_(pkey), K(tg), K(split_info));
  }
  return ret;
}

int ObPartitionGroup::push_reference_tables(const ObIArray<ObPartitionKey>& dest_array, const int64_t split_version)
{
  int ret = OB_SUCCESS;
  bool is_dest_partition_ready = false;
  SpinWLockGuard guard(split_lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(check_if_dest_pg_ready_(dest_array, is_dest_partition_ready))) {
    STORAGE_LOG(WARN, "check if dest partition group ready failed", K(ret), K(log));
  } else if (!is_dest_partition_ready) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "dest partition group is not ready, need retry", K(ret), K(log));
  } else if (OB_FAIL(push_reference_tables_(dest_array, split_version))) {
    STORAGE_LOG(WARN, "failed to push reference tables", K(ret), K(dest_array), K(split_version));
  }
  return ret;
}

int ObPartitionGroup::replay_split_state_slog(const ObSplitPartitionStateLogEntry& log_entry)
{
  int ret = OB_SUCCESS;
  const bool write_slog = false;
  ObTimeGuard tg(__func__, 1000000);
  SpinWLockGuard guard(split_lock_);
  tg.click();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (!log_entry.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(log_entry));
  } else if (OB_FAIL(split_state_.set_state(log_entry.get_state()))) {
    STORAGE_LOG(WARN, "set split state failed", K(ret));
  } else if (OB_FAIL(save_split_state_(log_entry.get_state(), write_slog))) {
    STORAGE_LOG(WARN, "save split state failed", K(ret), K(log_entry));
  } else {
    // do nothing
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "replay split state slog failed", K(ret), K_(pkey), K(log_entry));
  } else {
    STORAGE_LOG(INFO, "replay split state slog success", K_(pkey), K(log_entry));
  }
  return ret;
}

int ObPartitionGroup::replay_split_info_slog(const ObSplitPartitionInfoLogEntry& log_entry)
{
  int ret = OB_SUCCESS;
  const bool write_slog = false;
  ObTimeGuard tg(__func__, 1000000);
  SpinWLockGuard guard(split_lock_);
  tg.click();
  split_info_.set_split_version(log_entry.get_split_info().get_split_version());
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (!log_entry.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(log_entry));
  } else if (OB_FAIL(split_info_.set(log_entry.get_split_info().get_schema_version(),
                 log_entry.get_split_info().get_spp(),
                 log_entry.get_split_info().get_split_type()))) {
    STORAGE_LOG(WARN, "set split info failed", K(ret));
  } else if (OB_FAIL(save_split_info_(split_info_, write_slog))) {
    STORAGE_LOG(WARN, "save split info failed", K(ret), K(log_entry));
  } else {
    // do nothing
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "replay split info slog failed", K(ret), K_(pkey), K(log_entry));
  } else {
    STORAGE_LOG(INFO, "replay split info slog success", K_(pkey), K(log_entry));
  }
  return ret;
}

int ObPartitionGroup::set_dest_partition_split_progress(
    const int64_t schema_version, const ObPartitionKey& pkey, const int progress)
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg(__func__, 1000000);
  SpinWLockGuard guard(split_lock_);
  tg.click();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (0 > schema_version || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(schema_version), K_(pkey));
    // ignore warning
  } else if (schema_version < split_info_.get_schema_version()) {
    STORAGE_LOG(WARN, "schema version not match, ignore it", K(schema_version), K_(split_info));
  } else if (schema_version > split_info_.get_schema_version()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected schema version", K(ret), K(schema_version), K_(split_info));
  } else if (OB_FAIL(partition_split_progress_array_.set_progress(pkey, progress))) {
    STORAGE_LOG(WARN, "set progress failed", K(ret), K(schema_version), K(pkey), K(progress));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionGroup::get_reference_tables(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (0 >= index_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(index_id));
  } else {
    ret = get_reference_tables_(pkey, index_id, handle);
  }
  return ret;
}

int ObPartitionGroup::get_reference_memtables(ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    ret = pg_storage_.get_reference_memtables(handle);
  }
  return ret;
}

int ObPartitionGroup::get_all_table_ids(const ObPartitionKey& pkey, ObIArray<uint64_t>& index_tables)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    ret = get_all_table_ids_(pkey, index_tables);
  }
  return ret;
}

int ObPartitionGroup::set_reference_tables(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg(__func__, 1000000);
  SpinWLockGuard guard(split_lock_);
  tg.click();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (0 >= index_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(index_id));
  } else if (OB_FAIL(set_reference_tables_(pkey, index_id, handle))) {
    STORAGE_LOG(WARN, "set reference tables failed", K(ret), K(index_id));
  } else {
    // do nothing
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "set reference tables failed", K(ret), K_(pkey), K(index_id));
  } else {
    STORAGE_LOG(INFO, "set reference tables success", K_(pkey), K(index_id));
  }
  return ret;
}

int ObPartitionGroup::set_split_version(const int64_t split_version)
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg(__func__, 1000000);
  SpinWLockGuard guard(split_lock_);
  tg.click();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (0 >= split_version) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(split_version));
  } else {
    ret = set_split_version_(split_version);
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "set split version failed", K(ret), K_(pkey), K(split_version));
  } else {
    STORAGE_LOG(INFO, "set split version success", K_(pkey), K(split_version));
  }
  return ret;
}

int ObPartitionGroup::check_can_migrate(bool& can_migrate)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_storage_.check_can_migrate(can_migrate))) {
    STORAGE_LOG(WARN, "failed to check can migrate", K(ret));
  }
  return ret;
}

bool ObPartitionGroup::is_splitting() const
{
  ObTimeGuard tg(__func__, 1000000);
  SpinRLockGuard guard(split_lock_);
  tg.click();
  return in_splitting(split_state_.get_state());
}

bool ObPartitionGroup::is_split_source_partition() const
{
  return split_info_.get_src_partition() == pkey_;
}

bool ObPartitionGroup::is_in_dest_split() const
{
  ObTimeGuard tg(__func__, 1000000);
  SpinRLockGuard guard(split_lock_);
  tg.click();
  return is_dest_split(split_state_.get_state());
}

bool ObPartitionGroup::is_dest_logical_split_finish() const
{
  ObTimeGuard tg(__func__, 1000000);
  SpinRLockGuard guard(split_lock_);
  tg.click();
  return is_logical_split_dest_finish(split_state_.get_state());
}

int ObPartitionGroup::check_split_state_() const
{
  int ret = OB_SUCCESS;
  if (is_splitting_()) {
    ret = OB_PARTITION_IS_SPLITTING;
  }
  return ret;
}

int ObPartitionGroup::check_split_state() const
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg(__func__, 1000000);
  SpinRLockGuard guard(split_lock_);
  tg.click();
  return check_split_state_();
}

int ObPartitionGroup::get_split_progress(const int64_t schema_version, int& progress)
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg(__func__, 1000000);
  SpinRLockGuard guard(split_lock_);
  tg.click();
  if (schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(schema_version));
  } else if (ObReplicaTypeCheck::is_log_replica(get_replica_type())) {
    progress = NEED_NOT_SPLIT;
  } else if (schema_version < split_info_.get_schema_version()) {
    progress = PHYSICAL_SPLIT_FINISH;
  } else {
    ret = get_dest_split_progress_(progress);
  }
  TRANS_LOG(INFO, "get split progress", K(ret), K_(pkey), K(progress), K_(split_state));
  return ret;
}

int ObPartitionGroup::set_split_progress(const ObAddr& replica, const int progress)
{
  int ret = OB_SUCCESS;
  if (!is_leader_state(get_partition_state())) {
    ret = OB_NOT_MASTER;
    STORAGE_LOG(WARN, "not master", K(ret), K(replica), K(progress));
  } else if (OB_FAIL(replica_split_progress_array_.set_progress(replica, progress))) {
    STORAGE_LOG(WARN, "set split progress failed", K(ret), K(replica), K(progress));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionGroup::block_partition_split_by_mc()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(split_lock_);
  const ObPartitionKey& src_partition = split_info_.get_src_partition();
  if (src_partition == pkey_) {
    ret = OB_PARTITION_IS_SPLITTING;
    STORAGE_LOG(INFO, "source partition is splitting or splitted", K(ret), K_(pkey), K_(split_info));
  } else {
    if (is_splitting_()) {
      ret = OB_PARTITION_IS_SPLITTING;
      STORAGE_LOG(INFO, "partition is splitting", K(ret), K_(pkey));
    }
  }
  if (OB_SUCCESS == ret) {
    is_split_blocked_by_mc_ = true;
  }
  return ret;
}

int ObPartitionGroup::unblock_partition_split_by_mc()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(split_lock_);
  is_split_blocked_by_mc_ = false;
  return ret;
}

int64_t ObPartitionGroup::get_freeze_snapshot_ts() const
{
  int ret = OB_SUCCESS;
  int64_t snapshot_version = 0;
  if (OB_FAIL(freeze_record_.get_snapshot_version(snapshot_version))) {
    snapshot_version = -1;
  }
  return snapshot_version;
}

ObPartitionState ObPartitionGroup::get_partition_state() const
{
  return ATOMIC_LOAD(&partition_state_);
}

// return value : OB_SUCCESS,         abbr. 'Y'
// return value : OB_STATE_NOT_MATCH, abbr. 'N'
// return value : OB_EAGAIN,          abbr. 'A'
// return value : OB_NOT_MASTER,      abbr. 'NM'
// -----------------------------------------------------------------------
// |\            | I | F | F | L | L | L | L | L | L | L | O | O | R | I |
// | \   new     | N | _ | _ | _ | _ | _ | _ | _ | _ | _ | F | F | E | N |
// |  \  state   | I | W | M | T | T | C | W | F | M | R | F | F | M | V |
// |   \         | T | O | I | A | A | A | O | R | I | E | L | L | O | A |
// |    \        |   | R | N | K | K | N | R | O | N | V | I | I | V | L |
// |     \       |   | K | O | E | E | C | K | Z | O | O | N | N | E | I |
// |      \      |   | I | R | O | O | E | I | E | R | K | I | E |   | D |
// |       \     |   | N |   | V | V | L | N | N |   | E | N |   |   | _ |
// |        \    |   | G |   | E | E | E | G |   |   |   | G |   |   | S |
// |         \   |   |   |   | R | R | D |   |   |   |   |   |   |   | T |
// | old      \  |   |   |   |   | E |   |   |   |   |   |   |   |   | A |
// | state     \ |   |   |   |   | D |   |   |   |   |   |   |   |   | T |
// |            \|   |   |   |   |   |   |   |   |   |   |   |   |   | E |
// -----------------------------------------------------------------------
// |INIT         | N | Y | N | N | N | N | N | N | N | N | N | N | Y | Y |
// -----------------------------------------------------------------------
// |F_WORKING    | N | N | Y | Y | N | N | N | NM| N | N | Y | N | Y | N |
// -----------------------------------------------------------------------
// |F_MINOR      | N | Y | N | N | N | N | N | NM| N | N | A | N | A | N |
// -----------------------------------------------------------------------
// |L_TAKEOVER   | N | N | N | N | Y | N | N | A | A | A | N | N | N | N |
// -----------------------------------------------------------------------
// |L_TAKEOVERED | N | N | N | N | N | Y | Y | A | A | A | N | N | N | N |
// -----------------------------------------------------------------------
// |L_CANCELED   | N | N | N | N | N | N | N | NM| N | Y | N | N | N | N |
// -----------------------------------------------------------------------
// |L_WORKING    | N | N | N | N | N | N | N | Y | Y | Y | N | N | Y | N |
// -----------------------------------------------------------------------
// |L_FROZEN     | N | N | N | N | N | N | Y | A | A | A | N | N | N | N |
// -----------------------------------------------------------------------
// |L_MINOR      | N | N | N | N | N | N | Y | A | A | A | N | N | N | N |
// -----------------------------------------------------------------------
// |L_REVOKE     | N | Y | N | N | N | N | N | NM| N | N | A | N | A | N |
// -----------------------------------------------------------------------
// |OFFLINING    | N | N | N | N | N | N | N | N | N | N | N | Y | Y | N |
// -----------------------------------------------------------------------
// |OFFLINE      | N | Y | N | N | N | N | N | N | N | N | N | N | Y | N |
// -----------------------------------------------------------------------
// |REMOVE       | N | N | N | N | N | N | N | N | N | N | N | N | N | Y |
// -----------------------------------------------------------------------
// |INVALID_STATE| N | N | N | N | N | N | N | N | N | N | N | N | Y | N |
// -----------------------------------------------------------------------
//
// TODO Manage the state of partition and PG level together
int ObPartitionGroup::try_switch_partition_state(const ObPartitionState state)
{
  int ret = OB_SUCCESS;
  static const int N = OB_STATE_NOT_MATCH;
  static const int Y = OB_SUCCESS;
  static const int A = OB_EAGAIN;
  static const int NM = OB_NOT_MASTER;
  static const int64_t STATE_NUM = INVALID_STATE + 1;
  static const int STATE_MATRIX[STATE_NUM][STATE_NUM] = {{N, Y, N, N, N, N, N, N, N, N, N, N, Y, Y},
      {N, N, Y, Y, N, N, N, NM, N, N, Y, N, Y, N},
      {N, Y, N, N, N, N, N, NM, N, N, A, N, A, N},
      {N, N, N, N, Y, N, N, A, A, A, N, N, N, N},
      {N, N, N, N, N, Y, Y, A, A, A, N, N, N, N},
      {N, N, N, N, N, N, N, NM, N, Y, N, N, N, N},
      {N, N, N, N, N, N, N, Y, Y, Y, N, N, Y, N},
      {N, N, N, N, N, N, Y, A, A, A, N, N, N, N},
      {N, N, N, N, N, N, Y, A, A, A, N, N, N, N},
      {N, Y, N, N, N, N, N, NM, N, N, A, N, A, N},
      {N, N, N, N, N, N, N, N, N, N, N, Y, Y, N},
      {N, Y, N, N, N, N, N, N, N, N, N, N, Y, N},
      {N, N, N, N, N, N, N, N, N, N, N, N, N, Y},
      {N, N, N, N, N, N, N, N, N, N, N, N, Y, N}};
  ObTimeGuard time_guard("try_switch_partition_state", 100 * 1000);
  lib::ObMutexGuard guard(partition_state_lock_);
  time_guard.click();
  ObPartitionState cur_state = get_partition_state();

  ret = STATE_MATRIX[cur_state][state];
  if (OB_SUCC(ret)) {
    ATOMIC_STORE(&partition_state_, state);
  }

  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "switch partition state failed", K(pkey_), K(cur_state), K(state), K(ret));
  } else {
    STORAGE_LOG(INFO, "switch partition state successfully", K(pkey_), K(cur_state), K(state));
  }
  return ret;
}

int ObPartitionGroup::switch_partition_state(const ObPartitionState state)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  int64_t current_ts = 0;
  const int64_t PRINT_INTERVAL = 10000;
  const int64_t SLEEP_US = 1000;
  do {
    if (OB_FAIL(try_switch_partition_state(state))) {
      current_ts = ObTimeUtility::current_time();
      if (current_ts - start_ts > PRINT_INTERVAL) {
        start_ts = current_ts;
      }
      if (OB_EAGAIN == ret) {
        ObTransCond::usleep(SLEEP_US);
      }
    }
  } while (OB_EAGAIN == ret);
  return ret;
}

// Only check the schema_version of user table.
// System tables and dummy do not need to check, just skip it.
int ObPartitionGroup::check_schema_version(share::schema::ObMultiVersionSchemaService* schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(NULL == schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(schema_service));
  } else if (extract_pure_id(pkey_.get_table_id()) < OB_MIN_USER_TABLE_ID) {
    // skip it,
  } else if (OB_FAIL(schema_version_container_.check_base_schema_version(schema_service, pkey_))) {
    STORAGE_LOG(WARN, "fail to check base schema_version", K(ret), K(pkey_));
  } else {
  }  // do nothing
  return ret;
}

int ObPartitionGroup::set_base_schema_version(int64_t base_schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (base_schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(base_schema_version));
  } else if (OB_FAIL(schema_version_container_.update_and_leader_takeover(base_schema_version))) {
    STORAGE_LOG(WARN, "fail to update and leader_takeover", K(ret), K(base_schema_version));
  } else {
  }  // do nothing
  return ret;
}

int ObPartitionGroup::SchemaVersionContainer::update_and_leader_takeover(int64_t base_schema_version)
{
  int ret = OB_SUCCESS;
  if (base_schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(base_schema_version));
  } else {
    base_schema_version_ = base_schema_version;
    // mem barrier to make sure execute sequence
    ATOMIC_STORE(&schema_version_checked_, false);
  }
  return ret;
}

int ObPartitionGroup::get_latest_schema_version(share::schema::ObMultiVersionSchemaService* schema_service,
    const common::ObPartitionKey& pkey, int64_t& latest_schema_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_inner_table(pkey.get_table_id()) ? OB_SYS_TENANT_ID : pkey.get_tenant_id();
  if (OB_UNLIKELY(NULL == schema_service) || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_refreshed_schema_version(tenant_id, latest_schema_version))) {
    LOG_WARN("fail to get tenant refresh schema version", K(ret), K(pkey));
  }

  return ret;
}

int ObPartitionGroup::SchemaVersionContainer::check_base_schema_version(
    share::schema::ObMultiVersionSchemaService* schema_service, common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == schema_service) || OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(schema_service), K(pkey));
  } else {
    bool is_schema_valid = false;
    int64_t retry_cnt = 0;
    int64_t latest_schema_version = 0;
    const int64_t SLEEP_ON_NEED_RETRY = 15 * 1000;  // 15ms
    const uint64_t tenant_id = is_inner_table(pkey.get_table_id()) ? OB_SYS_TENANT_ID : pkey.get_tenant_id();
    while (!is_schema_valid && OB_SUCCESS == ret) {
      bool schema_version_checked = ATOMIC_LOAD(&schema_version_checked_);
      if (schema_version_checked) {
        // follower or leader already checked, go on break
        break;
      } else {
        if (OB_FAIL(schema_service->get_tenant_refreshed_schema_version(tenant_id, latest_schema_version))) {
          STORAGE_LOG(
              WARN, "fail to get tenant refreshed schema version", K(ret), K(tenant_id), K(latest_schema_version));
        } else if (latest_schema_version >= base_schema_version_) {
          is_schema_valid = true;
        }
      }
      retry_cnt++;
      if (is_schema_valid) {
        (void)ATOMIC_BCAS(&schema_version_checked_, schema_version_checked, true);
      } else if (retry_cnt >= INVALID_SCHEMA_RETRY_CNT || THIS_WORKER.is_timeout()) {
        const bool is_timeout = THIS_WORKER.is_timeout();
        ret = OB_SCHEMA_ERROR;
        STORAGE_LOG(WARN,
            "schema version error",
            K(ret),
            K(base_schema_version_),
            K(latest_schema_version),
            K(retry_cnt),
            K(is_timeout));
      } else {
        usleep(SLEEP_ON_NEED_RETRY);  // retry
        STORAGE_LOG(WARN, "retry to get schema", K(retry_cnt), K(base_schema_version_), K(latest_schema_version));
      }
    }
  }
  return ret;
}

int ObPartitionGroup::do_warm_up_request(const ObIWarmUpRequest* request)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(pg_storage_.do_warm_up_request(request))) {
    STORAGE_LOG(WARN, "failed to do warm up request", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionGroup::check_can_do_merge(bool& can_merge, bool& need_merge)
{
  int ret = OB_SUCCESS;
  can_merge = false;
  need_merge = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K_(pkey), K(ret));
  } else if (OB_ISNULL(pls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition log service is empty", K_(pkey), K(ret));
  } else {
    ObPartitionState state = get_partition_state();
    if (!is_leader_state(state) && !is_follower_state(state)) {
      need_merge = false;
      STORAGE_LOG(WARN,
          "current partition is not normal replica, "
          "no need to merge",
          K_(pkey),
          K(state),
          K(need_merge));
    } else {
      can_merge = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(E(EventTable::EN_CHECK_CAN_DO_MERGE) OB_SUCCESS)) {
      STORAGE_LOG(WARN, "ERRSIM: EN_CHECK_CAN_DO_MERGE", K(ret), K_(pkey));
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        need_merge = true;
        can_merge = false;
      }
    }
  }

  return ret;
}

ObReplicaType ObPartitionGroup::get_replica_type() const
{
  int tmp_ret = OB_SUCCESS;
  ObReplicaType replica_type;

  if (OB_SUCCESS != (tmp_ret = pg_storage_.get_replica_type(replica_type))) {
    STORAGE_LOG(WARN, "get replica_type error", K(tmp_ret), K_(pkey), K(replica_type));
  }

  return replica_type;
}

ObReplicaProperty ObPartitionGroup::get_replica_property() const
{
  int ret = OB_SUCCESS;
  ObReplicaProperty replica_property;

  if (OB_FAIL(pg_storage_.get_replica_property(replica_property))) {
    STORAGE_LOG(WARN, "get replica property error", K(ret), K(pkey_), K(replica_property));
  }

  return replica_property;
}

int ObPartitionGroup::set_replica_type_(const ObReplicaType& replica_type, const bool write_redo_log)
{
  int ret = OB_SUCCESS;
  ObReplicaType origin_replica_type;
  ObPartitionReplicaState state = OB_UNKNOWN_REPLICA;

  if (OB_FAIL(pg_storage_.get_replica_type(origin_replica_type))) {
    STORAGE_LOG(WARN, "get replica type error", K(ret), K_(pkey));
  } else if (replica_type != origin_replica_type) {
    if (OB_FAIL(pg_storage_.get_replica_state(false, state))) {
      STORAGE_LOG(WARN, "get replica state error", K_(pkey), K(ret));
    } else if (OB_FAIL(pls_->set_replica_type(replica_type))) {
      STORAGE_LOG(WARN, "set replica_type to partition log service failed", K_(pkey), K(replica_type), K(ret));
    } else {
      int tmp_ret = OB_SUCCESS;

      if (!ObReplicaTypeCheck::is_replica_with_memstore(replica_type) && OB_NORMAL_REPLICA == state) {
        STORAGE_LOG(INFO, "clear pending log task", K_(pkey), K(ret));
        if (OB_SUCCESS != (tmp_ret = replay_status_->set_need_filter_trans_log(pkey_, true /*need filter*/))) {
          STORAGE_LOG(ERROR,
              "fail to set_need_filter_trans_log",
              K(tmp_ret),
              K_(pkey),
              K(origin_replica_type),
              K(replica_type));
        } else if (OB_SUCCESS != (tmp_ret = txs_->clear_all_ctx(pkey_))) {
          STORAGE_LOG(ERROR,
              "fail to clear all trans ctx after set replica type",
              K(tmp_ret),
              K_(pkey),
              K(origin_replica_type),
              K(replica_type));
        }
      }

      if (OB_FAIL(pg_storage_.set_pg_replica_type(replica_type, write_redo_log))) {
        STORAGE_LOG(WARN, "set replica_type to storage failed", K_(pkey), K(replica_type), K(ret));
      } else {
        if (ObReplicaTypeCheck::is_log_replica(replica_type)) {
          if (OB_FAIL(save_split_state(static_cast<int64_t>(ObPartitionSplitStateEnum::FOLLOWER_INIT)))) {
            STORAGE_LOG(WARN, "failed to save split state", K(ret), K_(pkey));
          } else {
            STORAGE_LOG(INFO, "set replica_type successfully", K_(pkey), K(replica_type));
          }
        }
      }
    }
  } else {
    STORAGE_LOG(INFO, "no need to set replica_type", K_(pkey), K(replica_type));
  }

  return ret;
}

int ObPartitionGroup::set_replica_type(const ObReplicaType& replica_type, const bool write_redo_log)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K_(pkey), K(ret));
  } else if (OB_ISNULL(replay_status_) || OB_ISNULL(pls_) || OB_ISNULL(txs_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid members", KP(replay_status_), KP(pls_), KP(txs_), K_(pkey), K(ret));
  } else {
    ObPGLockWithPendingReplayGuard guard(
        lock_, *replay_status_, pkey_, 0, PGLOCKREPLAY | PGLOCKCLOG | PGLOCKTRANS | PGLOCKSTORAGE);
    ret = set_replica_type_(replica_type, write_redo_log);
  }

  return ret;
}

// Get the weak read timestamp from cache.
// The cache is refreshed by trans_view thread.
int ObPartitionGroup::get_weak_read_timestamp(int64_t& timestamp)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(pg_storage_.get_weak_read_timestamp(timestamp))) {
    STORAGE_LOG(WARN, "pg storage get weak read timestamp error", KR(ret), K_(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int64_t ObPartitionGroup::get_cur_min_log_service_ts()
{
  return partition_loop_worker_.get_cur_min_log_service_ts();
}

int64_t ObPartitionGroup::get_cur_min_trans_service_ts()
{
  return partition_loop_worker_.get_cur_min_trans_service_ts();
}

int64_t ObPartitionGroup::get_cur_min_replay_engine_ts()
{
  return partition_loop_worker_.get_cur_min_replay_engine_ts();
}

int ObPartitionGroup::update_last_checkpoint(const int64_t checkpoint)
{
  return partition_loop_worker_.update_last_checkpoint(checkpoint);
}

int ObPartitionGroup::set_replay_checkpoint(const int64_t checkpoint)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret), K_(pkey));
  } else if (OB_FAIL(partition_loop_worker_.set_replay_checkpoint(checkpoint))) {
    STORAGE_LOG(WARN, "partition loop worker set replay checkpoint error", K(ret), K_(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionGroup::get_replay_checkpoint(int64_t& checkpoint)
{
  return partition_loop_worker_.get_replay_checkpoint(checkpoint);
}

int ObPartitionGroup::generate_weak_read_timestamp(const int64_t max_stale_time, int64_t& timestamp)
{
  return partition_loop_worker_.generate_weak_read_timestamp(max_stale_time, timestamp);
}

int ObPartitionGroup::do_partition_loop_work()
{
  return partition_loop_worker_.do_partition_loop_work();
}

int ObPartitionGroup::need_minor_freeze(const uint64_t& log_id, bool& need_freeze)
{
  int ret = OB_SUCCESS;
  need_freeze = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K_(pkey), K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == log_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(log_id), K(ret));
  } else if (0 == log_id) {
    // need_freeze = false;
  } else if (OB_FAIL(pg_storage_.check_need_minor_freeze(log_id, need_freeze))) {
    STORAGE_LOG(WARN, "check need minor freeze error", K(ret), K_(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionGroup::set_emergency_release()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K_(pkey), K(ret));
  } else if (OB_FAIL(pg_storage_.set_emergency_release())) {
    STORAGE_LOG(WARN, "pg storage set emergency release error", K(ret), K_(pkey));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionGroup::get_freeze_log_(const bool is_leader, uint64_t& log_id, int64_t& log_ts)
{
  int ret = OB_SUCCESS;

  log_id = OB_INVALID_ID;
  log_ts = OB_INVALID_TIMESTAMP;
  uint64_t max_majority_log_id = OB_INVALID_ID;
  int64_t max_majority_log_ts = OB_INVALID_TIMESTAMP;
  uint64_t next_replay_log_id = OB_INVALID_ID;
  int64_t next_replay_log_ts = OB_INVALID_TIMESTAMP;

  if (is_leader) {
    (void)pls_->get_max_majority_log(max_majority_log_id, max_majority_log_ts);
    if (OB_FAIL(pls_->get_next_replay_log_info(next_replay_log_id, next_replay_log_ts))) {
      STORAGE_LOG(WARN, "get next replay log info failed", K(ret), K(*this));
    } else {
      log_id = MAX(max_majority_log_id, next_replay_log_id - 1);
      log_ts = MAX(max_majority_log_ts, next_replay_log_ts - 1);
    }
  } else {
    if (OB_FAIL(get_min_replayed_log_with_keepalive(log_id, log_ts))) {
      STORAGE_LOG(WARN, "get min replay log with keepalive failed", K(ret), K(*this));
    }
  }

  return ret;
}

// We guarantee that the data before freeze_id and the transactions before snapshot_version
// must be on the frozen memstore.
int ObPartitionGroup::get_freeze_cut_(ObMemtable& frozen_memtable, const bool is_leader, int64_t& snapshot_version,
    uint64_t& freeze_id, int64_t& freeze_ts)
{
  int ret = OB_SUCCESS;
  int64_t start_log_ts = frozen_memtable.get_start_log_ts();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K_(pkey));
  } else if (OB_ISNULL(pls_) || OB_ISNULL(replay_status_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition member is NULL", K(ret), K_(pkey), KP(pls_), KP(replay_status_));
  } else {
    if (is_leader) {
      // 1. The freeze_id of leader is the maximum log id that is submitted in sliding window.
      // This is mutually exclusive with transaction submission to ensure that transactions across
      // frozen point will be marked dirty
      SpinWLockGuard guard(freeze_lock_);

      if (OB_FAIL(get_and_submit_freeze_(frozen_memtable, true /*is_leader*/, freeze_id, freeze_ts))) {
        STORAGE_LOG(WARN,
            "get and submit freeze id failed",
            K(frozen_memtable),
            K(is_leader),
            K(freeze_id),
            K(freeze_ts),
            K(*this));
      }
    } else {
      // 2. The freeze_id of follower is the right boundary of replay queue.
      // The follower will block the replay, wait it to be empty and then get the freeze_id.
      if (OB_FAIL(wait_follower_no_pending_task_())) {
        STORAGE_LOG(WARN, "wait follower no pending task failed", K(is_leader), K(freeze_id), K(*this));
      } else if (OB_FAIL(get_and_submit_freeze_(frozen_memtable, false /*is_leader*/, freeze_id, freeze_ts))) {
        STORAGE_LOG(WARN,
            "get and submit freeze id failed",
            K(frozen_memtable),
            K(is_leader),
            K(freeze_id),
            K(freeze_ts),
            K(*this));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(get_freeze_snapshot_version_(freeze_ts, snapshot_version))) {
      STORAGE_LOG(WARN, "get freeze snapshot version error", K(ret), K_(pkey), K(freeze_ts));
    }

    if (OB_SUCC(ret)) {
      if (start_log_ts == freeze_ts) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR,
            "start log ts equal end log ts",
            K(ret),
            K(pkey_),
            K(freeze_id),
            K(freeze_ts),
            K(snapshot_version),
            K(is_leader),
            K(frozen_memtable),
            K(start_log_ts));
      } else {
        STORAGE_LOG(INFO,
            "freeze cut",
            K(ret),
            K(pkey_),
            K(freeze_id),
            K(freeze_ts),
            K(snapshot_version),
            K(is_leader),
            K(frozen_memtable),
            K(start_log_ts));
      }
    }
  }

  return ret;
}

int ObPartitionGroup::get_freeze_snapshot_version_(const int64_t freeze_ts, int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  int64_t min_prepare_version = INT64_MAX;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(txs_->get_min_prepare_version(pkey_, freeze_ts, min_prepare_version))) {
    STORAGE_LOG(ERROR, "get min prepare version failed", K(ret), K_(pkey), K(freeze_ts));
  } else if (INT64_MAX == min_prepare_version) {
    // If there is no prepared transaction across the frozen point,
    // the transactions with version smaller than the frozen point are all on the memtable
    snapshot_version = freeze_ts;
  } else {
    snapshot_version = min_prepare_version - 1;
  }

  return ret;
}

int ObPartitionGroup::get_and_submit_freeze_(
    ObMemtable& frozen_memtable, bool is_leader, uint64_t& freeze_id, int64_t& freeze_ts)
{
  int ret = OB_SUCCESS;
  int64_t start_log_ts = frozen_memtable.get_start_log_ts();

  if (OB_FAIL(get_freeze_log_(is_leader, freeze_id, freeze_ts))) {
    STORAGE_LOG(WARN, "get freeze log failed", K(ret), K(*this));
  } else if (start_log_ts == freeze_ts && OB_FAIL(wait_freeze_log_elapse_(freeze_ts, freeze_ts))) {
    STORAGE_LOG(WARN,
        "fail to submit freeze record",
        K(ret),
        K(pkey_),
        K(frozen_memtable),
        K(freeze_id),
        K(freeze_ts),
        K(start_log_ts));
  } else if (OB_FAIL(freeze_record_.submit_freeze(frozen_memtable, freeze_ts))) {
    STORAGE_LOG(WARN,
        "fail to submit freeze record",
        K(ret),
        K(pkey_),
        K(frozen_memtable),
        K(freeze_id),
        K(freeze_ts),
        K(start_log_ts));
  }

  return ret;
}

int ObPartitionGroup::wait_freeze_log_elapse_(const int64_t in_freeze_ts, int64_t& out_freeze_ts)
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  int64_t clog_timestamp = OB_INVALID_TIMESTAMP;
  uint64_t unused_log_id = OB_INVALID_ID;

  // The leader needs to guarantee clog_timestamp > freeze_ts + 1 before returning freeze_ts + 1
  bool need_retry = true;
  while (need_retry && OB_SUCC(ret)) {
    if (OB_FAIL(pls_->get_next_replay_log_info(unused_log_id, clog_timestamp))) {
      STORAGE_LOG(WARN, "fail to get next replay log timestamp", K(ret), K(pkey_), K(in_freeze_ts));
    } else if (OB_INVALID_TIMESTAMP == clog_timestamp) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "wrong clog timestamp", K(clog_timestamp), K(pkey_));
    } else if (clog_timestamp <= in_freeze_ts + 1) {
      cnt++;
      if (cnt >= WAIT_FREEZE_LOG_ELAPSE_CNT_LIMIT) {
        need_retry = false;
        ret = OB_EAGAIN;
        STORAGE_LOG(WARN,
            "wait freeze log elapse failed, may be no leader",
            K(ret),
            K(cnt),
            K(clog_timestamp),
            K(in_freeze_ts));
      } else {
        need_retry = true;
      }
    } else {
      out_freeze_ts = clog_timestamp - 1;
      need_retry = false;
      STORAGE_LOG(INFO,
          "start log ts equal end log ts, wait end log ts + 1 elapse succcess",
          K(in_freeze_ts),
          K(out_freeze_ts));
    }
  }

  return ret;
}

int ObPartitionGroup::freeze_log_(const bool force)
{
  int ret = OB_SUCCESS;

  ObSavedStorageInfoV2 info;
  ObBaseStorageInfo& clog_info = info.get_clog_info();
  bool log_changed = false;
  bool unused = false;

  if (OB_UNLIKELY(!is_inited_ || NULL == pls_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_clog_info_for_freeze_log_(info))) {
    STORAGE_LOG(WARN, "get clog info for freeze log failed", K(ret), K(pkey_));
  } else if (OB_FAIL(pg_storage_.check_log_or_data_changed(clog_info, log_changed, unused))) {
    STORAGE_LOG(WARN, "check log or data changed error", K(ret), K(clog_info));
  } else if (!log_changed && !force) {
    STORAGE_LOG(INFO, "skip freeze log", K(pkey_), K(clog_info));
  } else if (OB_FAIL(pg_storage_.set_pg_clog_info(clog_info, false))) {
    STORAGE_LOG(WARN, "fail to save clog info", K(ret), K(pkey_));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionGroup::get_clog_info_for_freeze_log_(ObSavedStorageInfoV2& info)
{
  int ret = OB_SUCCESS;
  uint64_t min_unreplay_log_id = OB_INVALID_ID;
  uint64_t last_replay_log_id = OB_INVALID_ID;
  ObBaseStorageInfo& clog_info = info.get_clog_info();
  ObBaseStorageInfo saved_clog_info;

  if (OB_FAIL(get_base_storage_info_(clog_info))) {
    STORAGE_LOG(WARN, "fail to get base clog info", K(ret), K(pkey_));
  } else if (OB_FAIL(get_saved_clog_info(saved_clog_info))) {
    STORAGE_LOG(WARN, "fail to get saved clog info", K(ret), K(pkey_));
  } else {
    last_replay_log_id = clog_info.get_last_replay_log_id();
    min_unreplay_log_id = replay_status_->get_min_unreplay_log_id();

    if (min_unreplay_log_id - 1 < last_replay_log_id) {
      clog_info.set_last_replay_log_id(min_unreplay_log_id - 1);
      if (OB_FAIL(info.update_last_replay_log_info(pkey_,
              false /*replica_with_data*/,
              saved_clog_info,
              PG_QUERY_LOG_INFO_TIMEOUT,
              false /* log_info_usable*/))) {
        STORAGE_LOG(WARN, "fail to get update last replay log info", K(ret), K(pkey_), K(saved_clog_info), K(info));
      }
    }
  }

  return ret;
}

int ObPartitionGroup::wait_follower_no_pending_task_()
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  int64_t task_cnt = replay_status_->get_pending_task_count();

  while (replay_status_->has_pending_task(pkey_) && OB_SUCC(ret)) {
    usleep(FREEZE_WAIT_RETRY_SLEEP_TS);
    cnt++;

    if (cnt >= MAX_FREEZE_WAIT_RETRY_SLEEP_CNT) {
      int last_task_cnt = task_cnt;
      if (last_task_cnt == (task_cnt = replay_status_->get_pending_task_count())) {
        ret = OB_EAGAIN;
        STORAGE_LOG(WARN, "replay too slow, retreat!", K(ret), K(pkey_));
      } else {
        STORAGE_LOG(WARN, "follower wait replay too long", K(ret), K(pkey_), K(task_cnt));
      }

      cnt = 0;
    }
  }

  return ret;
}

int ObPartitionGroup::check_range_changed_(ObTableHandle& handle, const bool is_leader, bool& changed)
{
  int ret = OB_SUCCESS;
  uint64_t unused = 0;
  int64_t tmp_freeze_ts = 0;
  int64_t tmp_snapshot_version = 0;
  int64_t start_log_ts = 0;
  int64_t base_version = 0;
  ObMemtable* mt = NULL;
  changed = false;

  if (OB_FAIL(handle.get_memtable(mt))) {
    STORAGE_LOG(WARN, "get memtable from handle fail", K(ret), K(handle));
  } else if (OB_ISNULL(mt)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "memtable is NULL", K(ret));
  } else if (OB_FAIL(get_freeze_log_(is_leader, unused, tmp_freeze_ts))) {
    STORAGE_LOG(WARN, "get freeze log failed", K(ret), K(*this));
  } else if (OB_FAIL(get_freeze_snapshot_version_(tmp_freeze_ts, tmp_snapshot_version))) {
    STORAGE_LOG(WARN, "failed to get_weak_read_timestamp", K(ret), K(pkey_));
  } else {
    start_log_ts = mt->get_start_log_ts();
    base_version = mt->get_base_version();

    if (tmp_freeze_ts < start_log_ts || tmp_snapshot_version < base_version) {
      ret = OB_EAGAIN;
      STORAGE_LOG(INFO,
          "skip freeze, maybe in the process of restarting",
          K(ret),
          K(pkey_),
          K(tmp_freeze_ts),
          K(tmp_snapshot_version),
          K(start_log_ts),
          K(base_version));
    } else {
      changed = tmp_freeze_ts >= start_log_ts && tmp_snapshot_version >= base_version &&
                (tmp_freeze_ts > start_log_ts || tmp_snapshot_version > base_version);
    }
  }

  if (OB_SUCC(ret) && !changed) {
    STORAGE_LOG(WARN,
        "log_id or version range not changed",
        K(ret),
        K(pkey_),
        K(is_leader),
        K(start_log_ts),
        K(base_version),
        K(tmp_freeze_ts),
        K(tmp_snapshot_version));
  }

  return ret;
}

int ObPartitionGroup::prepare_storage_info_(
    const int64_t snapshot_version, const uint64_t freeze_id, const int64_t freeze_ts, ObSavedStorageInfoV2& info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(get_base_storage_info_(info.get_clog_info()))) {
    STORAGE_LOG(WARN, "fail to get base storage info", K(ret), K(pkey_));
  } else {
    info.get_data_info().set_publish_version(snapshot_version);
    info.get_data_info().set_last_replay_log_id(freeze_id);
    info.get_data_info().set_last_replay_log_ts(freeze_ts);
    info.get_data_info().set_created_by_new_minor_freeze();
  }

  return ret;
}

int ObPartitionGroup::submit_freeze_and_effect_memstore_inner_(
    const bool is_leader, const bool emergency, ObMemtable& frozen_memtable, bool& effected, int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t freeze_id = 0;
  int64_t freeze_ts = 0;
  ObSavedStorageInfoV2 info;
  snapshot_version = 0;
  effected = false;

  if (!is_leader && OB_FAIL(replay_status_->set_pending())) {
    STORAGE_LOG(WARN, "set replay status pending failed", K(replay_status_), K(ret), K(*this));
  } else {
    if (OB_FAIL(cut_and_submit_freeze_(is_leader, frozen_memtable, snapshot_version, freeze_id, freeze_ts))) {
      STORAGE_LOG(INFO, "cut and submit freeze failed", K(pkey_));
    } else if (OB_FAIL(prepare_storage_info_(snapshot_version, freeze_id, freeze_ts, info))) {
      STORAGE_LOG(
          WARN, "fail to prepare storage info", K(ret), K(pkey_), K(freeze_id), K(freeze_ts), K(snapshot_version));
    } else if (OB_FAIL(pg_storage_.effect_new_active_memstore(info, emergency))) {
      frozen_memtable.clear_freeze_log_ts();
      if (OB_CANCELED == ret || OB_PARTITION_IS_REMOVED == ret || OB_NOT_INIT == ret) {
        STORAGE_LOG(INFO, "freeze skiped", K(ret), K(pkey_));
        ret = OB_SUCCESS;
      } else {
        // with resource allocated before, the failure is unexpected
        STORAGE_LOG(ERROR, "effect new active memstore failed", K(ret), K(pkey_), K(info));
      }
    } else {
      (void)frozen_memtable.update_max_log_ts(freeze_ts);
      MEM_BARRIER();
      frozen_memtable.set_frozen();
      effected = true;
    }

    while (!is_leader && OB_SUCCESS != (tmp_ret = replay_status_->erase_pending(
                                            ObTimeUtility::current_time() + OB_SET_REPLAY_ENGINE_PENDING_TIMEOUT))) {
      STORAGE_LOG(WARN, "unset replay status pending failed", K(replay_status_), K(tmp_ret), K(*this));
    }
  }

  return ret;
}

int ObPartitionGroup::cut_and_submit_freeze_(const bool is_leader, ObMemtable& frozen_memtable,
    int64_t& snapshot_version, uint64_t& freeze_id, int64_t& freeze_ts)
{
  int ret = OB_SUCCESS;

  if (is_leader && OB_FAIL(frozen_memtable.prepare_freeze_log_ts())) {
    STORAGE_LOG(ERROR, "fail to prepare freeze log id", K(ret), K(pkey_), K(frozen_memtable));
  } else if (OB_FAIL(get_freeze_cut_(frozen_memtable, is_leader, snapshot_version, freeze_id, freeze_ts))) {
    STORAGE_LOG(WARN, "fail to get freeze cut", K(ret), K(pkey_));
  }

  if (OB_FAIL(ret)) {
    frozen_memtable.clear_freeze_log_ts();
  }

  return ret;
}

int ObPartitionGroup::freeze_log_and_data_v2_(const bool emergency, const bool force, int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  ObTableHandle old_handle;
  ObTableHandle new_handle;
  ObBaseStorageInfo clog_info;
  bool changed = true;
  bool found = false;
  bool log_changed = true;
  bool need_freeze_data = true;
  bool effected = false;
  ObMemtable* frozen_memtable = NULL;
  snapshot_version = 0;

  bool is_leader = is_leader_state(get_partition_state());

  ObTimeGuard timeguard("freeze", 10L * 1000L);

  if (OB_UNLIKELY(!is_inited_ || NULL == pls_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(has_active_memtable_(found))) {
    STORAGE_LOG(INFO, "fail to check active memtable", K(ret), K(pkey_));
  } else if (!found) {
    STORAGE_LOG(INFO, "partition has no active memstore, skip freeze", K(ret), K(pkey_));
  } else if (!freeze_record_.available()) {
    // consider this as freeze success
    STORAGE_LOG(INFO, "freeze already in progress", K(ret), K(pkey_));
  } else if (OB_FAIL(pg_storage_.get_active_memtable(old_handle))) {
    STORAGE_LOG(WARN, "fail to get old active memtable", K(ret), K(pkey_));
  } else if (OB_FAIL(old_handle.get_memtable(frozen_memtable))) {
    STORAGE_LOG(WARN, "get memtable from handle fail", K(ret), K(old_handle));
  } else if (OB_ISNULL(frozen_memtable)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "memtable is NULL", K(ret));
  } else if (OB_FAIL(get_base_storage_info_(clog_info))) {
    STORAGE_LOG(WARN, "fail to get base storage info", K(ret), K(pkey_));
  } else if (!force && OB_FAIL(pg_storage_.check_log_or_data_changed(clog_info, log_changed, need_freeze_data))) {
    STORAGE_LOG(WARN, "fail to check log data changed", K(ret), K(pkey_));
  } else if (!need_freeze_data) {
    if (!log_changed) {
      STORAGE_LOG(INFO, "skip freeze log and data", K(pkey_));
    } else if (OB_FAIL(pg_storage_.set_pg_clog_info(clog_info, true))) {
      STORAGE_LOG(WARN, "fail to freeze log", K(ret), K(pkey_));
    }
  } else if (OB_FAIL(check_range_changed_(old_handle, is_leader, changed))) {
    if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "failed to check log_id or version range changed", K(ret), K(old_handle));
    }
  } else if (!changed) {
    // skip
  } else if (OB_FAIL(submit_freeze_and_effect_memstore_(
                 is_leader, emergency, *frozen_memtable, effected, snapshot_version))) {
    STORAGE_LOG(WARN, "submit freeze and prepare memstore", K(ret), K(pkey_), K(*frozen_memtable));
  } else if (effected) {
    if (OB_FAIL(pg_storage_.get_active_memtable(new_handle))) {
      STORAGE_LOG(WARN, "fail to get new active memtable", K(ret), K(pkey_));
    } else if (OB_FAIL(freeze_record_.submit_new_active_memtable(new_handle))) {
      // Submit a new memtable. Allow async_freeze threads to scan and synchronize log.
      STORAGE_LOG(ERROR, "fail to submit freeze record", K(ret), K(pkey_));
    } else {
      STORAGE_LOG(INFO, "submit_new_active_memtable success", K(ret), K(pkey_));
    }
  }

  return ret;
}

int ObPartitionGroup::submit_freeze_and_effect_memstore_(
    const bool is_leader, const bool emergency, ObMemtable& frozen_memtable, bool& effected, int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  uint64_t freeze_id = 0;
  int64_t freeze_ts = 0;
  int64_t unused = 0;
  ObSavedStorageInfoV2 info;
  snapshot_version = 0;
  effected = false;

  // allocate the resource in advance
  if (OB_FAIL(pg_storage_.new_active_memstore(unused))) {
    STORAGE_LOG(WARN, "fail to new active memstore", K(ret), K(pkey_));
  } else {
    if (OB_FAIL(submit_freeze_and_effect_memstore_inner_(
            is_leader, emergency, frozen_memtable, effected, snapshot_version))) {
      STORAGE_LOG(
          WARN, "submit freeze and effect memstore inner failed", K(ret), K(emergency), K(frozen_memtable), K(*this));
    }

    if (OB_FAIL(ret) || !effected) {
      // clean the active memstore if it's not effected
      int64_t tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = pg_storage_.clean_new_active_memstore()))) {
        STORAGE_LOG(ERROR, "rollback error", K(tmp_ret), K(pkey_));
      }
    } else {
      pg_storage_.set_freeze_ts(ObTimeUtility::current_time());
      STORAGE_LOG(INFO, "effect new active memstore success", K(ret), K(pkey_), K(frozen_memtable));
    }
  }

  return ret;
}

int ObPartitionGroup::freeze(const bool emergency, const bool force, int64_t& freeze_snapshot)
{
  int ret = OB_SUCCESS;

  ObPartitionGroupLockGuard guard(lock_, 0, PGLOCKSTORAGE);

  if (with_data_()) {
    // F replica, need freeze;
    ret = freeze_log_and_data_v2_(emergency, force, freeze_snapshot);
    // F/R replica are with data because they have sstable. The freeze operation will be skipped at
    // lower layer because they do not have memtable.
  } else {
    // L replica or empty PG
    ret = freeze_log_(force);
  }

  return ret;
}

int ObPartitionGroup::set_max_passed_trans_version_(const int64_t trans_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(trans_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey_));
  } else {
    max_passed_trans_version_ = trans_version;
    // Free record strongly depends on max_passed_trans_version if the latter is reset, the former must be reset too.
    freeze_record_.clear();
    partition_loop_worker_.reset_memstore_info_record();
    STORAGE_LOG(INFO, "set max passed trans version success", K_(pkey), K(trans_version));
  }

  return ret;
}

int ObPartitionGroup::raise_memstore_if_needed_(const int64_t trans_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret), K(pkey_));
  } else {
    int64_t tmp_ret = OB_SUCCESS;
    while (freeze_record_.need_raise_memstore(trans_version)) {  // Check in advance to reduce lock calls
      ObPartitionGroupLockGuard guard(lock_, 0, PGLOCKSTORAGE, true /* trylock */);
      if (guard.locked()) {
        if (freeze_record_.need_raise_memstore(trans_version)) {
          if (OB_FAIL(pg_storage_.effect_new_active_memstore(
                  freeze_record_.get_saved_storage_info(), freeze_record_.is_emergency()))) {
            if (OB_CANCELED == ret || OB_PARTITION_IS_REMOVED == ret || OB_NOT_INIT == ret) {
              STORAGE_LOG(INFO, "freeze skiped", K(ret), K(pkey_));
              ret = OB_SUCCESS;
            } else {
              // The resource has been reserved, expect no error
              STORAGE_LOG(ERROR, "effect new active memstore failed", K(ret), K(pkey_));
            }

            if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = pg_storage_.clean_new_active_memstore()))) {
              STORAGE_LOG(ERROR, "memstore rollback error", K(tmp_ret), K(pkey_));
            }
          } else {
            STORAGE_LOG(INFO, "raise memstore success", K(ret), K(pkey_), K(freeze_record_));
          }
          freeze_record_.clear();  // No matter success or not, this freezing point should be cleared.
        }
      } else {
        usleep(100);
      }
    }
  }

  return ret;
}

int ObPartitionGroup::mark_dirty_trans(bool& cleared)
{
  int ret = OB_SUCCESS;
  cleared = false;

  if (OB_ISNULL(pls_) || OB_ISNULL(txs_) || OB_ISNULL(replay_status_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else {
    ObPartitionGroupLockGuard guard(lock_, PGLOCKTRANS | PGLOCKREPLAY | PGLOCKCLOG | PGLOCKSTORAGE, 0);
    ObMemtable* frozen_memtable = NULL;
    ObMemtable* active_memtable = NULL;

    if (OB_FAIL(freeze_record_.get_pending_frozen_memtable(frozen_memtable, active_memtable))) {
      STORAGE_LOG(ERROR, "fail to get pending frozen memtable", K(ret), K(pkey_));
    } else if (OB_NOT_NULL(frozen_memtable) && OB_NOT_NULL(active_memtable)) {
      int64_t cb_cnt = 0;
      int64_t applied_log_ts = INT64_MAX;
      uint64_t unused = 0;
      int64_t replayed_ts = OB_INVALID_TIMESTAMP;

      if (OB_FAIL(get_min_replayed_log_with_keepalive(unused, replayed_ts))) {
        STORAGE_LOG(WARN, "get min replay log with keepalive failed", K(ret), K(*this));
      } else {
        if (OB_FAIL(txs_->mark_dirty_trans(pkey_, frozen_memtable, active_memtable, cb_cnt, applied_log_ts))) {
          STORAGE_LOG(WARN, "fail to mark dirty trans", K(ret), K(pkey_), KP(frozen_memtable), KP(active_memtable));
        }

        if (OB_SUCC(ret) || cb_cnt > 0) {
          STORAGE_LOG(INFO, "mark callback", K(ret), K(pkey_), K(cb_cnt), K(replayed_ts), K(applied_log_ts));
        }

        applied_log_ts = MIN(replayed_ts, applied_log_ts);

        freeze_record_.dirty_trans_marked(frozen_memtable, cb_cnt, OB_SUCC(ret), applied_log_ts, cleared);
      }
    } else {
      // no need freeze
      cleared = true;
    }
  }

  return ret;
}

int ObPartitionGroup::get_curr_data_info_(
    const bool use_slave_safe_read_ts, const ObDataStorageInfo& saved_data_info, ObDataStorageInfo& data_info)
{
  int ret = OB_SUCCESS;

  if (use_slave_safe_read_ts) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "saved storage info is not supported in 3.x version");
  } else {
    data_info = saved_data_info;
  }

  return ret;
}

// Check if it is a replica type with data
bool ObPartitionGroup::with_data_()
{
  const common::ObReplicaType replica_type = get_replica_type();
  bool with_data = (ObReplicaTypeCheck::is_replica_with_memstore(replica_type) ||
                    ObReplicaTypeCheck::is_replica_with_ssstore(replica_type));
  return with_data;
}

int ObPartitionGroup::get_curr_clog_info_(
    const int64_t src_cluster_id, ObBaseStorageInfo& clog_info, bool& log_info_usable)
{
  int ret = OB_SUCCESS;
  bool is_sys_tenant = OB_SYS_TENANT_ID == pkey_.get_tenant_id();
  const int64_t self_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  const bool is_restore = pg_storage_.is_restore();

  if (OB_FAIL(get_base_storage_info_(clog_info))) {
    STORAGE_LOG(WARN, "fail to get base storage info", K(ret), K(pkey_));
  } else if (0 == clog_info.get_last_replay_log_id()) {
    // skip fetching log_archive_status
  } else if (ObServerConfig::get_instance().enable_log_archive && !is_sys_tenant && !is_restore &&
             src_cluster_id == self_cluster_id) {
    // Only the requests from the database itself need to obtain the archive point if the archive is enabled.
    // To solve the circular dependency that the leader of primary database can not take over due to the restore
    // failure of the backup database replica here in the maximum protection mode (bug#30365449).
    ObLogArchiveBackupInfo info;
    if (OB_FAIL(ObBackupInfoMgr::get_instance().get_log_archive_backup_info(info))) {
      CLOG_LOG(WARN, "failed to get_log_archive_backup_info", K(pkey_), KR(ret));
    } else if (ObLogArchiveStatus::STATUS::BEGINNING == info.status_.status_ ||
               ObLogArchiveStatus::STATUS::DOING == info.status_.status_) {
      ObPGLogArchiveStatus log_archive_status;
      if (OB_FAIL(pls_->get_log_archive_status(log_archive_status))) {
        STORAGE_LOG(WARN, "failed to get_last_archived_log_info", KR(ret), K(pkey_), K(info));
      } else if (info.status_.incarnation_ != log_archive_status.archive_incarnation_ ||
                 info.status_.round_ != log_archive_status.log_archive_round_) {
        ret = OB_EAGAIN;
        STORAGE_LOG(
            WARN, "not the same round, try later", KR(ret), K(pkey_), K(info), K(clog_info), K(log_archive_status));
      } else if (ObLogArchiveStatus::INTERRUPTED == log_archive_status.status_ ||
                 ObLogArchiveStatus::STOP == log_archive_status.status_) {
        // just skip
        STORAGE_LOG(INFO, "log archive is not doing, just skip", K(pkey_), K(info), K(log_archive_status));
      } else if (!log_archive_status.is_valid_for_clog_info()) {
        ret = OB_EAGAIN;
        // attention: can not merge with the forword OB_EAGAIN branch
        STORAGE_LOG(WARN,
            "log_archive_status is not valid, try later",
            KR(ret),
            K(pkey_),
            K(info),
            K(clog_info),
            K(log_archive_status));
      } else {
        STORAGE_LOG(INFO, "update with archived clog info", K(pkey_), K(clog_info), K(log_archive_status));
        if (log_archive_status.last_archived_log_id_ < clog_info.get_last_replay_log_id()) {
          log_info_usable = true;
          clog_info.set_last_replay_log_id(log_archive_status.last_archived_log_id_);
          clog_info.set_submit_timestamp(log_archive_status.last_archived_log_submit_ts_);
          clog_info.set_epoch_id(log_archive_status.clog_epoch_id_);
          clog_info.set_accumulate_checksum(log_archive_status.accum_checksum_);
        }
      }
    } else { /*do nothing*/
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObPartitionGroup::get_curr_storage_info_for_migrate(const bool use_slave_safe_read_ts,
    const common::ObReplicaType replica_type, const int64_t src_cluster_id, ObSavedStorageInfoV2& info)
{
  int ret = OB_SUCCESS;

  static const int64_t LOG_INFO_QUERY_TIMEOUT = 5L * 1000L * 1000L;
  ObSavedStorageInfoV2 saved_info;
  info.reset();
  ObDataStorageInfo& data_info = info.get_data_info();
  ObBaseStorageInfo& clog_info = info.get_clog_info();
  bool remote_with_data = ObReplicaTypeCheck::is_replica_with_memstore(replica_type);
  bool with_data = false;
  bool log_info_usable = false;

  if (OB_UNLIKELY(!is_inited_ || NULL == pls_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), KP(pls_), K(pkey_));
  } else if (OB_INVALID_CLUSTER_ID == src_cluster_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(src_cluster_id), K(pkey_));
  } else if (OB_FAIL(pg_storage_.get_all_saved_info(saved_info))) {
    STORAGE_LOG(WARN, "fail to get saved info", K(ret), K(pkey_));
  } else if (FALSE_IT(with_data = with_data_() && remote_with_data)) {
  } else if (OB_FAIL(get_curr_data_info_(use_slave_safe_read_ts, saved_info.get_data_info(), data_info))) {
    STORAGE_LOG(WARN, "fail to get curr data info", K(ret), K(pkey_));
  } else if (OB_FAIL(get_curr_clog_info_(src_cluster_id, clog_info, log_info_usable))) {
    STORAGE_LOG(WARN, "fail to get curr clog info", K(ret), K(pkey_));
  } else if (OB_FAIL(info.update_and_fetch_log_info(
                 pkey_, with_data, saved_info.get_clog_info(), LOG_INFO_QUERY_TIMEOUT, log_info_usable))) {
    STORAGE_LOG(WARN, "failed to update_info_and_fetch_checksum", K(ret), K_(pkey), K(saved_info), K(info));
  }

  STORAGE_LOG(INFO, "get curr storage info", K(ret), K(pkey_), K(replica_type), K(saved_info), K(info));

  return ret;
}

int ObPartitionGroup::shutdown(
    const int64_t snapshot_version, const uint64_t replay_log_id, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(split_lock_);

  if (OB_FAIL(shutdown_(snapshot_version, replay_log_id, schema_version))) {
    STORAGE_LOG(WARN, "failed to shutdown", K(ret), K(pkey_), K(snapshot_version), K(replay_log_id), K(schema_version));
  }
  return ret;
}

int ObPartitionGroup::shutdown_(
    const int64_t snapshot_version, const uint64_t replay_log_id, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObSavedStorageInfoV2 info;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret), K(pkey_));
  } else if (OB_FAIL(get_base_storage_info_(info.get_clog_info()))) {
    STORAGE_LOG(WARN, "fail to get base storage info", K(ret), K(pkey_));
  } else {
    // This frozen memtable will be dumped into the ssstore directly, the log_id
    // is meaningless at the target partition and the checksum and timestamp will
    // not be checked.
    ObDataStorageInfo& data_info = info.get_data_info();
    data_info.set_last_replay_log_id(replay_log_id);
    data_info.set_last_replay_log_ts(snapshot_version);
    data_info.set_publish_version(snapshot_version);
    data_info.set_schema_version(schema_version);
    data_info.set_created_by_new_minor_freeze();

    if (OB_FAIL(pg_storage_.complete_active_memstore(info))) {
      STORAGE_LOG(WARN, "complete active memstore failed", K(ret), K(pkey_));
    } else {
      freeze_record_.clear();
      STORAGE_LOG(INFO, "complete active memstore successfully", K(pkey_));
    }
  }

  return ret;
}

int ObPartitionGroup::check_is_from_restore(bool& is_from_restore) const
{
  int ret = OB_SUCCESS;

  uint64_t last_restore_log_id = OB_INVALID_ID;
  int64_t restore_snapshot_version = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(pg_storage_.get_restore_replay_info(last_restore_log_id, restore_snapshot_version))) {
    STORAGE_LOG(WARN, "fail to get_restore_replay_info", K(ret), K(pkey_));
  } else {
    is_from_restore = (OB_INVALID_ID != last_restore_log_id);
  }

  return ret;
}

int ObPartitionGroup::get_all_saved_info(ObSavedStorageInfoV2& info) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(pg_storage_.get_all_saved_info(info))) {
    STORAGE_LOG(WARN, "fail to get saved clog info", K(ret), K(pkey_));
  }

  return ret;
}

int ObPartitionGroup::get_saved_clog_info(common::ObBaseStorageInfo& clog_info) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(pg_storage_.get_saved_clog_info(clog_info))) {
    STORAGE_LOG(WARN, "fail to get saved clog info", K(ret), K(pkey_));
  }

  return ret;
}

int ObPartitionGroup::get_saved_data_info(ObDataStorageInfo& data_info) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(pg_storage_.get_saved_data_info(data_info))) {
    STORAGE_LOG(WARN, "fail to get saved clog info", K(ret), K(pkey_));
  }

  return ret;
}

int ObPartitionGroup::get_saved_last_log_info(uint64_t& log_id, int64_t& submit_timestamp) const
{
  return get_saved_last_log_info_(log_id, submit_timestamp);
}

int ObPartitionGroup::get_saved_last_log_info_(uint64_t& log_id, int64_t& submit_timestamp) const
{
  int ret = OB_SUCCESS;

  // The clog_info and data_info is separate in design, but in order to reduce the performance
  // overhead the checkpoint of clog_info is the smaller one between clog_info and data_info in
  // fact. So we only take clog_info here.
  ObBaseStorageInfo clog_info;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition not init", K(ret));
  } else if (OB_FAIL(get_saved_clog_info(clog_info))) {
    STORAGE_LOG(WARN, "get clog info failed", K(ret), K(pkey_));
  } else {
    log_id = clog_info.get_last_replay_log_id();
    submit_timestamp = clog_info.get_submit_timestamp();
  }

  return ret;
}

int ObPartitionGroup::append_local_sort_data(
    const ObPartitionKey& pkey, const share::ObBuildIndexAppendLocalDataParam& param, ObNewRowIterator& iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_storage_.append_local_sort_data(pkey, param, iter))) {
    STORAGE_LOG(WARN, "fail to append local sort data", K(ret), K(param));
  }
  return ret;
}

int ObPartitionGroup::append_sstable(
    const ObPartitionKey& pkey, const share::ObBuildIndexAppendSSTableParam& param, common::ObNewRowIterator& iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_storage_.append_sstable(pkey, param, iter))) {
    STORAGE_LOG(WARN, "fail to append sstable", K(ret), K(param));
  }
  return ret;
}

int ObPartitionGroup::check_single_replica_major_sstable_exist(
    const ObPartitionKey& pkey, const uint64_t index_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_storage_.check_single_replica_major_sstable_exist(pkey, index_table_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to check single replica major sstable exist", K(ret));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionGroup::get_table_stat(const common::ObPartitionKey& pkey, ObTableStat& stat)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_storage_.get_table_stat(pkey, stat))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "failed to get table stat", K(ret), K(pkey));
    }
  }
  return ret;
}

int ObPartitionGroup::create_memtable(
    const bool in_slog_trans, const bool is_replay, const bool ignore_memstore_percent)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(split_lock_);

  if (OB_FAIL(create_memtable_(in_slog_trans, is_replay, ignore_memstore_percent))) {
    STORAGE_LOG(WARN, "create memtable erorr", K(ret), K(ignore_memstore_percent), K_(pkey));
  }
  return ret;
}
// The times to be called:
// 1. When the partition group is created at leader.
// 2. During the process of leader takeover after the D replica is selected as leader
// 3. During the migrator process
int ObPartitionGroup::create_memtable_(
    const bool in_slog_trans, const bool is_replay, const bool ignore_memstore_percent)
{
  int ret = OB_SUCCESS;
  ObDataStorageInfo data_info;

  if (OB_FAIL(pg_storage_.get_saved_data_info(data_info))) {
    STORAGE_LOG(WARN, "failed to get data info", K(ret), K(pkey_));
  } else if (OB_FAIL(set_max_passed_trans_version_(data_info.get_publish_version() + 1))) {
    STORAGE_LOG(WARN, "failed to set max passed trans version", K(ret), K(pkey_));
  } else if (!pg_storage_.need_create_memtable()) {
    STORAGE_LOG(INFO, "no need to create memtable", K(pkey_), K(split_info_), K(data_info));
  } else if (OB_FAIL(pg_storage_.create_memtable(in_slog_trans, is_replay, ignore_memstore_percent))) {
    STORAGE_LOG(WARN, "failed to create_memtable", K(ret), K(pkey_));
  } else {
    // do nothing
  }

  return ret;
}

int ObPartitionGroup::decide_split_version_(const int64_t base_ts, int64_t& split_version)
{
  int ret = OB_SUCCESS;
  bool need_retry = true;
  int64_t tmp_split_version = base_ts;

  while (OB_SUCCESS == ret && need_retry) {
    bool safe = false;
    need_retry = false;
    if (OB_FAIL(freeze_record_.set_freeze_upper_limit(tmp_split_version))) {
      // set limit
      if (OB_EAGAIN == ret) {
        // rewrite ret
        ret = OB_SUCCESS;
        need_retry = true;
      }
    } else if (OB_FAIL(is_split_version_safe_(tmp_split_version, safe))) {
      // check safe
      STORAGE_LOG(WARN, "fail to check split version safety", K(ret), K(pkey_), K(split_version));
    } else if (!safe) {
      need_retry = true;
    } else {
      split_version = tmp_split_version;
      break;
    }
    if (need_retry) {
      // Add 1ms if the split version is too small.
      tmp_split_version = tmp_split_version + SPLIT_FREEZE_WAIT_TS;
    }
  }
  if (OB_FAIL(ret)) {
    freeze_record_.clear_freeze_upper_limit();
  }

  return ret;
}

int ObPartitionGroup::is_split_version_safe_(const int64_t split_version, bool& safe)
{
  int ret = OB_SUCCESS;
  int64_t base_version = INT64_MAX;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (0 == pg_storage_.get_partition_cnt()) {
    safe = true;
  } else if (OB_FAIL(pg_storage_.get_active_memtable_base_version(base_version))) {
    STORAGE_LOG(WARN, "check split version safe or not", K(split_version), K(safe), K_(pkey));
  } else {
    safe = (base_version < split_version);
  }

  return ret;
}

int ObPartitionGroup::submit_split_source_log_(const ObPartitionSplitSourceLog& log, const int64_t base_ts)
{
  int ret = OB_SUCCESS;
  uint64_t log_id = 0;
  int64_t log_ts = 0;
  ObSplitLogCb* log_cb = NULL;
  const bool is_trans_log = false;

  if (!share::ObMultiClusterUtil::is_cluster_allow_submit_log(pkey_.get_table_id())) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "is_cluster_allow_submit_log return false", K(ret), K(pkey_));
  } else if (NULL == (log_cb = ObSplitLogCbFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "allocate memory failed", K(ret));
  } else {
    int64_t pos = 0;
    char* buf = NULL;
    if (OB_FAIL(log_cb->init(ps_, OB_LOG_SPLIT_SOURCE_PARTITION))) {
      STORAGE_LOG(WARN, "log callback init failed", K(ret));
    } else if (NULL ==
               (buf = reinterpret_cast<char*>(ob_malloc(OB_MAX_LOG_ALLOWED_SIZE, ObModIds::OB_SPLIT_LOG_BUF)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "allocate memory failed", K(ret));
    } else if (OB_FAIL(serialization::encode_i64(buf, OB_MAX_LOG_ALLOWED_SIZE, pos, OB_LOG_SPLIT_SOURCE_PARTITION))) {
    } else if (OB_FAIL(log.serialize(buf, OB_MAX_LOG_ALLOWED_SIZE, pos))) {
      STORAGE_LOG(WARN, "serialize log failed", K(ret));
    } else if (OB_FAIL(pls_->submit_log(buf, pos, base_ts, log_cb, is_trans_log, log_id, log_ts))) {
      STORAGE_LOG(WARN, "submit split source log failed", K(ret));
    } else {
      // do nothing
    }
    if (OB_SUCCESS != ret) {
      ObSplitLogCbFactory::release(log_cb);
      log_cb = NULL;
    }
    if (NULL != buf) {
      ob_free(buf);
      buf = NULL;
    }
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "submit split source log failed", K(ret), K_(pkey), K(base_ts), K(log), K(log_id), K(log_ts));
  } else {
    STORAGE_LOG(INFO, "submit split source log success", K_(pkey), K(base_ts), K(log), K(log_id), K(log_ts));
  }
  return ret;
}

int ObPartitionGroup::submit_split_dest_log_(const ObPartitionSplitDestLog& log)
{
  int ret = OB_SUCCESS;
  ObSplitLogCb* log_cb = NULL;
  uint64_t log_id = 0;
  int64_t log_timestamp = 0;
  const int64_t base_timestamp = 0;
  const bool is_trans_log = false;
  if (!share::ObMultiClusterUtil::is_cluster_allow_submit_log(pkey_.get_table_id())) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "is_cluster_allow_submit_log return false", K(ret), K(pkey_));
  } else if (NULL == (log_cb = ObSplitLogCbFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "allocate memory failed", K(ret));
  } else {
    int64_t pos = 0;
    char* buf = NULL;
    if (OB_FAIL(log_cb->init(ps_, OB_LOG_SPLIT_DEST_PARTITION))) {
      STORAGE_LOG(WARN, "log callback init failed", K(ret));
    } else if (NULL ==
               (buf = reinterpret_cast<char*>(ob_malloc(OB_MAX_LOG_ALLOWED_SIZE, ObModIds::OB_SPLIT_LOG_BUF)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "allocate memory failed", K(ret));
    } else if (OB_FAIL(serialization::encode_i64(buf, OB_MAX_LOG_ALLOWED_SIZE, pos, OB_LOG_SPLIT_DEST_PARTITION))) {
    } else if (OB_FAIL(log.serialize(buf, OB_MAX_LOG_ALLOWED_SIZE, pos))) {
      STORAGE_LOG(WARN, "serialize log failed", K(ret));
    } else if (OB_FAIL(pls_->submit_log(buf, pos, base_timestamp, log_cb, is_trans_log, log_id, log_timestamp))) {
      STORAGE_LOG(WARN, "submit split dest log failed", K(ret));
    } else {
      STORAGE_LOG(INFO, "submit split dest log success", K(log), KP(log_cb));
    }
    if (OB_SUCCESS != ret) {
      ObSplitLogCbFactory::release(log_cb);
      log_cb = NULL;
    }
    if (NULL != buf) {
      ob_free(buf);
      buf = NULL;
    }
  }
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN, "submit split dest log failed", K(ret), K_(pkey), K(log));
  } else {
    STORAGE_LOG(INFO, "submit split dest log success", K_(pkey), K(log));
  }
  return ret;
}

int ObPartitionGroup::set_split_version_(const int64_t split_version)
{
  int ret = OB_SUCCESS;
  int64_t base_version = INT64_MAX;
  if (OB_FAIL(pg_storage_.get_active_memtable_base_version(base_version))) {
    STORAGE_LOG(WARN, "failed to get active memtable", K(ret), K_(pkey));
  } else if (base_version >= split_version) {
    // base version has been set
    STORAGE_LOG(INFO, "split version has been set", K_(pkey), K(base_version), K(split_version));
  } else if (OB_FAIL(pg_storage_.set_publish_version_after_create(split_version))) {
    STORAGE_LOG(WARN, "set publish version after create failed", K(ret), K_(pkey), K(split_version));
  } else if (OB_FAIL(set_max_passed_trans_version_(split_version + 1))) {
    STORAGE_LOG(ERROR, "set max passed trans version failed", K(ret), K_(pkey), K(split_version));
  } else {
    STORAGE_LOG(INFO, "set publish version after create success", K_(pkey), K(split_version));
  }

  return ret;
}

int ObPartitionGroup::push_split_task_(const int64_t schema_version, const ObSplitPartitionPair& info)
{
  int ret = OB_SUCCESS;
  ObPartitionSplitTask* task = NULL;
  if (NULL == (task = ObPartitionSplitTaskFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "allocate memory failed", K(ret));
  } else {
    if (OB_FAIL(task->init(schema_version, info))) {
      STORAGE_LOG(WARN, "split task init failed", K(ret));
    } else {
      ObPartitionSplitWorker* split_worker = ps_->get_split_worker();
      if (NULL == split_worker) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "split worker is null", K(ret), KP(split_worker));
      } else if (OB_FAIL(split_worker->push(task))) {
        STORAGE_LOG(WARN, "push split worker failed", K(ret), K(*task));
      } else {
        // do nothing
      }
    }
    if (OB_SUCCESS != ret) {
      ObPartitionSplitTaskFactory::release(task);
      task = NULL;
    }
  }
  return ret;
}

int ObPartitionGroup::save_split_state_(const int64_t state, const bool write_slog)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ret = pg_storage_.save_split_state(state, write_slog);
  const int64_t end_ts = ObTimeUtility::current_time();
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(WARN,
        "save split state failed",
        K(ret),
        K_(pkey),
        "state",
        to_state_str(static_cast<ObPartitionSplitStateEnum>(state)),
        K(write_slog),
        "used_time",
        end_ts - start_ts);
  } else {
    STORAGE_LOG(INFO,
        "save split state success",
        K_(pkey),
        "state",
        to_state_str(static_cast<ObPartitionSplitStateEnum>(state)),
        K(write_slog),
        "used_time",
        end_ts - start_ts);
  }
  return ret;
}

int ObPartitionGroup::save_split_info(const ObPartitionSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(split_lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (!split_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "split info is not valid", K(ret), K(split_info));
  } else if (OB_FAIL(pg_storage_.save_split_info(split_info, true /*write_slog*/))) {
    STORAGE_LOG(WARN, "pg storage save split info and state", K(ret), K(pkey_), K(split_info));
  } else if (OB_FAIL(split_info_.assign(split_info))) {
    STORAGE_LOG(WARN, "failed to assign split info", K(ret), K_(pkey), K(split_info));
  }
  return ret;
}

int ObPartitionGroup::save_split_state(const int64_t split_state)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(split_lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else if (!is_valid_split_state(static_cast<ObPartitionSplitStateEnum>(split_state))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(pkey_), K(split_state));
  } else if (OB_FAIL(pg_storage_.save_split_state(split_state, true /*wrtie_slog*/))) {
    STORAGE_LOG(WARN, "pg storage save split info and state", K(ret), K(pkey_), K(split_state));
  } else if (OB_FAIL(split_state_.set_state(static_cast<ObPartitionSplitStateEnum>(split_state)))) {
    STORAGE_LOG(WARN, "failed to set split state", K(ret), K_(pkey), K(split_state));
  }
  return ret;
}

int ObPartitionGroup::save_split_info_(const ObPartitionSplitInfo& split_info, const bool write_slog)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ret = pg_storage_.save_split_info(split_info, write_slog);
  const int64_t end_ts = ObTimeUtility::current_time();
  if (OB_SUCCESS != ret) {
    STORAGE_LOG(
        WARN, "save split info failed", K(ret), K_(pkey), K(split_info), K(write_slog), "used_time", end_ts - start_ts);
  } else {
    STORAGE_LOG(
        INFO, "save split info success", K_(pkey), K(split_info), K(write_slog), "used_time", end_ts - start_ts);
  }
  return ret;
}

int ObPartitionGroup::split_dest_partitions_(bool& is_all_finished)
{
  int ret = OB_SUCCESS;
  // must be inited true
  bool tmp_all_finished = true;
  ObSArray<ObPartitionKey> partitions;
  if (OB_FAIL(partition_split_progress_array_.get_not_finished(partitions))) {
    STORAGE_LOG(WARN, "get not mask failed", K(ret));
  } else {
    for (int64_t i = 0; i < partitions.count(); i++) {
      enum ObSplitProgress progress = UNKNOWN_SPLIT_PROGRESS;
      const ObPartitionKey& dest_partition = partitions.at(i);
      ObAddr leader;
      // ignore tmp_ret, continue
      if (OB_FAIL(ps_->nonblock_get_strong_leader_from_loc_cache(dest_partition, leader))) {
        STORAGE_LOG(WARN, "get leader from loc cache failed", K(ret), K(dest_partition));
      } else if (ps_->get_self_addr() != leader) {
        tmp_all_finished = false;
        ObIPartitionServiceRpc& rpc = ps_->get_pts_rpc();
        ObSplitDestPartitionRequestArg arg;
        if (OB_FAIL(arg.init(dest_partition, split_info_))) {
          STORAGE_LOG(WARN, "init split dest partition request failed", K(ret), K(dest_partition));
        } else if (OB_FAIL(rpc.post_split_dest_partition_request(leader, dest_partition.get_tenant_id(), arg))) {
          STORAGE_LOG(WARN, "post split dest partition request failed", K(ret));
        } else {
          STORAGE_LOG(INFO, "post split dest partition request success", K(dest_partition), K(leader));
        }
      } else {
        if (OB_FAIL(ps_->split_dest_partition(dest_partition, split_info_, progress))) {
          STORAGE_LOG(WARN, "split dest partition failed", K(ret), K(dest_partition));
          (void)ps_->nonblock_get_strong_leader_from_loc_cache(dest_partition, leader, true);
        } else if (OB_FAIL(partition_split_progress_array_.set_progress(dest_partition, progress))) {
          STORAGE_LOG(WARN, "set progress failed", K(ret), K(dest_partition), K(progress));
        } else if (PHYSICAL_SPLIT_FINISH > progress) {
          tmp_all_finished = false;
        } else {
          // do nothing
        }
      }
      if (OB_SUCCESS != ret) {
        // rewrite ret
        ret = OB_SUCCESS;
        tmp_all_finished = false;
      }
    }
  }
  if (OB_SUCCESS == ret) {
    is_all_finished = tmp_all_finished;
  }
  return ret;
}

int ObPartitionGroup::query_replica_split_progress_(const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObReplicaSplitProgress>& progress_array = replica_split_progress_array_.get_progress_array();
  for (int64_t i = 0; OB_SUCC(ret) && i < progress_array.count(); i++) {
    const ObReplicaSplitProgress& split_progress = progress_array.at(i);
    if (PHYSICAL_SPLIT_FINISH != split_progress.progress_) {
      ObReplicaSplitProgressRequest arg;
      arg.schema_version_ = schema_version;
      arg.pkey_ = pkey_;
      arg.addr_ = split_progress.obj_;
      ObIPartitionServiceRpc& rpc = ps_->get_pts_rpc();
      if (OB_FAIL(rpc.post_replica_split_progress_request(split_progress.obj_, pkey_.get_tenant_id(), arg))) {
        STORAGE_LOG(WARN, "post replica split progress request failed", K(ret), K(arg));
      } else {
        STORAGE_LOG(INFO, "post replica split progress request success", K(arg));
      }
    }
  }
  return ret;
}

int ObPartitionGroup::get_split_partition_member_list_(
    const ObIArray<ObPartitionKey>& pkey_array, ObMemberList& member_list)
{
  int ret = OB_SUCCESS;
  ObMemberList tmp_member_list;
  ObMemberList out_member_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < pkey_array.count(); i++) {
    const ObPartitionKey& pkey = pkey_array.at(i);
    tmp_member_list.reset();
    if (OB_FAIL(ps_->get_curr_member_list(pkey, tmp_member_list))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        // rewrite ret
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "get curr member list failed", K(ret), K(pkey));
      }
    } else if (out_member_list.get_member_number() < tmp_member_list.get_member_number()) {
      out_member_list = tmp_member_list;
    } else {
      // do nothing
    }
  }
  if (OB_SUCC(ret)) {
    if (out_member_list.get_member_number() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected split partition member count", K(ret), K(out_member_list));
    } else {
      member_list = out_member_list;
    }
  }
  return ret;
}

int ObPartitionGroup::get_all_table_ids_(const ObPartitionKey& pkey, ObIArray<uint64_t>& index_tables)
{
  int ret = OB_SUCCESS;
  index_tables.reset();
  if (OB_FAIL(pg_storage_.get_all_table_ids(pkey, index_tables))) {
    STORAGE_LOG(WARN, "get all table ids failed", K(ret));
  }
  return ret;
}

int ObPartitionGroup::get_reference_tables_(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_storage_.get_reference_tables(pkey, index_id, handle))) {
    STORAGE_LOG(WARN, "get reference tables failed", K(ret), K(index_id));
  }
  return ret;
}

int ObPartitionGroup::set_reference_tables_(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_storage_.set_reference_tables(pkey, index_id, handle))) {
    STORAGE_LOG(WARN, "set reference tables failed", K(ret), K(index_id));
    if (OB_ENTRY_NOT_EXIST == ret) {
      // rewrite ret
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObPartitionGroup::push_reference_tables_(const ObIArray<ObPartitionKey>& dest_array, const int64_t split_version)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> index_tables;
  ObIPartitionGroupGuard dest_guard;
  ObIPartitionGroup* dest_partition_group = NULL;
  ObPartitionArray src_pkeys;
  ObPartitionArray dest_pkeys;
  ObTablesHandle handle;
  ObTablesHandle memtables;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("parttion group is not inited", K(ret));
  } else if (OB_FAIL(pg_storage_.get_reference_memtables(memtables))) {
    LOG_WARN("failed to get reference memtables", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_storage_.get_all_pg_partition_keys(src_pkeys))) {
    STORAGE_LOG(WARN, "failed to get all partition keys", K(ret), K_(pkey));
  } else if (0 == src_pkeys.count()) {
    // do nothing
  } else if (OB_FAIL(txs_->copy_trans_table(pkey_, dest_array))) {
    STORAGE_LOG(WARN, "failed to copy trans table", K(ret), K_(pkey), K(dest_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_array.count(); i++) {
      const ObPartitionKey& dest_pgkey = dest_array.at(i);
      bool is_complete = false;
      dest_pkeys.reset();
      dest_partition_group = NULL;
      if (OB_FAIL(ps_->get_partition(dest_pgkey, dest_guard))) {
        if (OB_PARTITION_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          STORAGE_LOG(INFO, "partition not exist, should have finished logical split", K(ret), K(dest_pgkey));
        } else {
          STORAGE_LOG(WARN, "get dest partition failed", K(ret), K(dest_pgkey), K_(pkey));
        }
        // rewrite ret
      } else if (OB_ISNULL(dest_partition_group = dest_guard.get_partition_group())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get dest partition failed, do not push reference tables", K(ret), K(dest_pgkey), K_(pkey));
      } else if (OB_FAIL(dest_partition_group->get_pg_storage().check_complete(is_complete))) {
        STORAGE_LOG(WARN, "failed to check complete", K(ret), K(dest_pgkey), K_(pkey));
      } else if (is_complete) {
        STORAGE_LOG(INFO, "dest pg is complete, no need to push reference tables", K(dest_pgkey), K_(pkey));
      } else if (OB_FAIL(dest_partition_group->set_split_version(split_version))) {
        STORAGE_LOG(WARN, "set split version failed", K(ret), K(split_version), K(dest_pgkey), K_(pkey));
      } else if (OB_FAIL(dest_partition_group->get_pg_storage().set_reference_memtables(memtables))) {
        STORAGE_LOG(WARN, "failed to set reference memtables", K(ret), K(dest_pgkey), K_(pkey));
      } else if (OB_FAIL(dest_partition_group->get_pg_storage().get_all_pg_partition_keys(dest_pkeys))) {
        STORAGE_LOG(WARN, "failed to get all partition keys", K(ret), K(dest_pgkey), K_(pkey));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < dest_pkeys.count(); j++) {
          bool found = false;
          const ObPartitionKey& dest_pkey = dest_pkeys.at(j);
          for (int64_t m = 0; OB_SUCC(ret) && m < src_pkeys.count(); m++) {
            if (src_pkeys.at(m).get_table_id() == dest_pkey.get_table_id()) {
              found = true;
              const ObPartitionKey& src_pkey = src_pkeys.at(m);
              if (OB_FAIL(get_all_table_ids_(src_pkey, index_tables))) {
                STORAGE_LOG(WARN, "failed to get all table ids", K(ret), K_(pkey), K(src_pkey), K(dest_pkey));
              } else {
                for (int64_t k = 0; OB_SUCC(ret) && k < index_tables.count(); k++) {
                  int64_t table_count = 0;
                  const int64_t index_id = index_tables.at(k);
                  if (OB_FAIL(pg_storage_.get_latest_table_count(src_pkey, index_id, table_count))) {
                    STORAGE_LOG(WARN, "failed to get latest table count", K(src_pkey), K(index_id));
                  } else if (0 == table_count) {
                    STORAGE_LOG(INFO, "no sstables, no need to set reference table", K(src_pkey), K(index_id));
                  } else if (OB_FAIL(get_reference_tables_(src_pkey, index_id, handle))) {
                    STORAGE_LOG(WARN, "get reference tables failed", K(ret), K(index_id), K(src_pkey));
                  } else if (OB_FAIL(dest_partition_group->set_reference_tables(dest_pkey, index_id, handle))) {
                    STORAGE_LOG(WARN, "set reference tables failed", K(ret), K(index_id), K(dest_pkey), K(dest_pgkey));
                  } else {
                    STORAGE_LOG(INFO, "set reference tables success", K(index_id), K(dest_pkey), K(dest_pgkey));
                  }
                }
              }
              break;
            }
          }
          if (!found) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "cannot find split source partition", K(ret), K(dest_pkey));
          }
        }
      }
      // The target partition is triggered to dump the transaction state table immediately
      // after the completion of logical splitting.
      if (OB_SUCC(ret)) {
        bool is_merged = false;
        ObPartitionScheduler::get_instance().schedule_pg(
            MINI_MERGE, *dest_partition_group, ObVersion::MIN_VERSION, is_merged);
      }
    }
  }
  return ret;
}

int ObPartitionGroup::get_max_decided_trans_version(int64_t& max_decided_trans_version) const
{
  int ret = OB_SUCCESS;
  int64_t unused_epoch = OB_INVALID_TIMESTAMP;
  ObTsWindows unused_windows;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartition is not inited", K(ret), K(pkey_));
  } else if (OB_FAIL(pls_->get_role_unlock(unused_epoch, unused_windows))) {
    STORAGE_LOG(WARN, "get_role_unlock failed", K(ret), K(pkey_));
  } else {
    const int64_t last_checkpoint = partition_loop_worker_.get_last_checkpoint();
    if (0 < last_checkpoint) {
      if (ObTimeUtility::current_time() - last_checkpoint > 120000000) {
        if (EXECUTE_COUNT_PER_SEC(16)) {
          TRANS_LOG(WARN, "checkpoint is too old", K_(pkey), K(last_checkpoint), K_(partition_loop_worker));
        }
      }
      max_decided_trans_version = last_checkpoint;
    } else {
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

int ObPartitionGroup::allow_gc(bool& allow_gc)
{
  int ret = OB_SUCCESS;
  bool has_migrate_task = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartition is not inited", K(ret), K(pkey_));
  } else if (OB_FAIL(pls_->allow_gc(allow_gc))) {
    STORAGE_LOG(WARN, "ObPartitionLogService allow_gc failed", K(ret), K(pkey_));
  } else if (!allow_gc) {
    STORAGE_LOG(INFO, "ObPartitionLogService is not allowed to gc", K(ret), K(pkey_));
  } else if (OB_FAIL(pg_storage_.allow_gc(allow_gc))) {
    STORAGE_LOG(WARN, "fail to check if pg_storage allow gc", K(ret), K(pkey_));
  } else if (!allow_gc) {
    STORAGE_LOG(INFO, "pg storage is not allowed to gc", K(ret), K(pkey_));
  }

  if (OB_SUCC(ret) && allow_gc) {
    if (OB_FAIL(ObPartGroupMigrator::get_instance().has_task(pkey_, has_migrate_task))) {
      STORAGE_LOG(WARN, "failed to check has task", K(ret), K_(pkey));
    } else if (has_migrate_task) {
      allow_gc = false;
      STORAGE_LOG(INFO, "this partition is migrating in, not allowed to gc", K(pkey_));
    }
  }
  return ret;
}

int ObPartitionGroup::gc_check_valid_member(const bool is_valid, const int64_t gc_seq, bool& need_gc)
{
  int ret = OB_SUCCESS;

  need_gc = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartition is not inited", K(ret));
  } else if (gc_seq <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), K(gc_seq));
  } else if (!is_valid) {
    STORAGE_LOG(INFO, "gc_check_valid_member, invalid member", K(is_valid), K(gc_seq), K(gc_seq_check_valid_member_));
    if (gc_seq == gc_seq_check_valid_member_ + 1) {
      need_gc = true;
    }
    gc_seq_check_valid_member_ = gc_seq;
  }
  return ret;
}

bool ObPartitionGroup::check_pg_partition_offline(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  bool is_offline = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObPartition is not inited", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
    // Both the stand alone partition and PG only need to check the offline_log_id.
  } else if (!pkey_.is_pg() || pkey_ == pkey) {
    is_offline = OB_INVALID_ID != offline_log_id_;
    // The partition in PG need check the tags at partition store.
  } else if (OB_FAIL(pg_storage_.check_pg_partition_offline(pkey, is_offline))) {
    STORAGE_LOG(WARN, "check pg partition offline error", K(ret), K(pkey), K(pkey_));
  } else {
    // do nothing
  }

  return is_offline;
}

int ObPartitionGroup::check_offline_log_archived(
    const ObPartitionKey& pkey, const int64_t incarnation, const int64_t archive_round, bool& has_archived) const
{
  int ret = OB_SUCCESS;
  has_archived = false;
  uint64_t last_archived_log_id = OB_INVALID_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObPartition is not inited", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
    // Both the stand alone partition and PG only need to check the offline_log_id.
  } else if (pkey_.is_pg() && pkey_ != pkey) {
    has_archived = true;
  } else if (OB_INVALID_ID == offline_log_id_) {
    has_archived = false;
  } else if (OB_ISNULL(pls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "NULL ptr", K(ret));
  } else if (OB_FAIL(pls_->get_last_archived_log_id(incarnation, archive_round, last_archived_log_id))) {
    STORAGE_LOG(
        WARN, "failed to get_last_archived_log_id", K(incarnation), K(archive_round), K(last_archived_log_id), K(ret));
  } else {
    has_archived = (last_archived_log_id >= offline_log_id_);
  }

  return ret;
}

int ObPartitionGroup::get_leader_epoch(int64_t& leader_epoch) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartition is not inited", K(ret));
  } else if (OB_ISNULL(pls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "NULL ptr", K(ret));
  } else {
    ObTsWindows unused_windows;
    ret = pls_->get_role_unlock(leader_epoch, unused_windows);
    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "get role unlock failed", K(ret));
    }
  }

  return ret;
}

// TODO reuse sstable in split dest partition
int ObPartitionGroup::get_replica_status(share::ObReplicaStatus& status)
{
  int ret = OB_SUCCESS;
  bool can_migrate = false;
  status = REPLICA_STATUS_MAX;
  ObMigrateStatus migrate_status;

  if (OB_FAIL(pg_storage_.get_pg_migrate_status(migrate_status))) {
    STORAGE_LOG(WARN, "failed to get migrate status", K(ret), K_(pkey));
  } else if (OB_FAIL(check_can_migrate(can_migrate))) {
    STORAGE_LOG(WARN, "failed to check can migrate", K(ret), K_(pkey));
  } else {
    ObPartitionState state = get_partition_state();
    if (!is_working_state(state)) {
      status = REPLICA_STATUS_OFFLINE;
    } else if (OB_MIGRATE_STATUS_MIGRATE_FAIL == migrate_status || OB_MIGRATE_STATUS_ADD_FAIL == migrate_status) {
      status = REPLICA_STATUS_OFFLINE;
    } else if (OB_MIGRATE_STATUS_ADD == migrate_status || OB_MIGRATE_STATUS_MIGRATE == migrate_status) {
      status = REPLICA_STATUS_FLAG;
    } else if (!can_migrate) {
      status = REPLICA_STATUS_UNMERGED;
    } else {
      status = REPLICA_STATUS_NORMAL;
    }
  }

  return ret;
}

// Check if the target replica need gc from replica_status.
// Target replica: R(read only) replica, F(full) replica of backup database (not in member list)
int ObPartitionGroup::is_replica_need_gc(bool& is_offline)
{
  int ret = OB_SUCCESS;

  share::ObReplicaStatus status;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartition is not inited", K(ret), K(pkey_));
  } else if (OB_FAIL(get_replica_status(status))) {
    STORAGE_LOG(WARN, "get_replica_status failed", K(ret), K(pkey_));
  } else {
    is_offline = (REPLICA_STATUS_OFFLINE == status);
  }
  return ret;
}

// Destroy the memtable and sstable of PG or pg partition
int ObPartitionGroup::clear_non_reused_stores_(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  bool cleared_memstore = false;

  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(pkey_));
  } else if (OB_FAIL(pg_storage_.clear_non_reused_stores(pkey, cleared_memstore))) {
    STORAGE_LOG(WARN, "clear non-base store(s) failed.", K(pkey_), K(ret));
  } else if (cleared_memstore) {
    if (OB_FAIL(set_max_passed_trans_version_(0))) {
      STORAGE_LOG(WARN, "set max passed trans version failed", K(ret), K(pkey_));
    } else {
      STORAGE_LOG(INFO, "reset max passed trans version success", K(pkey_));
    }
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    partition_loop_worker_.set_migrating_flag(false);
    STORAGE_LOG(INFO, "clear pg/partition non-reused store(s) success", K(pkey), K(cleared_memstore), K(pkey_));
  }

  return ret;
}

int ObPartitionGroup::set_storage_info(const ObSavedStorageInfoV2& info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(pg_storage_.set_pg_storage_info(info))) {
    STORAGE_LOG(WARN, "failed to set_storage_info", K(ret), K(pkey_));
  } else {
    STORAGE_LOG(INFO, "succeed to set_storage_info", K(pkey_));
  }
  return ret;
}

int ObPartitionGroup::fill_pg_partition_replica(
    const ObPartitionKey& pkey, ObReplicaStatus& replica_status, ObReportStatus& report_status)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(pg_storage_.fill_pg_partition_replica(pkey, report_status))) {
    STORAGE_LOG(WARN, "failed to fill pg partition replica", K(ret), K(pkey));
  } else if (OB_FAIL(get_replica_status(replica_status))) {
    STORAGE_LOG(WARN, "failed to get replica status", K(ret), K(pkey));
  } else {
    STORAGE_LOG(DEBUG, "succeed to get pg partition replica success", K(pkey), K(replica_status), K(report_status));
  }
  return ret;
}

int ObPartitionGroup::fill_replica(share::ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(pg_storage_.fill_replica(replica))) {
    STORAGE_LOG(WARN, "failed to fill replica", K(ret));
  } else if (OB_FAIL(get_replica_status(replica.status_))) {
    STORAGE_LOG(WARN, "failed to get replica status", K(ret));
  } else {
    STORAGE_LOG(TRACE, "succeed to get replica status", K(replica));
  }
  return ret;
}

int ObPartitionGroup::get_merge_priority_info(ObMergePriorityInfo& merge_priority_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_storage_.get_merge_priority_info(merge_priority_info))) {
    STORAGE_LOG(WARN, "failed to get first frozen memtable", K(ret), K_(pkey));
  }

  return ret;
}

// It will only be called in a single thread
int64_t ObPartitionGroup::get_gc_schema_drop_ts()
{
  if (OB_INVALID_TIMESTAMP == gc_schema_drop_ts_) {
    gc_schema_drop_ts_ = ObTimeUtility::current_time();
  }
  return gc_schema_drop_ts_;
}

bool ObPartitionGroup::is_splitting_() const
{
  return in_splitting(split_state_.get_state());
}

bool ObPartitionGroup::is_split_source_partition_() const
{
  return is_source_split(split_state_.get_state());
}

bool ObPartitionGroup::is_split_dest_partition_() const
{
  return is_dest_split(split_state_.get_state());
}

bool ObPartitionGroup::is_dest_splitting_() const
{
  return in_dest_splitting(split_state_.get_state());
}

bool ObPartitionGroup::is_physical_split_finished_() const
{
  return is_physical_split_finished(split_state_.get_state());
}

int ObPartitionGroup::check_physical_split_(bool& finished) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_storage_.check_physical_split(finished))) {
    STORAGE_LOG(WARN, "check physical split failed", K(ret));
  }
  return ret;
}

int ObPartitionGroup::get_dest_split_progress_(int& progress)
{
  int ret = OB_SUCCESS;
  switch (split_state_.get_state()) {
    case LEADER_INIT:
    case FOLLOWER_INIT: {
      progress = PHYSICAL_SPLIT_FINISH;
      break;
    }
    case LEADER_WAIT_SPLIT:
    case FOLLOWER_WAIT_SPLIT:
    case SPLIT_DEST_LOGGING: {
      progress = IN_SPLITTING;
      break;
    }
    case LEADER_LOGICAL_SPLIT_SUCCESS:
    case FOLLOWER_LOGICAL_SPLIT_SUCCESS: {
      progress = LOGICAL_SPLIT_FINISH;
      bool is_physical_split_finished = false;
      if (OB_FAIL(check_physical_split_(is_physical_split_finished))) {
        STORAGE_LOG(WARN, "check physical split failed", K(ret));
      } else if (is_physical_split_finished) {
        if (OB_FAIL(split_state_.switch_state(PHYSICAL_SPLIT_SUCCESS))) {
          STORAGE_LOG(WARN, "switch split state failed", K(ret));
        } else {
          progress = PHYSICAL_SPLIT_FINISH;
        }
      } else {
        // do nothing
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected split state", K(ret), K_(split_state));
    }
  }
  return ret;
}

int ObPartitionGroup::check_if_dest_pg_ready_(const common::ObIArray<common::ObPartitionKey>& dest_pgs, bool& is_ready)
{
  int ret = OB_SUCCESS;
  ObPartitionArray src_pkeys;
  if (OB_FAIL(get_all_pg_partition_keys(src_pkeys))) {
    STORAGE_LOG(WARN, "get source partition group partition failed", K(ret));
  } else {
    bool tmp_ready = true;
    ObPartitionArray dest_pkeys;
    for (int64_t i = 0; OB_SUCC(ret) && tmp_ready && i < dest_pgs.count(); i++) {
      dest_pkeys.reset();
      ObIPartitionGroupGuard pg_guard;
      storage::ObIPartitionGroup* pg = NULL;
      if (OB_FAIL(ps_->get_partition(dest_pgs.at(i), pg_guard))) {
        if (OB_PARTITION_NOT_EXIST == ret) {
          is_ready = true;
          ret = OB_SUCCESS;
          STORAGE_LOG(INFO, "partition not exist, possible for read only partition", K(ret), K(dest_pgs.at(i)));
          break;
        } else {
          STORAGE_LOG(WARN, "get dest partition failed", K(ret), K(dest_pgs.at(i)));
        }
      } else if (OB_ISNULL(pg = pg_guard.get_partition_group())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition should not be null", K(ret), K(dest_pgs.at(i)));
      } else if (OB_FAIL(pg->get_all_pg_partition_keys(dest_pkeys))) {
        STORAGE_LOG(WARN, "get dest partition group partition failed", K(ret), K(dest_pgs.at(i)));
      } else {
        for (int64_t j = 0; j < src_pkeys.count(); j++) {
          bool found = false;
          for (int64_t k = 0; !found && k < dest_pkeys.count(); k++) {
            if (src_pkeys.at(j).get_table_id() == dest_pkeys.at(k).get_table_id()) {
              found = true;
            }
          }
          if (!found) {
            tmp_ready = false;
            STORAGE_LOG(INFO, "some partitions not found", K(src_pkeys), K(dest_pkeys));
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_ready = tmp_ready;
    }
  }
  STORAGE_LOG(INFO, "check if dest partition group ready", K(ret), K(dest_pgs), K(is_ready));
  return ret;
}

int ObPartitionGroup::has_active_memtable(bool& found)
{
  ObTimeGuard tg(__func__, 1000000);
  SpinRLockGuard guard(split_lock_);
  tg.click();
  return has_active_memtable_(found);
}

int ObPartitionGroup::has_active_memtable_(bool& found)
{
  int ret = OB_SUCCESS;
  found = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition is not initialized", K(ret), K(pkey_));
  } else if (is_dest_splitting_()) {
    found = false;
  } else {
    if (!pg_storage_.has_active_memtable()) {
      found = false;
      if (EXECUTE_COUNT_PER_SEC(16)) {
        STORAGE_LOG(INFO, "the partition has no active memtable", K(ret), K(pkey_));
      }
    }
  }
  return ret;
}

int ObPartitionGroup::retire_warmup_store(const bool is_disk_full)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_storage_.retire_warmup_store(is_disk_full))) {
    STORAGE_LOG(WARN, "retire warmup store error", K(ret), K_(pkey));
  }

  return ret;
}

int ObPartitionGroup::enable_write_log(const bool is_replay_old)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_storage_.enable_write_log(is_replay_old))) {
    STORAGE_LOG(WARN, "enable write log error", K(ret), K_(pkey));
  }

  return ret;
}

uint64_t ObPartitionGroup::get_min_replayed_log_id()
{
  uint64_t min_replay_log_id = UINT64_MAX;
  int64_t unused = 0;

  get_min_replayed_log(min_replay_log_id, unused);

  return min_replay_log_id;
}

void ObPartitionGroup::get_min_replayed_log(uint64_t& min_replay_log_id, int64_t& min_replay_log_ts)
{
  uint64_t unreplay_log_id = UINT64_MAX;
  int64_t unreplay_log_ts = 0;
  uint64_t last_replay_log_id = UINT64_MAX;
  int64_t last_replay_log_ts = 0;

  // 1. The left boundary of sliding window.
  pls_->get_last_replay_log(last_replay_log_id, last_replay_log_ts);

  // 2. The minimum continuously replayed log of replay engine.
  replay_status_->get_min_unreplay_log(unreplay_log_id, unreplay_log_ts);
  if (unreplay_log_id <= last_replay_log_id) {
    min_replay_log_id = unreplay_log_id - 1;
    min_replay_log_ts = unreplay_log_ts - 1;
  } else {
    min_replay_log_id = last_replay_log_id;
    min_replay_log_ts = last_replay_log_ts;
  }

  STORAGE_LOG(INFO, "min replayed log", K(pkey_), K(min_replay_log_ts), K(unreplay_log_ts), K(last_replay_log_ts));
}

int ObPartitionGroup::get_min_replayed_log_with_keepalive(uint64_t& min_replay_log_id, int64_t& min_replay_log_ts)
{
  int ret = OB_SUCCESS;
  uint64_t unreplay_log_id = UINT64_MAX;
  int64_t unreplay_log_ts = 0;
  uint64_t next_replay_log_id = UINT64_MAX;
  int64_t next_replay_log_ts = 0;

  // 1. The left boundary of sliding window(including keepalive log).
  if (OB_FAIL(pls_->get_next_replay_log_info(next_replay_log_id, next_replay_log_ts))) {
    STORAGE_LOG(WARN, "get next replay log info failed", K(ret), K(*this));
  } else {
    // 2. The minimum continuously replayed log of replay engine.
    replay_status_->get_min_unreplay_log(unreplay_log_id, unreplay_log_ts);
    if (unreplay_log_id <= next_replay_log_id - 1) {
      min_replay_log_id = unreplay_log_id - 1;
      min_replay_log_ts = unreplay_log_ts - 1;
    } else {
      min_replay_log_id = next_replay_log_id - 1;
      min_replay_log_ts = next_replay_log_ts - 1;
    }

    STORAGE_LOG(INFO,
        "min replayed log with keepalive",
        K(pkey_),
        K(min_replay_log_ts),
        K(min_replay_log_id),
        K(unreplay_log_ts),
        K(unreplay_log_id),
        K(next_replay_log_ts),
        K(next_replay_log_id));
  }

  return ret;
}

int ObPartitionGroup::get_all_pg_partition_keys(ObPartitionArray& pkeys, const bool include_trans_table)
{
  return pg_storage_.get_all_pg_partition_keys(pkeys, include_trans_table);
}

int ObPartitionGroup::add_sstable_for_merge(const ObPartitionKey& pkey, storage::ObSSTable* sstable,
    const int64_t max_kept_major_version_number, ObIPartitionReport& report, ObSSTable* complement_minor_sstable)
{
  bool need_report = false;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sstable) && OB_ISNULL(complement_minor_sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "both sstable and complement sstable is null", K(ret));
  } else {
    ObPartitionKey dest_pkey;

    SpinRLockGuard guard(split_lock_);
    // synchronized with push_reference_tables
    if (OB_NOT_NULL(sstable) &&
        (is_split_source_partition_() || (is_split_dest_partition_() && sstable->get_partition_key() != pkey))) {
      ObIPartitionGroupGuard dest_guard;
      storage::ObIPartitionGroup* dest_partition = NULL;
      const common::ObIArray<common::ObPartitionKey>& dest_pgkeys = split_info_.get_dest_partitions();
      bool is_complete = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < dest_pgkeys.count(); i++) {
        const ObPartitionKey dest_pgkey = dest_pgkeys.at(i);
        if (OB_FAIL(ps_->get_partition(dest_pgkey, dest_guard))) {
          if (OB_PARTITION_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            // do nothing
          } else {
            STORAGE_LOG(WARN, "Fail to get partition", K(ret));
          }
        } else if (OB_ISNULL(dest_partition = dest_guard.get_partition_group())) {
          ret = OB_ERR_SYS;
          LOG_WARN("partition must not null", K(ret), K(dest_pgkey));
        } else if (OB_FAIL(dest_partition->check_complete(is_complete))) {
          LOG_WARN("failed to check complete", K(ret), K(dest_pgkey));
        } else if (!is_complete) {
          LOG_INFO("logical split has not finished, no need to update", K(ret));
          break;
        } else if (OB_FAIL(dest_partition->get_pg_storage().get_pkey_for_table(pkey.get_table_id(), dest_pkey))) {
          LOG_WARN("failed to get pkey for table", K(ret), K(pkey));
        } else if (OB_FAIL(dest_partition->get_pg_storage().add_sstable_for_merge(
                       dest_pkey, sstable, 1 /*max_kept_major_version_number*/, complement_minor_sstable))) {
          LOG_WARN("failed to add sstable", K(ret), K(dest_pkey));
        }
      }
      if (OB_SUCC(ret)) {
        need_report = true;
      }
    }

    if (OB_SUCC(ret)) {
      if ((OB_ISNULL(sstable) && OB_NOT_NULL(complement_minor_sstable)) || sstable->get_partition_key() == pkey ||
          sstable->is_trans_sstable()) {
        if (OB_FAIL(pg_storage_.add_sstable_for_merge(
                pkey, sstable, max_kept_major_version_number, complement_minor_sstable))) {
          LOG_WARN("failed to add table", K(ret), K_(pkey), K(pkey), K(sstable));
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(sstable) && sstable->is_minor_sstable()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = pg_storage_.clear_unused_trans_status()))) {
      LOG_WARN("failed to check release memtable", K(tmp_ret), K(pkey));
    }
  }

  if (need_report) {
    int tmp_ret = OB_SUCCESS;
    bool is_merged = false;
    const common::ObIArray<common::ObPartitionKey>& dest_pgkeys = split_info_.get_dest_partitions();
    for (int64_t i = 0; i < dest_pgkeys.count(); i++) {
      const ObPartitionKey dest_pgkey = dest_pgkeys.at(i);
      if (OB_SUCCESS != (tmp_ret = report.submit_pt_update_task(dest_pgkey))) {
        STORAGE_LOG(ERROR, "fail to submit pt update task", K(tmp_ret));
      }
      // Trigger target partition physical split immediately.
      ObPartitionScheduler::get_instance().schedule_merge(dest_pgkey, is_merged);
    }
  }

  return ret;
}

// Check if the replica is readable or not. The read snapshot version should
// be smaller than or equal to weak read timestamp.
int ObPartitionGroup::check_replica_ready_for_bounded_staleness_read(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  int64_t base_snapshot_version = 0;
  if (OB_FAIL(get_weak_read_timestamp(base_snapshot_version))) {
    TRANS_LOG(WARN, "get min safe slave read timestamp error", K(ret), K(base_snapshot_version), "context", *this);
  } else if (snapshot_version > base_snapshot_version) {
    TRANS_LOG(WARN,
        "read snapshot version is greater than replica slave read timestamp, "
        "replica not readable",
        K(snapshot_version),
        K(base_snapshot_version),
        "delta",
        snapshot_version - base_snapshot_version);
    ret = OB_REPLICA_NOT_READABLE;
  } else {
    // readable
  }
  return ret;
}

int ObPartitionGroup::set_offline_log_id(const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionGroup not inited", K(pkey_), K(ret));
  } else {
    ATOMIC_CAS(&offline_log_id_, common::OB_INVALID_ID, log_id);
    // Can only change offline_log_id smaller, due to the GC limitation of archiving
    while (true) {
      uint64_t old_offlinie_log_id = ATOMIC_LOAD(&offline_log_id_);
      if ((common::OB_INVALID_ID == old_offlinie_log_id) || log_id < old_offlinie_log_id) {
        if (ATOMIC_BCAS(&offline_log_id_, old_offlinie_log_id, log_id)) {
          break;
        }
      } else {
        break;
      }
    }
    STORAGE_LOG(INFO, "set_offline_log_id", K(pkey_), K(log_id), K(offline_log_id_));
  }
  return ret;
}

int ObPartitionGroup::feedback_scan_access_stat(const ObTableScanParam& param)
{
  return pg_storage_.feedback_scan_access_stat(param);
}

int ObPartitionGroup::create_sstable(
    const ObPGCreateSSTableParam& param, ObTableHandle& table_handle, const bool in_slog_trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionGroup has not been inited", K(ret));
  } else if (OB_FAIL(pg_storage_.create_sstable(param, in_slog_trans, table_handle))) {
    STORAGE_LOG(WARN, "fail to create sstable", K(ret));
  }
  return ret;
}

int ObPartitionGroup::create_sstables(const common::ObIArray<ObPGCreateSSTableParam>& create_sstable_params,
    ObTablesHandle& tables_handle, const bool in_slog_trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionGroup has not been inited", K(ret));
  } else if (OB_FAIL(pg_storage_.create_sstables(create_sstable_params, in_slog_trans, tables_handle))) {
    STORAGE_LOG(WARN, "fail to create sstables", K(ret));
  }
  return ret;
}

int ObPartitionGroup::get_checkpoint_info(common::ObArenaAllocator& allocator, ObPGCheckpointInfo& pg_checkpoint_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionGroup has not been inited", K(ret));
  } else if (OB_FAIL(pg_storage_.get_checkpoint_info(allocator, pg_checkpoint_info))) {
    LOG_WARN("fail to write pg checkpoint entry", K(ret));
  }
  return ret;
}

int ObPartitionGroup::acquire_sstable(const ObITable::TableKey& table_key, ObTableHandle& table_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionGroup has not been inited", K(ret));
  } else if (OB_FAIL(pg_storage_.acquire_sstable(table_key, table_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to acquire sstable", K(ret));
    }
  }
  return ret;
}

bool ObPartitionGroup::need_replay_redo() const
{
  return pg_storage_.need_create_memtable();
}
// Only deal with the situation that both the split info exists and the origin partition is in schema.
// The situation of tenant deletion or table deletion will not be handled.
int ObPartitionGroup::try_clear_split_info()
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  int64_t local_schema_version = OB_INVALID_VERSION;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!split_info_.is_valid()) {
    // nothing todo
  } else if (pkey_ != split_info_.get_src_partition()) {
    // not the origin partition
  } else {
    ObSchemaGetterGuard guard;
    const ObPartitionSchema* partition_schema = NULL;
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(pkey_.get_tenant_id(), guard))) {
      LOG_WARN("failed to get tenant schema guard", K(ret), K_(pkey));
    } else if (pkey_.is_pg()) {
      const ObTablegroupSchema* tablegroup_schema = NULL;
      if (OB_FAIL(guard.get_tablegroup_schema(pkey_.get_tablegroup_id(), tablegroup_schema))) {
        LOG_WARN("failed to get tablegroup schema", K(ret), K_(pkey));
      } else if (OB_ISNULL(tablegroup_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup schema is null", K(ret), K_(pkey));
      } else {
        partition_schema = tablegroup_schema;
      }
    } else {
      const ObSimpleTableSchemaV2* table_schema = NULL;
      if (OB_FAIL(guard.get_table_schema(pkey_.get_table_id(), table_schema))) {
        LOG_WARN("failed to get table schema guard", K(ret), K_(pkey));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get table schema", K(ret), K_(pkey));
      } else {
        partition_schema = table_schema;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObPartMgrUtils::check_part_exist(
                   *partition_schema, pkey_.get_partition_id(), false /*check_dropped_partition*/, is_exist))) {
      LOG_WARN("failed to check part exist", K(ret), K_(pkey), "schema", *partition_schema);
    } else if (!is_exist) {
      // Does not exist in the schema table, no need to consider it.
    } else if (OB_FAIL(guard.get_schema_version(pkey_.get_tenant_id(), local_schema_version))) {
      STORAGE_LOG(WARN, "fail to get schema guard version", K(ret), K_(pkey));
    } else {
      // It is the origin partition of split and is in the schema table, we need check whether split info
      // is valid.
      if (split_info_.get_schema_version() < local_schema_version) {
        // It means that the split request with a split_info is failed, if the local refreshed schema
        // version is greater than that in split_info, and the origin partition is still in schema table.
        // The split info can be deleted to prevent misjudgment.
        LOG_INFO("need to clear split info", K(ret), K(local_schema_version), K_(split_info));
        ObPartitionSplitInfo new_split_info;
        if (OB_FAIL(pg_storage_.clear_split_info())) {
          LOG_WARN("failed to save split info", K(ret), K_(split_info));
        } else {
          split_info_.reset();
          LOG_INFO("succeed to clear split info", K(ret), K_(pkey), K_(split_info));
        }
      }
    }
  }
  return OB_SUCCESS;
}

// If the log id of the transaction crosses the freeze_id, it means that it
// is dirty.
int ObPartitionGroup::check_dirty_txn(
    const int64_t min_log_ts, const int64_t max_log_ts, int64_t& freeze_ts, bool& is_dirty)
{
  int ret = OB_SUCCESS;
  const ObMemtable* mt = NULL;
  is_dirty = false;

  // Mutex with freeze point select
  SpinRLockGuard guard(freeze_lock_);

  freeze_ts = freeze_record_.get_freeze_ts();

  if (ObFreezeRecord::OB_INVALID_FREEZE_TS != freeze_ts) {
    is_dirty = min_log_ts <= freeze_ts && freeze_ts < max_log_ts;
  }

  return ret;
}

int ObPartitionGroup::try_update_clog_member_list(const uint64_t ms_log_id, const int64_t mc_timestamp,
    const int64_t replica_num, const ObMemberList& mlist, const common::ObProposalID& ms_proposal_id)
{
  int ret = OB_SUCCESS;

  ObBaseStorageInfo curr_clog_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionGroup not inited", K(ret), K(pkey_));
  } else if (OB_FAIL(get_saved_clog_info(curr_clog_info))) {
    STORAGE_LOG(WARN, "fail to get saved clog info", K(ret), K(pkey_));
  } else if (curr_clog_info.get_membership_timestamp() >= mc_timestamp) {
    // curr_clog_info is new enough, no need to update
    STORAGE_LOG(WARN,
        "curr_clog_info no need update",
        K(ret),
        K(pkey_),
        K(curr_clog_info),
        K(ms_log_id),
        K(mc_timestamp),
        K(replica_num),
        K(mlist),
        K(ms_proposal_id));
  } else if (OB_FAIL(pg_storage_.try_update_member_list(ms_log_id, mc_timestamp, replica_num, mlist, ms_proposal_id))) {
    STORAGE_LOG(WARN,
        "fail to update member list",
        K(ret),
        K(pkey_),
        K(ms_log_id),
        K(mc_timestamp),
        K(replica_num),
        K(mlist),
        K(ms_proposal_id));
  } else {
    STORAGE_LOG(INFO, "update member list success", K(pkey_), K(ms_log_id), K(mc_timestamp), K(replica_num), K(mlist));
  }

  return ret;
}

int ObPartitionGroup::check_complete(bool& is_complete)
{
  return pg_storage_.check_complete(is_complete);
}

int ObPartitionGroup::check_physical_flashback_succ(const obrpc::ObCheckPhysicalFlashbackArg& arg,
    const int64_t max_version, obrpc::ObPhysicalFlashbackResultArg& result)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionGroup not inited", K(pkey_), K(ret));
  } else if (ObMultiClusterUtil::is_cluster_private_table(pkey_.get_table_id())) {
    if (OB_FAIL(check_private_table_flashback_result_(max_version, result))) {
      LOG_WARN("failed to check private table flashback succ", K(ret), K(arg));
    }
  } else {
    if (OB_FAIL(check_non_private_table_flashback_result_(arg.flashback_scn_, result.enable_result_))) {
      LOG_WARN("failed to check non private table flashback result", K(arg), K(result));
    }
  }

  return ret;
}

int ObPartitionGroup::check_private_table_flashback_result_(
    const int64_t max_version, obrpc::ObPhysicalFlashbackResultArg& result)
{
  int ret = OB_SUCCESS;
  bool is_empty_pg = pg_storage_.is_empty_pg();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionGroup not inited", K(pkey_), K(ret));
  } else if (!ObMultiClusterUtil::is_cluster_private_table(pkey_.table_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("non private table is invalid", K(ret), K_(pkey));
  } else if (is_pg() && is_empty_pg) {
    result.enable_result_ = true;
    LOG_INFO("empty pg", K(ret), K_(pkey));
  } else if (!common::ObReplicaTypeCheck::is_replica_with_ssstore(get_replica_type())) {
    result.enable_result_ = true;
    LOG_INFO(
        "without sstable replica physical flashback success", K(ret), K_(pkey), "replica_type", get_replica_type());
  } else if (OB_FAIL(get_pg_storage().get_min_max_major_version(result.min_version_, result.max_version_))) {
    LOG_WARN("failed to get min and max major version", K(ret), K_(pkey));
  } else if (INT64_MIN == max_version || max_version == result.max_version_) {
    result.enable_result_ = true;
    LOG_INFO("private table is valid", K(ret), K_(pkey), K(result));
  } else if (max_version != result.max_version_) {
    result.enable_result_ = false;
    LOG_WARN("private table not valid", K(ret), K_(pkey), K(max_version), K(result));
  }
  return ret;
}

int ObPartitionGroup::check_non_private_table_flashback_result_(const int64_t flashback_scn, bool& result)
{
  int ret = OB_SUCCESS;
  result = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionGroup not inited", K(pkey_), K(ret));
  } else if (ObMultiClusterUtil::is_cluster_private_table(pkey_.table_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("private table is invalid", K(ret), K_(pkey));
  } else {
    ObIPartitionLogService* pls = NULL;
    int64_t sstable_ts = 0;
    if (OB_ISNULL(pls = get_log_service())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get_log_service failed", K(ret), K_(pkey));
    } else if (flashback_scn < pls->get_last_submit_timestamp()) {
      result = false;
      LOG_WARN("failed to do physical flashback",
          K(ret),
          "start_id",
          pls->get_last_submit_timestamp(),
          K_(pkey),
          K(flashback_scn));
    } else if (OB_FAIL(get_pg_storage().get_max_major_sstable_snapshot(sstable_ts))) {
      STORAGE_LOG(WARN, "failed to get max sstable snapshop", K(ret), K(sstable_ts));
    } else if (sstable_ts > flashback_scn) {
      result = false;
      LOG_WARN("physical flashback failed", K(ret), K(sstable_ts), K(flashback_scn), K_(pkey));
    } else {
      result = true;
      STORAGE_LOG(INFO, "check physical flashback succ", K(ret), K(sstable_ts), K_(pkey), K(result));
    }
  }
  return ret;
}

int ObPartitionGroup::get_merge_log_ts(int64_t& merge_ts)
{
  int ret = OB_SUCCESS;

  int64_t applied_log_ts = OB_INVALID_TIMESTAMP;

  ObPartitionGroupLockGuard guard(lock_, PGLOCKTRANS | PGLOCKREPLAY | PGLOCKCLOG, 0);
  uint64_t unused = 0;
  get_min_replayed_log(unused, merge_ts);

  if (OB_ISNULL(txs_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "txs_ is NULL", K(ret), K(pkey_));
  } else if (OB_FAIL(txs_->get_applied_log_ts(pkey_, applied_log_ts))) {
    STORAGE_LOG(WARN, "fail to get applied log id", K(ret), K(pkey_));
  } else {
    STORAGE_LOG(INFO, "get merge log id", K(merge_ts), K(applied_log_ts));
    merge_ts = MIN(merge_ts, applied_log_ts);
  }

  return ret;
}

int ObPartitionGroup::recycle_unused_sstables(const int64_t max_recycle_cnt, int64_t& recycled_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_storage_.recycle_unused_sstables(max_recycle_cnt, recycled_cnt))) {
    LOG_WARN("fail to recycle unused sstables", K(ret));
  }
  return ret;
}

int ObPartitionGroup::set_meta_block_list(const common::ObIArray<blocksstable::MacroBlockId>& meta_block_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionGroup has not been inited", K(ret));
  } else if (OB_FAIL(pg_storage_.set_meta_block_list(meta_block_list))) {
    LOG_WARN("fail to set meta block list", K(ret));
  }
  return ret;
}

int ObPartitionGroup::physical_flashback(const int64_t flashback_scn)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionGroup not inited", K(pkey_), K(ret));
  } else if (flashback_scn <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "physical flashback get invalid argument", K(ret), K(flashback_scn));
  } else if (OB_FAIL(pg_storage_.physical_flashback(flashback_scn))) {
    STORAGE_LOG(WARN, "failed to do physical flashback", K(ret), K(pkey_));
  }
  return ret;
}

int ObPartitionGroup::get_meta_block_list(common::ObIArray<blocksstable::MacroBlockId>& meta_block_list) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionGroup has not been inited", K(ret));
  } else if (OB_FAIL(pg_storage_.get_meta_block_list(meta_block_list))) {
    LOG_WARN("fail to get meta block list", K(ret));
  }
  return ret;
}

int ObPartitionGroup::set_storage_file(ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionGroup has not been inited", K(ret));
  } else if (OB_FAIL(pg_storage_.set_storage_file(file_handle))) {
    LOG_WARN("fail to set storage file", K(ret));
  }
  return ret;
}

int ObPartitionGroup::get_all_tables(ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  tables_handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionGroup has not been inited", K(ret));
  } else {
    ObPGPartition* pg_partition = nullptr;
    ObSinglePGPartitionIterator iter;
    ObTablesHandle tmp_tables_handle;
    const bool need_trans_table = true;
    if (OB_FAIL(iter.init(this, need_trans_table))) {
      LOG_WARN("fail to init single pg partition iter", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter.get_next(pg_partition))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next partition", K(ret));
          }
        } else if (OB_ISNULL(pg_partition)) {
          ret = OB_ERR_SYS;
          LOG_WARN("error sys, pg partition must not be null", K(ret));
        } else if (OB_ISNULL(pg_partition->get_storage())) {
          ret = OB_ERR_SYS;
          LOG_WARN("error sys, storage must not be null", K(ret));
        } else if (OB_FAIL(pg_partition->get_storage()->get_all_tables(tmp_tables_handle))) {
          LOG_WARN("fail to get all tables", K(ret));
        } else if (OB_FAIL(tables_handle.add_tables(tmp_tables_handle))) {
          LOG_WARN("fail to add tables", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPartitionGroup::get_table_store_cnt(int64_t& table_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_storage_.get_table_store_cnt(table_cnt))) {
    STORAGE_LOG(WARN, "fail to get table store cnt", K(ret), K(table_cnt));
  }
  return ret;
}

int ObPartitionGroup::check_can_free(bool& can_free)
{
  can_free = false;
  return pg_storage_.check_can_free(can_free);
}

int ObPartitionGroup::check_can_physical_flashback(const int64_t flashback_scn)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionGroup not inited", K(pkey_), K(ret));
  } else if (flashback_scn <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "physical flashback get invalid argument", K(ret), K(flashback_scn));
  } else if (OB_FAIL(pg_storage_.check_can_physical_flashback(flashback_scn))) {
    STORAGE_LOG(WARN, "[PHY_FLASHBACK]check_can_physical_flashback failed", K(ret), K(pkey_), K(flashback_scn));
  } else {
    STORAGE_LOG(INFO, "[PHY_FLASHBACK]check_can_physical_flashback success", K(ret), K(pkey_), K(flashback_scn));
  }
  return ret;
}

int ObPartitionGroup::clear_trans_after_restore_log(const uint64_t last_restore_log_id)
{
  int ret = OB_SUCCESS;

  ObPartitionGroupLockGuard guard(lock_, PGLOCKTRANS | PGLOCKSTORAGE, 0);
  if (OB_SYS_TENANT_ID == pkey_.get_tenant_id()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "sys partitions do not do physical restore", K(ret), K(pkey_));
  } else if (OB_ISNULL(txs_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "txs_ is NULL", KR(ret), K_(pkey));
  } else if (OB_UNLIKELY(OB_INVALID_ID == last_restore_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid last_restore_log_id", KR(ret), K_(pkey), K(last_restore_log_id));
  } else if (OB_FAIL(pg_storage_.set_last_restore_log_id(last_restore_log_id))) {
    CLOG_LOG(WARN, "failed to set_last_restore_log_id", K(ret), K_(pkey), K(last_restore_log_id));
  } else if (OB_FAIL(txs_->set_last_restore_log_id(pkey_, last_restore_log_id))) {
    STORAGE_LOG(WARN, "failed to set_last_restore_log_id", KR(ret), K_(pkey), K(last_restore_log_id));
  } else {
    ATOMIC_SET(&has_clear_trans_after_restore_, true);
  }
  return ret;
}

int ObPartitionGroup::get_base_storage_info_(common::ObBaseStorageInfo& info)
{
  int ret = OB_SUCCESS;
  uint64_t sw_last_replay_log_id = OB_INVALID_ID;
  if (OB_FAIL(pls_->get_base_storage_info(info, sw_last_replay_log_id))) {
    STORAGE_LOG(WARN, "fail to get base clog info", K(ret), K(pkey_));
  } else {
    int64_t restore_snapshot_version = OB_INVALID_TIMESTAMP;
    uint64_t last_restore_log_id = OB_INVALID_ID;
    if (OB_FAIL(pg_storage_.get_restore_replay_info(last_restore_log_id, restore_snapshot_version))) {
      STORAGE_LOG(WARN, "failed to get_restore_replay_info", KR(ret), K(pkey_));
    } else if (OB_INVALID_TIMESTAMP != restore_snapshot_version) {
      // The last_replay_log_id of recovered partition needs to be adjusted.
      if (OB_INVALID_ID == last_restore_log_id) {
        // It is still pulling log or before that, ajust info's last replay log id,
        // max_flush_ilog_id does not need consider.
        if (info.get_last_replay_log_id() < sw_last_replay_log_id) {
          info.set_last_replay_log_id(sw_last_replay_log_id);
        }
      } else if (info.get_last_replay_log_id() < last_restore_log_id) {
        info.set_last_replay_log_id(std::min(sw_last_replay_log_id, last_restore_log_id));
      }
    }
  }
  return ret;
}

int ObPartitionGroup::reset_for_replay()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rp_eg_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init, rp_eg_ is NULL", K(ret), K(pkey_));
  } else if (OB_FAIL(rp_eg_->reset_partition(pkey_))) {
    STORAGE_LOG(WARN, "failed to reset_partition", K(ret), K(pkey_));
  } else {
    STORAGE_LOG(INFO, "success to reset_for_replay", K(ret), K(pkey_));
  }
  return ret;
}

int ObPartitionGroup::get_trans_split_info(ObTransSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(split_lock_);
  split_info.reset();
  const share::ObSplitPartitionPair& spp = split_info_.get_spp();
  if (OB_FAIL(split_info.init(spp.get_source_pkey(), spp.get_dest_array()))) {
    TRANS_LOG(WARN, "init trans split info failed", KR(ret));
  }

  return ret;
}

int ObPartitionGroup::inc_pending_batch_commit_count(memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else {
    if (OB_FAIL(pg_storage_.inc_pending_batch_commit_count(mt_ctx, log_ts))) {
      STORAGE_LOG(WARN, "failed to inc_pending_batch_commit_count", K(ret), K_(pkey), K(log_ts));
    }
  }

  return ret;
}

int ObPartitionGroup::inc_pending_elr_count(memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Partition object not initialized", K(ret), K(is_inited_));
  } else {
    if (OB_FAIL(pg_storage_.inc_pending_elr_count(mt_ctx, log_ts))) {
      STORAGE_LOG(WARN, "failed to inc_pending_elr_count", K(ret), K_(pkey), K(log_ts));
    }
  }

  return ret;
}

}  // namespace storage
}  // end of namespace oceanbase
