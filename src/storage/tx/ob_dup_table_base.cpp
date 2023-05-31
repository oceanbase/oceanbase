// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "logservice/ob_log_base_header.h"
#include "ob_dup_table_base.h"
#include "ob_dup_table_lease.h"
#include "ob_dup_table_tablets.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{

using namespace storage;

namespace transaction
{

const uint64_t DupTableDiagStd::DUP_DIAG_INFO_LOG_BUF_LEN[3] = {
    1 << 12, // 4K
    1 << 16, // 64k
    1 << 12, // 4k
};
const char *DupTableDiagStd::DUP_DIAG_INDENT_SPACE = "    "; // 4
const char *DupTableDiagStd::DUP_DIAG_COMMON_PREFIX = "DUP_TABLE_DIAG: ";
const int64_t DupTableDiagStd::DUP_DIAG_PRINT_INTERVAL[DupTableDiagStd::TypeIndex::MAX_INDEX] = {
    ObDupTableLSLeaseMgr::DEFAULT_LEASE_INTERVAL,
    30 * 1000 * 1000,    // 10s , tablet_print_interval
    3 * 60 * 1000 * 1000 // 3min , ts_sync_print_interval
};

/*******************************************************
 *  HashMapTool (not thread safe)
 *******************************************************/
// nothing

/*******************************************************
 *  Dup_Table Lease
 *******************************************************/
OB_SERIALIZE_MEMBER(DupTableDurableLease, request_ts_, lease_interval_us_);
OB_SERIALIZE_MEMBER(DupTableLeaseItem, log_header_, durable_lease_);
OB_SERIALIZE_MEMBER(DupTableDurableLeaseLogBody, durable_lease_);
OB_SERIALIZE_MEMBER(DupTableLeaseLogHeader, addr_, lease_log_code_);

/*******************************************************
 *  Dup_Table Tablets
 *******************************************************/
// nothing

/*******************************************************
 *  Dup_Table Checkpoint
 *******************************************************/

OB_SERIALIZE_MEMBER(ObDupTableLSCheckpoint::ObLSDupTableMeta,
                    ls_id_,
                    lease_item_array_,
                    lease_log_applied_scn_,
                    readable_tablets_base_scn_,
                    readable_tablets_min_base_applied_scn_);

int ObDupTableLSCheckpoint::ObLSDupTableMeta::copy(const ObLSDupTableMeta &dup_ls_meta)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(this->lease_item_array_.assign(dup_ls_meta.lease_item_array_))) {
    DUP_TABLE_LOG(WARN, "assign lease item array failed", K(ret));
  } else {
    this->ls_id_ = dup_ls_meta.ls_id_;
    this->lease_log_applied_scn_ = dup_ls_meta.lease_log_applied_scn_;
    this->readable_tablets_base_scn_ = dup_ls_meta.readable_tablets_base_scn_;
    this->readable_tablets_min_base_applied_scn_ =
        dup_ls_meta.readable_tablets_min_base_applied_scn_;
  }

  return ret;
}

int ObDupTableLSCheckpoint::get_dup_ls_meta(ObLSDupTableMeta &dup_ls_meta_replica) const
{
  int ret = OB_SUCCESS;

  SpinRLockGuard r_guard(ckpt_rw_lock_);

  if (OB_FAIL(dup_ls_meta_replica.copy(dup_ls_meta_))) {
    DUP_TABLE_LOG(WARN, "copy from dup_ls_meta_replica failed", K(ret));
  } else if (!dup_ls_meta_replica.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    DUP_TABLE_LOG(WARN, "invalid dup_ls_meta", K(ret), KPC(this), K(dup_ls_meta_replica));
  }

  return ret;
}

int ObDupTableLSCheckpoint::set_dup_ls_meta(const ObLSDupTableMeta &dup_ls_meta_replica)
{
  int ret = OB_SUCCESS;

  SpinWLockGuard w_guard(ckpt_rw_lock_);

  if (dup_ls_meta_.ls_id_ != dup_ls_meta_replica.ls_id_) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid arguments", K(ret), KPC(this), K(dup_ls_meta_replica));
  } else if (OB_FAIL(dup_ls_meta_.copy(dup_ls_meta_replica))) {
    DUP_TABLE_LOG(WARN, "copy from dup_ls_meta_replica failed", K(ret));
  }

  return ret;
}

share::SCN ObDupTableLSCheckpoint::get_lease_log_rec_scn() const
{
  share::SCN rec_scn;

  SpinRLockGuard r_guard(ckpt_rw_lock_);

  if (lease_log_rec_scn_.is_valid()) {
    rec_scn = lease_log_rec_scn_;
  } else {
    rec_scn.set_max();
  }

  return rec_scn;
}

int ObDupTableLSCheckpoint::reserve_ckpt_memory(const DupTableLeaseItemArray &lease_log_items)
{
  int ret = OB_SUCCESS;
  int64_t lease_log_array_size = lease_log_items.count();

  SpinWLockGuard w_guard(ckpt_rw_lock_);

  if (OB_FAIL(dup_ls_meta_.lease_item_array_.reserve(lease_log_array_size))) {
    DUP_TABLE_LOG(WARN, "reserve lease_item_array_ failed", K(ret), K(lease_log_array_size),
                  K(dup_ls_meta_));
  }

  return ret;
}

int ObDupTableLSCheckpoint::update_ckpt_after_lease_log_synced(
    const DupTableLeaseItemArray &lease_log_items,
    const share::SCN &scn,
    const bool modify_readable_sets,
    const bool contain_all_readable,
    const bool for_replay)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard w_guard(ckpt_rw_lock_);

  if (OB_SUCC(ret) && for_replay) {
    if (!start_replay_scn_.is_valid()) {
      start_replay_scn_ = scn;
      DUP_TABLE_LOG(INFO, "[CKPT] replay the first dup_table log", K(ret), KPC(this), K(scn),
                    K(for_replay), K(modify_readable_sets), K(contain_all_readable));
    }
  }

  if (OB_SUCC(ret) && lease_log_items.count() > 0) {
    if (OB_FAIL(dup_ls_meta_.lease_item_array_.assign(lease_log_items))) {
      DUP_TABLE_LOG(WARN, "copy from lease item array failed", K(ret), K(lease_log_items), K(scn),
                    K(contain_all_readable), K(modify_readable_sets), K(for_replay));
    } else {
      dup_ls_meta_.lease_log_applied_scn_ = scn;

      if (!lease_log_rec_scn_.is_valid()) {
        DUP_TABLE_LOG(INFO, "[CKPT] set rec log scn for lease", K(ret), KPC(this), K(scn),
                      K(for_replay), K(modify_readable_sets), K(contain_all_readable));
        lease_log_rec_scn_ = scn;
      }
    }
  }

  if (OB_SUCC(ret) && modify_readable_sets) {
    if (contain_all_readable) {
      dup_ls_meta_.readable_tablets_base_scn_ = scn;
      dup_ls_meta_.readable_tablets_min_base_applied_scn_.reset();
    } else if (!dup_ls_meta_.readable_tablets_min_base_applied_scn_.is_valid()) {
      dup_ls_meta_.readable_tablets_min_base_applied_scn_ = scn;
    }
    DUP_TABLE_LOG(INFO, "[CKPT] modify ckpt scn for readable tablets", K(ret), KPC(this), K(scn),
                  K(for_replay), K(modify_readable_sets), K(contain_all_readable));
  }

  return ret;
}

bool ObDupTableLSCheckpoint::contain_all_readable_on_replica() const
{
  bool contain_all_readable = false;

  SpinRLockGuard r_guard(ckpt_rw_lock_);

  if (!start_replay_scn_.is_valid()) {
    // replay no log
    if (!dup_ls_meta_.readable_tablets_base_scn_.is_valid()
        && !dup_ls_meta_.readable_tablets_min_base_applied_scn_.is_valid()) {
      contain_all_readable = true;
      DUP_TABLE_LOG(INFO, "[CKPT] No changes to readable sets and no replay",
                    K(contain_all_readable), KPC(this));
    }

  } else {
    if (!dup_ls_meta_.readable_tablets_base_scn_.is_valid()
        && !dup_ls_meta_.readable_tablets_min_base_applied_scn_.is_valid()) {
      // no changes to readable tablets
      contain_all_readable = true;
    } else if (dup_ls_meta_.readable_tablets_base_scn_.is_valid()
               && !dup_ls_meta_.readable_tablets_min_base_applied_scn_.is_valid()) {
      if (start_replay_scn_ < dup_ls_meta_.readable_tablets_base_scn_) {
        contain_all_readable = true;
      }
    } else if (!dup_ls_meta_.readable_tablets_base_scn_.is_valid()
               && dup_ls_meta_.readable_tablets_min_base_applied_scn_.is_valid()) {
      if (start_replay_scn_ < dup_ls_meta_.readable_tablets_min_base_applied_scn_) {
        contain_all_readable = true;
      }
    } else if (dup_ls_meta_.readable_tablets_base_scn_.is_valid()
               && dup_ls_meta_.readable_tablets_min_base_applied_scn_.is_valid()) {
      if (start_replay_scn_ < dup_ls_meta_.readable_tablets_base_scn_) {
        contain_all_readable = true;
      }
    }

    DUP_TABLE_LOG(INFO, "[CKPT] check readable sets completed after replay",
                  K(contain_all_readable), KPC(this));
  }

  return contain_all_readable;
}

int ObDupTableLSCheckpoint::flush()
{
  int ret = OB_SUCCESS;

  SpinWLockGuard w_guard(ckpt_rw_lock_);

  ObDupTableCkptLog slog_entry;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(slog_entry.init(dup_ls_meta_))) {
      DUP_TABLE_LOG(WARN, "init slog entry failed", K(ret), K(slog_entry), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    ObStorageLogParam log_param;
    log_param.data_ = &slog_entry;
    log_param.cmd_ = ObIRedoModule::gen_cmd(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
                                            ObRedoLogSubType::OB_REDO_LOG_UPDATE_DUP_TABLE_LS);
    ObStorageLogger *slogger = nullptr;
    if (OB_ISNULL(slogger = MTL(ObStorageLogger *))) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(WARN, "get slog service failed", K(ret));
    } else if (OB_FAIL(slogger->write_log(log_param))) {
      DUP_TABLE_LOG(WARN, "fail to write ls meta slog", K(ret), K(log_param), KPC(this));
    } else {
      DUP_TABLE_LOG(INFO, "Write dup_table slog successfully", K(ret), K(log_param), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    lease_log_rec_scn_.reset();
  }

  return ret;
}

/*******************************************************
 *  Dup_Table Log
 *******************************************************/

// OB_SERIALIZE_MEMBER(ObDupTableLogBlockHeader, position_, remain_length_);
OB_SERIALIZE_MEMBER(DupTableLogEntryHeader, entry_type_);
OB_SERIALIZE_MEMBER(DupTableStatLog, lease_addr_cnt_, readable_cnt_, all_tablet_set_cnt_);

// OB_SERIALIZE_MEMBER(ObLSDupTabletsMgr, max_submitted_tablet_change_ts_);

void ObDupTableLogOperator::reuse()
{
  big_segment_buf_.reset();
  logging_tablet_set_ids_.reset();
  logging_lease_addrs_.reset();
  logging_scn_.reset();
  logging_lsn_.reset();
  stat_log_.reset();
  // durable_block_scn_arr_.reset();
}
void ObDupTableLogOperator::reset()
{
  reuse();
  if (OB_NOT_NULL(block_buf_)) {
    share::mtl_free(block_buf_);
  }
  block_buf_ = nullptr;
  stat_log_.reset();

  last_block_submit_us_ = 0;
  last_block_sync_us_ = 0;
  last_entry_submit_us_ = 0;
  last_entry_sync_us_ = 0;

  total_cb_wait_time_ = 0;
  append_block_count_ = 0;
  log_entry_count_ = 0;
  total_log_entry_wait_time_ = 0;
}

int ObDupTableLogOperator::submit_log_entry()
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(log_lock_);

  LOG_OPERATOR_INIT_CHECK

  int64_t max_ser_size = 0;
  bool submit_result = false;
  DupLogTypeArray type_array;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prepare_serialize_log_entry_(max_ser_size, type_array))) {
      DUP_TABLE_LOG(WARN, "prepare serialize log entry failed", K(ret));
    } else if (!type_array.empty()) {
      if (OB_FAIL(serialize_log_entry_(max_ser_size, type_array))) {
        DUP_TABLE_LOG(WARN, "serialize log entry failed", K(ret));
      } else if (OB_FAIL(retry_submit_log_block_())) {
        DUP_TABLE_LOG(WARN, "retry submit log block failed", K(ret), K(ls_id_), K(max_ser_size),
                      K(type_array), K(logging_lease_addrs_), K(logging_tablet_set_ids_),
                      K(logging_scn_), K(logging_lsn_));
      } else {
        DUP_TABLE_LOG(INFO, "submit log entry successfully", K(ret), K(ls_id_), K(max_ser_size),
                      K(type_array), K(logging_lease_addrs_), K(logging_tablet_set_ids_),
                      K(logging_scn_), K(logging_lsn_));
      }
    } else {
      DUP_TABLE_LOG(INFO, "no need submit log entry", K(ret), K(ls_id_), K(max_ser_size),
                    K(type_array));
    }

    if (OB_SUCC(ret)) {
      // if not fail, submit_result is true, else is false
      submit_result = true;
    } else {
      submit_result = false;
    }
    after_submit_log(submit_result,
                     false  /* for replay */);
  }
  return ret;
}

int ObDupTableLogOperator::merge_replay_block(const char *replay_buf, int64_t replay_buf_len)
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(log_lock_);

  LOG_OPERATOR_INIT_CHECK

  if (OB_SUCC(ret)) {
    logservice::ObLogBaseHeader base_header;
    int64_t replay_buf_pos = 0;
    if (OB_FAIL(base_header.deserialize(replay_buf, replay_buf_len, replay_buf_pos))) {
      DUP_TABLE_LOG(WARN, "deserialize base log header failed", K(ret), K(replay_buf_len),
                    K(replay_buf_pos));
    } else if (OB_FAIL(
                   big_segment_buf_.collect_one_part(replay_buf, replay_buf_len, replay_buf_pos))) {
      if (OB_ITER_END == ret) {
        // need clear big_segment after collected all part for replay
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(WARN, "Log entry is completed, can not merge new block", K(ret),
                      K(big_segment_buf_));
      } else if (OB_START_LOG_CURSOR_INVALID == ret) {
        DUP_TABLE_LOG(INFO, "start replay from the middle of a big log entry", K(ret),
                      K(big_segment_buf_));
      } else if (big_segment_buf_.is_completed()) {
        ret = OB_ITER_END;
      }
    }
  }

  return ret;
}

int ObDupTableLogOperator::deserialize_log_entry()
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(log_lock_);

  LOG_OPERATOR_INIT_CHECK

  if (OB_SUCC(ret)) {
    if (!big_segment_buf_.is_completed()) {
      ret = OB_STATE_NOT_MATCH;
      DUP_TABLE_LOG(WARN, "need collect more parts of log entry", K(ret), K(big_segment_buf_));
    } else if (OB_FAIL(deserialize_log_entry_())) {
      DUP_TABLE_LOG(WARN, "deserialize log entry failed", K(ret), K(big_segment_buf_));
    }
  }

  return ret;
}

bool ObDupTableLogOperator::is_busy()
{
  SpinRLockGuard guard(log_lock_);
  return !logging_tablet_set_ids_.empty() || !logging_lease_addrs_.empty();
}

int ObDupTableLogOperator::on_success()
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(log_lock_);

  LOG_OPERATOR_INIT_CHECK

  if (OB_SUCC(ret)) {
    // It will clear scn or lsn
    if (OB_FAIL(sync_log_succ_(false))) {
      DUP_TABLE_LOG(ERROR, "invoke sync_log_succ failed", K(ret));
    }
    // if (OB_FAIL(retry_submit_log_block_())) {
    //   if (OB_ITER_END != ret) {
    //     DUP_TABLE_LOG(WARN, "retry submit log block failed", K(ret), K(big_segment_buf_));
    //   } else if (OB_FAIL(sync_log_succ_(false))) {
    //     DUP_TABLE_LOG(WARN, "invoke sync_log_succ failed", K(ret));
    //   }
    // }
  }

  return ret;
}

int ObDupTableLogOperator::on_failure()
{
  int ret = OB_SUCCESS;

  bool modify_readable = false;

  SpinWLockGuard guard(log_lock_);

  LOG_OPERATOR_INIT_CHECK

  if (OB_SUCC(ret)) {
    if (OB_FAIL(lease_mgr_ptr_->lease_log_synced(false /*sync_result*/, logging_scn_,
                                                 false /*for_replay*/, logging_lease_addrs_))) {
      DUP_TABLE_LOG(WARN, "lease mgr on_success failed", K(ret));
    } else if (OB_FAIL(tablet_mgr_ptr_->tablet_log_synced(
                   false /*sync_result*/, logging_scn_, false /*for_replay*/,
                   logging_tablet_set_ids_, modify_readable))) {
      DUP_TABLE_LOG(ERROR, "tablets mgr on_failure failed", K(ret), K(logging_scn_),
                    K(logging_lease_addrs_), K(logging_tablet_set_ids_), K(modify_readable));
    } else {
      reuse();
    }
  }

  DUP_TABLE_LOG(INFO, "on failure", K(ret), K(logging_scn_), K(logging_lease_addrs_),
                K(logging_tablet_set_ids_), K(modify_readable));

  return ret;
}

int ObDupTableLogOperator::replay_succ()
{
  int ret = OB_SUCCESS;

  SpinWLockGuard guard(log_lock_);

  LOG_OPERATOR_INIT_CHECK

  if (OB_SUCC(ret)) {
    after_submit_log(true, /* submit_result */
                     true  /* for_replay */);

    if (OB_FAIL(sync_log_succ_(true))) {
      DUP_TABLE_LOG(WARN, "invoke sync_log_succ failed", K(ret));
    }
  }
  return ret;
}

int ObDupTableLogOperator::sync_log_succ_(const bool for_replay)
{
  int ret = OB_SUCCESS;

  bool modify_readable = false;
  bool contain_all_readable = false;

  if (OB_SUCC(ret)) {
    if (stat_log_.readable_cnt_ == tablet_mgr_ptr_->get_readable_tablet_set_count()) {
      contain_all_readable = true;
    }
  }

  if (OB_SUCC(ret)) {

    if (OB_FAIL(lease_mgr_ptr_->lease_log_synced(
            true /*sync_result*/, logging_scn_, for_replay /*for_replay*/, logging_lease_addrs_))) {
      DUP_TABLE_LOG(WARN, "apply lease_log failed", K(ret), K(logging_scn_),
                    K(logging_lease_addrs_));
    } else if (OB_FAIL(tablet_mgr_ptr_->tablet_log_synced(
                   true /*sync_result*/, logging_scn_, for_replay /*for_replay*/,
                   logging_tablet_set_ids_, modify_readable))) {
      DUP_TABLE_LOG(WARN, "apply tablet_log failed", K(ret), K(logging_scn_),
                    K(logging_tablet_set_ids_));
    } else if (OB_FAIL(dup_ls_ckpt_->update_ckpt_after_lease_log_synced(
                   logging_lease_addrs_, logging_scn_, modify_readable /*modify_readable_sets*/,
                   contain_all_readable /*contain_all_readable*/, for_replay /*for_replay*/))) {
      DUP_TABLE_LOG(WARN, "update lease log ckpt failed", K(ret), KPC(dup_ls_ckpt_));
    } else {
      reuse();
    }
  }

  return ret;
}

void ObDupTableLogOperator::set_logging_scn(const share::SCN &scn)
{
  SpinWLockGuard guard(log_lock_);

  logging_scn_ = scn;
}

int ObDupTableLogOperator::prepare_serialize_log_entry_(int64_t &max_ser_size,
                                                        DupLogTypeArray &type_array)
{
  int ret = OB_SUCCESS;

  int64_t origin_max_ser_size = 0;
  max_ser_size = 0;

  DupTableStatLog max_stat_log;
  max_stat_log.lease_addr_cnt_ = INT64_MAX;
  max_stat_log.readable_cnt_ = INT64_MAX;
  max_stat_log.all_tablet_set_cnt_ = INT64_MAX;

  if (big_segment_buf_.is_active()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid block buf", K(ret), K(big_segment_buf_));
  } else {
    if (OB_SUCC(ret)) {
      origin_max_ser_size = max_ser_size;
      if (OB_FAIL(lease_mgr_ptr_->prepare_serialize(max_ser_size, logging_lease_addrs_))) {
        DUP_TABLE_LOG(WARN, "prepare serialize lease_mgr failed", K(ret));
      } else if (max_ser_size > origin_max_ser_size
                 && OB_FAIL(type_array.push_back(DupTableLogEntryType::LeaseListLog))) {
        DUP_TABLE_LOG(WARN, "push back log entry type failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      origin_max_ser_size = max_ser_size;
      int64_t max_log_buf_size = MAX_LOG_BLOCK_SIZE - max_stat_log.get_serialize_size();
      if (OB_FAIL(tablet_mgr_ptr_->prepare_serialize(max_ser_size, logging_tablet_set_ids_,
                                                     max_log_buf_size))) {
        DUP_TABLE_LOG(WARN, "prepare serialize tablets_mgr failed", K(ret));
      } else if (max_ser_size > origin_max_ser_size
                 && OB_FAIL(type_array.push_back(DupTableLogEntryType::TabletChangeLog))) {
        DUP_TABLE_LOG(WARN, "push back log entry_type failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && max_ser_size > 0) {
      if (OB_FALSE_IT(max_ser_size += max_stat_log.get_serialize_size())) {
        // do nothing
      } else if (max_ser_size > 0
                 && OB_FAIL(type_array.push_back(DupTableLogEntryType::DuptableStatLog))) {
        DUP_TABLE_LOG(WARN, "push back log entry_type failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // for compute size
      DupTableLogEntryHeader tmp_entry_header;
      tmp_entry_header.entry_type_ = DupTableLogEntryType::MAX;
      int64_t entry_log_size = 0;

      int64_t entry_header_size = type_array.count()
                                  * (tmp_entry_header.get_serialize_size()
                                     + serialization::encoded_length_i64(entry_log_size));
      max_ser_size += entry_header_size;
    }
  }
  return ret;
}

int ObDupTableLogOperator::serialize_log_entry_(const int64_t max_ser_size,
                                                const DupLogTypeArray &type_array)
{
  int ret = OB_SUCCESS;

  if (max_ser_size > MAX_LOG_BLOCK_SIZE) {
    ret = OB_LOG_TOO_LARGE;
    DUP_TABLE_LOG(WARN, "serialize buf is not enough for a big log", K(ls_id_), K(max_ser_size),
                  K(type_array));
  } else if (OB_FAIL(big_segment_buf_.init_for_serialize(max_ser_size))) {
    DUP_TABLE_LOG(WARN, "init big_segment_buf_ failed", K(ret), K(max_ser_size),
                  K(big_segment_buf_));
  }

  int64_t data_pos = big_segment_buf_.get_serialize_buf_pos();
  for (int i = 0; i < type_array.count() && OB_SUCC(ret); i++) {
    int64_t after_header_pos = 0;
    int64_t log_entry_size = 0;
    const DupTableLogEntryType &entry_type = type_array[i];
    if (entry_type != DupTableLogEntryType::LeaseListLog
        && entry_type != DupTableLogEntryType::TabletChangeLog
        && entry_type != DupTableLogEntryType::DuptableStatLog) {
      ret = OB_INVALID_ARGUMENT;
      DUP_TABLE_LOG(WARN, "invalid arguments", K(ret), K(entry_type));
    } else {
      DupTableLogEntryHeader entry_header;
      entry_header.entry_type_ = entry_type;
      if (OB_FAIL(entry_header.serialize(big_segment_buf_.get_serialize_buf(),
                                         big_segment_buf_.get_serialize_buf_len(), data_pos))) {
        DUP_TABLE_LOG(WARN, "serialize entry header", K(ret), K(entry_header));
      } else if (OB_FALSE_IT(after_header_pos = data_pos)) {
        // do nothing
      } else if (OB_FALSE_IT(data_pos = after_header_pos
                                        + serialization::encoded_length_i64(log_entry_size))) {
        // do nothing
      } else {
        switch (entry_type) {
        case DupTableLogEntryType::TabletChangeLog: {
          if (OB_FAIL(tablet_mgr_ptr_->serialize_tablet_log(
                  logging_tablet_set_ids_, big_segment_buf_.get_serialize_buf(),
                  big_segment_buf_.get_serialize_buf_len(), data_pos))) {
            DUP_TABLE_LOG(WARN, "serialize tablet log failed", K(ret), K(data_pos));
          }
          break;
        }
        case DupTableLogEntryType::LeaseListLog: {
          if (OB_FAIL(lease_mgr_ptr_->serialize_lease_log(
                  logging_lease_addrs_, big_segment_buf_.get_serialize_buf(),
                  big_segment_buf_.get_serialize_buf_len(), data_pos))) {
            DUP_TABLE_LOG(WARN, "serialize lease log failed", K(ret), K(data_pos));
          }
          break;
        }
        case DupTableLogEntryType::DuptableStatLog: {
          DupTableStatLog stat_log;
          stat_log.lease_addr_cnt_ = logging_lease_addrs_.count();
          stat_log.readable_cnt_ = tablet_mgr_ptr_->get_readable_tablet_set_count();
          stat_log.all_tablet_set_cnt_ = tablet_mgr_ptr_->get_all_tablet_set_count();
          if (OB_FAIL(stat_log.serialize(big_segment_buf_.get_serialize_buf(),
                                         big_segment_buf_.get_serialize_buf_len(), data_pos))) {
            DUP_TABLE_LOG(WARN, "serialize stat log failed", K(ret), K(data_pos));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          DUP_TABLE_LOG(WARN, "unexpected log entry type", K(ret), K(entry_header));
          break;
        }
        }
        if (OB_SUCC(ret)) {
          log_entry_size =
              data_pos - after_header_pos - serialization::encoded_length_i64(log_entry_size);
          if (OB_FAIL(serialization::encode_i64(big_segment_buf_.get_serialize_buf(),
                                                big_segment_buf_.get_serialize_buf_len(),
                                                after_header_pos, log_entry_size))) {
            DUP_TABLE_LOG(WARN, "encode log entry size failed", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(big_segment_buf_.set_serialize_pos(data_pos))) {
      DUP_TABLE_LOG(WARN, "set serialize pos failed", K(ret), K(data_pos), K(big_segment_buf_));
    }
  }

  return ret;
}

int ObDupTableLogOperator::deserialize_log_entry_()
{
  int ret = OB_SUCCESS;

  int64_t data_pos = 0;

  data_pos = big_segment_buf_.get_deserialize_buf_pos();
  const int64_t segment_buf_len = big_segment_buf_.get_deserialize_buf_len();
  int64_t log_entry_size = 0;
  int64_t after_header_pos = 0;

  while (OB_SUCC(ret) && data_pos < segment_buf_len) {
    DupTableLogEntryHeader entry_header;
    log_entry_size = 0;
    after_header_pos = 0;
    if (OB_FAIL(entry_header.deserialize(big_segment_buf_.get_deserialize_buf(), segment_buf_len,
                                         data_pos))) {
      DUP_TABLE_LOG(WARN, "serialize entry header", K(ret), K(data_pos), K(big_segment_buf_),
                    K(entry_header));
    } else if (OB_FAIL(serialization::decode_i64(big_segment_buf_.get_deserialize_buf(),
                                                 segment_buf_len, data_pos, &log_entry_size))) {
      DUP_TABLE_LOG(WARN, "decode log entry size failed");
    } else if (OB_FALSE_IT(after_header_pos = data_pos)) {
      // do nothing
    } else {
      switch (entry_header.entry_type_) {
      case DupTableLogEntryType::TabletChangeLog: {
        if (OB_ISNULL(tablet_mgr_ptr_)) {
          ret = OB_ERR_UNEXPECTED;
          DUP_TABLE_LOG(WARN, "invalid tablet mgr", K(ret));
        } else if (OB_FAIL(tablet_mgr_ptr_->deserialize_tablet_log(
                       logging_tablet_set_ids_, big_segment_buf_.get_deserialize_buf(),
                       data_pos + log_entry_size, data_pos))) {
          DUP_TABLE_LOG(WARN, "deserialize tablet log failed", K(ret), K(data_pos));
        }
        break;
      }
      case DupTableLogEntryType::LeaseListLog: {
        if (OB_FAIL(lease_mgr_ptr_->deserialize_lease_log(logging_lease_addrs_,
                                                          big_segment_buf_.get_deserialize_buf(),
                                                          data_pos + log_entry_size, data_pos))) {
          DUP_TABLE_LOG(WARN, "deserialize lease log failed", K(ret), K(data_pos));
        }
        break;
      }
      case DupTableLogEntryType::DuptableStatLog: {
        if (OB_FAIL(stat_log_.deserialize(big_segment_buf_.get_deserialize_buf(),
                                          data_pos + log_entry_size, data_pos))) {
          DUP_TABLE_LOG(WARN, "deserialize stat log failed", K(ret), K(data_pos));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        DUP_TABLE_LOG(WARN, "unexpected log entry type", K(ret), K(entry_header), K(data_pos));
        break;
      }
      }

      if (OB_SUCC(ret) && data_pos < after_header_pos + log_entry_size) {
        DUP_TABLE_LOG(INFO, "try to deserialize a new version log", K(ret), K(data_pos),
                      K(after_header_pos), K(log_entry_size), K(entry_header));
        data_pos = after_header_pos + log_entry_size;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(big_segment_buf_.set_deserialize_pos(after_header_pos + log_entry_size))) {
      DUP_TABLE_LOG(WARN, "set deserialize pos failed", K(ret), K(after_header_pos),
                    K(log_entry_size), K(big_segment_buf_));
    }
    DUP_TABLE_LOG(DEBUG, "deser log succ", K(ret), K(data_pos), K(after_header_pos),
                  K(log_entry_size));
  } else {
    DUP_TABLE_LOG(WARN, "deser log failed", K(ret), K(data_pos));
  }

  return ret;
}

int ObDupTableLogOperator::retry_submit_log_block_()
{
  int ret = OB_SUCCESS;

  int64_t block_buf_pos = 0;
  if (OB_ISNULL(block_buf_)) {
    if (OB_ISNULL(block_buf_ = static_cast<char *>(
                      share::mtl_malloc(MAX_LOG_BLOCK_SIZE, "DUP_LOG_BLOCK")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DUP_TABLE_LOG(WARN, "alloc block memory failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    logservice::ObLogBaseHeader base_header(logservice::ObLogBaseType::DUP_TABLE_LOG_BASE_TYPE,
                                            logservice::ObReplayBarrierType::NO_NEED_BARRIER,
                                            ls_id_.hash());
    bool unused = false;
    if (!big_segment_buf_.is_active()) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(WARN, "big_segment_buf_ is not active", K(ret), K(big_segment_buf_));
    } else if (big_segment_buf_.is_completed()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(base_header.serialize(block_buf_, MAX_LOG_BLOCK_SIZE, block_buf_pos))) {
      DUP_TABLE_LOG(WARN, "serialize base header failed", K(ret), K(base_header));
    } else if (OB_FAIL(big_segment_buf_.split_one_part(block_buf_, MAX_LOG_BLOCK_SIZE,
                                                       block_buf_pos, unused))) {
      DUP_TABLE_LOG(WARN, "split one part of segment failed", K(ret), K(big_segment_buf_),
                    K(block_buf_pos));
    } else if (OB_FAIL(log_handler_->append(block_buf_, block_buf_pos, share::SCN::min_scn(), false,
                                            this, logging_lsn_, logging_scn_))) {
      DUP_TABLE_LOG(WARN, "append block failed", K(ret), K(ls_id_));
    }
  }

  return ret;
}

void ObDupTableLogOperator::after_submit_log(const bool submit_result, const bool for_replay)
{
  int ret = OB_SUCCESS;
  if (!logging_tablet_set_ids_.empty()) {
    if (OB_FAIL(tablet_mgr_ptr_->tablet_log_submitted(submit_result, logging_scn_, for_replay,
                                                      logging_tablet_set_ids_))) {
      DUP_TABLE_LOG(ERROR, "tablet log submitted failed", K(ret), K(ls_id_), K(logging_scn_),
                    K(logging_tablet_set_ids_));
    }
  }

  if (OB_SUCC(ret) && !logging_lease_addrs_.empty()) {
    if (OB_FAIL(lease_mgr_ptr_->lease_log_submitted(submit_result, logging_scn_, for_replay,
                                                    logging_lease_addrs_))) {
      DUP_TABLE_LOG(ERROR, "lease log submitted failed", K(ret), K(ls_id_), K(logging_scn_),
                    K(logging_lease_addrs_));
    }
  }

  if (OB_SUCC(ret)) {
    if (!submit_result) {
      reuse();
    }
  }
}

/*******************************************************
 *  Dup_Table Msg
 *******************************************************/

OB_SERIALIZE_MEMBER(ObDupTableMsgBase, src_, dst_, proxy_, ls_id_);
OB_SERIALIZE_MEMBER_INHERIT(ObDupTableTsSyncRequest, ObDupTableMsgBase, max_commit_scn_);
OB_SERIALIZE_MEMBER_INHERIT(ObDupTableTsSyncResponse,
                            ObDupTableMsgBase,
                            max_replayed_scn_,
                            max_commit_scn_,
                            max_read_scn_);
OB_SERIALIZE_MEMBER_INHERIT(ObDupTableLeaseRequest,
                            ObDupTableTsSyncResponse,
                            request_ts_,
                            lease_interval_us_);
OB_SERIALIZE_MEMBER_INHERIT(ObDupTableBeforePrepareRequest,
                            ObDupTableMsgBase,
                            tx_id_,
                            before_prepare_version_);

void ObDupTableMsgBase::reset()
{
  src_.reset();
  dst_.reset();
  proxy_.reset();
  ls_id_.reset();
}

void ObDupTableMsgBase::set_header(const ObAddr &src,
                                   const ObAddr &dst,
                                   const ObAddr &proxy,
                                   const share::ObLSID &ls_id)
{
  src_ = src;
  dst_ = dst;
  proxy_ = proxy;
  ls_id_ = ls_id;
}

/*******************************************************
 *  Dup_Table RPC
 *******************************************************/
int ObDupTableRpc::init(rpc::frame::ObReqTransport *req_transport,
                        const oceanbase::common::ObAddr &addr)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(proxy_.init(req_transport, addr))) {
    DUP_TABLE_LOG(WARN, "init dup_table rpc proxy failed", K(ret));
  }

  return ret;
}
} // namespace transaction

namespace obrpc
{

int ObDupTableLeaseRequestP::process()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;

  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid msg", K(ret), K(arg_));
  } else if (OB_FAIL(
                 MTL(ObLSService *)->get_ls(arg_.get_ls_id(), ls_handle, ObLSGetMod::TRANS_MOD))) {
    DUP_TABLE_LOG(WARN, "get ls failed", K(ret), K(arg_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_NULL_VALUE;
    DUP_TABLE_LOG(WARN, "ls pointer is nullptr", K(ret));
  } else if (ls_handle.get_ls()->get_dup_table_ls_handler()->recive_lease_request(arg_)) {
    DUP_TABLE_LOG(WARN, "recive_lease_request error", K(ret));
  }

  DUP_TABLE_LOG(DEBUG, "recive lease request", K(ret), K(arg_));
  return ret;
}

int ObDupTableTsSyncRequestP::process()
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;

  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid msg", K(ret), K(arg_));
  } else if (OB_FAIL(
                 MTL(ObLSService *)->get_ls(arg_.get_ls_id(), ls_handle, ObLSGetMod::TRANS_MOD))) {
    DUP_TABLE_LOG(WARN, "get ls failed", K(ret), K(arg_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_NULL_VALUE;
    DUP_TABLE_LOG(WARN, "ls pointer is nullptr", K(ret));
  } else if (ls_handle.get_ls()->get_dup_table_ls_handler()->handle_ts_sync_request(arg_)) {
    DUP_TABLE_LOG(WARN, "handle ts sync request error", K(ret));
  }

  DUP_TABLE_LOG(DEBUG, "recive ts sync request", K(ret), K(arg_));

  return ret;
}

int ObDupTableTsSyncResponseP::process()
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;

  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid msg", K(ret), K(arg_));
  } else if (OB_FAIL(
                 MTL(ObLSService *)->get_ls(arg_.get_ls_id(), ls_handle, ObLSGetMod::TRANS_MOD))) {
    DUP_TABLE_LOG(WARN, "get ls failed", K(ret), K(arg_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_NULL_VALUE;
    DUP_TABLE_LOG(WARN, "ls pointer is nullptr", K(ret));
  } else if (ls_handle.get_ls()->get_dup_table_ls_handler()->handle_ts_sync_response(arg_)) {
    DUP_TABLE_LOG(WARN, "handle ts sync request error", K(ret));
  }

  DUP_TABLE_LOG(DEBUG, "recive ts sync response", K(ret), K(arg_));

  return ret;
}

int ObDupTableBeforePrepareRequestP::process()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  transaction::ObPartTransCtx *part_ctx = nullptr;

  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid msg", K(ret), K(arg_));
  } else if (OB_FAIL(
                 MTL(ObLSService *)->get_ls(arg_.get_ls_id(), ls_handle, ObLSGetMod::TRANS_MOD))) {
    DUP_TABLE_LOG(WARN, "get ls failed", K(ret), K(arg_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_NULL_VALUE;
    DUP_TABLE_LOG(WARN, "ls pointer is nullptr", K(ret), K(arg_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tx_ctx(arg_.get_tx_id(), true, part_ctx))) {
    DUP_TABLE_LOG(WARN, "get part ctx failed", K(ret), K(arg_));
  } else {
    if (OB_ISNULL(part_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(WARN, "unexpected part ctx", K(ret), KPC(part_ctx), K(arg_));
    } else if (OB_FAIL(part_ctx->retry_dup_trx_before_prepare(arg_.get_before_prepare_version()))) {
      DUP_TABLE_LOG(WARN, "retry dup trx before_prepare failed", K(ret), KPC(part_ctx), K(arg_));
    }

    ls_handle.get_ls()->revert_tx_ctx(part_ctx);
  }

  DUP_TABLE_LOG(DEBUG, "recive before prepare request", K(ret), K(arg_));
  return ret;
}

} // namespace obrpc
} // namespace oceanbase
