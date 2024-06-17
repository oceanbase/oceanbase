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
#include "storage/slog/ob_storage_logger.h"
#include "storage/slog/ob_storage_log_replayer.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{

using namespace storage;

namespace transaction
{

const uint64_t DupTableDiagStd::DUP_DIAG_INFO_LOG_BUF_LEN[DupTableDiagStd::TypeIndex::MAX_INDEX] = {
    1 << 12, // 4K
    1 << 20, // 1M
    1 << 17, // 128K
    1 << 12, // 4k
};
const char *DupTableDiagStd::DUP_DIAG_INDENT_SPACE = "    "; // 4
const char *DupTableDiagStd::DUP_DIAG_COMMON_PREFIX = "DUP_TABLE_DIAG: ";
const int64_t DupTableDiagStd::DUP_DIAG_PRINT_INTERVAL[DupTableDiagStd::TypeIndex::MAX_INDEX] = {
    ObDupTableLSLeaseMgr::DEFAULT_LEASE_INTERVAL,
    30 * 1000 * 1000,    // 10s , tablet_print_interval
    30 * 1000 * 1000,    // 10s , tablet_print_interval
    5 * 60 * 1000 * 1000 // 3min , ts_sync_print_interval
};


const int64_t ObDupTableLogOperator::MAX_LOG_BLOCK_SIZE=common::OB_MAX_LOG_ALLOWED_SIZE;
const int64_t ObDupTableLogOperator::RESERVED_LOG_HEADER_SIZE = 100;


/*******************************************************
 *  Dup_Table LS Role State
 *******************************************************/
int64_t *ObDupTableLSRoleStateContainer::get_target_state_ref(ObDupTableLSRoleState target_state)
{
  int64_t *state_member = nullptr;
  switch (target_state) {
  case ObDupTableLSRoleState::LS_REVOKE_SUCC:
  case ObDupTableLSRoleState::LS_TAKEOVER_SUCC: {
    state_member = &role_state_;
    break;
  }
  case ObDupTableLSRoleState::LS_OFFLINE_SUCC:
  case ObDupTableLSRoleState::LS_ONLINE_SUCC: {
    state_member = &offline_state_;
    break;
  }
  case ObDupTableLSRoleState::LS_START_SUCC:
  case ObDupTableLSRoleState::LS_STOP_SUCC: {
    state_member = &stop_state_;
    break;
  }
  default: {
    state_member = nullptr;
    DUP_TABLE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "undefined target role state", K(target_state),
                      KPC(this));
    break;
  }
  }
  return state_member;
}

bool ObDupTableLSRoleStateContainer::check_target_state(ObDupTableLSRoleState target_state)
{
  bool is_same = false;
  int64_t *state_member = get_target_state_ref(target_state);
  if (OB_NOT_NULL(state_member)) {
    ObDupTableLSRoleState cur_state = static_cast<ObDupTableLSRoleState>(ATOMIC_LOAD(state_member));
    if (target_state == cur_state) {
      is_same = true;
    } else if (ObDupTableLSRoleState::ROLE_STATE_CHANGING == cur_state) {
      is_same = false;
      DUP_TABLE_LOG(INFO, "The Role State is Changing", K(is_same), K(cur_state), KPC(this));
    } else {
      is_same = false;
    }
  }
  return is_same;
}

bool ObDupTableLSRoleStateHelper::is_leader()
{
  return  cur_state_.check_target_state(ObDupTableLSRoleState::LS_TAKEOVER_SUCC);
}

bool ObDupTableLSRoleStateHelper::is_follower()
{
  return  cur_state_.check_target_state(ObDupTableLSRoleState::LS_REVOKE_SUCC);
}

bool ObDupTableLSRoleStateHelper::is_offline()
{
  return cur_state_.check_target_state(ObDupTableLSRoleState::LS_OFFLINE_SUCC);
}

bool ObDupTableLSRoleStateHelper::is_online()
{
  return cur_state_.check_target_state(ObDupTableLSRoleState::LS_ONLINE_SUCC);
}

bool ObDupTableLSRoleStateHelper::is_stopped()
{
  return cur_state_.check_target_state(ObDupTableLSRoleState::LS_STOP_SUCC);
}

bool ObDupTableLSRoleStateHelper::is_started()
{
  return cur_state_.check_target_state(ObDupTableLSRoleState::LS_START_SUCC);
}

#define DUP_TABLE_LS_STATE_GET_AND_CHECK(target_state, restore_state)                             \
  int64_t *state_member = cur_state_.get_target_state_ref(target_state);                          \
  int64_t *backup_state_member = restore_state.get_target_state_ref(target_state);                \
  ObDupTableLSRoleState cur_ls_state = ObDupTableLSRoleState::UNKNOWN;                            \
  if (OB_ISNULL(state_member) || OB_ISNULL(backup_state_member)) {                                \
    ret = OB_INVALID_ARGUMENT;                                                                    \
    DUP_TABLE_LOG(WARN, "invalid arguments", K(ret), KP(state_member), KP(backup_state_member), \
                  K(target_state), KPC(this));                                             \
  } else if (OB_FALSE_IT(cur_ls_state =                                                           \
                             static_cast<ObDupTableLSRoleState>(ATOMIC_LOAD(state_member)))) {    \
  }

int ObDupTableLSRoleStateHelper::prepare_state_change(const ObDupTableLSRoleState &target_state,
                                                      ObDupTableLSRoleStateContainer &restore_state)
{
  int ret = OB_SUCCESS;

  DUP_TABLE_LS_STATE_GET_AND_CHECK(target_state, restore_state)

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (target_state == cur_ls_state) {
    ret = OB_NO_NEED_UPDATE;
    DUP_TABLE_LOG(INFO, "the cur state has already been same as target_state", K(ret),
                  K(target_state), KPC(this));
  } else if (cur_ls_state == ObDupTableLSRoleState::UNKNOWN) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(INFO, "invalid role state", K(cur_ls_state), KPC(this));
  } else {
    if (ObDupTableLSRoleState::ROLE_STATE_CHANGING == cur_ls_state) {
      DUP_TABLE_LOG(INFO,
                    "cur_ls_state is the ROLE_STATE_CHANGING, may be error in the last role change",
                    K(ret), K(cur_ls_state), K(target_state), KPC(this));
    }
    *backup_state_member = static_cast<int64_t>(cur_ls_state);
    ATOMIC_STORE(state_member, static_cast<int64_t>(ObDupTableLSRoleState::ROLE_STATE_CHANGING));
  }

  return ret;
}

int ObDupTableLSRoleStateHelper::restore_state(const ObDupTableLSRoleState &target_state,
                                               ObDupTableLSRoleStateContainer &restore_state)
{
  int ret = OB_SUCCESS;
  DUP_TABLE_LS_STATE_GET_AND_CHECK(target_state, restore_state)

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (ObDupTableLSRoleState::ROLE_STATE_CHANGING != cur_ls_state) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid cur ls state", K(ret), K(cur_ls_state), KPC(this));
  } else {
    ATOMIC_STORE(state_member, *backup_state_member);
  }

  return ret;
}

int ObDupTableLSRoleStateHelper::state_change_succ(const ObDupTableLSRoleState &target_state,
                                                   ObDupTableLSRoleStateContainer &restore_state)
{
  int ret = OB_SUCCESS;
  DUP_TABLE_LS_STATE_GET_AND_CHECK(target_state, restore_state)

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (ObDupTableLSRoleState::ROLE_STATE_CHANGING != cur_ls_state
             && target_state != cur_ls_state) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid cur ls state", K(ret), K(cur_ls_state), KPC(this));
  } else {
    ATOMIC_STORE(state_member, static_cast<int64_t>(target_state));
  }

  return ret;
}

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
bool DupTabletSetCommonHeader::operator==(const DupTabletSetCommonHeader &dup_common_header) const
{
  bool compare_res = false;

  if (unique_id_ == dup_common_header.unique_id_
      && tablet_set_type_ == dup_common_header.tablet_set_type_
      && sp_op_type_ == dup_common_header.sp_op_type_) {
    compare_res = true;
  }

  return compare_res;
}

bool DupTabletSetCommonHeader::operator!=(const DupTabletSetCommonHeader &dup_common_header) const
{
  return !((*this) == dup_common_header);
}

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

bool ObDupTableLSCheckpoint::is_useful_meta() const
{
  SpinRLockGuard r_guard(ckpt_rw_lock_);
  bool useful =
      dup_ls_meta_.is_valid()
      && (!dup_ls_meta_.lease_item_array_.empty() || dup_ls_meta_.lease_log_applied_scn_.is_valid()
          || dup_ls_meta_.readable_tablets_base_scn_.is_valid()
          || dup_ls_meta_.readable_tablets_min_base_applied_scn_.is_valid());
  return useful;
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

  if (OB_SUCC(ret)) {
    if (dup_ls_meta_.readable_tablets_base_scn_.is_valid()
        && dup_ls_meta_.readable_tablets_base_scn_ == scn) {
      if (!contain_all_readable) {
        readable_ckpt_base_scn_is_accurate_ = false;
        DUP_TABLE_LOG(INFO, "[CKPT] check ckpt base scn not accurate", K(ret), KPC(this), K(scn),
                      K(for_replay), K(modify_readable_sets), K(contain_all_readable));
      } else {
        readable_ckpt_base_scn_is_accurate_ = true;
      }
    }
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

  } else if (!readable_ckpt_base_scn_is_accurate_) {
    contain_all_readable = false;
    DUP_TABLE_LOG(INFO, "[CKPT] readable base scn is not accurate, not contain all readable set",
                  K(contain_all_readable), KPC(this));
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

int ObDupTableLSCheckpoint::offline()
{
  int ret = OB_SUCCESS;

  // SpinWLockGuard w_guard(ckpt_rw_lock_);

  // DUP_TABLE_LOG(INFO, , args...)

  return ret;
}

int ObDupTableLSCheckpoint::online()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard w_guard(ckpt_rw_lock_);

  lease_log_rec_scn_.reset();
  start_replay_scn_.reset();
  readable_ckpt_base_scn_is_accurate_ = true;

  DUP_TABLE_LOG(INFO, "Dup Table LS Checkpoint Online", K(ret), KPC(this));
  return ret;
}

/*******************************************************
 *  Dup_Table Log
 *******************************************************/

// OB_SERIALIZE_MEMBER(ObDupTableLogBlockHeader, position_, remain_length_);
OB_SERIALIZE_MEMBER(DupTableLogEntryHeader, entry_type_);
OB_SERIALIZE_MEMBER(DupTableStatLog, lease_addr_cnt_, leader_readable_cnt_, all_tablet_set_cnt_);

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
  int tmp_ret = OB_SUCCESS;

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
        DUP_TABLE_LOG(DEBUG, "submit log entry successfully", K(ret), K(ls_id_), K(max_ser_size),
                      K(type_array), K(logging_lease_addrs_), K(logging_tablet_set_ids_),
                      K(logging_scn_), K(logging_lsn_));
      }
    } else {
      DUP_TABLE_LOG(DEBUG, "no need submit log entry", K(ret), K(ls_id_), K(max_ser_size),
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
  return check_is_busy_without_lock();
}

bool ObDupTableLogOperator::check_is_busy_without_lock()
{
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
    int64_t start_sync_time =ObTimeUtility::fast_current_time();
    if (OB_FAIL(lease_mgr_ptr_->lease_log_synced(false /*sync_result*/, logging_scn_,
                                                 false /*for_replay*/, logging_lease_addrs_))) {
      DUP_TABLE_LOG(WARN, "lease mgr on_success failed", K(ret));
    } else if (OB_FAIL(tablet_mgr_ptr_->tablet_log_synced(
                   false /*sync_result*/, logging_scn_, false /*for_replay*/,
                   logging_tablet_set_ids_, modify_readable, start_sync_time))) {
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
  int64_t logging_readable_cnt = 0;
  const int64_t all_readable_set_cnt = tablet_mgr_ptr_->get_readable_tablet_set_count();
  const share::SCN cur_sync_succ_scn = logging_scn_;
  ObDupTableLSCheckpoint::ObLSDupTableMeta tmp_dup_ls_meta;

  if (OB_SUCC(ret)) {
    if (stat_log_.logging_readable_cnt_ >= 0) {
      logging_readable_cnt = stat_log_.logging_readable_cnt_;
    } else {
      // for each logging id to check contain all readable
      for (int64_t i = 0; i < logging_tablet_set_ids_.count(); i++) {
        if (logging_tablet_set_ids_.at(i).is_readable_set()) {
          logging_readable_cnt++;
        }
      }
    }
    if (stat_log_.leader_readable_cnt_ > 0 && logging_readable_cnt > 0
        && stat_log_.leader_readable_cnt_ == logging_readable_cnt) {
      contain_all_readable = true;
    }
    if (!contain_all_readable) {
      DUP_TABLE_LOG(DEBUG, "this tablet log not contain all readable set", K(ret), K(logging_scn_),
                    K(stat_log_), K(logging_readable_cnt), K(all_readable_set_cnt));
    }
  }

  int64_t start_sync_time = ObTimeUtility::fast_current_time();
  int64_t lease_log_sync_cost_time, tablet_log_sync_cost_time, ckpt_update_cost_time;
  lease_log_sync_cost_time = tablet_log_sync_cost_time = ckpt_update_cost_time = start_sync_time;

  if (OB_SUCC(ret)) {

    if (OB_FAIL(lease_mgr_ptr_->lease_log_synced(
            true /*sync_result*/, logging_scn_, for_replay /*for_replay*/, logging_lease_addrs_))) {
      DUP_TABLE_LOG(WARN, "apply lease_log failed", K(ret), K(logging_scn_),
                    K(logging_lease_addrs_));
    } else if (OB_FALSE_IT(lease_log_sync_cost_time =
                               ObTimeUtility::fast_current_time() - start_sync_time)) {

    } else if (OB_FAIL(tablet_mgr_ptr_->tablet_log_synced(
                   true /*sync_result*/, logging_scn_, for_replay /*for_replay*/,
                   logging_tablet_set_ids_, modify_readable, start_sync_time))) {
      DUP_TABLE_LOG(WARN, "apply tablet_log failed", K(ret), K(logging_scn_),
                    K(logging_tablet_set_ids_));
    } else if (OB_FALSE_IT(tablet_log_sync_cost_time = ObTimeUtility::fast_current_time()
                                                       - start_sync_time
                                                       - lease_log_sync_cost_time)) {

    } else if (OB_FAIL(dup_ls_ckpt_->update_ckpt_after_lease_log_synced(
                   logging_lease_addrs_, logging_scn_, modify_readable /*modify_readable_sets*/,
                   contain_all_readable /*contain_all_readable*/, for_replay /*for_replay*/))) {
      DUP_TABLE_LOG(WARN, "update lease log ckpt failed", K(ret), KPC(dup_ls_ckpt_));
    } else if (OB_FALSE_IT(ckpt_update_cost_time = ObTimeUtility::fast_current_time()
                                                   - start_sync_time - lease_log_sync_cost_time
                                                   - tablet_log_sync_cost_time)) {
    } else {
      reuse();
    }
  }

  if (lease_log_sync_cost_time + tablet_log_sync_cost_time + ckpt_update_cost_time > 500 * 1000) {
    DUP_TABLE_LOG(INFO, "sync log succ cost too much time", K(ret), K(cur_sync_succ_scn),
                  K(logging_lease_addrs_.count()), K(logging_tablet_set_ids_.count()), K(stat_log_),
                  K(start_sync_time), K(lease_log_sync_cost_time), K(tablet_log_sync_cost_time),
                  K(ckpt_update_cost_time));
  }

  if (OB_NOT_NULL(interface_stat_ptr_)) {
    interface_stat_ptr_->dup_table_max_applying_scn_.inc_update(cur_sync_succ_scn);
    interface_stat_ptr_->dup_table_lease_log_sync_total_time_ += lease_log_sync_cost_time;
    interface_stat_ptr_->dup_table_tablet_log_sync_total_time_ += tablet_log_sync_cost_time;
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
  max_stat_log.leader_readable_cnt_ = INT64_MAX;
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
      int64_t max_log_buf_size = MAX_LOG_BLOCK_SIZE - RESERVED_LOG_HEADER_SIZE - max_stat_log.get_serialize_size();
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
                 && OB_FAIL(type_array.push_back(DupTableLogEntryType::DupTableStatLog))) {
        DUP_TABLE_LOG(WARN, "push back log entry_type failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // for compute size
      DupTableLogEntryHeader tmp_entry_header;
      tmp_entry_header.entry_type_ = DupTableLogEntryType::MAX;
      int64_t entry_log_size = 0;

      //depend on the size of type_array
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

  if (max_ser_size > MAX_LOG_BLOCK_SIZE - RESERVED_LOG_HEADER_SIZE) {
    ret = OB_LOG_TOO_LARGE;
    DUP_TABLE_LOG(WARN, "serialize buf is not enough for a big log", K(ls_id_), K(max_ser_size),
                  K(type_array), K(MAX_LOG_BLOCK_SIZE),K(RESERVED_LOG_HEADER_SIZE));
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
        && entry_type != DupTableLogEntryType::DupTableStatLog) {
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
        case DupTableLogEntryType::DupTableStatLog: {
          DupTableStatLog stat_log;
          stat_log.lease_addr_cnt_ = logging_lease_addrs_.count();
          stat_log.leader_readable_cnt_ = tablet_mgr_ptr_->get_readable_tablet_set_count();
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

  common::ObTimeGuard timeguard("deserialize_log_entry", 500 * 1000);

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
      case DupTableLogEntryType::DupTableStatLog: {
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

      timeguard.click(get_entry_type_str(entry_header.entry_type_));
      if (OB_SUCC(ret) && data_pos < after_header_pos + log_entry_size) {
        DUP_TABLE_LOG(INFO, "try to deserialize a new version dup_table log in older observer",
                      K(ret), K(data_pos), K(segment_buf_len), K(after_header_pos),
                      K(log_entry_size), K(entry_header));
        data_pos = after_header_pos + log_entry_size;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(big_segment_buf_.set_deserialize_pos(after_header_pos + log_entry_size))) {
      DUP_TABLE_LOG(WARN, "set deserialize pos failed", K(ret), K(after_header_pos),
                    K(log_entry_size), K(big_segment_buf_));
    }
  }

  int64_t time_diff = timeguard.get_diff();
  if (time_diff > 500 * 1000) {
    DUP_TABLE_LOG(INFO, "deserialize dup table log entry cost too much time", K(ret),
                  K(logging_scn_), K(logging_lease_addrs_.count()),
                  K(logging_tablet_set_ids_.count()), K(stat_log_), K(timeguard));
  }
  if (OB_NOT_NULL(interface_stat_ptr_)) {
    interface_stat_ptr_->dup_table_log_deser_total_time_ += time_diff;
  }

  return ret;
}

int ObDupTableLogOperator::retry_submit_log_block_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

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

    share::SCN gts_base_scn = share::SCN::min_scn();
    if (OB_TMP_FAIL(OB_TS_MGR.get_gts(MTL_ID(), nullptr, gts_base_scn))) {
      DUP_TABLE_LOG(WARN, "get gts cache for base scn failed", K(ret), K(tmp_ret), K(gts_base_scn));
      gts_base_scn = share::SCN::min_scn();
    }

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
    } else if (OB_FAIL(!big_segment_buf_.is_completed())) {
      ret = OB_LOG_TOO_LARGE;
      DUP_TABLE_LOG(WARN, "Too large dup table log. We can not submit it", K(ret),
                    K(big_segment_buf_.is_completed()), KPC(this));
    } else if (OB_FAIL(log_handler_->append(block_buf_, block_buf_pos, gts_base_scn, false,
                                            false/*allow_compression*/, this, logging_lsn_, logging_scn_))) {
      DUP_TABLE_LOG(WARN, "append block failed", K(ret), K(ls_id_));
    } else {
      if (OB_NOT_NULL(interface_stat_ptr_)) {
        interface_stat_ptr_->dup_table_log_entry_cnt_ += 1;
        interface_stat_ptr_->dup_table_log_entry_total_size_ += block_buf_pos;
      }
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
                            before_prepare_version_,
                            before_prepare_scn_src_);

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

int ObTxRedoSyncRetryTask::ObTxRedoSyncIterFunc::operator()(
    common::hash::HashSetTypes<RedoSyncKey>::pair_type &redo_sync_hash_pair)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (ls_handle_.is_valid()) {
    if (redo_sync_hash_pair.first.ls_id_ != ls_handle_.get_ls()->get_ls_id()) {
      ls_handle_.reset();
    }
  }

  // tmp_ret = OB_SUCCESS;
  ObPartTransCtx *tx_ctx = nullptr;

  if (!ls_handle_.is_valid() && OB_SUCC(ret) && OB_SUCCESS == tmp_ret) {
    if (OB_TMP_FAIL(
            MTL(ObLSService *)
                ->get_ls(redo_sync_hash_pair.first.ls_id_, ls_handle_, ObLSGetMod::TRANS_MOD))) {
      TRANS_LOG(WARN, "get ls failed", K(ret), K(redo_sync_hash_pair.first));
    }
  }

  if (OB_SUCC(ret) && OB_SUCCESS == tmp_ret) {
    if (OB_TMP_FAIL(ls_handle_.get_ls()->get_tx_ctx_with_timeout(
            redo_sync_hash_pair.first.tx_id_, false /*for_replay*/, tx_ctx, 100 * 1000))) {
      TRANS_LOG(WARN, "get tx ctx failed", K(ret), K(tmp_ret), K(redo_sync_hash_pair.first),
                K(ls_handle_), KP(tx_ctx));
      if (tmp_ret == OB_TIMEOUT) {
        tmp_ret = OB_SUCCESS;
      }
    } else if (OB_TMP_FAIL(tx_ctx->dup_table_tx_redo_sync(false /*need_retry_by_task*/))) {
      TRANS_LOG(WARN, "dup table redo sync failed", K(ret), K(tmp_ret),
                K(redo_sync_hash_pair.first), K(ls_handle_), KP(tx_ctx));
      if (tmp_ret == OB_EAGAIN) {
        tmp_ret = OB_SUCCESS;
      }
    }

    if (OB_NOT_NULL(tx_ctx)) {
      (void)ls_handle_.get_ls()->revert_tx_ctx(tx_ctx);
    }
  }

  TRANS_LOG(DEBUG, "iter tx redo sync by task", K(redo_sync_hash_pair.first), K(tmp_ret), K(ret),
            KP(this));

  if (OB_SUCCESS != tmp_ret) {
    int tmp_ret2 = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret2 = del_list_.push_back(redo_sync_hash_pair.first))) {
      TRANS_LOG(WARN, "push back into del_list failed", K(ret), K(tmp_ret), K(tmp_ret2),
                K(redo_sync_hash_pair.first));
    }
  }

  return ret;
}

void ObTxRedoSyncRetryTask::ObTxRedoSyncIterFunc::remove_unused_redo_sync_key()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  RedoSyncKey tmp_key;
  if (del_list_.size() > 0) {
    while (OB_SUCC(del_list_.pop_front(tmp_key))) {
      if (OB_TMP_FAIL(redo_sync_retry_set_.erase_refactored(tmp_key))) {
        TRANS_LOG(WARN, "erase from hash map failed", K(ret), K(tmp_ret),K(tmp_key));
      } else {
        // DUP_TABLE_LOG(INFO, "erase from hash map succ", K(ret), K(tmp_key));
      }
    }
    if (OB_ENTRY_NOT_EXIST == ret) {
      // when pop all list, rewrite ret code
      TRANS_LOG(DEBUG, "end del in while loop", K(ret));
      ret = OB_SUCCESS;
    }
  }
}

int ObTxRedoSyncRetryTask::iter_tx_retry_redo_sync()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  share::ObLSID last_ls_id;
  last_ls_id.reset();
  ObLSHandle ls_handle;
  TransModulePageAllocator allocator;

  ObTxRedoSyncIterFunc redo_sync_func(redo_sync_retry_set_, allocator);

  if (OB_FAIL(redo_sync_retry_set_.foreach_refactored(redo_sync_func))) {
    TRANS_LOG(WARN, "retry to start tx redo sync failed", K(ret), K(redo_sync_retry_set_.size()));
  }

  redo_sync_func.remove_unused_redo_sync_key();

  if (OB_SUCC(ret) && !redo_sync_retry_set_.empty() && ATOMIC_BCAS(&in_thread_pool_, false, true)) {
    set_retry_interval_us(5 * 1000, 5 * 1000);
    if (OB_TMP_FAIL(MTL(ObTransService *)->push(this))) {
      ATOMIC_BCAS(&in_thread_pool_, true, false);
      TRANS_LOG(WARN, "push redo sync task failed", K(ret), K(in_thread_pool_));
    }
  }

  return ret;
}
int ObTxRedoSyncRetryTask::push_back_redo_sync_object(ObTransID tx_id, share::ObLSID ls_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!redo_sync_retry_set_.created()) {
    if (OB_FAIL(redo_sync_retry_set_.create(100))) {
      TRANS_LOG(WARN, "alloc a redo sync set failed", K(ret), K(tx_id), K(ls_id));
    }
  }

  if (OB_SUCC(ret)) {
    RedoSyncKey redo_sync_key;
    redo_sync_key.tx_id_ = tx_id;
    redo_sync_key.ls_id_ = ls_id;
    if (OB_FAIL(redo_sync_retry_set_.set_refactored(redo_sync_key, 0))) {
      if (ret == OB_HASH_EXIST) {
        ret = OB_SUCCESS;
      } else {
        TRANS_LOG(WARN, "insert redo_sync_set into hash_set failed", K(ret), K(tx_id), K(ls_id));
      }
    }
  }

  if (OB_SUCC(ret) && !redo_sync_retry_set_.empty() && ATOMIC_BCAS(&in_thread_pool_, false, true)) {
    set_retry_interval_us(5 * 1000, 5 * 1000);
    if (OB_TMP_FAIL(MTL(ObTransService *)->push(this))) {
      ATOMIC_BCAS(&in_thread_pool_, true, false);
      TRANS_LOG(WARN, "push redo sync task failed", K(ret), K(tx_id), K(ls_id), K(in_thread_pool_));
    }
  }

  TRANS_LOG(INFO, "push redo sync task succ", K(ret), KP(this), K(tx_id), K(ls_id),
            K(in_thread_pool_));

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
  } else if (OB_FAIL(ls_handle.get_ls()->get_dup_table_ls_handler()->receive_lease_request(arg_))) {
    DUP_TABLE_LOG(WARN, "receive_lease_request error", K(ret));
  }

  DUP_TABLE_LOG(DEBUG, "receive lease request", K(ret), K(arg_));
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
  } else if (OB_FAIL(ls_handle.get_ls()->get_dup_table_ls_handler()->handle_ts_sync_request(arg_))) {
    DUP_TABLE_LOG(WARN, "handle ts sync request error", K(ret));
  }

  DUP_TABLE_LOG(DEBUG, "receive ts sync request", K(ret), K(arg_));

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
  } else if (OB_FAIL(ls_handle.get_ls()->get_dup_table_ls_handler()->handle_ts_sync_response(arg_))) {
    DUP_TABLE_LOG(WARN, "handle ts sync request error", K(ret));
  }

  DUP_TABLE_LOG(DEBUG, "receive ts sync response", K(ret), K(arg_));

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
    if (arg_.get_before_prepare_scn_src()
        <= transaction::ObDupTableBeforePrepareRequest::BeforePrepareScnSrc::UNKNOWN) {
      DUP_TABLE_LOG(WARN, "UNKOWN before prepare src", K(ret), K(arg_));
    }

    if (OB_ISNULL(part_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      DUP_TABLE_LOG(WARN, "unexpected part ctx", K(ret), KPC(part_ctx), K(arg_));
    } else if (OB_FAIL(part_ctx->retry_dup_trx_before_prepare(arg_.get_before_prepare_version(), arg_.get_before_prepare_scn_src()))) {
      DUP_TABLE_LOG(WARN, "retry dup trx before_prepare failed", K(ret), KPC(part_ctx), K(arg_));
    }

    ls_handle.get_ls()->revert_tx_ctx(part_ctx);
  }

  DUP_TABLE_LOG(DEBUG, "receive before prepare request", K(ret), K(arg_));
  return ret;
}

} // namespace obrpc
} // namespace oceanbase
