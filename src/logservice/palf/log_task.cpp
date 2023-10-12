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

#include "log_task.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "log_group_entry_header.h"
#include "log_sliding_window.h"
#include "palf_callback.h"
#include "palf_callback_wrapper.h"

namespace oceanbase
{
using namespace share;
namespace palf
{
LogTaskHeaderInfo& LogTaskHeaderInfo::operator=(const LogTaskHeaderInfo &rval)
{
  if (this != &rval) {
    this->begin_lsn_ = rval.begin_lsn_;
    this->end_lsn_ = rval.end_lsn_;
    this->log_id_ = rval.log_id_;
    this->min_scn_ = rval.min_scn_;
    this->max_scn_ = rval.max_scn_;
    this->data_len_ = rval.data_len_;
    this->proposal_id_ = rval.proposal_id_;
    this->prev_lsn_ = rval.prev_lsn_;
    this->prev_proposal_id_  = rval.prev_proposal_id_;
    this->committed_end_lsn_ = rval.committed_end_lsn_;
    this->data_checksum_ = rval.data_checksum_;
    this->accum_checksum_ = rval.accum_checksum_;
    this->is_padding_log_ = rval.is_padding_log_;
    this->is_raw_write_ = rval.is_raw_write_;
  }
  return *this;
}

void LogTaskHeaderInfo::reset()
{
  begin_lsn_.reset();
  end_lsn_.reset();
  log_id_ = OB_INVALID_LOG_ID;
  min_scn_.reset();
  max_scn_.reset();
  data_len_ = 0;
  proposal_id_ = INVALID_PROPOSAL_ID;
  prev_lsn_.reset();
  prev_proposal_id_ = INVALID_PROPOSAL_ID;
  committed_end_lsn_.reset();
  data_checksum_ = -1;
  accum_checksum_ = -1;
  is_padding_log_ = false;
  is_raw_write_ = false;
}

bool LogTaskHeaderInfo::is_valid() const
{
  return (is_valid_log_id(log_id_) && begin_lsn_.is_valid());
}

void LogSimpleBitMap::reset_all()
{
  ATOMIC_STORE(&val_, 0);
}

void LogSimpleBitMap::reset_map(const int64_t idx)
{
  uint16_t mask = static_cast<uint16_t>(~(1 << idx));
  uint16_t map = ATOMIC_LOAD(&val_);
  while (!ATOMIC_BCAS(&val_, map, map & mask)) {
    map = ATOMIC_LOAD(&val_);
    PAUSE();
  }
}

void LogSimpleBitMap::reset_map_unsafe(const int64_t idx)
{
  uint16_t mask = static_cast<uint16_t>(~(1 << idx));
  val_ = (val_ & mask);
}

void LogSimpleBitMap::set_map(const int64_t idx)
{
  uint16_t mask = static_cast<uint16_t>(1 << idx);
  uint16_t map = ATOMIC_LOAD(&val_);
  while (!ATOMIC_BCAS(&val_, map, map | mask)) {
    map = ATOMIC_LOAD(&val_);
    PAUSE();
  }
}

void LogSimpleBitMap::set_map_unsafe(const int64_t idx)
{
  uint16_t mask = static_cast<uint16_t>(1 << idx);
  val_ = (val_ | mask);
}

bool LogSimpleBitMap::test_map(const int64_t idx) const
{
  return (ATOMIC_LOAD(&val_) & (1 << idx)) != 0;
}

bool LogSimpleBitMap::test_map_unsafe(const int64_t idx) const
{
  return (val_ & (1 << idx)) != 0;
}

bool LogSimpleBitMap::test_and_set(const int64_t idx)
{
  bool bret = true;
  uint16_t mask = static_cast<uint16_t>(1 << idx);
  do {
    uint16_t val = ATOMIC_LOAD(&val_);
    if (ATOMIC_BCAS(&val_, val, val | mask)) {
      if ((val & mask) != 0) {
        bret = false;
      }
      break;
    } else {
      PAUSE();
    }
  } while (true);
  return bret;
}

LogTask::LogTask()
  :  state_map_(),
     header_(),
     ref_cnt_(0),
     log_cnt_(0),
     gen_ts_(OB_INVALID_TIMESTAMP),
     freeze_ts_(OB_INVALID_TIMESTAMP),
     submit_ts_(OB_INVALID_TIMESTAMP),
     flushed_ts_(OB_INVALID_TIMESTAMP),
     lock_()
{
  reset();
}

LogTask::~LogTask()
{
  reset();
  destroy();
}

void LogTask::destroy()
{}

void LogTask::reset()
{
  state_map_.reset_all();
  header_.reset();
  ref_cnt_ = 0;
  log_cnt_ = 0;
  gen_ts_ = OB_INVALID_TIMESTAMP;
  freeze_ts_ = OB_INVALID_TIMESTAMP;
  submit_ts_ = OB_INVALID_TIMESTAMP;
  flushed_ts_ = OB_INVALID_TIMESTAMP;
}

bool LogTask::can_be_slid()
{
  bool bret = false;
  if (is_pre_slide()) {
    bret = true;
  }
  return bret;
}

void LogTask::set_group_log_checksum(const int64_t data_checksum)
{
  header_.data_checksum_ = data_checksum;
}

void LogTask::inc_update_max_scn(const SCN &scn)
{
  header_.max_scn_.inc_update(scn);
}

void LogTask::update_data_len(const int64_t data_len)
{
  ATOMIC_AAF(&(header_.data_len_), data_len);
}

int LogTask::set_initial_header_info(const LogTaskHeaderInfo &header_info)
{
  // Caller need hold lock.
  // This is called by leader when submitting the first log entry for this log_task.
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "log_task has been valid, unexpected", K(ret), K(header_info));
  } else {
    header_.begin_lsn_ = header_info.begin_lsn_;
    header_.log_id_ = header_info.log_id_;
    header_.is_padding_log_ = header_info.is_padding_log_;
    header_.proposal_id_ = header_info.proposal_id_;
    header_.min_scn_ = header_info.min_scn_;
    update_data_len(header_info.data_len_);
    // Note: 这里不能直接赋值，需用inc_update，因为其他log_entry可能已经更新了max_scn_
    inc_update_max_scn(header_info.max_scn_);
    // The first log is responsible for changing state to valid.
    set_valid();
    PALF_LOG(TRACE, "set_initial_header_info", K(ret), K(header_info), KPC(this));
  }
  return ret;
}

int LogTask::update_header_info(const LSN &committed_end_lsn, const int64_t accum_checksum)
{
  // caller need hold lock
  // this function is called when receiving log
  int ret = OB_SUCCESS;
  if (!committed_end_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(committed_end_lsn), K(accum_checksum));
  } else if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "log_task is invalid, unexpected", K(ret), K(committed_end_lsn),
        K(accum_checksum), KPC(this));
  } else {
    header_.committed_end_lsn_ = committed_end_lsn;
    header_.accum_checksum_ = accum_checksum;
    PALF_LOG(TRACE, "update_header_info", K(ret), K(committed_end_lsn),
        K(accum_checksum), KPC(this));
  }
  return ret;
}

int LogTask::set_group_header(const LSN &lsn, const SCN &scn, const LogGroupEntryHeader &group_entry_header)
{
  // caller need hold lock
  // this function is called when receiving log
  int ret = OB_SUCCESS;
  if (!lsn.is_valid() || !group_entry_header.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(lsn), K(group_entry_header));
  } else if (is_valid()) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "log_task has been valid", K(ret), K(lsn), K(scn), K(group_entry_header), KPC(this));
  } else {
    header_.begin_lsn_ = lsn;
    header_.end_lsn_ = lsn + group_entry_header.get_serialize_size() + group_entry_header.get_data_len();
    header_.log_id_ = group_entry_header.get_log_id();
    header_.is_padding_log_ = group_entry_header.is_padding_log();
    header_.proposal_id_ = group_entry_header.get_log_proposal_id();  // leader's proposal_id when generate this log
    header_.min_scn_ = scn;
    header_.max_scn_ = group_entry_header.get_max_scn();
    header_.data_len_ = group_entry_header.get_data_len();          // total len without log_group_entry_header
    header_.committed_end_lsn_ = group_entry_header.get_committed_end_lsn();  // 乱序收日志时前面空洞补齐后需要该值
    header_.accum_checksum_ = group_entry_header.get_accum_checksum();
    header_.is_raw_write_ = group_entry_header.is_raw_write();
    set_valid();  // set valid
  }
  return ret;
}

void LogTask::set_prev_lsn(const LSN &prev_lsn)
{
  header_.prev_lsn_ = prev_lsn;
}

void LogTask::set_prev_log_proposal_id(const int64_t &prev_log_proposal_id)
{
  header_.prev_proposal_id_ = prev_log_proposal_id;
}

void LogTask::set_end_lsn(const LSN &end_lsn)
{
  header_.end_lsn_ = end_lsn;
}

int LogTask::try_freeze(const LSN &end_lsn)
{
  return try_freeze_(end_lsn);
}

int LogTask::try_freeze_by_myself()
{
  // caller need hold lock
  int ret = OB_SUCCESS;
  if (!header_.end_lsn_.is_valid()) {
    ret = OB_EAGAIN;
    PALF_LOG(TRACE, "header_.end_lsn_ is invalid, cannot freeze by myself", K(ret), K(*this));
  } else {
    // This log_task has not been freezed and its end_lsn_ has been set, try freeze
    ret = try_freeze_(header_.end_lsn_);
  }
  return ret;
}

int LogTask::try_freeze_(const LSN &end_lsn)
{
  // caller need hold lock
  int ret = OB_SUCCESS;
  if (is_freezed()) {
    // this log_task has been freezed, return OB_SUCCESS
    PALF_LOG(TRACE, "this log_task has been freezed", K(ret), K(*this));
  } else if (!end_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(end_lsn));
  } else if (!this->is_valid()) {
    ret = OB_EAGAIN;
    PALF_LOG(WARN, "this log_task is invalid now, need retry", K(ret), K(end_lsn));
  } else if (header_.begin_lsn_ >= end_lsn) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "begin_lsn_ is not less than arg end_lsn", K(ret), KPC(this), K(end_lsn));
  } else {
    const int64_t total_len = end_lsn - header_.begin_lsn_;
    assert(total_len > 0);
    // ref_cnt_ does not include log_entry_header part
    const int64_t ref_val = 0 - (total_len - LogGroupEntryHeader::HEADER_SER_SIZE);
    ref(ref_val, false);
    header_.end_lsn_ = end_lsn;  // update header_.end_lsn_
    state_map_.set_map(IS_FREEZED);
  }
  return ret;
}

void LogTask::set_valid()
{
  gen_ts_ = common::ObTimeUtility::current_time();
  state_map_.set_map(IS_VALID);
}

bool LogTask::is_valid() const
{
  return state_map_.test_map(IS_VALID);
}

void LogTask::set_freezed()
{
  state_map_.set_map(IS_FREEZED);
}

bool LogTask::is_freezed() const
{
  return state_map_.test_map(IS_FREEZED);
}

bool LogTask::try_pre_fill()
{
  return state_map_.test_and_set(PRE_FILL);
}

void LogTask::reset_pre_fill()
{
  state_map_.reset_map(PRE_FILL);
}

bool LogTask::try_pre_slide()
{
  return state_map_.test_and_set(PRE_SLIDE);
}

bool LogTask::is_pre_slide()
{
  return state_map_.test_map(PRE_SLIDE);
}

void LogTask::reset_pre_slide()
{
  // caller need hold lock
  state_map_.reset_map(PRE_SLIDE);
}

bool LogTask::try_pre_submit()
{
  // caller need hold lock
  bool bool_ret = false;
  if (is_freezed() && 0 == ATOMIC_LOAD(&ref_cnt_)) {
    bool_ret = state_map_.test_and_set(PRE_SUBMIT);
  }
  return bool_ret;
}

bool LogTask::is_pre_submit() const
{
  return state_map_.test_map(PRE_SUBMIT);
}

void LogTask::reset_pre_submit()
{
  // caller need hold lock
  state_map_.reset_map(PRE_SUBMIT);
}

void LogTask::reset_submit_log_exist()
{
  // caller need hold lock
  state_map_.reset_map(SUBMIT_LOG_EXIST);
}

bool LogTask::set_submit_log_exist()
{
  // caller need hold lock
  return state_map_.test_and_set(SUBMIT_LOG_EXIST);
}

bool LogTask::is_submit_log_exist() const
{
  return state_map_.test_map(SUBMIT_LOG_EXIST);
}

int64_t LogTask::ref(const int64_t val, const bool is_append_log)
{
  if (is_append_log) {
    ATOMIC_INC(&log_cnt_);
  }
  return ATOMIC_AAF(&ref_cnt_, val);
}

void LogTask::set_freeze_ts(const int64_t ts)
{
  // freeze ts may be setted more than once
  ATOMIC_CAS(&freeze_ts_, OB_INVALID_TIMESTAMP, ts);
}

void LogTask::set_submit_ts(const int64_t ts)
{
  ATOMIC_STORE(&submit_ts_, ts);
}

void LogTask::set_flushed_ts(const int64_t ts)
{
  ATOMIC_STORE(&flushed_ts_, ts);
}

}  // namespace palf
}  // namespace oceanbase
