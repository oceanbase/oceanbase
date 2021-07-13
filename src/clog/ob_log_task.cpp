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

#include "ob_log_task.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_mod_define.h"
#include "share/ob_bg_thread_monitor.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "lib/objectpool/ob_resource_pool.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "common/ob_trace_profile.h"
#include "ob_partition_log_service.h"

namespace oceanbase {
using namespace common;
using namespace lib;
using namespace share;
namespace clog {
void ObLogSimpleBitMap::reset_all()
{
  ATOMIC_STORE(&val_, 0);
}

void ObLogSimpleBitMap::reset_map(const int64_t idx)
{
  uint16_t mask = static_cast<uint16_t>(~(1 << idx));
  uint16_t map = ATOMIC_LOAD(&val_);
  while (!ATOMIC_BCAS(&val_, map, map & mask)) {
    map = ATOMIC_LOAD(&val_);
    PAUSE();
  }
}

void ObLogSimpleBitMap::reset_map_unsafe(const int64_t idx)
{
  uint16_t mask = static_cast<uint16_t>(~(1 << idx));
  val_ = (val_ & mask);
}

void ObLogSimpleBitMap::set_map(const int64_t idx)
{
  uint16_t mask = static_cast<uint16_t>(1 << idx);
  uint16_t map = ATOMIC_LOAD(&val_);
  while (!ATOMIC_BCAS(&val_, map, map | mask)) {
    map = ATOMIC_LOAD(&val_);
    PAUSE();
  }
}

void ObLogSimpleBitMap::set_map_unsafe(const int64_t idx)
{
  uint16_t mask = static_cast<uint16_t>(1 << idx);
  val_ = (val_ | mask);
}

bool ObLogSimpleBitMap::test_map(const int64_t idx) const
{
  return (ATOMIC_LOAD(&val_) & (1 << idx)) != 0;
}

bool ObLogSimpleBitMap::test_map_unsafe(const int64_t idx) const
{
  return (val_ & (1 << idx)) != 0;
}

bool ObLogSimpleBitMap::test_and_set(const int64_t idx)
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

ObLogTask::ObLogTask()
    : log_type_(0),
      majority_cnt_(0),
      state_map_(),
      log_buf_len_(0),
      proposal_id_(),
      log_buf_(NULL),
      generation_timestamp_(OB_INVALID_TIMESTAMP),
      submit_timestamp_(OB_INVALID_TIMESTAMP),
      next_replay_log_ts_(OB_INVALID_TIMESTAMP),
      epoch_id_(OB_INVALID_TIMESTAMP),
      data_checksum_(0),
      accum_checksum_(0),
      ack_list_(),
      submit_cb_(NULL),
      log_cursor_(),
      lock_()
// trace_profile_(NULL),
// flushed_begin_time_(OB_INVALID_TIMESTAMP),
{
  log_cursor_.reset();
}

ObLogTask::~ObLogTask()
{}

// submit_cb can be NULL for leader reconfirming and follower receiving log
int ObLogTask::init(ObISubmitLogCb* submit_cb, const int64_t replica_num, const bool need_replay)
{
  int ret = OB_SUCCESS;
  if (replica_num < 0 || replica_num > OB_MAX_MEMBER_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(replica_num));
  } else {
    majority_cnt_ = static_cast<int8_t>(replica_num / 2 + 1);
    submit_cb_ = submit_cb;
    // if (ENABLE_CLOG_TRACE_PROFILE) {
    //  trace_profile_ = alloc_mgr->alloc_trace_profile();
    //  if (NULL != trace_profile_) {
    //    trace_profile_->init(MODULE_NAME, CLOG_TRACE_PROFILE_WARN_THRESHOLD, true);
    //  }
    // }
    if (need_replay) {
      state_map_.set_map_unsafe(NEED_REPLAY);
    } else {
      state_map_.reset_map_unsafe(NEED_REPLAY);
    }
  }
  return ret;
}

int ObLogTask::quick_init(const int64_t replica_num, const ObLogEntry& log_entry, const bool need_copy)
{
  int ret = OB_SUCCESS;
  if (replica_num < 0 || replica_num > OB_MAX_MEMBER_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(replica_num));
  } else {
    majority_cnt_ = static_cast<int8_t>(replica_num / 2 + 1);
    // flushed_begin_time_ = ObClockGenerator::getClock();

    if (OB_FAIL(log_deep_copy_to_(log_entry, need_copy))) {
      CLOG_LOG(ERROR, "copy log entry failed", K(ret));
    } else {
      state_map_.set_map_unsafe(NEED_REPLAY);
      state_map_.set_map_unsafe(SUBMIT_LOG_EXIST);
      if (need_copy) {
        state_map_.set_map_unsafe(SUBMIT_LOG_BODY_EXIST);
      }
    }
  }
  return ret;
}

int ObLogTask::set_replica_num(const int64_t replica_num)
{
  int ret = OB_SUCCESS;
  if (replica_num < 0 || replica_num > OB_MAX_MEMBER_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(replica_num));
  } else {
    majority_cnt_ = static_cast<int8_t>(replica_num / 2 + 1);
  }
  return ret;
}

int ObLogTask::set_log(const ObLogEntryHeader& header, const char* buff, const bool need_copy)
{
  CLOG_LOG(DEBUG, "-->set_log", K(header));
  int ret = OB_SUCCESS;
  // flushed_begin_time_ = ObTimeUtility::current_time();
  ObLogEntry new_log;
  char* prev_buff = log_buf_;
  if (OB_FAIL(new_log.generate_entry(header, buff))) {
    CLOG_LOG(ERROR, "new_log.generate_entry failed", K(ret), K(header));
  } else if (OB_FAIL(log_deep_copy_to_(new_log, need_copy))) {
    CLOG_LOG(ERROR, "copy log entry failed", K(header.get_partition_key()), K(ret));
  } else {
    if (prev_buff != NULL) {
      TMA_MGR_INSTANCE.free_log_entry_buf(prev_buff);
      prev_buff = NULL;
    }
    state_map_.set_map(SUBMIT_LOG_EXIST);
    state_map_.reset_map(LOCAL_FLUSHED);
    state_map_.reset_map(ALREADY_SEND_TO_STANDBY);
    if (true == need_copy) {
      state_map_.set_map(SUBMIT_LOG_BODY_EXIST);
    } else {
      state_map_.reset_map(SUBMIT_LOG_BODY_EXIST);
    }
  }
  // if (NULL != trace_profile_) {
  //  trace_profile_->set_sign(header.get_log_id());
  // }
  CLOG_LOG(DEBUG, "<--set_log", K(header), K(ret));
  return ret;
}

int ObLogTask::reset_log()
{
  int ret = OB_SUCCESS;
  log_type_ = 0;
  proposal_id_.reset();
  if (NULL != log_buf_) {
    TMA_MGR_INSTANCE.free_log_entry_buf(log_buf_);
    log_buf_ = NULL;
  }
  log_buf_len_ = 0;
  generation_timestamp_ = OB_INVALID_TIMESTAMP;
  submit_timestamp_ = OB_INVALID_TIMESTAMP;
  next_replay_log_ts_ = OB_INVALID_TIMESTAMP;
  state_map_.reset_map(LOCAL_FLUSHED);
  state_map_.reset_map(ALREADY_SEND_TO_STANDBY);
  state_map_.reset_map(SUBMIT_LOG_EXIST);
  state_map_.reset_map(SUBMIT_LOG_BODY_EXIST);
  state_map_.reset_map(IS_TRANS_LOG);
  return ret;
}

const ObLogCursor& ObLogTask::get_log_cursor() const
{
  return log_cursor_;
}

bool ObLogTask::is_log_cursor_valid() const
{
  return log_cursor_.is_valid();
}

int ObLogTask::set_log_cursor(const ObLogCursor& cursor)
{
  int ret = OB_SUCCESS;
  if (!cursor.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(log_cursor_.deep_copy(cursor))) {
    // do nothing
  } else {
    EVENT_INC(CLOG_FLUSHED_COUNT);
    EVENT_ADD(CLOG_FLUSHED_SIZE, get_log_cursor().size_);
    // EVENT_ADD(CLOG_FLUSHED_TIME, ObTimeUtility::current_time() - flushed_begin_time_);
  }
  return ret;
}

int ObLogTask::set_log_cursor_without_stat(const ObLogCursor& cursor)
{
  int ret = OB_SUCCESS;
  if (!cursor.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(log_cursor_.deep_copy(cursor))) {
    // do nothing
  }
  return ret;
}

int ObLogTask::submit_log_succ_cb(const common::ObPartitionKey& pkey, const uint64_t log_id, const bool batch_committed,
    const bool batch_first_participant)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (submit_cb_ == NULL) {
  } else if (is_on_success_cb_called()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR,
        "on_success is already called, unexpectd error",
        K(ret),
        K(pkey),
        K(log_id),
        K(batch_committed),
        K(batch_first_participant));
  } else {
    // if (NULL != trace_profile_) {
    //  int tmp_ret = OB_SUCCESS;
    //  if (OB_SUCCESS != (tmp_ret = trace_profile_->trace(pkey,
    //                                                     ON_SUCCESS))) {
    //    CLOG_LOG(WARN, "trace_profile trace fail", "partition_key", pkey, K(tmp_ret));
    //  }
    //  trace_profile_->report_trace();
    // }
    bool need_post_cb = false;
    ObISubmitLogCb* tmp_cb = submit_cb_;
    ObISubmitLogCb* next_cb = NULL;
    ObISubmitLogCb* post_cb = NULL;
    do {
      // The next pointer must be saved in advance, the cb object may have been reused after the on_success callback
      next_cb = tmp_cb->next_;
      // The next_ pointer has been saved. At this time,
      // set this pointer to NULL to avoid using illegal pointers when this object is reused
      tmp_cb->next_ = NULL;
      if (OB_LOG_AGGRE != get_log_type() && NULL != next_cb) {
        // not aggre log, next_ must be NULL
        CLOG_LOG(ERROR, "unexpected next_cb", "log_type", get_log_type(), KP(next_cb), K(pkey), K(log_id));
        next_cb = NULL;
      }
      need_post_cb = tmp_cb->need_post_cb();
      const int64_t on_success_begin_time = ObTimeUtility::current_time();
      const int64_t submit_timestamp = (OB_LOG_AGGRE == log_type_) ? tmp_cb->submit_timestamp_ : submit_timestamp_;
      BG_MONITOR_GUARD_DEFAULT(1000 * 1000);
      if (OB_SUCCESS != (tmp_ret = tmp_cb->on_success(pkey,
                             static_cast<ObLogType>(log_type_),
                             log_id,
                             submit_timestamp,
                             batch_committed,
                             batch_first_participant))) {
        CLOG_LOG(ERROR, "on_success failed", K(ret), K(tmp_ret), K(pkey), K(log_id));
      } else if (need_post_cb) {
        if (NULL == post_cb) {
          post_cb = tmp_cb;
          tmp_cb->next_ = NULL;
        } else {
          tmp_cb->next_ = post_cb;
          post_cb = tmp_cb;
        }
      }
      const int64_t on_success_duration = ObTimeUtility::current_time() - on_success_begin_time;
      if (on_success_duration > TRANSACTION_ON_SUCC_CB_TIME_THRESHOLD) {
        CLOG_LOG(WARN, "transaction on success costs", K(on_success_duration), K(pkey), K(log_id));
      }
      EVENT_INC(ON_SUCCESS_COUNT);
      EVENT_ADD(ON_SUCCESS_TIME, on_success_duration);
      if (batch_committed) {
        EVENT_INC(CLOG_BATCH_COMMITTED_COUNT);
      }
    } while (NULL != (tmp_cb = next_cb));

    while (NULL != post_cb) {
      const int64_t submit_timestamp = (OB_LOG_AGGRE == log_type_) ? tmp_cb->submit_timestamp_ : submit_timestamp_;
      if (OB_SUCCESS != (tmp_ret = post_cb->on_success_post(
                             pkey, log_id, submit_timestamp, batch_committed, batch_first_participant))) {
        CLOG_LOG(ERROR, "on_success_post failed", K(ret), K(tmp_ret), K(pkey), K(log_id));
      }
      post_cb = post_cb->next_;
    }

    EVENT_INC(CLOG_LEADER_CONFIRM_COUNT);
    // EVENT_ADD(CLOG_LEADER_CONFIRM_TIME, on_success_end_time - flushed_begin_time_);
    (void)set_on_success_cb_called();
  }
  return ret;
}

int ObLogTask::submit_log_finished_cb(const common::ObPartitionKey& pkey, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (submit_cb_ == NULL) {
  } else {
    // if (NULL != trace_profile_) {
    //  int tmp_ret = OB_SUCCESS;
    //  if (OB_SUCCESS != (tmp_ret = trace_profile_->trace(submit_log_.get_header().get_partition_key(),
    //                                                     ON_FINISHED))) {
    //    CLOG_LOG(WARN, "trace_profile trace fail", "partition_key",
    //             submit_log_.get_header().get_partition_key(), K(tmp_ret));
    //  }
    //  trace_profile_->report_trace();
    // }
    ObISubmitLogCb* tmp_cb = submit_cb_;
    ObISubmitLogCb* next_cb = NULL;
    do {
      // The next pointer must be saved in advance, the cb object may have been reused after the on_finished callback
      next_cb = tmp_cb->next_;
      // The next_ pointer has been saved. At this time,
      // set this pointer to NULL to avoid using illegal pointers when this object is reused
      tmp_cb->next_ = NULL;
      if (OB_LOG_AGGRE != get_log_type() && NULL != next_cb) {
        // not aggre log, next_ must be NULL
        CLOG_LOG(ERROR, "unexpectd next_cb", "log_type", get_log_type(), KP(next_cb), K(pkey), K(log_id));
        next_cb = NULL;
      }
      if (OB_SUCCESS != (tmp_ret = tmp_cb->on_finished(pkey, log_id))) {
        CLOG_LOG(ERROR, "on_finished failed", K(ret), K(pkey), K(log_id));
      }
    } while (NULL != (tmp_cb = next_cb));

    (void)set_on_finished_cb_called();
  }
  return ret;
}

void ObLogTask::set_submit_cb(ObISubmitLogCb* cb)
{
  submit_cb_ = cb;
}

void ObLogTask::reset_submit_cb()
{
  submit_cb_ = NULL;
}

int ObLogTask::reset_state(const bool need_reset_confirmed_info)
{
  int ret = OB_SUCCESS;
  state_map_.reset_map(MAJORITY_FINISHED);
  state_map_.reset_map(STANDBY_MAJORITY_FINISHED);
  ack_list_.reset();
  if (!is_submit_log_exist() || need_reset_confirmed_info) {
    state_map_.reset_map(CONFIRMED_INFO_EXIST);
    // batch_committed flag must be cleard when reset confirmed_info
    state_map_.reset_map(BATCH_COMMITTED);
  }
  if (!is_confirmed_info_exist()) {
    state_map_.reset_map(IS_CONFIRMED);
  }
  reset_pinned();
  return ret;
}

int ObLogTask::reset_all_state()
{
  int ret = OB_SUCCESS;
  state_map_.reset_all();
  ack_list_.reset();
  return ret;
}

void ObLogTask::reset_log_cursor()
{
  log_cursor_.reset();
}

void ObLogTask::set_on_success_cb_called()
{
  state_map_.set_map(ON_SUCCESS_CB_CALLED);
}

bool ObLogTask::is_on_success_cb_called() const
{
  return state_map_.test_map(ON_SUCCESS_CB_CALLED);
}

bool ObLogTask::is_submit_log_exist() const
{
  return state_map_.test_map(SUBMIT_LOG_EXIST);
}

bool ObLogTask::is_submit_log_body_exist() const
{
  return state_map_.test_map(SUBMIT_LOG_BODY_EXIST);
}

bool ObLogTask::try_set_majority_finished()
{
  bool majority = false;
  // The log of leader has been flushed to disk is a necessary condition for majority
  // so it only count follower's ack here.
  if (ack_list_.get_count() >= majority_cnt_ - 1 && is_flush_local_finished() &&
      state_map_.test_and_set(MAJORITY_FINISHED)) {
    majority = true;
  }
  return majority;
}

void ObLogTask::set_standby_majority_finished()
{
  state_map_.set_map(STANDBY_MAJORITY_FINISHED);
}

void ObLogTask::set_flush_local_finished()
{
  state_map_.set_map(LOCAL_FLUSHED);
}

bool ObLogTask::is_flush_local_finished() const
{
  return state_map_.test_map(LOCAL_FLUSHED);
}

void ObLogTask::set_already_send_to_standby()
{
  state_map_.set_map(ALREADY_SEND_TO_STANDBY);
}

bool ObLogTask::is_already_send_to_standby() const
{
  return state_map_.test_map(ALREADY_SEND_TO_STANDBY);
}

bool ObLogTask::is_local_majority_flushed() const
{
  // only check majority in local cluster
  bool bool_ret = false;
  if (ack_list_.get_count() >= majority_cnt_ - 1 && is_flush_local_finished()) {
    bool_ret = true;
  }
  return bool_ret;
}

bool ObLogTask::is_majority_finished() const
{
  return state_map_.test_map(MAJORITY_FINISHED);
}

bool ObLogTask::is_standby_majority_finished() const
{
  return state_map_.test_map(STANDBY_MAJORITY_FINISHED);
}

bool ObLogTask::can_be_removed()
{
  bool bret = false;
  if ((!is_index_log_submitted()) || (!is_confirmed_info_exist())) {
  } else {
    bret = true;
  }
  return bret;
}

bool ObLogTask::can_overwrite(const ObILogExtRingBufferData* log_task)
{
  UNUSED(log_task);
  return false;
}

bool ObLogTask::need_replay() const
{
  return state_map_.test_map(NEED_REPLAY);
}

int ObLogTask::try_set_need_replay(const bool need_replay)
{
  int ret = OB_SUCCESS;
  if (is_on_success_cb_called()) {
    // on_success_cb_called is mutually execlusive with need_replay
    state_map_.reset_map(NEED_REPLAY);
    if (need_replay) {
      CLOG_LOG(WARN, "try set need_replay while on_success_cb_called is marked", "log_task", *this);
    }
  } else {
    if (need_replay) {
      state_map_.set_map(NEED_REPLAY);
    } else {
      state_map_.reset_map(NEED_REPLAY);
    }
  }
  return ret;
}

void ObLogTask::destroy()
{
  // if (NULL != trace_profile_) {
  //  ob_slice_free_trace_profile(trace_profile_);
  //  trace_profile_ = NULL;
  // }
  if (NULL != log_buf_) {
    TMA_MGR_INSTANCE.free_log_entry_buf(log_buf_);
    log_buf_ = NULL;
  }
  TMA_MGR_INSTANCE.free_log_task_buf(this);
}

int ObLogTask::ack_log(const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  ack_list_.add_server(server);
  // const int64_t current_time = ObTimeUtility::current_time();
  EVENT_INC(CLOG_ACK_COUNT);
  // EVENT_ADD(CLOG_ACK_TIME, current_time - flushed_begin_time_);
  if (ack_list_.get_count() == 1) {
    EVENT_INC(CLOG_FIRST_ACK_COUNT);
    // EVENT_ADD(CLOG_FIRST_ACK_TIME, current_time - flushed_begin_time_);
  }
  return ret;
}

void ObLogTask::set_on_finished_cb_called()
{
  state_map_.set_map(ON_FINISHED_CB_CALLED);
}

bool ObLogTask::is_on_finished_cb_called() const
{
  return state_map_.test_map(ON_FINISHED_CB_CALLED);
}

void ObLogTask::set_index_log_submitted()
{
  state_map_.set_map(INDEX_LOG_SUBMITTED);
}

bool ObLogTask::is_index_log_submitted() const
{
  return state_map_.test_map(INDEX_LOG_SUBMITTED);
}

bool ObLogTask::is_log_confirmed() const
{
  return state_map_.test_map(IS_CONFIRMED);
}

ObLogType ObLogTask::get_log_type() const
{
  return static_cast<ObLogType>(log_type_);
}

common::ObProposalID ObLogTask::get_proposal_id() const
{
  return proposal_id_;
}

char* ObLogTask::get_log_buf() const
{
  return log_buf_;
}

int32_t ObLogTask::get_log_buf_len() const
{
  return log_buf_len_;
}

int64_t ObLogTask::get_generation_timestamp() const
{
  return generation_timestamp_;
}

int64_t ObLogTask::get_submit_timestamp() const
{
  return submit_timestamp_;
}

int64_t ObLogTask::get_next_replay_log_ts() const
{
  return next_replay_log_ts_;
}

int64_t ObLogTask::get_data_checksum() const
{
  return data_checksum_;
}

int64_t ObLogTask::get_epoch_id() const
{
  return epoch_id_;
}

int64_t ObLogTask::get_accum_checksum() const
{
  return accum_checksum_;
}

void ObLogTask::set_confirmed_info(const ObConfirmedInfo& confirmed_info)
{
  state_map_.set_map(CONFIRMED_INFO_EXIST);
  const int64_t arg_data_checksum = confirmed_info.get_data_checksum();
  if (is_submit_log_exist()) {
    // check data_checksum_ and epoch_id_ when log exists
    if (data_checksum_ != arg_data_checksum || epoch_id_ != confirmed_info.get_epoch_id()) {
      CLOG_LOG(ERROR, "set_confirmed_info meta info not match", K(data_checksum_), K(epoch_id_), K(confirmed_info));
    }
  }
  epoch_id_ = confirmed_info.get_epoch_id();
  data_checksum_ = arg_data_checksum;
  accum_checksum_ = confirmed_info.get_accum_checksum();
}

void ObLogTask::set_log_confirmed()
{
  state_map_.set_map(IS_CONFIRMED);
}

// int ObLogTask::report_trace()
//{
//  int ret = OB_SUCCESS;
//  if (NULL != trace_profile_) {
//    trace_profile_->report_trace();
//  }
//  return ret;
//}

bool ObLogTask::is_confirmed_info_exist() const
{
  return state_map_.test_map(CONFIRMED_INFO_EXIST);
}

bool ObLogTask::try_pre_index_log_submitted()
{
  return state_map_.test_and_set(PRE_INDEX_LOG_SUBMITTED);
}

void ObLogTask::reset_pre_index_log_submitted()
{
  state_map_.reset_map(PRE_INDEX_LOG_SUBMITTED);
}

void ObLogTask::set_batch_committed()
{
  state_map_.set_map(BATCH_COMMITTED);
}

bool ObLogTask::is_batch_committed() const
{
  return state_map_.test_map(BATCH_COMMITTED);
}

void ObLogTask::set_pinned()
{
  state_map_.set_map(IS_PINNED);
}

void ObLogTask::reset_pinned()
{
  state_map_.reset_map(IS_PINNED);
}

bool ObLogTask::is_pinned() const
{
  return state_map_.test_map(IS_PINNED);
}

bool ObLogTask::is_trans_log() const
{
  return state_map_.test_map(IS_TRANS_LOG);
}

bool ObLogTask::is_checksum_verified(const int64_t data_checksum) const
{
  return (data_checksum_ == data_checksum);
}

int ObLogTask::log_deep_copy_to_(const ObLogEntry& log_entry, const bool need_copy)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  if (need_copy) {
    if (NULL ==
        (buf = static_cast<char*>(TMA_MGR_INSTANCE.alloc_log_entry_buf(log_entry.get_header().get_data_len())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(WARN, "allocate memory fail", K(ret), "header", log_entry.get_header());
    } else {
      MEMCPY(buf, log_entry.get_buf(), log_entry.get_header().get_data_len());
      log_buf_ = buf;
      log_buf_len_ = static_cast<int32_t>(log_entry.get_header().get_data_len());
    }
  } else {
    log_buf_ = NULL;
    log_buf_len_ = 0;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(log_entry.get_next_replay_ts_for_rg(next_replay_log_ts_))) {
      CLOG_LOG(WARN, "failed to get_next_replay_ts_for_rg", K(ret), "header", log_entry.get_header());
    } else {
      log_type_ = static_cast<uint8_t>(log_entry.get_header().get_log_type());
      const ObLogEntryHeader& log_header = log_entry.get_header();
      if (log_header.is_trans_log()) {
        state_map_.set_map(IS_TRANS_LOG);
      }
      proposal_id_ = log_header.get_proposal_id();
      generation_timestamp_ = log_header.get_generation_timestamp();
      submit_timestamp_ = log_header.get_submit_timestamp();
      data_checksum_ = log_header.get_data_checksum();
      epoch_id_ = log_header.get_epoch_id();
    }
  }
  return ret;
}
}  // namespace clog
}  // namespace oceanbase
