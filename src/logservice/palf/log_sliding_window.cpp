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

#define USING_LOG_PREFIX PALF
#include "log_sliding_window.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/queue/ob_link_queue.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/ob_define.h"
#include "palf_callback_wrapper.h"
#include "log_writer_utils.h"
#include "log_entry_header.h"
#include "log_group_entry_header.h"
#include "log_entry_header.h"
#include "log_engine.h"
#include "log_io_task_cb_utils.h"
#include "log_config_mgr.h"
#include "log_state_mgr.h"
#include "log_mode_mgr.h"
namespace oceanbase
{
using namespace share;
namespace palf
{

bool UpdateMatchLsnFunc::operator()(const common::ObAddr &server, LsnTsInfo &value)
{
  bool bool_ret = true;
  if (!value.is_valid()) {
    bool_ret = false;
  } else if (value.lsn_ <= new_end_lsn_) {
    old_end_lsn_ = value.lsn_;
    old_advance_time_us_ = value.last_advance_time_us_;
    if (value.lsn_ < new_end_lsn_) {
      // Update last_advance_time_us_ when lsn really changes.
      value.last_advance_time_us_ = new_ack_time_us_;
      value.last_ack_time_us_ = new_ack_time_us_;
    }
    value.lsn_ = new_end_lsn_;
    bool_ret = true;
  }
  return bool_ret;
}

bool GetLaggedListFunc::operator()(const common::ObAddr &server, LsnTsInfo &value)
{
  bool bool_ret = true;
  int tmp_ret = OB_SUCCESS;
  if (!value.is_valid()) {
    // skip
  } else if (value.lsn_ >= dst_lsn_) {
    // skip
  } else if (OB_SUCCESS != (tmp_ret = lagged_list_.add_server(server))){
    PALF_LOG_RET(ERROR, tmp_ret, "lagged_list_.add_server failed", K(tmp_ret));
  }
  return bool_ret;
}

LogSlidingWindow::LogSlidingWindow()
  : self_(),
    sw_(),
    checksum_(),
    state_mgr_(NULL),
    mm_(NULL),
    mode_mgr_(NULL),
    log_engine_(NULL),
    plugins_(NULL),
    lsn_allocator_(),
    group_buffer_(),
    last_submit_info_lock_(common::ObLatchIds::PALF_SW_SUBMIT_INFO_LOCK),
    last_submit_lsn_(),
    last_submit_end_lsn_(),
    last_submit_log_id_(OB_INVALID_LOG_ID),
    last_submit_log_pid_(INVALID_PROPOSAL_ID),
    max_flushed_info_lock_(),
    max_flushed_lsn_(),
    max_flushed_end_lsn_(),
    max_flushed_log_pid_(INVALID_PROPOSAL_ID),
    committed_end_lsn_(),
    last_slide_info_lock_(common::ObLatchIds::PALF_SW_SLIDE_INFO_LOCK),
    last_slide_log_id_(OB_INVALID_LOG_ID),
    last_slide_scn_(),
    last_slide_lsn_(),
    last_slide_log_pid_(INVALID_PROPOSAL_ID),
    last_slide_log_accum_checksum_(-1),
    fetch_info_lock_(common::ObLatchIds::PALF_SW_FETCH_INFO_LOCK),
    last_fetch_req_time_(0),
    last_fetch_end_lsn_(),
    last_fetch_max_log_id_(OB_INVALID_LOG_ID),
    last_fetch_committed_end_lsn_(),
    last_fetch_trigger_type_(FetchTriggerType::LOG_LOOP_TH),
    match_lsn_map_lock_(common::ObLatchIds::PALF_SW_MATCH_LSN_MAP_LOCK),
    match_lsn_map_(),
    last_truncate_lsn_(),
    cannot_fetch_log_warn_time_(OB_INVALID_TIMESTAMP),
    cannot_freeze_log_warn_time_(OB_INVALID_TIMESTAMP),
    larger_log_warn_time_(OB_INVALID_TIMESTAMP),
    log_life_long_warn_time_(OB_INVALID_TIMESTAMP),
    lc_cb_get_warn_time_(OB_INVALID_TIMESTAMP),
    fetch_failure_print_time_(OB_INVALID_TIMESTAMP),
    commit_log_handling_lease_(),
    submit_log_handling_lease_(),
    last_fetch_log_renew_leader_ts_us_(OB_INVALID_TIMESTAMP),
    end_lsn_stat_time_us_(OB_INVALID_TIMESTAMP),
    reconfirm_fetch_dest_(),
    is_truncating_(false),
    is_rebuilding_(false),
    last_rebuild_lsn_(),
    last_record_end_lsn_(PALF_INITIAL_LSN_VAL),
    fs_cb_cost_stat_("[PALF STAT FS CB EXCUTE COST TIME]", PALF_STAT_PRINT_INTERVAL_US),
    log_life_time_stat_("[PALF STAT LOG LIFE TIME]", PALF_STAT_PRINT_INTERVAL_US),
    accum_slide_log_cnt_(0),
    accum_log_gen_to_freeze_cost_(0),
    accum_log_gen_to_submit_cost_(0),
    accum_log_submit_to_flush_cost_(0),
    accum_log_submit_to_first_ack_cost_(0),
    accum_log_submit_to_commit_cost_(0),
    accum_log_submit_to_slide_cost_(0),
    group_log_stat_time_us_(OB_INVALID_TIMESTAMP),
    accum_log_cnt_(0),
    accum_group_log_size_(0),
    last_record_group_log_id_(FIRST_VALID_LOG_ID - 1),
    freeze_mode_(FEEDBACK_FREEZE_MODE),
    is_inited_(false)
{}

void LogSlidingWindow::destroy()
{
  is_inited_ = false;
  is_truncating_ = false;
  is_rebuilding_ = false;
  last_rebuild_lsn_.reset();
  int tmp_ret = OB_SUCCESS;
  sw_.destroy();
  group_buffer_.destroy();
  match_lsn_map_.destroy();
  reconfirm_fetch_dest_.reset();
  state_mgr_ = NULL;
  log_engine_ = NULL;
  plugins_ = NULL;
  mm_ = NULL;
  mode_mgr_ = NULL;
}

int LogSlidingWindow::flashback(const PalfBaseInfo &palf_base_info, const int64_t palf_id, common::ObILogAllocator *alloc_mgr)
{
  int ret = OB_SUCCESS;
  const LogInfo &prev_log_info = palf_base_info.prev_log_info_;
  sw_.destroy();
  lsn_allocator_.reset();
  WLockGuard guard(group_buffer_lock_);  // lock group_buffer_
  group_buffer_.destroy();
  checksum_.destroy();
  match_lsn_map_.destroy();
  if (OB_FAIL(sw_.init(prev_log_info.log_id_ + 1, PALF_SLIDING_WINDOW_SIZE, alloc_mgr))) {
    PALF_LOG(WARN, "sw init failed", K(ret), K(palf_id), K(palf_base_info));
  } else if (OB_FAIL(lsn_allocator_.init(prev_log_info.log_id_,
          prev_log_info.scn_, palf_base_info.curr_lsn_))) {
    PALF_LOG(WARN, "lsn_allocator_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(group_buffer_.init(palf_base_info.curr_lsn_))) {
    PALF_LOG(WARN, "group_buffer_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(checksum_.init(palf_id, prev_log_info.accum_checksum_))) {
    PALF_LOG(WARN, "checksum_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(match_lsn_map_.init("MatchOffsetMap", MTL_ID()))) {
    PALF_LOG(WARN, "match_lsn_map_ init failed", K(ret), K(palf_id));
  } else {
    last_submit_lsn_ = prev_log_info.lsn_;
    last_submit_end_lsn_ = palf_base_info.curr_lsn_;
    last_submit_log_id_ = prev_log_info.log_id_;
    last_submit_log_pid_ = prev_log_info.log_proposal_id_;

    max_flushed_lsn_ = prev_log_info.lsn_;
    max_flushed_end_lsn_ = palf_base_info.curr_lsn_;
    max_flushed_log_pid_ = prev_log_info.log_proposal_id_;

    last_slide_log_id_ = prev_log_info.log_id_;
    last_slide_scn_ = prev_log_info.scn_;
    last_slide_lsn_ = prev_log_info.lsn_;
    last_slide_end_lsn_ = palf_base_info.curr_lsn_;
    last_slide_log_pid_ = prev_log_info.log_proposal_id_;
    last_slide_log_accum_checksum_ = prev_log_info.accum_checksum_;

    committed_end_lsn_ = palf_base_info.curr_lsn_;
    reset_match_lsn_map_();

    LogGroupEntryHeader group_header;
    LogEntryHeader log_header;
    PALF_LOG(INFO, "sw flashback success", K(ret), KPC(this), K(palf_base_info),
        "group header size", LogGroupEntryHeader::HEADER_SER_SIZE, "log entry size",
        LogEntryHeader::HEADER_SER_SIZE, "group_header ser size", group_header.get_serialize_size(),
        "log header ser size", log_header.get_serialize_size());
  }
  return ret;
}

int LogSlidingWindow::init(const int64_t palf_id,
                           const common::ObAddr &self,
                           LogStateMgr *state_mgr,
                           LogConfigMgr *mm,
                           LogModeMgr *mode_mgr,
                           LogEngine *log_engine,
                           palf::PalfFSCbWrapper *palf_fs_cb,
                           common::ObILogAllocator *alloc_mgr,
                           LogPlugins *plugins,
                           const PalfBaseInfo &palf_base_info,
                           const bool is_normal_replica)
{
  int ret = OB_SUCCESS;
  const LogInfo &prev_log_info = palf_base_info.prev_log_info_;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (false == is_valid_palf_id(palf_id)
             || false == self.is_valid()
             || false == palf_base_info.is_valid()
             || NULL == state_mgr
             || NULL == mm
             || NULL == mode_mgr
             || NULL == log_engine
             || NULL == palf_fs_cb
             || NULL == plugins) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K(palf_id), K(self), K(palf_base_info),
        KP(state_mgr), KP(mm), KP(mode_mgr), KP(log_engine), KP(palf_fs_cb), KP(plugins));
  } else if (is_normal_replica && OB_FAIL(do_init_mem_(palf_id, palf_base_info, alloc_mgr))) {
    PALF_LOG(WARN, "do_init_mem_ failed", K(ret), K(palf_id));
  } else {
    palf_id_ = palf_id;
    self_ = self;
    state_mgr_ = state_mgr;
    mm_ = mm;
    mode_mgr_ = mode_mgr;
    log_engine_ = log_engine;
    palf_fs_cb_ = palf_fs_cb;
    plugins_ = plugins;

    last_submit_lsn_ = prev_log_info.lsn_;
    last_submit_end_lsn_ = palf_base_info.curr_lsn_;
    last_submit_log_id_ = prev_log_info.log_id_;
    last_submit_log_pid_ = prev_log_info.log_proposal_id_;

    max_flushed_lsn_ = prev_log_info.lsn_;
    max_flushed_end_lsn_ = palf_base_info.curr_lsn_;
    max_flushed_log_pid_ = prev_log_info.log_proposal_id_;

    last_slide_log_id_ = prev_log_info.log_id_;
    last_slide_scn_ = prev_log_info.scn_;
    last_slide_lsn_ = prev_log_info.lsn_;
    last_slide_end_lsn_ = palf_base_info.curr_lsn_;
    last_slide_log_pid_ = prev_log_info.log_proposal_id_;
    last_slide_log_accum_checksum_ = prev_log_info.accum_checksum_;

    committed_end_lsn_ = palf_base_info.curr_lsn_;

    MEMSET(append_cnt_array_, 0, APPEND_CNT_ARRAY_SIZE * sizeof(int64_t));

    PALF_REPORT_INFO_KV(K_(palf_id));
    fs_cb_cost_stat_.set_extra_info(EXTRA_INFOS);
    log_life_time_stat_.set_extra_info(EXTRA_INFOS);

    is_inited_ = true;
    LogGroupEntryHeader group_header;
    LogEntryHeader log_header;
    PALF_LOG(INFO, "sw init success", K(ret), K_(palf_id), K_(self), K(palf_base_info),
        "group header size", LogGroupEntryHeader::HEADER_SER_SIZE, "log entry size",
        LogEntryHeader::HEADER_SER_SIZE, "group_header ser size", group_header.get_serialize_size(),
        "log header ser size", log_header.get_serialize_size());
  }

  if (OB_SUCCESS != ret) {
    destroy();
  }
  return ret;
}

int LogSlidingWindow::do_init_mem_(const int64_t palf_id,
                                   const PalfBaseInfo &palf_base_info,
                                   common::ObILogAllocator *alloc_mgr)
{
  int ret = OB_SUCCESS;
  const LogInfo &prev_log_info = palf_base_info.prev_log_info_;
  if (OB_FAIL(sw_.init(prev_log_info.log_id_ + 1, PALF_SLIDING_WINDOW_SIZE, alloc_mgr))) {
    PALF_LOG(WARN, "sw init failed", K(ret), K(palf_id), K(palf_base_info));
  } else if (OB_FAIL(lsn_allocator_.init(prev_log_info.log_id_,
          prev_log_info.scn_, palf_base_info.curr_lsn_))) {
    PALF_LOG(WARN, "lsn_allocator_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(group_buffer_.init(palf_base_info.curr_lsn_))) {
    PALF_LOG(WARN, "group_buffer_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(checksum_.init(palf_id, prev_log_info.accum_checksum_))) {
    PALF_LOG(WARN, "checksum_ init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(match_lsn_map_.init("MatchLsnMap", MTL_ID()))) {
    PALF_LOG(WARN, "match_lsn_map_ init failed", K(ret), K(palf_id));
  }
  return ret;
}

bool LogSlidingWindow::can_receive_larger_log_(const int64_t log_id) const
{
  bool bool_ret = true;
  const int64_t start_log_id = get_start_id();
  const int64_t sw_end_log_id = sw_.get_end_sn();
  if (log_id - start_log_id >= PALF_SLIDING_WINDOW_SIZE
      || log_id >= sw_end_log_id) {
    // sw_end_log_id may be less than (start_log_id + PALF_SLIDING_WINDOW_SIZE),
    // because it is updated after the last slid log_task's ref_cnt decrease to 0.
    bool_ret = false;
    if (palf_reach_time_interval(5 * 1000 * 1000, larger_log_warn_time_)) {
      PALF_LOG(INFO, "sw is full, cannot submit larger log", K_(palf_id), K_(self), K(start_log_id), \
          K(sw_end_log_id), K(log_id));
    }
  }
  return bool_ret;
}

bool LogSlidingWindow::leader_can_submit_larger_log_(const int64_t log_id) const
{
  // leader submit new log时调用本接口
  bool bool_ret = true;
  const int64_t start_log_id = get_start_id();
  // sw_end_log_id may be less than (start_log_id + PALF_SLIDING_WINDOW_SIZE),
  // because it is updated after the last slid log_task's ref_cnt decrease to 0.
  const int64_t sw_end_log_id = sw_.get_end_sn();
  if (log_id - start_log_id >= PALF_MAX_LEADER_SUBMIT_LOG_COUNT
      || log_id >= sw_end_log_id) {
    // should guarantee:
    // 1. sliding window in follower - sliding window size in leader > 2, otherwise
    // logs in follower may not be slided, because the log which committed_end_lsn
    // can commit the first log may be out of sliding window.
    // 2. max number of concurrent submitting group log in leader better be half of
    // sliding window size in follower. If not, some logs may be rejected by follower
    // because sliding window is full.
    bool_ret = false;
    if (palf_reach_time_interval(5 * 1000 * 1000, larger_log_warn_time_)) {
      PALF_LOG(INFO, "sw is full, cannot submit larger log", K_(palf_id), K_(self), K(start_log_id), \
          K(sw_end_log_id), K(log_id));
    }
  }
  return bool_ret;
}

bool LogSlidingWindow::leader_can_submit_new_log_(const int64_t valid_log_size, LSN &lsn_upper_bound)
{
  // Check whether leader can submit new log.
  // The valid_log_size does not consider group_header for generating new group log case.
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;
  LSN curr_end_lsn;
  LSN curr_committed_end_lsn;
  get_committed_end_lsn_(curr_committed_end_lsn);
  // calculate lsn_upper_bound
  LSN buffer_reuse_lsn;
  (void) group_buffer_.get_reuse_lsn(buffer_reuse_lsn);
  const int64_t group_buffer_size = group_buffer_.get_available_buffer_size();
  LSN reuse_base_lsn = MIN(curr_committed_end_lsn, buffer_reuse_lsn);
  lsn_upper_bound = reuse_base_lsn + group_buffer_size;

  if (OB_SUCCESS != (tmp_ret = lsn_allocator_.get_curr_end_lsn(curr_end_lsn))) {
    PALF_LOG_RET(WARN, tmp_ret, "get_curr_end_lsn failed", K(tmp_ret), K_(palf_id), K_(self), K(valid_log_size));
  // NB: 采用committed_lsn作为可复用起点的下界，避免写盘立即复用group_buffer导致follower的
  //     group_buffer被uncommitted log填满而无法滑出
  } else if (!group_buffer_.can_handle_new_log(curr_end_lsn, valid_log_size, curr_committed_end_lsn)) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      PALF_LOG_RET(WARN, OB_ERR_UNEXPECTED, "group_buffer_ cannot handle new log now", K(tmp_ret), K_(palf_id), K_(self),
          K(valid_log_size), K(curr_end_lsn), K(curr_committed_end_lsn),
          "start_id", get_start_id(), "max_log_id", get_max_log_id());
    }
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

bool LogSlidingWindow::leader_can_submit_group_log_(const LSN &lsn, const int64_t group_log_size)
{
  // Check whether leader can submit new group log.
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;
  LSN curr_committed_end_lsn;
  get_committed_end_lsn_(curr_committed_end_lsn);
  // NB: 采用committed_lsn作为可复用起点的下界，避免写盘立即复用group_buffer导致follower的
  //     group_buffer被uncommitted log填满而无法滑出
  if (!group_buffer_.can_handle_new_log(lsn, group_log_size, curr_committed_end_lsn)) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      PALF_LOG_RET(WARN, OB_ERR_UNEXPECTED, "group_buffer_ cannot handle new log now", K(tmp_ret), K_(palf_id), K_(self),
          K(lsn), K(group_log_size), K(curr_committed_end_lsn),
          "start_id", get_start_id(), "max_log_id", get_max_log_id());
    }
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

int LogSlidingWindow::leader_wait_sw_slot_ready_(const int64_t log_id)
{
  // 等待sw槽位ready
  // 因为分配log_id时sw可能是满的
  int ret = OB_SUCCESS;
  LogTask *log_task = NULL;
  LogTaskGuard guard(this);
  do {
    if (false == leader_can_submit_larger_log_(log_id)) {
      // double check log_id是否超出leader sw的range限制,
      // 这一步是必要的，因为多线程并发submit时可能使用同一个max_log_id通过前置检查
      ret = OB_EAGAIN;
    } else if (OB_FAIL(guard.get_log_task(log_id, log_task))) {
      if (OB_ERR_OUT_OF_UPPER_BOUND == ret) {
        ret = OB_EAGAIN;
      } else {
        PALF_LOG(ERROR, "get_log_task failed", K(ret), K_(palf_id), K_(self), K(log_id));
      }
    } else {
      // get success, end loop
    }
    if (OB_EAGAIN == ret) {
      ob_usleep(100);  // sleep 100us
    }
  } while(OB_EAGAIN == ret);
  return ret;
}

int LogSlidingWindow::submit_log(const char *buf,
                                 const int64_t buf_len,
                                 const SCN &ref_scn,
                                 LSN &lsn,
                                 SCN &result_scn)
{
  int ret = OB_SUCCESS;
  int64_t log_id = OB_INVALID_LOG_ID;
  SCN scn;
  // whether need generate new log task
  bool is_new_log = false;
  // whether need generate a padding entry at the end of block
  bool need_gen_padding_entry = false;
  // length of padding part
  int64_t padding_size = 0;
  // group log valid size (without padding part)
  const int64_t valid_log_size = LogEntryHeader::HEADER_SER_SIZE + buf_len;
  const int64_t start_log_id = get_start_id();
  const int64_t log_id_upper_bound = start_log_id + PALF_MAX_LEADER_SUBMIT_LOG_COUNT - 1;
  LSN tmp_lsn, lsn_upper_bound;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == buf || buf_len <= 0 || buf_len > MAX_LOG_BODY_SIZE || (!ref_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K_(palf_id), K_(self), K(buf_len), KP(buf));
  } else if (!leader_can_submit_new_log_(valid_log_size, lsn_upper_bound)
             || !leader_can_submit_larger_log_(get_max_log_id() + 1)) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      PALF_LOG(WARN, "cannot submit new log now, try again", K(ret), K_(palf_id), K_(self),
          K(valid_log_size), K(buf_len), "start_id", get_start_id(), "max_log_id", get_max_log_id());
    }
    // sw_ cannot submit larger log
  } else if (OB_FAIL(lsn_allocator_.alloc_lsn_scn(ref_scn, valid_log_size, log_id_upper_bound, lsn_upper_bound,
            tmp_lsn, log_id, scn, is_new_log, need_gen_padding_entry, padding_size))) {
    PALF_LOG(WARN, "alloc_lsn_scn failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_FAIL(leader_wait_sw_slot_ready_(log_id))) {
    PALF_LOG(WARN, "leader_wait_sw_slot_ready_ failed", K(ret), K_(palf_id), K_(self), K(log_id));
  } else {
    PALF_LOG(TRACE, "alloc_lsn_scn success", K(ret), K_(palf_id), K_(self), K(tmp_lsn), K(scn),
        K(log_id), K(valid_log_size), K(is_new_log), K(need_gen_padding_entry), K(padding_size));
    bool is_need_handle_next = false;
    bool is_need_handle = false;
    if (need_gen_padding_entry) {
      // need generate padding entry
      const int64_t padding_entry_body_size = padding_size - LogGroupEntryHeader::HEADER_SER_SIZE;
      if (OB_FAIL(try_freeze_prev_log_(log_id, tmp_lsn, is_need_handle))) {
        // try freeze previous log
        PALF_LOG(ERROR, "try_freeze_prev_log_ failed", K(ret), K_(palf_id), K_(self), K(log_id), K(tmp_lsn),
            K(padding_size), K(is_new_log), K(valid_log_size));
      } else if (is_need_handle && FALSE_IT(is_need_handle_next |= is_need_handle)) {
      } else if (OB_FAIL(generate_new_group_log_(tmp_lsn, log_id, scn, padding_entry_body_size, LOG_PADDING, \
              NULL, padding_entry_body_size, is_need_handle))) {
        PALF_LOG(ERROR, "generate_new_group_log_ failed", K(ret), K_(palf_id), K_(self), K(log_id), K(tmp_lsn), K(padding_size),
            K(is_new_log), K(valid_log_size));
      } else if (is_need_handle && FALSE_IT(is_need_handle_next |= is_need_handle)) {
      } else {
        PALF_LOG(INFO, "generate_new_group_log_ for padding log success", K_(palf_id), K_(self), K(log_id),
            K(padding_size), K(tmp_lsn), K(scn), K(is_need_handle), K(is_need_handle_next));
        // after gen padding_entry, update lsn to next block
        tmp_lsn.val_ += padding_size;
        log_id++;  // inc log_id for following new log
        scn = SCN::plus(scn, 1);
      }
    }
    result_scn = scn;
    lsn = tmp_lsn;
    if (OB_SUCC(ret)) {
      if (is_new_log) {
        // output lsn does not contains log_group_entry_header
        lsn.val_ += LogGroupEntryHeader::HEADER_SER_SIZE;
        if (OB_FAIL(try_freeze_prev_log_(log_id, tmp_lsn, is_need_handle))) {
          PALF_LOG(WARN, "try_freeze_prev_log_ failed", K(ret), K_(palf_id), K_(self), K(log_id));
        } else if (is_need_handle && FALSE_IT(is_need_handle_next |= is_need_handle)) {
        } else if (OB_FAIL(generate_new_group_log_(tmp_lsn, log_id, scn, valid_log_size, LOG_SUBMIT, \
                buf, buf_len, is_need_handle))) {
          PALF_LOG(WARN, "generate_new_group_log_ failed", K(ret), K_(palf_id), K_(self), K(log_id));
        } else if (is_need_handle && FALSE_IT(is_need_handle_next |= is_need_handle)) {
        } else {
          PALF_LOG(TRACE, "generate_new_group_log_ success", K_(palf_id), K_(self), K(log_id), K(lsn), K(scn),
              K(valid_log_size), K(is_need_handle), K(is_need_handle_next));
        }
      } else {
        // this log need to be appended to last log
        if (OB_FAIL(append_to_group_log_(lsn, log_id, scn, valid_log_size, buf, buf_len, is_need_handle))) {
          PALF_LOG(WARN, "append_to_group_log_ failed", K(ret), K_(palf_id), K_(self), K(log_id));
        } else if (is_need_handle && FALSE_IT(is_need_handle_next |= is_need_handle)) {
        } else {
          PALF_LOG(TRACE, "append_to_group_log_ success", K_(palf_id), K_(self), K(log_id), K(lsn), K(scn),
              K(valid_log_size), K(is_need_handle), K(is_need_handle_next));
        }
      }
      // inc append count
      const int64_t array_idx = get_itid() & APPEND_CNT_ARRAY_MASK;
      OB_ASSERT(0 <= array_idx && array_idx < APPEND_CNT_ARRAY_SIZE);
      ATOMIC_INC(&append_cnt_array_[array_idx]);

      LSN last_submit_end_lsn, max_flushed_end_lsn;
      get_last_submit_end_lsn_(last_submit_end_lsn);
      get_max_flushed_end_lsn(max_flushed_end_lsn);
      if (max_flushed_end_lsn >= last_submit_end_lsn) {
        // all logs have been flushed, freeze last log in feedback mode
        (void) feedback_freeze_last_log_();
      }
    }
    if (OB_SUCC(ret) && is_need_handle_next) {
      // 这里无法使用log_id作为精确的调用条件，因为上一条日志和本条日志的处理可能并发
      // 比如上一条日志由本日志触发freeze后立即被其他线程处理了，此时本条日志还没fill完成故无法连续处理
      // 那么本条日志需要自己触发handle，这时候prev_log_id是小于本日志log_id的
      // 有了thread lease，这里可以不加log_id条件直接调用
      bool is_committed_lsn_updated = false;
      (void) handle_next_submit_log_(is_committed_lsn_updated);
    }
  }
  return ret;
}

int LogSlidingWindow::try_freeze_prev_log_(const int64_t next_log_id, const LSN &lsn, bool &is_need_handle)
{
  int ret = OB_SUCCESS;
  is_need_handle = false;
  if (OB_INVALID_LOG_ID == next_log_id || !lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(next_log_id), K(lsn));
  } else if (FIRST_VALID_LOG_ID == next_log_id) {
    // prev log_id is 0, skip
    PALF_LOG(INFO, "next log_id is 1, no need freeze prev log", K_(palf_id), K_(self), K(next_log_id), K(lsn));
  } else {
    const int64_t log_id = next_log_id - 1;
    LogTask *log_task = NULL;
    LogTaskGuard guard(this);
    if (OB_FAIL(guard.get_log_task(log_id, log_task))) {
      if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
        // this log has slide out, ignore
        ret = OB_SUCCESS;
      } else {
        PALF_LOG(ERROR, "get_log_task failed", K(ret), K_(palf_id), K_(self), K(log_id));
      }
    } else {
      log_task->lock();
      if (!log_task->is_valid()) {
        // Setting end_lsn for prev log_task, in case it can be freezed later by itself but not here.
        log_task->set_end_lsn(lsn);
        PALF_LOG(INFO, "log_task is invalid, its first log may has not filled, set end_lsn and skip freeze",
            K(ret), K(log_id), K_(palf_id), K_(self), KPC(log_task));
      } else {
        log_task->try_freeze(lsn);
      }
      log_task->unlock();
      // check if this log_task can be submitted
      if (log_task->is_freezed()) {
        log_task->set_freeze_ts(ObTimeUtility::current_time());
        is_need_handle = (0 == log_task->get_ref_cnt()) ? true : false;
      }
    }
  }
  return ret;
}

int LogSlidingWindow::wait_group_buffer_ready_(const LSN &lsn, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  // NB: 尽管已经使用'committed_end_lsn_'限制了'leader_can_submit_new_log_', 但我们依旧需要判断'group_buffer_'是否已经可以复用:
  // 1. 并发提交日志会导致所有日志都能进入到提交流程;
  // 2. 不能够使用'committed_end_lsn_'判断'group_buffer_'是否可以被复用, 因为'committed_end_lsn_'可能大于'max_flushed_end_lsn'.
  int64_t wait_times = 0;
  LSN curr_committed_end_lsn;
  get_committed_end_lsn_(curr_committed_end_lsn);
  while (false == group_buffer_.can_handle_new_log(lsn, data_len, curr_committed_end_lsn)) {
    // 要填充的终点超过了buffer可复用的范围
    // 需要重试直到可复用终点推大
    static const int64_t MAX_SLEEP_US = 100;
    ++wait_times;
    int64_t sleep_us = wait_times * 10;
    if (sleep_us > MAX_SLEEP_US) {
      sleep_us = MAX_SLEEP_US;
    }
    ob_usleep(sleep_us);
    PALF_LOG(WARN, "usleep wait", K_(palf_id), K_(self), K(lsn), K(data_len), K(curr_committed_end_lsn));
    get_committed_end_lsn_(curr_committed_end_lsn);
  }
  return ret;
}

int LogSlidingWindow::append_to_group_log_(const LSN &lsn,
                                           const int64_t log_id,
                                           const SCN &scn,
                                           const int64_t log_entry_size, // log_entry_header + log_data
                                           const char *log_data,
                                           const int64_t data_len,
                                           bool &is_need_handle)
{
  int ret = OB_SUCCESS;
  is_need_handle = false;
  LogTaskGuard guard(this);
  LogTask *log_task = NULL;
  if (!lsn.is_valid() || !scn.is_valid() || OB_INVALID_LOG_ID == log_id || log_entry_size <= 0
      || NULL == log_data || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(lsn), K(scn), K(log_id), K(log_entry_size),
        KP(log_data), K(data_len));
  } else if (OB_FAIL(guard.get_log_task(log_id, log_task))) {
    PALF_LOG(WARN, "get_log_task_ failed", K(ret), K(log_id), K_(palf_id), K_(self));
  } else {
    // Note: 此处无需判断log_task valid, 因为并发submit场景第一条log_entry可能还没更新log_task
    LogEntryHeader log_entry_header;
    // Firstly, we need update log_task info, so that later alloc_log_id() can succeed as soon as possible.
    log_task->inc_update_max_scn(scn);
    log_task->update_data_len(log_entry_size);

    const LSN log_entry_data_lsn = lsn + LogEntryHeader::HEADER_SER_SIZE;
    int64_t pos = 0;
    assert(LogEntryHeader::HEADER_SER_SIZE < TMP_HEADER_SER_BUF_LEN);
    char tmp_buf[TMP_HEADER_SER_BUF_LEN];
    // wait group buffer ready
    if (OB_FAIL(wait_group_buffer_ready_(lsn, log_entry_size))) {
      PALF_LOG(ERROR, "group_buffer wait failed", K(ret), K_(palf_id), K_(self), K(lsn), K(log_entry_size));
    } else if (OB_FAIL(group_buffer_.fill(log_entry_data_lsn, log_data, data_len))) {
      PALF_LOG(ERROR, "fill group buffer failed", K(ret), K_(palf_id), K_(self));
    } else if (OB_FAIL(log_entry_header.generate_header(log_data, data_len, scn))) {
      PALF_LOG(WARN, "genearate header failed", K(ret), K_(palf_id), K_(self));
    } else if (OB_FAIL(log_entry_header.serialize(tmp_buf, TMP_HEADER_SER_BUF_LEN, pos))) {
      PALF_LOG(WARN, "serialize log_entry_header failed", K(ret), K_(palf_id), K_(self));
    } else if (OB_FAIL(group_buffer_.fill(lsn, tmp_buf, pos))) {
      PALF_LOG(ERROR, "fill group buffer failed", K(ret), K_(palf_id), K_(self));
    } else {
      assert(LogEntryHeader::HEADER_SER_SIZE == pos);
      // inc ref by log_entry_size(LOG_HEADER_SIZE + date_len)
      log_task->ref(log_entry_size);
      // check if this log_task can be submitted
      if (log_task->is_freezed()) {
        is_need_handle = (0 == log_task->get_ref_cnt()) ? true : false;
      }
    }
  }
  return ret;
}

int LogSlidingWindow::generate_new_group_log_(const LSN &lsn,
                                              const int64_t log_id,
                                              const SCN &scn,
                                              const int64_t log_body_size,  // log_entry_header_size + log_data_len
                                              const LogType &log_type,
                                              const char *log_data,
                                              const int64_t data_len,
                                              bool &is_need_handle)
{
  int ret = OB_SUCCESS;
  is_need_handle = false;
  LogTaskGuard guard(this);
  LogTask *log_task = NULL;
  if (!lsn.is_valid() || !scn.is_valid()
      || log_body_size <= 0 || OB_INVALID_LOG_ID == log_id
      || LOG_UNKNOWN == log_type
      || (LOG_PADDING != log_type && (NULL == log_data || data_len <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(lsn), K(scn), K(log_id), K(log_body_size),
        K(log_type), KP(log_data), K(data_len));
  } else if (OB_FAIL(guard.get_log_task(log_id, log_task))) {
    PALF_LOG(ERROR, "get_log_task_ failed", K(ret), K(log_id), K_(palf_id), K_(self), "start_log_id", get_start_id(), "max_log_id", get_max_log_id());
  } else {
    LogEntryHeader log_entry_header;
    LogGroupEntryHeader header;
    const int64_t proposal_id = state_mgr_->get_proposal_id();
    const bool is_padding_log = (LOG_PADDING == log_type);

    LogTaskHeaderInfo header_info;
    header_info.begin_lsn_ = lsn;
    header_info.is_padding_log_ = is_padding_log;
    header_info.log_id_ = log_id;
    header_info.min_scn_= scn;
    header_info.max_scn_ = scn;
    header_info.data_len_ = log_body_size;
    header_info.proposal_id_ = proposal_id;
    header_info.is_raw_write_ = false;

    log_task->lock();
    if (log_task->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "log_task is valid, unexpected", K(ret), K(log_id), K_(palf_id), K_(self), K(lsn), K(scn),
          K(log_body_size), K(log_type), K(data_len), KPC(log_task));
    } else if (OB_FAIL(log_task->set_initial_header_info(header_info))) {
      PALF_LOG(WARN, "set_initial_header_info failed", K(ret), K_(palf_id), K_(self), K(log_id), KPC(log_task));
    } else {
      // The first log is responsible to try freezing self, if its end_lsn_ has been set by next log.
      log_task->try_freeze_by_myself();
    }
    log_task->unlock();

    if (OB_SUCC(ret)) {
      const LSN log_entry_data_lsn = lsn + LogGroupEntryHeader::HEADER_SER_SIZE + LogEntryHeader::HEADER_SER_SIZE;
      if (OB_FAIL(wait_group_buffer_ready_(lsn, log_body_size + LogGroupEntryHeader::HEADER_SER_SIZE))) {
        PALF_LOG(ERROR, "group_buffer wait failed", K(ret), K_(palf_id), K_(self));
      } else if (is_padding_log) {
        const int64_t padding_log_body_size = log_body_size - LogEntryHeader::HEADER_SER_SIZE;
        const int64_t padding_valid_data_len = LogEntryHeader::PADDING_LOG_ENTRY_SIZE;
        // padding_valid_data only include LogEntryHeader and ObLogBaseHeader
        // The format like follow:
        // | LogEntryHeader | ObLogBaseHeader|
        // and the format of padding log entry like follow:
        // | LogEntryHeader | ObLogBaseHeader| PADDING_LOG_CONTENT_CHAR |
        // |   32 BYTE      |   16 BYTE      | padding_log_body_size - 48 BYTE |
        char padding_valid_data[padding_valid_data_len];
        memset(padding_valid_data, 0, padding_valid_data_len);
        if (OB_FAIL(LogEntryHeader::generate_padding_log_buf(padding_log_body_size, scn, padding_valid_data, padding_valid_data_len))) {
          PALF_LOG(ERROR, "generate_padding_log_buf failed", K_(palf_id), K_(self), K(padding_valid_data_len),
            K(scn), K(padding_log_body_size));
        }
        // padding log, fill log body with PADDING_LOG_CONTENT_CHAR.
        else if (OB_FAIL(group_buffer_.fill_padding_body(lsn + LogGroupEntryHeader::HEADER_SER_SIZE, padding_valid_data, padding_valid_data_len, log_body_size))) {
          PALF_LOG(WARN, "group_buffer fill_padding_body failed", K(ret), K_(palf_id), K_(self), K(log_body_size));
        } else {
          // inc ref
          log_task->ref(log_body_size);
          const bool set_submit_tag_res = log_task->set_submit_log_exist();
          assert(true == set_submit_tag_res);
        }
      } else {
        int64_t pos = 0;
        assert(LogEntryHeader::HEADER_SER_SIZE < TMP_HEADER_SER_BUF_LEN);
        char tmp_buf[TMP_HEADER_SER_BUF_LEN];
        if (OB_FAIL(group_buffer_.fill(log_entry_data_lsn, log_data, data_len))) {
          PALF_LOG(ERROR, "fill group buffer failed", K(ret), K_(palf_id), K_(self));
        } else if (OB_FAIL(log_entry_header.generate_header(log_data, data_len, scn))) {
          PALF_LOG(WARN, "genearate header failed", K(ret), K_(palf_id), K_(self));
        } else if (OB_FAIL(log_entry_header.serialize(tmp_buf, TMP_HEADER_SER_BUF_LEN, pos))) {
          PALF_LOG(WARN, "serialize log_entry_header failed", K(ret), K_(palf_id), K_(self));
        } else if (OB_FAIL(group_buffer_.fill(lsn + LogGroupEntryHeader::HEADER_SER_SIZE, tmp_buf, pos))) {
          PALF_LOG(ERROR, "fill group buffer failed", K(ret), K_(palf_id), K_(self));
        } else {
          assert(LogEntryHeader::HEADER_SER_SIZE == pos);
          log_task->ref(log_body_size);
          const bool set_submit_tag_res = log_task->set_submit_log_exist();
          assert(true == set_submit_tag_res);
        }
      }
      // check if this log_task can be submitted
      if (log_task->is_freezed()) {
        log_task->set_freeze_ts(ObTimeUtility::current_time());
        is_need_handle = (0 == log_task->get_ref_cnt()) ? true : false;
      }
    }
  }
  return ret;
}

int LogSlidingWindow::handle_committed_log_()
{
  int ret = OB_SUCCESS;
  if (commit_log_handling_lease_.acquire()) {
    do {
      LSN unused_lsn, unused_start_lsn;
      int64_t unused_id = OB_INVALID_LOG_ID;
      LSN committed_end_lsn;
      if (is_all_committed_log_slided_out_(unused_lsn, unused_id, unused_start_lsn, committed_end_lsn)) {
        // all logs have slided out, no need continue
        PALF_LOG(TRACE, "is_all_committed_log_slided_out_ returns true", K_(palf_id), K_(self),
            K(committed_end_lsn));
      } else {
        LSN max_flushed_end_lsn;
        bool need_check_next = true;
        while(OB_SUCC(ret) && need_check_next) {
          need_check_next = false;
          const int64_t tmp_log_id = get_start_id();
          LogTask *log_task = NULL;
          LogTaskGuard guard(this);
          get_max_flushed_end_lsn(max_flushed_end_lsn);
          if (OB_FAIL(guard.get_log_task(tmp_log_id, log_task))) {
            if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
              // this log has slided out, retry
              ret = OB_SUCCESS;
              need_check_next = true;
            }
          } else if (!log_task->is_valid()) {
            // log_task is not valid, end loop
            break;
          } else {
            LogGroupEntryHeader header;
            LSN log_begin_lsn;
            LSN log_end_lsn;
            int64_t data_len = 0;
            LogTaskHeaderInfo log_task_header;
            log_task->lock();
            // Notice: the following lines' order is vital, it should execute is_freezed() firstly.
            // This order can ensure log_end_lsn is correct and decided.
            const bool is_freezed = log_task->is_freezed();
            const int64_t ref_cnt = log_task->get_ref_cnt();
            log_task_header = log_task->get_header_info();
            log_begin_lsn = log_task->get_begin_lsn();
            data_len = log_task->get_data_len();
            log_end_lsn = log_task->get_begin_lsn() + LogGroupEntryHeader::HEADER_SER_SIZE + data_len;
            log_task->unlock();

            PALF_LOG(TRACE, "handle_committed_log", K_(palf_id), K_(self), K(log_end_lsn), K(committed_end_lsn),
                K(max_flushed_end_lsn), K(tmp_log_id), KPC(log_task), K(need_check_next), "can_slide_sw", state_mgr_->can_slide_sw());

            if (is_freezed
                && max_flushed_end_lsn >= log_end_lsn
                && committed_end_lsn >= log_end_lsn
                && state_mgr_->can_slide_sw()) {
              if (log_task->try_pre_slide()) {
                if (OB_FAIL(sw_.slide(PALF_MAX_REPLAY_TIMEOUT, this))) {
                  // slide failed, reset tag
                  (void) log_task->reset_pre_slide();
                  PALF_LOG(ERROR, "sw slide failed", K_(palf_id), K_(self), K(ret), K(tmp_log_id), KPC(log_task));
                } else {
                  // pop successfully, check next log
                  need_check_next = true;
                }
              }
            }
          }
        }
      }
    } while (!commit_log_handling_lease_.revoke());
  }
  return ret;
}

int LogSlidingWindow::try_push_log_to_paxos_follower_(const int64_t curr_proposal_id,
                                                      const int64_t prev_log_pid,
                                                      const LSN &prev_lsn,
                                                      const LSN &lsn,
                                                      const LogWriteBuf &log_write_buf)
{
  int ret = OB_SUCCESS;
  ObMemberList dst_member_list;
  int64_t replica_num = 0;
  const bool need_send_log = (state_mgr_->is_leader_active()) ? true : false;
  if (false == need_send_log) {
    // no need send log to paxos follower
  } else if (OB_FAIL(mm_->get_log_sync_member_list(dst_member_list, replica_num))) {
    PALF_LOG(WARN, "get_log_sync_member_list failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_FAIL(dst_member_list.remove_server(self_))) {
    PALF_LOG(WARN, "dst_member_list remove_server failed", K(ret), K_(palf_id), K_(self));
  } else if (dst_member_list.is_valid()
      && OB_FAIL(log_engine_->submit_push_log_req(dst_member_list, PUSH_LOG, curr_proposal_id,
          prev_log_pid, prev_lsn, lsn, log_write_buf))) {
    PALF_LOG(WARN, "submit_push_log_req failed", K(ret), K_(palf_id), K_(self));
  } else {
    // do nothing
  }
  return ret;
}

int LogSlidingWindow::try_push_log_to_children_(const int64_t curr_proposal_id,
                                                const int64_t prev_log_pid,
                                                const LSN &prev_lsn,
                                                const LSN &lsn,
                                                const LogWriteBuf &log_write_buf)
{
  int ret = OB_SUCCESS;
  LogLearnerList children_list;
  common::GlobalLearnerList degraded_learner_list;
  const bool need_presend_log = (state_mgr_->is_leader_active()) ? true : false;
  if (OB_FAIL(mm_->get_children_list(children_list))) {
    PALF_LOG(WARN, "get_children_list failed", K(ret), K_(palf_id));
  } else if (children_list.is_valid()
      && OB_FAIL(log_engine_->submit_push_log_req(children_list, PUSH_LOG, curr_proposal_id,
          prev_log_pid, prev_lsn, lsn, log_write_buf))) {
    PALF_LOG(WARN, "submit_push_log_req failed", K(ret), K_(palf_id), K_(self));
  } else if (false == need_presend_log) {
  } else if (OB_FAIL(mm_->get_degraded_learner_list(degraded_learner_list))) {
    PALF_LOG(WARN, "get_degraded_learner_list failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_UNLIKELY(degraded_learner_list.is_valid() && mm_->is_sync_to_degraded_learners())) {
    (void) log_engine_->submit_push_log_req(degraded_learner_list, PUSH_LOG,
        curr_proposal_id, prev_log_pid, prev_lsn, lsn, log_write_buf);
  }
  return ret;
}

int LogSlidingWindow::handle_next_submit_log_(bool &is_committed_lsn_updated)
{
  int ret = OB_SUCCESS;
  if (submit_log_handling_lease_.acquire()) {
    do {
      while (OB_SUCC(ret)) {
        LSN last_submit_lsn;
        LSN last_submit_end_lsn;
        int64_t last_submit_log_id = OB_INVALID_LOG_ID;
        int64_t last_submit_log_pid = INVALID_PROPOSAL_ID;
        (void) get_last_submit_log_info_(last_submit_lsn, last_submit_end_lsn, last_submit_log_id, last_submit_log_pid);
        const int64_t tmp_log_id = last_submit_log_id + 1;
        PALF_LOG(TRACE, "handle_next_submit_log_ begin", K(ret), K_(palf_id), K_(self), K(tmp_log_id), K(last_submit_log_id));
        SCN scn;
        LogTask *log_task = NULL;
        LogTaskGuard guard(this);
        if (OB_FAIL(guard.get_log_task(tmp_log_id, log_task))) {
          // get log task failed, exit loop
        } else if (!log_task->is_valid()) {
          // this log is invalid, end loop
          break;
        } else {
          LSN begin_lsn;
          LSN log_end_lsn;
          int64_t log_proposal_id = INVALID_PROPOSAL_ID;
          bool is_need_submit = false;
          bool is_submitted = false;
          // log count of this group log
          int64_t log_cnt = 0;
          int64_t group_log_size = 0;

          log_task->lock();
          // Notice: the following lines' order is vital, it should execute try_pre_submit() firstly.
          // This order can ensure log_end_lsn is correct and decided.
          is_need_submit = log_task->try_pre_submit();
          begin_lsn = log_task->get_begin_lsn();
          log_end_lsn = begin_lsn + LogGroupEntryHeader::HEADER_SER_SIZE + log_task->get_data_len();
          log_proposal_id = log_task->get_proposal_id();
          log_cnt = log_task->get_log_cnt();
          log_task->unlock();

          group_log_size = log_end_lsn - begin_lsn;

          LogGroupEntryHeader group_entry_header;
          int64_t group_log_data_checksum = -1;
          bool is_accum_checksum_acquired = false;
          if (is_need_submit) {
            // generate group_entry_header
            log_task->lock();
            // Check the prev_proposal_id before submit this log.
            // Because there is maybe a gap when this replica receives this log.
            const LSN prev_lsn = log_task->get_prev_lsn();
            const int64_t prev_log_pid = log_task->get_prev_proposal_id();
            log_task->unlock();
            if (OB_UNLIKELY(last_submit_end_lsn != begin_lsn)) {
              ret = OB_ERR_UNEXPECTED;
              PALF_LOG(ERROR, "Current log's begin_lsn is not continuous with last_submit_end_lsn, unexpected",
                  K_(palf_id), K_(self), K(prev_lsn), K(last_submit_log_id), K(last_submit_lsn),
                  K(last_submit_end_lsn), K(last_submit_log_pid), K(tmp_log_id), KPC(log_task));
            } else if (!state_mgr_->is_leader_active()
                && tmp_log_id > FIRST_VALID_LOG_ID  // the first log doesn't need check prev_proposal_id
                && (last_submit_lsn != prev_lsn || last_submit_log_pid != prev_log_pid)) {
              ret = OB_STATE_NOT_MATCH;
              PALF_LOG(WARN, "prev_proposal_id does not match, cannot submit this log", K_(palf_id), K_(self),
                  K(prev_lsn), K(prev_log_pid), K(last_submit_log_id), K(last_submit_lsn), K(last_submit_log_pid),
                  K(tmp_log_id), KPC(log_task));
            } else if (OB_FAIL(generate_group_entry_header_(tmp_log_id, log_task, group_entry_header,
                    group_log_data_checksum, is_accum_checksum_acquired))) {
              PALF_LOG(WARN, "generate_group_entry_header_ failed", K_(palf_id), K_(self));
            } else {
              log_task->lock();
              if (!state_mgr_->is_follower_active()) {
                // Updating data_checksum, accum_checksum, committed_end_lsn for log_task.
                // Active follower can skip because it has done this in receive_log().
                log_task->set_group_log_checksum(group_log_data_checksum);
                if (OB_FAIL(log_task->update_header_info(group_entry_header.get_committed_end_lsn(),
                      group_entry_header.get_accum_checksum()))) {
                  PALF_LOG(WARN, "update_header_info failed", K(ret), K_(palf_id), K_(self), K(group_entry_header));
                }
              }
              scn = log_task->get_min_scn();
              log_task->unlock();
            }
          } else {
            PALF_LOG(TRACE, "cannot submit this log, break loop", K(ret), K_(palf_id), K_(self), K(tmp_log_id), KPC(log_task));
            break;
          }
          // serialize group_entry_header without log_task's lock
          if (OB_SUCC(ret) && is_need_submit) {
            int64_t pos = 0;
            const int64_t curr_proposal_id = state_mgr_->get_proposal_id();
            const int64_t group_entry_size = LogGroupEntryHeader::HEADER_SER_SIZE + group_entry_header.get_data_len();

            FlushLogCbCtx flush_log_cb_ctx;
            flush_log_cb_ctx.log_id_ = tmp_log_id;
            flush_log_cb_ctx.scn_ = scn;
            flush_log_cb_ctx.lsn_ = begin_lsn;
            flush_log_cb_ctx.curr_proposal_id_ = curr_proposal_id;
            flush_log_cb_ctx.log_proposal_id_ = log_proposal_id;
            flush_log_cb_ctx.total_len_ = group_entry_size;
            flush_log_cb_ctx.begin_ts_ = ObTimeUtility::current_time();

            LogWriteBuf log_write_buf;
            assert(LogGroupEntryHeader::HEADER_SER_SIZE < TMP_HEADER_SER_BUF_LEN);
            char tmp_buf[TMP_HEADER_SER_BUF_LEN];
            // Active follower no need serialize group header to group_buffer, it has done this in receive_log().
            const bool need_serialize_header = (state_mgr_->is_follower_active()) ? false : true;
            if (OB_FAIL(need_serialize_header
                        && group_entry_header.serialize(tmp_buf, TMP_HEADER_SER_BUF_LEN, pos))) {
              PALF_LOG(WARN, "serialize log_entry_header failed", K(ret), K_(palf_id), K_(self));
            } else if (need_serialize_header
                       && OB_FAIL(group_buffer_.fill(begin_lsn, tmp_buf, pos))) {
              PALF_LOG(WARN, "fill group buffer failed", K(ret), K_(palf_id), K_(self));
            } else if (OB_FAIL(group_buffer_.get_log_buf(begin_lsn, group_entry_size, log_write_buf))) {
              PALF_LOG(WARN, "get log buffer failed", K(ret), K_(palf_id), K_(self));
            } else {
              // Try push log to follower/children.
              // NB: Sending log before writing to disk, or log_write_buf may be free during sending.
              // Using tmp_ret to avoid handling failure because of rpc exception.
              int tmp_ret = OB_SUCCESS;
              // Push log to paxos follower.
              if (OB_SUCCESS != (tmp_ret = try_push_log_to_paxos_follower_(curr_proposal_id,
                      last_submit_log_pid, last_submit_lsn, begin_lsn, log_write_buf))) {
                PALF_LOG(WARN, "try_push_log_to_paxos_follower_ failed", K(tmp_ret), K_(palf_id), K_(self));
              }
              // Push log to children_list.
              if (OB_SUCCESS != (tmp_ret = try_push_log_to_children_(curr_proposal_id,
                         last_submit_log_pid, last_submit_lsn, begin_lsn, log_write_buf))) {
                PALF_LOG(WARN, "try_push_log_to_children_ failed", K(tmp_ret), K_(palf_id));
              }
            }

            log_task->set_submit_ts(ObTimeUtility::current_time());
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(log_engine_->submit_flush_log_task(flush_log_cb_ctx, log_write_buf))) {
              PALF_LOG(WARN, "submit_flush_log_task failed", K(ret), K_(palf_id), K_(self));
            } else {
              is_submitted = true;
              // statistics info for group log
              const common::ObRole role = state_mgr_->get_role();
              const int64_t total_log_cnt = ATOMIC_AAF(&accum_log_cnt_, log_cnt);
              const int64_t total_group_log_size = ATOMIC_AAF(&accum_group_log_size_, group_log_size);
              if (palf_reach_time_interval(PALF_STAT_PRINT_INTERVAL_US, group_log_stat_time_us_)) {
                const int64_t total_group_log_cnt = tmp_log_id - last_record_group_log_id_;
                if (total_group_log_cnt > 0) {
                  const int64_t avg_log_batch_cnt = total_log_cnt / total_group_log_cnt;
                  const int64_t avg_group_log_size = total_group_log_size / total_group_log_cnt;
                  PALF_LOG(INFO, "[PALF STAT GROUP LOG INFO]", K_(palf_id), K_(self), "role", role_to_string(role),
                      K(total_group_log_cnt), K(avg_log_batch_cnt), K(total_group_log_size), K(avg_group_log_size));
                }
                ATOMIC_STORE(&accum_log_cnt_, 0);
                ATOMIC_STORE(&accum_group_log_size_, 0);
                ATOMIC_STORE(&last_record_group_log_id_, tmp_log_id);
              }
              // submit success, update last_submit_log info
              (void) set_last_submit_log_info_(begin_lsn, log_end_lsn, tmp_log_id, \
                  group_entry_header.get_log_proposal_id());
              if (FOLLOWER == state_mgr_->get_role() || state_mgr_->is_leader_reconfirm()) {
                bool is_need_fetch = false;
                const LSN log_committed_end_lsn = group_entry_header.get_committed_end_lsn();
                try_update_committed_lsn_for_fetch_(log_end_lsn, tmp_log_id, \
                    log_committed_end_lsn, is_need_fetch);
                if (is_need_fetch) {
                  try_fetch_log_streamingly_(log_committed_end_lsn);
                }
                // Advance committed_end_lsn_, follower/reconfirm leader needs
                // the order with try_update_committed_lsn_for_fetch_() is important,
                // this exec order can avoid trigger failure of next round fetch by sliding_cb()
                (void) try_advance_committed_lsn_(log_committed_end_lsn);
                is_committed_lsn_updated = true;
              }
            }
          }
          if (is_need_submit && !is_submitted) {
            // Submitting log failed, reset its tag,
            // this log may need to be truncated later.
            (void) log_task->reset_pre_submit();
            // rollcack accum_checksum
            if (is_accum_checksum_acquired
                && OB_FAIL(checksum_.rollback_accum_checksum(group_entry_header.get_accum_checksum()))) {
              PALF_LOG(ERROR, "rollback_accum_checksum failed", K(ret), K_(palf_id), K_(self), KPC(log_task),
                  K(group_entry_header));
            }
            PALF_LOG(WARN, "submit log failed", K(ret), K_(palf_id), K_(self), KPC(log_task), K(is_accum_checksum_acquired));
          }
          PALF_LOG(TRACE, "handle one submit log", K(ret), K_(palf_id), K_(self), K(tmp_log_id), K(is_committed_lsn_updated),
              K(is_need_submit), K(is_submitted));
        }
      }
    } while (!submit_log_handling_lease_.revoke());
  }
  return ret;
}

int LogSlidingWindow::generate_group_entry_header_(const int64_t log_id,
                                                   LogTask *log_task,
                                                   LogGroupEntryHeader &group_header,
                                                   int64_t &group_log_data_checksum,
                                                   bool &is_accum_checksum_acquired)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_LOG_ID == log_id
      || NULL == log_task) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(log_id), KPC(log_task));
  } else {
    LSN global_committed_end_lsn;
    get_committed_end_lsn_(global_committed_end_lsn);
    LogTaskHeaderInfo header_info;
    log_task->lock();
    header_info = log_task->get_header_info();
    log_task->unlock();
    const bool is_padding_log = header_info.is_padding_log_;
    const bool is_raw_write  = header_info.is_raw_write_;
    const LSN begin_lsn = header_info.begin_lsn_;
    LSN log_committed_end_lsn = header_info.committed_end_lsn_;
    if (!log_committed_end_lsn.is_valid()) {
      // leader生成新日志时会走这个路径
      // follower直接用log_task中的committed_end_lsn_生成group header
      log_committed_end_lsn = global_committed_end_lsn;
    }
    const int64_t data_len = header_info.data_len_;
    const SCN max_scn = header_info.max_scn_;
    const int64_t log_proposal_id = header_info.proposal_id_;
    const int64_t group_entry_size = LogGroupEntryHeader::HEADER_SER_SIZE + data_len;
    LogWriteBuf log_write_buf;
    int64_t accum_checksum = 0;
    if (log_committed_end_lsn > begin_lsn) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "log_committed_end_lsn is larger than begin_lsn", K(ret), K_(palf_id), K_(self), K(global_committed_end_lsn),
          K(header_info));
    } else if (OB_FAIL(group_buffer_.get_log_buf(begin_lsn, group_entry_size, log_write_buf))) {
      PALF_LOG(WARN, "get log buffer failed", K(ret), K_(palf_id), K_(self));
    } else if (OB_FAIL(group_header.generate(is_raw_write, is_padding_log, log_write_buf, data_len, max_scn,
            log_id, log_committed_end_lsn, log_proposal_id, group_log_data_checksum))) {
      PALF_LOG(WARN, "group_header generate failed", K(ret), K_(palf_id), K_(self));
    } else if (OB_FAIL(checksum_.acquire_accum_checksum(group_log_data_checksum, accum_checksum))) {
      PALF_LOG(WARN, "update_accumulated_checksum failed", K(ret), K_(palf_id), K_(self));
    } else {
      // set flag for rollback accum_checksum
      is_accum_checksum_acquired = true;
      (void) group_header.update_accumulated_checksum(accum_checksum);
      (void) group_header.update_header_checksum();
      PALF_LOG(TRACE, "generate_group_entry_header_ success", K(ret), K_(palf_id), K_(self), K(is_padding_log),
          K(is_raw_write), K(group_log_data_checksum), K(group_header), KPC(log_task));
    }
  }
  return ret;
}

int LogSlidingWindow::try_freeze_last_log_task_(const int64_t expected_log_id,
                                                const LSN &expected_end_lsn,
                                                bool &is_need_handle)
{
  int ret = OB_SUCCESS;
  is_need_handle = false;
  if (OB_INVALID_LOG_ID == expected_log_id || !expected_end_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(expected_log_id), K(expected_end_lsn));
  } else {
    LogTask *log_task = NULL;
    LogTaskGuard guard(this);
    if (OB_FAIL(guard.get_log_task(expected_log_id, log_task))) {
      if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
        // this log has slide out, ignore
        ret = OB_SUCCESS;
      } else {
        PALF_LOG(ERROR, "get_log_task failed", K(ret), K_(palf_id), K_(self), K(expected_log_id));
      }
    } else {
      log_task->lock();
      // Current log_end_lsn of log_task is maybe less than expected_end_lsn, because there is maybe some log entry
      // submitting concurrently and it has not been filled into this log_task.
      const LSN log_end_lsn = log_task->get_begin_lsn() + LogGroupEntryHeader::HEADER_SER_SIZE
        + log_task->get_data_len();
      if (!log_task->is_valid()) {
        if (palf_reach_time_interval(1 * 1000 * 1000, cannot_freeze_log_warn_time_)) {
          PALF_LOG(INFO, "this log_task is invalid, cannot freeze", K_(palf_id), K_(self),
              K(expected_log_id), K(expected_end_lsn), KPC(log_task));
        }
      } else if (log_end_lsn > expected_end_lsn) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "last log's end_lsn is larger than expected", K(ret), K_(palf_id), K_(self),
            K(log_end_lsn), K(expected_log_id), K(expected_end_lsn), KPC(log_task));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = log_task->try_freeze(expected_end_lsn))) {
          PALF_LOG(WARN, "try_freeze failed", K(tmp_ret), K(expected_log_id), K_(palf_id), K_(self));
        } else {
          PALF_LOG(TRACE, "try_freeze success", K(ret), K_(palf_id), K_(self), K(expected_log_id), KPC(log_task),
              K(log_end_lsn), K(expected_end_lsn));
        }
      }
      log_task->unlock();
      // check if this log_task can be submitted
      if (log_task->is_freezed()) {
        log_task->set_freeze_ts(ObTimeUtility::current_time());
        is_need_handle = (0 == log_task->get_ref_cnt()) ? true : false;
      }
    }
  }
  return ret;
}

int LogSlidingWindow::feedback_freeze_last_log_()
{
  int ret = OB_SUCCESS;
  LSN last_log_end_lsn;
  int64_t last_log_id = OB_INVALID_LOG_ID;
  bool is_need_handle = false;
  if (FEEDBACK_FREEZE_MODE != freeze_mode_) {
    // Only FEEDBACK_FREEZE_MODE need exec this fucntion
    PALF_LOG(TRACE, "current freeze mode is not feedback", K_(palf_id), K_(self), "freeze_mode", freeze_mode_2_str(freeze_mode_));
  } else if (OB_FAIL(lsn_allocator_.try_freeze(last_log_end_lsn, last_log_id))) {
    PALF_LOG(WARN, "lsn_allocator try_freeze failed", K(ret), K_(palf_id), K_(self), K(last_log_end_lsn), K(last_log_id));
  } else if (last_log_id <= 0) {
    // no log, no need freeze
  } else if (OB_FAIL(try_freeze_last_log_task_(last_log_id, last_log_end_lsn, is_need_handle))) {
    PALF_LOG(WARN, "try_freeze_last_log_task_ failed", K(ret), K_(palf_id), K_(self), K(last_log_id), K(last_log_end_lsn));
  } else {
    bool is_committed_lsn_updated = false;
    (void) handle_next_submit_log_(is_committed_lsn_updated);
    (void) handle_committed_log_();
  }
  return ret;
}

bool LogSlidingWindow::is_in_period_freeze_mode() const
{
  return (PERIOD_FREEZE_MODE == freeze_mode_);
}

int LogSlidingWindow::check_and_switch_freeze_mode()
{
  int ret = OB_SUCCESS;
  int64_t total_append_cnt = 0;
  for (int i = 0; i < APPEND_CNT_ARRAY_SIZE; ++i) {
    total_append_cnt += ATOMIC_LOAD(&append_cnt_array_[i]);
    ATOMIC_STORE(&append_cnt_array_[i], 0);
  }
  if (FEEDBACK_FREEZE_MODE == freeze_mode_) {
    if (total_append_cnt >= APPEND_CNT_LB_FOR_PERIOD_FREEZE) {
      freeze_mode_ = PERIOD_FREEZE_MODE;
      PALF_LOG(INFO, "switch freeze_mode to period", K_(palf_id), K_(self), K(total_append_cnt));
    }
  } else if (PERIOD_FREEZE_MODE == freeze_mode_) {
    if (total_append_cnt < APPEND_CNT_LB_FOR_PERIOD_FREEZE) {
      freeze_mode_ = FEEDBACK_FREEZE_MODE;
      PALF_LOG(INFO, "switch freeze_mode to feedback", K_(palf_id), K_(self), K(total_append_cnt));
      (void) feedback_freeze_last_log_();
    }
  } else {}
  PALF_LOG(TRACE, "finish check_and_switch_freeze_mode", K_(palf_id), K_(self), K(total_append_cnt), "freeze_mode", freeze_mode_2_str(freeze_mode_));
  return ret;
}

int LogSlidingWindow::period_freeze_last_log()
{
  int ret = OB_SUCCESS;
  LSN last_log_end_lsn;
  int64_t last_log_id = OB_INVALID_LOG_ID;
  bool is_need_handle = false;
  if (PERIOD_FREEZE_MODE != freeze_mode_) {
    // Only PERIOD_FREEZE_MODE need exec this fucntion
    PALF_LOG(TRACE, "current freeze mode is not period", K_(palf_id), K_(self), "freeze_mode", freeze_mode_2_str(freeze_mode_));
  } else if (OB_FAIL(lsn_allocator_.try_freeze(last_log_end_lsn, last_log_id))) {
    PALF_LOG(WARN, "lsn_allocator try_freeze failed", K(ret), K_(palf_id), K_(self), K(last_log_end_lsn), K(last_log_id));
  } else if (last_log_id <= 0) {
    // no log, no need freeze
  } else if (OB_FAIL(try_freeze_last_log_task_(last_log_id, last_log_end_lsn, is_need_handle))) {
    PALF_LOG(WARN, "try_freeze_last_log_task_ failed", K(ret), K_(palf_id), K_(self), K(last_log_id), K(last_log_end_lsn));
  } else {
  }
  if (get_max_log_id() > get_last_submit_log_id_()) {
    // try handle next submit log
    bool is_committed_lsn_updated = false;
    (void) handle_next_submit_log_(is_committed_lsn_updated);
  }
  // handle committed log periodically
  // because committed_end_lsn may be advanced during reconfirm,
  // so there is maybe some log that can be slid in sw.
  (void) handle_committed_log_();
  return ret;
}

int LogSlidingWindow::after_rebuild(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_rebuilding_ = false;
    last_rebuild_lsn_.reset();
    LSN committed_end_lsn;
    get_committed_end_lsn_(committed_end_lsn);
    if (lsn >= committed_end_lsn) {
      (void) try_advance_committed_lsn_(lsn);
    }
  }
  return ret;
}

int LogSlidingWindow::after_truncate(const TruncateLogCbCtx &truncate_cb_ctx)
{
  // Caller holds palf_handle_impl's wrlock.
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "after_truncate begin", K_(palf_id), K_(self), K(truncate_cb_ctx));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!truncate_cb_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(truncate_cb_ctx));
  } else if (!is_truncating_
             || last_truncate_lsn_ != truncate_cb_ctx.lsn_) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "state not match", K(ret), K_(palf_id), K_(self), K(truncate_cb_ctx),
        K_(is_truncating), K_(last_truncate_lsn));
  } else {
    // Set reuse_lsn here, because it may be advanced larger than last_truncate_lsn_ during flush cb.
    // New logs beyond last_truncate_lsn_ won't be processed before after_truncate called,
    // which ensures correctness.
    (void) group_buffer_.truncate(last_truncate_lsn_);
    last_truncate_lsn_.reset();
    is_truncating_ = false;
    PALF_LOG(INFO, "after_truncate success", K_(palf_id), K_(self), K(truncate_cb_ctx));
  }
  return ret;
}

int LogSlidingWindow::after_flush_log(const FlushLogCbCtx &flush_cb_ctx)
{
  int ret = OB_SUCCESS;
  bool can_exec_cb = false;
  const int64_t log_id = flush_cb_ctx.log_id_;
  const LSN log_end_lsn = flush_cb_ctx.lsn_ + flush_cb_ctx.total_len_;
  const int64_t cb_begin_ts = ObTimeUtility::current_time();
  PALF_LOG(TRACE, "after_flush_log begin", K_(palf_id), K_(self), K(flush_cb_ctx));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!flush_cb_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(flush_cb_ctx));
  } else if (is_truncating_ && log_end_lsn > last_truncate_lsn_) {
    // truncate位点之后的日志无需执行回调
    PALF_LOG(INFO, "this log has been truncated, no need execute flush cb", K(ret), K_(palf_id), K_(self),
        K(flush_cb_ctx), K(log_end_lsn), K_(last_truncate_lsn));
  } else if (is_rebuilding_ && log_end_lsn > last_rebuild_lsn_) {
    // rebuild 位点之后的日志无需执行回调, 否则会错误更新 max_flushed_end_lsn
    PALF_LOG(INFO, "this replica is rebuilding, no need execute flush cb", K(ret), K_(palf_id), K_(self),
        K(flush_cb_ctx), K(log_end_lsn), K_(last_truncate_lsn));
  } else if (state_mgr_->get_proposal_id() != flush_cb_ctx.curr_proposal_id_) {
    // If curr_proposal_id_ has changed during flushing log, we need check log_proposal_id_ further.
    LogTask *log_task = NULL;
    LogTaskGuard guard(this);
    if (OB_FAIL(guard.get_log_task(log_id, log_task))) {
      PALF_LOG(WARN, "get_log_task failed", K(ret), K(log_id), K_(palf_id), K_(self));
    } else {
      log_task->lock();
      if (!log_task->is_valid()) {
        can_exec_cb = false;
        PALF_LOG(WARN, "log_task is invalid, it may be truncated, skip flush_cb", K_(palf_id), K_(self),
            K(log_id), KPC(log_task));
      } else if (log_task->get_proposal_id() != flush_cb_ctx.log_proposal_id_) {
        ret = OB_STATE_NOT_MATCH;
        PALF_LOG(WARN, "proposal_id of log_task has changed", K(ret), K_(palf_id), K_(self), K(flush_cb_ctx),
            KPC(log_task));
      } else {
        // log_proposal_id_ matches, can exec flush_cb
        can_exec_cb = true;
        // update log_task's flushed_ts
        log_task->set_flushed_ts(cb_begin_ts);
      }
      log_task->unlock();
    }
  } else {
    can_exec_cb = true;
    // update log_task's flushed_ts
    LogTask *log_task = NULL;
    LogTaskGuard guard(this);
    if (OB_FAIL(guard.get_log_task(log_id, log_task))) {
      PALF_LOG(WARN, "get_log_task failed", K(ret), K(log_id), K_(palf_id), K_(self));
    } else {
      log_task->set_flushed_ts(cb_begin_ts);
    }
  }

  common::ObTimeGuard time_guard("after flush log", 100 * 1000);
  if (OB_SUCC(ret) && can_exec_cb) {
    (void) inc_update_max_flushed_log_info_(flush_cb_ctx.lsn_, log_end_lsn, flush_cb_ctx.log_proposal_id_);
    if (LEADER == state_mgr_->get_role()) {
      // leader update match_lsn_map_
      (void) try_update_match_lsn_map_(self_, log_end_lsn);
    }
    time_guard.click();
    if (state_mgr_->is_leader_active()) {
      // leader try update committed_end_lsn
      // leader cannot update commit_offset before writing start_working log successfully during reconfirm
      LSN new_committed_end_lsn;
      (void) gen_committed_end_lsn_(new_committed_end_lsn);
    } else if (FOLLOWER == state_mgr_->get_role()) {
      // follower need send ack to leader
      const ObAddr &leader = (state_mgr_->get_leader().is_valid())? \
          state_mgr_->get_leader(): state_mgr_->get_broadcast_leader();
      // flush op for different role
      if (!leader.is_valid()) {
        PALF_LOG(TRACE, "current leader is invalid, cannot send ack", K(ret), K_(palf_id), K_(self),
            K(flush_cb_ctx), K(log_end_lsn), K(leader));
      } else if (OB_FAIL(submit_push_log_resp_(leader, flush_cb_ctx.curr_proposal_id_, log_end_lsn))) {
        PALF_LOG(WARN, "submit_push_log_resp failed", K(ret), K_(palf_id), K_(self), K(leader), K(flush_cb_ctx));
      } else {}
    } else {}

    time_guard.click("before handle log");

    if (OB_SUCC(ret)) {
      const int64_t last_submit_log_id = get_last_submit_log_id_();
      if (log_id == last_submit_log_id) {
        // 基于log_id连续性条件触发后续日志处理
        // feedback mode下尝试冻结后面的log
        (void) feedback_freeze_last_log_();
        // 非feedback mode需触发handle next log
        bool is_committed_lsn_updated = false;
        (void) handle_next_submit_log_(is_committed_lsn_updated);
      }
      time_guard.click("after handle next log");
      // Both leader and follower need handle committed logs here.
      (void) handle_committed_log_();
      time_guard.click("after handle committed log");
    }
  }
  PALF_LOG(TRACE, "sw after_flush_log success", K(ret), K_(palf_id), K_(self), K(can_exec_cb), K(flush_cb_ctx),
      K_(max_flushed_lsn), K_(max_flushed_end_lsn), K(time_guard));
  return ret;
}

int LogSlidingWindow::get_last_submit_log_info(LSN &last_submit_lsn,
    int64_t &log_id, int64_t &log_proposal_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LSN unused_lsn;
    get_last_submit_log_info_(last_submit_lsn, unused_lsn, log_id, log_proposal_id);
  }
  return ret;
}

int LogSlidingWindow::get_last_submit_log_info(LSN &last_submit_lsn,
    LSN &last_submit_end_lsn, int64_t &log_id, int64_t &log_proposal_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    get_last_submit_log_info_(last_submit_lsn, last_submit_end_lsn, log_id, log_proposal_id);
  }
  return ret;
}

int64_t LogSlidingWindow::get_last_submit_log_id_() const
{
  ObSpinLockGuard guard(last_submit_info_lock_);
  return last_submit_log_id_;
}

void LogSlidingWindow::get_last_submit_end_lsn_(LSN &end_lsn) const
{
  end_lsn.val_ = ATOMIC_LOAD(&last_submit_end_lsn_.val_);
}

void LogSlidingWindow::get_last_submit_log_info_(LSN &lsn, LSN &end_lsn,
    int64_t &log_id, int64_t &log_proposal_id) const
{
  ObSpinLockGuard guard(last_submit_info_lock_);
  lsn = last_submit_lsn_;
  end_lsn = last_submit_end_lsn_;
  log_id = last_submit_log_id_;
  log_proposal_id = last_submit_log_pid_;
}

int LogSlidingWindow::get_max_flushed_log_info(LSN &lsn,
                                               LSN &end_lsn,
                                               int64_t &log_proposal_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = get_max_flushed_log_info_(lsn, end_lsn, log_proposal_id);
  }
  return ret;
}

void LogSlidingWindow::get_max_flushed_end_lsn(LSN &end_lsn) const
{
  end_lsn.val_ = ATOMIC_LOAD(&max_flushed_end_lsn_.val_);
}

int LogSlidingWindow::get_max_flushed_log_info_(LSN &lsn,
                                                LSN &end_lsn,
                                                int64_t &log_proposal_id) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(max_flushed_info_lock_);
  lsn = max_flushed_lsn_;
  end_lsn = max_flushed_end_lsn_;
  log_proposal_id = max_flushed_log_pid_;
  return ret;
}

int LogSlidingWindow::get_last_slide_end_lsn(LSN &out_end_lsn) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    get_last_slide_end_lsn_(out_end_lsn);
  }
  return ret;
}

int64_t LogSlidingWindow::get_last_slide_log_id() const
{
  return ATOMIC_LOAD(&last_slide_log_id_);
}

int64_t LogSlidingWindow::get_last_slide_log_id_() const
{
  return ATOMIC_LOAD(&last_slide_log_id_);
}

const SCN LogSlidingWindow::get_last_slide_scn() const
{
  return last_slide_scn_;
}

void LogSlidingWindow::get_last_slide_end_lsn_(LSN &out_end_lsn) const
{
  int64_t last_slide_log_id = OB_INVALID_LOG_ID;
  SCN last_slide_scn;
  LSN last_slide_lsn;
  LSN last_slide_end_lsn;
  int64_t unused_pid = INVALID_PROPOSAL_ID;
  int64_t last_slide_accum_checksum = -1;
  get_last_slide_log_info_(last_slide_log_id, last_slide_scn, \
          last_slide_lsn, last_slide_end_lsn, unused_pid, last_slide_accum_checksum);
  out_end_lsn = last_slide_end_lsn;
}

void LogSlidingWindow::get_last_slide_log_info_(int64_t &log_id,
                                                SCN &scn,
                                                LSN &lsn,
                                                LSN &end_lsn,
                                                int64_t &log_proposal_id,
                                                int64_t &accum_checksum) const
{
  ObSpinLockGuard guard(last_slide_info_lock_);
  log_id = last_slide_log_id_;
  scn = last_slide_scn_;
  lsn = last_slide_lsn_;
  end_lsn = last_slide_end_lsn_;
  log_proposal_id = last_slide_log_pid_;
  accum_checksum = last_slide_log_accum_checksum_;
}

int LogSlidingWindow::set_last_submit_log_info_(const LSN &lsn,
                                                const LSN &end_lsn,
                                                const int64_t log_id,
                                                const int64_t &log_proposal_id)
{
  int ret = OB_SUCCESS;
  if (!lsn.is_valid() || !end_lsn.is_valid() || OB_INVALID_LOG_ID == log_id
      || (INVALID_PROPOSAL_ID == log_proposal_id && PALF_INITIAL_LSN_VAL < end_lsn.val_)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(lsn), K(end_lsn), K(log_id), K(log_proposal_id));
  } else {
    ObSpinLockGuard guard(last_submit_info_lock_);
    const int64_t old_submit_log_id = last_submit_log_id_;
    last_submit_lsn_ = lsn;
    ATOMIC_STORE(&last_submit_end_lsn_.val_, end_lsn.val_);
    last_submit_log_id_ = log_id;
    last_submit_log_pid_ = log_proposal_id;
    PALF_LOG(TRACE, "set_last_submit_log_info_ success", K_(palf_id), K_(self), K(old_submit_log_id), K(lsn), K(log_id), \
        K(log_proposal_id));
  }
  return ret;
}

int LogSlidingWindow::try_update_last_slide_log_info_(
    const int64_t log_id,
    const SCN &scn,
    const LSN &lsn,
    const LSN &end_lsn,
    const int64_t &proposal_id,
    const int64_t accum_checksum)
{
  int ret = OB_SUCCESS;
  if (!lsn.is_valid() ||
      !end_lsn.is_valid() ||
      INVALID_PROPOSAL_ID == proposal_id ||
      OB_INVALID_LOG_ID == log_id ||
      !scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(lsn), K(end_lsn), K(proposal_id), K(log_id), K(scn));
  } else {
    ObSpinLockGuard guard(last_slide_info_lock_);
    ATOMIC_STORE(&last_slide_log_id_, log_id);
    last_slide_scn_ = scn;
    last_slide_lsn_ = lsn;
    last_slide_end_lsn_ = end_lsn;
    last_slide_log_pid_ = proposal_id;
    last_slide_log_accum_checksum_ = accum_checksum;
    PALF_LOG(TRACE, "try_update_last_slide_log_info_ success", K_(palf_id), K_(self), K(log_id), K(scn),
        K(lsn), K(end_lsn), K(proposal_id), K(accum_checksum));
  }
  return ret;
}

int LogSlidingWindow::try_advance_committed_end_lsn(const LSN &end_lsn)
{
  return try_advance_committed_lsn_(end_lsn);
}

int LogSlidingWindow::try_advance_committed_lsn_(const LSN &end_lsn)
{
  int ret = OB_SUCCESS;
  if (!end_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K_(palf_id), K_(self), K(end_lsn));
  } else {
    LSN old_committed_end_lsn;
    get_committed_end_lsn_(old_committed_end_lsn);
    while (end_lsn > old_committed_end_lsn) {
      if (ATOMIC_BCAS(&committed_end_lsn_.val_, old_committed_end_lsn.val_, end_lsn.val_)) {
        break;
      } else {
        get_committed_end_lsn_(old_committed_end_lsn);
      }
    }
    PALF_LOG(TRACE, "try_advance_committed_lsn_ success", K_(palf_id), K_(self), K_(committed_end_lsn));
    if (palf_reach_time_interval(PALF_STAT_PRINT_INTERVAL_US, end_lsn_stat_time_us_)) {
      LSN curr_end_lsn;
      get_committed_end_lsn_(curr_end_lsn);
      PALF_LOG(INFO, "[PALF STAT COMMITTED LOG SIZE]", K_(palf_id), K_(self), "committed size", curr_end_lsn.val_ - last_record_end_lsn_.val_);
      last_record_end_lsn_ = curr_end_lsn;
    }
  }
  return ret;
}

int LogSlidingWindow::inc_update_scn_base(const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(lsn_allocator_.inc_update_scn_base(scn))) {
    PALF_LOG(WARN, "inc_update_scn_base failed", K(ret), K_(palf_id), K_(self), K(scn));
  }
  return ret;
}

int LogSlidingWindow::inc_update_max_flushed_log_info_(const LSN &lsn,
                                                       const LSN &end_lsn,
                                                       const int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  LSN curr_max_flushed_end_lsn;
  get_max_flushed_end_lsn(curr_max_flushed_end_lsn);
  if (!lsn.is_valid() || !end_lsn.is_valid() || INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K_(palf_id), K_(self), K(lsn), K(end_lsn), K(proposal_id));
  } else if (curr_max_flushed_end_lsn.is_valid() && curr_max_flushed_end_lsn >= end_lsn) {
    // no need update max_flushed_end_lsn_
  } else {
    WLockGuard guard(max_flushed_info_lock_);
    // double check
    if (max_flushed_end_lsn_.is_valid() && max_flushed_end_lsn_ >= end_lsn) {
      PALF_LOG(WARN, "arg end lsn is not larger than current, no need update", K_(palf_id), K_(self),
          K_(max_flushed_lsn), K_(max_flushed_end_lsn), K(lsn), K(end_lsn), K(proposal_id));
    } else {
      max_flushed_lsn_ = lsn;
      ATOMIC_STORE(&max_flushed_end_lsn_.val_, end_lsn.val_);
      max_flushed_log_pid_ = proposal_id;
      PALF_LOG(TRACE, "inc_update_max_flushed_log_info_ success", K_(palf_id), K_(self), K(lsn), K(end_lsn),
          K(proposal_id), K(max_flushed_end_lsn_));
    }
  }
  return ret;
}

int LogSlidingWindow::truncate_max_flushed_log_info_(const LSN &lsn,
                                                     const LSN &end_lsn,
                                                     const int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  if (!lsn.is_valid()
      || !end_lsn.is_valid()
      || (INVALID_PROPOSAL_ID == proposal_id && PALF_INITIAL_LSN_VAL < end_lsn.val_)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(lsn), K(end_lsn), K(proposal_id));
  } else {
    WLockGuard guard(max_flushed_info_lock_);
    max_flushed_lsn_ = lsn;
    max_flushed_end_lsn_ = end_lsn;
    max_flushed_log_pid_ = proposal_id;
    PALF_LOG(INFO, "truncate_max_flushed_log_info_ success", K_(palf_id), K_(self), K(lsn), K(end_lsn), K(proposal_id));
  }
  return ret;
}

void LogSlidingWindow::get_last_fetch_info_(LSN &last_fetch_end_lsn,
    LSN &last_committed_end_lsn, int64_t &last_fetch_max_log_id) const
{
  ObSpinLockGuard guard(fetch_info_lock_);
  last_fetch_end_lsn = last_fetch_end_lsn_;
  last_committed_end_lsn = last_fetch_committed_end_lsn_;
  last_fetch_max_log_id = last_fetch_max_log_id_;
}

void LogSlidingWindow::try_reset_last_fetch_log_info_(const LSN &expected_end_lsn, const int64_t log_id)
{
  ObSpinLockGuard guard(fetch_info_lock_);
  if (expected_end_lsn.is_valid() && expected_end_lsn != last_fetch_end_lsn_) {
    PALF_LOG(INFO, "last_fetch_end_lsn_ has changed, skip reset", K_(palf_id), K_(self),
        K(expected_end_lsn), K_(last_fetch_end_lsn));
  } else if (log_id <= last_fetch_max_log_id_) {
    // If it receives push log whose end_lsn AND log_id is less than or equal to last_fetch_info,
    // which means it can receive other logs from leader, so reset last fetch info.
    last_fetch_req_time_ = 0;
    last_fetch_end_lsn_.reset();
    last_fetch_committed_end_lsn_.reset();
    last_fetch_max_log_id_ = OB_INVALID_LOG_ID;
    last_fetch_trigger_type_ = FetchTriggerType::LOG_LOOP_TH;
    PALF_LOG(INFO, "reset last fetch log info", K_(palf_id), K_(self), K(expected_end_lsn));
  } else {
    // do nothing
  }
}

void LogSlidingWindow::try_update_committed_lsn_for_fetch_(
    const LSN &log_end_lsn,
    const int64_t &log_id,
    const LSN &log_committed_end_lsn,
    bool &is_need_fetch)
{
  bool need_update = false;
  LSN last_fetch_end_lsn;
  LSN last_committed_end_lsn;
  last_fetch_end_lsn.val_ = ATOMIC_LOAD(&last_fetch_end_lsn_.val_);
  last_committed_end_lsn.val_ = ATOMIC_LOAD(&last_fetch_committed_end_lsn_.val_);
  int64_t last_fetch_max_log_id = OB_INVALID_LOG_ID;
  get_last_fetch_info_(last_fetch_end_lsn, last_committed_end_lsn, last_fetch_max_log_id);
  if (!last_fetch_end_lsn.is_valid() || last_committed_end_lsn.is_valid()) {
    // no need update
  } else {
    if (last_fetch_end_lsn.is_valid()
        && !last_committed_end_lsn.is_valid()
        && (log_end_lsn >= last_fetch_end_lsn || log_id == last_fetch_max_log_id)) {
      // 本条日志是上一轮fetch的最后一条，更新last_fetch_committed_end_lsn_
      // 用于后续触发流式fetch
      need_update = true;
    }
  }

  if (need_update) {
    ObSpinLockGuard guard(fetch_info_lock_);
    if (last_fetch_committed_end_lsn_.is_valid()) {
      PALF_LOG(INFO, "last_fetch_committed_end_lsn_ has been set, skip update", K_(palf_id), K_(self),
          K(log_id), K_(last_fetch_max_log_id), K_(last_fetch_end_lsn),
          K_(last_fetch_committed_end_lsn));
    } else if (last_fetch_end_lsn == last_fetch_end_lsn_
               || last_fetch_max_log_id == last_fetch_max_log_id_) {
      LSN committed_end_lsn;
      get_committed_end_lsn_(committed_end_lsn);
      // The order is fatal:
      // 1) update last_fetch_committed_end_lsn_
      // 2) check last slide end_lsn to decide fetching log streamingly or not.
      ATOMIC_STORE(&last_fetch_committed_end_lsn_.val_, log_committed_end_lsn.val_);
      MEM_BARRIER();
      // The committed_end_lsn may already be updated at least to this value by previous group log.
      // And the logs before log_committed_end_lsn have been slid out.
      // For this scenario, it need triger fetch log streamingly now.
      LSN last_slide_end_lsn;
      get_last_slide_end_lsn_(last_slide_end_lsn);
      if (committed_end_lsn >= log_committed_end_lsn
          && last_slide_end_lsn >= log_committed_end_lsn) {
        is_need_fetch = true;
      }
      PALF_LOG(INFO, "update last fetch log info", K_(palf_id), K_(self), K(last_fetch_end_lsn), K(log_id),
          K_(last_fetch_end_lsn), K_(last_fetch_committed_end_lsn), K(committed_end_lsn), K(last_slide_end_lsn));
    } else {
      PALF_LOG(INFO, "last_fetch_max_log_id_ has changed, skip update", K_(palf_id), K_(self),
          K(last_fetch_end_lsn), K(log_id), K_(last_fetch_max_log_id), K_(last_fetch_end_lsn),
          K_(last_fetch_committed_end_lsn));
    }
  }
}

bool LogSlidingWindow::need_execute_fetch_(const FetchTriggerType &fetch_trigger_type)
{
  bool bool_ret = true;
  const int64_t now = ObTimeUtility::current_time();
  if (FetchTriggerType::SLIDING_CB == last_fetch_trigger_type_
      && (FetchTriggerType::ADD_MEMBER_PRE_CHECK == fetch_trigger_type
          || FetchTriggerType::MODE_META_BARRIER == fetch_trigger_type
          || FetchTriggerType::LEARNER_REGISTER == fetch_trigger_type
          || FetchTriggerType::RECONFIRM_NOTIFY_FETCH == fetch_trigger_type)) {
    // If self is currently in streamingly fetch state, it does not need fetch again
    // in some trigger cases.
    bool_ret = false;
  } else if (now - last_fetch_req_time_ < PALF_FETCH_LOG_OUTER_TRIGGER_INTERVAL_US
      && (FetchTriggerType::ADD_MEMBER_PRE_CHECK == fetch_trigger_type
          || FetchTriggerType::MODE_META_BARRIER == fetch_trigger_type
          || FetchTriggerType::LEARNER_REGISTER == fetch_trigger_type)) {
    // Prevent config pre check generating fetch req too frequently.
    bool_ret = false;
  } else {}
  return bool_ret;
}

int LogSlidingWindow::try_fetch_log(const FetchTriggerType &fetch_log_type,
                                    const LSN prev_lsn,
                                    const LSN fetch_start_lsn,
                                    const int64_t fetch_start_log_id)
{
  int ret = OB_SUCCESS;
  ObAddr fetch_log_dst;
  const bool all_valid = prev_lsn.is_valid() && fetch_start_lsn.is_valid() && is_valid_log_id(fetch_start_log_id);
  const bool all_invalid = !prev_lsn.is_valid() && !fetch_start_lsn.is_valid() && !is_valid_log_id(fetch_start_log_id);
  // default fetch_log_size is assigned by current group_buffer size
  const int64_t fetch_log_size = group_buffer_.get_available_buffer_size();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!all_valid && !all_invalid) {
    // require argument are all valid or all invalid
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K_(self), K(fetch_log_type), K(prev_lsn), K(fetch_start_lsn), K(fetch_start_log_id));
  } else if (OB_FAIL(get_fetch_log_dst_(fetch_log_dst))
      || !fetch_log_dst.is_valid()
      || fetch_log_dst == self_) {
    if (palf_reach_time_interval(5 * 1000 * 1000, fetch_failure_print_time_)) {
      PALF_LOG(WARN, "get_fetch_log_dst failed or invalid", K(ret), K_(palf_id), K_(self), K(fetch_log_dst));
    }
  } else if (false == need_execute_fetch_(fetch_log_type)) {
    if (palf_reach_time_interval(5 * 1000 * 1000, fetch_failure_print_time_)) {
      PALF_LOG(INFO, "no need execute fetch", K(ret), K_(palf_id), K_(self), K(fetch_log_type),
          "last_fetch_trigger_type", fetch_trigger_type_2_str(last_fetch_trigger_type_), K_(last_fetch_req_time));
    }
  } else if (FetchTriggerType::MODE_META_BARRIER == fetch_log_type) {
    int64_t last_slide_log_id = OB_INVALID_LOG_ID;
    SCN last_slide_scn;
    LSN last_slide_lsn;
    LSN last_slide_end_lsn;
    int64_t last_slide_log_pid = INVALID_PROPOSAL_ID;
    int64_t last_slide_accum_checksum = -1;
    get_last_slide_log_info_(last_slide_log_id, last_slide_scn, last_slide_lsn, \
        last_slide_end_lsn, last_slide_log_pid, last_slide_accum_checksum);
    if (OB_FAIL(do_fetch_log_(fetch_log_type, fetch_log_dst, last_slide_lsn, \
          last_slide_end_lsn, fetch_log_size, last_slide_log_id + 1))) {
      PALF_LOG(WARN, "do_fetch_log_ failed", K(ret), K_(palf_id), K_(self), K(fetch_log_type), K(fetch_log_dst),
          K(last_slide_lsn), K(last_slide_end_lsn), K(last_slide_log_id));
    }
  } else if (all_valid) {
    // use assigned arguments
    if (OB_FAIL(do_fetch_log_(fetch_log_type, fetch_log_dst, prev_lsn, \
          fetch_start_lsn, fetch_log_size, fetch_start_log_id))) {
      PALF_LOG(WARN, "do_fetch_log_ failed", K(ret), K_(palf_id), K_(self), K(fetch_log_type), K(fetch_log_dst),
          K(prev_lsn), K(fetch_start_lsn), K(fetch_start_log_id));
    }
  } else if (all_invalid) {
    // generate default arguments
    // 本接口由state_mgr调用，sw左边界不滑动时周期性触发fetch log
    // 触发前需满足所有committed logs都已滑出, 并以committed_end_lsn作为新的fetch起点
    LSN last_slide_lsn;
    int64_t last_slide_log_id;
    LSN sw_start_lsn;
    LSN committed_end_lsn;
    if (!is_all_committed_log_slided_out_(last_slide_lsn, last_slide_log_id, sw_start_lsn, committed_end_lsn)) {
      if (palf_reach_time_interval(1 * 1000 * 1000, cannot_fetch_log_warn_time_)) {
        PALF_LOG(WARN, "is_all_committed_log_slided_out_ return false, cannot fetch log now", K(ret),
            K_(palf_id), K_(self), K(committed_end_lsn));
      }
    } else if (OB_FAIL(do_fetch_log_(fetch_log_type, fetch_log_dst, last_slide_lsn, \
          sw_start_lsn, fetch_log_size, last_slide_log_id + 1))) {
      PALF_LOG(WARN, "do_fetch_log_ failed", K(ret), K_(palf_id), K_(self), K(fetch_log_type), K(fetch_log_dst),
          K(last_slide_lsn), K(sw_start_lsn), K(committed_end_lsn), K(last_slide_log_id));
    }
  }
  PALF_LOG(TRACE, "runlin trace try_fetch_log", K(ret), K(all_valid), K(all_invalid));
  return ret;
}

int LogSlidingWindow::try_fetch_log_for_reconfirm(const common::ObAddr &dest, const LSN &fetch_end_lsn, bool &is_fetched)
{
  int ret = OB_SUCCESS;
  LSN prev_lsn, sw_start_lsn;
  int64_t prev_log_id = OB_INVALID_LOG_ID;
  LSN committed_end_lsn;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!dest.is_valid() || !fetch_end_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(dest), K(fetch_end_lsn));
  } else if (!is_all_committed_log_slided_out_(prev_lsn, prev_log_id, sw_start_lsn, committed_end_lsn)) {
    if (palf_reach_time_interval(1 * 1000 * 1000, cannot_fetch_log_warn_time_)) {
      PALF_LOG(WARN, "is_all_committed_log_slided_out_ return false, cannot fetch log now", K(ret),
          K_(palf_id), K_(self), K(committed_end_lsn));
    }
  } else {
    reconfirm_fetch_dest_ = dest;
    const int64_t fetch_log_size = MIN(fetch_end_lsn - committed_end_lsn, group_buffer_.get_available_buffer_size());
    if (OB_FAIL(do_fetch_log_(FetchTriggerType::LEADER_RECONFIRM, dest, prev_lsn, \
            committed_end_lsn, fetch_log_size, prev_log_id + 1))) {
      PALF_LOG(WARN, "do_fetch_log_ failed", K(ret), K_(palf_id), K_(self));
    } else {
      is_fetched = true;
    }
  }
  return ret;
}

int LogSlidingWindow::do_fetch_log_(const FetchTriggerType &trigger_type,
                                    const common::ObAddr &dest,
                                    const LSN &prev_lsn,
                                    const LSN &fetch_start_lsn,
                                    const int64_t fetch_log_size,
                                    const int64_t fetch_start_log_id)
{
  int ret = OB_SUCCESS;
  if (false == state_mgr_->is_sync_enabled()) {
    if (palf_reach_time_interval(1 * 1000 * 1000, cannot_fetch_log_warn_time_)) {
      PALF_LOG(INFO, "sync is disabled, cannot fetch log", K_(palf_id), K_(self));
    }
  } else if (!dest.is_valid() || !fetch_start_lsn.is_valid() || fetch_start_log_id <= 0
      || fetch_log_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K_(self), K(prev_lsn), K(fetch_start_lsn),
        K(fetch_start_log_id), K(dest), K(fetch_log_size));
  } else {
    const int64_t last_slide_log_id = get_last_slide_log_id_();
    // 为了避免重复拉取，这里要用last_slide_log_id, 而不用sw_start_id，
    // 因为sliding_cb()执行完之前sw_start_id还没推进，last_slide_log_id已经推进
    int64_t skip_log_count = fetch_start_log_id - (last_slide_log_id + 1);
    if (skip_log_count < 0) {
      // The log of fetch_start_log_id maybe has slid out
      skip_log_count = 0;
    }
    // NB: follower's fetch_log_count must be bigger than PALF_MAX_LEADER_SUBMIT_LOG_COUNT,
    // otherwise committed_end_lsn in follower may never be inc updated,
    // so just set it to PALF_SLIDING_WINDOW_SIZE
    const int64_t fetch_log_count = PALF_SLIDING_WINDOW_SIZE - skip_log_count;
    // Update last_fetch_end_lsn_ AND last_fetch_max_log_id_
    bool need_exec_fetch = true;
    do {
      ObSpinLockGuard guard(fetch_info_lock_);
      const LSN tmp_end_lsn = fetch_start_lsn + fetch_log_size;
      if (FetchTriggerType::SLIDING_CB == trigger_type
          && !last_fetch_committed_end_lsn_.is_valid()) {
        // Streamingly fetching may trigger more than one time,
        // we need filter duplicated ops here.
        need_exec_fetch = false;
      } else {
        ATOMIC_STORE(&last_fetch_end_lsn_.val_, tmp_end_lsn.val_);
        last_fetch_max_log_id_ = fetch_start_log_id + fetch_log_count - 1;
        last_fetch_committed_end_lsn_.reset();
      }
    } while(0);

    if (OB_SUCC(ret) && need_exec_fetch) {
      FetchLogType fetch_type = FETCH_LOG_FOLLOWER;
      if (LEADER_RECONFIRM == trigger_type) {
        fetch_type = FETCH_LOG_LEADER_RECONFIRM;
      } else if (MODE_META_BARRIER == trigger_type) {
        fetch_type = FETCH_MODE_META;
      }
      const int64_t curr_proposal_id = state_mgr_->get_proposal_id();
      const int64_t accepted_mode_pid = mode_mgr_->get_accepted_mode_meta().proposal_id_;
      if (fetch_log_count <= 0) {
        ret = OB_EAGAIN;
      } else if (OB_FAIL(log_engine_->submit_fetch_log_req(dest, fetch_type, curr_proposal_id, prev_lsn,
              fetch_start_lsn, fetch_log_size, fetch_log_count, accepted_mode_pid))) {
        PALF_LOG(WARN, "submit_fetch_log_req failed", K(ret), K_(palf_id), K_(self));
      } else {
        // Record fetch trigger type
        last_fetch_req_time_ = ObTimeUtility::current_time();
        last_fetch_trigger_type_ = trigger_type;
      }
      PALF_LOG(INFO, "do_fetch_log_ finished", K(ret), K_(palf_id), K_(self), K(dest),
          K(fetch_log_count), K(fetch_start_lsn), K(prev_lsn), K(fetch_start_log_id),
          K(last_slide_log_id), K(fetch_log_size), K(accepted_mode_pid), K_(last_fetch_req_time),
          K_(last_fetch_end_lsn), K_(last_fetch_max_log_id), K_(last_fetch_committed_end_lsn),
          "trigger_type", fetch_trigger_type_2_str(trigger_type), KPC(this));
    }
  }
  return ret;
}

int LogSlidingWindow::get_leader_from_cache(common::ObAddr &leader) const
{
  return get_leader_from_cache_(leader);
}

int LogSlidingWindow::get_leader_from_cache_(common::ObAddr &leader) const
{
  int ret = OB_SUCCESS;
  const common::ObAddr state_mgr_leader = state_mgr_->get_leader();
  const common::ObAddr broadcast_leader = state_mgr_->get_broadcast_leader();
  if (state_mgr_leader.is_valid()) {
    leader = state_mgr_leader;
  } else if (broadcast_leader.is_valid()) {
    leader = broadcast_leader;
  } else if (palf_reach_time_interval(PALF_FETCH_LOG_RENEW_LEADER_INTERVAL_US,
             last_fetch_log_renew_leader_ts_us_) &&
             OB_FAIL(plugins_->nonblock_renew_leader(palf_id_))) {
    PALF_LOG(WARN, "nonblock_renew_leader failed", KR(ret), K_(palf_id), K_(self));
  } else if (OB_FAIL(plugins_->nonblock_get_leader(palf_id_, leader))) {
    if (palf_reach_time_interval(5 * 1000 * 1000, lc_cb_get_warn_time_)) {
      PALF_LOG(WARN, "nonblock_get_leader failed", KR(ret), K_(palf_id), K_(self));
    }
  } else {}
  return ret;
}

int LogSlidingWindow::get_fetch_log_dst_(common::ObAddr &fetch_dst) const
{
  int ret = OB_SUCCESS;
  const common::ObAddr parent = mm_->get_parent();
  if (parent.is_valid()) {
    fetch_dst = parent;
  } else if (OB_FAIL(get_leader_from_cache_(fetch_dst))) {
  }
  return ret;
}

bool LogSlidingWindow::is_all_committed_log_slided_out(LSN &prev_lsn, int64_t &prev_log_id, LSN &committed_end_lsn) const
{
  LSN unused_lsn;
  return is_all_committed_log_slided_out_(prev_lsn, prev_log_id, unused_lsn, committed_end_lsn);
}

bool LogSlidingWindow::is_all_committed_log_slided_out_(
    LSN &prev_lsn,
    int64_t &prev_log_id,
    LSN &start_lsn,
    LSN &committed_end_lsn) const
{
  bool bool_ret = false;
  int64_t last_slide_log_id = OB_INVALID_LOG_ID;
  SCN last_slide_scn;
  LSN last_slide_lsn;
  LSN last_slide_end_lsn;
  int64_t last_slide_log_pid = INVALID_PROPOSAL_ID;
  int64_t last_slide_accum_checksum = -1;
  get_last_slide_log_info_(last_slide_log_id, last_slide_scn, last_slide_lsn, \
      last_slide_end_lsn, last_slide_log_pid, last_slide_accum_checksum);
  get_committed_end_lsn_(committed_end_lsn);
  if (committed_end_lsn <= last_slide_end_lsn) {
    bool_ret = true;
  } else {
    bool_ret = false;
    PALF_LOG(TRACE, "is_all_committed_log_slided_out_ false", K_(palf_id), K_(self), K(bool_ret), K(committed_end_lsn),
        K(last_slide_end_lsn), K(last_slide_log_id));
  }
  prev_lsn = last_slide_lsn;
  prev_log_id = last_slide_log_id;
  start_lsn = last_slide_end_lsn;
  return bool_ret;
}

int LogSlidingWindow::sliding_cb(const int64_t sn, const FixedSlidingWindowSlot *data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K_(palf_id), K_(self), K(sn), K(ret));
  } else if (!state_mgr_->can_slide_sw()) {
    // can_slide_sw() returns false
    ret = OB_EAGAIN;
  } else {
    LSN log_begin_lsn;
    LSN log_end_lsn;
    const int64_t log_id = static_cast<int64_t>(sn);
    const LogTask *log_task = dynamic_cast<const LogTask *>(data);
    if (NULL == log_task) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "dynamic_cast return NULL", K_(palf_id), K_(self), K(ret));
    } else {
      LogGroupEntryHeader tmp_header;
      LogTaskHeaderInfo log_task_header;

      log_task->lock();
      log_begin_lsn = log_task->get_begin_lsn();
      const SCN log_max_scn = log_task->get_max_scn();
      log_end_lsn = log_begin_lsn + LogGroupEntryHeader::HEADER_SER_SIZE + log_task->get_data_len();
      log_task_header = log_task->get_header_info();
      const int64_t log_proposal_id = log_task->get_proposal_id();
      const int64_t log_accum_checksum = log_task->get_accum_checksum();
      const int64_t log_gen_ts = log_task->get_gen_ts();
      const int64_t log_freeze_ts = log_task->get_freeze_ts();
      const int64_t log_submit_ts = log_task->get_submit_ts();
      const int64_t log_flush_ts = log_task->get_flushed_ts();
      log_task->unlock();

      // Verifying accum_checksum firstly.
      if (OB_FAIL(checksum_.verify_accum_checksum(log_task_header.data_checksum_,
                                                  log_task_header.accum_checksum_))) {
        PALF_LOG(ERROR, "verify_accum_checksum failed", K(ret), KPC(this), K(log_id), KPC(log_task));
      } else {
        // Call fs_cb.
        int tmp_ret = OB_SUCCESS;
        const int64_t fs_cb_begin_ts = ObTimeUtility::current_time();
        if (OB_SUCCESS != (tmp_ret = palf_fs_cb_->update_end_lsn(palf_id_, log_end_lsn, log_proposal_id))) {
          if (OB_EAGAIN == tmp_ret) {
            if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
              PALF_LOG(WARN, "update_end_lsn eagain", K(tmp_ret), K_(palf_id), K_(self), K(log_id), KPC(log_task));
            }
          } else {
            PALF_LOG(WARN, "update_end_lsn failed", K(tmp_ret), K_(palf_id), K_(self), K(log_id), KPC(log_task));
          }
        }
        const int64_t fs_cb_cost = ObTimeUtility::current_time() - fs_cb_begin_ts;
        fs_cb_cost_stat_.stat(fs_cb_cost);
        if (fs_cb_cost > 1 * 1000) {
          PALF_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "fs_cb->update_end_lsn() cost too much time", K(tmp_ret), K_(palf_id), K_(self),
              K(fs_cb_cost), K(log_id), K(log_begin_lsn), K(log_end_lsn), K(log_proposal_id));
        }

        const int64_t log_life_time = fs_cb_begin_ts - log_gen_ts;
        log_life_time_stat_.stat(log_life_time);

        const int64_t total_slide_log_cnt = ATOMIC_AAF(&accum_slide_log_cnt_, 1);
        const int64_t total_log_gen_to_freeze_cost = ATOMIC_AAF(&accum_log_gen_to_freeze_cost_, log_freeze_ts - log_gen_ts);
        const int64_t total_log_gen_to_submit_cost = ATOMIC_AAF(&accum_log_gen_to_submit_cost_, log_submit_ts - log_gen_ts);
        const int64_t total_log_submit_to_flush_cost = ATOMIC_AAF(&accum_log_submit_to_flush_cost_, log_flush_ts - log_submit_ts);
        const int64_t total_log_submit_to_slide_cost = ATOMIC_AAF(&accum_log_submit_to_slide_cost_, fs_cb_begin_ts - log_submit_ts);
        if (palf_reach_time_interval(PALF_STAT_PRINT_INTERVAL_US, log_slide_stat_time_)) {
          const int64_t avg_log_gen_to_freeze_time = total_log_gen_to_freeze_cost / total_slide_log_cnt;
          const int64_t avg_log_gen_to_submit_time = total_log_gen_to_submit_cost / total_slide_log_cnt;
          const int64_t avg_log_submit_to_flush_time = total_log_submit_to_flush_cost / total_slide_log_cnt;
          const int64_t avg_log_submit_to_slide_time = total_log_submit_to_slide_cost / total_slide_log_cnt;
          PALF_LOG(INFO, "[PALF STAT LOG TASK TIME]", K_(palf_id), K_(self), K(total_slide_log_cnt),
              K(avg_log_gen_to_freeze_time), K(avg_log_gen_to_submit_time), K(avg_log_submit_to_flush_time),
              K(avg_log_submit_to_slide_time));
          ATOMIC_STORE(&accum_slide_log_cnt_, 0);
          ATOMIC_STORE(&accum_log_gen_to_freeze_cost_, 0);
          ATOMIC_STORE(&accum_log_gen_to_submit_cost_, 0);
          ATOMIC_STORE(&accum_log_submit_to_flush_cost_, 0);
          ATOMIC_STORE(&accum_log_submit_to_slide_cost_, 0);
        }

        if (log_life_time > 100 * 1000) {
          if (palf_reach_time_interval(100 * 1000, log_life_long_warn_time_)) {
            PALF_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "log_task life cost too much time", K_(palf_id), K_(self), K(log_id), KPC(log_task),
                K(fs_cb_begin_ts), K(log_life_time));
          }
        }

        // update last_slide_lsn_
        if (OB_SUCC(ret)) {
          (void) try_update_last_slide_log_info_(log_id, log_max_scn, log_begin_lsn, log_end_lsn, \
              log_proposal_id, log_accum_checksum);
        }

        MEM_BARRIER();  // ensure last_slide_log_info_ has been updated before fetch log streamingly

        if (OB_SUCC(ret)
            && (FOLLOWER == state_mgr_->get_role() || state_mgr_->is_leader_reconfirm())) {
          // Check if need fetch log streamingly,
          try_fetch_log_streamingly_(log_end_lsn);
        }
      }
    }
  }
  return ret;
}

void LogSlidingWindow::try_fetch_log_streamingly_(const LSN &log_end_lsn)
{
  // 本接口由sliding_cb()调用(单线程)，触发流式拉取日志
  LSN last_committed_end_lsn;
  last_committed_end_lsn.val_ = ATOMIC_LOAD(&last_fetch_committed_end_lsn_.val_);
  if (last_committed_end_lsn.is_valid() && log_end_lsn == last_committed_end_lsn) {
    // 本条日志的end_lsn与上一轮fetch的committed_end_lsn匹配时说明上一轮拉取的日志
    // 中committed logs都已滑出, 以 (last_submit_log_id + 1) 为新的起点触发下一轮fetch
    LSN last_submit_lsn;
    LSN last_submit_end_lsn;
    int64_t last_submit_log_id = OB_INVALID_LOG_ID;
    int64_t last_submit_log_pid = INVALID_PROPOSAL_ID;
    (void) get_last_submit_log_info_(last_submit_lsn, last_submit_end_lsn, last_submit_log_id, last_submit_log_pid);
    const int64_t fetch_start_log_id = last_submit_log_id + 1;
    // fetch_log_size need sub MAX_LOG_BUFFER_SIZE to ensure the incoming last fetched log's end_lsn
    // is not smaller than last_fetch_end_lsn, then it can successfully trigger next streaming fetch.
    // And all the incoming fetched logs can be filled into group_buffer.
    const int64_t fetch_log_size = group_buffer_.get_available_buffer_size() - MAX_LOG_BUFFER_SIZE;
    const LSN fetch_begin_lsn = last_submit_end_lsn;
    const LSN prev_lsn = last_submit_lsn;
    ObAddr dest;
    if (FOLLOWER == state_mgr_->get_role()) {
      get_fetch_log_dst_(dest);
    } else if (state_mgr_->is_leader_reconfirm()) {
      dest = reconfirm_fetch_dest_;
    } else {
      // do nothing
    }
    (void) do_fetch_log_(FetchTriggerType::SLIDING_CB, dest, prev_lsn, \
        fetch_begin_lsn, fetch_log_size, fetch_start_log_id);
  }
}

bool LogSlidingWindow::is_all_log_flushed_()
{
  // Check if all logs have been flushed
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;
  LSN max_flushed_end_lsn;
  get_max_flushed_end_lsn(max_flushed_end_lsn);
  LSN curr_end_lsn;
  if (OB_SUCCESS != (tmp_ret = lsn_allocator_.get_curr_end_lsn(curr_end_lsn))) {
    PALF_LOG_RET(WARN, tmp_ret, "get_curr_end_lsn failed", K(tmp_ret), K_(palf_id), K_(self));
  } else if (max_flushed_end_lsn < curr_end_lsn) {
    PALF_LOG_RET(WARN, OB_EAGAIN, "there is some log has not been flushed", K_(palf_id), K_(self), K(curr_end_lsn),
        K(max_flushed_end_lsn), K_(max_flushed_lsn));
  } else {
    bool_ret = true;
  }
  PALF_LOG(INFO, "is_all_log_flushed_", K(bool_ret), K_(palf_id), K_(self), K(curr_end_lsn), K(max_flushed_end_lsn));
  return bool_ret;
}

int LogSlidingWindow::check_all_log_task_freezed_(bool &is_all_freezed)
{
  int ret = OB_SUCCESS;
  is_all_freezed = true;
  const int64_t start_log_id = get_start_id();
  const int64_t max_log_id = get_max_log_id();
  LogTask *log_task = NULL;
  for (int64_t tmp_log_id = start_log_id; OB_SUCC(ret) && tmp_log_id <= max_log_id; ++tmp_log_id) {
    LogTaskGuard guard(this);
    if (OB_FAIL(guard.get_log_task(tmp_log_id, log_task))) {
      PALF_LOG(ERROR, "get_log_task failed", K(ret), K(tmp_log_id), K_(palf_id), K_(self));
    } else {
      log_task->lock();
      if (!log_task->is_valid()) {
        // Its previous state maybe reconfirm, and it may not fetch all logs form dest follower.
        // So maybe there are holes in sw.
        PALF_LOG(INFO, "log_task is invalid, this log (hole) may be not received during reconfirm state", K(ret),
            K(tmp_log_id), K_(palf_id), K_(self), K(start_log_id), K(max_log_id), KPC(log_task));
      } else if (!log_task->is_freezed()) {
        is_all_freezed = false;
        PALF_LOG(WARN, "this log_task is not freezed", K(ret), K(tmp_log_id), K(start_log_id), K(max_log_id),
            K_(palf_id), K_(self), KPC(log_task));
        break;
      } else {
        // do nothing
      }
      log_task->unlock();
    }
  }
  return ret;
}

int LogSlidingWindow::freeze_pending_log_(LSN &last_lsn)
{
  // try freeze last log when switch to pending state
  int ret = OB_SUCCESS;
  int64_t last_log_id = OB_INVALID_LOG_ID;
  bool is_need_handle = false;
  if (OB_FAIL(lsn_allocator_.try_freeze(last_lsn, last_log_id))) {
    PALF_LOG(WARN, "lsn_allocator try_freeze failed", K(ret), K_(palf_id), K_(self), K(last_lsn));
  } else if (last_log_id <= 0) {
    // no log, no need freeze
  } else if (OB_FAIL(try_freeze_last_log_task_(last_log_id, last_lsn, is_need_handle))) {
    PALF_LOG(WARN, "try_freeze_last_log_task_ failed", K(ret), K_(palf_id), K_(self), K(last_lsn));
  } else {
    const int64_t last_submit_log_id = get_last_submit_log_id_();
    if (last_log_id == last_submit_log_id + 1) {
      bool is_committed_lsn_updated = false;
      (void) handle_next_submit_log_(is_committed_lsn_updated);
    }
    PALF_LOG(INFO, "freeze_pending_log success", K(ret), K_(palf_id), K_(self), K(last_lsn));
  }
  return ret;
}

int LogSlidingWindow::to_follower_pending(LSN &last_lsn)
{
  int ret = OB_SUCCESS;
  bool is_all_freezed = false;
  LSN max_flushed_end_lsn;
  get_max_flushed_end_lsn(max_flushed_end_lsn);
  LSN curr_end_lsn;
  if (OB_FAIL(freeze_pending_log_(last_lsn))) {
    PALF_LOG(WARN, "freeze_pending_log_ failed", K(ret), K_(palf_id), K_(self), K(last_lsn));
  } else if (OB_FAIL(check_all_log_task_freezed_(is_all_freezed))) {
    PALF_LOG(WARN, "freeze_pending_log_ failed", K(ret), K_(palf_id), K_(self), K(last_lsn));
  } else if (!is_all_freezed) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "there is some log has not been freezed", K(ret), K_(palf_id), K_(self), K(last_lsn));
  } else if (OB_FAIL(lsn_allocator_.get_curr_end_lsn(curr_end_lsn))) {
    PALF_LOG(WARN, "get_curr_end_lsn failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_FAIL(group_buffer_.to_follower())) {
    PALF_LOG(WARN, "group_buffer_.to_follower failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_FAIL(reset_match_lsn_map_())) {
    PALF_LOG(WARN, "reset_match_lsn_map_ failed", K(ret), K_(palf_id), K_(self));
  } else {
    reconfirm_fetch_dest_.reset();
    FLOG_INFO("to_follower_pending success", K(ret), K_(palf_id), K_(self), K(last_lsn),
        K(max_flushed_end_lsn), K(curr_end_lsn), K(is_all_freezed), KPC(this));
  }
  return ret;
}

int LogSlidingWindow::clean_cached_log(const int64_t begin_log_id,
                                       const LSN &lsn,
                                       const LSN &prev_lsn,
                                       const int64_t prev_log_pid)
{
  // Caller holds palf_handle's wrlock.
  // This func is used to clean cached log_task that has not been processed.
  // The arg begin_log_id is expected to be equal  to (last_submit_log_id + 1).
  // Before executing clean op, we need double check prev_log info.
  int ret = OB_SUCCESS;
  const int64_t last_submit_log_id = get_last_submit_log_id_();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_LOG_ID == begin_log_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (begin_log_id != last_submit_log_id + 1) {
    // begin_log_id is not larger than last_submit_log_id, last_submit_log maybe changed after check,
    // cannot clean cached logs.
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "begin_log_id is not larger than last_submit_log_id, cannot clean", K(ret),
        K_(palf_id), K_(self), K(begin_log_id));
  } else {
    // double check if prev_log matches with local.
    // If not match, report -4109 and do nothing.
    bool is_prev_log_exist = false;
    const bool is_prev_log_match = is_prev_log_pid_match(begin_log_id, lsn, prev_lsn, \
        prev_log_pid, is_prev_log_exist);
    if (false == is_prev_log_match) {
      ret = OB_STATE_NOT_MATCH;
      PALF_LOG(WARN, "prev_log does not match with local, cannot clean", K(ret),
          K_(palf_id), K_(self), K(begin_log_id), K(lsn), K(prev_lsn), K(prev_log_pid));
      // call clean_log_() to reset log_tasks beyond last_submit_log_id.
    } else if (OB_FAIL(clean_log_())) {
      PALF_LOG(ERROR, "clean_log_ failed", K(ret), K_(palf_id), K_(self), K(last_submit_log_id));
    } else {
      // do nothing
    }
  }
  PALF_LOG(INFO, "clean_cached_log finished", K(ret), K_(palf_id), K_(self), K(begin_log_id), K(lsn), K(prev_lsn), K(prev_log_pid));
  return ret;
}

int LogSlidingWindow::clean_log()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = clean_log_();
  }
  return ret;
}

int LogSlidingWindow::clean_log_()
{
  // Caller holds palf_handle's wrlock.
  // This func is used to clear log tasks beyond the last_submit_log_id in sw.
  int ret = OB_SUCCESS;
  const int64_t start_log_id = get_start_id();
  const int64_t max_log_id = get_max_log_id();
  LSN curr_end_lsn;
  (void) lsn_allocator_.get_curr_end_lsn(curr_end_lsn);

  int64_t last_slide_log_id = OB_INVALID_LOG_ID;
  SCN last_slide_scn;
  LSN last_slide_lsn;
  LSN last_slide_end_lsn;
  int64_t last_slide_log_pid = INVALID_PROPOSAL_ID;
  int64_t last_slide_accum_checksum = -1;
  get_last_slide_log_info_(last_slide_log_id, last_slide_scn, last_slide_lsn, \
      last_slide_end_lsn, last_slide_log_pid, last_slide_accum_checksum);
  LSN last_submit_lsn;
  LSN last_submit_end_lsn;
  int64_t last_submit_log_id = OB_INVALID_LOG_ID;
  int64_t last_submit_log_pid = INVALID_PROPOSAL_ID;
  (void) get_last_submit_log_info_(last_submit_lsn, last_submit_end_lsn, last_submit_log_id, last_submit_log_pid);
  // new_last_log_xxx are used to truncate lsn_allocator.
  int64_t new_last_log_id = OB_INVALID_LOG_ID;
  SCN new_last_scn;
  LSN new_last_log_end_lsn;
  if (last_slide_end_lsn == last_submit_end_lsn) {
    new_last_log_id = last_slide_log_id;
    new_last_scn = last_slide_scn;
    new_last_log_end_lsn = last_slide_end_lsn;
    PALF_LOG(INFO, "record last slide log info", K(ret), K(last_slide_log_id),
        K(last_slide_scn), K(last_slide_end_lsn), K_(palf_id), K_(self));
  }

  int64_t first_empty_log_id = OB_INVALID_LOG_ID;  // record the first hole in sw, just for debug
  LogTask *log_task = NULL;
  for (int64_t tmp_log_id = start_log_id; OB_SUCC(ret) && tmp_log_id <= max_log_id; ++tmp_log_id) {
    LogTaskGuard guard(this);
    if (OB_FAIL(guard.get_log_task(tmp_log_id, log_task))) {
      // caller hold wrlock, so this step is expected to succeed.
      PALF_LOG(ERROR, "get_log_task failed", K(ret), K(tmp_log_id), K_(palf_id), K_(self));
    } else {
      log_task->lock();
      if (!log_task->is_valid()) {
        PALF_LOG(INFO, "log_task is invalid", K(ret), K(tmp_log_id), K_(palf_id), K_(self), K(first_empty_log_id),
            K(max_log_id), KPC(log_task));
        if (OB_INVALID_LOG_ID == first_empty_log_id) {
          first_empty_log_id = tmp_log_id;
          PALF_LOG(INFO, "found first empty log slot", K(ret), K(tmp_log_id), K_(palf_id), K_(self));
        }
      } else {
        const SCN curr_scn = log_task->get_max_scn();
        const LSN log_end_lsn = log_task->get_begin_lsn() + LogGroupEntryHeader::HEADER_SER_SIZE + log_task->get_data_len();
        PALF_LOG(INFO, "log_task is valid, check if need clean", K(ret), K(tmp_log_id), K_(palf_id), K_(self), KPC(log_task));
        if (log_end_lsn == last_submit_end_lsn) {
          if (OB_INVALID_LOG_ID == new_last_log_id) {
            // record max flushed log_task info
            new_last_log_id = tmp_log_id;
            new_last_scn = curr_scn;
            new_last_log_end_lsn = log_end_lsn;
            PALF_LOG(INFO, "find last submit log_task", K(ret), K(tmp_log_id), K_(palf_id), K_(self),
                KPC(log_task), K(last_submit_log_id));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_INVALID_LOG_ID != last_submit_log_id && tmp_log_id > last_submit_log_id) {
            // The logs beyond last_submit_log_id need to be reset(reconfirm).
            PALF_LOG(INFO, "clean log task beyond last_submit_log_id", K(ret), K_(palf_id), K_(self), K(max_log_id), K(tmp_log_id),
                K(first_empty_log_id), K(last_submit_log_id), KPC(log_task));
            log_task->reset();
          }
        }
      }
      log_task->unlock();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_INVALID_LOG_ID == new_last_log_id
        || !new_last_scn.is_valid()
        || !new_last_log_end_lsn.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "last_log info is invalid", K(ret), K(max_log_id), K(first_empty_log_id), K(last_submit_log_id),
           K_(palf_id), K_(self), K(new_last_log_end_lsn), K(new_last_log_id), K(new_last_scn), K(start_log_id), K(max_log_id));
    } else if (new_last_log_end_lsn <= curr_end_lsn
               && OB_FAIL(truncate_lsn_allocator_(new_last_log_end_lsn, new_last_log_id, new_last_scn))) {
      // truncate lsn_allocator_ by new_last_log info
      PALF_LOG(ERROR, "truncate_lsn_allocator_ failed", K(ret), K_(palf_id), K_(self), K(new_last_log_id), K(new_last_log_end_lsn),
          K(new_last_scn));
    } else {
      // do nothing
    }
  }
  PALF_LOG(INFO, "clean log finished", K(ret), K_(palf_id), K_(self), K(max_log_id), K(first_empty_log_id), K(last_submit_log_id),
      K(start_log_id), K(max_log_id), K(new_last_log_id), K(new_last_scn), K(new_last_log_end_lsn));
  return ret;
}

int LogSlidingWindow::to_leader_reconfirm()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(reset_match_lsn_map_())) {
    PALF_LOG(WARN, "reset_match_lsn_map_ failed", K(ret), K_(palf_id), K_(self));
  } else {
    PALF_LOG(INFO, "to_leader_reconfirm success", K(ret), K_(palf_id), K_(self));
  }
  return ret;
}

int LogSlidingWindow::to_leader_active()
{
  // Check if all group entries have been flushed
  // Reset log_tasks' IS_SUBMIT_LOG_EXIST flag
  // Resize group_buffer
  int ret = OB_SUCCESS;
  SCN ref_scn;
  int64_t mode_version = INVALID_PROPOSAL_ID;
  AccessMode access_mode = AccessMode::INVALID_ACCESS_MODE;
  const int64_t curr_proposal_id = state_mgr_->get_proposal_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(mode_mgr_->get_access_mode_ref_scn(mode_version, access_mode, ref_scn))) {
    PALF_LOG(INFO, "get_access_mode_ref_scn failed", K(ret), K_(palf_id), K_(self));
  } else if (curr_proposal_id < mode_version) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "curr_proposal_id is less than proposal_id in ModeMeta", K(ret),
        K_(palf_id), K_(self), K(mode_version), K(curr_proposal_id));
  } else if (!is_all_log_flushed_()) {
    ret = OB_EAGAIN;
    PALF_LOG(WARN, "to_leader_active need retry, because there is some log has not been flushed", K(ret),
        K_(palf_id), K_(self));
  } else if (OB_FAIL(clean_log_())) {
    PALF_LOG(INFO, "clean_log_ failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_FAIL(group_buffer_.to_leader())) {
    PALF_LOG(WARN, "group_buffer_.to_leader failed", K(ret), K_(palf_id), K_(self));
  } else if (ref_scn.is_valid() && AccessMode::APPEND == access_mode &&
             OB_FAIL(lsn_allocator_.inc_update_scn_base(ref_scn))) {
    PALF_LOG(ERROR, "inc_update_scn_base failed", K(ret), K_(palf_id), K_(self), K(ref_scn));
  } else if (OB_FAIL(reset_match_lsn_map_())) {
    // Reset match_lsn_map to handle case that some follower's match_lsn is larger than
    // majority_max_lsn of reconfirm(it has phantom logs generated by old leader).
    PALF_LOG(WARN, "reset_match_lsn_map_ failed", K(ret), K_(palf_id), K_(self));
  } else {
    reconfirm_fetch_dest_.reset();
    PALF_LOG(INFO, "to_leader_active success", K(ret), K_(palf_id), K_(self));
  }
  return ret;
}

int64_t LogSlidingWindow::get_start_id() const
{
  return sw_.get_begin_sn();
}

int LogSlidingWindow::gen_committed_end_lsn_(LSN &new_committed_end_lsn)
{
  int ret = OB_SUCCESS;
  ObMemberList curr_member_list, prev_member_list;
  int64_t curr_replica_num = 0, prev_replica_num = 0;
  LSN curr_result_lsn, prev_result_lsn;
  bool is_before_barrier = false;
  LSN barrier_lsn;
  if (OB_FAIL(mm_->get_log_sync_member_list_for_generate_committed_lsn(prev_member_list,
      prev_replica_num, curr_member_list, curr_replica_num, is_before_barrier, barrier_lsn))) {
    PALF_LOG(WARN, "get_log_sync_member_list failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_FAIL(get_majority_lsn_(curr_member_list, curr_replica_num, curr_result_lsn))) {
    PALF_LOG(WARN, "get_majority_lsn failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_UNLIKELY(true == is_before_barrier) &&
      OB_FAIL(get_majority_lsn_(prev_member_list, prev_replica_num, prev_result_lsn))) {
    PALF_LOG(WARN, "get_majority_lsn failed", K(ret), K_(palf_id), K_(self));
  } else {
    // Note: the leader generates committed_end_lsn based on different memberlists before
    // and after a reconfiguration, barrier_lsn is the boundary.
    // - Logs which is before barrier_lsn could be committed by previous/current memberlist.
    LSN result_lsn = (OB_UNLIKELY(is_before_barrier))? MAX(prev_result_lsn, curr_result_lsn): curr_result_lsn;
    // - Logs which is after barrier_lsn should be committed by current memberlist.
    // - If current committed_end_lsn is smaller than barrier_lsn, then new committed_end_lsn
    //   generated by previous memberlist must be smaller than barrier_lsn.
    //   For example, memberlist:{A} + replica:B. After adding B successfully, Logs after the
    //   barrier may have been persisted by A, but not B. The leader A can not commit logs after
    //   the barrier with memberlist:{A}.
    result_lsn = (OB_UNLIKELY(is_before_barrier))? MIN(result_lsn, barrier_lsn): result_lsn;
    // Note: The leader is not allowed to generate new committed_end_lsn while changing configs with arb.
    // 1. {A, B, C(arb)}, A is the leader, end_lsns of A and B are both 100.
    // 2. B crashes and A decicdes to degrade B to a learner, A changes memberlist to {A, C(arb)} and
    //    sends config log to C. While the config log is flying, applications submits logs [100, 200).
    //    A can not commit [100, 200] logs before A receives ack messages to the config log from C.
    //    Otherwise, if the config log do not reaches majority and B is elected to be new leader later,
    //    new leader B may lost committed logs [100, 200).
    if (OB_LIKELY(false == state_mgr_->is_changing_config_with_arb())) {
      (void) try_advance_committed_lsn_(result_lsn);
    }
    new_committed_end_lsn = result_lsn;
  }
  return ret;
}

int LogSlidingWindow::gen_committed_end_lsn_with_memberlist_(
    const ObMemberList &member_list,
    const int64_t replica_num)
{
  int ret = OB_SUCCESS;
  LSN result_lsn;
  if (!member_list.is_valid() || replica_num <= 0 ||
      replica_num < member_list.get_member_number()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(member_list), K(replica_num));
  } else if (OB_FAIL(get_majority_lsn_(member_list, replica_num, result_lsn))) {
    PALF_LOG(WARN, "get_majority_lsn failed", K(ret), K_(palf_id), K_(self));
  } else {
    (void) try_advance_committed_lsn_(result_lsn);
    PALF_LOG(INFO, "gen_committed_end_lsn_with_memberlist_ finished", K(ret), K_(palf_id),
        K_(self), K(result_lsn), K(member_list), K(replica_num));
  }
  return ret;
}

int LogSlidingWindow::get_server_ack_info(const common::ObAddr &server, LsnTsInfo &ack_info) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(false == server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K_(palf_id), K_(self), K(server));
  } else {
    ObSpinLockGuard guard(match_lsn_map_lock_);
    if (OB_FAIL(match_lsn_map_.get(server, ack_info))) {
      PALF_LOG(WARN, "match_lsn_map_ get failed", K(ret), K_(palf_id), K_(self), K(server));
    }
  }
  return ret;
}

int LogSlidingWindow::get_ack_info_array(LogMemberAckInfoList &ack_info_array) const
{
  int ret = OB_SUCCESS;
  common::ObMemberList member_list;
  int64_t replica_num = 0;
  common::GlobalLearnerList degraded_learner_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(mm_->get_log_sync_member_list(member_list, replica_num))) {
    PALF_LOG(WARN, "get_log_sync_member_list failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_FAIL(mm_->get_degraded_learner_list(degraded_learner_list))) {
    PALF_LOG(WARN, "get_degraded_learner_list failed", K(ret), K_(palf_id), K_(self));
  } else {
    // TODO by yunlong: optimize for loop
    ObSpinLockGuard guard(match_lsn_map_lock_);
    ObMember tmp_server;
    LsnTsInfo tmp_val;
    for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
      tmp_server.reset();
      if (OB_FAIL(member_list.get_member_by_index(i, tmp_server))) {
        PALF_LOG(WARN, "get_server_by_index failed", K(ret), K_(palf_id), K_(self));
      } else if (OB_FAIL(match_lsn_map_.get(tmp_server.get_server(), tmp_val))) {
        // 预期不应该失败，每次成员变更时同步更新保持map与member_list一致
        PALF_LOG(WARN, "match_lsn_map_ get failed", K(ret), K_(palf_id), K_(self), K(tmp_server));
      } else {
        LogMemberAckInfo ack_info;
        ack_info.member_ = tmp_server;
        ack_info.last_ack_time_us_ = tmp_val.last_ack_time_us_;
        ack_info.last_flushed_end_lsn_ = tmp_val.lsn_;
        ack_info_array.push_back(ack_info);
        PALF_LOG(TRACE, "push ack info for match_lsn_map_ success", K(ack_info));
      }
    }
    PALF_LOG(TRACE, "begin push ack info for degraded_learner_list", K(degraded_learner_list));
    for (int64_t i = 0; OB_SUCC(ret) && i < degraded_learner_list.get_member_number(); ++i) {
      tmp_server.reset();
      if (OB_FAIL(degraded_learner_list.get_learner(i, tmp_server))) {
        PALF_LOG(WARN, "get_server_by_index failed", K(ret), K_(palf_id), K_(self));
      } else if (OB_FAIL(match_lsn_map_.get(tmp_server.get_server(), tmp_val))) {
        ret = OB_SUCCESS;
        PALF_LOG(TRACE, "get server from match_lsn_map_ success", K(tmp_server), K(ret));
      } else {
        LogMemberAckInfo ack_info;
        ack_info.member_ = tmp_server;
        ack_info.last_ack_time_us_ = tmp_val.last_ack_time_us_;
        ack_info.last_flushed_end_lsn_ = tmp_val.lsn_;
        ack_info_array.push_back(ack_info);
        PALF_LOG(TRACE, "push ack info for degraded_learner_list success", K(ack_info), K(degraded_learner_list));
      }
    }
  }
  return ret;
}

int LogSlidingWindow::pre_check_before_degrade_upgrade(const LogMemberAckInfoList &servers,
                                                       bool is_degrade)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_degrade) {
    // for degrading, double check if last_ack_ts of degraded servers has changed.
    // if current last_ack_ts == ack_ts, do degrade
    ObSpinLockGuard guard(match_lsn_map_lock_);
    for (int i = 0; OB_SUCC(ret) && i < servers.count(); i++) {
      LsnTsInfo tmp_val;
      const LogMemberAckInfo &ack_info = servers.at(i);
      const common::ObAddr &server = ack_info.member_.get_server();
      if (OB_FAIL(match_lsn_map_.get(server, tmp_val))) {
        PALF_LOG(WARN, "do not degrade, arb_reason: match_lsn_map_ get failed", K(ret), K_(palf_id), K_(self), K(server));
        ret = OB_OP_NOT_ALLOW;
      } else if (tmp_val.last_ack_time_us_ != ack_info.last_ack_time_us_) {
        PALF_LOG(WARN, "do not degrade, arb_reason: last_ack_ts has changed", K(ret), K_(palf_id), K_(self), K(ack_info), K(tmp_val));
        ret = OB_OP_NOT_ALLOW;
      }
    }
  } else {
    // for upgrading, double check if last_lsn of degraded servers has inc updated
    // if current match_lsn >= last_lsn, do upgrade
    ObSpinLockGuard guard(match_lsn_map_lock_);
    for (int i = 0; OB_SUCC(ret) && i < servers.count(); i++) {
      LsnTsInfo tmp_val;
      const LogMemberAckInfo &ack_info = servers.at(i);
      const common::ObAddr &server = ack_info.member_.get_server();
      if (OB_FAIL(match_lsn_map_.get(server, tmp_val))) {
        PALF_LOG(WARN, "do not upgrade, arb_reason: match_lsn_map_ get failed", K(ret), K_(palf_id), K_(self), K(server));
        ret = OB_OP_NOT_ALLOW;
      } else if (tmp_val.lsn_ < ack_info.last_flushed_end_lsn_) {
        PALF_LOG(WARN, "do not degrade, arb_reason: current match_lsn is less than ack_info", K(ret), K_(palf_id), K_(self),
            K(ack_info), K(tmp_val));
        ret = OB_OP_NOT_ALLOW;
      }
    }
  }
  return ret;
}

int LogSlidingWindow::get_lagged_member_list(const LSN &dst_lsn, ObMemberList &lagged_list)
{
  int ret = OB_SUCCESS;
  if (!dst_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    GetLaggedListFunc get_lagged_list_func(dst_lsn);
    ObSpinLockGuard guard(match_lsn_map_lock_);
    if (OB_FAIL(match_lsn_map_.for_each(get_lagged_list_func))) {
      PALF_LOG(WARN, "match_lsn_map_.operate() failed", K(ret), K_(palf_id), K_(self));
    } else if (OB_FAIL(get_lagged_list_func.get_lagged_list(lagged_list))) {
      PALF_LOG(WARN, "get_lagged_list failed", K(ret), K_(palf_id), K_(self));
    } else {
      PALF_LOG(INFO, "get_lagged_list success", K(ret), K_(palf_id), K_(self), K(dst_lsn), K(lagged_list));
    }
  }
  return ret;
}


int LogSlidingWindow::get_majority_lsn_(const ObMemberList &member_list,
                                        const int64_t replica_num,
                                        LSN &result_lsn) const
{
  int ret = OB_SUCCESS;
  assert(replica_num > 0);
  LSN lsn_array[OB_MAX_MEMBER_NUMBER];
  int64_t valid_member_cnt = 0;
  do {
    ObSpinLockGuard guard(match_lsn_map_lock_);
    ObAddr tmp_server;
    LsnTsInfo tmp_val;
    for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
      int tmp_ret = OB_SUCCESS;
      tmp_server.reset();
      if (OB_FAIL(member_list.get_server_by_index(i, tmp_server))) {
        PALF_LOG(WARN, "get_server_by_index failed", K(ret), K_(palf_id), K_(self));
      } else if (OB_TMP_FAIL(match_lsn_map_.get(tmp_server, tmp_val))) {
        // Note: the leader may generate committed_end_lsn based on previous member list,
        // members in member_list may do not exist in match_lsn_map. For example, removing D from
        // (ABCD), previous member_list is (ABCD) but D has been removed from match_lsn_map.
        // Therefore, we just skip members that do not exist in match_lsn_map.
        PALF_LOG(WARN, "match_lsn_map_ get failed", K(tmp_ret), K_(palf_id), K_(self), K(tmp_server));
      } else {
        lsn_array[valid_member_cnt++] = tmp_val.lsn_;
        PALF_LOG(TRACE, "current matched lsn", K_(palf_id), K_(self), "server:", tmp_server, "lsn:", tmp_val.lsn_);
      }
    }
  } while(0);

  if (valid_member_cnt < replica_num / 2 + 1) {
    PALF_LOG(WARN, "match_lsn_map do not reach majority", K(ret), K_(palf_id), K_(self),
        K(member_list), K(replica_num), K(valid_member_cnt));
  } else if (OB_SUCC(ret)) {
    std::sort(lsn_array, lsn_array + valid_member_cnt, LSNCompare());
    assert(replica_num / 2 < OB_MAX_MEMBER_NUMBER);
    result_lsn = lsn_array[replica_num / 2];
    if (!result_lsn.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "result_lsn is invalid, unexpected", K(ret), K_(palf_id), K_(self), K(replica_num));
    }
  }
  return ret;
}

bool LogSlidingWindow::is_allow_rebuild() const
{
  // Caller holds palf_handle_impl's rlock.
  bool bool_ret = false;
  if (IS_INIT) {
    bool_ret = !is_truncating_;
  }
  return bool_ret;
}

int LogSlidingWindow::truncate_for_rebuild(const PalfBaseInfo &palf_base_info)
{
  // Caller holds palf_handle_impl's wrlock.
  int ret = OB_SUCCESS;
  const LogInfo &prev_log_info = palf_base_info.prev_log_info_;
  const int64_t new_start_log_id = prev_log_info.log_id_ + 1;
  const int64_t start_log_id = get_start_id();
  const int64_t max_log_id = get_max_log_id();
  // The 'log_id' of migrage src is smaller than self's 'start_log_id'
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (new_start_log_id <= start_log_id) {
    PALF_LOG(INFO, "new_start_log_id is smaller than start_log_id, do nothing", K_(palf_id), K_(self),
        K(new_start_log_id), K(start_log_id), K(max_log_id), K(palf_base_info));
  // 1) check previous log_tasks
  } else {
    if (max_log_id >= prev_log_info.log_id_) {
      int tmp_ret = OB_SUCCESS;
      LSN local_prev_lsn, local_prev_end_lsn;
      SCN local_prev_scn;
      int64_t local_prev_log_pid = INVALID_PROPOSAL_ID;
      int64_t local_prev_log_accum_checksum = -1;
      if (OB_SUCCESS != (tmp_ret = get_prev_log_info_(new_start_log_id, local_prev_lsn, \
              local_prev_end_lsn, local_prev_scn, local_prev_log_pid, local_prev_log_accum_checksum))) {
        if (OB_ENTRY_NOT_EXIST != tmp_ret) {
          ret = tmp_ret;
          PALF_LOG(WARN, "get_prev_log_info_ failed", K(ret), K(new_start_log_id), K(palf_base_info), K_(palf_id), K_(self));
        } else {
          LSN last_slide_end_lsn;
          get_last_slide_end_lsn_(last_slide_end_lsn);
          PALF_LOG(WARN, "prev_log does not exist", K(ret), K(tmp_ret), K(new_start_log_id), K(palf_base_info), K_(palf_id), K_(self),
              K(last_slide_end_lsn));
        }
      } else if (local_prev_lsn != prev_log_info.lsn_
                 || local_prev_log_pid != prev_log_info.log_proposal_id_) {
        PALF_LOG(INFO, "prev_log does not match with local during rebuild", K(ret), K_(palf_id), K_(self), K(start_log_id),
              K(max_log_id), K(new_start_log_id), K(palf_base_info), K(local_prev_lsn), K(local_prev_log_pid));
      } else {}
    }

    // 2) truncate log_task before new_start_log_id
    if (OB_SUCC(ret)) {
      // inc sw's start_id
      if (OB_FAIL(sw_.truncate_and_reset_begin_sn(new_start_log_id))) {
        PALF_LOG(WARN, "sw_.truncate_and_reset_begin_sn failed", K(ret), K_(palf_id), K_(self), K(start_log_id),
            K(new_start_log_id));
      }
    }

    // 3) truncate lsn_allocator_
    if (OB_SUCC(ret)) {
      // 与普通truncate log场景不同的是，此处只会将last_log_meta改大，无回退的需求
      // 只有当curr_end_lsn < palf_base_info.curr_lsn_时才需要更新
      LSN curr_end_lsn;
      (void) lsn_allocator_.get_curr_end_lsn(curr_end_lsn);
      if (curr_end_lsn.is_valid()
          && curr_end_lsn < palf_base_info.curr_lsn_) {
        if (OB_FAIL(lsn_allocator_.truncate(palf_base_info.curr_lsn_, prev_log_info.log_id_, prev_log_info.scn_))) {
          PALF_LOG(WARN, "truncate lsn_allocator_ failed", K(ret), K_(palf_id), K_(self), K(curr_end_lsn), K(palf_base_info));
        } else {
          PALF_LOG(INFO, "truncate lsn_allocator_ success", K(ret), K_(palf_id), K_(self), K(curr_end_lsn), K(palf_base_info));
        }
      }
    }
    // 4) inc update last_submit_log info, max_flushed_log info, last_slide_log info and max_committed_log info
    //    NB: 不能回退这些值，否则后续会发生日志覆盖写，log_storage层会报错
    if (OB_SUCC(ret)) {
      LSN last_submit_lsn;
      LSN last_submit_end_lsn;
      int64_t last_submit_log_id = OB_INVALID_LOG_ID;
      int64_t last_submit_log_pid = INVALID_PROPOSAL_ID;
      (void) get_last_submit_log_info_(last_submit_lsn, last_submit_end_lsn, last_submit_log_id, last_submit_log_pid);

      if (last_submit_end_lsn <= palf_base_info.curr_lsn_) {
        (void) set_last_submit_log_info_(prev_log_info.lsn_, palf_base_info.curr_lsn_, \
            prev_log_info.log_id_, prev_log_info.log_proposal_id_);
        // update local accum_checksum when last_submit_log_info updated
        checksum_.set_accum_checksum(prev_log_info.accum_checksum_);
      }
      LSN max_flushed_end_lsn;
      get_max_flushed_end_lsn(max_flushed_end_lsn);
      if (max_flushed_end_lsn <= palf_base_info.curr_lsn_) {
        (void) truncate_max_flushed_log_info_(prev_log_info.lsn_, palf_base_info.curr_lsn_,
            prev_log_info.log_proposal_id_);
        (void) group_buffer_.truncate(palf_base_info.curr_lsn_);
      }
      const int64_t last_slide_log_id = get_last_slide_log_id_();
      if (last_slide_log_id <= prev_log_info.log_id_) {
        (void) try_update_last_slide_log_info_(prev_log_info.log_id_, prev_log_info.scn_, \
            prev_log_info.lsn_, palf_base_info.curr_lsn_, prev_log_info.log_proposal_id_, \
            prev_log_info.accum_checksum_);
        // update local verify_checksum when last_slide_log_info updated
        checksum_.set_verify_checksum(prev_log_info.accum_checksum_);
      }
      is_rebuilding_ = true;
      last_rebuild_lsn_ = palf_base_info.curr_lsn_;
      // Note: can not inc update committed_end_lsn in here, otherwise iterator will read uncommitted log unexpectedly.
    }
  }
  // TODO by yunlong: dump all log_info before and after truncating
  PALF_LOG(INFO, "truncate_for_rebuild finished", K(ret), K_(palf_id), K_(self), K(palf_base_info), K(start_log_id),
      K(max_log_id));
  return ret;
}

int LogSlidingWindow::truncate(const TruncateLogInfo &truncate_log_info, const LSN &expected_prev_lsn,
    const int64_t expected_prev_log_pid)
{
  // Caller holds palf_handle_impl's wrlock.
  int ret = OB_SUCCESS;
  LogTask *log_task = NULL;
  LogTaskGuard guard(this);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!truncate_log_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(truncate_log_info));
  } else if (OB_FAIL(guard.get_log_task(truncate_log_info.truncate_log_id_, log_task))) {
    PALF_LOG(WARN, "get_log_task failed when truncate log", K(ret), K(truncate_log_info), K_(palf_id), K_(self));
  } else {
    // check if dst log matches with arg
    const int64_t truncate_log_id = truncate_log_info.truncate_log_id_;
    const LSN &truncate_begin_lsn = truncate_log_info.truncate_begin_lsn_;
    const int64_t truncate_log_proposal_id = truncate_log_info.truncate_log_proposal_id_;
    LSN log_begin_lsn;
    LSN log_end_lsn;
    int64_t prev_log_id = truncate_log_id - 1;
    LSN prev_lsn, prev_end_lsn;
    SCN prev_scn;
    int64_t prev_proposal_id = INVALID_PROPOSAL_ID;
    int64_t prev_accum_checksum = -1;
    SCN max_scn;
    // double check the prev log is not changed
    bool is_prev_log_exist = false;
    const bool is_prev_log_match = is_prev_log_pid_match(truncate_log_id, truncate_begin_lsn, expected_prev_lsn,
        expected_prev_log_pid, is_prev_log_exist);
    log_task->lock();
    if (false == is_prev_log_match
        || !log_task->is_valid()
        || log_task->get_log_id() != truncate_log_id
        || log_task->get_proposal_id() != truncate_log_proposal_id) {
      ret = OB_STATE_NOT_MATCH;
      PALF_LOG(WARN, "log/prev_log does not match, it may has changed", K(ret), K_(palf_id), K_(self), K(truncate_log_info),
          K(expected_prev_lsn), K(expected_prev_log_pid), KPC(log_task), K(is_prev_log_exist),
          K(is_prev_log_match));
    } else {
      log_begin_lsn = log_task->get_begin_lsn();
      log_end_lsn = log_task->get_begin_lsn() + LogGroupEntryHeader::HEADER_SER_SIZE + log_task->get_data_len();
      prev_lsn = log_task->get_prev_lsn();
      prev_proposal_id = log_task->get_prev_proposal_id();
      max_scn = log_task->get_max_scn();
      log_task->reset();
    }
    log_task->unlock();
    // get prev log info
    if (OB_SUCC(ret)) {
      if (FIRST_VALID_LOG_ID == truncate_log_id) {
        // 第一条日志需要truncate, prev_log_info生成为初始值
        LogInfo prev_log_info;
        prev_log_info.generate_by_default();
        prev_log_id = prev_log_info.log_id_;
        prev_lsn = prev_log_info.lsn_;
        prev_scn = prev_log_info.scn_;
        prev_proposal_id = prev_log_info.log_proposal_id_;
        prev_accum_checksum = prev_log_info.accum_checksum_;
      } else if (OB_FAIL(get_prev_log_info_(truncate_log_id, prev_lsn, prev_end_lsn, prev_scn, prev_proposal_id, prev_accum_checksum))) {
        PALF_LOG(WARN, "get_prev_log_info_ failed when truncate log", K(ret), K_(palf_id), K_(self), K(truncate_log_info), KPC(log_task));
      } else {
        // do nothing
      }
    }
    // NB: must revert log_task manually in here to avoid truncate getting stuck
    guard.revert_log_task();
    // truncate sw and log storage
    if (OB_SUCC(ret)) {
      TruncateLogCbCtx truncate_log_cb_ctx(truncate_begin_lsn);
      LSN committed_end_lsn;
      get_committed_end_lsn_(committed_end_lsn);
      if (truncate_begin_lsn < committed_end_lsn) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "truncate begin lsn is less than committed_end_lsn, unexpected", K(ret), K_(palf_id), K_(self),
            K(truncate_log_info), K(committed_end_lsn));
      } else if (OB_FAIL(sw_.truncate(truncate_log_id))) {
        PALF_LOG(WARN, "sw_ truncate failed", K(ret), K_(palf_id), K_(self), K(truncate_log_info));
      } else if (OB_FAIL(log_engine_->submit_truncate_log_task(truncate_log_cb_ctx))) {
        PALF_LOG(WARN, "log_engine_ truncate failed", K(ret), K_(palf_id), K_(self), K(truncate_log_info));
      } else {
        // update truncating state
        is_truncating_ = true;
        last_truncate_lsn_ = truncate_begin_lsn;

        // truncate max log meta of lsn_allocator
        (void) truncate_lsn_allocator_(truncate_begin_lsn, prev_log_id, prev_scn);
        // truncate last_submit_log info
        // 当被truncate的log已经submit时才需要truncate last_submit_log_info
        const int64_t last_submit_log_id = get_last_submit_log_id_();
        if (last_submit_log_id >= truncate_log_id) {
          (void) set_last_submit_log_info_(prev_lsn, truncate_begin_lsn, prev_log_id, prev_proposal_id);
          // reset accum_checksum only when last_submit_log_id >= log_id (larger than prev_log_id)
          checksum_.set_accum_checksum(prev_accum_checksum);
          PALF_LOG(INFO, "truncate last_submit_log_info_ and accum_checksum", K_(palf_id), K_(self), K(truncate_log_info), K(last_submit_log_id),
              K(prev_accum_checksum));
        }
        // truncate max_flushed_log info
        // 考虑到sw中truncate位点之前可能存在空洞, 故这里需要与当前max_flushed_end_lsn做比较
        LSN max_flushed_end_lsn;
        get_max_flushed_end_lsn(max_flushed_end_lsn);
        if (max_flushed_end_lsn > truncate_begin_lsn) {
          // flush位点已推过，需要truncate
          (void) truncate_max_flushed_log_info_(prev_lsn, truncate_begin_lsn, prev_proposal_id);
          (void) group_buffer_.truncate(truncate_begin_lsn);
          PALF_LOG(INFO, "truncate max_flushed_log_info_", K_(palf_id), K_(self), K(truncate_log_info), K(log_end_lsn),
              "old flushed_end_lsn", max_flushed_end_lsn);
        }
        PALF_LOG(INFO, "truncate success", K(ret), K_(palf_id), K_(self), K(truncate_log_info), "max_log_id", get_max_log_id(),
            K(log_begin_lsn), K(expected_prev_lsn), K(expected_prev_log_pid), K(prev_accum_checksum));
      }
    }
  }
  return ret;
}

int LogSlidingWindow::receive_log(const common::ObAddr &src_server,
                                  const PushLogType push_log_type,
                                  const LSN &prev_lsn,
                                  const int64_t &prev_log_proposal_id,
                                  const LSN &lsn,
                                  const char *buf,
                                  const int64_t buf_len,
                                  const bool need_check_clean_log,
                                  TruncateLogInfo &truncate_log_info)
{
  int ret = OB_SUCCESS;
  int64_t log_id = OB_INVALID_LOG_ID;
  LSN last_submit_lsn;
  LSN last_submit_end_lsn;
  int64_t last_submit_log_id = OB_INVALID_LOG_ID;
  int64_t last_submit_log_pid = INVALID_PROPOSAL_ID;
  get_last_submit_log_info_(last_submit_lsn, last_submit_end_lsn, last_submit_log_id, last_submit_log_pid);
  int64_t log_proposal_id = INVALID_PROPOSAL_ID;
  LSN max_flushed_end_lsn;
  get_max_flushed_end_lsn(max_flushed_end_lsn);
  int64_t pos = 0;
  LogGroupEntryHeader group_entry_header;
  int64_t group_log_data_checksum = 0;
  LSN log_end_lsn;
  LogTaskGuard guard(this);
  LogTask *log_task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!src_server.is_valid() || !lsn.is_valid() || NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(src_server), K(lsn),
        KP(buf), K(buf_len));
  } else if (is_truncating_ || is_rebuilding_) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "sw is truncatiing/rebuilding now, cannot receive log", K(ret), K_(palf_id), K_(self), K_(is_truncating),
        K_(last_truncate_lsn), K_(is_rebuilding), K(src_server), K(lsn), KP(buf), K(buf_len));
  } else if (!group_buffer_.can_handle_new_log(lsn, buf_len)) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      PALF_LOG(WARN, "group_buffer_ cannot handle new log", K(ret), K_(palf_id), K_(self), K(lsn));
    }
  } else if (OB_FAIL(group_entry_header.deserialize(buf, buf_len, pos))) {
    PALF_LOG(WARN, "group_entry_header deserialize failed", K(ret), K_(palf_id), K_(self));
  } else if (!group_entry_header.check_integrity(buf + LogGroupEntryHeader::HEADER_SER_SIZE,
        buf_len - LogGroupEntryHeader::HEADER_SER_SIZE, group_log_data_checksum)) {
    ret = OB_INVALID_DATA;
    PALF_LOG(WARN, "group_entry_header check_integrity failed", K(ret), K_(palf_id), K_(self));
  } else if (FALSE_IT(log_id = group_entry_header.get_log_id())) {
  } else if (!can_receive_larger_log_(log_id)) {
    ret = OB_EAGAIN;
    if (palf_reach_time_interval(5 * 1000 * 1000, larger_log_warn_time_)) {
      PALF_LOG(WARN, "sw is full, cannot receive larger log", K(ret), K_(palf_id), K_(self), K(group_entry_header),
          "start_id", get_start_id());
    }
  } else if (OB_FAIL(guard.get_log_task(log_id, log_task))) {
    if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        PALF_LOG(WARN, "this log has slide out, no need receive", K(ret), K(log_id), K_(palf_id), K_(self));
      }
    } else {
      PALF_LOG(ERROR, "get_log_task failed", K(ret), K(log_id), K_(palf_id), K_(self), K(group_entry_header));
    }
  } else {
    log_proposal_id = group_entry_header.get_log_proposal_id();
    log_end_lsn = lsn + group_entry_header.get_serialize_size() + group_entry_header.get_data_len();
    bool is_prev_log_exist = false;
    const bool is_prev_log_match = is_prev_log_pid_match(log_id, lsn, prev_lsn, \
        prev_log_proposal_id, is_prev_log_exist);
    bool need_send_ack = false;
    bool is_log_pid_match = false;
    SCN min_scn;
    if (is_prev_log_exist && !is_prev_log_match) {
      // prev log exists and its proposal_id does not match with arg, cannot receive this log
      ret = OB_STATE_NOT_MATCH;
      PALF_LOG(WARN, "previous log does not match", K(ret), K_(palf_id), K_(self), K(log_id), K(log_proposal_id),
          K(prev_log_proposal_id), K(is_prev_log_exist), K(group_entry_header));
    }
    // If log_id > last_submit_log_id + 1, check if we can cache it.
    if (OB_SUCC(ret)
        && log_id > (last_submit_log_id + 1)
        && log_proposal_id != last_submit_log_pid) {
      // if its proposal_id does not equal to last_submit_log_pid,
      // and it's not continuous with last_submit_log_id, we cannot receive it.
      // Only logs whose proposal_id is equal to last_submit_log_pid can be cached into sw.
      ret = OB_EAGAIN;
      PALF_LOG(WARN, "new log's proposal_id does not equal to last submit log's, and log_id is not continuous with last "\
          "submit log, cannot receive(cache) it", K(ret), K_(palf_id), K_(self), K(log_id), K(log_proposal_id),
          K(prev_log_proposal_id), K(is_prev_log_exist), K(group_entry_header), K(last_submit_log_id), K(last_submit_log_pid));
    }
    // check if need clean cached log_tasks.
    if (OB_SUCC(ret)
        && is_prev_log_match
        && need_check_clean_log
        && log_id == last_submit_log_id + 1
        && log_proposal_id != last_submit_log_pid) {
      // prev log matches, new log's proposal_id does not equal to last submit log's,
      // and it is continuous with last submit log,
      // check if need clean cached log_tasks.
      if (INVALID_PROPOSAL_ID != last_submit_log_pid && log_proposal_id < last_submit_log_pid) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "new log's proposal_id is smaller than last submit log, unexpected", K(ret),
            K_(palf_id), K_(self), K(log_id), K(log_proposal_id), K(is_prev_log_exist),
            K(group_entry_header), K(last_submit_log_id), K(last_submit_log_pid));
      } else {
        ret = OB_EAGAIN;
        // new log's proposal_id is larger than last submit log, and prev log check successfully,
        // we need clean cached log_tasks whose log_id is larger than this log.
        // And we reuse the truncate_log_id_ of truncate_log_info to output dest log_id.
        truncate_log_info.truncate_type_ = TRUNCATE_CACHED_LOG_TASK;
        truncate_log_info.truncate_log_id_ = log_id;
      }
      PALF_LOG(INFO, "new log's proposal_id does not equal to last submit log's, and it is continuous, "\
          "need clean cached log_tasks beyond this log", K(ret), K_(palf_id), K_(self), K(log_id), K(log_proposal_id),
          K(is_prev_log_exist), K(group_entry_header), K(last_submit_log_id), K(last_submit_log_pid));
    }

    // check if need update log_task.
    if (OB_SUCC(ret)) {
      bool is_local_log_valid = false;
      if (!need_update_log_task_(group_entry_header, log_task, need_send_ack, is_local_log_valid, is_log_pid_match)) {
        PALF_LOG(INFO, "no need update log", K(log_id), K_(palf_id), K_(self), K(need_send_ack), K(is_log_pid_match),
            K(is_local_log_valid), K(is_prev_log_exist), K(is_prev_log_match), K(group_entry_header), KPC(log_task));
        if (false == is_local_log_valid) {
          // local log_task is invalid, and it does not need update, this means that it's maybe in PRE_FILL state.
        } else if (false == is_log_pid_match) {
          // local log_task's proposal_id does not match with new log.
          //
          // (log_id <= last_submit_log_id) must be true.
          //
          // Because if log_id > last_submit_log_id, there are only too cases:
          // 1) new log_proposal_id != last_submit_log_pid, it cannot reach here, because it cannot be received.
          //
          // 2) new log_proposal_id == last_submit_log_pid, the local log_task's propsal_id should be equal
          //      with last_submit_log_pid too. If not, local log shouldn't exist (it should already be
          //      truncated by previous receive operation).
          //
          // In summary, (log_id <= last_submit_log_id) must be true.
          if (lsn <= last_submit_end_lsn) {
            // It means that this log is the first mismatch one with request server,
            // because it has passed prev log check.
            // We need truncate log at this log's begin lsn.
            truncate_log_info.truncate_type_ = TRUNCATE_LOG;
            truncate_log_info.truncate_log_id_ = log_id;
            // lsn is expected to be equal to log_task's end_lsn.
            // So we can use lsn as the truncate_begin_lsn_.
            truncate_log_info.truncate_begin_lsn_ = lsn;
            log_task->lock();
            truncate_log_info.truncate_log_proposal_id_ = log_task->get_proposal_id();
            log_task->unlock();
            // return -4023 when need truncate
            ret = OB_EAGAIN;
            PALF_LOG(WARN, "need truncate log", K(ret), K(log_id), K_(palf_id), K_(self), K(lsn), K(last_submit_end_lsn),
                K(last_submit_log_id), KPC(log_task), K(is_prev_log_exist), K(is_prev_log_match),
                K(truncate_log_info));
          } else {
            // Unexpected case:
            // (log_id <= last_submit_log_id) and (lsn > last_submit_end_lsn).
            // If the expected truncating log is some previous one,
            // this log cannot pass the prev log check.
            ret = OB_ERR_UNEXPECTED;
            PALF_LOG(ERROR, "lsn > last_submit_end_lsn and log_id <= last_submit_log_id, \
                and local log_task's proposal_id != arg proposal_id, unexpected",
                K(ret), K(log_id), K_(palf_id), K_(self), K(lsn), K(last_submit_end_lsn), K(last_submit_log_id), KPC(log_task),
                K(is_prev_log_exist), K(is_prev_log_match));
          }
          if (state_mgr_->is_leader_reconfirm()) {
            // NB: In leader reconfirm, it's not possibale truncate log, because we guarantee that:
            //     1. election leader has the highest config version;
            //     2. changing config must need forward check;
            //     3. receiving config log need clean rubbish logs afterwards;
            //     3. only after start working, election leader can become palf leader.
            ret = OB_ERR_UNEXPECTED;
            PALF_LOG(ERROR, "unexpected truncate for reconfirm state", K(ret), K(log_id), K_(palf_id), K_(self), K(need_send_ack),
                K(truncate_log_info), K(group_entry_header), KPC(log_task), K(src_server), K(push_log_type));
          }
        } else if (need_send_ack) {
          // This log matches with msg and it has been flushed, just sending ack directly.
          const int64_t curr_proposal_id = state_mgr_->get_proposal_id();
          if (OB_FAIL(submit_push_log_resp_(src_server, curr_proposal_id, log_end_lsn))) {
            PALF_LOG(WARN, "submit_push_log_resp failed", K(ret), K_(palf_id), K_(self), K(src_server));
          } else {
            PALF_LOG(INFO, "submit_push_log_resp succ", K(ret), K_(palf_id), K_(self), K(src_server), K(curr_proposal_id),
                K(log_proposal_id), K(log_end_lsn), K(group_entry_header));
          }
        } else {
          // This log_task matches with msg and does not need to update.
        }
      } else {
        // Need update log_task, it means that PRE_FILL tag must be set by self.
        if (lsn < max_flushed_end_lsn) {
          // lsn比max_flushed_end_lsn小, prev log可能不存在
          // 这种日志后续可能需要truncate(比如<log_id, lsn>与源端不一致), 为了避免覆盖本地已有日志需先拒收，
          // 等前面的日志更新后重新拉取
          ret = OB_STATE_NOT_MATCH;
          PALF_LOG(WARN, "lsn is smaller than max_flushed_end_lsn, this log may need truncate later, cannot receive",
              K(ret), K_(palf_id), K_(self), K(lsn), K(log_end_lsn), K(max_flushed_end_lsn), K(group_entry_header),
              K(is_prev_log_exist), K(is_prev_log_match), K(last_submit_log_id), K(last_submit_log_pid));
        } else if (lsn < last_submit_end_lsn) {
          // lsn < last_submit_end_lsn, it is unexpected.
          // This log should have been processed(no need update) or has slid out(get log_task will encounter OUT OF LOWER BOUND).
          // For any case, it shouldn't run here.
          ret = OB_ERR_UNEXPECTED;
          PALF_LOG(ERROR, "lsn is smaller than last_submit_end_lsn, unexpected",
              K(ret), K_(palf_id), K_(self), KPC(log_task), K(lsn), K(log_end_lsn), K(max_flushed_end_lsn), K(group_entry_header),
              K(is_prev_log_exist), K(is_prev_log_match), K(last_submit_log_id), K(last_submit_log_pid),
              K(last_submit_log_id), K(last_submit_lsn), K(last_submit_end_lsn));
        } else if (OB_FAIL(get_min_scn_from_buf_(group_entry_header, buf + LogGroupEntryHeader::HEADER_SER_SIZE,
                buf_len - LogGroupEntryHeader::HEADER_SER_SIZE, min_scn))) {
          PALF_LOG(WARN, "get_min_scn_from_buf_ failed", K(ret), K_(palf_id), K_(self), K(lsn), K(group_entry_header));
        // local log_task is invalid, receive it.
        // 这里不需要wait_group_buffer_ready_，因为lsn是确定且唯一的，前置检查通过即可
        // fill需做内存copy, 故不能持有log_task的锁, 通过PRE_FILL控制至多一个线程执行fill.
        } else if (OB_FAIL(group_buffer_.fill(lsn, buf, buf_len))) {
          PALF_LOG(ERROR, "fill group buffer failed", K(ret), K_(palf_id), K_(self), K(group_entry_header));
        } else if (OB_FAIL(try_update_max_lsn_(lsn, group_entry_header))) {
          PALF_LOG(WARN, "try_update_max_lsn_ failed", K(ret), K_(palf_id), K_(self), K(lsn), K(group_entry_header));
        } else {
          log_task->lock();
          if (log_task->is_valid()) {
            // log_task可能被其他线程并发收取了,预期内容与本线程一致.
            ret = OB_ERR_UNEXPECTED;
            PALF_LOG(ERROR, "log_task has been updated during filling group buffer, unexpected", K(ret), K_(palf_id),
                K_(self), K(lsn), K(group_entry_header), KPC(log_task));
          } else if (OB_FAIL(log_task->set_group_header(lsn, min_scn, group_entry_header))) {
            PALF_LOG(ERROR, "log_task->set_group_header failed", K(ret), K_(palf_id), K_(self), K(group_entry_header));
          } else {
            // update prev_lsn
            log_task->set_prev_lsn(prev_lsn);
            // update prev_log_proposal_id
            log_task->set_prev_log_proposal_id(prev_log_proposal_id);
            // update group log data_checksum
            log_task->set_group_log_checksum(group_log_data_checksum);
            (void) log_task->set_freezed();
            log_task->set_freeze_ts(ObTimeUtility::current_time());
          }
          log_task->unlock();
        }
        // Reset PRE_FILL tag unconditionally.
        log_task->reset_pre_fill();
      }
      PALF_LOG(TRACE, "receive_log", K(ret), K_(palf_id), K_(self), K(src_server), K(group_entry_header),
          K(log_id), KPC(log_task));
      // check if need reset last fetch log info
      LSN last_fetch_end_lsn;
      last_fetch_end_lsn.val_ = ATOMIC_LOAD(&last_fetch_end_lsn_.val_);
      if (OB_SUCC(ret)
          && PUSH_LOG == push_log_type
          && last_fetch_end_lsn.is_valid()
          && log_end_lsn <= last_fetch_end_lsn) {
        // 只有当收到push log的end_lsn和log_id均处于last fetch范围内时
        // 才能重置fetch info
        (void) try_reset_last_fetch_log_info_(last_fetch_end_lsn, log_id);
      }
    }
  }

  if (OB_SUCC(ret)) {
    bool is_committed_lsn_updated = false;
    // try handle submit log
    if (log_id == get_last_submit_log_id_() + 1) {
      (void) handle_next_submit_log_(is_committed_lsn_updated);
    }
    if (is_committed_lsn_updated) {
      // handle committed logs
      (void) handle_committed_log_();
    }
    // check and try fetch log for proposal_id increasing log
    if (false == need_check_clean_log
        && log_id == last_submit_log_id + 1
        && log_proposal_id != last_submit_log_pid) {
      // this log will leads to last_submit_log_pid increase,
      // some logs with new proposal_id may have been dropped before it arrives,
      // so we need trigger fetching to get these dropped logs quickly.
      (void) try_fetch_log(CLEAN_CACHED_LOG, lsn, log_end_lsn, log_id + 1);
    }
  }
  return ret;
}

int LogSlidingWindow::submit_push_log_resp(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    // Follower replies local committed_end_lsn to reconfirming leader.
    const int64_t curr_proposal_id = state_mgr_->get_proposal_id();
    LSN committed_end_lsn;
    get_committed_end_lsn_(committed_end_lsn);
    ret = submit_push_log_resp_(server, curr_proposal_id, committed_end_lsn);
  }
  return ret;
}

int LogSlidingWindow::submit_push_log_resp_(const common::ObAddr &server,
                                            const int64_t &msg_proposal_id,
                                            const LSN &log_end_lsn)
{
  int ret = OB_SUCCESS;
  if (state_mgr_->is_allow_vote() && OB_FAIL(log_engine_->submit_push_log_resp(server, msg_proposal_id, log_end_lsn))) {
    PALF_LOG(WARN, "submit_push_log_resp failed", K(ret), K_(palf_id), K_(self), K(server), K(log_end_lsn));
  }
  return ret;
}

int LogSlidingWindow::submit_group_log(const LSN &lsn,
                                       const char *buf,
                                       const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t log_id = OB_INVALID_LOG_ID;
  const int64_t curr_proposal_id = state_mgr_->get_proposal_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!lsn.is_valid() || NULL == buf || buf_len <= 0 || buf_len > MAX_LOG_BUFFER_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(lsn),
        KP(buf), K(buf_len));
  } else if (is_truncating_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "sw is executing truncation now, unexpected state", K(ret), K_(palf_id), K_(self),
        K(lsn), KP(buf), K(buf_len));
  } else {
    LogGroupEntryHeader group_entry_header;
    LogTask *log_task = NULL;
    LogTaskGuard guard(this);
    int64_t group_log_data_checksum = 0;
    int64_t pos = 0;
    LSN last_slide_end_lsn;
    get_last_slide_end_lsn_(last_slide_end_lsn);

    if (OB_FAIL(group_entry_header.deserialize(buf, buf_len, pos))) {
      PALF_LOG(WARN, "group_entry_header deserialize failed", K(ret), K_(palf_id), K_(self));
    } else if (lsn < last_slide_end_lsn && group_entry_header.get_log_id() < get_start_id()) {
      // raw_write may submit an old group_log which is smaller than start log of sw,
      // just return success for this case.
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        PALF_LOG(INFO, "this group_log has slid out, no need submit", K(ret), K_(palf_id), K_(self),
            K(lsn), K(last_slide_end_lsn), K(group_entry_header));
      }
    } else if (!leader_can_submit_group_log_(lsn, buf_len)) {
      LSN curr_max_lsn;
      (void) lsn_allocator_.get_curr_end_lsn(curr_max_lsn);
      if (lsn >= curr_max_lsn && group_entry_header.get_log_id() < get_start_id()) {
        // This group log's lsn is larger than max_lsn, but its log_id is less than sw_start_id.
        // This secenio is unexpected.
        ret = OB_ERR_UNEXPECTED;
      } else {
        ret = OB_EAGAIN;
      }
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        PALF_LOG(WARN, "leader cannot submit group log", K(ret), K_(palf_id), K_(self), K(lsn), K(buf_len));
      }
    } else if (!group_entry_header.check_integrity(buf + LogGroupEntryHeader::HEADER_SER_SIZE,
          buf_len - LogGroupEntryHeader::HEADER_SER_SIZE, group_log_data_checksum)) {
      ret = OB_INVALID_DATA;
      PALF_LOG(WARN, "group_entry_header check_integrity failed", K(ret), K_(palf_id), K_(self));
    } else if (!leader_can_submit_larger_log_(group_entry_header.get_log_id())) {
      ret = OB_EAGAIN;
      PALF_LOG(WARN, "sw is full, cannot receive larger log", K(ret), K_(palf_id), K_(self), K(group_entry_header),
          "start_id", get_start_id(), K(lsn));
    } else if ((log_id = group_entry_header.get_log_id()) < get_start_id()) {
      PALF_LOG(WARN, "this log has slided out, no need receive", K(ret), K_(palf_id), K_(self),
          K(group_entry_header), "start_id", get_start_id());
    // update log proposal_id
    } else if (OB_FAIL(group_entry_header.update_log_proposal_id(curr_proposal_id))) {
      PALF_LOG(WARN, "group_entry_header update_log_proposal_id failed", K(ret), K_(palf_id), K_(self));
    } else if (FALSE_IT(group_entry_header.update_write_mode(true))) {
    } else {
      // update header's committed_end_lsn
      LSN commited_end_lsn;
      get_committed_end_lsn_(commited_end_lsn);
      if (OB_FAIL(group_entry_header.update_committed_end_lsn(commited_end_lsn))) {
        PALF_LOG(WARN, "update commited_end_lsn failed", K(ret), K(commited_end_lsn));
      } else if (OB_FAIL(guard.get_log_task(log_id, log_task))) {
        if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
          PALF_LOG(WARN, "this log has slide out, no need receive", K(ret), K(log_id), K_(palf_id), K_(self));
        } else {
          PALF_LOG(ERROR, "get_log_task failed", K(ret), K(log_id), K_(palf_id), K_(self));
        }
      } else {
        // get log_task success
      }
      if (OB_SUCC(ret)) {
        SCN min_scn;
        if (log_task->is_valid()) {
          if (lsn != log_task->get_begin_lsn()
              || group_entry_header.get_max_scn() != log_task->get_max_scn()
              || group_entry_header.get_accum_checksum() != log_task->get_accum_checksum()) {
            ret = OB_ERR_UNEXPECTED;
            PALF_LOG(ERROR, "local log_task is valid, but its content does not match "\
                "with argument, unexpected", K(ret), K_(palf_id), K_(self), K(lsn), K(group_entry_header), KPC(log_task));
          } else {
            PALF_LOG(INFO, "log_task is already valid, no need receive log", K(ret), K(log_id), K_(palf_id), K_(self),
                K(group_entry_header), KPC(log_task));
          }
        } else if (OB_FAIL(get_min_scn_from_buf_(group_entry_header, buf + LogGroupEntryHeader::HEADER_SER_SIZE,
                buf_len - LogGroupEntryHeader::HEADER_SER_SIZE, min_scn))) {
          PALF_LOG(WARN, "get_min_scn_from_buf_ failed", K(ret), K_(palf_id), K_(self));
        // 这里不需要wait_group_buffer_ready_，因为lsn是确定且唯一的，因此前置检查通过即可.
        // fill期间不能持有log_task的锁, 因为耗时可能较长.
        } else if (OB_FAIL(group_buffer_.fill(lsn, buf, buf_len))) {
          PALF_LOG(WARN, "fill group buffer failed", K(ret), K_(palf_id), K_(self));
        } else if (OB_FAIL(try_update_max_lsn_(lsn, group_entry_header))) {
          PALF_LOG(WARN, "try_update_max_lsn_ failed", K(ret), K_(palf_id), K_(self), K(lsn), K(group_entry_header));
        } else {
          // prev_log_proposal_id match or not exist, receive this log
          log_task->lock();
          if (log_task->is_valid()) {
            // log_task可能被其他线程并发收取了,预期内容与本线程一致.
            if (group_entry_header.get_log_proposal_id() != log_task->get_proposal_id()) {
              ret = OB_ERR_UNEXPECTED;
              PALF_LOG(ERROR, "log_task has been updated during filling group buffer, and log proposal_id does not match "\
                  "with this req, unexpected", K(ret), K_(palf_id), K_(self), K(lsn), K(group_entry_header), KPC(log_task));
            }
          } else if (OB_FAIL(log_task->set_group_header(lsn, min_scn, group_entry_header))) {
            PALF_LOG(WARN, "log_task->set_group_header failed", K(ret), K_(palf_id), K_(self));
          } else {
            // update group log data_checksum
            log_task->set_group_log_checksum(group_log_data_checksum);
            (void) log_task->set_submit_log_exist();
            (void) log_task->set_freezed();
            log_task->set_freeze_ts(ObTimeUtility::current_time());
          }
          log_task->unlock();

          PALF_LOG(TRACE, "submit_group_log", K(ret), K_(palf_id), K_(self), K(group_entry_header),
              K(log_id), KPC(log_task));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    bool is_committed_lsn_updated = false;
    // try submit log
    const int64_t last_submit_log_id = get_last_submit_log_id_();
    if (log_id == last_submit_log_id + 1) {
      (void) handle_next_submit_log_(is_committed_lsn_updated);
    }
    // handle committed logs
    if (is_committed_lsn_updated) {
      (void) handle_committed_log_();
    }
  }
  return ret;
}

bool LogSlidingWindow::need_update_log_task_(LogGroupEntryHeader &header,
                                             LogTask *log_task,
                                             bool &need_send_ack,
                                             bool &is_local_log_valid,
                                             bool &is_log_pid_match) const
{
  bool bool_ret = false;
  const int64_t log_id = header.get_log_id();
  if (IS_NOT_INIT) {
  } else if (!header.is_valid() || NULL == log_task) {
    PALF_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid argumetns", K_(palf_id), K_(self), K(header), KP(log_task));
  } else {
    int64_t old_pid = INVALID_PROPOSAL_ID;
    LSN log_end_lsn;

    log_task->lock();
    if (!log_task->is_valid()) {
      if (log_task->try_pre_fill()) {
        bool_ret = true;
      } else {
        // Other thread has set PRE_FILL tag, skip update.
        bool_ret = false;
      }
    } else {
      is_local_log_valid = true;
      old_pid = log_task->get_proposal_id();
      log_end_lsn = (log_task->get_begin_lsn() + LogGroupEntryHeader::HEADER_SER_SIZE + log_task->get_data_len());
    }
    log_task->unlock();

    if (false == bool_ret) {
      int64_t new_pid = header.get_log_proposal_id();
      if (INVALID_PROPOSAL_ID == old_pid) {
        // current pid of log_task is invalid, it's maybe in PRE_FILL state, skip
      } else if (old_pid == new_pid) {
        is_log_pid_match = true;
        // check if need send ack
        LSN max_flushed_end_lsn;
        get_max_flushed_end_lsn(max_flushed_end_lsn);
        if (max_flushed_end_lsn >= log_end_lsn) {
          need_send_ack = true;
        }
        PALF_LOG(INFO, "receive log with same proposal_id", K(bool_ret), K_(palf_id), K_(self), K(log_id), K(log_end_lsn),
            K(need_send_ack), K(old_pid), KPC(log_task), K(header), K(max_flushed_end_lsn), KPC(log_task));
      } else if (old_pid > new_pid) {
        // 收到了proposal_id更小的日志，预期不应该出现.
        // 这里间接依赖了: follower响应fetch log请求时只能返回committed logs, 因此在同一个proposal_id下,
        // 不会有两个副本同时发送log_id相同但proposal_id不同的日志给同一个目的端.
        PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "receive log with smaller proposal_id, unexpected error", K_(palf_id), K_(self), K(log_id),
                   K(new_pid), KPC(log_task), K(header), KPC(log_task));
      } else {
        PALF_LOG(INFO, "receive log with larger proposal_id, maybe need truncate", K_(palf_id), K_(self), K(log_id),
                   K(new_pid), K(header), KPC(log_task));
      }
    }
  }
  return bool_ret;
}

int LogSlidingWindow::get_prev_log_info_(const int64_t log_id,
                                         LSN &prev_lsn,
                                         LSN &prev_end_lsn,
                                         SCN &prev_scn,
                                         int64_t &prev_log_pid,
                                         int64_t &prev_log_accum_checksum)
{
  int ret = OB_SUCCESS;
  if (log_id < 1) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(log_id));
  } else if (FIRST_VALID_LOG_ID == log_id) {
    ret = OB_ENTRY_NOT_EXIST;
    PALF_LOG(INFO, "this is the first log, previous log not exist", K(ret),
        K(log_id), K_(palf_id), K_(self));
  } else {
    const int64_t prev_log_id = log_id - 1;
    const int64_t start_id = get_start_id();
    LogTask *prev_log_task = NULL;
    LogTaskGuard guard(this);
    if (OB_FAIL(guard.get_log_task(prev_log_id, prev_log_task))) {
      if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
        // log has slide out, need check last_slide_log info
        ret = OB_SUCCESS;
        if (start_id == log_id) {
          int64_t last_slide_log_id = OB_INVALID_LOG_ID;
          SCN last_slide_scn;
          LSN last_slide_lsn;
          LSN last_slide_end_lsn;
          int64_t last_slide_log_pid = INVALID_PROPOSAL_ID;
          int64_t last_slide_accum_checksum = -1;
          get_last_slide_log_info_(last_slide_log_id, last_slide_scn, last_slide_lsn, \
              last_slide_end_lsn, last_slide_log_pid, last_slide_accum_checksum);
          if (get_start_id() == start_id) {
            // double check log_id == start_id
            prev_lsn = last_slide_lsn;
            prev_end_lsn = last_slide_end_lsn;
            prev_scn = last_slide_scn;
            prev_log_pid = last_slide_log_pid;
            prev_log_accum_checksum = last_slide_accum_checksum;
          } else {
            ret = OB_ENTRY_NOT_EXIST;
          }
        } else {
          ret = OB_ENTRY_NOT_EXIST;
          PALF_LOG(WARN, "prev log has slide out, but log_id is not equal to start_id",
              K(ret), K_(palf_id), K_(self), K(log_id), K(start_id));
        }
      } else {
        PALF_LOG(ERROR, "get_log_task failed", K(ret), K(log_id), K_(palf_id), K_(self));
      }
    } else {
      prev_log_task->lock();
      if (!prev_log_task->is_valid()) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        prev_lsn = prev_log_task->get_begin_lsn();
        prev_end_lsn = prev_log_task->get_begin_lsn() + LogGroupEntryHeader::HEADER_SER_SIZE
              + prev_log_task->get_data_len();
        prev_scn = prev_log_task->get_max_scn();
        prev_log_pid = prev_log_task->get_proposal_id();
        prev_log_accum_checksum = prev_log_task->get_accum_checksum();
      }
      prev_log_task->unlock();
    }
  }
  return ret;
}

bool LogSlidingWindow::pre_check_for_config_log(const int64_t &msg_proposal_id,
                                                const LSN &lsn,
                                                const int64_t &log_proposal_id,
                                                TruncateLogInfo &truncate_log_info)
{
  // lsn may be invalid (before writing the first log)
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;
  int64_t last_slide_log_id = OB_INVALID_LOG_ID;
  SCN last_slide_scn;
  LSN last_slide_lsn;
  LSN last_slide_end_lsn;
  int64_t last_slide_log_pid = INVALID_PROPOSAL_ID;
  int64_t last_slide_accum_checksum = -1;
  get_last_slide_log_info_(last_slide_log_id, last_slide_scn, last_slide_lsn, \
      last_slide_end_lsn, last_slide_log_pid, last_slide_accum_checksum);
  int64_t match_log_id = OB_INVALID_LOG_ID;
  LSN match_log_end_lsn;
  if (IS_NOT_INIT) {
    bool_ret = false;
  } else if (PALF_INITIAL_LSN_VAL == lsn.val_ && INVALID_PROPOSAL_ID == log_proposal_id) {
    // prev log of this config log is the first invalid log, no need do pre check
    bool_ret = true;
    match_log_id = FIRST_VALID_LOG_ID - 1;
  } else if (FIRST_VALID_LOG_ID <= last_slide_log_id
             && last_slide_lsn >= lsn) {
    // the last_slide_log is valid and its lsn >= lsn
    if (last_slide_lsn == lsn && last_slide_log_pid == log_proposal_id) {
      // 1. When last_slide_lsn == lsn, that means log matches.
      bool_ret = true;
      match_log_id = last_slide_log_id;
      match_log_end_lsn = last_slide_end_lsn;
    } else if (last_slide_lsn > lsn && last_slide_log_pid >= log_proposal_id) {
      // 2. When last_slide_lsn > lsn, and log_proposal_id of last_slide_log >= log_proposal_id,
      //    that means:
      //    case 1. prev log may has been committed by this leader
      //    case 2. prev log may has been truncated by another new leader
      //    if case 2 happens, that means old leader and new leader send config log/log with same proposal_id,
      //    it's impossible in our consensus protocol.
      //    Therefore only case 1 is possible, we can receive this config log safely.
      bool_ret = true;
      PALF_LOG(INFO, "last_slide_log_pid matches with arg or lsn has slid", K(bool_ret), K_(palf_id), K_(self), K(msg_proposal_id),
          K(last_slide_lsn), K(last_slide_log_pid), K(lsn), K(log_proposal_id));
    } else if (last_slide_lsn > lsn && last_slide_log_pid < log_proposal_id) {
      // 3. When last_slide_lsn > lsn, and log_proposal_id of last_slide_log < log_proposal_id
      //    that means more old/new logs with smaller/begger proposal_id have been committed,
      //    but current leader did not reconfirm them, it is unexpected error case!!!
      bool_ret = false;
      PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "last_slide_log_pid is unexpected", K(bool_ret), K_(palf_id), K_(self), K(msg_proposal_id),
          K(last_slide_lsn), K(last_slide_log_pid), K(lsn), K(log_proposal_id));
    } else {
      bool_ret = false;
      tmp_ret = OB_STATE_NOT_MATCH;
      PALF_LOG_RET(WARN, tmp_ret, "log proposal_id does not match", K(bool_ret), K(msg_proposal_id), K_(palf_id), K_(self),
          K(lsn), K(last_slide_lsn), K(last_slide_log_pid), K(log_proposal_id));
    }
  } else {
    LSN max_flushed_end_lsn;
    get_max_flushed_end_lsn(max_flushed_end_lsn);
    for (int64_t tmp_log_id = get_start_id(); !bool_ret && (OB_SUCCESS == tmp_ret); ++tmp_log_id) {
      LogTask *log_task = NULL;
      LogTaskGuard guard(this);
      if (OB_SUCCESS != (tmp_ret = guard.get_log_task(tmp_log_id, log_task))) {
        PALF_LOG_RET(WARN, tmp_ret, "get_log_task failed", K(tmp_ret), K_(palf_id), K_(self), K(tmp_log_id));
      } else {
        log_task->lock();
        if (!log_task->is_valid()) {
          tmp_ret = OB_ENTRY_NOT_EXIST;
          PALF_LOG_RET(WARN, tmp_ret, "this log_task is invalid", K(tmp_ret), K(tmp_log_id), K_(palf_id), K_(self), KPC(log_task));
        } else {
          const LSN curr_lsn = log_task->get_begin_lsn();
          const LSN curr_log_end_lsn = (log_task->get_begin_lsn() + LogGroupEntryHeader::HEADER_SER_SIZE
              + log_task->get_data_len());
          const int64_t curr_log_proposal_id = log_task->get_proposal_id();
          if (curr_lsn == lsn) {
            if (curr_log_proposal_id == log_proposal_id) {
              // proposal_id matches
              if (max_flushed_end_lsn >= curr_log_end_lsn) {
                // this log has been flushed or no need check if it's flushed
                bool_ret = true;
                match_log_id = tmp_log_id;
                match_log_end_lsn = curr_log_end_lsn;
              } else {
                // this log has not been flushed
                PALF_LOG_RET(WARN, OB_STATE_NOT_MATCH, "local log is match with arg, but it has not been flushed", K(bool_ret),
                    K(msg_proposal_id), K_(palf_id), K_(self), K(max_flushed_end_lsn), K(curr_log_end_lsn),
                    K(lsn), K(log_proposal_id));
                int local_ret = OB_SUCCESS;
                if (OB_SUCCESS != (local_ret = log_engine_->submit_purge_throttling_task(PURGE_BY_PRE_CHECK_FOR_CONFIG))) {
                  PALF_LOG_RET(WARN, local_ret, "submit_purge_throttling_task failed", K_(palf_id), K_(self));
                }
              }
            } else {
              // proposal_id does not match with arg
              tmp_ret = OB_STATE_NOT_MATCH;
              PALF_LOG_RET(WARN, tmp_ret, "log proposal_id dest not match", K(bool_ret), K(msg_proposal_id), K_(palf_id), K_(self),
                 K(lsn), KPC(log_task));
            }
          } else if (curr_lsn > lsn) {
            // curr_lsn > lsn, dest log was not found
            tmp_ret = OB_ENTRY_NOT_EXIST;
            PALF_LOG_RET(WARN, tmp_ret, "curr_lsn is larger than than arg lsn, the request is maybe stale",
                K(bool_ret), K(msg_proposal_id), K_(palf_id), K_(self), K(curr_lsn), K(lsn), K(log_proposal_id));
          } else {
            // curr_lsn < lsn, need check next log_task
          }
        }
        log_task->unlock();
      }
    }
  }
  if (bool_ret && OB_INVALID_LOG_ID != match_log_id) {
    // if log matches, check if need truncate following logs
    // 对于已经提交写盘/即将提交写盘(无空洞)的幽灵日志(proposal_id < msg_proposal_id),
    // 需要立即执行truncate,
    // 对于空洞之后无法提交写盘的幽灵日志，本轮leader未感知到它们的存在，可暂不处理，
    // 因为这部分日志之后也不会提交写盘(msg_proposal_id确保空洞位置无法再收取旧主的日志)，
    // 后续如果自己当选为leader，依赖reconfirm前truncate掉所有未提交写盘日志的逻辑清理掉.
    // 为了简化处理，我们只根据下一条日志决定是否truncate:
    // - 如果下一条日志是空洞，那么空洞位置一定无法再收旧主的旧日志，故空洞之后的幽灵
    //   日志也不会写盘；
    // - 如果下一条日志不是空洞，判断其proposal_id若比msg_proposal_id小，说明是幽灵
    //   日志，直接触发truncate，否则不是幽灵日志，无需处理.
    LogTask *log_task = NULL;
    LogTaskGuard guard(this);
    const int64_t next_log_id = match_log_id + 1;
    if (OB_SUCCESS != (tmp_ret = guard.get_log_task(next_log_id, log_task))) {
      PALF_LOG_RET(WARN, tmp_ret, "get_log_task failed", K(tmp_ret), K_(palf_id), K_(self), K(next_log_id));
    } else {
      log_task->lock();
      if (!log_task->is_valid()) {
        PALF_LOG(INFO, "next log_task is invalid, no need truncate", K(next_log_id),
            K_(palf_id), K_(self), KPC(log_task));
      } else if (log_task->get_proposal_id() < msg_proposal_id) {
        truncate_log_info.truncate_type_ = TRUNCATE_LOG;
        truncate_log_info.truncate_log_id_ = next_log_id;
        if (!match_log_end_lsn.is_valid()) {
          // match_log_id is 0, the match_log_end_lsn will be invalid,
          // it means there is no prev log for this config_log.
          // Here we need set truncate_begin_lsn_ to the begin_lsn of the first log.
          assert(FIRST_VALID_LOG_ID == next_log_id);
          truncate_log_info.truncate_begin_lsn_ = log_task->get_begin_lsn();
        } else {
          truncate_log_info.truncate_begin_lsn_ = match_log_end_lsn;
        }
        truncate_log_info.truncate_log_proposal_id_ = log_task->get_proposal_id();
        PALF_LOG_RET(WARN, OB_ERR_UNEXPECTED, "next log proposal_id is less than msg_proposal_id, need truncate", K_(palf_id), K_(self),
            K(next_log_id), K(msg_proposal_id), K(truncate_log_info), KPC(log_task));
      } else {
        PALF_LOG(INFO, "next log proposal_id is not less than msg_proposal_id, no need truncate",
            K_(palf_id), K_(self), K(next_log_id), K(msg_proposal_id), KPC(log_task), K(truncate_log_info));
      }
      log_task->unlock();
    }
  }

  return bool_ret;
}

bool LogSlidingWindow::is_prev_log_pid_match(const int64_t log_id,
                                             const LSN &lsn,
                                             const LSN &prev_lsn,
                                             const int64_t &prev_log_pid,
                                             bool &is_prev_log_exist)
{
  bool bool_ret = false;
  is_prev_log_exist = false;
  LSN local_prev_lsn, local_prev_end_lsn;
  SCN local_prev_scn;
  int64_t local_prev_log_pid = INVALID_PROPOSAL_ID;
  int64_t local_prev_log_accum_checksum = -1;
  int tmp_ret = OB_SUCCESS;
  if (OB_INVALID_LOG_ID == log_id || !lsn.is_valid()) {
    PALF_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid argumetns", K(log_id), K_(palf_id), K_(self), K(lsn));
  } else if (FIRST_VALID_LOG_ID == log_id) {
    bool_ret = true;
    PALF_LOG(INFO, "this is the first log, no need check prev log proposal_id", K(log_id),
        K_(palf_id), K_(self), K(lsn), K(prev_log_pid));
  } else if (OB_SUCCESS != (tmp_ret = get_prev_log_info_(log_id, local_prev_lsn, local_prev_end_lsn,
          local_prev_scn, local_prev_log_pid, local_prev_log_accum_checksum))) {
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      PALF_LOG_RET(WARN, tmp_ret, "get_prev_log_info_ failed", K(tmp_ret), K(log_id), K_(palf_id), K_(self), K(log_id),
          K(lsn), K(prev_lsn), K(prev_log_pid));
    }
  } else {
    is_prev_log_exist = true;
    if (local_prev_log_pid == prev_log_pid
        && local_prev_lsn == prev_lsn
        && local_prev_end_lsn == lsn) {
      // We also need check if end_lsn of prev log matches with next lsn, because the group entry
      // may be truncated at some inner log entry for flashback case.
      bool_ret = true;
    } else {
      bool_ret = false;
      PALF_LOG_RET(WARN, OB_ERR_UNEXPECTED, "prev_log_task does not match with arg", K(log_id), K_(palf_id), K_(self), K(lsn),
          K(prev_lsn), K(prev_log_pid), K(local_prev_lsn), K(local_prev_end_lsn), K(local_prev_log_pid));
    }
  }
  return bool_ret;
}

int LogSlidingWindow::get_committed_end_lsn(LSN &committed_end_lsn) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    get_committed_end_lsn_(committed_end_lsn);
  }
  return ret;
}

void LogSlidingWindow::get_committed_end_lsn_(LSN &out_lsn) const
{
  out_lsn.val_ = ATOMIC_LOAD(&committed_end_lsn_.val_);
}

bool LogSlidingWindow::is_empty() const
{
  return get_max_log_id() == (sw_.get_begin_sn() - 1);
}

int64_t LogSlidingWindow::get_max_log_id() const
{
  return lsn_allocator_.get_max_log_id();
}

LSN LogSlidingWindow::get_max_lsn() const
{
  LSN max_lsn;
  (void) lsn_allocator_.get_curr_end_lsn(max_lsn);
  return max_lsn;
}

const SCN LogSlidingWindow::get_max_scn() const
{
  return lsn_allocator_.get_max_scn();
}

int LogSlidingWindow::get_majority_match_lsn(LSN &majority_match_lsn)
{
  int ret = OB_SUCCESS;
  ObMemberList member_list;
  int64_t replica_num = 0;
  LSN result_lsn;
  if (OB_FAIL(mm_->get_log_sync_member_list(member_list, replica_num))) {
    PALF_LOG(WARN, "get_log_sync_member_list failed", K(ret), KPC(this));
  } else if (OB_FAIL(get_majority_lsn_(member_list, replica_num, result_lsn))) {
    PALF_LOG(WARN, "get_majority_lsn failed", K(ret), KPC(this));
  } else {
    majority_match_lsn = result_lsn;
  }
  return ret;
}

bool LogSlidingWindow::check_all_log_has_flushed()
{
  bool bool_ret = is_all_log_flushed_();
  return bool_ret;
}

int LogSlidingWindow::reset_match_lsn_map_()
{
  int ret = OB_SUCCESS;
  ObMemberList member_list;
  int64_t replica_num = 0;
  LSN max_flushed_end_lsn;
  LSN committed_end_lsn;
  const int64_t now_us = ObTimeUtility::current_time();
  ObSpinLockGuard guard(match_lsn_map_lock_);
  if (OB_FAIL(mm_->get_log_sync_member_list(member_list, replica_num))) {
    PALF_LOG(WARN, "get_log_sync_member_list failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_FAIL(match_lsn_map_.clear())) {
    PALF_LOG(WARN, "match_lsn_map_ clear failed", K(ret), K_(palf_id), K_(self));
  } else {
    get_max_flushed_end_lsn(max_flushed_end_lsn);
    get_committed_end_lsn_(committed_end_lsn);
    ObAddr tmp_server;
    LSN tmp_match_lsn;
    for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
      tmp_server.reset();
      if (OB_FAIL(member_list.get_server_by_index(i, tmp_server))) {
        PALF_LOG(WARN, "get_server_by_index failed", K(ret), KPC(this));
      } else {
        // Leader update match_lsn for each paxos member.
        // Setting match_lsn to max_flushed_end_lsn for itself and committed_end_lsn for others.
        tmp_match_lsn = (self_ == tmp_server) ? max_flushed_end_lsn : committed_end_lsn;
        if (OB_FAIL(match_lsn_map_.insert(tmp_server, LsnTsInfo(tmp_match_lsn, now_us)))) {
          PALF_LOG(WARN, "match_lsn_map_.insert failed", K(ret), KPC(this));
        }
      }
    }
  }
  PALF_LOG(INFO, "reset_match_lsn_map_ finished", K(ret), K_(palf_id), K_(self), K(member_list),
      K(max_flushed_end_lsn), K(committed_end_lsn));
  return ret;
}

// need caller hold wlock in PalfHandleImpl
int LogSlidingWindow::config_change_update_match_lsn_map(
    const ObMemberList &added_memberlist,
    const ObMemberList &removed_memberlist,
    const ObMemberList &new_log_sync_memberlist,
    const int64_t new_replica_num)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LSN init_lsn(PALF_INITIAL_LSN_VAL);
    const int64_t now_us = ObTimeUtility::current_time();
    ObSpinLockGuard guard(match_lsn_map_lock_);
    int tmp_ret = OB_SUCCESS;
    ObAddr tmp_server;
    for (int64_t i = 0; OB_SUCC(tmp_ret) && i < added_memberlist.get_member_number(); ++i) {
      tmp_server.reset();
      if (OB_SUCCESS != (tmp_ret = added_memberlist.get_server_by_index(i, tmp_server))) {
        PALF_LOG(WARN, "get_server_by_index failed", K(tmp_ret), K_(palf_id), K_(self));
      } else if (OB_SUCCESS != (tmp_ret = match_lsn_map_.insert(tmp_server, LsnTsInfo(init_lsn, now_us))) &&
          OB_ENTRY_EXIST != tmp_ret) {
        PALF_LOG(WARN, "match_lsn_map_.insert failed", K(tmp_ret), K(tmp_server), K_(palf_id), K_(self));
      } else {
        tmp_ret = OB_SUCCESS;
        PALF_LOG(INFO, "match_lsn_map_.insert success", K(tmp_ret), K(tmp_server), K_(palf_id), K_(self));
      }
    }
    for (int64_t i = 0; OB_SUCC(tmp_ret) && i < removed_memberlist.get_member_number(); ++i) {
      tmp_server.reset();
      if (OB_SUCCESS != (tmp_ret = removed_memberlist.get_server_by_index(i, tmp_server))) {
        PALF_LOG(WARN, "get_server_by_index failed", K(tmp_ret), K_(palf_id), K_(self));
      } else if (OB_SUCCESS != (tmp_ret = match_lsn_map_.erase(tmp_server)) &&
          OB_ENTRY_NOT_EXIST != tmp_ret) {
        PALF_LOG(WARN, "match_lsn_map_.erase failed", K(tmp_ret), K(tmp_server), K_(self), K_(self));
      } else {
        tmp_ret = OB_SUCCESS;
      }
      PALF_LOG(INFO, "match_lsn_map_.erase finish", K(tmp_ret), K(tmp_server), K_(self));
    }
  }
  if (OB_SUCC(ret)) {
    LSN new_committed_end_lsn;
    (void) gen_committed_end_lsn_(new_committed_end_lsn);
    (void) handle_committed_log_();
  }
  // Try to advance committed_end_lsn after updating match_lsn_map_.
  if (OB_SUCC(ret)) {
    if (state_mgr_->is_leader_active()) {
      // Only leader with ACTIVE state can generate new committed_end_lsn.
      // This step is necessary because after removing member, current log_sync_memeber_list
      // may become self (single member). And if all local logs have been flushed, but
      // committed_end_lsn is smaller than flushed lsn, self won't have chance to advance it.
      // So we need try to advance committed_end_lsn here.
      (void) gen_committed_end_lsn_with_memberlist_(new_log_sync_memberlist, new_replica_num);
    }
  }
  PALF_LOG(INFO, "config_change_update_match_lsn_map_ finished", K(ret), K_(palf_id), K_(self), K(added_memberlist), K(removed_memberlist));
  return ret;
}

int LogSlidingWindow::try_update_match_lsn_map_(const common::ObAddr &server, const LSN &end_lsn)
{
  int ret = OB_SUCCESS;
  if (!server.is_valid() || !end_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(server), K(end_lsn));
  } else {
    const int64_t now_us = ObTimeUtility::current_time();
    int tmp_ret = OB_SUCCESS;
    LsnTsInfo tmp_val;
    UpdateMatchLsnFunc update_func(end_lsn, now_us);
    ObSpinLockGuard guard(match_lsn_map_lock_);
    if (OB_SUCCESS != (tmp_ret = match_lsn_map_.get(server, tmp_val))) {
      if (OB_ENTRY_NOT_EXIST == tmp_ret) {
        if (OB_FAIL(match_lsn_map_.insert(server, LsnTsInfo(end_lsn, now_us)))) {
          PALF_LOG(WARN, "match_lsn_map_.insert failed", K(ret), KPC(this), K(server));
        } else {
          PALF_LOG(INFO, "match_lsn_map_.insert success", K(ret), K_(palf_id), K_(self), K(server), K(end_lsn));
        }
      } else {
        ret = tmp_ret;
        PALF_LOG(WARN, "match_lsn_map_.get failed", K(ret), K_(palf_id), K_(self), K(server));
      }
    } else if (OB_FAIL(match_lsn_map_.operate(server, update_func))) {
      // entry exists, try inc update it
      (void) match_lsn_map_.get(server, tmp_val);
      PALF_LOG(WARN, "match_lsn_map_.operate() failed", K(ret), K_(palf_id), K_(self),
          K(server), K(end_lsn), "curr val", tmp_val);
    } else {
      // Update successfully, check if it advances delay too long.
      if (update_func.is_advance_delay_too_long()) {
        PALF_LOG(WARN, "[MATCH LSN ADVANCE DELAY]match_lsn advance delay too much time",
            K(ret), K_(palf_id), K_(self), K(server), K(update_func));
      }
    }
  }
  PALF_LOG(TRACE, "try_update_match_lsn_map_ finished", K(ret), K_(palf_id), K_(self), K(server), K(end_lsn));
  return ret;
}

int LogSlidingWindow::try_send_committed_info(const common::ObAddr &server,
                                               const LSN &log_lsn,
                                               const LSN &log_end_lsn,
                                               const int64_t &log_proposal_id)
{
  int ret = OB_SUCCESS;
  LSN committed_end_lsn;
  get_committed_end_lsn_(committed_end_lsn);
  const int64_t curr_proposal_id = state_mgr_->get_proposal_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!log_lsn.is_valid() || !log_end_lsn.is_valid() || INVALID_PROPOSAL_ID == log_proposal_id) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    // leader/follower can send committed_info to request server,
    // if the arg log has slid out and its end_lsn equals to committed_end_lsn.
    int64_t last_slide_log_id = OB_INVALID_LOG_ID;
    SCN last_slide_scn;
    LSN last_slide_lsn;
    LSN last_slide_end_lsn;
    int64_t last_slide_log_pid = INVALID_PROPOSAL_ID;
    int64_t last_slide_accum_checksum = -1;
    get_last_slide_log_info_(last_slide_log_id, last_slide_scn, last_slide_lsn, \
        last_slide_end_lsn, last_slide_log_pid, last_slide_accum_checksum);
    if (log_lsn == last_slide_lsn
        && log_proposal_id == last_slide_log_pid
        && committed_end_lsn == log_end_lsn) {
      // If arg log does match with last slide log, follower can send committed_info to server.
      OB_ASSERT(log_end_lsn == last_slide_end_lsn);
      if (OB_FAIL(log_engine_->submit_committed_info_req(server, curr_proposal_id,
            last_slide_log_id, log_proposal_id, committed_end_lsn))) {
        PALF_LOG(WARN, "submit_committed_info_req failed", K(ret), K_(palf_id), K_(self), K(server));
      } else {
        PALF_LOG(TRACE, "try_send_committed_info success", K(ret), K_(palf_id), K_(self),
            K(last_slide_log_id), K(log_proposal_id), K(committed_end_lsn));
      }
    }
  }
  return ret;
}

int LogSlidingWindow::leader_get_committed_log_info_(const LSN &committed_end_lsn,
                                                     int64_t &log_id,
                                                     int64_t &log_proposal_id)
{
  int ret = OB_SUCCESS;
  const int64_t max_log_id = get_max_log_id();
  LSN curr_max_lsn;
  (void) lsn_allocator_.get_curr_end_lsn(curr_max_lsn);
  LogTask *log_task = NULL;
  LogTaskGuard guard(this);
  if (curr_max_lsn > committed_end_lsn) {
    // There is new log generated, no need broadcast committed_info.
  } else if (OB_FAIL(guard.get_log_task(max_log_id, log_task))) {
    PALF_LOG(WARN, "get_log_task failed", K(ret), K_(palf_id), K_(self), K(max_log_id), KPC(log_task));
  } else if (!log_task->is_valid() || !log_task->is_freezed()) {
    // log_task is invalid or not freezed, that means there is maybe new log after committed_end_lsn.
    // No need broadcast commonitted_info.
  } else {
    LSN log_end_lsn;
    log_task->lock();
    log_proposal_id = log_task->get_proposal_id();
    log_end_lsn = log_task->get_begin_lsn() + LogGroupEntryHeader::HEADER_SER_SIZE + log_task->get_data_len();
    log_task->unlock();
    if (log_end_lsn == committed_end_lsn) {
      log_id = max_log_id;
    }
  }
  return ret;
}

int LogSlidingWindow::leader_broadcast_committed_info_(const LSN &committed_end_lsn)
{
  int ret = OB_SUCCESS;
  const int64_t curr_proposal_id = state_mgr_->get_proposal_id();
  int64_t log_id = OB_INVALID_LOG_ID;
  int64_t log_proposal_id = INVALID_PROPOSAL_ID;
  ObMemberList dst_member_list;
  int64_t replica_num = 0;
  if (OB_FAIL(leader_get_committed_log_info_(committed_end_lsn, log_id, log_proposal_id))
      || OB_INVALID_LOG_ID == log_id) {
    // no need send committed_info
  } else if (OB_FAIL(mm_->get_log_sync_member_list(dst_member_list, replica_num))) {
    PALF_LOG(WARN, "get_log_sync_member_list failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_FAIL(dst_member_list.remove_server(self_))) {
    PALF_LOG(WARN, "dst_member_list remove_server failed", K(ret), K_(palf_id), K_(self));
  } else if (dst_member_list.is_valid()
             && OB_FAIL(log_engine_->submit_committed_info_req(dst_member_list, curr_proposal_id,
                log_id, log_proposal_id, committed_end_lsn))) {
    PALF_LOG(WARN, "submit_committed_info_req failed", K(ret), K_(palf_id), K_(self), K(log_id));
  } else {
    PALF_LOG(TRACE, "leader_broadcast_committed_info_", K(ret), K_(palf_id), K_(self), K(log_id));
  }
  return ret;
}

int LogSlidingWindow::ack_log(const common::ObAddr &src_server, const LSN &end_lsn)
{
  int ret = OB_SUCCESS;
  ObMemberList member_list;
  int64_t replica_num = 0;
  GlobalLearnerList degraded_learner_list;
  bool in_member_list = false;
  bool in_degraded_list = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!src_server.is_valid() || !end_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K_(self), K(src_server), K(end_lsn));
  } else if (OB_FAIL(mm_->get_log_sync_member_list(member_list, replica_num))) {
    PALF_LOG(WARN, "get_log_sync_member_list failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_FAIL(mm_->get_degraded_learner_list(degraded_learner_list))) {
    PALF_LOG(WARN, "get_degraded_learner_list failed", K(ret), K_(palf_id), K_(self));
  } else if (FALSE_IT(in_member_list = member_list.contains(src_server))) {
  } else if (FALSE_IT(in_degraded_list = degraded_learner_list.contains(src_server))) {
  } else if (!in_member_list && !in_degraded_list) {
    // src_server is not paxos member or degraded learners, skip
    // why record end_lsn of degraded learners in match_lsn_map?
    // if a degraded server recovers,
    PALF_LOG(WARN, "src_server is not in curr_member_list/degraded_learner_list", K(ret), K_(palf_id), K_(self),
        K(src_server), K(member_list), K(degraded_learner_list));
  } else if (OB_FAIL(try_update_match_lsn_map_(src_server, end_lsn))) {
    PALF_LOG(WARN, "try_update_match_lsn_map_ failed", K(ret), K_(palf_id), K_(self), K(src_server), K(end_lsn));
  } else {
    LSN old_committed_end_lsn;
    get_committed_end_lsn_(old_committed_end_lsn);
    LSN new_committed_end_lsn;
    if (state_mgr_->is_leader_active()) {
      // Only leader with ACTIVE state can generate new committed_end_lsn.
      (void) gen_committed_end_lsn_(new_committed_end_lsn);
      if (new_committed_end_lsn > old_committed_end_lsn) {
        (void) leader_broadcast_committed_info_(new_committed_end_lsn);
      }
      // if the leader receives ack from degraded learners and the log
      // is in the range of sliding window, start to send logs to degraded learners
      if (OB_UNLIKELY(in_degraded_list &&
          end_lsn >= get_max_lsn() - 2 * LEADER_DEFAULT_GROUP_BUFFER_SIZE)) {
        mm_->set_sync_to_degraded_learners();
      }
    }
    (void) handle_committed_log_();
    PALF_LOG(TRACE, "ack log finished", K(ret), K_(palf_id), K_(self), K(src_server), K(end_lsn),
        K(old_committed_end_lsn), K(new_committed_end_lsn));
  }
  return ret;
}

int LogSlidingWindow::append_disk_log(const LSN &lsn,
                                      const LogGroupEntry &group_entry)
{
  int ret = OB_SUCCESS;
  const LogGroupEntryHeader &group_entry_header = group_entry.get_header();
  const int64_t group_entry_len = group_entry_header.get_serialize_size() + group_entry_header.get_data_len();
  const LSN log_end_lsn = lsn + group_entry_len;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (false == lsn.is_valid() || false == group_entry.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K_(palf_id), K_(self), K(lsn), K(group_entry));
  } else if (OB_FAIL(append_disk_log_to_sw_(lsn, group_entry))) {
    PALF_LOG(WARN, "append_disk_log_to_sw_ failed", K(ret), K_(palf_id), K_(self), K(lsn), K(group_entry));
  } else if (OB_FAIL(try_update_max_lsn_(lsn, group_entry_header))){
    PALF_LOG(WARN, "try_update_max_lsn_ failed", K(ret), K_(palf_id), K_(self), K(lsn));
  // Update group_buffer's readable_begin_lsn.
  // Because these logs' data do not fill into group_buffer, so it cannot
  // be read by hot cache.
  } else if (OB_FAIL(group_buffer_.inc_update_readable_begin_lsn(log_end_lsn))) {
    PALF_LOG(WARN, "inc_update_readable_begin_lsn failed", K(ret), K(log_end_lsn));
  } else if (OB_FAIL(group_buffer_.inc_update_reuse_lsn(log_end_lsn))) {
    PALF_LOG(WARN, "inc_update_reuse_lsn failed", K(ret), K(log_end_lsn));
  } else {
    // update max_flushed log info
    const int64_t &log_proposal_id = group_entry_header.get_log_proposal_id();
    (void) inc_update_max_flushed_log_info_(lsn, log_end_lsn, log_proposal_id);
    (void) set_last_submit_log_info_(lsn, log_end_lsn, group_entry_header.get_log_id(), group_entry_header.get_log_proposal_id());
    // update saved accum_checksum_
    (void) checksum_.set_accum_checksum(group_entry_header.get_accum_checksum());
    (void) try_advance_committed_lsn_(group_entry_header.get_committed_end_lsn());
    (void) handle_committed_log_();
    PALF_LOG(INFO, "append_disk_log success", K(ret), K_(palf_id), K_(self), K(lsn), K(group_entry));
  }
  return ret;
}

int LogSlidingWindow::report_log_task_trace(const int64_t log_id)
{
  int ret = OB_SUCCESS;
  LogTask *log_task = NULL;
  LogTaskGuard guard(this);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCC(guard.get_log_task(log_id, log_task))) {
    LogMemberAckInfoList ack_info_list;
    get_ack_info_array(ack_info_list);
    PALF_LOG(INFO, "current log_task status", K_(palf_id), K_(self), K(log_id), KPC(log_task),
        K(ack_info_list));
  } else {
    // do nothing
  }
  return ret;
}

int LogSlidingWindow::append_disk_log_to_sw_(const LSN &lsn,
                                             const LogGroupEntry &entry)
{
  int ret = OB_SUCCESS;
  LogTask *log_task = NULL;
  LogTaskGuard guard(this);
  const LogGroupEntryHeader &header = entry.get_header();
  SCN min_scn;
  const int64_t log_id = header.get_log_id();
  const char *buf = entry.get_data_buf();
  const int64_t buf_len = entry.get_data_len();
  int64_t group_log_data_checksum = 0;
  if (false == header.check_integrity(buf, buf_len, group_log_data_checksum)) {
    ret = OB_INVALID_DATA;
    PALF_LOG(ERROR, "group_entry_header check_integrity failed", K(ret), K_(palf_id), K_(self));
  } else if (OB_FAIL(guard.get_log_task(log_id, log_task))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "get log task failed", K(ret), K_(palf_id), K_(self), K(log_id), K(lsn), K(header), "start id", sw_.get_begin_sn());
  } else if (log_task->is_valid()) {
    PALF_LOG(ERROR, "it's not possible to get valid log_task from sw successfully in scan disk phase", K(ret), K_(palf_id), K_(self),
        K(lsn), K(header), "start_id", sw_.get_begin_sn());
  } else if (OB_FAIL(get_min_scn_from_buf_(header, buf, buf_len, min_scn))) {
    PALF_LOG(WARN, "get_min_scn_from_buf_ failed", K(ret), K_(palf_id), K_(self));
  } else {
    LSN max_flushed_lsn;
    LSN max_flushed_end_lsn;
    int64_t max_flushed_log_pid = INVALID_PROPOSAL_ID;
    (void) get_max_flushed_log_info_(max_flushed_lsn, max_flushed_end_lsn, max_flushed_log_pid);
    log_task->lock();
    if (OB_FAIL(log_task->set_group_header(lsn, min_scn, header))) {
      PALF_LOG(WARN, "set_group_header failed", K(ret), K_(palf_id), K_(self), K(lsn), K(header), KPC(log_task));
    } else {
      log_task->set_group_log_checksum(group_log_data_checksum);
      log_task->set_prev_lsn(max_flushed_lsn);
      log_task->set_prev_log_proposal_id(max_flushed_log_pid);
      log_task->set_freezed();
      log_task->set_freeze_ts(ObTimeUtility::current_time());
      log_task->try_pre_submit();
      PALF_LOG(TRACE, "append_disk_log success", K(ret), K_(palf_id), K_(self), K(lsn), K(header), KPC(log_task));
    }
    log_task->unlock();
  }
  return ret;
}

int LogSlidingWindow::try_update_max_lsn_(const LSN &lsn, const LogGroupEntryHeader &header)
{
  int ret = OB_SUCCESS;
  const SCN &scn = header.get_max_scn();
  const int64_t log_id = header.get_log_id();
  const int64_t &log_proposal_id = header.get_log_proposal_id();
  const int64_t group_entry_len = header.get_serialize_size() + header.get_data_len();
  const LSN end_lsn = lsn + group_entry_len;
  if (OB_FAIL(lsn_allocator_.inc_update_last_log_info(end_lsn, log_id, scn))) {
    PALF_LOG(WARN, "inc_update_last_log_info failed", K(ret), K_(palf_id), K_(self), K(lsn), K(scn));
  } else {
    PALF_LOG(TRACE, "try_update_max_lsn_ success", K(ret), K_(palf_id), K_(self), K(lsn), K(end_lsn),
        K(log_id), K(scn));
  }
  return ret;
}

int LogSlidingWindow::truncate_lsn_allocator_(const LSN &last_lsn, const int64_t last_log_id,
    const SCN &last_scn)
{
  int ret = OB_SUCCESS;
  if (!last_lsn.is_valid() || OB_INVALID_LOG_ID == last_log_id || (!last_scn.is_valid() && 0 != last_log_id)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(lsn_allocator_.truncate(last_lsn, last_log_id, last_scn))) {
    PALF_LOG(WARN, "lsn_allocator_.truncate failed", K(ret), K_(palf_id), K_(self));
  } else {
    PALF_LOG(INFO, "lsn_allocator_.truncate success", K(ret), K_(palf_id), K_(self), K(last_lsn),
        K(last_log_id), K(last_scn));
  }
  return ret;
}

int LogSlidingWindow::LogTaskGuard::get_log_task(const int64_t log_id, LogTask *&log_task) {
  int ret = OB_SUCCESS;
  LogTask *log_data = NULL;
  if (NULL == sw_) {
    ret = OB_NOT_INIT;
  } else if (!is_valid_log_id(log_id)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCC(sw_->sw_.get(log_id, log_data))) {
    log_task = log_data;
    log_id_ = log_id;
  } else {
    // get failed
  }
  return ret;
}

void LogSlidingWindow::LogTaskGuard::revert_log_task() {
  int ret = OB_SUCCESS;
  if (NULL != sw_ && is_valid_log_id(log_id_)) {
    if (OB_FAIL(sw_->sw_.revert(log_id_))) {
      const int64_t  palf_id = sw_->palf_id_;
      const int64_t begin_sn = sw_->sw_.get_begin_sn();
      PALF_LOG(ERROR, "revert failed", K(ret), K(palf_id), K_(log_id), K(begin_sn));
    }
  }
  sw_ = NULL;
  log_id_ = -1;
}

int LogSlidingWindow::get_min_scn_from_buf_(const LogGroupEntryHeader &header,
                                            const char *buf,
                                            const int64_t buf_len,
                                            SCN &min_scn)
{
  int ret = OB_SUCCESS;
  LogEntryHeader log_entry_header;
  int64_t pos = 0;
  if (true == header.is_padding_log()) {
    min_scn = header.get_max_scn();
  } else if (OB_FAIL(log_entry_header.deserialize(buf, buf_len, pos))) {
    PALF_LOG(WARN, "LogEntryHeader deserialize failed", K(ret), K(header), K(buf_len));
  } else {
    min_scn = log_entry_header.get_scn();
  }
  return ret;
}

int LogSlidingWindow::handle_committed_info(const common::ObAddr &server,
                                            const int64_t prev_log_id,
                                            const int64_t &prev_log_proposal_id,
                                            const LSN &committed_end_lsn)
{
  int ret = OB_SUCCESS;
  LogTask *log_task = NULL;
  LogTaskGuard guard(this);
  const int64_t last_submit_log_id = get_last_submit_log_id_();
  LSN max_flushed_end_lsn;
  get_max_flushed_end_lsn(max_flushed_end_lsn);
  LSN curr_committed_end_lsn;
  get_committed_end_lsn_(curr_committed_end_lsn);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !committed_end_lsn.is_valid()
             || OB_INVALID_LOG_ID == prev_log_id || INVALID_PROPOSAL_ID == prev_log_proposal_id) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K_(palf_id), K_(self), K(server), K(prev_log_id),
       K(prev_log_proposal_id), K(committed_end_lsn));
  } else if (last_submit_log_id < prev_log_id) {
    // last_submit_log_id is smaller than prev_log_id, ignore msg
  } else if (max_flushed_end_lsn < committed_end_lsn) {
    // max_flushed_end_lsn is smaller than committed_end_lsn, ignore msg
  } else if (curr_committed_end_lsn >= committed_end_lsn) {
    // curr_committed_end_lsn is not smaller than committed_end_lsn, ignore msg
  } else if (OB_FAIL(guard.get_log_task(prev_log_id, log_task))) {
    PALF_LOG(WARN, "get_log_task failed", K(ret), K_(palf_id), K_(self), K(prev_log_id), KPC(log_task));
  } else if (!log_task->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "log_task is invalid, unexpected", K(ret), K_(palf_id), K_(self), K(prev_log_id), KPC(log_task),
        K(last_submit_log_id), K(max_flushed_end_lsn));
  } else {
    int64_t log_proposal_id = INVALID_PROPOSAL_ID;
    LSN log_end_lsn;
    log_task->lock();
    log_proposal_id = log_task->get_proposal_id();
    log_end_lsn = log_task->get_begin_lsn() + LogGroupEntryHeader::HEADER_SER_SIZE + log_task->get_data_len();
    log_task->unlock();
    if (log_proposal_id == prev_log_proposal_id) {
      OB_ASSERT(log_end_lsn == committed_end_lsn);
      (void) try_advance_committed_lsn_(committed_end_lsn);
      (void) handle_committed_log_();
    }
    PALF_LOG(TRACE, "handle_committed_info", K(ret), K_(palf_id), K_(self), K(prev_log_id), K(prev_log_proposal_id),
        K(committed_end_lsn), K(last_submit_log_id), K(max_flushed_end_lsn));
  }
  return ret;
}

int LogSlidingWindow::advance_reuse_lsn(const LSN &flush_log_end_lsn)
{
  // Do not hold lock here.
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!flush_log_end_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(group_buffer_.inc_update_reuse_lsn(flush_log_end_lsn))) {
    PALF_LOG(WARN, "inc_update_reuse_lsn failed", K(ret), K_(palf_id), K(flush_log_end_lsn));
  } else {
    PALF_LOG(TRACE, "advance_reuse_lsn success", K(ret), K_(palf_id), K(flush_log_end_lsn));
  }
  return ret;
}

int LogSlidingWindow::read_data_from_buffer(const LSN &read_begin_lsn,
                                            const int64_t in_read_size,
                                            char *buf,
                                            int64_t &out_read_size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!read_begin_lsn.is_valid() || in_read_size <= 0 || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argumetns", K(ret), K_(palf_id), K(read_begin_lsn), K(in_read_size), KP(buf));
  } else {
    RLockGuard guard(group_buffer_lock_);  // protect group_buffer_ from destroy by flashback().
    if (OB_FAIL(group_buffer_.read_data(read_begin_lsn, in_read_size, buf, out_read_size))) {
      if (OB_ERR_OUT_OF_LOWER_BOUND != ret) {
        PALF_LOG(WARN, "read_data failed", K(ret), K_(palf_id), K(read_begin_lsn), K(in_read_size));
      }
    } else {
      PALF_LOG(TRACE, "read_data_from_buffer success", K(ret), K_(palf_id), K(read_begin_lsn),
          K(in_read_size), K(out_read_size));
    }
  }
  return ret;
}

}  // namespace palf
}  // namespace oceanbase
