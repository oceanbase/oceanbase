/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

// Test-only IPalfHandleImpl implementation for the async planner and ctx tests.
// It records physical submission, persistence publication, memory reuse, block
// preparation, storage snapshots, and tail-page restoration. Other virtual
// methods use inert defaults so tests can instantiate a complete handle.
#ifndef OCEANBASE_UNITTEST_LOGSERVICE_MOCK_PALF_HANDLE_IMPL_H_
#define OCEANBASE_UNITTEST_LOGSERVICE_MOCK_PALF_HANDLE_IMPL_H_

#include <vector>
#include <functional>
#include "share/ob_errno.h"
#include "share/scn.h"
#include "logservice/palf/palf_handle_impl.h"
#include "logservice/palf/log_async_io_struct.h"
#include "logservice/palf/lsn.h"

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::palf;

// One recorded async-write forwarder call, in invocation order.
struct RecordedHandleCall
{
  enum Kind { SUBMIT, COMMIT, ADVANCE_REUSE, BLOCK_HEADER, SWITCH_BLOCK };
  Kind kind;
  LSN begin_lsn;
  LSN submitted_begin_lsn;
  LSN end_lsn;
  int64_t submitted_write_len;
  unsigned char submitted_first_byte;
  FragmentRef fragment_ref;
};

// Recording IPalfHandleImpl test double for planner and AsyncPalfIOCtx tests.
class MockAsyncPalfHandleImpl : public IPalfHandleImpl
{
public:
  MockAsyncPalfHandleImpl()
    : palf_epoch_(0),
      can_be_used_(true),
      planned_tail_(LSN(0)),
      need_block_header_(false),
      planned_writable_size_(palf::PALF_BLOCK_SIZE),
      switch_block_ret_(common::OB_SUCCESS),
      submit_ret_(common::OB_SUCCESS),
      model_real_submit_gates_(false),
      commit_ret_(common::OB_SUCCESS),
      advance_ret_(common::OB_SUCCESS),
      carry_buffer_read_ret_(common::OB_NOT_SUPPORTED),
      carry_buffer_read_size_(0),
      carry_buffer_fill_(0),
      carry_buffer_read_cnt_(0),
      carry_read_ret_(common::OB_SUCCESS),
      carry_read_fail_remaining_(0),
      carry_read_transient_ret_(common::OB_IO_ERROR),
      carry_page_fill_(0),
      carry_read_size_(4 * 1024),
      carry_read_cnt_(0),
      tail_prefix_fill_cnt_(0),
      tail_prefix_fill_size_(0),
      tail_prefix_fill_first_byte_(0),
      tail_prefix_fill_ret_(common::OB_SUCCESS),
      async_storage_snapshot_cnt_(0),
      truncate_prefix_cnt_(0),
      after_truncate_prefix_cnt_(0),
      truncate_prefix_lsn_(),
      raw_read_cnt_(0),
      readable_end_lsn_(LSN(LOG_MAX_LSN_VAL)),
      outstanding_aio_cnt_(0),
      max_lsn_(LSN()),
      handle_next_submit_call_cnt_(0) {}
  virtual ~MockAsyncPalfHandleImpl() {}

  // ---- test knobs ----
  void set_palf_epoch(int64_t e) { palf_epoch_ = e; }
  void set_planned_tail(const LSN &lsn) { planned_tail_ = lsn; }
  void set_need_block_header(bool v) { need_block_header_ = v; }
  // A writable size of zero models a block that must be prepared before data
  // submission. A successful prepare restores the whole-block writable size;
  // switch_block_ret_ lets a test inject the prepare result.
  void set_planned_writable_size(int64_t v) { planned_writable_size_ = v; }
  void set_switch_block_ret(int r) { switch_block_ret_ = r; }
  // When true, async_pwrite enforces block readiness and DIO alignment.
  // It is disabled by default for tests that only need call recording.
  void set_model_real_submit_gates(bool v) { model_real_submit_gates_ = v; }
  void set_submit_ret(int r) { submit_ret_ = r; }
  void set_commit_ret(int r) { commit_ret_ = r; }
  void set_advance_ret(int r) { advance_ret_ = r; }
  void set_carry_buffer_read_ret(int r) { carry_buffer_read_ret_ = r; }
  void set_carry_buffer_read_size(int64_t v) { carry_buffer_read_size_ = v; }
  void set_carry_buffer_fill(unsigned char v) { carry_buffer_fill_ = v; }
  int64_t get_carry_buffer_read_cnt() const { return carry_buffer_read_cnt_; }
  // Tail-page knobs. read_log_storage_tail_page returns a preset page used to
  // restore the valid prefix before planning the first unaligned write.
  void set_carry_read_ret(int r) { carry_read_ret_ = r; }
  // Make the first n tail-page reads fail, then recover. A permanent
  // carry_read_ret_ failure takes precedence over this transient injection.
  void set_carry_read_fail_n(int64_t n, int ret) { carry_read_fail_remaining_ = n; carry_read_transient_ret_ = ret; }
  void set_carry_page_fill(unsigned char v) { carry_page_fill_ = v; }
  void set_carry_read_size(int64_t v) { carry_read_size_ = v; }
  int64_t get_carry_read_cnt() const { return carry_read_cnt_; }
  void set_tail_page_fill(unsigned char v) { carry_page_fill_ = v; }
  void set_tail_page_read_size(int64_t v) { carry_read_size_ = v; }
  int64_t get_tail_page_read_cnt() const { return carry_read_cnt_; }
  int64_t get_tail_prefix_fill_cnt() const { return tail_prefix_fill_cnt_; }
  int64_t get_tail_prefix_fill_size() const { return tail_prefix_fill_size_; }
  unsigned char get_tail_prefix_fill_first_byte() const { return tail_prefix_fill_first_byte_; }
  void set_tail_prefix_fill_ret(int ret) { tail_prefix_fill_ret_ = ret; }
  int64_t get_async_storage_snapshot_cnt() const { return async_storage_snapshot_cnt_; }
  int64_t get_truncate_prefix_cnt() const { return truncate_prefix_cnt_; }
  int64_t get_after_truncate_prefix_cnt() const
  { return ATOMIC_LOAD(&after_truncate_prefix_cnt_); }
  LSN get_truncate_prefix_lsn() const { return truncate_prefix_lsn_; }
  int64_t get_raw_read_cnt() const { return raw_read_cnt_; }
  void set_carry_read_hook(const std::function<void()> &hook) { carry_read_hook_ = hook; }
  void set_submit_logical_len_hook(
      const std::function<int64_t(const LSN &, int64_t)> &hook)
  { submit_logical_len_hook_ = hook; }
  void set_submit_logical_begin_hook(
      const std::function<LSN(const LSN &, int64_t)> &hook)
  { submit_logical_begin_hook_ = hook; }
  // Model raw_read's readable upper bound
  // (PalfHandleImpl::raw_read returns OB_ERR_OUT_OF_UPPER_BOUND for lsn >=
  // get_end_lsn()). Default is huge so existing tests keep their disk-read success.
  // Set it below page_begin_lsn to force the disk read to fail with -4234.
  void set_readable_end_lsn(const LSN &lsn) { readable_end_lsn_ = lsn; }
  // Outstanding AIO counter knob. In production this counter is owned by the
  // callback object lifetime; the mock increments it on successful submit so
  // tests can assert submit completion does not own that accounting.
  int64_t get_outstanding_aio_cnt() const { return outstanding_aio_cnt_; }
  const std::vector<RecordedHandleCall> &calls() const { return calls_; }
  void clear_calls() { calls_.clear(); }

  // Sliding-window producer knobs model the visible max LSN and count worker
  // requests that ask the producer to dispatch the next flush task.
  void set_max_lsn(const LSN &lsn) { max_lsn_ = lsn; }
  void set_producer_hook(const std::function<void()> &hook) { producer_hook_ = hook; }
  int64_t get_handle_next_submit_call_cnt() const { return handle_next_submit_call_cnt_; }

  LSN get_async_planned_tail_lsn() const { return planned_tail_; }
  int64_t get_async_planned_curr_block_writable_size() const { return planned_writable_size_; }
  bool get_async_planned_need_append_block_header() const { return need_block_header_; }

  // ---- recorded async-write forwarder subset ----
  int get_palf_epoch(int64_t &palf_epoch) const override
  { palf_epoch = palf_epoch_; return common::OB_SUCCESS; }

  int async_pwrite(const AsyncPwriteRequest &req,
                   common::ObIOHandle &out_handle) override
  {
    UNUSED(out_handle);
    const LSN aligned_begin_lsn = req.get_aligned_begin_lsn();
    const char *aligned_buf = req.get_aligned_buf();
    const int64_t aligned_buf_len = req.get_aligned_buf_len();
    const FragmentRef &fragment_ref = req.get_fragment_ref();
    const LSN logical_begin_lsn = submit_logical_begin_hook_
        ? submit_logical_begin_hook_(aligned_begin_lsn, aligned_buf_len)
        : aligned_begin_lsn;
    const int64_t logical_data_len = submit_logical_len_hook_
        ? submit_logical_len_hook_(aligned_begin_lsn, aligned_buf_len)
        : aligned_buf_len;
    if (model_real_submit_gates_) {
      if (need_block_header_) {
        return common::OB_STATE_NOT_MATCH;
      }
      if (0 != (lsn_2_offset(aligned_begin_lsn, PALF_BLOCK_SIZE) % LOG_DIO_ALIGN_SIZE)
          || 0 != (aligned_buf_len % LOG_DIO_ALIGN_SIZE)) {
        return common::OB_INVALID_ARGUMENT;
      }
    }
    if (common::OB_SUCCESS == submit_ret_) {
      RecordedHandleCall c;
      c.kind = RecordedHandleCall::SUBMIT;
      c.begin_lsn = logical_begin_lsn;
      c.submitted_begin_lsn = aligned_begin_lsn;
      c.submitted_write_len = aligned_buf_len;
      c.submitted_first_byte = (OB_NOT_NULL(aligned_buf) && aligned_buf_len > 0)
          ? static_cast<unsigned char>(aligned_buf[0])
          : 0;
      c.end_lsn = logical_begin_lsn + logical_data_len;
      c.fragment_ref = fragment_ref;
      calls_.push_back(c);
      ++outstanding_aio_cnt_;
    }
    return submit_ret_;
  }

  int commit_async_append(const LSN &begin_lsn, const LSN &end_lsn) override
  {
    if (common::OB_SUCCESS == commit_ret_) {
      RecordedHandleCall c;
      c.kind = RecordedHandleCall::COMMIT;
      c.begin_lsn = begin_lsn;
      c.submitted_begin_lsn.reset();
      c.end_lsn = end_lsn;
      c.submitted_write_len = 0;
      c.submitted_first_byte = 0;
      c.fragment_ref.reset();
      calls_.push_back(c);
    }
    return commit_ret_;
  }

  int advance_reuse_lsn(const LSN &flush_log_end_lsn) override
  {
    if (common::OB_SUCCESS == advance_ret_) {
      RecordedHandleCall c;
      c.kind = RecordedHandleCall::ADVANCE_REUSE;
      c.begin_lsn = LSN();
      c.submitted_begin_lsn.reset();
      c.end_lsn = flush_log_end_lsn;
      c.submitted_write_len = 0;
      c.submitted_first_byte = 0;
      c.fragment_ref.reset();
      calls_.push_back(c);
    }
    return advance_ret_;
  }


  int prepare_async_block_for_write(const share::SCN &new_block_min_scn) override
  {
    UNUSED(new_block_min_scn);
    RecordedHandleCall c;
    c.kind = RecordedHandleCall::SWITCH_BLOCK;
    c.begin_lsn = planned_tail_;
    c.submitted_begin_lsn.reset();
    c.end_lsn = LSN();
    c.submitted_write_len = 0;
    c.submitted_first_byte = 0;
    c.fragment_ref.reset();
    calls_.push_back(c);
    if (common::OB_SUCCESS == switch_block_ret_) {
      need_block_header_ = false;
      planned_writable_size_ = palf::PALF_BLOCK_SIZE;
      RecordedHandleCall hc;
      hc.kind = RecordedHandleCall::BLOCK_HEADER;
      hc.begin_lsn = planned_tail_;
      hc.submitted_begin_lsn.reset();
      hc.end_lsn = LSN();
      hc.submitted_write_len = 0;
      hc.submitted_first_byte = 0;
      hc.fragment_ref.reset();
      calls_.push_back(hc);
    }
    return switch_block_ret_;
  }

  void get_async_storage_snapshot(LogStorage::AsyncStorageSnapshot &out) const override
  {
    ++async_storage_snapshot_cnt_;
    out.log_tail = planned_tail_;
    out.curr_block_writable_size = planned_writable_size_;
    out.need_append_block_header = need_block_header_;
  }

  // ---- generated no-op defaults for every other pure virtual ----
  virtual bool check_can_be_used() const override { return can_be_used_; }
  virtual int set_initial_member_list(
      const common::ObMemberList &member_list,
      const int64_t paxos_replica_num,
      const common::GlobalLearnerList &learner_list) override
  { return common::OB_NOT_SUPPORTED; }
#ifdef OB_BUILD_ARBITRATION
  virtual int set_initial_member_list(
      const common::ObMemberList &member_list,
      const common::ObMember &arb_member,
      const int64_t paxos_replica_num,
      const common::GlobalLearnerList &learner_list) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_remote_arb_member_info(
      ArbMemberInfo &arb_member_info) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_arb_member_info(
      ArbMemberInfo &arb_member_info) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_arbitration_member(
      common::ObMember &arb_member) const override
  { return common::OB_NOT_SUPPORTED; }
#endif
  virtual int submit_log(
      const PalfAppendOptions &opts, const char *buf,
      const int64_t buf_len, const share::SCN &ref_scn,
      logservice::AppendCb *cb, LSN &lsn, share::SCN &scn) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int submit_group_log(
      const PalfAppendOptions &opts, const LSN &lsn,
      const char *buf, const int64_t buf_len) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_role(
      common::ObRole &role, int64_t &proposal_id,
      bool &is_pending_state) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_proposal_id_and_sync_mode(
      int64_t &proposal_id, SyncMode &sync_mode,
      bool &is_pending_state) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_palf_id(int64_t &palf_id) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int change_leader_to(
      const common::ObAddr &dest_addr) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_global_learner_list(
      common::GlobalLearnerList &learner_list) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_paxos_member_list(
      common::ObMemberList &member_list,
      int64_t &paxos_replica_num,
      const bool &filter_logonly_replica) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_config_version(
      LogConfigVersion &config_version) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_paxos_member_list_and_learner_list(
      common::ObMemberList &member_list,
      int64_t &paxos_replica_num,
      common::GlobalLearnerList &learner_list,
      const bool &filter_logonly_replica) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_election_leader(
      common::ObAddr &addr) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_parent(
      common::ObAddr &parent) const override
  { return common::OB_NOT_SUPPORTED; }
#ifdef OB_BUILD_ARBITRATION
  virtual int add_arb_member(
      const common::ObMember &added_member,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int remove_arb_member(
      const common::ObMember &arb_member,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int degrade_acceptor_to_learner(
      const LogMemberAckInfoList &degrade_servers,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int upgrade_learner_to_acceptor(
      const LogMemberAckInfoList &upgrade_servers,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int set_election_silent_flag(
      const bool election_silent_flag) override
  { return common::OB_NOT_SUPPORTED; }
  virtual bool is_election_silent() const override { return false; }
#endif
  virtual int set_base_lsn(const LSN &lsn) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int enable_sync() override
  { return common::OB_NOT_SUPPORTED; }
  virtual int disable_sync() override
  { return common::OB_NOT_SUPPORTED; }
  virtual void set_deleted() override { can_be_used_ = false; }
  virtual void mark_deleted_atomic_only() override { can_be_used_ = false; }
  virtual void drain_inflight_readers() override {}
  virtual bool is_sync_enabled() const override { return false; }
  virtual int advance_base_info(
      const PalfBaseInfo &palf_base_info,
      const bool is_rebuild) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int locate_by_scn_coarsely(
      const share::SCN &scn, LSN &result_lsn) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int locate_by_lsn_coarsely(
      const LSN &lsn, share::SCN &result_scn) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_begin_lsn(LSN &lsn) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_begin_scn(share::SCN &scn) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_base_lsn(LSN &lsn) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_base_info(
      const LSN &base_lsn, PalfBaseInfo &base_info) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_min_block_info_for_gc(
      block_id_t &min_block_id, share::SCN &max_scn) override
  { return common::OB_NOT_SUPPORTED; }
  virtual const LSN get_end_lsn() const override { return LSN(); }
  virtual LSN get_max_lsn() const override { return max_lsn_; }
  virtual const share::SCN get_max_scn() const override
  { return share::SCN(); }
  virtual const share::SCN get_end_scn() const override
  { return share::SCN(); }
  virtual int get_last_rebuild_lsn(
      LSN &last_rebuild_lsn) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual const LSN get_readable_end_lsn() const override
  { return LSN(); }
  virtual int get_total_used_disk_space(
      int64_t &total_used_disk_space,
      int64_t &unrecyclable_disk_space) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual const LSN &get_base_lsn_used_for_block_gc() const override
  { static LSN v; return v; }
  virtual int get_ack_info_array(
      LogMemberAckInfoList &ack_info_array,
      common::GlobalLearnerList &degraded_list) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int delete_block(const block_id_t &block_id) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int inner_after_flush_log(
      const FlushLogCbCtx &flush_log_cb_ctx) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int inner_after_truncate_log(
      const TruncateLogCbCtx &truncate_log_cb_ctx) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int inner_after_flush_meta(
      const FlushMetaCbCtx &flush_meta_cb_ctx) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int inner_after_truncate_prefix_blocks(
      const TruncatePrefixBlocksCbCtx &truncate_prefix_cb_ctx) override
  {
    UNUSED(truncate_prefix_cb_ctx);
    ATOMIC_INC(&after_truncate_prefix_cnt_);
    return common::OB_SUCCESS;
  }
  virtual int inner_after_flashback(
      const FlashbackCbCtx &flashback_ctx) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int inner_append_log(
      const LSN &lsn, const LogWriteBuf &write_buf,
      const share::SCN &scn) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int inner_append_log(
      const LSNArray &lsn_array,
      const LogWriteBufArray &write_buf_array,
      const SCNArray &scn_array) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int inner_append_meta(
      const char *buf, const int64_t buf_len) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int inner_truncate_log(const LSN &lsn) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int inner_truncate_prefix_blocks(const LSN &lsn) override
  {
    ++truncate_prefix_cnt_;
    truncate_prefix_lsn_ = lsn;
    planned_tail_ = lsn;
    planned_writable_size_ = 0;
    need_block_header_ = true;
    return common::OB_SUCCESS;
  }
  virtual int inner_flashback(
      const share::SCN &flashback_scn) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int check_and_switch_state() override
  { return common::OB_NOT_SUPPORTED; }
  virtual int check_and_switch_freeze_mode() override
  { return common::OB_NOT_SUPPORTED; }
  virtual bool is_in_period_freeze_mode() const override { return false; }
  virtual int period_freeze_last_log() override
  { return common::OB_NOT_SUPPORTED; }
  virtual int handle_prepare_request(
      const common::ObAddr &server,
      const int64_t &proposal_id) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int handle_prepare_response(
      const common::ObAddr &server, const int64_t &proposal_id,
      const bool vote_granted, const int64_t &accept_proposal_id,
      const LSN &last_lsn, const LSN &committed_end_lsn,
      const LogModeMeta &log_mode_meta) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int handle_election_message(
      const election::ElectionPrepareRequestMsg &msg) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int handle_election_message(
      const election::ElectionPrepareResponseMsg &msg) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int handle_election_message(
      const election::ElectionAcceptRequestMsg &msg) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int handle_election_message(
      const election::ElectionAcceptResponseMsg &msg) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int handle_election_message(
      const election::ElectionChangeLeaderMsg &msg) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int receive_log(
      const common::ObAddr &server,
      const PushLogType push_log_type,
      const int64_t &proposal_id, const LSN &prev_lsn,
      const int64_t &prev_proposal_id, const LSN &lsn,
      const char *buf, const int64_t buf_len) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int receive_batch_log(
      const common::ObAddr &server,
      const int64_t msg_proposal_id,
      const int64_t prev_log_proposal_id,
      const LSN &prev_lsn, const LSN &curr_lsn,
      const char *buf, const int64_t buf_len) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int ack_log(
      const common::ObAddr &server,
      const int64_t &proposal_id,
      const LSN &log_end_lsn) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_log(
      const common::ObAddr &server,
      const FetchLogType fetch_type,
      const int64_t msg_proposal_id,
      const LSN &prev_lsn, const LSN &start_lsn,
      const int64_t fetch_log_size,
      const int64_t fetch_log_count,
      const int64_t accepted_mode_pid) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int fetch_log_from_storage(
      const common::ObAddr &server,
      const FetchLogType fetch_type,
      const int64_t &req_proposal_id,
      const LSN &prev_log_offset, const LSN &log_offset,
      const int64_t fetch_log_size,
      const int64_t fetch_log_count,
      const int64_t accepted_mode_pid,
      const SCN &replayable_point,
      FetchLogStat &fetch_stat) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int receive_config_log(
      const common::ObAddr &server,
      const int64_t &msg_proposal_id,
      const int64_t &prev_log_proposal_id,
      const LSN &prev_lsn, const int64_t &prev_mode_pid,
      const LogConfigMeta &meta) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int ack_config_log(
      const common::ObAddr &server,
      const int64_t proposal_id,
      const LogConfigVersion &config_version) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int receive_mode_meta(
      const common::ObAddr &server,
      const int64_t msg_proposal_id,
      const bool is_applied_mode_meta,
      const LogModeMeta &meta) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int ack_mode_meta(
      const common::ObAddr &server,
      const int64_t proposal_id) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int handle_notify_fetch_log_req(
      const common::ObAddr &server) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int handle_notify_rebuild_req(
      const common::ObAddr &server, const LSN &base_lsn,
      const LogInfo &base_prev_log_info) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int handle_config_change_pre_check(
      const ObAddr &server, const LogGetMCStReq &req,
      LogGetMCStResp &resp) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int handle_register_parent_req(
      const LogLearner &child, const bool is_to_leader) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int handle_register_parent_resp(
      const LogLearner &server,
      const LogCandidateList &candidate_list,
      const RegisterReturn reg_ret) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int handle_learner_req(
      const LogLearner &server,
      const LogLearnerReqType req_type) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int set_scan_disk_log_finished() override
  { return common::OB_NOT_SUPPORTED; }
  virtual int change_access_mode(
      const int64_t proposal_id, const int64_t mode_version,
      const AccessMode &access_mode,
      const share::SCN &ref_scn) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_access_mode(
      int64_t &mode_version,
      AccessMode &access_mode) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_access_mode(
      AccessMode &access_mode) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_access_mode_version(
      int64_t &mode_version) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_access_mode_ref_scn(
      int64_t &mode_version, AccessMode &access_mode,
      SCN &ref_scn) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int change_sync_mode(
      const int64_t proposal_id, const int64_t mode_version,
      const SyncMode &sync_mode, int64_t &new_mode_version,
      int64_t &out_proposal_id) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_sync_mode(
      int64_t &mode_version,
      SyncMode &sync_mode) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_sync_mode(
      SyncMode &sync_mode) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_sync_mode_version(
      int64_t &mode_version) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int handle_committed_info(
      const common::ObAddr &server,
      const int64_t &msg_proposal_id,
      const int64_t prev_log_id,
      const int64_t &prev_log_proposal_id,
      const LSN &committed_end_lsn) override
  { return common::OB_NOT_SUPPORTED; }
  virtual bool is_vote_enabled() const override { return false; }
  virtual int disable_vote(
      const bool need_check_log_missing) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int enable_vote() override
  { return common::OB_NOT_SUPPORTED; }
  virtual int alloc_palf_buffer_iterator(
      const LSN &offset,
      PalfBufferIterator &iterator) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int alloc_palf_buffer_iterator(
      const SCN &scn,
      PalfBufferIterator &iterator) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int alloc_palf_group_buffer_iterator(
      const LSN &offset,
      PalfGroupBufferIterator &iterator) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int alloc_palf_group_buffer_iterator(
      const share::SCN &scn,
      PalfGroupBufferIterator &iterator) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int register_file_size_cb(
      palf::PalfFSCbNode *fs_cb) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int unregister_file_size_cb(
      palf::PalfFSCbNode *fs_cb) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int register_role_change_cb(
      palf::PalfRoleChangeCbNode *role_change_cb) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int unregister_role_change_cb(
      palf::PalfRoleChangeCbNode *role_change_cb) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int register_rebuild_cb(
      palf::PalfRebuildCbNode *rebuild_cb) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int unregister_rebuild_cb(
      palf::PalfRebuildCbNode *rebuild_cb) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int set_location_cache_cb(
      PalfLocationCacheCb *lc_cb) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int reset_location_cache_cb() override
  { return common::OB_NOT_SUPPORTED; }
  virtual int set_election_priority(
      election::ElectionPriority *priority) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int reset_election_priority() override
  { return common::OB_NOT_SUPPORTED; }
  virtual int set_locality_cb(
      palf::PalfLocalityInfoCb *locality_cb) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int reset_locality_cb() override
  { return common::OB_NOT_SUPPORTED; }
  virtual int set_reconfig_checker_cb(
      palf::PalfReconfigCheckerCb *reconfig_checker) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int reset_reconfig_checker_cb() override
  { return common::OB_NOT_SUPPORTED; }
  virtual int advance_election_epoch_and_downgrade_priority(
      const int64_t proposal_id,
      const int64_t downgrade_priority_time_us,
      const char *reason) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int flashback(
      const int64_t mode_version,
      const share::SCN &flashback_scn,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int stat(PalfStat &palf_stat) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int try_lock_config_change(
      int64_t lock_owner, int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int unlock_config_change(
      int64_t lock_owner, int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_config_change_lock_stat(
      int64_t &lock_owner, bool &is_locked) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int diagnose(
      PalfDiagnoseInfo &diagnose_info) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int update_palf_stat() override
  { return common::OB_NOT_SUPPORTED; }
  virtual int read_data_from_buffer(
      const LSN &read_begin_lsn, const int64_t in_read_size,
      char *buf, int64_t &out_read_size) const override
  {
    UNUSED(read_begin_lsn);
    ++carry_buffer_read_cnt_;
    out_read_size = 0;
    if (common::OB_SUCCESS == carry_buffer_read_ret_) {
      const int64_t fill = MIN(in_read_size, carry_buffer_read_size_);
      if (OB_NOT_NULL(buf) && fill > 0) {
        MEMSET(buf, carry_buffer_fill_, fill);
      }
      out_read_size = carry_buffer_read_size_;
    }
    return carry_buffer_read_ret_;
  }
  virtual int read_log_storage_tail_page(
      const palf::LSN &page_begin_lsn, char *buf,
      const int64_t buf_len, int64_t &read_size) override
  {
    UNUSED(page_begin_lsn);
    ++carry_read_cnt_;
    if (carry_read_hook_) {
      carry_read_hook_();
    }
    read_size = 0;
    if (common::OB_SUCCESS != carry_read_ret_) {
      return carry_read_ret_;
    }
    if (carry_read_fail_remaining_ > 0) {
      --carry_read_fail_remaining_;
      return carry_read_transient_ret_;
    }
    const int64_t fill = MIN(buf_len, carry_read_size_);
    if (OB_NOT_NULL(buf) && fill > 0) {
      MEMSET(buf, carry_page_fill_, fill);
    }
    read_size = carry_read_size_;
    return common::OB_SUCCESS;
  }
  virtual int fill_tail_prefix_after_reset(
      const LSN &prefix_begin_lsn,
      const LSN &tail_lsn,
      const char *buf,
      const int64_t buf_len) override
  {
    UNUSED(prefix_begin_lsn);
    UNUSED(tail_lsn);
    ++tail_prefix_fill_cnt_;
    tail_prefix_fill_size_ = buf_len;
    tail_prefix_fill_first_byte_ = (OB_NOT_NULL(buf) && buf_len > 0)
        ? static_cast<unsigned char>(buf[0])
        : 0;
    return tail_prefix_fill_ret_;
  }
  virtual int raw_read(
      const palf::LSN &lsn, char *read_buf,
      const int64_t nbytes, int64_t &read_size,
      LogIOContext &io_ctx) override
  {
    UNUSED(io_ctx);
    ++raw_read_cnt_;
    read_size = 0;
    if (lsn.is_valid()
        && readable_end_lsn_.is_valid()
        && lsn >= readable_end_lsn_) {
      return common::OB_ERR_OUT_OF_UPPER_BOUND;
    }
    if (common::OB_SUCCESS != carry_read_ret_) {
      return carry_read_ret_;
    }
    if (carry_read_fail_remaining_ > 0) {
      --carry_read_fail_remaining_;
      return carry_read_transient_ret_;
    }
    const int64_t fill = MIN(nbytes, carry_read_size_);
    if (OB_NOT_NULL(read_buf) && fill > 0) {
      MEMSET(read_buf, carry_page_fill_, fill);
    }
    read_size = carry_read_size_;
    return common::OB_SUCCESS;
  }
  virtual int try_handle_next_submit_log() override
  {
    ++handle_next_submit_call_cnt_;
    if (producer_hook_) {
      producer_hook_();
    }
    return common::OB_SUCCESS;
  }
  virtual int fill_cache_when_slide(
      const LSN &read_begin_lsn,
      const int64_t in_read_size) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_io_statistic_info(
      int64_t &last_working_time, int64_t &last_write_size,
      int64_t &accum_write_size, int64_t &accum_write_count,
      int64_t &accum_write_rt) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int get_stable_membership(
      palf::LogConfigVersion &config_version,
      common::ObMemberList &member_list,
      int64_t &paxos_replica_num,
      common::GlobalLearnerList &learner_list,
      const bool &filter_logonly_replica) const override
  { return common::OB_NOT_SUPPORTED; }
  virtual int change_replica_num(
      const common::ObMemberList &member_list,
      const int64_t curr_replica_num,
      const int64_t new_replica_num,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int force_set_as_single_replica() override
  { return common::OB_NOT_SUPPORTED; }
  virtual int force_set_member_list(
      const common::ObMemberList &new_member_list,
      const int64_t new_replica_num) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int inc_config_version(int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int add_member(
      const common::ObMember &member,
      const int64_t new_replica_num,
      const palf::LogConfigVersion &config_version,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int remove_member(
      const common::ObMember &member,
      const int64_t new_replica_num,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int replace_member(
      const common::ObMember &added_member,
      const common::ObMember &removed_member,
      const palf::LogConfigVersion &config_version,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int add_learner(
      const common::ObMember &added_learner,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int remove_learner(
      const common::ObMember &removed_learner,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int switch_learner_to_acceptor(
      const common::ObMember &learner,
      const int64_t new_replica_num,
      const palf::LogConfigVersion &config_version,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int switch_acceptor_to_learner(
      const common::ObMember &member,
      const int64_t new_replica_num,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int replace_learners(
      const common::ObMemberList &added_learners,
      const common::ObMemberList &removed_learners,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int replace_member_with_learner(
      const common::ObMember &added_member,
      const common::ObMember &removed_member,
      const palf::LogConfigVersion &config_version,
      const int64_t timeout_us) override
  { return common::OB_NOT_SUPPORTED; }
  virtual int64_t to_string(char *buf, const int64_t buf_len) const override
    {
      int64_t pos = 0;
      common::databuff_printf(buf, buf_len, pos, "{MockAsyncPalfHandleImpl}");
      return pos;
    }
private:
  int64_t palf_epoch_;
  bool can_be_used_;
  LSN planned_tail_;
  bool need_block_header_;
  int64_t planned_writable_size_;
  int switch_block_ret_;
  int submit_ret_;
  bool model_real_submit_gates_;
  int commit_ret_;
  int advance_ret_;
  int carry_buffer_read_ret_;
  int64_t carry_buffer_read_size_;
  unsigned char carry_buffer_fill_;
  mutable int64_t carry_buffer_read_cnt_;
  int carry_read_ret_;
  int64_t carry_read_fail_remaining_;
  int carry_read_transient_ret_;
  unsigned char carry_page_fill_;
  int64_t carry_read_size_;
  int64_t carry_read_cnt_;
  int64_t tail_prefix_fill_cnt_;
  int64_t tail_prefix_fill_size_;
  unsigned char tail_prefix_fill_first_byte_;
  int tail_prefix_fill_ret_;
  mutable int64_t async_storage_snapshot_cnt_;
  int64_t truncate_prefix_cnt_;
  int64_t after_truncate_prefix_cnt_;
  LSN truncate_prefix_lsn_;
  int64_t raw_read_cnt_;
  LSN readable_end_lsn_;
  int64_t outstanding_aio_cnt_;
  LSN max_lsn_;
  int64_t handle_next_submit_call_cnt_;
  std::function<void()> carry_read_hook_;
  std::function<void()> producer_hook_;
  std::function<int64_t(const LSN &, int64_t)> submit_logical_len_hook_;
  std::function<LSN(const LSN &, int64_t)> submit_logical_begin_hook_;
  std::vector<RecordedHandleCall> calls_;
};

} // namespace unittest
} // namespace oceanbase

#endif // OCEANBASE_UNITTEST_LOGSERVICE_MOCK_PALF_HANDLE_IMPL_H_
