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

#ifndef OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_LOG_SLIDING_WINDOW_
#define OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_LOG_SLIDING_WINDOW_

#define private public
#include "logservice/palf/log_sliding_window.h"
#include "share/scn.h"
#include "mock_log_state_mgr.h"
#undef private

namespace oceanbase
{
namespace palf
{
class PalfFSCbWrapper;

class MockLogSlidingWindow : public LogSlidingWindow
{
public:
  MockLogSlidingWindow()
    : pending_end_lsn_(0), mock_start_id_(1)
  {}
  virtual ~MockLogSlidingWindow() {}
public:
  void destroy() {}
  int init(const int64_t palf_id,
           const common::ObAddr &self,
           LogStateMgr *state_mgr,
           LogConfigMgr *mm,
           LogEngine *log_engine,
           palf::PalfFSCbWrapper *palf_fs_cb,
           common::ObILogAllocator *alloc_mgr,
           const PalfBaseInfo &palf_base_info,
           const bool is_normal_replica)
  {
    int ret = OB_SUCCESS;
    UNUSED(palf_id);
    UNUSED(self);
    UNUSED(state_mgr);
    UNUSED(mm);
    UNUSED(log_engine);
    UNUSED(palf_fs_cb);
    UNUSED(alloc_mgr);
    UNUSED(palf_base_info);
    UNUSED(is_normal_replica);
    return ret;
  }
  int sliding_cb(const int64_t sn, const FixedSlidingWindowSlot *data)
  {
    int ret = OB_SUCCESS;
    UNUSED(sn);
    UNUSED(data);
    return ret;
  }
  int64_t get_max_log_id() const
  {
    return 1;
  }
  int64_t get_max_log_ts() const
  {
    return 1;
  }
  LSN get_max_lsn() const
  {
    LSN lsn;
    lsn.val_ = 0;
    return lsn;
  }
  int64_t get_start_id() const
  {
    return mock_start_id_;
  }
  int get_committed_end_lsn(LSN &committed_end_lsn) const
  {
    int ret = OB_SUCCESS;
    UNUSED(committed_end_lsn);
    return ret;
  }
  int get_server_ack_info(const common::ObAddr &server, LsnTsInfo &lsn_ts_info)
  {
    UNUSEDx(server, lsn_ts_info);
    return OB_SUCCESS;
  }
  int try_fetch_log(const FetchTriggerType &fetch_log_type,
                    const LSN prev_lsn = LSN(),
                    const LSN fetch_start_lsn = LSN(),
                    const int64_t fetch_start_log_id = OB_INVALID_LOG_ID)
  {
    int ret = OB_SUCCESS;
    UNUSED(prev_lsn);
    UNUSED(fetch_start_lsn);
    UNUSED(fetch_start_log_id);
    return ret;
  }
  int try_fetch_log_for_reconfirm(const common::ObAddr &dest, const LSN &fetch_end_lsn, bool &is_fetched)
  {
    int ret = OB_SUCCESS;
    UNUSED(dest);
    UNUSED(fetch_end_lsn);
    is_fetched = true;
    return ret;
  }
  bool is_empty() const
  {
    return true;
  }
  bool check_all_log_has_flushed()
  {
    return true;
  }
  int get_majority_match_lsn(LSN &majority_match_lsn)
  {
    UNUSED(majority_match_lsn);
    return OB_SUCCESS;
  }
  // ================= log sync part begin
  int submit_log(const char *buf,
                 const int64_t buf_len,
                 const int64_t ref_ts_ns,
                 LSN &lsn,
                 int64_t &log_timestamp)
  {
    int ret = OB_SUCCESS;
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(ref_ts_ns);
    UNUSED(lsn);
    UNUSED(log_timestamp);
    return ret;
  }
  int submit_group_log(const LSN &lsn,
                       const char *buf,
                       const int64_t buf_len)
  {
    int ret = OB_SUCCESS;
    UNUSED(lsn);
    UNUSED(buf);
    UNUSED(buf_len);
    return ret;
  }
  int receive_log(const common::ObAddr &src_server,
                  const PushLogType push_log_type,
                  const LSN &prev_lsn,
                  const int64_t &prev_log_pid,
                  const LSN &lsn,
                  const char *buf,
                  const int64_t buf_len,
                  const bool need_check_clean_log,
                  TruncateLogInfo &truncate_log_info) override
  {
    int ret = OB_SUCCESS;
    UNUSED(src_server);
    UNUSED(push_log_type);
    UNUSED(prev_lsn);
    UNUSED(prev_log_pid);
    UNUSED(lsn);
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(need_check_clean_log);
    UNUSED(truncate_log_info);
    return ret;
  }
  int after_flush_log(const FlushLogCbCtx &flush_cb_ctx)
  {
    int ret = OB_SUCCESS;
    UNUSED(flush_cb_ctx);
    return ret;
  }
  int after_truncate(const TruncateLogCbCtx &truncate_cb_ctx)
  {
    int ret = OB_SUCCESS;
    UNUSED(truncate_cb_ctx);
    return ret;
  }
  int ack_log(const common::ObAddr &src_server, const LSN &end_lsn)
  {
    int ret = OB_SUCCESS;
    UNUSED(src_server);
    UNUSED(end_lsn);
    return ret;
  }
  int truncate(const TruncateLogInfo &truncate_log_info, const LSN &expected_prev_lsn, const int64_t expected_prev_log_pid) override
  {
    int ret = OB_SUCCESS;
    UNUSED(truncate_log_info);
    UNUSED(expected_prev_lsn);
    UNUSED(expected_prev_log_pid);
    return ret;
  }
  int truncate_for_rebuild(const PalfBaseInfo &palf_base_info)
  {
    int ret = OB_SUCCESS;
    UNUSED(palf_base_info);
    return ret;
  }
  bool pre_check_for_config_log(const int64_t &msg_proposal_id,
                                const LSN &lsn,
                                const int64_t &log_proposal_id,
                                TruncateLogInfo &truncate_log_info) override
  {
    UNUSED(msg_proposal_id);
    UNUSED(lsn);
    UNUSED(log_proposal_id);
    UNUSED(truncate_log_info);
    return true;
  }
  // ================= log sync part end
  int append_disk_log(const LSN &lsn, const LogGroupEntry &group_entry)
  {
    int ret = OB_SUCCESS;
    UNUSED(lsn);
    UNUSED(group_entry);
    return ret;
  }
  int report_log_task_trace(const int64_t log_id)
  {
    int ret = OB_SUCCESS;
    UNUSED(log_id);
    return ret;
  }
  void get_max_flushed_end_lsn(LSN &end_lsn) const
  {
    end_lsn = max_flushed_end_lsn_;
  }
  int get_max_flushed_log_info(LSN &lsn,
                               LSN &end_lsn,
                               int64_t &log_proposal_id) const
  {
    int ret = OB_SUCCESS;
    lsn = mock_max_flushed_lsn_;
    end_lsn = mock_max_flushed_end_lsn_;
    log_proposal_id = mock_max_flushed_log_pid_;
    return ret;
  }
  int clean_log(const bool need_clear_log_exist_flag)
  {
    int ret = OB_SUCCESS;
    UNUSED(need_clear_log_exist_flag);
    return ret;
  }
  int clean_cached_log(const int64_t begin_log_id,
                       const LSN &lsn,
                       const LSN &prev_lsn,
                       const int64_t prev_log_pid) override
  {
    int ret = OB_SUCCESS;
    UNUSED(begin_log_id);
    UNUSED(lsn);
    UNUSED(prev_lsn);
    UNUSED(prev_log_pid);
    return ret;
  }
  int to_follower_pending(LSN &last_lsn)
  {
    int ret = OB_SUCCESS;
    last_lsn = pending_end_lsn_;
    return ret;
  }
  int to_leader_reconfirm() override
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  int to_leader_active()
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  int try_advance_committed_end_lsn(const LSN &end_lsn)
  {
    int ret = OB_SUCCESS;
    UNUSED(end_lsn);
    return ret;
  }
  int64_t get_last_submit_log_id_() const
  {
    return 1;
  }
  int get_last_submit_log_info(LSN &last_submit_lsn, int64_t &log_id, int64_t &log_proposal_id) const
  {
    int ret = OB_SUCCESS;
    last_submit_lsn = mock_last_submit_lsn_;
    log_id = mock_last_submit_log_id_;
    log_proposal_id = mock_last_submit_pid_;
    return ret;
  }
  int get_last_submit_log_info(LSN &last_submit_lsn,
                               LSN &last_submit_end_lsn,
                               int64_t &log_id,
                               int64_t &log_proposal_id) const
  {
    int ret = OB_SUCCESS;
    last_submit_lsn = mock_last_submit_lsn_;
    last_submit_end_lsn = mock_last_submit_end_lsn_;
    log_id = mock_last_submit_log_id_;
    log_proposal_id = mock_last_submit_pid_;
    return ret;
  }
  int get_last_slide_end_lsn(LSN &out_end_lsn) const
  {
    int ret = OB_SUCCESS;
    out_end_lsn = last_slide_end_lsn_;
    return ret;
  }
  int64_t get_last_slide_log_ts() const
  {
    return 1;
  }
  int try_freeze_last_log()
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  int inc_update_scn_base(const share::SCN &scn)
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  // location cache will be removed TODO by yunlong
  int set_location_cache_cb(PalfLocationCacheCb *lc_cb)
  {
    int ret = OB_SUCCESS;
    UNUSED(lc_cb);
    return ret;
  }
  int reset_location_cache_cb()
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  int config_change_update_match_lsn_map(const ObMemberList &added_memberlist,
      const ObMemberList &removed_memberlist,
      const ObMemberList &new_log_sync_memberlist,
      const int64_t new_replica_num)
  {
    UNUSED(added_memberlist);
    UNUSED(removed_memberlist);
    UNUSED(new_log_sync_memberlist);
    UNUSED(new_replica_num);
    return OB_SUCCESS;
  }
  int get_server_ack_info(const common::ObAddr &server, LsnTsInfo &ack_info) const
  {
    UNUSED(server);
    ack_info.last_ack_time_us_ = common::ObTimeUtility::current_time();
    ack_info.lsn_ = LSN(PALF_INITIAL_LSN_VAL);
    return OB_SUCCESS;
  }
  int get_leader_from_cache(common::ObAddr &leader) const
  {
    leader = state_mgr_->get_leader();
    return OB_SUCCESS;
  }
public:
  palf::MockLogStateMgr *state_mgr_;
  LSN pending_end_lsn_;
  int64_t mock_start_id_;
  int64_t mock_last_submit_log_id_;
  LSN mock_last_submit_lsn_;
  LSN mock_last_submit_end_lsn_;
  int64_t mock_last_submit_pid_;
  LSN mock_max_flushed_lsn_;
  LSN mock_max_flushed_end_lsn_;
  int64_t mock_max_flushed_log_pid_;
};

} // end of palf
} // end of oceanbase

#endif
