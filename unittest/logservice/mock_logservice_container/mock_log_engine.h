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

#ifndef OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_LOG_ENGINE_
#define OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_LOG_ENGINE_

#define private public
#include "logservice/palf/log_engine.h"
#undef private

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace share;

namespace palf
{

class MockLogEngine : public LogEngine
{
public:
  MockLogEngine()
    : parent_itself_(),
      candidate_list_(),
      reg_ret_(RegisterReturn::INVALID_REG_RET)
    {}
  virtual ~MockLogEngine() {}

  int init(const int64_t palf_id,
           const char *base_dir,
           const LogMeta &log_meta,
           const LSN &base_lsn,
           const int64_t base_lsn_ts_ns,
           common::ObILogAllocator *alloc_mgr,
           LogRpc *log_rpc,
           LogIOWorker *log_io_worker)
  {
    int ret = OB_SUCCESS;
    UNUSED(palf_id);
    UNUSED(base_dir);
    UNUSED(log_meta);
    UNUSED(base_lsn);
    UNUSED(base_lsn_ts_ns);
    UNUSED(alloc_mgr);
    UNUSED(log_rpc);
    UNUSED(log_io_worker);
    return ret;
  }
  void destroy() {}

  int load(const int64_t palf_id,
           const char *base_dir,
           common::ObILogAllocator *alloc_mgr,
           LogRpc *log_rpc,
           LogIOWorker *log_io_worker,
           LogGroupEntryHeader &entry_header)
  {
    int ret = OB_SUCCESS;
    UNUSED(palf_id);
    UNUSED(base_dir);
    UNUSED(alloc_mgr);
    UNUSED(log_rpc);
    UNUSED(log_io_worker);
    UNUSED(entry_header);
    return ret;
  }

  int submit_flush_log_task(
      const FlushLogCbCtx &flush_log_cb_ctx,
      const char *buf,
      const int64_t buf_len)
  {
    int ret = OB_SUCCESS;
    UNUSED(flush_log_cb_ctx);
    UNUSED(buf);
    UNUSED(buf_len);
    return ret;
  }

  int submit_flush_log_task(
      const FlushLogCbCtx &flush_log_cb_ctx,
      const LogWriteBuf &write_buf) override
  {
    int ret = OB_SUCCESS;
    UNUSED(flush_log_cb_ctx);
    UNUSED(write_buf);
    return ret;
  }

  int submit_flush_prepare_meta_task(
      const FlushMetaCbCtx &flush_meta_cb_ctx,
      const LogPrepareMeta &prepare_meta)
  {
    int ret = OB_SUCCESS;
    UNUSED(flush_meta_cb_ctx);
    UNUSED(prepare_meta);
    return ret;
  }

  int submit_flush_change_config_meta_task(
      const FlushMetaCbCtx &flush_meta_cb_ctx,
      const LogConfigMeta &config_meta)
  {
    int ret = OB_SUCCESS;
    UNUSED(flush_meta_cb_ctx);
    UNUSED(config_meta);
    return ret;
  }
  int submit_flush_mode_meta_task(
      const FlushMetaCbCtx &flush_meta_cb_ctx,
      const LogModeMeta &mode_meta)
  {
    int ret = OB_SUCCESS;
    UNUSED(flush_meta_cb_ctx);
    UNUSED(mode_meta);
    return ret;
  }

  int update_config_meta_sync(
      const LogConfigMeta &config_meta)
  {
    int ret = OB_SUCCESS;
    UNUSED(config_meta);
    return ret;
  }

  int submit_flush_snapshot_meta_task(
      const FlushMetaCbCtx &flush_meta_cb_ctx,
      const LogSnapshotMeta &log_snapshot_meta)
  {
    int ret = OB_SUCCESS;
    UNUSED(flush_meta_cb_ctx);
    UNUSED(log_snapshot_meta);
    return ret;
  }

  int submit_truncate_log_task(
      const TruncateLogCbCtx &truncate_log_cb_ctx)
  {
    int ret = OB_SUCCESS;
    UNUSED(truncate_log_cb_ctx);
    return ret;
  }

  int submit_truncate_prefix_blocks_task(
      const TruncatePrefixBlocksCbCtx &truncate_prefix_blocks_ctx)
  {
    int ret = OB_SUCCESS;
    UNUSED(truncate_prefix_blocks_ctx);
    return ret;
  }

  int after_flush_log(
      LogIOFlushLogTask *log_io_task)
  {
    int ret = OB_SUCCESS;
    UNUSED(log_io_task);
    return ret;
  }

  int after_flush_meta(
      LogIOFlushMetaTask *log_io_task)
  {
    int ret = OB_SUCCESS;
    UNUSED(log_io_task);
    return ret;
  }

  int after_truncate_log(
      LogIOTruncateLogTask *log_io_task)
  {
    int ret = OB_SUCCESS;
    UNUSED(log_io_task);
    return ret;
  }

  int after_truncate_prefix_blocks(
      LogIOTruncatePrefixBlocksTask *log_io_task)
  {
    int ret = OB_SUCCESS;
    UNUSED(log_io_task);
    return ret;
  }
  int append_log(const LSN &lsn,
                 const LogWriteBuf &write_buf,
                 const int64_t log_ts)
  {
    int ret = OB_SUCCESS;
    UNUSED(lsn);
    UNUSED(write_buf);
    UNUSED(log_ts);
    return ret;
  }
  int read_log(const LSN &lsn,
               const int64_t in_read_size,
               ReadBuf &read_buf,
               int64_t &out_read_size)
  {
    int ret = OB_SUCCESS;
    UNUSED(lsn);
    UNUSED(in_read_size);
    UNUSED(read_buf);
    UNUSED(out_read_size);
    return ret;
  }
  int read_group_entry_header(const LSN &lsn,
                              LogGroupEntryHeader &log_group_entry_header)
  {
    int ret = OB_SUCCESS;
    UNUSED(lsn);
    UNUSED(log_group_entry_header);
    return ret;
  }
  int truncate(const LSN &prev_lsn, const LSN &lsn)
  {
    int ret = OB_SUCCESS;
    UNUSED(prev_lsn);
    UNUSED(lsn);
    return ret;
  }
  int truncate_prefix_blocks(const LSN &lsn)
  {
    int ret = OB_SUCCESS;
    UNUSED(lsn);
    return ret;
  }
  int delete_block(const block_id_t &block_id)
  {
    int ret = OB_SUCCESS;
    UNUSED(block_id);
    return ret;
  }

  const LSN get_begin_lsn() const
  {
    LSN lsn(0);
    return lsn;
  }
  int get_block_id_range(block_id_t &min_block_id, block_id_t &max_block_id) const
  {
    int ret = OB_SUCCESS;
    UNUSED(min_block_id);
    UNUSED(max_block_id);
    return ret;
  }
  int get_block_min_ts_ns(const block_id_t &block_id, int64_t &ts_ns)
  {
    int ret = OB_SUCCESS;
    UNUSED(block_id);
    UNUSED(ts_ns);
    return ret;
  }
  int update_base_lsn_used_for_gc(const LSN &lsn)
  {
    int ret = OB_SUCCESS;
    UNUSED(lsn);
    return ret;
  }
  int append_meta(const char *buf,
                  const int64_t buf_len)
  {
    int ret = OB_SUCCESS;
    UNUSED(buf);
    UNUSED(buf_len);
    return ret;
  }

  int submit_push_log_req(
      const common::ObMemberList &member_list,
      const PushLogType &push_log_type,
      const int64_t &msg_proposal_id,
      const int64_t &prev_log_proposal_id,
      const LSN &prev_lsn,
      const LSN &curr_lsn,
      const LogWriteBuf &write_buf)
  {
    int ret = OB_SUCCESS;
    UNUSED(member_list);
    UNUSED(push_log_type);
    UNUSED(msg_proposal_id);
    UNUSED(prev_log_proposal_id);
    UNUSED(prev_lsn);
    UNUSED(curr_lsn);
    UNUSED(write_buf);
    return ret;
  }

  int submit_push_log_req(
      const common::ObAddr &addr,
      const PushLogType &push_log_type,
      const int64_t &msg_proposal_id,
      const int64_t &prev_log_proposal_id,
      const LSN &prev_lsn,
      const LSN &curr_lsn,
      const LogWriteBuf &write_buf) override
  {
    int ret = OB_SUCCESS;
    UNUSED(addr);
    UNUSED(push_log_type);
    UNUSED(msg_proposal_id);
    UNUSED(prev_log_proposal_id);
    UNUSED(prev_lsn);
    UNUSED(curr_lsn);
    UNUSED(write_buf);
    return ret;
  }

  int submit_push_log_resp(
      const common::ObAddr &server,
      const int64_t &msg_proposal_id,
      const LSN &lsn) override
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(msg_proposal_id);
    UNUSED(lsn);
    return ret;
  }

  int submit_prepare_meta_req(const ObMemberList &member_list, const int64_t &log_proposal_id) override
  {
    UNUSEDx(member_list, log_proposal_id);
    return OB_SUCCESS;
  }

  int submit_prepare_meta_resp(
      const common::ObAddr &server,
      const int64_t &msg_proposal_id,
      const bool vote_granted,
      const int64_t &log_proposal_id,
      const LSN &lsn,
      const LSN &committed_end_lsn,
      const LogModeMeta &mode_meta) override
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(msg_proposal_id);
    UNUSED(vote_granted);
    UNUSED(log_proposal_id);
    UNUSED(lsn);
    UNUSED(committed_end_lsn);
    UNUSED(mode_meta);
    return ret;
  }

  int submit_change_config_meta_req(
      const common::ObMemberList &member_list,
      const int64_t &msg_proposal_id,
      const int64_t &prev_log_proposal_id,
      const LSN &prev_lsn,
      const int64_t &prev_mode_pid,
      const LogConfigMeta &config_meta) override
  {
    int ret = OB_SUCCESS;
    UNUSED(member_list);
    UNUSED(msg_proposal_id);
    UNUSED(prev_log_proposal_id);
    UNUSED(prev_lsn);
    UNUSED(prev_mode_pid);
    UNUSED(config_meta);
    return ret;
  }

  int submit_change_config_meta_req(
      const common::GlobalLearnerList &member_list,
      const int64_t &msg_proposal_id,
      const int64_t &prev_log_proposal_id,
      const LSN &prev_lsn,
      const int64_t &prev_mode_pid,
      const LogConfigMeta &config_meta) override
  {
    int ret = OB_SUCCESS;
    UNUSED(member_list);
    UNUSED(msg_proposal_id);
    UNUSED(prev_log_proposal_id);
    UNUSED(prev_lsn);
    UNUSED(prev_mode_pid);
    UNUSED(config_meta);
    return ret;
  }

  int submit_change_config_meta_req(
      const common::ResendConfigLogList &member_list,
      const int64_t &msg_proposal_id,
      const int64_t &prev_log_proposal_id,
      const LSN &prev_lsn,
      const int64_t &prev_mode_pid,
      const LogConfigMeta &config_meta) override
  {
    int ret = OB_SUCCESS;
    UNUSED(member_list);
    UNUSED(msg_proposal_id);
    UNUSED(prev_log_proposal_id);
    UNUSED(prev_lsn);
    UNUSED(prev_mode_pid);
    UNUSED(config_meta);
    return ret;
  }

  int submit_change_config_meta_resp(
      const common::ObAddr &server,
      const int64_t msg_proposal_id,
      const LogConfigVersion &config_version) override
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(msg_proposal_id);
    UNUSED(config_version);
    return ret;
  }

  int submit_change_mode_meta_req(
      const common::ObMemberList &member_list,
      const int64_t &msg_proposal_id,
      const bool is_applied_mode_meta,
      const LogModeMeta &mode_meta) override
  {
    int ret = OB_SUCCESS;
    UNUSED(member_list);
    UNUSED(msg_proposal_id);
    UNUSED(is_applied_mode_meta);
    UNUSED(mode_meta);
    return ret;
  }

  int submit_change_mode_meta_resp(
      const common::ObAddr &server,
      const int64_t &msg_proposal_id) override
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(msg_proposal_id);
    return ret;
  }

  int submit_config_change_pre_check_req(
      const common::ObAddr &server,
      const LogConfigVersion &config_version,
      const bool need_purge_throttling,
      const int64_t timeout_ns,
      LogGetMCStResp &resp) override
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(config_version);
    UNUSED(timeout_ns);
    UNUSED(need_purge_throttling);
    resp.is_normal_replica_ = true;
    resp.need_update_config_meta_ = false;
    resp.max_flushed_end_lsn_ = LSN(PALF_INITIAL_LSN_VAL);
    return ret;
  }

  int submit_fetch_log_req(
      const common::ObAddr &server,
      const FetchLogType fetch_type,
      const int64_t msg_proposal_id,
      const LSN &prev_lsn,
      const LSN &lsn,
      const int64_t fetch_log_size,
      const int64_t fetch_log_count,
      const int64_t accepted_mode_pid)
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(fetch_type);
    UNUSED(msg_proposal_id);
    UNUSED(prev_lsn);
    UNUSED(lsn);
    UNUSED(fetch_log_size);
    UNUSED(fetch_log_count);
    UNUSED(accepted_mode_pid);
    return ret;
  }

  int submit_notify_rebuild_req(
    const ObAddr &server,
    const LSN &base_lsn,
    const LogInfo &base_prev_log_info) override
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(base_lsn);
    return ret;
  }

  int submit_register_parent_req(const common::ObAddr &server,
                                 const LogLearner &child_itself,
                                 const bool is_to_leader) override
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(child_itself);
    UNUSED(is_to_leader);
    return ret;
  }
  int submit_register_parent_resp(const common::ObAddr &server,
                                  const LogLearner &parent_itself,
                                  const LogCandidateList &candidate_list,
                                  const RegisterReturn reg_ret) override
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    parent_itself_ = parent_itself;
    candidate_list_ = candidate_list;
    reg_ret_ = reg_ret;
    return ret;
  }
  int submit_retire_parent_req(const common::ObAddr &server, const LogLearner &child_itself) override
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(child_itself);
    return ret;
  }
  int submit_retire_child_req(const common::ObAddr &server, const LogLearner &parent_itself) override
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(parent_itself);
    return ret;
  }
  int submit_learner_keepalive_req(const common::ObAddr &server, const LogLearner &sender_itself) override
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(sender_itself);
    return ret;
  }

  LogMeta get_log_meta() const
  {
    LogMeta meta;
    return meta;
  }
  const LSN &get_base_lsn_used_for_block_gc() const
  {
    return base_lsn_for_block_gc_;
  }
  int get_min_block_info_for_gc(block_id_t &block_id, int64_t &ts_ns)
  {
    int ret = OB_SUCCESS;
    UNUSED(block_id);
    UNUSED(ts_ns);
    return ret;
  }
  LogStorage *get_log_storage() { return &log_storage_; }
  LogStorage *get_log_meta_storage() { return &log_meta_storage_; }

  void reset_register_parent_resp_ret()
  {
    parent_itself_.reset();
    candidate_list_.reset();
    reg_ret_ = RegisterReturn::INVALID_REG_RET;
  }
public:
  // register_parent_resp ret
  LogLearner parent_itself_;
  LogCandidateList candidate_list_;
  RegisterReturn reg_ret_;
};

} // end of palf
} // end of oceanbase
#endif
