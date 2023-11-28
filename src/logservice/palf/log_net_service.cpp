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

#include "log_net_service.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"                   // For UNUSED
#include "lib/net/ob_addr.h"                              // ObAddr
#include "common/ob_member_list.h"                        // ObMemberList
#include "log_req.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"    // ObILogAllocator
#include "lsn.h"                                // LSN
#include "log_rpc.h"                                   // ObLgRpc
#include "log_meta_info.h"                             // LogPrepareMeta
#include "log_writer_utils.h"                          // LogWriteBuf

namespace oceanbase
{
using namespace common;
namespace palf
{
LogNetService::LogNetService() : palf_id_(),
                                 log_rpc_(NULL),
                                 is_inited_(false)
{
}

LogNetService::~LogNetService()
{
  destroy();
}

int LogNetService::init(const int64_t palf_id,
                        LogRpc *log_rpc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else {
    palf_id_ = palf_id;
    log_rpc_ = log_rpc;
    is_inited_ = true;
  }
  return ret;
}

void LogNetService::destroy()
{
  if (IS_INIT) {
    PALF_LOG(INFO, "LogNetService destroy success", K(palf_id_));
    is_inited_ = false;
    log_rpc_ = NULL;
    palf_id_ = 0;;
  }
}

int LogNetService::start()
{
  int ret = OB_SUCCESS;

  return ret;
}

int LogNetService::submit_push_log_req(
    const common::ObAddr &server,
    const PushLogType &push_log_type,
    const int64_t &msg_proposal_id,
    const int64_t &prev_log_proposal_id,
    const LSN &prev_lsn,
    const LSN &curr_lsn,
    const LogWriteBuf &write_buf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogNetService has not inited!!!", K(ret));
  } else {
    LogPushReq push_log_req(push_log_type,
                            msg_proposal_id,
                            prev_log_proposal_id,
                            prev_lsn,
                            curr_lsn,
                            write_buf);
    ret = post_request_to_server_(server, push_log_req);
  }
  return ret;
}

int LogNetService::submit_committed_info_req(
      const common::ObAddr &server,
      const int64_t &msg_proposal_id,
      const int64_t prev_log_id,
      const int64_t &prev_log_proposal_id,
      const LSN &committed_end_lsn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogNetService has not inited!!!", K(ret));
  } else {
    CommittedInfo committed_info_req(msg_proposal_id, prev_log_id,
                            prev_log_proposal_id, committed_end_lsn);
    ret = post_request_to_server_(server, committed_info_req);
  }
  return ret;
}

int LogNetService::submit_push_log_resp(
    const ObAddr &server,
    const int64_t &msg_proposal_id,
    const LSN &lsn,
    const bool is_batch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogNetService not inited", K(ret), K(palf_id_),
        K(server), K(msg_proposal_id), K(lsn));
  } else if (false == server.is_valid()
             || INVALID_PROPOSAL_ID == msg_proposal_id
             || false == lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id_),
        K(server), K(msg_proposal_id), K(lsn));
  } else if (is_batch) {
    LogBatchPushResp push_log_resp(msg_proposal_id, lsn);
    ret = post_request_to_server_(server, push_log_resp);
  } else {
    LogPushResp push_log_resp(msg_proposal_id, lsn);
    ret = post_request_to_server_(server, push_log_resp);
  }
  return ret;
}

int LogNetService::submit_fetch_log_req(
    const ObAddr &server,
    const FetchLogType fetch_type,
    const int64_t msg_proposal_id,
    const LSN &prev_lsn,
    const LSN &lsn,
    const int64_t fetch_log_size,
    const int64_t fetch_log_count,
    const int64_t accepted_mode_pid)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogNetService not inited!!!", K(ret), K(palf_id_));
  } else {
    LogFetchReq fetch_log_req(fetch_type, msg_proposal_id, prev_lsn, lsn, fetch_log_size,
        fetch_log_count, accepted_mode_pid);
    ret = post_request_to_server_(server, fetch_log_req);
  }
  return ret;
}

int LogNetService::submit_batch_fetch_log_resp(
    const common::ObAddr &server,
    const int64_t msg_proposal_id,
    const int64_t prev_log_proposal_id,
    const LSN &prev_lsn,
    const LSN &curr_lsn,
    const LogWriteBuf &write_buf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogNetService not inited!!!", K(ret), K(palf_id_));
  } else {
    LogBatchFetchResp batch_fetch_log_resp(msg_proposal_id, prev_log_proposal_id,
        prev_lsn, curr_lsn, write_buf);
    ret = post_request_to_server_(server, batch_fetch_log_resp);
  }
  return ret;
}

int LogNetService::submit_notify_rebuild_req(
  const ObAddr &server,
  const LSN &base_lsn,
  const LogInfo &base_prev_log_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    NotifyRebuildReq notify_rebuild_req(base_lsn, base_prev_log_info);
    ret = post_request_to_server_(server, notify_rebuild_req);
  }
  return ret;
}

int LogNetService::submit_notify_fetch_log_req(const ObMemberList &dst_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    NotifyFetchLogReq notify_req;
    ret = post_request_to_member_list_(dst_list, notify_req);
  }
  return ret;
}

int LogNetService::submit_prepare_meta_resp(
    const common::ObAddr &server,
    const int64_t &msg_proposal_id,
    const bool vote_granted,
    const int64_t &log_proposal_id,
    const LSN &max_flushed_lsn,
    const LSN &committed_end_lsn,
    const LogModeMeta &mode_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogNetService not inited!!!", K(ret), K(palf_id_));
  } else {
    LogPrepareResp prepare_meta_resp(msg_proposal_id, vote_granted, log_proposal_id, max_flushed_lsn, \
        mode_meta, committed_end_lsn);
    ret = post_request_to_server_(server, prepare_meta_resp);
    PALF_LOG(INFO, "submit_prepare_meta_resp success", K(ret), K(server), K(msg_proposal_id));
  }
  return ret;
}

int LogNetService::submit_change_config_meta_resp(
    const ObAddr &server,
    const int64_t msg_proposal_id,
    const LogConfigVersion &config_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LogChangeConfigMetaResp req(msg_proposal_id, config_version);
    ret = post_request_to_server_(server, req);
  }
  return ret;
}

int LogNetService::submit_change_mode_meta_resp(
    const common::ObAddr &server,
    const int64_t &msg_proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LogChangeModeMetaResp req(msg_proposal_id);
    ret = post_request_to_server_(server, req);
  }
  return ret;
}

int LogNetService::submit_config_change_pre_check_req(
    const ObAddr &server,
    const LogConfigVersion &config_version,
    const bool need_purge_throttling,
    const int64_t timeout_us,
    LogGetMCStResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LogGetMCStReq req(config_version, need_purge_throttling);
    ret = post_sync_request_to_server_(server, timeout_us, req, resp);
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
int LogNetService::submit_get_arb_member_info_req(
      const common::ObAddr &server,
      const int64_t timeout_us,
      LogGetArbMemberInfoResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LogGetArbMemberInfoReq req(palf_id_);
    ret = post_sync_request_to_server_(server, timeout_us, req, resp);
  }
  return ret;
}
#endif

int LogNetService::submit_register_parent_req(
    const common::ObAddr &server,
    const LogLearner &child_itself,
    const bool is_to_leader)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LogRegisterParentReq req(child_itself, is_to_leader);
    ret = post_request_to_server_(server, req);
  }
  return ret;
}

int LogNetService::submit_register_parent_resp(
    const common::ObAddr &server,
    const LogLearner &parent_itself,
    const LogCandidateList &candidate_list,
    const RegisterReturn reg_ret)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LogRegisterParentResp resp(parent_itself, candidate_list, reg_ret);
    ret = post_request_to_server_(server, resp);
  }
  return ret;
}

int LogNetService::submit_retire_parent_req(
    const common::ObAddr &server,
    const LogLearner &child_itself)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LogLearnerReq retire_parent_req(child_itself, LogLearnerReqType::RETIRE_PARENT);
    ret = post_request_to_server_(server, retire_parent_req);
  }
  return ret;
}

int LogNetService::submit_retire_child_req(const common::ObAddr &server, const LogLearner &parent_itself)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LogLearnerReq retire_child_req(parent_itself, LogLearnerReqType::RETIRE_CHILD);
    ret = post_request_to_server_(server, retire_child_req);
  }
  return ret;
}

int LogNetService::submit_learner_keepalive_req(const common::ObAddr &server, const LogLearner &sender_itself)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LogLearnerReq req(sender_itself, LogLearnerReqType::KEEPALIVE_REQ);
    ret = post_request_to_server_(server, req);
  }
  return ret;
}

int LogNetService::submit_learner_keepalive_resp(const common::ObAddr &server, const LogLearner &sender_itself)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    LogLearnerReq req(sender_itself, LogLearnerReqType::KEEPALIVE_RESP);
    ret = post_request_to_server_(server, req);
  }
  return ret;
}

int LogNetService::submit_get_stat_req(const common::ObAddr &server,
                                       const int64_t timeout_us,
                                       const LogGetStatReq &req,
                                       LogGetStatResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = post_sync_request_to_server_(server, timeout_us, req, resp);
  }
  return ret;
}
} // end namespace palf
} // end namespace oceanbase
