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

#include "log_request_handler.h"
#include "log_req.h"
#include "palf_handle_impl_guard.h"
#include "palf_handle_impl.h"
#include "palf_env_impl.h"
#include "share/ob_occam_time_guard.h"

namespace oceanbase
{
namespace palf
{

using namespace election;

LogRequestHandler::LogRequestHandler(IPalfEnvImpl *palf_env_impl) : palf_env_impl_(palf_env_impl)
{
}

LogRequestHandler::~LogRequestHandler()
{
  palf_env_impl_ = NULL;
}

template <>
int LogRequestHandler::handle_request<LogPushReq>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogPushReq &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    const char *buf = req.write_buf_.write_buf_[0].buf_;
    const int64_t buf_len = req.write_buf_.write_buf_[0].buf_len_;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->receive_log(server,
                                                                 (PushLogType) req.push_log_type_,
                                                                 req.msg_proposal_id_,
                                                                 req.prev_lsn_,
                                                                 req.prev_log_proposal_id_,
                                                                 req.curr_lsn_,
                                                                 buf, buf_len))) {
      PALF_LOG(TRACE, "PalfHandleImpl receive_log failed", K(ret), K(palf_id),
          K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "PalfHandleImpl receive_log success", K(ret), K(palf_id),
          K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template <>
int LogRequestHandler::handle_request<LogPushResp>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogPushResp &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->ack_log(server, req.msg_proposal_id_,
          req.lsn_))){
      PALF_LOG(WARN, "PalfHandleImpl ack_log failed", K(ret), K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "PalfHandleImpl ack_log success", K(ret), K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template <>
int LogRequestHandler::handle_request<NotifyRebuildReq>(
    const int64_t palf_id,
    const ObAddr &server,
    const NotifyRebuildReq &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->handle_notify_rebuild_req(server,
            req.base_lsn_, req.base_prev_log_info_))){
      PALF_LOG(WARN, "PalfHandleImpl handle_notify_rebuild_req failed", K(ret), K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "PalfHandleImpl handle_notify_rebuild_req success", K(ret), K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template <>
int LogRequestHandler::handle_request<NotifyFetchLogReq>(
    const int64_t palf_id,
    const ObAddr &server,
    const NotifyFetchLogReq &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->handle_notify_fetch_log_req(server))){
      PALF_LOG(WARN, "PalfHandleImpl handle_notify_fetch_log_req failed", K(ret), K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "PalfHandleImpl handle_notify_fetch_log_req success", K(ret), K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template <>
int LogRequestHandler::handle_request<CommittedInfo>(
    const int64_t palf_id,
    const ObAddr &server,
    const CommittedInfo &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->handle_committed_info(server,
                                                                 req.msg_proposal_id_,
                                                                 req.prev_log_id_,
                                                                 req.prev_log_proposal_id_,
                                                                 req.committed_end_lsn_))) {
      PALF_LOG(WARN, "PalfHandleImpl handle_committed_info failed", K(ret), K(palf_id),
          K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "PalfHandleImpl handle_committed_info success", K(ret), K(palf_id),
          K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template <>
int LogRequestHandler::handle_request<LogFetchReq>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogFetchReq &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->get_log(server, (FetchLogType) req.fetch_type_, req.msg_proposal_id_,
          req.prev_lsn_, req.lsn_, req.fetch_log_size_, req.fetch_log_count_, req.accepted_mode_pid_))) {
      PALF_LOG(WARN, "PalfHandleImpl get_log failed", K(ret), K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "PalfHandleImpl get_log success", K(ret), K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template <>
int LogRequestHandler::handle_request<LogBatchFetchResp>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogBatchFetchResp &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    const char *buf = req.write_buf_.write_buf_[0].buf_;
    const int64_t buf_len = req.write_buf_.write_buf_[0].buf_len_;
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->receive_batch_log(server, req.msg_proposal_id_,
        req.prev_log_proposal_id_, req.prev_lsn_, req.curr_lsn_, buf, buf_len))) {
      PALF_LOG(WARN, "PalfHandleImpl receive_batch_log failed", K(ret), K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "PalfHandleImpl receive_batch_log success", K(ret), K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template <>
int LogRequestHandler::handle_request<LogPrepareReq>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogPrepareReq &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id,guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->handle_prepare_request(server, req.log_proposal_id_))) {
      PALF_LOG(WARN, "PalfHandleImpl handle_prepare_request failed", K(ret), K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "PalfHandleImpl handle_prepare_request success", K(ret), K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template <>
int LogRequestHandler::handle_request<LogPrepareResp>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogPrepareResp &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->handle_prepare_response(server, req.msg_proposal_id_,
          req.vote_granted_, req.log_proposal_id_, req.max_flushed_lsn_, req.committed_end_lsn_, req.log_mode_meta_))) {
      PALF_LOG(WARN, "PalfHandleImpl handle_prepare_response failed", K(ret), K(server),
          K(palf_id), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "PalfHandleImpl handle_prepare_response success", K(ret), K(server),
          K(palf_id), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template <>
int LogRequestHandler::handle_request<LogChangeConfigMetaReq>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogChangeConfigMetaReq &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id)
      || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->receive_config_log(server, req.msg_proposal_id_,
          req.prev_log_proposal_id_, req.prev_lsn_, req.prev_mode_pid_, req.meta_))) {
      PALF_LOG(WARN, "receive_config_log failed", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "receive_config_log success", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template <>
int LogRequestHandler::handle_request<LogChangeConfigMetaResp>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogChangeConfigMetaResp &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->ack_config_log(server, req.proposal_id_, req.config_version_))) {
      PALF_LOG(WARN, "ack_config_log failed", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "ack_config_log success", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template <>
int LogRequestHandler::handle_request<LogChangeModeMetaReq>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogChangeModeMetaReq &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id)
      || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->receive_mode_meta(server, req.msg_proposal_id_,
        req.is_applied_mode_meta_, req.meta_))) {
      PALF_LOG(WARN, "receive_mode_meta failed", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(INFO, "receive_mode_meta success", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template <>
int LogRequestHandler::handle_request<LogChangeModeMetaResp>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogChangeModeMetaResp &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->ack_mode_meta(server, req.msg_proposal_id_))) {
      PALF_LOG(WARN, "ack_mode_meta failed", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "ack_mode_meta success", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template<>
int LogRequestHandler::handle_request<LogLearnerReq>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogLearnerReq &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->handle_learner_req(req.sender_, req.req_type_))) {
      PALF_LOG(WARN, "handle_learner_req failed", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "handle_learner_req success", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template<>
int LogRequestHandler::handle_request<LogRegisterParentReq>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogRegisterParentReq &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->handle_register_parent_req(req.child_, req.is_to_leader_))) {
      PALF_LOG(WARN, "handle_register_parent_req failed", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "handle_register_parent_req success", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template<>
int LogRequestHandler::handle_request<LogRegisterParentResp>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogRegisterParentResp &req)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->handle_register_parent_resp(req.parent_, req.candidate_list_, req.reg_ret_))) {
      PALF_LOG(WARN, "handle_register_parent_resp failed", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(TRACE, "handle_register_parent_resp success", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    }
  }
  return ret;
}

/********************[Election Message]********************/
#define HANDLE_ELECTION_MSG(MsgType) \
template <>\
int LogRequestHandler::handle_request<MsgType>(\
    const int64_t palf_id,\
    const ObAddr &server,\
    const MsgType &req)\
{\
  TIMEGUARD_INIT(ELECT, 50_ms, 10_s);\
  int ret = common::OB_SUCCESS;\
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {\
    ret = OB_INVALID_ARGUMENT;\
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req));\
  } else {\
    IPalfHandleImplGuard guard;\
    if (CLICK_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {\
      PALF_LOG(WARN, "ObLogMgr get_log_service failed", K(ret), K(palf_id), KP(palf_env_impl_));\
    } else if (CLICK_FAIL(guard.get_palf_handle_impl()->handle_election_message(req))) {\
      PALF_LOG(WARN, "handle message failed", K(ret), K(palf_id), K(server), K(req));\
    } else {\
      PALF_LOG(DEBUG, "handle message success", K(ret), K(palf_id), K(server), K(req));\
    }\
  }\
  return ret;\
}
HANDLE_ELECTION_MSG(ElectionPrepareRequestMsg)
HANDLE_ELECTION_MSG(ElectionPrepareResponseMsg)
HANDLE_ELECTION_MSG(ElectionAcceptRequestMsg)
HANDLE_ELECTION_MSG(ElectionAcceptResponseMsg)
HANDLE_ELECTION_MSG(ElectionChangeLeaderMsg)

template <>
int LogRequestHandler::handle_sync_request<LogGetMCStReq, LogGetMCStResp>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogGetMCStReq &req,
    LogGetMCStResp &resp)
{
  int ret = common::OB_SUCCESS;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else {
    IPalfHandleImplGuard guard;
    if (false == palf_env_impl_->check_disk_space_enough()) {
      resp.is_normal_replica_ = false;
      PALF_LOG(WARN, "check_disk_space_enough returns false", K(req), K(resp));
    } else if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
      PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
    } else if (OB_FAIL(guard.get_palf_handle_impl()->handle_config_change_pre_check(server, req, resp))) {
      PALF_LOG(WARN, "PalfHandleImpl config_change_pre_check failed", K(ret), K(palf_id), K(server), K(req), KPC(palf_env_impl_));
    } else {
      PALF_LOG(INFO, "PalfHandleImpl config_change_pre_check success", K(ret), K(palf_id), K(server), K(req), K(resp), KPC(palf_env_impl_));
    }
  }
  return ret;
}

template <>
int LogRequestHandler::handle_sync_request<LogGetStatReq, LogGetStatResp>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogGetStatReq &req,
    LogGetStatResp &resp)
{
  int ret = common::OB_SUCCESS;
  IPalfHandleImplGuard guard;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
    PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
  } else if (req.get_type_ == LogGetStatType::GET_LEADER_MAX_SCN) {
    common::ObRole role = FOLLOWER;
    int64_t unused_pid;
    bool is_pending_state = true;
    if (OB_FAIL(guard.get_palf_handle_impl()->get_role(role, unused_pid, is_pending_state))) {
      CLOG_LOG(WARN, "palf_handle get_role failed", K(ret), K(palf_id), K(server));
    } else if ((role != LEADER || true == is_pending_state)) {
      ret = OB_NOT_MASTER;
      CLOG_LOG(INFO, "i am not leader", K(ret), K(palf_id), K(req), K(role), K(is_pending_state));
    } else {
      resp.max_scn_ = guard.get_palf_handle_impl()->get_max_scn();
      resp.end_lsn_ = guard.get_palf_handle_impl()->get_end_lsn();
      CLOG_LOG(TRACE, "get_leader_max_scn success", K(ret), K(palf_id), K(server), K(req), K(resp));
    }
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
template <>
int LogRequestHandler::handle_sync_request<LogGetArbMemberInfoReq, LogGetArbMemberInfoResp>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogGetArbMemberInfoReq &req,
    LogGetArbMemberInfoResp &resp)
{
  int ret = common::OB_SUCCESS;
  IPalfHandleImplGuard guard;
  if (false == is_valid_palf_id(palf_id) || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_env_impl_));
  } else if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
    PALF_LOG(WARN, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id), K(server));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->get_arb_member_info(resp.arb_member_info_))) {
    PALF_LOG(WARN, "get_arb_member_info failed", K(ret), K(palf_id), K(server));
  } else {
    PALF_LOG(INFO, "get_arb_member_info succ", K(ret), K(palf_id), K(server), K(req), K(resp));
  }
  return ret;
}
#endif

} // end namespace palf
} // end namespace oceanbase
