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
#include "ob_log_request_handler.h"
#include "ob_log_rpc_req.h"
#include "logservice/ob_log_service.h"
#include "logservice/ob_log_handler.h"
#include "logservice/logrpc/ob_log_rpc_proxy.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "logservice/palf/log_define.h"
#include "logservice/replayservice/ob_log_replay_service.h"

namespace oceanbase
{
namespace logservice
{
LogRequestHandler::LogRequestHandler()
{
}

LogRequestHandler::~LogRequestHandler()
{
}

int LogRequestHandler::get_palf_handle_guard_(const int64_t palf_id,
    palf::PalfHandleGuard &palf_handle_guard) const
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = nullptr;
  share::ObLSID ls_id(palf_id);
  if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get_log_service failed", K(ret));
  } else if (OB_FAIL(log_service->open_palf(ls_id, palf_handle_guard))) {
    CLOG_LOG(WARN, "open palf failed", K(ret), K(palf_id));
  }
	return ret;
}

int LogRequestHandler::get_log_handler_(
    const int64_t palf_id,
    logservice::ObLogHandler *&log_handler) const
{
  log_handler = nullptr;
  storage::ObLSService *ls_svr = MTL(ObLSService*);
  storage::ObLS *ls = nullptr;
  share::ObLSID ls_id(palf_id);
  storage::ObLSHandle handle;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "mtl ObLSService should not be null", K(ret));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, handle, ObLSGetMod::LOG_MOD))) {
    CLOG_LOG(WARN, "get ls failed", KR(ret), K(ls_id));
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "ls should not be null", KR(ret));
  } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "log_handler is null", KR(ret), K(ls_id));
  }
  return ret;
}

int LogRequestHandler::get_self_addr_(common::ObAddr &self) const
{
  int ret = OB_SUCCESS;
  const common::ObAddr self_addr = GCTX.self_addr();
  if (false == self_addr.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    self = self_addr;
  }
  return ret;
}

int LogRequestHandler::get_rpc_proxy_(obrpc::ObLogServiceRpcProxy *&rpc_proxy) const
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = NULL;
  if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get_log_service failed", K(ret));
  } else if (OB_ISNULL(rpc_proxy = log_service->get_rpc_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "log_service.get_rpc_proxy failed", K(ret));
  } else {
    CLOG_LOG(TRACE, "get_rpc_proxy_", KP(rpc_proxy), KP(log_service), K(MTL_ID()));
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
int LogRequestHandler::get_arb_service_(ObArbitrationService *&arb_service) const
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = NULL;
  if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get_log_service failed", K(ret));
  } else if (OB_ISNULL(arb_service = log_service->get_arbitration_service())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "log_service.get_arbitration_service failed", K(ret));
  } else {
    CLOG_LOG(TRACE, "get_arb_service_", KP(arb_service), KP(log_service), K(MTL_ID()));
  }
  return ret;
}
#endif

int LogRequestHandler::get_flashback_service_(ObLogFlashbackService *&flashback_srv) const
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = NULL;
  if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get_log_service failed", K(ret));
  } else if (OB_ISNULL(flashback_srv = log_service->get_flashback_service())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "log_service.get_flashback_service failed", K(ret));
  } else {
    CLOG_LOG(TRACE, "get_flashback_service success", KP(flashback_srv), KP(log_service), K(MTL_ID()));
  }
  return ret;
}

int LogRequestHandler::get_replay_service_(ObLogReplayService *&replay_srv) const
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = NULL;
  if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get_log_service failed", K(ret));
  } else if (OB_ISNULL(replay_srv = log_service->get_log_replay_service())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "log_service.get_log_replay_service failed", K(ret));
  } else {
    CLOG_LOG(TRACE, "get_replay_service success", KP(replay_srv), KP(log_service), K(MTL_ID()));
  }
  return ret;
}

template <>
int LogRequestHandler::handle_sync_request<LogConfigChangeCmd, LogConfigChangeCmdResp>(
    const LogConfigChangeCmd &req,
    LogConfigChangeCmdResp &resp)
{
  int ret = common::OB_SUCCESS;
  if (false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "Invalid argument!!!", K(ret), K(req));
  } else {
    const int64_t palf_id = req.palf_id_;
    const common::ObAddr &server = req.src_;
    palf::PalfHandleGuard palf_handle_guard;
    if (OB_FAIL(get_palf_handle_guard_(palf_id, palf_handle_guard))) {
      CLOG_LOG(WARN, "get_palf_handle_guard_ failed", K(ret), K(palf_id));
    } else {
      palf::PalfHandle *palf_handle = palf_handle_guard.get_palf_handle();
      ConfigChangeCmdHandler cmd_handler(palf_handle);
      if (OB_FAIL(cmd_handler.handle_config_change_cmd(req, resp))) {
        CLOG_LOG(WARN, "handle_config_change_cmd failed", K(ret), K(palf_id), K(server), K(req));
      } else {
        CLOG_LOG(INFO, "handle_config_change_cmd success", K(ret), K(palf_id), K(server), K(req), K(resp));
      }
      resp.ret_ = ret;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

template <>
int LogRequestHandler::handle_sync_request<LogGetPalfStatReq, LogGetPalfStatResp>(
    const LogGetPalfStatReq &req,
    LogGetPalfStatResp &resp)
{
  int ret = common::OB_SUCCESS;
  if (false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "Invalid argument!!!", K(ret), K(req));
  } else {
    palf::PalfHandleGuard palf_handle_guard;
    const int64_t palf_id = req.palf_id_;
    const common::ObAddr &server = req.src_;
    common::ObRole role = FOLLOWER;
    int64_t unused_pid;
    bool is_pending_state = true;
    int64_t paxos_replica_num = 0;
    if (OB_FAIL(get_palf_handle_guard_(palf_id, palf_handle_guard))) {
      CLOG_LOG(WARN, "get_palf_handle_guard_ failed", K(ret), K(palf_id));
    } else if (req.is_to_leader_ && OB_FAIL(palf_handle_guard.get_role(role, unused_pid, is_pending_state))) {
      CLOG_LOG(WARN, "palf_handle get_role failed", K(ret), K(palf_id), K(server));
    } else if (req.is_to_leader_ && (role != LEADER || true == is_pending_state)) {
      ret = OB_NOT_MASTER;
      CLOG_LOG(WARN, "get_palf_stat failed", K(ret), K(req), K(role), K(is_pending_state));
    } else if (OB_FAIL(palf_handle_guard.stat(resp.palf_stat_))) {
      CLOG_LOG(WARN, "palf stat failed", K(ret), K(palf_id), K(server));
    } else {
      CLOG_LOG(TRACE, "get_palf_stat success", K(ret), K(palf_id), K(server), K(req), K(resp));
    }
  }
  return ret;
}

int ConfigChangeCmdHandler::get_reporter_(ObLogReporterAdapter *&reporter) const
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = NULL;
  if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get_log_service failed", K(ret));
  } else if (OB_ISNULL(reporter = log_service->get_reporter())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "log_service.get_reporter failed", K(ret));
  } else {
    CLOG_LOG(TRACE, "__get_reporter", KP(reporter), KP(log_service), K(MTL_ID()));
  }
  return ret;
}

int ConfigChangeCmdHandler::handle_config_change_cmd(const LogConfigChangeCmd &req,
                                                     LogConfigChangeCmdResp &resp) const
{
  int ret = OB_SUCCESS;
  ObLogReporterAdapter *reporter;
  if (NULL == palf_handle_) {
    ret = OB_NOT_INIT;
  } else if (false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_reporter_(reporter))) {
    CLOG_LOG(ERROR, "get_reporter failed", K(req.palf_id_));
  } else {
    switch (req.cmd_type_) {
      case FORCE_SINGLE_MEMBER_CMD:
        ret = palf_handle_->force_set_as_single_replica();
        break;
      case CHANGE_REPLICA_NUM_CMD:
        ret = palf_handle_->change_replica_num(req.curr_member_list_, req.curr_replica_num_,
            req.new_replica_num_, req.timeout_us_);
        break;
      case ADD_MEMBER_CMD:
        ret = palf_handle_->add_member(req.added_member_, req.new_replica_num_, req.config_version_, req.timeout_us_);
        break;
      case REMOVE_MEMBER_CMD:
        ret = palf_handle_->remove_member(req.removed_member_, req.new_replica_num_, req.timeout_us_);
        break;
#ifdef OB_BUILD_ARBITRATION
      case ADD_ARB_MEMBER_CMD:
        ret = palf_handle_->add_arb_member(req.added_member_, req.timeout_us_);
        break;
      case REMOVE_ARB_MEMBER_CMD:
        ret = palf_handle_->remove_arb_member(req.removed_member_, req.timeout_us_);
        break;
#endif
      case REPLACE_MEMBER_CMD:
        ret = palf_handle_->replace_member(req.added_member_, req.removed_member_, req.config_version_, req.timeout_us_);
        break;
      case ADD_LEARNER_CMD:
        ret = palf_handle_->add_learner(req.added_member_, req.timeout_us_);
        break;
      case REMOVE_LEARNER_CMD:
        ret = palf_handle_->remove_learner(req.removed_member_, req.timeout_us_);
        break;
      case SWITCH_TO_ACCEPTOR_CMD:
        ret = palf_handle_->switch_learner_to_acceptor(req.added_member_, req.new_replica_num_, req.config_version_, req.timeout_us_);
        break;
      case SWITCH_TO_LEARNER_CMD:
        ret = palf_handle_->switch_acceptor_to_learner(req.removed_member_, req.new_replica_num_, req.timeout_us_);
        break;
      case TRY_LOCK_CONFIG_CHANGE_CMD:
        ret = palf_handle_->try_lock_config_change(req.lock_owner_, req.timeout_us_);
        break;
      case UNLOCK_CONFIG_CHANGE_CMD:
        ret = palf_handle_->unlock_config_change(req.lock_owner_, req.timeout_us_);
        break;
      case GET_CONFIG_CHANGE_LOCK_STAT_CMD:
        ret = palf_handle_->get_config_change_lock_stat(resp.lock_owner_, resp.is_locked_);
        break;
      case REPLACE_LEARNERS_CMD:
        ret = palf_handle_->replace_learners(req.added_list_, req.removed_list_, req.timeout_us_);
        break;
      case REPLACE_MEMBER_WITH_LEARNER_CMD:
        ret = palf_handle_->replace_member_with_learner(req.added_member_, req.removed_member_, req.config_version_, req.timeout_us_);
        break;
      default:
        break;
    }
  }
  resp.ret_ = ret;
  if (OB_SUCC(ret) && OB_FAIL(reporter->report_replica_info(req.palf_id_))) {
    CLOG_LOG(WARN, "report_replica_info failed", K(ret), K(req.palf_id_), K(req));
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
template <>
int LogRequestHandler::handle_request<LogServerProbeMsg>(const LogServerProbeMsg &req)
{
  int ret = common::OB_SUCCESS;
  ObArbitrationService *arb_service;
  const common::ObAddr &server = req.src_;
  if (false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "Invalid argument!!!", K(ret), K(req));
  } else if (OB_FAIL(get_arb_service_(arb_service))) {
    CLOG_LOG(ERROR, "get_arb_service_ failed", K(ret), K(req));
  } else if (OB_FAIL(arb_service->handle_server_probe_msg(server, req))) {
    CLOG_LOG(WARN, "handle_server_probe_msg failed", K(ret), K(req));
  } else {
    CLOG_LOG(TRACE, "handle_server_probe_msg success", K(ret), K(server), K(req));
  }
  return ret;
}
#endif

template <>
int LogRequestHandler::handle_request<LogChangeAccessModeCmd>(const LogChangeAccessModeCmd &req)
{
  int ret = common::OB_SUCCESS;
  if (false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "Invalid argument!!!", K(ret), K(req));
  } else if (OB_FAIL(change_access_mode_(req))) {
    CLOG_LOG(WARN, "handle_request fail", K(ret), K(req));
  } else {
    CLOG_LOG(TRACE, "handle_request success", K(ret), K(req));
  }
  return ret;
}

int LogRequestHandler::change_access_mode_(const LogChangeAccessModeCmd &req)
{
  int ret = OB_SUCCESS;
  const int64_t palf_id = req.ls_id_;
  const common::ObAddr &server = req.src_;
  logservice::ObLogHandler *log_handler = nullptr;
  if (OB_FAIL(get_log_handler_(palf_id, log_handler))) {
    CLOG_LOG(WARN, "get_log_handler_ failed", K(ret), K(palf_id), K(server));
  } else if (OB_FAIL(log_handler->change_access_mode(req.mode_version_, req.access_mode_, req.ref_scn_))) {
    CLOG_LOG(WARN, "change_access_mode failed", K(ret), K(palf_id), K(server));
  } else {
    CLOG_LOG(INFO, "change_access_mode success", K(ret), K(req));
  }
  return ret;
}

template <>
int LogRequestHandler::handle_request<LogFlashbackMsg>(const LogFlashbackMsg &req)
{
  int ret = common::OB_SUCCESS;
  const int64_t palf_id = req.ls_id_;
  share::ObLSID ls_id(palf_id);
  const common::ObAddr &server = req.src_;
  const bool is_flashback_req = req.is_flashback_req();
  if (false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "Invalid argument!!!", K(ret), K(req));
  } else if (is_flashback_req) {
    constexpr int64_t FLASHBACK_TIMEOUT_US = 2 * 1000L * 1000L;     // 1s
    constexpr int64_t CONN_TIMEOUT_US = 1000 * 1000;
    palf::PalfHandleGuard palf_handle_guard;
    obrpc::ObLogServiceRpcProxy *rpc_proxy = NULL;
    common::ObAddr self;
    logservice::ObLogReplayService *replay_srv = nullptr;
    palf::AccessMode curr_access_mode = palf::AccessMode::INVALID_ACCESS_MODE;
    int64_t curr_mode_version = 0;
    if (OB_FAIL(get_replay_service_(replay_srv))) {
      CLOG_LOG(WARN, "get_replay_service_ failed", K(ret), K(palf_id));
    } else if (OB_FAIL(get_palf_handle_guard_(palf_id, palf_handle_guard))) {
      CLOG_LOG(WARN, "get_palf_handle_guard_ failed", K(ret), K(palf_id));
    } else if (OB_FAIL(palf_handle_guard.get_access_mode(curr_mode_version, curr_access_mode))) {
      CLOG_LOG(WARN, "get_access_mode failed", K(ret), K(palf_id), K(self));
    } else if (req.mode_version_ != curr_mode_version || palf::AccessMode::FLASHBACK != curr_access_mode) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "access_mode do not match, can not do flashback", K(ret), K(palf_id), K(self), K(curr_mode_version),
          K(curr_access_mode), K(req));
    } else if (OB_FAIL(palf_handle_guard.flashback(req.mode_version_, req.flashback_scn_, FLASHBACK_TIMEOUT_US))) {
      CLOG_LOG(WARN, "flashback failed", K(ret), K(palf_id), K(req));
    } else if (OB_FAIL(get_rpc_proxy_(rpc_proxy))) {
      CLOG_LOG(WARN, "get_rpc_proxy_ failed", K(ret), K(palf_id));
    } else if (OB_FAIL(get_self_addr_(self))) {
      CLOG_LOG(WARN, "get_self_addr_ failed", K(ret), K(palf_id), K(self));
    } else {
      const uint64_t src_tenant_id = req.src_tenant_id_;
      LogFlashbackMsg flashback_resp(MTL_ID(), self, palf_id, req.mode_version_, req.flashback_scn_, false);
      if (OB_FAIL(rpc_proxy->to(server).timeout(CONN_TIMEOUT_US).trace_time(true).
          max_process_handler_time(CONN_TIMEOUT_US).by(src_tenant_id).send_log_flashback_msg(flashback_resp, NULL))) {
        CLOG_LOG(WARN, "send_log_flashback_msg failed", K(ret), K(palf_id), K(server), K(flashback_resp));
      }
    }
    CLOG_LOG(INFO, "handle_log_flashback_msg finish", K(ret), K(req));
  } else {
    logservice::ObLogFlashbackService *flashback_srv = nullptr;
    if (OB_FAIL(get_flashback_service_(flashback_srv))) {
      CLOG_LOG(WARN, "get_flashback_service_ failed", K(ret), K(palf_id));
    } else if (OB_FAIL(flashback_srv->handle_flashback_resp(req))) {
      CLOG_LOG(WARN, "handle_flashback_resp failed", K(ret), K(req));
    } else {
      CLOG_LOG(INFO, "handle_flashback_resp success", K(ret), K(req));
    }
  }
  return ret;
}
} // end namespace logservice
} // end namespace oceanbase
