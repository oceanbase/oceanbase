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
#include "logservice/palf/log_define.h"

namespace oceanbase
{
namespace logservice
{
LogRequestHandler::LogRequestHandler(palf::PalfHandle *palf_handle) : palf_handle_(palf_handle),
                                                                      handle_request_print_time_(OB_INVALID_TIMESTAMP)
{
}

LogRequestHandler::~LogRequestHandler()
{
  palf_handle_ = NULL;
  handle_request_print_time_ = OB_INVALID_TIMESTAMP;
}

template <>
int LogRequestHandler::handle_sync_request<LogConfigChangeCmd, LogConfigChangeCmdResp>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogConfigChangeCmd &req,
    LogConfigChangeCmdResp &resp)
{
  int ret = common::OB_SUCCESS;
  if (palf_id < 0 || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_handle_));
  } else {
    ConfigChangeCmdHandler cmd_handler(palf_handle_);
    if (OB_FAIL(cmd_handler.handle_config_change_cmd(req))) {
      if (palf::palf_reach_time_interval(100 * 1000, handle_request_print_time_))
      PALF_LOG(WARN, "handle_config_change_cmd failed", K(ret), K(palf_id), K(server), K(req), KPC(palf_handle_));
    } else {
      PALF_LOG(INFO, "handle_config_change_cmd success", K(ret), K(palf_id), K(server), K(req), K(resp), KPC(palf_handle_));
    }
    resp.ret_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

template <>
int LogRequestHandler::handle_sync_request<LogGetPalfStatReq, LogGetPalfStatResp>(
    const int64_t palf_id,
    const ObAddr &server,
    const LogGetPalfStatReq &req,
    LogGetPalfStatResp &resp)
{
  int ret = common::OB_SUCCESS;
  common::ObRole role = FOLLOWER;
  int64_t unused_pid;
  bool unused_bool;
  if (palf_id < 0 || false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(palf_id), K(req), KPC(palf_handle_));
  } else if (OB_FAIL(palf_handle_->get_role(role, unused_pid, unused_bool))) {
    PALF_LOG(WARN, "palf_handle get_role failed", K(ret), K(palf_id), K(server), KPC(palf_handle_));
  } else if (role != LEADER) {
    ret = OB_NOT_MASTER;
  } else if (OB_FAIL(palf_handle_->get_max_ts_ns(resp.max_ts_ns_))) {
    PALF_LOG(WARN, "palf_handle stat failed", K(ret), K(palf_id), K(server), KPC(palf_handle_));
  } else {
    PALF_LOG(INFO, "palf_handle stat success", K(ret), K(palf_id), K(server), K(resp));
  }
  return ret;
}

int ConfigChangeCmdHandler::get_reporter_(ObLogReporterAdapter *&reporter) const
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = NULL;
  if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "get_log_service failed", K(ret));
  } else if (OB_ISNULL(reporter = log_service->get_reporter())) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "log_service.get_reporter failed", K(ret));
  } else {
    PALF_LOG(TRACE, "__get_reporter", KP(reporter), KP(log_service), K(MTL_ID()));
  }
  return ret;
}

int ConfigChangeCmdHandler::handle_config_change_cmd(const LogConfigChangeCmd &req) const
{
  int ret = OB_SUCCESS;
  ObLogReporterAdapter *reporter;
  if (NULL == palf_handle_) {
    ret = OB_NOT_INIT;
  } else if (false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_reporter_(reporter))) {
    PALF_LOG(ERROR, "get_reporter failed", K(req.palf_id_));
  } else {
    switch (req.cmd_type_) {
      case CHANGE_REPLICA_NUM_CMD:
        ret = palf_handle_->change_replica_num(req.curr_member_list_, req.curr_replica_num_,
            req.new_replica_num_, req.timeout_ns_);
        break;
      case ADD_MEMBER_CMD:
        ret = palf_handle_->add_member(req.added_member_, req.new_replica_num_, req.timeout_ns_);
        break;
      case ADD_ARB_MEMBER_CMD:
        ret = palf_handle_->add_arb_member(req.added_member_, req.new_replica_num_,
            req.timeout_ns_);
        break;
      case REMOVE_MEMBER_CMD:
        ret = palf_handle_->remove_member(req.removed_member_, req.new_replica_num_,
            req.timeout_ns_);
        break;
      case REMOVE_ARB_MEMBER_CMD:
        ret = palf_handle_->remove_arb_member(req.removed_member_, req.new_replica_num_,
            req.timeout_ns_);
        break;
      case REPLACE_MEMBER_CMD:
        ret = palf_handle_->replace_member(req.added_member_, req.removed_member_, req.timeout_ns_);
        break;
      case REPLACE_ARB_MEMBER_CMD:
        ret = palf_handle_->replace_arb_member(req.added_member_, req.removed_member_, req.timeout_ns_);
        break;
      case ADD_LEARNER_CMD:
        ret = palf_handle_->add_learner(req.added_member_, req.timeout_ns_);
        break;
      case REMOVE_LEARNER_CMD:
        ret = palf_handle_->remove_learner(req.removed_member_, req.timeout_ns_);
        break;
      case SWITCH_TO_ACCEPTOR_CMD:
        ret = palf_handle_->switch_learner_to_acceptor(req.removed_member_, req.timeout_ns_);
        break;
      case SWITCH_TO_LEARNER_CMD:
        ret = palf_handle_->switch_acceptor_to_learner(req.removed_member_, req.timeout_ns_);
        break;
      default:
        break;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(reporter->report_replica_info(req.palf_id_))) {
    PALF_LOG(WARN, "report_replica_info failed", K(ret), K(req.palf_id_), K(req));
  }
  return ret;
}
} // end namespace logservice
} // end namespace oceanbase
