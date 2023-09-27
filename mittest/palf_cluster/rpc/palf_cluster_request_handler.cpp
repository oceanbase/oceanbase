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
#include "palf_cluster_request_handler.h"
#include "palf_cluster_rpc_req.h"
#include "palf_cluster_rpc_proxy.h"
#include "logservice/ob_log_service.h"
#include "logservice/palf/log_define.h"

namespace oceanbase
{
namespace palfcluster
{
LogRequestHandler::LogRequestHandler()
  : log_service_(NULL)
{
}

LogRequestHandler::~LogRequestHandler()
{
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

template <>
int LogRequestHandler::handle_request<LogCreateReplicaCmd>(
    const LogCreateReplicaCmd &req)
{
  int ret = common::OB_SUCCESS;
  const int64_t palf_id = req.ls_id_;
  const common::ObAddr &server = req.src_;
  share::ObLSID ls_id(palf_id);
  if (OB_ISNULL(log_service_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "get_log_service failed", K(ret));
  } else if (false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "Invalid argument!!!", K(ret), K(req));
  } else if (OB_FAIL(log_service_->create_palf_replica(req.ls_id_, req.member_list_, req.replica_num_, req.leader_idx_))) {
    CLOG_LOG(ERROR, "create_palf_replica failed", K(ret));
  }
  CLOG_LOG(ERROR, "create_palf_replica finish", K(ret), K(req));
  return ret;
}

template <>
int LogRequestHandler::handle_request<SubmitLogCmd>(
    const SubmitLogCmd &req)
{
  int ret = common::OB_SUCCESS;
  if (false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "Invalid argument!!!", K(ret), K(req));
  } else {
    const int64_t palf_id = req.ls_id_;
    share::ObLSID ls_id(palf_id);
    const common::ObAddr &src = req.src_;
    const int64_t client_id = req.client_id_;

    palfcluster::LogClientMap *log_clients = nullptr;
    palfcluster::ObLogClient *log_client = nullptr;

    if (OB_ISNULL(log_service_)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "get_log_service failed", K(ret));
    } else if (FALSE_IT(log_clients = log_service_->get_log_client_map())) {
    } else if (OB_FAIL(log_clients->get(ls_id, log_client))) {
      CLOG_LOG(ERROR, "get_log_client failed", K(ret), K(palf_id));
    } else if (OB_FAIL(log_client->submit_log(src, client_id, req.log_buf_))) {
      CLOG_LOG(ERROR, "submit_log failed", K(ret));
    }
    ret = OB_SUCCESS;
  }
  return ret;
}

template <>
int LogRequestHandler::handle_request<SubmitLogCmdResp>(
    const SubmitLogCmdResp &req)
{
  int ret = common::OB_SUCCESS;
  if (false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "Invalid argument!!!", K(ret), K(req));
  } else {
    const int64_t palf_id = req.ls_id_;
    share::ObLSID ls_id(palf_id);
    const common::ObAddr &server = req.src_;
    const int64_t client_id = req.client_id_;

    palfcluster::LogClientMap *log_clients = nullptr;
    palfcluster::ObLogClient *log_client = nullptr;

    if (OB_ISNULL(log_service_)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "get_log_service failed", K(ret));
    } else {
      log_service_->clients_[client_id].has_returned();
    }
    ret = OB_SUCCESS;
  }
  return ret;
}
} // end namespace logservice
} // end namespace oceanbase
