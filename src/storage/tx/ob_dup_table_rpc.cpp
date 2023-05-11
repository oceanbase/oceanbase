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

#include "storage/tx/ob_dup_table_rpc.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_dup_table.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace storage;

namespace transaction
{
OB_SERIALIZE_MEMBER(ObDupTableMsgHeader, src_, dst_, proxy_);
OB_SERIALIZE_MEMBER(ObDupTableLeaseRequestMsg, request_ts_, addr_, last_log_id_, request_lease_interval_us_, src_, dst_, proxy_, gts_);
OB_SERIALIZE_MEMBER(ObDupTableLeaseResponseMsg, request_ts_, cur_log_id_, status_, lease_interval_us_, src_, dst_, proxy_, gts_);
OB_SERIALIZE_MEMBER(ObRedoLogSyncRequestMsg, log_id_, log_ts_, trans_id_, src_, dst_, proxy_, log_type_);
OB_SERIALIZE_MEMBER(ObRedoLogSyncResponseMsg, log_id_, trans_id_, addr_, status_, src_, dst_, proxy_, gts_);
OB_SERIALIZE_MEMBER(ObPreCommitRequestMsg, src_, dst_, proxy_, tenant_id_, trans_id_, commit_version_);
OB_SERIALIZE_MEMBER(ObPreCommitResponseMsg, src_, dst_, proxy_, trans_id_, addr_, result_);

void ObDupTableMsgHeader::reset()
{
  src_.reset();
  dst_.reset();
  proxy_.reset();
}

int ObDupTableMsgHeader::set_header(const ObAddr &src, const ObAddr &proxy, const ObAddr &dst)
{
  int ret = OB_SUCCESS;
  src_ = src;
  dst_ = dst;
  proxy_ = proxy;
  return ret;
}

void ObDupTableLeaseRequestMsg::reset()
{
  request_ts_ = -1;
  addr_.reset();
  last_log_id_ = 0;
  request_lease_interval_us_ = 0;
  gts_ = -1;
}

bool ObDupTableLeaseRequestMsg::is_valid() const
{
  return request_ts_ > 0 && 
    addr_.is_valid() && last_log_id_ > 0 &&
    request_lease_interval_us_ > 0 && gts_ > 0;
}

int ObDupTableLeaseRequestMsg::init(const int64_t request_ts,
                                    const common::ObAddr &addr,
                                    const uint64_t last_log_id,
                                    const int64_t request_lease_interval_us,
                                    const int64_t gts)
{
  int ret = OB_SUCCESS;

  // if (OB_UNLIKELY(request_ts <= 0 ||
  //                 !partition.is_valid() ||
  //                 !addr.is_valid() ||
  //                 last_log_id <= 0 ||
  //                 request_lease_interval_us <= 0 ||
  //                 gts <= 0)) {
  //   ret = OB_INVALID_ARGUMENT;
  //   TRANS_LOG(WARN, "invalid argument", KR(ret), K(request_ts), K(partition), K(addr),
  //                                       K(last_log_id), K(request_lease_interval_us), K(gts));
  // } else {
  //   request_ts_ = request_ts;
  //   partition_ = partition;
  //   addr_ = addr;
  //   last_log_id_ = last_log_id;
  //   request_lease_interval_us_ = request_lease_interval_us;
  //   gts_ = gts;
  // }

  return ret;
}

void ObDupTableLeaseResponseMsg::reset()
{
  request_ts_ = -1;
  cur_log_id_ = 0;
  status_ = ObDupTableLeaseStatus::OB_DUP_TABLE_LEASE_UNKNOWN;
  lease_interval_us_ = 0;
  gts_ = -1;
}

bool ObDupTableLeaseResponseMsg::is_valid() const
{
  return request_ts_ > 0 && lease_interval_us_ > 0 && gts_ > 0;
}

int ObDupTableLeaseResponseMsg::init(const int64_t request_ts,
                                     const uint64_t cur_log_id,
                                     const enum ObDupTableLeaseStatus status,
                                     const int64_t lease_interval_us,
                                     const int64_t gts)
{
  int ret = OB_SUCCESS;

  // if (OB_UNLIKELY(request_ts <= 0 ||
  //                 !partition.is_valid() ||
  //                 lease_interval_us <= 0 ||
  //                 gts <= 0)) {
  //   ret = OB_INVALID_ARGUMENT;
  //   TRANS_LOG(WARN, "invalid argument", KR(ret), K(request_ts), K(partition),
  //                                       K(lease_interval_us), K(gts));
  // } else {
  //   request_ts_ = request_ts;
  //   partition_ = partition;
  //   cur_log_id_ = cur_log_id;
  //   status_ = status;
  //   lease_interval_us_ = lease_interval_us;
  //   gts_ = gts;
  // }

  return ret;
}

int ObRedoLogSyncRequestMsg::init(
                                  const uint64_t log_id,
                                  const int64_t log_ts,
                                  const int64_t log_type,
                                  const ObTransID &trans_id)
{
  int ret = OB_SUCCESS;

  // if (OB_UNLIKELY(!partition.is_valid() ||
  //                 log_id <= 0 ||
  //                 log_ts <= 0 ||
  //                 !ObTransLogType::is_valid(log_type) ||
  //                 !trans_id.is_valid())) {
  //   ret = OB_INVALID_ARGUMENT;
  //   TRANS_LOG(WARN, "invalid argument", KR(ret),
  //             K(partition), K(log_id), K(log_ts),
  //             K(log_type), K(trans_id));
  // } else {
  //   partition_ = partition;
  //   log_id_ = log_id;
  //   log_ts_ = log_ts;
  //   trans_id_ = trans_id;
  //   log_type_ = log_type;
  // }

  return ret;
}

void ObRedoLogSyncRequestMsg::reset()
{
  log_id_ = 0;
  log_ts_ = 0;
  log_type_ = ObStorageLogType::OB_LOG_UNKNOWN;
  trans_id_.reset();
}

bool ObRedoLogSyncRequestMsg::is_valid() const
{
  return log_id_ > 0
    && log_ts_ > 0
    && ObTransLogType::is_valid(log_type_)
    && trans_id_.is_valid();
}

int ObRedoLogSyncResponseMsg::init(
                                   const uint64_t log_id,
                                   const ObTransID &trans_id,
                                   const common::ObAddr &addr,
                                   const enum ObRedoLogSyncResponseStatus status)
{
  int ret = OB_SUCCESS;

  // if (OB_UNLIKELY(!partition.is_valid() ||
  //                 !trans_id.is_valid() ||
  //                 !addr.is_valid())) {
  //   ret = OB_INVALID_ARGUMENT;
  //   TRANS_LOG(WARN, "invalid argument", KR(ret), K(partition),
  //             K(trans_id), K(addr));
  // } else {
  //   partition_ = partition;
  //   log_id_ = log_id;
  //   trans_id_ = trans_id;
  //   addr_ = addr;
  //   status_ = status;
  // }

  return ret;
}

void ObRedoLogSyncResponseMsg::reset()
{
  log_id_ = 0;
  trans_id_.reset();
  addr_.reset();
  status_ = ObRedoLogSyncResponseStatus::OB_REDO_LOG_SYNC_UNKNOWN;
  gts_ = 0;
}

bool ObRedoLogSyncResponseMsg::is_valid() const
{
  return trans_id_.is_valid() && addr_.is_valid() && gts_ > 0;
}

}//transaction

namespace obrpc
{

// int ObDupTableLeaseRequestMsgP::process()
// {
//   int ret = OB_SUCCESS;
// //  transaction::ObPartTransCtxMgr *ctx_mgr = NULL;
// //  transaction::ObTransService *trans_service = global_ctx_.par_ser_->get_trans_service();
// //
// //  if (OB_ISNULL(trans_service)) {
// //    ret = OB_ERR_UNEXPECTED;
// //    TRANS_LOG(WARN, "trans service is NULL", KR(ret));
// //  } else if (OB_ISNULL(ctx_mgr = &trans_service->get_part_trans_ctx_mgr())) {
// //    ret = OB_ERR_UNEXPECTED;
// //    TRANS_LOG(WARN, "ObPartTransCtxMgr is NULL", K(ret));
// //  } else if (OB_FAIL(ctx_mgr->handle_dup_lease_request(arg_.get_partition(),
// //                                                       arg_))) {
// //    TRANS_LOG(WARN, "handle lease request error", K(ret), K(arg_.get_partition()));
// //  } else {
// //    //do nothing
// //  }
// //
//   return ret;
// }
//
// int ObDupTableLeaseResponseMsgP::process()
// {
//   int ret = common::OB_SUCCESS;
// //  transaction::ObPartTransCtxMgr *ctx_mgr = NULL;
// //  transaction::ObTransService *trans_service = global_ctx_.par_ser_->get_trans_service();
// //
// //  if (OB_ISNULL(trans_service)) {
// //    ret = OB_ERR_UNEXPECTED;
// //    TRANS_LOG(WARN, "trans service is NULL", KR(ret));
// //  } else if (OB_ISNULL(ctx_mgr = &trans_service->get_part_trans_ctx_mgr())) {
// //    ret = OB_ERR_UNEXPECTED;
// //    TRANS_LOG(WARN, "ObPartTransCtxMgr is NULL", K(ret));
// //  } else if (OB_FAIL(ctx_mgr->handle_dup_lease_response(arg_.get_partition(),
// //                                                        arg_,
// //                                                        trans_service))) {
// //    TRANS_LOG(WARN, "handle lease response error", K(ret), K(arg_.get_partition()));
// //  } else {
// //    //do nothing
// //  }
// //
//   return ret;
// }
//
// int ObRedoLogSyncRequestP::process()
// {
//   int ret = OB_SUCCESS;
// //  transaction::ObPartTransCtxMgr *ctx_mgr = NULL;
// //  transaction::ObTransService *trans_service = global_ctx_.par_ser_->get_trans_service();
// //
// //  if (OB_ISNULL(trans_service)) {
// //    ret = OB_ERR_UNEXPECTED;
// //    TRANS_LOG(WARN, "trans service is NULL", KR(ret));
// //  } else if (OB_ISNULL(ctx_mgr = &trans_service->get_part_trans_ctx_mgr())) {
// //    ret = OB_ERR_UNEXPECTED;
// //    TRANS_LOG(WARN, "ObPartTransCtxMgr is NULL", K(ret));
// //  } else if (OB_FAIL(ctx_mgr->handle_dup_redo_log_sync_request(arg_.get_partition(),
// //                                                               arg_,
// //                                                               trans_service))) {
// //    TRANS_LOG(WARN, "handle redo log sync request error", K(ret), K(arg_.get_partition()));
// //  } else {
// //    //do nothing
// //  }
// //
//   return ret;
// }
//
// int ObRedoLogSyncResponseP::process()
// {
//   int ret = OB_SUCCESS;
// //  transaction::ObPartTransCtxMgr *ctx_mgr = NULL;
// //  transaction::ObTransService *trans_service = global_ctx_.par_ser_->get_trans_service();
// //
// //  if (OB_ISNULL(trans_service)) {
// //    ret = OB_ERR_UNEXPECTED;
// //    TRANS_LOG(WARN, "trans service is NULL", KR(ret));
// //  } else if (OB_ISNULL(ctx_mgr = &trans_service->get_part_trans_ctx_mgr())) {
// //    ret = OB_ERR_UNEXPECTED;
// //    TRANS_LOG(WARN, "ObPartTransCtxMgr is NULL", K(ret));
// //  } else if (OB_FAIL(ctx_mgr->handle_dup_redo_log_sync_response(arg_.get_partition(),
// //                                                                arg_))) {
// //    TRANS_LOG(WARN, "handle redo log sync response error", K(ret), K(arg_.get_partition()));
// //  } else {
// //    //do nothing
// //  }
// //
//   return ret;
// }
//
// int ObPreCommitRequestP::process()
// {
//   int ret = OB_SUCCESS;
//   transaction::ObIDupTableRpc *dup_table_rpc = NULL;
//   transaction::ObTransService *trans_service = global_ctx_.par_ser_->get_trans_service();
//
//   if (OB_ISNULL(trans_service)) {
//     ret = OB_ERR_UNEXPECTED;
//     TRANS_LOG(WARN, "trans service is NULL", KR(ret));
//   } else if (OB_ISNULL(dup_table_rpc = trans_service->get_dup_table_rpc())) {
//     ret = OB_ERR_UNEXPECTED;
//     TRANS_LOG(WARN, "dup table rpc is null", KR(ret), K(arg_));
//   } else {
//     trans_service->get_tx_version_mgr().update_max_commit_ts(arg_.get_commit_version(), false);
//     // respond leader
//     // TODO, whether need to check lease_expired and update
//     // ObPartitionTransCtxMgr::update_max_trans_version?
//     transaction::ObPreCommitResponseMsg msg;
//     if (OB_FAIL(msg.init(arg_.get_partition(),
//                          arg_.get_trans_id(),
//                          trans_service->get_server(),
//                          OB_SUCCESS))) {
//       TRANS_LOG(WARN, "init pre commit response msg failed", KR(ret), K(arg_), K(msg));
//     } else if (OB_FAIL(msg.set_header(arg_.get_dst(), arg_.get_dst(), arg_.get_src()))) {
//       TRANS_LOG(WARN, "ObPreCommitResponseMsg set header error", KR(ret), K(arg_), K(msg));
//     } else if (OB_FAIL(dup_table_rpc->post_pre_commit_response(arg_.get_tenant_id(),
//                                                                arg_.get_src(), msg))) {
//       TRANS_LOG(WARN, "post pre commit response failed", KR(ret), K(arg_), K(msg));
//     }
//   }
//
//   return ret;
// }
//
//  if (OB_ISNULL(trans_service)) {
//    ret = OB_ERR_UNEXPECTED;
//    TRANS_LOG(WARN, "trans service is NULL", KR(ret));
//  } else if (OB_ISNULL(ctx_mgr = &trans_service->get_part_trans_ctx_mgr())) {
//    ret = OB_ERR_UNEXPECTED;
//    TRANS_LOG(WARN, "ObPartTransCtxMgr is NULL", K(ret));
//  } else if (OB_FAIL(ctx_mgr->handle_dup_redo_log_sync_response(arg_.get_partition(),
//                                                                arg_))) {
//    TRANS_LOG(WARN, "handle redo log sync response error", K(ret), K(arg_.get_partition()));
//  } else {
//    //do nothing
//  }
//
//   return ret;
// }

// int ObPreCommitRequestP::process()
// {
//   int ret = OB_NOT_SUPPORTED;
  // transaction::ObIDupTableRpc *dup_table_rpc = NULL;
  // transaction::ObTransService *trans_service = global_ctx_.par_ser_->get_trans_service();

  // if (OB_ISNULL(trans_service)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   TRANS_LOG(WARN, "trans service is NULL", KR(ret));
  // } else if (OB_ISNULL(dup_table_rpc = trans_service->get_dup_table_rpc())) {
  //   ret = OB_ERR_UNEXPECTED;
  //   TRANS_LOG(WARN, "dup table rpc is null", KR(ret), K(arg_));
  // } else {
  //   trans_service->get_tx_version_mgr().update_max_commit_ts(arg_.get_commit_version(), false);
  //   // respond leader
  //   // TODO, whether need to check lease_expired and update
  //   // ObPartitionTransCtxMgr::update_max_trans_version?
  //   transaction::ObPreCommitResponseMsg msg;
  //   if (OB_FAIL(msg.init(arg_.get_partition(),
  //                        arg_.get_trans_id(),
  //                        trans_service->get_server(),
  //                        OB_SUCCESS))) {
  //     TRANS_LOG(WARN, "init pre commit response msg failed", KR(ret), K(arg_), K(msg));
  //   } else if (OB_FAIL(msg.set_header(arg_.get_dst(), arg_.get_dst(), arg_.get_src()))) {
  //     TRANS_LOG(WARN, "ObPreCommitResponseMsg set header error", KR(ret), K(arg_), K(msg));
  //   } else if (OB_FAIL(dup_table_rpc->post_pre_commit_response(arg_.get_tenant_id(),
  //                                                              arg_.get_src(), msg))) {
  //     TRANS_LOG(WARN, "post pre commit response failed", KR(ret), K(arg_), K(msg));
  //   }
  // }

//   return ret;
// }
//
// int ObPreCommitResponseP::process()
// {
//   int ret = OB_SUCCESS;
//  transaction::ObPartTransCtxMgr *ctx_mgr = NULL;
//  transaction::ObTransService *trans_service = global_ctx_.par_ser_->get_trans_service();
//  if (OB_ISNULL(trans_service)) {
//    ret = OB_ERR_UNEXPECTED;
//    TRANS_LOG(WARN, "trans service is NULL", KR(ret));
//  } else if (OB_ISNULL(ctx_mgr = &trans_service->get_part_trans_ctx_mgr())) {
//    ret = OB_ERR_UNEXPECTED;
//    TRANS_LOG(WARN, "ObPartTransCtxMgr is NULL", K(ret));
//  } else if (OB_FAIL(ctx_mgr->handle_dup_pre_commit_response(arg_))) {
//    TRANS_LOG(WARN, "handle dup pre commit response error", KR(ret), K(arg_));
//  }
//
}// obrpc

namespace transaction
{
int ObDupTableRpc_old::init(ObTransService *trans_service, rpc::frame::ObReqTransport *transport, const ObAddr &addr)
{
  int ret = OB_SUCCESS;

  // if (OB_UNLIKELY(is_inited_)) {
  //   ret = OB_INIT_TWICE;
  //   TRANS_LOG(WARN, "duplicate table rpc init error", KR(ret));
  // } else if (OB_ISNULL(trans_service) || OB_ISNULL(transport)) {
  //   ret = OB_INVALID_ARGUMENT;
  //   TRANS_LOG(WARN, "invalid argument", KR(ret), KP(trans_service), KP(transport));
  // } else if (OB_FAIL(rpc_proxy_.init(transport, addr))) {
  //   TRANS_LOG(WARN, "init rpc proxy fail", K(ret));
  // } else {
  //   trans_service_ = trans_service;
  //   is_inited_ = true;
  //   TRANS_LOG(INFO, "dup table rpc init success");

  return ret;
}

int ObDupTableRpc_old::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "dup table rpc not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "dup table rpc already running", KR(ret));
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "dup table rpc start success");
  }
  return ret;
}

int ObDupTableRpc_old::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "dup table rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "dup table rpc has already been stopped", KR(ret));
  } else {
    is_running_ = false;
    TRANS_LOG(INFO, "dup table rpc stop success");
  }
  return ret;
}

int ObDupTableRpc_old::wait()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "dup table rpc not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "dup table rpc is running", KR(ret));
  } else {
    TRANS_LOG(INFO, "dup table rpc wait success");
  }
  return ret;
}

void ObDupTableRpc_old::destroy()
{
  int tmp_ret = OB_SUCCESS;
  if (is_inited_) {
    if (is_running_) {
      if (OB_SUCCESS != (tmp_ret = stop())) {
        TRANS_LOG_RET(WARN, tmp_ret, "dup table rpc stop error", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = wait())) {
        TRANS_LOG_RET(WARN, tmp_ret, "dup table rpc wait error", K(tmp_ret));
      } else {
        // do nothing
      }
    }
    is_inited_ = false;
    TRANS_LOG(INFO, "dup table rpc destroyed");
  }
}

int ObDupTableRpc_old::post_dup_table_lease_request(const uint64_t tenant_id,
                                                const common::ObAddr &server,
                                                const ObDupTableLeaseRequestMsg &msg)
{
  int ret = OB_SUCCESS;
  // if (!is_inited_) {
  //   ret = OB_NOT_INIT;
  //   TRANS_LOG(WARN, "dup table rpc not inited", KR(ret));
  // } else if (!is_running_) {
  //   ret = OB_NOT_RUNNING;
  //   TRANS_LOG(WARN, "dup table rpc not running", KR(ret));
  // } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !msg.is_valid()) {
  //   ret = OB_INVALID_ARGUMENT;
  //   TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(msg));
  // } else if (OB_FAIL(rpc_proxy_->to(server)
  //                    .by(tenant_id)
  //                    .post_dup_table_lease_request(msg, NULL))) {
  //   TRANS_LOG(WARN, "post dup table lease message error", KR(ret), K(server), K(msg));
  // } else {
  //   TRANS_LOG(DEBUG, "post dup table lease message success", K(server), K(msg));
  // }
  return ret;
}

int ObDupTableRpc_old::post_dup_table_lease_response(const uint64_t tenant_id,
                                                 const common::ObAddr &server,
                                                 const ObDupTableLeaseResponseMsg &msg)
{
  int ret = OB_SUCCESS;
  // if (!is_inited_) {
  //   ret = OB_NOT_INIT;
  //   TRANS_LOG(WARN, "dup table rpc not inited", KR(ret));
  // } else if (!is_running_) {
  //   ret = OB_NOT_RUNNING;
  //   TRANS_LOG(WARN, "dup table rpc not running", KR(ret));
  // } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !msg.is_valid()) {
  //   ret = OB_INVALID_ARGUMENT;
  //   TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(msg));
  // } else if (OB_FAIL(rpc_proxy_->to(server)
  //                    .by(tenant_id)
  //                    .post_dup_table_lease_response(msg, NULL))) {
  //   TRANS_LOG(WARN, "post dup table lease message error", KR(ret), K(server), K(msg));
  // } else {
  //   TRANS_LOG(DEBUG, "post dup table lease message success", K(server), K(msg));
  // }
  return ret;
}


int ObDupTableRpc_old::post_redo_log_sync_request(const uint64_t tenant_id,
                                              const common::ObAddr &server,
                                              const ObRedoLogSyncRequestMsg &msg)
{
  int ret = OB_SUCCESS;
  // if (!is_inited_) {
  //   ret = OB_NOT_INIT;
  //   TRANS_LOG(WARN, "dup table rpc not inited", KR(ret));
  // } else if (!is_running_) {
  //   ret = OB_NOT_RUNNING;
  //   TRANS_LOG(WARN, "dup table rpc not running", KR(ret));
  // } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !msg.is_valid()) {
  //   ret = OB_INVALID_ARGUMENT;
  //   TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(msg));
  // } else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).post_redo_log_sync_request(msg, NULL))) {
  //   TRANS_LOG(WARN, "post redo log sync request message error", KR(ret), K(server), K(msg));
  // } else {
  //   TRANS_LOG(DEBUG, "post redo log sync request message success", K(server), K(msg));
  // }
  return ret;
}

int ObDupTableRpc_old::post_redo_log_sync_response(const uint64_t tenant_id,
                                               const common::ObAddr &server,
                                               const ObRedoLogSyncResponseMsg &msg)
{
  int ret = OB_SUCCESS;
  // if (!is_inited_) {
  //   ret = OB_NOT_INIT;
  //   TRANS_LOG(WARN, "dup table rpc not inited", KR(ret));
  // } else if (!is_running_) {
  //   ret = OB_NOT_RUNNING;
  //   TRANS_LOG(WARN, "dup table rpc not running", KR(ret));
  // } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !msg.is_valid()) {
  //   ret = OB_INVALID_ARGUMENT;
  //   TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(msg));
  // } else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).post_redo_log_sync_response(msg, NULL))) {
  //   TRANS_LOG(WARN, "post redo log sync response message error", KR(ret), K(server), K(msg));
  // } else {
  //   TRANS_LOG(DEBUG, "post redo log sync response message success", K(server), K(msg));
  // }
  return ret;
}

int ObDupTableRpc_old::post_pre_commit_request(const uint64_t tenant_id,
                                           const common::ObAddr &server,
                                           const ObPreCommitRequestMsg &msg)
{
  int ret = OB_SUCCESS;
  // if (!is_inited_) {
  //   ret = OB_NOT_INIT;
  //   TRANS_LOG(WARN, "dup table rpc not inited", KR(ret));
  // } else if (!is_running_) {
  //   ret = OB_NOT_RUNNING;
  //   TRANS_LOG(WARN, "dup table rpc not running", KR(ret));
  // } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !msg.is_valid()) {
  //   ret = OB_INVALID_ARGUMENT;
  //   TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(msg));
  // } else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).post_pre_commit_request(msg, NULL))) {
  //   TRANS_LOG(WARN, "post pre commit request error", KR(ret), K(server), K(msg));
  // }
  return ret;
}

int ObDupTableRpc_old::post_pre_commit_response(const uint64_t tenant_id,
                                            const common::ObAddr &server,
                                            const ObPreCommitResponseMsg &msg)
{
  int ret = OB_SUCCESS;
  // if (!is_inited_) {
  //   ret = OB_NOT_INIT;
  //   TRANS_LOG(WARN, "dup table rpc not inited", KR(ret));
  // } else if (!is_running_) {
  //   ret = OB_NOT_RUNNING;
  //   TRANS_LOG(WARN, "dup table rpc not running", KR(ret));
  // } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !msg.is_valid()) {
  //   ret = OB_INVALID_ARGUMENT;
  //   TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(msg));
  // } else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).post_pre_commit_response(msg, NULL))) {
  //   TRANS_LOG(WARN, "post pre commit response error", KR(ret), K(server), K(msg));
  // }
  return ret;
}

}//transaction
}//oceanbase
