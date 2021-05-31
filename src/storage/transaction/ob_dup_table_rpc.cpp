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

#include "storage/transaction/ob_dup_table_rpc.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/transaction/ob_dup_table.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace storage;

namespace transaction {
OB_SERIALIZE_MEMBER(ObDupTableMsgHeader, src_, dst_, proxy_);
OB_SERIALIZE_MEMBER(ObDupTableLeaseRequestMsg, request_ts_, partition_, addr_, last_log_id_, request_lease_interval_us_,
    src_, dst_, proxy_);
OB_SERIALIZE_MEMBER(
    ObDupTableLeaseResponseMsg, request_ts_, partition_, cur_log_id_, status_, lease_interval_us_, src_, dst_, proxy_);
OB_SERIALIZE_MEMBER(ObRedoLogSyncRequestMsg, partition_, log_id_, log_ts_, trans_id_, src_, dst_, proxy_, log_type_);
OB_SERIALIZE_MEMBER(ObRedoLogSyncResponseMsg, partition_, log_id_, trans_id_, addr_, status_, src_, dst_, proxy_);

void ObDupTableMsgHeader::reset()
{
  src_.reset();
  dst_.reset();
  proxy_.reset();
}

int ObDupTableMsgHeader::set_header(const ObAddr& src, const ObAddr& proxy, const ObAddr& dst)
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
  partition_.reset();
  addr_.reset();
  last_log_id_ = 0;
  request_lease_interval_us_ = 0;
}

bool ObDupTableLeaseRequestMsg::is_valid() const
{
  return request_ts_ > 0 && partition_.is_valid() && addr_.is_valid() && last_log_id_ > 0 &&
         request_lease_interval_us_ > 0;
}

int ObDupTableLeaseRequestMsg::init(const int64_t request_ts, const common::ObPartitionKey& partition,
    const common::ObAddr& addr, const uint64_t last_log_id, const int64_t request_lease_interval_us)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(request_ts <= 0 || !partition.is_valid() || !addr.is_valid() || last_log_id <= 0 ||
                  request_lease_interval_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN,
        "invalid argument",
        KR(ret),
        K(request_ts),
        K(partition),
        K(addr),
        K(last_log_id),
        K(request_lease_interval_us));
  } else {
    request_ts_ = request_ts;
    partition_ = partition;
    addr_ = addr;
    last_log_id_ = last_log_id;
    request_lease_interval_us_ = request_lease_interval_us;
  }

  return ret;
}

void ObDupTableLeaseResponseMsg::reset()
{
  request_ts_ = -1;
  partition_.reset();
  cur_log_id_ = 0;
  status_ = ObDupTableLeaseStatus::OB_DUP_TABLE_LEASE_UNKNOWN;
  lease_interval_us_ = 0;
}

bool ObDupTableLeaseResponseMsg::is_valid() const
{
  return request_ts_ > 0 && partition_.is_valid() && lease_interval_us_ > 0;
}

int ObDupTableLeaseResponseMsg::init(const int64_t request_ts, const common::ObPartitionKey& partition,
    const uint64_t cur_log_id, const enum ObDupTableLeaseStatus status, const int64_t lease_interval_us)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(request_ts <= 0 || !partition.is_valid() || lease_interval_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(request_ts), K(partition), K(lease_interval_us));
  } else {
    request_ts_ = request_ts;
    partition_ = partition;
    cur_log_id_ = cur_log_id;
    status_ = status;
    lease_interval_us_ = lease_interval_us;
  }

  return ret;
}

int ObRedoLogSyncRequestMsg::init(const common::ObPartitionKey& partition, const uint64_t log_id, const int64_t log_ts,
    const int64_t log_type, const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!partition.is_valid() || log_id <= 0 || log_ts <= 0 || !ObTransLogType::is_valid(log_type) ||
                  !trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(partition), K(log_id), K(log_ts), K(log_type), K(trans_id));
  } else {
    partition_ = partition;
    log_id_ = log_id;
    log_ts_ = log_ts;
    trans_id_ = trans_id;
    log_type_ = log_type;
  }

  return ret;
}

void ObRedoLogSyncRequestMsg::reset()
{
  partition_.reset();
  log_id_ = 0;
  log_ts_ = 0;
  log_type_ = ObStorageLogType::OB_LOG_UNKNOWN;
  trans_id_.reset();
}

bool ObRedoLogSyncRequestMsg::is_valid() const
{
  return partition_.is_valid() && log_id_ > 0 && log_ts_ > 0 && ObTransLogType::is_valid(log_type_) &&
         trans_id_.is_valid();
}

int ObRedoLogSyncResponseMsg::init(const common::ObPartitionKey& partition, const uint64_t log_id,
    const ObTransID& trans_id, const common::ObAddr& addr, const enum ObRedoLogSyncResponseStatus status)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!partition.is_valid() || !trans_id.is_valid() || !addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(partition), K(trans_id), K(addr));
  } else {
    partition_ = partition;
    log_id_ = log_id;
    trans_id_ = trans_id;
    addr_ = addr;
    status_ = status;
  }

  return ret;
}

void ObRedoLogSyncResponseMsg::reset()
{
  partition_.reset();
  log_id_ = 0;
  trans_id_.reset();
  addr_.reset();
  status_ = ObRedoLogSyncResponseStatus::OB_REDO_LOG_SYNC_UNKNOWN;
}

bool ObRedoLogSyncResponseMsg::is_valid() const
{
  return partition_.is_valid() && trans_id_.is_valid() && addr_.is_valid();
}

}  // namespace transaction

namespace obrpc {

int ObDupTableLeaseRequestMsgP::process()
{
  int ret = OB_SUCCESS;
  transaction::ObPartTransCtxMgr* ctx_mgr = NULL;
  transaction::ObTransService* trans_service = global_ctx_.par_ser_->get_trans_service();

  if (OB_ISNULL(trans_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans service is NULL", KR(ret));
  } else if (OB_ISNULL(ctx_mgr = &trans_service->get_part_trans_ctx_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ObPartTransCtxMgr is NULL", K(ret));
  } else if (OB_FAIL(ctx_mgr->handle_dup_lease_request(arg_.get_partition(), arg_))) {
    TRANS_LOG(WARN, "handle lease request error", K(ret), K(arg_.get_partition()));
  } else {
    // do nothing
  }

  return ret;
}

int ObDupTableLeaseResponseMsgP::process()
{
  int ret = common::OB_SUCCESS;
  transaction::ObPartTransCtxMgr* ctx_mgr = NULL;
  transaction::ObTransService* trans_service = global_ctx_.par_ser_->get_trans_service();

  if (OB_ISNULL(trans_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans service is NULL", KR(ret));
  } else if (OB_ISNULL(ctx_mgr = &trans_service->get_part_trans_ctx_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ObPartTransCtxMgr is NULL", K(ret));
  } else if (OB_FAIL(ctx_mgr->handle_dup_lease_response(arg_.get_partition(), arg_))) {
    TRANS_LOG(WARN, "handle lease response error", K(ret), K(arg_.get_partition()));
  } else {
    // do nothing
  }

  return ret;
}

int ObRedoLogSyncRequestP::process()
{
  int ret = OB_SUCCESS;
  transaction::ObPartTransCtxMgr* ctx_mgr = NULL;
  transaction::ObTransService* trans_service = global_ctx_.par_ser_->get_trans_service();

  if (OB_ISNULL(trans_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans service is NULL", KR(ret));
  } else if (OB_ISNULL(ctx_mgr = &trans_service->get_part_trans_ctx_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ObPartTransCtxMgr is NULL", K(ret));
  } else if (OB_FAIL(ctx_mgr->handle_dup_redo_log_sync_request(arg_.get_partition(), arg_, global_ctx_.par_ser_))) {
    TRANS_LOG(WARN, "handle redo log sync request error", K(ret), K(arg_.get_partition()));
  } else {
    // do nothing
  }

  return ret;
}

int ObRedoLogSyncResponseP::process()
{
  int ret = OB_SUCCESS;
  transaction::ObPartTransCtxMgr* ctx_mgr = NULL;
  transaction::ObTransService* trans_service = global_ctx_.par_ser_->get_trans_service();

  if (OB_ISNULL(trans_service)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans service is NULL", KR(ret));
  } else if (OB_ISNULL(ctx_mgr = &trans_service->get_part_trans_ctx_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ObPartTransCtxMgr is NULL", K(ret));
  } else if (OB_FAIL(ctx_mgr->handle_dup_redo_log_sync_response(arg_.get_partition(), arg_))) {
    TRANS_LOG(WARN, "handle redo log sync response error", K(ret), K(arg_.get_partition()));
  } else {
    // do nothing
  }

  return ret;
}
}  // namespace obrpc

namespace transaction {
int ObDupTableRpc::init(ObTransService* trans_service, obrpc::ObDupTableRpcProxy* rpc_proxy)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "duplicate table rpc init error", KR(ret));
  } else if (OB_ISNULL(trans_service) || OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), KP(trans_service), KP(rpc_proxy));
  } else {
    trans_service_ = trans_service;
    rpc_proxy_ = rpc_proxy;
    is_inited_ = true;
    TRANS_LOG(INFO, "dup table rpc init success");
  }

  return ret;
}

int ObDupTableRpc::start()
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

int ObDupTableRpc::stop()
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

int ObDupTableRpc::wait()
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

void ObDupTableRpc::destroy()
{
  int tmp_ret = OB_SUCCESS;
  if (is_inited_) {
    if (is_running_) {
      if (OB_SUCCESS != (tmp_ret = stop())) {
        TRANS_LOG(WARN, "dup table rpc stop error", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = wait())) {
        TRANS_LOG(WARN, "dup table rpc wait error", K(tmp_ret));
      } else {
        // do nothing
      }
    }
    is_inited_ = false;
    TRANS_LOG(INFO, "dup table rpc destroyed");
  }
}

int ObDupTableRpc::post_dup_table_lease_request(
    const uint64_t tenant_id, const common::ObAddr& server, const ObDupTableLeaseRequestMsg& msg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "dup table rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "dup table rpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(msg));
  } else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).post_dup_table_lease_request(msg, NULL))) {
    TRANS_LOG(WARN, "post dup table lease message error", KR(ret), K(server), K(msg));
  } else {
    TRANS_LOG(DEBUG, "post dup table lease message success", K(server), K(msg));
  }
  return ret;
}

int ObDupTableRpc::post_dup_table_lease_response(
    const uint64_t tenant_id, const common::ObAddr& server, const ObDupTableLeaseResponseMsg& msg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "dup table rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "dup table rpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(msg));
  } else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).post_dup_table_lease_response(msg, NULL))) {
    TRANS_LOG(WARN, "post dup table lease message error", KR(ret), K(server), K(msg));
  } else {
    TRANS_LOG(DEBUG, "post dup table lease message success", K(server), K(msg));
  }
  return ret;
}

int ObDupTableRpc::post_redo_log_sync_request(
    const uint64_t tenant_id, const common::ObAddr& server, const ObRedoLogSyncRequestMsg& msg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "dup table rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "dup table rpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(msg));
  } else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).post_redo_log_sync_request(msg, NULL))) {
    TRANS_LOG(WARN, "post redo log sync request message error", KR(ret), K(server), K(msg));
  } else {
    TRANS_LOG(DEBUG, "post redo log sync request message success", K(server), K(msg));
  }
  return ret;
}

int ObDupTableRpc::post_redo_log_sync_response(
    const uint64_t tenant_id, const common::ObAddr& server, const ObRedoLogSyncResponseMsg& msg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "dup table rpc not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "dup table rpc not running", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(server), K(msg));
  } else if (OB_FAIL(rpc_proxy_->to(server).by(tenant_id).post_redo_log_sync_response(msg, NULL))) {
    TRANS_LOG(WARN, "post redo log sync response message error", KR(ret), K(server), K(msg));
  } else {
    TRANS_LOG(DEBUG, "post redo log sync response message success", K(server), K(msg));
  }
  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
