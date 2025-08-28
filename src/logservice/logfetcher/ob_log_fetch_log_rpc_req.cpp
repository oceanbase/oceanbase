/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_fetch_log_rpc_req.h"
#include "ob_log_trace_id.h"
#include "ob_log_fetch_log_rpc.h"

namespace oceanbase
{
namespace logfetcher
{

FetchLogRpcReq::~FetchLogRpcReq() {}

///////////////////////////// FetchLogARpc::RpcCB /////////////////////////
ERRSIM_POINT_DEF(ALLOC_FETCH_LOG_ARPC_CB_FAIL);
rpc::frame::ObReqTransport::AsyncCB *LogGroupEntryRpcCB::clone(const rpc::frame::SPAlloc &alloc) const
{
  return static_cast<rpc::frame::ObReqTransport::AsyncCB *>(const_cast<LogGroupEntryRpcCB*>(this));
}

int LogGroupEntryRpcCB::process()
{
  int ret = OB_SUCCESS;
  obrpc::ObCdcLSFetchLogResp &result = LogGroupEntryRpcCBBase::result_;
  obrpc::ObRpcResultCode rcode = LogGroupEntryRpcCBBase::rcode_;
  const common::ObAddr svr = LogGroupEntryRpcCBBase::dst_;

  if (OB_FAIL(do_process_(rcode, &result))) {
    LOG_ERROR("process fetch log callback fail", KR(ret), K(rcode), K(svr));
  }

  return ret;
}

void LogGroupEntryRpcCB::on_timeout()
{
  int ret = OB_SUCCESS;
  obrpc::ObRpcResultCode rcode;
  const common::ObAddr svr = LogGroupEntryRpcCBBase::dst_;

  ObCStringHelper helper;
  rcode.rcode_ = OB_TIMEOUT;
  (void)snprintf(rcode.msg_, sizeof(rcode.msg_), "fetch log rpc timeout, svr=%s",
      helper.convert(svr));

  if (OB_FAIL(do_process_(rcode, NULL))) {
    LOG_ERROR("process fetch log callback on timeout fail", KR(ret), K(rcode), K(svr));
  }
}

void LogGroupEntryRpcCB::on_invalid()
{
  int ret = OB_SUCCESS;
  obrpc::ObRpcResultCode rcode;
  const common::ObAddr svr = LogGroupEntryRpcCBBase::dst_;

  ObCStringHelper helper;
  // Invalid package encountered, decode failed
  rcode.rcode_ = OB_RPC_PACKET_INVALID;
  (void)snprintf(rcode.msg_, sizeof(rcode.msg_),
      "fetch log rpc response packet is invalid, svr=%s",
      helper.convert(svr));

  if (OB_FAIL(do_process_(rcode, NULL))) {
    LOG_ERROR("process fetch log callback on invalid fail", KR(ret), K(rcode), K(svr));
  }
}

int LogGroupEntryRpcCB::do_process_(const obrpc::ObRpcResultCode &rcode, const obrpc::ObCdcLSFetchLogResp *resp)
{
  int ret = OB_SUCCESS;
  LogGroupEntryRpcRequest &rpc_req = host_;
  FetchLogARpc &rpc_host = rpc_req.host_;

  if (OB_FAIL(rpc_host.handle_rpc_response(rpc_req, rcode, resp))) {
    LOG_ERROR("set fetch log response fail", KR(ret), K(rcode));
  } else {
    // success
  }
  return ret;
}

///////////////////////////// FetchLogARpc::RpcRequest /////////////////////////

LogGroupEntryRpcRequest::LogGroupEntryRpcRequest(FetchLogARpc &host,
    const share::ObLSID &ls_id,
    const int64_t rpc_timeout) :
    FetchLogRpcReq(obrpc::ObCdcFetchLogProtocolType::LogGroupEntryProto, host, rpc_timeout),
    cb_(*this),
    req_()
{
  // Initializing the request
  req_.set_ls_id(ls_id);
}

void LogGroupEntryRpcRequest::update_request(const int64_t upper_limit,
    const int64_t rpc_timeout)
{
  req_.set_upper_limit_ts(upper_limit);
  rpc_timeout_ = rpc_timeout;
}

int LogGroupEntryRpcRequest::prepare(
    const palf::LSN &req_start_lsn,
    const int64_t upper_limit,
    const int64_t progress)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! req_start_lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(req_start_lsn));
  } else {
    req_.set_client_pid(static_cast<uint64_t>(getpid()));
    req_.set_progress(progress);
    // set start lsn
    req_.set_start_lsn(req_start_lsn);
    // set request parameter: upper limit
    // upper limit may need reset before rpc send, thus value of upper limit should be provided dynamicly
    //
    // Set request parameter: upper limit
    req_.set_upper_limit_ts(upper_limit);

    // Update the next round of RPC trace id
    trace_id_.init(get_self_addr());

    // Update request time
    rpc_start_time_ = get_timestamp();

    // reset stop flag
    force_stop_flag_ = false;
  }

  return ret;
}

////////////////////////////// RawLogFileRpcRequest //////////////////////////////

// should get the flying state of this request before destory it
void RawLogFileRpcRequest::destroy()
{
  if (nullptr != buffer_) {
    buffer_->revert();
    buffer_ = nullptr;
  }
  ls_id_.reset();
  self_tenant_id_ = OB_INVALID_TENANT_ID;
  start_lsn_.reset();
  rpc_start_time_ = OB_INVALID_TIMESTAMP;
  prepare_end_time_ = OB_INVALID_TIMESTAMP;
  progress_ = OB_INVALID_TIMESTAMP;
  sub_rpc_req_count_ = 0;
  complete_count_ = 0;
  failed_count_ = 0;
  flying_count_ = 0;
  free_all_sub_rpc_request_();
}

int RawLogFileRpcRequest::prepare(const uint64_t self_tenant_id,
    const palf::LSN &start_lsn,
    const int64_t progress)
{
  int ret = OB_SUCCESS;

  if (! start_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get invalid start_lsn when preparing raw log request", K(start_lsn), K(progress));
  } else {
    start_lsn_ = start_lsn;
    progress_ = progress;
    self_tenant_id_ = self_tenant_id;
    rpc_start_time_ = get_timestamp();
    trace_id_.init(get_self_addr());
    force_stop_flag_ = false;
    sub_rpc_req_count_ = 0;
    complete_count_ = 0;
    failed_count_ = 0;
    flying_count_ = 0;
    if (OB_FAIL(host_.split_rpc_request(*this))) {
      if (OB_EAGAIN != ret) {
        LOG_ERROR("failed to split rpc request", KPC(this), K(start_lsn));
      }
    }
    prepare_end_time_ = get_timestamp();
    LOG_INFO("request finish to prepare", KPC(this));
  }

  return ret;
}

int RawLogFileRpcRequest::handle_sub_rpc_response(RawLogDataRpcRequest &rpc_req,
    const obrpc::ObRpcResultCode &rcode,
    const obrpc::ObCdcFetchRawLogResp *resp,
    const bool need_lock)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t sub_rpc_cb_start_ts = get_timestamp();
  const int32_t seq_no = rpc_req.req_.get_seq_no();
  const int64_t req_size = rpc_req.req_.get_req_size();
  const palf::LSN data_start_lsn = rpc_req.req_.get_start_lsn();
  const palf::LSN next_req_lsn = data_start_lsn + req_size;
  const int64_t sub_rpc_send_time = rpc_req.cb_.get_send_ts();

  if (OB_SUCCESS == rcode.rcode_ && nullptr == resp) {
    ATOMIC_INC(&failed_count_);
    ret = OB_INVALID_ERROR;
    LOG_ERROR("invalid response packet", K(rpc_req), K(rcode), K(resp));
  } else if (nullptr == resp) {
    ATOMIC_INC(&failed_count_);
    LOG_WARN_RET(rcode.rcode_, "failed to fetch log, null resp returned", K(rcode), K(rpc_req));
  } else if (OB_SUCCESS != rcode.rcode_) {
    ATOMIC_INC(&failed_count_);
    LOG_WARN_RET(rcode.rcode_, "failed to fetch log, unexpected rcode", K(rcode), K(rpc_req));
  }

  if (OB_ISNULL(buffer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get null buffer, unexpected", K(buffer_), K(rpc_req));
  } else if (OB_FAIL(ret) || (OB_SUCCESS != rcode.rcode_)) {
    if (OB_TMP_FAIL(buffer_->write_data(seq_no, nullptr, 0, data_start_lsn, next_req_lsn, share::SCN::invalid_scn(),
        obrpc::ObCdcFetchRawStatus(), obrpc::FeedbackType::INVALID_FEEDBACK, ret, rcode, sub_rpc_cb_start_ts,
        sub_rpc_send_time))) {
      LOG_ERROR_RET(tmp_ret, "failed to write failed state into buffer", KR(ret), K(rcode),
          K(seq_no), K(data_start_lsn));
    }
  } else {
    const int rpc_err = resp->get_err();
    const obrpc::FeedbackType feed_back = resp->get_feedback();
    const obrpc::ObCdcFetchRawStatus &status = resp->get_fetch_status();
    const int64_t read_size = resp->get_read_size();
    const int64_t buffer_len = resp->get_buffer_len();
    const share::SCN replayable_point = resp->get_replayable_point_scn();
    const char *data = resp->get_log_data();

    if (read_size > buffer_len) {
      ATOMIC_INC(&failed_count_);
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("read_size exceed buffer_len, unexpected", K(buffer_), KPC(this),
          K(rpc_req), KPC(resp));
    } else if (read_size >= 0 ) {
      if (OB_FAIL(buffer_->write_data(seq_no, data, read_size, data_start_lsn,
          next_req_lsn, replayable_point, status, feed_back, rpc_err, rcode,
          sub_rpc_cb_start_ts, sub_rpc_send_time))) {
        ATOMIC_INC(&failed_count_);
        LOG_ERROR("failed to write data to buffer", K(rpc_req), KPC(resp));
      }

      if (sub_rpc_cb_start_ts - sub_rpc_send_time > 200 * 1000L) {
        LOG_INFO("sub_rpc cost too much time", "cost_time", sub_rpc_cb_start_ts-sub_rpc_send_time,
            K(sub_rpc_cb_start_ts), K(sub_rpc_send_time), K(status), K(rpc_req));
      }
    } else {
      ATOMIC_INC(&failed_count_);
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("read_size less than zero, unexpected", K(read_size), K(seq_no), K(data_start_lsn));
    }
  }

  if (ATOMIC_AAF(&complete_count_, 1) == sub_rpc_req_count_) {
    if (OB_TMP_FAIL(host_.handle_rpc_response(*this, need_lock))) {
      LOG_ERROR_RET(tmp_ret, "failed to handle rpc response");
    }
  }

  return ret;
}

int RawLogFileRpcRequest::alloc_sub_rpc_request(RawLogDataRpcRequest *&req)
{
  int ret = OB_SUCCESS;

  lib::ObMemAttr attr(self_tenant_id_, "SubRpcReq");
  RawLogDataRpcRequest *sub_req = nullptr;
  if (free_list_.is_empty()) {
    if (OB_ISNULL(req = OB_NEW(RawLogDataRpcRequest, attr, *this))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate rawlogdata rpc request", K(self_tenant_id_));
    }
  } else {
    if (OB_ISNULL(sub_req = free_list_.remove_first())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get null item when pop item from free_list", KP(sub_req));
    } else {
      req = sub_req;
    }
  }

  return ret;
}

int RawLogFileRpcRequest::mark_sub_rpc_req_busy(RawLogDataRpcRequest *req)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get null req, unexpected", K(req));
  } else if (! busy_list_.add_last(req)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to push req into busy_list", KPC(req));
  }

  return ret;
}

int RawLogFileRpcRequest::free_sub_rpc_request(RawLogDataRpcRequest *req)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get null request when free sub rpc request", K(req));
  } else {
    OB_DELETE(RawLogDataRpcRequest, "SubRpcReq", req);
  }

  return ret;
}

int RawLogFileRpcRequest::free_requests_in_free_list()
{
  int ret = OB_SUCCESS;

  RawLogDataRpcRequest *sub_req = nullptr;
  DLIST_REMOVE_ALL(sub_req, free_list_) {
    if (OB_ISNULL(sub_req)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(free_sub_rpc_request(sub_req))) {
      LOG_ERROR("failed to free sub rpc request when free redundant requests", KP(sub_req));
    }
  }

  return ret;
}

int RawLogFileRpcRequest::revert_request_in_busy_list()
{
  int ret = OB_SUCCESS;

  RawLogDataRpcRequest *sub_req = nullptr;
  DLIST_REMOVE_ALL(sub_req, busy_list_) {
    if (OB_ISNULL(sub_req)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "sub_req_tmp is null", KP(sub_req));
    } else {
      sub_req->reset();
      if (! free_list_.add_last(sub_req)) {
        OB_DELETE(RawLogDataRpcRequest, "SubRpcReq", sub_req);
        LOG_ERROR_RET(OB_INVALID_ARGUMENT, "failed to push sub_req into free_list", KPC(sub_req));
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int RawLogFileRpcRequest::free_all_sub_rpc_request_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(free_requests_in_free_list())) {
    LOG_ERROR("failed to free requests in free_list", KPC(this));
  } else if (! busy_list_.is_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("there are still some busy_requests, unexpected", KPC(this));
  }

  return ret;
}

///////////////////////////// RawLogDataRequestCB /////////////////////////

rpc::frame::ObReqTransport::AsyncCB *RawLogDataRpcCB::clone(const rpc::frame::SPAlloc &alloc) const
{
  return static_cast<rpc::frame::ObReqTransport::AsyncCB *>(const_cast<RawLogDataRpcCB*>(this));
}

int RawLogDataRpcCB::process()
{
  int ret = OB_SUCCESS;
  obrpc::ObCdcFetchRawLogResp &result = RawLogDataRpcCB::result_;
  obrpc::ObRpcResultCode rcode = RawLogDataRpcCB::rcode_;
  const common::ObAddr svr = RawLogDataRpcCB::dst_;

  if (OB_FAIL(do_process_(rcode, &result))) {
    LOG_ERROR("process fetch log callback fail", KR(ret), K(rcode), K(svr));
  }

  return ret;
}

void RawLogDataRpcCB::on_timeout()
{
  int ret = OB_SUCCESS;
  obrpc::ObRpcResultCode rcode;
  const common::ObAddr svr = RawLogDataRpcCB::dst_;

  ObCStringHelper helper;
  rcode.rcode_ = OB_TIMEOUT;
  (void)snprintf(rcode.msg_, sizeof(rcode.msg_), "fetch log rpc timeout, svr=%s",
      helper.convert(svr));

  // do not access (*this) after do_process_, because (this) may be freed
  if (OB_FAIL(do_process_(rcode, NULL))) {
    LOG_ERROR("process fetch log callback on timeout fail", K(rcode), K(svr));
  }
}

void RawLogDataRpcCB::on_invalid()
{
  int ret = OB_SUCCESS;
  obrpc::ObRpcResultCode rcode;
  const common::ObAddr svr = RawLogDataRpcCB::dst_;

  ObCStringHelper helper;
  // Invalid package encountered, decode failed
  rcode.rcode_ = OB_RPC_PACKET_INVALID;
  (void)snprintf(rcode.msg_, sizeof(rcode.msg_),
      "fetch log rpc response packet is invalid, svr=%s",
      helper.convert(svr));

  if (OB_FAIL(do_process_(rcode, NULL))) {
    LOG_ERROR("process fetch log callback on invalid fail", K(rcode), K(svr));
  }
}

int RawLogDataRpcCB::do_process_(const obrpc::ObRpcResultCode &rcode, const obrpc::ObCdcFetchRawLogResp *resp)
{
  int ret = OB_SUCCESS;
  RawLogDataRpcRequest &rpc_req = host_;
  RawLogFileRpcRequest &rpc_host = rpc_req.host_;

  if (OB_FAIL(rpc_host.handle_sub_rpc_response(rpc_req, rcode, resp, true))) {
    LOG_ERROR("set fetch log response fail");
  } else {
    // success
  }
  return ret;
}

////////////////////////////// RawLogDataRpcRequest //////////////////////////////

int RawLogDataRpcRequest::prepare(const share::ObLSID &ls_id,
    const palf::LSN &start_lsn,
    const int64_t req_size,
    const int64_t progress,
    const int64_t file_id,
    const int32_t seq_no,
    const int32_t cur_round_rpc_count)
{
  int ret = OB_SUCCESS;

  req_.reset(ls_id, start_lsn, req_size, progress);
  req_.set_aux_param(file_id, seq_no, cur_round_rpc_count);
  rpc_prepare_time_ = get_timestamp();

  return ret;
}

void RawLogDataRpcRequest::reset()
{
  rpc_is_flying_ = false;
  rpc_prepare_time_ = OB_INVALID_TIMESTAMP;
  req_.reset();
}

////////////////////////////// RpcRequestList //////////////////////////////

void RpcRequestList::add(FetchLogRpcReq *req)
{
  if (OB_NOT_NULL(req)) {
    req->next_ = NULL;

    if (NULL == head_) {
      head_ = req;
      tail_ = req;
    } else {
      tail_->next_ = req;
      // fix 
      tail_ = req;
    }

    count_++;
  }
}

int RpcRequestList::remove(FetchLogRpcReq *target)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(target)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    bool found = false;
    FetchLogRpcReq *pre_request = NULL;
    FetchLogRpcReq *request = head_;

    // Find the matching request structure
    while (NULL != request && ! found) {
      if (target == request) {
        found = true;
      } else {
        pre_request = request;
        request = request->next_;
      }
    }

    if (found) {
      // Delete the corresponding node
      if (NULL == pre_request) {
        head_ = target->next_;
        if (target == tail_) {
          tail_ = head_;
        }
      } else {
        pre_request->next_ = target->next_;
        if (target == tail_) {
          tail_ = pre_request;
        }
      }

      count_--;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }

  return ret;
}


}
}
