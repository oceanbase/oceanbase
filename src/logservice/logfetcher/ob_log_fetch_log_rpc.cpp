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
 *
 * Fetching log-related RPC implementation
 */

#define USING_LOG_PREFIX OBLOG_FETCHER


#include "ob_log_fetch_log_rpc.h"
#include <unistd.h>                       // getpid
#include "share/ob_errno.h"                 // OB_SUCCESS
#include "lib/atomic/ob_atomic.h"         // ATOMIC_*
#include "lib/utility/ob_macro_utils.h"   // OB_FAIL
#include "lib/oblog/ob_log_module.h"      // LOG_ERROR
#include "lib/allocator/ob_malloc.h"      // ob_malloc/ob_free

#include "ob_log_rpc.h"                   // IObLogRpc
#include "ob_ls_worker.h"                 // IObLSWorker
#include "ob_log_ls_fetch_stream.h"       // FetchStream
#include "ob_log_trace_id.h"              // ObLogTraceIdGuard
#include "ob_log_config.h"                // ObLogFetcherConfig
#include "ob_log_fetch_log_rpc_req.h"
#include "ob_log_fetch_log_rpc_result.h"

using namespace oceanbase::common;
using namespace oceanbase::obrpc;

namespace oceanbase
{
namespace logfetcher
{

// #ifdef ERRSIM
ERRSIM_POINT_DEF(ALLOC_FETCH_LOG_ARPC_REQ_FAIL)
ERRSIM_POINT_DEF(ERRSIM_SEND_FETCH_LOG_REQ_FAIL)
ERRSIM_POINT_DEF(ERRSIM_SEND_FETCH_RAW_LOG_REQ_FAIL)
// #endif

////////////////////////////// FetchLogSRpc //////////////////////////////
FetchLogSRpc::FetchLogSRpc() :
    req_(),
    resp_(),
    rcode_(),
    cb_(*this),
    cond_(),
    rpc_done_(false)
{
}

FetchLogSRpc::~FetchLogSRpc()
{
  reset();
}

void FetchLogSRpc::reset()
{
  req_.reset();
  resp_.reset();
  rcode_.reset();
  rpc_done_ = false;
}

int FetchLogSRpc::fetch_log(IObLogRpc &rpc,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObIArray<ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array,
    const common::ObAddr &svr,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  reset();

  // build request
  if (OB_FAIL(build_request_(tenant_id, ls_id, miss_log_array))) {
    LOG_ERROR("build request fail", KR(ret), K(tenant_id), K(ls_id));
  }
  // Send asynchronous fetch log RPC
  else if (OB_FAIL(rpc.async_stream_fetch_missing_log(tenant_id, svr, req_, cb_, timeout))) {
    LOG_ERROR("send async stream fetch log rpc fail", KR(ret), K(tenant_id), K(svr), K(req_), K(timeout));

    rcode_.rcode_ = ret;
    (void)snprintf(rcode_.msg_, sizeof(rcode_.msg_), "send async stream fetch missing log rpc fail");

    // RPC send fails, finish directly
    rpc_done_ = true;

    // RPC sending errors are always considered to be Server problems and require switching servers
    // Reset the error code
    ret = OB_SUCCESS;
  } else {
    // If the RPC is sent successfully, block waits for rpc done
    while (! ATOMIC_LOAD(&rpc_done_)) {
      // This relies on the fact that the RPC must eventually call back, so the TIMEOUT time is not set
      cond_.wait();
    }
  }

  return ret;
}

int FetchLogSRpc::build_request_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObIArray<ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array)
{
  int ret = OB_SUCCESS;
  reset();

  // Set request parameters
  req_.set_ls_id(ls_id);
  req_.set_client_pid(static_cast<uint64_t>(getpid()));

  ARRAY_FOREACH_N(miss_log_array, idx, count) {
    const ObCdcLSFetchMissLogReq::MissLogParam &miss_log_param = miss_log_array.at(idx);

    if (OB_FAIL(req_.append_miss_log(miss_log_param))) {
      LOG_ERROR("req append_miss_log failed", KR(ret), K(tenant_id), K(ls_id), K(miss_log_param));
    }
  }

  return ret;
}

int FetchLogSRpc::set_resp(const obrpc::ObRpcResultCode &rcode,
    const obrpc::ObCdcLSFetchLogResp *resp)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_SUCCESS == rcode.rcode_ && NULL == resp)) {
    LOG_ERROR("invalid fetch log response", K(rcode), K(resp));
    ret = OB_INVALID_ARGUMENT;
  } else {
    rcode_ = rcode;

    if (OB_SUCCESS == rcode.rcode_) {
      if (OB_FAIL(resp_.assign(*resp))) {
        LOG_ERROR("assign new fetch log resp fail", KR(ret), KPC(resp), K(resp_));
      }
    }
  }

  if (OB_SUCCESS == ret) {
    // mark rpc done
    ATOMIC_SET(&rpc_done_, true);
    cond_.signal();
  }
  return ret;
}

////////////////////////////// FetchLogSRpc::RpcCB //////////////////////////////
FetchLogSRpc::RpcCB::RpcCB(FetchLogSRpc &host) : host_(host)
{}

FetchLogSRpc::RpcCB::~RpcCB()
{}

rpc::frame::ObReqTransport::AsyncCB *FetchLogSRpc::RpcCB::clone(const rpc::frame::SPAlloc &alloc) const
{
  void *buf = NULL;
  RpcCB *cb = NULL;

  if (OB_ISNULL(buf = alloc(sizeof(RpcCB)))) {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "clone rpc callback fail", K(buf), K(sizeof(RpcCB)));
  } else if (OB_ISNULL(cb = new(buf) RpcCB(host_))) {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "construct RpcCB fail", K(buf));
  } else {
    // success
  }

  return cb;
}

int FetchLogSRpc::RpcCB::process()
{
  int ret = OB_SUCCESS;
  ObCdcLSFetchLogResp &result = RpcCBBase::result_;
  ObRpcResultCode &rcode = RpcCBBase::rcode_;
  const common::ObAddr &svr = RpcCBBase::dst_;

  if (OB_FAIL(do_process_(rcode, &result))) {
    LOG_ERROR("process fetch log callback fail", KR(ret), K(result), K(rcode), K(svr));
  }
  // Aone:
  // Note: destruct response after asynchronous RPC processing
  result.reset();

  return ret;
}

void FetchLogSRpc::RpcCB::on_timeout()
{
  int ret = OB_SUCCESS;
  ObRpcResultCode rcode;
  const common::ObAddr &svr = RpcCBBase::dst_;

  rcode.rcode_ = OB_TIMEOUT;
  ObCStringHelper helper;
  (void)snprintf(rcode.msg_, sizeof(rcode.msg_), "fetch log rpc timeout, svr=%s",
      helper.convert(svr));

  if (OB_FAIL(do_process_(rcode, NULL))) {
    LOG_ERROR("process fetch log callback on timeout fail", KR(ret), K(rcode), K(svr));
  }
}

void FetchLogSRpc::RpcCB::on_invalid()
{
  int ret = OB_SUCCESS;
  ObRpcResultCode rcode;
  const common::ObAddr &svr = RpcCBBase::dst_;

  // 遇到无效的包，decode失败
  rcode.rcode_ = OB_RPC_PACKET_INVALID;
  ObCStringHelper helper;
  (void)snprintf(rcode.msg_, sizeof(rcode.msg_),
      "fetch log rpc response packet is invalid, svr=%s",
      helper.convert(svr));

  if (OB_FAIL(do_process_(rcode, NULL))) {
    LOG_ERROR("process fetch log callback on invalid fail", KR(ret), K(rcode), K(svr));
  }
}

int FetchLogSRpc::RpcCB::do_process_(const ObRpcResultCode &rcode, const ObCdcLSFetchLogResp *resp)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_SUCCESS == rcode.rcode_ && NULL == resp)) {
    LOG_ERROR("invalid response packet", K(rcode), K(resp));
    ret = OB_INVALID_ERROR;
  }
  else if (OB_FAIL(host_.set_resp(rcode, resp))) {
    LOG_ERROR("set fetch log response fail", KR(ret), K(resp), K(rcode));
  } else {
    // success
  }

  return ret;
}

////////////////////////////// FetchLogARpc //////////////////////////////

int64_t FetchLogARpc::g_rpc_result_count_per_rpc_upper_limit =
    ObLogFetcherConfig::default_rpc_result_count_per_rpc_upper_limit;

bool FetchLogARpc::g_print_rpc_handle_info = ObLogFetcherConfig::default_print_rpc_handle_info;

void FetchLogARpc::configure(const ObLogFetcherConfig &config)
{
  int64_t rpc_result_count_per_rpc_upper_limit = config.rpc_result_count_per_rpc_upper_limit;
  bool print_rpc_handle_info = config.print_rpc_handle_info;

  ATOMIC_STORE(&g_rpc_result_count_per_rpc_upper_limit, rpc_result_count_per_rpc_upper_limit);
  LOG_INFO("[CONFIG]", K(rpc_result_count_per_rpc_upper_limit));
  ATOMIC_STORE(&g_print_rpc_handle_info, print_rpc_handle_info);
  LOG_INFO("[CONFIG]", K(print_rpc_handle_info));
}

FetchLogARpc::FetchLogARpc(FetchStream &host) :
    host_(host),
    source_tenant_id_(OB_INVALID_TENANT_ID),
    self_tenant_id_(OB_INVALID_TENANT_ID),
    svr_(),
    rpc_(NULL),
    stream_worker_(NULL),
    result_pool_(NULL),
    log_file_pool_(NULL),
    state_(IDLE),
    cur_req_(NULL),
    flying_req_list_(),
    res_queue_(),
    proto_type_(obrpc::ObCdcFetchLogProtocolType::UnknownProto),
    splitter_(NULL),
    lock_(ObLatchIds::OBCDC_FETCHLOG_ARPC_LOCK)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(res_queue_.init(ObModIds::OB_LOG_FETCH_LOG_ARPC_RES_QUEUE))) {
    LOG_ERROR("init result queue fail", KR(ret));
  }
}

FetchLogARpc::~FetchLogARpc()
{
  reset();
  res_queue_.destroy();
}

void FetchLogARpc::reset()
{
  // Wait for all asynchronous RPCs to complete
  stop();

  source_tenant_id_ = OB_INVALID_TENANT_ID;
  self_tenant_id_ = OB_INVALID_TENANT_ID;
  svr_.reset();
  rpc_ = NULL;
  stream_worker_ = NULL;
  result_pool_ = NULL;
  state_ = IDLE;
  cur_req_ = NULL;
  flying_req_list_.reset();
  (void)res_queue_.reset();
  proto_type_ = obrpc::ObCdcFetchLogProtocolType::UnknownProto;
  if (NULL != splitter_) {
    OB_DELETE(IFetchLogRpcSplitter, "FetcherRpcSplit", splitter_);
    splitter_ = NULL;
  }
}

int FetchLogARpc::init(
    const uint64_t source_tenant_id,
    const uint64_t self_tenant_id,
    IObLogRpc &rpc,
    IObLSWorker &stream_worker,
    FetchLogRpcResultPool &result_pool,
    LogFileDataBufferPool &log_file_pool)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == source_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(source_tenant_id));
  } else if (OB_ISNULL(splitter_ = OB_NEW(ObLogFetchLogRpcSplitter,
        ObMemAttr(self_tenant_id, "FetcherRpcSplit"), RawLogFileRpcRequest::MAX_SEND_REQ_CNT))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory for splitter", K(self_tenant_id));
  } else {
    source_tenant_id_ = source_tenant_id;
    self_tenant_id_ = self_tenant_id;
    rpc_ = &rpc;
    stream_worker_ = &stream_worker;
    result_pool_ = &result_pool;
    log_file_pool_ = &log_file_pool;
    proto_type_ = obrpc::ObCdcFetchLogProtocolType::RawLogDataProto;
  }

  return ret;
}

int FetchLogARpc::set_server(const common::ObAddr &svr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! svr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("svr is not valid", KR(ret), K_(source_tenant_id), K(svr));
  } else {
    svr_ = svr;
  }

  return ret;
}

int FetchLogARpc::prepare_request(const share::ObLSID &ls_id,
    const int64_t rpc_timeout)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  void *fetch_stream = &host_;
  const bool print_rpc_handle_info = ATOMIC_LOAD(&g_print_rpc_handle_info);
  if (print_rpc_handle_info) {
    LOG_INFO("[STAT] [FETCH_LOG_ARPC] prepare rpc request", K(fetch_stream),
        "ready_result", res_queue_.count(),
        "flying_rpc", flying_req_list_.count_,
        K(rpc_timeout));
  } else {
    LOG_TRACE("[STAT] [FETCH_LOG_ARPC] prepare rpc request", K(fetch_stream),
        "ready_result", res_queue_.count(),
        "flying_rpc", flying_req_list_.count_,
        K(rpc_timeout));
  }

  // Requires IDLE status
  if (OB_UNLIKELY(IDLE != state_)) {
    LOG_ERROR("state not match which is not IDLE", K(state_));
    ret = OB_STATE_NOT_MATCH;
  }
  // Requirement to discard previous requests first
  else if (OB_UNLIKELY(NULL != cur_req_)) {
    LOG_ERROR("request has not been discarded", K(cur_req_), KPC(cur_req_));
    ret = OB_INVALID_ERROR;
  }
  // Allocate a new RPC request to carry the new stream
  else if (is_v1_fetch_log_protocol(proto_type_)) {
    if (OB_FAIL(alloc_log_group_entry_rpc_request_(ls_id, rpc_timeout, cur_req_))) {
      LOG_ERROR("alloc rpc request fail", KR(ret));
    } else if (OB_ISNULL(cur_req_)) {
      LOG_ERROR("alloc rpc request fail", K(cur_req_));
      ret = OB_ERR_UNEXPECTED;
    } else {
      // success
    }
  } else if (is_v2_fetch_log_protocol(proto_type_)) {
    if (OB_FAIL(alloc_raw_log_file_rpc_request_(ls_id, rpc_timeout, cur_req_))) {
      LOG_ERROR("alloc rpc request fail", KR(ret));
    } else if (OB_ISNULL(cur_req_)) {
      LOG_ERROR("alloc rpc request fail", K(cur_req_));
      ret = OB_ERR_UNEXPECTED;
    } else {
      // success
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to get valid protocol type", KPC(this));
  }

  return ret;
}

// Requires repeatable calls
void FetchLogARpc::discard_request(const char *discard_reason, const bool is_normal_discard)
{
  ObSpinLockGuard guard(lock_);

  // Reset current request
  if (NULL != cur_req_) {
    void *fetch_stream = &host_;
    const bool print_rpc_handle_info = ATOMIC_LOAD(&g_print_rpc_handle_info);
    if (print_rpc_handle_info || ! is_normal_discard) {
      LOG_INFO("[STAT] [FETCH_LOG_ARPC] discard rpc request", K(discard_reason),
          K(fetch_stream), K_(cur_req), KPC_(cur_req),
          "ready_result", res_queue_.count(),
          "flying_rpc", flying_req_list_.count_);
    } else {
      LOG_TRACE("[STAT] [FETCH_LOG_ARPC] discard rpc request", K(discard_reason),
          K(fetch_stream), K_(cur_req), KPC_(cur_req),
          "ready_result", res_queue_.count(),
          "flying_rpc", flying_req_list_.count_);
    }

    // If the RPC has finished, the request structure is cleaned up directly
    if (! cur_req_->rpc_is_flying()) {
      free_rpc_request_(cur_req_);
    } else {
      // Add to the run list if the RPC has not finished
      flying_req_list_.add(cur_req_);
    }

    cur_req_ = NULL;

    // Clear all requests and then set the status to IDLE
    clear_result_();
  }
}

int FetchLogARpc::async_fetch_log(
    const palf::LSN &req_start_lsn,
    const int64_t client_progress,
    const int64_t upper_limit,
    bool &rpc_send_succeed)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  rpc_send_succeed = false;

  // Requires IDLE status
  if (OB_UNLIKELY(IDLE != state_)) {
    LOG_ERROR("state not match which is not IDLE", K(state_));
    ret = OB_STATE_NOT_MATCH;
  }
  // Requires the request structure to be prepared
  else if (OB_ISNULL(cur_req_)) {
    LOG_ERROR("current request is invalid", K(cur_req_));
    ret = OB_INVALID_ERROR;
  }
  // Requirement that no RPC requests are being executed
  else if (OB_UNLIKELY(cur_req_->rpc_is_flying())) {
    LOG_ERROR("RPC is flying, can not launch async fetch log request",
        K(cur_req_->rpc_is_flying()), KPC(cur_req_));
    ret = OB_INVALID_ERROR;
  } else if (is_v1_fetch_log_protocol(cur_req_->get_proto_type())) {
    if (OB_FAIL(launch_async_rpc_(static_cast<LogGroupEntryRpcRequest&>(*cur_req_), req_start_lsn,
      client_progress, upper_limit, false, rpc_send_succeed))) {
      LOG_ERROR("launch async rpc fail", KR(ret), K(req_start_lsn), K(upper_limit), KPC(cur_req_));
    }
  } else if (is_v2_fetch_log_protocol(cur_req_->get_proto_type())) {
    if (OB_FAIL(launch_async_raw_file_rpc_(static_cast<RawLogFileRpcRequest&>(*cur_req_), req_start_lsn,
      client_progress, false, rpc_send_succeed))) {
      if (OB_EAGAIN != ret) {
        // we should have hold lock_ here, it's safe to print cur_req because there is
        // no concurrency issue under lock protection
        LOG_ERROR("failed to launch raw file rpc", K(req_start_lsn), K(client_progress), KPC(cur_req_));
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
  }

  return ret;
}

int64_t FetchLogARpc::get_flying_request_count()
{
  ObSpinLockGuard guard(lock_);
  return flying_req_list_.count_;
}

void FetchLogARpc::print_flying_request_list()
{
  ObSpinLockGuard guard(lock_);
  FetchLogRpcReq *req = flying_req_list_.head_;
  int64_t index = 0;
  while (NULL != req) {
    LOG_INFO("[FLYING_RPC_REQUEST]", "fetch_stream", &host_, K_(svr), K(index), K(req), KPC(req));
    req = req->next_;
    index++;
  }
}

void FetchLogARpc::stop()
{
  int64_t wait_count = 0;

  // Note: this function does not lock itself, it locks in the following sub-functions

  // Dispose the current request
  discard_request("stop");

  // Wait for the request list to be empty, i.e. all asynchronous RPCs have been completed
  if (get_flying_request_count() > 0) {
    LOG_INFO("wait for flying async fetch log rpc done",
        "fetch_stream", &host_, K_(svr),
        "flying_request_count", get_flying_request_count());
    print_flying_request_list();

    int64_t start_time = get_timestamp();
    while (get_flying_request_count() > 0) {
      wait_count++;
      if (0 == (wait_count % WARN_COUNT_ON_STOP)) {
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "wait for flying async fetch log rpc done",
            "fetch_stream", &host_, K_(svr),
            "flying_request_count", get_flying_request_count(),
            "wait_time", get_timestamp() - start_time);
        print_flying_request_list();
      }

      usec_sleep(WAIT_TIME_ON_STOP);
    }

    LOG_INFO("all flying async fetch log rpc is done",
        "fetch_stream", &host_, K_(svr),
        "wait_time", get_timestamp() - start_time,
        "flying_request_count", get_flying_request_count());
  }
}

int FetchLogARpc::next_result(FetchLogRpcResult *&result, bool &rpc_is_flying)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  // Requires current RPC request to be valid
  if (OB_ISNULL(cur_req_)) {
    LOG_ERROR("current request is invalid", K(cur_req_));
    ret = OB_INVALID_ERROR;
  }
  // Requires status is READY
  else if (OB_UNLIKELY(READY != state_)) {
    LOG_ERROR("state not match which is not READY", K(state_));
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_ISNULL(result_pool_)) {
    LOG_ERROR("invalid rpc result pool", K(result_pool_));
    ret = OB_INVALID_ERROR;
  } else {
    rpc_is_flying = cur_req_->rpc_is_flying();

    result = NULL;
    while (OB_SUCCESS == ret && NULL == result) {
      if (OB_FAIL(pop_result_(result))) {
        if (OB_EAGAIN == ret) {
          // Iteration complete
          ret = OB_ITER_END;
        } else {
          LOG_ERROR("pop result fail", KR(ret));
        }
      } else if (OB_ISNULL(result)) {
        LOG_ERROR("invalid result", K(result));
        ret = OB_ERR_UNEXPECTED;
      } else {
        // success
      }
    }
  }
  return ret;
}

void FetchLogARpc::revert_result(FetchLogRpcResult *result)
{
  ObSpinLockGuard guard(lock_);
  if (OB_ISNULL(result_pool_)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid rpc result pool", K(result_pool_));
  } else if (OB_NOT_NULL(result)) {
    result_pool_->free(result);
    result = NULL;
  }
}

int FetchLogARpc::update_request(const int64_t upper_limit,
    const int64_t rpc_timeout)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  // Requires current RPC request to be valid
  if (OB_ISNULL(cur_req_)) {
    LOG_ERROR("current request is invalid", K(cur_req_));
    ret = OB_INVALID_ERROR;
  } else {
    // Update request parameters
    if (is_v1_fetch_log_protocol(cur_req_->get_proto_type())) {
      LogGroupEntryRpcRequest *log_group_entry_req = static_cast<LogGroupEntryRpcRequest*>(cur_req_);
      if (OB_ISNULL(log_group_entry_req)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("failed to dynamic cast cur_req_ to log_group_entry_req", KPC(cur_req_));
      } else {
        log_group_entry_req->update_request(upper_limit, rpc_timeout);
      }
    }

  }

  return ret;
}

int FetchLogARpc::mark_request_stop()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);

  // Requires current RPC request to be valid
  if (OB_ISNULL(cur_req_)) {
    LOG_ERROR("current request is invalid", K(cur_req_));
    ret = OB_INVALID_ERROR;
  } else if (cur_req_->rpc_is_flying()) {
    // If the RPC is running, mark the next round of RPCs to stop
    cur_req_->mark_stop_flag();
  } else {
    // RPC stop case, no need to mark stop
  }
  return ret;
}

int FetchLogARpc::handle_rpc_response(LogGroupEntryRpcRequest &rpc_req,
    const obrpc::ObRpcResultCode &rcode,
    const obrpc::ObCdcLSFetchLogResp *resp)
{
  int ret = OB_SUCCESS;
  int64_t start_proc_time = get_timestamp();
  bool need_dispatch_stream_task = false;
  // Locked mutually exclusive access
  ObSpinLockGuard lock_guard(lock_);

  // Use the trace id associated with the request
  ObLogTraceIdGuard guard(rpc_req.get_trace_id());

  if (OB_UNLIKELY(OB_SUCCESS == rcode.rcode_ && NULL == resp)) {
    LOG_ERROR("invalid response packet", K(rcode), K(resp));
    ret = OB_INVALID_ERROR;
  }
  // Processing RPC response results
  else if (OB_ISNULL(stream_worker_)) {
    LOG_ERROR("invalid stream worker", K(stream_worker_));
    ret = OB_INVALID_ERROR;
  }
  // Verify that the request is the same as the current request: that is, whether it is the same object
  else if (NULL == cur_req_ || cur_req_ != &rpc_req) {
    // RPC requests have been deprecated
    LOG_INFO("rpc request has been discarded", K_(svr),
        "fetch_stream", &host_, K(rpc_req), KPC(cur_req_));

    // Try to find the corresponding RPC request structure, then destroy the
    if (OB_FAIL(destroy_flying_request_(&rpc_req))) {
      LOG_ERROR("destroy_flying_request_ fail", KR(ret), K(rpc_req));
    }
  } else {
    bool need_stop_rpc = false;
    RpcStopReason rpc_stop_reason = RpcStopReason::INVALID_REASON;
    int64_t next_upper_limit = OB_INVALID_TIMESTAMP;
    palf::LSN req_start_lsn;
    // Timeout or rpc failed, resp is nullptr.
    // Try to get_next_req_lsn when resp is not nullptr
    if (nullptr != resp) {
      // Note: this is copy to avoid resp is free !
      req_start_lsn = resp->get_next_req_lsn();
    }

    // Analyze the results and make decisions accordingly
    // 1. Decide if the next RPC needs to be launched
    // 2. Decide the upper limit of the next RPC
    //
    // TODO: Make more decisions based on request results
    //  1. Decide whether to dynamically modify the upper limit interval based on whether the upper limit is reached or not
    if (OB_FAIL(analyze_result_(rpc_req, rcode, resp, need_stop_rpc, rpc_stop_reason,
        next_upper_limit))) {
      LOG_ERROR("analyze_result_ fail", KR(ret), K(rpc_req), K(rcode), K(resp));
    }
    // Generate RPC results and determine if a log fetching task needs to be assigned
    else if (OB_FAIL(generate_rpc_result_(rpc_req, rcode, resp, start_proc_time,
        need_stop_rpc,
        rpc_stop_reason,
        need_dispatch_stream_task))) {
      LOG_ERROR("generate_rpc_result_ fail", KR(ret), K(rpc_req), K(rcode), K(resp),
          K(start_proc_time), K(need_stop_rpc), K(rpc_stop_reason));
    } else {
      // Print monitoring logs
      print_handle_info_(rpc_req, resp, next_upper_limit, need_stop_rpc, rpc_stop_reason,
          need_dispatch_stream_task);

      if (need_stop_rpc) {
        // If you do not need to continue fetching logs, mark the RPC as not running
        rpc_req.mark_flying_state(false);
      } else {
        bool rpc_send_succeed = false;
        int64_t server_progress = resp->get_progress();
        // Launch the next RPC
        if (OB_FAIL(launch_async_rpc_(rpc_req, req_start_lsn, server_progress, next_upper_limit, true, rpc_send_succeed))) {
          LOG_ERROR("launch_async_rpc_ fail", KR(ret), K(rpc_req), K(req_start_lsn), K(next_upper_limit));
        }
      }

      // Assign log stream fetching tasks as needed
      if (OB_SUCCESS == ret && need_dispatch_stream_task) {
        if (OB_FAIL(stream_worker_->dispatch_stream_task(host_, "RpcCallback"))) {
          LOG_ERROR("dispatch stream task fail", KR(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    rpc_req.mark_flying_state(false);
    if (need_dispatch_stream_task || IDLE == state_) {
      // if need_dispatch_stream_task is true, generate_rpc_result_ must succeed, then state_ should be READY
      // which means there is a comsumable result. In this scenario, launch_async_rpc_ may fail.
      // So FetchStream only switch state to IDLE iff IDLE == state_
      if (IDLE == state_) {
        host_.switch_state(FetchStream::State::IDLE);
      }
      if (OB_TMP_FAIL(stream_worker_->dispatch_stream_task(host_, "FailPostProcess", true))) {
        LOG_ERROR_RET(tmp_ret, "dispatch stream task fail", KR(ret));
      }
    }
    LOG_ERROR("handle_rpc_response failed, need reschedule", K(need_dispatch_stream_task), K(state_));
  }


  return ret;
}

int FetchLogARpc::handle_rpc_response(RawLogFileRpcRequest &request,
    const bool need_lock)
{
  int ret = OB_SUCCESS;

  if (need_lock) {
    ObSpinLockGuard guard(lock_);
    ret = handle_rpc_response_no_lock_(request);
  } else {
    ret = handle_rpc_response_no_lock_(request);
  }

  return ret;
}


int FetchLogARpc::split_rpc_request(RawLogFileRpcRequest &request)
{
  int ret = OB_SUCCESS;
  LogFileDataBuffer *data_buf = nullptr;

  if (OB_ISNULL(splitter_) || OB_ISNULL(log_file_pool_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("splitter or log_file_pool is null, unexpected", K(splitter_),
        K(request), K(log_file_pool_));
  } else if (OB_FAIL(splitter_->split(request))) {
    LOG_ERROR("splitter failed to split rpc request", K(request), K(splitter_));
  }
  // must split first, if we get log_file_pool first,
  // request.sub_rpc_req_count_ hasn't been determined
  else if (OB_FAIL(log_file_pool_->get(request.sub_rpc_req_count_,
      request.start_lsn_, &request, data_buf))) {
    if (OB_EAGAIN != ret) {
      LOG_ERROR("failed to get data_buf from log_file_pool", K(request));
    } else {
      LOG_INFO("no free data_buf in log_file_pool, need retry", K(request));
    }
  } else if (OB_ISNULL(data_buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("databuff in log file pool is null", K(data_buf));
  } else {
    request.buffer_ = data_buf;
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(request.revert_request_in_busy_list())) {
      LOG_ERROR_RET(tmp_ret, "failed to revert request in busy_list");
    }
  }

  return ret;
}

const char *FetchLogARpc::print_state(State state)
{
  const char *str = "UNKNOWN";
  switch (state) {
    case IDLE:
      str = "IDLE";
      break;
    case READY:
      str = "READY";
      break;
    default:
      str = "UNKNOWN";
      break;
  }
  return str;
}

int FetchLogARpc::alloc_log_group_entry_rpc_request_(const ObLSID &ls_id,
    const int64_t rpc_timeout,
    FetchLogRpcReq *&req)
{
  int ret = OB_SUCCESS;
  int64_t size = sizeof(LogGroupEntryRpcRequest);
  void *buf = NULL;
  ObMemAttr attr(self_tenant_id_, ObModIds::OB_LOG_FETCH_LOG_ARPC_REQUEST);

  if (OB_UNLIKELY(! ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid ls_id", KR(ret), K(ls_id));
  } else if (OB_ISNULL(buf = ob_malloc(size, attr))) {
    LOG_ERROR("allocate memory for RpcRequest fail", K(size));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(req = new(buf) LogGroupEntryRpcRequest(*this, ls_id, rpc_timeout))) {
    LOG_ERROR("construct RpcRequest fail", K(buf), K(req));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    // success
  }

  return ret;
}

int FetchLogARpc::alloc_raw_log_file_rpc_request_(const ObLSID &ls_id,
    const int64_t rpc_timeout,
    FetchLogRpcReq *&req)
{
  int ret = OB_SUCCESS;

  int64_t size = sizeof(RawLogFileRpcRequest);
  void *buf = NULL;
  ObMemAttr attr(self_tenant_id_, ObModIds::OB_LOG_FETCH_LOG_ARPC_REQUEST);

  if (OB_UNLIKELY(! ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid ls_id", KR(ret), K(ls_id));
// #ifdef ERRSIM
  } else if (OB_FAIL(ALLOC_FETCH_LOG_ARPC_REQ_FAIL)) {
    LOG_ERROR("ALLOC_FETCH_LOG_ARPC_REQ_FAIL");
// #endif
  } else if (OB_ISNULL(buf = ob_malloc(size, attr))) {
    LOG_ERROR("allocate memory for RpcRequest fail", K(size));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(req = new(buf) RawLogFileRpcRequest(*this, ls_id, rpc_timeout))) {
    LOG_ERROR("construct RpcRequest fail", K(buf), K(req));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    // success
  }

  return ret;
}

void FetchLogARpc::free_rpc_request_(FetchLogRpcReq *request)
{
  if (OB_NOT_NULL(request)) {
    request->~FetchLogRpcReq();
    ob_free(request);
    request = NULL;
  }
}

int FetchLogARpc::generate_rpc_result_(LogGroupEntryRpcRequest &rpc_req,
    const obrpc::ObRpcResultCode &rcode,
    const obrpc::ObCdcLSFetchLogResp *resp,
    const int64_t rpc_callback_start_time,
    const bool need_stop_rpc,
    const RpcStopReason rpc_stop_reason,
    bool &need_dispatch_stream_task)
{
  int ret = OB_SUCCESS;
  LogGroupEntryRpcResult *result = NULL;
  bool is_state_idle = false;
  int64_t rpc_start_time = rpc_req.get_rpc_start_time();
  const common::ObCurTraceId::TraceId &trace_id = rpc_req.get_trace_id();

  if (OB_ISNULL(result_pool_)) {
    LOG_ERROR("invalid result pool", K(result_pool_));
    ret = OB_INVALID_ERROR;
  }
  // Assign an RPC result
  else if (OB_FAIL(result_pool_->alloc(result))) {
    LOG_ERROR("alloc rpc result fail", KR(ret));
  } else if (OB_ISNULL(result)) {
    LOG_ERROR("invalid result", K(result));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(result->set(rcode, resp, trace_id,
      rpc_start_time,
      rpc_callback_start_time,
      need_stop_rpc,
      rpc_stop_reason))) {
    LOG_ERROR("set rpc result fail", KR(ret), K(rcode), KPC(resp), K(rpc_start_time),
        K(rpc_callback_start_time), K(need_stop_rpc), K(rpc_stop_reason));
    result_pool_->free(result);
    result = NULL;
  }
  // Push the results to the queue and modify the status at the same time
  else if (OB_FAIL(push_result_and_be_ready_(result, is_state_idle))) {
    LOG_ERROR("push result and be ready fail", KR(ret), KPC(result));
  } else {
    // If the status is IDLE before inserting the result, the task needs to be assigned
    need_dispatch_stream_task = is_state_idle;
  }

  // Reclaiming memory in case of failure
  if (OB_SUCCESS != ret && NULL != result_pool_ && NULL != result) {
    result_pool_->free(result);
    result = NULL;
  }

  return ret;
}

int FetchLogARpc::handle_rpc_response_no_lock_(RawLogFileRpcRequest &request)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t start_proc_time = get_timestamp();
  ObLogTraceIdGuard guard(request.get_trace_id());

  // revert sub rpc req in busy list anyway, these sub rpc req are not busy;
  // when there is no busy sub rpc req, the main request could be destroyed safely
  // without memleak
  if (OB_TMP_FAIL(request.revert_request_in_busy_list())) {
    LOG_ERROR_RET(tmp_ret, "failed to revert request in busy list");
  }

  if (OB_ISNULL(stream_worker_)) {
    LOG_ERROR("invalid stream worker", K(stream_worker_));
    ret = OB_INVALID_ERROR;
  } else if (NULL == cur_req_ || cur_req_ != &request) {
    // RPC requests have been deprecated
    LOG_INFO("rpc request has been discarded", K_(svr),
        "fetch_stream", &host_, K(request), KPC(cur_req_));

    // Try to find the corresponding RPC request structure, then destroy the
    if (OB_FAIL(destroy_flying_request_(&request))) {
      LOG_ERROR("destroy_flying_request_ fail", KR(ret), K(request));
    }
  } else  {
    bool need_stop_rpc = false;
    RpcStopReason reason = RpcStopReason::INVALID_REASON;
    bool need_dispatch_stream_task = false;
    palf::LSN next_req_lsn;
    if (OB_FAIL(generate_rpc_result_(request, start_proc_time, need_stop_rpc, reason,
        need_dispatch_stream_task, next_req_lsn))) {
      LOG_ERROR("failed to generate rpc result", K(request), K(need_stop_rpc), K(reason), K(start_proc_time));
    } else {
      if (need_stop_rpc) {
        request.mark_flying_state(false);
      } else {
        bool rpc_send_succeed = false;
        int64_t progress = OB_INVALID_TIMESTAMP;
        // Launch the next RPC
        if (OB_FAIL(host_.get_progress(progress))) {
          LOG_ERROR("failed to get progress from host", K(host_));
        } else if (OB_FAIL(launch_async_raw_file_rpc_(request, next_req_lsn, progress, true, rpc_send_succeed))) {
          if (OB_EAGAIN != ret) {
            // we should have hold lock_ here, it's safe to print request because there is
            // no concurrency issue under lock protection and request must be cur_req_
            LOG_ERROR("launch_async_rpc_ fail", KR(ret), K(request), K(next_req_lsn), K(progress));
          } else {
            ret = OB_SUCCESS;
            request.mark_flying_state(false);
            LOG_INFO("not enough LogFileDataBuffer, stop launch rpc");
          }
        }
      }

      if (OB_SUCC(ret) && need_dispatch_stream_task) {
        if (OB_FAIL(stream_worker_->dispatch_stream_task(host_, "RawLogRpcCallback"))) {
          LOG_ERROR("dispatch stream task fail", KR(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      request.mark_flying_state(false);
      if (need_dispatch_stream_task || IDLE == state_) {
        if (IDLE == state_) {
          host_.switch_state(FetchStream::State::IDLE);
        }
        if (OB_TMP_FAIL(stream_worker_->dispatch_stream_task(host_, "FailPostProcess"))) {
          LOG_ERROR_RET(tmp_ret, "dispatch stream task fail", KR(ret));
        }
      }
      LOG_ERROR("handle_rpc_response failed, need reschedule", K(need_dispatch_stream_task), K(state_));
    }
  }

  return ret;
}

void FetchLogARpc::print_handle_info_(LogGroupEntryRpcRequest &rpc_req,
    const obrpc::ObCdcLSFetchLogResp *resp,
    const int64_t next_upper_limit,
    const bool need_stop_rpc,
    const RpcStopReason rpc_stop_reason,
    const bool need_dispatch_stream_task)
{
  const bool print_rpc_handle_info = ATOMIC_LOAD(&g_print_rpc_handle_info);
  int64_t req_upper_limit = rpc_req.get_upper_limit();
  int64_t rpc_time = get_timestamp() - rpc_req.get_rpc_start_time();
  void *fetch_stream = &host_;

  if (print_rpc_handle_info) {
    LOG_INFO("handle rpc result by rpc callback",
        K(fetch_stream),
        K(need_stop_rpc),
        "stop_reason", print_rpc_stop_reason(rpc_stop_reason),
        "ready_result", res_queue_.count(),
        "max_result", ATOMIC_LOAD(&g_rpc_result_count_per_rpc_upper_limit),
        K(need_dispatch_stream_task),
        "flying_rpc", flying_req_list_.count_,
        "upper_limit", NTS_TO_STR(req_upper_limit),
        "delta", next_upper_limit - req_upper_limit,
        K(rpc_time), KPC(resp));
  } else {
    LOG_TRACE("handle rpc result by rpc callback",
        K(fetch_stream),
        K(need_stop_rpc),
        "stop_reason", print_rpc_stop_reason(rpc_stop_reason),
        "ready_result", res_queue_.count(),
        "max_result", ATOMIC_LOAD(&g_rpc_result_count_per_rpc_upper_limit),
        K(need_dispatch_stream_task),
        "flying_rpc", flying_req_list_.count_,
        "upper_limit", NTS_TO_STR(req_upper_limit),
        "delta", next_upper_limit - req_upper_limit,
        K(rpc_time), KPC(resp));
  }
}

int FetchLogARpc::launch_async_raw_file_rpc_(RawLogFileRpcRequest &request,
    const palf::LSN &req_start_lsn,
    const int64_t progress,
    const bool launch_by_cb,
    bool &rpc_send_succeed)
{
  int ret = OB_SUCCESS;

  rpc_send_succeed = false;

  if (OB_ISNULL(rpc_)) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("invalid handlers", K(rpc_));
  } else if (OB_FAIL(request.prepare(self_tenant_id_, req_start_lsn, progress))) {
    if (OB_EAGAIN != ret) {
      LOG_ERROR("raw log file request failed to prepare", K(self_tenant_id_), K(req_start_lsn), K(progress));
    } else {
      LOG_INFO("not enough buffer, need retry", K(self_tenant_id_), K(req_start_lsn), K(progress));
    }
  } else {
    ObLogTraceIdGuard guard(request.get_trace_id());
    ObSEArray<RawLogDataRpcRequest* , RawLogFileRpcRequest::MAX_SEND_REQ_CNT> failed_list;
    obrpc::ObRpcResultCode first_fail_rcode;
    ObCStringHelper helper;
    _LOG_TRACE("launch async fetch log rpc by %s, request=%s",
        launch_by_cb ? "callback" : "fetch stream", helper.convert(request));

    request.mark_flying_state(true);
    rpc_send_succeed = true;


    DLIST_FOREACH(sub_req, request.busy_list_) {
      if (OB_ISNULL(sub_req)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("sub request is null, unexpected", K(sub_req));
      }
      // TODO: assume the process above never fails, it's not able to recover when the process above failed
      else if (OB_FAIL(first_fail_rcode.rcode_)) {
        LOG_WARN("rpc send error previously, skip send rpc, generate error resp", K(first_fail_rcode));
// #ifdef ERRSIM
      } else if (OB_FAIL(ERRSIM_SEND_FETCH_RAW_LOG_REQ_FAIL)) {
        LOG_WARN("ERRSIM: ERRSIM_SEND_FETCH_RAW_LOG_REQ_FAIL", K(request));
// #endif
      } else if (OB_FAIL(rpc_->async_stream_fetch_raw_log(source_tenant_id_, svr_, sub_req->req_, sub_req->cb_, request.rpc_timeout_))) {
        LOG_ERROR("failed to send rpc to fetch raw log", K(source_tenant_id_), K(svr_), KPC(sub_req));
      }

      if (OB_FAIL(ret)) {
        // overwrite ret here, for complete rpc req send process
        first_fail_rcode.rcode_ = ret;
        (void) snprintf(first_fail_rcode.msg_, sizeof(first_fail_rcode.msg_),
            "send async stream fetch raw log rpc fail");
        if (OB_FAIL(failed_list.push_back(sub_req))) {
          LOG_ERROR("failed list failed to add failed sub_req", KPC(sub_req), K(failed_list));
        }
      }
    }

    ARRAY_FOREACH_NORET(failed_list, idx) {
      // Mustn't unlock the lock_ before handle_sub_rpc_response and lock the lock_ after handle_sub_rpc_response,
      // the request would not be freed under the protection of the lock_, once the lock_ in unlocked,
      // the LSWorker may call discard_request, and the request could be freed in handle_sub_rpc_response,
      // so that the subsequent access of request would be illegal thus cause coredump.
      RawLogDataRpcRequest *sub_req = failed_list.at(idx);
      // overwrite ret
      if (OB_FAIL(request.handle_sub_rpc_response(*sub_req, first_fail_rcode, nullptr, false))) {
        LOG_WARN("failed to handle sub rpc response", K(first_fail_rcode));
      }
    }
  }


  return ret;
}

int FetchLogARpc::launch_async_rpc_(LogGroupEntryRpcRequest &rpc_req,
    const palf::LSN &req_start_lsn,
    const int64_t progress,
    const int64_t upper_limit,
    const bool launch_by_cb,
    bool &rpc_send_succeed)
{
  int ret = OB_SUCCESS;

  rpc_send_succeed = false;

  if (OB_ISNULL(rpc_)) {
    LOG_ERROR("invalid handlers", K(rpc_));
    ret = OB_INVALID_ERROR;
  } else if (OB_FAIL(rpc_req.prepare(req_start_lsn, upper_limit, progress))) {
    // First prepare the RPC request and update the req_start_lsn and upper limit
    LOG_ERROR("rpc_req prepare failed", KR(ret), K(req_start_lsn), K(upper_limit));
  } else {
    // Use the trace id of the request
    ObLogTraceIdGuard guard(rpc_req.get_trace_id());

    ObCStringHelper helper;
    _LOG_TRACE("launch async fetch log rpc by %s, request=%s",
        launch_by_cb ? "callback" : "fetch stream", helper.convert(rpc_req));

    // The default setting is flyin before sending an asynchronous request
    // The reason for not setting it up after sending is that there is a concurrency problem after successful sending,
    // and the RPC request may have come back before it is set up
    rpc_req.mark_flying_state(true);

    // Sending the asynchronous RPC
// #ifdef ERRSIM
    if (OB_FAIL(ERRSIM_SEND_FETCH_LOG_REQ_FAIL)) {
      LOG_WARN("ERRSIM: ERRSIM_SEND_FETCH_LOG_REQ_FAIL", K(ERRSIM_SEND_FETCH_LOG_REQ_FAIL));
    } else {
// #endif
    ret = rpc_->async_stream_fetch_log(source_tenant_id_, svr_, rpc_req.req_, rpc_req.cb_, rpc_req.rpc_timeout_);
// #ifdef ERRSIM
    }
// #endif

    if (OB_SUCC(ret)) {
      // RPC sent successfully
      // You can't continue to manipulate the related structures afterwards
      rpc_send_succeed = true;
    } else {
      LOG_ERROR("send async stream fetch log rpc fail", KR(ret), K(svr_), K(rpc_req), K(launch_by_cb));

      int64_t start_proc_time = get_timestamp();

      // First reset the ret return value
      int err_code = ret;
      ret = OB_SUCCESS;

      // RPC send failure
      rpc_send_succeed = false;

      // Mark RPC is not running
      rpc_req.mark_flying_state(false);

      // Set error code
      ObRpcResultCode rcode;
      rcode.rcode_ = err_code;
      (void)snprintf(rcode.msg_, sizeof(rcode.msg_), "send async stream fetch log rpc fail");

      // RPC send failure, uniformly considered to be a problem of the observer, directly generated RPC results
      bool rpc_stopped = true;
      RpcStopReason reason = RpcStopReason::FETCH_LOG_FAIL;
      // Note: No need to process the return value here: need_dispatch_stream_task
      // This function initiates the RPC request, and only determines whether the dispatch task is needed
      // when the RPC result is processed by the asynchronous callback
      bool need_dispatch_stream_task = false;
      if (OB_FAIL(generate_rpc_result_(rpc_req, rcode, NULL, start_proc_time,
          rpc_stopped, reason, need_dispatch_stream_task))) {
        LOG_ERROR("generate rpc result fail", KR(ret), K(rpc_req), K(rcode), K(start_proc_time),
            K(rpc_stopped), K(reason));
      }
    }
  }
  return ret;
}

int FetchLogARpc::push_result_and_be_ready_(FetchLogRpcResult *result, bool &is_state_idle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result)) {
    LOG_ERROR("invalid argument", K(result));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(res_queue_.push(result))) {
    LOG_ERROR("push result into queue fail", KR(ret), K(result));
  } else {
    is_state_idle = (IDLE == state_);

    // After generating the data, modify the status and set it to READY unconditionally
    if (READY != state_) {
      (void)ATOMIC_SET(&state_, READY);
    }
  }
  return ret;
}

int FetchLogARpc::pop_result_(FetchLogRpcResult *&result)
{
  int ret = OB_SUCCESS;

  ret = res_queue_.pop(result);

  if (OB_SUCCESS == ret) {
    // success
  } else if (OB_EAGAIN == ret) {
    // No data
    // Change the status to IDLE
    (void)ATOMIC_SET(&state_, IDLE);
  } else {
    LOG_ERROR("pop result from queue fail", KR(ret));
  }
  return ret;
}

void FetchLogARpc::clear_result_()
{
  int ret = OB_SUCCESS;
  FetchLogRpcResult *data = NULL;

  // Empty the data when the queue is not empty
  if (res_queue_.count() > 0) {
    // Require result_pool_ be valid
    if (OB_ISNULL(result_pool_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid rpc result pool, can not clear results", KR(ret), K(result_pool_));
    } else {
      while (OB_SUCC(res_queue_.pop(data))) {
        if (OB_NOT_NULL(data)) {
          result_pool_->free(data);
          data = NULL;
        }
        data = NULL;
      }
    }
  }

  // Status forced to IDLE
  (void)ATOMIC_SET(&state_, IDLE);
}

int FetchLogARpc::destroy_flying_request_(FetchLogRpcReq *target_request)
{
  int ret = OB_SUCCESS;

  // Remove the request from the run list
  // If the request is not in the list, an exception must have occurred
  if (OB_ISNULL(target_request)) {
    LOG_ERROR("invalid argument", K(target_request));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(flying_req_list_.remove(target_request))) {
    LOG_ERROR("remove rpc request from flying request list fail", KR(ret), K(target_request));
  } else {
    free_rpc_request_(target_request);
    target_request = NULL;
  }

  return ret;
}

int FetchLogARpc::generate_rpc_result_(RawLogFileRpcRequest &rpc_req,
    const int64_t rpc_callback_start_time,
    bool &need_stop_rpc,
    RpcStopReason &rpc_stop_reason,
    bool &need_dispatch_stream_task,
    palf::LSN &next_req_lsn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  RawLogFileRpcResult *result = nullptr;
  const int64_t rpc_result_count_per_rpc_upper_limit =
      ATOMIC_LOAD(&g_rpc_result_count_per_rpc_upper_limit) / 4;
  const int64_t res_queue_cnt = res_queue_.count();
  bool is_state_idle = false;

  need_stop_rpc = false;
  rpc_stop_reason = RpcStopReason::INVALID_REASON;

  if (OB_ISNULL(result_pool_) || OB_ISNULL(splitter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid result pool or splitter", K(result_pool_), K(splitter_));
  } else if (OB_FAIL(result_pool_->alloc(result))) {
    LOG_ERROR("failed to allocate raw log data protocol result");
  } else if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get null RawLogFileRpcResult, unexpected", K(result));
  } else if (OB_ISNULL(rpc_req.buffer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("buffer in rpc_req is null");
  } else {
    int32_t cur_valid_rpc_cnt = RawLogFileRpcRequest::MAX_SEND_REQ_CNT;
    next_req_lsn.reset();
    result->trace_id_ = rpc_req.get_trace_id();
    if (OB_FAIL(result->generate_result_from_data(rpc_req.buffer_))) {
      LOG_ERROR("failed to get data", K(result));
    } else if (! result->is_readable_) {
      need_stop_rpc = true;
      rpc_stop_reason = RpcStopReason::RESULT_NOT_READABLE;
    } else {
      if (get_fetch_log_proto() != rpc_req.get_proto_type()) {
        need_stop_rpc = true;
        rpc_stop_reason = RpcStopReason::RPC_PROTO_NOT_MATCH;
      } else if (0 >= result->data_len_) {
        need_stop_rpc = true;
        rpc_stop_reason = RpcStopReason::FETCH_NO_LOG;
      } else if (result->is_active_) {
        need_stop_rpc = true;
        rpc_stop_reason = RpcStopReason::REACH_MAX_LOG;
      } else if (res_queue_cnt >= rpc_result_count_per_rpc_upper_limit) {
        need_stop_rpc = true;
        rpc_stop_reason = RpcStopReason::REACH_MAX_RPC_RESULT;
      } else {
        next_req_lsn = rpc_req.start_lsn_ + result->data_len_;
      }
      cur_valid_rpc_cnt = max(1, result->valid_rpc_cnt_);
    }

    if (OB_SUCC(ret)) {
      const int64_t cur_time = get_timestamp();
      result->rpc_stop_upon_result_ = need_stop_rpc;
      result->rpc_stop_reason_ = rpc_stop_reason;
      result->rpc_time_ = cur_time - rpc_req.rpc_start_time_;
      result->rpc_callback_time_ = cur_time - rpc_callback_start_time;
      result->rpc_prepare_time_ = rpc_req.prepare_end_time_ - rpc_req.rpc_start_time_;
      if (OB_FAIL(splitter_->update_stat(cur_valid_rpc_cnt))) {
        LOG_ERROR("failed to update splitter stat", K(result));
      } else if (OB_FAIL(push_result_and_be_ready_(result, need_dispatch_stream_task))) {
        LOG_ERROR("push result and be ready fail", KPC(result));
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(result_pool_) && OB_NOT_NULL(result)) {
    result_pool_->free(result);
  }

  return ret;
}

int FetchLogARpc::analyze_result_(LogGroupEntryRpcRequest &rpc_req,
    const obrpc::ObRpcResultCode &rcode,
    const obrpc::ObCdcLSFetchLogResp *resp,
    bool &need_stop_rpc,
    RpcStopReason &rpc_stop_reason,
    int64_t &next_upper_limit)
{
  int ret = OB_SUCCESS;
  const int64_t rpc_result_count_per_rpc_upper_limit =
      ATOMIC_LOAD(&g_rpc_result_count_per_rpc_upper_limit);
  const int64_t cur_rpc_result_count = res_queue_.count();
  bool reach_max_rpc_result = (cur_rpc_result_count >= rpc_result_count_per_rpc_upper_limit);
  bool force_stop_rpc = rpc_req.get_stop_flag();

  need_stop_rpc = false;
  rpc_stop_reason = RpcStopReason::INVALID_REASON;
  next_upper_limit = rpc_req.get_upper_limit();

  // If the RPC send fails, or the server returns a failure, there is no need to continue fetching logs
  if (OB_SUCCESS != rcode.rcode_ || NULL == resp || OB_SUCCESS != resp->get_err()) {
    need_stop_rpc = true;
    rpc_stop_reason = RpcStopReason::FETCH_LOG_FAIL;
  } else if (reach_max_rpc_result) {
    // If the number of RPC results reaches the threshold, stop sending RPCs
    need_stop_rpc = true;
    rpc_stop_reason = RpcStopReason::REACH_MAX_RPC_RESULT;
  } else if (force_stop_rpc) {
    // External forced stop RPC
    need_stop_rpc = true;
    rpc_stop_reason = RpcStopReason::FORCE_STOP_RPC;
  } else {
    const int64_t last_upper_limit = rpc_req.get_upper_limit();
    const ObCdcFetchStatus &fetch_status = resp->get_fetch_status();

    // The LS reach progress limit
    bool is_reach_upper_limit_ts = fetch_status.is_reach_upper_limit_ts_;
    // The LS reach maximum log
    bool is_reach_max_lsn = fetch_status.is_reach_max_lsn_;
    // The LS fetch none log
    bool fetch_no_log = (resp->get_log_num() <= 0);

    // If the LS have reached the maximum log, there is no need to continue fetching logs
    if (get_fetch_log_proto() != rpc_req.get_proto_type()) {
      need_stop_rpc = true;
      rpc_stop_reason = RpcStopReason::RPC_PROTO_NOT_MATCH;
    } else if (fetch_no_log) {
      // No logs need to be fetched next time, if no logs are fetched
      need_stop_rpc = true;
      rpc_stop_reason = RpcStopReason::FETCH_NO_LOG;
    } else if (is_reach_max_lsn) {
      need_stop_rpc = true;
      rpc_stop_reason = RpcStopReason::REACH_MAX_LOG;
    } else if (is_reach_upper_limit_ts) {
      // For LS that have reached their upper limit, get the latest upper limit
      // Decide whether to continue to send RPC by whether the upper limit has changed
      if (OB_FAIL(host_.get_upper_limit(next_upper_limit))) {
        LOG_ERROR("get upper limit fail", KR(ret));
      } else {
        need_stop_rpc = (next_upper_limit <= last_upper_limit);
        rpc_stop_reason = RpcStopReason::REACH_UPPER_LIMIT;
      }
    } else {
      // The logs are requested to continue to be fetched in all other cases
      need_stop_rpc = false;
      rpc_stop_reason = RpcStopReason::INVALID_REASON;
    }
  }

  return ret;
}

}
}
