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

#ifndef OCEANBASE_LOG_FETCHER_FETCH_LOG_RPC_H__
#define OCEANBASE_LOG_FETCHER_FETCH_LOG_RPC_H__

#include "lib/container/ob_ext_ring_buffer.h"
#include "lib/lock/ob_spin_lock.h"              // ObSpinLock
#include "lib/net/ob_addr.h"                    // ObAddr
#include "lib/profile/ob_trace_id.h"            // TraceId
#include "lib/objectpool/ob_small_obj_pool.h"   // ObSmallObjPool
#include "common/ob_queue_thread.h"             // ObCond
#include "logservice/cdcservice/ob_cdc_struct.h"
#include "ob_log_file_buffer_pool.h"
#include "ob_log_fetch_log_rpc_splitter.h"
#include "ob_log_fetch_log_rpc_req.h"
#include "ob_log_fetch_log_rpc_stop_reason.h"
#include "rpc/frame/ob_req_transport.h"         // ObReqTranslator::AsyncCB
#include "rpc/obrpc/ob_rpc_proxy.h"             // ObRpcProxy
#include "rpc/obrpc/ob_rpc_packet.h"            // OB_LOG_OPEN_STREAM
#include "rpc/obrpc/ob_rpc_result_code.h"       // ObRpcResultCode
#include "logservice/cdcservice/ob_cdc_rpc_proxy.h"  // ObCdcProxy
#include "ob_log_ls_fetch_ctx.h"                // FetchTaskList

namespace oceanbase
{
namespace logfetcher
{

class FetchStream;
class RawLogFileRpcResult;

////////////////////////////// FetchLogSRpc //////////////////////////////
// Fetch log synchronous RPC wrapper class
// Wrapping synchronous RPC with asynchronous interface
// Use for fetch missing log
class FetchLogSRpc
{
  typedef obrpc::ObCdcProxy::AsyncCB<obrpc::OB_LS_FETCH_MISSING_LOG> RpcCBBase;

public:
  FetchLogSRpc();
  virtual ~FetchLogSRpc();

public:
  // Perform synchronous RPC requests
  // The ret return value only indicates whether the function was successful, not whether the RPC was successful
  // RPC-related error code is set to result code
  int fetch_log(IObLogRpc &rpc,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array,
      const common::ObAddr &svr,
      const int64_t timeout);

  int set_resp(const obrpc::ObRpcResultCode &rcode,
      const obrpc::ObCdcLSFetchLogResp *resp);

  void reset();

  const obrpc::ObRpcResultCode &get_result_code() const { return rcode_; }
  const obrpc::ObCdcLSFetchLogResp &get_resp() const { return resp_; }
  const obrpc::ObCdcLSFetchMissLogReq &get_req() const { return req_; }

private:
  int build_request_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array);

private:
  ////////////////////////////// RpcCB //////////////////////////////
  // Callback of Fetch log synchronization RPC
  class RpcCB : public RpcCBBase
  {
  public:
    explicit RpcCB(FetchLogSRpc &host);
    virtual ~RpcCB();

  public:
    rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const;
    int process();
    void on_timeout();
    void on_invalid();
    typedef typename obrpc::ObCdcProxy::ObRpc<obrpc::OB_LS_FETCH_MISSING_LOG> ProxyRpc;
    void set_args(const typename ProxyRpc::Request &args) { UNUSED(args); }

  private:
    int do_process_(const obrpc::ObRpcResultCode &rcode, const obrpc::ObCdcLSFetchLogResp *resp);

  private:
    FetchLogSRpc &host_;

  private:
    DISALLOW_COPY_AND_ASSIGN(RpcCB);
  };

private:
  obrpc::ObCdcLSFetchMissLogReq   req_;     // Fetch Missing log request
  obrpc::ObCdcLSFetchLogResp      resp_;    // Fetch log response
  obrpc::ObRpcResultCode          rcode_;   // Fetch log RPC result code
  RpcCB                           cb_;
  common::ObCond                  cond_;

  // Marking the completion of RPC
  volatile bool rpc_done_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(FetchLogSRpc);
};

struct LogGroupEntryRpcResult;
class FetchLogRpcResultPool;
class FetchLogRpcResult;
class IObLogRpc;
class IObLSWorker;
class ObLogFetcherConfig;
////////////////////////////// FetchLogARpc //////////////////////////////
// Fetch log asynchronous RPC wrapper class
// 1. To achieve the ultimate performance, the fetch log RPC is streamed with the log processing logic:
//    after one RPC completes, the next RPC is issued immediately without waiting for log processing to complete
// 2. "Issue the next RPC immediately" has a certain condition: the next RPC is guaranteed to fetch the log
//    The following cases do not require the next RPC to be issued immediately.
//    1). The upper limit is reached
//    2). All partition logs are up to date
//    3). RPC failed or observer failed to return
//    4). ...
// 3. Streaming mode is a significant performance improvement for fetching historical log scenarios. To maximize performance, the ideal case is.
//    1). Each RPC packet is full, carrying as much data as possible at a time, which can reduce the loss caused by network overhead
//    2). Since the upper limit is updated on a delayed basis (the log is updated only after processing), the upper limit should be set as large as possible.
//        The optimal case should be the time range covered by at least two RPCs.
//        Due to the different data distribution, the upper limit should support dynamic updates.
//    3). The partition list inside the stream should be stable for a long time to avoid the loss caused by open streams and thread scheduling
//    4). The number of streams should be as small as possible, preferably less than or equal to the number of worker threads
//
class FetchLogARpc
{
private:
  static const int64_t WAIT_TIME_ON_STOP = 10 * _MSEC_;
  static const int64_t WARN_COUNT_ON_STOP = 500;

public:
  // The maximum number of results each RPC can have, and stop sending RPCs if this number is exceeded
  static int64_t g_rpc_result_count_per_rpc_upper_limit;
  static bool g_print_rpc_handle_info;

  static void configure(const ObLogFetcherConfig &config);

public:
  ////////////////////////// RPC状态 //////////////////////////
  // IDLE:      No RPC task processing
  // READY:     Have RPC tasks to be processed
  enum State
  {
    IDLE = 0,
    READY = 1,
  };

public:
  explicit FetchLogARpc(FetchStream &host);
  virtual ~FetchLogARpc();

  void reset();

  int init(
      const uint64_t source_tenant_id,
      const uint64_t self_tenant_id,
      IObLogRpc &rpc,
      IObLSWorker &stream_worker,
      FetchLogRpcResultPool &result_pool,
      LogFileDataBufferPool &log_file_pool);

  // We should set server before start fetch logs
  int set_server(const common::ObAddr &svr);

  // Preparing an RPC request
  // 1. Require discard_request to be called first if an RPC request has been prepared before, to discard an existing request
  // 2. Require the status to be IDLE
  int prepare_request(const share::ObLSID &ls_id,
      const int64_t rpc_timeout);

  // Discard the current request
  // Set the status to IDLE
  void discard_request(const char *discard_reason, const bool is_normal_discard = true);

  // Launch an asynchronous RPC request
  // 1. Requires that the request structure is ready
  // 2. Requires a stream match
  // 3. Requires that no asynchronous RPC is currently executing
  // 4. requires the status to be IDLE
  //
  // Note that.
  // 1. The ret return value only indicates whether the function was executed successfully, not whether the RPC was successful
  // 2. The success of the RPC is returned using the rpc_send_succeed parameter
  // 3. if the RPC fails, the result will be generated immediately, you can use next_result() to iterate through the results
  // 4. If the RPC succeeds, you need to wait for the asynchronous callback to set the result
  int async_fetch_log(
      const palf::LSN &req_start_lsn,
      const int64_t client_progress,
      const int64_t upper_limit,
      bool &rpc_send_succeed);

  /// Discard the current request and wait for the end of the asynchronous RPC
  void stop();

  // Iterate over the RPC results
  // 1. requires the current RPC request to be valid
  // 2. require the status to be READY, i.e., there is data to iterate over; this avoids concurrent access by multiple threads
  // 3. return the error code OB_ITER_END if the RPC result iteration is complete, and return whether the RPC is running, then mark the status as IDLE
  // 4. only iterate over results that match the current request
  int next_result(FetchLogRpcResult *&result, bool &rpc_is_flying);

  // Recycling results
  void revert_result(FetchLogRpcResult *result);

  // Update the request parameters
  // Require the current request to be valid
  int update_request(
      const int64_t upper_limit,
      const int64_t rpc_timeout);

  // Mark the end of the request
  // Require the current request to be valid
  int mark_request_stop();

  // Process the RPC request result, called by the RPC callback thread
  // 1. If it matches the current RPC request, push the result to the request queue
  // 2. If it doesn't match the current RPC request, it is a deprecated RPC request, so the RPC result is discarded and the deprecated RPC request is recycled
  // 3. Based on the request result, decide whether to launch the next RPC request immediately
  int handle_rpc_response(LogGroupEntryRpcRequest &rpc_request,
    const obrpc::ObRpcResultCode &rcode,
    const obrpc::ObCdcLSFetchLogResp *resp);

  // Lock is not needed when in the post-process after the failure of rpc
  int handle_rpc_response(RawLogFileRpcRequest &rpc_request,
      const bool need_lock);

  // split the RawLogFileRpcRequest into several RawLogDataRpcRequests,
  // which requires that all other info of RawLogFileRpcRequest is complete
  int split_rpc_request(RawLogFileRpcRequest &request);

  static const char *print_state(State state);

  int64_t get_flying_request_count();
  void print_flying_request_list();
  bool is_rpc_ready() const { return State::READY == state_; }

  void set_fetch_log_proto(const obrpc::ObCdcFetchLogProtocolType proto) {
    ATOMIC_STORE(&proto_type_, proto);
  }

  obrpc::ObCdcFetchLogProtocolType get_fetch_log_proto() const {
    return ATOMIC_LOAD(&proto_type_);
  }

private:
  int alloc_log_group_entry_rpc_request_(const share::ObLSID &ls_id,
      const int64_t rpc_timeout,
      FetchLogRpcReq *&req);

  int alloc_raw_log_file_rpc_request_(const share::ObLSID &ls_id,
      const int64_t rpc_timeout,
      FetchLogRpcReq *&req);

  void free_rpc_request_(FetchLogRpcReq *request);
  int generate_rpc_result_(LogGroupEntryRpcRequest &rpc_req,
      const obrpc::ObRpcResultCode &rcode,
      const obrpc::ObCdcLSFetchLogResp *resp,
      const int64_t rpc_callback_start_time,
      const bool need_stop_rpc,
      const RpcStopReason rpc_stop_reason,
      bool &need_dispatch_stream_task);

  int handle_rpc_response_no_lock_(RawLogFileRpcRequest &request);

  int generate_rpc_result_(RawLogFileRpcRequest &rpc_req,
      const int64_t rpc_callback_start_time,
      bool &need_stop_rpc,
      RpcStopReason &rpc_stop_reason,
      bool &need_dispatch_stream_task,
      palf::LSN &next_req_lsn);

  int launch_async_raw_file_rpc_(RawLogFileRpcRequest &request,
      const palf::LSN &req_start_lsn,
      const int64_t progress,
      const bool lauch_by_cb,
      bool &rpc_send_succeed);

  // when launching async rpc, make sure that lock_ is hold
  int launch_async_rpc_(LogGroupEntryRpcRequest &request,
      const palf::LSN &req_start_lsn,
      const int64_t progress,
      const int64_t upper_limit,
      const bool launch_by_cb,
      bool &rpc_send_succeed);
  int push_result_and_be_ready_(FetchLogRpcResult *result, bool &is_state_idle);
  int pop_result_(FetchLogRpcResult *&result);
  void clear_result_();
  int destroy_flying_request_(FetchLogRpcReq *target_request);
  int analyze_result_(LogGroupEntryRpcRequest &rpc_req,
      const obrpc::ObRpcResultCode &rcode,
      const obrpc::ObCdcLSFetchLogResp *resp,
      bool &need_stop_rpc,
      RpcStopReason &rpc_stop_reason,
      int64_t &next_upper_limit);
  void print_handle_info_(LogGroupEntryRpcRequest &rpc_req,
      const obrpc::ObCdcLSFetchLogResp *resp,
      const int64_t next_upper_limit,
      const bool need_stop_rpc,
      const RpcStopReason rpc_stop_reason,
      const bool need_dispatch_stream_task);

public:
  TO_STRING_KV(
      K_(source_tenant_id),
      "host", reinterpret_cast<void *>(&host_),
      "state", print_state(state_),
      "rpc_result_cnt", res_queue_.count(),
      KP_(cur_req),
      K_(flying_req_list));

private:
  typedef common::ObMapQueue<FetchLogRpcResult *> ResQueue;

  FetchStream               &host_;

  uint64_t                  source_tenant_id_;
  uint64_t                  self_tenant_id_;
  common::ObAddr            svr_;
  IObLogRpc                 *rpc_;
  IObLSWorker               *stream_worker_;
  FetchLogRpcResultPool    *result_pool_;
  LogFileDataBufferPool     *log_file_pool_;

  State                     state_ CACHE_ALIGNED;
  FetchLogRpcReq            *cur_req_ CACHE_ALIGNED;

  // List of running RPC requests that have been deprecated
  // These RPC requests are executing, no callbacks yet, but have been deprecated
  // [ObStreamSeq <--> RpcRequest] One by one correspondence
  RpcRequestList            flying_req_list_;

  // Request Results Queue
  ResQueue                  res_queue_;

  obrpc::ObCdcFetchLogProtocolType proto_type_;
  IFetchLogRpcSplitter      *splitter_;

  common::ObSpinLock        lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(FetchLogARpc);
};

}
}

#endif
