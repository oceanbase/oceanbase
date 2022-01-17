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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_FETCH_LOG_RPC_H__
#define OCEANBASE_LIBOBLOG_OB_LOG_FETCH_LOG_RPC_H__

#include "lib/lock/ob_spin_lock.h"              // ObSpinLock
#include "lib/net/ob_addr.h"                    // ObAddr
#include "lib/profile/ob_trace_id.h"            // TraceId
#include "lib/objectpool/ob_small_obj_pool.h"   // ObSmallObjPool
#include "common/ob_partition_key.h"            // ObPartitionKey
#include "common/ob_queue_thread.h"             // ObCond
#include "rpc/frame/ob_req_transport.h"         // ObReqTranslator::AsyncCB
#include "rpc/obrpc/ob_rpc_proxy.h"             // ObRpcProxy
#include "rpc/obrpc/ob_rpc_packet.h"            // OB_LOG_OPEN_STREAM
#include "rpc/obrpc/ob_rpc_result_code.h"       // ObRpcResultCode
#include "clog/ob_log_external_rpc.h"           // obrpc

#include "ob_log_part_fetch_ctx.h"              // FetchTaskList

namespace oceanbase
{
namespace liboblog
{

class FetchStream;

////////////////////////////// OpenStreamSRpc //////////////////////////////
// Open stream synchronous RPC wrapper class
class OpenStreamSRpc
{
public:
  explicit OpenStreamSRpc();
  virtual ~OpenStreamSRpc();

  void reset();

  /// Execute synchronous RPC request
  /// The ret return value only indicates whether the function succeeds, not whether the RPC succeeds
  /// When the ret returns OB_SUCCESS, you should check result code to determine whether the RPC was successful
  int open_stream(IObLogRpc &rpc,
      const common::ObAddr &svr,
      const int64_t timeout,
      FetchTaskList &part_list,
      const obrpc::ObStreamSeq &stale_stream_seq,
      const int64_t stream_life_time);

  /// Open stream for a single partition
  /// The ret return value only indicates whether the function succeeds, not whether the RPC succeeds
  /// When the ret returns OB_SUCCESS, you should check result code to determine whether the RPC was successful
  int open_stream(IObLogRpc &rpc,
      const common::ObAddr &svr,
      const int64_t timeout,
      const common::ObPartitionKey &pkey,
      const uint64_t next_log_id,
      const obrpc::ObStreamSeq &stale_stream_seq,
      const int64_t stream_life_time);

  const obrpc::ObRpcResultCode &get_result_code() const { return rcode_; }
  const obrpc::ObLogOpenStreamResp &get_resp() const { return resp_; }
  const obrpc::ObLogOpenStreamReq &get_req() const { return req_; }

  TO_STRING_KV(K_(req), K_(resp), K_(rcode));

private:
  int launch_open_stream_rpc_(IObLogRpc &rpc,
      const common::ObAddr &svr,
      const int64_t timeout);
  int build_request_(FetchTaskList &part_list,
      const obrpc::ObStreamSeq &stale_stream_seq,
      const int64_t stream_life_time);
  int build_request_(const common::ObPartitionKey &pkey,
      const uint64_t next_log_id,
      const obrpc::ObStreamSeq &stale_stream_seq,
      const int64_t stream_life_time);
  int set_request_basic_param_(const obrpc::ObStreamSeq &stale_stream_seq,
      const int64_t stream_life_time);
  int set_request_part_list_(FetchTaskList &part_list);
  int set_request_part_list_(const common::ObPartitionKey &pkey,
      const uint64_t next_log_id);

private:
  obrpc::ObLogOpenStreamReq  req_;   // Open stream request
  obrpc::ObLogOpenStreamResp resp_;  // Open stream response
  obrpc::ObRpcResultCode     rcode_; // Open stream RPC result code

private:
  DISALLOW_COPY_AND_ASSIGN(OpenStreamSRpc);
};

////////////////////////////// FetchLogSRpc //////////////////////////////
// Fetch log synchronous RPC wrapper class
// Wrapping synchronous RPC with asynchronous interface
class FetchLogSRpc
{
  typedef obrpc::ObLogExternalProxy::AsyncCB<obrpc::OB_LOG_STREAM_FETCH_LOG> RpcCBBase;

public:
  FetchLogSRpc();
  virtual ~FetchLogSRpc();

public:
  // Perform synchronous RPC requests
  // The ret return value only indicates whether the function was successful, not whether the RPC was successful
  // RPC-related error code is set to result code
  int fetch_log(IObLogRpc &rpc,
      const common::ObAddr &svr,
      const int64_t timeout,
      const obrpc::ObStreamSeq &seq,
      const int64_t upper_limit,
      const int64_t fetch_log_cnt_per_part_per_round,
      const bool need_feed_back);

  int set_resp(const obrpc::ObRpcResultCode &rcode,
      const obrpc::ObLogStreamFetchLogResp *resp);

  void reset();

  const obrpc::ObRpcResultCode &get_result_code() const { return rcode_; }
  const obrpc::ObLogStreamFetchLogResp &get_resp() const { return resp_; }
  const obrpc::ObLogStreamFetchLogReq &get_req() const { return req_; }

private:
  int build_request_(const obrpc::ObStreamSeq &seq,
      const int64_t upper_limit,
      const int64_t fetch_log_cnt_per_part_per_round,
      const bool need_feed_back);

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
    typedef typename obrpc::ObLogExternalProxy::ObRpc<obrpc::OB_LOG_STREAM_FETCH_LOG> ProxyRpc;
    void set_args(const typename ProxyRpc::Request &args) { UNUSED(args); }

  private:
    int do_process_(const obrpc::ObRpcResultCode &rcode, const obrpc::ObLogStreamFetchLogResp *resp);

  private:
    FetchLogSRpc &host_;

  private:
    DISALLOW_COPY_AND_ASSIGN(RpcCB);
  };

private:
  obrpc::ObLogStreamFetchLogReq   req_;     // Fetch log request
  obrpc::ObLogStreamFetchLogResp  resp_;    // Fetch log response
  obrpc::ObRpcResultCode          rcode_;   // Fetch log RPC result code
  RpcCB                           cb_;
  common::ObCond                  cond_;

  // Marking the completion of RPC
  volatile bool rpc_done_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(FetchLogSRpc);
};

struct FetchLogARpcResult;
class IFetchLogARpcResultPool;
class IObLogRpc;
class IObLogStreamWorker;
class ObLogConfig;
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
  struct RpcRequest;

public:
  // The maximum number of results each RPC can have, and stop sending RPCs if this number is exceeded
  static int64_t g_rpc_result_count_per_rpc_upper_limit;
  static bool g_print_rpc_handle_info;

  static void configure(const ObLogConfig &config);

public:
  ////////////////////////// RPC状态 //////////////////////////
  // IDLE:      No RPC task processing
  // READY:     Have RPC tasks to be processed
  enum State
  {
    IDLE = 0,
    READY = 1,
  };

  // RPC stop reason
  enum RpcStopReason
  {
    INVALID_REASON = -1,
    REACH_MAX_LOG = 0,        // Reach maximum log
    REACH_UPPER_LIMIT = 1,    // Reach progress limit
    FETCH_NO_LOG = 2,         // Fetched 0 logs
    FETCH_LOG_FAIL = 3,       // Fetch log failure
    REACH_MAX_RPC_RESULT = 4, // The number of RPC results reaches the upper limit
    FORCE_STOP_RPC = 5,       // Exnernal forced stop of RPC
  };
  static const char *print_rpc_stop_reason(const RpcStopReason reason);

public:
  explicit FetchLogARpc(FetchStream &host);
  virtual ~FetchLogARpc();

  void reset();

  void reset(const common::ObAddr &svr,
      IObLogRpc &rpc,
      IObLogStreamWorker &stream_worker,
      IFetchLogARpcResultPool &result_pool);

  // Preparing an RPC request
  // 1. Require discard_request to be called first if an RPC request has been prepared before, to discard an existing request
  // 2. Require the status to be IDLE
  int prepare_request(const obrpc::ObStreamSeq &seq,
      const int64_t part_cnt,
      const int64_t fetch_log_cnt_per_part_per_round,
      const bool need_feed_back,
      const int64_t rpc_timeout);

  // Discard the current request
  // Set the status to IDLE
  void discard_request(const char *discard_reason);

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
  int async_fetch_log(const obrpc::ObStreamSeq &seq,
      const int64_t upper_limit,
      bool &rpc_send_succeed);

  /// Discard the current request and wait for the end of the asynchronous RPC
  void stop();

  // Iterate over the RPC results
  // 1. requires the current RPC request to be valid
  // 2. require the status to be READY, i.e., there is data to iterate over; this avoids concurrent access by multiple threads
  // 3. return the error code OB_ITER_END if the RPC result iteration is complete, and return whether the RPC is running, then mark the status as IDLE
  // 4. only iterate over results that match the current request
  int next_result(FetchLogARpcResult *&result, bool &rpc_is_flying);

  // Recycling results
  void revert_result(FetchLogARpcResult *result);

  // Update the request parameters
  // Require the current request to be valid
  int update_request(const obrpc::ObStreamSeq &seq,
      const int64_t upper_limit,
      const int64_t fetch_log_cnt_per_part_per_round,
      const bool need_feed_back,
      const int64_t rpc_timeout);

  // Mark the end of the request
  // Require the current request to be valid
  int mark_request_stop(const obrpc::ObStreamSeq &seq);

  // Process the RPC request result, called by the RPC callback thread
  // 1. If it matches the current RPC request, push the result to the request queue
  // 2. If it doesn't match the current RPC request, it is a deprecated RPC request, so the RPC result is discarded and the deprecated RPC request is recycled
  // 3. Based on the request result, decide whether to launch the next RPC request immediately
  int handle_rpc_response(RpcRequest &rpc_request,
    const obrpc::ObRpcResultCode &rcode,
    const obrpc::ObLogStreamFetchLogResp *resp);

  static const char *print_state(State state);

  int64_t get_flying_request_count();
  void print_flying_request_list();

private:
  int alloc_rpc_request_(const obrpc::ObStreamSeq &seq,
      const int64_t part_cnt,
      const int64_t fetch_log_cnt_per_part_per_round,
      const bool need_feed_back,
      const int64_t rpc_timeout,
      RpcRequest *&req);
  void free_rpc_request_(RpcRequest *request);
  int generate_rpc_result_(RpcRequest &rpc_req,
      const obrpc::ObRpcResultCode &rcode,
      const obrpc::ObLogStreamFetchLogResp *resp,
      const int64_t rpc_callback_start_time,
      const bool need_stop_rpc,
      const RpcStopReason rpc_stop_reason,
      bool &need_dispatch_stream_task);
  int launch_async_rpc_(RpcRequest &request,
      const int64_t upper_limit,
      const bool launch_by_cb,
      bool &rpc_send_succeed);
  int push_result_and_be_ready_(FetchLogARpcResult *result, bool &is_state_idle);
  int pop_result_(FetchLogARpcResult *&result);
  void clear_result_();
  int destroy_flying_request_(RpcRequest *target_request);
  int analyze_result_(RpcRequest &rpc_req,
      const obrpc::ObRpcResultCode &rcode,
      const obrpc::ObLogStreamFetchLogResp *resp,
      bool &need_stop_rpc,
      RpcStopReason &rpc_stop_reason,
      int64_t &next_upper_limit);
  void print_handle_info_(RpcRequest &rpc_req,
      const obrpc::ObLogStreamFetchLogResp *resp,
      const int64_t next_upper_limit,
      const bool need_stop_rpc,
      const RpcStopReason rpc_stop_reason,
      const bool need_dispatch_stream_task);

private:
  ////////////////////////////// RpcCB //////////////////////////////
  // Callback of Fetch log RPC
  typedef obrpc::ObLogExternalProxy::AsyncCB<obrpc::OB_LOG_STREAM_FETCH_LOG> RpcCBBase;
  class RpcCB : public RpcCBBase
  {
  public:
    explicit RpcCB(RpcRequest &host) : host_(host) {}
    virtual ~RpcCB() {}

  public:
    rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const;
    int process();
    void on_timeout();
    void on_invalid();
    typedef typename obrpc::ObLogExternalProxy::ObRpc<obrpc::OB_LOG_STREAM_FETCH_LOG> ProxyRpc;
    void set_args(const typename ProxyRpc::Request &args) { UNUSED(args); }

    TO_STRING_KV("host", reinterpret_cast<void *>(&host_));

  private:
    int do_process_(const obrpc::ObRpcResultCode &rcode, const obrpc::ObLogStreamFetchLogResp *resp);

  private:
    RpcRequest &host_;

  private:
    DISALLOW_COPY_AND_ASSIGN(RpcCB);
  };

  ////////////////////////////// RpcRequest //////////////////////////////
  // RPC request structure
  // Each RPC request corresponds to a stream identifier uniquely
  struct RpcRequest
  {
    // Invariant member variables within the request
    FetchLogARpc                    &host_;
    const int64_t                   part_cnt_;        // partition count
    RpcCB                           cb_;              // RPC callback

    // Variables that change with the request
    bool                            need_feed_back_;  // need feedback
    int64_t                         rpc_timeout_;     // RPC timeout
    obrpc::ObLogStreamFetchLogReq   req_;             // Fetch log request
    common::ObCurTraceId::TraceId   trace_id_;

    RpcRequest                      *next_;           // Link list Structure

    // Forced stop flag
    // The life of this flag is consistent with the RPC, and the flag is to be reset before each round of RPC starts
    bool                            force_stop_flag_ CACHE_ALIGNED;

    //  Start time of this RPC
    int64_t                         rpc_start_time_ CACHE_ALIGNED;

    // Whether the RPC is being executed and no callback has been executed yet
    volatile bool                   rpc_is_flying_ CACHE_ALIGNED;

    RpcRequest(FetchLogARpc &host,
        const obrpc::ObStreamSeq &seq,
        const int64_t part_cnt,
        const int64_t fetch_log_cnt_per_part_per_round,
        const bool need_feed_back,
        const int64_t rpc_timeout);
    virtual ~RpcRequest() {}

    // Update request parameters
    void update_request(const int64_t upper_limit,
        const int64_t fetch_log_cnt_per_part_per_round,
        const bool need_feed_back,
        const int64_t rpc_timeout);

    // Prepare the RPC request structure, set the final parameters, and immediately launch the RPC request next
    void prepare(const int64_t upper_limit);

    // Marking RPC run status
    void mark_flying_state(const bool rpc_is_flying);

    // Mark the stop of RPC
    // Request the next round of RPC to stop
    void mark_stop_flag() { ATOMIC_STORE(&force_stop_flag_, true); }

    const obrpc::ObStreamSeq &get_stream_seq() const { return req_.get_stream_seq(); }
    const common::ObCurTraceId::TraceId &get_trace_id() const { return trace_id_; }
    int64_t get_rpc_start_time() const { return rpc_start_time_; }
    bool rpc_is_flying() const { return ATOMIC_LOAD(&rpc_is_flying_); }
    int64_t get_upper_limit() const { return req_.get_upper_limit_ts(); }
    int64_t get_part_count() const { return part_cnt_; }
    bool get_stop_flag() const { return ATOMIC_LOAD(&force_stop_flag_); }

    TO_STRING_KV(K_(rpc_is_flying),
        K_(part_cnt),
        "rpc_start_time", TS_TO_STR(rpc_start_time_),
        K_(force_stop_flag),
        K_(need_feed_back),
        K_(rpc_timeout),
        K_(req),
        K_(trace_id),
        KP_(next));

  private:
    DISALLOW_COPY_AND_ASSIGN(RpcRequest);
  };

  ////////////////////////////// RpcRequestList //////////////////////////////
  // RPC request list
  struct RpcRequestList
  {
    RpcRequest  *head_;
    RpcRequest  *tail_;
    int64_t     count_;

    RpcRequestList() { reset(); }

    void reset()
    {
      head_ = NULL;
      tail_ = NULL;
      count_ = 0;
    }

    void add(RpcRequest *req);

    // Use the stream identifier to delete the request structure
    int remove(RpcRequest *target);

    TO_STRING_KV(K_(count), K_(head), K_(tail));
  };

public:
  TO_STRING_KV(
      "host", reinterpret_cast<void *>(&host_),
      "state", print_state(state_),
      "rpc_result_cnt", res_queue_.count(),
      KPC_(cur_req),
      K_(flying_req_list));

private:
  typedef common::ObMapQueue<void *> ResQueue;

  FetchStream               &host_;

  common::ObAddr            svr_;
  IObLogRpc                 *rpc_;
  IObLogStreamWorker        *stream_worker_;
  IFetchLogARpcResultPool   *result_pool_;

  State                     state_ CACHE_ALIGNED;
  RpcRequest                *cur_req_ CACHE_ALIGNED;

  // List of running RPC requests that have been deprecated
  // These RPC requests are executing, no callbacks yet, but have been deprecated
  // [ObStreamSeq <--> RpcRequest] One by one correspondence
  RpcRequestList            flying_req_list_;

  // Request Results Queue
  ResQueue                  res_queue_;

  common::ObSpinLock        lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(FetchLogARpc);
};

////////////////////////////// FetchLogARpc Request Result //////////////////////////////
struct FetchLogARpcResult
{
  obrpc::ObStreamSeq              seq_;     // Stream Identifier
  obrpc::ObLogStreamFetchLogResp  resp_;    // Fetch log response
  obrpc::ObRpcResultCode          rcode_;   // Fetch log result
  common::ObCurTraceId::TraceId   trace_id_;

  // Statistical items
  // The time spent on the server side is in the fetch log result
  int64_t                         rpc_time_;              // Total RPC time: network + server + asynchronous processing
  int64_t                         rpc_callback_time_;     // RPC asynchronous processing time
  bool                            rpc_stop_upon_result_;  // Whether the RPC stops after the result is processed, i.e. whether it stops because of that result
  FetchLogARpc::RpcStopReason     rpc_stop_reason_;       // RPC stop reason

  FetchLogARpcResult() { reset(); }
  virtual ~FetchLogARpcResult() {}

  int set(const obrpc::ObStreamSeq &seq,
      const obrpc::ObRpcResultCode &rcode,
      const obrpc::ObLogStreamFetchLogResp *resp,
      const common::ObCurTraceId::TraceId &trace_id,
      const int64_t rpc_start_time,
      const int64_t rpc_callback_start_time,
      const bool rpc_stop_upon_result,
      const FetchLogARpc::RpcStopReason rpc_stop_reason);

  void reset()
  {
    seq_.reset();
    resp_.reset();
    rcode_.reset();
    trace_id_.reset();
    rpc_time_ = 0;
    rpc_callback_time_ = 0;
    rpc_stop_upon_result_ = false;
    rpc_stop_reason_ = FetchLogARpc::INVALID_REASON;
  }

  TO_STRING_KV(K_(seq), K_(rcode), K_(resp), K_(trace_id), K_(rpc_time),
      K_(rpc_callback_time), K_(rpc_stop_upon_result),
      "rpc_stop_reason", FetchLogARpc::print_rpc_stop_reason(rpc_stop_reason_));
};

////////////////////////////// FetchLogARpcResult对象池 //////////////////////////////
class IFetchLogARpcResultPool
{
public:
  virtual ~IFetchLogARpcResultPool() {}

public:
  virtual int alloc(FetchLogARpcResult *&result) = 0;
  virtual void free(FetchLogARpcResult *result) = 0;
};

class FetchLogARpcResultPool : public IFetchLogARpcResultPool
{
  typedef common::ObSmallObjPool<FetchLogARpcResult> ResultPool;
  static const int64_t DEFAULT_RESULT_POOL_BLOCK_SIZE = 1L << 24;

public:
  FetchLogARpcResultPool() : inited_(false), pool_() {}
  virtual ~FetchLogARpcResultPool() { destroy(); }

public:
  int init(const int64_t cached_obj_count);
  void destroy();
  void print_stat();

public:
  virtual int alloc(FetchLogARpcResult *&result);
  virtual void free(FetchLogARpcResult *result);

private:
  bool        inited_;
  ResultPool  pool_;

private:
  DISALLOW_COPY_AND_ASSIGN(FetchLogARpcResultPool);
};

}
}

#endif
