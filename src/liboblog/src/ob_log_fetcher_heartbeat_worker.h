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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_FETCHER_HEARTBEAT_WORKER_H_
#define OCEANBASE_LIBOBLOG_OB_LOG_FETCHER_HEARTBEAT_WORKER_H_

#include "share/ob_define.h"                // OB_INVALID_ID
#include "share/ob_errno.h"                 // OB_SUCCESS
#include "lib/utility/ob_print_utils.h"   // TO_STRING_KV
#include "lib/atomic/ob_atomic.h"         // ATOMIC_*
#include "lib/net/ob_addr.h"              // ObAddr
#include "lib/container/ob_array.h"       // ObArray
#include "lib/allocator/ob_safe_arena.h"  // ObSafeArena
#include "lib/hash/ob_linear_hash_map.h"  // ObLinearHashMap
#include "lib/profile/ob_trace_id.h"      // ObCurTraceId
#include "common/ob_partition_key.h"      // ObPartitionKey
#include "clog/ob_log_external_rpc.h"     // obrpc

#include "ob_map_queue_thread.h"          // ObMapQueueThread
#include "ob_log_config.h"                // ObLogConfig
#include "ob_log_utils.h"                 // _SEC_

namespace oceanbase
{
namespace liboblog
{

struct HeartbeatRequest;
class IObLogFetcherHeartbeatWorker
{
public:
  virtual ~IObLogFetcherHeartbeatWorker() {}

public:
  virtual int async_heartbeat_req(HeartbeatRequest *req) = 0;

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
};

/////////////////////////////////////// ObLogFetcherHeartbeatWorker ///////////////////////////////////////

typedef common::ObMapQueueThread<ObLogConfig::max_fetcher_heartbeat_thread_num> HeartbeatThread;

class IObLogRpc;
class IObLogErrHandler;

class ObLogFetcherHeartbeatWorker : public IObLogFetcherHeartbeatWorker, public HeartbeatThread
{
  static const int64_t DATA_OP_TIMEOUT = 100 * _MSEC_;

  typedef obrpc::ObLogLeaderHeartbeatReq RpcReq;
  typedef obrpc::ObLogLeaderHeartbeatResp RpcResp;

  // Class global variables
public:
  static int64_t g_rpc_timeout;
  static int64_t g_batch_count;

public:
  ObLogFetcherHeartbeatWorker();
  virtual ~ObLogFetcherHeartbeatWorker();

  int init(const int64_t thread_num,
      IObLogRpc &rpc,
      IObLogErrHandler &err_handler);
  void destroy();

public:
  int async_heartbeat_req(HeartbeatRequest *req);
  int start();
  void stop();
  void mark_stop_flag() { HeartbeatThread::mark_stop_flag(); }

public:
  // Implement HeartbeatThread's thread handling functions
  void run(const int64_t thread_index);

public:
  static void configure(const ObLogConfig &config);

private:
  struct WorkerData;
  struct SvrReq;
  int do_retrieve_(const int64_t thread_index, WorkerData &data);
  SvrReq *alloc_svr_req_(const common::ObAddr &svr);
  void free_svr_req_(SvrReq *req);
  void free_all_svr_req_(WorkerData &data);
  int get_svr_req_(WorkerData &data,
      const common::ObAddr &svr,
      SvrReq *&svr_req);
  int do_request_(WorkerData &data);
  int build_rpc_request_(RpcReq &rpc_req,
      SvrReq &svr_req,
      const int64_t start_idx);
  int request_heartbeat_(const RpcReq &rpc_req,
      SvrReq &svr_req,
      const int64_t start_idx);

private:
  // Single server request
  struct SvrReq
  {
    typedef common::ObArray<HeartbeatRequest*> ReqList;

    const common::ObAddr  svr_;
    ReqList               hb_req_list_;   // Heartbeat Request List

    explicit SvrReq(const common::ObAddr &svr) : svr_(svr), hb_req_list_()
    {}

    TO_STRING_KV(K_(svr), "hb_req_cnt", hb_req_list_.count(), K_(hb_req_list));

    void reset()
    {
      hb_req_list_.reset();
    }

    int push(HeartbeatRequest *req)
    {
      return hb_req_list_.push_back(req);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(SvrReq);
  };

  typedef common::ObArray<SvrReq *> SvrReqList;
  typedef common::ObLinearHashMap<common::ObAddr, SvrReq*> SvrReqMap;

  // Local data per Worker
  struct WorkerData
  {
    SvrReqList svr_req_list_;
    SvrReqMap  svr_req_map_;

    WorkerData() : svr_req_list_(), svr_req_map_()
    {}
    ~WorkerData() { destroy(); }

    int init();
    void destroy();

    void reset()
    {
      svr_req_list_.reset();
      svr_req_map_.reset();
    }
  };

private:
  bool                inited_;
  int64_t             thread_num_;
  IObLogRpc           *rpc_;
  IObLogErrHandler    *err_handler_;
  WorkerData          *worker_data_;

  // Module Arena allocator with multi-threaded support
  typedef common::ObSafeArena AllocatorType;
  AllocatorType       allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFetcherHeartbeatWorker);
};

/////////////////////////////////////// HeartbeatResponse ///////////////////////////////////////

/// HeartbeatRequest result
struct HeartbeatResponse
{
  uint64_t next_served_log_id_ ;  // The next log ID of the server services
  int64_t next_served_tstamp_;    // Lower bound for the next log timestamp of the server services

  int rpc_err_;                   // rpc error
  int svr_err_;                   // server error
  int partition_err_;             // partition error

  void reset()
  {
    next_served_log_id_ = common::OB_INVALID_ID;
    next_served_tstamp_ = common::OB_INVALID_TIMESTAMP;
    rpc_err_ = common::OB_SUCCESS;
    svr_err_ = common::OB_SUCCESS;
    partition_err_ = common::OB_SUCCESS;
  }

  void set(const int rpc_err, const int svr_err, const int partition_err,
      const uint64_t next_served_log_id, const int64_t next_served_tstamp)
  {
    rpc_err_ = rpc_err;
    svr_err_ = svr_err;
    partition_err_ = partition_err;
    next_served_log_id_ = next_served_log_id;
    next_served_tstamp_ = next_served_tstamp;
  }

  TO_STRING_KV(K_(next_served_log_id), K_(next_served_tstamp),
      K_(rpc_err), K_(svr_err), K_(partition_err));
};

/////////////////////////////////////// HeartbeatRequest ///////////////////////////////////////

typedef common::ObCurTraceId::TraceId TraceIdType;

/*
 * HeartbeatRequest
 * Request Status:
 *  - IDLE: Idle state
 *  - REQ:  Requesting status, the result is not readable, external need to ensure the validity of the request memory
 *  - DONE: When the request is finished, you can read the result and reset it
 */
struct HeartbeatRequest
{
  enum State { IDLE = 0, REQ, DONE };

  // Request Status
  State                   state_;

  // Request Parameters
  common::ObAddr          svr_;
  common::ObPartitionKey  pkey_;
  uint64_t                next_log_id_;

  // Request Result
  HeartbeatResponse       resp_;

  // Trace ID used in the request process
  TraceIdType             trace_id_;

  TO_STRING_KV(K_(pkey), K_(state), K_(next_log_id), K_(svr), K_(resp), K_(trace_id));

  void reset()
  {
    set_state(IDLE);
    svr_.reset();
    pkey_.reset();
    next_log_id_ = common::OB_INVALID_ID;
    resp_.reset();
    trace_id_.reset();
  }

  void reset(const common::ObPartitionKey &pkey,
      const uint64_t next_log_id,
      const common::ObAddr &svr)
  {
    reset();

    svr_ = svr;
    pkey_ = pkey;
    next_log_id_ = next_log_id;
  }

  void set_resp(const int rpc_err, const int svr_err, const int partition_err,
      const uint64_t next_served_log_id, const int64_t next_served_tstamp,
      TraceIdType *trace_id)
  {
    resp_.set(rpc_err, svr_err, partition_err, next_served_log_id, next_served_tstamp);
    if (NULL != trace_id) {
      trace_id_ = *trace_id;
    }
  }

  const HeartbeatResponse &get_resp() const { return resp_; }

  void set_state(const State state) { ATOMIC_STORE(&state_, state); }
  State get_state() const { return (ATOMIC_LOAD(&state_)); }

  void set_state_idle() { ATOMIC_STORE(&state_, IDLE); }
  void set_state_req() { ATOMIC_STORE(&state_, REQ); }
  void set_state_done() { ATOMIC_STORE(&state_, DONE); }
  bool is_state_idle() const { return (ATOMIC_LOAD(&state_)) == IDLE; }
  bool is_state_req() const { return (ATOMIC_LOAD(&state_)) == REQ; }
  bool is_state_done() const { return (ATOMIC_LOAD(&state_)) == DONE; }
};

}
}

#endif
