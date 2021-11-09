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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_START_LOG_ID_LOCATOR_H_
#define OCEANBASE_LIBOBLOG_OB_LOG_START_LOG_ID_LOCATOR_H_

#include "lib/atomic/ob_atomic.h"         // ATOMIC_*
#include "lib/net/ob_addr.h"              // ObAddr
#include "lib/container/ob_array.h"       // ObArray
#include "lib/hash/ob_linear_hash_map.h"  // ObLinearHashMap
#include "lib/utility/ob_print_utils.h"   // TO_STRING_KV
#include "lib/allocator/ob_safe_arena.h"  // ObSafeArena
#include "lib/profile/ob_trace_id.h"      // ObCurTraceId
#include "common/ob_partition_key.h"      // ObPartitionKey
#include "clog/ob_log_external_rpc.h"     // BreakInfo

#include "ob_map_queue_thread.h"          // ObMapQueueThread
#include "ob_log_config.h"                // ObLogConfig
#include "ob_log_utils.h"                 // _SEC_

namespace oceanbase
{
namespace liboblog
{

struct StartLogIdLocateReq;
class IObLogStartLogIdLocator
{
public:
  virtual ~IObLogStartLogIdLocator() {}

public:
  virtual int async_start_log_id_req(StartLogIdLocateReq *req) = 0;

  virtual int  start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
};

//////////////////////////// ObLogStartLogIdLocator ////////////////////////////

typedef common::ObMapQueueThread<ObLogConfig::max_start_log_id_locator_thread_num> LocateWorker;

class IObLogRpc;
class IObLogErrHandler;
class ObLogStartLogIdLocator : public IObLogStartLogIdLocator, public LocateWorker
{
  static const int64_t DATA_OP_TIMEOUT = 100 * _MSEC_;
  typedef obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint RpcReq;
  typedef obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint RpcRes;
  typedef common::ObSafeArena AllocatorType;

  // Class member variables
public:
  static int64_t g_batch_count;
  static int64_t g_rpc_timeout;
  static int64_t g_observer_clog_save_time;      // Maximum log retention time of observer
  static bool    g_enable_force_start_mode;      // Forced start mode, at least one server returns OB_ERR_OUT_OF_LOWER_BOUND

public:
  ObLogStartLogIdLocator();
  virtual ~ObLogStartLogIdLocator();

  int init(
      const int64_t worker_cnt,
      const int64_t locate_count,
      IObLogRpc &rpc,
      IObLogErrHandler &err_handle);
  void destroy();

public:
  int async_start_log_id_req(StartLogIdLocateReq *req);
  int start();
  void stop();
  void mark_stop_flag() { LocateWorker::mark_stop_flag(); }

public:
  // Implementation of LocateWorker's thread handling functions
  void run(const int64_t thread_index);

  // Class member function
public:
  static void configure(const ObLogConfig &config);

// private member function
private:
  struct WorkerData;
  struct SvrReq;
  int dispatch_worker_(StartLogIdLocateReq *req);
  int do_retrieve_(const int64_t thread_index, WorkerData &data);
  int get_svr_req_(WorkerData &data,
      const common::ObAddr &svr,
      SvrReq *&svr_list);
  SvrReq *alloc_svr_req_(const common::ObAddr &svr);
  void free_svr_req_(SvrReq *req);
  void free_all_svr_req_(WorkerData &data);
  int do_request_(WorkerData &data);
  int build_request_params_(RpcReq &req,
      const SvrReq &svr_req,
      const int64_t req_cnt);
  int init_worker_data_(WorkerData &data);
  void destroy_worker_data_(WorkerData &data);
  int do_rpc_and_dispatch_(
      IObLogRpc &rpc,
      RpcReq &rpc_req,
      SvrReq &svr_req,
      int64_t &succ_req_cnt);

// private structs
private:
  // Requests from a single server
  struct SvrReq
  {
    typedef common::ObArray<StartLogIdLocateReq*> ReqList;

    common::ObAddr  svr_;
    ReqList         locate_req_list_;

    explicit SvrReq(const common::ObAddr &svr) : svr_(svr), locate_req_list_()
    {}

    TO_STRING_KV(K_(svr), "req_cnt", locate_req_list_.count(), K_(locate_req_list));

    // won't reset of server information
    void reset()
    {
      locate_req_list_.reset();
    }

    void reset(const common::ObAddr &svr)
    {
      svr_ = svr;
      locate_req_list_.reset();
    }

    int push(StartLogIdLocateReq *req)
    {
      return locate_req_list_.push_back(req);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(SvrReq);
  };

  typedef common::ObArray<SvrReq *> SvrReqList;
  typedef common::ObLinearHashMap<common::ObAddr, SvrReq*> SvrReqMap;

  // Data local to each Worker
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

// member variables
private:
  bool              inited_;
  int64_t           worker_cnt_;
  int64_t           locate_count_;
  IObLogRpc         *rpc_;
  IObLogErrHandler  *err_handler_;
  WorkerData        *worker_data_;     // The data belonging to each worker

  // Module Arena dispenser with multi-threaded support
  AllocatorType     allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStartLogIdLocator);
};

typedef common::ObCurTraceId::TraceId TraceIdType;

//////////////////////////// StartLogIdLocateReq ////////////////////////////
///
/// StartLogIdLocateReq
////
//// Request Status.
/// - IDLE: Idle state
/// - REQ: requesting state, result is not readable, external need to ensure request memory validity
/// - DONE: request processing completed, result can be read and reset
struct StartLogIdLocateReq
{
  enum State { IDLE = 0, REQ, DONE };
  struct SvrItem
  {
    common::ObAddr    svr_;
    bool              rpc_executed_;
    int               rpc_err_;
    int               svr_err_;
    int               partition_err_;
    obrpc::BreakInfo  breakinfo_;
    uint64_t          start_log_id_;
    uint64_t          start_log_tstamp_;
    TraceIdType       trace_id_;

    TO_STRING_KV(
        K_(svr),
        K_(rpc_executed),
        K_(rpc_err),
        K_(svr_err),
        K_(partition_err),
        K_(breakinfo),
        K_(start_log_id),
        K_(start_log_tstamp),
        K_(trace_id));

    void reset();
    void reset(const common::ObAddr &svr);

    void set_result(const int rpc_err,
        const int svr_err,
        const int partition_err,
        const obrpc::BreakInfo &breakinfo,
        const uint64_t start_log_id,
        const int64_t start_log_tstamp,
        const TraceIdType *trace_id);
  };
  static const int64_t DEFAULT_SERVER_NUM = 16;
  typedef common::ObSEArray<SvrItem, DEFAULT_SERVER_NUM> SvrList;

  // state
  State                   state_;

  // request parameters
  common::ObPartitionKey  pkey_;
  int64_t                 start_tstamp_;

  // server list
  SvrList                 svr_list_;
  int64_t                 svr_list_consumed_;

  // The server index where the result of a valid request is located, always pointing to the maximum start_log_id-server
  // Invalid value: -1
  int64_t                 result_svr_list_idx_;
  // Record the maximum start_log_id and the corresponding timestamp for the current partition location
  uint64_t                cur_max_start_log_id_;
  int64_t                 cur_max_start_log_tstamp_;
  // Record the number of start_log_id's that have been successfully located at the server
  int64_t                 succ_locate_count_;

  TO_STRING_KV(
      K_(state),
      K_(pkey),
      K_(start_tstamp),
      "svr_cnt", svr_list_.count(),
      K_(svr_list_consumed),
      K_(result_svr_list_idx),
      K_(cur_max_start_log_id),
      K_(cur_max_start_log_tstamp),
      K_(succ_locate_count),
      K_(svr_list));

  void reset();
  void reset(const common::ObPartitionKey &pkey, const int64_t start_tstamp);
  void set_state(const State state) { ATOMIC_STORE(&state_, state); }
  State get_state() const { return (ATOMIC_LOAD(&state_)); }

  void set_state_idle() { ATOMIC_STORE(&state_, IDLE); }
  void set_state_req() { ATOMIC_STORE(&state_, REQ); }
  void set_state_done() { ATOMIC_STORE(&state_, DONE); }
  bool is_state_idle() const { return (ATOMIC_LOAD(&state_)) == IDLE; }
  bool is_state_req() const { return (ATOMIC_LOAD(&state_)) == REQ; }
  bool is_state_done() const { return (ATOMIC_LOAD(&state_)) == DONE; }

  int next_svr_item(SvrItem *&svr_item);
  int cur_svr_item(SvrItem *&svr_item);

  bool is_request_ended(const int64_t locate_count) const;
  bool get_result(uint64_t &start_log_id, common::ObAddr &svr);

  int set_result(const common::ObAddr &svr,
      const int rpc_err,
      const int svr_err,
      const int partition_err,
      const obrpc::BreakInfo &breakinfo,
      const uint64_t start_log_id,
      const int64_t start_log_tstamp,
      const TraceIdType *trace_id);

private:
  void check_locate_result_(const int64_t start_log_tstamp,
      const uint64_t start_log_id,
      const common::ObAddr &svr,
      bool &is_consistent) const;

};

}
}

#endif
