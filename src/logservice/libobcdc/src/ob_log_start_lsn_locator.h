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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_START_LOG_ID_LOCATOR_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_START_LOG_ID_LOCATOR_H_

#include "lib/atomic/ob_atomic.h"         // ATOMIC_*
#include "lib/net/ob_addr.h"              // ObAddr
#include "lib/container/ob_array.h"       // ObArray
#include "lib/hash/ob_linear_hash_map.h"  // ObLinearHashMap
#include "lib/utility/ob_print_utils.h"   // TO_STRING_KV
#include "lib/allocator/ob_safe_arena.h"  // ObSafeArena
#include "lib/profile/ob_trace_id.h"      // ObCurTraceId
#include "logservice/cdcservice/ob_cdc_req.h"
#include "ob_log_fetching_mode.h"
#include "logservice/common_util/ob_log_ls_define.h"

#include "ob_map_queue_thread.h"          // ObMapQueueThread
#include "ob_log_config.h"                // ObLogConfig
#include "ob_log_utils.h"                 // _SEC_

namespace oceanbase
{
namespace libobcdc
{

struct StartLSNLocateReq;
class IObLogStartLSNLocator
{
public:
  virtual ~IObLogStartLSNLocator() {}

public:
  virtual int async_start_lsn_req(StartLSNLocateReq *req) = 0;

  virtual int  start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
};

//////////////////////////// ObLogStartLSNLocator ////////////////////////////

typedef common::ObMapQueueThread<ObLogConfig::max_start_lsn_locator_thread_num> LocateWorker;

class IObLogRpc;
class IObLogErrHandler;
class ObLogStartLSNLocator : public IObLogStartLSNLocator, public LocateWorker
{
  static const int64_t DATA_OP_TIMEOUT = 100 * _MSEC_;
  typedef obrpc::ObCdcReqStartLSNByTsReq RpcReq;
  typedef obrpc::ObCdcReqStartLSNByTsResp RpcRes;
  typedef common::ObSafeArena AllocatorType;

  // Class member variables
public:
  static int64_t g_batch_count;
  static int64_t g_rpc_timeout;

public:
  ObLogStartLSNLocator();
  virtual ~ObLogStartLSNLocator();

  int init(
      const int64_t worker_cnt,
      const int64_t locate_count,
      const ClientFetchingMode fetching_mode,
      const share::ObBackupPathString &archive_dest_str,
      IObLogRpc &rpc,
      IObLogErrHandler &err_handle);
  void destroy();

public:
  int async_start_lsn_req(StartLSNLocateReq *req);
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
  int dispatch_worker_(StartLSNLocateReq *req);
  int do_retrieve_(const int64_t thread_index, WorkerData &data);
  int get_svr_req_(WorkerData &data,
      const uint64_t tenant_id,
      common::ObAddr &svr,
      SvrReq *&svr_list);
  SvrReq *alloc_svr_req_(
      const uint64_t tenant_id,
      const common::ObAddr &svr);
  void free_svr_req_(SvrReq *req);
  void free_all_svr_req_(WorkerData &data);
  int do_request_(WorkerData &data);
  int do_integrated_request_(WorkerData &data);
  int do_direct_request_(WorkerData &data);
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
  struct TenantSvr
  {
  public:
    TenantSvr(const uint64_t tenant_id, const common::ObAddr &request_svr) :
      tenant_id_(tenant_id), request_svr_(request_svr)
    {}

    bool operator==(const TenantSvr &other) const
    {
      return tenant_id_ == other.tenant_id_ && request_svr_ == other.request_svr_;
    }

    bool operator!=(const TenantSvr &other) const
    {
      return !operator==(other);
    }

    uint64_t hash() const
    {
      uint64_t hash_val = 0;
      uint64_t svr_hash = request_svr_.hash();
      hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
      hash_val = common::murmurhash(&svr_hash, sizeof(svr_hash), hash_val);

      return hash_val;
    }

    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }

    TO_STRING_KV(K_(tenant_id), K_(request_svr));

  private:
    uint64_t tenant_id_;
    common::ObAddr request_svr_;   // server that execute locate_start_lsn rpc
  };

  // Requests from a single server and tenant
  struct SvrReq
  {
    typedef common::ObArray<StartLSNLocateReq*> ReqList;

    uint64_t        tenant_id_;
    common::ObAddr  svr_;
    ReqList         locate_req_list_;

    SvrReq(const uint64_t tenant_id, const common::ObAddr &svr)
      : tenant_id_(tenant_id), svr_(svr), locate_req_list_()
    {
    }
    ~SvrReq() { reset(); }

    void reset()
    {
      tenant_id_ = common::OB_INVALID_TENANT_ID;
      svr_.reset();
      locate_req_list_.reset();
    }

    int push(StartLSNLocateReq *req)
    {
      return locate_req_list_.push_back(req);
    }

    TO_STRING_KV(
        K_(tenant_id),
        K_(svr),
        "req_cnt", locate_req_list_.count(),
        K_(locate_req_list));

  private:
    DISALLOW_COPY_AND_ASSIGN(SvrReq);
  };

  typedef common::ObArray<SvrReq *> SvrReqList;
  typedef common::ObLinearHashMap<TenantSvr, SvrReq*> SvrReqMap;
  typedef common::ObArray<StartLSNLocateReq*> DirectReqList;

  // Data local to each Worker
  struct WorkerData
  {
    SvrReqList svr_req_list_;
    SvrReqMap  svr_req_map_;
    DirectReqList archive_req_list_;

    WorkerData() : svr_req_list_(), svr_req_map_(), archive_req_list_()
    {}

    ~WorkerData() { destroy(); }

    int init();
    void destroy();
    void reset()
    {
      svr_req_list_.reset();
      svr_req_map_.reset();
      archive_req_list_.reset();
    }
    bool has_valid_req() const
    {
      return svr_req_list_.count() > 0 || archive_req_list_.count() > 0;
    }
  };

// member variables
private:
  bool              is_inited_;
  share::ObBackupDest archive_dest_;
  int64_t           worker_cnt_;
  int64_t           locate_count_;
  IObLogRpc         *rpc_;
  IObLogErrHandler  *err_handler_;
  WorkerData        *worker_data_;     // The data belonging to each worker

  // Module Arena dispenser with multi-threaded support
  AllocatorType     allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStartLSNLocator);
};

typedef common::ObCurTraceId::TraceId TraceIdType;

//////////////////////////// StartLSNLocateReq ////////////////////////////
///
/// StartLSNLocateReq
////
//// Request Status.
/// - IDLE: Idle state
/// - REQ: requesting state, result is not readable, external need to ensure request memory validity
/// - DONE: request processing completed, result can be read and reset
struct StartLSNLocateReq
{
  enum State { IDLE = 0, REQ, DONE };
  struct DirectLocateResult
  {
    palf::LSN         start_lsn_;
    int               loc_err_;
    TO_STRING_KV(K_(start_lsn), K_(loc_err));
    void reset() {
      start_lsn_.reset();
      loc_err_ = OB_SUCCESS;
    }

    void reset(const palf::LSN &start_lsn, const int err) {
      start_lsn_ = start_lsn;
      loc_err_ = err;
    }
  };
  struct SvrItem
  {
    common::ObAddr    svr_;
    bool              rpc_executed_;
    int               rpc_err_;
    int               svr_err_;
    int               ls_err_;
    palf::LSN         start_lsn_;
    uint64_t          start_log_tstamp_;
    TraceIdType       trace_id_;

    TO_STRING_KV(
        K_(svr),
        K_(rpc_executed),
        K_(rpc_err),
        K_(svr_err),
        K_(ls_err),
        K_(start_lsn),
        K_(start_log_tstamp),
        K_(trace_id));

    void reset();
    void reset(const common::ObAddr &request_svr);

    void set_result(const int rpc_err,
        const int svr_err,
        const int ls_err,
        const palf::LSN &start_lsn,
        const int64_t start_log_tstamp,
        const TraceIdType *trace_id);
  };
  static const int64_t DEFAULT_SERVER_NUM = 16;
  typedef common::ObSEArray<SvrItem, DEFAULT_SERVER_NUM> SvrList;

  // state
  State                   state_;

  // request parameters
  logservice::TenantLSID  tls_id_;
  int64_t                 start_tstamp_ns_;

  // server list
  SvrList                 svr_list_;
  int64_t                 svr_list_consumed_;

  // The server index where the result of a valid request is located, always pointing to the maximum start_lsn-server
  // Invalid value: -1
  int64_t                 result_svr_list_idx_;
  // Record the maximum start_lsn and the corresponding timestamp for the current LS location
  palf::LSN               cur_max_start_lsn_;
  int64_t                 cur_max_start_log_tstamp_;
  // Record the number of start_lsn that have been successfully located at the server
  int64_t                 succ_locate_count_;
  ClientFetchingMode      fetching_mode_;
  DirectLocateResult      archive_locate_rs_;

  TO_STRING_KV(
      K_(state),
      K_(tls_id),
      K_(start_tstamp_ns),
      "svr_cnt", svr_list_.count(),
      K_(svr_list_consumed),
      K_(result_svr_list_idx),
      K_(cur_max_start_lsn),
      K_(cur_max_start_log_tstamp),
      K_(succ_locate_count),
      K_(svr_list),
      "fetching_mode", print_fetching_mode(fetching_mode_),
      K_(archive_locate_rs));

  void reset();
  void reset(
      const logservice::TenantLSID &tls_id,
      const int64_t start_tstamp_ns);
  void set_state(const State state) { ATOMIC_STORE(&state_, state); }
  State get_state() const { return (ATOMIC_LOAD(&state_)); }

  void set_state_idle() { ATOMIC_STORE(&state_, IDLE); }
  void set_state_req() { ATOMIC_STORE(&state_, REQ); }
  void set_state_done() { ATOMIC_STORE(&state_, DONE); }
  bool is_state_idle() const { return (ATOMIC_LOAD(&state_)) == IDLE; }
  bool is_state_req() const { return (ATOMIC_LOAD(&state_)) == REQ; }
  bool is_state_done() const { return (ATOMIC_LOAD(&state_)) == DONE; }

  void set_fetching_mode(const ClientFetchingMode mode) { fetching_mode_ = mode; }
  ClientFetchingMode get_fetching_mode() const { return fetching_mode_; }

  int next_svr_item(SvrItem *&svr_item);
  int cur_svr_item(SvrItem *&svr_item);

  bool is_request_ended(const int64_t locate_count) const;
  bool get_result(palf::LSN &start_lsn, common::ObAddr &svr);
  void get_direct_result(palf::LSN &start_lsn, int &err);

  int set_result(const common::ObAddr &svr,
      const int rpc_err,
      const int svr_err,
      const int ls_err,
      const palf::LSN &start_lsn,
      const int64_t start_log_tstamp,
      const TraceIdType *trace_id);

private:
  void check_locate_result_(const int64_t start_log_tstamp,
      const palf::LSN &start_lsn,
      const common::ObAddr &svr,
      bool &is_consistent) const;

};

}
}

#endif
