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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_SVR_FINDER_H_
#define OCEANBASE_LIBOBLOG_OB_LOG_SVR_FINDER_H_

#include "lib/utility/ob_print_utils.h" // TO_STRING_KV
#include "lib/oblog/ob_log_module.h"    // K_
#include "lib/atomic/ob_atomic.h"       // ATOMIC_*
#include "lib/net/ob_addr.h"            // ObAddr
#include "lib/profile/ob_trace_id.h"    // ObCurTraceId
#include "common/ob_partition_key.h"    // ObPartitionKey

#include "ob_log_utils.h"               // TS_TO_STR
#include "ob_map_queue_thread.h"        // ObMapQueueThread
#include "ob_log_part_svr_list.h"       // IPartSvrList
#include "ob_log_systable_helper.h"     // IObLogSysTableHelper
#include "ob_log_config.h"              // MAX_SVR_FINDER_THREAD_NUM
#include "ob_log_svr_blacklist.h"       // ObLogSvrBlacklist

namespace oceanbase
{
namespace liboblog
{

struct ISvrFinderReq;
struct SvrFindReq;
struct LeaderFindReq;
class IObLogSvrFinder
{
public:
  virtual ~IObLogSvrFinder() {}

public:
  // Asynchronous request server list
  virtual int async_svr_find_req(SvrFindReq *req) = 0;
  // Asynchronous request leader
  virtual int async_leader_find_req(LeaderFindReq *req) = 0;

  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
};

/////////////////////////////////// ObLogSvrFinder ///////////////////////////////////////

typedef ObMapQueueThread<ObLogConfig::max_svr_finder_thread_num> SvrFinderWorker;

class IObLogErrHandler;
class IObLogAllSvrCache;

class ObLogSvrFinder : public IObLogSvrFinder, public SvrFinderWorker
{
public:
  ObLogSvrFinder();
  virtual ~ObLogSvrFinder();

  // Class global variables
public:
  static int64_t g_sql_batch_count;

public:
  int init(const int64_t thread_num,
      IObLogErrHandler &err_handler,
      IObLogAllSvrCache &all_svr_cache,
      IObLogSysTableHelper &systable_helper);
  void destroy();

public:
  // Asynchronous request server list
  int async_svr_find_req(SvrFindReq *req);

  // Asynchronous request server leader
  int async_leader_find_req(LeaderFindReq *req);

  int start();
  void stop();
  void mark_stop_flag() { SvrFinderWorker::mark_stop_flag(); }

public:
  // Processing data: support batch tasks
  // 1. Batch aggregation of a batch of requests: request heartbeat/request server list, SQL aggregation
  // 2. Initiate batch SQL requests
  // 3. Process return batch results
  virtual void run(const int64_t thread_index);

public:
  void configure(const ObLogConfig &config);

private:
  static const int64_t COND_TIME_WAIT = 1L * 1000L * 1000L;
  static const int64_t DEFAULT_SQL_LENGTH = 1024;
  static const int64_t SQL_TIME_THRESHOLD = 1 * _SEC_;

  typedef common::ObArray<ISvrFinderReq *> SvrFinderReqList;

  // Data local to each Worker
  struct WorkerData
  {
    SvrFinderReqList req_list_;
    IObLogSysTableHelper::BatchSQLQuery query_;

    WorkerData() : req_list_(), query_() {}
    ~WorkerData() { destroy(); }

    int init(const int64_t sql_buf_len);
    void destroy();

    void reset()
    {
      req_list_.reset();
      query_.reset();
    }
  };

  int init_worker_data_(const int64_t thread_num);
  void destory_worker_data_();

private:
  int async_req_(ISvrFinderReq *req);
  int dispatch_worker_(ISvrFinderReq *req);

  // Asynchronous request for bulk acquisition
  int do_retrieve_(const int64_t thread_index, WorkerData &data);
  // SQL agregate
  int do_aggregate_(const int64_t thread_index, WorkerData &data);
  int do_sql_aggregate_for_svr_list_(ISvrFinderReq *orig_req, WorkerData &data);
  // aggregate query for meta_table
  int do_sql_aggregate_for_query_meta_info_(SvrFindReq &req, IObLogSysTableHelper::BatchSQLQuery &query);
  // aggregate query log_history_info_v2
  int do_sql_aggregate_for_query_clog_history_(SvrFindReq &req, IObLogSysTableHelper::BatchSQLQuery &query);
  // aggregate query leader
  int do_sql_aggregate_for_leader_(ISvrFinderReq *orig_req, WorkerData &data);
  // batch query of SQL
  int do_batch_query_(WorkerData &data);
  // Processing bulk search results
  int handle_batch_query_(WorkerData &data);
  int handle_batch_query_for_svr_list_(ISvrFinderReq *req,
      IObLogSysTableHelper::BatchSQLQuery &query);
  int handle_clog_history_info_records_(SvrFindReq &req,
      const IObLogSysTableHelper::ClogHistoryRecordArray &records);
  int handle_meta_info_records_(SvrFindReq &req,
      const IObLogSysTableHelper::MetaRecordArray &records);
  int handle_batch_query_for_leader_(ISvrFinderReq *req,
      IObLogSysTableHelper::BatchSQLQuery &query);
  // reset
  void reset_all_req_(const int err_code, WorkerData &data);

// private data members
private:
  bool                  inited_;
  int64_t               thread_num_;
  IObLogErrHandler      *err_handler_;
  IObLogAllSvrCache     *all_svr_cache_;
  IObLogSysTableHelper  *systable_helper_;
  WorkerData            *worker_data_;
  ObLogSvrBlacklist     svr_blacklist_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogSvrFinder);
};

////////////////////////////////// request struct //////////////////////////////

typedef common::ObCurTraceId::TraceId TraceIdType;
/*
 * Base class for request
 * Request status:
 * - IDLE: Idle state
 * - REQ: requesting state, result is not readable, external need to ensure request memory validity
 * - DONE: request processing completed, result can be read and reset
 */
struct ISvrFinderReq
{
  enum State { IDLE = 0, REQ, DONE };

  // request type
  enum Type
  {
    REQ_UNKNOWN = 0,
    REQ_FIND_SVR = 1,           // request server list
    REQ_FIND_LEADER = 2,        // request leader
  };

  int           type_;
  int           state_;
  int           err_code_;
  int           mysql_err_code_;
  TraceIdType   trace_id_;

  explicit ISvrFinderReq(Type type) : type_(type) { reset(); }
  virtual ~ISvrFinderReq() { reset(); }

  // Note: reset does not modify type
  void reset()
  {
    state_ = IDLE;
    err_code_ = 0;
    mysql_err_code_ = 0;
    trace_id_.reset();
  }

  virtual uint64_t hash() const = 0;

  void set_state_idle() { ATOMIC_STORE(&state_, IDLE); }
  void set_state_req() { ATOMIC_STORE(&state_, REQ); }
  void set_state_done(const int err_code, const int mysql_err_code, const TraceIdType *trace_id)
  {
    ATOMIC_STORE(&err_code_, err_code);
    ATOMIC_STORE(&mysql_err_code_, mysql_err_code);
    ATOMIC_STORE(&state_, DONE);
    if (NULL != trace_id) {
      trace_id_ = *trace_id;
    } else {
      trace_id_.reset();
    }
  }

  int get_err_code() const { return ATOMIC_LOAD(&err_code_); }
  int get_mysql_err_code() const { return ATOMIC_LOAD(&mysql_err_code_); }
  int get_state() const { return (ATOMIC_LOAD(&state_)); }

  bool is_state_idle() const { return (ATOMIC_LOAD(&state_)) == IDLE; }
  bool is_state_req() const { return (ATOMIC_LOAD(&state_)) == REQ; }
  bool is_state_done() const { return (ATOMIC_LOAD(&state_)) == DONE; }

  bool is_find_svr_req() const { return REQ_FIND_SVR == type_; }
  bool is_find_leader_req() const { return REQ_FIND_LEADER == type_; }

  static const char *print_state(const int state);
  static const char *print_type(const int type);

  TO_STRING_KV("type", print_type(type_),
      "state", print_state(state_),
      K_(err_code),
      K_(mysql_err_code),
      K_(trace_id));
};

// Request structure for requesting Server lists
struct SvrFindReq : public ISvrFinderReq
{
  // request paraments
  common::ObPartitionKey  pkey_;
  int64_t                 start_tstamp_;
  uint64_t                next_log_id_;
  bool                    req_by_start_tstamp_;
  bool                    req_by_next_log_id_;

  // request result
  IPartSvrList            *svr_list_;

  SvrFindReq() : ISvrFinderReq(REQ_FIND_SVR) { reset(); }
  virtual ~SvrFindReq() { reset(); }

  void reset();
  virtual uint64_t hash() const;

  bool is_valid() const;

  // Rest for timestamp-based request server list
  void reset_for_req_by_tstamp(IPartSvrList &svr_list,
      const common::ObPartitionKey &pkey,
      const int64_t tstamp);

  // Reset for log id based request server list
  void reset_for_req_by_log_id(IPartSvrList &svr_list,
      const common::ObPartitionKey &pkey,
      const uint64_t id);

  TO_STRING_KV("base", (ISvrFinderReq&)(*this),
      K_(pkey),
      K_(req_by_start_tstamp),
      "start_tstamp", TS_TO_STR(start_tstamp_),
      K_(req_by_next_log_id),
      K_(next_log_id),
      KPC_(svr_list));
};

// Request structure for requesting Leader information
struct LeaderFindReq : public ISvrFinderReq
{
  // request paraments
  common::ObPartitionKey  pkey_;

  // request result
  bool                    has_leader_;
  common::ObAddr          leader_;

  LeaderFindReq() : ISvrFinderReq(REQ_FIND_LEADER) { reset(); }
  virtual ~LeaderFindReq() { reset(); }

  virtual uint64_t hash() const
  {
    return pkey_.hash();
  }

  void reset()
  {
    ISvrFinderReq::reset();

    pkey_.reset();
    has_leader_ = false;
    leader_.reset();
  }

  void reset(const common::ObPartitionKey &pkey)
  {
    reset();
    pkey_ = pkey;
  }

  TO_STRING_KV("base", (ISvrFinderReq&)(*this),
      K_(pkey),
      K_(has_leader),
      K_(leader));
};

}
}

#endif
