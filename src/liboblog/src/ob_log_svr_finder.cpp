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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_svr_finder.h"

#include "lib/hash_func/murmur_hash.h"  // murmurhash

#include "ob_log_trace_id.h"            // ObLogTraceIdGuard
#include "ob_log_instance.h"            // IObLogErrHandler
#include "ob_log_all_svr_cache.h"       // IObLogAllSvrCache
#include "ob_log_server_priority.h"     // RegionPriority, ReplicaPriority

namespace oceanbase
{
using namespace common;
namespace liboblog
{
int64_t ObLogSvrFinder::g_sql_batch_count = ObLogConfig::default_svr_finder_sql_batch_count;

ObLogSvrFinder::ObLogSvrFinder() :
    inited_(false),
    thread_num_(0),
    err_handler_(NULL),
    all_svr_cache_(NULL),
    systable_helper_(NULL),
    worker_data_(NULL),
    svr_blacklist_()
{
}

ObLogSvrFinder::~ObLogSvrFinder()
{
  destroy();
}

int ObLogSvrFinder::init(const int64_t thread_num,
    IObLogErrHandler &err_handler,
    IObLogAllSvrCache &all_svr_cache,
    IObLogSysTableHelper &systable_helper)
{
  int ret = OB_SUCCESS;
  int64_t max_thread_num = ObLogConfig::max_svr_finder_thread_num;
  const char *server_blacklist = TCONF.server_blacklist.str();
  const bool is_sql_server = false;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if ((OB_UNLIKELY(thread_num) <= 0) || OB_UNLIKELY(max_thread_num < thread_num)) {
    LOG_ERROR("invalid thread number", K(thread_num), K(max_thread_num));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(SvrFinderWorker::init(thread_num, ObModIds::OB_LOG_FETCHER_SVR_FINDER))) {
    LOG_ERROR("init svr finder worker fail", KR(ret), K(thread_num));
  } else if (OB_FAIL(init_worker_data_(thread_num))) {
    LOG_ERROR("init_worker_data_ fail", KR(ret), K(thread_num));
  } else if (OB_FAIL(svr_blacklist_.init(server_blacklist, is_sql_server))) {
    LOG_ERROR("svr_blacklist_ init fail", KR(ret), K(server_blacklist), K(is_sql_server));
  } else {
    thread_num_ = thread_num;
    err_handler_ = &err_handler;
    all_svr_cache_ = &all_svr_cache;
    systable_helper_ = &systable_helper;
    inited_ = true;

    LOG_INFO("init svr finder succ", K(thread_num));
  }
  return ret;
}

void ObLogSvrFinder::destroy()
{
  inited_ = false;

  thread_num_ = 0;
  SvrFinderWorker::destroy();
  err_handler_ = NULL;
  all_svr_cache_ = NULL;
  systable_helper_ = NULL;
  destory_worker_data_();
  svr_blacklist_.destroy();

  LOG_INFO("destroy svr finder succ");
}

int ObLogSvrFinder::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(SvrFinderWorker::start())) {
    LOG_ERROR("start svr finder worker fail", KR(ret));
  } else {
    LOG_INFO("start svr finder succ", "thread_num", SvrFinderWorker::get_thread_num());
  }
  return ret;
}

void ObLogSvrFinder::stop()
{
  if (OB_LIKELY(inited_)) {
    SvrFinderWorker::stop();
    LOG_INFO("stop svr finder succ");
  }
}

int ObLogSvrFinder::async_svr_find_req(SvrFindReq *req)
{
  return async_req_(req);
}

int ObLogSvrFinder::async_leader_find_req(LeaderFindReq *req)
{
  return async_req_(req);
}

int ObLogSvrFinder::async_req_(ISvrFinderReq *req)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req)) {
    LOG_ERROR("invalid argument", K(req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! req->is_state_idle())) {
    LOG_ERROR("invalid request, state is not IDLE", KPC(req));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Setting the status to request status
    req->set_state_req();

    if (OB_FAIL(dispatch_worker_(req))) {
      LOG_ERROR("dispatch worker fail", KR(ret), KPC(req));
    }

    LOG_DEBUG("svr finder handle async req", KR(ret), KPC(req));
  }
  return ret;
}

int ObLogSvrFinder::dispatch_worker_(ISvrFinderReq *req)
{
  // No special err.
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req)) {
    LOG_ERROR("invalid argument", K(req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! req->is_state_req())) {
    LOG_ERROR("invalid request, state is not REQ", KPC(req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(SvrFinderWorker::push(req, req->hash()))) {
    LOG_ERROR("push request fail", KR(ret), KPC(req), K(req->hash()));
  } else {
    // dispatch worker succ
  }
  return ret;
}

void ObLogSvrFinder::run(const int64_t thread_index)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(thread_index < 0) || OB_UNLIKELY(thread_index >= thread_num_)) {
    LIB_LOG(ERROR, "invalid thread index", K(thread_index), K(thread_num_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    WorkerData &data = worker_data_[thread_index];

    LOG_INFO("svr finder thread start", K(thread_index), K_(thread_num));

    while (! stop_flag_ && OB_SUCCESS == ret) {
      // query with different Trace ID for each request
      ObLogTraceIdGuard trace_guard;

      if (OB_FAIL(do_retrieve_(thread_index, data))) {
        if (OB_ITER_END != ret) {
          LOG_ERROR("retrieve request fail", KR(ret), K(thread_index));
        }
      } else if (OB_FAIL(do_aggregate_(thread_index, data))) {
        LOG_ERROR("do_aggregate_ fail", KR(ret), K(thread_index));
      } else if (OB_FAIL(do_batch_query_(data))) {
        if (OB_NEED_RETRY != ret) {
          LOG_ERROR("do_batch_query_ fail", KR(ret), K(thread_index));
        }
      }
      // handle query result
      else if (OB_FAIL(handle_batch_query_(data))) {
        if (OB_NEED_RETRY != ret) {
          LOG_ERROR("handle_batch_query_ fail", KR(ret), K(thread_index));
        }
      } else {
        // do nothing
      }

      // No requests to process, waiting
      if (OB_ITER_END == ret) {
        cond_timedwait(thread_index, COND_TIME_WAIT);
        ret = OB_SUCCESS;
      } else {
        // Reset whether successful or not and mark outstanding requests as completed
        reset_all_req_(ret, data);

        if (OB_NEED_RETRY == ret) {
          ret = OB_SUCCESS;
          // If this query or processing fails, the connection is reset to ensure that the next query or processing is done with a different server
          if (OB_FAIL(systable_helper_->reset_connection())) {
            LOG_ERROR("reset_connection fail", KR(ret), K(thread_index));
          }
        }
      }
    } // while

    if (stop_flag_) {
      ret = OB_IN_STOP_STATE;
    }
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
    LOG_ERROR("svr finder worker exit on fail", KR(ret), K(thread_index));
    if (OB_NOT_NULL(err_handler_)) {
      err_handler_->handle_error(ret, "SvrFinderWorker worker exits on fail, ret=%d, thread_index=%ld",
          ret, thread_index);
    }
  }
}

void ObLogSvrFinder::configure(const ObLogConfig &config)
{
  int64_t svr_finder_sql_batch_count = config.svr_finder_sql_batch_count;
  const char *server_blacklist = config.server_blacklist.str();

  ATOMIC_STORE(&g_sql_batch_count, svr_finder_sql_batch_count);
  svr_blacklist_.refresh(server_blacklist);

  LOG_INFO("[CONFIG]", K(svr_finder_sql_batch_count));
  LOG_INFO("[CONFIG]", K(server_blacklist));
}

// OB_ITER_END: no request available
int ObLogSvrFinder::do_retrieve_(const int64_t thread_index, WorkerData &worker_data)
{
  int ret = OB_SUCCESS;
  int64_t sql_batch_count = ATOMIC_LOAD(&g_sql_batch_count);
  // Each request corresponds to two SQL
  int64_t req_batch_count = (sql_batch_count/2);
  req_batch_count = req_batch_count > 0 ? req_batch_count : 1;    // Guaranteed greater than 0
  SvrFinderReqList &req_list = worker_data.req_list_;

  // Fetching data from queues, batch processing
  for (int64_t cnt = 0; OB_SUCCESS == ret && (cnt < req_batch_count); ++cnt) {
    void *data = NULL;
    ISvrFinderReq *request = NULL;

    if (OB_FAIL(SvrFinderWorker::pop(thread_index, data))) {
      if (OB_EAGAIN != ret) {
        LOG_ERROR("pop data from queue fail", KR(ret), K(thread_index), K(data));
      }
    } else if (OB_ISNULL(request = static_cast<ISvrFinderReq *>(data))) {
      LOG_ERROR("request is NULL", K(request), K(thread_index), K(data));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(req_list.push_back(request))) {
      LOG_ERROR("req_list push_back fail", KR(ret), KPC(request));
    } else {
      // success
    }
  }

  if (OB_EAGAIN == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCCESS == ret) {
    // No requests pending, set iteration termination directly
    if (req_list.count() <= 0) {
      ret = OB_ITER_END;
    }
  } else {
    LOG_ERROR("pop and aggregate request fail", KR(ret), K(thread_index));
  }

  LOG_DEBUG("svr finder do_retrieve done", KR(ret), K(thread_index),
      "req_count", req_list.count(), K(req_batch_count), K(sql_batch_count));

  return ret;
}

int ObLogSvrFinder::do_aggregate_(const int64_t thread_index, WorkerData &worker_data)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else {
    SvrFinderReqList &req_list = worker_data.req_list_;

    LOG_DEBUG("svr finder do_aggregate", K(thread_index), "req_count", req_list.count());

    for (int64_t idx = 0; OB_SUCCESS == ret && idx < req_list.count(); ++idx) {
      ISvrFinderReq *req = req_list.at(idx);

      if (OB_ISNULL(req)) {
        LOG_ERROR("req is null", K(thread_index), KPC(req));
        ret = OB_ERR_UNEXPECTED;
      } else {
        if (req->is_find_svr_req()) {
         if (OB_FAIL(do_sql_aggregate_for_svr_list_(req, worker_data))) {
           LOG_ERROR("do_sql_aggregate_for_svr_list_ fail", KR(ret), KPC(req));
         }
        } else if (req->is_find_leader_req()) {
         if (OB_FAIL(do_sql_aggregate_for_leader_(req, worker_data))) {
           LOG_ERROR("do_sql_aggregate_for_leader_ fail", KR(ret), KPC(req));
         }
        } else {
          LOG_ERROR("invalid request, unknown type", KPC(req));
          ret = OB_INVALID_ARGUMENT;
        }
      }
    } // for

    // print count of aggregated request
    if (req_list.count() > 0) {
      LOG_DEBUG("svr finder do_aggregate_ succ", K(thread_index), "cnt", req_list.count());
    }
  }

  return ret;
}

int ObLogSvrFinder::do_batch_query_(WorkerData &worker_data)
{
  int ret = OB_SUCCESS;
  IObLogSysTableHelper::BatchSQLQuery &query = worker_data.query_;
  SvrFinderReqList &req_list = worker_data.req_list_;
  int64_t req_list_cnt = req_list.count();

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (req_list_cnt <=0) {
    // no request, do nothing
  } else {
    int64_t start_ts = get_timestamp();
    if (OB_FAIL(systable_helper_->query_with_multiple_statement(query))) {
      if (OB_NEED_RETRY == ret) {
        LOG_WARN("query_with_multiple_statement fail, need retry", KR(ret),
            "svr", query.get_server(),
            "mysql_error_code", query.get_mysql_err_code(),
            "mysql_error_msg", query.get_mysql_err_msg());
      } else {
        LOG_ERROR("query_with_multiple_statement fail", KR(ret),
            "svr", query.get_server(),
            "mysql_error_code", query.get_mysql_err_code(),
            "mysql_error_msg", query.get_mysql_err_msg());
      }
    } else {
    }

    int64_t end_ts = get_timestamp();
    if (end_ts - start_ts > SQL_TIME_THRESHOLD) {
      LOG_WARN("SvrFinder do_batch_query cost too much time", "cost", end_ts - start_ts,
          KR(ret), "svr", query.get_server(), K(req_list_cnt));
    }

    LOG_DEBUG("svr finder do_batch_query done", KR(ret), "cost", end_ts - start_ts,
        "svr", query.get_server(), K(req_list_cnt));
  }

  return ret;
}

void ObLogSvrFinder::reset_all_req_(const int err_code, WorkerData &data)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else {
    TraceIdType *trace_id = ObCurTraceId::get_trace_id();

    // If one SQL fails, all subsequent SQL is marked as failed
    // TODO: If only one SQL fails, the subsequent SQL is retried in place
    SvrFinderReqList &req_list = data.req_list_;
    // Get mysql error for the last SQL error
    int mysql_err_code = data.query_.get_mysql_err_code();

    for (int64_t idx = 0; OB_SUCCESS == ret && idx < req_list.count(); ++idx) {
      ISvrFinderReq *req = req_list.at(idx);

      // End the request, regardless of whether it was processed successfully or not, and set the error code
      // Note: the request may end early, here the req may be NULL
      if (NULL != req) {
        req->set_state_done(err_code, mysql_err_code, trace_id);
      }
    }
    // Reset local data (including BatchSQLQuery and reset of req_list) after all requests have been processed
    data.reset();
  }
}

int ObLogSvrFinder::handle_batch_query_(WorkerData &data)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else {
    IObLogSysTableHelper::BatchSQLQuery &query = data.query_;
    SvrFinderReqList &req_list = data.req_list_;
    int64_t req_idx = 0;
    int64_t start_time = get_timestamp();
    int64_t handle_time = 0;
    TraceIdType *trace_id = ObCurTraceId::get_trace_id();

    // Iterate through all requests and exit if a particular request is encountered and fails
    for (req_idx = 0; OB_SUCCESS == ret && req_idx < req_list.count(); ++req_idx) {
      ISvrFinderReq *req = req_list.at(req_idx);

      if (OB_ISNULL(req)) {
        LOG_ERROR("req is null", KPC(req));
        ret = OB_ERR_UNEXPECTED;
      } else {
        if (req->is_find_svr_req()) {
          if (OB_FAIL(handle_batch_query_for_svr_list_(req, query))) {
            if (OB_NEED_RETRY != ret) {
              LOG_ERROR("handle_batch_query_for_svr_list_ fail", KR(ret), KPC(req));
            }
          }
        } else if (req->is_find_leader_req()) {
          if (OB_FAIL(handle_batch_query_for_leader_(req, query))) {
            if (OB_NEED_RETRY != ret) {
              LOG_ERROR("handle_batch_query_for_leader_ fail", KR(ret), KPC(req));
            }
          }
        } else {
          LOG_ERROR("invalid request, unknown type", KPC(req));
          ret = OB_INVALID_ARGUMENT;
        }

        // The end is set immediately, whether successful or not
        req->set_state_done(ret, query.get_mysql_err_code(), trace_id);

        // Finally mark the request as empty and no further references may be made
        req = NULL;
        req_list[req_idx] = NULL;
      }
    }

    handle_time = get_timestamp() - start_time;

    LOG_DEBUG("svr finder handle_batch_query done", KR(ret), "cost", handle_time,
            "svr", query.get_server(), "batch_req_count", req_list.count(),
            "batch_sql_count", query.get_batch_sql_count());

    if (OB_SUCCESS != ret) {
      LOG_ERROR("SvrFinder handle_batch_query fail", KR(ret),
          "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg(),
          "fail_req_idx", req_idx - 1,
          "batch_req_count", req_list.count(),
          "batch_sql_count", query.get_batch_sql_count());

      // For the rest of the requests, regardless of success, mark completion directly
      // FIXME: where a request SQL fails, all subsequent unexecuted SQL will be marked as failed
      for (; req_idx < req_list.count(); ++req_idx) {
        if (NULL != req_list[req_idx]) {
          req_list[req_idx]->set_state_done(ret, query.get_mysql_err_code(), trace_id);
          req_list[req_idx] = NULL;
        }
      }
    } else {
      // Execution takes too long, prints WARN log
      if (handle_time > SQL_TIME_THRESHOLD) {
        LOG_WARN("SvrFinder handle_batch_query cost too much time", "cost", handle_time,
            "svr", query.get_server(), "batch_req_count", req_list.count(),
            "batch_sql_count", query.get_batch_sql_count());
      }
    }

    // Check here that all requests are processed to avoid starving some requests after the reset
    if (OB_UNLIKELY(req_idx < req_list.count())) {
      LOG_ERROR("some requests have not been handled, unexpected error", K(req_idx),
          K(req_list.count()), KR(ret), K(req_list));
      ret = OB_ERR_UNEXPECTED;
    }

    // Finally the request list is reset anyway and the external no longer needs to be traversed
    req_list.reset();
  }

  return ret;
}

int ObLogSvrFinder::handle_batch_query_for_svr_list_(ISvrFinderReq *orig_req,
    IObLogSysTableHelper::BatchSQLQuery &query)
{
  int ret = OB_SUCCESS;
  SvrFindReq *req = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(orig_req)) {
    LOG_ERROR("invalid argument", K(orig_req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! orig_req->is_state_req())) {
    LOG_ERROR("invalid request, state is not REQ", KR(ret), KPC(orig_req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! orig_req->is_find_svr_req())) {
    LOG_ERROR("invalid request, type is not REQ_FIND_SVR", KPC(orig_req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(req = static_cast<SvrFindReq *>(orig_req))) {
    LOG_ERROR("SvrFindReq is null", K(req));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // Process clog_history_records first, then meta_records according to SQL aggregation order
    IObLogSysTableHelper::ClogHistoryRecordArray clog_history_records(common::ObModIds::OB_LOG_CLOG_HISTORY_RECORD_ARRAY, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
    clog_history_records.reset();
    IObLogSysTableHelper::MetaRecordArray meta_records(common::ObModIds::OB_LOG_META_RECORD_ARRAY, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
    meta_records.reset();

    if (OB_FAIL(query.get_records(clog_history_records))) {
      // OB_NEED_RETRY indicates that a retry is required
      LOG_WARN("get_records fail for query clog history records", KR(ret),
          "svr", query.get_server(), "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg(), KPC(req));
    } else if (OB_FAIL(handle_clog_history_info_records_(*req, clog_history_records))) {
      LOG_ERROR("handle clog history info records fail", KR(ret), KPC(req), K(clog_history_records));
    } else {
      // succ
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(query.get_records(meta_records))) {
        // OB_NEED_RETRY indicates that a retry is required
        LOG_WARN("get_records fail for query meta table records", KR(ret),
            "svr", query.get_server(), "mysql_error_code", query.get_mysql_err_code(),
            "mysql_error_msg", query.get_mysql_err_msg(), KPC(req));
      } else if (OB_FAIL(handle_meta_info_records_(*req, meta_records))) {
        LOG_ERROR("handle meta info records fail", KR(ret), KPC(req), K(meta_records));
      } else {
        // succ
      }
    }

    LOG_DEBUG("svr finder get_records for query svr_list done", KR(ret), KPC(req));
  }

  return ret;
}

int ObLogSvrFinder::handle_batch_query_for_leader_(ISvrFinderReq *orig_req,
    IObLogSysTableHelper::BatchSQLQuery &query)
{
  int ret = OB_SUCCESS;
  LeaderFindReq *req = NULL;

  if (OB_ISNULL(orig_req)) {
    LOG_ERROR("invalid argument", K(orig_req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! orig_req->is_find_leader_req())) {
    LOG_ERROR("invalid request, type is not REQ_FIND_LEADER", KPC(orig_req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! orig_req->is_state_req())) {
    LOG_ERROR("invalid request, state is not REQ", KPC(orig_req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(req = static_cast<LeaderFindReq *>(orig_req))) {
    LOG_ERROR("dynamic cast from ISvrFinderReq to LeaderFindReq fail", K(orig_req), KPC(orig_req));
    ret = OB_ERR_UNEXPECTED;
  } else {
    req->has_leader_ = false;
    req->leader_.reset();

    if (OB_FAIL(query.get_records(req->has_leader_, req->leader_))) {
      LOG_ERROR("query leader info fail", KR(ret), "pkey", req->pkey_,
          "svr", query.get_server(), "mysql_error_code", query.get_mysql_err_code(),
          "mysql_error_msg", query.get_mysql_err_msg());
    }

    LOG_DEBUG("svr finder get_records for query leader done", KR(ret), KPC(req));
  }

  return ret;
}

int ObLogSvrFinder::do_sql_aggregate_for_svr_list_(ISvrFinderReq *orig_req, WorkerData &data)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(orig_req)) {
    LOG_ERROR("invalid argument", K(orig_req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! orig_req->is_state_req())) {
    LOG_ERROR("invalid request, state is not REQ", KR(ret), KPC(orig_req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! orig_req->is_find_svr_req())) {
    LOG_ERROR("invalid request, type is not REQ_FIND_SVR", KPC(orig_req));
    ret = OB_INVALID_ARGUMENT;
  } else {
    SvrFindReq *req = static_cast<SvrFindReq *>(orig_req);
    IObLogSysTableHelper::BatchSQLQuery &query = data.query_;

    if (OB_ISNULL(req)) {
      LOG_ERROR("dynamic cast from ISvrFinderReq to SvrFindReq fail", K(orig_req), KPC(orig_req));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(do_sql_aggregate_for_query_clog_history_(*req, query))) {
      LOG_ERROR("do_sql_aggregate_for_query_clog_history_ fail", KR(ret), KPC(req));
    } else if (OB_FAIL(do_sql_aggregate_for_query_meta_info_(*req, query))) {
      LOG_ERROR("do_sql_aggregate_for_query_meta_info_ fail", KR(ret), KPC(req));
    } else {
      LOG_DEBUG("svr finder do_aggregate for svr_list req", KPC(req));
    }
  }

  return ret;
}

int ObLogSvrFinder::do_sql_aggregate_for_query_clog_history_(SvrFindReq &req, IObLogSysTableHelper::BatchSQLQuery &query)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! req.is_valid())) {
    LOG_ERROR("invalid argument", K(req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(systable_helper_)) {
    LOG_ERROR("invalid parameters", K(systable_helper_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    QueryClogHistorySQLStrategy strategy;
    // Query clog history table based on timestamp
    if (req.req_by_start_tstamp_) {
      if (OB_FAIL(strategy.init_by_tstamp_query(req.pkey_, req.start_tstamp_))) {
        LOG_ERROR("init_by_tstamp_query fail", KR(ret),
            "pkey", req.pkey_, "tstamp", req.start_tstamp_);
      }
    }
    // Query clog history table based on log id
    else if (req.req_by_next_log_id_) {
      if (OB_FAIL(strategy.init_by_log_id_query(req.pkey_, req.next_log_id_))) {
        LOG_ERROR("init_by_log_id_query fail", KR(ret),
            "pkey", req.pkey_, "log_id", req.next_log_id_);
      }
    } else {
      LOG_ERROR("invalid request which is not requested by tstamp or log id", K(req));
      ret = OB_INVALID_ARGUMENT;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(query.do_sql_aggregate(&strategy))) {
        LOG_ERROR("do_sql_aggregate fail", KR(ret), K(strategy));
      }
    }
  }

  return ret;
}

int ObLogSvrFinder::do_sql_aggregate_for_query_meta_info_(SvrFindReq &req, IObLogSysTableHelper::BatchSQLQuery &query)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! req.is_valid())) {
    LOG_ERROR("invalid argument", K(req));
    ret = OB_INVALID_ARGUMENT;
  } else {
    QueryMetaInfoSQLStrategy strategy;
    bool only_query_leader = false;

    if (OB_FAIL(strategy.init(req.pkey_, only_query_leader))) {
      LOG_ERROR("QueryMetaInfoSQLStrategy fail", KR(ret), K(req));
    } else if (OB_FAIL(query.do_sql_aggregate(&strategy))) {
      LOG_ERROR("do_sql_aggregate fail", KR(ret), K(strategy));
    } else {
      // succ
    }
  }

  return ret;
}

int ObLogSvrFinder::do_sql_aggregate_for_leader_(ISvrFinderReq *orig_req, WorkerData &data)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(orig_req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(orig_req));
  } else if (OB_ISNULL(systable_helper_)) {
    LOG_ERROR("invalid parameters", K(systable_helper_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(! orig_req->is_find_leader_req())) {
    LOG_ERROR("invalid request, type is not REQ_FIND_LEADER", KPC(orig_req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! orig_req->is_state_req())) {
    LOG_ERROR("invalid request, state is not REQ", KPC(orig_req));
    ret = OB_INVALID_ARGUMENT;
  } else {
    LeaderFindReq *req = static_cast<LeaderFindReq *>(orig_req);
    IObLogSysTableHelper::BatchSQLQuery &query = data.query_;

    if (OB_ISNULL(req)) {
      LOG_ERROR("dynamic cast from ISvrFinderReq to LeaderFindReq fail", K(orig_req), KPC(orig_req));
      ret = OB_ERR_UNEXPECTED;
    } else {
      req->has_leader_ = false;
      req->leader_.reset();
      QueryMetaInfoSQLStrategy strategy;
      bool only_query_leader = true;

      if (OB_FAIL(strategy.init(req->pkey_, only_query_leader))) {
        LOG_ERROR("QueryMetaInfoSQLStrategy init fail", KR(ret), KPC(req), K(only_query_leader));
      } else if (OB_FAIL(query.do_sql_aggregate(&strategy))) {
        LOG_ERROR("do_sql_aggregate fail", KR(ret), K(strategy));
      } else {
        LOG_DEBUG("svr finder do_aggregate for leader req", KPC(req));
      }
    }
  }

  return ret;
}

int ObLogSvrFinder::handle_meta_info_records_(SvrFindReq &req,
    const IObLogSysTableHelper::MetaRecordArray &records)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! req.is_valid())) {
    LOG_ERROR("invalid argument", K(req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(systable_helper_) || OB_ISNULL(all_svr_cache_)) {
    LOG_ERROR("invalid parameters", K(systable_helper_), K(all_svr_cache_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    IPartSvrList &svr_list = *(req.svr_list_);
    // No log service range in the meta table, the maximum log service range is used by default
    // Note: the service range is left open and right closed
    const uint64_t start_log_id = 0;
    const uint64_t end_log_id = OB_INVALID_ID;
    // However, the log id range is not valid and there is no need to update the log service range when the server exists
    const bool is_log_range_valid = false;

    // Use meta table records to supplement clog history records to prevent all clog history records from being incomplete and to avoid missing servers
    for (int64_t idx = 0, cnt = records.count(); OB_SUCCESS == ret && (idx < cnt); ++idx) {
      const IObLogSysTableHelper::MetaRecord &record = records.at(idx);
      ObAddr svr;
      svr.set_ip_addr(record.svr_ip_, record.svr_port_);

      RegionPriority region_prio = REGION_PRIORITY_UNKNOWN;
      ReplicaPriority replica_prio = REPLICA_PRIORITY_UNKNOWN;

      // 1. meta table records have higher priority than clog history records, the latest record recorded in clog history does
      // not mean it is the most appropriate one, because it may be a replica that is being migrated or doing rebuild,
      // it may be very backward, in this case, it is not suitable for locating and fetching logs
      // 2. for log locating scenarios, priority is given to locating the LEADER. it requires that: the LEADER replica has a higher
      // priority than the other replicas, in most cases the LEADER has the most complete log on it and can be located successfully,
      // and the LEADER must not return a very old log ID, it will either return the exact log ID or an error
      // 3. The LEADER replica also has a high probability of avoiding the inaccurate timestamp range of the partition logs maintained
      // by the ilog info block module, as it will periodically switch ilog files in 30 minutes, basically not allowing the time lapse in an ilog file to exceed 2100 seconds.
      //
      const bool is_located_in_meta_table = true;
      const bool is_leader = (common::is_strong_leader(static_cast<common::ObRole>(record.role_)));

      if (! all_svr_cache_->is_svr_avail(svr, region_prio)) {
        LOG_DEBUG("ignore server from meta table which is not active in all server table or server in encryption zone",
            K(svr), "pkey", req.pkey_, "next_log_id", req.next_log_id_,
            "start_tstamp", req.start_tstamp_,
            "region_prio", print_region_priority(region_prio));
      } else if (OB_FAIL(get_replica_priority(record.replica_type_, replica_prio))) {
        LOG_ERROR("get priority based replica fail", KR(ret), K(record),
            "replica_prio", print_replica_priority(replica_prio));
      }
      // Add or update server information
      else if (OB_FAIL(svr_list.add_server_or_update(
          svr,
          start_log_id,
          end_log_id,
          is_located_in_meta_table,
          region_prio,
          replica_prio,
          is_leader,
          is_log_range_valid))) {
        LOG_ERROR("add server into server list fail", KR(ret), K(svr),
            K(is_located_in_meta_table),
            "region_prio", print_region_priority(region_prio),
            "replica_prio", print_replica_priority(replica_prio),
            K(is_leader),
            K(is_log_range_valid));
      }
    } // for

    if (OB_SUCCESS == ret) {
      // After processing __all_meta_table records, sort the server list
      svr_list.sort_by_priority();
    }

    if (OB_SUCC(ret)) {
      const int64_t svr_count_before_filter = svr_list.count();
      ObArray<ObAddr> remove_svrs;

      if (OB_FAIL(svr_list.filter_by_svr_blacklist(svr_blacklist_, remove_svrs))) {
        LOG_ERROR("svr_list filter_by_svr_blacklist fail", KR(ret), K(remove_svrs));
      } else {
        const int64_t svr_count_after_filter = svr_list.count();

        // print if has svr filtered
        if (svr_count_before_filter > svr_count_after_filter) {
          _LOG_INFO("[SERVER_BLACKLIST] [FILTER] [PKEY=%s] [FILTER_SVR_CNT=%ld(%ld/%ld)] [REMOVE_SVR=%s]",
              to_cstring(req.pkey_), svr_count_before_filter - svr_count_after_filter,
              svr_count_before_filter, svr_count_after_filter, to_cstring(remove_svrs));
        }
      }
    }

    LOG_DEBUG("update server list by meta table", KR(ret), "pkey", req.pkey_,
        "next_log_id", req.next_log_id_, "start_tstamp", req.start_tstamp_,
        "svr_list", *(req.svr_list_), "meta_records", records);
  } // else

  return ret;
}

int ObLogSvrFinder::handle_clog_history_info_records_(SvrFindReq &req,
    const IObLogSysTableHelper::ClogHistoryRecordArray &records)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! req.is_valid())) {
    LOG_ERROR("invalid argument", K(req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(all_svr_cache_)) {
    LOG_ERROR("invalid parameters", K(all_svr_cache_));
    ret = OB_INVALID_ERROR;
  } else {
    IPartSvrList &svr_list = *(req.svr_list_);
    ObAddr svr;
    RegionPriority region_prio = REGION_PRIORITY_UNKNOWN;
    // The logging service scope of clog history is valid and should be updated
    const bool is_log_range_valid = true;
    // __all_clog_history_info_v2 returns records with no replica type, default full functional replica
    // Wait for __all_meta_table record to return copy type, update the corresponding replica priority
    ReplicaPriority replica_prio = REPLICA_PRIORITY_UNKNOWN;
    if (OB_FAIL(get_replica_priority(REPLICA_TYPE_FULL, replica_prio))) {
      LOG_ERROR("get priority based replica fail", KR(ret), K(REPLICA_TYPE_FULL),
                "replica_prio", print_replica_priority(replica_prio));
    } else {
      // located in clog history table
      const bool is_located_in_meta_table = false;
      // Query clog history table does not know the leader information, later query meta table and update
      const bool is_leader = false;

      for (int64_t idx = 0, cnt = records.count(); OB_SUCCESS == ret && (idx < cnt); ++idx) {
        const IObLogSysTableHelper::ClogHistoryRecord &info_record = records.at(idx);
        svr.reset();
        svr.set_ip_addr(info_record.svr_ip_, info_record.svr_port_);

        if (! all_svr_cache_->is_svr_avail(svr, region_prio)) {
          // Filter machines that are not in the __all_server list
          LOG_DEBUG("ignore server from clog history which is not active in all server table or server in encryption zone",
              K(svr), "pkey", req.pkey_, "next_log_id", req.next_log_id_,
              "start_tstamp", req.start_tstamp_, K(info_record),
              "region_prio", print_region_priority(region_prio));
        } else if (OB_FAIL(svr_list.add_server_or_update(svr,
                info_record.start_log_id_,
                info_record.end_log_id_,
                is_located_in_meta_table,
                region_prio,
                replica_prio,
                is_leader,
                is_log_range_valid))) {
          LOG_ERROR("add server into server list fail", KR(ret), K(svr),
              "start_log_id", info_record.start_log_id_,
              "end_log_id", info_record.end_log_id_,
               K(is_located_in_meta_table),
              "region_prio", print_region_priority(region_prio),
              "replica_prio", print_replica_priority(replica_prio),
              K(is_leader),
              K(idx),
              K(is_log_range_valid));
        } else {
          // success
        }
      } // for

      LOG_DEBUG("update server list by clog history", KR(ret), "pkey", req.pkey_,
          "next_log_id", req.next_log_id_, "start_tstamp", req.start_tstamp_,
          "svr_list", req.svr_list_, "clog_history_records", records);
    }
  }
  return ret;
}

int ObLogSvrFinder::init_worker_data_(const int64_t thread_num)
{
  int ret = OB_SUCCESS;

  int64_t alloc_size = thread_num * sizeof(WorkerData);
  // The number of aggregated SQL entries is initialised using the maximum value
  int64_t max_sql_batch_count = ObLogConfig::max_svr_finder_sql_batch_count;
  const int64_t sql_buf_len = max_sql_batch_count * DEFAULT_SQL_LENGTH;
  worker_data_ = static_cast<WorkerData*>(ob_malloc(alloc_size, ObModIds::OB_LOG_FETCHER_SVR_FINDER));

  if (OB_ISNULL(worker_data_)) {
    LOG_ERROR("allocate memory fail", K(worker_data_), K(alloc_size), K(thread_num));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    // init worker data
    for (int64_t idx = 0, cnt = thread_num; OB_SUCCESS == ret && idx < cnt; ++idx) {
      new (worker_data_ + idx) WorkerData();
      WorkerData &data = worker_data_[idx];

      if (OB_FAIL(data.init(sql_buf_len))) {
        LOG_ERROR("init worker data fail", KR(ret), K(sql_buf_len));
      } else {
        LOG_INFO("init worker data succ", "thread_idx", idx, K(thread_num), K(sql_buf_len));
      }
    }
  }

  return ret;
}

void ObLogSvrFinder::destory_worker_data_()
{
  if (NULL != worker_data_) {
    for (int64_t idx = 0, cnt = thread_num_; idx < cnt; ++idx) {
      worker_data_[idx].~WorkerData();
    }

    ob_free(worker_data_);
    worker_data_ = NULL;
  }
}

int ObLogSvrFinder::WorkerData::init(const int64_t sql_buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(query_.init(sql_buf_len))) {
    LOG_ERROR("BatchSQLQuery init fail", KR(ret), K(sql_buf_len));
  } else {
    // succ
  }

  return ret;
}

void ObLogSvrFinder::WorkerData::destroy()
{
  req_list_.reset();
  query_.destroy();
}

///////////////////////////////////////////// ISvrFinderReq /////////////////////////////////////////////

const char* ISvrFinderReq::print_state(const int state)
{
  const char *str = "INVALID";

  switch (state) {
    case IDLE:
      str = "IDLE";
      break;
    case REQ:
      str = "REQ";
      break;
    case DONE:
      str = "DONE";
      break;
    default:
      str = "INVALID";
      break;
  }

  return str;
}

const char * ISvrFinderReq::print_type(const int type)
{
  const char *str = "INVALID";
  switch (type) {
    case REQ_UNKNOWN:
      str = "REQ_UNKNOWN";
      break;
    case REQ_FIND_SVR:
      str = "REQ_FIND_SVR";
      break;
    case REQ_FIND_LEADER:
      str = "REQ_FIND_LEADER";
      break;
    default:
      str = "INVALID";
      break;
  }
  return str;
}

///////////////////////////////////////////// ISvrFinderReq /////////////////////////////////////////////

void SvrFindReq::reset()
{
  ISvrFinderReq::reset();

  pkey_.reset();
  start_tstamp_ = OB_INVALID_TIMESTAMP;
  next_log_id_ = OB_INVALID_ID;
  req_by_start_tstamp_ = false;
  req_by_next_log_id_ = false;

  svr_list_ = NULL;
}

uint64_t SvrFindReq::hash() const
{
  // Hash by "PKEY + next_log_id"
  return murmurhash(&next_log_id_, sizeof(next_log_id_), pkey_.hash());
}

bool SvrFindReq::is_valid() const
{
  // Requires that the list of servers cannot be queried based on both timestamp and log id
  return NULL != svr_list_ && (req_by_start_tstamp_ != req_by_next_log_id_);
}

void SvrFindReq::reset_for_req_by_tstamp(IPartSvrList &svr_list,
    const common::ObPartitionKey &pkey,
    const int64_t tstamp)
{
  reset();
  svr_list_ = &svr_list;
  pkey_ = pkey;
  req_by_start_tstamp_ = true;
  start_tstamp_ = tstamp;
}

void SvrFindReq::reset_for_req_by_log_id(IPartSvrList &svr_list,
    const common::ObPartitionKey &pkey,
    const uint64_t id)
{
  reset();
  svr_list_ = &svr_list;
  pkey_ = pkey;
  req_by_next_log_id_ = true;
  next_log_id_ = id;
}

}
}
