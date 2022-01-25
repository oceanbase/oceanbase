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

#define USING_LOG_PREFIX    OBLOG_FETCHER

#include "ob_log_fetcher_heartbeat_worker.h"

#include "lib/allocator/ob_mod_define.h"  // ObModIds
#include "lib/allocator/ob_malloc.h"      // ob_malloc

#include "ob_log_rpc.h"                   // IObLogRpc
#include "ob_log_instance.h"              // IObLogErrHandler
#include "ob_log_trace_id.h"              // ObLogTraceIdGuard

using namespace oceanbase::common;
namespace oceanbase
{
namespace liboblog
{

/////////////////////////////////////// ObLogFetcherHeartbeatWorker ///////////////////////////////////////

int64_t ObLogFetcherHeartbeatWorker::g_rpc_timeout = ObLogConfig::default_heartbeater_rpc_timeout_sec * _SEC_;
int64_t ObLogFetcherHeartbeatWorker::g_batch_count = ObLogConfig::default_heartbeater_batch_count;

ObLogFetcherHeartbeatWorker::ObLogFetcherHeartbeatWorker() :
    inited_(false),
    thread_num_(0),
    rpc_(NULL),
    err_handler_(NULL),
    worker_data_(NULL),
    allocator_(ObModIds::OB_LOG_HEARTBEATER)
{
}

ObLogFetcherHeartbeatWorker::~ObLogFetcherHeartbeatWorker()
{
  destroy();
}

int ObLogFetcherHeartbeatWorker::init(const int64_t thread_num,
    IObLogRpc &rpc,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;
  int64_t max_thread_num = ObLogConfig::max_fetcher_heartbeat_thread_num;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num_ = thread_num) <= 0
      || OB_UNLIKELY(thread_num > max_thread_num)) {
    LOG_ERROR("invalid thread num", K(thread_num), K(max_thread_num));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(HeartbeatThread::init(thread_num, ObModIds::OB_LOG_HEARTBEATER))) {
    LOG_ERROR("init heartbeat worker fail", KR(ret), K(thread_num));
  } else {
    int64_t alloc_size = thread_num * sizeof(WorkerData);
    worker_data_ = static_cast<WorkerData*>(ob_malloc(alloc_size, ObModIds::OB_LOG_HEARTBEATER));

    if (OB_ISNULL(worker_data_)) {
      LOG_ERROR("allocate memory fail", K(worker_data_), K(alloc_size), K(thread_num));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      // Init worker data
      for (int64_t idx = 0, cnt = thread_num; OB_SUCCESS == ret && idx < cnt; ++idx) {
        new (worker_data_ + idx) WorkerData();
        WorkerData &data = worker_data_[idx];

        if (OB_FAIL(data.init())) {
          LOG_ERROR("init worker data fail", KR(ret));
        }
      }
    }

    if (OB_SUCCESS == ret) {
      rpc_ = &rpc;
      err_handler_ = &err_handler;
      inited_ = true;
      LOG_INFO("init heartbeater succ", K(thread_num));
    }
  }

  if (OB_SUCCESS != ret) {
    destroy();
  }
  return ret;
}

void ObLogFetcherHeartbeatWorker::destroy()
{
  stop();

  inited_ = false;

  // Destroy the heartbeat worker thread pool
  HeartbeatThread::destroy();

  if (NULL != worker_data_) {
    for (int64_t idx = 0, cnt = thread_num_; idx < cnt; ++idx) {
      free_all_svr_req_(worker_data_[idx]);
      worker_data_[idx].~WorkerData();
    }

    ob_free(worker_data_);
    worker_data_ = NULL;
  }

  thread_num_ = 0;
  rpc_ = NULL;
  err_handler_ = NULL;
  worker_data_ = NULL;

  allocator_.clear();

  LOG_INFO("destroy heartbeater succ");
}

int ObLogFetcherHeartbeatWorker::async_heartbeat_req(HeartbeatRequest *req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(req)) {
    LOG_ERROR("invalid argument", K(req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! req->is_state_idle())) {
    LOG_ERROR("invalid request, state is not IDLE", KPC(req));
    ret = OB_INVALID_ARGUMENT;
  } else {
    req->set_state_req();

    // Hash by server to the corresponding worker thread
    // Ensure that requests from the same server are aggregated
    uint64_t hash_val = req->svr_.hash();

    if (OB_FAIL(HeartbeatThread::push(req, hash_val))) {
      LOG_ERROR("push request into worker queue fail", KR(ret), K(req), K(hash_val), KPC(req));
    }
  }

  return ret;
}

int ObLogFetcherHeartbeatWorker::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(HeartbeatThread::start())) {
    LOG_ERROR("start heartbeater worker fail", KR(ret));
  } else {
    LOG_INFO("start heartbeater succ", K_(thread_num));
  }
  return ret;
}

void ObLogFetcherHeartbeatWorker::stop()
{
  if (OB_LIKELY(inited_)) {
    HeartbeatThread::stop();
    LOG_INFO("stop heartbeater succ");
  }
}

void ObLogFetcherHeartbeatWorker::run(const int64_t thread_index)
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

    while (! stop_flag_ && OB_SUCCESS == ret) {
      if (OB_FAIL(do_retrieve_(thread_index, data))) {
        LOG_ERROR("retrieve request fail", KR(ret), K(thread_index));
      } else if (OB_FAIL(do_request_(data))) {
        LOG_ERROR("do request fail", KR(ret));
      } else {
        cond_timedwait(thread_index, DATA_OP_TIMEOUT);
      }
    }

    if (stop_flag_) {
      ret = OB_IN_STOP_STATE;
    }
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
    LOG_ERROR("heartbeater worker exit on fail", KR(ret), K(thread_index));
    if (OB_NOT_NULL(err_handler_)) {
      err_handler_->handle_error(ret, "heartbeater worker exits on fail, ret=%d, thread_index=%ld",
          ret, thread_index);
    }
  }
}

void ObLogFetcherHeartbeatWorker::configure(const ObLogConfig &config)
{
  int64_t heartbeater_rpc_timeout_sec = config.heartbeater_rpc_timeout_sec;
  int64_t heartbeater_batch_count = config.heartbeater_batch_count;

  ATOMIC_STORE(&g_rpc_timeout, heartbeater_rpc_timeout_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(heartbeater_rpc_timeout_sec));
  ATOMIC_STORE(&g_batch_count, heartbeater_batch_count);
  LOG_INFO("[CONFIG]", K(heartbeater_batch_count));
}

int ObLogFetcherHeartbeatWorker::do_retrieve_(const int64_t thread_index, WorkerData &worker_data)
{
  int ret = OB_SUCCESS;
  int64_t batch_count = ATOMIC_LOAD(&g_batch_count);

  // Get data from the queue and process it in bulk
  for (int64_t cnt = 0; OB_SUCCESS == ret && (cnt < batch_count); ++cnt) {
    void *data = NULL;
    HeartbeatRequest *request = NULL;
    SvrReq *svr_req = NULL;

    if (OB_FAIL(HeartbeatThread::pop(thread_index, data))) {
      if (OB_EAGAIN != ret) {
        LOG_ERROR("pop data from queue fail", KR(ret), K(thread_index), K(data));
      }
    } else if (OB_ISNULL(request = static_cast<HeartbeatRequest *>(data))) {
      LOG_ERROR("request is NULL", K(request), K(thread_index), K(data));
      ret = OB_ERR_UNEXPECTED;
    }
    // Aggregate by server
    else if (OB_FAIL(get_svr_req_(worker_data, request->svr_, svr_req))) {
      LOG_ERROR("get svr req fail", KR(ret), K(request->svr_));
    } else if (OB_ISNULL(svr_req)) {
      LOG_ERROR("invalid svr req", K(request->svr_), K(svr_req));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(svr_req->push(request))) {
      LOG_ERROR("push request into request list fail", KR(ret), KPC(svr_req), K(request));
    } else {
      // success
    }
  }

  if (OB_SUCCESS == ret) {
    // Meet the number of batches requirement
  } else if (OB_EAGAIN == ret) {
    // Queue is empty
    ret = OB_SUCCESS;
  } else {
    LOG_ERROR("pop and aggregate request fail", KR(ret), K(thread_index));
  }
  return ret;
}

ObLogFetcherHeartbeatWorker::SvrReq *ObLogFetcherHeartbeatWorker::alloc_svr_req_(const common::ObAddr &svr)
{
  SvrReq *svr_req = NULL;
  void *buf = allocator_.alloc(sizeof(SvrReq));
  if (OB_NOT_NULL(buf)) {
    svr_req = new(buf) SvrReq(svr);
  }
  return svr_req;
}

void ObLogFetcherHeartbeatWorker::free_svr_req_(SvrReq *req)
{
  if (NULL != req) {
    req->~SvrReq();
    allocator_.free(req);
    req = NULL;
  }
}

void ObLogFetcherHeartbeatWorker::free_all_svr_req_(WorkerData &data)
{
  for (int64_t index = 0; index < data.svr_req_list_.count(); index++) {
    SvrReq *svr_req = data.svr_req_list_.at(index);

    if (OB_NOT_NULL(svr_req)) {
      free_svr_req_(svr_req);
      svr_req = NULL;
    }
  }

  data.reset();
}

int ObLogFetcherHeartbeatWorker::get_svr_req_(WorkerData &data,
    const common::ObAddr &svr,
    SvrReq *&svr_req)
{
  int ret = OB_SUCCESS;
  SvrReqList &svr_req_list = data.svr_req_list_;
  SvrReqMap &svr_req_map = data.svr_req_map_;

  svr_req = NULL;

  // 优先从Map中获取对应的记录
  if (OB_FAIL(svr_req_map.get(svr, svr_req))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;

      // 分配一个新的请求
      if (OB_ISNULL(svr_req = alloc_svr_req_(svr))) {
        LOG_ERROR("allocate svr request fail", K(svr));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(svr_req_list.push_back(svr_req))) {
        LOG_ERROR("push svr req into array fail", KR(ret), K(svr_req), K(svr_req_list));
      } else if (OB_FAIL(svr_req_map.insert(svr, svr_req))) {
        LOG_ERROR("insert svr req into map fail", KR(ret), K(svr), KPC(svr_req));
      }
    } else {
      LOG_ERROR("get svr req from map fail", KR(ret), K(svr));
    }
  } else {
    // succ
  }
  return ret;
}

int ObLogFetcherHeartbeatWorker::do_request_(WorkerData &data)
{
  int ret = OB_SUCCESS;
  SvrReqList &svr_req_list = data.svr_req_list_;
  RpcReq rpc_req;

  // Trace ID
  ObLogTraceIdGuard trace_id_guard;

  if (OB_ISNULL(rpc_)) {
    LOG_ERROR("invalid rpc handle", K(rpc_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Iterate through the list of all server requests
    for (int64_t idx = 0, cnt = svr_req_list.count(); OB_SUCCESS == ret && (idx < cnt); ++idx) {
      if (OB_ISNULL(svr_req_list.at(idx))) {
        LOG_ERROR("svr request is NULL", K(idx), K(cnt), K(svr_req_list));
        ret = OB_ERR_UNEXPECTED;
      } else {
        SvrReq &svr_req = *(svr_req_list.at(idx));

        // Batch processing of heartbeat requests
        for (int64_t start_idx = 0; OB_SUCCESS == ret && start_idx < svr_req.hb_req_list_.count();) {
          rpc_req.reset();

          // Build RPC request
          if (OB_FAIL(build_rpc_request_(rpc_req, svr_req, start_idx))) {
            LOG_ERROR("build rpc request fail", KR(ret), K(start_idx), K(svr_req), K(rpc_req));
          }
          // Executing RPC requests
          else if (OB_FAIL(request_heartbeat_(rpc_req, svr_req, start_idx))) {
            LOG_ERROR("request heartbeat fail", KR(ret), K(rpc_req), K(svr_req), K(start_idx));
          } else {
            // Update the starting point of the next request
            start_idx += rpc_req.get_params().count();
          }
        }

        // Reset server list
        svr_req.reset();
      }
    }
  }
  return ret;
}

int ObLogFetcherHeartbeatWorker::build_rpc_request_(RpcReq &rpc_req,
    SvrReq &svr_req,
    const int64_t start_idx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(start_idx < 0) || OB_UNLIKELY(svr_req.hb_req_list_.count() <= start_idx)) {
    LOG_ERROR("invalid start_idx", K(start_idx), K(svr_req));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t max_req_cnt = RpcReq::ITEM_CNT_LMT;
    int64_t avail_req_cnt = svr_req.hb_req_list_.count() - start_idx;

    // The maximum number of heartbeat requests aggregated in this request
    int64_t req_cnt = std::min(max_req_cnt, avail_req_cnt);

    for (int64_t idx = 0; OB_SUCCESS == ret && idx < req_cnt; idx++) {
      HeartbeatRequest *hb_req = svr_req.hb_req_list_.at(idx + start_idx);

      if (OB_ISNULL(hb_req)) {
        LOG_ERROR("invalid heartbeat request", K(hb_req), K(idx), K(start_idx), K(svr_req));
        ret = OB_ERR_UNEXPECTED;
      } else {
        RpcReq::Param param;
        param.reset(hb_req->pkey_, hb_req->next_log_id_);

        if (OB_FAIL(rpc_req.append_param(param))) {
          if (OB_BUF_NOT_ENOUGH == ret) {
            // buffer is full
          } else {
            LOG_ERROR("append param fail", KR(ret), K(param), K(rpc_req));
          }
        }
      }
    }

    if (OB_BUF_NOT_ENOUGH == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLogFetcherHeartbeatWorker::request_heartbeat_(const RpcReq &rpc_req,
    SvrReq &svr_req,
    const int64_t start_idx)
{
  int ret = OB_SUCCESS;
  RpcResp rpc_resp;
  const ObAddr &svr = svr_req.svr_;
  int64_t total_hb_req_cnt = svr_req.hb_req_list_.count();
  int64_t req_param_cnt = rpc_req.get_params().count();
  int64_t rpc_timeout = ATOMIC_LOAD(&g_rpc_timeout);

  // Use a different Trace ID for each request
  ObLogTraceIdGuard guard;

  if (OB_ISNULL(rpc_)) {
    LOG_ERROR("invalid rpc handler", K(rpc_));
    ret = OB_INVALID_ERROR;
  } else if (OB_UNLIKELY(start_idx < 0)
      || OB_UNLIKELY(start_idx > (total_hb_req_cnt - req_param_cnt))) {
    LOG_ERROR("invalid argument", K(start_idx), K(total_hb_req_cnt), K(req_param_cnt));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int rpc_err = rpc_->req_leader_heartbeat(svr, rpc_req, rpc_resp, rpc_timeout);
    int svr_err = rpc_resp.get_err();
    int64_t resp_result_cnt = rpc_resp.get_results().count();

    // Check RPC result
    if (OB_SUCCESS != rpc_err) {
      LOG_ERROR("request heartbeat fail on rpc", K(svr), K(rpc_err), K(rpc_req));
    } else if (OB_SUCCESS != svr_err) {
      LOG_ERROR("request heartbeat fail on server", K(svr), K(rpc_err), K(svr_err),
          "svr_debug_err", rpc_resp.get_debug_err(), K(rpc_req), K(rpc_resp));
    } else if (OB_UNLIKELY(req_param_cnt != resp_result_cnt)) {
      LOG_ERROR("heartbeat rpc request does not match rpc response",
          K(req_param_cnt), K(resp_result_cnt), K(svr), K(rpc_req), K(rpc_resp));
      ret = OB_INVALID_DATA;
    } else {
      // success
    }

    if (OB_SUCCESS == ret) {
      TraceIdType *trace_id = ObCurTraceId::get_trace_id();

      // Iterate through all heartbeat requests, set the corresponding result, and mark completion regardless of success or failure
      for (int64_t idx = 0; OB_SUCCESS == ret && idx < req_param_cnt; idx++) {
        HeartbeatRequest *hb_req = svr_req.hb_req_list_.at(start_idx + idx);

        if (OB_ISNULL(hb_req)) {
          LOG_ERROR("invalid heartbeat request", K(hb_req), K(idx), K(start_idx),
              K(svr_req.hb_req_list_));
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_SUCCESS != rpc_err || OB_SUCCESS != svr_err) {
          // Setting Failure Results
          hb_req->set_resp(rpc_err, svr_err, OB_SUCCESS, OB_INVALID_ID, OB_INVALID_TIMESTAMP,
              trace_id);
        } else {
          const RpcResp::Result &result = rpc_resp.get_results().at(idx);

          // Setting Success Results
          hb_req->set_resp(rpc_err, svr_err, result.err_, result.next_served_log_id_,
              result.next_served_ts_, trace_id);
        }

        // After setting up the results, mark the heartbeat complete
        if (OB_SUCCESS == ret) {
          hb_req->set_state_done();
          // The request cannot be continued afterwards, there is a concurrency scenario
          // Mark the corresponding request as invalid to avoid revisiting it
          svr_req.hb_req_list_.at(start_idx + idx) = NULL;
        }
      }
    }
  }
  return ret;
}


//////////////////////////// ObLogFetcherHeartbeatWorker::WorkerData ////////////////////////////

int ObLogFetcherHeartbeatWorker::WorkerData::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(svr_req_map_.init(ObModIds::OB_LOG_HEARTBEATER))) {
    LOG_ERROR("init request map fail", KR(ret));
  } else {
    svr_req_list_.set_label(ObModIds::OB_LOG_HEARTBEATER);
    svr_req_list_.reset();
  }
  return ret;
}

void ObLogFetcherHeartbeatWorker::WorkerData::destroy()
{
  (void)svr_req_map_.destroy();
  svr_req_list_.reset();
}

}
}

