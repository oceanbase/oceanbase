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

#include "ob_log_start_log_id_locator.h"

#include "lib/allocator/ob_mod_define.h"          // ObModIds

#include "ob_log_instance.h"                      // IObLogErrHandler
#include "ob_log_trace_id.h"                      // ObLogTraceIdGuard
#include "ob_log_rpc.h"                           // IObLogRpc

namespace oceanbase
{
using namespace common;

namespace liboblog
{

int64_t ObLogStartLogIdLocator::g_batch_count =
    ObLogConfig::default_start_log_id_locator_batch_count;
int64_t ObLogStartLogIdLocator::g_rpc_timeout =
    ObLogConfig::default_start_log_id_locator_rpc_timeout_sec * _SEC_;
int64_t ObLogStartLogIdLocator::g_observer_clog_save_time =
    ObLogConfig::default_observer_clog_save_time_minutes * _MIN_;
bool ObLogStartLogIdLocator::g_enable_force_start_mode =
    ObLogConfig::default_enable_force_start_mode;

ObLogStartLogIdLocator::ObLogStartLogIdLocator() :
    inited_(false),
    worker_cnt_(0),
    locate_count_(0),
    rpc_(NULL),
    err_handler_(NULL),
    worker_data_(NULL),
    allocator_(ObModIds::OB_LOG_START_LOG_ID_LOCATOR)
{
}

ObLogStartLogIdLocator::~ObLogStartLogIdLocator()
{
  destroy();
}

int ObLogStartLogIdLocator::init(
    const int64_t worker_cnt,
    const int64_t locate_count,
    IObLogRpc &rpc,
    IObLogErrHandler &err_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY((worker_cnt_ = worker_cnt) <= 0)
      || OB_UNLIKELY(locate_count <= 0)
      || OB_UNLIKELY(MAX_THREAD_NUM < worker_cnt)) {
    LOG_ERROR("invalid worker cnt", K(worker_cnt), "max_thread_num", MAX_THREAD_NUM, K(locate_count));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(LocateWorker::init(worker_cnt, ObModIds::OB_LOG_START_LOG_ID_LOCATOR))) {
    LOG_ERROR("init locate worker fail", KR(ret), K(worker_cnt));
  } else {
    int64_t alloc_size = worker_cnt * sizeof(WorkerData);
    void *buf = ob_malloc(alloc_size, ObModIds::OB_LOG_START_LOG_ID_LOCATOR);

    if (OB_ISNULL(worker_data_ = static_cast<WorkerData *>(buf))) {
      LOG_ERROR("allocate memory fail", K(worker_data_), K(alloc_size), K(worker_cnt));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (int64_t idx = 0, cnt = worker_cnt; OB_SUCCESS == ret && idx < cnt; ++idx) {
        new (worker_data_ + idx) WorkerData();
        WorkerData &data = worker_data_[idx];

        if (OB_FAIL(data.init())) {
          LOG_ERROR("init worker data fail", KR(ret));
        }
      }
    }

    if (OB_SUCCESS == ret) {
      locate_count_ = locate_count;
      rpc_ = &rpc;
      err_handler_ = &err_handle;

      inited_ = true;
      LOG_INFO("init start log id locator succ", "thread_num", LocateWorker::get_thread_num());
    }
  }

  if (OB_SUCCESS != ret) {
    destroy();
  }
  return ret;
}

void ObLogStartLogIdLocator::destroy()
{
  stop();

  inited_ = false;

  LocateWorker::destroy();

  if (NULL != worker_data_) {
    for (int64_t idx = 0, cnt = worker_cnt_; idx < cnt; ++idx) {
      // free SvrReq memory here
      free_all_svr_req_(worker_data_[idx]);
      worker_data_[idx].~WorkerData();
    }

    ob_free(worker_data_);
    worker_data_ = NULL;
  }

  worker_cnt_ = 0;
  locate_count_ = 0;
  rpc_ = NULL;
  err_handler_ = NULL;
  worker_data_ = NULL;
  allocator_.clear();

  LOG_INFO("destroy start log id locator succ");
}

int ObLogStartLogIdLocator::async_start_log_id_req(StartLogIdLocateReq *req)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(req)) {
    LOG_ERROR("invalid argument", K(req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! req->is_state_idle())) {
    LOG_ERROR("invalid request, state is not IDLE", KPC(req));
    ret = OB_INVALID_ARGUMENT;
  } else {
    req->set_state_req();

    if (OB_FAIL(dispatch_worker_(req))) {
      LOG_ERROR("dispatch worker fail", KR(ret), KPC(req));
    }
  }
  return ret;
}

// 1. if the request is finished, set its status to DONE
// 2. otherwise assign it to the next server
int ObLogStartLogIdLocator::dispatch_worker_(StartLogIdLocateReq *req)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(req)) {
    LOG_ERROR("invalid request", K(req));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! req->is_state_req())) {
    LOG_ERROR("invalid request, state is not REQ", KPC(req), K(req->get_state()));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObAddr svr;
    StartLogIdLocateReq::SvrItem *item = NULL;

    // Mark DONE if the request is completed, or if all servers are requested.
    if (req->is_request_ended(locate_count_)) {
      // If the request ends, set to DONE
      // NOTE: After setting to DONE, no further access is possible
      LOG_DEBUG("start log id locate request ended", KPC(req));
      req->set_state_done();
    } else if (OB_FAIL(req->next_svr_item(item)) || OB_ISNULL(item)) {
      LOG_ERROR("get next server item fail", KR(ret), KPC(req), K(item));
      ret = (OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret);
    } else {
      const ObAddr &svr = item->svr_;

      // Hashing by server to the corresponding worker thread
      // The purpose is to ensure that requests from the same server are aggregated
      uint64_t hash_val = svr.hash();

      LOG_DEBUG("dispatch start log id locate request",
          "worker_idx", hash_val % worker_cnt_,
          K(svr),
          KPC(req));

      if (OB_FAIL(LocateWorker::push(req, hash_val))) {
        LOG_ERROR("push req to worker fail", KR(ret), KPC(req), K(hash_val), K(svr));
      } else {
        // done
      }
    }
  }
  return ret;
}

int ObLogStartLogIdLocator::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(LocateWorker::start())) {
    LOG_ERROR("start locate worker fail", KR(ret));
  } else {
    LOG_INFO("start log id locator succ", K_(worker_cnt));
  }
  return ret;
}

void ObLogStartLogIdLocator::stop()
{
  if (OB_LIKELY(inited_)) {
    LocateWorker::stop();
    LOG_INFO("stop log id locator succ");
  }
}

// start log id locator worker thread
void ObLogStartLogIdLocator::run(const int64_t thread_index)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(thread_index < 0) || OB_UNLIKELY(thread_index >= worker_cnt_)) {
    LIB_LOG(ERROR, "invalid thread index", K(thread_index), K(worker_cnt_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    WorkerData &data = worker_data_[thread_index];

    while (! stop_flag_ && OB_SUCCESS == ret) {
      if (OB_FAIL(do_retrieve_(thread_index, data))) {
        LOG_ERROR("retrieve request fail", KR(ret), K(thread_index));
      } else if (OB_FAIL(do_request_(data))) {
				if (OB_IN_STOP_STATE != ret) {
        	LOG_ERROR("do request fail", KR(ret));
				}
      } else {
        cond_timedwait(thread_index, DATA_OP_TIMEOUT);
      }
    }

    if (stop_flag_) {
      ret = OB_IN_STOP_STATE;
    }
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
    LOG_ERROR("start log id locator worker exit on fail", KR(ret), K(thread_index));
    if (OB_NOT_NULL(err_handler_)) {
      err_handler_->handle_error(ret,
          "start log id locator worker exit on fail, ret=%d, thread_index=%ld",
          ret, thread_index);
    }
  }
}

void ObLogStartLogIdLocator::configure(const ObLogConfig &config)
{
  int64_t start_log_id_locator_rpc_timeout_sec = config.start_log_id_locator_rpc_timeout_sec;
  int64_t start_log_id_locator_batch_count = config.start_log_id_locator_batch_count;
  int64_t observer_clog_save_time_minutes = config.observer_clog_save_time_minutes;
  bool enable_force_start_mode = config.enable_force_start_mode;

  ATOMIC_STORE(&g_rpc_timeout, start_log_id_locator_rpc_timeout_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(start_log_id_locator_rpc_timeout_sec));

  ATOMIC_STORE(&g_batch_count, start_log_id_locator_batch_count);
  LOG_INFO("[CONFIG]", K(start_log_id_locator_batch_count));

  ATOMIC_STORE(&g_observer_clog_save_time, observer_clog_save_time_minutes * _MIN_);
  LOG_INFO("[CONFIG]", K(observer_clog_save_time_minutes));

  ATOMIC_STORE(&g_enable_force_start_mode, enable_force_start_mode);
  LOG_INFO("[CONFIG]", K(enable_force_start_mode));
}

// Pop out data from Queue, then use Map, aggregating requests by server
int ObLogStartLogIdLocator::do_retrieve_(const int64_t thread_index, WorkerData &worker_data)
{
  int ret = OB_SUCCESS;
  int64_t batch_count = ATOMIC_LOAD(&g_batch_count);

  for (int64_t cnt = 0; OB_SUCCESS == ret && (cnt < batch_count); ++cnt) {
    StartLogIdLocateReq *request = NULL;
    StartLogIdLocateReq::SvrItem *item = NULL;
    SvrReq *svr_req = NULL;
    void *data = NULL;

    if (OB_FAIL(LocateWorker::pop(thread_index, data))) {
      if (OB_EAGAIN != ret) {
        LOG_ERROR("pop data from queue fail", KR(ret), K(thread_index), K(request));
      }
    } else if (OB_ISNULL(request = static_cast<StartLogIdLocateReq *>(data))) {
      LOG_ERROR("request is NULL", K(request), K(thread_index));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(request->cur_svr_item(item)) || OB_ISNULL(item)) {
      LOG_ERROR("get current server item fail", KR(ret), KPC(request), K(item));
      ret = (OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret);
    } else if (OB_FAIL(get_svr_req_(worker_data, item->svr_, svr_req)) || OB_ISNULL(svr_req)) {
      LOG_ERROR("get svr req fail", KR(ret), K(item->svr_), K(svr_req));
      ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
    } else if (OB_FAIL(svr_req->push(request))) {
      LOG_ERROR("push request into request list fail", KR(ret), KPC(svr_req), K(request));
    } else {
      // succ
    }
  }

  if (OB_SUCCESS == ret) {
    // Meeting the volume requirements for batch processing
  } else if (OB_EAGAIN == ret) {
    // empty queue
    ret = OB_SUCCESS;
  } else {
    LOG_ERROR("pop and aggregate request fail", KR(ret), K(thread_index));
  }
  return ret;
}

int ObLogStartLogIdLocator::get_svr_req_(WorkerData &data,
    const common::ObAddr &svr,
    SvrReq *&svr_req)
{
  int ret = OB_SUCCESS;
  SvrReqList &svr_req_list = data.svr_req_list_;
  SvrReqMap &svr_req_map = data.svr_req_map_;

  svr_req = NULL;

  // Fetching the corresponding record from the Map is preferred
  if (OB_FAIL(svr_req_map.get(svr, svr_req))) {
    // If the record does not exist, insert the corresponding record in the Array first, and then insert the object in the Array into the Map
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;

      // Assign a new request
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

ObLogStartLogIdLocator::SvrReq *ObLogStartLogIdLocator::alloc_svr_req_(const common::ObAddr &svr)
{
  SvrReq *svr_req = NULL;
  void *buf = allocator_.alloc(sizeof(SvrReq));
  if (OB_NOT_NULL(buf)) {
    svr_req = new(buf) SvrReq(svr);
  }
  return svr_req;
}

void ObLogStartLogIdLocator::free_svr_req_(SvrReq *req)
{
  if (NULL != req) {
    req->~SvrReq();
    allocator_.free(req);
    req = NULL;
  }
}

void ObLogStartLogIdLocator::free_all_svr_req_(WorkerData &data)
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

int ObLogStartLogIdLocator::do_request_(WorkerData &data)
{
  int ret = OB_SUCCESS;
  SvrReqList &svr_req_list = data.svr_req_list_;

  if (OB_ISNULL(rpc_)) {
    LOG_ERROR("invalid rpc handle", K(rpc_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t idx = 0, cnt = svr_req_list.count();
				 ! stop_flag_ && OB_SUCCESS == ret && (idx < cnt); ++idx) {
      if (OB_ISNULL(svr_req_list.at(idx))) {
        LOG_ERROR("svr request is NULL", K(idx), K(cnt), K(svr_req_list));
        ret = OB_ERR_UNEXPECTED;
      } else {
        SvrReq &svr_req = *(svr_req_list.at(idx));

        // Requesting a single server
        // 1. The number of partitions on a single server may be greater than the maximum number of partitions for a single RPC and needs to be split into multiple requests
        // 2. Each partition request is removed from the request list as soon as it completes, so each request is split into multiple requests, each starting with the first element
        // 3. Partition request completion condition: regardless of success, as long as no breakpoint message is returned, the request is considered completed
        while (! stop_flag_ && OB_SUCCESS == ret && svr_req.locate_req_list_.count() > 0) {
          // 一次请求的最大个数
          int64_t item_cnt_limit = RpcReq::ITEM_CNT_LMT;
          int64_t req_cnt = std::min(svr_req.locate_req_list_.count(), item_cnt_limit);

          // A single request does not guarantee that all partition requests will be successful, and partition requests that return a breakpoint message need to be retried immediately
          // Note: A separate loop must be used here to ensure that the partition in the retry request is the same "breakpoint partition",
          // if the requested partition is not the same "breakpoint partition" but a new partition is added, the server will have
          // to scan the file from the "head" again and the breakpoint information will be invalid.
          while (! stop_flag_ && OB_SUCCESS == ret && req_cnt > 0) {
            // Set different trace ids for different requests
            ObLogTraceIdGuard trace_guard;

            RpcReq rpc_req;
            int64_t succ_req_cnt = 0;

            // Build request parameters
            if (OB_FAIL(build_request_params_(rpc_req, svr_req, req_cnt))) {
              LOG_ERROR("build request params fail", KR(ret), K(rpc_req), K(req_cnt), K(svr_req));
            }
            // Executing RPC requests
            else if (OB_FAIL(do_rpc_and_dispatch_(*(rpc_), rpc_req, svr_req, succ_req_cnt))) {
              LOG_ERROR("do rpc and dispatch fail", KR(ret), K(rpc_req), K(svr_req), K(succ_req_cnt));
            } else {
              // One request completed
              req_cnt -= succ_req_cnt;
            }
          }
        }
      }
    }
  }

  if (stop_flag_) {
		ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogStartLogIdLocator::build_request_params_(RpcReq &req,
    const SvrReq &svr_req,
    const int64_t req_cnt)
{
  int ret = OB_SUCCESS;
  int64_t total_cnt = svr_req.locate_req_list_.count();

  req.reset();

  for (int64_t index = 0; OB_SUCCESS == ret && index < req_cnt && index < total_cnt; ++index) {
    StartLogIdLocateReq *request = svr_req.locate_req_list_.at(index);
    StartLogIdLocateReq::SvrItem *svr_item = NULL;

    if (OB_ISNULL(request)) {
      LOG_ERROR("invalid request", K(index), "req_list", svr_req.locate_req_list_);
      ret = OB_ERR_UNEXPECTED;
    }
    // Get the current SvrItem corresponding to the partition request
    else if (OB_FAIL(request->cur_svr_item(svr_item))) {
      LOG_ERROR("get current server item fail", KR(ret), KPC(request));
    }
    // Verify the validity of the SvrItem, to avoid setting the breakpoint information incorrectly, the server is required to match
    else if (OB_ISNULL(svr_item) || OB_UNLIKELY(svr_item->svr_ != svr_req.svr_)) {
      LOG_ERROR("invalid svr_item which does not match SvrReq", KPC(svr_item), K(svr_req),
          KPC(request));
      ret = OB_ERR_UNEXPECTED;
    } else {
      RpcReq::Param param;
      // Maintaining breakpoint information
      param.reset(request->pkey_, request->start_tstamp_, svr_item->breakinfo_);

      // Requires append operation to be successful
      if (OB_FAIL(req.append_param(param))) {
        LOG_ERROR("append param fail", KR(ret), K(req_cnt), K(index), K(req), K(param));
      }
    }
  }
  return ret;
}

int ObLogStartLogIdLocator::do_rpc_and_dispatch_(
    IObLogRpc &rpc,
    RpcReq &rpc_req,
    SvrReq &svr_req,
    int64_t &succ_req_cnt)
{
  int ret = OB_SUCCESS;
  int rpc_err = OB_SUCCESS;
  int svr_err = OB_SUCCESS;
  RpcRes rpc_res;
  const int64_t request_cnt = rpc_req.get_params().count();
  int64_t rpc_timeout = ATOMIC_LOAD(&g_rpc_timeout);
  TraceIdType *trace_id = ObCurTraceId::get_trace_id();

  succ_req_cnt = 0;

  rpc_err = rpc.req_start_log_id_by_tstamp(svr_req.svr_, rpc_req, rpc_res, rpc_timeout);

  // send rpc fail
  if (OB_SUCCESS != rpc_err) {
    LOG_ERROR("rpc request start log id by tstamp fail, rpc error",
        K(rpc_err), "svr", svr_req.svr_, K(rpc_req), K(rpc_res));
  }
  // observer handle fail
  else if (OB_SUCCESS != (svr_err = rpc_res.get_err())) {
    LOG_ERROR("rpc request start log id by tstamp fail, server error",
        K(svr_err), "svr", svr_req.svr_, K(rpc_req), K(rpc_res));
  }
  // Both the RPC and server return success, requiring the number of results returned to match the number of requests
  else {
    const int64_t result_cnt = rpc_res.get_results().count();

    if (request_cnt != result_cnt) {
      LOG_ERROR("result count does not equal to request count",
          K(request_cnt), K(result_cnt), K(rpc_req), K(rpc_res));
      ret = OB_ERR_UNEXPECTED;
    }
  }

  if (OB_SUCCESS == ret) {
    // Scanning of arrays in reverse order to support deletion of completed partition requests
    for (int64_t idx = request_cnt - 1; OB_SUCCESS == ret && idx >= 0; idx--) {
      bool has_break_info = false;
      int partition_err = OB_SUCCESS;
      const obrpc::BreakInfo *bkinfo = NULL;
      obrpc::BreakInfo default_breakinfo;
      uint64_t start_log_id = OB_INVALID_ID;
      int64_t start_log_tstamp = OB_INVALID_TIMESTAMP;
      StartLogIdLocateReq *request = svr_req.locate_req_list_.at(idx);

      if (OB_ISNULL(request)) {
        LOG_ERROR("invalid request in server request list", K(request), K(idx),
            K(request_cnt), "req_list", svr_req.locate_req_list_);
        ret = OB_ERR_UNEXPECTED;
      }
      // Set an invalid start log id if the RPC or server returns a failure
      else if (OB_SUCCESS != rpc_err || OB_SUCCESS != svr_err) {
        default_breakinfo.reset();

        partition_err = OB_SUCCESS;
        bkinfo = &default_breakinfo;
        start_log_id = OB_INVALID_ID;
        start_log_tstamp = OB_INVALID_TIMESTAMP;
      }
      // If the result is valid, get the corresponding result and set the result
      else {
        const RpcRes::Result &result = rpc_res.get_results().at(idx);

        has_break_info = (OB_EXT_HANDLE_UNFINISH == result.err_);

        partition_err = result.err_;
        bkinfo = &result.break_info_;
        start_log_id = result.start_log_id_;
        start_log_tstamp = result.start_log_ts_;
      }

      if (OB_SUCCESS == ret) {
        // set result
        if (OB_FAIL(request->set_result(svr_req.svr_, rpc_err, svr_err,
            partition_err, *bkinfo, start_log_id, start_log_tstamp, trace_id))) {
          LOG_ERROR("request set result fail", KR(ret), "svr", svr_req.svr_,
              K(rpc_err), K(svr_err), K(partition_err),
              KPC(bkinfo), K(start_log_id), K(start_log_tstamp), KPC(request), K(trace_id));
        }
        // For requests without breakpoint information, dispatch directly and remove from the request array
        else if (! has_break_info) {
          if (OB_FAIL(dispatch_worker_(request))) {
            LOG_ERROR("dispatch worker fail", KR(ret), KPC(request));
          } else if (OB_FAIL(svr_req.locate_req_list_.remove(idx))) {
            LOG_ERROR("remove from request list fail", KR(ret), K(idx),
                "req_list", svr_req.locate_req_list_);
          } else {
            succ_req_cnt++;
            request = NULL;
          }
        } else {
          // Do not process requests with break info
          LOG_DEBUG("start log id locate done with break info", KPC(request));
        }
      }
    }
  }

  return ret;
}


//////////////////////////// ObLogStartLogIdLocator::WorkerData ////////////////////////////

int ObLogStartLogIdLocator::WorkerData::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(svr_req_map_.init(ObModIds::OB_LOG_START_LOG_ID_LOCATOR))) {
    LOG_ERROR("init request map fail", KR(ret));
  } else {
    svr_req_list_.set_label(ObModIds::OB_LOG_START_LOG_ID_LOCATOR);
    svr_req_list_.reset();
  }
  return ret;
}

void ObLogStartLogIdLocator::WorkerData::destroy()
{
  svr_req_list_.reset();
  (void)svr_req_map_.destroy();
}


//////////////////////////// StartLogIdLocateReq ////////////////////////////

void StartLogIdLocateReq::reset()
{
  set_state(IDLE);
  pkey_.reset();
  start_tstamp_ = OB_INVALID_TIMESTAMP;
  svr_list_.reset();
  svr_list_consumed_ = 0;
  result_svr_list_idx_ = -1;
  cur_max_start_log_id_ = OB_INVALID_ID;
  cur_max_start_log_tstamp_ = OB_INVALID_TIMESTAMP;
  succ_locate_count_ = 0;
}

void StartLogIdLocateReq::reset(const common::ObPartitionKey &pkey, const int64_t start_tstamp)
{
  reset();
  pkey_ = pkey;
  start_tstamp_ = start_tstamp;
}

int StartLogIdLocateReq::next_svr_item(SvrItem *&svr_item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(svr_list_consumed_ < 0)) {
    LOG_ERROR("invalid parameter", K(svr_list_consumed_));
    ret = OB_INDEX_OUT_OF_RANGE;
  } else if (svr_list_consumed_ >= svr_list_.count()) {
    ret = OB_ITER_END;
  } else {
    svr_item = &(svr_list_.at(svr_list_consumed_));
    svr_list_consumed_++;
  }

  return ret;
}

int StartLogIdLocateReq::cur_svr_item(SvrItem *&svr_item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(svr_list_consumed_ <= 0 || svr_list_consumed_ > svr_list_.count())) {
    LOG_ERROR("index out of range", K(svr_list_consumed_), K(svr_list_.count()));
    ret = OB_INDEX_OUT_OF_RANGE;
  } else {
    svr_item = &(svr_list_.at(svr_list_consumed_ - 1));
  }
  return ret;
}

void StartLogIdLocateReq::check_locate_result_(const int64_t start_log_tstamp,
    const uint64_t start_log_id,
    const common::ObAddr &svr,
    bool &is_consistent) const
{
  // default to be consistent
  is_consistent = true;

  // There may be a bug in the OB where the logs located are large, add checks here and report an error if multiple locations return logs larger than the start-up timestamp
  if (start_tstamp_ > 0
      && start_log_tstamp >= start_tstamp_
      && cur_max_start_log_tstamp_ >= start_tstamp_
      && start_log_tstamp != cur_max_start_log_tstamp_) {
    LOG_ERROR("start log id locate results from different servers are not consistent, "
        "may be OceanBase server BUG, need check manually",
        K_(pkey), K(svr), K(start_log_id), K(start_log_tstamp), K(cur_max_start_log_tstamp_),
        K(start_tstamp_), K(cur_max_start_log_id_), K(svr_list_consumed_),
        K(succ_locate_count_), K(svr_list_));
    // get un-consistent result
    is_consistent = false;
  } else {
    is_consistent = true;
  }
}

int StartLogIdLocateReq::set_result(const common::ObAddr &svr,
    const int rpc_err,
    const int svr_err,
    const int part_err,
    const obrpc::BreakInfo &breakinfo,
    const uint64_t start_log_id,
    const int64_t start_log_tstamp,
    const TraceIdType *trace_id)
{
  int ret = OB_SUCCESS;
  SvrItem *item = NULL;

  if (OB_FAIL(cur_svr_item(item)) || OB_ISNULL(item)) {
    LOG_ERROR("get current server item fail", KR(ret), KPC(item));
    ret = (OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret);
  } else if (OB_UNLIKELY(svr != item->svr_)) {
    LOG_ERROR("server does not match, result is invalid", K(svr), KPC(item));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int new_part_err = part_err;

    // If it returns less than a lower bound error and the lower bound is 1, then it has a start log id of 1
    if (OB_ERR_OUT_OF_LOWER_BOUND == part_err && 1 == start_log_id) {
      new_part_err = OB_SUCCESS;
    }

    item->set_result(rpc_err, svr_err, new_part_err, breakinfo, start_log_id, start_log_tstamp, trace_id);

    // If the result is valid, set a valid server item index
    if (OB_SUCCESS == rpc_err && OB_SUCCESS == svr_err && OB_SUCCESS == new_part_err) {
      // No need to update results by default
      bool need_update_result = false;

      // Increase the number of successfully located servers
      ++succ_locate_count_;

      // First successful locating start log id, results need to be recorded
      if (OB_INVALID_ID == cur_max_start_log_id_) {
        need_update_result = true;
      } else {
        bool is_consistent = true;

        // Verify the second and subsequent results for incorrect results
        check_locate_result_(start_log_tstamp, start_log_id, svr, is_consistent);

        if (OB_UNLIKELY(! is_consistent)) {
          // Scenarios with inconsistent processing results
          if (TCONF.skip_start_log_id_locator_result_consistent_check) {
            // Skip inconsistent data checks
            //
            // For multiple logs with a timestamp greater than the starting timestamp, take the smallest value to ensure the result is safe
            if (start_log_tstamp < cur_max_start_log_tstamp_ && start_log_tstamp >= start_tstamp_) {
              // Overwrite the previous positioning result with the current result
              need_update_result = true;
            }
          } else {
            ret = OB_INVALID_DATA;
          }
        } else {
          // The result is normal, updated maximum log information
          if (start_log_id > cur_max_start_log_id_) {
            need_update_result = true;
          }
        }
      }

      // Update final location log result: Use current server location result as final location result
      if (OB_SUCCESS == ret && need_update_result) {
        cur_max_start_log_id_ = start_log_id;
        cur_max_start_log_tstamp_ = start_log_tstamp;
        result_svr_list_idx_ = (svr_list_consumed_ - 1);
      }
    }

    LOG_INFO("start log id locate request of one server is done",
        KR(ret),
        K_(pkey), "svr", item->svr_, K(start_log_id), K(start_log_tstamp), K_(start_tstamp),
        "delta", start_log_tstamp - start_tstamp_,
        "rpc_err", ob_error_name(rpc_err),
        "svr_err", ob_error_name(svr_err),
        "part_err", ob_error_name(new_part_err),
        K(cur_max_start_log_id_), K(cur_max_start_log_tstamp_),
        K(result_svr_list_idx_), K(succ_locate_count_),
        K_(svr_list_consumed),
        "svr_cnt", svr_list_.count());
  }

  return ret;
}

bool StartLogIdLocateReq::is_request_ended(const int64_t locate_count) const
{
  bool bool_ret = false;

  // Ending conditions.
  // 1. all servers are exhausted
  // 2. or the required number of servers has been located
  bool_ret = (svr_list_.count() <= svr_list_consumed_)
      || (succ_locate_count_ == locate_count);

  return bool_ret;
}

bool StartLogIdLocateReq::get_result(uint64_t &start_log_id, common::ObAddr &svr)
{
  start_log_id = common::OB_INVALID_ID;
  bool succeed = false;

  if (result_svr_list_idx_ >= 0 && result_svr_list_idx_ < svr_list_.count()) {
    succeed = true;
    SvrItem &item = svr_list_.at(result_svr_list_idx_);
    start_log_id = item.start_log_id_;
    svr = item.svr_;
  } else if (svr_list_.count() == svr_list_consumed_) {
        // Handle all server return lower bound cases
    // FIXME: In version 2.0, one log is no longer guaranteed to be written per major version, and it is probable that all logs for a cold partition are recycled,
    // with only the largest log information ever recorded in saved storage info. This inevitably leads to a serious problem: if all the logs of a cold partition are recycled,
    // and a log is written at that point, then the log location request before that log will fail. To solve this type of problem, the following solutions complement each other.
    // 1. observer maintains accurate local log service information to ensure that locates do not fail. This feature will not be available until the ilog refactoring
    // 2. until 1 is completed, combined with the last version of log information recorded in saved storage info, to ensure that the last version of the log is located
    // 3. but 2 does not solve the problem completely, once the partition is minimally freeze, then there are also problems.For this reason, liboblog needs a further protection solution.
    // When all servers return less than the lower bound, and the startup timestamp is not too old (say within 2 hours of the current time), we assume that the observer
    // will not recycle the logs during this time, then if the server returns less than the lower bound at this point, then the logs must be served. Therefore, we can safely start from the lower bound log.
    // 4. But in practice it is not guaranteed that all servers will return less than the lower bound, it is possible that some servers will not serve this log, in order
    // to be able to operate and maintain in this case, we need to add a configuration item, whether or not to force the start log id to be considered successful
    // and force the start with the minimum log id, so as to facilitate operation and maintenance
    uint64_t min_start_log_id = OB_INVALID_ID;
    ObAddr  min_start_log_svr;
    int64_t lower_bound_svr_cnt = 0;

    bool enable_force_start_mode =
          ATOMIC_LOAD(&ObLogStartLogIdLocator::g_enable_force_start_mode);

    // Iterate through all servers to check if all of them return less than the lower bound
    // If some of the servers fail in their requests, or if the RPC fails, then do not continue and assume that there is a problem with that server.
    // Because further protection is provided by options 3 and 4, there is no requirement for a server to fail and for all servers to be online and return less than the lower bound.
    for (int64_t index = 0; index < svr_list_.count(); index++) {
      SvrItem &item = svr_list_.at(index);
      if (item.rpc_executed_
          && OB_SUCCESS == item.rpc_err_
          && OB_SUCCESS == item.svr_err_
          && OB_ERR_OUT_OF_LOWER_BOUND == item.partition_err_) {
        if (OB_INVALID_ID == min_start_log_id || min_start_log_id > item.start_log_id_) {
          min_start_log_id = item.start_log_id_;
          min_start_log_svr = item.svr_;
        }
        lower_bound_svr_cnt++;
      }
    }


    int64_t start_tstamp_delay_time = get_timestamp() - start_tstamp_;
    int64_t observer_clog_save_time =
      ATOMIC_LOAD(&ObLogStartLogIdLocator::g_observer_clog_save_time);

    if (lower_bound_svr_cnt == svr_list_.count() && OB_INVALID_ID != min_start_log_id) {
      if (start_tstamp_delay_time > observer_clog_save_time) {
        LOG_ERROR("start tstamp is too old, can not force-startup "
            "when all server is out of lower bound",
            K(start_tstamp_delay_time), K(observer_clog_save_time), K(start_tstamp_),
            K(pkey_), "svr_cnt", svr_list_.count(), K_(svr_list_consumed),
            K_(result_svr_list_idx), K_(svr_list));
      } else {
        LOG_WARN("****** FIXME ******* all server is out of lower bound. "
            "we will force-startup. this might lose data.",
            K_(pkey), K_(start_tstamp), K(min_start_log_id), K(min_start_log_svr),
            K(observer_clog_save_time), "svr_cnt", svr_list_.count(), K_(svr_list_consumed),
            K_(result_svr_list_idx), K_(svr_list));

        start_log_id = min_start_log_id;
        svr = min_start_log_svr;
        succeed = true;
      }
    }

    // Forced start mode is only required when option 3 is unsuccessful
    if (! succeed) {
      // Forced mode take in effect
      if (enable_force_start_mode) {
        // At least one server returns OB_ERR_OUT_OF_LOWER_BOUND
        if (lower_bound_svr_cnt > 0) {
          if (start_tstamp_delay_time > observer_clog_save_time) {
            LOG_ERROR("start tstamp is too old, can not force-startup "
                "when handle enable force start mode",
                K(enable_force_start_mode), K(lower_bound_svr_cnt),
                K(start_tstamp_delay_time), K(observer_clog_save_time), K(start_tstamp_),
                K(pkey_), "svr_cnt", svr_list_.count(), K_(svr_list_consumed),
                K_(result_svr_list_idx), K_(svr_list));
          } else {
            LOG_ERROR("at least one server is out of lower bound. "
                "we will force-startup. this might lose data.",
                K(enable_force_start_mode), K(lower_bound_svr_cnt),
                K_(pkey), K_(start_tstamp), K(min_start_log_id), K(min_start_log_svr),
                K(observer_clog_save_time), "svr_cnt", svr_list_.count(), K_(svr_list_consumed),
                K_(result_svr_list_idx), K_(svr_list));

            start_log_id = min_start_log_id;
            svr = min_start_log_svr;
            succeed = true;
          }
        } else {
          LOG_ERROR("no one server is out of lower bound. we can not force-startup.",
              K(enable_force_start_mode), K(lower_bound_svr_cnt),
              K_(pkey), K_(start_tstamp), K(min_start_log_id), K(min_start_log_svr),
              K(observer_clog_save_time), "svr_cnt", svr_list_.count(), K_(svr_list_consumed),
              K_(result_svr_list_idx), K_(svr_list));
        }
      }
    }
  }

  if (! succeed) {
    LOG_ERROR("request start log id from all server fail", K_(pkey),
        K_(start_tstamp), "svr_cnt", svr_list_.count(), K_(svr_list_consumed),
        K_(result_svr_list_idx), K_(svr_list));
  }

  return succeed;
}

void StartLogIdLocateReq::SvrItem::reset()
{
  svr_.reset();
  rpc_executed_ = false;
  rpc_err_ = OB_SUCCESS;
  svr_err_ = OB_SUCCESS;
  partition_err_ = OB_SUCCESS;
  breakinfo_.reset();
  start_log_id_ = OB_INVALID_ID;
  start_log_tstamp_ = OB_INVALID_TIMESTAMP;
  trace_id_.reset();
}

void StartLogIdLocateReq::SvrItem::reset(const common::ObAddr &svr)
{
  reset();
  svr_ = svr;
}

// Set result.
void StartLogIdLocateReq::SvrItem::set_result(const int rpc_err,
    const int svr_err,
    const int partition_err,
    const obrpc::BreakInfo &breakinfo,
    const uint64_t start_log_id,
    const int64_t start_log_tstamp,
    const TraceIdType *trace_id)
{
  rpc_executed_ = true;
  rpc_err_ = rpc_err;
  svr_err_ = svr_err;
  partition_err_ = partition_err;
  breakinfo_ = breakinfo;
  start_log_id_ = start_log_id;
  start_log_tstamp_ = start_log_tstamp;

  if (OB_NOT_NULL(trace_id)) {
    trace_id_ = *trace_id;
  } else {
    trace_id_.reset();
  }
}

}
}
