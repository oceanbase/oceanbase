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

#include "ob_log_start_lsn_locator.h"
#include "logservice/restoreservice/ob_log_archive_piece_mgr.h"

#include "lib/allocator/ob_mod_define.h"          // ObModIds

#include "ob_log_instance.h"                      // IObLogErrHandler
#include "ob_log_trace_id.h"                      // ObLogTraceIdGuard
#include "ob_log_rpc.h"                           // IObLogRpc

namespace oceanbase
{
using namespace common;

namespace libobcdc
{

int64_t ObLogStartLSNLocator::g_batch_count =
    ObLogConfig::default_start_lsn_locator_batch_count;
int64_t ObLogStartLSNLocator::g_rpc_timeout =
    ObLogConfig::default_start_lsn_locator_rpc_timeout_sec * _SEC_;

ObLogStartLSNLocator::ObLogStartLSNLocator() :
    is_inited_(false),
    archive_dest_(),
    worker_cnt_(0),
    locate_count_(0),
    rpc_(NULL),
    err_handler_(NULL),
    worker_data_(NULL),
    allocator_(ObModIds::OB_LOG_START_LOG_ID_LOCATOR)
{
}

ObLogStartLSNLocator::~ObLogStartLSNLocator()
{
  destroy();
}

int ObLogStartLSNLocator::init(
    const int64_t worker_cnt,
    const int64_t locate_count,
    const ClientFetchingMode fetching_mode,
    const share::ObBackupPathString &archive_dest_str,
    IObLogRpc &rpc,
    IObLogErrHandler &err_handle)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogStartLSNLocator init twice", KR(ret));
  } else if (OB_UNLIKELY((worker_cnt_ = worker_cnt) <= 0)
      || OB_UNLIKELY(locate_count <= 0)
      || OB_UNLIKELY(MAX_THREAD_NUM < worker_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid worker cnt", KR(ret), K(worker_cnt), "max_thread_num", MAX_THREAD_NUM, K(locate_count));
  } else if (OB_FAIL(LocateWorker::init(worker_cnt, ObModIds::OB_LOG_START_LOG_ID_LOCATOR))) {
    LOG_ERROR("init locate worker fail", KR(ret), K(worker_cnt));
  } else {
    int64_t alloc_size = worker_cnt * sizeof(WorkerData);
    void *buf = ob_malloc(alloc_size, ObModIds::OB_LOG_START_LOG_ID_LOCATOR);

    if (OB_ISNULL(worker_data_ = static_cast<WorkerData *>(buf))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate memory fail", KR(ret), K(worker_data_), K(alloc_size), K(worker_cnt));
    } else {
      for (int64_t idx = 0, cnt = worker_cnt; OB_SUCCESS == ret && idx < cnt; ++idx) {
        new (worker_data_ + idx) WorkerData();
        WorkerData &data = worker_data_[idx];

        if (OB_FAIL(data.init())) {
          LOG_ERROR("init worker data fail", KR(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (is_direct_fetching_mode(fetching_mode) && OB_FAIL(archive_dest_.set(archive_dest_str))) {
        LOG_ERROR("set archive dest failed", KR(ret), K(archive_dest_str));
      }
    }

    if (OB_SUCCESS == ret) {
      locate_count_ = locate_count;
      rpc_ = &rpc;
      err_handler_ = &err_handle;

      is_inited_ = true;
      LOG_INFO("init ObLogStartLSNLocator succ", "thread_num", LocateWorker::get_thread_num());
    }
  }

  if (OB_SUCCESS != ret) {
    destroy();
  }

  return ret;
}

void ObLogStartLSNLocator::destroy()
{
  stop();

  is_inited_ = false;

  LocateWorker::destroy();
  archive_dest_.reset();
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

  LOG_INFO("destroy ObLogStartLSNLocator succ");
}

int ObLogStartLSNLocator::async_start_lsn_req(StartLSNLocateReq *req)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogStartLSNLocator not inited", KR(ret));
  } else if (OB_ISNULL(req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(req));
  } else if (OB_UNLIKELY(! req->is_state_idle())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid request, state is not IDLE", KR(ret), KPC(req));
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
int ObLogStartLSNLocator::dispatch_worker_(StartLSNLocateReq *req)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogStartLSNLocator not inited", KR(ret));
  } else if (OB_ISNULL(req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid request", KR(ret), K(req));
  } else if (OB_UNLIKELY(! req->is_state_req())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid request, state is not REQ", KR(ret), KPC(req), K(req->get_state()));
  } else {
    const ClientFetchingMode fetching_mode = req->get_fetching_mode();
    if (is_integrated_fetching_mode(fetching_mode)) {
      StartLSNLocateReq::SvrItem *item = NULL;

      // Mark DONE if the request is completed, or if all servers are requested.
      if (req->is_request_ended(locate_count_)) {
        // If the request ends, set to DONE
        // NOTE: After setting to DONE, no further access is possible
        LOG_DEBUG("start lsn locate request ended", KPC(req));
        req->set_state_done();
      } else if (OB_FAIL(req->next_svr_item(item)) || OB_ISNULL(item)) {
        LOG_ERROR("get next server item fail", KR(ret), KPC(req), K(item));
        ret = (OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret);
      } else {
        const uint64_t tenant_id = req->tls_id_.get_tenant_id();
        const ObAddr &request_svr = item->svr_;

        // Hashing by server to the corresponding worker thread
        // The purpose is to ensure that requests from the same server and tenant are aggregated
        uint64_t hash_val = 0;
        uint64_t svr_hash = request_svr.hash();
        hash_val = common::murmurhash(&tenant_id, sizeof(tenant_id), hash_val);
        hash_val = common::murmurhash(&svr_hash, sizeof(svr_hash), hash_val);

        LOG_DEBUG("dispatch start lsn locate request",
            "worker_idx", hash_val % worker_cnt_,
            K(request_svr),
            KPC(req));

        if (OB_FAIL(LocateWorker::push(req, hash_val))) {
          LOG_ERROR("push req to worker fail", KR(ret), KPC(req), K(hash_val), K(request_svr));
        } else {
          // done
        }
      }
    } else if (is_direct_fetching_mode(fetching_mode)) {
      static int64_t seq_no = 0;
      int64_t seq_no_local = ATOMIC_AAF(&seq_no, 1);
      if (OB_FAIL(LocateWorker::push(req, seq_no_local))) {
        LOG_ERROR("push req to worker fail", KR(ret), KPC(req), K(seq_no));
      } else { }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid fetching log mode", KR(ret), K(fetching_mode), KPC(req));
    }
  }

  return ret;
}

int ObLogStartLSNLocator::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogStartLSNLocator not inited", KR(ret));
  } else if (OB_FAIL(LocateWorker::start())) {
    LOG_ERROR("start locate worker fail", KR(ret));
  } else {
    LOG_INFO("ObLogStartLSNLocator start succ", K_(worker_cnt));
  }

  return ret;
}

void ObLogStartLSNLocator::stop()
{
  if (OB_LIKELY(is_inited_)) {
    LocateWorker::stop();
    LOG_INFO("ObLogStartLSNLocator stop succ");
  }
}

// start lsn locator worker thread
void ObLogStartLSNLocator::run(const int64_t thread_index)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogStartLSNLocator not inited", KR(ret));
  } else if (OB_UNLIKELY(thread_index < 0) || OB_UNLIKELY(thread_index >= worker_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "invalid thread index", KR(ret), K(thread_index), K(worker_cnt_));
  } else {
    WorkerData &data = worker_data_[thread_index];

    while (! stop_flag_ && OB_SUCCESS == ret) {
      if (OB_FAIL(do_retrieve_(thread_index, data))) {
        LOG_ERROR("retrieve request fail", KR(ret), K(thread_index));
      } else if (! data.has_valid_req()) {
        if (REACH_TIME_INTERVAL(30 * _SEC_)) {
          LOG_INFO("no request should be launch, ignore");
        }
      } else if (OB_FAIL(do_request_(data))) {
				if (OB_IN_STOP_STATE != ret) {
		      LOG_ERROR("do request fail", KR(ret));
				}
      }

      if (OB_SUCC(ret)) {
        cond_timedwait(thread_index, DATA_OP_TIMEOUT);
      }
    }

    if (stop_flag_) {
      ret = OB_IN_STOP_STATE;
    }
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
    LOG_ERROR("start lsn locator worker exit on fail", KR(ret), K(thread_index));
    if (OB_NOT_NULL(err_handler_)) {
      err_handler_->handle_error(ret,
          "start lsn locator worker exit on fail, ret=%d, thread_index=%ld",
          ret, thread_index);
    }
  }
}

void ObLogStartLSNLocator::configure(const ObLogConfig &config)
{
  int64_t start_lsn_locator_rpc_timeout_sec = config.start_lsn_locator_rpc_timeout_sec;
  int64_t start_lsn_locator_batch_count = config.start_lsn_locator_batch_count;

  ATOMIC_STORE(&g_rpc_timeout, start_lsn_locator_rpc_timeout_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(start_lsn_locator_rpc_timeout_sec));

  ATOMIC_STORE(&g_batch_count, start_lsn_locator_batch_count);
  LOG_INFO("[CONFIG]", K(start_lsn_locator_batch_count));
}

// Pop out data from Queue, then use Map, aggregating requests by server
int ObLogStartLSNLocator::do_retrieve_(const int64_t thread_index, WorkerData &worker_data)
{
  int ret = OB_SUCCESS;
  int64_t batch_count = ATOMIC_LOAD(&g_batch_count);

  for (int64_t cnt = 0; OB_SUCCESS == ret && !stop_flag_ && (cnt < batch_count); ++cnt) {
    StartLSNLocateReq *request = NULL;
    StartLSNLocateReq::SvrItem *item = NULL;
    SvrReq *svr_req = NULL;
    void *data = NULL;

    if (OB_FAIL(LocateWorker::pop(thread_index, data))) {
      if (OB_EAGAIN != ret) {
        LOG_ERROR("pop data from queue fail", KR(ret), K(thread_index), K(request));
      }
    } else if (OB_ISNULL(request = static_cast<StartLSNLocateReq *>(data))) {
      LOG_ERROR("request is NULL", K(request), K(thread_index));
      ret = OB_ERR_UNEXPECTED;
    } else {
      const ClientFetchingMode fetching_mode = request->get_fetching_mode();
      if (is_integrated_fetching_mode(fetching_mode)) {
        if (OB_FAIL(request->cur_svr_item(item)) || OB_ISNULL(item)) {
          LOG_ERROR("get current server item fail", KR(ret), KPC(request), K(item));
          ret = (OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret);
        } else if (OB_FAIL(get_svr_req_(worker_data, request->tls_id_.get_tenant_id(), item->svr_, svr_req)) || OB_ISNULL(svr_req)) {
          LOG_ERROR("get svr req fail", KR(ret), "actual_svr", item->svr_, K(svr_req));
          ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
        } else if (OB_FAIL(svr_req->push(request))) {
          LOG_ERROR("push request into request list fail", KR(ret), KPC(svr_req), K(request));
        } else {
          // succ
        }
      } else if (is_direct_fetching_mode(fetching_mode)) {
        DirectReqList &archive_req_list = worker_data.archive_req_list_;
        if (OB_FAIL(archive_req_list.push_back(request))) {
          LOG_ERROR("push request into archive request lis fail", KR(ret), K(request));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid fetching mode", KR(ret), K(fetching_mode), KPC(request));
      }
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

int ObLogStartLSNLocator::get_svr_req_(WorkerData &data,
    const uint64_t tenant_id,
    common::ObAddr &svr,
    SvrReq *&svr_req)
{
  int ret = OB_SUCCESS;
  SvrReqList &svr_req_list = data.svr_req_list_;
  SvrReqMap &svr_req_map = data.svr_req_map_;
  svr_req = NULL;
  TenantSvr tenant_svr(tenant_id, svr);

  // Fetching the corresponding record from the Map is preferred
  if (OB_FAIL(svr_req_map.get(tenant_svr, svr_req))) {
    // If the record does not exist, insert the corresponding record in the Array first, and then insert the object in the Array into the Map
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;

      // Assign a new request
      if (OB_ISNULL(svr_req = alloc_svr_req_(tenant_id, svr))) {
        LOG_ERROR("allocate svr request fail", K(svr));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(svr_req_list.push_back(svr_req))) {
        LOG_ERROR("push svr req into array fail", KR(ret), K(svr_req), K(svr_req_list));
      } else if (OB_FAIL(svr_req_map.insert(tenant_svr, svr_req))) {
        LOG_ERROR("insert svr req into map fail", KR(ret), K(tenant_svr), KPC(svr_req));
      }
    } else {
      LOG_ERROR("get svr req from map fail", KR(ret), K(tenant_svr));
    }
  } else {
    // succ
  }

  return ret;
}

ObLogStartLSNLocator::SvrReq *ObLogStartLSNLocator::alloc_svr_req_(
    const uint64_t tenant_id,
    const common::ObAddr &svr)
{
  SvrReq *svr_req = NULL;

  void *buf = allocator_.alloc(sizeof(SvrReq));
  if (OB_NOT_NULL(buf)) {
    svr_req = new(buf) SvrReq(tenant_id, svr);
  }

  return svr_req;
}

void ObLogStartLSNLocator::free_svr_req_(SvrReq *req)
{
  if (NULL != req) {
    req->~SvrReq();
    allocator_.free(req);
    req = NULL;
  }
}

void ObLogStartLSNLocator::free_all_svr_req_(WorkerData &data)
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

int ObLogStartLSNLocator::do_request_(WorkerData &data)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(do_integrated_request_(data))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("locate start lsn for integrated request failed", KR(ret));
    }
  } else if (OB_FAIL(do_direct_request_(data))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("locate start lsn for direct request failed", KR(ret));
    }
  }

  return ret;
}

int ObLogStartLSNLocator::do_integrated_request_(WorkerData &data)
{
  int ret = OB_SUCCESS;
  SvrReqList &svr_req_list = data.svr_req_list_;

  if (OB_ISNULL(rpc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid rpc handle", KR(ret), K(rpc_));
  } else {
    for (int64_t idx = 0, cnt = svr_req_list.count();
				 ! stop_flag_ && OB_SUCCESS == ret && (idx < cnt); ++idx) {
      if (OB_ISNULL(svr_req_list.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("svr request is NULL", KR(ret), K(idx), K(cnt), K(svr_req_list));
      } else {
        SvrReq &svr_req = *(svr_req_list.at(idx));

        // Requesting a single server
        // 1. The number of partitions on a single server may be greater than the maximum number of partitions for a single RPC and needs to be split into multiple requests
        // 2. Each partition request is removed from the request list as soon as it completes, so each request is split into multiple requests, each starting with the first element
        // 3. Partition request completion condition: regardless of success, as long as no breakpoint message is returned, the request is considered completed
        while (! stop_flag_ && OB_SUCCESS == ret && svr_req.locate_req_list_.count() > 0) {
          // maximum request count
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
            } else if (svr_req.locate_req_list_.count() <= 0) {
              if (REACH_TIME_INTERVAL(30 * _SEC_)) {
                LOG_INFO("no svr_req to request, ignore", K(svr_req));
              }
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

int ObLogStartLSNLocator::do_direct_request_(WorkerData &data)
{
  int ret = OB_SUCCESS;
  DirectReqList &archive_req_lst = data.archive_req_list_;
  const int64_t lst_cnt = archive_req_lst.count();
  for (int64_t idx = lst_cnt - 1;
          OB_SUCC(ret) && ! stop_flag_ && idx >= 0; --idx) {
    LSN start_lsn;
    StartLSNLocateReq *req = archive_req_lst.at(idx);
    if (OB_ISNULL(req)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("archive req is null", KR(ret), K(idx));
    } else {
      logservice::ObLogArchivePieceContext piece_ctx;
      const ObLSID &ls_id = req->tls_id_.get_ls_id();
      const uint64_t tenant_id = req->tls_id_.get_tenant_id();
      const int64_t start_ts_ns = req->start_tstamp_ns_;
      StartLSNLocateReq::DirectLocateResult &loc_rs = req->archive_locate_rs_;
      share::SCN start_scn;
      if (OB_FAIL(piece_ctx.init(ls_id, archive_dest_))) {
        LOG_ERROR("init piece ctx failed", KR(ret), K(ls_id), K(tenant_id), KPC(req), K_(archive_dest));
      } else if (OB_FAIL(start_scn.convert_from_ts(start_ts_ns/1000L))) {
        LOG_ERROR("convert ts to scn failed", KR(ret), K(start_ts_ns), K(ls_id), K(tenant_id));
      } else if (OB_FAIL(piece_ctx.seek(start_scn, start_lsn))) {
        LOG_ERROR("piece ctx seek start lsn failed", KR(ret), K(start_scn), K(ls_id), K(tenant_id));
      }
      // set result no matter start lsn is successfully located or not
      loc_rs.reset(start_lsn, ret);
      // overwrite the return code
      ret = OB_SUCCESS;
      // no need to call dispatch_worker_, only do_direct_request_ once.
      // whether retry upon failure relies on the implement of need_locate_start_lsn
      req->set_state_done();
      archive_req_lst.remove(idx);
    }
  }

  if (stop_flag_) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogStartLSNLocator::build_request_params_(RpcReq &req,
    const SvrReq &svr_req,
    const int64_t req_cnt)
{
  int ret = OB_SUCCESS;
  int64_t total_cnt = svr_req.locate_req_list_.count();
  req.reset();

  for (int64_t index = 0; OB_SUCCESS == ret && ! stop_flag_ && index < req_cnt && index < total_cnt; ++index) {
    StartLSNLocateReq *request = svr_req.locate_req_list_.at(index);
    StartLSNLocateReq::SvrItem *svr_item = NULL;

    if (OB_ISNULL(request)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid request", KR(ret), K(index), "req_list", svr_req.locate_req_list_);
    }
    // Get the current SvrItem corresponding to the partition request
    else if (OB_FAIL(request->cur_svr_item(svr_item))) {
      LOG_ERROR("get current server item fail", KR(ret), KPC(request));
    }
    // Verify the validity of the SvrItem, to avoid setting the breakpoint information incorrectly, the server is required to match
    else if (OB_ISNULL(svr_item) || OB_UNLIKELY(svr_item->svr_ != svr_req.svr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid svr_item which does not match SvrReq", KR(ret), KPC(svr_item), K(svr_req),
          KPC(request));
    } else {
      RpcReq::LocateParam locate_param;
      locate_param.reset(request->tls_id_.get_ls_id(), request->start_tstamp_ns_);

      // Requires append operation to be successful
      if (OB_FAIL(req.append_param(locate_param))) {
        LOG_ERROR("append param fail", KR(ret), K(req_cnt), K(index), K(req), K(locate_param));
      }
    }
  }

  return ret;
}

int ObLogStartLSNLocator::do_rpc_and_dispatch_(
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

  rpc_err = rpc.req_start_lsn_by_tstamp(svr_req.tenant_id_, svr_req.svr_, rpc_req, rpc_res, rpc_timeout);

  // send rpc fail
  if (OB_SUCCESS != rpc_err) {
    LOG_ERROR("rpc request start lsn by tstamp fail, rpc error",
        K(rpc_err), "svr", svr_req.svr_, K(rpc_req), K(rpc_res));
  }
  // observer handle fail
  else if (OB_SUCCESS != (svr_err = rpc_res.get_err())) {
    LOG_ERROR("rpc request start lsn by tstamp fail, server error",
        K(svr_err), "svr", svr_req.svr_, K(rpc_req), K(rpc_res));
  }
  // Both the RPC and server return success, requiring the number of results returned to match the number of requests
  else {
    const int64_t result_cnt = rpc_res.get_results().count();

    if (request_cnt != result_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("result count does not equal to request count", KR(ret),
          K(request_cnt), K(result_cnt), K(rpc_req), K(rpc_res));
    }
  }

  if (OB_SUCCESS == ret) {
    // Scanning of arrays in reverse order to support deletion of completed ls requests
    for (int64_t idx = request_cnt - 1; OB_SUCCESS == ret && ! stop_flag_ && idx >= 0; idx--) {
      int ls_err = OB_SUCCESS;
      palf::LSN start_lsn;
      int64_t start_log_tstamp = OB_INVALID_TIMESTAMP;
      StartLSNLocateReq *request = svr_req.locate_req_list_.at(idx);

      if (OB_ISNULL(request)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid request in server request list", KR(ret), K(request), K(idx),
            K(request_cnt), "req_list", svr_req.locate_req_list_);
      }
      // Set an invalid start lsn if the RPC or server returns a failure
      else if (OB_SUCCESS != rpc_err || OB_SUCCESS != svr_err) {
        ls_err = OB_SUCCESS;
        start_lsn.reset();
        start_log_tstamp = OB_INVALID_TIMESTAMP;
      }
      // If the result is valid, get the corresponding result and set the result
      else {
        const RpcRes::LocateResult &result = rpc_res.get_results().at(idx);
        ls_err = result.err_;
        start_lsn = result.start_lsn_;
        start_log_tstamp = result.start_ts_ns_;
      }

      if (OB_SUCCESS == ret) {
        // set result
        if (OB_FAIL(request->set_result(svr_req.svr_, rpc_err, svr_err,
            ls_err, start_lsn, start_log_tstamp, trace_id))) {
          LOG_ERROR("request set result fail", KR(ret), "svr", svr_req.svr_,
              K(rpc_err), K(svr_err), K(ls_err),
              K(start_lsn), K(start_log_tstamp), KPC(request), K(trace_id));
        }
        // If set_result fail, dispatch directly and remove from the request array
        else {
          if (OB_FAIL(dispatch_worker_(request))) {
            LOG_ERROR("dispatch worker fail", KR(ret), KPC(request));
          } else if (OB_FAIL(svr_req.locate_req_list_.remove(idx))) {
            LOG_ERROR("remove from request list fail", KR(ret), K(idx),
                "req_list", svr_req.locate_req_list_);
          } else {
            succ_req_cnt++;
            request = NULL;
          }
        }
      }
    }
  }

  return ret;
}


//////////////////////////// ObLogStartLSNLocator::WorkerData ////////////////////////////

int ObLogStartLSNLocator::WorkerData::init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(svr_req_map_.init(ObModIds::OB_LOG_START_LOG_ID_LOCATOR))) {
    LOG_ERROR("init request map fail", KR(ret));
  } else {
    svr_req_list_.set_label(ObModIds::OB_LOG_START_LOG_ID_LOCATOR);
    svr_req_list_.reset();
    archive_req_list_.reset();
  }

  return ret;
}

void ObLogStartLSNLocator::WorkerData::destroy()
{
  svr_req_list_.reset();
  (void)svr_req_map_.destroy();
  archive_req_list_.destroy();
}


//////////////////////////// StartLSNLocateReq ////////////////////////////

void StartLSNLocateReq::reset()
{
  set_state(IDLE);
  tls_id_.reset();
  start_tstamp_ns_ = OB_INVALID_TIMESTAMP;
  svr_list_.reset();
  svr_list_consumed_ = 0;
  result_svr_list_idx_ = -1;
  cur_max_start_lsn_.reset();
  cur_max_start_log_tstamp_ = OB_INVALID_TIMESTAMP;
  succ_locate_count_ = 0;
  fetching_mode_ = ClientFetchingMode::FETCHING_MODE_UNKNOWN;
  archive_locate_rs_.reset();
}

void StartLSNLocateReq::reset(
    const logservice::TenantLSID &tls_id,
    const int64_t start_tstamp_ns)
{
  reset();
  tls_id_ = tls_id;
  start_tstamp_ns_ = start_tstamp_ns;
}

int StartLSNLocateReq::next_svr_item(SvrItem *&svr_item)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(svr_list_consumed_ < 0)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_ERROR("invalid parameter", KR(ret), K(svr_list_consumed_));
  } else if (svr_list_consumed_ >= svr_list_.count()) {
    ret = OB_ITER_END;
  } else {
    svr_item = &(svr_list_.at(svr_list_consumed_));
    svr_list_consumed_++;
  }

  return ret;
}

int StartLSNLocateReq::cur_svr_item(SvrItem *&svr_item)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(svr_list_consumed_ <= 0 || svr_list_consumed_ > svr_list_.count())) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_ERROR("index out of range", KR(ret), K(svr_list_consumed_), K(svr_list_.count()));
  } else {
    svr_item = &(svr_list_.at(svr_list_consumed_ - 1));
  }

  return ret;
}

void StartLSNLocateReq::check_locate_result_(const int64_t start_log_tstamp,
    const palf::LSN &start_lsn,
    const common::ObAddr &svr,
    bool &is_consistent) const
{
  // default to be consistent
  is_consistent = true;

  // There may be a bug in the OB where the logs located are large, add checks here and report an error if multiple locations return logs larger than the start-up timestamp
  if (start_tstamp_ns_ > 0
      && start_log_tstamp >= start_tstamp_ns_
      && cur_max_start_log_tstamp_ >= start_tstamp_ns_
      && start_log_tstamp != cur_max_start_log_tstamp_) {
    LOG_ERROR_RET(OB_ERROR, "start lsn locate results from different servers are not consistent, "
        "may be OceanBase server BUG, need check manually",
        K_(tls_id), K(svr), K(start_lsn), K(start_log_tstamp), K(cur_max_start_log_tstamp_),
        K(start_tstamp_ns_), K(cur_max_start_lsn_), K(svr_list_consumed_),
        K(succ_locate_count_), K(svr_list_));
    // get un-consistent result
    is_consistent = false;
  } else {
    is_consistent = true;
  }
}

int StartLSNLocateReq::set_result(const common::ObAddr &svr,
    const int rpc_err,
    const int svr_err,
    const int ls_err,
    const palf::LSN &start_lsn,
    const int64_t start_log_tstamp,
    const TraceIdType *trace_id)
{
  int ret = OB_SUCCESS;
  SvrItem *item = NULL;

  if (OB_FAIL(cur_svr_item(item)) || OB_ISNULL(item)) {
    LOG_ERROR("get current server item fail", KR(ret), KPC(item));
    ret = (OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret);
  } else if (OB_UNLIKELY(svr!= item->svr_)) {
    LOG_ERROR("server does not match, result is invalid", K(svr), KPC(item));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int new_ls_err = ls_err;

    // If it returns less than a lower bound error and the lower bound is 1, then it has a start lsn of 1
    if (OB_ERR_OUT_OF_LOWER_BOUND == ls_err && palf::PALF_INITIAL_LSN_VAL == start_lsn.val_) {
      new_ls_err = OB_SUCCESS;
    }

    item->set_result(rpc_err, svr_err, new_ls_err, start_lsn, start_log_tstamp, trace_id);

    // If the result is valid, set a valid server item index
    if (OB_SUCCESS == rpc_err && OB_SUCCESS == svr_err && OB_SUCCESS == new_ls_err) {
      // No need to update results by default
      bool need_update_result = false;

      // Increase the number of successfully located servers
      ++succ_locate_count_;

      // First successful locating start lsn, results need to be recorded
      if (! cur_max_start_lsn_.is_valid()) {
        need_update_result = true;
      } else {
        bool is_consistent = true;

        // Verify the second and subsequent results for incorrect results
        check_locate_result_(start_log_tstamp, start_lsn, svr, is_consistent);

        if (OB_UNLIKELY(! is_consistent)) {
          // Scenarios with inconsistent processing results
          if (TCONF.skip_start_lsn_locator_result_consistent_check) {
            // Skip inconsistent data checks
            //
            // For multiple logs with a timestamp greater than the starting timestamp, take the smallest value to ensure the result is safe
            if (start_log_tstamp < cur_max_start_log_tstamp_ && start_log_tstamp >= start_tstamp_ns_) {
              // Overwrite the previous positioning result with the current result
              need_update_result = true;
            }
          } else {
            ret = OB_INVALID_DATA;
          }
        } else {
          // The result is normal, updated maximum log information
          if (start_lsn > cur_max_start_lsn_) {
            need_update_result = true;
          }
        }
      }

      // Update final location log result: Use current server location result as final location result
      if (OB_SUCCESS == ret && need_update_result) {
        cur_max_start_lsn_ = start_lsn;
        cur_max_start_log_tstamp_ = start_log_tstamp;
        result_svr_list_idx_ = (svr_list_consumed_ - 1);
      }
    }

    LOG_INFO("start lsn locate request of one server is done",
        KR(ret),
        K_(tls_id), "svr_identity", item->svr_, K(start_lsn), K(start_log_tstamp), K_(start_tstamp_ns),
        "delta", start_log_tstamp - start_tstamp_ns_,
        "rpc_err", ob_error_name(rpc_err),
        "svr_err", ob_error_name(svr_err),
        "ls_err", ob_error_name(new_ls_err),
        K(cur_max_start_lsn_), K(cur_max_start_log_tstamp_),
        K(result_svr_list_idx_), K(succ_locate_count_),
        K_(svr_list_consumed),
        "svr_cnt", svr_list_.count());
  }

  return ret;
}

bool StartLSNLocateReq::is_request_ended(const int64_t locate_count) const
{
  bool bool_ret = false;

  // Ending conditions.
  // 1. all servers are exhausted
  // 2. or the required number of servers has been located
  bool_ret = (svr_list_.count() <= svr_list_consumed_)
      || (succ_locate_count_ == locate_count);

  return bool_ret;
}

bool StartLSNLocateReq::get_result(palf::LSN &start_lsn, common::ObAddr &svr)
{
  start_lsn.reset();
  bool succeed = false;

  if (result_svr_list_idx_ >= 0 && result_svr_list_idx_ < svr_list_.count()) {
    succeed = true;
    SvrItem &item = svr_list_.at(result_svr_list_idx_);
    start_lsn = item.start_lsn_;
    svr = item.svr_;
  }

  if (! succeed) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "request start lsn from all server fail", K_(tls_id),
        K_(start_tstamp_ns), "svr_cnt", svr_list_.count(), K_(svr_list_consumed),
        K_(result_svr_list_idx), K_(svr_list));
  }

  return succeed;
}

void StartLSNLocateReq::get_direct_result(LSN &start_lsn, int &err)
{
  start_lsn = archive_locate_rs_.start_lsn_;
  err = archive_locate_rs_.loc_err_;
}

void StartLSNLocateReq::SvrItem::reset()
{
  svr_.reset();
  rpc_executed_ = false;
  rpc_err_ = OB_SUCCESS;
  svr_err_ = OB_SUCCESS;
  ls_err_ = OB_SUCCESS;
  start_lsn_.reset();
  start_log_tstamp_ = OB_INVALID_TIMESTAMP;
  trace_id_.reset();
}

void StartLSNLocateReq::SvrItem::reset(const common::ObAddr &svr)
{
  reset();
  svr_ = svr;
}

// Set result.
void StartLSNLocateReq::SvrItem::set_result(const int rpc_err,
    const int svr_err,
    const int ls_err,
    const palf::LSN &start_lsn,
    const int64_t start_log_tstamp,
    const TraceIdType *trace_id)
{
  rpc_executed_ = true;
  rpc_err_ = rpc_err;
  svr_err_ = svr_err;
  ls_err_ = ls_err;
  start_lsn_ = start_lsn;
  start_log_tstamp_ = start_log_tstamp;

  if (OB_NOT_NULL(trace_id)) {
    trace_id_ = *trace_id;
  } else {
    trace_id_.reset();
  }
}

}
}
