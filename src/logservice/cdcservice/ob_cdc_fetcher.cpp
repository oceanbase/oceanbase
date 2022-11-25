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

#define USING_LOG_PREFIX EXTLOG
#include "logservice/ob_log_service.h"          // ObLogService
#include "ob_cdc_service_monitor.h"
#include "ob_cdc_fetcher.h"
#include "ob_cdc_define.h"
#include "storage/tx_storage/ob_ls_handle.h"

namespace oceanbase
{
using namespace obrpc;
using namespace oceanbase::logservice;
using namespace oceanbase::palf;

namespace cdc
{
ObCdcFetcher::ObCdcFetcher()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    ls_service_(NULL)
{
}

ObCdcFetcher::~ObCdcFetcher()
{
  destroy();
}

int ObCdcFetcher::init(const uint64_t tenant_id,
    ObLSService *ls_service)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ls_service));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
    ls_service_ = ls_service;
  }

  return ret;
}

void ObCdcFetcher::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    tenant_id_ = OB_INVALID_TENANT_ID;
    ls_service_ = NULL;
  }
}

int ObCdcFetcher::fetch_log(const ObCdcLSFetchLogReq &req,
    ObCdcLSFetchLogResp &resp)
{
  int ret = OB_SUCCESS;
  FetchRunTime frt;
  const int64_t cur_tstamp = ObTimeUtility::current_time();

  // Generate this RPC ID using the current timestamp directly.
  ObLogRpcIDType rpc_id = cur_tstamp;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fetch_log req", KR(ret), K(req));
  } else if (OB_FAIL(frt.init(rpc_id, cur_tstamp, req.get_upper_limit_ts()))) {
    LOG_WARN("fetch runtime init error", KR(ret), K(rpc_id), K(req));
  } else {
    const ObLSID &ls_id = req.get_ls_id();
    const LSN &start_lsn = req.get_start_lsn();
    PalfHandleGuard palf_handle_guard;
    PalfGroupBufferIterator group_iter;

    if (OB_FAIL(init_group_iterator_(ls_id, start_lsn, palf_handle_guard, group_iter))) {
      LOG_WARN("init_group_iterator_ fail", KR(ret), K(tenant_id_), K(ls_id), K(start_lsn));
    } else if (OB_FAIL(do_fetch_log_(req, palf_handle_guard, group_iter, frt, resp))) {
      LOG_WARN("do fetch log error", KR(ret), K(req));
    } else {}
  }

  // set debug error
  resp.set_debug_err(ret);
  if (OB_SUCC(ret)) {
    // do nothing
  } else if (OB_LS_NOT_EXIST == ret) {
    LOG_INFO("LS not exist when fetch log", KR(ret), K(tenant_id_), K(req));
  } else {
    LOG_WARN("fetch log error", KR(ret), K(tenant_id_), K(req));
    ret = OB_ERR_SYS;
  }

  if (OB_SUCC(ret)) {
    const int64_t fetch_log_size = resp.get_pos();
    const int64_t fetch_log_count = resp.get_log_num();
    ObCdcServiceMonitor::fetch_size(fetch_log_size);
    EVENT_ADD(CLOG_EXTLOG_FETCH_LOG_SIZE, fetch_log_size);
    ObCdcServiceMonitor::fetch_log_count(fetch_log_count);
    EVENT_ADD(CLOG_EXTLOG_FETCH_LOG_COUNT, fetch_log_count);
  }

  resp.set_err(ret);
  return ret;
}

int ObCdcFetcher::fetch_missing_log(const obrpc::ObCdcLSFetchMissLogReq &req,
    obrpc::ObCdcLSFetchLogResp &resp)
{
  int ret = OB_SUCCESS;
  FetchRunTime frt;
  const int64_t cur_tstamp = ObTimeUtility::current_time();

  // Generate this RPC ID using the current timestamp directly.
  ObLogRpcIDType rpc_id = cur_tstamp;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fetch missing log req", KR(ret), K(req));
  } else if (OB_FAIL(frt.init(rpc_id, cur_tstamp))) {
    LOG_WARN("fetch runtime init error", KR(ret), K(rpc_id), K(req));
  } else {
    const ObLSID &ls_id = req.get_ls_id();
    PalfHandleGuard palf_handle_guard;

    if (OB_FAIL(init_palf_handle_guard_(ls_id, palf_handle_guard))) {
      LOG_WARN("init_palf_handle_guard_ fail", KR(ret), K(tenant_id_), K(ls_id));
    } else if (OB_FAIL(do_fetch_missing_log_(req, palf_handle_guard, frt, resp))) {
      LOG_WARN("do fetch log error", KR(ret), K(req));
    } else {}
  }

  // set debug error
  resp.set_debug_err(ret);
  if (OB_SUCC(ret)) {
    // do nothing
  } else if (OB_LS_NOT_EXIST == ret) {
    LOG_INFO("LS not exist when fetch missing log", KR(ret), K(tenant_id_), K(req));
  } else {
    LOG_WARN("fetch missing log error", KR(ret), K(tenant_id_), K(req));
    ret = OB_ERR_SYS;
  }

  if (OB_SUCC(ret)) {
    const int64_t fetch_log_size = resp.get_pos();
    const int64_t fetch_log_count = resp.get_log_num();
    ObCdcServiceMonitor::fetch_size(fetch_log_size);
    EVENT_ADD(CLOG_EXTLOG_FETCH_LOG_SIZE, fetch_log_size);
    ObCdcServiceMonitor::fetch_log_count(fetch_log_count);
    EVENT_ADD(CLOG_EXTLOG_FETCH_LOG_COUNT, fetch_log_count);
  }

  resp.set_err(ret);
  return ret;
}

int ObCdcFetcher::init_palf_handle_guard_(const ObLSID &ls_id,
    palf::PalfHandleGuard &palf_handle_guard)
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService *);

  if (OB_FAIL(log_service->open_palf(ls_id, palf_handle_guard))) {
    if (OB_LS_NOT_EXIST != ret) {
      LOG_WARN("ObLogService open_palf fail", KR(ret), K(tenant_id_), K(ls_id));
    }
  }

  return ret;
}

int ObCdcFetcher::init_group_iterator_(const ObLSID &ls_id,
    const LSN &start_lsn,
    palf::PalfHandleGuard &palf_handle_guard,
    palf::PalfGroupBufferIterator &group_iter)
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService *);

  if (OB_FAIL(log_service->open_palf(ls_id, palf_handle_guard))) {
    if (OB_LS_NOT_EXIST != ret) {
      LOG_WARN("ObLogService open_palf fail", KR(ret), K(tenant_id_), K(ls_id));
    }
  } else if (OB_FAIL(palf_handle_guard.seek(start_lsn, group_iter))) {
    LOG_WARN("PalfHandleGuard seek fail", KR(ret), K(ls_id), K(start_lsn));
  } else {
    LOG_INFO("init_group_iterator succ", K(tenant_id_), K(ls_id), K(start_lsn));
  }

  return ret;
}

int ObCdcFetcher::do_fetch_log_(const ObCdcLSFetchLogReq &req,
    PalfHandleGuard &palf_handle_guard,
    PalfGroupBufferIterator &group_iter,
    FetchRunTime &frt,
    ObCdcLSFetchLogResp &resp)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = req.get_ls_id();
  const int64_t end_tstamp = frt.rpc_deadline_ - RPC_QIT_RESERVED_TIME;
  bool reach_upper_limit = false;
  bool reach_max_lsn = false;
  int64_t scan_round_count = 0;        // epoch of fetching
  int64_t fetched_log_count = 0;       // count of log fetched
  // The start LSN of the next RPC request.
  // 1. The initial value is start LSN of the current request. So next RPC request will retry even if without fetching one log.
  // 2. We will update next_req_lsn when fetching log, in order to help next RPC request.
  resp.set_next_req_lsn(req.get_start_lsn());
  resp.set_ls_id(ls_id);

  // execute specific logging logic
  if (OB_FAIL(ls_fetch_log_(ls_id, group_iter, end_tstamp, resp, frt, reach_upper_limit,
          reach_max_lsn, scan_round_count, fetched_log_count))) {
    LOG_WARN("ls_fetch_log_ error", KR(ret), K(ls_id), K(frt));
  } else {
    if (reach_max_lsn) {
      handle_when_reach_max_lsn_(ls_id, palf_handle_guard, fetched_log_count, frt, resp);
    }
  }

  // Update statistics
  if (OB_SUCC(ret)) {
    frt.fetch_status_.reset(reach_max_lsn, reach_upper_limit, scan_round_count);
    resp.set_fetch_status(frt.fetch_status_);
    // update_monitor(frt.fetch_status_);
  } else {
    LOG_WARN("fetch log fail", KR(ret), "CDC_Connector_PID", req.get_client_pid(),
        K(req), K(resp));
  }

  LOG_TRACE("do_fetch_log done", KR(ret), K(frt), K(req), K(resp));
  return ret;
}

int ObCdcFetcher::ls_fetch_log_(const ObLSID &ls_id,
    palf::PalfGroupBufferIterator &group_iter,
    const int64_t end_tstamp,
    obrpc::ObCdcLSFetchLogResp &resp,
    FetchRunTime &frt,
    bool &reach_upper_limit,
    bool &reach_max_lsn,
    int64_t &scan_round_count,
    int64_t &log_count)
{
  int ret = OB_SUCCESS;

  // Support cyclic scanning multiple rounds
  // Stop condition:
  // 1. Time up, timeout
  // 2. Buffer is full
  // 3. LS do not need to fetch logs, reach upper limit or max lsn
  // 4. LS no log fetched
  while (OB_SUCC(ret) && ! frt.is_stopped()) {
    LogGroupEntry log_group_entry;
    LSN lsn;
    // update fetching rounds
    scan_round_count++;
    int64_t start_fetch_ts = ObTimeUtility::current_time();

    if (is_time_up_(log_count, end_tstamp)) { // time up, stop fetching logs globally
      frt.stop("TimeUP");
      LOG_INFO("fetch log quit in time", K(end_tstamp), K(frt), K(log_count));
    } else if (OB_FAIL(group_iter.next())) {
      if (OB_ITER_END == ret) {
        // If return OB_ITER_END, considered that the maximum lsn of the server is reached.
        reach_max_lsn = true;
      } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
        // Lower bound, no log get
      } else if (OB_ALLOCATE_MEMORY_FAILED == ret) {
        // retry
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("group_iter next fail", KR(ret), K(tenant_id_), K(ls_id));
      }
    } else if (OB_FAIL(group_iter.get_entry(log_group_entry, lsn))) {
      LOG_WARN("group_iter get_entry fail", KR(ret), K(tenant_id_), K(ls_id));
    } else {
      resp.inc_log_fetch_time(ObTimeUtility::current_time() - start_fetch_ts);
      check_next_group_entry_(lsn, log_group_entry, log_count, resp, frt, reach_upper_limit);

      if (frt.is_stopped()) {
        // Stop fetching log
      } else if (OB_FAIL(prefill_resp_with_group_entry_(ls_id, lsn, log_group_entry, resp))) {
        if (OB_BUF_NOT_ENOUGH == ret) {
          handle_when_buffer_full_(frt); // stop
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("prefill_resp_with_group_entry fail", KR(ret), K(frt), K(end_tstamp), K(resp));
        }
      } else {
        // log fetched successfully
        log_count++;

        LOG_TRACE("LS fetch a log", K(ls_id), K(log_count), K(frt));
      }
    }
  } // while

  if (OB_SUCCESS == ret) {
    // do nothing
  } else if (OB_ITER_END == ret) {
    // has iterated to the end of block.
    ret = OB_SUCCESS;
  } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
    // log not exists
    ret = OB_SUCCESS;
    if (OB_FAIL(handle_log_not_exist_(ls_id, resp))) {
      LOG_WARN("handle log_not_exist error", K(ret));
    }
  } else {
    // other error code
  }

  LOG_TRACE("LS fetch log done", KR(ret), K(log_count), K(frt), K(resp));

  return ret;
}

void ObCdcFetcher::check_next_group_entry_(const LSN &next_lsn,
    const LogGroupEntry &next_log_group_entry,
    const int64_t fetched_log_count,
    obrpc::ObCdcLSFetchLogResp &resp,
    FetchRunTime &frt,
    bool &reach_upper_limit)
{
  int64_t submit_ts = next_log_group_entry.get_log_ts();
  int64_t entry_size = next_log_group_entry.get_serialize_size();
  bool is_buf_full = (! resp.has_enough_buffer(entry_size));

  if (is_buf_full) {
    // This log may not have the desired timestamp, but it is still regarded as buf_full.
    handle_when_buffer_full_(frt);
  } else if (submit_ts > frt.upper_limit_ts_) {
    // The next LogGroupEntry beyond the upper bound:
    // (1) The LS stops fetching logs when has fethed log, that is fetched_log_count > 0
    // (2) The LS still fetch the log which submit_ts > upper_limit_ts when has not fetched any log,
    //     that is fetched_log_count = 0; Because Keepalive log may not fall within the configuration range.
    //     So if no log is obtained in this flow, try returning the first log greater than upper_limit to avoid being stuck.
    // (3) The marching Fetch Log strategy isn't exactly precise, but it's controlled.
	  //     The purpose of marching Fetch Log strategy is to ensure that each LS "goes in step" to avoid data skew.
    if (fetched_log_count > 0) {
      reach_upper_limit = true;
      frt.stop("ReachUpperLimit");
    } else {
      // Still fetch log
    }
    LOG_TRACE("LS reach upper limit", K(submit_ts), K(frt), K(fetched_log_count), K(reach_upper_limit));
  } else {
    // do nothing
  }
}

int ObCdcFetcher::prefill_resp_with_group_entry_(const ObLSID &ls_id,
    const LSN &lsn,
    LogGroupEntry &log_group_entry,
    obrpc::ObCdcLSFetchLogResp &resp)
{
  int ret = OB_SUCCESS;
  int64_t entry_size = log_group_entry.get_serialize_size();

  if (! resp.has_enough_buffer(entry_size)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    int64_t remain_size = 0;
    char *remain_buf = resp.get_remain_buf(remain_size);
    int64_t pos = 0;

    if (OB_ISNULL(remain_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("remain_buf is NULL", KR(ret), K(ls_id), K(lsn));
    } else if (OB_FAIL(log_group_entry.serialize(remain_buf, remain_size, pos))) {
      LOG_WARN("LogGroupEntry serialize fail", KR(ret), K(remain_size), K(pos), K(ls_id), K(lsn),
          K(log_group_entry));
    } else {
      resp.log_entry_filled(entry_size); // modify the buf pointer of resp
      LSN next_req_lsn = lsn + entry_size;
      // Update next_req_lsn only when LogGroupEntry has been filled into buffer successfully.
      resp.set_next_req_lsn(next_req_lsn);
    }
  }

  return ret;
}

void ObCdcFetcher::handle_when_buffer_full_(FetchRunTime &frt)
{
  frt.stop("BufferFull");
}

int ObCdcFetcher::handle_log_not_exist_(const ObLSID &ls_id,
    obrpc::ObCdcLSFetchLogResp &resp)
{
  int ret = OB_SUCCESS;

  resp.set_feedback_type(ObCdcLSFetchLogResp::LOG_NOT_IN_THIS_SERVER);

  LOG_INFO("handle log_not_exist feedback success", K(ls_id), "lsn", resp.get_next_req_lsn());

  return ret;
}

void ObCdcFetcher::handle_when_reach_max_lsn_(const ObLSID &ls_id,
    palf::PalfHandleGuard &palf_handle_guard,
    const int64_t fetched_log_count,
    FetchRunTime &frt,
    ObCdcLSFetchLogResp &resp)
{
  int ret = OB_SUCCESS;
  bool is_ls_fall_behind = frt.is_ls_fall_behind();

  // Because we cannot determine whether this LS is a backward standby or a normal LS,
  // we can only check the LS.
  // Optimize: No feedback is needed for real-time log fetching case, because no ls fall behind.
  // Only meet the following conditions to check lag
  // 1. Reach max lsn but the progress is behind the upper limit
  // 2. No logs fetched in this round of RPC
  // 2. The overall progress is behind
  if (fetched_log_count <= 0 && is_ls_fall_behind) {
    if (OB_FAIL(check_lag_follower_(ls_id, palf_handle_guard, resp))) {
      LOG_WARN("check_lag_follower_ fail", KR(ret), K(ls_id), K(frt));
    }
  }
}

int ObCdcFetcher::check_lag_follower_(const ObLSID &ls_id,
    palf::PalfHandleGuard &palf_handle_guard,
    ObCdcLSFetchLogResp &resp)
{
  int ret = OB_SUCCESS;
  ObRole role = INVALID_ROLE;
  int64_t leader_epoch = OB_INVALID_TIMESTAMP;

  if (OB_FAIL(palf_handle_guard.get_role(role, leader_epoch))) {
    LOG_WARN("get_role fail", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(INVALID_ROLE == role)
      || OB_UNLIKELY(LEADER == role && OB_INVALID_TIMESTAMP == leader_epoch)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid role info", K(ret), K(ls_id), K(role), K(leader_epoch));
  }
  // If is standby replica, check whether it is a backward replica, if it is backward, set feedback
  else if (FOLLOWER == role) {
    storage::ObLSHandle ls_handle;
    ObLS *ls = NULL;
    logservice::ObLogHandler *log_handler = NULL;
    bool is_sync = false;
    bool is_need_rebuild = false;

    if (OB_FAIL(ls_service_->get_ls(ls_id, ls_handle, ObLSGetMod::LOG_MOD))) {
      LOG_WARN("ls_service get_ls fail", KR(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is NULL", KR(ret), K(ls_id));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log_handler is NULL", KR(ret), K(ls_id));
    } else if (OB_FAIL(log_handler->is_in_sync(is_sync, is_need_rebuild))) {
      LOG_WARN("log_handler is_in_sync fail", KR(ret), K(ls_id), K(is_sync));
    } else if (! is_sync) {
      resp.set_feedback_type(ObCdcLSFetchLogResp::LAGGED_FOLLOWER);
      LOG_INFO("catch a lag follower");
    }
  } else {}

  return ret;
}

int ObCdcFetcher::do_fetch_missing_log_(const obrpc::ObCdcLSFetchMissLogReq &req,
    palf::PalfHandleGuard &palf_handle_guard,
    FetchRunTime &frt,
    obrpc::ObCdcLSFetchLogResp &resp)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = req.get_ls_id();
  const obrpc::ObCdcLSFetchMissLogReq::MissLogParamArray &miss_log_array = req.get_miss_log_array();
  const int64_t end_tstamp = frt.rpc_deadline_ - RPC_QIT_RESERVED_TIME;
  bool reach_max_lsn = false;
  int64_t scan_round_count = 0;        // epoch of fetching
  int64_t fetched_log_count = 0;       // count of log fetched

  if (OB_UNLIKELY(miss_log_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("miss_log_array is not valid", KR(ret));
  } else {
    // The missing LSN of the next RPC request.
    // 1. The initial value is the first missing LSN of the current request. So next RPC request will retry even if without fetching one missing log.
    // 2. We will update next_miss_lsn when fetching log, in order to help next RPC request.
    resp.set_next_miss_lsn(miss_log_array[0].miss_lsn_);
    resp.set_ls_id(ls_id);

    for (int64_t idx = 0; OB_SUCC(ret) && ! frt.is_stopped() && idx < miss_log_array.count(); idx++) {
      const obrpc::ObCdcLSFetchMissLogReq::MissLogParam &miss_log_info = miss_log_array[idx];
      const LSN &missing_lsn = miss_log_info.miss_lsn_;
      palf::PalfBufferIterator log_entry_iter;
      LogEntry log_entry;
      LSN lsn;
      resp.set_next_miss_lsn(missing_lsn);
      int64_t start_fetch_ts = ObTimeUtility::current_time();

      if (is_time_up_(fetched_log_count, end_tstamp)) { // time up, stop fetching logs globally
        frt.stop("TimeUP");
        LOG_INFO("fetch log quit in time", K(end_tstamp), K(frt), K(fetched_log_count));
      } else if (OB_FAIL(palf_handle_guard.seek(missing_lsn, log_entry_iter))) {
        LOG_WARN("PalfHandleGuard seek fail", KR(ret), K(ls_id), K(log_entry_iter));
      } else if (OB_FAIL(log_entry_iter.next())) {
        if (OB_ITER_END == ret) {
          // If return OB_ITER_END, considered that the maximum lsn of the server is reached.
          reach_max_lsn = true;
        } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
          // Lower bound, no log get
        } else if (OB_ALLOCATE_MEMORY_FAILED == ret) {
          // retry
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("log_entry_iter next fail", KR(ret), K(tenant_id_), K(ls_id));
        }
      } else if (OB_FAIL(log_entry_iter.get_entry(log_entry, lsn))) {
        LOG_WARN("log_entry_iter get_entry fail", KR(ret), K(tenant_id_), K(ls_id));
      } else if (OB_UNLIKELY(missing_lsn != lsn)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("do_fetch_missing_log missing_lsn not match", KR(ret), K(tenant_id_), K(ls_id),
            K(missing_lsn), K(lsn));
      } else {
        resp.inc_log_fetch_time(ObTimeUtility::current_time() - start_fetch_ts);
        check_next_entry_(lsn, log_entry, resp, frt);

        if (frt.is_stopped()) {
          // Stop fetching log
        } else if (OB_FAIL(prefill_resp_with_log_entry_(ls_id, lsn, log_entry, resp))) {
          if (OB_BUF_NOT_ENOUGH == ret) {
            handle_when_buffer_full_(frt); // stop
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("prefill_resp_with_log_entry fail", KR(ret), K(frt), K(end_tstamp), K(resp));
          }
        } else {
          // log fetched successfully
          fetched_log_count++;

          LOG_TRACE("LS fetch a missing log", K(tenant_id_), K(ls_id), K(fetched_log_count), K(frt));
        }
      }
    } // for

    if (OB_SUCCESS == ret) {
      // do nothing
    } else if (OB_ITER_END == ret) {
      // has iterated to the end of block.
      ret = OB_SUCCESS;
    } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
      // log not exists
      ret = OB_SUCCESS;
      if (OB_FAIL(handle_log_not_exist_(ls_id, resp))) {
        LOG_WARN("handle log_not_exist error", K(ret));
      }
    } else {
      // other error code
    }

    LOG_TRACE("LS fetch missing log done", KR(ret), K(fetched_log_count), K(frt), K(resp));
  }

  return ret;
}

void ObCdcFetcher::check_next_entry_(const LSN &next_lsn,
    const LogEntry &next_log_entry,
    obrpc::ObCdcLSFetchLogResp &resp,
    FetchRunTime &frt)
{
  int64_t entry_size = next_log_entry.get_serialize_size();
  bool is_buf_full = (! resp.has_enough_buffer(entry_size));

  if (is_buf_full) {
    // This log may not have the desired timestamp, but it is still regarded as buf_full.
    handle_when_buffer_full_(frt);
  } else {
    // do nothing
  }
}

int ObCdcFetcher::prefill_resp_with_log_entry_(const ObLSID &ls_id,
    const LSN &lsn,
    LogEntry &log_entry,
    obrpc::ObCdcLSFetchLogResp &resp)
{
  int ret = OB_SUCCESS;
  int64_t entry_size = log_entry.get_serialize_size();

  if (! resp.has_enough_buffer(entry_size)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    int64_t remain_size = 0;
    char *remain_buf = resp.get_remain_buf(remain_size);
    int64_t pos = 0;

    if (OB_ISNULL(remain_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("remain_buf is NULL", KR(ret), K(ls_id), K(lsn));
    } else if (OB_FAIL(log_entry.serialize(remain_buf, remain_size, pos))) {
      LOG_WARN("LogEntry serialize fail", KR(ret), K(remain_size), K(pos), K(ls_id), K(lsn),
          K(log_entry));
    } else {
      resp.log_entry_filled(entry_size); // modify the buf pointer of resp
    }
  }

  return ret;
}

/////////////////////// FetchRunTime ///////////////////////
FetchRunTime::FetchRunTime() :
    rpc_id_(OB_LOG_INVALID_RPC_ID),
    rpc_start_tstamp_(0),
    upper_limit_ts_(0),
    rpc_deadline_(0),
    is_stopped_(false),
    stop_reason_("NONE"),
    fetch_status_()
{}

FetchRunTime::~FetchRunTime()
{
  // for performance, no longer resetting each variable here
}

int FetchRunTime::init(const ObLogRpcIDType rpc_id,
    const int64_t rpc_start_tstamp,
    const int64_t upper_limit_ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_LOG_INVALID_RPC_ID == rpc_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("init fetch_runtime error", KR(ret), K(rpc_id), K(upper_limit_ts));
  } else {
    rpc_id_ = rpc_id;
    rpc_start_tstamp_ = rpc_start_tstamp;
    upper_limit_ts_ = upper_limit_ts;
    rpc_deadline_ = THIS_WORKER.get_timeout_ts();

    is_stopped_ = false;
    stop_reason_ = "NONE";

    fetch_status_.reset();
  }

  return ret;
}

} // namespace cdc
} // namespace oceanbase
