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
#include "logservice/restoreservice/ob_remote_log_source_allocator.h"

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
    ls_service_(NULL),
    large_buffer_pool_(NULL),
    log_ext_handler_(NULL)
{
}

ObCdcFetcher::~ObCdcFetcher()
{
  destroy();
}

int ObCdcFetcher::init(const uint64_t tenant_id,
    ObLSService *ls_service,
    archive::LargeBufferPool *buffer_pool,
    logservice::ObLogExternalStorageHandler *log_ext_handler)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(ls_service) || OB_ISNULL(buffer_pool)
      || OB_ISNULL(log_ext_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_service), K(buffer_pool));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
    ls_service_ = ls_service;
    large_buffer_pool_ = buffer_pool;
    log_ext_handler_ = log_ext_handler;
  }

  return ret;
}

void ObCdcFetcher::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    tenant_id_ = OB_INVALID_TENANT_ID;
    ls_service_ = NULL;
    large_buffer_pool_ = NULL;
    log_ext_handler_ = NULL;
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
  ObCdcFetchLogTimeStats fetch_log_time_stat;
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
    const ObCdcRpcId &rpc_id = req.get_client_id();
    ClientLSKey ls_key(rpc_id.get_addr(), rpc_id.get_pid(), req.get_tenant_id(), ls_id);
    ClientLSCtxMap &ctx_map = MTL(ObLogService*)->get_cdc_service()->get_ls_ctx_map();
    ClientLSCtx *ls_ctx = NULL;
    int8_t fetch_log_flag = req.get_flag();

    // create ClientLSCtx when fetch_log for better maintainablity
    // about the reason why not create ClientLSCtx when locating start lsn, considering the case:
    // 1. Client locate lsn at server1 and server1 create the clientlsctx;
    // 2. when client is about to fetch log in server1, server1 becomes unavailable;
    // 3. then the client switch to server2 to fetch log;
    // 4. server2 doesn't have the context of clientls, and need to create one
    // So we just create ctx when fetching log
    if (OB_FAIL(ctx_map.get(ls_key, ls_ctx))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(ctx_map.create(ls_key, ls_ctx))) {
          LOG_WARN("create client ls ctx failed", KR(ret), K(ls_key));
        } else if (OB_FAIL(ls_ctx->init(req.get_progress()))) {
          LOG_WARN("failed to init client ls ctx", KR(ret), K(req));
        } else {
          // if test_switch_mode is enabled, set the init fetch mode to FETCHMODE_ARCHIVE
          // so that the fetch mode could be switched to FETCHMODE_ONLINE in the next rpc in some test cases.
          if (fetch_log_flag & ObCdcRpcTestFlag::OBCDC_RPC_TEST_SWITCH_MODE) {
            ls_ctx->set_fetch_mode(FetchMode::FETCHMODE_ARCHIVE, "InitTestSwitchMode");
          }
          LOG_INFO("create client ls ctx succ", K(ls_key), K(ls_ctx));
        }
      } else {
        LOG_ERROR("get client ls ctx from ctx map failed", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ls_ctx->update_touch_ts();
      if (OB_FAIL(do_fetch_log_(req, frt, resp, *ls_ctx, fetch_log_time_stat))) {
      LOG_WARN("do fetch log error", KR(ret), K(req));
      } else {}
    }

    if (OB_NOT_NULL(ls_ctx)) {
      ctx_map.revert(ls_ctx);
    }
  }

  // set debug error
  resp.set_debug_err(ret);
  resp.set_err(ret);

  if (OB_SUCC(ret)) {
    const int64_t fetch_log_size = resp.get_pos();
    const int64_t fetch_log_count = resp.get_log_num();
    ObCdcServiceMonitor::fetch_size(fetch_log_size);
    EVENT_ADD(CLOG_EXTLOG_FETCH_LOG_SIZE, fetch_log_size);
    ObCdcServiceMonitor::fetch_log_count(fetch_log_count);
    EVENT_ADD(CLOG_EXTLOG_FETCH_LOG_COUNT, fetch_log_count);
  }

  fetch_log_time_stat.inc_fetch_total_time(ObTimeUtility::current_time() - cur_tstamp);

  LOG_INFO("fetch_log done", K(req), K(resp), K(fetch_log_time_stat));
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
    const ObCdcRpcId &rpc_id = req.get_client_id();
    ClientLSKey ls_key(rpc_id.get_addr(), rpc_id.get_pid(), req.get_tenant_id(), ls_id);
    ClientLSCtxMap &ctx_map = MTL(ObLogService*)->get_cdc_service()->get_ls_ctx_map();
    ClientLSCtx *ls_ctx = NULL;

    if (OB_FAIL(ctx_map.get(ls_key, ls_ctx))) {
      LOG_WARN("get client ls ctx from ctx map failed", KR(ret));
    } else if (OB_ISNULL(ls_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls ctx is null, unexpected", KR(ret), K(ls_key));
    } else {
      ls_ctx->update_touch_ts();
      if (OB_FAIL(do_fetch_missing_log_(req, frt, resp, *ls_ctx))) {
        LOG_WARN("do fetch log error", KR(ret), K(req));
      } else {}
      ctx_map.revert(ls_ctx);
    }
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

  LOG_INFO("fetch_missing_log done", K(req), K(resp));
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
    FetchRunTime &frt,
    ObCdcLSFetchLogResp &resp,
    ClientLSCtx &ctx,
    ObCdcFetchLogTimeStats &fetch_time_stat)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = req.get_ls_id();
  const LSN &start_lsn = req.get_start_lsn();
  const int8_t fetch_flag = req.get_flag();
  const int64_t end_tstamp = frt.rpc_deadline_ - RPC_QIT_RESERVED_TIME;
  bool reach_upper_limit = false;
  bool reach_max_lsn = false;
  int64_t scan_round_count = 0;        // epoch of fetching
  int64_t fetched_log_count = 0;       // count of log fetched
  // The start LSN of the next RPC request.
  // 1. The initial value is start LSN of the current request. So next RPC request will retry even if without fetching one log.
  // 2. We will update next_req_lsn when fetching log, in order to help next RPC request.
  // 3. the next req lsn is used to record the lsn of the next log, so when switch fetching mode, we can use the lsn
  //    recorded in the resp to initialize a new iterator.
  resp.set_next_req_lsn(req.get_start_lsn());
  resp.set_ls_id(ls_id);

  // execute specific logging logic
  if (OB_FAIL(ls_fetch_log_(ls_id, end_tstamp, fetch_flag, resp, frt, reach_upper_limit,
          reach_max_lsn, scan_round_count, fetched_log_count, ctx, fetch_time_stat))) {
    LOG_WARN("ls_fetch_log_ error", KR(ret), K(ls_id), K(frt));
  } else { }

  // Update statistics
  frt.fetch_status_.reset(reach_max_lsn, reach_upper_limit, scan_round_count);
  resp.set_fetch_status(frt.fetch_status_);
  // update_monitor(frt.fetch_status_);
  if (OB_FAIL(ret)) {
    LOG_WARN("fetch log fail", KR(ret), "CDC_Connector_PID", req.get_client_pid(),
        K(req), K(resp));
  }

  LOG_TRACE("do_fetch_log done", KR(ret), K(frt), K(req), K(resp));
  return ret;
}

// ------------ template method, used in this file only --------------
// can't use iter.is_inited to replace need_init_iter, because they have different semantics.
// don't block any error code here, let the caller handle the errcode for generality
template <class LogEntryType>
int ObCdcFetcher::fetch_log_in_palf_(const ObLSID &ls_id,
    PalfIterator<DiskIteratorStorage, LogEntryType> &iter,
    PalfHandleGuard &palf_guard,
    const LSN &start_lsn,
    const bool need_init_iter,
    const SCN &replayable_point_scn,
    LogEntryType &log_group_entry,
    LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (need_init_iter && OB_FAIL(palf_guard.seek(start_lsn, iter))) {
    LOG_WARN("PalfHandleGuard seek fail", KR(ret), K(ls_id), K(lsn));
  } else if (OB_FAIL(iter.next(replayable_point_scn))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("palf_iter next fail", KR(ret), K(tenant_id_), K(ls_id));
    }
  } else if (OB_FAIL(iter.get_entry(log_group_entry, lsn))) {
    LOG_WARN("group_iter get_entry fail", KR(ret), K(tenant_id_), K(ls_id));
  }
  return ret;
}

// ------------ template method, used in this file only --------------
// init archive source when it's used
// don't block any error code here, let the caller handle the errcode for generality
// return OB_SUCCESS when log_entry is successfully fetched
template <class LogEntryType>
int ObCdcFetcher::fetch_log_in_archive_(
    const ObLSID &ls_id,
    ObRemoteLogIterator<LogEntryType> &remote_iter,
    const LSN &start_lsn,
    const bool need_init_iter,
    LogEntryType &log_entry,
    LSN &lsn,
    ClientLSCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_source()) && OB_FAIL(init_archive_source_(ctx, ls_id))) {
    LOG_WARN("init archive source failed", K(ctx), K(ls_id));
  } else {
    const char *buf = NULL;
    int64_t buf_size = 0;
    share::SCN pre_scn;
    if (OB_FAIL(pre_scn.convert_from_ts(ctx.get_progress()/1000L))) {
      LOG_WARN("convert progress to scn failed", KR(ret), K(ctx));
    } else if (need_init_iter && OB_FAIL(remote_iter.init(tenant_id_, ls_id, pre_scn,
                                                          start_lsn, LSN(LOG_MAX_LSN_VAL), large_buffer_pool_,
                                                          log_ext_handler_))) {
      LOG_WARN("init remote log iterator failed", KR(ret), K(tenant_id_), K(ls_id));
    } else if (OB_FAIL(remote_iter.next(log_entry, lsn, buf, buf_size))) {
      // expected OB_ITER_END and OB_SUCCEES, error occurs when other code is returned.
      if (OB_ITER_END != ret) {
        LOG_WARN("iterate remote log failed", KR(ret), K(need_init_iter), K(ls_id));
      }
    } else if (start_lsn != lsn) {
      // to keep consistency with the ret code of palf
      ret = OB_INVALID_DATA;
      LOG_WARN("remote iterator returned unexpected log entry lsn", K(start_lsn), K(lsn), K(log_entry), K(ls_id),
          K(remote_iter));
    } else {

    }
  }
  return ret;
}

int ObCdcFetcher::set_fetch_mode_before_fetch_log_(const ObLSID &ls_id,
    const bool test_switch_fetch_mode,
    bool &ls_exist_in_palf,
    palf::PalfHandleGuard &palf_guard,
    ClientLSCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_palf_handle_guard_(ls_id, palf_guard))) {
    if (OB_LS_NOT_EXIST != ret) {
      LOG_WARN("ObLogService open_palf fail", KR(ret), K(tenant_id_), K(ls_id));
    } else if (OB_SYS_TENANT_ID != tenant_id_) {
      LOG_INFO("ls not exist in palf, switch mode to archive", KR(ret), K(tenant_id_), K(ls_id));
      ls_exist_in_palf = false;
      ctx.set_fetch_mode(FetchMode::FETCHMODE_ARCHIVE, "LSNotExistInPalf");
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("logstream in sys tenant doesn't exist, unexpected", KR(ret), K(ls_id));
    }
  } else if (FetchMode::FETCHMODE_ARCHIVE == ctx.get_fetch_mode()) {
    int64_t end_ts_ns = OB_INVALID_TIMESTAMP;
    int64_t time_diff = OB_INVALID_TIMESTAMP;
    const int64_t ctx_progress = ctx.get_progress();
    // if the gap between current progress and the latest is less than 1 min, which means
    // current fetch progress close to the latest log progress, try to switch fetch mode
    // set the switch interval to 10s in test switch fetch mode, the unit of measurement of log_ts(scn) is nano second.
    const int64_t SECOND_NS = 1000L * 1000 * 1000;
    SCN end_scn;
    const int64_t SWITCH_INTERVAL = test_switch_fetch_mode ? 10L * SECOND_NS : 60 * SECOND_NS; //default 60s
    if (OB_FAIL(palf_guard.get_end_scn(end_scn))) {
      LOG_WARN("get palf end ts failed", KR(ret));
    } else {
      end_ts_ns = end_scn.get_val_for_logservice();
      time_diff = end_ts_ns - ctx_progress;
      if (time_diff <= SWITCH_INTERVAL) {
        ctx.set_fetch_mode(FetchMode::FETCHMODE_ONLINE, "LogNearLatest");
      }
    }
  } else {}

  return ret;
}

int ObCdcFetcher::get_replayable_point_scn_(const ObLSID &ls_id, SCN &replayable_point_scn)
{
  int ret = OB_SUCCESS;
  ObLogService *log_service = MTL(ObLogService*);
  if (OB_ISNULL(log_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log service is null, unexpected", KR(ret), K(tenant_id_), K(ls_id));
  } else if (OB_FAIL(log_service->get_replayable_point(replayable_point_scn))) {
    LOG_WARN("get replayable point scn failed", KR(ret), K(ls_id));
  }
  return ret;
}

FetchMode ObCdcFetcher::get_fetch_mode_when_fetching_log_(const ClientLSCtx &ctx,
          const bool fetch_archive_only)
{
  FetchMode ret_mode = FetchMode::FETCHMODE_UNKNOWN;
  if (OB_SYS_TENANT_ID == tenant_id_) {
    ret_mode = FetchMode::FETCHMODE_ONLINE;
  } else if (fetch_archive_only) {
    ret_mode = FetchMode::FETCHMODE_ARCHIVE;
  } else {
    ret_mode = ctx.get_fetch_mode();
  }
  return ret_mode;
}

// make sure ctx is not null
// set need_init_iter true when switching fetch mode
int ObCdcFetcher::ls_fetch_log_(const ObLSID &ls_id,
    const int64_t end_tstamp,
    const int8_t fetch_flag,
    obrpc::ObCdcLSFetchLogResp &resp,
    FetchRunTime &frt,
    bool &reach_upper_limit,
    bool &reach_max_lsn,
    int64_t &scan_round_count,
    int64_t &fetched_log_count,
    ClientLSCtx &ctx,
    ObCdcFetchLogTimeStats &fetch_time_stat)
{
  int ret = OB_SUCCESS;
  const int64_t start_ls_fetch_log_time = ObTimeUtility::current_time();
  PalfGroupBufferIterator palf_iter;
  PalfHandleGuard palf_guard;
  // use cached remote_iter
  ObCdcGetSourceFunctor get_source_func(ctx);
  ObCdcUpdateSourceFunctor update_source_func(ctx);
  ObRemoteLogGroupEntryIterator remote_iter(get_source_func, update_source_func);
  bool ls_exist_in_palf = true;
  // always reset remote_iter when need_init_iter is true
  // always set need_init_inter=true when switch fetch_mode
  bool need_init_iter = true;
  bool log_exist_in_palf = true;
  int64_t retry_count = 0;
  const bool fetch_archive_only = ObCdcRpcTestFlag::is_fetch_archive_only(fetch_flag);
  // test switch fetch mode requires that the fetch mode should be FETCHMODE_ARCHIVE at first, and then
  // switch to FETCHMODE_ONLINE when processing next rpc
  const bool test_switch_fetch_mode = ObCdcRpcTestFlag::is_test_switch_mode(fetch_flag);
  SCN replayable_point_scn;
  // find out whether logstream exists in palf, if it exists try switch mode to online when
  // the gap between progress in ctx and the latest log progress is less than 1 min
  if (OB_FAIL(get_replayable_point_scn_(ls_id, replayable_point_scn))) {
    LOG_WARN("get replayable point scn failed", KR(ret), K(ls_id));
  } else if (OB_FAIL(set_fetch_mode_before_fetch_log_(ls_id, test_switch_fetch_mode,
          ls_exist_in_palf, palf_guard, ctx))) {
    LOG_WARN("set fetch mode before fetch log failed", KR(ret), K(ls_id), K(tenant_id_));
  } else if (fetch_archive_only) {
    // overwrite ls_exist_in_palf for test
    ls_exist_in_palf = false;
  }
  // Support cyclic scanning multiple rounds
  // Stop condition:
  // 1. Time up, timeout
  // 2. Buffer is full
  // 3. LS do not need to fetch logs, reach upper limit or max lsn
  // 4. LS no log fetched
  while (OB_SUCC(ret) && ! frt.is_stopped()) {
    LogGroupEntry log_group_entry;
    LSN lsn;
    FetchMode fetch_mode = get_fetch_mode_when_fetching_log_(ctx, fetch_archive_only);
    if (fetch_mode != ctx.get_fetch_mode()) {
      // when in force_fetch_archive mode, if we don't set fetch mode here,
      // the ability of reading archive log concurrently can't be utilized
      ctx.set_fetch_mode(fetch_mode, "ModeConsistence");
    }
    int64_t finish_fetch_ts = OB_INVALID_TIMESTAMP;
    // update fetching rounds
    scan_round_count++;
    int64_t start_fetch_ts = ObTimeUtility::current_time();
    bool fetch_log_succ = false;
    const int64_t MAX_RETRY_COUNT = 3;
    if (is_time_up_(scan_round_count, end_tstamp)) { // time up, stop fetching logs globally
      frt.stop("TimeUP");
      LOG_INFO("fetch log quit in time", K(end_tstamp), K(frt), K(fetched_log_count));
    } // time up
    else if (FetchMode::FETCHMODE_ONLINE == fetch_mode) {
      if (OB_FAIL(fetch_log_in_palf_(ls_id, palf_iter, palf_guard,
          resp.get_next_req_lsn(), need_init_iter, replayable_point_scn,
          log_group_entry, lsn))) {
        if (OB_ITER_END == ret) {
          reach_max_lsn = true;
        } else if (OB_ALLOCATE_MEMORY_FAILED == ret) {
          need_init_iter = false;
          ret = OB_SUCCESS;
        } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
          // switch to fetchmode_archive, when in FETCHMODE_ONLINE, remote_iter is not inited
          if (OB_SYS_TENANT_ID != tenant_id_) {
            need_init_iter = true;
            log_exist_in_palf = false;
            ctx.set_fetch_mode(FetchMode::FETCHMODE_ARCHIVE, "PalfOutOfLowerBound");
            ret = OB_SUCCESS;
          } else {
            LOG_INFO("log in sys tenant may be recycled", KR(ret), K(ls_id), K(resp));
          }
        } else {
          LOG_WARN("fetching log in palf failed", KR(ret));
        }
      } else {
        log_exist_in_palf = true;
        need_init_iter = false;
        fetch_log_succ = true;
      } // fetch log succ
      fetch_time_stat.inc_fetch_palf_time(ObTimeUtility::current_time() - start_fetch_ts);
    } // fetch palf log
    else if (FetchMode::FETCHMODE_ARCHIVE == fetch_mode) {
      if (OB_FAIL(fetch_log_in_archive_(ls_id, remote_iter, resp.get_next_req_lsn(),
              need_init_iter, log_group_entry, lsn, ctx))) {
        if (OB_ITER_END == ret) {
          // when fetch to the end, the iter become invalid even if the new log is archived later,
          // cdcservice would continue to fetch log in palf or return result to cdc-connector,
          // reset remote_iter in either condition.
          remote_iter.update_source_cb();
          remote_iter.reset();
          if (ls_exist_in_palf) {
            if (log_exist_in_palf) {
              // switch to palf, reset remote_iter
              need_init_iter = true;
              ctx.set_fetch_mode(FetchMode::FETCHMODE_ONLINE, "ArchiveIterEnd");
              ret = OB_SUCCESS;
            } else {
              ret = OB_ERR_OUT_OF_LOWER_BOUND;
            }
          } else {
            // exit
            resp.set_feedback_type(obrpc::ObCdcLSFetchLogResp::ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF);
            reach_max_lsn = true;
            LOG_INFO("reach max lsn in archive but ls not exists in this server, need switch server", K(ls_id));
          }
        } else if (OB_NEED_RETRY == ret) {
          frt.stop("ArchiveNeedRetry");
          ret = OB_SUCCESS;
        } else if (OB_ALREADY_IN_NOARCHIVE_MODE == ret || OB_ENTRY_NOT_EXIST == ret) {
          // archive is not on or lsn less than the start_lsn in archive
          ret = OB_ERR_OUT_OF_LOWER_BOUND;
        } else {
          // other error code, retry because various error code would be returned, retry could fix some problem
          // TODO: process the error code with clear semantic
          LOG_WARN("fetching log in archive failed", KR(ret), K(remote_iter), K(ls_id), K(resp));
          remote_iter.reset();
          if (retry_count < MAX_RETRY_COUNT) {
            LOG_TRACE("retry on fetching remote log failure", KR(ret), K(retry_count), K(ctx));
            retry_count++;
            need_init_iter = true;
            ret = OB_SUCCESS;
          }
        }
      } else { // OB_SUCCESS
        log_exist_in_palf = true;
        need_init_iter = false;
        fetch_log_succ = true;
      } // fetch log succ
      fetch_time_stat.inc_fetch_archive_time(ObTimeUtility::current_time() - start_fetch_ts);
    } // fetch archive log
    else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fetch mode is invalid", KR(ret), K(fetch_mode));
    } // unexpected branch
    finish_fetch_ts = ObTimeUtility::current_time();
    // inc log fetch time in any condition
    resp.inc_log_fetch_time(finish_fetch_ts - start_fetch_ts);

    // retry on OB_ITER_END (when fetching logs in archive), OB_ALLOCATE_MEMORY_FAILED and
    // OB_ERR_OUT_OF_LOWER_BOUND (when fetching logs in palf), thus some return codes are blocked and the
    // return code is unable to be used for determine whether logEntry is successfully fetched.
    // update the resp/frt/ctx when the logentry is successfully fetched
    if (OB_SUCC(ret) && fetch_log_succ) {
      check_next_group_entry_(lsn, log_group_entry, fetched_log_count, resp, frt, reach_upper_limit, ctx);
      resp.set_progress(ctx.get_progress());
      // There is reserved space for the last log group entry, so we assume that the buffer is always enough here,
      // So we could fill response buffer without checking buffer full
      if (OB_FAIL(prefill_resp_with_group_entry_(ls_id, lsn, log_group_entry, resp, fetch_time_stat))) {
        if (OB_BUF_NOT_ENOUGH == ret) {
          handle_when_buffer_full_(frt); // stop
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("prefill_resp_with_group_entry fail", KR(ret), K(frt), K(end_tstamp), K(resp));
        }
      } else {
        // log fetched successfully
        fetched_log_count++;
        if (resp.log_reach_threshold()) {
          frt.stop("LogReachThreshold");
        }
        LOG_TRACE("LS fetch a log", K(ls_id), K(fetched_log_count), K(frt));
      }
    }
    fetch_time_stat.inc_fetch_log_post_process_time(ObTimeUtility::current_time() - finish_fetch_ts);
  } // while

  // update source back when remote_iter is valid, needn't reset remote iter,
  // because it won't be used afterwards
  if (remote_iter.is_init()) {
    remote_iter.update_source_cb();
  }

  if (OB_SUCCESS == ret) {
    // do nothing
    if (ls_exist_in_palf && reach_max_lsn) {
      handle_when_reach_max_lsn_in_palf_(ls_id, palf_guard, fetched_log_count, frt, resp);
    }
  } else if (OB_ITER_END == ret) {
    // has iterated to the end of block.
    ret = OB_SUCCESS;
  } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
    // log not exists
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(handle_log_not_exist_(ls_id, resp))) {
      LOG_WARN("handle log_not_exist error", K(ret));
    }
  } else {
    // other error code
  }

  fetch_time_stat.inc_fetch_log_time(ObTimeUtility::current_time() - start_ls_fetch_log_time);

  LOG_TRACE("LS fetch log done", KR(ret), K(fetched_log_count), K(frt), K(resp));

  return ret;
}

void ObCdcFetcher::check_next_group_entry_(const LSN &next_lsn,
    const LogGroupEntry &next_log_group_entry,
    const int64_t fetched_log_count,
    obrpc::ObCdcLSFetchLogResp &resp,
    FetchRunTime &frt,
    bool &reach_upper_limit,
    ClientLSCtx &ctx)
{
  //TODO(scn)
  int64_t submit_ts = next_log_group_entry.get_scn().get_val_for_logservice();
  int64_t entry_size = next_log_group_entry.get_serialize_size();
  bool is_buf_full = (! resp.has_enough_buffer(entry_size));
  // if a valid log entry is fetched, update the ctx progress
  if (entry_size > 0) {
    ctx.set_progress(submit_ts);
  }

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
    obrpc::ObCdcLSFetchLogResp &resp,
    ObCdcFetchLogTimeStats &fetch_time_stat)
{
  int ret = OB_SUCCESS;
  const int64_t start_fill_ts = ObTimeUtility::current_time();
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

  fetch_time_stat.inc_prefill_resp_time(ObTimeUtility::current_time() - start_fill_ts);

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

void ObCdcFetcher::handle_when_reach_max_lsn_in_palf_(const ObLSID &ls_id,
    palf::PalfHandleGuard &palf_handle_guard,
    const int64_t fetched_log_count,
    FetchRunTime &frt,
    ObCdcLSFetchLogResp &resp)
{
  int ret = OB_SUCCESS;

  // Because we cannot determine whether this LS is a backward standby or a normal LS,
  // we can only check the LS.
  // Optimize: No feedback is needed for real-time log fetching case, because no ls fall behind.
  // Only meet the following conditions to check lag
  // 1. Reach max lsn but the progress is behind the upper limit
  // 2. No logs fetched in this round of RPC
  // 2. The overall progress is behind
  if (OB_FAIL(check_lag_follower_(ls_id, palf_handle_guard, resp))) {
    LOG_WARN("check_lag_follower_ fail", KR(ret), K(ls_id), K(frt));
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
    FetchRunTime &frt,
    obrpc::ObCdcLSFetchLogResp &resp,
    ClientLSCtx &ctx)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = req.get_ls_id();
  const obrpc::ObCdcLSFetchMissLogReq::MissLogParamArray &miss_log_array = req.get_miss_log_array();
  const int64_t end_tstamp = frt.rpc_deadline_ - RPC_QIT_RESERVED_TIME;
  int64_t scan_round_count = 0;        // epoch of fetching
  int64_t fetched_log_count = 0;       // count of log fetched
  const FetchMode ctx_fetch_mode = ctx.get_fetch_mode();
  int8_t req_flag = req.get_flag();
  bool fetch_archive_only = is_sys_tenant(tenant_id_) ? false : ObCdcRpcTestFlag::is_fetch_archive_only(req_flag);

  if (OB_UNLIKELY(miss_log_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("miss_log_array is not valid", KR(ret));
  } else {
    PalfHandleGuard palf_guard;
    GetSourceFunc get_source_func = ObCdcGetSourceFunctor(ctx);
    bool ls_exist_in_palf = true;
    bool archive_is_on = true;
    bool need_init_iter = true;
    // The missing LSN of the next RPC request.
    // 1. The initial value is the first missing LSN of the current request.
    //    So next RPC request will retry even if without fetching one missing log.
    // 2. We will update next_miss_lsn when fetching log, in order to help next RPC request.
    resp.set_next_miss_lsn(miss_log_array[0].miss_lsn_);
    resp.set_ls_id(ls_id);
    SCN replayable_point_scn;
    if (OB_FAIL(get_replayable_point_scn_(ls_id, replayable_point_scn))) {
      LOG_WARN("get replayable point scn failed", KR(ret), K(ls_id));
    } else if (OB_FAIL(prepare_berfore_fetch_missing_(ls_id, ctx, palf_guard, ls_exist_in_palf, archive_is_on))) {
      LOG_WARN("failed to prepare before fetching missing log", KR(ret), K(ls_id), K(tenant_id_));
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && ! frt.is_stopped() && idx < miss_log_array.count(); idx++) {
        // need_init_iter should always be true, declared here to ensure need init iter be true in each loop
        PalfBufferIterator palf_iter;
        ObRemoteLogpEntryIterator remote_iter(get_source_func);
        const obrpc::ObCdcLSFetchMissLogReq::MissLogParam &miss_log_info = miss_log_array[idx];
        const LSN &missing_lsn = miss_log_info.miss_lsn_;
        palf::PalfBufferIterator log_entry_iter;
        LogEntry log_entry;
        LSN lsn;
        resp.set_next_miss_lsn(missing_lsn);
        int64_t start_fetch_ts = ObTimeUtility::current_time();
        bool log_fetched_in_palf = false;
        bool log_fetched_in_archive = false;

        if (is_time_up_(fetched_log_count, end_tstamp)) { // time up, stop fetching logs globally
          frt.stop("TimeUP");
          LOG_INFO("fetch log quit in time", K(end_tstamp), K(frt), K(fetched_log_count));
        } else {
          // first, try to fetch logs in palf
          if (!fetch_archive_only && ls_exist_in_palf)  {
            if (OB_FAIL(fetch_log_in_palf_(ls_id, palf_iter, palf_guard,
                missing_lsn, need_init_iter, replayable_point_scn,
                log_entry, lsn))) {
              if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
                // block OB_ERR_OUT_OF_LOWER_BOUND
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fetch missing log in palf failed", KR(ret), K(missing_lsn));
              }
            } else {
              log_fetched_in_palf = true;
            }
          }
          // if no log fetched in palf, try to fetch log in archive
          if (OB_SUCC(ret) && !log_fetched_in_palf && archive_is_on) {
            if (OB_FAIL(fetch_log_in_archive_(ls_id, remote_iter, missing_lsn,
                    need_init_iter, log_entry, lsn, ctx))) {
              LOG_WARN("fetch missng log in archive failed", KR(ret), K(missing_lsn));
            } else {
              log_fetched_in_archive = true;
            }
          }

          if (OB_SUCC(ret) && (log_fetched_in_palf || log_fetched_in_archive)) {
            if (OB_UNLIKELY(missing_lsn != lsn)) {
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
          }
        }
      } // for
    } // else

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

// called when source in ctx is null
int ObCdcFetcher::init_archive_source_(ClientLSCtx &ctx, ObLSID ls_id) {
  int ret = OB_SUCCESS;
  ObRemoteLogParent *source = ctx.get_source();
  if (OB_NOT_NULL(source)) {
    LOG_WARN("archive source is not null, no need to init");
  } else if (OB_ISNULL(source = logservice::ObResSrcAlloctor::alloc(ObLogRestoreSourceType::LOCATION, ls_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc RemoteLocationParent failed", KR(ret), K(tenant_id_), K(ls_id));
  } else {
    share::ObBackupDest archive_dest;
    if (OB_FAIL(ObCdcService::get_backup_dest(ls_id, archive_dest))) {
      LOG_WARN("get backupdest from archivedestinfo failed", KR(ret), K(ls_id));
    } else if (OB_FAIL(static_cast<ObRemoteLocationParent*>(source)->set(archive_dest, SCN::max_scn()))) {
      LOG_WARN("source set archive dest info failed", KR(ret), K(archive_dest));
    } else {
      ctx.set_source(source);
      LOG_INFO("init archive source succ", K(ctx), K(ls_id));
    }

    if (OB_FAIL(ret)) {
      logservice::ObResSrcAlloctor::free(source);
      source = nullptr;
    }
  }
  return ret;
}

int ObCdcFetcher::prepare_berfore_fetch_missing_(const ObLSID &ls_id,
    ClientLSCtx &ctx,
    palf::PalfHandleGuard &palf_handle_guard,
    bool &ls_exist_in_palf,
    bool &archive_is_on)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && OB_FAIL(init_palf_handle_guard_(ls_id, palf_handle_guard))) {
      if (OB_LS_NOT_EXIST != ret) {
        LOG_WARN("ObLogService open_palf fail", KR(ret), K(tenant_id_), K(ls_id));
      } else {
        ret = OB_SUCCESS;
        ls_exist_in_palf = false;
      }
    }

    if (OB_SUCC(ret) && OB_ISNULL(ctx.get_source()) && OB_FAIL(init_archive_source_(ctx, ls_id))) {
      if (OB_ALREADY_IN_NOARCHIVE_MODE == ret) {
        ret = OB_SUCCESS;
        archive_is_on = false;
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!ls_exist_in_palf && !archive_is_on)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls not exist in palf and archive is not on, not able to fetch missing log", KR(ret), K(ls_id));
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
