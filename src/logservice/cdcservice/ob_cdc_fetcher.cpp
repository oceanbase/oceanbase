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
#include "logservice/restoreservice/ob_remote_log_raw_reader.h"

namespace oceanbase
{
using namespace obrpc;
using namespace oceanbase::logservice;
using namespace oceanbase::palf;

namespace cdc
{
ERRSIM_POINT_DEF(ERRSIM_FETCH_LOG_RESP_ERROR);

ObCdcFetcher::ObCdcFetcher()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    host_(NULL),
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
    ObCdcService *host,
    archive::LargeBufferPool *buffer_pool,
    logservice::ObLogExternalStorageHandler *log_ext_handler)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(ls_service) || OB_ISNULL(buffer_pool)
      || OB_ISNULL(log_ext_handler) || OB_ISNULL(host)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_service), K(buffer_pool));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
    host_ = host;
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
    host_ = NULL;
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
    PalfGroupBufferIterator group_iter(ls_id.id(), palf::LogIOUser::CDC);
    const ObCdcRpcId &rpc_id = req.get_client_id();

    ClientLSCtx *ls_ctx = NULL;
    int8_t fetch_log_flag = req.get_flag();

    if (OB_FAIL(host_->get_or_create_client_ls_ctx(req.get_client_id(),
        req.get_tenant_id(), ls_id, fetch_log_flag,
        req.get_progress(), FetchLogProtocolType::LogGroupEntryProto, ls_ctx))) {
      LOG_WARN("failed to get or create client ls ctx", K(req), KP(ls_ctx));
    } else if (OB_ISNULL(ls_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null ctx afeter get_or_create_client_ls_ctx, unexpected", KP(ls_ctx), K(req));
    } else {
      ls_ctx->update_touch_ts();
      if (OB_FAIL(do_fetch_log_(req, frt, resp, *ls_ctx, fetch_log_time_stat))) {
        LOG_WARN("do fetch log error", KR(ret), K(req));
      }

      if (OB_NOT_NULL(ls_ctx)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(host_->revert_client_ls_ctx(ls_ctx))) {
          LOG_WARN_RET(tmp_ret, "failed to revert client ls ctx", K(req));
        } else {
          ls_ctx = nullptr;
        }
      }
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

int ObCdcFetcher::fetch_raw_log(const ObCdcFetchRawLogReq &req,
    ObCdcFetchRawLogResp &resp)
{
  int ret = OB_SUCCESS;
  const int64_t cur_tstamp = ObTimeUtility::current_time();
  ObCdcFetchRawStatus &stat = resp.get_fetch_status();
  ClientLSCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(host_->get_or_create_client_ls_ctx(req.get_client_id(),
      req.get_tenant_id(), req.get_ls_id(), req.get_flag(),
      req.get_progress(), FetchLogProtocolType::RawLogDataProto, ctx))) {
    LOG_WARN("failed to get or create client ls ctx when fetching raw log", K(req), KP(ctx));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null ctx afeter get_or_create_client_ls_ctx, unexpected", KP(ctx), K(req));
  } else {
    ctx->update_touch_ts();
    if (OB_FAIL(do_fetch_raw_log_(req, resp, *ctx))) {
      LOG_WARN("failed to fetch raw log", K(req), K(resp), KPC(ctx));
    }

    if (OB_NOT_NULL(ctx)) {
      if (OB_FAIL(host_->revert_client_ls_ctx(ctx))) {
        LOG_WARN("failed to revert client ls ctx", K(req));
      } else {
        ctx = nullptr;
      }
    }
  }

  LOG_INFO("fetch_raw_log done", K(req), K(resp));

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
  } else if (ERRSIM_FETCH_LOG_RESP_ERROR) {
    ret = ERRSIM_FETCH_LOG_RESP_ERROR;
    LOG_WARN("ERRSIM fetch log fail", KR(ret), "CDC_Connector_PID", req.get_client_pid(),
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
  // always reserve 4K for archive header
  const int64_t SINGLE_READ_SIZE = 16 * 1024 * 1024L - 4 * 1024;
  const int64_t MAX_RETRY_COUNT = 5;
  if (OB_FAIL(host_->init_archive_source_if_needed(ls_id, ctx))) {
    LOG_WARN("init archive source failed", K(ctx), K(ls_id));
  } else {
    const char *buf = NULL;
    int64_t buf_size = 0;
    share::SCN pre_scn;
    if (OB_FAIL(pre_scn.convert_from_ts(ctx.get_progress()/1000L))) {
      LOG_WARN("convert progress to scn failed", KR(ret), K(ctx));
    } else  {
      int64_t retry_count = 0;
      // Retry when we encounter OB_ITER_END, here is the scenario:
      // 1. Logs does not exist in palf, and it's the first time to read archive log;
      // 2. The lastest archive log is read, and the lastest archive file is larger than SINGLE_READ_SIZE;
      // 3. The start_lsn is larger than SINGLE_READ_SIZE;
      // 4. remoter iter read the latest archive file from the start;
      // 5. then the iter encountered a incomplete log entry, and find it's reading the latest archive file, then
      // return OB_ITER_END instead of OB_NEED_RETRY;
      // 6. So the PieceContext is maintained outside of the iter, if OB_ITER_END is encountered, just call
      // update_source_cb to update PieceContext, and remote iter would continue to read from the position where
      // it interrupted last time;
      // 7. update_source_cb must be valid in this scenario;
      do {
        if (! remote_iter.is_init() && OB_FAIL(remote_iter.init(tenant_id_, ls_id, pre_scn,
            start_lsn, LSN(LOG_MAX_LSN_VAL), large_buffer_pool_, log_ext_handler_, SINGLE_READ_SIZE))) {
          LOG_WARN("init remote log iterator failed", KR(ret), K(tenant_id_), K(ls_id));
        } else if (OB_FAIL(remote_iter.next(log_entry, lsn, buf, buf_size))) {
          // expected OB_ITER_END and OB_SUCCEES, error occurs when other code is returned.
          if (OB_ITER_END != ret) {
            LOG_WARN("iterate remote log failed", KR(ret), K(need_init_iter), K(ls_id));
          } else {
            remote_iter.update_source_cb();
            remote_iter.reset();
            LOG_INFO("get iter end from remote_iter, retry", K(retry_count), K(MAX_RETRY_COUNT));
          }
        } else if (start_lsn != lsn) {
          // to keep consistency with the ret code of palf
          ret = OB_INVALID_DATA;
          LOG_WARN("remote iterator returned unexpected log entry lsn", K(start_lsn), K(lsn), K(log_entry), K(ls_id),
              K(remote_iter));
        } else {

        }
      } while (OB_ITER_END == ret && ++retry_count < MAX_RETRY_COUNT);
    }
  }
  return ret;
}

int ObCdcFetcher::set_fetch_mode_before_fetch_log_(const ObLSID &ls_id,
    const LSN &start_lsn,
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
    // int64_t end_ts_ns = OB_INVALID_TIMESTAMP;
    // int64_t time_diff = OB_INVALID_TIMESTAMP;
    // const int64_t ctx_progress = ctx.get_progress();

    // if the gap between start_lsn and begin_lsn is less thant 2 * palf_block_size
    LSN begin_lsn;
    constexpr offset_t lsn_diff = 2 * palf::PALF_BLOCK_SIZE;
    if (OB_FAIL(palf_guard.get_begin_lsn(begin_lsn))) {
      LOG_WARN("get palf begin lsn failed", K(ls_id));
    } else if (start_lsn > begin_lsn && start_lsn - begin_lsn >= lsn_diff) {
      ctx.set_fetch_mode(FetchMode::FETCHMODE_ONLINE, "LogExistInPalf");
    }
    // if the gap between current progress and the latest is less than 1 min, which means
    // current fetch progress close to the latest log progress, try to switch fetch mode
    // set the switch interval to 10s in test switch fetch mode, the unit of measurement of log_ts(scn) is nano second.
    // const int64_t SECOND_NS = 1000L * 1000 * 1000;
    // SCN end_scn;
    // const int64_t SWITCH_INTERVAL = test_switch_fetch_mode ? 10L * SECOND_NS : 60 * SECOND_NS; //default 60s
    // if (OB_FAIL(palf_guard.get_end_scn(end_scn))) {
    //   LOG_WARN("get palf end ts failed", KR(ret));
    // } else {
    //   end_ts_ns = end_scn.get_val_for_logservice();
    //   time_diff = end_ts_ns - ctx_progress;
    //   if (time_diff <= SWITCH_INTERVAL) {
    //     ctx.set_fetch_mode(FetchMode::FETCHMODE_ONLINE, "LogNearLatest");
    //   }
    // }
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
  PalfGroupBufferIterator palf_iter(ls_id.id(), palf::LogIOUser::CDC);
  PalfHandleGuard palf_guard;
  int64_t version = 0;
  // use cached remote_iter
  ObCdcUpdateSourceFunctor update_source_func(ctx, version);
  ObCdcGetSourceFunctor get_source_func(ctx, version);
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
  } else if (OB_FAIL(set_fetch_mode_before_fetch_log_(ls_id, resp.get_next_req_lsn(), test_switch_fetch_mode,
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
            resp.set_feedback_type(FeedbackType::ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF);
            reach_max_lsn = true;
            LOG_INFO("reach max lsn in archive but ls not exists in this server, need switch server", K(ls_id));
          }
        } else if (OB_NEED_RETRY == ret) {
          frt.stop("ArchiveNeedRetry");
          ret = OB_SUCCESS;
        } else if (OB_ALREADY_IN_NOARCHIVE_MODE == ret || OB_ENTRY_NOT_EXIST == ret) {
          // archive is not on or lsn less than the start_lsn in archive
          ctx.set_fetch_mode(FetchMode::FETCHMODE_ONLINE, "LSReplicaMayMigrate");
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
          } else {
            ctx.set_fetch_mode(FetchMode::FETCHMODE_ONLINE, "LSReplicaMayMigrate");
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

  if (OB_ITER_END == ret) {
    // has iterated to the end of block.
    ret = OB_SUCCESS;
  }

  if (OB_SUCCESS == ret) {
    // do nothing
    if (ls_exist_in_palf && reach_max_lsn) {
      handle_when_reach_max_lsn_in_palf_(ls_id, palf_guard, fetched_log_count, frt, resp);
    }
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

  resp.set_feedback_type(FeedbackType::LOG_NOT_IN_THIS_SERVER);

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
  bool is_in_sync = false;
  int64_t leader_epoch = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(check_ls_sync_status_(ls_id, palf_handle_guard, role, is_in_sync))) {
    LOG_WARN("failed to check ls sync status", K(ls_id), K(role), K(is_in_sync));
  } else if (ObRole::FOLLOWER == role && ! is_in_sync) {
    resp.set_feedback_type(FeedbackType::LAGGED_FOLLOWER);
    LOG_INFO("catch a lag follower");
  }

  return ret;
}

int ObCdcFetcher::check_ls_sync_status_(const ObLSID &ls_id,
    palf::PalfHandleGuard &palf_handle_guard,
    ObRole &role,
    bool &in_sync)
{
  int ret = OB_SUCCESS;
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
    bool is_need_rebuild = false;

    if (OB_FAIL(ls_service_->get_ls(ls_id, ls_handle, ObLSGetMod::LOG_MOD))) {
      LOG_WARN("ls_service get_ls fail", KR(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is NULL", KR(ret), K(ls_id));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log_handler is NULL", KR(ret), K(ls_id));
    } else if (OB_FAIL(log_handler->is_in_sync(in_sync, is_need_rebuild))) {
      LOG_WARN("log_handler is_in_sync fail", KR(ret), K(ls_id), K(in_sync));
    }
  } else {
    // leader must be in_sync
    in_sync = true;
  }

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
    int64_t version = 0;
    ObCdcGetSourceFunctor get_source_func(ctx, version);
    ObCdcUpdateSourceFunctor update_source_func(ctx, version);
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
        PalfBufferIterator palf_iter(ls_id.id(), palf::LogIOUser::CDC);
        ObRemoteLogpEntryIterator remote_iter(get_source_func, update_source_func);
        const obrpc::ObCdcLSFetchMissLogReq::MissLogParam &miss_log_info = miss_log_array[idx];
        const LSN &missing_lsn = miss_log_info.miss_lsn_;
        palf::PalfBufferIterator log_entry_iter(ls_id.id(), palf::LogIOUser::CDC);
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
          } else if (! (log_fetched_in_palf || log_fetched_in_archive)) {
            ret = OB_ERR_OUT_OF_LOWER_BOUND;
            LOG_WARN("no log fetched from palf or archive, lower bound", K(log_fetched_in_palf),
                K(log_fetched_in_archive), K(missing_lsn), K(idx));
          } else {
            // failed
          }
        }
      } // for
    } // else

    if (OB_ITER_END == ret) {
      // has iterated to the end of block.
      ret = OB_SUCCESS;
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

    if (OB_SUCC(ret) && OB_FAIL(host_->init_archive_source_if_needed(ls_id, ctx))) {
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

int ObCdcFetcher::do_fetch_raw_log_(const obrpc::ObCdcFetchRawLogReq &req,
    obrpc::ObCdcFetchRawLogResp &resp,
    ClientLSCtx &ctx)
{
  int ret = OB_SUCCESS;

  const ObLSID &ls_id = req.get_ls_id();
  const LSN &start_lsn = req.get_start_lsn();
  const int64_t req_size = req.get_req_size();
  char *buffer = resp.get_log_buffer();
  SCN replayable_point_scn;
  const int64_t buffer_len = resp.get_buffer_len();
  const int64_t progress = req.get_progress();
  ObCdcFetchRawStatus &status = resp.get_fetch_status();
  palf::PalfHandleGuard palf_guard;
  palf::PalfHandle *palf_handle = nullptr;

  bool ls_exist_in_palf = true;
  bool log_may_exist_in_palf = true;

  bool archive_is_on = false;
  bool need_retry = true;
  bool fetch_log_succ = false;
  bool need_fetch_archive = false;

  constexpr int MAX_RETRY_COUNT = 3;
  int retry_count = 0;

  int64_t read_palf_time = 0;
  int64_t read_archive_time = 0;


  while (retry_count < MAX_RETRY_COUNT && !fetch_log_succ && need_retry) {
    int64_t fetch_palf_start_time = OB_INVALID_TIMESTAMP;
    int64_t fetch_archive_start_time = OB_INVALID_TIMESTAMP;
    need_fetch_archive = false;

    if (req_size > buffer_len) {
      ret = OB_BUF_NOT_ENOUGH;
      need_retry = false;
      LOG_WARN("buffer not enough, cannot fetch log", K(req_size), K(buffer_len));
    }

    if (OB_SUCC(ret) && OB_FAIL(get_replayable_point_scn_(ls_id, replayable_point_scn))) {
      LOG_WARN("failed to get replayable point scn", K(req));
    }

    if (OB_SUCC(ret) && log_may_exist_in_palf) {
      fetch_palf_start_time = ObTimeUtility::current_time();
      if (OB_FAIL(fetch_raw_log_in_palf_(ls_id, start_lsn, req_size, resp,
          ls_exist_in_palf, fetch_log_succ, ctx))) {
        if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
          ret = OB_SUCCESS;
          need_fetch_archive = true;
          log_may_exist_in_palf = false;
          LOG_INFO("lowerbound, the requested log doesn't exist in palf, try fetch archive", K(start_lsn),
              K(ls_id), K(req_size));
        } else if (OB_ERR_OUT_OF_UPPER_BOUND == ret) {
          need_retry = false;
          LOG_INFO("the requested log doesn't exist in palf, return", K(start_lsn), K(ls_id), K(req_size));
        } else {
          need_fetch_archive = true;
          LOG_WARN("fetch log from palf failed, try fetch archive", K(req));
        }
      } else if (! ls_exist_in_palf) {
        need_fetch_archive = true;
        log_may_exist_in_palf = false;
        LOG_INFO("logstream doesn't exist in palf", K(ls_id), K(start_lsn), K(req_size), K(ls_exist_in_palf));
      } else if (fetch_log_succ) {
        status.set_source(ObCdcFetchRawSource::PALF);
        if (FetchMode::FETCHMODE_ONLINE != ctx.get_fetch_mode()) {
          ctx.set_fetch_mode(FetchMode::FETCHMODE_ONLINE, "RawReadPalfSucc");
        }
      }
      read_palf_time += ObTimeUtility::current_time() - fetch_palf_start_time;
    }

    if (!fetch_log_succ && need_fetch_archive) {
      int last_ret = ret;
      fetch_archive_start_time = ObTimeUtility::current_time();
      if (OB_FAIL(fetch_raw_log_in_archive_(ls_id, start_lsn, req_size, progress, resp, archive_is_on, fetch_log_succ, ctx))) {
        if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
          need_retry = false;
          LOG_INFO("lower bound of archive log, there is no log in this server", K(start_lsn), K(ls_id), K(req_size));
        } else if (OB_ERR_OUT_OF_UPPER_BOUND == ret) {
          if (! log_may_exist_in_palf) {
            need_retry = false;
            ret = OB_ERR_OUT_OF_LOWER_BOUND;
            LOG_INFO("log doesn't exist in palf, and exceed the upper bound of archive", K(ls_exist_in_palf),
                K(log_may_exist_in_palf));
          }
        } else {
          LOG_WARN("fetch log from archive failed", K(req), K(ctx));
        }
      } else if (! archive_is_on) {
        if (! log_may_exist_in_palf) {
            need_retry = false;
            ret = OB_ERR_OUT_OF_LOWER_BOUND;
            LOG_INFO("archive seems not on and log doesn't exit in palf", K(archive_is_on), K(log_may_exist_in_palf));
          }
      } else if (fetch_log_succ) {
        status.set_source(ObCdcFetchRawSource::ARCHIVE);

        if (resp.get_read_size() < req_size && ! log_may_exist_in_palf) {
          resp.set_feedback(FeedbackType::ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF);
        }

        if (FetchMode::FETCHMODE_ARCHIVE != ctx.get_fetch_mode()) {
          ctx.set_fetch_mode(FetchMode::FETCHMODE_ARCHIVE, "RawReadArchiveSucc");
        }
      }
      read_archive_time += ObTimeUtility::current_time() - fetch_archive_start_time;
    }

    retry_count++;
  }

  status.set_read_palf_time(read_palf_time);
  status.set_read_archive_time(read_archive_time);

  if (retry_count > 1) {
    LOG_INFO("retry multiple times to read log", KR(ret), K(req), K(resp), K(retry_count),
        K(need_retry), K(fetch_log_succ));
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fetch raw log fail", K(req), K(resp));
  } else if (ERRSIM_FETCH_LOG_RESP_ERROR) {
    ret = ERRSIM_FETCH_LOG_RESP_ERROR;
    LOG_WARN("ERRSIM fetch raw log fail", K(req), K(resp));
  }

  return ret;
}

int ObCdcFetcher::fetch_raw_log_in_palf_(const ObLSID &ls_id,
    const LSN &start_lsn,
    const int64_t req_size,
    obrpc::ObCdcFetchRawLogResp &resp,
    bool &ls_exist_in_palf,
    bool &fetch_log_succ,
    ClientLSCtx &ctx)
{
  int ret = OB_SUCCESS;
  PalfHandleGuard palf_guard;
  PalfHandle *palf_handle = nullptr;
  int64_t read_size = 0;
  const int64_t buffer_len = resp.get_buffer_len();
  fetch_log_succ = false;

  if (OB_FAIL(init_palf_handle_guard_(ls_id, palf_guard))) {
    if (OB_LS_NOT_EXIST != ret) {
      LOG_WARN("failed to get palf handle guard", K(ls_id));
    } else {
      ls_exist_in_palf = false;
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(palf_handle = palf_guard.get_palf_handle())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null palf handle from palf guard", KP(palf_handle), K(ls_id));
  } else if(req_size > buffer_len) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("req_size is larger than buffer_len, buf not enough", K(req_size), K(buffer_len),
        K(resp));
  } else if (OB_FAIL(palf_handle->raw_read(start_lsn, resp.get_log_buffer(),
      req_size, read_size))) {
    if (OB_ERR_OUT_OF_LOWER_BOUND != ret &&
        OB_ERR_OUT_OF_UPPER_BOUND != ret &&
        OB_NEED_RETRY != ret) {
      LOG_WARN("raw read from palf failed", K(ls_id), K(start_lsn), K(req_size), K(resp));
    }
  } else {
    resp.set_read_size(read_size);
    if (read_size < req_size) {
      ObRole role = ObRole::INVALID_ROLE;
      bool is_in_sync = true;
      if (OB_FAIL(check_ls_sync_status_(ls_id, palf_guard, role, is_in_sync))) {
        LOG_WARN("failed to check ls sync status", K(ls_id), K(role), K(is_in_sync));
      } else if (ObRole::FOLLOWER == role && ! is_in_sync) {
        resp.set_feedback(FeedbackType::LAGGED_FOLLOWER);
      }
    }
  }

  return ret;
}

int ObCdcFetcher::fetch_raw_log_in_archive_(const ObLSID &ls_id,
    const LSN &start_lsn,
    const int64_t req_size,
    const int64_t progress,
    obrpc::ObCdcFetchRawLogResp &resp,
    bool &archive_is_on,
    bool &fetch_log_succ,
    ClientLSCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(host_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null host", KP(host_), K(ls_id), K(start_lsn));
  } else if (OB_FAIL(host_->init_archive_source_if_needed(ls_id, ctx))) {
    if (OB_ALREADY_IN_NOARCHIVE_MODE == ret) {
      archive_is_on = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to init archive source if needed", K(ls_id), K(ctx));
    }
  } else {
    int64_t version = 0;
    ObCdcUpdateSourceFunctor update_source_func(ctx, version);
    ObCdcGetSourceFunctor get_source_func(ctx, version);
    ObRemoteLogRawReader raw_reader(get_source_func, update_source_func);
    int64_t read_size = 0;

    if (OB_FAIL(raw_reader.init(tenant_id_, ls_id, resp.get_progress(), log_ext_handler_))) {
      LOG_WARN("raw reader failed to init", K(tenant_id_), K(ls_id), K(resp), KP(log_ext_handler_));
    } else if (OB_FAIL(raw_reader.raw_read(start_lsn, resp.get_log_buffer(), req_size, read_size))) {
       if (OB_ERR_OUT_OF_LOWER_BOUND != ret &&
          OB_ERR_OUT_OF_UPPER_BOUND != ret &&
          OB_NEED_RETRY != ret) {
        LOG_WARN("raw reader failed to read", K(start_lsn), K(req_size), K(read_size), K(resp));
      }
    } else {
      fetch_log_succ = true;
      resp.set_read_size(read_size);
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
