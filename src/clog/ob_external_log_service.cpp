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

#include "ob_external_log_service.h"
#include "lib/ob_running_mode.h"
#include "lib/stat/ob_diagnose_info.h"
#include "storage/ob_partition_service.h"
#include "ob_i_log_engine.h"
#include "ob_external_log_service_monitor.h"
#include "ob_clog_mgr.h"  // ObICLogMgr

namespace oceanbase {
using namespace common;
using namespace share;
using namespace obrpc;
using namespace clog;
using namespace storage;
namespace logservice {

int ObExtLogService::init(ObPartitionService* partition_service, ObILogEngine* log_engine, clog::ObICLogMgr* clog_mgr,
    const ObAddr& self_addr)
{
  int ret = OB_SUCCESS;

  const int64_t line_cache_size_in_file_count =
      !lib::is_mini_mode() ? LINE_CACHE_FIXED_SIZE_IN_FILE_COUNT : MINI_MODE_LINE_CACHE_FIXED_SIZE_IN_FILE_COUNT;
  if (OB_UNLIKELY(NULL == partition_service) || OB_UNLIKELY(NULL == log_engine) || OB_ISNULL(clog_mgr_ = clog_mgr) ||
      OB_UNLIKELY(!self_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "init error", K(ret), KP(log_engine), K(self_addr), K(clog_mgr));
  } else if (OB_FAIL(line_cache_.init(
                 LINE_CACHE_MAX_CACHE_FILE_COUNT, line_cache_size_in_file_count, ObModIds::OB_CLOG_EXT_LINE_CACHE))) {
    EXTLOG_LOG(WARN,
        "init line cache fail",
        K(ret),
        LITERAL_K(LINE_CACHE_MAX_CACHE_FILE_COUNT),
        LITERAL_K(LINE_CACHE_FIXED_SIZE_IN_FILE_COUNT));
  } else if (OB_FAIL(log_archive_line_cache_.init(LINE_CACHE_MAX_CACHE_FILE_COUNT,
                 LINE_CACHE_FIXED_SIZE_IN_FILE_COUNT_FOR_LOG_ARCHIVE,
                 ObModIds::OB_LOG_ARCHIVE_LINE_CACHE))) {
    EXTLOG_LOG(WARN,
        "init log archive line cache fail",
        K(ret),
        LITERAL_K(LINE_CACHE_MAX_CACHE_FILE_COUNT),
        LITERAL_K(LINE_CACHE_FIXED_SIZE_IN_FILE_COUNT));
  } else if (OB_FAIL(locator_.init(partition_service, log_engine))) {
    EXTLOG_LOG(WARN, "locator_ init error", K(ret), K(log_engine));
  } else if (OB_FAIL(fetcher_.init(line_cache_, log_engine, partition_service, self_addr))) {
    EXTLOG_LOG(WARN, "fetcher init error", K(ret), KP(log_engine), K(self_addr));
  } else if (OB_FAIL(archive_log_fetcher_.init(log_archive_line_cache_, log_engine))) {
    EXTLOG_LOG(WARN, "log_archive_fetcher init error", K(ret), KP(log_engine), K(self_addr));
  } else if (OB_FAIL(hb_handler_.init(partition_service, log_engine))) {
    EXTLOG_LOG(WARN, "hb_handler_ init error", K(ret));
  } else if (OB_FAIL(leader_hb_handler_.init(partition_service))) {
    EXTLOG_LOG(WARN, "leader_hb_handler_ init error", K(ret));
  } else {
    is_inited_ = true;
    EXTLOG_LOG(INFO,
        "ObExtLogService init success",
        KP(log_engine),
        LITERAL_K(LINE_CACHE_MAX_CACHE_FILE_COUNT),
        K(line_cache_size_in_file_count));
  }
  return ret;
}

void ObExtLogService::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    locator_.destroy();
    fetcher_.destroy();
    archive_log_fetcher_.destroy();
    hb_handler_.destroy();
    leader_hb_handler_.destroy();
    clog_mgr_ = NULL;
    line_cache_.destroy();
    log_archive_line_cache_.destroy();
  }
}

int ObExtLogService::req_start_log_id_by_ts_with_breakpoint(
    const ObLogReqStartLogIdByTsRequestWithBreakpoint& req_msg, ObLogReqStartLogIdByTsResponseWithBreakpoint& resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(clog_mgr_)) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObExtLogService not init", K(ret), K(clog_mgr_));
  } else if (OB_UNLIKELY(!clog_mgr_->is_scan_finished())) {
    resp.set_err(OB_SERVER_IS_INIT);
    EXTLOG_LOG(WARN,
        "all clog and ilog file are not scan-finised, can not serve "
        "req_start_log_id_by_ts_with_breakpoint RPC",
        K(req_msg),
        K(resp));
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    ret = locator_.req_start_log_id_by_ts_with_breakpoint(req_msg, resp);
    const int64_t end_ts = ObTimeUtility::current_time();
    ObExtLogServiceMonitor::locate_count();
    ObExtLogServiceMonitor::locate_time(end_ts - start_ts);
  }
  return ret;
}

int ObExtLogService::open_stream(const ObLogOpenStreamReq& req, const ObAddr& addr, ObLogOpenStreamResp& resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObExtLogService not init", K(ret));
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    ret = fetcher_.open_stream(req, addr, resp);
    const int64_t end_ts = ObTimeUtility::current_time();
    ObExtLogServiceMonitor::open_count();
    ObExtLogServiceMonitor::open_time(end_ts - start_ts);
  }
  return ret;
}

int ObExtLogService::fetch_log(
    const ObLogStreamFetchLogReq& req, ObLogStreamFetchLogResp& resp, const int64_t send_ts, const int64_t recv_ts)
{
  int ret = OB_SUCCESS;
  static const int64_t WARN_THRESHOLD = 1 * 1000 * 1000;  // 1 second

  if (IS_NOT_INIT || OB_ISNULL(clog_mgr_)) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObExtLogService not init", K(ret), K(clog_mgr_));
  } else if (OB_UNLIKELY(!clog_mgr_->is_scan_finished())) {
    resp.set_err(OB_SERVER_IS_INIT);
    EXTLOG_LOG(WARN,
        "all clog and ilog file are not scan-finised, can not serve fetch_log RPC",
        K(req),
        K(resp),
        K(send_ts),
        K(recv_ts));
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    ret = fetcher_.fetch_log(req, resp);
    const int64_t end_ts = ObTimeUtility::current_time();
    if (end_ts - start_ts > WARN_THRESHOLD) {
      EXTLOG_LOG(WARN, "fetch log cost too much time", "time", end_ts - start_ts, K(req), K(resp));
    }

    ObExtLogServiceMonitor::fetch_count();
    EVENT_INC(CLOG_EXTLOG_FETCH_RPC_COUNT);
    resp.set_l2s_net_time(recv_ts - send_ts);
    resp.set_svr_queue_time(start_ts - recv_ts);
    resp.set_process_time(end_ts - start_ts);
    ObExtLogServiceMonitor::l2s_time(recv_ts - send_ts);
    ObExtLogServiceMonitor::svr_queue_time(start_ts - recv_ts);
    ObExtLogServiceMonitor::fetch_time(end_ts - start_ts);

    EXTLOG_LOG(TRACE, "ObExtLogService fetch_log", K(ret), K(req), K(resp));
  }
  return ret;
}

int ObExtLogService::archive_fetch_log(const ObPGKey& pg_key, const ObReadParam& param, ObReadBuf& rbuf, ObReadRes& res)
{
  int ret = OB_SUCCESS;
  static const int64_t WARN_THRESHOLD = 1 * 1000 * 1000;  // 1 second

  if (IS_NOT_INIT || OB_ISNULL(clog_mgr_)) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObExtLogService not init", K(ret), K(clog_mgr_));
  } else if (OB_UNLIKELY(!clog_mgr_->is_scan_finished())) {
    ret = OB_EAGAIN;
    EXTLOG_LOG(
        WARN, "all clog and ilog file are not scan-finised, can not serve fetch_log", K(pg_key), K(param), KR(ret));
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    if (OB_FAIL(archive_log_fetcher_.fetch_log(pg_key, param, rbuf, res))) {
      EXTLOG_LOG(WARN, "failed to fetch log for log archiving", K(pg_key), K(param), KR(ret));
    } else {
      const int64_t end_ts = ObTimeUtility::current_time();
      if (end_ts - start_ts > WARN_THRESHOLD) {
        EXTLOG_LOG(
            WARN, "log archive fetch log cost too much time", K(pg_key), "time", end_ts - start_ts, KR(ret), K(param));
      }
      // TODO:stat info
      EXTLOG_LOG(TRACE, "ObExtLogService log_archive_fetch_log", KR(ret), K(param));
    }
  }
  return ret;
}

int ObExtLogService::req_heartbeat_info(
    const ObLogReqHeartbeatInfoRequest& req_msg, ObLogReqHeartbeatInfoResponse& resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObExtLogService not init", K(ret));
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    ret = hb_handler_.req_heartbeat_info(req_msg, resp);
    const int64_t end_ts = ObTimeUtility::current_time();
    ObExtLogServiceMonitor::heartbeat_count();
    EVENT_INC(CLOG_EXTLOG_HEARTBEAT_RPC_COUNT);
    ObExtLogServiceMonitor::heartbeat_time(end_ts - start_ts);
  }
  return ret;
}

int ObExtLogService::leader_heartbeat(
    const obrpc::ObLogLeaderHeartbeatReq& req_msg, obrpc::ObLogLeaderHeartbeatResp& resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObExtLogService not init", K(ret));
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    ret = leader_hb_handler_.leader_heartbeat(req_msg, resp);
    const int64_t end_ts = ObTimeUtility::current_time();
    ObExtLogServiceMonitor::heartbeat_count();
    EVENT_INC(CLOG_EXTLOG_HEARTBEAT_RPC_COUNT);
    ObExtLogServiceMonitor::heartbeat_time(end_ts - start_ts);
  }
  return ret;
}

int ObExtLogService::wash_expired_stream()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObExtLogService not init", K(ret));
  } else {
    ret = fetcher_.wash();
  }
  return ret;
}

int ObExtLogService::report_all_stream()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObExtLogService not init", K(ret));
  } else {
    fetcher_.print_all_stream();
  }
  return ret;
}

int ObExtLogService::StreamTimerTask::init(ObExtLogService* els)
{
  int ret = OB_SUCCESS;
  if (NULL == els) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    els_ = els;
  }
  EXTLOG_LOG(INFO, "StreamTimerTask init finish", K(ret), KP(els));
  return ret;
}

void ObExtLogService::StreamTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(els_)) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "timer task not init", K(ret));
  } else {
    // one wash stream operation per second
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      (void)els_->wash_expired_stream();
    }
    // print all stream status once in 30 seconds
    if (REACH_TIME_INTERVAL(30 * 1000 * 1000)) {
      (void)els_->report_all_stream();
    }
  }
}

/////////////////////////// LineCacheTimerTask ///////////////////////////
int ObExtLogService::LineCacheTimerTask::init(ObExtLogService* els)
{
  int ret = OB_SUCCESS;
  if (NULL == els) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    els_ = els;
  }
  EXTLOG_LOG(INFO, "LineCacheTimerTask init finish", K(ret), KP(els));
  return ret;
}

void ObExtLogService::LineCacheTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(els_)) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "timer task not init", K(ret));
  } else {
    // print monitoring
    if (REACH_TIME_INTERVAL(LINE_CACHE_STAT_INTERVAL)) {
      ObExtLogServiceMonitor::report();
      els_->line_cache_stat();
    }

    // cache elimination operation
    if (REACH_TIME_INTERVAL(LINE_CACHE_WASH_INTERVAL)) {
      els_->line_cache_wash();
    }
  }
}

}  // namespace logservice
}  // namespace oceanbase
