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

#include "ob_cdc_service.h"
#include "ob_cdc_service_monitor.h"

namespace oceanbase
{
namespace cdc
{
ObCdcService::ObCdcService()
  : is_inited_(false),
    stop_flag_(true),
    locator_(),
    fetcher_()
{
}

ObCdcService::~ObCdcService()
{
  destroy();
}

int ObCdcService::init(const uint64_t tenant_id,
    ObLSService *ls_service)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;;
    EXTLOG_LOG(WARN, "ObCdcService has inited", KR(ret));
  } else if (OB_FAIL(locator_.init(tenant_id))) {
    EXTLOG_LOG(WARN, "ObCdcStartLsnLocator init failed", KR(ret));
  } else if (OB_FAIL(fetcher_.init(tenant_id, ls_service))) {
    EXTLOG_LOG(WARN, "ObCdcFetcher init failed", KR(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObCdcService::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObCdcService not init", K(ret));
  } else {
    stop_flag_ = false;
  }

  return ret;
}

void ObCdcService::stop()
{
  ATOMIC_STORE(&stop_flag_, true);
}

void ObCdcService::wait()
{
  // do nothing
}

void ObCdcService::destroy()
{
  is_inited_ = false;
  stop_flag_ = true;
  locator_.destroy();
  fetcher_.destroy();
}

int ObCdcService::req_start_lsn_by_ts_ns(const obrpc::ObCdcReqStartLSNByTsReq &req,
    obrpc::ObCdcReqStartLSNByTsResp &resp)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObCdcService not init", K(ret));
  } else if (is_stoped()) {
    resp.set_err(OB_IN_STOP_STATE);
    EXTLOG_LOG(INFO, "ObCdcService is stopped", K(req));
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    ret = locator_.req_start_lsn_by_ts_ns(req, resp, stop_flag_);
    const int64_t end_ts = ObTimeUtility::current_time();
    ObCdcServiceMonitor::locate_count();
    ObCdcServiceMonitor::locate_time(end_ts - start_ts);
  }

  return ret;
}

int ObCdcService::fetch_log(const obrpc::ObCdcLSFetchLogReq &req,
    obrpc::ObCdcLSFetchLogResp &resp,
    const int64_t send_ts,
    const int64_t recv_ts)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObCdcService not init", KR(ret));
  } else if (is_stoped()) {
    resp.set_err(OB_IN_STOP_STATE);
    EXTLOG_LOG(INFO, "ObCdcService is stopped", K(req));
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    ret = fetcher_.fetch_log(req, resp);
    const int64_t end_ts = ObTimeUtility::current_time();
    if (end_ts - start_ts > FETCH_LOG_WARN_THRESHOLD) {
      EXTLOG_LOG(WARN, "fetch log cost too much time", "time", end_ts - start_ts, K(req), K(resp));
    }

    resp.set_l2s_net_time(recv_ts - send_ts);
    resp.set_svr_queue_time(start_ts - recv_ts);
    resp.set_process_time(end_ts - start_ts);
    do_monitor_stat_(start_ts, end_ts, send_ts, recv_ts);

    EXTLOG_LOG(TRACE, "ObCdcService fetch_log", K(ret), K(req), K(resp));
  }

  return ret;
}

int ObCdcService::fetch_missing_log(const obrpc::ObCdcLSFetchMissLogReq &req,
    obrpc::ObCdcLSFetchLogResp &resp,
    const int64_t send_ts,
    const int64_t recv_ts)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObCdcService not init", KR(ret));
  } else if (is_stoped()) {
    resp.set_err(OB_IN_STOP_STATE);
    EXTLOG_LOG(INFO, "ObCdcService is stopped", K(req));
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    ret = fetcher_.fetch_missing_log(req, resp);
    const int64_t end_ts = ObTimeUtility::current_time();
    if (end_ts - start_ts > FETCH_LOG_WARN_THRESHOLD) {
      EXTLOG_LOG(WARN, "fetch log cost too much time", "time", end_ts - start_ts, K(req), K(resp));
    }

    resp.set_l2s_net_time(recv_ts - send_ts);
    resp.set_svr_queue_time(start_ts - recv_ts);
    resp.set_process_time(end_ts - start_ts);
    do_monitor_stat_(start_ts, end_ts, send_ts, recv_ts);

    EXTLOG_LOG(TRACE, "ObCdcService fetch_log", K(ret), K(req), K(resp));
  }

  return ret;
}

void ObCdcService::do_monitor_stat_(const int64_t start_ts,
    const int64_t end_ts,
    const int64_t send_ts,
    const int64_t recv_ts)
{
  ObCdcServiceMonitor::fetch_count();
  EVENT_INC(CLOG_EXTLOG_FETCH_RPC_COUNT);
  ObCdcServiceMonitor::l2s_time(recv_ts - send_ts);
  ObCdcServiceMonitor::svr_queue_time(start_ts - recv_ts);
  ObCdcServiceMonitor::fetch_time(end_ts - start_ts);
}

} // namespace cdc
} // namespace oceanbase
