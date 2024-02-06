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
#include "logservice/ob_log_service.h"
#include "share/backup/ob_archive_persist_helper.h" // share::ObArchivePersistHelper
#include "logservice/restoreservice/ob_remote_log_source_allocator.h" // ObResSrcAlloctor::free

namespace oceanbase
{
namespace cdc
{
/////////////////////////////// ExpiredLSArchiveEntryFunctor /////////////////////////////////

ExpiredArchiveClientLSFunctor::ExpiredArchiveClientLSFunctor(const int64_t current_time):
    current_time_us_(current_time),
    valid_client_ls_cnt_(0),
    other_client_ls_cnt_(0)
{
}

ExpiredArchiveClientLSFunctor::~ExpiredArchiveClientLSFunctor()
{
  valid_client_ls_cnt_ = 0;
  other_client_ls_cnt_ = 0;
}

bool ExpiredArchiveClientLSFunctor::operator()(const ClientLSKey &key, ClientLSCtx *value)
{
  int ret = OB_SUCCESS;
  bool bret = true;
  if (OB_ISNULL(value)) {
    EXTLOG_LOG(WARN, "get null clientls ctx", K(key));
  } else {
    const FetchMode fetch_mode = value->get_fetch_mode();
    if (FetchMode::FETCHMODE_ARCHIVE == fetch_mode) {
      valid_client_ls_cnt_++;
    } else {
      other_client_ls_cnt_++;
    }
  }

  return bret;
}

///////////////////////////////////////////ObCdcService///////////////////////////////////////////

// suppose archive log only has one destination.
int ObCdcService::get_backup_dest(const share::ObLSID &ls_id, share::ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  ObCdcService *cdc_service = MTL(logservice::ObLogService *)->get_cdc_service();
  ObArchiveDestInfo archive_dest;
  if (OB_ISNULL(cdc_service)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(WARN, "cdc service is null, unexpected", KR(ret));
  } else if (FALSE_IT(archive_dest = cdc_service->get_archive_dest_info())) {
  } else if (archive_dest.empty()) {
    ret = OB_ALREADY_IN_NOARCHIVE_MODE;
    EXTLOG_LOG(WARN, "archivelog is off yet", KR(ret), K(MTL_ID()));
  } else if (OB_FAIL(backup_dest.set(archive_dest.at(0).second))) {
    EXTLOG_LOG(WARN, "failed to set backup dest info", KR(ret), K(archive_dest));
  } else { }
  return ret;
}

ObCdcService::ObCdcService()
  : is_inited_(false),
    stop_flag_(true),
    locator_(),
    fetcher_(),
    tg_id_(-1),
    dest_info_(),
    dest_info_lock_(),
    ls_ctx_map_(),
    large_buffer_pool_(),
    log_ext_handler_()
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
  } else if (OB_FAIL(ls_ctx_map_.init("ExtClientLSCtxM", tenant_id))) {
    EXTLOG_LOG(WARN, "ls ctx map init failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(large_buffer_pool_.init("CDCService", 1L * 1024 * 1024 * 1024))) {
    EXTLOG_LOG(WARN, "large buffer pool init failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(log_ext_handler_.init())) {
    EXTLOG_LOG(WARN, "log ext handler init failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(locator_.init(tenant_id, &large_buffer_pool_, &log_ext_handler_))) {
    EXTLOG_LOG(WARN, "ObCdcStartLsnLocator init failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(fetcher_.init(tenant_id, ls_service, &large_buffer_pool_, &log_ext_handler_))) {
    EXTLOG_LOG(WARN, "ObCdcFetcher init failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(create_tenant_tg_(tenant_id))) {
    EXTLOG_LOG(WARN, "cdc thread group create failed", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObCdcService::run1()
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = MTL_ID();
  lib::set_thread_name("CdcSrv");
  if (IS_NOT_INIT) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "ObCdcService is not initialized", KR(ret));
  } else {
    static const int64_t BASE_INTERVAL = 1L * 1000 * 1000;
    static const int64_t QUERY_INTERVAL = 10L * BASE_INTERVAL;
    static const int64_t RECYCLE_INTERVAL = 10L * 60 * BASE_INTERVAL;
    static const int64_t BUFFER_POOL_PURGE_INTERVAL = 10L * 60 * BASE_INTERVAL;
    static const int64_t CHECK_CDC_READ_ARCHIVE_INTERVAL = 10L * BASE_INTERVAL;
    int64_t last_query_ts = 0;
    int64_t last_recycle_ts = 0;
    int64_t last_purge_ts = 0;
    int64_t last_check_cdc_read_archive_ts = 0;
    while(! is_stoped()) {
      // archive is always off for sys tenant, no need to query archive dest
      int64_t current_ts = ObTimeUtility::current_time();
      IGNORE_RETURN lib::Thread::update_loop_ts(current_ts);
      if (OB_SYS_TENANT_ID != tenant_id) {
        if (current_ts - last_query_ts >= QUERY_INTERVAL) {
          // the change of archive dest info is not supported
          // TODO
          if (OB_FAIL(query_tenant_archive_info_())) {
            EXTLOG_LOG(WARN, "query_tenant_archive_info_ failed", KR(ret));
          } else {
            EXTLOG_LOG(INFO, "query dest_info_ succ", K_(dest_info));
          }
          last_query_ts = current_ts;
        }
      }
      // but sys tenant still hold the ctx when fetching log
      if (current_ts - last_recycle_ts >= RECYCLE_INTERVAL) {
        if (OB_FAIL(recycle_expired_ctx_(current_ts))) {
          EXTLOG_LOG(WARN, "failed to recycle expired ctx", KR(ret));
        } else {
          int64_t count = ls_ctx_map_.count();
          EXTLOG_LOG(INFO, "total number of items in ctx map ", K(count));
        }
        last_recycle_ts = current_ts;
      }

      if (current_ts - last_purge_ts >= BUFFER_POOL_PURGE_INTERVAL) {
        large_buffer_pool_.weed_out();
        last_purge_ts = current_ts;
      }

      if (current_ts - last_check_cdc_read_archive_ts >= CHECK_CDC_READ_ARCHIVE_INTERVAL) {
        if (OB_FAIL(resize_log_ext_handler_())) {
          EXTLOG_LOG(WARN, "failed to resize log ext handler");
        }
        last_check_cdc_read_archive_ts = current_ts;
      }
      ob_usleep(static_cast<uint32_t>(BASE_INTERVAL));
    }
  }
}

int ObCdcService::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObCdcService not init", K(ret));
  } else if (OB_FAIL(log_ext_handler_.start(0))) {
    EXTLOG_LOG(WARN, "log ext handler start failed", K(ret));
  } else if (OB_FAIL(start_tenant_tg_(MTL_ID()))) {
    EXTLOG_LOG(ERROR, "start CDCService failed", KR(ret));
  } else {
    stop_flag_ = false;
  }

  return ret;
}

void ObCdcService::stop()
{
  ATOMIC_STORE(&stop_flag_, true);
  stop_tenant_tg_(MTL_ID());
  log_ext_handler_.stop();
}

void ObCdcService::wait()
{
  wait_tenant_tg_(MTL_ID());
  log_ext_handler_.wait();
  // do nothing
}

void ObCdcService::destroy()
{
  is_inited_ = false;
  stop_flag_ = true;
  destroy_tenant_tg_(MTL_ID());
  fetcher_.destroy();
  locator_.destroy();
  dest_info_.reset();
  large_buffer_pool_.destroy();
  ls_ctx_map_.destroy();
  log_ext_handler_.destroy();
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
      EXTLOG_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "fetch log cost too much time", "time", end_ts - start_ts, K(req), K(resp));
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
      EXTLOG_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "fetch log cost too much time", "time", end_ts - start_ts, K(req), K(resp));
    }

    resp.set_l2s_net_time(recv_ts - send_ts);
    resp.set_svr_queue_time(start_ts - recv_ts);
    resp.set_process_time(end_ts - start_ts);
    do_monitor_stat_(start_ts, end_ts, send_ts, recv_ts);

    EXTLOG_LOG(TRACE, "ObCdcService fetch_log", K(ret), K(req), K(resp));
  }

  return ret;
}

int ObCdcService::query_tenant_archive_info_()
{
  int ret = OB_SUCCESS;
  share::ObArchivePersistHelper helper;
  uint64_t tenant_id = MTL_ID();
  ObMySQLProxy *mysql_proxy = GCTX.sql_proxy_;
  ObArchiveDestInfo tmp_info;

  if (OB_ISNULL(mysql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "mysql_proxy is null, unexpected", KR(ret));
  } else if (OB_FAIL(helper.init(tenant_id))) {
    EXTLOG_LOG(WARN, "init ObArchivePersistHelper failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(helper.get_valid_dest_pairs(*mysql_proxy, tmp_info))) {
    EXTLOG_LOG(WARN, "get_valid_dest_pairs failed", KR(ret), K(tenant_id));
  } else {
    // to minimize lock conflict
    ObSpinLockGuard lock_guard(dest_info_lock_);
    dest_info_ = tmp_info;
  }
  return ret;
}

int ObCdcService::recycle_expired_ctx_(const int64_t cur_ts)
{
  int ret = OB_SUCCESS;
  RecycleCtxFunctor recycle_func(cur_ts);
  if (OB_FAIL(ls_ctx_map_.remove_if(recycle_func))) {
    EXTLOG_LOG(WARN, "recycle expired ctx failed", KR(ret), K(cur_ts));
  }
  return OB_SUCCESS;
}

int ObCdcService::resize_log_ext_handler_()
{
  int ret = OB_SUCCESS;

  const int64_t current_ts = ObTimeUtility::current_time();
  const int64_t tenant_max_cpu = MTL_CPU_COUNT();
  ExpiredArchiveClientLSFunctor functor(current_ts);
  ObStorageType type = common::OB_STORAGE_MAX_TYPE;
  ObArchiveDestInfo dest_info = get_archive_dest_info();

  if (OB_FAIL(ls_ctx_map_.for_each(functor))) {
    EXTLOG_LOG(ERROR, "failed to get expired archive client ls key in ls_ctx_map");
  } else {
    const int64_t other_ls_count = functor.get_other_client_ls_cnt();
    const int64_t valid_ls_count = functor.get_valid_client_ls_cnt();
    const int64_t single_read_concurrency = 8; // default 8
    const int64_t new_concurrency = min(tenant_max_cpu, (single_read_concurrency - 1) * valid_ls_count);

    if (OB_FAIL(log_ext_handler_.resize(new_concurrency))) {
      EXTLOG_LOG(WARN, "log_ext_handler failed to resize", K(new_concurrency));
    }

    if (OB_SUCC(ret)) {
      EXTLOG_LOG(INFO, "finish to resize log external storage handler", K(current_ts),
          K(tenant_max_cpu), K(valid_ls_count), K(other_ls_count), K(new_concurrency));
    }
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

int ObCdcService::create_tenant_tg_(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (! is_meta_tenant(tenant_id)) {
    if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::CDCService, tg_id_))) {
      EXTLOG_LOG(WARN, "cdc thread group create for non-meta-tenant failed", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObCdcService::start_tenant_tg_(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (! is_meta_tenant(tenant_id)) {
    if (OB_FAIL(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
      EXTLOG_LOG(WARN, "cdcservie thread group set runnable and start failed", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

void ObCdcService::wait_tenant_tg_(const int64_t tenant_id)
{
  if (! is_meta_tenant(tenant_id)) {
    TG_WAIT(tg_id_);
  }
}

void ObCdcService::stop_tenant_tg_(const int64_t tenant_id)
{
  if (! is_meta_tenant(tenant_id)) {
    TG_STOP(tg_id_);
  }
}

void ObCdcService::destroy_tenant_tg_(const int64_t tenant_id)
{
  if (! is_meta_tenant(tenant_id)) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = -1;
}

} // namespace cdc
} // namespace oceanbase
