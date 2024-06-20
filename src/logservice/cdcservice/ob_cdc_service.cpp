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
    // ignore ret
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

/////////////////////////////////////////// UpdateCtxFunctor ///////////////////////////////////////////

int UpdateCtxFunctor::init(const ObBackupPathString &dest_str, const int64_t version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dest_.set(dest_str))) {
    EXTLOG_LOG(WARN, "failed to set dest_str to dest", K(dest_str), K(version));
  } else {
    dest_ver_ = version;
    is_inited_ = true;
  }
  return ret;
}

bool UpdateCtxFunctor::operator()(const ClientLSKey &key, ClientLSCtx *value)
{
  bool bret = true;
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    bret = false;
    EXTLOG_LOG(WARN, "update ctx functor has not been inited");
  } else if (OB_ISNULL(value)) {
    // fatal error, not continue
    ret = OB_ERR_UNEXPECTED;
    bret = false;
    EXTLOG_LOG(WARN, "get null ctx when updating ctx", KP(value), K(key));
  } else if (OB_FAIL(value->try_change_archive_source(key.get_ls_id(), dest_, dest_ver_))) {
    if (OB_NO_NEED_UPDATE != ret) {
      bret = false;
      EXTLOG_LOG(WARN, "failed to change archive source for ctx", KPC(value), K(dest_), K(dest_ver_));
    }
  }

  return bret;
}

/////////////////////////////////////////// ObCdcService ///////////////////////////////////////////

ObCdcService::ObCdcService()
  : is_inited_(false),
    stop_flag_(true),
    tenant_id_(OB_INVALID_TENANT_ID),
    locator_(),
    fetcher_(),
    tg_id_(-1),
    dest_info_version_(0),
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
  } else if (OB_FAIL(locator_.init(tenant_id, this, &large_buffer_pool_, &log_ext_handler_))) {
    EXTLOG_LOG(WARN, "ObCdcStartLsnLocator init failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(fetcher_.init(tenant_id, ls_service, this, &large_buffer_pool_, &log_ext_handler_))) {
    EXTLOG_LOG(WARN, "ObCdcFetcher init failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(create_tenant_tg_(tenant_id))) {
    EXTLOG_LOG(WARN, "cdc thread group create failed", KR(ret), K(tenant_id));
  } else {
    dest_info_version_ = 0;
    tenant_id_ = tenant_id;
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
    while(! has_set_stop()) {
      // archive is always off for sys tenant, no need to query archive dest
      int64_t current_ts = ObTimeUtility::current_time();
      IGNORE_RETURN lib::Thread::update_loop_ts(current_ts);
      if (OB_SYS_TENANT_ID != tenant_id) {
        if (current_ts - last_query_ts >= QUERY_INTERVAL) {
          // the change of archive dest info is not supported
          bool archive_dest_changed = false;
          if (OB_FAIL(query_tenant_archive_info_(archive_dest_changed))) {
            EXTLOG_LOG(WARN, "query_tenant_archive_info_ failed", KR(ret));
          } else {
            EXTLOG_LOG(INFO, "query dest_info_ succ", K_(dest_info), K(archive_dest_changed));
            if (archive_dest_changed) {
              if (OB_FAIL(update_archive_dest_for_ctx_())) {
                EXTLOG_LOG(WARN, "failed to update archive dest", K(dest_info_), K(dest_info_version_));
              } else {
                EXTLOG_LOG(INFO, "update archive dest succ", K(dest_info_), K(dest_info_version_));
              }
            }
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

    EXTLOG_LOG(INFO, "CdcSrv Thread Exit due to has_set_stop", "stop_flag", has_set_stop());
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
  } else if (OB_FAIL(start_tenant_tg_(tenant_id_))) {
    EXTLOG_LOG(ERROR, "start CDCService failed", KR(ret));
  } else {
    stop_flag_ = false;
  }

  return ret;
}

void ObCdcService::stop()
{
  ATOMIC_STORE(&stop_flag_, true);
  stop_tenant_tg_(tenant_id_);
  log_ext_handler_.stop();
}

void ObCdcService::wait()
{
  wait_tenant_tg_(tenant_id_);
  log_ext_handler_.wait();
  // do nothing
}

void ObCdcService::destroy()
{
  is_inited_ = false;
  stop_flag_ = true;
  destroy_tenant_tg_(tenant_id_);
  fetcher_.destroy();
  locator_.destroy();
  dest_info_.reset();
  dest_info_version_ = 0;
  large_buffer_pool_.destroy();
  ls_ctx_map_.destroy();
  log_ext_handler_.destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
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

int ObCdcService::get_archive_dest_snapshot(const ObLSID &ls_id, ObBackupDest &archive_dest)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObCdcService not init", KR(ret));
  } else if (is_stoped()) {
    ret = OB_IN_STOP_STATE;
    EXTLOG_LOG(INFO, "ObCdcService is stopped");
  } else {
    ObBackupPathString archive_dest_str;
    if (OB_FAIL(get_archive_dest_path_snapshot_(archive_dest_str))) {
      EXTLOG_LOG(WARN, "failed to get archive dest path string", K(archive_dest_str), K(ls_id));
    } else if (OB_FAIL(archive_dest.set(archive_dest_str))) {
      EXTLOG_LOG(WARN, "failed to set archive_dest_str to archive dest", K(archive_dest_str), K(ls_id));
    }
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

int ObCdcService::fetch_raw_log(const obrpc::ObCdcFetchRawLogReq &req,
    obrpc::ObCdcFetchRawLogResp &resp,
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
    ObCdcFetchRawStatus &status = resp.get_fetch_status();
    ret = fetcher_.fetch_raw_log(req, resp);
    const int64_t end_ts = ObTimeUtility::current_time();
    if (end_ts - start_ts > FETCH_LOG_WARN_THRESHOLD) {
      EXTLOG_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "fetch raw log cost too much time", "time", end_ts - start_ts, K(req), K(resp));
    }
    status.set_local_to_svr_time(recv_ts - send_ts);
    status.set_queue_time(start_ts - recv_ts);
    status.set_process_time(end_ts - start_ts);
    do_monitor_stat_(start_ts, end_ts, send_ts, recv_ts);

    EXTLOG_LOG(TRACE, "ObCdcService fetch_raw_log", K(ret), K(req), K(resp));
  }

  return ret;
}

int ObCdcService::get_or_create_client_ls_ctx(const obrpc::ObCdcRpcId &client_id,
    const uint64_t client_tenant_id,
    const ObLSID &ls_id,
    const int8_t flag,
    const int64_t client_progress,
    const FetchLogProtocolType proto_type,
    ClientLSCtx *&ctx)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObCdcService not init", KR(ret));
  } else if (is_stoped()) {
    ret = OB_IN_STOP_STATE;
    EXTLOG_LOG(INFO, "ObCdcService is stopped", K(client_id), K(client_tenant_id),
        K(ls_id));
  } else {
    ClientLSKey ls_key(client_id.get_addr(), client_id.get_pid(), client_tenant_id, ls_id);

    // create ClientLSCtx when fetch_log for better maintainablity
    // about the reason why not create ClientLSCtx when locating start lsn, considering the case:
    // 1. Client locate lsn at server1 and server1 create the clientlsctx;
    // 2. when client is about to fetch log in server1, server1 becomes unavailable;
    // 3. then the client switch to server2 to fetch log;
    // 4. server2 doesn't have the context of clientls, and need to create one
    // So we just create ctx when fetching log
    if (OB_FAIL(ls_ctx_map_.get(ls_key, ctx))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(ls_ctx_map_.create(ls_key, ctx))) {
          if (OB_ENTRY_EXIST != ret) {
            EXTLOG_LOG(WARN, "create client ls ctx failed", KR(ret), K(ls_key));
          } else if (OB_FAIL(ls_ctx_map_.get(ls_key, ctx))) {
            EXTLOG_LOG(WARN, "failed to get ctx from ctx_map when creating ctx failed", K(ls_key));
          }
        } else if (FetchLogProtocolType::LogGroupEntryProto == proto_type) {
          if (OB_FAIL(ctx->init(client_progress, proto_type))) {
            EXTLOG_LOG(WARN, "failed to init client ls ctx", KR(ret), K(client_progress), K(proto_type));
          } else {
            // if test_switch_mode is enabled, set the init fetch mode to FETCHMODE_ARCHIVE
            // so that the fetch mode could be switched to FETCHMODE_ONLINE in the next rpc in some test cases.
            if (flag & ObCdcRpcTestFlag::OBCDC_RPC_TEST_SWITCH_MODE) {
              ctx->set_fetch_mode(FetchMode::FETCHMODE_ARCHIVE, "InitTestSwitchMode");
            }
            EXTLOG_LOG(INFO, "create client ls ctx succ", K(ls_key), K(ctx));
          }
        } else if (FetchLogProtocolType::RawLogDataProto == proto_type) {
          ctx->set_proto_type(proto_type);
        } else {
          ret = OB_INVALID_ARGUMENT;
          EXTLOG_LOG(WARN, "get invalid proto_type", K(proto_type), K(ls_id), K(client_id));
        }
      } else {
        EXTLOG_LOG(ERROR, "get client ls ctx from ctx map failed", KR(ret));
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(ctx)) {
      ctx->update_touch_ts();
    }
  }

  return ret;
}

int ObCdcService::revert_client_ls_ctx(ClientLSCtx *ctx)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObCdcService not init", KR(ret));
  } else if (is_stoped()) {
    ret = OB_IN_STOP_STATE;
    EXTLOG_LOG(INFO, "ObCdcService is stopped");
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "get null client ls ctx", KP(ctx));
  } else {
    ls_ctx_map_.revert(ctx);
  }

  return ret;
}

int ObCdcService::init_archive_source_if_needed(const ObLSID &ls_id, ClientLSCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "ObCdcService not init", KR(ret));
  } else if (is_stoped()) {
    ret = OB_IN_STOP_STATE;
    EXTLOG_LOG(INFO, "ObCdcService is stopped");
  } else if (ctx.archive_source_inited()) {
    EXTLOG_LOG(TRACE, "the archive source of ctx has been inited", K(ctx));
  } else {
    share::ObBackupDest archive_dest;
    // we hold dest_info_lock here, and we would try to get source_lock_ in ctx later.
    // make sure when we hold source_lock_, we wouldn't try to get dest_info_lock_,
    // make sure the lock is locked in certain order to prevent deadlock.
    SpinRLockGuard dest_info_guard(dest_info_lock_);

    if (dest_info_.empty()) {
      ret = OB_ALREADY_IN_NOARCHIVE_MODE;
      EXTLOG_LOG(WARN, "archivelog is off yet", KR(ret), K(MTL_ID()));
    } else if (OB_FAIL(archive_dest.set(dest_info_.at(0).second))) {
      EXTLOG_LOG(WARN, "failed to set backup dest info", KR(ret), K(archive_dest), "path",
          dest_info_.at(0).second);
    } else if (OB_FAIL(ctx.try_init_archive_source(ls_id, archive_dest, dest_info_version_))) {
      if (OB_INIT_TWICE != ret) {
        EXTLOG_LOG(WARN, "failed to init archive source", K(ls_id), K(archive_dest), K(dest_info_version_));
      } else {
        ret = OB_SUCCESS;
        EXTLOG_LOG(INFO, "ctx source has been inited", K(ls_id), K(archive_dest), K(dest_info_version_));
      }
    }
  }

  return ret;
}

int ObCdcService::query_tenant_archive_info_(bool &archive_dest_changed)
{
  int ret = OB_SUCCESS;
  share::ObArchivePersistHelper helper;
  uint64_t tenant_id = MTL_ID();
  ObMySQLProxy *mysql_proxy = GCTX.sql_proxy_;
  ObArchiveDestInfo tmp_info;
  archive_dest_changed = false;
  if (OB_ISNULL(mysql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "mysql_proxy is null, unexpected", KR(ret));
  } else if (OB_FAIL(helper.init(tenant_id))) {
    EXTLOG_LOG(WARN, "init ObArchivePersistHelper failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(helper.get_valid_dest_pairs(*mysql_proxy, tmp_info))) {
    EXTLOG_LOG(WARN, "get_valid_dest_pairs failed", KR(ret), K(tenant_id));
  } else if (is_archive_dest_changed_(tmp_info)) {
    // to minimize lock conflict
    SpinWLockGuard lock_guard(dest_info_lock_);
    dest_info_ = tmp_info;
    dest_info_version_++;
    archive_dest_changed = true;
  }
  return ret;
}

bool ObCdcService::is_archive_dest_changed_(const ObArchiveDestInfo &info)
{
  bool bret = false;
  // no need to use lock to protect dest_info here, dest_info will only be changed
  // in caller thread.

  if (dest_info_.count() != info.count()) {
    bret = true;
  } else if (0 == dest_info_.count()) {
    bret = false;
  } else if (dest_info_.at(0).second != info.at(0).second) {
    // TODO: multi archive destination is not supported,
    // use a more comprehensively comparation method to find differece for each
    // archive destination
    bret = true;
  }

  return bret;
}

int ObCdcService::update_archive_dest_for_ctx_()
{
  int ret = OB_SUCCESS;

  UpdateCtxFunctor update_func;
  if (dest_info_.count() > 0) {
    if (OB_FAIL(update_func.init(dest_info_.at(0).second, dest_info_version_))) {
      EXTLOG_LOG(WARN, "update functor failed to init", K(dest_info_), K(dest_info_version_));
    } else if (OB_FAIL(ls_ctx_map_.for_each(update_func))) {
      EXTLOG_LOG(WARN, "failed to update ctx in ctx map");
    }
  } else {
    EXTLOG_LOG(INFO, "don't update archive dest if no archive dest found", K(dest_info_),
        K(dest_info_version_));
  }

  return ret;
}

int ObCdcService::get_archive_dest_path_snapshot_(ObBackupPathString &archive_dest_str)
{
  int ret = OB_SUCCESS;

  SpinRLockGuard dest_info_read_lock(dest_info_lock_);
  if (dest_info_.empty()) {
    ret = OB_ALREADY_IN_NOARCHIVE_MODE;
    EXTLOG_LOG(INFO, "archive is not enabled yet", K(dest_info_));
  } else if (OB_FAIL(archive_dest_str.assign(dest_info_.at(0).second))) {
    EXTLOG_LOG(WARN, "failed to assign dest_info to archive_dest_str", K(dest_info_), K(dest_info_version_));
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
  // TODO: adapt threads number according to different storage devices

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
