/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * Fetcher
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_fetcher.h"

#include "lib/oblog/ob_log_module.h"  // LOG_*
#include "logservice/common_util/ob_log_time_utils.h"

#include "ob_log_config.h"            // ObLogFetcherConfig
#include "ob_log_trace_id.h"          // get_self_addr
#include "ob_log_fetcher_err_handler.h"          // IObLogErrHandler

using namespace oceanbase::common;

namespace oceanbase
{
namespace logfetcher
{

///////////////////////////////////////////// ObLogFetcher /////////////////////////////////////////////

bool ObLogFetcher::g_print_ls_heartbeat_info =
    ObLogFetcherConfig::default_print_ls_heartbeat_info;

ObLogFetcher::ObLogFetcher() :
    is_inited_(false),
    log_fetcher_user_(LogFetcherUser::UNKNOWN),
    cluster_id_(OB_INVALID_CLUSTER_ID),
    source_tenant_id_(OB_INVALID_TENANT_ID),
    self_tenant_id_(OB_INVALID_TENANT_ID),
    cfg_(nullptr),
    is_loading_data_dict_baseline_data_(false),
    fetching_mode_(ClientFetchingMode::FETCHING_MODE_UNKNOWN),
    archive_dest_(),
    large_buffer_pool_(),
    log_ext_handler_concurrency_(0),
    log_ext_handler_(),
    ls_ctx_add_info_factory_(NULL),
    err_handler_(NULL),
    ls_fetch_mgr_(),
    progress_controller_(),
    rpc_(),
    log_route_service_(),
    start_lsn_locator_(),
    idle_pool_(),
    dead_pool_(),
    stream_worker_(),
    fs_container_mgr_(),
    stop_flag_(true),
    paused_(false),
    pause_time_(OB_INVALID_TIMESTAMP),
    resume_time_(OB_INVALID_TIMESTAMP),
    decompression_blk_mgr_(DECOMPRESSION_MEM_LIMIT_THRESHOLD),
    decompression_alloc_(ObMemAttr(OB_SERVER_TENANT_ID, "decompress_buf"), common::OB_MALLOC_BIG_BLOCK_SIZE, decompression_blk_mgr_)
{
}

ObLogFetcher::~ObLogFetcher()
{
  destroy();
}

#ifdef ERRSIM
ERRSIM_POINT_DEF(LOG_FETCHER_LOG_EXT_HANDLER_INIT_FAIL);
ERRSIM_POINT_DEF(LOG_FETCHER_STREAM_WORKER_INIT_FAIL);
#endif
int ObLogFetcher::init(
    const LogFetcherUser &log_fetcher_user,
    const int64_t cluster_id,
    const uint64_t source_tenant_id,
    const uint64_t self_tenant_id,
    const bool is_loading_data_dict_baseline_data,
    const ClientFetchingMode fetching_mode,
    const ObBackupPathString &archive_dest,
    const ObLogFetcherConfig &cfg,
    ObILogFetcherLSCtxFactory &ls_ctx_factory,
    ObILogFetcherLSCtxAddInfoFactory &ls_ctx_add_info_factory,
    ILogFetcherHandler &log_handler,
    ObISQLClient *proxy,
    logfetcher::IObLogErrHandler *err_handler)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", KR(ret));
  } else if (OB_ISNULL(ls_ctx_add_info_factory_ = &ls_ctx_add_info_factory)
      || OB_ISNULL(err_handler_ = err_handler)
      || OB_ISNULL(proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(err_handler), K(proxy));
  } else {
    cfg_ = &cfg;
    // set self_tenant_id before suggest_cached_rpc_res_count
    log_fetcher_user_ = log_fetcher_user;
    fetching_mode_ = fetching_mode;
    self_tenant_id_ = self_tenant_id;
    // Before the LogFetcher module is initialized, the following configuration items need to be loaded
    configure(cfg);
    int64_t cached_fetch_log_arpc_res_cnt = cfg.rpc_result_cached_count;
    const int64_t MIN_FETCH_LOG_ARPC_RES_CNT = 4;
    if (is_standby(log_fetcher_user)) {
      cached_fetch_log_arpc_res_cnt =
        suggest_cached_rpc_res_count_(MIN_FETCH_LOG_ARPC_RES_CNT, cached_fetch_log_arpc_res_cnt);
    }
    const common::ObRegion region(cfg.region.str());

    if (is_integrated_fetching_mode(fetching_mode_) && OB_FAIL(log_route_service_.init(
        proxy,
        region,
        cluster_id,
        false/*is_across_cluster*/,
        err_handler,
        cfg.server_blacklist.str(),
        cfg.log_router_background_refresh_interval_sec,
        cfg.all_server_cache_update_interval_sec,
        cfg.all_zone_cache_update_interval_sec,
        cfg.blacklist_survival_time_sec,
        cfg.blacklist_survival_time_upper_limit_min,
        cfg.blacklist_survival_time_penalty_period_min,
        cfg.blacklist_history_overdue_time_min,
        cfg.blacklist_history_clear_interval_min,
        true/*is_tenant_mode*/,
        source_tenant_id,
        self_tenant_id))) {
      LOG_ERROR("ObLogRouterService init failer", KR(ret), K(region), K(cluster_id), K(source_tenant_id));
    } else if (OB_FAIL(progress_controller_.init(cfg.ls_count_upper_limit))) {
      LOG_ERROR("init progress controller fail", KR(ret));
    } else if (OB_FAIL(large_buffer_pool_.init("ObLogFetcher", 1L * 1024 * 1024 * 1024))) {
      LOG_ERROR("init large buffer pool failed", KR(ret));
#ifdef ERRSIM
    } else if (is_direct_fetching_mode(fetching_mode_) && OB_FAIL(LOG_FETCHER_LOG_EXT_HANDLER_INIT_FAIL)) {
      LOG_ERROR("ERRSIM: LOG_FETCHER_LOG_EXT_HANDLER_INIT_FAIL", KR(ret));
#endif
    } else if (is_direct_fetching_mode(fetching_mode_) && OB_FAIL(log_ext_handler_.init())) {
      LOG_ERROR("init failed", KR(ret));
    } else if (OB_FAIL(ls_fetch_mgr_.init(
            progress_controller_,
            ls_ctx_factory,
            ls_ctx_add_info_factory,
            static_cast<void *>(this)))) {
      LOG_ERROR("init part fetch mgr fail", KR(ret));
    } else if (OB_FAIL(init_self_addr_())) {
      LOG_ERROR("init_self_addr_ fail", KR(ret));
    } else if (OB_FAIL(rpc_.init(cluster_id, self_tenant_id, cfg.io_thread_num, cfg))) {
      LOG_ERROR("init rpc handler fail", KR(ret));
    } else if (is_cdc(log_fetcher_user_) && OB_FAIL(start_lsn_locator_.init(
            cfg.start_lsn_locator_thread_num,
            cfg.start_lsn_locator_locate_count,
            fetching_mode_,
            archive_dest,
            cfg,
            rpc_, *err_handler))) {
      LOG_ERROR("init start log id locator fail", KR(ret));
    } else if (OB_FAIL(idle_pool_.init(
            log_fetcher_user_,
            cfg.idle_pool_thread_num,
            cfg,
            static_cast<void *>(this),
            *err_handler,
            stream_worker_,
            start_lsn_locator_))) {
      LOG_ERROR("init idle pool fail", KR(ret));
    } else if (OB_FAIL(dead_pool_.init(cfg.dead_pool_thread_num,
            static_cast<void *>(this),
            ls_fetch_mgr_,
            *err_handler))) {
      LOG_ERROR("init dead pool fail", KR(ret));
#ifdef ERRSIM
    } else if (OB_FAIL(LOG_FETCHER_STREAM_WORKER_INIT_FAIL)) {
      LOG_ERROR("ERRSIM: LOG_FETCHER_STREAM_WORKER_INIT_FAIL");
#endif
    } else if (OB_FAIL(stream_worker_.init(cfg.stream_worker_thread_num,
            cfg.timer_task_count_upper_limit,
            static_cast<void *>(this),
            idle_pool_,
            dead_pool_,
            *err_handler))) {
      LOG_ERROR("init stream worker fail", KR(ret));
    } else if (OB_FAIL(fs_container_mgr_.init(
            source_tenant_id,
            self_tenant_id,
            cfg.svr_stream_cached_count,
            cfg.fetch_stream_cached_count,
            cached_fetch_log_arpc_res_cnt,
            rpc_,
            stream_worker_,
            progress_controller_,
            log_handler))) {
    } else {
      cluster_id_ = cluster_id;
      source_tenant_id_ = source_tenant_id;
      is_loading_data_dict_baseline_data_ = is_loading_data_dict_baseline_data;
      archive_dest_ = archive_dest;

      paused_ = false;
      pause_time_ = OB_INVALID_TIMESTAMP;
      resume_time_ = OB_INVALID_TIMESTAMP;
      log_ext_handler_concurrency_ = cfg.cdc_read_archive_log_concurrency;
      stop_flag_ = true;
      is_inited_ = true;

      LOG_INFO("LogFetcher init succ", K_(cluster_id), K_(source_tenant_id),
          K_(self_tenant_id), K_(is_loading_data_dict_baseline_data),
          "fetching_mode", print_fetching_mode(fetching_mode_), K(archive_dest_));
    }
  }
  return ret;
}

void ObLogFetcher::destroy()
{
  LOG_INFO("LogFetcher destroy start");
  stop();

  // TODO: Global destroy all memory
  is_inited_ = false;
  is_loading_data_dict_baseline_data_ = false;
  archive_dest_.reset();
  large_buffer_pool_.destroy();
  ls_ctx_add_info_factory_ = NULL;
  err_handler_ = NULL;

  stop_flag_ = true;
  paused_ = false;
  pause_time_ = OB_INVALID_TIMESTAMP;
  resume_time_ = OB_INVALID_TIMESTAMP;

  stream_worker_.destroy();
  fs_container_mgr_.destroy();
  idle_pool_.destroy();
  dead_pool_.destroy();
  if (is_cdc(log_fetcher_user_)) {
    start_lsn_locator_.destroy();
  }
  rpc_.destroy();
  progress_controller_.destroy();
  ls_fetch_mgr_.destroy();
  if (is_integrated_fetching_mode(fetching_mode_)) {
    log_route_service_.wait();
    log_route_service_.destroy();
  }
  log_ext_handler_concurrency_ = 0;
  if (is_direct_fetching_mode(fetching_mode_)) {
    log_ext_handler_.wait();
    log_ext_handler_.destroy();
  }
  // Finally reset fetching_mode_ because of some processing dependencies, such as ObLogRouteService
  fetching_mode_ = ClientFetchingMode::FETCHING_MODE_UNKNOWN;
  log_fetcher_user_ = LogFetcherUser::UNKNOWN;

  LOG_INFO("LogFetcher destroy succ");
}

int ObLogFetcher::start()
{
  int ret = OB_SUCCESS;
  int pthread_ret = 0;
  LOG_INFO("LogFetcher start begin");

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcher has not been inited", KR(ret));
  } else if (OB_UNLIKELY(! stop_flag_)) {
    LOG_ERROR("fetcher has been started", K(stop_flag_));
    ret = OB_INIT_TWICE;
  } else {
    stop_flag_ = false;

    if (is_integrated_fetching_mode(fetching_mode_) && OB_FAIL(log_route_service_.start())) {
      LOG_ERROR("start LogRouterService fail", KR(ret));
    } else if (is_cdc(log_fetcher_user_) && OB_FAIL(start_lsn_locator_.start())) {
      LOG_ERROR("start 'start_lsn_locator' fail", KR(ret));
    } else if (OB_FAIL(idle_pool_.start())) {
      LOG_ERROR("start idle pool fail", KR(ret));
    } else if (OB_FAIL(dead_pool_.start())) {
      LOG_ERROR("start dead pool fail", KR(ret));
    } else if (OB_FAIL(stream_worker_.start())) {
      LOG_ERROR("start stream worker fail", KR(ret));
      // TODO by wenyue.zxl: change the concurrency of 'log_ext_handler_'(see resize interface)
    } else if (is_direct_fetching_mode(fetching_mode_) &&
        OB_FAIL(log_ext_handler_.start(log_ext_handler_concurrency_))) {
      LOG_ERROR("start log external handler failed", KR(ret));
    } else {
      LOG_INFO("LogFetcher start success");
    }
  }

  return ret;
}

void ObLogFetcher::stop()
{
  stop_flag_ = true;

  LOG_INFO("LogFetcher stop begin");
  stream_worker_.stop();
  dead_pool_.stop();
  idle_pool_.stop();
  if (is_cdc(log_fetcher_user_)) {
    start_lsn_locator_.stop();
  }

  if (is_integrated_fetching_mode(fetching_mode_)) {
    log_route_service_.stop();
  }
  if (is_direct_fetching_mode(fetching_mode_)) {
    log_ext_handler_.stop();
  }

  LOG_INFO("LogFetcher stop success");
}

void ObLogFetcher::pause()
{
  if (OB_LIKELY(is_inited_)) {
    int64_t pause_time = get_timestamp();
    int64_t last_pause_time = ATOMIC_LOAD(&pause_time_);
    int64_t last_resume_time = ATOMIC_LOAD(&resume_time_);

    ATOMIC_STORE(&pause_time_, pause_time);
    ATOMIC_STORE(&paused_, true);
    stream_worker_.pause();
    LOG_INFO("LogFetcher pause succ", "last pause time", TS_TO_STR(last_pause_time),
        "last resume time", TS_TO_STR(last_resume_time));
  }
}

void ObLogFetcher::resume()
{
  if (OB_LIKELY(is_inited_)) {
    int64_t resume_time = get_timestamp();
    int64_t pause_time = ATOMIC_LOAD(&pause_time_);
    int64_t pause_interval = resume_time - pause_time;

    ATOMIC_STORE(&resume_time_, resume_time);
    ATOMIC_STORE(&paused_, false);
    stream_worker_.resume(resume_time);
    LOG_INFO("LogFetcher resume succ", "pause interval", TVAL_TO_STR(pause_interval));
  }
}

bool ObLogFetcher::is_paused()
{
  return ATOMIC_LOAD(&paused_);
}

void ObLogFetcher::mark_stop_flag()
{
  if (OB_UNLIKELY(is_inited_)) {
    LOG_INFO("LogFetcher mark stop begin");
    stop_flag_ = true;

    stream_worker_.mark_stop_flag();
    dead_pool_.mark_stop_flag();
    idle_pool_.mark_stop_flag();
    if (is_cdc(log_fetcher_user_)) {
      start_lsn_locator_.mark_stop_flag();
    }
    if (is_direct_fetching_mode(fetching_mode_)) {
      log_ext_handler_.stop();
    }
    LOG_INFO("LogFetcher mark stop succ");
  }
}

int ObLogFetcher::update_preferred_upstream_log_region(const common::ObRegion &region)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcher has not been inited", KR(ret));
  } else if (OB_FAIL(log_route_service_.update_preferred_upstream_log_region(region))) {
    LOG_WARN("ObLogRouteService update_preferred_upstream_log_region failed", KR(ret), K(region));
  }

  return ret;
}

int ObLogFetcher::get_preferred_upstream_log_region(common::ObRegion &region)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcher has not been inited", KR(ret));
  } else if (OB_FAIL(log_route_service_.get_preferred_upstream_log_region(region))) {
    LOG_WARN("ObLogRouteService get_preferred_upstream_log_region failed", KR(ret), K(region));
  }

  return ret;
}

int ObLogFetcher::add_ls(
    const share::ObLSID &ls_id,
    const ObLogFetcherStartParameters &start_parameters)
{
  int ret = OB_SUCCESS;
  logservice::TenantLSID tls_id(source_tenant_id_, ls_id);
  LSFetchCtx *ls_fetch_ctx = NULL;
  FetchStreamType type = FETCH_STREAM_TYPE_UNKNOWN;
  const int64_t start_tstamp_ns = start_parameters.get_start_tstamp_ns();
  const palf::LSN &start_lsn = start_parameters.get_start_lsn();

  if (tls_id.is_sys_log_stream()) {
    type = FETCH_STREAM_TYPE_SYS_LS;
  } else {
    type = FETCH_STREAM_TYPE_HOT;
  }

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcher has not been inited", KR(ret));
  }
  // Requires a valid start-up timestamp
  else if (OB_UNLIKELY(start_tstamp_ns <= -1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid start tstamp", KR(ret), K(start_tstamp_ns), K(tls_id), K(start_lsn));
  } else if (is_integrated_fetching_mode(fetching_mode_)
      && OB_FAIL(log_route_service_.registered(tls_id.get_tenant_id(), tls_id.get_ls_id()))) {
    LOG_WARN("ObLogRouteService registered fail", KR(ret), K(start_tstamp_ns), K(tls_id), K(start_lsn));
  }
  // Push LS into ObLogLSFetchMgr
  else if (OB_FAIL(ls_fetch_mgr_.add_ls(tls_id, start_parameters, is_loading_data_dict_baseline_data_,
      fetching_mode_, archive_dest_, *err_handler_))) {
    LOG_ERROR("add partition by part fetch mgr fail", KR(ret), K(tls_id), K(start_parameters),
        K(is_loading_data_dict_baseline_data_));
  } else if (OB_FAIL(ls_fetch_mgr_.get_ls_fetch_ctx(tls_id, ls_fetch_ctx))) {
    LOG_ERROR("get part fetch ctx fail", KR(ret), K(tls_id));
  } else if (OB_ISNULL(ls_fetch_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls fetch ctx is NULL", KR(ret), K(ls_fetch_ctx));
  } else if (OB_FAIL(fs_container_mgr_.add_fsc(type, tls_id))) {
    LOG_ERROR("fs_container_mgr_ add_fsc failed", KR(ret), K(type), "type", print_fetch_stream_type(type),
        K(tls_id));
  // First enter the IDLE POOL to initialize basic information
  } else if (OB_FAIL(idle_pool_.push(ls_fetch_ctx))) {
    LOG_ERROR("push task into idle pool fail", KR(ret), K(ls_fetch_ctx));
  } else {
    LOG_INFO("LogFetcher add ls succ", K(tls_id), K(is_loading_data_dict_baseline_data_),
        K(start_parameters));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    logservice::TenantLSID failed_tls_id(source_tenant_id_, ls_id);
    if (OB_TMP_FAIL(ls_fetch_mgr_.recycle_ls(failed_tls_id))) {
      if (OB_ENTRY_NOT_EXIST != tmp_ret) {
        LOG_WARN_RET(tmp_ret, "failed to recycle ls in failure post process", K(failed_tls_id));
      } else {
        LOG_INFO("tls_id is not in ls_fetch_mgr, recycle done", K(failed_tls_id));
      }
    }

    if (OB_TMP_FAIL(fs_container_mgr_.remove_fsc(failed_tls_id))) {
      if (OB_ENTRY_NOT_EXIST != tmp_ret) {
        LOG_WARN_RET(tmp_ret, "failed ", K(failed_tls_id));
      } else {
        LOG_INFO("tls_id not exist in fs_container_mgr_, remove done", K(failed_tls_id));
      }
    }

    if (OB_TMP_FAIL(ls_fetch_mgr_.remove_ls(failed_tls_id))) {
      if (OB_ENTRY_NOT_EXIST != tmp_ret) {
        LOG_WARN_RET(tmp_ret, "failed to remove ls in ls_fetch_mgr ", K(failed_tls_id));
      } else {
        LOG_INFO("tls_id not exist in ls_fetch_mgr_, remove done", K(failed_tls_id));
      }
    }
  }

  return ret;
}

int ObLogFetcher::recycle_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  logservice::TenantLSID tls_id(source_tenant_id_, ls_id);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcher has not been inited", KR(ret));
  } else if (OB_FAIL(ls_fetch_mgr_.recycle_ls(tls_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("ls has been recycled in fetcher", K(tls_id));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("recycle ls fail", KR(ret), K(tls_id));
    }
  } else {
    LOG_INFO("LogFetcher recycle ls succ", K(tls_id));
  }

  return ret;
}

int ObLogFetcher::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  // Copy the logservice::TenantLSID to avoid recycle
  const logservice::TenantLSID removed_tls_id(source_tenant_id_, ls_id);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcher has not been inited", KR(ret));
  } else if (OB_FAIL(recycle_ls(ls_id))) {
    LOG_ERROR("recycle_ls failed", KR(ret), K(ls_id));
  } else {
    bool is_done = false;

    while (OB_SUCC(ret) && ! is_done) {
      LSFetchCtx *ls_fetch_ctx = nullptr;

      if (OB_FAIL(ls_fetch_mgr_.get_ls_fetch_ctx(removed_tls_id, ls_fetch_ctx))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_ERROR("get part fetch ctx fail", KR(ret), K(removed_tls_id));
        } else {
          is_done = true;
        }
      } else {
        usec_sleep(200L);
      }
    } // while

    LOG_INFO("LogFetcher remove ls succ", K(removed_tls_id));
  }

  return ret;
}

int ObLogFetcher::remove_ls_physically(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  // Copy the logservice::TenantLSID to avoid recycle
  const logservice::TenantLSID removed_tls_id(source_tenant_id_, ls_id);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcher has not been inited", KR(ret));
  } else if (OB_FAIL(fs_container_mgr_.remove_fsc(removed_tls_id))) {
    LOG_ERROR("fs_container_mgr_ remove_fsc failed", KR(ret), K(removed_tls_id));
  } else if (OB_FAIL(ls_fetch_mgr_.remove_ls(removed_tls_id))) {
    LOG_ERROR("ls_fetch_mgr_ remove_ls failed", KR(ret), K(removed_tls_id));
  } else {
    LOG_INFO("LogFetcher remove_ls_physically succ", K(removed_tls_id));
  }

  return ret;
}

int ObLogFetcher::get_all_ls(ObIArray<share::ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcher has not been inited", KR(ret));
  } else {
    FetchCtxMapLSGetter ls_getter(ls_ids);

    if (OB_FAIL(ls_fetch_mgr_.for_each_ls(ls_getter))) {
      LOG_ERROR("for each LS fetch ctx fail", KR(ret));
    }
  }

  return ret;
}

bool ObLogFetcher::is_ls_exist(const share::ObLSID &ls_id) const
{
  logservice::TenantLSID tls_id(source_tenant_id_, ls_id);
  return ls_fetch_mgr_.is_tls_exist(tls_id);
}

int ObLogFetcher::get_ls_proposal_id(const share::ObLSID &ls_id, int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  const logservice::TenantLSID tls_id(source_tenant_id_, ls_id);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcher has not been inited", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ls_id));
  } else if (OB_FAIL(ls_fetch_mgr_.get_tls_proposal_id(tls_id, proposal_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get_tls_proposal_id failed", K(tls_id));
    }
  }

  return ret;
}

int ObLogFetcher::update_fetching_log_upper_limit(const share::SCN &upper_limit_scn)
{
  int ret = OB_SUCCESS;
  int64_t upper_limit_ts_ns = -1;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcher has not been inited", KR(ret));
  } else if (OB_UNLIKELY(!upper_limit_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(upper_limit_scn));
  } else if (FALSE_IT(upper_limit_ts_ns = static_cast<int64_t>(upper_limit_scn.get_val_for_logservice()))) {
  } else if (OB_FAIL(progress_controller_.set_global_upper_limit(upper_limit_ts_ns))) {
    LOG_WARN("set_global_upper_limit failed", K(upper_limit_scn), K(upper_limit_ts_ns));
  }

  return ret;
}

int ObLogFetcher::update_compressor_type(const common::ObCompressorType &compressor_type)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcher has not been inited", KR(ret));
  } else if (OB_FAIL(rpc_.update_compressor_type(compressor_type))) {
    LOG_WARN("ObLogRpc update_compressor_type failed", K(compressor_type));
  }

  return ret;
}

int ObLogFetcher::get_progress_info(ProgressInfo &progress_info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcher has not been inited", KR(ret));
  } else {
    if (OB_FAIL(ls_fetch_mgr_.for_each_ls(progress_info))) {
      LOG_ERROR("for each part fetch ctx fail", KR(ret));
    }
  }

  return ret;
}

int ObLogFetcher::wait_for_all_ls_to_be_removed(const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcher has not been inited", KR(ret));
  } else {
    const int64_t start_time = get_timestamp();

    while (OB_SUCC(ret) && ls_fetch_mgr_.get_total_count() > 0) {
      int64_t end_time = get_timestamp();
      if (end_time - start_time >= timeout) {
        ret = OB_TIMEOUT;
        break;
      } else {
        usec_sleep(100L);
      }
    } // while
  }

  return ret;
}

void ObLogFetcher::configure(const ObLogFetcherConfig &cfg)
{
  int ret = OB_SUCCESS;
  bool print_ls_heartbeat_info = cfg.print_ls_heartbeat_info;

  const int64_t log_router_background_refresh_interval_sec = cfg.log_router_background_refresh_interval_sec;
  const int64_t all_server_cache_update_interval_sec = cfg.all_server_cache_update_interval_sec;
  const int64_t all_zone_cache_update_interval_sec = cfg.all_zone_cache_update_interval_sec;

  const int64_t blacklist_survival_time_sec = cfg.blacklist_survival_time_sec;
  const int64_t blacklist_survival_time_upper_limit_min = cfg.blacklist_survival_time_upper_limit_min;
  const int64_t blacklist_survival_time_penalty_period_min = cfg.blacklist_survival_time_penalty_period_min;
  const int64_t blacklist_history_overdue_time_min = cfg.blacklist_history_overdue_time_min;
  const int64_t blacklist_history_clear_interval_min = cfg.blacklist_history_clear_interval_min;
  const int64_t log_ext_handler_concurrency = cfg.cdc_read_archive_log_concurrency;

  ATOMIC_STORE(&g_print_ls_heartbeat_info, print_ls_heartbeat_info);

  ObLogStartLSNLocator::configure(cfg);
  FetchStream::configure(cfg);
  ObLogRpc::configure(cfg);
  ObLSWorker::configure(cfg);
  ObLogLSFetchMgr::configure(cfg);
  FetchLogARpc::configure(cfg);

  // only update LogRouteServer::update_blacklist_parameter if fetcher is inited.
  if (IS_INIT && is_integrated_fetching_mode(fetching_mode_)) {
    if (OB_FAIL(log_route_service_.update_blacklist_parameter(
        blacklist_survival_time_sec,
        blacklist_survival_time_upper_limit_min,
        blacklist_survival_time_penalty_period_min,
        blacklist_history_overdue_time_min,
        blacklist_history_clear_interval_min))) {
      LOG_WARN("update_blacklist_parameter failed", KR(ret),
        K(blacklist_survival_time_sec),
        K(blacklist_survival_time_upper_limit_min),
        K(blacklist_survival_time_penalty_period_min),
        K(blacklist_history_overdue_time_min),
        K(blacklist_history_clear_interval_min));
    } else if (OB_FAIL(log_route_service_.update_preferred_upstream_log_region(cfg.region.str()))) {
      LOG_ERROR("update_preferred_upstream_log_region failed", KR(ret), "region", cfg.region);
    } else if (OB_FAIL(log_route_service_.update_cache_update_interval(
        all_server_cache_update_interval_sec,
        all_zone_cache_update_interval_sec))) {
      LOG_ERROR("update_cache_update_interval failed", KR(ret),
          K(all_server_cache_update_interval_sec), K(all_zone_cache_update_interval_sec));
    } else if (OB_FAIL(log_route_service_.update_background_refresh_time(log_router_background_refresh_interval_sec))) {
      LOG_ERROR("update_background_refresh_time failed", KR(ret),
          "log_router_background_refresh_interval_sec", log_router_background_refresh_interval_sec);
    }
  } else if (IS_INIT && is_direct_fetching_mode(fetching_mode_)) {
    if (OB_FAIL(log_ext_handler_.resize(log_ext_handler_concurrency))) {
      LOG_ERROR("log_ext_handler failed to resize when reloading configure", K(log_ext_handler_concurrency));
    } else {
      log_ext_handler_concurrency_ = log_ext_handler_concurrency;
    }
  }
}

int ObLogFetcher::get_fs_container_mgr(IObFsContainerMgr *&fs_container_mgr)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("LogFetcher is not inited, fs_container_mgr not available currently", KR(ret), K_(is_inited));
  } else {
    fs_container_mgr = &fs_container_mgr_;
  }

  return ret;
}

int ObLogFetcher::get_log_route_service(logservice::ObLogRouteService *&log_route_service)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("LogFetcher is not inited, log_route_service not available currently", KR(ret), K_(is_inited));
  } else {
    log_route_service = &log_route_service_;
  }

  return ret;
}

int ObLogFetcher::get_large_buffer_pool(archive::LargeBufferPool *&large_buffer_pool)
{
  int ret = OB_SUCCESS;

  if(IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("LogFetcher is not inited, could not get large buffer pool", KR(ret), K_(is_inited));
  } else {
    large_buffer_pool = &large_buffer_pool_;
  }

  return ret;
}

int ObLogFetcher::get_log_ext_handler(logservice::ObLogExternalStorageHandler *&log_ext_hander)
{
  int ret = OB_SUCCESS;

  if(IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("LogFetcher is not inited, could not get log ext handler", KR(ret), K_(is_inited));
  } else {
    log_ext_hander = &log_ext_handler_;
  }

  return ret;
}

int ObLogFetcher::get_fetcher_config(const ObLogFetcherConfig *&cfg)
{
  int ret = OB_SUCCESS;

  if(IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("LogFetcher is not inited, could not get ObLogFetcherConfig", KR(ret), K_(is_inited));
  } else {
    cfg = cfg_;
  }

  return ret;
}

int ObLogFetcher::check_progress(
    const uint64_t tenant_id,
    const int64_t timestamp,
    bool &is_exceeded,
    int64_t &cur_progress)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("LogFetcher is not inited", KR(ret), K_(is_inited));
  } else {
    is_exceeded = false;
    logservice::TenantLSID tls_id(tenant_id, share::SYS_LS);
    LSFetchCtx *ls_fetch_ctx = nullptr;
    int64_t progress = OB_INVALID_TIMESTAMP;
    PartTransDispatchInfo dispatch_info;

    if (OB_FAIL(ls_fetch_mgr_.get_ls_fetch_ctx(tls_id, ls_fetch_ctx))) {
      LOG_ERROR("ls_fetch_mgr_ get_ls_fetch_ctx failed", KR(ret), K(tls_id));
    } else if (OB_FAIL(ls_fetch_ctx->get_dispatch_progress(progress, dispatch_info))) {
      LOG_ERROR("ls_fetch_ctx get_dispatch_progress failed", KR(ret), K(tls_id), KPC(ls_fetch_ctx));
    } else {
      is_exceeded = (progress >= timestamp);
      cur_progress = progress;
    }
  }

  return ret;
}

void ObLogFetcher::print_stat()
{
  // Periodic printing progress slowest k LS
  if (REACH_TIME_INTERVAL_THREAD_LOCAL(PRINT_K_SLOWEST_LS)) {
    fs_container_mgr_.print_stat();
    // Print upper_limt, fetcher_delay
    print_fetcher_stat_();
    // Print the slowest k LS
    ls_fetch_mgr_.print_k_slowest_ls();
    // Print delay
    (void)print_delay();
  }
}

int ObLogFetcher::suggest_cached_rpc_res_count_(const int64_t min_res_cnt,
    const int64_t max_res_cnt)
{
  const int64_t memory_limit = get_tenant_memory_limit(self_tenant_id_);
  // the maximum memory hold by rpc_result should be 1/32 of the memory limit.
  const int64_t rpc_res_hold_max = (memory_limit >> 5);
  int64_t rpc_res_cnt = rpc_res_hold_max / FetchLogARpcResultPool::DEFAULT_RESULT_POOL_BLOCK_SIZE;
  if (rpc_res_cnt < min_res_cnt) {
    rpc_res_cnt = min_res_cnt;
  }
  if (rpc_res_cnt > max_res_cnt) {
    rpc_res_cnt = max_res_cnt;
  }

  LOG_INFO("suggest fetchlog arpc cached rpc result count", K(self_tenant_id_),
      K(min_res_cnt), K(max_res_cnt), K(rpc_res_cnt), K(memory_limit));
  return rpc_res_cnt;
}

void ObLogFetcher::print_fetcher_stat_()
{
  int ret = OB_SUCCESS;
  int64_t min_progress = OB_INVALID_TIMESTAMP;
  int64_t upper_limit_ns = OB_INVALID_TIMESTAMP;
  int64_t fetcher_delay = OB_INVALID_TIMESTAMP;
  int64_t global_upper_limit = OB_INVALID_TIMESTAMP;
  int64_t dml_progress_limit = 0;

  // Get global minimum progress
  if (OB_FAIL(progress_controller_.get_min_progress(min_progress))) {
    LOG_ERROR("get_min_progress fail", KR(ret), K(progress_controller_));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == min_progress)) {
    //LOG_ERROR("current min progress is invalid", K(min_progress), K(progress_controller_));
    ret = OB_INVALID_ERROR;
  } else {
    dml_progress_limit = ATOMIC_LOAD(&FetchStream::g_dml_progress_limit);
    upper_limit_ns = min_progress + dml_progress_limit * NS_CONVERSION;
    fetcher_delay = get_timestamp() - min_progress / NS_CONVERSION;
    global_upper_limit = progress_controller_.get_global_upper_limit();
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("[STAT] [LOG_FETCHER]", "upper_limit", NTS_TO_STR(upper_limit_ns),
        "global_upper_limit", NTS_TO_STR(global_upper_limit),
        "dml_progress_limit_sec", dml_progress_limit / _SEC_,
        "fetcher_delay", TVAL_TO_STR(fetcher_delay));
  }
}

void *ObLogFetcher::alloc_decompression_buf(int64_t size)
{
  void *buf = NULL;
  if (IS_INIT) {
    buf = decompression_alloc_.alloc(size);
  }
  return buf;
}
void ObLogFetcher::free_decompression_buf(void *buf)
{
  if (NULL != buf) {
    decompression_alloc_.free(buf);
    buf = NULL;
  }
}

bool ObLogFetcher::FetchCtxMapLSGetter::operator()(const logservice::TenantLSID &tls_id, LSFetchCtx *&ctx)
{
  bool bool_ret = true;
  int ret = OB_SUCCESS;

  if (tls_id.is_valid()) {
    if (OB_FAIL(ls_ids_.push_back(tls_id.get_ls_id()))) {
      LOG_ERROR("ls_ids_ push_back failed", KR(ret), K(tls_id));
    }
  }

  return (OB_SUCCESS == ret);
}

int ObLogFetcher::init_self_addr_()
{
  int ret = OB_SUCCESS;
  static const int64_t BUF_SIZE = 128;
  char BUFFER[BUF_SIZE];
  const int32_t self_pid = static_cast<int32_t>(getpid());
  ObString local_ip(sizeof(BUFFER), 0, BUFFER);

  if (OB_FAIL(get_local_ip(local_ip))) {
    LOG_ERROR("get_local_ip fail", KR(ret), K(local_ip));
  } else if (! get_self_addr().set_ip_addr(local_ip, self_pid)) {
    LOG_ERROR("self addr set ip addr error", K(local_ip), K(self_pid));
  } else {
    LOG_INFO("init_self_addr_ succ", K(local_ip), K(self_pid));
  }

  return ret;
}

int ObLogFetcher::print_delay()
{
  int ret = OB_SUCCESS;
  FetchCtxMapHBFunc hb_func(g_print_ls_heartbeat_info);

  // Then iterate through all the LS to get the distribution progress of each LS, i.e. the progress of Fetcher's distribution data
  // Note: Here we also get the progress of the DDL distribution, which is only used for printing
  if (OB_FAIL(ls_fetch_mgr_.for_each_ls(hb_func))) {
    LOG_ERROR("for each part fetch ctx fail", KR(ret));
  } else {
    int64_t data_progress = hb_func.data_progress_;
    logservice::TenantLSID min_progress_tls_id = hb_func.min_progress_ls_;
    logservice::TenantLSID max_progress_tls_id = hb_func.max_progress_ls_;

    if (REACH_TIME_INTERVAL_THREAD_LOCAL(PRINT_HEARTBEAT_INTERVAL) || g_print_ls_heartbeat_info) {
      // Calculation of the minimum and maximum progress, and the corresponding LS
      int64_t min_progress = hb_func.min_progress_;
      int64_t max_progress = hb_func.max_progress_;

      _LOG_INFO("[STAT] [LOG_FETCHER] DELAY=[%.3lf, %.3lf](sec) LS_COUNT=%ld "
          "MIN_DELAY=%s(%ld) MAX_DELAY=%s(%ld) DATA_PROGRESS=%s(%ld)",
          get_delay_sec(max_progress),
          get_delay_sec(min_progress),
          hb_func.ls_count_,
          to_cstring(max_progress_tls_id),
          max_progress,
          to_cstring(min_progress_tls_id),
          min_progress,
          NTS_TO_STR(data_progress),
          data_progress);
    }
  }

  return ret;
}

}
}
