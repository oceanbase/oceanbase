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
 *
 * Fetcher
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_fetcher.h"

#include "lib/oblog/ob_log_module.h"  // LOG_*

#include "ob_log_config.h"            // ObLogConfig
#include "ob_log_timer.h"             // ObLogFixedTimer
#include "ob_log_sys_ls_task_handler.h"  // IObLogSysLsTaskHandler
#include "ob_log_task_pool.h"         // ObLogTransTaskPool
#include "ob_log_instance.h"          // IObLogErrHandler
#include "logservice/restoreservice/ob_log_restore_net_driver.h"   // logfetcher::LogErrHandler

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{

///////////////////////////////////////////// ObLogFetcher /////////////////////////////////////////////

bool ObLogFetcher::g_print_ls_heartbeat_info =
    ObLogConfig::default_print_ls_heartbeat_info;
int64_t ObLogFetcher::g_inner_heartbeat_interval =
    ObLogConfig::default_output_inner_heartbeat_interval_msec * _MSEC_;

ObLogFetcher::ObLogFetcher() :
    is_inited_(false),
    is_running_(false),
    is_loading_data_dict_baseline_data_(false),
    fetching_mode_(ClientFetchingMode::FETCHING_MODE_UNKNOWN),
    archive_dest_(),
    large_buffer_pool_(),
    log_ext_handler_concurrency_(0),
    log_ext_handler_(),
    task_pool_(NULL),
    sys_ls_handler_(NULL),
    err_handler_(NULL),
    cluster_id_(OB_INVALID_CLUSTER_ID),
    part_trans_resolver_factory_(),
    ls_fetch_mgr_(),
    progress_controller_(),
    rpc_(),
    log_route_service_(),
    start_lsn_locator_(),
    idle_pool_(),
    dead_pool_(),
    stream_worker_(),
    fs_container_mgr_(),
    dispatcher_(nullptr),
    cluster_id_filter_(),
    misc_tid_(0),
    heartbeat_dispatch_tid_(0),
    last_timestamp_(OB_INVALID_TIMESTAMP),
    stop_flag_(true),
    paused_(false),
    pause_time_(OB_INVALID_TIMESTAMP),
    resume_time_(OB_INVALID_TIMESTAMP),
    decompression_blk_mgr_(DECOMPRESSION_MEM_LIMIT_THRESHOLD),
    decompression_alloc_(ObMemAttr(OB_SERVER_TENANT_ID, "decompress_buf"), common::OB_MALLOC_BIG_BLOCK_SIZE, decompression_blk_mgr_)
{
  decompression_alloc_.set_nway(DECOMPRESSION_MEM_LIMIT_THRESHOLD);
}

ObLogFetcher::~ObLogFetcher()
{
  destroy();
}

int ObLogFetcher::init(
    const bool is_loading_data_dict_baseline_data,
    const bool enable_direct_load_inc,
    const ClientFetchingMode fetching_mode,
    const ObBackupPathString &archive_dest,
    IObLogFetcherDispatcher *dispatcher,
    IObLogSysLsTaskHandler *sys_ls_handler,
    TaskPool *task_pool,
    IObLogEntryTaskPool *log_entry_task_pool,
    ObISQLClient *proxy,
    IObLogErrHandler *err_handler,
    const int64_t cluster_id,
    const ObLogConfig &cfg,
    const int64_t start_seq)
{
  int ret = OB_SUCCESS;
  int64_t max_cached_ls_fetch_ctx_count = cfg.active_ls_count;
  LogFetcherErrHandler *fake_err_handler = NULL; // TODO: CDC need to process error handler

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", KR(ret));
  } else if (OB_ISNULL(dispatcher_ = dispatcher)
      || OB_ISNULL(sys_ls_handler_ = sys_ls_handler)
      || OB_ISNULL(err_handler_ = err_handler)
      || OB_ISNULL(task_pool_ = task_pool)
      || OB_ISNULL(log_entry_task_pool)
      || OB_ISNULL(proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(dispatcher), K(sys_ls_handler), K(err_handler),
        K(task_pool), K(log_entry_task_pool), K(proxy));
  } else {
    // Before the Fetcher module is initialized, the following configuration items need to be loaded
    configure(cfg);
    const common::ObRegion prefer_region(cfg.region.str());

    const bool is_tenant_mode = TCTX.is_tenant_sync_mode();

    if (is_integrated_fetching_mode(fetching_mode) && OB_FAIL(log_route_service_.init(
        proxy,
        prefer_region,
        cluster_id,
        false/*is_across_cluster*/,
        fake_err_handler,
        cfg.server_blacklist.str(),
        cfg.log_router_background_refresh_interval_sec,
        cfg.all_server_cache_update_interval_sec,
        cfg.all_zone_cache_update_interval_sec,
        cfg.blacklist_survival_time_sec,
        cfg.blacklist_survival_time_upper_limit_min,
        cfg.blacklist_survival_time_penalty_period_min,
        cfg.blacklist_history_overdue_time_min,
        cfg.blacklist_history_clear_interval_min,
        is_tenant_mode,
        TCTX.tenant_id_,
        OB_SERVER_TENANT_ID))) {
      LOG_ERROR("ObLogRouterService init failer", KR(ret), K(prefer_region), K(cluster_id));
    } else if (OB_FAIL(progress_controller_.init(cfg.ls_count_upper_limit))) {
      LOG_ERROR("init progress controller fail", KR(ret));
    } else if (OB_FAIL(cluster_id_filter_.init(cfg.cluster_id_black_list.str(),
        cfg.cluster_id_black_value_min, cfg.cluster_id_black_value_max))) {
      LOG_ERROR("init cluster_id_filter fail", KR(ret));
    } else if (OB_FAIL(part_trans_resolver_factory_.init(*task_pool, *log_entry_task_pool, *dispatcher_, cluster_id_filter_))) {
      LOG_ERROR("init part trans resolver factory fail", KR(ret));
    } else if (OB_FAIL(large_buffer_pool_.init("ObLogFetcher", 1L * 1024 * 1024 * 1024))) {
      LOG_ERROR("init large buffer pool failed", KR(ret));
    } else if (is_direct_fetching_mode(fetching_mode) && OB_FAIL(log_ext_handler_.init())) {
      LOG_ERROR("init log ext handler failed", KR(ret));
    } else if (OB_FAIL(ls_fetch_mgr_.init(
        max_cached_ls_fetch_ctx_count,
        progress_controller_,
        part_trans_resolver_factory_,
        static_cast<void *>(this)))) {
      LOG_ERROR("init part fetch mgr fail", KR(ret));
    } else if (OB_FAIL(rpc_.init(cfg.io_thread_num))) {
      LOG_ERROR("init rpc handler fail", KR(ret));
    } else if (OB_FAIL(start_lsn_locator_.init(
        cfg.start_lsn_locator_thread_num,
        cfg.start_lsn_locator_locate_count,
        fetching_mode,
        archive_dest,
        rpc_, *err_handler))) {
      LOG_ERROR("init start log id locator fail", KR(ret));
    } else if (OB_FAIL(idle_pool_.init(
        cfg.idle_pool_thread_num,
        *err_handler,
        stream_worker_,
        start_lsn_locator_))) {
      LOG_ERROR("init idle pool fail", KR(ret));
    } else if (OB_FAIL(dead_pool_.init(
        cfg.dead_pool_thread_num,
        static_cast<void *>(this),
        ls_fetch_mgr_,
        *err_handler))) {
      LOG_ERROR("init dead pool fail", KR(ret));
    } else if (OB_FAIL(stream_worker_.init(
        cfg.stream_worker_thread_num,
        cfg.timer_task_count_upper_limit,
        static_cast<void *>(this),
        idle_pool_,
        dead_pool_,
        *err_handler))) {
      LOG_ERROR("init stream worker fail", KR(ret));
    } else if (OB_FAIL(fs_container_mgr_.init(
        cfg.svr_stream_cached_count,
        cfg.fetch_stream_cached_count,
        cfg.rpc_result_cached_count,
        rpc_,
        stream_worker_,
        progress_controller_))) {
      LOG_ERROR("init fs_container_mgr_ failed", KR(ret));
    } else {
      paused_ = false;
      pause_time_ = OB_INVALID_TIMESTAMP;
      resume_time_ = OB_INVALID_TIMESTAMP;
      misc_tid_ = 0;
      heartbeat_dispatch_tid_ = 0;
      last_timestamp_ = OB_INVALID_TIMESTAMP;
      is_loading_data_dict_baseline_data_ = is_loading_data_dict_baseline_data;
      enable_direct_load_inc_ = enable_direct_load_inc;
      fetching_mode_ = fetching_mode;
      archive_dest_ = archive_dest;
      log_ext_handler_concurrency_ = cfg.cdc_read_archive_log_concurrency;
      stop_flag_ = true;
      is_inited_ = true;

      // Initialization test mode
      IObCDCPartTransResolver::test_mode_on = cfg.test_mode_on;
      IObCDCPartTransResolver::test_mode_ignore_redo_count = cfg.test_mode_ignore_redo_count;
      IObCDCPartTransResolver::test_checkpoint_mode_on = cfg.test_checkpoint_mode_on;
      IObCDCPartTransResolver::test_mode_ignore_log_type = static_cast<IObCDCPartTransResolver::IgnoreLogType>(cfg.test_mode_ignore_log_type.get());

      LOG_INFO("init fetcher succ", K_(is_loading_data_dict_baseline_data), K(enable_direct_load_inc),
          "test_mode_on", IObCDCPartTransResolver::test_mode_on,
          "test_mode_ignore_log_type", IObCDCPartTransResolver::test_mode_ignore_log_type,
          "test_mode_ignore_redo_count", IObCDCPartTransResolver::test_mode_ignore_redo_count,
          "test_checkpoint_mode_on", IObCDCPartTransResolver::test_checkpoint_mode_on);
    }
  }
  return ret;
}

void ObLogFetcher::destroy()
{
  stop();

  if (is_inited_) {
    // TODO: Global destroy all memory
    is_inited_ = false;
    is_loading_data_dict_baseline_data_ = false;
    archive_dest_.reset();
    large_buffer_pool_.destroy();
    task_pool_ = NULL;
    sys_ls_handler_ = NULL;
    err_handler_ = NULL;

    misc_tid_ = 0;
    heartbeat_dispatch_tid_ = 0;
    last_timestamp_ = OB_INVALID_TIMESTAMP;
    stop_flag_ = true;
    paused_ = false;
    pause_time_ = OB_INVALID_TIMESTAMP;
    resume_time_ = OB_INVALID_TIMESTAMP;

    stream_worker_.destroy();
    fs_container_mgr_.destroy();
    idle_pool_.destroy();
    dead_pool_.destroy();
    start_lsn_locator_.destroy();
    rpc_.destroy();
    progress_controller_.destroy();
    ls_fetch_mgr_.destroy();
    part_trans_resolver_factory_.destroy();
    dispatcher_ = nullptr;
    cluster_id_filter_.destroy();
    if (is_integrated_fetching_mode(fetching_mode_)) {
      log_route_service_.wait();
      log_route_service_.destroy();
    }
    // Finally reset fetching_mode_ because of some processing dependencies, such as ObLogRouteService
    if (is_direct_fetching_mode(fetching_mode_)) {
      log_ext_handler_.wait();
      log_ext_handler_.destroy();
    }
    fetching_mode_ = ClientFetchingMode::FETCHING_MODE_UNKNOWN;
    log_ext_handler_concurrency_ = 0;


    LOG_INFO("destroy fetcher succ");
  }
}

int ObLogFetcher::start()
{
  int ret = OB_SUCCESS;
  int pthread_ret = 0;

  LOG_INFO("begin start fetcher");

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", KR(ret));
  } else if (OB_UNLIKELY(! stop_flag_)) {
    LOG_ERROR("fetcher has been started", K(stop_flag_));
    ret = OB_INIT_TWICE;
  } else {
    stop_flag_ = false;

    if (is_direct_fetching_mode(fetching_mode_) &&
        OB_FAIL(log_ext_handler_.start(log_ext_handler_concurrency_))) {
      LOG_ERROR("start ObLogExternalStorageHandler fail", KR(ret));
    } else if (is_integrated_fetching_mode(fetching_mode_) && OB_FAIL(log_route_service_.start())) {
      LOG_ERROR("start LogRouterService fail", KR(ret));
    } else if (OB_FAIL(start_lsn_locator_.start())) {
      LOG_ERROR("start 'start_lsn_locator' fail", KR(ret));
    } else if (OB_FAIL(idle_pool_.start())) {
      LOG_ERROR("start idle pool fail", KR(ret));
    } else if (OB_FAIL(dead_pool_.start())) {
      LOG_ERROR("start dead pool fail", KR(ret));
    } else if (OB_FAIL(stream_worker_.start())) {
      LOG_ERROR("start stream worker fail", KR(ret));
    } else if (0 != (pthread_ret = pthread_create(&misc_tid_, NULL, misc_thread_func_, this))) {
      LOG_ERROR("start fetcher misc thread fail", K(pthread_ret), KERRNOMSG(pthread_ret));
      ret = OB_ERR_UNEXPECTED;
    } else if (0 != (pthread_ret = pthread_create(&heartbeat_dispatch_tid_, NULL,
        heartbeat_dispatch_thread_func_, this))) {
      LOG_ERROR("start fetcher heartbeat dispatch thread fail", K(pthread_ret), KERRNOMSG(pthread_ret));
      ret = OB_ERR_UNEXPECTED;
    } else {
      is_running_ = true;
      LOG_INFO("start fetcher succ", K(misc_tid_), K(heartbeat_dispatch_tid_));
    }
  }
  return ret;
}

void ObLogFetcher::stop()
{
  if (OB_LIKELY(is_inited_)) {
    mark_stop_flag();

    if (is_running_) {
      is_running_ = false;
      LOG_INFO("stop fetcher begin");
      stream_worker_.stop();
      dead_pool_.stop();
      idle_pool_.stop();
      start_lsn_locator_.stop();

      if (0 != misc_tid_) {
        int pthread_ret = pthread_join(misc_tid_, NULL);
        if (0 != pthread_ret) {
          LOG_ERROR_RET(OB_ERR_SYS, "join fetcher misc thread fail", K(misc_tid_), K(pthread_ret),
              KERRNOMSG(pthread_ret));
        }
        misc_tid_ = 0;
      }

      if (0 != heartbeat_dispatch_tid_) {
        int pthread_ret = pthread_join(heartbeat_dispatch_tid_, NULL);
        if (0 != pthread_ret) {
          LOG_ERROR_RET(OB_ERR_SYS, "join fetcher heartbeat dispatch thread fail", K(heartbeat_dispatch_tid_),
              K(pthread_ret), KERRNOMSG(pthread_ret));
        }
        heartbeat_dispatch_tid_ = 0;
      }
      if (is_integrated_fetching_mode(fetching_mode_)) {
        log_route_service_.stop();
      }
      log_ext_handler_.stop();
      LOG_INFO("stop fetcher succ");
    }
    if (is_direct_fetching_mode(fetching_mode_)) {
      log_ext_handler_.stop();
    }

    LOG_INFO("stop fetcher succ");
  }
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
    LOG_INFO("pause fetcher succ", "last pause time", TS_TO_STR(last_pause_time),
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
    LOG_INFO("resume fetcher succ", "pause interval", TVAL_TO_STR(pause_interval));
  }
}

bool ObLogFetcher::is_paused()
{
  return ATOMIC_LOAD(&paused_);
}

void ObLogFetcher::mark_stop_flag()
{
  if (OB_UNLIKELY(is_inited_)) {
    LOG_INFO("mark fetcher stop begin", K_(is_loading_data_dict_baseline_data));
    stop_flag_ = true;

    stream_worker_.mark_stop_flag();
    dead_pool_.mark_stop_flag();
    idle_pool_.mark_stop_flag();
    start_lsn_locator_.mark_stop_flag();
    if (is_direct_fetching_mode(fetching_mode_)) {
      log_ext_handler_.stop();
    }
    LOG_INFO("mark fetcher stop succ",K_(is_loading_data_dict_baseline_data));
  }
}

int ObLogFetcher::add_ls(
    const logservice::TenantLSID &tls_id,
    const logfetcher::ObLogFetcherStartParameters &start_parameters)
{
  int ret = OB_SUCCESS;
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
    LOG_ERROR("not inited", KR(ret));
  }
  // Requires a valid start-up timestamp
  else if (OB_UNLIKELY(start_tstamp_ns <= -1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid start tstamp", KR(ret), K(start_tstamp_ns), K(tls_id), K(start_lsn));
  } else if (is_integrated_fetching_mode(fetching_mode_)
      && OB_FAIL(log_route_service_.registered(tls_id.get_tenant_id(), tls_id.get_ls_id()))) {
    LOG_ERROR("ObLogRouteService registered fail", KR(ret), K(start_tstamp_ns), K(tls_id), K(start_lsn));
  }
  // Push LS into ObLogLSFetchMgr
  else if (OB_FAIL(ls_fetch_mgr_.add_ls(tls_id, start_parameters, is_loading_data_dict_baseline_data_,
      enable_direct_load_inc_, fetching_mode_, archive_dest_))) {
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
    LOG_INFO("Fetcher add ls succ", K(tls_id), K(is_loading_data_dict_baseline_data_),
        K(start_parameters));
  }

  return ret;
}

int ObLogFetcher::recycle_ls(const logservice::TenantLSID &tls_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("Fetcher not inited", KR(ret));
  } else if (OB_FAIL(ls_fetch_mgr_.recycle_ls(tls_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("ls has been recycled in fetcher", K(tls_id));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("recycle ls fail", KR(ret), K(tls_id));
    }
  } else {
    LOG_INFO("Fetcher recycle ls succ", K(tls_id));
  }

  return ret;
}

int ObLogFetcher::remove_ls(const logservice::TenantLSID &tls_id)
{
  int ret = OB_SUCCESS;
  // Copy the tls_id to avoid recycle
  const logservice::TenantLSID removed_tls_id = tls_id;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("Fetcher not inited", KR(ret));
  } else if (OB_FAIL(fs_container_mgr_.remove_fsc(tls_id))) {
    LOG_ERROR("fs_container_mgr_ remove_fsc failed", KR(ret), K(tls_id));
  } else if (OB_FAIL(ls_fetch_mgr_.remove_ls(tls_id))) {
    LOG_ERROR("ls_fetch_mgr_ remove_ls failed", KR(ret), K(tls_id));
  } else {
    LOG_INFO("Fetcher remove ls succ", K(removed_tls_id));
  }

  return ret;
}

int ObLogFetcher::wait_for_all_ls_to_be_removed(const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("Fetcher not inited", KR(ret));
  } else {
    int64_t start_time = get_timestamp();

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

int ObLogFetcher::set_start_global_trans_version(const int64_t start_global_trans_version)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_global_trans_version)) {
    LOG_ERROR("invalid argument", K(start_global_trans_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ls_fetch_mgr_.set_start_global_trans_version(start_global_trans_version))) {
    LOG_ERROR("ls_fetch_mgr_ set_start_global_trans_version fail", KR(ret), K(start_global_trans_version));
  } else {
    // succ
  }

  return ret;
}

void ObLogFetcher::configure(const ObLogConfig &cfg)
{
  int ret = OB_SUCCESS;
  bool print_ls_heartbeat_info = cfg.print_ls_heartbeat_info;
  const int64_t inner_heartbeat_interval = cfg.output_inner_heartbeat_interval_msec * _MSEC_;

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
  ATOMIC_STORE(&g_inner_heartbeat_interval, inner_heartbeat_interval);

  ObLogStartLSNLocator::configure(cfg);
  LSFetchCtx::configure(cfg);
  FetchStream::configure(cfg);
  ObLogFixedTimer::configure(cfg);
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
      LOG_ERROR("update_preferred_upsteam_log_region failed", KR(ret), "region", cfg.region);
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
    LOG_ERROR("fetcher not inited, fs_container_mgr not available currently", KR(ret), K_(is_inited));
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
    LOG_ERROR("fetcher not inited, log_route_service not available currently", KR(ret), K_(is_inited));
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
    LOG_ERROR("fetcher not inited, could not get large buffer pool", KR(ret), K_(is_inited));
  } else {
    large_buffer_pool = &large_buffer_pool_;
  }
  return ret;
}

int ObLogFetcher::get_log_ext_handler(logservice::ObLogExternalStorageHandler *&log_ext_handler)
{
  int ret = OB_SUCCESS;

  if(IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("fetcher not inited, could not get log ext handler", KR(ret), K_(is_inited));
  } else {
    log_ext_handler = &log_ext_handler_;
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
    LOG_ERROR("fetcher is not inited", KR(ret), K_(is_inited));
  } else {
    is_exceeded = false;
    logservice::TenantLSID tls_id(tenant_id, share::SYS_LS);
    LSFetchCtx *ls_fetch_ctx = nullptr;
    int64_t progress = OB_INVALID_TIMESTAMP;
    logfetcher::PartTransDispatchInfo dispatch_info;

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

void *ObLogFetcher::misc_thread_func_(void *arg)
{
  if (NULL != arg) {
    ObLogFetcher *host = static_cast<ObLogFetcher *>(arg);
    host->run_misc_thread();
  }
  return NULL;
}

void ObLogFetcher::run_misc_thread()
{
  LOG_INFO("fetcher misc thread start");

  while (! stop_flag_) {
    // Periodic printing progress slowest k partitions
    if (REACH_TIME_INTERVAL(PRINT_K_SLOWEST_PARTITION)) {
      fs_container_mgr_.print_stat();

      // Print upper_limt, fetcher_delay
      print_fetcher_stat_();
      // Print the slowest k ls
      ls_fetch_mgr_.print_k_slowest_ls();
    }

    if (REACH_TIME_INTERVAL(PRINT_CLUSTER_ID_IGNORE_TPS_INTERVAL)) {
      cluster_id_filter_.stat_ignored_tps();
    }

    ob_usleep(MISC_THREAD_SLEEP_TIME);
  }

  LOG_INFO("fetcher misc thread stop");
}

void *ObLogFetcher::heartbeat_dispatch_thread_func_(void *arg)
{
  if (NULL != arg) {
    ObLogFetcher *host = static_cast<ObLogFetcher *>(arg);
    host->heartbeat_dispatch_routine();
  }
  return NULL;
}

void ObLogFetcher::heartbeat_dispatch_routine()
{
  int ret = OB_SUCCESS;
  LOG_INFO("fetcher heartbeat dispatch thread start");
  // Global heartbeat invalid tls_id
  logservice::TenantLSID hb_tls_id;

  if (is_loading_data_dict_baseline_data_) {
    // Don't have to deal with heartbeat when is loading data dictionary baseline data
  } else if (OB_ISNULL(task_pool_)) {
    LOG_ERROR("invalid task pool", K(task_pool_));
    ret = OB_NOT_INIT;
  } else {
    while (OB_SUCCESS == ret && ! stop_flag_) {
      int64_t heartbeat_tstamp = OB_INVALID_TIMESTAMP;
      PartTransTask *task = NULL;

      if (!TCTX.is_running()) {
        LOG_INFO("OBCDC not launch finish, skip generate heartbeat");
      // Get the next heartbeat timestamp
      } else if (OB_FAIL(next_heartbeat_timestamp_(heartbeat_tstamp, last_timestamp_))) {
        if (OB_NEED_RETRY != ret) {
          if (OB_EMPTY_RESULT != ret) {
            LOG_ERROR("next_heartbeat_timestamp_ fail", KR(ret), K(heartbeat_tstamp), K_(last_timestamp));
          } else {
            ret = OB_SUCCESS;
          }
        }
      } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == heartbeat_tstamp)) {
        LOG_ERROR("heartbeat timestamp is invalid", K(heartbeat_tstamp));
        ret = OB_ERR_UNEXPECTED;
      } else if (heartbeat_tstamp == last_timestamp_) {
        // Heartbeat is not updated, no need to generate
      }
      else if (OB_ISNULL(task = task_pool_->get(NULL, hb_tls_id))) {
        LOG_ERROR("alloc part trans task fail", K(task));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_FAIL(task->init_global_heartbeat_info(heartbeat_tstamp))) {
        LOG_ERROR("init heartbeat task fail", KR(ret), K(heartbeat_tstamp), KPC(task));
      } else if (OB_ISNULL(dispatcher_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("dispatcher_ is nullptr", KR(ret));
      // Dispatch heartbeat task
      } else if (OB_FAIL(dispatcher_->dispatch(*task, stop_flag_))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("dispatch heartbeat task fail", KR(ret), KPC(task));
        }
      } else {
        last_timestamp_ = heartbeat_tstamp;
      }

      if (OB_SUCCESS == ret) {
        ob_usleep((useconds_t)g_inner_heartbeat_interval);
      }

      if (OB_NEED_RETRY == ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  if (stop_flag_) {
    ret = OB_IN_STOP_STATE;
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "Fetcher HEARTBEAT thread exits, err=%d", ret);
    stop_flag_ = true;
  }

  LOG_INFO("fetcher heartbeat dispatch thread stop", KR(ret), K(stop_flag_));
}

void ObLogFetcher::print_fetcher_stat_()
{
  int ret = OB_SUCCESS;
  int64_t min_progress = OB_INVALID_TIMESTAMP;
  int64_t upper_limit_ns = OB_INVALID_TIMESTAMP;
  int64_t fetcher_delay = OB_INVALID_TIMESTAMP;
  int64_t dml_progress_limit = 0;

  // Get global minimum progress
  if (OB_FAIL(progress_controller_.get_min_progress(min_progress))) {
    if (OB_EMPTY_RESULT != ret) {
      LOG_ERROR("get_min_progress fail", KR(ret), K(progress_controller_));
    }
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == min_progress)) {
    //LOG_ERROR("current min progress is invalid", K(min_progress), K(progress_controller_));
    ret = OB_INVALID_ERROR;
  } else {
    dml_progress_limit = ATOMIC_LOAD(&FetchStream::g_dml_progress_limit);
    upper_limit_ns = min_progress + dml_progress_limit * NS_CONVERSION;
    fetcher_delay = get_timestamp() - min_progress / NS_CONVERSION;
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("[STAT] [FETCHER]", "upper_limit", NTS_TO_STR(upper_limit_ns),
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

ObLogFetcher::FetchCtxMapHBFunc::FetchCtxMapHBFunc() :
    data_progress_(OB_INVALID_TIMESTAMP),
    ddl_progress_(OB_INVALID_TIMESTAMP),
    ddl_last_dispatch_log_lsn_(),
    min_progress_(OB_INVALID_TIMESTAMP),
    max_progress_(OB_INVALID_TIMESTAMP),
    min_progress_ls_(),
    max_progress_ls_(),
    part_count_(0),
    ls_progress_infos_()
{
}

bool ObLogFetcher::FetchCtxMapHBFunc::operator()(const logservice::TenantLSID &tls_id, LSFetchCtx *&ctx)
{
  int ret = OB_SUCCESS;
  bool bool_ret = true;
  int64_t progress = OB_INVALID_TIMESTAMP;
  logfetcher::PartTransDispatchInfo dispatch_info;
  palf::LSN last_dispatch_log_lsn;

  if (NULL == ctx) {
    // ctx is invalid, not processed
  } else if (OB_FAIL(ctx->get_dispatch_progress(progress, dispatch_info))) {
    LOG_ERROR("get_dispatch_progress fail", KR(ret), K(tls_id), KPC(ctx));
  }
  // The progress returned by the fetch log context must be valid, and its progress value must be a valid value, underlined by the fetch log progress
  else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == progress)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("partition dispatch progress is invalid", KR(ret), K(progress), K(tls_id), KPC(ctx),
        K(dispatch_info));
  } else {
    last_dispatch_log_lsn = dispatch_info.last_dispatch_log_lsn_;

    if (tls_id.is_sys_log_stream()) {
      // Assuming only one DDL partition
      // Update the DDL partition
      ddl_progress_ = progress;
      ddl_last_dispatch_log_lsn_ = last_dispatch_log_lsn;
    } else {
      // Update data progress
      if (OB_INVALID_TIMESTAMP == data_progress_) {
        data_progress_ = progress;
      } else {
        data_progress_ = std::min(data_progress_, progress);
      }

      if (OB_FAIL(ls_progress_infos_.push_back(LSProgressInfo(tls_id, progress)))) {
        LOG_ERROR("ls_progress_infos_ push_back failed", KR(ret));
      }
    }

    // Update maximum and minimum progress
    if (OB_INVALID_TIMESTAMP == max_progress_ || progress > max_progress_) {
      max_progress_ = progress;
      max_progress_ls_ = tls_id;
    }

    if (OB_INVALID_TIMESTAMP == min_progress_ || progress < min_progress_) {
      min_progress_ = progress;
      min_progress_ls_ = tls_id;
    }

    part_count_++;

    if (g_print_ls_heartbeat_info) {
      _LOG_INFO("[STAT] [FETCHER] [HEARTBEAT] TLS=%s PROGRESS=%ld DISPATCH_LOG_LSN=%lu "
          "DATA_PROGRESS=%ld DDL_PROGRESS=%ld DDL_DISPATCH_LOG_LSN=%lu", to_cstring(tls_id),
          progress, last_dispatch_log_lsn.val_, data_progress_, ddl_progress_, ddl_last_dispatch_log_lsn_.val_);
    }
  }

  return bool_ret;
}

// Get the heartbeat progress
//
// Principle: get producer progress first, then consumer progress; ensure progress values are safe
//
// DDL partition is the producer of data partition, DDL will trigger new partition, so you should get the DDL progress first before getting the data partition progress
int ObLogFetcher::next_heartbeat_timestamp_(int64_t &heartbeat_tstamp, const int64_t last_timestamp)
{
  int ret = OB_SUCCESS;
  static int64_t cdc_start_tstamp_ns = TCTX.start_tstamp_ns_;
  static int64_t last_data_progress = OB_INVALID_TIMESTAMP;
  static int64_t last_ddl_handle_progress = OB_INVALID_TIMESTAMP;
  static palf::LSN last_ddl_handle_lsn;
  static logservice::TenantLSID last_min_data_progress_ls;
  static LSProgressInfoArray last_ls_progress_infos;

  FetchCtxMapHBFunc hb_func;
  uint64_t ddl_min_progress_tenant_id = OB_INVALID_TENANT_ID;
  palf::LSN ddl_handle_lsn;
  int64_t ddl_handle_progress = OB_INVALID_TIMESTAMP;

  if (OB_ISNULL(sys_ls_handler_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("invalid sys_ls_handler", KR(ret), K(sys_ls_handler_));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP >= cdc_start_tstamp_ns)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("cdc_start_tstamp_ns is invalid", KR(ret), K(cdc_start_tstamp_ns));
  }
  // Get the DDL processing progress first, because the DDL is the producer of the data partition, and getting it first will ensure that the overall progress is not reverted
  // Note: the progress value should not be invalid
  else if (OB_FAIL(sys_ls_handler_->get_progress(ddl_min_progress_tenant_id, ddl_handle_progress,
          ddl_handle_lsn))) {
    if (OB_EMPTY_RESULT != ret) {
      LOG_ERROR("sys_ls_handler get_progress fail", KR(ret), K(ddl_min_progress_tenant_id),
          K(ddl_handle_progress), K(ddl_handle_lsn));
    } else {
      LOG_INFO("no valid tenant is in serve, skip get next_heartbeat_timestamp_", KR(ret));
    }
  }
  else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == ddl_handle_progress)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get DDL handle progress is invalid", KR(ret), K(ddl_handle_progress), K(ddl_handle_lsn));
  }
  // Then iterate through all the partitions to get the distribution progress of each partition, i.e. the progress of Fetcher's distribution data
  // Note: Here we also get the progress of the DDL distribution, which is only used for printing
  else if (OB_FAIL(ls_fetch_mgr_.for_each_ls(hb_func))) {
    LOG_WARN("for each part fetch ctx fail", KR(ret));
  } else {
    int64_t data_progress = hb_func.data_progress_;
    // TODO
    logservice::TenantLSID min_progress_tls_id = hb_func.min_progress_ls_;
    logservice::TenantLSID max_progress_tls_id = hb_func.max_progress_ls_;

    // The final heartbeat timestamp is equal to the minimum value of the DDL processing progress and data progress
    if (OB_INVALID_TIMESTAMP != data_progress) {
      heartbeat_tstamp = std::min(data_progress, ddl_handle_progress);
    } else {
      heartbeat_tstamp = ddl_handle_progress;
    }

    if (REACH_TIME_INTERVAL(PRINT_HEARTBEAT_INTERVAL) || g_print_ls_heartbeat_info) {
      // Calculation of the minimum and maximum progress, and the corresponding partitions
      int64_t min_progress = hb_func.min_progress_;
      int64_t max_progress = hb_func.max_progress_;

      // If the DDL processing progress is smaller than the minimum progress, the minimum progress takes the DDL progress
      if (min_progress > ddl_handle_progress) {
        min_progress = ddl_handle_progress;
        // FIXME: Here the tls_id of the DDL is constructed directly, which may be wrong for the partition count field of the sys tenant, but here it is just
        // printing logs does not affect
        // TODO modifies
        min_progress_tls_id = logservice::TenantLSID(ddl_min_progress_tenant_id, share::SYS_LS);
      }

      _LOG_INFO("[STAT] [FETCHER] [HEARTBEAT] DELAY=[%.3lf, %.3lf](sec) LS_COUNT=%ld "
          "MIN_DELAY=%s(%ld) MAX_DELAY=%s(%ld) DATA_PROGRESS=%s(%ld) "
          "DDL_PROGRESS=%s(%ld) DDL_TENANT=%lu DDL_LOG_LSN=%s",
          get_delay_sec(max_progress),
          get_delay_sec(min_progress),
          hb_func.part_count_,
          to_cstring(max_progress_tls_id),
          max_progress,
          to_cstring(min_progress_tls_id),
          min_progress,
          NTS_TO_STR(data_progress),
          data_progress,
          NTS_TO_STR(ddl_handle_progress),
          ddl_handle_progress,
          ddl_min_progress_tenant_id,
          to_cstring(ddl_handle_lsn));
    }

    if (OB_UNLIKELY(OB_INVALID_TIMESTAMP != heartbeat_tstamp && cdc_start_tstamp_ns -1 > heartbeat_tstamp)) {
      ret = OB_NEED_RETRY;
      if (REACH_TIME_INTERVAL(PRINT_HEARTBEAT_INTERVAL)) {
        LOG_INFO("skip heartbeat_tstamp less than cdc_start_tstamp_ns_", KR(ret),
            K(heartbeat_tstamp), K(cdc_start_tstamp_ns));
      }
    }
    // Checks if the heartbeat timestamp is reverted
    else if (OB_INVALID_TIMESTAMP != last_timestamp && heartbeat_tstamp < last_timestamp) {
      LOG_ERROR("heartbeat timestamp is rollback, unexcepted error",
          "last_timestamp", NTS_TO_STR(last_timestamp),
          K(last_timestamp),
          "heartbeat_tstamp", NTS_TO_STR(heartbeat_tstamp),
          K(heartbeat_tstamp),
          "data_progress", NTS_TO_STR(data_progress),
          "last_data_progress", NTS_TO_STR(last_data_progress),
          K(min_progress_tls_id), K(last_min_data_progress_ls),
          "ddl_handle_progress", NTS_TO_STR(ddl_handle_progress),
          "last_ddl_handle_progress", NTS_TO_STR(last_ddl_handle_progress),
          "ddl_handle_lsn", to_cstring(ddl_handle_lsn),
          "last_ddl_handle_lsn", to_cstring(last_ddl_handle_lsn),
          K(last_ls_progress_infos));
      ret = OB_ERR_UNEXPECTED;
    } else {
      last_data_progress = data_progress;
      last_ddl_handle_progress = ddl_handle_progress;
      last_ddl_handle_lsn = ddl_handle_lsn;
      last_min_data_progress_ls = min_progress_tls_id;
      last_ls_progress_infos = hb_func.ls_progress_infos_;
    }
  }
  return ret;
}

}
}
