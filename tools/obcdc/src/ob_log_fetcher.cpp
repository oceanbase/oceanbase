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

#include "ob_log_fetcher.h"

#include "lib/oblog/ob_log_module.h"  // LOG_*

#include "ob_log_config.h"            // ObLogConfig
#include "ob_log_timer.h"             // ObLogFixedTimer
#include "ob_log_ddl_handler.h"       // IObLogDDLHandler
#include "ob_log_task_pool.h"         // ObLogTransTaskPool
#include "ob_log_instance.h"          // IObLogErrHandler

using namespace oceanbase::common;

namespace oceanbase
{
namespace liboblog
{

///////////////////////////////////////////// ObLogFetcher /////////////////////////////////////////////

bool ObLogFetcher::g_print_partition_heartbeat_info =
    ObLogConfig::default_print_partition_heartbeat_info;
int64_t ObLogFetcher::g_inner_heartbeat_interval =
    ObLogConfig::default_output_inner_heartbeat_interval_msec * _MSEC_;

ObLogFetcher::ObLogFetcher() :
    inited_(false),
    task_pool_(NULL),
    ddl_handler_(NULL),
    err_handler_(NULL),
    part_trans_resolver_factory_(),
    part_fetch_mgr_(),
    progress_controller_(),
    rpc_(),
    all_svr_cache_(),
    svr_finder_(),
    heartbeater_(),
    start_log_id_locator_(),
    idle_pool_(),
    dead_pool_(),
    stream_worker_(),
    dispatcher_(),
    cluster_id_filter_(),
    misc_tid_(0),
    heartbeat_dispatch_tid_(0),
    last_timestamp_(OB_INVALID_TIMESTAMP),
    stop_flag_(true),
    paused_(false),
    pause_time_(OB_INVALID_TIMESTAMP),
    resume_time_(OB_INVALID_TIMESTAMP)
{
}

ObLogFetcher::~ObLogFetcher()
{
  destroy();
}

int ObLogFetcher::init(IObLogDmlParser *dml_parser,
    IObLogDDLHandler *ddl_handler,
    IObLogErrHandler *err_handler,
    ObLogSysTableHelper &systable_helper,
    TaskPool *task_pool,
    IObLogEntryTaskPool *log_entry_task_pool,
    IObLogCommitter *committer,
    const ObLogConfig &cfg,
    const int64_t start_seq)
{
  int ret = OB_SUCCESS;
  int64_t max_cached_part_fetch_ctx_count = cfg.active_partition_count;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(dml_parser)
      || OB_ISNULL(ddl_handler_ = ddl_handler)
      || OB_ISNULL(err_handler_ = err_handler)
      || OB_ISNULL(task_pool_ = task_pool)
      || OB_ISNULL(log_entry_task_pool)
      || OB_ISNULL(committer)) {
    LOG_ERROR("invalid argument", K(dml_parser), K(ddl_handler), K(err_handler),
        K(task_pool), K(log_entry_task_pool));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Before the Fetcher module is initialized, the following configuration items need to be loaded
    configure(cfg);
    if (OB_FAIL(progress_controller_.init(cfg.partition_count_upper_limit))) {
      LOG_ERROR("init progress controller fail", KR(ret));
    } else if (OB_FAIL(dispatcher_.init(ddl_handler, committer, start_seq))) {
      LOG_ERROR("init fetcher dispatcher fail", KR(ret), K(ddl_handler),
          K(committer), K(start_seq));
    } else if (OB_FAIL(cluster_id_filter_.init(cfg.cluster_id_black_list.str(),
        cfg.cluster_id_black_value_min, cfg.cluster_id_black_value_max))) {
      LOG_ERROR("init cluster_id_filter fail", KR(ret));
    } else if (OB_FAIL(part_trans_resolver_factory_.init(*task_pool, *log_entry_task_pool, dispatcher_, cluster_id_filter_))) {
      LOG_ERROR("init part trans resolver factory fail", KR(ret));
    } else if (OB_FAIL(part_fetch_mgr_.init(max_cached_part_fetch_ctx_count,
            progress_controller_,
            part_trans_resolver_factory_))) {
      LOG_ERROR("init part fetch mgr fail", KR(ret));
    } else if (OB_FAIL(rpc_.init(cfg.rpc_tenant_id, cfg.io_thread_num))) {
      LOG_ERROR("init rpc handler fail", KR(ret));
    } else if (OB_FAIL(all_svr_cache_.init(systable_helper, *err_handler))) {
      LOG_ERROR("init all svr cache fail", KR(ret));
    } else if (OB_FAIL(svr_finder_.init(cfg.svr_finder_thread_num, *err_handler,
            all_svr_cache_, systable_helper))) {
      LOG_ERROR("init svr finder fail", KR(ret));
    } else if (OB_FAIL(heartbeater_.init(cfg.fetcher_heartbeat_thread_num, rpc_, *err_handler))) {
      LOG_ERROR("init heartbeater fail", KR(ret));
    } else if (OB_FAIL(start_log_id_locator_.init(cfg.start_log_id_locator_thread_num,
            cfg.start_log_id_locator_locate_count,
            rpc_, *err_handler))) {
      LOG_ERROR("init start log id locator fail", KR(ret));
    } else if (OB_FAIL(idle_pool_.init(cfg.idle_pool_thread_num,
            *err_handler,
            svr_finder_,
            stream_worker_,
            start_log_id_locator_))) {
      LOG_ERROR("init idle pool fail", KR(ret));
    } else if (OB_FAIL(dead_pool_.init(cfg.dead_pool_thread_num,
            part_fetch_mgr_,
            *err_handler))) {
      LOG_ERROR("init dead pool fail", KR(ret));
    } else if (OB_FAIL(stream_worker_.init(cfg.stream_worker_thread_num,
            cfg.svr_stream_cached_count,
            cfg.fetch_stream_cached_count,
            cfg.rpc_result_cached_count,
            cfg.timer_task_count_upper_limit,
            rpc_,
            idle_pool_,
            dead_pool_,
            svr_finder_,
            *err_handler,
            all_svr_cache_,
            heartbeater_,
            progress_controller_))) {
      LOG_ERROR("init stream worker fail", KR(ret));
    } else {
      paused_ = false;
      pause_time_ = OB_INVALID_TIMESTAMP;
      resume_time_ = OB_INVALID_TIMESTAMP;
      misc_tid_ = 0;
      heartbeat_dispatch_tid_ = 0;
      last_timestamp_ = OB_INVALID_TIMESTAMP;
      stop_flag_ = true;
      inited_ = true;

      // Initialization test mode
      IObLogPartTransResolver::test_mode_on = cfg.test_mode_on;
      IObLogPartTransResolver::test_mode_ignore_redo_count = cfg.test_mode_ignore_redo_count;
      IObLogPartTransResolver::test_checkpoint_mode_on = cfg.test_checkpoint_mode_on;

      LOG_INFO("init fetcher succ", "test_mode_on", IObLogPartTransResolver::test_mode_on,
          "test_mode_ignore_redo_count", IObLogPartTransResolver::test_mode_ignore_redo_count,
          "test_checkpoint_mode_on", IObLogPartTransResolver::test_checkpoint_mode_on);
    }
  }
  return ret;
}

void ObLogFetcher::destroy()
{
  stop();

  // TODO: Global destroy all memory
  inited_ = false;
  task_pool_ = NULL;
  ddl_handler_ = NULL;
  err_handler_ = NULL;

  misc_tid_ = 0;
  heartbeat_dispatch_tid_ = 0;
  last_timestamp_ = OB_INVALID_TIMESTAMP;
  stop_flag_ = true;
  paused_ = false;
  pause_time_ = OB_INVALID_TIMESTAMP;
  resume_time_ = OB_INVALID_TIMESTAMP;

  stream_worker_.destroy();
  idle_pool_.destroy();
  dead_pool_.destroy();
  start_log_id_locator_.destroy();
  all_svr_cache_.destroy();
  heartbeater_.destroy();
  svr_finder_.destroy();
  rpc_.destroy();
  progress_controller_.destroy();
  part_fetch_mgr_.destroy();
  part_trans_resolver_factory_.destroy();
  dispatcher_.destroy();
  cluster_id_filter_.destroy();

  LOG_INFO("destroy fetcher succ");
}

int ObLogFetcher::start()
{
  int ret = OB_SUCCESS;
  int pthread_ret = 0;

  LOG_INFO("begin start fetcher");

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! stop_flag_)) {
    LOG_ERROR("fetcher has been started", K(stop_flag_));
    ret = OB_INIT_TWICE;
  } else {
    stop_flag_ = false;

    if (OB_FAIL(svr_finder_.start())) {
      LOG_ERROR("start svr finder fail", KR(ret));
    } else if (OB_FAIL(heartbeater_.start())) {
      LOG_ERROR("start heartbeater fail", KR(ret));
    } else if (OB_FAIL(start_log_id_locator_.start())) {
      LOG_ERROR("start 'start_log_id_locator' fail", KR(ret));
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
      LOG_INFO("start fetcher succ", K(misc_tid_), K(heartbeat_dispatch_tid_));
    }
  }
  return ret;
}

void ObLogFetcher::stop()
{
  if (OB_LIKELY(inited_)) {
    stop_flag_ = true;

    LOG_INFO("stop fetcher begin");
    stream_worker_.stop();
    dead_pool_.stop();
    idle_pool_.stop();
    start_log_id_locator_.stop();
    heartbeater_.stop();
    svr_finder_.stop();

    if (0 != misc_tid_) {
      int pthread_ret = pthread_join(misc_tid_, NULL);
      if (0 != pthread_ret) {
        LOG_ERROR("join fetcher misc thread fail", K(misc_tid_), K(pthread_ret),
            KERRNOMSG(pthread_ret));
      }
      misc_tid_ = 0;
    }

    if (0 != heartbeat_dispatch_tid_) {
      int pthread_ret = pthread_join(heartbeat_dispatch_tid_, NULL);
      if (0 != pthread_ret) {
        LOG_ERROR("join fetcher heartbeat dispatch thread fail", K(heartbeat_dispatch_tid_),
            K(pthread_ret), KERRNOMSG(pthread_ret));
      }
      heartbeat_dispatch_tid_ = 0;
    }

    LOG_INFO("stop fetcher succ");
  }
}

void ObLogFetcher::pause()
{
  if (OB_LIKELY(inited_)) {
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
  if (OB_LIKELY(inited_)) {
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
  if (OB_UNLIKELY(inited_)) {
    LOG_INFO("mark fetcher stop begin");
    stop_flag_ = true;

    stream_worker_.mark_stop_flag();
    dead_pool_.mark_stop_flag();
    idle_pool_.mark_stop_flag();
    start_log_id_locator_.mark_stop_flag();
    heartbeater_.mark_stop_flag();
    svr_finder_.mark_stop_flag();
    LOG_INFO("mark fetcher stop succ");
  }
}

int ObLogFetcher::add_partition(const common::ObPartitionKey &pkey,
    const int64_t start_tstamp,
    const uint64_t start_log_id)
{
  int ret = OB_SUCCESS;
  PartFetchCtx *part_fetch_ctx = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  }
  // Requires a valid start-up timestamp
  else if (OB_UNLIKELY(start_tstamp <= 0)) {
    LOG_ERROR("invalid start tstamp", K(start_tstamp), K(pkey), K(start_log_id));
    ret = OB_INVALID_ARGUMENT;
  }
  // Push partition into PartFetchMgr
  else if (OB_FAIL(part_fetch_mgr_.add_partition(pkey, start_tstamp, start_log_id))) {
    LOG_ERROR("add partition by part fetch mgr fail", KR(ret), K(pkey), K(start_tstamp),
        K(start_log_id));
  } else if (OB_FAIL(part_fetch_mgr_.get_part_fetch_ctx(pkey, part_fetch_ctx))) {
    LOG_ERROR("get part fetch ctx fail", KR(ret), K(pkey));
  } else if (OB_ISNULL(part_fetch_ctx)) {
    LOG_ERROR("part fetch ctx is NULL", K(part_fetch_ctx));
    ret = OB_ERR_UNEXPECTED;
  }
  // First enter the IDLE POOL to initialize basic information
  else if (OB_FAIL(idle_pool_.push(part_fetch_ctx))) {
    LOG_ERROR("push task into idle pool fail", KR(ret), K(part_fetch_ctx));
  } else {
    LOG_INFO("fetcher add partition succ", K(pkey), K(start_tstamp), K(start_log_id));
  }
  return ret;
}

int ObLogFetcher::recycle_partition(const common::ObPartitionKey &pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(part_fetch_mgr_.recycle_partition(pkey))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("partition has been recycled in fetcher", K(pkey));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("recycle partition fail", KR(ret), K(pkey));
    }
  } else {
    LOG_INFO("fetcher recycle partition succ", K(pkey));
  }
  return ret;
}

int ObLogFetcher::set_start_global_trans_version(const int64_t start_global_trans_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_global_trans_version)) {
    LOG_ERROR("invalid argument", K(start_global_trans_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_fetch_mgr_.set_start_global_trans_version(start_global_trans_version))) {
    LOG_ERROR("part_fetch_mgr_ set_start_global_trans_version fail", KR(ret), K(start_global_trans_version));
  } else {
    // succ
  }

  return ret;
}

void ObLogFetcher::configure(const ObLogConfig &cfg)
{
  bool print_partition_heartbeat_info = cfg.print_partition_heartbeat_info;
  const int64_t inner_heartbeat_interval = TCONF.output_inner_heartbeat_interval_msec * _MSEC_;

  ATOMIC_STORE(&g_print_partition_heartbeat_info, print_partition_heartbeat_info);
  ATOMIC_STORE(&g_inner_heartbeat_interval, inner_heartbeat_interval);
  LOG_INFO("[CONFIG]", K(print_partition_heartbeat_info), K(g_inner_heartbeat_interval));

  ObLogStartLogIdLocator::configure(cfg);
  ObLogFetcherHeartbeatWorker::configure(cfg);
  PartFetchCtx::configure(cfg);
  FetchStream::configure(cfg);
	ObLogAllSvrCache::configure(cfg);
  ObLogFixedTimer::configure(cfg);
  ObLogRpc::configure(cfg);
  ObLogStreamWorker::configure(cfg);
  BlackList::configure(cfg);
  ObLogPartFetchMgr::configure(cfg);
  FetchLogARpc::configure(cfg);
  svr_finder_.configure(cfg);
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
      // Print upper_limt, fetcher_delay
      print_fetcher_stat_();
      // Print the slowest k partitions
      part_fetch_mgr_.print_k_slowest_partition();
    }

    if (REACH_TIME_INTERVAL(PRINT_CLUSTER_ID_IGNORE_TPS_INTERVAL)) {
      cluster_id_filter_.stat_ignored_tps();
    }

    if (REACH_TIME_INTERVAL(TRANS_ABORT_INFO_GC_INTERVAL)) {
      if (OB_INVALID_TIMESTAMP != last_timestamp_) {
        part_trans_resolver_factory_.gc_commit_trans_info(last_timestamp_);
      }
    }

    usleep(MISC_THREAD_SLEEP_TIME);
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
  // Global heartbeat invalid pkey
  ObPartitionKey hb_pkey;

  if (OB_ISNULL(task_pool_)) {
    LOG_ERROR("invalid task pool", K(task_pool_));
    ret = OB_NOT_INIT;
  } else {
    while (OB_SUCCESS == ret && ! stop_flag_) {
      int64_t heartbeat_tstamp = OB_INVALID_TIMESTAMP;
      PartTransTask *task = NULL;

      // Get the next heartbeat timestamp
      if (OB_FAIL(next_heartbeat_timestamp_(heartbeat_tstamp, last_timestamp_))) {
        LOG_ERROR("next_heartbeat_timestamp_ fail", KR(ret), K(last_timestamp_));
      } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == heartbeat_tstamp)) {
        LOG_ERROR("heartbeat timestamp is invalid", K(heartbeat_tstamp));
        ret = OB_ERR_UNEXPECTED;
      } else if (heartbeat_tstamp == last_timestamp_) {
        // Heartbeat is not updated, no need to generate
      }
      else if (OB_ISNULL(task = task_pool_->get(NULL, hb_pkey))) {
        LOG_ERROR("alloc part trans task fail", K(task));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_FAIL(task->init_global_heartbeat_info(heartbeat_tstamp))) {
        LOG_ERROR("init heartbeat task fail", KR(ret), K(heartbeat_tstamp), KPC(task));
      }
      // Dispatch heartbeat task
      else if (OB_FAIL(dispatcher_.dispatch(*task, stop_flag_))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("dispatch heartbeat task fail", KR(ret), KPC(task));
        }
      } else {
        last_timestamp_ = heartbeat_tstamp;
      }

      if (OB_SUCCESS == ret) {
        usleep((useconds_t)g_inner_heartbeat_interval);
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
  int64_t upper_limit_us = OB_INVALID_TIMESTAMP;
  int64_t fetcher_delay = OB_INVALID_TIMESTAMP;
  int64_t dml_progress_limit = 0;

  // Get global minimum progress
  if (OB_FAIL(progress_controller_.get_min_progress(min_progress))) {
    LOG_ERROR("get_min_progress fail", KR(ret), K(progress_controller_));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == min_progress)) {
    LOG_ERROR("current min progress is invalid", K(min_progress), K(progress_controller_));
    ret = OB_INVALID_ERROR;
  } else {
    dml_progress_limit = ATOMIC_LOAD(&FetchStream::g_dml_progress_limit);
    upper_limit_us = min_progress + dml_progress_limit;
    fetcher_delay = get_timestamp() - min_progress;
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("[STAT] [FETCHER]", "upper_limit", TS_TO_STR(upper_limit_us),
        "dml_progress_limit_sec", dml_progress_limit / _SEC_,
        "fetcher_delay", TVAL_TO_STR(fetcher_delay));
  }
}
ObLogFetcher::FetchCtxMapHBFunc::FetchCtxMapHBFunc() :
    data_progress_(OB_INVALID_TIMESTAMP),
    ddl_progress_(OB_INVALID_TIMESTAMP),
    ddl_last_dispatch_log_id_(OB_INVALID_ID),
    min_progress_(OB_INVALID_TIMESTAMP),
    max_progress_(OB_INVALID_TIMESTAMP),
    min_progress_pkey_(),
    max_progress_pkey_(),
    part_count_(0)
{}

bool ObLogFetcher::FetchCtxMapHBFunc::operator()(const common::ObPartitionKey &pkey, PartFetchCtx *&ctx)
{
  int ret = OB_SUCCESS;
  bool bool_ret = true;
  int64_t progress = OB_INVALID_TIMESTAMP;
  PartTransDispatchInfo dispatch_info;
  uint64_t last_dispatch_log_id = OB_INVALID_ID;

  if (NULL == ctx) {
    // ctx is invalid, not processed
  } else if (OB_FAIL(ctx->get_dispatch_progress(progress, dispatch_info))) {
    LOG_ERROR("get_dispatch_progress fail", KR(ret), K(pkey), KPC(ctx));
  }
  // The progress returned by the fetch log context must be valid, and its progress value must be a valid value, underlined by the fetch log progress
  else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == progress)) {
    LOG_ERROR("partition dispatch progress is invalid", K(progress), K(pkey), KPC(ctx),
        K(dispatch_info));
    ret = OB_ERR_UNEXPECTED;
  } else {
    last_dispatch_log_id = dispatch_info.last_dispatch_log_id_;

    if (is_ddl_table(pkey.get_table_id())) {
      // Assuming only one DDL partition
      // Update the DDL partition
      ddl_progress_ = progress;
      ddl_last_dispatch_log_id_ = last_dispatch_log_id;
    } else {
      // Update data progress
      if (OB_INVALID_TIMESTAMP == data_progress_) {
        data_progress_ = progress;
      } else {
        data_progress_ = std::min(data_progress_, progress);
      }
    }

    // Update maximum and minimum progress
    if (OB_INVALID_TIMESTAMP == max_progress_ || progress > max_progress_) {
      max_progress_ = progress;
      max_progress_pkey_ = pkey;
    }

    if (OB_INVALID_TIMESTAMP == min_progress_ || progress < min_progress_) {
      min_progress_ = progress;
      min_progress_pkey_ = pkey;
    }

    part_count_++;

    if (g_print_partition_heartbeat_info) {
      _LOG_INFO("[STAT] [FETCHER] [HEARTBEAT] PART=%s PROGRESS=%ld DISPATCH_LOG_ID=%lu "
          "DATA_PROGRESS=%ld DDL_PROGRESS=%ld DDL_DISPATCH_LOG_ID=%lu", to_cstring(pkey),
          progress, last_dispatch_log_id, data_progress_, ddl_progress_, ddl_last_dispatch_log_id_);
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
  static int64_t last_data_progress = OB_INVALID_TIMESTAMP;
  static int64_t last_ddl_handle_progress = OB_INVALID_TIMESTAMP;
  static int64_t last_ddl_last_handle_log_id = OB_INVALID_ID;
  static common::ObPartitionKey       last_min_data_progress_pkey;

  FetchCtxMapHBFunc hb_func;
  uint64_t ddl_min_progress_tenant_id = OB_INVALID_TENANT_ID;
  uint64_t ddl_last_handle_log_id = OB_INVALID_ID;
  int64_t ddl_handle_progress = OB_INVALID_TIMESTAMP;

  if (OB_ISNULL(ddl_handler_)) {
    LOG_ERROR("invalid ddl handler", K(ddl_handler_));
    ret = OB_NOT_INIT;
  }
  // Get the DDL processing progress first, because the DDL is the producer of the data partition, and getting it first will ensure that the overall progress is not reverted
  // Note: the progress value should not be invalid
  else if (OB_FAIL(ddl_handler_->get_progress(ddl_min_progress_tenant_id, ddl_handle_progress,
          ddl_last_handle_log_id))) {
    LOG_ERROR("ddl_handler get_progress fail", KR(ret), K(ddl_min_progress_tenant_id),
        K(ddl_handle_progress), K(ddl_last_handle_log_id));
  }
  else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == ddl_handle_progress)) {
    LOG_ERROR("get DDL handle progress is invalid", K(ddl_handle_progress), K(ddl_last_handle_log_id));
    ret = OB_ERR_UNEXPECTED;
  }
  // Then iterate through all the partitions to get the distribution progress of each partition, i.e. the progress of Fetcher's distribution data
  // Note: Here we also get the progress of the DDL distribution, which is only used for printing
  else if (OB_FAIL(part_fetch_mgr_.for_each_part(hb_func))) {
    LOG_ERROR("for each part fetch ctx fail", KR(ret));
  } else {
    int64_t data_progress = hb_func.data_progress_;
    ObPartitionKey min_progress_pkey = hb_func.min_progress_pkey_;
    ObPartitionKey max_progress_pkey = hb_func.max_progress_pkey_;

    // The final heartbeat timestamp is equal to the minimum value of the DDL processing progress and data progress
    if (OB_INVALID_TIMESTAMP != data_progress) {
      heartbeat_tstamp = std::min(data_progress, ddl_handle_progress);
    } else {
      heartbeat_tstamp = ddl_handle_progress;
    }

    if (REACH_TIME_INTERVAL(PRINT_HEARTBEAT_INTERVAL) || g_print_partition_heartbeat_info) {
      // Calculation of the minimum and maximum progress, and the corresponding partitions
      int64_t min_progress = hb_func.min_progress_;
      int64_t max_progress = hb_func.max_progress_;

      // If the DDL processing progress is smaller than the minimum progress, the minimum progress takes the DDL progress
      if (min_progress > ddl_handle_progress) {
        min_progress = ddl_handle_progress;
        // FIXME: Here the pkey of the DDL is constructed directly, which may be wrong for the partition count field of the sys tenant, but here it is just
        // printing logs does not affect
        min_progress_pkey = ObPartitionKey(ddl_min_progress_tenant_id, 0, 0);
      }

      _LOG_INFO("[STAT] [FETCHER] [HEARTBEAT] DELAY=[%.3lf, %.3lf](sec) PART_COUNT=%ld "
          "MIN_DELAY=%s MAX_DELAY=%s DATA_PROGRESS=%s "
          "DDL_PROGRESS=%s DDL_TENANT=%lu DDL_LOG_ID=%lu",
          get_delay_sec(max_progress),
          get_delay_sec(min_progress),
          hb_func.part_count_,
          to_cstring(max_progress_pkey),
          to_cstring(min_progress_pkey),
          TS_TO_STR(data_progress),
          TS_TO_STR(ddl_handle_progress),
          ddl_min_progress_tenant_id,
          ddl_last_handle_log_id);
    }

    // Checks if the heartbeat timestamp is reverted
    if (OB_INVALID_TIMESTAMP != last_timestamp && heartbeat_tstamp < last_timestamp) {
      LOG_ERROR("heartbeat timestamp is rollback, unexcepted error",
          "last_timestamp", TS_TO_STR(last_timestamp),
          K(last_timestamp),
          "heartbeat_tstamp", TS_TO_STR(heartbeat_tstamp),
          K(heartbeat_tstamp),
          "data_progress", TS_TO_STR(data_progress),
          "last_data_progress", TS_TO_STR(last_data_progress),
          K(min_progress_pkey), K(last_min_data_progress_pkey),
          "ddl_handle_progress", TS_TO_STR(ddl_handle_progress),
          "last_ddl_handle_progress", TS_TO_STR(last_ddl_handle_progress),
          "ddl_last_handle_log_id", TS_TO_STR(ddl_last_handle_log_id),
          "last_ddl_last_handle_log_id", TS_TO_STR(last_ddl_last_handle_log_id));
      ret = OB_ERR_UNEXPECTED;
    } else {
      last_data_progress = data_progress;
      last_ddl_handle_progress = ddl_handle_progress;
      last_ddl_last_handle_log_id = ddl_last_handle_log_id;
      last_min_data_progress_pkey = min_progress_pkey;
    }
  }
  return ret;
}

}
}
