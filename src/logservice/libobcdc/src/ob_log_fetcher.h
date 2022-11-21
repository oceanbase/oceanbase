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

#ifndef OCEANBASE_LIBOBCDC_LOG_FETCHER_H_
#define OCEANBASE_LIBOBCDC_LOG_FETCHER_H_

#include "ob_log_ls_fetch_mgr.h"                // ObLogLSFetchMgr
#include "ob_log_fetch_stream_container_mgr.h"  // ObFsContainerMgr
#include "ob_log_rpc.h"                         // ObLogRpc
#include "ob_log_part_progress_controller.h"    // PartProgressController
#include "ob_log_start_lsn_locator.h"           // ObLogStartLSNLocator
#include "ob_log_fetcher_idle_pool.h"           // ObLogFetcherIdlePool
#include "ob_log_fetcher_dead_pool.h"           // ObLogFetcherDeadPool
#include "ob_ls_worker.h"                       // ObLSWorker
#include "ob_log_utils.h"                       // _SEC_
#include "ob_log_cluster_id_filter.h"           // ObLogClusterIDFilter
#include "ob_log_part_trans_resolver_factory.h" // ObLogPartTransResolverFactory
#include "ob_log_fetcher_dispatcher.h"          // ObLogFetcherDispatcher
#include "logservice/logrouteservice/ob_log_route_service.h" // ObLogRouteService
#include "ob_log_ls_callback.h"
#include "ob_log_systable_helper.h"

namespace oceanbase
{
namespace libobcdc
{

class ObLogConfig;
class IObLogFetcher : public LSAddCallback, public LSRecycleCallback
{
public:
  virtual ~IObLogFetcher() { }

  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void pause() = 0;
  virtual void resume() = 0;
  virtual bool is_paused() = 0;
  virtual void mark_stop_flag() = 0;

  // Update fetcher configure
  virtual void configure(const ObLogConfig &cfg) = 0;

  // Add LS
  virtual int add_ls(const TenantLSID &tls_id,
      const int64_t start_tstamp_ns,
      const palf::LSN &start_lsn) = 0;

  // Recycle LS
  virtual int recycle_ls(const TenantLSID &tls_id) = 0;

  virtual int64_t get_part_trans_task_count() const = 0;

  virtual int set_start_global_trans_version(const int64_t start_global_trans_version) = 0;

  virtual int get_fs_container_mgr(IObFsContainerMgr *&fs_container_mgr) = 0;

  virtual int get_log_route_service(logservice::ObLogRouteService *&log_route_service) = 0;
};

/////////////////////////////////////////////////////////////////////////////////

class IObLogDmlParser;
class IObLogErrHandler;
class PartTransTask;
class IObLogSysLsTaskHandler;
class IObLogCommitter;
template <typename T> class ObLogTransTaskPool;
typedef ObLogTransTaskPool<PartTransTask> TaskPool;
class IObLogEntryTaskPool;

class ObLogFetcher : public IObLogFetcher
{
  static const int64_t MISC_THREAD_SLEEP_TIME = 1 * _SEC_;
  static const int64_t PRINT_K_SLOWEST_PARTITION = 10 * _SEC_;
  static const int64_t PRINT_CLUSTER_ID_IGNORE_TPS_INTERVAL = 10 * _SEC_;
  static const int64_t PRINT_HEARTBEAT_INTERVAL = 10 * _SEC_;

  static bool g_print_ls_heartbeat_info;
  static int64_t g_inner_heartbeat_interval;

public:
  ObLogFetcher();
  virtual ~ObLogFetcher();

  int init(
      IObLogDmlParser *dml_parser,
      IObLogSysLsTaskHandler *sys_ls_handler,
      IObLogErrHandler *err_handler,
      ObLogSysTableHelper &systable_helper,
      TaskPool *task_pool,
      IObLogEntryTaskPool *log_entry_task_pool,
      IObLogCommitter *committer,
      ObISQLClient *proxy,
      const int64_t cluster_id,
      const common::ObRegion &prefer_region,
      const ObLogConfig &cfg,
      const int64_t start_seq);

  void destroy();

public:
  virtual int start();
  virtual void stop();
  virtual void pause();
  virtual void resume();
  virtual bool is_paused();
  virtual void mark_stop_flag();
  virtual void configure(const ObLogConfig &cfg);

  virtual int add_ls(const TenantLSID &tls_id,
      const int64_t start_tstamp_ns,
      const palf::LSN &start_lsn);

  virtual int recycle_ls(const TenantLSID &tls_id);

  virtual int64_t get_part_trans_task_count() const
  { return ATOMIC_LOAD(&PartTransDispatcher::g_part_trans_task_count); }

  virtual int set_start_global_trans_version(const int64_t start_global_trans_version);

  virtual int get_fs_container_mgr(IObFsContainerMgr *&fs_container_mgr);

  virtual int get_log_route_service(logservice::ObLogRouteService *&log_route_service);

private:
  static void *misc_thread_func_(void *);
  void run_misc_thread();
  static void *heartbeat_dispatch_thread_func_(void *);
  void heartbeat_dispatch_routine();
  void print_fetcher_stat_();

  int next_heartbeat_timestamp_(int64_t &hb_ts, const int64_t last_hb_ts);

private:
  struct LSProgressInfo
  {
    LSProgressInfo() : tls_id_(), progress_(0) {}
    LSProgressInfo(const TenantLSID &tls_id, const int64_t progress) : tls_id_(tls_id), progress_(progress) {}

    TenantLSID tls_id_;
    int64_t progress_;
    TO_STRING_KV(K_(tls_id), K_(progress));
  };
  // Used to diagnosis and monitoring
  typedef common::ObSEArray<LSProgressInfo, 16> LSProgressInfoArray;

  struct FetchCtxMapHBFunc
  {
    FetchCtxMapHBFunc();
    bool operator()(const TenantLSID &tls_id, LSFetchCtx *&ctx);

    int64_t                 data_progress_;
    int64_t                 ddl_progress_;
    palf::LSN               ddl_last_dispatch_log_lsn_;
    int64_t                 min_progress_;
    int64_t                 max_progress_;
    TenantLSID              min_progress_ls_;
    TenantLSID              max_progress_ls_;
    int64_t                 part_count_;
    LSProgressInfoArray     ls_progress_infos_;

    TO_STRING_KV(K_(data_progress),
        K_(ddl_progress),
        K_(ddl_last_dispatch_log_lsn),
        K_(min_progress),
        K_(max_progress),
        K_(min_progress_ls),
        K_(max_progress_ls),
        K_(part_count));
  };

private:
  bool                          is_inited_;
  TaskPool                      *task_pool_;
  IObLogSysLsTaskHandler        *sys_ls_handler_;
  IObLogErrHandler              *err_handler_;

  int64_t                       cluster_id_;
  // Manager
  ObLogPartTransResolverFactory part_trans_resolver_factory_;
  ObLogLSFetchMgr               ls_fetch_mgr_;                  // Fetch Log Task Manager
  PartProgressController        progress_controller_;           // Process Controller

  // Function Modules
  ObLogRpc                      rpc_;
  logservice::ObLogRouteService log_route_service_;
  ObLogStartLSNLocator          start_lsn_locator_;
  ObLogFetcherIdlePool          idle_pool_;
  ObLogFetcherDeadPool          dead_pool_;
  ObLSWorker                    stream_worker_;
  ObFsContainerMgr              fs_container_mgr_;
  ObLogFetcherDispatcher        dispatcher_;
  ObLogClusterIDFilter          cluster_id_filter_;

  pthread_t                     misc_tid_;                // Fetcher misc thread
  pthread_t                     heartbeat_dispatch_tid_;  // Dispatch heartbeat thread
  int64_t                       last_timestamp_;          // Record heartbeat timestamp
  volatile bool                 stop_flag_ CACHE_ALIGNED;

  // stop flag
  bool                          paused_ CACHE_ALIGNED;
  int64_t                       pause_time_ CACHE_ALIGNED;
  int64_t                       resume_time_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFetcher);
};

}
}


#endif
