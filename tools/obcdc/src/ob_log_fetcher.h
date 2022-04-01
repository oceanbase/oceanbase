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

#ifndef OCEANBASE_LIBOBLOG_LOG_FETCHER_H_
#define OCEANBASE_LIBOBLOG_LOG_FETCHER_H_

#include "common/ob_partition_key.h"                    // ObPartitionKey

#include "ob_log_part_mgr.h"                    // PartAddCallback, PartRecycleCallback
#include "ob_log_part_fetch_mgr.h"              // ObLogPartFetchMgr
#include "ob_log_rpc.h"                         // ObLogRpc
#include "ob_log_part_progress_controller.h"    // PartProgressController
#include "ob_log_all_svr_cache.h"               // ObLogAllSvrCache
#include "ob_log_svr_finder.h"                  // ObLogSvrFinder
#include "ob_log_fetcher_heartbeat_worker.h"    // ObLogFetcherHeartbeatWorker
#include "ob_log_start_log_id_locator.h"        // ObLogStartLogIdLocator
#include "ob_log_fetcher_idle_pool.h"           // ObLogFetcherIdlePool
#include "ob_log_fetcher_dead_pool.h"           // ObLogFetcherDeadPool
#include "ob_log_stream_worker.h"               // ObLogStreamWorker
#include "ob_log_utils.h"                       // _SEC_
#include "ob_log_cluster_id_filter.h"           // ObLogClusterIDFilter
#include "ob_log_part_trans_resolver_factory.h" // ObLogPartTransResolverFactory
#include "ob_log_fetcher_dispatcher.h"          // ObLogFetcherDispatcher

namespace oceanbase
{
namespace liboblog
{

class ObLogConfig;
class IObLogFetcher : public PartAddCallback, public PartRecycleCallback
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

  // Add partition
  virtual int add_partition(const common::ObPartitionKey &pkey,
      const int64_t start_tstamp,
      const uint64_t start_log_id) = 0;

  // Recycling partition
  virtual int recycle_partition(const common::ObPartitionKey &pkey) = 0;

  virtual int64_t get_part_trans_task_count() const = 0;

  virtual int set_start_global_trans_version(const int64_t start_global_trans_version) = 0;
};

/////////////////////////////////////////////////////////////////////////////////

class IObLogDmlParser;
class IObLogErrHandler;
class PartTransTask;
class IObLogDDLHandler;
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
  static const int64_t TRANS_ABORT_INFO_GC_INTERVAL = 10 * _SEC_;

  static bool g_print_partition_heartbeat_info;
  static int64_t g_inner_heartbeat_interval;

public:
  ObLogFetcher();
  virtual ~ObLogFetcher();

  int init(IObLogDmlParser *dml_parser,
      IObLogDDLHandler *ddl_handler,
      IObLogErrHandler *err_handler,
      ObLogSysTableHelper &systable_helper,
      TaskPool *task_pool,
      IObLogEntryTaskPool *log_entry_task_pool,
      IObLogCommitter *committer,
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

  virtual int add_partition(const common::ObPartitionKey &pkey,
      const int64_t start_tstamp,
      const uint64_t start_log_id);

  virtual int recycle_partition(const common::ObPartitionKey &pkey);

  virtual int64_t get_part_trans_task_count() const
  { return ATOMIC_LOAD(&PartTransDispatcher::g_part_trans_task_count); }

  virtual int set_start_global_trans_version(const int64_t start_global_trans_version);

  virtual void configure(const ObLogConfig &cfg);

private:
  static void *misc_thread_func_(void *);
  void run_misc_thread();
  static void *heartbeat_dispatch_thread_func_(void *);
  void heartbeat_dispatch_routine();
  void print_fetcher_stat_();

  int next_heartbeat_timestamp_(int64_t &hb_ts, const int64_t last_hb_ts);

private:
  struct FetchCtxMapHBFunc
  {
    FetchCtxMapHBFunc();
    bool operator()(const common::ObPartitionKey &pkey, PartFetchCtx *&ctx);

    int64_t                 data_progress_;
    int64_t                 ddl_progress_;
    int64_t                 ddl_last_dispatch_log_id_;
    int64_t                 min_progress_;
    int64_t                 max_progress_;
    common::ObPartitionKey  min_progress_pkey_;
    common::ObPartitionKey  max_progress_pkey_;
    int64_t                 part_count_;

    TO_STRING_KV(K_(data_progress), K_(ddl_progress), K_(ddl_last_dispatch_log_id),
        K_(min_progress),
        K_(max_progress),
        K_(min_progress_pkey),
        K_(max_progress_pkey),
        K_(part_count));
  };

private:
  bool                          inited_;
  TaskPool                      *task_pool_;
  IObLogDDLHandler              *ddl_handler_;
  IObLogErrHandler              *err_handler_;

  // Manager
  ObLogPartTransResolverFactory part_trans_resolver_factory_;
  ObLogPartFetchMgr             part_fetch_mgr_;                // Fetch Log Task Manager
  PartProgressController        progress_controller_;           // Process Controller

  // Function Modules
  ObLogRpc                      rpc_;
  ObLogAllSvrCache              all_svr_cache_;
  ObLogSvrFinder                svr_finder_;
  ObLogFetcherHeartbeatWorker   heartbeater_;
  ObLogStartLogIdLocator        start_log_id_locator_;
  ObLogFetcherIdlePool          idle_pool_;
  ObLogFetcherDeadPool          dead_pool_;
  ObLogStreamWorker             stream_worker_;
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
