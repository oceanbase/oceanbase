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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_STREAM_WORKER_H__
#define OCEANBASE_LIBOBLOG_OB_LOG_STREAM_WORKER_H__

#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap
#include "lib/net/ob_addr.h"                    // ObAddr
#include "lib/objectpool/ob_small_obj_pool.h"   // ObSmallObjPool
#include "lib/lock/ob_spin_lock.h"              // ObSpinLock

#include "ob_log_config.h"                      // ObLogConfig
#include "ob_log_svr_stream.h"                  // SvrStream
#include "ob_map_queue_thread.h"                // ObMapQueueThread
#include "ob_log_timer.h"                       // ObLogFixedTimer
#include "ob_log_fetch_stream_pool.h"           // FetchStreamPool
#include "ob_log_fetch_log_rpc.h"               // FetchLogARpcResultPool

namespace oceanbase
{
namespace liboblog
{

class PartFetchCtx;
class IObLogStreamWorker
{
public:
  static const int64_t MAX_THREAD_NUM = ObLogConfig::max_stream_worker_thread_num;
public:
  virtual ~IObLogStreamWorker() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void pause() = 0;
  virtual void resume(int64_t fetcher_resume_tstamp) = 0;
  virtual void mark_stop_flag() = 0;
  virtual int64_t get_fetcher_resume_tstamp() = 0;

  // Assigning partitioning tasks to a particular fetch log stream
  virtual int dispatch_fetch_task(PartFetchCtx &task, const char *dispatch_reason) = 0;

  // Putting the fetch log stream task into the work thread task pool
  virtual int dispatch_stream_task(FetchStream &task, const char *from_mod) = 0;

  // Hibernate fetch log stream task
  virtual int hibernate_stream_task(FetchStream &task, const char *from_mod) = 0;
};

//////////////////////////////////////////// ObLogStreamWorker ////////////////////////////////////////////

class IObLogRpc;
class IObLogFetcherIdlePool;
class IObLogFetcherDeadPool;
class IObLogSvrFinder;
class IObLogErrHandler;
class IObLogAllSvrCache;
class IObLogFetcherHeartbeatWorker;
class PartProgressController;

typedef common::ObMapQueueThread<IObLogStreamWorker::MAX_THREAD_NUM> StreamWorkerThread;

class ObLogStreamWorker : public IObLogStreamWorker, public StreamWorkerThread
{
  typedef common::ObLinearHashMap<common::ObAddr, SvrStream*> SvrStreamMap;
  typedef common::ObSmallObjPool<SvrStream> SvrStreamPool;

  // block size of SvrStreamPool
  static const int64_t SVR_STREAM_POOL_BLOCK_SIZE = 1 << 22;
  static const int64_t STAT_INTERVAL = 5 * _SEC_;

  // Class global variables
public:
  // Hibernate time of the stream in case of fetch stream is paused
  static int64_t g_blacklist_survival_time;
  static bool g_print_stream_dispatch_info;

public:
  ObLogStreamWorker();
  virtual ~ObLogStreamWorker();

public:
  int init(const int64_t worker_thread_num,
      const int64_t svr_stream_cached_count,
      const int64_t fetch_stream_cached_count,
      const int64_t rpc_result_cached_count,
      const int64_t max_timer_task_count,
      IObLogRpc &rpc,
      IObLogFetcherIdlePool &idle_pool,
      IObLogFetcherDeadPool &dead_pool,
      IObLogSvrFinder &svr_finder,
      IObLogErrHandler &err_handler,
      IObLogAllSvrCache &all_svr_cache,
      IObLogFetcherHeartbeatWorker &heartbeater,
      PartProgressController &progress_controller);
  void destroy();

public:
  int start();
  void stop();
  void pause();
  void resume(int64_t fetcher_resume_tstamp);
  void mark_stop_flag();
  int64_t get_fetcher_resume_tstamp();

  int dispatch_fetch_task(PartFetchCtx &task, const char *dispatch_reason);
  int dispatch_stream_task(FetchStream &task, const char *from_mod);
  int hibernate_stream_task(FetchStream &task, const char *from_mod);

public:
  // Overloading thread handling functions
  virtual int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);

public:
  static void configure(const ObLogConfig & config);

private:
  bool is_svr_avail_(IObLogAllSvrCache &all_svr_cache, const common::ObAddr &svr);
  int dispatch_fetch_task_to_svr_(PartFetchCtx &task, const common::ObAddr &svr);
  int get_svr_stream_(const common::ObAddr &svr, SvrStream *&svr_stream);
  int get_svr_stream_when_not_exist_(const common::ObAddr &svr, SvrStream *&svr_stream);
  int free_svr_stream_(SvrStream *svr_stream);
  void free_all_svr_stream_();
  void print_stat_();

private:
  struct SvrStreamFreeFunc
  {
    SvrStreamPool &pool_;

    explicit SvrStreamFreeFunc(SvrStreamPool &pool) : pool_(pool) {}
    bool operator() (const common::ObAddr &key, SvrStream* value);
  };

  struct SvrStreamStatFunc
  {
    bool operator() (const common::ObAddr &key, SvrStream* value)
    {
      UNUSED(key);
      if (NULL != value) {
        value->do_stat();
      }
      return true;
    }
  };

  // private members
private:
  bool                          inited_;

  // Is the stream task suspended
  bool                          stream_paused_ CACHE_ALIGNED;
  // record time of fetcher resume
  int64_t                       fetcher_resume_time_ CACHE_ALIGNED;

  // External modules
  IObLogRpc                     *rpc_;                    // RPC handler
  IObLogFetcherIdlePool         *idle_pool_;              // IDLE POOl
  IObLogFetcherDeadPool         *dead_pool_;              // DEAD POOL
  IObLogSvrFinder               *svr_finder_;             // SvrFinder
  IObLogErrHandler              *err_handler_;            // error handler
  IObLogAllSvrCache             *all_svr_cache_;          // server cache
  IObLogFetcherHeartbeatWorker  *heartbeater_;            // heatbeat mgr/worker
  PartProgressController        *progress_controller_;    // progress controller

  // private module
  ObLogFixedTimer               timer_;                   // timer
  FetchStreamPool               fs_pool_;                 // FetchStream object pool
  FetchLogARpcResultPool        rpc_result_pool_;         // RPC resujt object pool

  // SvrStream manager struct
  // TODO: Support for recycling of useless SvrStream
  SvrStreamMap                  svr_stream_map_;
  SvrStreamPool                 svr_stream_pool_;         // Supports multi-threaded alloc/release
  common::ObSpinLock            svr_stream_alloc_lock_;   // SvrStream alloctor lock

  /// Fetch log stream task processing serial number for rotating the assignment of fetch log stream tasks
  int64_t                       stream_task_seq_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStreamWorker);
};

}
}
#endif
