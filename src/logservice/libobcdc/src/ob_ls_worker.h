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
 * Streamn Worker
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LS_WORKER_H__
#define OCEANBASE_LIBOBCDC_OB_LS_WORKER_H__

#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap
#include "lib/net/ob_addr.h"                    // ObAddr
#include "lib/lock/ob_spin_lock.h"              // ObSpinLock

#include "ob_log_config.h"                      // ObLogConfig
#include "ob_map_queue_thread.h"                // ObMapQueueThread
#include "ob_log_timer.h"                       // ObLogFixedTimer
#include "ob_log_ls_fetch_stream.h"             // FetchStream

namespace oceanbase
{
namespace libobcdc
{

class LSFetchCtx;
class IObLSWorker
{
public:
  static const int64_t MAX_THREAD_NUM = ObLogConfig::max_stream_worker_thread_num;
public:
  virtual ~IObLSWorker() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void pause() = 0;
  virtual void resume(int64_t fetcher_resume_tstamp) = 0;
  virtual void mark_stop_flag() = 0;
  virtual int64_t get_fetcher_resume_tstamp() = 0;

  // Assigning partitioning tasks to a particular fetch log stream
  virtual int dispatch_fetch_task(LSFetchCtx &task, const char *dispatch_reason) = 0;

  // Putting the fetch log stream task into the work thread task pool
  virtual int dispatch_stream_task(FetchStream &task, const char *from_mod) = 0;

  // Hibernate fetch log stream task
  virtual int hibernate_stream_task(FetchStream &task, const char *from_mod) = 0;
};

//////////////////////////////////////////// ObLSWorker ////////////////////////////////////////////

class IObLogFetcherIdlePool;
class IObLogFetcherDeadPool;
class IObLogSvrFinder;
class IObLogErrHandler;

typedef common::ObMapQueueThread<IObLSWorker::MAX_THREAD_NUM> StreamWorkerThread;

class ObLSWorker : public IObLSWorker, public StreamWorkerThread
{
  static const int64_t STAT_INTERVAL = 5 * _SEC_;

  // Class global variables
public:
  // Hibernate time of the stream in case of fetch stream is paused
  static int64_t g_blacklist_survival_time;
  static bool g_print_stream_dispatch_info;

public:
  ObLSWorker();
  virtual ~ObLSWorker();

public:
  int init(const int64_t worker_thread_num,
      const int64_t max_timer_task_count,
      void *fetcher_host,
      IObLogFetcherIdlePool &idle_pool,
      IObLogFetcherDeadPool &dead_pool,
      IObLogErrHandler &err_handler);
  void destroy();

public:
  int start();
  void stop();
  void pause();
  void resume(int64_t fetcher_resume_tstamp);
  void mark_stop_flag();
  int64_t get_fetcher_resume_tstamp();

  int dispatch_fetch_task(LSFetchCtx &task, const char *dispatch_reason);
  int dispatch_stream_task(FetchStream &task, const char *from_mod);
  int hibernate_stream_task(FetchStream &task, const char *from_mod);

public:
  // Overloading thread handling functions
  virtual int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);

public:
  static void configure(const ObLogConfig & config);

private:
  // bool is_svr_avail_(IObLogAllSvrCache &all_svr_cache, const common::ObAddr &request_svr);
  int dispatch_fetch_task_to_svr_(LSFetchCtx &task, const common::ObAddr &request_svr);

  void print_stat_();

private:
  // private members
private:
  bool                          inited_;

  // Is the stream task suspended
  bool                          stream_paused_ CACHE_ALIGNED;
  // record time of fetcher resume
  int64_t                       fetcher_resume_time_ CACHE_ALIGNED;

  void                          *fetcher_host_;
  // External modules
  IObLogFetcherIdlePool         *idle_pool_;              // IDLE POOl
  IObLogFetcherDeadPool         *dead_pool_;              // DEAD POOL
  IObLogErrHandler              *err_handler_;            // error handler

  // private module
  ObLogFixedTimer               timer_;                   // timer

  /// Fetch log stream task processing serial number for rotating the assignment of fetch log stream tasks
  int64_t                       stream_task_seq_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLSWorker);
};

}
}
#endif
