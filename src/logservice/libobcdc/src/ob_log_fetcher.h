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
#include "ob_log_fetching_mode.h"

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
  virtual int add_ls(
      const logservice::TenantLSID &tls_id,
      const logfetcher::ObLogFetcherStartParameters &start_parameters) = 0;

  // Recycle LS
  virtual int recycle_ls(const logservice::TenantLSID &tls_id) = 0;

  // Remove LS
  virtual int remove_ls(const logservice::TenantLSID &tls_id) = 0;

  virtual int wait_for_all_ls_to_be_removed(const int64_t timeout) = 0;

  virtual int64_t get_part_trans_task_count() const = 0;

  virtual int set_start_global_trans_version(const int64_t start_global_trans_version) = 0;

  virtual int get_fs_container_mgr(IObFsContainerMgr *&fs_container_mgr) = 0;

  virtual int get_log_route_service(logservice::ObLogRouteService *&log_route_service) = 0;

  virtual int get_large_buffer_pool(archive::LargeBufferPool *&large_buffer_pool) = 0;

  virtual int get_log_ext_handler(logservice::ObLogExternalStorageHandler *&log_ext_handler) = 0;

  // Checks if the sys progress of specified tenant exceeds the timestamp
  // For LogMetaDataService:
  // 1. At startup time, it need to build the baseline data for the startup timestamp,
  //    so we need to ensure that we have fetched all logs.
  // 2. We can use the check_progress function to determine that the log is complete,
  //    including the data dictionary baseline data and the incremental data that needs to be replayed.
  //
  // @param  [in]   tenant_id    Tenant ID
  // @param  [in]   timestamp    The timestamp which you want to  check
  // @param  [out]  is_exceeded  Returns whether it was exceeded
  // @param  [out]  cur_progress Returns current progress
  //
  // @retval OB_SUCCESS          success
  // @retval Other error codes   Failed
  virtual int check_progress(
      const uint64_t tenant_id,
      const int64_t timestamp,
      bool &is_exceeded,
      int64_t &cur_progress) = 0;
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
      const bool is_loading_data_dict_baseline_data,
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

  virtual int add_ls(
      const logservice::TenantLSID &tls_id,
      const logfetcher::ObLogFetcherStartParameters &start_parameters);

  virtual int recycle_ls(const logservice::TenantLSID &tls_id);

  virtual int remove_ls(const logservice::TenantLSID &tls_id);

  virtual int wait_for_all_ls_to_be_removed(const int64_t timeout);

  virtual int64_t get_part_trans_task_count() const
  { return ATOMIC_LOAD(&PartTransDispatcher::g_part_trans_task_count); }

  virtual int set_start_global_trans_version(const int64_t start_global_trans_version);

  virtual int get_fs_container_mgr(IObFsContainerMgr *&fs_container_mgr);

  virtual int get_log_route_service(logservice::ObLogRouteService *&log_route_service);

  virtual int get_large_buffer_pool(archive::LargeBufferPool *&large_buffer_pool);

  virtual int get_log_ext_handler(logservice::ObLogExternalStorageHandler *&log_ext_handler);

  virtual int check_progress(
      const uint64_t tenant_id,
      const int64_t timestamp,
      bool &is_exceeded,
      int64_t &cur_progress);

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
    LSProgressInfo(const logservice::TenantLSID &tls_id, const int64_t progress) : tls_id_(tls_id), progress_(progress) {}

    logservice::TenantLSID tls_id_;
    int64_t progress_;
    TO_STRING_KV(K_(tls_id), K_(progress));
  };
  // Used to diagnosis and monitoring
  typedef common::ObSEArray<LSProgressInfo, 16> LSProgressInfoArray;

  struct FetchCtxMapHBFunc
  {
    FetchCtxMapHBFunc();
    bool operator()(const logservice::TenantLSID &tls_id, LSFetchCtx *&ctx);

    int64_t                 data_progress_;
    int64_t                 ddl_progress_;
    palf::LSN               ddl_last_dispatch_log_lsn_;
    int64_t                 min_progress_;
    int64_t                 max_progress_;
    logservice::TenantLSID  min_progress_ls_;
    logservice::TenantLSID  max_progress_ls_;
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

  class LogFetcherErrHandler : public logfetcher::IObLogErrHandler
  {
    public:
      LogFetcherErrHandler();
      virtual ~LogFetcherErrHandler() {}
    public:
      virtual void handle_error(const int err_no, const char *fmt, ...) override {}
      virtual void handle_error(const share::ObLSID &ls_id,
          const ErrType &err_type,
          share::ObTaskId &trace_id,
          const palf::LSN &lsn,
          const int err_no,
          const char *fmt, ...) override {}
  };

private:
  bool                          is_inited_;
  bool                          is_running_;
  bool                          is_loading_data_dict_baseline_data_;
  ClientFetchingMode            fetching_mode_;
  ObBackupPathString            archive_dest_;
  archive::LargeBufferPool      large_buffer_pool_;
  int64_t                       log_ext_handler_concurrency_;
  logservice::ObLogExternalStorageHandler log_ext_handler_;
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
  IObLogFetcherDispatcher       *dispatcher_;
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
