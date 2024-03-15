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

#ifndef OCEANBASE_LOG_FETCHER_LOG_FETCHER_H_
#define OCEANBASE_LOG_FETCHER_LOG_FETCHER_H_

#include "share/scn.h"                          // SCN
#include "lib/compress/ob_compress_util.h"      // ObCompressorType
#include "ob_log_ls_fetch_mgr.h"                // ObLogLSFetchMgr
#include "ob_log_fetch_stream_container_mgr.h"  // ObFsContainerMgr
#include "ob_log_rpc.h"                         // ObLogRpc
#include "ob_log_part_progress_controller.h"    // PartProgressController
#include "ob_log_start_lsn_locator.h"           // ObLogStartLSNLocator
#include "ob_log_fetcher_idle_pool.h"           // ObLogFetcherIdlePool
#include "ob_log_fetcher_dead_pool.h"           // ObLogFetcherDeadPool
#include "ob_ls_worker.h"                       // ObLSWorker
#include "ob_log_utils.h"                       // _SEC_
#include "ob_log_fetcher_ls_ctx_factory.h"      // ObILogFetcherLSCtxFactory
#include "ob_log_fetcher_ls_ctx_additional_info_factory.h" // ObILogFetcherLSCtxAddInfoFactory
#include "ob_log_handler.h"  // ILogFetcherHandler
#include "logservice/logrouteservice/ob_log_route_service.h" // ObLogRouteService
#include "ob_log_fetcher_user.h"  // LogFetcherUser
#include "ob_log_fetching_mode.h"
#include "ob_log_progress_info.h"               // ProgressInfo
#include "ob_log_fetcher_start_parameters.h"    // ObLogFetcherStartParameters
#include "ob_log_fetcher_err_handler.h"         // IObLogErrHandler
#include "logservice/ob_log_external_storage_handler.h" // ObLogExternalStorageHandler

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace logfetcher
{
class ObLogFetcherConfig;

class IObLogFetcher
{
public:
  virtual ~IObLogFetcher() {}

  // LogFetcher start interface
  virtual int start() = 0;
  // LogFetcher stop interface
  virtual void stop() = 0;
  // LogFetcher pause interface: pause fetching log for all the log streams
  virtual void pause() = 0;
  // LogFetcher resume interface: resume fetching log for all the log streams
  virtual void resume() = 0;
  // Getting the state
  virtual bool is_paused() = 0;
  virtual void mark_stop_flag() = 0;

  // Update LogFetcher configure
  virtual void configure(const ObLogFetcherConfig &cfg) = 0;

  // Get Cluster ID
  virtual int64_t get_cluster_id() const = 0;

  // Get Tenant ID
  virtual uint64_t get_source_tenant_id() const = 0;

  // Update assign region to fetch log
  virtual int update_preferred_upstream_log_region(const common::ObRegion &region) = 0;

  // Get assign region
  virtual int get_preferred_upstream_log_region(common::ObRegion &region) = 0;

  // Add the log stream
  //
  // @param  [in]   ls_id        LS ID
  // @param  [in]   start_parameters  start parameters of the LS
  //
  // @retval OB_SUCCESS          success
  // @retval Other error codes   Failed
  virtual int add_ls(
      const share::ObLSID &ls_id,
      const ObLogFetcherStartParameters &start_parameters) = 0;

  // Recycle the log stream, mark the log stream for deletion
  //
  // @param  [in]   ls_id        LS ID
  //
  // @retval OB_SUCCESS          success
  // @retval Other error codes   Failed
  virtual int recycle_ls(const share::ObLSID &ls_id) = 0;

  // Remove LS
  // When the consumer needs to delete the log stream, it needs to call the interface.
  // remove_ls(...) is a synchronous interface that waits for all resources of the log stream to be reclaimed
  // and then physically removed it from the LogFetcher
  //
  // @param  [in]   ls_id        LS ID
  //
  // @retval OB_SUCCESS          success
  // @retval Other error codes   Failed
  virtual int remove_ls(const share::ObLSID &ls_id) = 0;

  // Get the all LS info
  //
  // @param  [out]   ls_ids      LS ID array
  //
  // @retval OB_SUCCESS          success
  // @retval Other error codes   Failed
  virtual int get_all_ls(ObIArray<share::ObLSID> &ls_ids) = 0;

  // Update fetching log upper limit, used for log streams "simultaneous pull the log strategy"
  //
  // @param  [in]   upper_limit_scn      fetching upper limit
  //
  // @retval OB_SUCCESS          success
  // @retval Other error codes   Failed
  virtual int update_fetching_log_upper_limit(const share::SCN &upper_limit_scn) = 0;

  virtual int update_compressor_type(const common::ObCompressorType &compressor_type) = 0;

  virtual int get_progress_info(ProgressInfo &progress_info) = 0;

  virtual int wait_for_all_ls_to_be_removed(const int64_t timeout) = 0;

  virtual int get_fs_container_mgr(IObFsContainerMgr *&fs_container_mgr) = 0;

  virtual int get_log_route_service(logservice::ObLogRouteService *&log_route_service) = 0;

  virtual int get_large_buffer_pool(archive::LargeBufferPool *&large_buffer_pool) = 0;

  virtual int get_log_ext_handler(logservice::ObLogExternalStorageHandler *&log_ext_handler) = 0;

  virtual int get_fetcher_config(const ObLogFetcherConfig *&cfg) = 0;

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

  // Print the monitoring stat info periodically
  virtual void print_stat() = 0;
  //allocate memory for log decompression
  virtual void *alloc_decompression_buf(int64_t size) = 0;
  virtual common::ObIAllocator *get_decompression_allocator() = 0;
  //free memory for log decompression
  virtual void free_decompression_buf(void *buf)  = 0;
};

/////////////////////////////////////////////////////////////////////////////////

class ObLogFetcher : public IObLogFetcher
{
  static const int64_t PRINT_K_SLOWEST_LS = 10 * _SEC_;
  static const int64_t PRINT_HEARTBEAT_INTERVAL = 10 * _SEC_;

  static bool g_print_ls_heartbeat_info;

public:
  ObLogFetcher();
  virtual ~ObLogFetcher();

  // ObLogFetcher init function
  //
  // @param  [in]   cluster_id    Cluster id of the source cluster
  // @param  [in]   tenant_id     Tenant id of the source cluster
  // @param  [in]   is_loading_data_dict_baseline_data  Set it to false, except for data dictionary
  // @param  [in]   fetching_mode Set it to INTEGRATED_MODE or DIRECT_MODE
  // @param  [in]   archive_dest  Archive dest info for DIRECT_MODE, set it to empty for INTEGRATED_MODE
  // @param  [in]   cfg           Configuration
  // @param  [in]   proxy         SQL Client
  // @param  [in]   error_handler Interface for error handler
  //
  // @retval OB_SUCCESS          success
  // @retval Other error codes   Failed
  int init(
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
      IObLogErrHandler *err_handler);
  void destroy();
  virtual int start();
  virtual void stop();
  virtual void pause();
  virtual void resume();
  virtual bool is_paused();
  virtual void mark_stop_flag();

public:
  virtual void configure(const ObLogFetcherConfig &cfg);

  virtual int64_t get_cluster_id() const { return cluster_id_; }
  virtual uint64_t get_source_tenant_id() const { return source_tenant_id_; }

  virtual int update_preferred_upstream_log_region(const common::ObRegion &region);
  virtual int get_preferred_upstream_log_region(common::ObRegion &region);

  virtual int add_ls(
      const share::ObLSID &ls_id,
      const ObLogFetcherStartParameters &start_parameters);

  virtual int remove_ls(const share::ObLSID &ls_id);

  virtual int recycle_ls(const share::ObLSID &ls_id);

  virtual int remove_ls_physically(const share::ObLSID &ls_id);

  virtual int get_all_ls(ObIArray<share::ObLSID> &ls_ids);

  bool is_ls_exist(const share::ObLSID &id) const;

  int get_ls_proposal_id(const share::ObLSID &id, int64_t &proposal_id);

  virtual int update_fetching_log_upper_limit(const share::SCN &upper_limit_scn);

  virtual int update_compressor_type(const common::ObCompressorType &compressor_type);

  virtual int get_progress_info(ProgressInfo &progress_info);

  virtual int wait_for_all_ls_to_be_removed(const int64_t timeout);

  virtual int get_fs_container_mgr(IObFsContainerMgr *&fs_container_mgr);

  virtual int get_log_route_service(logservice::ObLogRouteService *&log_route_service);

  virtual int get_large_buffer_pool(archive::LargeBufferPool *&large_buffer_pool);

  virtual int get_log_ext_handler(logservice::ObLogExternalStorageHandler *&log_ext_handler);

  virtual int get_fetcher_config(const ObLogFetcherConfig *&cfg);

  virtual int check_progress(
      const uint64_t tenant_id,
      const int64_t timestamp,
      bool &is_exceeded,
      int64_t &cur_progress);

  virtual void print_stat();
  void *alloc_decompression_buf(int64_t size);
  common::ObIAllocator *get_decompression_allocator() {return &decompression_alloc_;}
  void free_decompression_buf(void *buf);

private:
  int suggest_cached_rpc_res_count_(const int64_t min_res_cnt,
      const int64_t max_res_cnt);
  int init_self_addr_();
  void print_fetcher_stat_();
  int print_delay();

  int wait_for_ls_to_be_removed_(const share::ObLSID &ls_id,
      const int64_t timeout);

private:
  struct FetchCtxMapLSGetter
  {
    FetchCtxMapLSGetter(ObIArray<share::ObLSID> &ls_ids) : ls_ids_(ls_ids) {}
    bool operator()(const logservice::TenantLSID &tls_id, LSFetchCtx *&ctx);

    ObIArray<share::ObLSID> &ls_ids_;
  };
  const int64_t DECOMPRESSION_MEM_LIMIT_THRESHOLD = 512 * 1024 * 1024L;
  const int64_t DECOMPRSSION_BUF_ALLOC_NWAY = 4;
private:
  bool                                     is_inited_;
  LogFetcherUser                           log_fetcher_user_;
  int64_t                                  cluster_id_;
  uint64_t                                 source_tenant_id_;
  uint64_t                                 self_tenant_id_;
  const ObLogFetcherConfig                 *cfg_;

  bool                                     is_loading_data_dict_baseline_data_;
  ClientFetchingMode                       fetching_mode_;
  ObBackupPathString                       archive_dest_;
  archive::LargeBufferPool                 large_buffer_pool_;
  int64_t                                  log_ext_handler_concurrency_;
  logservice::ObLogExternalStorageHandler  log_ext_handler_;
  ObILogFetcherLSCtxAddInfoFactory         *ls_ctx_add_info_factory_;
  IObLogErrHandler                         *err_handler_;

  ObLogLSFetchMgr                          ls_fetch_mgr_;                  // Fetch Log Task Manager
  PartProgressController                   progress_controller_;           // Process Controller

  // Function Modules
  ObLogRpc                                 rpc_;
  logservice::ObLogRouteService            log_route_service_;
  ObLogStartLSNLocator                     start_lsn_locator_;
  ObLogFetcherIdlePool                     idle_pool_;
  ObLogFetcherDeadPool                     dead_pool_;
  ObLSWorker                               stream_worker_;
  // TODO
  ObFsContainerMgr                         fs_container_mgr_;

  volatile bool                            stop_flag_ CACHE_ALIGNED;

  // stop flag
  bool                                     paused_ CACHE_ALIGNED;
  int64_t                                  pause_time_ CACHE_ALIGNED;
  int64_t                                  resume_time_ CACHE_ALIGNED;
  ObBlockAllocMgr decompression_blk_mgr_;
  ObVSliceAlloc decompression_alloc_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFetcher);
};

}
}


#endif
