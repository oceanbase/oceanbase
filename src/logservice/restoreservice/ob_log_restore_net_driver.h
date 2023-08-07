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
 */

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_NET_DRIVER_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_NET_DRIVER_H_

#include "lib/container/ob_iarray.h"     // Array
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/compress/ob_compress_util.h" // ObCompressorType
#include "common/ob_region.h"  // ObRegion
#include "logservice/logfetcher/ob_log_fetcher_ls_ctx_additional_info_factory.h"
#include "logservice/logfetcher/ob_log_fetcher_err_handler.h"
#include "logservice/logfetcher/ob_log_fetcher_ls_ctx_default_factory.h"
#include "share/backup/ob_log_restore_struct.h"   // ObRestoreSourceServiceAttr
#include "share/ob_log_restore_proxy.h"   // ObLogRestoreProxyUtil
#include "ob_log_restore_driver_base.h"
#include "ob_restore_log_function.h"  // ObRestoreLogFunction
#include "storage/ls/ob_ls.h"         // ObLS
namespace oceanbase
{
namespace share
{
struct ObLSID;
struct SCN;
}

namespace storage
{
class ObLSService;
}

namespace logfetcher
{
class ObLogFetcher;
}

namespace logservice
{
class ObLogService;
// The driver for standby based on net service, and its functions includes:
// 1. fetcher and proxy management;
// 2. schedule ls, add ls leader to log fetcher and remove it if it changes to follower
// 3. scan_ls, if the log restore source is not service, stop fetcher
// 4. set the max restore upper limit
class ObLogRestoreNetDriver : public ObLogRestoreDriverBase
{
public:
  ObLogRestoreNetDriver();
  ~ObLogRestoreNetDriver();
public:
  int init(const uint64_t tenant_id,
      storage::ObLSService *ls_svr,
      ObLogService *log_service);
  void destroy();
  int start();
  void stop();
  void wait();

  // Schedule all ls leader to do restore
  // Init Log Fetcher and Add LS into Log Fetcher
  int do_schedule(share::ObRestoreSourceServiceAttr &source);

  // Remove LS if it is not leader or log_restore_source is not service
  // Destroy Fetcher if No LS exist in Fetcher
  int scan_ls(const share::ObLogRestoreSourceType &type);

  // Remove All LS and destroy Fetcher
  // Only happend when tenant role not match or log_restore_source is empty
  void clean_resource();

  // set the max scn can be restored
  int set_restore_log_upper_limit();

  int set_compressor_type(const common::ObCompressorType &compressor_type);

private:
  // TODO LogFetcher如何区分LogRestoreSource变化了, 比如从cluster 1的tenant A, 变为了cluster 2的tenant B
  // LogFetcher需要提供接口, 区分不同cluster_id, tenant_id
private:
  int copy_source_(const share::ObRestoreSourceServiceAttr &source);
  int refresh_fetcher_if_needed_(const share::ObRestoreSourceServiceAttr &source);
  int init_fetcher_if_needed_(const int64_t cluster_id, const uint64_t tenant_id);
  void delete_fetcher_if_needed_with_lock_();
  void update_config_();
  int64_t get_rpc_timeout_sec_();
  // update standby_fetch_log_specified_region
  void update_standby_preferred_upstream_log_region_();

  int refresh_proxy_(const share::ObRestoreSourceServiceAttr &source);


  int do_fetch_log_(storage::ObLS &ls);
  int check_need_schedule_(storage::ObLS &ls,
      int64_t &proposal_id,
      bool &need_schedule);
  int add_ls_();
  int remove_stale_ls_();

  int add_ls_if_needed_with_lock_(const share::ObLSID &id, const int64_t proposal_id);
  // The cluster id and tenant id indicate the unique tenant
  bool is_fetcher_stale_(const int64_t cluster_id, const uint64_t tenant_id);
  int stop_fetcher_safely_();
  void destroy_fetcher_();

  int check_ls_stale_(const share::ObLSID &id, const int64_t proposal_id, bool &is_stale);
  int get_ls_count_in_fetcher_(int64_t &count);

private:
  class LogErrHandler : public logfetcher::IObLogErrHandler
  {
    public:
      LogErrHandler() : inited_(false), ls_svr_(NULL) {}
      virtual ~LogErrHandler() { destroy(); }
      int init(storage::ObLSService *ls_svr);
      void destroy();
      virtual void handle_error(const int err_no, const char *fmt, ...) override {}
      virtual void handle_error(const share::ObLSID &ls_id,
          const ErrType &err_type,
          share::ObTaskId &trace_id,
          const palf::LSN &lsn,
          const int err_no,
          const char *fmt, ...) override;
    private:
      bool inited_;
      storage::ObLSService *ls_svr_;
  };

  class LogFetcherLSCtxAddInfoFactory : public logfetcher::ObILogFetcherLSCtxAddInfoFactory
  {
    const char *ALLOC_LABEL = "LSCtxAddInfo";
    public:
      virtual ~LogFetcherLSCtxAddInfoFactory() {}
      virtual void destroy() override {}
      virtual int alloc(const char *str, logfetcher::ObILogFetcherLSCtxAddInfo *&ptr) override;
      virtual void free(logfetcher::ObILogFetcherLSCtxAddInfo *ptr) override;
  };
  class LogFetcherLSCtxAddInfo : public logfetcher::ObILogFetcherLSCtxAddInfo
  {

   virtual int init(
       const logservice::TenantLSID &ls_id,
       const int64_t start_commit_version) { return 0;}

  /// get tps info of current LS
  virtual double get_tps() { return 0;}

  /// get dispatch progress and dispatch info of current LS
  virtual int get_dispatch_progress(
      const share::ObLSID &ls_id,
      int64_t &progress,
      logfetcher::PartTransDispatchInfo &dispatch_info);
  };
private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  mutable RWLock lock_;
  bool stop_flag_;     // flag marks tenant threads stop
  share::ObRestoreSourceServiceAttr source_;

  // components for log fetcher
  ObRestoreLogFunction restore_function_;
  LogErrHandler error_handler_;
  logfetcher::ObLogFetcherLSCtxDefaultFactory ls_ctx_factory_;
  LogFetcherLSCtxAddInfoFactory ls_ctx_add_info_factory_;
  logfetcher::ObLogFetcherConfig cfg_;

  // LS resource in Fetcher can be freed only before fetcher stops,
  // so in the tenant drop scene, remove all ls from Fetcher and set fetcher stop.
  logfetcher::ObLogFetcher *fetcher_;
  // proxy will be inited with user info and server list
  share::ObLogRestoreProxyUtil proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreNetDriver);
};
} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_NET_DRIVER_H_ */
