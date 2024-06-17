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

#ifndef OCEANBASE_LOG_FETCHER_LS_FETCH_MGR_H__
#define OCEANBASE_LOG_FETCHER_LS_FETCH_MGR_H__

#include "ob_log_fetching_mode.h"
#include "ob_log_ls_fetch_ctx.h"                // LSFetchCtx, LSFetchInfoForPrint

#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap
#include "lib/container/ob_array.h"             // ObArray
#include "lib/allocator/ob_safe_arena.h"        // ObSafeArena

#include "ob_log_config.h"                      // ObLogFetcherConfig
#include "ob_log_fetcher_start_parameters.h"    // ObLogFetcherStartParameters

namespace oceanbase
{
namespace logfetcher
{
// LS fetch manager
class IObLogLSFetchMgr
{
public:
  typedef common::ObArray<LSFetchInfoForPrint> LSFetchInfoArray;

public:
  virtual ~IObLogLSFetchMgr() {}

public:
  /// add a new LS
  virtual int add_ls(
      const logservice::TenantLSID &tls_id,
      const ObLogFetcherStartParameters &start_parameters,
      const bool is_loading_data_dict_baseline_data,
      const ClientFetchingMode fetching_mode,
      const ObBackupPathString &archive_dest_str,
      IObLogErrHandler &err_handler) = 0;

  /// recycle a LS
  /// mark LS deleted and begin recycle resource
  virtual int recycle_ls(const logservice::TenantLSID &tls_id) = 0;

  /// remove LS
  /// delete LS by physical
  /// only invaked by FetcherDeadPool
  virtual int remove_ls(const logservice::TenantLSID &tls_id) = 0;

  /// get LS fetch context
  virtual int get_ls_fetch_ctx(const logservice::TenantLSID &tls_id, LSFetchCtx *&ctx) = 0;

  /// get the slowest k ls (Top K algorithm)
  virtual void print_k_slowest_ls() = 0;

  virtual void *get_fetcher_host() const = 0;

  virtual int64_t get_total_count() const = 0;
};

///////////////////////////////////////////////////////////////////////////////////////////////////

class PartProgressController;
class ObILogFetcherLSCtxFactory;
class ObILogFetcherLSCtxAddInfoFactory;
template <typename T> class ObLogTransTaskPool;

class ObLogLSFetchMgr : public IObLogLSFetchMgr
{
  // static golbal class variable
  static int64_t g_print_slowest_ls_num;

  typedef common::ObLinearHashMap<logservice::TenantLSID, LSFetchCtx *> LSFetchCtxMap;

public:
  ObLogLSFetchMgr();
  virtual ~ObLogLSFetchMgr();

public:
  int init(
      PartProgressController &progress_controller,
      ObILogFetcherLSCtxFactory &ls_ctx_factory,
      ObILogFetcherLSCtxAddInfoFactory &ls_ctx_add_info_factory,
      void *fetcher_host);
  void destroy();

public:
  virtual int add_ls(
      const logservice::TenantLSID &tls_id,
      const ObLogFetcherStartParameters &start_parameters,
      const bool is_loading_data_dict_baseline_data,
      const ClientFetchingMode fetching_mode,
      const ObBackupPathString &archive_dest_str,
      IObLogErrHandler &err_handler);
  virtual int recycle_ls(const logservice::TenantLSID &tls_id);
  virtual int remove_ls(const logservice::TenantLSID &tls_id);
  virtual int get_ls_fetch_ctx(const logservice::TenantLSID &tls_id, LSFetchCtx *&ctx);
  bool is_tls_exist(const logservice::TenantLSID &tls_id) const;
  int get_tls_proposal_id(const logservice::TenantLSID &tls_id, int64_t &proposal_id) const;
  virtual void print_k_slowest_ls();
  virtual void *get_fetcher_host() const { return fetcher_; }
  virtual int64_t get_total_count() const { return ctx_map_.count(); }

  template <typename Func> int for_each_ls(Func &func)
  {
    return ctx_map_.for_each(func);
  }

public:
  static void configure(const ObLogFetcherConfig & config);

private:
  // init tenent_ls_id_str
  int init_tls_info_(const logservice::TenantLSID &tls_id, char *&tls_id_str);
  struct CtxRecycleCond
  {
    bool operator() (const logservice::TenantLSID &tls_id, LSFetchCtx *&ctx);
  };

  struct CtxLSProgressCond
  {
    CtxLSProgressCond() : ctx_cnt_(0), ls_fetch_info_array_() {}
    int init(const int64_t count);
    bool operator() (const logservice::TenantLSID &tls_id, LSFetchCtx *ctx);

    int64_t ctx_cnt_;
    LSFetchInfoArray ls_fetch_info_array_;
  };

  class FetchProgressCompFunc
  {
  public:
    bool operator() (const LSFetchInfoForPrint &a, const LSFetchInfoForPrint &b)
    {
      return a.get_progress() < b.get_progress();
    }
  };

  class DispatchProgressCompFunc
  {
  public:
    bool operator() (const LSFetchInfoForPrint &a, const LSFetchInfoForPrint &b)
    {
      return a.get_dispatch_progress() < b.get_dispatch_progress();
    }
  };

private:
  bool                            is_inited_;
  void                            *fetcher_;
  PartProgressController          *progress_controller_;
  ObILogFetcherLSCtxFactory        *ls_ctx_factory_;
  ObILogFetcherLSCtxAddInfoFactory *ls_ctx_add_info_factory_;

  LSFetchCtxMap                   ctx_map_;
  // 1. tenant_ls_id_str is maintained globally and can't be placed in modules such as PartTransDispatcher,
  // because after ls is recycled, the ls context may be cleaned up, but reader still needs to read data based on tenant_ls_id_str
  common::ObSafeArena             tls_info_serialize_allocator_;



private:
  DISALLOW_COPY_AND_ASSIGN(ObLogLSFetchMgr);
};

}
}
#endif
