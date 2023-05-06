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

#ifndef OCEANBASE_LIBOBCDC_COLLECTOR_FETCH_STREAM_CONTAINER_H__
#define OCEANBASE_LIBOBCDC_COLLECTOR_FETCH_STREAM_CONTAINER_H__

#include "lib/objectpool/ob_small_obj_pool.h"   // ObSmallObjPool
#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap
#include "logservice/common_util/ob_log_ls_define.h" // logservice::TenantLSID
#include "ob_log_fetch_stream_container.h"      // FetchStreamContainer
#include "ob_log_fetch_stream_pool.h"           // FetchStreamPool
#include "ob_log_fetch_log_rpc.h"               // FetchLogARpcResultPool

namespace oceanbase
{
namespace libobcdc
{
class IObFsContainerMgr
{
public:
  /// add a new fetch stream container
  virtual int add_fsc(const FetchStreamType stype,
      const logservice::TenantLSID &tls_id) = 0;

  /// remove the fetch stream container
  virtual int remove_fsc(const logservice::TenantLSID &tls_id) = 0;

  /// get the fetch stream container
  virtual int get_fsc(const logservice::TenantLSID &tls_id,
      FetchStreamContainer *&fsc) = 0;
};

class IObLogRpc;
class IObLSWorker;
class PartProgressController;

class ObFsContainerMgr : public IObFsContainerMgr
{
public:
  ObFsContainerMgr();
  virtual ~ObFsContainerMgr();
  int init(const int64_t svr_stream_cached_count,
      const int64_t fetch_stream_cached_count,
      const int64_t rpc_result_cached_count,
      IObLogRpc &rpc,
      IObLSWorker &stream_worker,
      PartProgressController &progress_controller);
  void destroy();

public:
  virtual int add_fsc(const FetchStreamType stype,
      const logservice::TenantLSID &tls_id);
  virtual int remove_fsc(const logservice::TenantLSID &tls_id);
  virtual int get_fsc(const logservice::TenantLSID &tls_id,
      FetchStreamContainer *&fsc);
  void print_stat();

private:
  //struct FscStatFunc
  // TODO
  struct SvrStreamStatFunc
  {
    bool operator() (const logservice::TenantLSID &key, FetchStreamContainer* value)
    {
      UNUSED(key);
      if (NULL != value) {
        value->do_stat();
      }
      return true;
    }
  };

  typedef common::ObLinearHashMap<logservice::TenantLSID, FetchStreamContainer*> FscMap;
  typedef common::ObSmallObjPool<FetchStreamContainer> FscPool;
  // TODO
  static const int64_t SVR_STREAM_POOL_BLOCK_SIZE = 1 << 22;

private:
  bool is_inited_;

  // External modules
  IObLogRpc                     *rpc_;                    // RPC handler
  IObLSWorker                   *stream_worker_;          // Stream master
  PartProgressController        *progress_controller_;    // progress controller

  FscMap                        fsc_map_;
  FscPool                       fsc_pool_;                // Supports multi-threaded alloc/release
  FetchStreamPool               fs_pool_;                 // FetchStream object pool
  FetchLogARpcResultPool        rpc_result_pool_;         // RPC resujt object pool
};

} // namespace libobcdc
} // namespace oceanbase

#endif
