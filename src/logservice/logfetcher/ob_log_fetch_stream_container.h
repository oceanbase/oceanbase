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
 * Fetch Stream Container
 */

#ifndef OCEANBASE_LOG_FETCHER_FETCH_STREAM_CONTAINER_H__
#define OCEANBASE_LOG_FETCHER_FETCH_STREAM_CONTAINER_H__

#include "lib/lock/ob_spin_rwlock.h"      // SpinRWLock
#include "lib/net/ob_addr.h"              // ObAddr

#include "ob_log_fetch_stream_type.h"     // FetchStreamType
#include "ob_log_ls_fetch_stream.h"       // FSList, FetchStream
#include "ob_log_handler.h"               // ILogFetcherHandler

namespace oceanbase
{
namespace logfetcher
{
class IFetchStreamPool;
class IObLogRpc;
class IObLSWorker;
class IFetchLogARpcResultPool;
class PartProgressController;
class FetchStreamContainer
{
public:
  FetchStreamContainer();
  virtual ~FetchStreamContainer();

public:
  void reset();
  void reset(const FetchStreamType stype,
      const uint64_t self_tenant_id,
      IObLogRpc &rpc,
      IFetchStreamPool &fs_pool,
      IObLSWorker &stream_worker,
      IFetchLogARpcResultPool &rpc_result_pool,
      PartProgressController &progress_controller,
      ILogFetcherHandler &log_handler);

public:
  // Assign the fetch log task to a FetchStream
  // If the target is a "new fetch stream task", assign it to a worker thread for processing
  int dispatch(LSFetchCtx &task,
      const common::ObAddr &request_svr);

  // print stat info
  void do_stat(int64_t &traffic);

public:
  TO_STRING_KV("stype", print_fetch_stream_type(stype_),
      K_(fs_list));

private:
  static const int64_t MAX_FS_COUNT = 1;
  void free_fs_list_();
  int try_create_new_fs_and_add_task_(LSFetchCtx &task,
      const common::ObAddr &request_svr);
  int alloc_fetch_stream_(
      const uint64_t tenant_id,
      LSFetchCtx &ls_fetch_ctx,
      FetchStream *&fs);

private:
  FetchStreamType           stype_;
  uint64_t                  self_tenant_id_;
  IObLogRpc                 *rpc_;                    // RPC Processor
  IFetchStreamPool          *fs_pool_;                // Fetch log stream task object pool
  IObLSWorker               *stream_worker_;          // Stream master
  IFetchLogARpcResultPool   *rpc_result_pool_;        // RPC result pool
  PartProgressController    *progress_controller_;    // Progress controller
  ILogFetcherHandler        *log_handler_;

  // Fetch log stream task
  // Use read/write locks to control the reading and writing of tasks
  FSList                  fs_list_;
  common::SpinRWLock      lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(FetchStreamContainer);
};

}
}

#endif
