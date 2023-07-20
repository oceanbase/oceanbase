// Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_LOG_META_DATA_FETCHER_H_
#define OCEANBASE_LOG_META_DATA_FETCHER_H_

#include "lib/allocator/ob_concurrent_fifo_allocator.h"  // ObConcurrentFIFOAllocator
#include "ob_log_config.h"
#include "ob_log_fetching_mode.h"    // ClientFetchingMode
#include "ob_log_task_pool.h"        // ObLogTransTaskPool
#include "ob_log_entry_task_pool.h"  // ObLogEntryTaskPool
#include "ob_log_meta_data_fetcher_dispatcher.h"  // ObLogMetaDataFetcherDispatcher
#include "logservice/logfetcher/ob_log_fetcher_start_parameters.h"

namespace oceanbase
{
namespace libobcdc
{
class IObLogFetcher;
class IObLogSysLsTaskHandler;
class IObLogErrHandler;
class PartTransTask;

class ObLogMetaDataFetcher
{
public:
  ObLogMetaDataFetcher();
  ~ObLogMetaDataFetcher();

  int init(
      const ClientFetchingMode fetching_mode,
      const share::ObBackupPathString &archive_dest,
      IObLogFetcherDispatcher *dispatcher,
      IObLogSysLsTaskHandler *sys_ls_handler,
      common::ObISQLClient *proxy,
      IObLogErrHandler *err_handler,
      const int64_t cluster_id,
      const ObLogConfig &cfg,
      const int64_t start_seq);
  int start();
  void stop();
  void destroy();

public:
  int add_ls_and_fetch_until_the_progress_is_reached(
      const uint64_t tenant_id,
      const logfetcher::ObLogFetcherStartParameters &start_parameters,
      const int64_t timeout);

private:
  static const int64_t TASK_POOL_ALLOCATOR_TOTAL_LIMIT = (1LL << 32); // 4G
  // Setting the page size to 64K can prevent ObVSliceAlloc from caching too much memory. The scenario is as follows:
  // 1. During the startup, the baseline data for the data dictionary is constructed. This may replay too many
  //    SYS log stream transactions, in which only a portion of them are DDL transactions that need to be concerned.
  // 2. If the page size is set to 2M, many pages (blocks) cannot be completely returned and released,
  //    which can cause the allocator to hold too much memory, thus causing allocation errors.                                                                     //
  static const int64_t TASK_POOL_ALLOCATOR_PAGE_SIZE = common::OB_MALLOC_MIDDLE_BLOCK_SIZE;
  static const int64_t TASK_POOL_ALLOCATOR_HOLD_LIMIT = TASK_POOL_ALLOCATOR_PAGE_SIZE;
  static const int64_t PART_TRANS_TASK_PREALLOC_COUNT = 1000;
  static const int64_t PART_TRANS_TASK_PREALLOC_PAGE_COUNT = 1000;
  static const int64_t LOG_ENTRY_TASK_COUNT= 100;
  static const int64_t FETCHER_WAIT_LS_TIMEOUT = 2 * 1000L * 1000L;
  typedef ObLogTransTaskPool<PartTransTask> PartTransTaskPool;

private:
  bool is_inited_;
  common::ObConcurrentFIFOAllocator trans_task_pool_alloc_;
  PartTransTaskPool trans_task_pool_;
  ObLogEntryTaskPool log_entry_task_pool_;
  IObLogFetcher *log_fetcher_;

  DISALLOW_COPY_AND_ASSIGN(ObLogMetaDataFetcher);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
