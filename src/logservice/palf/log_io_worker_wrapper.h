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

#ifndef OCEANBASE_LOGSERVIVE_LOG_IO_WORKER_WRAPPER_
#define OCEANBASE_LOGSERVIVE_LOG_IO_WORKER_WRAPPER_

#include "log_throttle.h"
#include "log_io_worker.h"
#include "log_async_io_struct.h"
#include "log_async_io_worker.h"   // LogAsyncIOWorker
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace palf
{
// LogIOWorkerWrapper owns exactly one worker pool selected during init(). Sync
// mode owns LogIOWorker[], while async mode owns LogAsyncIOWorker[]. Common
// lifecycle and submit operations go through LogIOWorkerBase.
class LogIOWorkerWrapper
{
public:
  LogIOWorkerWrapper();
  ~LogIOWorkerWrapper();
  // enable_async_io applies only to user tenants; other tenants always build
  // the legacy worker pool.
  int init(const LogIOWorkerConfig &config,
           const int64_t tenant_id,
           int cb_thread_pool_tg_id,
           ObIAllocator *allocator,
           const bool enable_async_io,
           IPalfEnvImpl *palf_env_impl);
  void destroy();
  int start();
  void stop();
  void wait();
  // create/load 阶段只选择候选 submitter，不发布 PALF 到 async worker 的绑定；
  // 只有 async ctx 注册成功后才写入 palf_async_index_map_。
  int select_palf_io_submitter(const int64_t palf_id, LogIOWorkerBase *&submitter);

  // ---- Facade: async ctx lifecycle ----
  // Register a per-PALF async ctx when |submitter| belongs to the async pool.
  // Sync-mode wrappers are accepted and skipped internally.
  int register_async_palf_ctx_if_needed(const int64_t palf_id,
                                        const int cb_thread_pool_tg_id,
                                        LogIOWorkerBase *submitter);
  // Unregister and drain the async ctx. No binding, including a sync or already
  // unregistered PALF, returns OB_ENTRY_NOT_EXIST. Drain failure keeps the map
  // binding for retry; success or an already absent ctx removes it.
  int unregister_async_palf_ctx(const int64_t palf_id);

  // Forward the tenant-level write-throttle notification to the active pool.
  int notify_need_writing_throttling(const bool need_throttling);
  // Return the earliest valid pending timestamp across the active pool, or
  // OB_INVALID_TIMESTAMP when every worker is idle.
  int64_t get_oldest_pending_io_start_ts() const;
  TO_STRING_KV(K_(is_inited), K_(is_user_tenant), K_(enable_async_io),
               K_(worker_count), KP(log_io_workers_), KP(async_workers_),
               K_(round_robin_idx));

private:
  int create_and_init_log_io_workers_(const LogIOWorkerConfig &config,
                                      const int64_t tenant_id,
                                      int cb_thread_pool_tg_id,
                                      ObIAllocator *allocator,
                                      IPalfEnvImpl *palf_env_impl);
  int create_and_init_async_workers_(const LogIOWorkerConfig &config,
                                     const int64_t tenant_id,
                                     IPalfEnvImpl *palf_env_impl);
  int start_();
  void stop_();
  void wait_();
  void destroy_and_free_log_io_workers_();
  void destroy_and_free_async_pool_();
  int select_worker_index_for_palf_(const int64_t palf_id,
                                    const bool allow_single_worker_fallback,
                                    int64_t &worker_index);
  int build_palf_io_submitter_(const int64_t palf_id, LogIOWorkerBase *&submitter);
  LogIOWorkerBase *get_io_worker_by_index_(const int64_t worker_index);
  const LogIOWorkerBase *get_io_worker_by_index_(const int64_t worker_index) const;
  void merge_last_working_time_(const int64_t worker_last_working_time,
                                int64_t &last_working_time) const;
  // Internal: resolve the async-pool worker for |palf_id|, or NULL.
  LogAsyncIOWorker *get_async_io_worker_(const int64_t palf_id);
  int get_async_worker_index_by_submitter_(LogIOWorkerBase *submitter,
                                           int64_t &async_index) const;
  LogAsyncIOWorker *get_async_io_worker_by_index_(const int64_t async_index);
  int register_palf_async_index_(const int64_t palf_id, const int64_t async_index);
  void unregister_palf_async_index_(const int64_t palf_id);
  constexpr static int64_t SYS_LOG_IO_WORKER_INDEX = 0;

private:
  bool is_user_tenant_;
  bool enable_async_io_;
  int64_t worker_count_;
  // Sync mode pool: LogIOWorker[]. Layout: | sys log ioworker(idx 0) | others |.
  LogIOWorker *log_io_workers_;
  // Async mode pool: LogAsyncIOWorker[]. Layout: | sys async worker(idx 0) | others |.
  LogAsyncIOWorker *async_workers_;
  LogWritingThrottle throttle_;
  NeedPurgingThrottlingFunc need_purging_throttling_func_;
  int64_t purge_task_count_;
  // Round-robin counter for assigning user data LS across the relevant pool.
  int64_t round_robin_idx_;
  // In-memory PALF-to-worker mapping published after async ctx registration.
  // It keeps each PALF on the same async worker until that ctx is unregistered.
  hash::ObHashMap<int64_t, int64_t> palf_async_index_map_;
  IPalfEnvImpl *palf_env_impl_;
  bool is_inited_;
};

}//end of namespace palf
}//end of namespace oceanbase
#endif
