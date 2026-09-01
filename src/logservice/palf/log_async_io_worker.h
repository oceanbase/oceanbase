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

#ifndef OCEANBASE_LOGSERVICE_LOG_ASYNC_IO_WORKER_
#define OCEANBASE_LOGSERVICE_LOG_ASYNC_IO_WORKER_

#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_qsync_lock.h"
#include "log_async_palf_ctx_interface.h"
#include "log_async_io_struct.h"
#include "log_throttle.h"
#include "log_define.h"
#include "log_io_task.h"
#include "log_io_worker_base.h"

namespace oceanbase
{
namespace palf
{
class IPalfEnvImpl;

// ctx_map_ 同时拥有 entry 和 ctx。entry 引用保护 worker 离开 map 桶锁后的
// ctx 访问；ctx 自身引用只保护已经提交的 AIO callback。unregistering_ 只阻止
// 新任务接入，注销仍须等两类引用和 ctx 内已接收任务全部归零后才能释放。
class AsyncPalfIOCtxEntry
{
public:
  AsyncPalfIOCtxEntry();
  ~AsyncPalfIOCtxEntry();
  // Bind the entry to one ctx. Callers must ensure no active reference remains
  // before reset(); a violation is reported as an unexpected error.
  int init(IAsyncPalfIOCtx *ctx);
  void reset();
  bool is_valid() const;
  IAsyncPalfIOCtx *get_ctx() const { return ctx_; }
  // Once unregistering is set, producer admission must reject new tasks.
  bool is_unregistering() const { return unregistering_; }
  void set_unregistering() { unregistering_ = true; }
  // Hold the entry, rather than the ctx pin, while accessing ctx outside the
  // map bucket lock.
  void acquire_ref();
  void release_ref();
  int64_t get_active_ref() const;

  TO_STRING_KV(KP_(ctx), K_(unregistering),
               "active_ref", ATOMIC_LOAD(&active_ref_));

private:
  IAsyncPalfIOCtx *ctx_;
  bool unregistering_;
  int64_t active_ref_;

  DISALLOW_COPY_AND_ASSIGN(AsyncPalfIOCtxEntry);
};

// RAII guard for one AsyncPalfIOCtxEntry reference. It does not pin the ctx for
// an AIO callback; that separate lifetime starts in LogIOAdapter::aio_write().
class AsyncPalfIOCtxEntryGuard
{
public:
  AsyncPalfIOCtxEntryGuard();
  ~AsyncPalfIOCtxEntryGuard();
  // Replace the held entry reference. A failed call leaves the guard empty.
  int set_entry(AsyncPalfIOCtxEntry *entry);
  void reset();
  bool is_valid() const;
  // These accessors are valid only while this guard holds an entry reference.
  AsyncPalfIOCtxEntry *get_entry() const { return entry_; }
  IAsyncPalfIOCtx *get_ctx() const;
  IAsyncPalfIOCtx *operator->() const;

  TO_STRING_KV(KP_(entry));

private:
  AsyncPalfIOCtxEntry *entry_;

  DISALLOW_COPY_AND_ASSIGN(AsyncPalfIOCtxEntryGuard);
};

// Thread-safe cumulative counters and worker-owned scheduling state.
class LogAsyncIOWorkerDiagnostics
{
public:
  LogAsyncIOWorkerDiagnostics();
  ~LogAsyncIOWorkerDiagnostics() {}
  void reset();
  void set_next_drive_deadline(const int64_t ts);
  // submitted counts tasks accepted by the input queue; dropped counts failed
  // submissions; handled counts tasks transferred to a ctx. Wake counters
  // distinguish queued drive markers from requests merged into an existing one.
  void inc_submitted_task_count();
  void inc_dropped_submit_count();
  void inc_handled_task_count();
  void inc_drive_wake_count();
  void inc_coalesced_drive_wake_count();
  // Copy an atomic snapshot. subtract() differences cumulative counters but
  // copies current timestamps; has_task_activity_since() compares task counters.
  void snapshot_from(const LogAsyncIOWorkerDiagnostics &other);
  void subtract(const LogAsyncIOWorkerDiagnostics &base,
                LogAsyncIOWorkerDiagnostics &delta) const;
  bool has_task_activity_since(const LogAsyncIOWorkerDiagnostics &base) const;
  int64_t get_submitted_task_count() const;
  int64_t get_dropped_submit_count() const;
  int64_t get_handled_task_count() const;
  int64_t get_drive_wake_count() const;
  int64_t get_coalesced_drive_wake_count() const;
  int64_t get_next_drive_deadline() const;

  TO_STRING_KV(K_(submitted_task_count),
               K_(dropped_submit_count),
               K_(handled_task_count),
               K_(drive_wake_count),
               K_(coalesced_drive_wake_count),
               K_(next_drive_deadline));
private:
  int64_t submitted_task_count_;
  int64_t dropped_submit_count_;
  int64_t handled_task_count_;
  int64_t drive_wake_count_;
  int64_t coalesced_drive_wake_count_;
  // Accessed only by the worker thread.
  int64_t next_drive_deadline_;

  DISALLOW_COPY_AND_ASSIGN(LogAsyncIOWorkerDiagnostics);
};

// 租户级异步 IO worker，每个对象只有一个写线程：
// 1. producer 先在目标 ctx 预留任务槽，再把 LogIOTask 放入 input_queue_；
// 2. 写线程按 FIFO 将任务交给对应 ctx，ctx 暂时拒绝时保留队头任务重试；
// 3. AIO callback 直接更新所属 ctx，并用唯一的 ASYNC_MARK 合并并发唤醒；
// 4. 队列空、分发受阻、收到唤醒标记或达到批量上限时，统一 drive 所有 ctx；
// 5. stop 只禁止新任务，写线程继续排空已接收任务和 ctx 内 AIO；owner 注销
//    所有 ctx 后，wait 才允许线程退出并返回。
class LogAsyncIOWorker : public LogIOWorkerBase, public IAsyncDriveWaker
{
public:
  // Producers and ASYNC_MARK wake the worker directly. This backstop handles
  // idle polling and caps the wait for a future drive deadline.
  static constexpr int64_t QUEUE_WAIT_TIME_US = 100 * 1000; // 100ms backstop
  static constexpr int64_t THREAD_COUNT = 1;

  LogAsyncIOWorker();
  virtual ~LogAsyncIOWorker();

  // Initialize queues and maps without starting the worker thread.
  int init(const int64_t tenant_id,
           IPalfEnvImpl *palf_env_impl,
           const int64_t input_queue_capacity);
  // Stop and join the thread, then release worker resources. The owning wrapper
  // serializes calls and must unregister every PALF ctx before destruction.
  void destroy() override final;
  // Initialized and not destroying; this does not imply the thread is running.
  bool is_valid() const;

  // Start task dispatch. stop() rejects new submissions. During graceful
  // shutdown, wait() also requires the owner to unregister every PALF ctx
  // before it joins the drained worker thread.
  int start() override;
  void stop() override;
  void wait() override;

  // Create and register the per-PALF async write ctx used for later dispatch.
  // The lifecycle read lock serializes ctx creation and map installation with
  // worker destruction.
  int register_and_create_ctx(const int64_t palf_id,
                              const int cb_tg_id,
                              IPalfEnvImpl *palf_env_impl,
                              const AsyncThrottleContext &throttle_ctx);
  // Reject new submissions for this PALF, wait until accepted tasks, entry
  // references, and callback pins drain, then erase and release the ctx. The
  // lifecycle read lock only protects the unregister boundary; the owning
  // wrapper keeps worker resources alive during the lock-free drain.
  int unregister_palf_ctx_and_wait(const int64_t palf_id);

  // Producer-thread entry. Failure leaves ownership with the caller. Success
  // transfers both task and reserved ctx slot to the worker; dispatch failure
  // retains the same task for retry and never releases or skips it.
  int submit_io_task(LogIOTask *task) override final;

  // Callback short-path producer: wake up the write thread so admission /
  // READY fragment submit can advance. Safe to call from IOManager callback
  // thread. Idempotent: many concurrent callbacks may discover more work, but
  // one queued ASYNC_MARK task is enough to make the worker run another drive pass.
  // drive_pending_ coalesces those duplicate wake requests. Callback pins and
  // synchronous ctx unregistration keep the worker alive during this call.
  int wake_up_for_drive() override;

  // 线程主循环不能长期持有 lifecycle_lock_，否则 stop() 无法设置停止状态，
  // 已接收任务也就无法进入排空阶段。
  void run1() override;

  // Return the earliest positive pending IO start time across registered ctxs,
  // or OB_INVALID_TIMESTAMP when no ctx reports one. The map snapshot is taken
  // under lifecycle protection; ctx inspection runs after releasing that lock.
  int64_t get_oldest_pending_io_start_ts() const override final;

private:
  // Underscored lifecycle implementations run under their public wrapper's lock.
  static constexpr int64_t CTX_MAP_BUCKET_NUM = 4;
  static constexpr int64_t UNREGISTER_WAIT_INTERVAL_US = 100 * 1000;

  // Reusable queue token that requests a drive pass; it is not a PALF IO task.
  class AsyncMarkTask : public LogIOTask
  {
  public:
    AsyncMarkTask();
    ~AsyncMarkTask() override;

  private:
    int do_task_(int tg_id, IPalfHandleImplGuard &guard) override final;
    int after_consume_(IPalfHandleImplGuard &guard) override final;
    LogIOTaskType get_io_task_type_() const override final;
    void free_this_(IPalfEnvImpl *palf_env_impl) override final;
    int64_t get_io_size_() const override final;
    bool need_purge_throttling_() const override final;

    DISALLOW_COPY_AND_ASSIGN(AsyncMarkTask);
  };
  // Snapshot container that holds one entry reference for every collected ctx,
  // allowing the worker to access them after releasing map bucket locks.
  class EntryGuardSet;
  class GetEntryOp;
  class MarkUnregisteringOp;
  class SnapshotEntryOp;
  class EraseDrainedEntryOp;

  int init_(const int64_t tenant_id,
            IPalfEnvImpl *palf_env_impl,
            const int64_t input_queue_capacity);
  void destroy_();
  bool is_valid_() const;
  int start_();
  void stop_();
  int register_and_create_ctx_(const int64_t palf_id,
                               const int cb_tg_id,
                               IPalfEnvImpl *palf_env_impl,
                               const AsyncThrottleContext &throttle_ctx);
  int mark_ctx_unregistering_(const int64_t palf_id);
  int create_ctx_entry_(const int64_t palf_id,
                        const int cb_tg_id,
                        IPalfEnvImpl *palf_env_impl,
                        const AsyncThrottleContext &throttle_ctx,
                        AsyncPalfIOCtxEntry *&entry);
  int get_entry_guard_(const int64_t palf_id,
                       const bool allow_unregistering,
                       AsyncPalfIOCtxEntryGuard &guard);
  int snapshot_entries_(EntryGuardSet &out) const;
  int dispatch_task_(LogIOTask *task);
  int drive_write_all_(int64_t &next_drive_interval_us);
  bool is_drained_() const;
  int64_t get_ctx_count_() const;
  int64_t get_dropped_task_count_() const;
  int64_t get_dispatched_task_count_() const;
  void destroy_entry_(AsyncPalfIOCtxEntry *entry);
  int try_erase_ctx_(const int64_t palf_id,
                     AsyncPalfIOCtxEntry *&entry_to_destroy);
  void drain_and_erase_ctx_(const int64_t palf_id,
                            AsyncPalfIOCtxEntry *&entry_to_destroy);

  // Test-only helpers implemented in unittest. Production registration and
  // unregister must use the public worker APIs so task reservation and entry
  // lifecycle stay synchronized.
  int register_ctx_entry_(const int64_t palf_id, IAsyncPalfIOCtx *ctx);
  int unregister_ctx_entry_(const int64_t palf_id);

  int submit_io_task_(LogIOTask *task);
  int wake_up_for_drive_();
  int enqueue_drive_mark_();
  int write_loop_();
  static int64_t calc_queue_wait_us_(const int64_t deadline,
                                     const int64_t now);
  int64_t get_next_queue_wait_us_() const;
  bool is_async_mark_task_(LogIOTask *task) const;
  int handle_queued_log_io_task_(LogIOTask *&task);
  int drive_all_ctx_once_();
  void print_worker_stat_();
  void add_worker_server_event_(const char *event,
                                const int ret,
                                const int64_t palf_id);
  void add_worker_summary_server_event_();
  bool can_exit_write_loop_() const;

private:
  // Public APIs hold the write side for lifecycle transitions and the read
  // side for other worker state access. Unregister releases the read side after
  // marking its map entry so start/stop can drive accepted tasks to completion.
  // Worker-thread-only helpers run without this lock and are joined before
  // resources are freed.
  mutable common::ObQSyncLock lifecycle_lock_;
  bool is_inited_;
  bool is_running_;
  bool is_destroying_;
  int64_t tenant_id_;
  IPalfEnvImpl *palf_env_impl_;
  common::ObLightyQueue input_queue_;   // LogIOTask *
  // Reusable queue marker. drive_pending_ keeps at most one copy in input_queue_.
  AsyncMarkTask drive_mark_task_;
  typedef common::hash::ObHashMap<int64_t, AsyncPalfIOCtxEntry *> CtxMap;
  CtxMap ctx_map_;
  // Mutated only by the worker and read by it or after it has joined.
  int64_t dropped_task_count_;
  int64_t dispatched_task_count_;
  // Worker-owned task popped from input_queue_ but not yet accepted by its
  // per-PALF ctx. It must be retried before any later queue item.
  LogIOTask *pending_dispatch_task_;
  // ASYNC_MARK 合并标记，不表示异步写进度：0->1 的线程负责入队一个 marker；
  // 其余并发唤醒保持 1；worker 取出 marker 后先恢复为 0，再 drive ctx，使
  // drive 期间新产生的工作仍能入队下一枚 marker。入队失败也必须回到 0。
  int64_t drive_pending_;
  LogAsyncIOWorkerDiagnostics diagnostics_;
  common::ObMiniStat::ObStatItem input_queue_size_stat_;
  common::ObMiniStat::ObStatItem wait_cost_stat_;
  common::ObMiniStat::ObStatItem dispatch_cost_stat_;
  common::ObMiniStat::ObStatItem drive_cost_stat_;
  int64_t print_log_interval_;
  int64_t event_summary_interval_;
  LogAsyncIOWorkerDiagnostics last_summary_stat_;
  LogAsyncIOWorkerDiagnostics last_event_stat_;

  DISALLOW_COPY_AND_ASSIGN(LogAsyncIOWorker);
};

} // end namespace palf
} // end namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_ASYNC_IO_WORKER_
