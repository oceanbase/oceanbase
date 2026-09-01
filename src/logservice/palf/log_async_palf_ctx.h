/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOGSERVICE_LOG_ASYNC_PALF_CTX_
#define OCEANBASE_LOGSERVICE_LOG_ASYNC_PALF_CTX_

#include <stdint.h>
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_se_array.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/objectpool/ob_small_obj_pool.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/lock/ob_qsync_lock.h"
#include "share/scn.h"
#include "log_async_fragment.h"
#include "log_async_io_struct.h"
#include "log_async_palf_ctx_diag.h"
#include "log_async_write_planner.h"   // LogAsyncWritePlanner
#include "log_storage.h"
#include "log_throttle.h"          // LogWritingThrottle, NeedPurgingThrottlingFunc
#include "log_async_palf_ctx_interface.h"
#include "log_io_task.h"           // LogIOTask
#include "palf_handle_impl_guard.h"

namespace oceanbase
{
namespace palf
{
class LogIOTask;
class LogIOFlushLogTask;
class IPalfEnvImpl;
class IPalfHandleImpl;

// 每个 PALF 一个异步写状态机, 由所属 LogAsyncIOWorker 线程推进.
//
// 一轮 drive 按以下顺序执行:
//   1. 队首 barrier 在前序 flush/AIO/publish 排空后执行;
//   2. planner 无有效 tail 时从 storage snapshot 恢复状态;
//   3. 收割 AIO 完成, 推进持久化前缀并 publish 已持久化 task;
//   4. 按需切 block, 再做限流判断、task admission 和 fragment 规划;
//   5. 提交 READY 或达到重试时间的 FAILED fragment.
//
// on_aio_complete() 在 IOManager callback 线程上只关闭本次 AIO 状态并
// 更新 inflight 计数, 通过出参通知调用者是否需要唤醒 worker. publish
// 始终留在 worker 线程. callback 路径不能做磁盘 IO、sleep, 也不能等待
// 依赖 AIO 完成的资源.
class AsyncPalfIOCtx : public IAsyncPalfIOCtx
{
public:
  AsyncPalfIOCtx();
  ~AsyncPalfIOCtx() override;

  // 初始化每 PALF 异步上下文. cb_tg_id 用于投递逻辑回调; drive_waker 和
  // throttle_ctx 中的指针均为借用. ctx 从 palf_env_impl 获取并持有 handle
  // guard, 因此 palf_env_impl 必须晚于 ctx 销毁.
  // Takes qsync write lock: lifecycle init must exclude all worker/callback entrypoints.
  int init(const int64_t palf_id,
           const int cb_tg_id,
           IPalfEnvImpl *palf_env_impl,
           IAsyncDriveWaker *drive_waker,
           const AsyncThrottleContext &throttle_ctx);
  // Takes qsync write lock: destroy/unregister must wait for all normal readers to leave.
  void destroy();

  // ---- IAsyncPalfIOCtx ----
  // Pure getter; no qsync lock.
  int64_t get_palf_id() const override { return palf_id_; }
  // Takes qsync read lock: task slot reservation is a normal producer entrypoint.
  int try_reserve_task_slot(const LogIOTaskType task_type) override;
  // Takes qsync read lock: task slot release may run on normal producer/worker paths.
  void release_task_slot(const LogIOTaskType task_type) override;
  // Takes qsync read lock: enqueue is a normal producer entrypoint.
  int enqueue_task(LogIOTask *task) override;
  // Takes qsync read lock: worker progress must be excluded from destroy.
  int drive_write(int64_t &next_drive_interval_us) override;
  // Takes qsync read lock: AIO callback must be excluded from destroy.
  int on_aio_complete(const AsyncIOCompletionEvent &event,
                                      bool &need_wake_worker) override;
  // Takes qsync read lock: wake request reads ctx wiring and must be excluded from destroy.
  int request_drive() override;
  // unregister 调用方自身持有一个基准 pin. 只要 ctx 仍持有 task credit、
  // fragment、AIO 或额外 pin, 本接口就返回非零; 返回值只是排空判据,
  // 不是去重后的 task 数量.
  // Takes qsync write lock: unregister uses this stable snapshot before destroy.
  int64_t get_inflight_count() const override;
  // Takes qsync read lock: diagnostic snapshot reads fragment pool internals.
  int64_t get_oldest_pending_io_start_ts() const override;

  // Pure deadline getter; no qsync lock.
  int64_t get_throttle_next_admit_ts() const override { return throttle_next_admit_ts_; }
  // No qsync lock: unregister calls this only after all ctx users are drained.
  // The ctx must be allocated with OB_NEW using the "PalfAsyncCtx" label.
  void free_this() override;

  // Lifetime pin is intentionally lock-free: it protects ctx pointer lifetime
  // before a caller enters another public method guarded by qsync.
  void pin() override { ATOMIC_INC(&active_ref_); }
  // See pin(); no qsync lock.
  void unpin() override { ATOMIC_DEC(&active_ref_); }
  // Pure lifetime getter; no qsync lock.
  int64_t get_active_ref() const override { return ATOMIC_LOAD(&active_ref_); }

  TO_STRING_KV(K_(palf_id), K_(is_inited),
               K_(cb_tg_id), KP_(palf_env_impl), KP_(drive_waker),
               "task_queue_count", task_queue_.get_total(),
               "task_queue_capacity", task_queue_.capacity(), K_(inflight_aio_cnt),
               "available_flush_task_slot_count", ATOMIC_LOAD(&available_flush_task_slot_count_),
               "available_barrier_task_slot_count", ATOMIC_LOAD(&available_barrier_task_slot_count_),
               "active_ref", ATOMIC_LOAD(&active_ref_),
               K_(block_switch_pending), K_(current_write_block_id),
               KP_(control_barrier_task),
               K_(popped_task_trace_next_idx), K_(popped_task_trace_cnt),
               K_(last_popped_task_trace_print_ts), K_(throttle_ctx),
               K_(ignore_throttle_once),
               "fragment_max_size", planner_.get_fragment_max_size(),
               "wait_parent_max_size", planner_.get_wait_parent_max_size(),
               K_(aio_delay_us), "planner_aio_delay_us", planner_.get_aio_delay(),
               "submit_fail_cnt", ATOMIC_LOAD(&submit_fail_cnt_),
               "complete_fail_cnt", ATOMIC_LOAD(&complete_fail_cnt_),
               "stale_completion_cnt", ATOMIC_LOAD(&stale_completion_cnt_),
               "published_entry_cnt", ATOMIC_LOAD(&published_entry_cnt_),
               K_(block_switch_pending_since_ts),
               K_(throttle_error_count), K_(last_throttle_error_ts),
               K_(throttle_next_admit_ts));

private:
  int init_(const int64_t palf_id,
             const int cb_tg_id,
             IPalfEnvImpl *palf_env_impl,
             IAsyncDriveWaker *drive_waker,
             const AsyncThrottleContext &throttle_ctx);
  void destroy_();
  int try_reserve_task_slot_(const LogIOTaskType task_type);
  void release_task_slot_(const LogIOTaskType task_type);
  int enqueue_task_(LogIOTask *&task);
  int drive_write_(int64_t &next_drive_interval_us);
  int on_aio_complete_(const AsyncIOCompletionEvent &event,
                        bool &need_wake_worker);
  int request_drive_();
  int64_t get_inflight_count_() const;
  int64_t get_oldest_pending_io_start_ts_() const;

  enum class QueueItemType : int8_t {
    FLUSH_LOG = 0,      // LogIOFlushLogTask*
    BARRIER_TASK = 1,   // Generic LogIOTask*, executed through control barrier
  };
  // Type-erased FIFO record. While queued, this ctx owns the payload task.
  struct AsyncQueueItem {
    QueueItemType type_;
    LogIOTaskType task_type_;
    void *payload_;
    AsyncQueueItem()
      : type_(QueueItemType::FLUSH_LOG), task_type_(LogIOTaskType::FLUSH_LOG_TYPE), payload_(NULL)
    {}
    AsyncQueueItem(QueueItemType type, LogIOTaskType task_type, void *payload)
      : type_(type), task_type_(task_type), payload_(payload)
    {}
    void reset()
    {
      type_ = QueueItemType::FLUSH_LOG;
      task_type_ = LogIOTaskType::FLUSH_LOG_TYPE;
      payload_ = NULL;
    }
    void init(const QueueItemType type, const LogIOTaskType task_type, void *payload)
    {
      type_ = type;
      task_type_ = task_type;
      payload_ = payload;
    }
    TO_STRING_KV("type", static_cast<int>(type_),
                 "task_type", static_cast<int>(task_type_), KP_(payload));
  };

  // Compact diagnostic record retained after a task leaves task_queue_.
  struct PoppedTaskTraceEntry {
    PoppedTaskTraceEntry();
    void reset();
    void init(const int64_t pop_ts,
              const QueueItemType queue_type,
              const LogIOTaskType task_type,
              const LSN &begin_lsn,
              const int64_t data_len,
              const char *buf0,
              const char *buf1,
              const int64_t buf1_len,
              LogIOTask *task);
    int64_t pop_ts;
    QueueItemType queue_type;
    LogIOTaskType task_type;
    LSN begin_lsn;
    int64_t data_len;
    const char *buf0;
    const char *buf1;
    int64_t buf1_len;
    LogIOTask *task;
    // Keep field names short so the ring dump stays below the single-log-line limit.
    TO_STRING_KV("ts", pop_ts, "q", static_cast<int>(queue_type),
                 "t", static_cast<int>(task_type), "b", begin_lsn, "l", data_len,
                 "p0", OB_P(buf0), "p1", OB_P(buf1), "l1", buf1_len, "tk", OB_P(task));
  };

  int get_next_drive_interval_(int64_t &next_drive_interval_us);
  static inline QueueItemType get_queue_item_type_(LogIOTask *task)
  {
    return task->get_io_task_type() == LogIOTaskType::FLUSH_LOG_TYPE
        ? QueueItemType::FLUSH_LOG
        : QueueItemType::BARRIER_TASK;
  }
  static inline bool is_barrier_task_type_(const LogIOTaskType task_type)
  {
    return LogIOTaskType::FLUSH_META_TYPE == task_type
        || LogIOTaskType::TRUNCATE_PREFIX_TYPE == task_type
        || LogIOTaskType::TRUNCATE_LOG_TYPE == task_type
        || LogIOTaskType::FLASHBACK_LOG_TYPE == task_type
        || LogIOTaskType::PURGE_THROTTLING_TYPE == task_type;
  }
  // 对同步提交错误分类. 资源、队列和瞬时错误在下一轮直接重试;
  // 设备、fd 和非预期错误保留退避时间及周期性 WARN.
  static inline bool is_submit_transient_error_(const int ret)
  {
    return common::OB_ALLOCATE_MEMORY_FAILED == ret
        || common::OB_EAGAIN == ret
        || common::OB_SIZE_OVERFLOW == ret
        || common::OB_RESOURCE_OUT == ret
        || common::OB_TIMEOUT == ret
        || common::OB_IO_TIMEOUT == ret;
  }

  // ASYNC-CTX-STAT: snapshot the async control-plane state into |out|. Pure read of
  // the queues / pool / counters; mutates nothing. Called on the worker thread.
  // Split out from the print so aggregation can be unit tested directly without
  // the throttle / logging plumbing.
  void collect_stat_(AsyncCtxStatSnapshot &out);
  // Refresh ERRSIM fragment limits and AIO completion delay at most once per
  // second. Fragment limits use 4KB pages; AIO delay uses microseconds.
  void refresh_errsim_options_();
  // ASYNC-CTX-STAT: throttled periodic dump of the async ctx state. Called once
  // per drive round on the worker thread; the throttle
  // (palf_reach_time_interval on last_stat_print_ts) keeps it to one line every
  // ASYNC_CTX_STAT_PRINT_INTERVAL_US so it is cheap to leave on always. Prints at
  // INFO normally (so a healthy LS's advancing LSNs are visible for
  // comparison) and prints WARN when the snapshot looks_stuck(). NEVER
  // advances state / submits / blocks -- pure debug info.
  void print_async_ctx_stat_();
  void add_async_ctx_server_event_(const char *event, const AsyncCtxStatSnapshot &stat, const int ret);
  void reset_diag_fields_();
  void reset_perf_items_();
  void print_perf_stat_(const int64_t now_us, const int64_t oldest_task_age_us);
  void reset_popped_task_trace_();
  void record_popped_task_trace_(AsyncQueueItem *item);
  void print_popped_task_trace_();

  // ---- worker drive stages ----
  // Completion stage: 轮询未收到 callback 的 AIO, 回收 FINISHED slot,
  // 推进 persisted prefix, 再 publish 已持久化 task. WAIT_PARENT 在后续
  // fragment stage 收集 READY slot 时唤醒.
  int drive_phase_completion_(IPalfHandleImpl *handle);
  // worker 无阻塞轮询 SUBMITTED fragment 保留的 ObIOHandle, 兜底处理 callback
  // 未执行但底层 result 已结束的 AIO. fragment 状态机是 close-once 门闩:
  // 获胜方转 FINISHED/FAILED 并减少 inflight, 另一方看到非 SUBMITTED 后退出.
  int poll_submitted_fragment_(PhysicalWriteFragment &frag,
                               const int64_t now);
  int handle_completed_fragment_(PhysicalWriteFragment &frag,
                                 const FragmentRef &fragment_ref,
                                 const int io_ret,
                                 const int64_t finish_ts,
                                 const bool polled_completion,
                                 bool &completed_by_me);
  int poll_submitted_fragments_(int64_t &remaining_submitted_count);
  // Fragment stage: submit READY / due-retry FAILED fragments via
  // handle->async_pwrite. Called after task planning on the worker thread.
  int drive_phase_fragment_(IPalfHandleImpl *handle);
  int append_due_failed_fragments_(common::ObIArray<PhysicalWriteFragment *> &submit_fragments);
  // Task stage: switch block, apply throttle, admit new tasks, and plan them.
  // Called between completion processing and fragment submission.
  int drive_phase_task_(IPalfHandleImpl *handle);

  // 按 persisted prefix 连续 publish task: commit storage tail、推进内存复用
  // 位点、转交 callback pool, 最后从 planner 队列出队.
  int try_publish_contiguous_prefix_(IPalfHandleImpl *handle);
  // callback pool 接收成功后取得 task 所有权, 同时清空调用方别名.
  int publish_one_task_(IPalfHandleImpl *handle, LogIOFlushLogTask *&task);

  // Admission: drain task_queue_ into planner. The task phase plans queued
  // write sources once after this step.
  int admit_ready_tasks_();
  // Admit flush tasks from producer FIFO into planner while preserving enqueue
  // order. Stop at the first barrier and leave it at the queue head.
  int drain_producer_queue_to_planner_();
  int pop_head_control_barrier_task_();
  int64_t get_pending_async_stage_count_() const;
  bool is_control_barrier_drained_() const;
  // barrier 仅执行一次: 成功后 callback pool 接管 task, 失败时由 ctx 释放.
  int execute_control_barrier_task_(bool &consumed);
  // planner 无效或 storage tail 改变后, 在数据路径开始前重建 planner 位点和
  // 当前 block 状态. 调用方必须先保证旧 planner/fragment 已排空.
  int reset_async_state_after_tail_changed_(IPalfHandleImpl *handle,
      const LogStorage::AsyncStorageSnapshot &storage_snapshot);
  void record_purge_task_finished_(const bool need_purge_throttling);
  int refresh_planner_state_before_drive_(IPalfHandleImpl *handle);
  void set_block_switch_pending_(const bool pending);
  int mark_block_switch_if_needed_();
  int refresh_current_write_block_id_(const PlannerStatus &planner_status);
  // 新 task admission 的非阻塞限流门闩. 只记录 deadline 和诊断数据,
  // 不在共享 async worker 上 sleep.
  void check_throttle_admission_(bool &can_admit);
  // Probe the tenant-shared LogWritingThrottle with the current deadline and
  // return its refreshed value. A false result skips only new task admission.
  int can_admit_new_entry_(const int64_t logical_bytes,
                           const int64_t current_next_admit_ts,
                           bool &can_admit,
                           int64_t &next_admit_ts);
  bool has_async_throttle_() const;
  bool has_valid_async_throttle_admission_() const;
  // 估算下一条待 admission flush task 的逻辑数据量, 只用于限流节奏;
  // publish 时 after_append_log 会使用精确字节数更新衰减模型, 因此估算偏差
  // 不影响正确性.
  int64_t peek_pending_admit_bytes_();
  AsyncQueueItem *alloc_task_queue_item_(const QueueItemType type, LogIOTask *task);
  // Clear the caller's alias only after the item has returned to the pool.
  void free_task_queue_item_(AsyncQueueItem *&item);
  int peek_not_null_task_queue_item_(const QueueItemType type, AsyncQueueItem *&item);
  int pop_task_queue_item_(AsyncQueueItem *&item);
  int discard_queued_tasks_(int64_t &discarded_flush_count,
                            int64_t &discarded_barrier_count);
  int discard_tasks_after_handle_deleted_(int64_t &next_drive_interval_us);
  // Submit one READY or due-retry FAILED fragment via the handle.
  int submit_fragment_(IPalfHandleImpl *handle, PhysicalWriteFragment &frag);
  // Try the pending block switch when persisted LSN reaches planned end.
  int try_switch_block_(IPalfHandleImpl *handle);
  // 从队首 flush task 读取 SCN, 供首 block 和后续 block switch 写 header.
  int extract_first_queued_scn_(share::SCN &scn);
private:
  // Retry interval for FAILED fragments (submit or completion failure).
  static constexpr int64_t FRAGMENT_RETRY_INTERVAL_US = 10 * 1000; // 10ms
  // A failed throttle decision must not cause the worker to spin immediately.
  // Retry at the same cadence as the rate-limited throttle error log.
  static constexpr int64_t THROTTLE_ERROR_RETRY_INTERVAL_US = 1 * 1000 * 1000; // 1s
  // ASYNC-CTX-STAT: throttle interval for the periodic state dump.
  // Healthy LSs also print (so advancing LSNs are visible for comparison),
  // so keep it coarse enough to never flood observer.log at scale (hundreds of
  // LS x worker rounds) yet fine enough to catch a stall promptly. 1s keeps
  // PALF_DUMP close to other async WARN throttles.
  static constexpr int64_t ASYNC_CTX_STAT_PRINT_INTERVAL_US = 1 * 1000 * 1000; // 1s
  // 每个 PALF 一个 producer queue. flush task 受 sliding window 限制;
  // 其他类型是 barrier 或元数据维护 task, 数量应保持 O(1), 只预留短时突发容量.
  static constexpr int64_t FLUSH_LOG_TASK_QUEUE_CAPACITY = PALF_SLIDING_WINDOW_SIZE;
  static constexpr int64_t BARRIER_TASK_QUEUE_CAPACITY = 128;
  static constexpr int64_t TASK_QUEUE_CAPACITY =
      FLUSH_LOG_TASK_QUEUE_CAPACITY + BARRIER_TASK_QUEUE_CAPACITY;
  static constexpr int64_t POPPED_TASK_TRACE_CAPACITY = 128;
  static constexpr int64_t POPPED_TASK_TRACE_PRINT_INTERVAL_US = 5 * 1000 * 1000; // 5s

  // public 接口通过 qsync 隔离 destroy/unregister 与 worker、producer、
  // AIO callback 的正常推进路径.
  mutable common::ObQSyncLock qsync_lock_;
  bool is_inited_;
  int64_t palf_id_;
  int cb_tg_id_;
  IPalfEnvImpl *palf_env_impl_;
  // Pins PalfHandleImpl until unregister destroys this ctx. The pin protects
  // memory lifetime only; check_can_be_used() still controls whether work may run.
  IPalfHandleImplGuard palf_handle_guard_;
  IAsyncDriveWaker *drive_waker_;

  common::ObFIFOAllocator task_queue_allocator_;
  // Queue items use an object pool to avoid one malloc/free per task.
  common::ObSmallObjPool<AsyncQueueItem> task_queue_item_pool_;
  // One FIFO preserves the original order across flush and barrier tasks. A
  // successful enqueue transfers the payload task to this ctx.
  common::ObFixedQueue<AsyncQueueItem> task_queue_;
  // Physical AIO attempts not yet closed by callback or worker polling.
  int64_t inflight_aio_cnt_;
  // Available credits for tasks owned by LogAsyncIOWorker or this ctx.
  int64_t available_flush_task_slot_count_;
  int64_t available_barrier_task_slot_count_;
  // producer、worker 和 callback 在无锁调用 ctx 前增加 pin. unregister 自身
  // 持有一个基准 pin, 额外 pin 未释放时不能销毁 ctx.
  int64_t active_ref_;

  // Diagnostic counters and timestamps. These are not state-machine inputs
  // except throttle_next_admit_ts_, which the owning async worker reads when
  // choosing the next wake-up deadline.
  int64_t submit_fail_cnt_;
  int64_t complete_fail_cnt_;
  int64_t stale_completion_cnt_;
  int64_t published_entry_cnt_;
  int64_t block_switch_pending_since_ts_;
  int64_t throttle_error_count_;
  int64_t last_throttle_error_ts_;
  int64_t throttle_next_admit_ts_;
  int64_t throttle_wait_until_ts_;

  PalfPerfItem admit_bytes_;
  PalfPerfItem publish_bytes_;
  PalfPerfItem task_publish_us_;
  PalfPerfItem submit_cnt_;
  PalfPerfItem submit_fail_perf_cnt_;
  PalfPerfItem submit_logical_bytes_;
  PalfPerfItem submit_aio_bytes_;
  PalfPerfItem complete_cnt_;
  PalfPerfItem complete_bytes_;
  PalfPerfItem aio_rt_us_;
  PalfPerfItem fragment_recycle_delay_us_;
  PalfPerfItem wait_parent_wake_cnt_;
  PalfPerfItem wait_parent_wait_us_;
  PalfPerfItem wait_parent_data_bytes_;
  PalfPerfItem throttle_block_cnt_;
  PalfPerfItem throttle_block_us_;
  PalfPerfReporter perf_reporter_;
  int64_t last_stat_print_ts_;
  int64_t last_stat_print_interval_check_ts_;
  int64_t last_errsim_options_refresh_ts_;
  int64_t aio_delay_us_;
  int64_t stat_print_interval_us_;
  int64_t last_server_event_ts_;

  // -------- Fragment pool and write planner ----------
  PhysicalWriteFragmentPool pool_;
  // Worker-only ordering and fragment-planning layer. It borrows pool_, owns no
  // lock, and is declared after pool_ so the reference is valid during its
  // complete lifetime.
  LogAsyncWritePlanner planner_;
  // Set after the current block reaches its boundary and cleared after the
  // storage layer prepares the next block.
  bool block_switch_pending_;
  // 当前允许提交物理 fragment 的 block. 位点处于边界且 switch pending 时仍
  // 属于左侧旧 block; storage 完成 switch 后才切换到右侧新 block.
  block_id_t current_write_block_id_;
  // 等待前序异步 flush 阶段排空的第一条非 FLUSH_LOG task. ctx 持有其所有权,
  // 直到 do_task 将它转交 callback pool 或失败路径释放.
  LogIOTask *control_barrier_task_;

  // Ring buffer for the latest tasks popped from task_queue_. This is only for
  // diagnosis; old records are overwritten when the fixed array is full.
  PoppedTaskTraceEntry popped_task_trace_[POPPED_TASK_TRACE_CAPACITY];
  int64_t popped_task_trace_next_idx_;
  int64_t popped_task_trace_cnt_;
  int64_t last_popped_task_trace_print_ts_;

  // 异步路径不能像同步 LogIOWorker 一样 sleep, 因此在 admission 阶段只记录
  // tenant throttle deadline. 限流期间暂停接收新 flush task, 但 completion、
  // publish、retry 和 block switch 仍可继续. 上下文在注册前设置, 之后只读.
  AsyncThrottleContext throttle_ctx_;
  // deadline 到期后的首轮 admission 跳过一次限流计算, 随后清除此标记.
  // 这样等价于同步路径 sleep 结束后至少提交一轮, 避免持续重新限流而不推进.
  bool ignore_throttle_once_;

private:
  DISALLOW_COPY_AND_ASSIGN(AsyncPalfIOCtx);
};

} // end namespace palf
} // end namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_ASYNC_PALF_CTX_
