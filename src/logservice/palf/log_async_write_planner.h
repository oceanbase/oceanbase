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

#ifndef OCEANBASE_LOGSERVICE_LOG_ASYNC_WRITE_PLANNER_
#define OCEANBASE_LOGSERVICE_LOG_ASYNC_WRITE_PLANNER_

#include <stdint.h>
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/objectpool/ob_small_obj_pool.h"
#include "log_async_fragment.h"
#include "log_async_palf_ctx_diag.h"

namespace oceanbase
{
namespace palf
{
class LogIOTask;
class LogIOFlushLogTask;
struct LogWriteBuf;
class IPalfHandleImpl;
class IPalfEnvImpl;

// Planner control-plane snapshot. The planner owns these state variables; ctx
// only reads this snapshot when deciding or printing.
struct PlannerStatus
{
  PlannerStatus();
  void reset();
  void init(const LSN &planned_end_lsn,
            const LSN &persisted_lsn,
            const int64_t pending_task_count,
            const int64_t active_fragment_count,
            const bool has_pending_source);
  const LSN &get_planned_end_lsn() const { return planned_end_lsn_; }
  const LSN &get_persisted_lsn() const { return persisted_lsn_; }
  int64_t get_pending_task_count() const { return pending_task_count_; }
  int64_t get_active_fragment_count() const { return active_fragment_count_; }
  bool has_pending_source() const { return has_pending_source_; }

private:
  LSN planned_end_lsn_;
  LSN persisted_lsn_;
  int64_t pending_task_count_;
  int64_t active_fragment_count_;
  bool has_pending_source_;

public:
  TO_STRING_KV(K_(planned_end_lsn), K_(persisted_lsn), K_(pending_task_count),
               K_(active_fragment_count), K_(has_pending_source));
};

// 每个 PALF 一个异步写规划器. FLUSH_LOG task 按 LSN 顺序进入后, planner
// 最多保留 group buffer 暴露的两段物理内存, 将连续数据规划为 fragment,
// 再按 fragment_ref_queue_ 的 LSN 顺序推进持久化前缀. task 只有在其完整
// LSN 区间被 persisted_lsn_ 覆盖后才允许 publish.
class LogAsyncWritePlanner
{
private:
  static const int64_t TASK_QUEUE_CAPACITY = PALF_SLIDING_WINDOW_SIZE + 128;
  static const int64_t FRAGMENT_REF_QUEUE_SLACK = 256;
  // Extra fragment refs created by splitting one group-buffer window must fit
  // in FRAGMENT_REF_QUEUE_SLACK. The current values give 40 pages (160KB).
  static const int64_t MIN_FRAGMENT_MAX_SIZE =
      FOLLOWER_DEFAULT_GROUP_BUFFER_SIZE / FRAGMENT_REF_QUEUE_SLACK;
  // slot 回收后, FragmentRef 仍要保留到 persisted prefix 推过对应区间, 因此
  // 队列容量不能按 slot 数量计算. 每个 flush task 最多产生一个 wait-parent
  // fragment 和一个 normal fragment; FRAGMENT_REF_QUEUE_SLACK 为环形内存
  // 跨尾及调度偏差预留余量.
  static const int64_t FRAGMENT_REF_QUEUE_CAPACITY =
      TASK_QUEUE_CAPACITY * 2 + FRAGMENT_REF_QUEUE_SLACK;
  static const int64_t PENDING_SOURCE_CNT = 2;
  static const int64_t FRAGMENT_REF_TRACE_CAPACITY = 128;
  static const int64_t FRAGMENT_REF_TRACE_PRINT_INTERVAL_US = 5 * 1000 * 1000; // 5s

  // 将一段逻辑 LSN 与对应物理内存绑定. fragment 规划成功后 consume()
  // 同步推进 LSN、指针和长度, 避免三者状态分离.
  class PendingSourceRange
  {
  public:
    PendingSourceRange();
    int init(const LSN &begin_lsn, const char *buf, int64_t len);
    int append(const LSN &begin_lsn, const char *buf, int64_t len);
    int consume(int64_t len);
    void reset();
    bool is_valid() const;
    bool is_empty() const;
    bool can_append(const LSN &begin_lsn, const char *buf, int64_t len) const;
    const LSN &get_begin_lsn() const { return begin_lsn_; }
    LSN get_end_lsn() const;
    const char *get_buf() const { return buf_; }
    int64_t get_len() const { return len_; }
    TO_STRING_KV(K_(begin_lsn), KP_(buf), K_(len));

  private:
    LSN begin_lsn_;
    const char *buf_;
    int64_t len_;
  };

  struct QueuedFragmentRef
  {
    // slot 可在 IO 完成后先回收, 因此队列单独保留 fragment 的 LSN 区间,
    // 供后续按序推进 persisted_lsn_ 使用.
    QueuedFragmentRef() : ref_(), begin_lsn_(), end_lsn_() {}
    void reset()
    {
      ref_.reset();
      begin_lsn_.reset();
      end_lsn_.reset();
    }
    FragmentRef ref_;
    LSN begin_lsn_;
    LSN end_lsn_;
    TO_STRING_KV(K_(ref), K_(begin_lsn), K_(end_lsn));
  };

  // Compact history record for fragment_ref_queue_ admission diagnostics.
  struct QueuedFragmentRefTraceEntry
  {
    QueuedFragmentRefTraceEntry() { reset(); }
    void reset()
    {
      enqueue_ts_ = 0;
      queued_ref_.reset();
    }
    void init(const int64_t enqueue_ts, const QueuedFragmentRef &queued_ref)
    {
      enqueue_ts_ = enqueue_ts;
      queued_ref_ = queued_ref;
    }
    int64_t enqueue_ts_;
    QueuedFragmentRef queued_ref_;
    TO_STRING_KV("ts", enqueue_ts_, "r", queued_ref_.ref_,
                 "b", queued_ref_.begin_lsn_, "e", queued_ref_.end_lsn_);
  };

public:
  explicit LogAsyncWritePlanner(PhysicalWriteFragmentPool &pool);
  ~LogAsyncWritePlanner();

  // Initialize planner queues and allocator under the current tenant MTL.
  int init();
  // 销毁 planner 资源并回到未初始化状态. 调用方必须先排空 task 和
  // fragment-ref 队列; reset() 只报告残留项, 不接管其所有权.
  void reset();

  // Update byte limits for fragments allocated after this call. The normal
  // limit caps WAIT_PARENT as well; existing slots keep their immutable limit.
  int update_fragment_size_limits(const int64_t fragment_max_size,
                                  const int64_t wait_parent_max_size);
  // Update the injected delay between physical AIO completion and planner
  // visibility. Existing FINISHED fragments use the latest value.
  int update_aio_delay(const int64_t aio_delay_us);
  int64_t get_fragment_max_size() const { return fragment_max_size_; }
  int64_t get_wait_parent_max_size() const { return wait_parent_max_size_; }
  int64_t get_aio_delay() const { return aio_delay_us_; }

  // Reset planned/persisted positions to log_tail.
  void init_plan_state(const LSN &log_tail);
  // Mark the planner state invalid so the next flush task refreshes it from storage.
  void invalidate_plan_state();
  // truncate/flashback 后重置规划状态. tail 未对齐时先从磁盘恢复所在 DIO 页
  // 的有效前缀到 group buffer, 再允许后续零拷贝 fragment 规划.
  int reset_after_tail_changed(IPalfHandleImpl *handle, const LSN &log_tail);

  // Admit one flush task into the pending task queue.
  // consumed=false means caller must retry after planner makes progress.
  int admit_task(LogIOTask *task, bool &consumed);

  // Convert pending source ranges into physical fragments until the source is
  // exhausted or the fragment pool has no free slot.
  int plan_pending_tasks();
  // Consume the ordered finished-fragment prefix and advance persisted_lsn_.
  int advance_finished_fragment_prefix();

  // 返回 end_lsn 已被 persisted_lsn_ 覆盖的借用队首 task. 调用
  // pop_published_task() 前, 所有权仍在 pending_task_queue_ 中.
  int peek_publishable_task(LogIOFlushLogTask *&task);
  // Remove the task returned by peek_publishable_task().
  void pop_published_task();
  // Drop all unpublished tasks and fragment-planning state after PALF has
  // entered its permanent deleted state. Caller must first drain physical AIO
  // and recycle the fragment pool because task buffers are non-owning sources.
  int discard_all_tasks(IPalfEnvImpl *palf_env_impl,
                        int64_t &discarded_task_count);

  // Copy planner state into a read-only diagnostic snapshot.
  void get_status(PlannerStatus &status) const;
  // Periodically dump the latest fragment refs pushed into fragment_ref_queue_.
  void print_fragment_ref_trace();
private:
  int append_pending_write_buf_(const LSN &begin_lsn, const LogWriteBuf &write_buf);
  int append_pending_source_(const LSN &begin_lsn, const char *buf, int64_t len);
  int plan_source_range_(PendingSourceRange &source);
  int prepare_fragment_for_source_range_(const LSN &begin_lsn, const char *source_buf, int64_t max_len,
                                         FragmentRef &ref, int64_t &planned_len);
  int alloc_new_fragment_(const LSN &begin_lsn, const char *source_buf, int64_t max_len, FragmentRef &ref,
                          int64_t &planned_len);
  int push_fragment_ref_(const FragmentRef &ref, const LSN &begin_lsn, const LSN &end_lsn);
  QueuedFragmentRef *alloc_fragment_ref_item_();
  void free_fragment_ref_item_(QueuedFragmentRef *queued_ref);
  int append_source_range_to_fragment_(PhysicalWriteFragment &fragment, const FragmentRef &ref,
                                       const LSN &begin_lsn, const char *source_buf, int64_t max_len,
                                       int64_t &planned_len);
  int get_not_null_fragment_(const FragmentRef &ref, PhysicalWriteFragment *&fragment);
  int peek_head_fragment_for_advance_(QueuedFragmentRef *&queued_ref, PhysicalWriteFragment *&fragment);
  void reset_fragment_ref_trace_();
  void record_fragment_ref_trace_(const QueuedFragmentRef &queued_ref);
  int get_appendable_fragment_(PhysicalWriteFragment *&fragment);
  LSN get_planned_end_lsn_() const;
  bool is_valid_() const;
  int need_wait_parent_for_tail_page_(const PhysicalWriteFragment &parent,
                                      const LSN &child_begin_lsn,
                                      bool &need_wait_parent) const;
  int is_fragment_finish_visible_(const PhysicalWriteFragment &fragment,
                                  bool &is_visible) const;
  int check_queue_empty_() const;

private:
  PhysicalWriteFragmentPool &pool_;
  bool inited_;
  int64_t fragment_max_size_;
  int64_t wait_parent_max_size_;
  int64_t aio_delay_us_;
  common::ObFIFOAllocator queue_allocator_;
  common::ObSmallObjPool<QueuedFragmentRef> fragment_ref_pool_;
  LSN persisted_lsn_;
  common::ObFixedQueue<LogIOFlushLogTask> pending_task_queue_;
  common::ObFixedQueue<QueuedFragmentRef> fragment_ref_queue_;
  // last_fragment_ref_ 用于定位仍存活的尾 fragment; 对应 slot 回收后,
  // last_fragment_ref_item_ 仍保留并维护队列中的 begin/end, 直到该记录出队.
  FragmentRef last_fragment_ref_;
  QueuedFragmentRef *last_fragment_ref_item_;
  QueuedFragmentRefTraceEntry fragment_ref_trace_[FRAGMENT_REF_TRACE_CAPACITY];
  int64_t fragment_ref_trace_next_idx_;
  int64_t fragment_ref_trace_cnt_;
  int64_t last_fragment_ref_trace_print_ts_;
  LSN queue_end_lsn_;
  PendingSourceRange pending_sources_[PENDING_SOURCE_CNT];

  DISALLOW_COPY_AND_ASSIGN(LogAsyncWritePlanner);
};

} // namespace palf
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_ASYNC_WRITE_PLANNER_
