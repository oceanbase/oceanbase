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

#ifndef OCEANBASE_LOGSERVICE_LOG_ASYNC_FRAGMENT_
#define OCEANBASE_LOGSERVICE_LOG_ASYNC_FRAGMENT_

#include <stdint.h>
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "share/io/ob_io_define.h"        // ObIOHandle (keeps the AIO out-ref alive)
#include "log_async_io_struct.h"
#include "log_async_palf_ctx_diag.h"
#include "log_define.h"
#include "lsn.h"

namespace oceanbase
{
namespace palf
{
// AsyncPalfIOCtx 固定 slot 池中的物理 AIO 单元.
//
// pool 初始化 slot 身份; 每次分配只借用上层 group buffer 的 DIO 源地址,
// 并通过显式状态接口推进生命周期. slot 回收后 generation 递增, 旧 callback
// 不能误操作复用后的 fragment.
//
// State transitions:
//   FREE -> READY -> SUBMITTED -> FINISHED -> FREE
//   SUBMITTED -> FAILED -> SUBMITTED (retry)
//   FREE -> WAIT_PARENT -> READY (after parent FINISHED)
class PhysicalWriteFragment
{
public:
  PhysicalWriteFragment();
  ~PhysicalWriteFragment();

  // Reset to FREE and release the parked ObIOHandle. Caller must first ensure
  // the AIO completion has been observed or all inflight AIO has been drained.
  void reset();

  FragmentRef get_fragment_ref() const;
  // Generation-checked state transitions. mark_io_completed() is the
  // close-once gate shared by callback and worker-poll completion paths.
  int mark_ready(const FragmentRef &ref);
  int mark_submitted(const FragmentRef &ref, int64_t submit_ts);
  int mark_failed(const FragmentRef &ref, const int ret_code, const int64_t next_retry_ts);
  // 只有当前调用赢得 close-once 竞争时 completed_by_me 才为 true;
  // 此时 completed_data_len 和 submit_ts 才是本次 AIO 的有效统计值.
  int mark_io_completed(const FragmentRef &ref,
                        const int ret_code,
                        const int64_t next_retry_ts,
                        const int64_t finish_ts,
                        bool &completed_by_me,
                        int64_t &completed_data_len,
                        int64_t &submit_ts);
  AsyncFragmentState get_state() const
  {
    common::ObSpinLockGuard guard(lock_);
    return state_;
  }
  bool is_free() const { return AsyncFragmentState::FREE == get_state(); }
  bool is_state(const AsyncFragmentState state) const { return state == get_state(); }
  bool is_finished() const { return AsyncFragmentState::FINISHED == get_state(); }
  bool is_ready() const { return AsyncFragmentState::READY == get_state(); }
  bool is_failed() const { return AsyncFragmentState::FAILED == get_state(); }
  bool is_submitted() const { return AsyncFragmentState::SUBMITTED == get_state(); }
  bool is_wait_parent() const { return AsyncFragmentState::WAIT_PARENT == get_state(); }
  bool is_appendable() const { return is_wait_parent(); }
  bool is_data_valid() const;

  // Non-owning source pointer into the upper log group buffer.
  const char *get_buf() const { return buf_; }
  int get_data_len(int64_t &data_len) const;
  int64_t get_fragment_max_size() const { return fragment_max_size_; }
  int get_wait_parent_stat(const int64_t now_us, int64_t &wait_us, int64_t &data_len) const;
  // Extend a WAIT_PARENT fragment with the next contiguous LSN and memory range.
  int append_source(const LSN &begin_lsn,
                    int64_t len,
                    const char *buf);

  // LSN range covered by this fragment.
  LSN get_begin_lsn() const { return begin_lsn_; }
  LSN get_end_lsn() const { return end_lsn_; }

  // Parent dependency (child fragments only).
  FragmentRef get_parent_ref() const
  {
    common::ObSpinLockGuard guard(lock_);
    return parent_;
  }

  // Remaining bytes from this fragment's end_lsn to its aligned buffer end.
  int get_appendable_data_len(int64_t &appendable_len) const;

  // Result / retry bookkeeping.
  int get_ret_code() const
  {
    common::ObSpinLockGuard guard(lock_);
    return ret_code_;
  }
  // Earliest timestamp (us) at which a FAILED fragment may be resubmitted.
  int64_t get_next_retry_ts() const
  {
    common::ObSpinLockGuard guard(lock_);
    return next_retry_ts_;
  }
  int64_t get_submit_ts() const
  {
    common::ObSpinLockGuard guard(lock_);
    return submit_ts_;
  }
  int64_t get_finish_ts() const
  {
    common::ObSpinLockGuard guard(lock_);
    return finish_ts_;
  }
  // Return the remaining injected delay before a FINISHED fragment may be
  // observed by the planner or recycled by the pool.
  int get_remaining_finish_delay(const int64_t now_us,
                                 const int64_t aio_delay_us,
                                 int64_t &remaining_delay_us) const;
  int64_t get_generation() const
  {
    common::ObSpinLockGuard guard(lock_);
    return generation_;
  }

  // SUBMITTED 期间 fragment 持有 ObIOHandle 的 out-ref, 避免提交函数返回后
  // handle 析构导致 AIO 被取消且 callback 不再执行. callback 线程不能 reset
  // 这个 handle, 否则可能在 inner_process 尚未返回时释放 callback 对象;
  // 只能由 worker 在观察到完成结果后统一 reset.
  common::ObIOHandle &get_io_handle() { return io_handle_; }
  void reset_io_handle() { io_handle_.reset(); }

  DECLARE_TO_STRING;

  // Pool-owned lifecycle operations. These are intentionally narrow so the
  // pool does not need unrestricted friend access to fragment internals.
  int alloc_from_free(int64_t slot_id, const LSN &begin_lsn, const char *buf, int64_t max_len,
                      int64_t fragment_max_size, const FragmentRef &parent, FragmentRef &ref,
                      int64_t &planned_len);
  int recycle_slot();

private:
  // White-box tests include this header with private remapped to public. Keep
  // these direct mutators out of the production interface.
  void set_state_(const AsyncFragmentState state)
  {
    common::ObSpinLockGuard guard(lock_);
    state_ = state;
  }
  int check_init_source_(const LSN &begin_lsn,
                         const char *buf,
                         int64_t max_len,
                         int64_t fragment_max_size) const;
  int check_append_source_(const LSN &begin_lsn, int64_t len, const char *buf) const;
  int transition_state_locked_(const FragmentRef &ref,
                               const AsyncFragmentState next_state);

  int64_t slot_id_;

  // Protected by lock_ together with FragmentRef validation.
  int64_t generation_;

  // lock_ protects FragmentRef validation and state transitions as one
  // critical section. Do not use it for worker-only range/source fields below.
  mutable common::ObSpinLock lock_;
  // Protected by lock_ together with FragmentRef validation and state
  // transition checks.
  AsyncFragmentState state_;

  // Worker-only fields. They are prepared before SUBMITTED and are not modified
  // by the AIO callback thread.
  const char *buf_;
  LSN begin_lsn_;
  LSN end_lsn_;
  int64_t fragment_max_size_;

  // Protected by lock_ together with WAIT_PARENT/READY transitions.
  FragmentRef parent_;
  int64_t wait_parent_since_ts_;

  // Protected by lock_ together with FAILED/FINISHED transitions.
  int ret_code_;
  int64_t next_retry_ts_;
  // Protected by lock_ together with SUBMITTED/FAILED/FINISHED
  // transitions. It records the current submit attempt for AIO latency stats.
  int64_t submit_ts_;
  // Protected by lock_. A successful AIO completion records the timestamp used
  // by delay injection; FAILED and reusable slots keep it invalid.
  int64_t finish_ts_;

  // In-flight AIO out-ref holder (see get_io_handle). Empty unless SUBMITTED.
  common::ObIOHandle io_handle_;

private:
  DISALLOW_COPY_AND_ASSIGN(PhysicalWriteFragment);
};

// Predicate interface used by PhysicalWriteFragmentPool scans.
class PhysicalWriteFragmentFilter
{
public:
  virtual ~PhysicalWriteFragmentFilter();
  virtual bool operator()(const PhysicalWriteFragment &fragment) const = 0;
};

// Selects fragments in one exact state during a pool scan.
class PhysicalWriteFragmentStateFilter : public PhysicalWriteFragmentFilter
{
public:
  explicit PhysicalWriteFragmentStateFilter(const AsyncFragmentState state);
  virtual ~PhysicalWriteFragmentStateFilter();
  virtual bool operator()(const PhysicalWriteFragment &fragment) const override;

private:
  AsyncFragmentState state_;
};

// 每个 AsyncPalfIOCtx 一个固定大小的 PhysicalWriteFragment slot 池. slot
// 不拥有 DIO buffer, 只借用上层 group buffer 地址. alloc 返回当前 generation
// 对应的 FragmentRef; free 递增 generation, get_fragment() 因而可以识别旧引用.
class PhysicalWriteFragmentPool
{
public:
  PhysicalWriteFragmentPool();
  ~PhysicalWriteFragmentPool();

  // Initializes all slots to FREE.
  int init();
  // Recycles every slot while keeping the pool initialized. Caller must ensure
  // no in-flight AIO is referencing a non-owning source pointer kept by a slot.
  void reuse();
  // Recycles every slot and marks the pool uninitialized.
  void destroy();

  // Allocate the next FREE slot and initialize its source range.
  // State moves FREE -> READY, or FREE -> WAIT_PARENT when parent is valid.
  // Returns OB_SIZE_OVERFLOW when all slots are in use.
  int alloc_slot(const LSN &begin_lsn, const char *buf, int64_t max_len, int64_t fragment_max_size,
                 const FragmentRef &parent, FragmentRef &ref, int64_t &planned_len);

  // Release a slot. Generation is bumped so stale completions hit
  // OB_ENTRY_NOT_EXIST in get_fragment().
  // Returns OB_ENTRY_NOT_EXIST if generation does not match (stale ref).
  // Returns OB_INVALID_ARGUMENT if slot_id is out of range.
  int free_slot(const FragmentRef &ref);

  // completion 路径借用 pool 中的稳定 slot 地址, 但 slot 内容可能并发复用.
  // 后续状态修改仍必须传入 FragmentRef 做 generation 校验; 旧 ref 返回
  // OB_ENTRY_NOT_EXIST.
  int get_fragment(const FragmentRef &ref, PhysicalWriteFragment *&fragment);
  int get_fragment(const FragmentRef &ref, const PhysicalWriteFragment *&fragment) const;

  // Number of slots currently not FREE.
  int64_t get_used_slot_count() const;
  void get_stat(PhysicalWriteFragmentPoolStat &stat) const;
  int64_t get_oldest_pending_io_start_ts() const;
  DECLARE_TO_STRING;
  // Collect slots accepted by filter without transferring slot ownership.
  int collect_fragments(common::ObIArray<PhysicalWriteFragment *> &fragments,
                        const PhysicalWriteFragmentFilter &filter);
  // 唤醒 parent 完成延迟已到期或已回收的 WAIT_PARENT slot, 记录等待开销,
  // 再收集 READY slot.
  int collect_ready_fragments(common::ObIArray<PhysicalWriteFragment *> &fragments,
                              const int64_t aio_delay_us,
                              PalfPerfItem *wait_parent_wake_cnt,
                              PalfPerfItem *wait_parent_wait_us,
                              PalfPerfItem *wait_parent_data_bytes);
  // Recycle FINISHED slots whose injected delay has expired. The planner
  // retains the LSN range needed to advance persisted_lsn_. A non-NULL metric
  // records the elapsed time from physical completion to successful recycle.
  int free_all_finished_fragments(const int64_t aio_delay_us,
                                  PalfPerfItem *fragment_recycle_delay_us);
  // Return the relative interval until the earliest READY, FAILED retry, or
  // FINISHED delay work. INT64_MAX means no timed work is pending.
  int get_next_drive_interval(const int64_t now_us,
                              const int64_t aio_delay_us,
                              int64_t &next_drive_interval_us) const;

private:
  int get_fragment_(const int64_t slot_id,
                    const int64_t generation,
                    const PhysicalWriteFragment *&fragment) const;
  // Check a WAIT_PARENT fragment's own parent ref and transition it to READY
  // once the parent's completion delay expires or it has already been recycled.
  int try_wake_wait_parent_(PhysicalWriteFragment &fragment,
                            const int64_t aio_delay_us,
                            PalfPerfItem *wait_parent_wake_cnt,
                            PalfPerfItem *wait_parent_wait_us,
                            PalfPerfItem *wait_parent_data_bytes,
                            bool &woken);

  bool is_inited_;
  // Slot array. Indexed by slot_id_ directly.
  PhysicalWriteFragment slots_[FRAGMENT_SLOT_CNT_PER_PALF];
  DISALLOW_COPY_AND_ASSIGN(PhysicalWriteFragmentPool);
};

} // end namespace palf
} // end namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_ASYNC_FRAGMENT_
