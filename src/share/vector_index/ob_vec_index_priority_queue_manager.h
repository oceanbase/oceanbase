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

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_OB_VEC_INDEX_PRIORITY_QUEUE_MANAGER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_OB_VEC_INDEX_PRIORITY_QUEUE_MANAGER_H_

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/queue/ob_link_queue.h"
#include "share/ob_ls_id.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"

namespace oceanbase
{
namespace share
{

// Priority levels for vector index async tasks.
// P0 is the highest (always schedulable); P5 is the lowest (only when threads are idle).
// See get_priority_by_task_type() for the mapping from task type to priority level.
// PRIORITY_MAX is the number of schedulable levels and is also the invalid sentinel for
// get_auto_priority_by_task_type() when the task type has no AUTO priority.
enum ObVecIndexTaskPriority {
  PRIORITY_P0 = 0,  // reserved for manual trigger (DBMS_VECTOR.TRIGGER_ASYNC_TASK), uses manual_queue_
  PRIORITY_P1,
  PRIORITY_P2,
  PRIORITY_P3,
  PRIORITY_P4,
  PRIORITY_P5,
  PRIORITY_MAX
};

// Water-level threshold (%) for each priority level.
// A task at level P can be scheduled only when: running_count * 100 / thread_count <= threshold[P]
// (integer division, same as ob_vec_idx_async_task_scheduler).
// Tuned for a 12-thread pool so P0..P4 cap total running tasks at about 12/10/8/6/4
// (water_level = running*100/thread_count, integer division). P1/P2 use 85/70 for round numbers.
// P5 is reserved for lower tiers (20% ≈ ~2-thread equivalent on a 12-thread pool).
extern const int64_t WATER_LEVEL_THRESHOLD[PRIORITY_MAX];

// Default maximum number of tasks allowed in each queue.
// Some task types may override this in get_task_type_queue_max_size().
// Exceeding the effective max size causes push() to return OB_SIZE_OVERFLOW.
extern const int64_t PRIORITY_QUEUE_MAX_SIZE[PRIORITY_MAX];

// Map a task type + trigger type to a scheduling priority.
// MANUAL trigger always gets PRIORITY_P0 (highest, user-triggered).
// AUTO trigger matches ObVecIndexAsyncTaskType comments in ob_vector_index_async_task_util.h:
// IVF_LOAD/MEM_SYNC -> P1; IVF_CLEAN/FREEZE/MERGE -> P2; HYBRID_EMBEDDING -> P3; OPTIONAL -> P4;
// BUILT / unknown / OB_VECTOR_ASYNC_TASK_TYPE_INVALID -> PRIORITY_MAX (invalid, not scheduled).
ObVecIndexTaskPriority get_priority_by_task_type(ObVecIndexAsyncTaskType task_type,
                                                  int64_t trigger_type);

// Get the AUTO-trigger priority for a task type (ignoring trigger type).
ObVecIndexTaskPriority get_auto_priority_by_task_type(ObVecIndexAsyncTaskType task_type);

// Wrapper node for the priority queue. ObVecIndexAsyncTaskCtx is no longer ObLink;
// each enqueue allocates one node. LS switchover marks is_valid_ false, nulls ctx_
// on the node and queue_node_ on the ctx, and decrements ls_queued_task_cnt_.
// pop()/drain() consumes invalid nodes (ctx_==nullptr) without returning them.
struct ObVecIndexQueueNode : public common::ObLink
{
  ObVecIndexAsyncTaskCtx *ctx_;
  bool is_valid_;
  ObLSID ls_id_;
  ObVecIndexQueueNode() : common::ObLink(), ctx_(nullptr), is_valid_(true), ls_id_() {}
};

struct ObVecIndexQueuePopDiag
{
  ObVecIndexQueuePopDiag() { reset(); }
  void reset();
  DECLARE_TO_STRING;

  int64_t total_queued_;
  int64_t manual_queued_;
  int64_t queue_by_priority_[PRIORITY_MAX];
  int64_t queue_by_type_[OB_VECTOR_ASYNC_TASK_TYPE_INVALID];
  int64_t raw_threshold_[OB_VECTOR_ASYNC_TASK_TYPE_INVALID];
  bool configured_[OB_VECTOR_ASYNC_TASK_TYPE_INVALID];
  int64_t effective_threshold_[OB_VECTOR_ASYNC_TASK_TYPE_INVALID];

  bool has_queued_;
  bool blocked_by_water_level_;
  bool has_schedulable_type_;

  int64_t water_level_;
  int64_t min_queued_effective_threshold_;
  int64_t max_queued_effective_threshold_;

  ObVecIndexTaskPriority first_blocked_priority_;
  ObVecIndexAsyncTaskType first_blocked_task_type_;
  int64_t first_blocked_type_queued_;
  int64_t first_blocked_effective_threshold_;

  ObVecIndexTaskPriority first_schedulable_priority_;
  ObVecIndexAsyncTaskType first_schedulable_task_type_;
  int64_t first_schedulable_type_queued_;
  int64_t first_schedulable_effective_threshold_;
};

// Per-task-type priority queue manager for vector index async task contexts.
//
// Design:
//   - One FIFO sub-queue per task type (OB_VECTOR_ASYNC_TASK_TYPE_INVALID queues).
//   - One additional manual_queue_ for MANUAL-triggered tasks (always P0, threshold 100%).
//   - pop() iterates priorities P0→P5; within each priority, round-robin across task types
//     belonging to that priority. Each task type queue has an independent water-level threshold
//     (defaulting to the priority-based threshold, overridable via config).
//   - Water level = running_count * 100 / thread_count (0–100%).
//   - Not thread-safe; designed to be accessed only from the single-threaded
//     vector-index async-task scheduler timer task.
class ObVecIndexPriorityQueueManager
{
public:
  ObVecIndexPriorityQueueManager();
  ~ObVecIndexPriorityQueueManager() = default;

  int init(uint64_t tenant_id);
  void destroy();
  void drain();

  // Enqueue an AUTO-triggered task into its task-type queue.
  int push(ObVecIndexAsyncTaskCtx *ctx, ObVecIndexAsyncTaskType task_type);
  // Enqueue a MANUAL-triggered task into the manual queue (P0, threshold 100%).
  int push_manual(ObVecIndexAsyncTaskCtx *ctx);

  // Dequeue the highest-priority schedulable task.
  // First tries manual_queue_ (P0, always schedulable), then iterates P0→P5
  // round-robin across per-task-type queues, respecting per-type water-level thresholds.
  int pop(ObVecIndexAsyncTaskCtx *&task_ctx, int64_t water_level, ObLSID *pop_ls_id = nullptr);

  // Parse config string and update per-task-type water_level_threshold_.
  // Config format: 'TASK_TYPE:PERCENT,...'. Unset types revert to default.
  int refresh_water_level_config(const char *config_str);

  // Recompute effective per-task-type water-level thresholds for a given thread-pool size.
  // Called whenever max thread count changes (CPU scale-up/down or initial start).
  // Each priority's allowed concurrent slots = max(min_slots[p], floor(max_threads * pct[p]/100)),
  // then converted back to a percentage for use in pop().
  // Caller must ensure single-threaded access (scheduler timer thread only).
  // When force=true, recompute even if max_threads equals the cached value.
  void update_thread_limit(int64_t max_threads, bool force = false);

  int64_t get_queued_count(ObVecIndexAsyncTaskType task_type) const;
  int64_t get_queued_count_by_priority(ObVecIndexTaskPriority priority) const;
  int64_t get_total_queued_count() const;
  void get_pop_diag(int64_t water_level, ObVecIndexQueuePopDiag &diag) const;

  TO_STRING_KV(K_(tenant_id));

private:
  // Get default water-level threshold for a task type (based on its AUTO priority).
  static int64_t get_default_water_level_threshold(ObVecIndexAsyncTaskType type);
  // Get max queue size for a task type (based on its AUTO priority).
  static int64_t get_task_type_queue_max_size(ObVecIndexAsyncTaskType type);
  // Build priority_to_task_types_ mapping.
  void build_priority_to_task_types_map();
  // Internal pop helper: pop one valid node from queue, free invalid nodes.
  int pop_one_from_queue(common::ObSimpleLinkQueue &queue, int64_t &queue_size,
                         ObVecIndexAsyncTaskCtx *&task_ctx, ObLSID *pop_ls_id);

  uint64_t tenant_id_;
  common::ObFIFOAllocator queue_node_allocator_;

  // Per-task-type queues (for AUTO-triggered tasks)
  common::ObSimpleLinkQueue queue_[OB_VECTOR_ASYNC_TASK_TYPE_INVALID];
  int64_t queue_size_[OB_VECTOR_ASYNC_TASK_TYPE_INVALID];

  // Per-task-type water-level thresholds (overridable via config string).
  // These are the raw user/default percentage values before CPU-size clamping.
  int64_t water_level_threshold_[OB_VECTOR_ASYNC_TASK_TYPE_INVALID];
  // Whether water_level_threshold_[task_type] comes from an explicit user config.
  bool water_level_threshold_configured_[OB_VECTOR_ASYNC_TASK_TYPE_INVALID];

  // Effective per-task-type water-level thresholds after applying update_thread_limit().
  // pop() uses these instead of water_level_threshold_ directly, so that small thread
  // pools automatically enforce absolute minimum-slot guarantees per priority.
  // If a task type is explicitly configured, the effective value is based on
  // max(user_config, min_slots[p]) instead of the priority default threshold.
  int64_t effective_threshold_[OB_VECTOR_ASYNC_TASK_TYPE_INVALID];

  // Last thread count seen; used to detect changes and trigger update_thread_limit().
  int64_t max_threads_;

  // Hash of last parsed water level config string; skip re-parsing when unchanged.
  uint64_t last_water_level_hash_;

  // Manual-trigger queue (always P0, threshold 100%)
  common::ObSimpleLinkQueue manual_queue_;
  int64_t manual_queue_size_;

  // Priority → task types mapping for round-robin pop
  ObVecIndexAsyncTaskType priority_to_task_types_[PRIORITY_MAX][OB_VECTOR_ASYNC_TASK_TYPE_INVALID];
  int64_t priority_task_type_count_[PRIORITY_MAX];
  // Round-robin index per priority level
  int64_t rr_index_[PRIORITY_MAX];

  DISALLOW_COPY_AND_ASSIGN(ObVecIndexPriorityQueueManager);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_OB_VEC_INDEX_PRIORITY_QUEUE_MANAGER_H_
