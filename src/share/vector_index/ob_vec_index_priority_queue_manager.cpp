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
#define USING_LOG_PREFIX SERVER

#include "share/vector_index/ob_vec_index_priority_queue_manager.h"
#include "lib/allocator/ob_malloc.h"
#include "storage/ls/ob_ls.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{

const int64_t WATER_LEVEL_THRESHOLD[PRIORITY_MAX] = {100, 85, 70, 50, 35, 20};
const int64_t PRIORITY_QUEUE_MAX_SIZE[PRIORITY_MAX] = {512, 256, 256, 256, 256, 256};

void ObVecIndexQueuePopDiag::reset()
{
  total_queued_ = 0;
  manual_queued_ = 0;
  for (int64_t i = 0; i < PRIORITY_MAX; ++i) {
    queue_by_priority_[i] = 0;
  }
  for (int64_t i = 0; i < OB_VECTOR_ASYNC_TASK_TYPE_INVALID; ++i) {
    queue_by_type_[i] = 0;
    raw_threshold_[i] = 0;
    configured_[i] = false;
    effective_threshold_[i] = 0;
  }
  has_queued_ = false;
  blocked_by_water_level_ = false;
  has_schedulable_type_ = false;
  water_level_ = 0;
  min_queued_effective_threshold_ = -1;
  max_queued_effective_threshold_ = -1;
  first_blocked_priority_ = PRIORITY_MAX;
  first_blocked_task_type_ = OB_VECTOR_ASYNC_TASK_TYPE_INVALID;
  first_blocked_type_queued_ = 0;
  first_blocked_effective_threshold_ = 0;
  first_schedulable_priority_ = PRIORITY_MAX;
  first_schedulable_task_type_ = OB_VECTOR_ASYNC_TASK_TYPE_INVALID;
  first_schedulable_type_queued_ = 0;
  first_schedulable_effective_threshold_ = 0;
}

DEF_TO_STRING(ObVecIndexQueuePopDiag)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(total_queued), K_(manual_queued), K_(has_queued),
       K_(blocked_by_water_level), K_(has_schedulable_type), K_(water_level),
       K_(min_queued_effective_threshold), K_(max_queued_effective_threshold),
       K_(first_blocked_priority), K_(first_blocked_task_type),
       K_(first_blocked_type_queued), K_(first_blocked_effective_threshold),
       K_(first_schedulable_priority), K_(first_schedulable_task_type),
       K_(first_schedulable_type_queued), K_(first_schedulable_effective_threshold));
  J_COMMA();
  BUF_PRINTF("queue_by_priority:[");
  for (int64_t i = 0; i < PRIORITY_MAX; ++i) {
    if (i > 0) {
      BUF_PRINTF(",");
    }
    BUF_PRINTF("P%ld:%ld", i, queue_by_priority_[i]);
  }
  BUF_PRINTF("]");
  J_COMMA();
  BUF_PRINTF("queue_by_type:[");
  bool first = true;
  for (int64_t i = 0; i < OB_VECTOR_ASYNC_TASK_TYPE_INVALID; ++i) {
    if (queue_by_type_[i] > 0) {
      const char *name = ObVecIndexAsyncTaskUtil::get_vec_task_type_short_name(
          static_cast<ObVecIndexAsyncTaskType>(i));
      if (!first) {
        BUF_PRINTF(",");
      }
      first = false;
      BUF_PRINTF("%s:%ld", OB_ISNULL(name) ? "UNKNOWN" : name, queue_by_type_[i]);
    }
  }
  if (first) {
    BUF_PRINTF("empty");
  }
  BUF_PRINTF("]");
  J_COMMA();
  BUF_PRINTF("threshold_by_type:[");
  first = true;
  for (int64_t i = 0; i < OB_VECTOR_ASYNC_TASK_TYPE_INVALID; ++i) {
    const char *name = ObVecIndexAsyncTaskUtil::get_vec_task_type_short_name(
        static_cast<ObVecIndexAsyncTaskType>(i));
    if (OB_NOT_NULL(name)) {
      if (!first) {
        BUF_PRINTF(",");
      }
      first = false;
      BUF_PRINTF("%s:{raw:%ld,configured:%d,effective:%ld}",
                 name, raw_threshold_[i], configured_[i], effective_threshold_[i]);
    }
  }
  if (first) {
    BUF_PRINTF("empty");
  }
  BUF_PRINTF("]");
  J_OBJ_END();
  return pos;
}

// ---------------------------------------------------------------------------
// get_priority_by_task_type
// ---------------------------------------------------------------------------
ObVecIndexTaskPriority get_priority_by_task_type(ObVecIndexAsyncTaskType task_type,
                                                  int64_t trigger_type)
{
  if (trigger_type == OB_VEC_TRIGGER_MANUAL) {
    return PRIORITY_P0;
  }
  return get_auto_priority_by_task_type(task_type);
}

ObVecIndexTaskPriority get_auto_priority_by_task_type(ObVecIndexAsyncTaskType task_type)
{
  ObVecIndexTaskPriority p = PRIORITY_MAX;
  switch (task_type) {
    case OB_VECTOR_ASYNC_INDEX_IVF_LOAD:
    case OB_VECTOR_ASYNC_MEM_SYNC_TASK:
      p = PRIORITY_P1;
      break;
    case OB_VECTOR_ASYNC_INDEX_IVF_CLEAN:
    case OB_VECTOR_ASYNC_INDEX_FREEZE:
    case OB_VECTOR_ASYNC_INDEX_MERGE:
      p = PRIORITY_P2;
      break;
    case OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING:
      p = PRIORITY_P3;
      break;
    case OB_VECTOR_ASYNC_INDEX_OPTINAL:
      p = PRIORITY_P4;
      break;
    case OB_VECTOR_ASYNC_INDEX_BUILT:
    case OB_VECTOR_ASYNC_TASK_TYPE_INVALID:
    default:
      p = PRIORITY_MAX;
      break;
  }
  return p;
}

// ---------------------------------------------------------------------------
// ObVecIndexPriorityQueueManager
// ---------------------------------------------------------------------------

ObVecIndexPriorityQueueManager::ObVecIndexPriorityQueueManager()
    : tenant_id_(OB_INVALID_TENANT_ID),
      queue_size_(),
      water_level_threshold_(),
      water_level_threshold_configured_(),
      effective_threshold_(),
      max_threads_(0),
      last_water_level_hash_(UINT64_MAX),
      manual_queue_size_(0),
      priority_to_task_types_(),
      priority_task_type_count_(),
      rr_index_()
{
}

int64_t ObVecIndexPriorityQueueManager::get_default_water_level_threshold(ObVecIndexAsyncTaskType type)
{
  ObVecIndexTaskPriority p = get_auto_priority_by_task_type(type);
  if (p >= PRIORITY_P0 && p < PRIORITY_MAX) {
    return WATER_LEVEL_THRESHOLD[p];
  }
  return 0;
}

int64_t ObVecIndexPriorityQueueManager::get_task_type_queue_max_size(ObVecIndexAsyncTaskType type)
{
  ObVecIndexTaskPriority p = get_auto_priority_by_task_type(type);
  if (p >= PRIORITY_P0 && p < PRIORITY_MAX) {
    return PRIORITY_QUEUE_MAX_SIZE[p];
  }
  return 0;
}

void ObVecIndexPriorityQueueManager::build_priority_to_task_types_map()
{
  for (int p = 0; p < PRIORITY_MAX; ++p) {
    priority_task_type_count_[p] = 0;
  }
  for (int t = 0; t < OB_VECTOR_ASYNC_TASK_TYPE_INVALID; ++t) {
    ObVecIndexAsyncTaskType task_type = static_cast<ObVecIndexAsyncTaskType>(t);
    ObVecIndexTaskPriority p = get_auto_priority_by_task_type(task_type);
    if (p >= PRIORITY_P0 && p < PRIORITY_MAX) {
      int64_t idx = priority_task_type_count_[p];
      priority_to_task_types_[p][idx] = task_type;
      priority_task_type_count_[p] = idx + 1;
    }
  }
}

int ObVecIndexPriorityQueueManager::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(queue_node_allocator_.init(nullptr, OB_MALLOC_MIDDLE_BLOCK_SIZE,
          lib::ObMemAttr(tenant_id, "VecIdxPriQ")))) {
    LOG_WARN("VecIdxPriQ allocator init failed", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    for (int64_t i = 0; i < OB_VECTOR_ASYNC_TASK_TYPE_INVALID; i++) {
      queue_size_[i] = 0;
      water_level_threshold_[i] = get_default_water_level_threshold(
          static_cast<ObVecIndexAsyncTaskType>(i));
      water_level_threshold_configured_[i] = false;
      effective_threshold_[i] = water_level_threshold_[i];
    }
    max_threads_ = 0;
    manual_queue_size_ = 0;
    for (int p = 0; p < PRIORITY_MAX; ++p) {
      rr_index_[p] = 0;
    }
    build_priority_to_task_types_map();
  }
  return ret;
}

void ObVecIndexPriorityQueueManager::destroy()
{
  drain();
  queue_node_allocator_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  for (int64_t i = 0; i < OB_VECTOR_ASYNC_TASK_TYPE_INVALID; i++) {
    queue_size_[i] = 0;
    water_level_threshold_[i] = 0;
    water_level_threshold_configured_[i] = false;
    effective_threshold_[i] = 0;
  }
  max_threads_ = 0;
  manual_queue_size_ = 0;
}

void ObVecIndexPriorityQueueManager::drain()
{
  // Drain per-task-type queues
  for (int t = 0; t < OB_VECTOR_ASYNC_TASK_TYPE_INVALID; t++) {
    ObLink *link = nullptr;
    while (OB_SUCCESS == queue_[t].pop(link)) {
      ObVecIndexQueueNode *node = static_cast<ObVecIndexQueueNode *>(link);
      if (OB_NOT_NULL(node) && OB_NOT_NULL(node->ctx_)) {
        common::ObSpinLockGuard ctx_guard(node->ctx_->lock_);
        if (node->ctx_->queue_node_ == node) {
          node->ctx_->queue_node_ = nullptr;
          node->ctx_->in_queue_ = false;
        }
      }
      if (OB_NOT_NULL(node)) {
        OB_DELETEx(ObVecIndexQueueNode, &queue_node_allocator_, node);
      }
      link = nullptr;
    }
    queue_size_[t] = 0;
  }
  // Drain manual queue
  {
    ObLink *link = nullptr;
    while (OB_SUCCESS == manual_queue_.pop(link)) {
      ObVecIndexQueueNode *node = static_cast<ObVecIndexQueueNode *>(link);
      if (OB_NOT_NULL(node) && OB_NOT_NULL(node->ctx_)) {
        common::ObSpinLockGuard ctx_guard(node->ctx_->lock_);
        if (node->ctx_->queue_node_ == node) {
          node->ctx_->queue_node_ = nullptr;
          node->ctx_->in_queue_ = false;
        }
      }
      if (OB_NOT_NULL(node)) {
        OB_DELETEx(ObVecIndexQueueNode, &queue_node_allocator_, node);
      }
      link = nullptr;
    }
    manual_queue_size_ = 0;
  }
}

int ObVecIndexPriorityQueueManager::push(ObVecIndexAsyncTaskCtx *ctx,
                                          ObVecIndexAsyncTaskType task_type)
{
  int ret = OB_SUCCESS;
  ObVecIndexQueueNode *node = nullptr;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ctx is null", KR(ret));
  } else if (OB_UNLIKELY(OB_NOT_NULL(ctx->queue_node_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx already in priority queue", KR(ret), KPC(ctx));
  } else if (OB_UNLIKELY(task_type < 0 || task_type >= OB_VECTOR_ASYNC_TASK_TYPE_INVALID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task_type", KR(ret), K(task_type));
  } else if (OB_UNLIKELY(queue_size_[task_type] >= get_task_type_queue_max_size(task_type))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("task type queue is full, task dropped",
             KR(ret), K(task_type), K(queue_size_[task_type]));
  } else if (OB_ISNULL(node = OB_NEWx(ObVecIndexQueueNode, &queue_node_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc queue node", KR(ret));
  } else {
    node->ctx_ = ctx;
    ATOMIC_STORE(&node->is_valid_, true);
    if (OB_NOT_NULL(ctx->get_ls())) {
      node->ls_id_ = ctx->get_ls()->get_ls_id();
    }
    ctx->queue_node_ = node;
    if (OB_FAIL(queue_[task_type].push(node))) {
      LOG_WARN("fail to push task into task type queue", KR(ret), K(task_type), KPC(ctx));
      ctx->queue_node_ = nullptr;
      OB_DELETEx(ObVecIndexQueueNode, &queue_node_allocator_, node);
      node = nullptr;
    } else {
      queue_size_[task_type]++;
    }
  }
  return ret;
}

int ObVecIndexPriorityQueueManager::push_manual(ObVecIndexAsyncTaskCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObVecIndexQueueNode *node = nullptr;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ctx is null", KR(ret));
  } else if (OB_UNLIKELY(OB_NOT_NULL(ctx->queue_node_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx already in priority queue", KR(ret), KPC(ctx));
  } else if (OB_UNLIKELY(manual_queue_size_ >= PRIORITY_QUEUE_MAX_SIZE[PRIORITY_P0])) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("manual queue is full, task dropped", KR(ret), K(manual_queue_size_));
  } else if (OB_ISNULL(node = OB_NEWx(ObVecIndexQueueNode, &queue_node_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc queue node", KR(ret));
  } else {
    node->ctx_ = ctx;
    ATOMIC_STORE(&node->is_valid_, true);
    if (OB_NOT_NULL(ctx->get_ls())) {
      node->ls_id_ = ctx->get_ls()->get_ls_id();
    }
    ctx->queue_node_ = node;
    if (OB_FAIL(manual_queue_.push(node))) {
      LOG_WARN("fail to push task into manual queue", KR(ret), KPC(ctx));
      ctx->queue_node_ = nullptr;
      OB_DELETEx(ObVecIndexQueueNode, &queue_node_allocator_, node);
      node = nullptr;
    } else {
      manual_queue_size_++;
    }
  }
  return ret;
}

int ObVecIndexPriorityQueueManager::pop_one_from_queue(
    common::ObSimpleLinkQueue &queue, int64_t &queue_size,
    ObVecIndexAsyncTaskCtx *&task_ctx, ObLSID *pop_ls_id)
{
  int ret = OB_ENTRY_NOT_EXIST;
  task_ctx = nullptr;
  while (!queue.is_empty() && OB_ISNULL(task_ctx)) {
    ObLink *link = nullptr;
    const int tmp_ret = queue.pop(link);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to pop from sub-queue", K(tmp_ret));
      break;
    }
    if (OB_ISNULL(link)) {
      LOG_WARN("popped null link from non-empty queue");
      break;
    }
    ObVecIndexQueueNode *node = static_cast<ObVecIndexQueueNode *>(link);
    queue_size--;
    ObVecIndexAsyncTaskCtx *ctx = node->ctx_;
    if (OB_ISNULL(ctx)) {
      OB_DELETEx(ObVecIndexQueueNode, &queue_node_allocator_, node);
      LOG_WARN("queue node has null ctx");
      continue;
    }
    if (!ATOMIC_LOAD(&node->is_valid_)) {
      {
        common::ObSpinLockGuard ctx_guard(ctx->lock_);
        if (ctx->queue_node_ == node) {
          ctx->queue_node_ = nullptr;
          ctx->in_queue_ = false;
        }
      }
      OB_DELETEx(ObVecIndexQueueNode, &queue_node_allocator_, node);
      continue;
    }
    {
      common::ObSpinLockGuard ctx_guard(ctx->lock_);
      ctx->queue_node_ = nullptr;
      // in_queue_ stays true here; caller clears it after processing the popped task.
    }
    if (OB_NOT_NULL(pop_ls_id)) {
      *pop_ls_id = node->ls_id_;
    }
    OB_DELETEx(ObVecIndexQueueNode, &queue_node_allocator_, node);
    task_ctx = ctx;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObVecIndexPriorityQueueManager::pop(ObVecIndexAsyncTaskCtx *&task_ctx,
                                         int64_t water_level,
                                         ObLSID *pop_ls_id)
{
  int ret = OB_ENTRY_NOT_EXIST;
  task_ctx = nullptr;
  if (OB_NOT_NULL(pop_ls_id)) {
    pop_ls_id->reset();
  }

  // 1. First try manual queue (P0, threshold 100% - always schedulable)
  if (water_level <= WATER_LEVEL_THRESHOLD[PRIORITY_P0]) {
    ret = pop_one_from_queue(manual_queue_, manual_queue_size_, task_ctx, pop_ls_id);
    if (OB_SUCCESS == ret && OB_NOT_NULL(task_ctx)) {
      return ret;
    }
  }

  // 2. Iterate priorities P0→P5, round-robin within each priority's task types
  ret = OB_ENTRY_NOT_EXIST;
  for (int p = PRIORITY_P0; p < PRIORITY_MAX && OB_ISNULL(task_ctx); p++) {
    const int64_t type_count = priority_task_type_count_[p];
    if (0 == type_count) {
      continue;
    }
    // Try each task type in this priority level, starting from rr_index_
    for (int64_t i = 0; i < type_count && OB_ISNULL(task_ctx); i++) {
      const int64_t idx = (rr_index_[p] + i) % type_count;
      const ObVecIndexAsyncTaskType task_type = priority_to_task_types_[p][idx];
      // Check per-type water level threshold (use effective_threshold_ which accounts for
      // both user config overrides and minimum-slot guarantees for small thread pools)
      if (water_level > effective_threshold_[task_type]) {
        continue;
      }
      if (queue_[task_type].is_empty()) {
        continue;
      }
      ret = pop_one_from_queue(queue_[task_type], queue_size_[task_type], task_ctx, pop_ls_id);
      if (OB_SUCCESS == ret && OB_NOT_NULL(task_ctx)) {
        // Advance round-robin index to next position
        rr_index_[p] = (idx + 1) % type_count;
        return ret;
      }
    }
  }
  return ret;
}

// Recompute effective water-level thresholds for a given thread-pool size.
//
// Thread pool sizing (calc_max_thread_count = clamp(floor(cpu * 0.8), 2, 12)):
//
//   tenant CPU | max_threads
//   -----------+------------
//         1~2  |  2         (MIN_THREAD_COUNT floor)
//         3    |  2         (floor(3*0.8)=2)
//         4    |  3
//         5    |  4
//         6~7  |  4~5
//         8~9  |  6~7
//        10~14 |  8~11
//          15+ | 12         (MAX_THREAD_COUNT cap)
//
// Unconfigured task types use priority defaults:
//   allowed concurrent slots = max(min_slots[p], floor(max_threads * default_pct[p] / 100))
// Explicitly configured task types use user water level with only min_slots fallback:
//   effective threshold = max(user_pct, min_slots[p] * 100 / max_threads)
//
//   Priority | pct  | min_slots | 2 thr | 3 thr | 4 thr | 6 thr | 8 thr | 12 thr
//   ---------+------+-----------+-------+-------+-------+-------+-------+-------
//   P0 100%  |  100 |    2      |  *2   |   3   |   4   |   6   |   8   |  12
//   P1  85%  |   85 |    2      |  *2   |  *2   |   3   |   5   |   6   |  10
//   P2  70%  |   70 |    1      |   1   |   2   |   2   |   4   |   5   |   8
//   P3  50%  |   50 |    1      |   1   |   1   |   2   |   3   |   4   |   6
//   P4  35%  |   35 |    1      |  *1   |  *1   |   1   |   2   |   2   |   4
//   P5  20%  |   20 |    0      |   0   |   0   |   0   |   1   |   1   |   2
//
//   (*) entries where min_slots override the percentage floor
//
// Key behaviors for small tenants:
//   - 2-thread pool: P5 has 0 slots, meaning it only runs when running_count=0 (idle).
//     P0/P1 are guaranteed 2 slots (= all threads), P4 is guaranteed 1 slot via min_slots.
//   - 4-thread pool: all priorities except P5 have at least 1 slot; real differentiation exists.
//   - 12-thread pool: identical to original static WATER_LEVEL_THRESHOLD behavior.
void ObVecIndexPriorityQueueManager::update_thread_limit(int64_t max_threads, bool force /*= false*/)
{
  if (max_threads <= 0) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "update_thread_limit called with non-positive max_threads", K(max_threads));
    return;
  }
  if (!force && max_threads == max_threads_) {
    return;
  }
  max_threads_ = max_threads;
  static const int64_t PRIO_MIN_SLOTS[PRIORITY_MAX] = {2, 2, 1, 1, 1, 0};

  for (int t = 0; t < OB_VECTOR_ASYNC_TASK_TYPE_INVALID; ++t) {
    ObVecIndexTaskPriority p = get_auto_priority_by_task_type(
        static_cast<ObVecIndexAsyncTaskType>(t));
    if (p < PRIORITY_P0 || p >= PRIORITY_MAX) {
      effective_threshold_[t] = 0;
      continue;
    }
    const int64_t min_slot_pct = PRIO_MIN_SLOTS[p] * 100 / max_threads;
    if (water_level_threshold_configured_[t]) {
      const int64_t config_pct = water_level_threshold_[t];
      effective_threshold_[t] = OB_MAX(config_pct, min_slot_pct);
    } else {
      const int64_t slots = OB_MAX(PRIO_MIN_SLOTS[p],
                                   max_threads * WATER_LEVEL_THRESHOLD[p] / 100);
      effective_threshold_[t] = slots * 100 / max_threads;
    }
  }
}

int ObVecIndexPriorityQueueManager::refresh_water_level_config(const char *config_str)
{
  int ret = OB_SUCCESS;
  const uint64_t new_hash = (OB_ISNULL(config_str) || '\0' == *config_str)
      ? 0 : murmurhash(config_str, static_cast<int32_t>(STRLEN(config_str)), 0);
  if (new_hash == last_water_level_hash_) {
    return OB_SUCCESS;
  }
  int64_t tmp_threshold[OB_VECTOR_ASYNC_TASK_TYPE_INVALID];
  bool tmp_configured[OB_VECTOR_ASYNC_TASK_TYPE_INVALID];
  for (int t = 0; t < OB_VECTOR_ASYNC_TASK_TYPE_INVALID; ++t) {
    tmp_threshold[t] = get_default_water_level_threshold(
        static_cast<ObVecIndexAsyncTaskType>(t));
    tmp_configured[t] = false;
  }
  if (OB_ISNULL(config_str) || '\0' == *config_str) {
    // Empty config: use defaults
  } else {
    const int64_t MAX_LEN = 4096;
    const size_t str_len = STRLEN(config_str);
    if (str_len > MAX_LEN) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("vector_task_thread_limit_percent too long", KR(ret), K(str_len));
    } else {
      char buf[MAX_LEN + 1];
      MEMCPY(buf, config_str, str_len);
      buf[str_len] = '\0';
      char *saveptr = nullptr;
      char *token = STRTOK_R(buf, ",", &saveptr);
      while (OB_SUCC(ret) && OB_NOT_NULL(token)) {
        char *colon = STRCHR(token, ':');
        if (OB_ISNULL(colon) || colon == token) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("invalid config token format", KR(ret), K(token));
          break;
        }
        *colon = '\0';
        const char *key = token;
        const char *val_str = colon + 1;
        ObVecIndexAsyncTaskType task_type = OB_VECTOR_ASYNC_TASK_TYPE_INVALID;
        if (OB_FAIL(ObVecIndexAsyncTaskUtil::get_vec_task_type_by_short_name(key, task_type))) {
          LOG_WARN("unknown task type in config", KR(ret), K(key));
        } else {
          char *endptr = nullptr;
          long val = strtol(val_str, &endptr, 10);
          if (OB_ISNULL(endptr) || '\0' != *endptr || val < 0 || val > 100) {
            ret = OB_INVALID_CONFIG;
            LOG_WARN("invalid percent value in config", KR(ret), K(key), K(val_str));
          } else {
            tmp_threshold[task_type] = val;
            tmp_configured[task_type] = true;
          }
        }
        token = STRTOK_R(nullptr, ",", &saveptr);
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int t = 0; t < OB_VECTOR_ASYNC_TASK_TYPE_INVALID; ++t) {
      water_level_threshold_[t] = tmp_threshold[t];
      water_level_threshold_configured_[t] = tmp_configured[t];
    }
    last_water_level_hash_ = new_hash;
  }
  // Re-sync effective thresholds to reflect any config changes.
  const int64_t cur_max = max_threads_ > 0 ? max_threads_ : ObVecIndexAsyncTaskHandler::MAX_THREAD_COUNT;
  update_thread_limit(cur_max, true /*force*/);
  return ret;
}

int64_t ObVecIndexPriorityQueueManager::get_queued_count(ObVecIndexAsyncTaskType task_type) const
{
  if (OB_UNLIKELY(task_type < 0 || task_type >= OB_VECTOR_ASYNC_TASK_TYPE_INVALID)) {
    return 0;
  }
  return queue_size_[task_type];
}

int64_t ObVecIndexPriorityQueueManager::get_queued_count_by_priority(
    ObVecIndexTaskPriority priority) const
{
  if (OB_UNLIKELY(priority < PRIORITY_P0 || priority >= PRIORITY_MAX)) {
    return 0;
  }
  int64_t total = 0;
  if (PRIORITY_P0 == priority) {
    total += manual_queue_size_;
  }
  for (int64_t i = 0; i < priority_task_type_count_[priority]; ++i) {
    total += queue_size_[priority_to_task_types_[priority][i]];
  }
  return total;
}

int64_t ObVecIndexPriorityQueueManager::get_total_queued_count() const
{
  int64_t total = manual_queue_size_;
  for (int64_t i = 0; i < OB_VECTOR_ASYNC_TASK_TYPE_INVALID; i++) {
    total += queue_size_[i];
  }
  return total;
}

void ObVecIndexPriorityQueueManager::get_pop_diag(
    int64_t water_level,
    ObVecIndexQueuePopDiag &diag) const
{
  diag.reset();
  diag.water_level_ = water_level;
  diag.manual_queued_ = manual_queue_size_;
  diag.total_queued_ = manual_queue_size_;
  diag.queue_by_priority_[PRIORITY_P0] = manual_queue_size_;

  if (manual_queue_size_ > 0) {
    diag.min_queued_effective_threshold_ = WATER_LEVEL_THRESHOLD[PRIORITY_P0];
    diag.max_queued_effective_threshold_ = WATER_LEVEL_THRESHOLD[PRIORITY_P0];
    if (water_level <= WATER_LEVEL_THRESHOLD[PRIORITY_P0]) {
      diag.has_schedulable_type_ = true;
      diag.first_schedulable_priority_ = PRIORITY_P0;
      diag.first_schedulable_task_type_ = OB_VECTOR_ASYNC_TASK_TYPE_INVALID;
      diag.first_schedulable_type_queued_ = manual_queue_size_;
      diag.first_schedulable_effective_threshold_ = WATER_LEVEL_THRESHOLD[PRIORITY_P0];
    } else {
      diag.first_blocked_priority_ = PRIORITY_P0;
      diag.first_blocked_task_type_ = OB_VECTOR_ASYNC_TASK_TYPE_INVALID;
      diag.first_blocked_type_queued_ = manual_queue_size_;
      diag.first_blocked_effective_threshold_ = WATER_LEVEL_THRESHOLD[PRIORITY_P0];
    }
  }

  for (int64_t t = 0; t < OB_VECTOR_ASYNC_TASK_TYPE_INVALID; ++t) {
    ObVecIndexAsyncTaskType task_type = static_cast<ObVecIndexAsyncTaskType>(t);
    ObVecIndexTaskPriority priority = get_auto_priority_by_task_type(task_type);
    diag.queue_by_type_[t] = queue_size_[t];
    diag.raw_threshold_[t] = water_level_threshold_[t];
    diag.configured_[t] = water_level_threshold_configured_[t];
    diag.effective_threshold_[t] = effective_threshold_[t];
    diag.total_queued_ += queue_size_[t];
    if (priority >= PRIORITY_P0 && priority < PRIORITY_MAX) {
      diag.queue_by_priority_[priority] += queue_size_[t];
    }
    if (queue_size_[t] <= 0) {
      continue;
    }

    const int64_t threshold = effective_threshold_[t];
    if (diag.min_queued_effective_threshold_ < 0
        || threshold < diag.min_queued_effective_threshold_) {
      diag.min_queued_effective_threshold_ = threshold;
    }
    if (diag.max_queued_effective_threshold_ < 0
        || threshold > diag.max_queued_effective_threshold_) {
      diag.max_queued_effective_threshold_ = threshold;
    }
    if (water_level <= threshold) {
      if (!diag.has_schedulable_type_) {
        diag.first_schedulable_priority_ = priority;
        diag.first_schedulable_task_type_ = task_type;
        diag.first_schedulable_type_queued_ = queue_size_[t];
        diag.first_schedulable_effective_threshold_ = threshold;
      }
      diag.has_schedulable_type_ = true;
    } else if (diag.first_blocked_priority_ == PRIORITY_MAX) {
      diag.first_blocked_priority_ = priority;
      diag.first_blocked_task_type_ = task_type;
      diag.first_blocked_type_queued_ = queue_size_[t];
      diag.first_blocked_effective_threshold_ = threshold;
    }
  }
  diag.has_queued_ = diag.total_queued_ > 0;
  diag.blocked_by_water_level_ = diag.has_queued_ && !diag.has_schedulable_type_;
}

} // namespace share
} // namespace oceanbase
