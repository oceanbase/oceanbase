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

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/vector_index/ob_vec_index_priority_queue_manager.h"
#undef private
#undef protected
#include "lib/alloc/ob_malloc_allocator.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace unittest
{

// ---------------------------------------------------------------------------
// Helper: make a minimal ctx with only the fields push/pop touch
// ---------------------------------------------------------------------------
struct FakeCtx : public share::ObVecIndexAsyncTaskCtx
{
  FakeCtx()
  {
    // ls_handle_ is default-constructed (null ls), get_ls() returns nullptr -> node->ls_id_ skipped
    // queue_node_ is initialized to nullptr in ObVecIndexAsyncTaskCtx ctor
    // in_queue_ is false
  }
};

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class TestVecIndexPriorityQueueManager : public ::testing::Test
{
public:
  TestVecIndexPriorityQueueManager() {}
  virtual ~TestVecIndexPriorityQueueManager() {}

  static void SetUpTestCase()
  {
    // ObFIFOAllocator internally calls ob_malloc which looks up tenant allocator by id.
    // Register a tenant allocator for tenant 1001 so ob_malloc succeeds.
    lib::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
  }

  static void TearDownTestCase()
  {
    lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
  }

  virtual void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, mgr_.init(1001 /* tenant_id */));
  }

  virtual void TearDown() override
  {
    // drain() clears all queues unconditionally (ignores water_level thresholds).
    // Each test is responsible for draining before its local ctxs go out of scope.
    // TearDown only needs to destroy the manager cleanly.
    mgr_.destroy();
  }

protected:
  ObVecIndexPriorityQueueManager mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(TestVecIndexPriorityQueueManager);
};

// ---------------------------------------------------------------------------
// Test: init / destroy / double-init guard
// ---------------------------------------------------------------------------
TEST(TestPriorityQueueManagerBasic, init_destroy)
{
  ObVecIndexPriorityQueueManager mgr;
  // init with valid tenant
  ASSERT_EQ(OB_SUCCESS, mgr.init(1001));
  ASSERT_EQ(0, mgr.get_total_queued_count());
  mgr.destroy();
  // after destroy, total count should still be 0
  ASSERT_EQ(0, mgr.get_total_queued_count());
}

// ---------------------------------------------------------------------------
// Test: push NULL ctx returns OB_INVALID_ARGUMENT
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, push_null_ctx)
{
  int ret = mgr_.push(nullptr, OB_VECTOR_ASYNC_INDEX_FREEZE);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ASSERT_EQ(0, mgr_.get_total_queued_count());
}

// ---------------------------------------------------------------------------
// Test: push invalid task type returns OB_INVALID_ARGUMENT
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, push_invalid_task_type)
{
  FakeCtx ctx;
  int ret = mgr_.push(&ctx, OB_VECTOR_ASYNC_TASK_TYPE_INVALID);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ASSERT_EQ(0, mgr_.get_total_queued_count());
}

// ---------------------------------------------------------------------------
// Test: push task type with no AUTO priority (BUILT) returns OB_SIZE_OVERFLOW
// because get_task_type_queue_max_size returns 0 for PRIORITY_MAX types
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, push_built_type_overflow)
{
  FakeCtx ctx;
  // OB_VECTOR_ASYNC_INDEX_BUILT maps to PRIORITY_MAX -> max_size == 0 -> overflow immediately
  int ret = mgr_.push(&ctx, OB_VECTOR_ASYNC_INDEX_BUILT);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
}

// ---------------------------------------------------------------------------
// Test: task-type queue max sizes
// MEM_SYNC has a larger queue budget; other schedulable task types use default.
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, task_type_queue_max_size)
{
  ASSERT_EQ(10000, mgr_.get_task_type_queue_max_size(OB_VECTOR_ASYNC_MEM_SYNC_TASK));
  ASSERT_EQ(2048, mgr_.get_task_type_queue_max_size(OB_VECTOR_ASYNC_INDEX_IVF_LOAD));
  ASSERT_EQ(2048, mgr_.get_task_type_queue_max_size(OB_VECTOR_ASYNC_INDEX_FREEZE));
  ASSERT_EQ(2048, mgr_.get_task_type_queue_max_size(OB_VECTOR_ASYNC_INDEX_OPTINAL));
  ASSERT_EQ(0, mgr_.get_task_type_queue_max_size(OB_VECTOR_ASYNC_INDEX_BUILT));
  ASSERT_EQ(2048, PRIORITY_QUEUE_MAX_SIZE[PRIORITY_P0]);
}

// ---------------------------------------------------------------------------
// Test: basic push + pop (AUTO, single task)
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, push_and_pop_single)
{
  FakeCtx ctx;
  // push a FREEZE task (P2)
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx, OB_VECTOR_ASYNC_INDEX_FREEZE));
  ASSERT_EQ(1, mgr_.get_queued_count(OB_VECTOR_ASYNC_INDEX_FREEZE));
  ASSERT_EQ(1, mgr_.get_total_queued_count());

  // water_level=0 -> well below any threshold -> should pop
  share::ObVecIndexAsyncTaskCtx *out = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, 0));
  ASSERT_EQ(&ctx, out);
  ASSERT_EQ(0, mgr_.get_total_queued_count());
  ASSERT_EQ(0, mgr_.get_queued_count(OB_VECTOR_ASYNC_INDEX_FREEZE));
  // ctx.queue_node_ must be cleared by pop
  ASSERT_EQ(nullptr, ctx.queue_node_);
}

// ---------------------------------------------------------------------------
// Test: pop from empty queue returns OB_ENTRY_NOT_EXIST
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, pop_empty)
{
  share::ObVecIndexAsyncTaskCtx *out = nullptr;
  int ret = mgr_.pop(out, 0);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ASSERT_EQ(nullptr, out);
}

// ---------------------------------------------------------------------------
// Test: push_manual + pop (manual queue has P0, always schedulable)
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, push_manual_and_pop)
{
  FakeCtx ctx;
  ASSERT_EQ(OB_SUCCESS, mgr_.push_manual(&ctx));
  ASSERT_EQ(1, mgr_.get_total_queued_count());
  // manual queue counted under P0
  ASSERT_EQ(1, mgr_.get_queued_count_by_priority(PRIORITY_P0));

  share::ObVecIndexAsyncTaskCtx *out = nullptr;
  // Even with water_level=100 (max), manual (threshold=100) should pop
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, 100));
  ASSERT_EQ(&ctx, out);
  ASSERT_EQ(0, mgr_.get_total_queued_count());
  ASSERT_EQ(nullptr, ctx.queue_node_);
}

// ---------------------------------------------------------------------------
// Test: water_level blocks lower-priority tasks
// FREEZE is P2 (threshold=70). At water_level=80 it should NOT pop.
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, water_level_blocks_low_priority)
{
  FakeCtx ctx;
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx, OB_VECTOR_ASYNC_INDEX_FREEZE)); // P2, threshold=70

  share::ObVecIndexAsyncTaskCtx *out = nullptr;
  // water_level=80 > 70 -> blocked
  int ret = mgr_.pop(out, 80);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ASSERT_EQ(nullptr, out);
  ASSERT_EQ(1, mgr_.get_total_queued_count()); // still in queue

  // water_level=70 == threshold -> allowed (<=)
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, 70));
  ASSERT_EQ(&ctx, out);
  ASSERT_EQ(0, mgr_.get_total_queued_count());
}

// ---------------------------------------------------------------------------
// Test: priority ordering — manual (P0) is popped before AUTO P2
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, manual_before_auto_priority)
{
  FakeCtx ctx_auto;
  FakeCtx ctx_manual;
  // push AUTO P2 first
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx_auto, OB_VECTOR_ASYNC_INDEX_FREEZE));
  // then push MANUAL
  ASSERT_EQ(OB_SUCCESS, mgr_.push_manual(&ctx_manual));
  ASSERT_EQ(2, mgr_.get_total_queued_count());

  share::ObVecIndexAsyncTaskCtx *out = nullptr;
  // First pop should give manual task
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, 0));
  ASSERT_EQ(&ctx_manual, out);

  // Second pop gives AUTO P2
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, 0));
  ASSERT_EQ(&ctx_auto, out);

  ASSERT_EQ(0, mgr_.get_total_queued_count());
}

// ---------------------------------------------------------------------------
// Test: HIGH priority (P1) is popped before LOW priority (P4)
// IVF_LOAD=P1(threshold=90), OPTINAL=P4(threshold=35)
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, high_priority_before_low)
{
  FakeCtx ctx_low;
  FakeCtx ctx_high;
  // push low priority (P4) first
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx_low, OB_VECTOR_ASYNC_INDEX_OPTINAL));
  // then push high priority (P1)
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx_high, OB_VECTOR_ASYNC_INDEX_IVF_LOAD));
  ASSERT_EQ(2, mgr_.get_total_queued_count());

  share::ObVecIndexAsyncTaskCtx *out = nullptr;
  // First pop: P1 (IVF_LOAD) should come out
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, 0));
  ASSERT_EQ(&ctx_high, out);

  // Second pop: P4 (OPTIONAL)
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, 0));
  ASSERT_EQ(&ctx_low, out);

  ASSERT_EQ(0, mgr_.get_total_queued_count());
}

// ---------------------------------------------------------------------------
// Test: get_priority_by_task_type helper
// ---------------------------------------------------------------------------
TEST(TestPriorityHelpers, get_priority_by_task_type)
{
  // MANUAL trigger always P0
  ASSERT_EQ(PRIORITY_P0,
      get_priority_by_task_type(OB_VECTOR_ASYNC_INDEX_FREEZE, OB_VEC_TRIGGER_MANUAL));
  ASSERT_EQ(PRIORITY_P0,
      get_priority_by_task_type(OB_VECTOR_ASYNC_INDEX_IVF_LOAD, OB_VEC_TRIGGER_MANUAL));

  // AUTO priorities
  ASSERT_EQ(PRIORITY_P1,
      get_priority_by_task_type(OB_VECTOR_ASYNC_INDEX_IVF_LOAD, OB_VEC_TRIGGER_AUTO));
  ASSERT_EQ(PRIORITY_P1,
      get_priority_by_task_type(OB_VECTOR_ASYNC_MEM_SYNC_TASK, OB_VEC_TRIGGER_AUTO));
  ASSERT_EQ(PRIORITY_P2,
      get_priority_by_task_type(OB_VECTOR_ASYNC_INDEX_IVF_CLEAN, OB_VEC_TRIGGER_AUTO));
  ASSERT_EQ(PRIORITY_P2,
      get_priority_by_task_type(OB_VECTOR_ASYNC_INDEX_FREEZE, OB_VEC_TRIGGER_AUTO));
  ASSERT_EQ(PRIORITY_P3,
      get_priority_by_task_type(OB_VECTOR_ASYNC_INDEX_MERGE, OB_VEC_TRIGGER_AUTO));
  ASSERT_EQ(PRIORITY_P3,
      get_priority_by_task_type(OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING, OB_VEC_TRIGGER_AUTO));
  ASSERT_EQ(PRIORITY_P4,
      get_priority_by_task_type(OB_VECTOR_ASYNC_INDEX_OPTINAL, OB_VEC_TRIGGER_AUTO));
  // BUILT / INVALID -> PRIORITY_MAX
  ASSERT_EQ(PRIORITY_MAX,
      get_priority_by_task_type(OB_VECTOR_ASYNC_INDEX_BUILT, OB_VEC_TRIGGER_AUTO));
}

// ---------------------------------------------------------------------------
// Test: get_queued_count_by_priority accumulates correctly
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, queued_count_by_priority)
{
  FakeCtx ctx1, ctx2, ctx3;
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx1, OB_VECTOR_ASYNC_INDEX_FREEZE)); // P2
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx2, OB_VECTOR_ASYNC_INDEX_MERGE));  // P3
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx3, OB_VECTOR_ASYNC_INDEX_IVF_LOAD)); // P1

  ASSERT_EQ(1, mgr_.get_queued_count_by_priority(PRIORITY_P2));
  ASSERT_EQ(1, mgr_.get_queued_count_by_priority(PRIORITY_P3));
  ASSERT_EQ(1, mgr_.get_queued_count_by_priority(PRIORITY_P1));
  ASSERT_EQ(0, mgr_.get_queued_count_by_priority(PRIORITY_P4));
  ASSERT_EQ(3, mgr_.get_total_queued_count());

  // drain() before local ctxs go out of scope: TearDown executes after locals destruct,
  // so we must clear the queue here while ctx1/ctx2/ctx3 are still alive.
  mgr_.drain();
}

// ---------------------------------------------------------------------------
// Test: drain clears all queues
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, drain_clears_queues)
{
  FakeCtx ctx1, ctx2, ctx3;
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx1, OB_VECTOR_ASYNC_INDEX_FREEZE));
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx2, OB_VECTOR_ASYNC_INDEX_IVF_LOAD));
  ASSERT_EQ(OB_SUCCESS, mgr_.push_manual(&ctx3));
  ASSERT_EQ(3, mgr_.get_total_queued_count());

  mgr_.drain();

  ASSERT_EQ(0, mgr_.get_total_queued_count());
  // After drain, ctxs should have queue_node_ cleared
  ASSERT_EQ(nullptr, ctx1.queue_node_);
  ASSERT_EQ(nullptr, ctx2.queue_node_);
  ASSERT_EQ(nullptr, ctx3.queue_node_);
}

// ---------------------------------------------------------------------------
// Test: double-push of the same ctx should return OB_ERR_UNEXPECTED
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, double_push_same_ctx)
{
  FakeCtx ctx;
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx, OB_VECTOR_ASYNC_INDEX_FREEZE));
  // queue_node_ is now non-null, second push should fail
  int ret = mgr_.push(&ctx, OB_VECTOR_ASYNC_INDEX_FREEZE);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  ASSERT_EQ(1, mgr_.get_total_queued_count()); // still 1

  // drain() before local ctx goes out of scope.
  mgr_.drain();
  ASSERT_EQ(nullptr, ctx.queue_node_);
}

// ---------------------------------------------------------------------------
// Scenario 1: Push tasks in arbitrary order (manual + various AUTO priorities),
// verify pop order is strictly: manual(P0) -> P1 -> P2 -> P3 -> P4.
//
// Priority mapping reminder:
//   manual queue  : threshold=100 (always schedulable)
//   IVF_LOAD      : P1, threshold=90
//   MEM_SYNC      : P1, threshold=90
//   FREEZE        : P2, threshold=70
//   MERGE/HYBRID  : P3, threshold=50
//   OPTIONAL      : P4, threshold=35
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, pop_order_full_priority_spectrum)
{
  // Push in arbitrary order: P4, P2, manual, P1, P3, P3-again
  FakeCtx ctx_p4, ctx_p2, ctx_manual, ctx_p1, ctx_p3, ctx_merge;

  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx_p4,  OB_VECTOR_ASYNC_INDEX_OPTINAL));       // P4
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx_p2, OB_VECTOR_ASYNC_INDEX_FREEZE));          // P2
  ASSERT_EQ(OB_SUCCESS, mgr_.push_manual(&ctx_manual));                              // manual
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx_p1,  OB_VECTOR_ASYNC_INDEX_IVF_LOAD));       // P1
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx_p3,  OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING)); // P3
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx_merge, OB_VECTOR_ASYNC_INDEX_MERGE));        // P3
  ASSERT_EQ(6, mgr_.get_total_queued_count());

  share::ObVecIndexAsyncTaskCtx *out = nullptr;

  // 1st pop: manual queue has highest priority
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, 0));
  ASSERT_EQ(&ctx_manual, out);

  // 2nd pop: P1 (IVF_LOAD)
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, 0));
  ASSERT_EQ(&ctx_p1, out);

  // 3rd pop: P2 (FREEZE)
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, 0));
  ASSERT_EQ(&ctx_p2, out);

  // 4th pop: P3 (HYBRID_EMBEDDING comes first in the P3 round-robin type map)
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, 0));
  ASSERT_EQ(&ctx_p3, out);

  // 5th pop: P3 (MERGE)
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, 0));
  ASSERT_EQ(&ctx_merge, out);

  // 6th pop: P4 (OPTIONAL)
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, 0));
  ASSERT_EQ(&ctx_p4, out);

  ASSERT_EQ(0, mgr_.get_total_queued_count());
}

// ---------------------------------------------------------------------------
// Scenario 2: Simulate real water-level rise with a 12-thread pool.
//
// Water level formula (integer division, same as production scheduler):
//   water_level = running_count * 100 / thread_count
//
// Thresholds: P4/OPTIONAL=35, P1/IVF_LOAD=90
//
// Steps:
//   Phase 1 - Push 12 P4 tasks only. Pop them one-by-one; each popped task
//             is assumed to start running (running_count++). Once
//             water_level > 35, no more P4 tasks can be popped.
//   Phase 2 - Push 1 P1 task. Despite the elevated water level, P1
//             (threshold=90) is still schedulable. Verify it can be popped.
//   Verify  - After popping P1, remaining P4 tasks are still blocked.
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, water_level_rise_blocks_low_priority)
{
  const int64_t THREAD_COUNT = 12;
  const int64_t P4_TASK_CNT  = 12;

  // Phase 1: push only P4 tasks, pop until water_level exceeds P4 threshold.
  // P4 threshold=35; at running=5: 5*100/12=41 > 35 -> blocked.
  FakeCtx p4_ctxs[P4_TASK_CNT];
  for (int64_t i = 0; i < P4_TASK_CNT; i++) {
    ASSERT_EQ(OB_SUCCESS, mgr_.push(&p4_ctxs[i], OB_VECTOR_ASYNC_INDEX_OPTINAL));
  }
  ASSERT_EQ(P4_TASK_CNT, mgr_.get_total_queued_count());

  int64_t running = 0;
  share::ObVecIndexAsyncTaskCtx *out = nullptr;

  while (true) {
    int64_t wl = running * 100 / THREAD_COUNT;
    if (mgr_.pop(out, wl) == OB_SUCCESS) {
      running++;
    } else {
      break; // water_level now exceeds P4 threshold
    }
  }

  int64_t current_wl = running * 100 / THREAD_COUNT;
  ASSERT_GT(running, 0);                                                // at least some popped
  ASSERT_GT(mgr_.get_queued_count(OB_VECTOR_ASYNC_INDEX_OPTINAL), 0);  // some remain blocked
  ASSERT_GT(current_wl, 35);  // P4 is blocked
  ASSERT_LE(current_wl, 90);  // still below P1 threshold

  // Phase 2: push a P1 task (IVF_LOAD, threshold=90).
  FakeCtx p1_ctx;
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&p1_ctx, OB_VECTOR_ASYNC_INDEX_IVF_LOAD));

  // At current_wl (>35, <=90): P4 blocked, P1 schedulable.
  out = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, current_wl));
  ASSERT_EQ(&p1_ctx, out);  // P1 popped despite elevated water level

  // P4 tasks are still blocked.
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr_.pop(out, current_wl));
  ASSERT_GT(mgr_.get_queued_count(OB_VECTOR_ASYNC_INDEX_OPTINAL), 0);

  // Cleanup before P4 ctxs go out of scope.
  mgr_.drain();
  ASSERT_EQ(0, mgr_.get_total_queued_count());
}

// ---------------------------------------------------------------------------
// Test: explicit water-level config overrides priority defaults but keeps
// min_slots fallback. SEMANTIC_INDEX_REFRESH is P3. With 12 threads and
// explicit 0%, P3 should be capped by min_slots=1 instead of the default 50%
// priority threshold.
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, explicit_config_uses_min_slots_not_default_threshold)
{
  const int64_t THREAD_COUNT = 12;
  ASSERT_EQ(OB_SUCCESS, mgr_.refresh_water_level_config("SEMANTIC_INDEX_REFRESH:0"));
  mgr_.update_thread_limit(THREAD_COUNT, true /*force*/);

  ASSERT_TRUE(mgr_.water_level_threshold_configured_[OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING]);
  ASSERT_EQ(0, mgr_.water_level_threshold_[OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING]);
  ASSERT_EQ(8, mgr_.effective_threshold_[OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING]);

  FakeCtx ctx1, ctx2;
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx1, OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING));
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx2, OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING));

  share::ObVecIndexAsyncTaskCtx *out = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, (0 + 1) * 100 / THREAD_COUNT));
  ASSERT_EQ(&ctx1, out);

  out = nullptr;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr_.pop(out, (1 + 1) * 100 / THREAD_COUNT));
  ASSERT_EQ(nullptr, out);
  ASSERT_EQ(1, mgr_.get_queued_count(OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING));

  mgr_.drain();
}

// ---------------------------------------------------------------------------
// Test: unset task types still use priority defaults. P3 with 12 threads keeps
// the default 50% threshold, allowing 6 post-admission slots.
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, unconfigured_task_uses_default_priority_threshold)
{
  const int64_t THREAD_COUNT = 12;
  ASSERT_EQ(OB_SUCCESS, mgr_.refresh_water_level_config(""));
  mgr_.update_thread_limit(THREAD_COUNT, true /*force*/);

  ASSERT_FALSE(mgr_.water_level_threshold_configured_[OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING]);
  ASSERT_EQ(50, mgr_.effective_threshold_[OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING]);

  FakeCtx ctxs[7];
  for (int64_t i = 0; i < 7; ++i) {
    ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctxs[i], OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING));
  }

  share::ObVecIndexAsyncTaskCtx *out = nullptr;
  for (int64_t running = 0; running < 6; ++running) {
    ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, (running + 1) * 100 / THREAD_COUNT));
    ASSERT_EQ(&ctxs[running], out);
    out = nullptr;
  }
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr_.pop(out, (6 + 1) * 100 / THREAD_COUNT));
  ASSERT_EQ(1, mgr_.get_queued_count(OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING));

  mgr_.drain();
}

// ---------------------------------------------------------------------------
// Scenario 3: Simulate LS stop invalidating queued nodes.
//
// In production, ObPluginVectorIndexLoadScheduler::stop() iterates all queued
// ctxs for the stopped LS and sets queue_node_->is_valid_ = false.
// ObVecIndexPriorityQueueManager::pop_one_from_queue() then skips invalid nodes.
//
// We replicate that behavior directly (without needing a real ObLS):
//   1. Push several ctxs into the queue.
//   2. Simulate LS stop: under ctx->lock_, set queue_node_->is_valid_ = false
//      and ctx->in_queue_ = false  (mirroring stop() logic exactly).
//   3. Pop: all invalidated nodes are silently consumed; pop returns OB_ENTRY_NOT_EXIST.
// ---------------------------------------------------------------------------
TEST_F(TestVecIndexPriorityQueueManager, ls_stop_invalidates_queued_nodes)
{
  // Push tasks across multiple priority levels.
  FakeCtx ctx_a, ctx_b, ctx_c;
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx_a, OB_VECTOR_ASYNC_INDEX_FREEZE)); // P2
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx_b, OB_VECTOR_ASYNC_INDEX_MERGE));  // P3
  ASSERT_EQ(OB_SUCCESS, mgr_.push(&ctx_c, OB_VECTOR_ASYNC_INDEX_IVF_LOAD)); // P1
  ASSERT_EQ(3, mgr_.get_total_queued_count());

  // Simulate LS stop: invalidate ctx_a and ctx_c (leave ctx_b valid).
  // This mirrors the loop inside ObPluginVectorIndexLoadScheduler::stop():
  //   common::ObSpinLockGuard ctx_guard(task_ctx->lock_);
  //   if (task_ctx->in_queue_ && OB_NOT_NULL(task_ctx->queue_node_)) {
  //     task_ctx->queue_node_->is_valid_ = false;
  //     task_ctx->in_queue_ = false;
  //   }
  // Note: in_queue_ is set by the scheduler (not by push()), so it stays false
  // in unit tests. We only check queue_node_ which push() does set.
  {
    common::ObSpinLockGuard guard_a(ctx_a.lock_);
    ASSERT_NE(nullptr, ctx_a.queue_node_);
    ctx_a.queue_node_->is_valid_ = false;
    ctx_a.in_queue_ = false;
  }
  {
    common::ObSpinLockGuard guard_c(ctx_c.lock_);
    ASSERT_NE(nullptr, ctx_c.queue_node_);
    ctx_c.queue_node_->is_valid_ = false;
    ctx_c.in_queue_ = false;
  }

  // Now pop: invalid nodes (ctx_a, ctx_c) should be silently consumed.
  // Only ctx_b (valid, P2) should be returned.
  share::ObVecIndexAsyncTaskCtx *out = nullptr;

  // Pop at water_level=0: should skip ctx_c (P1, invalid) and ctx_a (P2, invalid),
  // return ctx_b (P2, valid).
  ASSERT_EQ(OB_SUCCESS, mgr_.pop(out, 0));
  ASSERT_EQ(&ctx_b, out);

  // Next pop: queue is now empty of valid nodes.
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr_.pop(out, 0));

  // queue_size_ accounting: pop_one_from_queue decrements queue_size_ for every
  // dequeued node (valid or invalid), so total should be 0 now.
  ASSERT_EQ(0, mgr_.get_total_queued_count());

  // ctx_a and ctx_c had their queue_node_ cleared by pop_one_from_queue.
  ASSERT_EQ(nullptr, ctx_a.queue_node_);
  ASSERT_EQ(nullptr, ctx_c.queue_node_);
  // ctx_b's queue_node_ was cleared when it was successfully popped.
  ASSERT_EQ(nullptr, ctx_b.queue_node_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
