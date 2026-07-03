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

#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#define private public
#define protected public
#include "share/rc/ob_tenant_base.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "sql/das/iter/ob_das_fusion_parallel.h"
#include "sql/das/iter/ob_das_fusion_iter.h"
#include "sql/das/search/ob_das_search_define.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr.h"
#include "share/datum/ob_datum.h"
#include "lib/container/ob_se_array.h"
#include "common/object/ob_obj_type.h"
#undef private

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace
{

// ============================================================================
// Section 1: ObDASFusionMaterializedRow Tests
// ============================================================================

TEST(ObDASFusionMaterializedRowTest, default_constructor)
{
  ObDASFusionMaterializedRow row;
  EXPECT_EQ(0UL, row.rowkey_);
  EXPECT_DOUBLE_EQ(0.0, row.score_);
}

TEST(ObDASFusionMaterializedRowTest, parameterized_constructor)
{
  ObDASFusionMaterializedRow row(42, 3.14);
  EXPECT_EQ(42UL, row.rowkey_);
  EXPECT_DOUBLE_EQ(3.14, row.score_);
}

TEST(ObDASFusionMaterializedRowTest, boundary_values)
{
  ObDASFusionMaterializedRow row_max(UINT64_MAX, DBL_MAX);
  EXPECT_EQ(UINT64_MAX, row_max.rowkey_);
  EXPECT_DOUBLE_EQ(DBL_MAX, row_max.score_);

  ObDASFusionMaterializedRow row_zero(0, 0.0);
  EXPECT_EQ(0UL, row_zero.rowkey_);
  EXPECT_DOUBLE_EQ(0.0, row_zero.score_);

  ObDASFusionMaterializedRow row_neg(1, -1.0);
  EXPECT_DOUBLE_EQ(-1.0, row_neg.score_);
}

// ============================================================================
// Section 2: ObSharedBitmapSlot Tests
// ============================================================================

TEST(ObSharedBitmapSlotTest, default_constructor)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObSharedBitmapSlot slot;
  EXPECT_EQ(nullptr, slot.bitmap_);
  EXPECT_EQ(-1, slot.bitmap_occurrence_idx_);
  EXPECT_FALSE(slot.is_built_);
  EXPECT_EQ(OB_SUCCESS, slot.build_ret_);
}

TEST(ObSharedBitmapSlotTest, release_resets_state)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObSharedBitmapSlot slot;
  slot.bitmap_occurrence_idx_ = 5;
  ATOMIC_STORE(&slot.is_built_, true);
  slot.build_ret_ = OB_ERR_UNEXPECTED;

  slot.release();

  EXPECT_EQ(nullptr, slot.bitmap_);
  EXPECT_FALSE(slot.is_built_);
  EXPECT_EQ(OB_SUCCESS, slot.build_ret_);
}

// ============================================================================
// Section 3: ObBitmapPhaseBarrier Tests
// ============================================================================

class ObBitmapPhaseBarrierTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }
};

TEST_F(ObBitmapPhaseBarrierTest, init_with_invalid_count)
{
  ObBitmapPhaseBarrier barrier;
  EXPECT_EQ(OB_INVALID_ARGUMENT, barrier.init(0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, barrier.init(-1));
}

TEST_F(ObBitmapPhaseBarrierTest, init_success)
{
  ObBitmapPhaseBarrier barrier;
  EXPECT_EQ(OB_SUCCESS, barrier.init(3));
  EXPECT_TRUE(barrier.is_inited_);
  EXPECT_EQ(3, barrier.total_cnt_);
  EXPECT_EQ(0, barrier.built_cnt_);
  EXPECT_EQ(OB_SUCCESS, barrier.first_err_);
}

TEST_F(ObBitmapPhaseBarrierTest, wait_not_inited)
{
  ObBitmapPhaseBarrier barrier;
  EXPECT_EQ(OB_NOT_INIT, barrier.wait_all_built(INT64_MAX));
}

TEST_F(ObBitmapPhaseBarrierTest, single_bitmap_signal_and_wait)
{
  ObBitmapPhaseBarrier barrier;
  ASSERT_EQ(OB_SUCCESS, barrier.init(1));

  barrier.on_bitmap_built(OB_SUCCESS);
  EXPECT_EQ(OB_SUCCESS, barrier.wait_all_built(INT64_MAX));
}

TEST_F(ObBitmapPhaseBarrierTest, multiple_bitmap_signal_and_wait)
{
  ObBitmapPhaseBarrier barrier;
  ASSERT_EQ(OB_SUCCESS, barrier.init(3));

  barrier.on_bitmap_built(OB_SUCCESS);
  barrier.on_bitmap_built(OB_SUCCESS);
  barrier.on_bitmap_built(OB_SUCCESS);

  EXPECT_EQ(OB_SUCCESS, barrier.wait_all_built(INT64_MAX));
}

TEST_F(ObBitmapPhaseBarrierTest, error_propagation)
{
  ObBitmapPhaseBarrier barrier;
  ASSERT_EQ(OB_SUCCESS, barrier.init(3));

  barrier.on_bitmap_built(OB_SUCCESS);
  barrier.on_bitmap_built(OB_ERR_UNEXPECTED);
  barrier.on_bitmap_built(OB_SUCCESS);

  // wait_all_built returns first_err_ when all are built
  EXPECT_EQ(OB_ERR_UNEXPECTED, barrier.wait_all_built(INT64_MAX));
}

TEST_F(ObBitmapPhaseBarrierTest, first_error_wins)
{
  ObBitmapPhaseBarrier barrier;
  ASSERT_EQ(OB_SUCCESS, barrier.init(3));

  barrier.on_bitmap_built(OB_TIMEOUT);
  barrier.on_bitmap_built(OB_ERR_UNEXPECTED);
  barrier.on_bitmap_built(OB_SUCCESS);

  // First non-success error should be preserved
  EXPECT_EQ(OB_TIMEOUT, barrier.wait_all_built(INT64_MAX));
}

TEST_F(ObBitmapPhaseBarrierTest, timeout_when_not_all_built)
{
  ObBitmapPhaseBarrier barrier;
  ASSERT_EQ(OB_SUCCESS, barrier.init(3));

  barrier.on_bitmap_built(OB_SUCCESS);
  // Only 1 out of 3 built

  // Use a very short timeout (already expired)
  int64_t expired_ts = ObTimeUtility::current_time() - 1000;
  EXPECT_EQ(OB_TIMEOUT, barrier.wait_all_built(expired_ts));
}

TEST_F(ObBitmapPhaseBarrierTest, concurrent_signal_and_wait)
{
  ObBitmapPhaseBarrier barrier;
  ASSERT_EQ(OB_SUCCESS, barrier.init(4));

  std::atomic<int> errors(0);

  // Spawn threads to signal
  std::thread t1([&]() {
    usleep(1000);  // 1ms
    barrier.on_bitmap_built(OB_SUCCESS);
  });
  std::thread t2([&]() {
    usleep(2000);  // 2ms
    barrier.on_bitmap_built(OB_SUCCESS);
  });
  std::thread t3([&]() {
    usleep(3000);  // 3ms
    barrier.on_bitmap_built(OB_SUCCESS);
  });
  std::thread t4([&]() {
    usleep(4000);  // 4ms
    barrier.on_bitmap_built(OB_SUCCESS);
  });

  int64_t timeout_ts = ObTimeUtility::current_time() + 5000000;  // 5 seconds
  EXPECT_EQ(OB_SUCCESS, barrier.wait_all_built(timeout_ts));

  t1.join();
  t2.join();
  t3.join();
  t4.join();
}

// ============================================================================
// Section 4: ObDASFusionParallelCoordinator Tests
// ============================================================================

class ObDASFusionParallelCoordinatorTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }
};

TEST_F(ObDASFusionParallelCoordinatorTest, init_with_invalid_count)
{
  ObDASFusionParallelCoordinator coord;
  EXPECT_EQ(OB_INVALID_ARGUMENT, coord.init(0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, coord.init(-1));
}

TEST_F(ObDASFusionParallelCoordinatorTest, init_success)
{
  ObDASFusionParallelCoordinator coord;
  EXPECT_EQ(OB_SUCCESS, coord.init(3));
  EXPECT_TRUE(coord.is_inited());
  EXPECT_EQ(OB_SUCCESS, coord.get_first_error());
}

TEST_F(ObDASFusionParallelCoordinatorTest, wait_not_inited)
{
  ObDASFusionParallelCoordinator coord;
  EXPECT_EQ(OB_NOT_INIT, coord.wait_all_complete(INT64_MAX));
}

TEST_F(ObDASFusionParallelCoordinatorTest, single_child_finish)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));

  coord.on_child_finish(OB_SUCCESS);
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(INT64_MAX));
}

TEST_F(ObDASFusionParallelCoordinatorTest, multiple_children_finish)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(3));

  coord.on_child_finish(OB_SUCCESS);
  coord.on_child_finish(OB_SUCCESS);
  coord.on_child_finish(OB_SUCCESS);

  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(INT64_MAX));
}

TEST_F(ObDASFusionParallelCoordinatorTest, error_propagation)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(3));

  coord.on_child_finish(OB_SUCCESS);
  coord.on_child_finish(OB_ERR_UNEXPECTED);
  coord.on_child_finish(OB_SUCCESS);

  EXPECT_EQ(OB_ERR_UNEXPECTED, coord.wait_all_complete(INT64_MAX));
}

TEST_F(ObDASFusionParallelCoordinatorTest, first_error_wins)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(3));

  coord.set_first_error(OB_TIMEOUT);
  coord.on_child_finish(OB_ERR_UNEXPECTED);
  coord.on_child_finish(OB_SUCCESS);
  coord.on_child_finish(OB_SUCCESS);

  EXPECT_EQ(OB_TIMEOUT, coord.get_first_error());
}

TEST_F(ObDASFusionParallelCoordinatorTest, set_first_error_ignores_success)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));

  coord.set_first_error(OB_SUCCESS);  // Should be ignored
  EXPECT_EQ(OB_SUCCESS, coord.get_first_error());

  coord.set_first_error(OB_ERR_UNEXPECTED);
  EXPECT_EQ(OB_ERR_UNEXPECTED, coord.get_first_error());

  // Second error should NOT overwrite first
  coord.set_first_error(OB_TIMEOUT);
  EXPECT_EQ(OB_ERR_UNEXPECTED, coord.get_first_error());
}

TEST_F(ObDASFusionParallelCoordinatorTest, timeout_when_not_all_finished)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(3));

  coord.on_child_finish(OB_SUCCESS);
  // Only 1 out of 3 finished

  int64_t expired_ts = ObTimeUtility::current_time() - 1000;
  EXPECT_EQ(OB_TIMEOUT, coord.wait_all_complete(expired_ts));
}

TEST_F(ObDASFusionParallelCoordinatorTest, reset)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(3));

  coord.on_child_finish(OB_ERR_UNEXPECTED);
  coord.on_child_finish(OB_SUCCESS);
  coord.on_child_finish(OB_SUCCESS);

  coord.reset();
  EXPECT_EQ(0, coord.total_cnt_);
  EXPECT_EQ(0, coord.finished_cnt_);
  EXPECT_EQ(OB_SUCCESS, coord.first_err_code_);
}

TEST_F(ObDASFusionParallelCoordinatorTest, concurrent_on_child_finish)
{
  ObDASFusionParallelCoordinator coord;
  const int64_t N = 8;
  ASSERT_EQ(OB_SUCCESS, coord.init(N));

  std::vector<std::thread> threads;
  for (int64_t i = 0; i < N; ++i) {
    threads.emplace_back([&coord, i]() {
      usleep(static_cast<useconds_t>(i * 500));  // Stagger slightly
      coord.on_child_finish(OB_SUCCESS);
    });
  }

  int64_t timeout_ts = ObTimeUtility::current_time() + 5000000;  // 5 seconds
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(timeout_ts));

  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(ObDASFusionParallelCoordinatorTest, concurrent_with_one_error)
{
  ObDASFusionParallelCoordinator coord;
  const int64_t N = 8;
  ASSERT_EQ(OB_SUCCESS, coord.init(N));

  std::vector<std::thread> threads;
  for (int64_t i = 0; i < N; ++i) {
    threads.emplace_back([&coord, i]() {
      usleep(static_cast<useconds_t>(i * 500));
      int err = (i == 3) ? OB_CANCELED : OB_SUCCESS;
      coord.on_child_finish(err);
    });
  }

  int64_t timeout_ts = ObTimeUtility::current_time() + 5000000;
  int ret = coord.wait_all_complete(timeout_ts);
  EXPECT_EQ(OB_CANCELED, ret);

  for (auto &t : threads) {
    t.join();
  }
}

// ============================================================================
// Section 5: ObDASFusionChildRuntime Tests
// ============================================================================

class ObDASFusionChildRuntimeTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }
};

TEST_F(ObDASFusionChildRuntimeTest, default_constructor)
{
  ObDASFusionChildRuntime runtime;
  EXPECT_EQ(-1, runtime.path_idx_);
  EXPECT_EQ(nullptr, runtime.child_iter_);
  EXPECT_FALSE(runtime.owns_child_iter_);
  EXPECT_EQ(nullptr, runtime.child_exec_ctx_);
  EXPECT_EQ(nullptr, runtime.child_eval_ctx_);
  EXPECT_EQ(nullptr, runtime.child_search_ctx_);
  EXPECT_EQ(nullptr, runtime.rowkey_expr_);
  EXPECT_EQ(nullptr, runtime.score_expr_);
  EXPECT_EQ(1, runtime.max_batch_size_);
  EXPECT_FALSE(runtime.submitted_);
  EXPECT_FALSE(runtime.finished_);
  EXPECT_EQ(OB_SUCCESS, runtime.err_code_);
  EXPECT_FALSE(runtime.use_rescan_);
  EXPECT_FALSE(runtime.is_range_parallel_);
  EXPECT_EQ(-1, runtime.range_top_k_limit_);
  EXPECT_EQ(nullptr, runtime.cloned_rtdef_root_);
  EXPECT_EQ(nullptr, runtime.profile_);
  EXPECT_EQ(nullptr, runtime.parallel_ctx_);
}

TEST_F(ObDASFusionChildRuntimeTest, reset_result)
{
  ObDASFusionChildRuntime runtime;
  runtime.submitted_ = true;
  runtime.finished_ = true;
  runtime.err_code_ = OB_ERR_UNEXPECTED;
  runtime.rows_.push_back(ObDASFusionMaterializedRow(1, 1.0));
  runtime.rows_.push_back(ObDASFusionMaterializedRow(2, 2.0));

  EXPECT_EQ(2, runtime.rows_.count());

  runtime.reset_result();

  EXPECT_FALSE(runtime.submitted_);
  EXPECT_FALSE(runtime.finished_);
  EXPECT_EQ(OB_SUCCESS, runtime.err_code_);
  EXPECT_EQ(0, runtime.rows_.count());
}

TEST_F(ObDASFusionChildRuntimeTest, release_clears_all)
{
  ObDASFusionChildRuntime runtime;
  runtime.rows_.push_back(ObDASFusionMaterializedRow(1, 1.0));
  runtime.submitted_ = true;
  runtime.finished_ = true;
  runtime.err_code_ = OB_TIMEOUT;

  runtime.reset_result();
  runtime.release_parallel_resources();

  EXPECT_FALSE(runtime.submitted_);
  EXPECT_FALSE(runtime.finished_);
  EXPECT_EQ(OB_SUCCESS, runtime.err_code_);
  EXPECT_EQ(0, runtime.rows_.count());
  EXPECT_EQ(nullptr, runtime.child_iter_);
  EXPECT_EQ(nullptr, runtime.child_exec_ctx_);
  EXPECT_EQ(nullptr, runtime.child_eval_ctx_);
  EXPECT_EQ(nullptr, runtime.child_search_ctx_);
  EXPECT_EQ(nullptr, runtime.cloned_rtdef_root_);
}

TEST_F(ObDASFusionChildRuntimeTest, release_does_not_free_unowned_iter)
{
  // When owns_child_iter_ is false, release_parallel_resources should NOT free child_iter_
  ObDASFusionChildRuntime runtime;
  int dummy_iter = 42;  // fake pointer
  runtime.child_iter_ = reinterpret_cast<ObDASIter *>(&dummy_iter);
  runtime.owns_child_iter_ = false;

  runtime.release_parallel_resources();

  // child_iter_ should be set to null but the original object should not be freed
  EXPECT_EQ(nullptr, runtime.child_iter_);
}

TEST_F(ObDASFusionChildRuntimeTest, prepare_parallel_resources_null_sources)
{
  ObDASFusionChildRuntime runtime;
  // src pointers are null by default
  EXPECT_EQ(OB_ERR_UNEXPECTED, runtime.prepare_parallel_resources());
}

TEST_F(ObDASFusionChildRuntimeTest, init_with_invalid_path_idx)
{
  ObArenaAllocator alloc("TestRT");
  ObDASFusionCtDef ctdef(alloc);
  ctdef.op_type_ = DAS_OP_FUSION_QUERY;
  ctdef.children_cnt_ = 2;

  // score_exprs_ has 3 elements (2 paths + fusion), but path_idx = 5 is out of range
  ObExpr dummy_expr1, dummy_expr2, dummy_expr3;
  ctdef.score_exprs_.init(3);
  ctdef.score_exprs_.push_back(&dummy_expr1);
  ctdef.score_exprs_.push_back(&dummy_expr2);
  ctdef.score_exprs_.push_back(&dummy_expr3);

  // rowid_exprs_ has 1 element
  ObExpr rowid_expr;
  ctdef.rowid_exprs_.init(1);
  ctdef.rowid_exprs_.push_back(&rowid_expr);

  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  eval_ctx.max_batch_size_ = 256;

  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);

  ObDASFusionChildRuntime runtime;
  // path_idx = 5, but score_exprs_ only has count=3, valid range is [0, 1]
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            runtime.init(5, ctdef, exec_ctx, eval_ctx, search_ctx, nullptr));
}

TEST_F(ObDASFusionChildRuntimeTest, init_with_empty_rowid_exprs)
{
  ObArenaAllocator alloc("TestRT");
  ObDASFusionCtDef ctdef(alloc);
  ctdef.op_type_ = DAS_OP_FUSION_QUERY;
  ctdef.children_cnt_ = 1;

  // score_exprs_ has elements but rowid_exprs_ is empty
  ObExpr dummy_expr1, dummy_expr2;
  ctdef.score_exprs_.init(2);
  ctdef.score_exprs_.push_back(&dummy_expr1);
  ctdef.score_exprs_.push_back(&dummy_expr2);
  // rowid_exprs_ is empty

  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);

  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);

  ObDASFusionChildRuntime runtime;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            runtime.init(0, ctdef, exec_ctx, eval_ctx, search_ctx, nullptr));
}

TEST_F(ObDASFusionChildRuntimeTest, init_success)
{
  ObArenaAllocator alloc("TestRT");
  ObDASFusionCtDef ctdef(alloc);
  ctdef.op_type_ = DAS_OP_FUSION_QUERY;
  ctdef.children_cnt_ = 2;

  ObExpr rowid_expr, score_expr0, score_expr1, fusion_score;
  ctdef.rowid_exprs_.init(1);
  ctdef.rowid_exprs_.push_back(&rowid_expr);

  ctdef.score_exprs_.init(3);
  ctdef.score_exprs_.push_back(&score_expr0);
  ctdef.score_exprs_.push_back(&score_expr1);
  ctdef.score_exprs_.push_back(&fusion_score);

  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);
  eval_ctx.max_batch_size_ = 256;

  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc, mock_scan_op);

  ObDASFusionChildRuntime runtime;
  ASSERT_EQ(OB_SUCCESS,
            runtime.init(0, ctdef, exec_ctx, eval_ctx, search_ctx, nullptr));

  EXPECT_EQ(0, runtime.path_idx_);
  EXPECT_EQ(&rowid_expr, runtime.rowkey_expr_);
  EXPECT_EQ(&score_expr0, runtime.score_expr_);
  EXPECT_EQ(256, runtime.max_batch_size_);
  EXPECT_EQ(&exec_ctx, runtime.src_exec_ctx_);
  EXPECT_EQ(&eval_ctx, runtime.src_eval_ctx_);
  EXPECT_EQ(&search_ctx, runtime.src_search_ctx_);
  EXPECT_EQ(&ctdef, runtime.fusion_ctdef_);
}

// ============================================================================
// Section 6: ObDASFusionParallelCtx Tests - should_enable_parallel
// ============================================================================

class ObDASFusionParallelCtxTest : public ::testing::Test
{
protected:
  ObDASFusionParallelCtxTest()
      : alloc_("TestPCtx"),
        ctdef_(alloc_)
  {}

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
    ctdef_.op_type_ = DAS_OP_FUSION_QUERY;
  }

  ObArenaAllocator alloc_;
  ObDASFusionCtDef ctdef_;
};

TEST_F(ObDASFusionParallelCtxTest, should_enable_parallel_false_when_not_enabled)
{
  ctdef_.enable_parallel_ = false;
  ctdef_.children_cnt_ = 2;
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  EXPECT_FALSE(ObDASFusionParallelCtx::should_enable_parallel(ctdef_, search_ctx));
}

TEST_F(ObDASFusionParallelCtxTest, should_enable_parallel_false_when_no_children)
{
  ctdef_.enable_parallel_ = true;
  ctdef_.children_cnt_ = 0;
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  EXPECT_FALSE(ObDASFusionParallelCtx::should_enable_parallel(ctdef_, search_ctx));
}

TEST_F(ObDASFusionParallelCtxTest, should_enable_parallel_false_when_single_child)
{
  ctdef_.enable_parallel_ = true;
  ctdef_.children_cnt_ = 1;
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  EXPECT_FALSE(ObDASFusionParallelCtx::should_enable_parallel(ctdef_, search_ctx));
}

TEST_F(ObDASFusionParallelCtxTest, should_enable_parallel_false_when_non_uint64_rowkey)
{
  ctdef_.enable_parallel_ = true;
  ctdef_.children_cnt_ = 2;
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  search_ctx.set_rowid_type(DAS_ROWID_TYPE_COMPACT);
  EXPECT_FALSE(ObDASFusionParallelCtx::should_enable_parallel(ctdef_, search_ctx));
}

TEST_F(ObDASFusionParallelCtxTest, should_enable_parallel_true)
{
  ctdef_.enable_parallel_ = true;
  ctdef_.children_cnt_ = 2;
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  EXPECT_TRUE(ObDASFusionParallelCtx::should_enable_parallel(ctdef_, search_ctx));
}

TEST_F(ObDASFusionParallelCtxTest, should_enable_parallel_with_single_child)
{
  ctdef_.enable_parallel_ = true;
  ctdef_.children_cnt_ = 1;
  ctdef_.query_dop_ = 2;
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(alloc_, mock_scan_op);
  EXPECT_TRUE(ObDASFusionParallelCtx::should_enable_parallel(ctdef_, search_ctx));
}

// ============================================================================
// Section 7: ObDASFusionChildTask Tests
// ============================================================================

class ObDASFusionChildTaskTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }
};

TEST_F(ObDASFusionChildTaskTest, default_constructor)
{
  ObDASFusionChildTask task;
  EXPECT_EQ(nullptr, task.runtime_);
  EXPECT_EQ(nullptr, task.coordinator_);
  EXPECT_EQ(INT64_MAX, task.timeout_ts_);
}

TEST_F(ObDASFusionChildTaskTest, init_with_null_runtime)
{
  ObDASFusionChildTask task;
  ObDASFusionParallelCoordinator coord;
  EXPECT_EQ(OB_INVALID_ARGUMENT, task.init(nullptr, &coord, INT64_MAX, 0));
}

TEST_F(ObDASFusionChildTaskTest, init_with_null_coordinator)
{
  ObDASFusionChildTask task;
  ObDASFusionChildRuntime runtime;
  EXPECT_EQ(OB_INVALID_ARGUMENT, task.init(&runtime, nullptr, INT64_MAX, 0));
}

TEST_F(ObDASFusionChildTaskTest, init_success)
{
  ObDASFusionChildTask task;
  ObDASFusionChildRuntime runtime;
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));

  ASSERT_EQ(OB_SUCCESS, task.init(&runtime, &coord, 12345, 7));
  EXPECT_EQ(&runtime, task.get_runtime());
  EXPECT_EQ(&coord, task.get_coordinator());
  EXPECT_EQ(12345, task.get_timeout_ts());
}

TEST_F(ObDASFusionChildTaskTest, reset)
{
  ObDASFusionChildTask task;
  ObDASFusionChildRuntime runtime;
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));
  ASSERT_EQ(OB_SUCCESS, task.init(&runtime, &coord, 12345, 7));

  task.reset();
  EXPECT_EQ(nullptr, task.runtime_);
  EXPECT_EQ(nullptr, task.coordinator_);
  EXPECT_EQ(INT64_MAX, task.timeout_ts_);
}

// ============================================================================
// Section 8: ObDASFusionChildTaskHandler Tests
// ============================================================================

TEST_F(ObDASFusionChildTaskTest, handler_init_null_task)
{
  ObDASFusionChildTaskHandler handler;
  EXPECT_EQ(OB_INVALID_ARGUMENT, handler.init(nullptr));
}

TEST_F(ObDASFusionChildTaskTest, handler_init_success)
{
  ObDASFusionChildTask task;
  ObDASFusionChildTaskHandler handler;
  EXPECT_EQ(OB_SUCCESS, handler.init(&task));
}

// ============================================================================
// Section 9: Materialized Row Collection Tests
// ============================================================================

TEST(MaterializedRowCollectionTest, push_and_iterate)
{
  ObSEArray<ObDASFusionMaterializedRow, 256> rows;
  for (uint64_t i = 0; i < 100; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows.push_back(ObDASFusionMaterializedRow(i, static_cast<double>(i) * 0.1)));
  }
  EXPECT_EQ(100, rows.count());

  for (int64_t i = 0; i < rows.count(); ++i) {
    EXPECT_EQ(static_cast<uint64_t>(i), rows.at(i).rowkey_);
    EXPECT_DOUBLE_EQ(static_cast<double>(i) * 0.1, rows.at(i).score_);
  }
}

TEST(MaterializedRowCollectionTest, reserve_capacity)
{
  ObSEArray<ObDASFusionMaterializedRow, 256> rows;
  ASSERT_EQ(OB_SUCCESS, rows.reserve(1000));

  for (uint64_t i = 0; i < 1000; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows.push_back(ObDASFusionMaterializedRow(i, 1.0)));
  }
  EXPECT_EQ(1000, rows.count());
}

// ============================================================================
// Section 10: Coordinator + Barrier Integration Tests (multi-threaded)
// ============================================================================

class ParallelIntegrationTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }
};

TEST_F(ParallelIntegrationTest, coordinator_stress_test)
{
  // Test with many concurrent finishes
  ObDASFusionParallelCoordinator coord;
  const int64_t N = 32;
  ASSERT_EQ(OB_SUCCESS, coord.init(N));

  std::vector<std::thread> threads;
  for (int64_t i = 0; i < N; ++i) {
    threads.emplace_back([&coord]() {
      coord.on_child_finish(OB_SUCCESS);
    });
  }

  int64_t timeout_ts = ObTimeUtility::current_time() + 10000000;  // 10 seconds
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(timeout_ts));

  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(ParallelIntegrationTest, bitmap_barrier_stress_test)
{
  ObBitmapPhaseBarrier barrier;
  const int64_t N = 16;
  ASSERT_EQ(OB_SUCCESS, barrier.init(N));

  std::vector<std::thread> threads;
  for (int64_t i = 0; i < N; ++i) {
    threads.emplace_back([&barrier]() {
      barrier.on_bitmap_built(OB_SUCCESS);
    });
  }

  int64_t timeout_ts = ObTimeUtility::current_time() + 10000000;
  EXPECT_EQ(OB_SUCCESS, barrier.wait_all_built(timeout_ts));

  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(ParallelIntegrationTest, coordinator_mixed_errors_stress)
{
  ObDASFusionParallelCoordinator coord;
  const int64_t N = 16;
  ASSERT_EQ(OB_SUCCESS, coord.init(N));

  std::vector<std::thread> threads;
  std::atomic<int> error_count(0);

  for (int64_t i = 0; i < N; ++i) {
    threads.emplace_back([&coord, &error_count, i]() {
      int err = OB_SUCCESS;
      if (i % 5 == 0) {
        err = OB_CANCELED;
        error_count.fetch_add(1);
      }
      coord.on_child_finish(err);
    });
  }

  int64_t timeout_ts = ObTimeUtility::current_time() + 10000000;
  int ret = coord.wait_all_complete(timeout_ts);

  for (auto &t : threads) {
    t.join();
  }

  if (error_count.load() > 0) {
    EXPECT_EQ(OB_CANCELED, ret);
  } else {
    EXPECT_EQ(OB_SUCCESS, ret);
  }
}

// ============================================================================
// Section 11: ObDASFusionParallelCtx - release tests
// ============================================================================

TEST_F(ObDASFusionParallelCtxTest, release_empty_ctx)
{
  // Releasing an empty context should not crash
  ObDASFusionParallelCtx ctx;
  ctx.release();
  EXPECT_EQ(0, ctx.get_runtime_count());
}

TEST_F(ObDASFusionParallelCtxTest, at_returns_null_for_invalid_index)
{
  ObDASFusionParallelCtx ctx;
  EXPECT_EQ(nullptr, ctx.at(-1));
  EXPECT_EQ(nullptr, ctx.at(0));
  EXPECT_EQ(nullptr, ctx.at(100));
}

TEST_F(ObDASFusionParallelCtxTest, has_shared_bitmaps_false_by_default)
{
  ObDASFusionParallelCtx ctx;
  EXPECT_FALSE(ctx.has_shared_bitmaps());
  EXPECT_EQ(0, ctx.get_shared_bitmap_count());
}

TEST_F(ObDASFusionParallelCtxTest, get_shared_bitmap_slot_invalid)
{
  ObDASFusionParallelCtx ctx;
  EXPECT_EQ(nullptr, ctx.get_shared_bitmap_slot(-1));
  EXPECT_EQ(nullptr, ctx.get_shared_bitmap_slot(0));
}

// ============================================================================
// Section 12: Edge case tests for coordinator wait semantics
// ============================================================================

TEST_F(ObDASFusionParallelCoordinatorTest, wait_indefinitely_when_timeout_is_max)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));

  // Finish immediately, then wait with INT64_MAX
  coord.on_child_finish(OB_SUCCESS);
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(INT64_MAX));
}

TEST_F(ObDASFusionParallelCoordinatorTest, multiple_errors_only_first_preserved)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(5));

  // All finish with different errors
  coord.on_child_finish(OB_TIMEOUT);
  coord.on_child_finish(OB_ERR_UNEXPECTED);
  coord.on_child_finish(OB_CANCELED);
  coord.on_child_finish(OB_ALLOCATE_MEMORY_FAILED);
  coord.on_child_finish(OB_SUCCESS);

  // First error (OB_TIMEOUT) should win
  EXPECT_EQ(OB_TIMEOUT, coord.wait_all_complete(INT64_MAX));
}

// ============================================================================
// Section 13: ObDASFusionChildRuntime rows_ accumulation test
// ============================================================================

TEST_F(ObDASFusionChildRuntimeTest, rows_accumulate_correctly)
{
  ObDASFusionChildRuntime runtime;

  // Simulate worker thread pushing materialized rows
  for (uint64_t i = 0; i < 500; ++i) {
    ASSERT_EQ(OB_SUCCESS,
              runtime.rows_.push_back(ObDASFusionMaterializedRow(i, static_cast<double>(i))));
  }

  EXPECT_EQ(500, runtime.rows_.count());

  // Verify first and last
  EXPECT_EQ(0UL, runtime.rows_.at(0).rowkey_);
  EXPECT_DOUBLE_EQ(0.0, runtime.rows_.at(0).score_);
  EXPECT_EQ(499UL, runtime.rows_.at(499).rowkey_);
  EXPECT_DOUBLE_EQ(499.0, runtime.rows_.at(499).score_);

  // reset_result should clear
  runtime.reset_result();
  EXPECT_EQ(0, runtime.rows_.count());
}

// ============================================================================
// Section 14: ObDASFusionIter parallel/serial path switch test
// ============================================================================

class ObDASFusionIterParallelSwitchTest : public ::testing::Test
{
protected:
  ObDASFusionIterParallelSwitchTest()
      : alloc_("TestSwitch"),
        scan_alloc_("MockScan"),
        exec_ctx_(alloc_),
        eval_ctx_(exec_ctx_),
        ctdef_(alloc_),
        mock_scan_op_(scan_alloc_),
        search_ctx_(alloc_, mock_scan_op_)
  {
    eval_ctx_.max_batch_size_ = 256;
  }

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }

  ObArenaAllocator alloc_;
  ObArenaAllocator scan_alloc_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObDASFusionCtDef ctdef_;
  ObDASScanOp mock_scan_op_;
  ObDASSearchCtx search_ctx_;
};

TEST_F(ObDASFusionIterParallelSwitchTest, enable_parallel_flag_set_in_inner_init)
{
  // When enable_parallel_ is true in ctdef and exec_ctx is valid,
  // ObDASFusionIter should set enable_parallel_ = true
  ctdef_.op_type_ = DAS_OP_FUSION_QUERY;
  ctdef_.enable_parallel_ = true;
  ctdef_.children_cnt_ = 2;
  ctdef_.has_search_subquery_ = true;
  ctdef_.has_vector_subquery_ = true;

  // Setup minimal expressions to avoid nullptr crashes
  ObExpr rowid_expr, score0, score1, fusion_score;
  ctdef_.rowid_exprs_.init(1);
  ctdef_.rowid_exprs_.push_back(&rowid_expr);
  ctdef_.score_exprs_.init(3);
  ctdef_.score_exprs_.push_back(&score0);
  ctdef_.score_exprs_.push_back(&score1);
  ctdef_.score_exprs_.push_back(&fusion_score);
  ctdef_.result_output_.init(4);
  ctdef_.result_output_.push_back(&rowid_expr);
  ctdef_.result_output_.push_back(&score0);
  ctdef_.result_output_.push_back(&score1);
  ctdef_.result_output_.push_back(&fusion_score);

  // Minimal expr setup to pass set_rowkey_is_uint64_flag
  rowid_expr.obj_meta_.set_type(ObUInt64Type);

  ObDASFusionIter iter;
  ObDASFusionIterParam param;
  param.type_ = ObDASIterType::DAS_ITER_FUSION;
  param.fusion_ctdef_ = &ctdef_;
  param.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  param.rank_window_size_ = 10;
  param.size_ = 10;
  param.eval_ctx_ = &eval_ctx_;
  param.exec_ctx_ = &exec_ctx_;
  param.max_size_ = 1024;
  param.output_ = &ctdef_.result_output_;
  param.weights_.push_back(0.5);
  param.weights_.push_back(0.5);
  param.path_top_k_limits_.push_back(10);
  param.path_top_k_limits_.push_back(10);
  param.search_ctx_ = &search_ctx_;

  // inner_init will call should_enable_parallel
  ASSERT_EQ(OB_SUCCESS, iter.init(param));
  EXPECT_TRUE(iter.enable_parallel_);

  iter.release();
}

TEST_F(ObDASFusionIterParallelSwitchTest, disable_parallel_flag_in_inner_init)
{
  ctdef_.op_type_ = DAS_OP_FUSION_QUERY;
  ctdef_.enable_parallel_ = false;
  ctdef_.children_cnt_ = 2;
  ctdef_.has_search_subquery_ = true;
  ctdef_.has_vector_subquery_ = true;

  ObExpr rowid_expr, score0, score1, fusion_score;
  ctdef_.rowid_exprs_.init(1);
  ctdef_.rowid_exprs_.push_back(&rowid_expr);
  ctdef_.score_exprs_.init(3);
  ctdef_.score_exprs_.push_back(&score0);
  ctdef_.score_exprs_.push_back(&score1);
  ctdef_.score_exprs_.push_back(&fusion_score);
  ctdef_.result_output_.init(4);
  ctdef_.result_output_.push_back(&rowid_expr);
  ctdef_.result_output_.push_back(&score0);
  ctdef_.result_output_.push_back(&score1);
  ctdef_.result_output_.push_back(&fusion_score);

  rowid_expr.obj_meta_.set_type(ObUInt64Type);

  ObDASFusionIter iter;
  ObDASFusionIterParam param;
  param.type_ = ObDASIterType::DAS_ITER_FUSION;
  param.fusion_ctdef_ = &ctdef_;
  param.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  param.rank_window_size_ = 10;
  param.size_ = 10;
  param.eval_ctx_ = &eval_ctx_;
  param.exec_ctx_ = &exec_ctx_;
  param.max_size_ = 1024;
  param.output_ = &ctdef_.result_output_;
  param.weights_.push_back(0.5);
  param.weights_.push_back(0.5);
  param.path_top_k_limits_.push_back(10);
  param.path_top_k_limits_.push_back(10);
  param.search_ctx_ = &search_ctx_;

  ASSERT_EQ(OB_SUCCESS, iter.init(param));
  EXPECT_FALSE(iter.enable_parallel_);

  iter.release();
}

// ============================================================================
// Section 15: ObDASFusionIter merge_parallel_results Tests
// ============================================================================

class ObDASFusionIterMergeTest : public ::testing::Test
{
protected:
  ObDASFusionIterMergeTest()
      : alloc_("TestMerge"),
        exec_ctx_(alloc_),
        eval_ctx_(exec_ctx_),
        ctdef_(alloc_)
  {
    eval_ctx_.max_batch_size_ = 256;
  }

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;

    // Setup frames for expression evaluation
    const int64_t frame_size = 1024 * 1024;  // 1MB
    exec_ctx_.frames_ = (char **)alloc_.alloc(2 * sizeof(char*));
    if (OB_NOT_NULL(exec_ctx_.frames_)) {
      exec_ctx_.frames_[0] = (char *)alloc_.alloc(frame_size);
      if (OB_NOT_NULL(exec_ctx_.frames_[0])) {
        memset(exec_ctx_.frames_[0], 0, frame_size);
      }
      exec_ctx_.frames_[1] = (char *)alloc_.alloc(frame_size);
      if (OB_NOT_NULL(exec_ctx_.frames_[1])) {
        memset(exec_ctx_.frames_[1], 0, frame_size);
      }
      exec_ctx_.frame_cnt_ = 2;
    }
    eval_ctx_.frames_ = exec_ctx_.frames_;
  }

  ObArenaAllocator alloc_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObDASFusionCtDef ctdef_;
};

TEST_F(ObDASFusionIterMergeTest, merge_with_unfinished_runtime)
{
  // If a runtime is not finished, merge_parallel_results should fail
  ObDASFusionIter iter;

  // Manually create a runtime that is not finished
  ObDASFusionChildRuntime runtime;
  runtime.finished_ = false;
  runtime.err_code_ = OB_SUCCESS;
  runtime.path_idx_ = 0;

  iter.parallel_ctx_.child_runtimes_.push_back(&runtime);

  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.merge_parallel_results());

  // Clean up - remove from array without destroying (stack allocated)
  iter.parallel_ctx_.child_runtimes_.reset();
}

TEST_F(ObDASFusionIterMergeTest, merge_with_failed_runtime)
{
  ObDASFusionIter iter;

  ObDASFusionChildRuntime runtime;
  runtime.finished_ = true;
  runtime.err_code_ = OB_CANCELED;
  runtime.path_idx_ = 0;

  iter.parallel_ctx_.child_runtimes_.push_back(&runtime);

  EXPECT_EQ(OB_CANCELED, iter.merge_parallel_results());

  iter.parallel_ctx_.child_runtimes_.reset();
}

// ============================================================================
// Section 16: release_parallel_runtime Tests
// ============================================================================

TEST_F(ObDASFusionIterMergeTest, release_parallel_runtime_when_not_inited)
{
  // Calling release_parallel_runtime when coordinator is not inited should be safe
  ObDASFusionIter iter;
  EXPECT_FALSE(iter.parallel_coordinator_.is_inited());
  iter.release_parallel_runtime();  // Should not crash
}

// ============================================================================
// Section 17: Bitmap discovery helper tests (static functions)
// ============================================================================
// Note: discover_bitmap_ops_dfs and find_bitmap_op_by_dfs_idx are file-static
// (anonymous namespace) in ob_das_fusion_parallel.cpp, so we test them indirectly
// through ObDASFusionParallelCtx::discover_shared_bitmaps.

TEST_F(ObDASFusionParallelCtxTest, discover_shared_bitmaps_null_iter)
{
  ObDASFusionParallelCtx ctx;
  // Should be safe with null iter
  EXPECT_EQ(OB_SUCCESS, ctx.discover_shared_bitmaps(alloc_, nullptr, 4));
  EXPECT_FALSE(ctx.has_shared_bitmaps());
}

TEST_F(ObDASFusionParallelCtxTest, discover_shared_bitmaps_dop_1)
{
  ObDASFusionParallelCtx ctx;
  // dop <= 1 means no range parallel, skip discovery
  int dummy = 0;
  ObDASIter *fake_iter = reinterpret_cast<ObDASIter *>(&dummy);
  EXPECT_EQ(OB_SUCCESS, ctx.discover_shared_bitmaps(alloc_, fake_iter, 1));
  EXPECT_FALSE(ctx.has_shared_bitmaps());
}

// ============================================================================
// Section 18: FusionCtDef parallel config tests
// ============================================================================

TEST_F(ObDASFusionParallelCtxTest, ctdef_parallel_config)
{
  ctdef_.enable_parallel_ = true;
  ctdef_.query_dop_ = 4;
  ctdef_.children_cnt_ = 3;
  ctdef_.has_search_subquery_ = true;

  EXPECT_TRUE(ctdef_.enable_parallel_);
  EXPECT_EQ(4, ctdef_.query_dop_);
}

TEST_F(ObDASFusionParallelCtxTest, ctdef_is_search_index)
{
  ctdef_.children_cnt_ = 3;
  ctdef_.has_search_subquery_ = true;
  ctdef_.set_search_index(1);

  EXPECT_TRUE(ctdef_.is_search_index(1));
  EXPECT_FALSE(ctdef_.is_search_index(0));
  EXPECT_FALSE(ctdef_.is_search_index(2));

  // When has_search_subquery_ is false, always returns false
  ctdef_.has_search_subquery_ = false;
  EXPECT_FALSE(ctdef_.is_search_index(1));
}

TEST_F(ObDASFusionParallelCtxTest, ctdef_is_vector_index)
{
  ctdef_.children_cnt_ = 3;
  ctdef_.has_vector_subquery_ = true;
  ctdef_.has_search_subquery_ = true;
  ctdef_.set_search_index(1);

  // Indices 0 and 2 are NOT the search index, so they are vector indices
  EXPECT_TRUE(ctdef_.is_vector_index(0));
  EXPECT_FALSE(ctdef_.is_vector_index(1));  // This is the search index
  EXPECT_TRUE(ctdef_.is_vector_index(2));

  // When has_vector_subquery_ is false, always returns false
  ctdef_.has_vector_subquery_ = false;
  EXPECT_FALSE(ctdef_.is_vector_index(0));
}

// ============================================================================
// Section 19: End-to-end simulation of parallel merge flow
// ============================================================================

class ParallelMergeSimulationTest : public ::testing::Test
{
protected:
  ParallelMergeSimulationTest()
      : alloc_("TestE2E"),
        exec_ctx_(alloc_),
        eval_ctx_(exec_ctx_),
        ctdef_(alloc_)
  {
    eval_ctx_.max_batch_size_ = 256;
  }

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;

    // Setup frames
    const int64_t frame_size = 1024 * 1024;
    exec_ctx_.frames_ = (char **)alloc_.alloc(2 * sizeof(char*));
    if (OB_NOT_NULL(exec_ctx_.frames_)) {
      exec_ctx_.frames_[0] = (char *)alloc_.alloc(frame_size);
      if (OB_NOT_NULL(exec_ctx_.frames_[0])) {
        memset(exec_ctx_.frames_[0], 0, frame_size);
      }
      exec_ctx_.frames_[1] = (char *)alloc_.alloc(frame_size);
      if (OB_NOT_NULL(exec_ctx_.frames_[1])) {
        memset(exec_ctx_.frames_[1], 0, frame_size);
      }
      exec_ctx_.frame_cnt_ = 2;
    }
    eval_ctx_.frames_ = exec_ctx_.frames_;

    // Setup ctdef
    ctdef_.op_type_ = DAS_OP_FUSION_QUERY;
    ctdef_.children_cnt_ = 2;
    ctdef_.has_search_subquery_ = true;
    ctdef_.has_vector_subquery_ = true;
    ctdef_.set_search_index(0);

    // Expressions
    rowid_expr_.obj_meta_.set_type(ObUInt64Type);
    rowid_expr_.datum_meta_.type_ = ObUInt64Type;
    rowid_expr_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    rowid_expr_.frame_idx_ = 0;
    int64_t pos = 0;
    rowid_expr_.datum_off_ = pos; pos += sizeof(ObDatum);
    rowid_expr_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    rowid_expr_.res_buf_off_ = pos; rowid_expr_.res_buf_len_ = 8; pos += 8;
    rowid_expr_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    score0_.obj_meta_.set_type(ObDoubleType);
    score0_.datum_meta_.type_ = ObDoubleType;
    score0_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    score0_.frame_idx_ = 0;
    score0_.datum_off_ = pos; pos += sizeof(ObDatum);
    score0_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    score0_.res_buf_off_ = pos; score0_.res_buf_len_ = 8; pos += 8;
    score0_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    score1_.obj_meta_.set_type(ObDoubleType);
    score1_.datum_meta_.type_ = ObDoubleType;
    score1_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    score1_.frame_idx_ = 0;
    score1_.datum_off_ = pos; pos += sizeof(ObDatum);
    score1_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    score1_.res_buf_off_ = pos; score1_.res_buf_len_ = 8; pos += 8;
    score1_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    fusion_score_.obj_meta_.set_type(ObDoubleType);
    fusion_score_.datum_meta_.type_ = ObDoubleType;
    fusion_score_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    fusion_score_.frame_idx_ = 0;
    fusion_score_.datum_off_ = pos; pos += sizeof(ObDatum);
    fusion_score_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    fusion_score_.res_buf_off_ = pos; fusion_score_.res_buf_len_ = 8; pos += 8;
    fusion_score_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    ctdef_.rowid_exprs_.init(1);
    ctdef_.rowid_exprs_.push_back(&rowid_expr_);
    ctdef_.score_exprs_.init(3);
    ctdef_.score_exprs_.push_back(&score0_);
    ctdef_.score_exprs_.push_back(&score1_);
    ctdef_.score_exprs_.push_back(&fusion_score_);
    ctdef_.result_output_.init(4);
    ctdef_.result_output_.push_back(&rowid_expr_);
    ctdef_.result_output_.push_back(&score0_);
    ctdef_.result_output_.push_back(&score1_);
    ctdef_.result_output_.push_back(&fusion_score_);
  }

  ObArenaAllocator alloc_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObDASFusionCtDef ctdef_;
  ObExpr rowid_expr_;
  ObExpr score0_;
  ObExpr score1_;
  ObExpr fusion_score_;
};

TEST_F(ParallelMergeSimulationTest, simulate_two_path_merge)
{
  // Simulate what happens after parallel tasks complete:
  // Path 0 (search): rows (1, 0.8), (2, 0.6)
  // Path 1 (vec):    rows (1, 0.7), (3, 0.9)
  // Expected after merge + fusion (weight_sum 0.5/0.5):
  //   doc 1: 0.8*0.5 + 0.7*0.5 = 0.75
  //   doc 2: 0.6*0.5 = 0.30
  //   doc 3: 0.9*0.5 = 0.45

  ObDASFusionIter iter;

  // Manually set up parallel_ctx_ with pre-populated runtimes
  ObDASFusionChildRuntime rt0, rt1;
  rt0.path_idx_ = 0;
  rt0.finished_ = true;
  rt0.err_code_ = OB_SUCCESS;
  rt0.rows_.push_back(ObDASFusionMaterializedRow(1, 0.8));
  rt0.rows_.push_back(ObDASFusionMaterializedRow(2, 0.6));

  rt1.path_idx_ = 1;
  rt1.finished_ = true;
  rt1.err_code_ = OB_SUCCESS;
  rt1.rows_.push_back(ObDASFusionMaterializedRow(1, 0.7));
  rt1.rows_.push_back(ObDASFusionMaterializedRow(3, 0.9));

  iter.parallel_ctx_.child_runtimes_.push_back(&rt0);
  iter.parallel_ctx_.child_runtimes_.push_back(&rt1);

  // Setup iter state
  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 10;
  iter.children_cnt_ = 2;

  // Create memctx for fusion_docs_
  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  // Call merge_parallel_results
  ASSERT_EQ(OB_SUCCESS, iter.merge_parallel_results());

  // Verify fusion_docs_
  EXPECT_EQ(3, iter.fusion_docs_.count());

  // Doc 1 should have both paths
  bool found_doc1 = false, found_doc2 = false, found_doc3 = false;
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    const ObDASFusionDocInfo &doc = iter.fusion_docs_.at(i);
    uint64_t rk = doc.get_uint64_rowkey();
    if (rk == 1) {
      found_doc1 = true;
      EXPECT_TRUE(doc.has_path(0));
      EXPECT_TRUE(doc.has_path(1));
      double s0 = 0, s1 = 0;
      ASSERT_EQ(OB_SUCCESS, doc.get_raw_score(0, s0));
      ASSERT_EQ(OB_SUCCESS, doc.get_raw_score(1, s1));
      EXPECT_DOUBLE_EQ(0.8, s0);
      EXPECT_DOUBLE_EQ(0.7, s1);
    } else if (rk == 2) {
      found_doc2 = true;
      EXPECT_TRUE(doc.has_path(0));
      EXPECT_FALSE(doc.has_path(1));
    } else if (rk == 3) {
      found_doc3 = true;
      EXPECT_FALSE(doc.has_path(0));
      EXPECT_TRUE(doc.has_path(1));
    }
  }
  EXPECT_TRUE(found_doc1);
  EXPECT_TRUE(found_doc2);
  EXPECT_TRUE(found_doc3);

  // Clean up
  iter.parallel_ctx_.child_runtimes_.reset();
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    iter.fusion_docs_.at(i).reset();
  }
  iter.fusion_docs_.reset();
  iter.destroy_rowid_map();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ParallelMergeSimulationTest, simulate_range_parallel_merge)
{
  // Simulate range-parallel merge: 3 runtimes all on path_idx_ = 0 (search path)
  // with different docid ranges. After merge, duplicates should be handled correctly.
  //
  // Range 0: (1, 0.9), (2, 0.8)
  // Range 1: (3, 0.7), (4, 0.6)
  // Range 2: (5, 0.5)

  ObDASFusionIter iter;

  ObDASFusionChildRuntime rt0, rt1, rt2;
  rt0.path_idx_ = 0;
  rt0.finished_ = true;
  rt0.err_code_ = OB_SUCCESS;
  rt0.is_range_parallel_ = true;
  rt0.range_top_k_limit_ = 5;
  rt0.rows_.push_back(ObDASFusionMaterializedRow(1, 0.9));
  rt0.rows_.push_back(ObDASFusionMaterializedRow(2, 0.8));

  rt1.path_idx_ = 0;
  rt1.finished_ = true;
  rt1.err_code_ = OB_SUCCESS;
  rt1.is_range_parallel_ = true;
  rt1.range_top_k_limit_ = 5;
  rt1.rows_.push_back(ObDASFusionMaterializedRow(3, 0.7));
  rt1.rows_.push_back(ObDASFusionMaterializedRow(4, 0.6));

  rt2.path_idx_ = 0;
  rt2.finished_ = true;
  rt2.err_code_ = OB_SUCCESS;
  rt2.is_range_parallel_ = true;
  rt2.range_top_k_limit_ = 5;
  rt2.rows_.push_back(ObDASFusionMaterializedRow(5, 0.5));

  iter.parallel_ctx_.child_runtimes_.push_back(&rt0);
  iter.parallel_ctx_.child_runtimes_.push_back(&rt1);
  iter.parallel_ctx_.child_runtimes_.push_back(&rt2);

  // Only 1 child (search path) in ctdef
  ObDASFusionCtDef single_ctdef(alloc_);
  single_ctdef.op_type_ = DAS_OP_FUSION_QUERY;
  single_ctdef.children_cnt_ = 1;
  single_ctdef.has_search_subquery_ = true;
  single_ctdef.set_search_index(0);

  single_ctdef.rowid_exprs_.init(1);
  single_ctdef.rowid_exprs_.push_back(&rowid_expr_);
  single_ctdef.score_exprs_.init(2);
  single_ctdef.score_exprs_.push_back(&score0_);
  single_ctdef.score_exprs_.push_back(&fusion_score_);

  iter.fusion_ctdef_ = &single_ctdef;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 10;
  iter.children_cnt_ = 1;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  ASSERT_EQ(OB_SUCCESS, iter.merge_parallel_results());

  // All 5 rows merged into path 0, no duplicates
  EXPECT_EQ(5, iter.fusion_docs_.count());

  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    const ObDASFusionDocInfo &doc = iter.fusion_docs_.at(i);
    EXPECT_TRUE(doc.has_path(0));
  }

  // Clean up
  iter.parallel_ctx_.child_runtimes_.reset();
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    iter.fusion_docs_.at(i).reset();
  }
  iter.fusion_docs_.reset();
  iter.destroy_rowid_map();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ParallelMergeSimulationTest, simulate_range_parallel_merge_respects_logical_path_topk)
{
  // The logical Query path has global top-2 = {(1, 100.0), (2, 99.0)}.
  // Range-parallel workers each return local top-2:
  //   range 0 -> (1, 100.0), (3, 97.0)
  //   range 1 -> (2, 99.0),  (4, 96.0)
  // If we skip the path-local top-k merge before Fusion, docs 3/4 would
  // incorrectly keep Query-path contributions.
  ObDASFusionIter iter;

  ObDASFusionChildRuntime query_rt0, query_rt1, vec_rt;
  query_rt0.path_idx_ = 0;
  query_rt0.finished_ = true;
  query_rt0.err_code_ = OB_SUCCESS;
  query_rt0.is_range_parallel_ = true;
  query_rt0.range_top_k_limit_ = 2;
  query_rt0.rows_.push_back(ObDASFusionMaterializedRow(1, 100.0));
  query_rt0.rows_.push_back(ObDASFusionMaterializedRow(3, 97.0));

  query_rt1.path_idx_ = 0;
  query_rt1.finished_ = true;
  query_rt1.err_code_ = OB_SUCCESS;
  query_rt1.is_range_parallel_ = true;
  query_rt1.range_top_k_limit_ = 2;
  query_rt1.rows_.push_back(ObDASFusionMaterializedRow(2, 99.0));
  query_rt1.rows_.push_back(ObDASFusionMaterializedRow(4, 96.0));

  vec_rt.path_idx_ = 1;
  vec_rt.finished_ = true;
  vec_rt.err_code_ = OB_SUCCESS;
  vec_rt.rows_.push_back(ObDASFusionMaterializedRow(3, 100.0));
  vec_rt.rows_.push_back(ObDASFusionMaterializedRow(1, 1.0));

  iter.parallel_ctx_.child_runtimes_.push_back(&query_rt0);
  iter.parallel_ctx_.child_runtimes_.push_back(&query_rt1);
  iter.parallel_ctx_.child_runtimes_.push_back(&vec_rt);

  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 2;
  iter.children_cnt_ = 2;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  ASSERT_EQ(OB_SUCCESS, iter.merge_parallel_results());

  EXPECT_EQ(3, iter.fusion_docs_.count());

  bool found_doc1 = false;
  bool found_doc2 = false;
  bool found_doc3 = false;
  bool found_doc4 = false;
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    const ObDASFusionDocInfo &doc = iter.fusion_docs_.at(i);
    const uint64_t rowkey = doc.get_uint64_rowkey();
    if (1 == rowkey) {
      found_doc1 = true;
      EXPECT_TRUE(doc.has_path(0));
      EXPECT_TRUE(doc.has_path(1));
    } else if (2 == rowkey) {
      found_doc2 = true;
      EXPECT_TRUE(doc.has_path(0));
      EXPECT_FALSE(doc.has_path(1));
    } else if (3 == rowkey) {
      found_doc3 = true;
      EXPECT_FALSE(doc.has_path(0));
      EXPECT_TRUE(doc.has_path(1));
    } else if (4 == rowkey) {
      found_doc4 = true;
    }
  }

  EXPECT_TRUE(found_doc1);
  EXPECT_TRUE(found_doc2);
  EXPECT_TRUE(found_doc3);
  EXPECT_FALSE(found_doc4);

  iter.parallel_ctx_.child_runtimes_.reset();
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    iter.fusion_docs_.at(i).reset();
  }
  iter.fusion_docs_.reset();
  iter.destroy_rowid_map();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ParallelMergeSimulationTest, simulate_empty_runtimes)
{
  // All runtimes finished successfully but with no rows
  ObDASFusionIter iter;

  ObDASFusionChildRuntime rt0, rt1;
  rt0.path_idx_ = 0;
  rt0.finished_ = true;
  rt0.err_code_ = OB_SUCCESS;

  rt1.path_idx_ = 1;
  rt1.finished_ = true;
  rt1.err_code_ = OB_SUCCESS;

  iter.parallel_ctx_.child_runtimes_.push_back(&rt0);
  iter.parallel_ctx_.child_runtimes_.push_back(&rt1);

  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 10;
  iter.children_cnt_ = 2;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  ASSERT_EQ(OB_SUCCESS, iter.merge_parallel_results());
  EXPECT_EQ(0, iter.fusion_docs_.count());

  iter.parallel_ctx_.child_runtimes_.reset();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ParallelMergeSimulationTest, simulate_null_runtime_in_list)
{
  ObDASFusionIter iter;

  iter.parallel_ctx_.child_runtimes_.push_back(nullptr);

  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.children_cnt_ = 2;

  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.merge_parallel_results());

  iter.parallel_ctx_.child_runtimes_.reset();
}

// ============================================================================
// Section 20: ObFastBitmap and Iterator basic tests
// ============================================================================

class ObFastBitmapTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }
};

TEST_F(ObFastBitmapTest, init_and_add)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));

  EXPECT_EQ(0, bitmap.cardinality());

  ASSERT_EQ(OB_SUCCESS, bitmap.add(0));
  EXPECT_EQ(1, bitmap.cardinality());

  ASSERT_EQ(OB_SUCCESS, bitmap.add(100));
  EXPECT_EQ(2, bitmap.cardinality());

  // Adding the same value again should not increase cardinality
  ASSERT_EQ(OB_SUCCESS, bitmap.add(100));
  EXPECT_EQ(2, bitmap.cardinality());
}

TEST_F(ObFastBitmapTest, init_invalid_arg)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  // Negative max_value_count should fail
  EXPECT_EQ(OB_INVALID_ARGUMENT, bitmap.init(-1));
}

TEST_F(ObFastBitmapTest, add_large_value)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));

  // Value that spans multiple chunks (each chunk covers 2^16 = 65536 values)
  const uint64_t large_val = 100000;
  ASSERT_EQ(OB_SUCCESS, bitmap.add(large_val));
  EXPECT_EQ(1, bitmap.cardinality());
}

TEST_F(ObFastBitmapTest, add_across_chunk_boundary)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));

  // Values at the chunk boundary (65535 and 65536)
  const uint64_t last_in_chunk0 = (1ULL << 16) - 1; // 65535
  const uint64_t first_in_chunk1 = (1ULL << 16);     // 65536
  ASSERT_EQ(OB_SUCCESS, bitmap.add(last_in_chunk0));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(first_in_chunk1));
  EXPECT_EQ(2, bitmap.cardinality());
}

// ============================================================================
// Section 21: ObFastBitmap::Iterator edge cases
// ============================================================================

TEST_F(ObFastBitmapTest, iterator_init_null)
{
  ObFastBitmap::Iterator iter;
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.init(nullptr));
}

TEST_F(ObFastBitmapTest, iterator_not_inited)
{
  ObFastBitmap::Iterator iter;
  EXPECT_FALSE(iter.is_inited());
}

TEST_F(ObFastBitmapTest, iterator_init_and_next)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(10));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(20));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(30));

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));

  uint64_t val = 0;
  EXPECT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(10ULL, val);

  EXPECT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(20ULL, val);

  EXPECT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(30ULL, val);

  // Past the end
  EXPECT_EQ(OB_ITER_END, iter.next_id(val));
}

TEST_F(ObFastBitmapTest, iterator_advance_to)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(10));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(20));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(30));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(50));

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));

  uint64_t val = 0;
  // Advance to 15 -> should land on 20
  EXPECT_EQ(OB_SUCCESS, iter.advance_to(15, val));
  EXPECT_EQ(20ULL, val);

  // Advance to 20 -> should stay at 20
  EXPECT_EQ(OB_SUCCESS, iter.advance_to(20, val));
  EXPECT_EQ(20ULL, val);

  // Advance to 25 -> should land on 30
  EXPECT_EQ(OB_SUCCESS, iter.advance_to(25, val));
  EXPECT_EQ(30ULL, val);

  // Advance to 40 -> should land on 50
  EXPECT_EQ(OB_SUCCESS, iter.advance_to(40, val));
  EXPECT_EQ(50ULL, val);

  // Advance beyond any value -> ITER_END
  EXPECT_EQ(OB_ITER_END, iter.advance_to(100, val));
}


TEST_F(ObFastBitmapTest, iterator_on_empty_bitmap)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  // No values added

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));

  uint64_t val = 0;
  EXPECT_EQ(OB_ITER_END, iter.next_id(val));
  EXPECT_EQ(OB_ITER_END, iter.advance_to(0, val));
}

TEST_F(ObFastBitmapTest, iterator_reuse)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(5));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(15));

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));

  uint64_t val = 0;
  EXPECT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(5ULL, val);

  iter.reuse();
  // After reuse, should start from the beginning again
  EXPECT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(5ULL, val);
  EXPECT_EQ(OB_SUCCESS, iter.next_id(val));
  EXPECT_EQ(15ULL, val);
}

TEST_F(ObFastBitmapTest, iterator_reset)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(5));

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));
  EXPECT_TRUE(iter.is_inited());

  iter.reset();
  EXPECT_FALSE(iter.is_inited());
}

TEST_F(ObFastBitmapTest, iterator_advance_to_before_current)
{
  ObArenaAllocator alloc("TestBmp");
  ObFastBitmap bitmap(alloc);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(1024));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(10));
  ASSERT_EQ(OB_SUCCESS, bitmap.add(30));

  ObFastBitmap::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(&bitmap));

  uint64_t val = 0;
  // Move to 30
  EXPECT_EQ(OB_SUCCESS, iter.advance_to(30, val));
  EXPECT_EQ(30ULL, val);

  // Advance to 10 (before current) -> should return current position (30)
  EXPECT_EQ(OB_SUCCESS, iter.advance_to(10, val));
  EXPECT_EQ(30ULL, val);
}

// ============================================================================
// Section 22: ObBitmapPhaseBarrier additional tests
// ============================================================================

TEST_F(ObBitmapPhaseBarrierTest, double_init)
{
  ObBitmapPhaseBarrier barrier;
  ASSERT_EQ(OB_SUCCESS, barrier.init(3));
  EXPECT_TRUE(barrier.is_inited_);
  EXPECT_EQ(3, barrier.total_cnt_);

  // Second init should re-init since the implementation checks
  // is_inited_ and skips cond_.init() if already inited
  ASSERT_EQ(OB_SUCCESS, barrier.init(5));
  EXPECT_EQ(5, barrier.total_cnt_);
  EXPECT_EQ(0, barrier.built_cnt_);
}

TEST_F(ObBitmapPhaseBarrierTest, wait_returns_first_error_when_all_built)
{
  ObBitmapPhaseBarrier barrier;
  ASSERT_EQ(OB_SUCCESS, barrier.init(3));

  // Two succeed, one fails
  barrier.on_bitmap_built(OB_SUCCESS);
  barrier.on_bitmap_built(OB_ALLOCATE_MEMORY_FAILED);
  barrier.on_bitmap_built(OB_SUCCESS);

  // wait_all_built should return the first error
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, barrier.wait_all_built(INT64_MAX));
}

TEST_F(ObBitmapPhaseBarrierTest, concurrent_with_some_errors)
{
  ObBitmapPhaseBarrier barrier;
  const int64_t N = 8;
  ASSERT_EQ(OB_SUCCESS, barrier.init(N));

  std::vector<std::thread> threads;
  std::atomic<int> error_count(0);

  for (int64_t i = 0; i < N; ++i) {
    threads.emplace_back([&barrier, &error_count, i]() {
      usleep(static_cast<useconds_t>(i * 300));
      int err = (i % 3 == 0) ? OB_ALLOCATE_MEMORY_FAILED : OB_SUCCESS;
      if (err != OB_SUCCESS) {
        error_count.fetch_add(1);
      }
      barrier.on_bitmap_built(err);
    });
  }

  int64_t timeout_ts = ObTimeUtility::current_time() + 10000000;
  int ret = barrier.wait_all_built(timeout_ts);

  for (auto &t : threads) {
    t.join();
  }

  if (error_count.load() > 0) {
    EXPECT_NE(OB_SUCCESS, ret);
  } else {
    EXPECT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(ObBitmapPhaseBarrierTest, on_bitmap_built_with_success_code)
{
  ObBitmapPhaseBarrier barrier;
  ASSERT_EQ(OB_SUCCESS, barrier.init(2));

  // Calling on_bitmap_built with OB_SUCCESS should not affect first_err_
  barrier.on_bitmap_built(OB_SUCCESS);
  barrier.on_bitmap_built(OB_SUCCESS);
  EXPECT_EQ(OB_SUCCESS, ATOMIC_LOAD(&barrier.first_err_));
}

TEST_F(ObBitmapPhaseBarrierTest, already_complete_wait_returns_immediately)
{
  ObBitmapPhaseBarrier barrier;
  ASSERT_EQ(OB_SUCCESS, barrier.init(2));
  barrier.on_bitmap_built(OB_SUCCESS);
  barrier.on_bitmap_built(OB_SUCCESS);

  // Should return immediately with success
  EXPECT_EQ(OB_SUCCESS, barrier.wait_all_built(INT64_MAX));

  // Second wait should also return immediately
  EXPECT_EQ(OB_SUCCESS, barrier.wait_all_built(INT64_MAX));
}

// ============================================================================
// Section 23: ObDASFusionParallelCoordinator additional tests
// ============================================================================

TEST_F(ObDASFusionParallelCoordinatorTest, double_init)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(3));
  EXPECT_TRUE(coord.is_inited());
  EXPECT_EQ(3, coord.total_cnt_);

  // Second init should update count
  ASSERT_EQ(OB_SUCCESS, coord.init(5));
  EXPECT_EQ(5, coord.total_cnt_);
  EXPECT_EQ(0, coord.finished_cnt_);
}

TEST_F(ObDASFusionParallelCoordinatorTest, wait_already_complete)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(2));
  coord.on_child_finish(OB_SUCCESS);
  coord.on_child_finish(OB_SUCCESS);

  // Already complete, wait should return immediately
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(INT64_MAX));

  // Calling wait again should also return immediately
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(INT64_MAX));
}

TEST_F(ObDASFusionParallelCoordinatorTest, set_first_error_ignores_success_detailed)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));

  // Setting OB_SUCCESS should not change anything
  coord.set_first_error(OB_SUCCESS);
  EXPECT_EQ(OB_SUCCESS, coord.get_first_error());

  // Setting an actual error should update
  coord.set_first_error(OB_ERR_UNEXPECTED);
  EXPECT_EQ(OB_ERR_UNEXPECTED, coord.get_first_error());

  // A second error should not overwrite the first
  coord.set_first_error(OB_TIMEOUT);
  EXPECT_EQ(OB_ERR_UNEXPECTED, coord.get_first_error());

  // OB_SUCCESS should not overwrite the error
  coord.set_first_error(OB_SUCCESS);
  EXPECT_EQ(OB_ERR_UNEXPECTED, coord.get_first_error());
}

// ============================================================================
// Section 24: ObSharedBitmapSlot additional tests
// ============================================================================

TEST(ObSharedBitmapSlotAdditionalTest, occurrence_index_default_and_reset)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObSharedBitmapSlot slot;
  EXPECT_EQ(-1, slot.bitmap_occurrence_idx_);
  EXPECT_EQ(nullptr, slot.bitmap_);
  EXPECT_FALSE(slot.is_built_);
  EXPECT_EQ(OB_SUCCESS, slot.build_ret_);

  slot.bitmap_occurrence_idx_ = 3;
  slot.is_built_ = true;
  slot.build_ret_ = OB_ERR_UNEXPECTED;

  slot.release();

  // After release, occurrence idx is NOT reset (only bitmap_, is_built_, build_ret_ are reset)
  // The release() function only resets bitmap-related state
  EXPECT_EQ(nullptr, slot.bitmap_);
  EXPECT_FALSE(slot.is_built_);
  EXPECT_EQ(OB_SUCCESS, slot.build_ret_);
}

// ============================================================================
// Section 25: ObDASFusionChildRuntime materialize_batch_result null guards
// ============================================================================

TEST_F(ObDASFusionChildRuntimeTest, materialize_batch_result_null_eval_ctx)
{
  ObDASFusionChildRuntime runtime;
  runtime.child_eval_ctx_ = nullptr;
  runtime.rowkey_expr_ = reinterpret_cast<ObExpr *>(0xDEAD);  // non-null placeholder
  runtime.score_expr_ = reinterpret_cast<ObExpr *>(0xBEEF);   // non-null placeholder
  runtime.max_batch_size_ = 1;

  EXPECT_EQ(OB_ERR_UNEXPECTED, runtime.materialize_batch_result(1));
}

TEST_F(ObDASFusionChildRuntimeTest, materialize_batch_result_null_rowkey_expr)
{
  ObDASFusionChildRuntime runtime;
  ObExecContext exec_ctx(runtime.child_allocator_);
  ObEvalCtx eval_ctx(exec_ctx);
  runtime.child_eval_ctx_ = &eval_ctx;
  runtime.rowkey_expr_ = nullptr;
  runtime.score_expr_ = reinterpret_cast<ObExpr *>(0xBEEF);
  runtime.max_batch_size_ = 1;

  EXPECT_EQ(OB_ERR_UNEXPECTED, runtime.materialize_batch_result(1));
}

TEST_F(ObDASFusionChildRuntimeTest, materialize_batch_result_null_score_expr)
{
  ObDASFusionChildRuntime runtime;
  ObExecContext exec_ctx(runtime.child_allocator_);
  ObEvalCtx eval_ctx(exec_ctx);
  runtime.child_eval_ctx_ = &eval_ctx;
  runtime.rowkey_expr_ = reinterpret_cast<ObExpr *>(0xDEAD);
  runtime.score_expr_ = nullptr;
  runtime.max_batch_size_ = 1;

  EXPECT_EQ(OB_ERR_UNEXPECTED, runtime.materialize_batch_result(1));
}

TEST_F(ObDASFusionChildRuntimeTest, materialize_batch_result_zero_batch_size)
{
  ObDASFusionChildRuntime runtime;
  ObExecContext exec_ctx(runtime.child_allocator_);
  ObEvalCtx eval_ctx(exec_ctx);
  runtime.child_eval_ctx_ = &eval_ctx;
  runtime.rowkey_expr_ = reinterpret_cast<ObExpr *>(0xDEAD);
  runtime.score_expr_ = reinterpret_cast<ObExpr *>(0xBEEF);
  runtime.max_batch_size_ = 1;

  // batch_size <= 0 should fail
  EXPECT_EQ(OB_ERR_UNEXPECTED, runtime.materialize_batch_result(0));
  EXPECT_EQ(OB_ERR_UNEXPECTED, runtime.materialize_batch_result(-1));
}

TEST_F(ObDASFusionChildRuntimeTest, materialize_batch_result_negative_batch_size)
{
  ObDASFusionChildRuntime runtime;
  ObExecContext exec_ctx(runtime.child_allocator_);
  ObEvalCtx eval_ctx(exec_ctx);
  runtime.child_eval_ctx_ = &eval_ctx;
  runtime.rowkey_expr_ = reinterpret_cast<ObExpr *>(0xDEAD);
  runtime.score_expr_ = reinterpret_cast<ObExpr *>(0xBEEF);

  EXPECT_EQ(OB_ERR_UNEXPECTED, runtime.materialize_batch_result(-5));
}

// ============================================================================
// Section 26: ObDASFusionChildRuntime drain_child_iter null iter guard
// ============================================================================

TEST_F(ObDASFusionChildRuntimeTest, drain_child_iter_null_iter)
{
  ObDASFusionChildRuntime runtime;
  runtime.child_iter_ = nullptr;

  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));

  EXPECT_EQ(OB_ERR_UNEXPECTED, runtime.drain_child_iter(coord));
}

// ============================================================================
// Section 27: ObDASFusionChildRuntime inject_shared_bitmaps validation
// ============================================================================

TEST_F(ObDASFusionChildRuntimeTest, inject_shared_bitmaps_null_parallel_ctx)
{
  ObDASFusionChildRuntime runtime;
  runtime.parallel_ctx_ = nullptr;

  // When parallel_ctx_ is null, inject_shared_bitmaps should return OB_ERR_UNEXPECTED
  EXPECT_EQ(OB_ERR_UNEXPECTED, runtime.inject_shared_bitmaps());
}

TEST_F(ObDASFusionChildRuntimeTest, inject_shared_bitmaps_no_shared_bitmaps)
{
  // When parallel_ctx_ has no shared bitmaps, inject_shared_bitmaps should return OB_SUCCESS
  ObDASFusionChildRuntime runtime;
  ObDASFusionParallelCtx ctx;
  runtime.parallel_ctx_ = &ctx;
  // ctx has no shared bitmaps by default
  EXPECT_FALSE(ctx.has_shared_bitmaps());

  EXPECT_EQ(OB_SUCCESS, runtime.inject_shared_bitmaps());
}

TEST_F(ObDASFusionChildRuntimeTest, inject_shared_bitmaps_null_bitmap_in_slot)
{
  // When a shared bitmap slot has a null bitmap after barrier, inject should fail
  ObDASFusionChildRuntime runtime;
  ObArenaAllocator alloc("TestInj");
  ObDASFusionParallelCtx ctx;
  runtime.parallel_ctx_ = &ctx;

  // Manually push a slot with null bitmap
  ObSharedBitmapSlot *slot = nullptr;
  void *slot_buf = alloc.alloc(sizeof(ObSharedBitmapSlot));
  ASSERT_NE(nullptr, slot_buf);
  slot = new (slot_buf) ObSharedBitmapSlot();
  slot->bitmap_occurrence_idx_ = 0;
  slot->bitmap_ = nullptr;  // null bitmap!
  ATOMIC_STORE(&slot->is_built_, true);

  ctx.shared_bitmap_slots_.push_back(slot);

  // child_bitmap_ops_ is empty, so bitmap_occurrence_idx_ check should also fail
  // But first comes the null bitmap check
  EXPECT_EQ(OB_ERR_UNEXPECTED, runtime.inject_shared_bitmaps());

  // Clean up
  slot->release();
  slot->~ObSharedBitmapSlot();
}

// ============================================================================
// Section 28: ObDASFusionChildRuntime wait_all_bitmaps_ready early returns
// ============================================================================

TEST_F(ObDASFusionChildRuntimeTest, wait_all_bitmaps_ready_null_parallel_ctx)
{
  ObDASFusionChildRuntime runtime;
  runtime.parallel_ctx_ = nullptr;

  // When parallel_ctx_ is null, wait_all_bitmaps_ready should return OB_ERR_UNEXPECTED
  EXPECT_EQ(OB_ERR_UNEXPECTED, runtime.wait_all_bitmaps_ready(INT64_MAX));
}

TEST_F(ObDASFusionChildRuntimeTest, wait_all_bitmaps_ready_no_shared_bitmaps)
{
  ObDASFusionChildRuntime runtime;
  ObDASFusionParallelCtx ctx;
  runtime.parallel_ctx_ = &ctx;
  EXPECT_FALSE(ctx.has_shared_bitmaps());

  // When there are no shared bitmaps, should return OB_SUCCESS
  EXPECT_EQ(OB_SUCCESS, runtime.wait_all_bitmaps_ready(INT64_MAX));
}

// ============================================================================
// Section 29: ObDASFusionChildRuntime build_assigned_bitmaps empty slots
// ============================================================================

TEST_F(ObDASFusionChildRuntimeTest, build_assigned_bitmaps_empty_slots)
{
  ObDASFusionChildRuntime runtime;
  // assigned_bitmap_slots_ is empty by default

  // When there are no assigned slots, build_assigned_bitmaps should return OB_SUCCESS
  EXPECT_EQ(OB_SUCCESS, runtime.build_assigned_bitmaps(INT64_MAX));
}

TEST_F(ObDASFusionChildRuntimeTest, build_assigned_bitmaps_null_parallel_ctx_barrier)
{
  ObDASFusionChildRuntime runtime;
  // parallel_ctx_ is null by default. When assigned_bitmap_slots_ is empty,
  // the function returns early with OB_SUCCESS without accessing parallel_ctx_.

  EXPECT_EQ(OB_SUCCESS, runtime.build_assigned_bitmaps(INT64_MAX));
}

// ISSUE: build_assigned_bitmaps() has a null-deref bug when a null slot is
// encountered. After `ret = OB_ERR_UNEXPECTED` in the null-slot check, the
// code still executes `slot->build_ret_ = ret;` and
// `ATOMIC_STORE(&slot->is_built_, true);` within the same loop iteration,
// dereferencing the null slot pointer. This would cause a SEGFAULT.
// The production code should guard these assignments with `if (OB_NOT_NULL(slot))`.
// A unit test for this case cannot be written until the bug is fixed.
//
// TEST_F(ObDASFusionChildRuntimeTest, build_assigned_bitmaps_null_slot) { ... }

// ============================================================================
// Section 30: ObDASFusionChildRuntime create_parallel_iter null context guards
// ============================================================================

TEST_F(ObDASFusionChildRuntimeTest, create_parallel_iter_null_contexts)
{
  ObDASFusionChildRuntime runtime;
  runtime.path_idx_ = 0;
  // All contexts are null by default
  runtime.child_exec_ctx_ = nullptr;
  runtime.child_eval_ctx_ = nullptr;
  runtime.child_search_ctx_ = nullptr;

  EXPECT_EQ(OB_ERR_UNEXPECTED, runtime.create_parallel_iter());
}

// ============================================================================
// Section 31: ObDASFusionChildRuntime prepare_parallel_resources null sources
// ============================================================================

TEST_F(ObDASFusionChildRuntimeTest, prepare_parallel_resources_null_src_search_ctx)
{
  ObDASFusionChildRuntime runtime;
  ObArenaAllocator alloc("TestRT");
  ObExecContext exec_ctx(alloc);
  ObEvalCtx eval_ctx(exec_ctx);

  runtime.src_exec_ctx_ = &exec_ctx;
  runtime.src_eval_ctx_ = &eval_ctx;
  runtime.src_search_ctx_ = nullptr;  // null search ctx

  // Should fail because src_search_ctx_ is null
  EXPECT_EQ(OB_ERR_UNEXPECTED, runtime.prepare_parallel_resources());
}

// ============================================================================
// Section 32: merge_parallel_results additional edge cases
// ============================================================================

class ObDASFusionIterMergeEdgeCaseTest : public ::testing::Test
{
protected:
  ObDASFusionIterMergeEdgeCaseTest()
      : alloc_("TestMergeEdge"),
        exec_ctx_(alloc_),
        eval_ctx_(exec_ctx_),
        ctdef_(alloc_)
  {
    eval_ctx_.max_batch_size_ = 256;
  }

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;

    const int64_t frame_size = 1024 * 1024;
    exec_ctx_.frames_ = (char **)alloc_.alloc(2 * sizeof(char*));
    if (OB_NOT_NULL(exec_ctx_.frames_)) {
      exec_ctx_.frames_[0] = (char *)alloc_.alloc(frame_size);
      if (OB_NOT_NULL(exec_ctx_.frames_[0])) {
        memset(exec_ctx_.frames_[0], 0, frame_size);
      }
      exec_ctx_.frames_[1] = (char *)alloc_.alloc(frame_size);
      if (OB_NOT_NULL(exec_ctx_.frames_[1])) {
        memset(exec_ctx_.frames_[1], 0, frame_size);
      }
      exec_ctx_.frame_cnt_ = 2;
    }
    eval_ctx_.frames_ = exec_ctx_.frames_;

    rowid_expr_.obj_meta_.set_type(ObUInt64Type);
    rowid_expr_.datum_meta_.type_ = ObUInt64Type;
    rowid_expr_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    rowid_expr_.frame_idx_ = 0;
    int64_t pos = 0;
    rowid_expr_.datum_off_ = pos; pos += sizeof(ObDatum);
    rowid_expr_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    rowid_expr_.res_buf_off_ = pos; rowid_expr_.res_buf_len_ = 8; pos += 8;
    rowid_expr_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    score0_.obj_meta_.set_type(ObDoubleType);
    score0_.datum_meta_.type_ = ObDoubleType;
    score0_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    score0_.frame_idx_ = 0;
    score0_.datum_off_ = pos; pos += sizeof(ObDatum);
    score0_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    score0_.res_buf_off_ = pos; score0_.res_buf_len_ = 8; pos += 8;
    score0_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    score1_.obj_meta_.set_type(ObDoubleType);
    score1_.datum_meta_.type_ = ObDoubleType;
    score1_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    score1_.frame_idx_ = 0;
    score1_.datum_off_ = pos; pos += sizeof(ObDatum);
    score1_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    score1_.res_buf_off_ = pos; score1_.res_buf_len_ = 8; pos += 8;
    score1_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    fusion_score_.obj_meta_.set_type(ObDoubleType);
    fusion_score_.datum_meta_.type_ = ObDoubleType;
    fusion_score_.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    fusion_score_.frame_idx_ = 0;
    fusion_score_.datum_off_ = pos; pos += sizeof(ObDatum);
    fusion_score_.eval_info_off_ = pos; pos += sizeof(ObEvalInfo);
    fusion_score_.res_buf_off_ = pos; fusion_score_.res_buf_len_ = 8; pos += 8;
    fusion_score_.vector_header_off_ = pos; pos += sizeof(VectorHeader);

    ctdef_.op_type_ = DAS_OP_FUSION_QUERY;
    ctdef_.children_cnt_ = 2;
    ctdef_.has_search_subquery_ = true;
    ctdef_.has_vector_subquery_ = true;
    ctdef_.set_search_index(0);
    ctdef_.rowid_exprs_.init(1);
    ctdef_.rowid_exprs_.push_back(&rowid_expr_);
    ctdef_.score_exprs_.init(3);
    ctdef_.score_exprs_.push_back(&score0_);
    ctdef_.score_exprs_.push_back(&score1_);
    ctdef_.score_exprs_.push_back(&fusion_score_);
    ctdef_.result_output_.init(4);
    ctdef_.result_output_.push_back(&rowid_expr_);
    ctdef_.result_output_.push_back(&score0_);
    ctdef_.result_output_.push_back(&score1_);
    ctdef_.result_output_.push_back(&fusion_score_);
  }

  ObArenaAllocator alloc_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObDASFusionCtDef ctdef_;
  ObExpr rowid_expr_;
  ObExpr score0_;
  ObExpr score1_;
  ObExpr fusion_score_;
};

TEST_F(ObDASFusionIterMergeEdgeCaseTest, merge_with_inconsistent_range_top_k_limit)
{
  // Range-parallel runtimes of the same path always have the same top_k_limit,
  // so no consistency check is needed. Last-set value wins.
  ObDASFusionIter iter;

  ObDASFusionChildRuntime rt0, rt1;
  rt0.path_idx_ = 0;
  rt0.finished_ = true;
  rt0.err_code_ = OB_SUCCESS;
  rt0.is_range_parallel_ = true;
  rt0.range_top_k_limit_ = 10;
  rt0.rows_.push_back(ObDASFusionMaterializedRow(1, 0.9));

  rt1.path_idx_ = 0;
  rt1.finished_ = true;
  rt1.err_code_ = OB_SUCCESS;
  rt1.is_range_parallel_ = true;
  rt1.range_top_k_limit_ = 5;
  rt1.rows_.push_back(ObDASFusionMaterializedRow(2, 0.8));

  iter.parallel_ctx_.child_runtimes_.push_back(&rt0);
  iter.parallel_ctx_.child_runtimes_.push_back(&rt1);

  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 10;
  iter.children_cnt_ = 2;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  EXPECT_EQ(OB_SUCCESS, iter.merge_parallel_results());

  iter.parallel_ctx_.child_runtimes_.reset();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ObDASFusionIterMergeEdgeCaseTest, merge_with_invalid_path_idx)
{
  // Runtime with path_idx_ out of range should fail
  ObDASFusionIter iter;

  ObDASFusionChildRuntime rt;
  rt.path_idx_ = 99;  // Out of range for children_cnt_ = 2
  rt.finished_ = true;
  rt.err_code_ = OB_SUCCESS;
  rt.rows_.push_back(ObDASFusionMaterializedRow(1, 0.9));

  iter.parallel_ctx_.child_runtimes_.push_back(&rt);

  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 10;
  iter.children_cnt_ = 2;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.merge_parallel_results());

  iter.parallel_ctx_.child_runtimes_.reset();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ObDASFusionIterMergeEdgeCaseTest, merge_single_range_parallel_runtime)
{
  // A single range-parallel runtime (no path-top-k merge needed)
  ObDASFusionIter iter;

  ObDASFusionChildRuntime rt;
  rt.path_idx_ = 0;
  rt.finished_ = true;
  rt.err_code_ = OB_SUCCESS;
  rt.is_range_parallel_ = true;
  rt.range_top_k_limit_ = 3;
  rt.rows_.push_back(ObDASFusionMaterializedRow(10, 0.9));
  rt.rows_.push_back(ObDASFusionMaterializedRow(20, 0.8));

  iter.parallel_ctx_.child_runtimes_.push_back(&rt);

  ObDASFusionCtDef single_ctdef(alloc_);
  single_ctdef.op_type_ = DAS_OP_FUSION_QUERY;
  single_ctdef.children_cnt_ = 1;
  single_ctdef.has_search_subquery_ = true;
  single_ctdef.set_search_index(0);
  single_ctdef.rowid_exprs_.init(1);
  single_ctdef.rowid_exprs_.push_back(&rowid_expr_);
  single_ctdef.score_exprs_.init(2);
  single_ctdef.score_exprs_.push_back(&score0_);
  single_ctdef.score_exprs_.push_back(&fusion_score_);

  iter.fusion_ctdef_ = &single_ctdef;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 10;
  iter.children_cnt_ = 1;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  // Single range-parallel runtime should not need path-top-k merge;
  // it should be handled as a normal runtime directly merged.
  ASSERT_EQ(OB_SUCCESS, iter.merge_parallel_results());
  EXPECT_EQ(2, iter.fusion_docs_.count());

  iter.parallel_ctx_.child_runtimes_.reset();
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    iter.fusion_docs_.at(i).reset();
  }
  iter.fusion_docs_.reset();
  iter.destroy_rowid_map();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ObDASFusionIterMergeEdgeCaseTest, merge_negative_range_top_k_limit)
{
  // Range-parallel runtimes with negative range_top_k_limit_ (invalid) should fail
  ObDASFusionIter iter;

  ObDASFusionChildRuntime rt0, rt1;
  rt0.path_idx_ = 0;
  rt0.finished_ = true;
  rt0.err_code_ = OB_SUCCESS;
  rt0.is_range_parallel_ = true;
  rt0.range_top_k_limit_ = -1;  // Invalid!
  rt0.rows_.push_back(ObDASFusionMaterializedRow(1, 0.9));

  rt1.path_idx_ = 0;
  rt1.finished_ = true;
  rt1.err_code_ = OB_SUCCESS;
  rt1.is_range_parallel_ = true;
  rt1.range_top_k_limit_ = -1;  // Invalid!
  rt1.rows_.push_back(ObDASFusionMaterializedRow(2, 0.8));

  iter.parallel_ctx_.child_runtimes_.push_back(&rt0);
  iter.parallel_ctx_.child_runtimes_.push_back(&rt1);

  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 10;
  iter.children_cnt_ = 2;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  // Negative range_top_k_limit_ should cause OB_ERR_UNEXPECTED in Phase 3
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.merge_parallel_results());

  iter.parallel_ctx_.child_runtimes_.reset();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

TEST_F(ObDASFusionIterMergeEdgeCaseTest, merge_mixed_range_parallel_and_normal)
{
  // Mix of range-parallel runtimes (same path) and a normal runtime (different path)
  ObDASFusionIter iter;

  // Two range-parallel runtimes on path 0
  ObDASFusionChildRuntime rt0, rt1, rt2;
  rt0.path_idx_ = 0;
  rt0.finished_ = true;
  rt0.err_code_ = OB_SUCCESS;
  rt0.is_range_parallel_ = true;
  rt0.range_top_k_limit_ = 2;
  rt0.rows_.push_back(ObDASFusionMaterializedRow(1, 100.0));
  rt0.rows_.push_back(ObDASFusionMaterializedRow(3, 97.0));

  rt1.path_idx_ = 0;
  rt1.finished_ = true;
  rt1.err_code_ = OB_SUCCESS;
  rt1.is_range_parallel_ = true;
  rt1.range_top_k_limit_ = 2;
  rt1.rows_.push_back(ObDASFusionMaterializedRow(2, 99.0));
  rt1.rows_.push_back(ObDASFusionMaterializedRow(4, 96.0));

  // A normal (non-range-parallel) runtime on path 1
  rt2.path_idx_ = 1;
  rt2.finished_ = true;
  rt2.err_code_ = OB_SUCCESS;
  rt2.is_range_parallel_ = false;
  rt2.rows_.push_back(ObDASFusionMaterializedRow(1, 0.8));
  rt2.rows_.push_back(ObDASFusionMaterializedRow(5, 0.7));

  iter.parallel_ctx_.child_runtimes_.push_back(&rt0);
  iter.parallel_ctx_.child_runtimes_.push_back(&rt1);
  iter.parallel_ctx_.child_runtimes_.push_back(&rt2);

  iter.fusion_ctdef_ = &ctdef_;
  iter.eval_ctx_ = &eval_ctx_;
  iter.exec_ctx_ = &exec_ctx_;
  iter.rowkey_is_uint64_ = true;
  iter.fusion_method_ = ObFusionMethod::WEIGHT_SUM;
  iter.rank_window_size_ = 5;
  iter.children_cnt_ = 2;

  lib::ContextParam ctx_param;
  ctx_param.set_mem_attr(500, "TestFusion", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(iter.fusion_memctx_, ctx_param));

  ASSERT_EQ(OB_SUCCESS, iter.merge_parallel_results());

  // Path 0 has top-2 = {1, 2} after merge, plus path 1 has {1, 5}
  // So docs should be {1 (both paths), 2 (path 0 only), 5 (path 1 only)}
  // Note: 3 and 4 are cut by top-2 limit on path 0
  EXPECT_GE(iter.fusion_docs_.count(), 2);

  bool found_doc1 = false;
  bool found_doc2 = false;
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    const ObDASFusionDocInfo &doc = iter.fusion_docs_.at(i);
    uint64_t rk = doc.get_uint64_rowkey();
    if (rk == 1) {
      found_doc1 = true;
      EXPECT_TRUE(doc.has_path(0));
      EXPECT_TRUE(doc.has_path(1));
    } else if (rk == 2) {
      found_doc2 = true;
      EXPECT_TRUE(doc.has_path(0));
      EXPECT_FALSE(doc.has_path(1));
    }
    // doc 3 and 4 should be filtered out by top-k limit
    EXPECT_NE(3ULL, rk);
    EXPECT_NE(4ULL, rk);
  }
  EXPECT_TRUE(found_doc1);
  EXPECT_TRUE(found_doc2);

  iter.parallel_ctx_.child_runtimes_.reset();
  for (int64_t i = 0; i < iter.fusion_docs_.count(); ++i) {
    iter.fusion_docs_.at(i).reset();
  }
  iter.fusion_docs_.reset();
  iter.destroy_rowid_map();
  if (OB_NOT_NULL(iter.fusion_memctx_)) {
    DESTROY_CONTEXT(iter.fusion_memctx_);
    iter.fusion_memctx_ = nullptr;
  }
}

// ============================================================================
// Section 33: ObDASFusionParallelCtx additional tests
// ============================================================================

TEST_F(ObDASFusionParallelCtxTest, discover_shared_bitmaps_dop_less_than_1)
{
  ObDASFusionParallelCtx ctx;
  // dop <= 1 should skip discovery
  int dummy = 0;
  ObDASIter *fake_iter = reinterpret_cast<ObDASIter *>(&dummy);
  EXPECT_EQ(OB_SUCCESS, ctx.discover_shared_bitmaps(alloc_, fake_iter, 0));
  EXPECT_FALSE(ctx.has_shared_bitmaps());

  EXPECT_EQ(OB_SUCCESS, ctx.discover_shared_bitmaps(alloc_, fake_iter, -1));
  EXPECT_FALSE(ctx.has_shared_bitmaps());
}

TEST_F(ObDASFusionParallelCtxTest, at_out_of_bounds)
{
  ObDASFusionParallelCtx ctx;
  EXPECT_EQ(nullptr, ctx.at(-1));
  EXPECT_EQ(nullptr, ctx.at(0));
  EXPECT_EQ(nullptr, ctx.at(999));
}

// ============================================================================
// Section 34: ObDASFusionChildTask additional init validation
// ============================================================================

TEST_F(ObDASFusionChildTaskTest, init_with_zero_timeout)
{
  ObDASFusionChildTask task;
  ObDASFusionChildRuntime runtime;
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));

  // Zero timeout is technically valid (means already expired)
  ASSERT_EQ(OB_SUCCESS, task.init(&runtime, &coord, 0, 0));
  EXPECT_EQ(0, task.get_timeout_ts());
}

TEST_F(ObDASFusionChildTaskTest, init_with_negative_timeout)
{
  ObDASFusionChildTask task;
  ObDASFusionChildRuntime runtime;
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(1));

  // Negative timeout (already expired)
  ASSERT_EQ(OB_SUCCESS, task.init(&runtime, &coord, -1, 0));
  EXPECT_EQ(-1, task.get_timeout_ts());
}

// ============================================================================
// Section 35: ObDASFusionMaterializedRow additional edge cases
// ============================================================================

TEST(ObDASFusionMaterializedRowAdditionalTest, copy_and_assign)
{
  ObDASFusionMaterializedRow row1(42, 3.14);
  ObDASFusionMaterializedRow row2 = row1;
  EXPECT_EQ(42UL, row2.rowkey_);
  EXPECT_DOUBLE_EQ(3.14, row2.score_);

  ObDASFusionMaterializedRow row3;
  row3 = row1;
  EXPECT_EQ(42UL, row3.rowkey_);
  EXPECT_DOUBLE_EQ(3.14, row3.score_);
}

TEST(ObDASFusionMaterializedRowAdditionalTest, negative_score)
{
  ObDASFusionMaterializedRow row(100, -5.5);
  EXPECT_EQ(100UL, row.rowkey_);
  EXPECT_DOUBLE_EQ(-5.5, row.score_);
}

TEST(ObDASFusionMaterializedRowAdditionalTest, very_large_rowkey)
{
  const uint64_t max_key = UINT64_MAX - 1;
  ObDASFusionMaterializedRow row(max_key, 1.0);
  EXPECT_EQ(max_key, row.rowkey_);
}

// ============================================================================
// Section 36: ObDASBitmapOp::set_external_bitmap validation
// ============================================================================

TEST(ObDASBitmapOpSetExternalTest, set_null_bitmap_fails)
{
  // set_external_bitmap should fail with null bitmap
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObArenaAllocator scan_alloc("MockScan");
  ObDASScanOp mock_scan_op(scan_alloc);
  ObDASSearchCtx search_ctx(scan_alloc, mock_scan_op);
  ObDASBitmapOp bitmap_op(search_ctx);

  EXPECT_EQ(OB_INVALID_ARGUMENT, bitmap_op.set_external_bitmap(nullptr));
}

// ============================================================================
// Section 37: ObDASFusionParallelCoordinator reset semantics
// ============================================================================

TEST_F(ObDASFusionParallelCoordinatorTest, reset_then_reinit_with_different_count)
{
  ObDASFusionParallelCoordinator coord;
  ASSERT_EQ(OB_SUCCESS, coord.init(5));

  coord.on_child_finish(OB_SUCCESS);
  coord.on_child_finish(OB_ERR_UNEXPECTED);
  EXPECT_EQ(OB_ERR_UNEXPECTED, coord.get_first_error());

  coord.reset();
  // After reset, counts should be zeroed, but is_inited_ should remain true
  // per the implementation comment: "Intentionally keep is_inited_ = true"
  // so that the cond_ can be reused.
  EXPECT_EQ(0, coord.total_cnt_);
  EXPECT_EQ(0, coord.finished_cnt_);
  EXPECT_EQ(OB_SUCCESS, coord.first_err_code_);

  // Re-init with different count
  ASSERT_EQ(OB_SUCCESS, coord.init(3));
  EXPECT_EQ(3, coord.total_cnt_);
  EXPECT_EQ(0, coord.finished_cnt_);

  coord.on_child_finish(OB_SUCCESS);
  coord.on_child_finish(OB_SUCCESS);
  coord.on_child_finish(OB_SUCCESS);
  EXPECT_EQ(OB_SUCCESS, coord.wait_all_complete(INT64_MAX));
}

// ============================================================================
// Section 38: ObDASFusionChildRuntime is_range_parallel defaults
// ============================================================================

TEST_F(ObDASFusionChildRuntimeTest, is_range_parallel_default_false)
{
  ObDASFusionChildRuntime runtime;
  EXPECT_FALSE(runtime.is_range_parallel_);
  EXPECT_EQ(-1, runtime.range_top_k_limit_);
}

TEST_F(ObDASFusionChildRuntimeTest, docid_range_defaults)
{
  ObDASFusionChildRuntime runtime;
  // docid_range_lo_ and docid_range_hi_ should be default-constructed ObObj
  EXPECT_TRUE(runtime.docid_range_lo_.is_null());
  EXPECT_TRUE(runtime.docid_range_hi_.is_null());
}

TEST_F(ObDASFusionChildRuntimeTest, parallel_ctx_default_null)
{
  ObDASFusionChildRuntime runtime;
  EXPECT_EQ(nullptr, runtime.parallel_ctx_);
  EXPECT_EQ(0, runtime.assigned_bitmap_slots_.count());
  EXPECT_EQ(0, runtime.child_bitmap_ops_.count());
}

// ============================================================================
// Section 39: ObSharedBitmapSlot lifecycle
// ============================================================================

TEST(ObSharedBitmapSlotLifecycleTest, release_null_bitmap_is_safe)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObSharedBitmapSlot slot;
  // bitmap_ is null by default; release should not crash
  slot.release();
  EXPECT_EQ(nullptr, slot.bitmap_);
  EXPECT_FALSE(slot.is_built_);
  EXPECT_EQ(OB_SUCCESS, slot.build_ret_);
}

TEST(ObSharedBitmapSlotLifecycleTest, build_ret_and_is_built_states)
{
  share::ObTenantEnv::get_tenant_local()->id_ = 500;
  ObSharedBitmapSlot slot;
  EXPECT_FALSE(slot.is_built_);
  EXPECT_EQ(OB_SUCCESS, slot.build_ret_);

  // Simulate a build failure
  ATOMIC_STORE(&slot.is_built_, true);
  slot.build_ret_ = OB_ALLOCATE_MEMORY_FAILED;
  EXPECT_TRUE(slot.is_built_);
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, slot.build_ret_);

  // Release should reset
  slot.release();
  EXPECT_FALSE(slot.is_built_);
  EXPECT_EQ(OB_SUCCESS, slot.build_ret_);
}

// ============================================================================
// Section 40: ObDASFusionChildRuntime reset_result clears rows
// ============================================================================

TEST_F(ObDASFusionChildRuntimeTest, reset_result_preserves_non_result_state)
{
  ObDASFusionChildRuntime runtime;
  runtime.path_idx_ = 3;
  runtime.is_range_parallel_ = true;
  runtime.range_top_k_limit_ = 42;
  runtime.rows_.push_back(ObDASFusionMaterializedRow(1, 1.0));
  runtime.submitted_ = true;
  runtime.finished_ = true;
  runtime.err_code_ = OB_CANCELED;

  runtime.reset_result();

  // reset_result should clear submitted, finished, err_code, rows
  EXPECT_FALSE(runtime.submitted_);
  EXPECT_FALSE(runtime.finished_);
  EXPECT_EQ(OB_SUCCESS, runtime.err_code_);
  EXPECT_EQ(0, runtime.rows_.count());

  // But should preserve non-result state
  EXPECT_EQ(3, runtime.path_idx_);
  EXPECT_TRUE(runtime.is_range_parallel_);
  EXPECT_EQ(42, runtime.range_top_k_limit_);
}

// ============================================================================
// Section 41: ObDASFusionParallelCtx release releases bitmap slots
// ============================================================================

TEST_F(ObDASFusionParallelCtxTest, release_with_bitmap_slots)
{
  // Verify that release properly cleans up shared bitmap slots
  ObDASFusionParallelCtx ctx;
  ObArenaAllocator alloc("TestBmpSlot");

  // Manually create a shared bitmap slot
  ObSharedBitmapSlot *slot = nullptr;
  void *slot_buf = alloc.alloc(sizeof(ObSharedBitmapSlot));
  ASSERT_NE(nullptr, slot_buf);
  slot = new (slot_buf) ObSharedBitmapSlot();
  slot->bitmap_occurrence_idx_ = 0;
  ATOMIC_STORE(&slot->is_built_, false);

  ctx.shared_bitmap_slots_.push_back(slot);
  EXPECT_EQ(1, ctx.get_shared_bitmap_count());
  EXPECT_TRUE(ctx.has_shared_bitmaps());

  // Release should clean up the slot
  ctx.release();
  EXPECT_EQ(0, ctx.get_shared_bitmap_count());
  EXPECT_FALSE(ctx.has_shared_bitmaps());
  EXPECT_EQ(0, ctx.get_runtime_count());
}

// ============================================================================
// Section 42: ObOpProfile::adopt_child tests
// ============================================================================
// adopt_child links an externally-allocated child profile into this profile's
// child list. The child keeps its own allocator; only a ProfileWrap is allocated
// from the parent's alloc_. Used by ObDASFusionIter to attach per-task profiles
// (allocated on a thread-safe arena) under the fusion profile.

class ObOpProfileAdoptChildTest : public ::testing::Test
{
protected:
  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
  }
};

TEST_F(ObOpProfileAdoptChildTest, adopt_null_child_fails)
{
  ObArenaAllocator alloc("TestAdopt");
  common::ObOpProfile<common::ObMetric> parent(
      common::ObProfileId::HYBRID_SEARCH_FUSION_ITER, &alloc);

  EXPECT_EQ(OB_INVALID_ARGUMENT, parent.adopt_child(nullptr));
  EXPECT_EQ(nullptr, parent.get_child_head());
  EXPECT_EQ(0, parent.child_array_.count());
}

TEST_F(ObOpProfileAdoptChildTest, adopt_single_child_success)
{
  ObArenaAllocator parent_alloc("TestParent");
  ObArenaAllocator child_alloc("TestChild");
  common::ObOpProfile<common::ObMetric> parent(
      common::ObProfileId::HYBRID_SEARCH_FUSION_ITER, &parent_alloc);
  common::ObOpProfile<common::ObMetric> child(
      common::ObProfileId::HYBRID_SEARCH_PARALLEL_TASK, &child_alloc);

  EXPECT_EQ(nullptr, child.get_parent());
  EXPECT_EQ(nullptr, parent.get_child_head());

  ASSERT_EQ(OB_SUCCESS, parent.adopt_child(&child));

  // Parent pointer is set on the child
  EXPECT_EQ(&parent, child.get_parent());

  // child_head_ / child_tail_ / child_array_ all reference the child
  ASSERT_NE(nullptr, parent.get_child_head());
  EXPECT_EQ(&child, parent.get_child_head()->elem_);
  EXPECT_EQ(nullptr, parent.get_child_head()->next_);
  ASSERT_EQ(1, parent.child_array_.count());
  EXPECT_EQ(&child, parent.child_array_.at(0));

  // Child's allocator is preserved (parent does NOT own the child's storage)
  EXPECT_EQ(&child_alloc, child.alloc_);
}

TEST_F(ObOpProfileAdoptChildTest, adopt_multiple_children_chain_in_order)
{
  ObArenaAllocator parent_alloc("TestParent");
  ObArenaAllocator child_alloc("TestChild");
  common::ObOpProfile<common::ObMetric> parent(
      common::ObProfileId::HYBRID_SEARCH_FUSION_ITER, &parent_alloc);
  common::ObOpProfile<common::ObMetric> child0(
      common::ObProfileId::HYBRID_SEARCH_PARALLEL_TASK, &child_alloc);
  common::ObOpProfile<common::ObMetric> child1(
      common::ObProfileId::HYBRID_SEARCH_PARALLEL_TASK, &child_alloc);
  common::ObOpProfile<common::ObMetric> child2(
      common::ObProfileId::HYBRID_SEARCH_PARALLEL_TASK, &child_alloc);

  ASSERT_EQ(OB_SUCCESS, parent.adopt_child(&child0));
  ASSERT_EQ(OB_SUCCESS, parent.adopt_child(&child1));
  ASSERT_EQ(OB_SUCCESS, parent.adopt_child(&child2));

  // child_array_ preserves insertion order
  ASSERT_EQ(3, parent.child_array_.count());
  EXPECT_EQ(&child0, parent.child_array_.at(0));
  EXPECT_EQ(&child1, parent.child_array_.at(1));
  EXPECT_EQ(&child2, parent.child_array_.at(2));

  // Linked list (head -> next -> next) follows insertion order
  auto *wrap0 = parent.get_child_head();
  ASSERT_NE(nullptr, wrap0);
  EXPECT_EQ(&child0, wrap0->elem_);
  auto *wrap1 = wrap0->next_;
  ASSERT_NE(nullptr, wrap1);
  EXPECT_EQ(&child1, wrap1->elem_);
  auto *wrap2 = wrap1->next_;
  ASSERT_NE(nullptr, wrap2);
  EXPECT_EQ(&child2, wrap2->elem_);
  EXPECT_EQ(nullptr, wrap2->next_);

  EXPECT_EQ(&parent, child0.get_parent());
  EXPECT_EQ(&parent, child1.get_parent());
  EXPECT_EQ(&parent, child2.get_parent());

  // Counts traversal walks the adopted subtree
  int64_t metric_count = 0;
  int64_t profile_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, parent.get_all_count(metric_count, profile_cnt));
  EXPECT_EQ(3, profile_cnt);
}

// ============================================================================
// Section 43: TODO tests for functions requiring complex infrastructure
// ============================================================================

// TODO: compute_docid_split_points() - requires MTL(ObAccessService*) which is
// not available in unit test. To test properly, we would need to:
//   1. Mock ObAccessService::split_multi_ranges
//   2. Set up ObDASSearchCtx with valid ls_id and tablet_id
//   3. Verify:
//      - Storage returning fewer splits than requested (actual_dop < range_dop)
//      - Storage returning 0 splits (empty)
//      - MIN/MAX boundary keys
//      - Exclusive vs inclusive border flags
//      - Non-uint64 docid in split keys (should fail)
//
// TEST(ComputeDocidSplitPointsTest, fewer_splits_than_requested) { ... }
// TEST(ComputeDocidSplitPointsTest, empty_splits) { ... }
// TEST(ComputeDocidSplitPointsTest, boundary_keys) { ... }
// TEST(ComputeDocidSplitPointsTest, exclusive_vs_inclusive_flags) { ... }
// TEST(ComputeDocidSplitPointsTest, non_uint64_docid_fails) { ... }

// TODO: create_parallel_iter() vector index path - requires ObDASVecIndexDriverIter
// mock infrastructure. The path involves:
//   1. fill_related_tablet_ids / set_related_tablet_ids propagation
//   2. deep_copy_rtdef_tree with vec ctdef/rtdef
//   3. ObDASIterUtils::create_vec_search_iter
//
// TEST(CreateParallelIterVecTest, tablet_id_propagation) { ... }

// TODO: swizzle_child_rtdef() with different op types - requires constructing
// full rtdef trees for DAS_OP_TABLE_SCAN, DAS_OP_TABLE_BATCH_SCAN,
// DAS_OP_SCALAR_SCAN_QUERY which need ObDASTaskFactory, ObPhysicalPlan,
// and many other dependent objects.
//
// TEST(SwizzleChildRtdefTest, table_scan_type) { ... }
// TEST(SwizzleChildRtdefTest, table_batch_scan_type) { ... }
// TEST(SwizzleChildRtdefTest, scalar_scan_query_type) { ... }

// TODO: alloc_frames_safe() - file-static function, cannot call directly from
// test. To test, we would need a wrapper or test ObDASFusionChildRuntime::
// create_fusion_child_eval_ctx with varying frame configurations:
//   1. Missing param frames (param_frame_.count() > param_frame_ptrs.count())
//   2. Zero frame count
//
// TEST(AllocFramesSafeTest, missing_param_frames) { ... }
// TEST(AllocFramesSafeTest, zero_frame_count) { ... }

// TODO: drain_child_iter() cancellation and timeout - requires a mock ObDASIter
// that responds to get_next_rows. The function checks:
//   1. coordinator.get_first_error() for cancellation
//   2. THIS_WORKER.is_timeout() for timeout
//   3. OB_ITER_END with partial batch handling
//
// TEST(DrainChildIterTest, cancellation_via_first_error) { ... }
// TEST(DrainChildIterTest, timeout_detection) { ... }
// TEST(DrainChildIterTest, iter_end_with_partial_batch) { ... }

// TODO: do_parallel_table_scan() partial task submission - requires full
// ObDASFusionIter setup with mock task scheduler. Some tasks submitted,
// some not.
//
// TEST(DoParallelTableScanTest, partial_submission) { ... }

// TODO: ObDASBitmapOp::do_open() with external bitmap - requires full
// ObDASSearchCtx and ObIDASSearchOp tree setup.
// The external bitmap path (bitmap_ != null && owns_bitmap_ == false) skips
// the scan and uses the injected bitmap directly.
//
// TEST(ObDASBitmapOpDoOpenTest, external_bitmap_path) { ... }
// TEST(ObDASBitmapOpDoOpenTest, self_build_path) { ... }

// TODO: materialize_batch_result() rich-format (vectorized) and
// non-rich-format (datum) paths with actual expression evaluation.
// The null-score-datum path and batch_size edge cases are tested above
// via guards, but full expression evaluation testing requires:
//   1. Properly initialized ObExpr with vector format
//   2. ObEvalCtx with batch info
//   3. Actual datum values written to expression frames
//
// TEST(MaterializeBatchResultTest, rich_format_path) { ... }
// TEST(MaterializeBatchResultTest, datum_path_with_null_score) { ... }
// TEST(MaterializeBatchResultTest, batch_size_one) { ... }

} // anonymous namespace

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
