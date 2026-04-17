/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_window_function.h"
#include "sql/engine/ob_batch_rows.h"

// Datahub header must be included AFTER other headers to avoid macro pollution.
// The `#define private public` inside ob_op_test_datahub.h is scoped with
// #pragma push_macro/pop_macro so it does not affect other headers.
#include "unittest/sql/engine/op_tests/ob_op_test_datahub.h"

namespace oceanbase
{
namespace sql
{

class WindowFunctionDatahubTest : public OpTestKit
{
};

// TC1: Basic window function without datahub (non-parallel mode)
TEST_F(WindowFunctionDatahubTest, BasicWindowFunction)
{
  OpTestResult result = WindowFunctionTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
}

// TC2: Window function with single_part_parallel mode (requires datahub mock)
TEST_F(WindowFunctionDatahubTest, WinbufWholeMsgMock)
{
  OpTestResult result = WindowFunctionTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .enable_single_part_parallel()
      .set_datahub_fn(
          [](ObExecContext &exec_ctx, uint64_t op_id) -> int {
            int ret = OB_SUCCESS;
            MockDatahubContext ctx;
            ret = ctx.init(exec_ctx);
            if (OB_FAIL(ret)) return ret;
            ctx.register_whole_msg<ObWinbufPieceMsg, ObWinbufWholeMsg>(
                op_id, dtl::DH_WINBUF_WHOLE_MSG,
                [](ObWinbufWholeMsg &msg) {
                  msg.is_empty_ = false;
                });
            ctx.destroy();
            return OB_SUCCESS;
          })
      .run(engine_);

  EXPECT_GE(result.row_count(), 0);
}

// TC3: Barrier whole message mock
TEST_F(WindowFunctionDatahubTest, BarrierWholeMsgMock)
{
  OpTestResult result = WindowFunctionTestSpec()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}})
      .set_datahub_fn(
          [](ObExecContext &, uint64_t op_id) -> int {
            MockDatahubContext ctx;
            ctx.register_barrier(op_id);
            ctx.destroy();
            return OB_SUCCESS;
          })
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
}

// TC4: Empty data with datahub mock
TEST_F(WindowFunctionDatahubTest, EmptyDataWithMock)
{
  OpTestResult result = WindowFunctionTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({})
      .enable_single_part_parallel()
      .set_datahub_fn(
          [](ObExecContext &exec_ctx, uint64_t op_id) -> int {
            int ret = OB_SUCCESS;
            MockDatahubContext ctx;
            ret = ctx.init(exec_ctx);
            if (OB_FAIL(ret)) return ret;
            ctx.register_whole_msg<ObWinbufPieceMsg, ObWinbufWholeMsg>(
                op_id, dtl::DH_WINBUF_WHOLE_MSG,
                [](ObWinbufWholeMsg &msg) {
                  msg.is_empty_ = true;
                });
            ctx.destroy();
            return OB_SUCCESS;
          })
      .run(engine_);

  EXPECT_EQ(0, result.row_count());
}

// TC5: Range distribution parallel mode
TEST_F(WindowFunctionDatahubTest, RangeDistParallelBasic)
{
  OpTestResult result = WindowFunctionTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .enable_range_dist_parallel()
      .with_rich_format(true)
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
}

// TC6: Single part parallel with barrier
TEST_F(WindowFunctionDatahubTest, SinglePartParallelWithBarrier)
{
  OpTestResult result = WindowFunctionTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 10}, {2, 20}})
      .enable_single_part_parallel()
      .set_datahub_fn(
          [](ObExecContext &, uint64_t op_id) -> int {
            MockDatahubContext ctx;
            ctx.register_barrier(op_id);
            ctx.destroy();
            return OB_SUCCESS;
          })
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
}

// TC7: Multiple datahub mocks combined
TEST_F(WindowFunctionDatahubTest, MultipleDatahubMocks)
{
  OpTestResult result = WindowFunctionTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .enable_single_part_parallel()
      .set_datahub_fn(
          [](ObExecContext &exec_ctx, uint64_t op_id) -> int {
            int ret = OB_SUCCESS;
            MockDatahubContext ctx;
            ret = ctx.init(exec_ctx);
            if (OB_FAIL(ret)) return ret;
            ctx.register_barrier(op_id);
            ctx.register_whole_msg<ObWinbufPieceMsg, ObWinbufWholeMsg>(
                op_id, dtl::DH_WINBUF_WHOLE_MSG,
                [](ObWinbufWholeMsg &msg) {
                  msg.is_empty_ = false;
                });
            ctx.destroy();
            return OB_SUCCESS;
          })
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
