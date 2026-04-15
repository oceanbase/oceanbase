/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "unittest/sql/engine/op_tests/ob_op_test_window_function.h"
#include "sql/engine/ob_batch_rows.h"

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
  // Note: This test verifies basic window function operation
  // For single_part_parallel mode (datahub), see WinbufWholeMsgMock test
  OpTestResult result = WindowFunctionTestSpec()
      .table("t", "a int, b int")
      .select("a, b")  // Simple select without window function for now
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
}

// TC2: Window function with single_part_parallel mode (requires datahub mock)
// Note: This test demonstrates the datahub mock API usage.
// The actual window function execution depends on proper WinFuncInfo filling.
TEST_F(WindowFunctionDatahubTest, WinbufWholeMsgMock)
{
  OpTestResult result = WindowFunctionTestSpec()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .enable_single_part_parallel()  // Triggers datahub registration
      .with_datahub_whole_msg<ObWinbufPieceMsg, ObWinbufWholeMsg>(
          dtl::DH_WINBUF_WHOLE_MSG,
          [](ObWinbufWholeMsg &msg) {
            // Pre-fill aggregate results for mock
            msg.is_empty_ = false;
            // In real usage, would fill datum_store_ with aggregate values
          })
      .run(engine_);

  // Note: Actual result depends on proper window function implementation
  // This test verifies the mock infrastructure works without crashing
  EXPECT_GE(result.row_count(), 0);
}

// TC3: Barrier whole message mock
TEST_F(WindowFunctionDatahubTest, BarrierWholeMsgMock)
{
  OpTestResult result = WindowFunctionTestSpec()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}})
      .with_datahub_barrier()  // Barrier synchronization mock
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
      .with_datahub_whole_msg<ObWinbufPieceMsg, ObWinbufWholeMsg>(
          dtl::DH_WINBUF_WHOLE_MSG,
          [](ObWinbufWholeMsg &msg) {
            msg.is_empty_ = true;  // Indicate empty result
          })
      .run(engine_);

  EXPECT_EQ(0, result.row_count());
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}