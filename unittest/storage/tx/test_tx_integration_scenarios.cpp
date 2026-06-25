/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You may use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

// Integration scenario tests for TxRedoFlushStatus leader switch safety
// Tests aggregation guard behavior under realistic conditions

#include <gtest/gtest.h>
#define USING_LOG_PREFIX TRANS

#include "storage/tx/ob_tx_log_cb_define.h"
#include "storage/tx/ob_tx_hotspot_define.h"
#include "storage/tx/ob_tx_hotspot_helper.h"
#include "share/ob_errno.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;

class ObTestTxIntegrationScenarios : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  }
  virtual void TearDown() override {}
};

// ============================================================================
// TC-UT-01: 并发转换race模拟测试
// ============================================================================
TEST_F(ObTestTxIntegrationScenarios, test_concurrent_transition_simulation)
{
  int ret1 = OB_SUCCESS;
  int ret2 = OB_SUCCESS;

  // 模拟：两个线程同时尝试从PRIMARY_COLLECTING转换
  // Thread 1: COLLECTING → AGGR_SUCCEEDED
  // Thread 2: COLLECTING → AGGR_FAILED

  ret1 = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING,
                             TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED);
  ret2 = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING,
                             TxRedoFlushStatus::PRIMARY_AGGR_FAILED);

  // 两个转换都有效，但在实际场景中只有一个会成功（取决于执行顺序）
  EXPECT_EQ(OB_SUCCESS, ret1);
  EXPECT_EQ(OB_SUCCESS, ret2);

  // 状态机设计允许两种路径，实际执行时由业务逻辑选择
  // 关键是：一旦状态改变，另一个转换会失败
  ret1 = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED,
                             TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret1); // AGGR_SUCCEEDED只能到COMPLETED
}

// ============================================================================
// TC-UT-02: TxSecondaryRespStatus所有状态有效性测试
// ============================================================================
TEST_F(ObTestTxIntegrationScenarios, test_secondary_resp_status_completeness)
{
  // 有效状态
  EXPECT_STREQ("RESP_NOT_SENT", to_cstr(TxSecondaryRespStatus::RESP_NOT_SENT));
  EXPECT_STREQ("RESP_COMMITTED", to_cstr(TxSecondaryRespStatus::RESP_COMMITTED));
  EXPECT_STREQ("RESP_ABORTED_BY_SESSION", to_cstr(TxSecondaryRespStatus::RESP_ABORTED_BY_SESSION));
  EXPECT_STREQ("RESP_SKIPPED_BY_PRIMARY", to_cstr(TxSecondaryRespStatus::RESP_SKIPPED_BY_PRIMARY));

  // 无效状态
  EXPECT_STREQ("INVALID_RESP_STATUS", to_cstr(static_cast<TxSecondaryRespStatus>(999)));
  EXPECT_STREQ("INVALID_RESP_STATUS", to_cstr(static_cast<TxSecondaryRespStatus>(-1)));
  EXPECT_STREQ("INVALID_RESP_STATUS", to_cstr(static_cast<TxSecondaryRespStatus>(100)));
}

// ============================================================================
// TC-UT-03: PRIMARY_PREPARING立即失败转换测试（核心bug修复验证）
// ============================================================================
TEST_F(ObTestTxIntegrationScenarios, test_preparing_immediate_abort_allowed)
{
  int ret = OB_SUCCESS;

  // 核心修复：PRIMARY_PREPARING可以直接转换到PRIMARY_AGGR_FAILED
  // 这是原始bug的关键修复点
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING,
                            TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // 验证AGGR_FAILED是终态，防止后续错误转换
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED,
                            TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
}

// ============================================================================
// TC-UT-04: PRIMARY_COLLECTING强制切换允许测试（核心bug修复验证）
// ============================================================================
TEST_F(ObTestTxIntegrationScenarios, test_collecting_forced_switch_allowed)
{
  int ret = OB_SUCCESS;

  // 核心修复：PRIMARY_COLLECTING可以直接转换到PRIMARY_AGGR_FAILED
  // 允许forced leader switch直接abort，无需wait
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING,
                            TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // 验证幂等性：COLLECTING→COLLECTING允许（用于状态检查）
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING,
                            TxRedoFlushStatus::PRIMARY_COLLECTING);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// ============================================================================
// TC-UT-05: SECONDARY组完整生命周期测试
// ============================================================================
TEST_F(ObTestTxIntegrationScenarios, test_secondary_lifecycle_complete)
{
  int ret = OB_SUCCESS;

  // SECONDARY完整路径1: MIGRATING → SYNCED → SUCCEEDED → COMPLETED
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING,
                            TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED,
                            TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // SECONDARY完整路径2: MIGRATING → SYNCED → FAILED (abort path)
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED,
                            TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // SECONDARY直接失败路径: MIGRATING → FAILED
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING,
                            TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// ============================================================================
// TC-UT-06: 辅助函数边界值测试
// ============================================================================
TEST_F(ObTestTxIntegrationScenarios, test_helper_function_edge_cases)
{
  // is_primary_status边界
  EXPECT_FALSE(is_primary_status(TxRedoFlushStatus::NORMAL_START));
  EXPECT_FALSE(is_primary_status(TxRedoFlushStatus::NORMAL_FLUSHED));
  EXPECT_TRUE(is_primary_status(TxRedoFlushStatus::PRIMARY_PREPARING));
  EXPECT_FALSE(is_primary_status(TxRedoFlushStatus::SECONDARY_MIGRATING));

  // is_secondary_status边界
  EXPECT_FALSE(is_secondary_status(TxRedoFlushStatus::NORMAL_START));
  EXPECT_TRUE(is_secondary_status(TxRedoFlushStatus::SECONDARY_MIGRATING));
  EXPECT_FALSE(is_secondary_status(TxRedoFlushStatus::PRIMARY_PREPARING));

  // has_completed_lifecycle边界（验证所有终态）
  EXPECT_TRUE(has_completed_lifecycle(TxRedoFlushStatus::NORMAL_FLUSHED));
  EXPECT_TRUE(has_completed_lifecycle(TxRedoFlushStatus::PRIMARY_AGGR_FAILED));
  EXPECT_TRUE(has_completed_lifecycle(TxRedoFlushStatus::PRIMARY_COMPLETED));
  EXPECT_TRUE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));
  EXPECT_TRUE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED));

  // 验证中间态不是终态
  EXPECT_FALSE(has_completed_lifecycle(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED));
  EXPECT_FALSE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED));
}

// ============================================================================
// TC-UT-07: 状态转换完整性矩阵测试（安全审查推荐）
// ============================================================================
TEST_F(ObTestTxIntegrationScenarios, test_transition_matrix_completeness)
{
  int ret = OB_SUCCESS;

  // PRIMARY_PREPARING的允许转换
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING,
                            TxRedoFlushStatus::PRIMARY_COLLECTING);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING,
                            TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // PRIMARY_COLLECTING的允许转换
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING,
                            TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING,
                            TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // PRIMARY_AGGR_SUCCEEDED只能到PRIMARY_COMPLETED
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED,
                            TxRedoFlushStatus::PRIMARY_COMPLETED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // 验证禁止的转换
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED,
                            TxRedoFlushStatus::PRIMARY_COMPLETED);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_tx_integration_scenarios.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_tx_integration_scenarios.log", true, false,
                       "test_tx_integration_scenarios.log",
                       "test_tx_integration_scenarios.log",
                       "test_tx_integration_scenarios.log");
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
