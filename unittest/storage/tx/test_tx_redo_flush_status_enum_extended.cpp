/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

// Extended unit tests for TxRedoFlushStatus enum refactoring
// Tests missing scenarios identified by multi-model cross-review
// Review models: GLM5, Qwen3.6-plus, MiniMax, Kimi

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

class ObTestTxRedoFlushStatusEnumExtended : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  }
  virtual void TearDown() override {}
};

// ============================================================================
// TC-01: 组内空洞值检测 - 所有边界值
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnumExtended, test_all_enum_gap_values)
{
  int ret = OB_SUCCESS;

  // Normal组空洞 (10-80之间)
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(0)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(5)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(15)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(20)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(50)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(75)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(90)));

  // Primary组空洞 (110-180之间)
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(105)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(115)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(130)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(140)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(155)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(170)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(185)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(190)));

  // Secondary组空洞 (510-580之间)
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(505)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(515)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(525)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(535)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(545)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(555)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(565)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(575)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(590)));

  // 超出范围
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(200)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(300)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(400)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(600)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(999)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(-1)));

  // 空洞值的转换应返回OB_ERR_UNEXPECTED
  TxRedoFlushStatus gap = static_cast<TxRedoFlushStatus>(115);
  ret = is_valid_transition(gap, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_ERR_UNEXPECTED, ret);

  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, gap);
  EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
}

// ============================================================================
// TC-02: 终态保护 - 所有终态不允许任何转换
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnumExtended, test_terminal_state_protection)
{
  int ret = OB_SUCCESS;

  // NORMAL_FLUSHED终态测试
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_FLUSHED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_FLUSHED, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // PRIMARY_AGGR_FAILED终态测试
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED, TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // PRIMARY_COMPLETED终态测试
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COMPLETED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COMPLETED, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // SECONDARY_MIGRATE_SUCCEEDED终态测试
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED, TxRedoFlushStatus::SECONDARY_MIGRATING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // 终态幂等性测试 - 应允许
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_FLUSHED, TxRedoFlushStatus::NORMAL_FLUSHED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED, TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COMPLETED, TxRedoFlushStatus::PRIMARY_COMPLETED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED, TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED, TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// ============================================================================
// TC-03: 非终态中间状态的完整性测试
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnumExtended, test_intermediate_state_completeness)
{
  int ret = OB_SUCCESS;

  // PRIMARY_AGGR_SUCCEEDED不是终态，必须能转换到PRIMARY_COMPLETED
  EXPECT_FALSE(has_completed_lifecycle(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED));
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED, TxRedoFlushStatus::PRIMARY_COMPLETED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // SECONDARY_MIGRATE_SYNCED不是终态，必须能转换到SECONDARY_MIGRATE_SUCCEEDED或FAILED
  EXPECT_FALSE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED));
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // SECONDARY_MIGRATE_SUCCEEDED是终态（scheduler response成功），不允许任何出边转换
  EXPECT_TRUE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED));
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // SECONDARY_MIGRATE_FAILED是终态（聚合失败），不允许任何出边转换
  EXPECT_TRUE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // SECONDARY_MIGRATE_SYNCED不是终态，必须有后续转换
  EXPECT_FALSE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED));
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// ============================================================================
// TC-04: is_primary_tx_aggregating边界测试
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnumExtended, test_is_primary_tx_aggregating_edge_cases)
{
  // 只有PREPARING和COLLECTING是聚合中的状态
  EXPECT_TRUE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_PREPARING));
  EXPECT_TRUE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_COLLECTING));

  // 所有其他Primary状态都不是聚合中
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_AGGR_FAILED));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_COMPLETED));

// 所有非Primary状态都不是聚合中
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::NORMAL_START));
  EXPECT_FALSE(is_primary_status(TxRedoFlushStatus::NORMAL_START));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::NORMAL_FLUSHED));
  EXPECT_FALSE(is_primary_status(TxRedoFlushStatus::NORMAL_FLUSHED));

  // Secondary states
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::SECONDARY_PREPARING));
  EXPECT_FALSE(is_primary_status(TxRedoFlushStatus::SECONDARY_PREPARING));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::SECONDARY_MIGRATING));
  EXPECT_FALSE(is_primary_status(TxRedoFlushStatus::SECONDARY_MIGRATING));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED));
  EXPECT_FALSE(is_primary_status(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));
  EXPECT_FALSE(is_primary_status(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));
}

// ============================================================================
// TC-06: 状态机完整路径测试 - Primary全流程
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnumExtended, test_primary_full_lifecycle)
{
  int ret = OB_SUCCESS;

  // 正常聚合成功路径
  // NORMAL_START -> PRIMARY_PREPARING -> PRIMARY_COLLECTING -> PRIMARY_AGGR_SUCCEEDED -> PRIMARY_COMPLETED
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::PRIMARY_COLLECTING);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING, TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED, TxRedoFlushStatus::PRIMARY_COMPLETED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // PREPARING阶段失败路径
  // NORMAL_START -> PRIMARY_PREPARING -> PRIMARY_AGGR_FAILED
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // COLLECTING阶段失败路径
  // NORMAL_START -> PRIMARY_PREPARING -> PRIMARY_COLLECTING -> PRIMARY_AGGR_FAILED
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING, TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// ============================================================================
// TC-07: 状态机完整路径测试 - Secondary全流程
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnumExtended, test_secondary_full_lifecycle)
{
  int ret = OB_SUCCESS;

  // 正常迁移成功路径
  // NORMAL_START -> SECONDARY_PREPARING -> SECONDARY_MIGRATING -> SECONDARY_MIGRATE_SYNCED -> SECONDARY_MIGRATE_SUCCEEDED (终态)
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::SECONDARY_PREPARING);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_PREPARING, TxRedoFlushStatus::SECONDARY_MIGRATING);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING, TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // PREPARING阶段失败路径
  // SECONDARY_PREPARING -> SECONDARY_MIGRATE_FAILED (终态)
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_PREPARING, TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // MIGRATING阶段失败路径
  // SECONDARY_MIGRATING -> SECONDARY_MIGRATE_FAILED (终态)
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING, TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // SYNCED阶段失败路径
  // SECONDARY_MIGRATE_SYNCED -> SECONDARY_MIGRATE_FAILED (终态)
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// ============================================================================
// TC-08: 跨组非法转换测试 - 全面覆盖
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnumExtended, test_cross_group_invalid_transitions)
{
  int ret = OB_SUCCESS;

  // Normal组 → Primary组（NORMAL_START → PRIMARY_PREPARING 是合法的！）
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_SUCCESS, ret);  // 合法：进入聚合准备
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::PRIMARY_COLLECTING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);  // 非法：不能跳过PREPARING
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_FLUSHED, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);  // 非法：NORMAL_FLUSHED是终态

  // Normal组 → Secondary组（NORMAL_START → SECONDARY_PREPARING 是合法的！）
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::SECONDARY_PREPARING);
  EXPECT_EQ(OB_SUCCESS, ret);  // 合法：进入聚合准备
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::SECONDARY_MIGRATING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);  // 非法：不能跳过PREPARING
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_FLUSHED, TxRedoFlushStatus::SECONDARY_MIGRATING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);  // 非法：NORMAL_FLUSHED是终态

  // Primary组 → Normal组（非法，聚合后不能回退）
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::NORMAL_FLUSHED);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // Primary组 → Secondary组（非法）
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::SECONDARY_MIGRATING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING, TxRedoFlushStatus::SECONDARY_MIGRATING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // Secondary组 → Normal组（非法，聚合后不能回退）
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING, TxRedoFlushStatus::NORMAL_FLUSHED);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // Secondary组 → Primary组（非法）
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, TxRedoFlushStatus::PRIMARY_COLLECTING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
}

// ============================================================================
// TC-09: TxSecondaryRespStatus完整性测试
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnumExtended, test_secondary_resp_status_completeness)
{
  // 所有有效值
  EXPECT_STREQ("RESP_NOT_SENT", to_cstr(TxSecondaryRespStatus::RESP_NOT_SENT));
  EXPECT_STREQ("RESP_COMMITTED", to_cstr(TxSecondaryRespStatus::RESP_COMMITTED));
  EXPECT_STREQ("RESP_ABORTED_BY_SESSION", to_cstr(TxSecondaryRespStatus::RESP_ABORTED_BY_SESSION));
  EXPECT_STREQ("RESP_SKIPPED_BY_PRIMARY", to_cstr(TxSecondaryRespStatus::RESP_SKIPPED_BY_PRIMARY));

  // 无效值
  EXPECT_STREQ("INVALID_RESP_STATUS", to_cstr(static_cast<TxSecondaryRespStatus>(999)));
  EXPECT_STREQ("INVALID_RESP_STATUS", to_cstr(static_cast<TxSecondaryRespStatus>(-1)));
  EXPECT_STREQ("INVALID_RESP_STATUS", to_cstr(static_cast<TxSecondaryRespStatus>(100)));
}

// ============================================================================
// TC-10: 幂等转换测试 - 所有状态
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnumExtended, test_idempotent_transitions_all_states)
{
  int ret = OB_SUCCESS;

  // 所有状态的幂等转换都应允许
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_FLUSHED, TxRedoFlushStatus::NORMAL_FLUSHED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING, TxRedoFlushStatus::PRIMARY_COLLECTING);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED, TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED, TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COMPLETED, TxRedoFlushStatus::PRIMARY_COMPLETED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_PREPARING, TxRedoFlushStatus::SECONDARY_PREPARING);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING, TxRedoFlushStatus::SECONDARY_MIGRATING);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED, TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED, TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// ============================================================================
// TC-11: SECONDARY_PREPARING 辅助函数属性测试
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnumExtended, test_secondary_preparing_helper_functions)
{
  // is_secondary_status
  EXPECT_TRUE(is_secondary_status(TxRedoFlushStatus::SECONDARY_PREPARING));

  // is_normal_status = false
  EXPECT_FALSE(is_normal_status(TxRedoFlushStatus::SECONDARY_PREPARING));

  // is_primary_status = false
  EXPECT_FALSE(is_primary_status(TxRedoFlushStatus::SECONDARY_PREPARING));

  // is_primary_tx_aggregating = false (only checks PRIMARY_PREPARING and PRIMARY_COLLECTING)
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::SECONDARY_PREPARING));

  // has_completed_lifecycle = false
  EXPECT_FALSE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_PREPARING));

  // is_eligible_for_aggregation_entry = false (PREPARING already in aggregation)
  EXPECT_FALSE(is_eligible_for_aggregation_entry(TxRedoFlushStatus::SECONDARY_PREPARING));

  // is_valid_status = true (valid enum value)
  EXPECT_TRUE(is_valid_status(TxRedoFlushStatus::SECONDARY_PREPARING));

  // Value range check (>=500)
  EXPECT_GE(static_cast<int64_t>(TxRedoFlushStatus::SECONDARY_PREPARING), 500);
  EXPECT_EQ(510, static_cast<int64_t>(TxRedoFlushStatus::SECONDARY_PREPARING));

  // to_cstr conversion
  EXPECT_STREQ("SECONDARY_PREPARING", to_cstr(TxRedoFlushStatus::SECONDARY_PREPARING));
}

// ============================================================================
// TC-12: is_in_active_aggregation 与 is_primary_status 区分测试
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnumExtended, test_is_in_active_aggregation_distinction)
{
  // is_in_active_aggregation = is_primary_tx_aggregating in helper code.
  // Key distinction:
  // - is_primary_status: returns true for ALL 5 Primary states
  // - is_primary_tx_aggregating: returns true ONLY for PREPARING and COLLECTING

  // Aggregating states (active in aggregation)
  EXPECT_TRUE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_PREPARING));
  EXPECT_TRUE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_COLLECTING));

  // Non-aggregating Primary states (aggregation completed or failed)
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_AGGR_FAILED));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_COMPLETED));

  // But these are still Primary states
  EXPECT_TRUE(is_primary_status(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED));
  EXPECT_TRUE(is_primary_status(TxRedoFlushStatus::PRIMARY_AGGR_FAILED));
  EXPECT_TRUE(is_primary_status(TxRedoFlushStatus::PRIMARY_COMPLETED));
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_tx_redo_flush_status_enum_extended.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_tx_redo_flush_status_enum_extended.log", true, false,
                       "test_tx_redo_flush_status_enum_extended.log",
                       "test_tx_redo_flush_status_enum_extended.log",
                       "test_tx_redo_flush_status_enum_extended.log");
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
