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

// Unit tests for TxRedoFlushStatus enum refactoring (Phase 02)
// Tests is_valid_transition, is_valid_status, and all helper functions

#include <gtest/gtest.h>
#define USING_LOG_PREFIX TRANS

// Include order matters: ob_tx_log_cb_define.h must come before ob_trans_submit_log_cb.h
// to resolve ObTxLogCbGroup incomplete type error in INHERIT_TO_STRING_KV macro
#include "storage/tx/ob_tx_log_cb_define.h"
#include "storage/tx/ob_tx_hotspot_define.h"
#include "storage/tx/ob_tx_hotspot_helper.h"
#include "share/ob_errno.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;

class ObTestTxRedoFlushStatusEnum : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  }
  virtual void TearDown() override {}
};

// ============================================================================
// Test 1: is_valid_status - 验证所有 11 个有效枚举值
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_is_valid_status_all_enum_values)
{
  // 所有 11 个有效状态
  EXPECT_TRUE(is_valid_status(TxRedoFlushStatus::NORMAL_START));
  EXPECT_TRUE(is_valid_status(TxRedoFlushStatus::NORMAL_FLUSHED));
  EXPECT_TRUE(is_valid_status(TxRedoFlushStatus::PRIMARY_PREPARING));
  EXPECT_TRUE(is_valid_status(TxRedoFlushStatus::PRIMARY_COLLECTING));
  EXPECT_TRUE(is_valid_status(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED));
  EXPECT_TRUE(is_valid_status(TxRedoFlushStatus::PRIMARY_AGGR_FAILED));
  EXPECT_TRUE(is_valid_status(TxRedoFlushStatus::PRIMARY_COMPLETED));
  EXPECT_TRUE(is_valid_status(TxRedoFlushStatus::SECONDARY_MIGRATING));
  EXPECT_TRUE(is_valid_status(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED));
  EXPECT_TRUE(is_valid_status(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED));
  EXPECT_TRUE(is_valid_status(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));


  // 无效值（组内空洞）
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(0)));    // 低于最小值
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(15)));   // Normal 组空洞
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(115)));  // Primary 组空洞
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(515)));  // Secondary 组空洞
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(999)));  // 超出范围
}

// ============================================================================
// Test 2: is_valid_transition - 验证 Normal 组转换
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_is_valid_transition_normal_group)
{
  int ret = OB_SUCCESS;

  // NORMAL_START -> NORMAL_FLUSHED: 有效
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::NORMAL_FLUSHED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // NORMAL_START -> PRIMARY_PREPARING: 有效 (进入主聚合流程)
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_SUCCESS, ret);

  // NORMAL_START -> SECONDARY_PREPARING: 有效 (进入从迁移准备流程)
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::SECONDARY_PREPARING);
  EXPECT_EQ(OB_SUCCESS, ret);

  // NORMAL_START -> SECONDARY_MIGRATING: 无效 (必须先经过 SECONDARY_PREPARING)
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::SECONDARY_MIGRATING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // NORMAL_FLUSHED 是终态，无任何转换
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_FLUSHED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // 幂等性: NORMAL_START -> NORMAL_START
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// ============================================================================
// Test 3: is_valid_transition - 验证 Primary 组转换
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_is_valid_transition_primary_group)
{
  int ret = OB_SUCCESS;

  // PRIMARY_PREPARING -> PRIMARY_COLLECTING: 有效
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::PRIMARY_COLLECTING);
  EXPECT_EQ(OB_SUCCESS, ret);

  // PRIMARY_PREPARING -> PRIMARY_AGGR_FAILED: 有效 (核心修复！)
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // PRIMARY_COLLECTING -> PRIMARY_AGGR_SUCCEEDED: 有效
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING, TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // PRIMARY_COLLECTING -> PRIMARY_AGGR_FAILED: 有效 (核心修复！)
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING, TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // PRIMARY_COLLECTING -> NORMAL_START: 无效
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // PRIMARY_AGGR_SUCCEEDED -> PRIMARY_COMPLETED: 有效 (聚合完成后刷第一条普通 redo)
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED, TxRedoFlushStatus::PRIMARY_COMPLETED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // PRIMARY_AGGR_SUCCEEDED -> 其他状态: 无效
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // PRIMARY_AGGR_FAILED 是终态
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // PRIMARY_COMPLETED 是终态
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COMPLETED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
}

// ============================================================================
// Test 4: is_valid_transition - 验证 Secondary 组转换
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_is_valid_transition_secondary_group)
{
  int ret = OB_SUCCESS;

  // SECONDARY_MIGRATING -> SECONDARY_MIGRATE_SYNCED: 有效
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING, TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // SECONDARY_MIGRATING -> SECONDARY_MIGRATE_FAILED: 有效
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING, TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // SECONDARY_MIGRATE_SYNCED -> SECONDARY_MIGRATE_SUCCEEDED: 有效
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // SECONDARY_MIGRATE_SYNCED -> SECONDARY_MIGRATE_FAILED: 有效
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // SECONDARY_MIGRATE_SUCCEEDED 是终态，不允许任何出边转换
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // SECONDARY_MIGRATE_FAILED 是终态，不允许任何出边转换
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // SECONDARY_PREPARING → NORMAL_START: 状态机不允许回退
  // reset_hotspot_redo_status_() 是特殊路径，直接赋值，不走状态机检查
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_PREPARING, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
}

// ============================================================================
// Test 5: is_valid_transition - 无效值检测
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_is_valid_transition_undefined_values)
{
  int ret = OB_SUCCESS;
  TxRedoFlushStatus undefined = static_cast<TxRedoFlushStatus>(999);

  // 从无效值转换 -> OB_ERR_UNEXPECTED
  ret = is_valid_transition(undefined, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_ERR_UNEXPECTED, ret);

  // 转换到无效值 -> OB_ERR_UNEXPECTED
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, undefined);
  EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
}

// ============================================================================
// Test 6: 辅助函数 - is_normal_status
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_is_normal_status)
{
  EXPECT_TRUE(is_normal_status(TxRedoFlushStatus::NORMAL_START));
  EXPECT_TRUE(is_normal_status(TxRedoFlushStatus::NORMAL_FLUSHED));

  EXPECT_FALSE(is_normal_status(TxRedoFlushStatus::PRIMARY_PREPARING));
  EXPECT_FALSE(is_normal_status(TxRedoFlushStatus::SECONDARY_MIGRATING));
}

// ============================================================================
// Test 7: 辅助函数 - has_completed_lifecycle
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_has_completed_lifecycle)
{
  // 终态：生命周期已完结
  EXPECT_TRUE(has_completed_lifecycle(TxRedoFlushStatus::NORMAL_FLUSHED));
  EXPECT_TRUE(has_completed_lifecycle(TxRedoFlushStatus::PRIMARY_AGGR_FAILED));
  EXPECT_TRUE(has_completed_lifecycle(TxRedoFlushStatus::PRIMARY_COMPLETED));
  EXPECT_TRUE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));
  EXPECT_TRUE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED));

  // 非终态：中间状态，还有后续转换
  EXPECT_FALSE(has_completed_lifecycle(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED)); // 还需要到 PRIMARY_COMPLETED
  EXPECT_FALSE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED)); // 还需要到 SUCCEEDED 或 FAILED
  EXPECT_FALSE(has_completed_lifecycle(TxRedoFlushStatus::NORMAL_START));
  EXPECT_FALSE(has_completed_lifecycle(TxRedoFlushStatus::PRIMARY_PREPARING));
  EXPECT_FALSE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_MIGRATING));
}

// ============================================================================
// Test 7b: 辅助函数 - is_eligible_for_aggregation_entry
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_is_eligible_for_aggregation_entry)
{
  // 只有 NORMAL_START 允许进入聚合流程
  EXPECT_TRUE(is_eligible_for_aggregation_entry(TxRedoFlushStatus::NORMAL_START));

  // 所有其他状态都不允许进入聚合
  EXPECT_FALSE(is_eligible_for_aggregation_entry(TxRedoFlushStatus::NORMAL_FLUSHED));
  EXPECT_FALSE(is_eligible_for_aggregation_entry(TxRedoFlushStatus::PRIMARY_PREPARING));
  EXPECT_FALSE(is_eligible_for_aggregation_entry(TxRedoFlushStatus::PRIMARY_COLLECTING));
  EXPECT_FALSE(is_eligible_for_aggregation_entry(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED));
  EXPECT_FALSE(is_eligible_for_aggregation_entry(TxRedoFlushStatus::PRIMARY_AGGR_FAILED));
  EXPECT_FALSE(is_eligible_for_aggregation_entry(TxRedoFlushStatus::PRIMARY_COMPLETED));
  EXPECT_FALSE(is_eligible_for_aggregation_entry(TxRedoFlushStatus::SECONDARY_PREPARING));
  EXPECT_FALSE(is_eligible_for_aggregation_entry(TxRedoFlushStatus::SECONDARY_MIGRATING));
  EXPECT_FALSE(is_eligible_for_aggregation_entry(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED));
  EXPECT_FALSE(is_eligible_for_aggregation_entry(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED));
  EXPECT_FALSE(is_eligible_for_aggregation_entry(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));

}

// ============================================================================
// Test 8: 辅助函数 - is_primary_status / is_secondary_status
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_is_primary_and_secondary_status)
{
  // Primary 组
  EXPECT_TRUE(is_primary_status(TxRedoFlushStatus::PRIMARY_PREPARING));
  EXPECT_TRUE(is_primary_status(TxRedoFlushStatus::PRIMARY_COLLECTING));
  EXPECT_TRUE(is_primary_status(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED));
  EXPECT_TRUE(is_primary_status(TxRedoFlushStatus::PRIMARY_AGGR_FAILED));
  EXPECT_TRUE(is_primary_status(TxRedoFlushStatus::PRIMARY_COMPLETED));

  // Secondary 组
  EXPECT_TRUE(is_secondary_status(TxRedoFlushStatus::SECONDARY_MIGRATING));
  EXPECT_TRUE(is_secondary_status(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED));
  EXPECT_TRUE(is_secondary_status(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED));
  EXPECT_TRUE(is_secondary_status(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));


  // 交叉验证: Primary 不属于 Secondary
  EXPECT_FALSE(is_secondary_status(TxRedoFlushStatus::PRIMARY_PREPARING));
  EXPECT_FALSE(is_primary_status(TxRedoFlushStatus::SECONDARY_MIGRATING));

  // Normal 不属于 Primary 或 Secondary
  EXPECT_FALSE(is_primary_status(TxRedoFlushStatus::NORMAL_START));
  EXPECT_FALSE(is_secondary_status(TxRedoFlushStatus::NORMAL_START));
}

// ============================================================================
// Test 9: to_cstr 字符串转换
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_to_cstr)
{
  EXPECT_STREQ("NORMAL_START", to_cstr(TxRedoFlushStatus::NORMAL_START));
  EXPECT_STREQ("NORMAL_FLUSHED", to_cstr(TxRedoFlushStatus::NORMAL_FLUSHED));
  EXPECT_STREQ("PRIMARY_PREPARING", to_cstr(TxRedoFlushStatus::PRIMARY_PREPARING));
  EXPECT_STREQ("PRIMARY_COLLECTING", to_cstr(TxRedoFlushStatus::PRIMARY_COLLECTING));
  EXPECT_STREQ("PRIMARY_AGGR_SUCCEEDED", to_cstr(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED));
  EXPECT_STREQ("PRIMARY_AGGR_FAILED", to_cstr(TxRedoFlushStatus::PRIMARY_AGGR_FAILED));
  EXPECT_STREQ("PRIMARY_COMPLETED", to_cstr(TxRedoFlushStatus::PRIMARY_COMPLETED));
  EXPECT_STREQ("SECONDARY_MIGRATING", to_cstr(TxRedoFlushStatus::SECONDARY_MIGRATING));
  EXPECT_STREQ("SECONDARY_MIGRATE_SYNCED", to_cstr(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED));
  EXPECT_STREQ("SECONDARY_MIGRATE_SUCCEEDED", to_cstr(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED));
  EXPECT_STREQ("SECONDARY_MIGRATE_FAILED", to_cstr(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));


  // 无效值返回 "INVALID_STATUS"
  EXPECT_STREQ("INVALID_STATUS", to_cstr(static_cast<TxRedoFlushStatus>(999)));
}

// ============================================================================
// Test 10: 值范围隔离验证
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_value_range_isolation)
{
  // Normal 组: <100
  EXPECT_LT(static_cast<int64_t>(TxRedoFlushStatus::NORMAL_START), 100);
  EXPECT_LT(static_cast<int64_t>(TxRedoFlushStatus::NORMAL_FLUSHED), 100);

  // Primary 组: 100-199
  EXPECT_GE(static_cast<int64_t>(TxRedoFlushStatus::PRIMARY_PREPARING), 100);
  EXPECT_LE(static_cast<int64_t>(TxRedoFlushStatus::PRIMARY_AGGR_FAILED), 199);

  // Secondary 组: >=500
  EXPECT_GE(static_cast<int64_t>(TxRedoFlushStatus::SECONDARY_MIGRATING), 500);


  // 确认组间有足够间隔（无交叉）
  EXPECT_LT(static_cast<int64_t>(TxRedoFlushStatus::NORMAL_FLUSHED),
            static_cast<int64_t>(TxRedoFlushStatus::PRIMARY_PREPARING));
  EXPECT_LT(static_cast<int64_t>(TxRedoFlushStatus::PRIMARY_AGGR_FAILED),
            static_cast<int64_t>(TxRedoFlushStatus::SECONDARY_MIGRATING));
}

// ============================================================================
// Test 11: 辅助函数 - is_primary_tx_aggregating
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_is_primary_tx_aggregating)
{
  // 聚合中的状态（返回 true）
  EXPECT_TRUE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_PREPARING));
  EXPECT_TRUE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_COLLECTING));

  // 非聚合状态（返回 false）
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::NORMAL_START));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::NORMAL_FLUSHED));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_AGGR_FAILED));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::PRIMARY_COMPLETED));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::SECONDARY_MIGRATING));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED));
  EXPECT_FALSE(is_primary_tx_aggregating(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));

}

// ============================================================================
// Test 13: TxSecondaryRespStatus::to_cstr 字符串转换
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_tx_secondary_resp_status_to_cstr)
{
  EXPECT_STREQ("RESP_NOT_SENT", to_cstr(TxSecondaryRespStatus::RESP_NOT_SENT));
  EXPECT_STREQ("RESP_COMMITTED", to_cstr(TxSecondaryRespStatus::RESP_COMMITTED));
  EXPECT_STREQ("RESP_ABORTED_BY_SESSION", to_cstr(TxSecondaryRespStatus::RESP_ABORTED_BY_SESSION));
  EXPECT_STREQ("RESP_SKIPPED_BY_PRIMARY", to_cstr(TxSecondaryRespStatus::RESP_SKIPPED_BY_PRIMARY));

  // 无效值返回 "INVALID_RESP_STATUS"
  EXPECT_STREQ("INVALID_RESP_STATUS", to_cstr(static_cast<TxSecondaryRespStatus>(999)));
}

// ============================================================================
// Test 14: 跨组转换测试 (WARN-5)
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_is_valid_transition_cross_group)
{
  int ret = OB_SUCCESS;

  // NORMAL_START -> PRIMARY_PREPARING: 有效 (进入主聚合流程)
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_SUCCESS, ret);

  // NORMAL_START -> SECONDARY_PREPARING: 有效 (进入从迁移准备流程)
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::SECONDARY_PREPARING);
  EXPECT_EQ(OB_SUCCESS, ret);

  // NORMAL_START -> SECONDARY_MIGRATING: 无效 (必须先经过 SECONDARY_PREPARING)
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::SECONDARY_MIGRATING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // NORMAL_FLUSHED -> Primary: 无效 (NORMAL_FLUSHED是终态)
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_FLUSHED, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // NORMAL_FLUSHED -> Secondary: 无效
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_FLUSHED, TxRedoFlushStatus::SECONDARY_MIGRATING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // Primary -> Normal: 无效
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::NORMAL_FLUSHED);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // Primary -> Secondary (非NORMAL_START): 无效
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::SECONDARY_MIGRATING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING, TxRedoFlushStatus::SECONDARY_MIGRATING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // Secondary -> Normal (非NORMAL_START): 无效
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING, TxRedoFlushStatus::NORMAL_FLUSHED);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // Secondary -> Primary: 无效
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, TxRedoFlushStatus::PRIMARY_COLLECTING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
}

// ============================================================================
// Test 15: 组内空洞全量检测（扩展测试 - QWEN/MiniMax review）
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_all_enum_gap_values_extended)
{
  // Normal组完整空洞 (10-80之间)
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(0)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(5)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(15)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(20)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(50)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(75)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(90)));

  // Primary组完整空洞 (110-180之间)
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(105)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(115)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(130)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(140)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(155)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(170)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(185)));
  EXPECT_FALSE(is_valid_status(static_cast<TxRedoFlushStatus>(190)));

  // Secondary组完整空洞 (510-580之间)
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
}

// ============================================================================
// Test 16: 终态保护 - 所有终态不允许任何出边转换（扩展测试 - GLM5 review）
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_terminal_state_protection)
{
  int ret = OB_SUCCESS;

  // NORMAL_FLUSHED 终态
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_FLUSHED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_FLUSHED, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // PRIMARY_AGGR_FAILED 终态
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED, TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // PRIMARY_COMPLETED 终态
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COMPLETED, TxRedoFlushStatus::NORMAL_START);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COMPLETED, TxRedoFlushStatus::PRIMARY_PREPARING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // SECONDARY_MIGRATE_SUCCEEDED 终态

  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED, TxRedoFlushStatus::SECONDARY_MIGRATING);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  // 终态幂等性测试
  ret = is_valid_transition(TxRedoFlushStatus::NORMAL_FLUSHED, TxRedoFlushStatus::NORMAL_FLUSHED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED, TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COMPLETED, TxRedoFlushStatus::PRIMARY_COMPLETED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED, TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// ============================================================================
// Test 17: 非终态中间状态完整性（扩展测试 - Kimi review）
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_intermediate_state_completeness)
{
  int ret = OB_SUCCESS;

  // PRIMARY_AGGR_SUCCEEDED 不是终态，必须能到 PRIMARY_COMPLETED
  EXPECT_FALSE(has_completed_lifecycle(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED));
  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED, TxRedoFlushStatus::PRIMARY_COMPLETED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // SECONDARY_MIGRATE_SYNCED 不是终态，必须能转换到 SUCCEEDED 或 FAILED
  EXPECT_FALSE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED));
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  // SECONDARY_MIGRATE_FAILED 是终态（聚合失败），不允许任何出边转换
  EXPECT_TRUE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));

  // SECONDARY_MIGRATE_SUCCEEDED 是终态（scheduler response成功），不允许任何出边转换
  EXPECT_TRUE(has_completed_lifecycle(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED));
}

// ============================================================================
// Test 18: 全状态幂等转换测试（扩展测试 - QWEN review）
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_idempotent_transitions_all_states)
{
  // 所有状态的 from==to 幂等转换都应返回 OB_SUCCESS
  EXPECT_EQ(OB_SUCCESS, is_valid_transition(TxRedoFlushStatus::NORMAL_START, TxRedoFlushStatus::NORMAL_START));
  EXPECT_EQ(OB_SUCCESS, is_valid_transition(TxRedoFlushStatus::NORMAL_FLUSHED, TxRedoFlushStatus::NORMAL_FLUSHED));
  EXPECT_EQ(OB_SUCCESS, is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::PRIMARY_PREPARING));
  EXPECT_EQ(OB_SUCCESS, is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING, TxRedoFlushStatus::PRIMARY_COLLECTING));
  EXPECT_EQ(OB_SUCCESS, is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED, TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED));
  EXPECT_EQ(OB_SUCCESS, is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED, TxRedoFlushStatus::PRIMARY_AGGR_FAILED));
  EXPECT_EQ(OB_SUCCESS, is_valid_transition(TxRedoFlushStatus::PRIMARY_COMPLETED, TxRedoFlushStatus::PRIMARY_COMPLETED));
  EXPECT_EQ(OB_SUCCESS, is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING, TxRedoFlushStatus::SECONDARY_MIGRATING));
  EXPECT_EQ(OB_SUCCESS, is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED));
  EXPECT_EQ(OB_SUCCESS, is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED, TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED));
  EXPECT_EQ(OB_SUCCESS, is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED, TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));

}

// ============================================================================
// Test 19: 空洞值的 is_valid_transition 返回 OB_ERR_UNEXPECTED（扩展测试）
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_gap_values_transition_returns_unexpected)
{
  TxRedoFlushStatus gap = static_cast<TxRedoFlushStatus>(115);
  EXPECT_EQ(OB_ERR_UNEXPECTED, is_valid_transition(gap, TxRedoFlushStatus::NORMAL_START));
  EXPECT_EQ(OB_ERR_UNEXPECTED, is_valid_transition(TxRedoFlushStatus::NORMAL_START, gap));

  gap = static_cast<TxRedoFlushStatus>(515);
  EXPECT_EQ(OB_ERR_UNEXPECTED, is_valid_transition(gap, TxRedoFlushStatus::SECONDARY_MIGRATING));
  EXPECT_EQ(OB_ERR_UNEXPECTED, is_valid_transition(TxRedoFlushStatus::SECONDARY_MIGRATING, gap));
}

// ============================================================================
// Test 20: PREPARING直接FAILED转换（核心修复验证 - GLM5/Kimi）
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_preparing_to_failed_immediate)
{
  int ret = OB_SUCCESS;

  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_PREPARING, TxRedoFlushStatus::PRIMARY_COMPLETED);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
}

// ============================================================================
// Test 21: COLLECTING到FAILED转换（核心修复 - 四模型共识）
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_collecting_to_failed_forced_switch)
{
  int ret = OB_SUCCESS;

  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING, TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING, TxRedoFlushStatus::PRIMARY_COMPLETED);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);

  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_COLLECTING, TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED);
  EXPECT_EQ(OB_SUCCESS, ret);
}

// ============================================================================
// Test 22: 二次幂等保护测试
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_double_set_idempotent_protection)
{
  int ret = OB_SUCCESS;

  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED, TxRedoFlushStatus::PRIMARY_AGGR_FAILED);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = is_valid_transition(TxRedoFlushStatus::PRIMARY_AGGR_FAILED, TxRedoFlushStatus::PRIMARY_COMPLETED);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
}

// ============================================================================
// Test 23: NEGATIVE值检测
// ============================================================================
TEST_F(ObTestTxRedoFlushStatusEnum, test_negative_status_value)
{
  TxRedoFlushStatus negative = static_cast<TxRedoFlushStatus>(-1);
  EXPECT_FALSE(is_valid_status(negative));
  EXPECT_EQ(OB_ERR_UNEXPECTED, is_valid_transition(negative, TxRedoFlushStatus::NORMAL_START));
  EXPECT_EQ(OB_ERR_UNEXPECTED, is_valid_transition(TxRedoFlushStatus::NORMAL_START, negative));
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_tx_redo_flush_status_enum.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_tx_redo_flush_status_enum.log", true, false,
                       "test_tx_redo_flush_status_enum.log",
                       "test_tx_redo_flush_status_enum.log",
                       "test_tx_redo_flush_status_enum.log");
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
