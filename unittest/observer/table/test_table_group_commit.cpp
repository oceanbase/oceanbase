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

#include <gtest/gtest.h>
#define private public  // 获取private成员
#define protected public  // 获取protect成员
#include "observer/table/group/ob_table_tenant_group.h"

using namespace oceanbase::common;
using namespace oceanbase::table;

class TestTableGroupCommit: public ::testing::Test
{
public:
  static const int64_t DEFAULT_MAX_GROUP_SIZE;
  static const int64_t DEFAULT_MIN_GROUP_SIZE;
  TestTableGroupCommit() {};
  virtual ~TestTableGroupCommit() {}
public:
  ObTableGroupCommitMgr mgr_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestTableGroupCommit);
};

const int64_t TestTableGroupCommit::DEFAULT_MAX_GROUP_SIZE = 50;
const int64_t TestTableGroupCommit::DEFAULT_MIN_GROUP_SIZE = 1;

// 测试用例1: 递减小于0.5，不会小于最小组大小
TEST_F(TestTableGroupCommit, DecreaseLessThanHalf) {
  int64_t cur_size = 10;
  double percent = 0.25; // percent*2 < 0.5
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, true), 9);
}

// 测试用例2: 递增小于0.5，但不会超过最大组大小
TEST_F(TestTableGroupCommit, IncreaseLessThanHalf) {
  int64_t cur_size = 10;
  double percent = 0.25; // percent*2 < 0.5
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, false), 11);
}

// 测试用例3: 递减0.5或更大，不会小于最小组大小
TEST_F(TestTableGroupCommit, DecreaseMoreThanHalf) {
  int64_t cur_size = 5;
  double percent = 0.5; // percent*2 >= 0.5
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, true), 3);
}

// 测试用例4: 递增0.5或更大，但不会超过最大组大小
TEST_F(TestTableGroupCommit, IncreaseMoreThanHalf) {
  int64_t cur_size = 5;
  double percent = 0.5; // percent*2 >= 0.5
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, false), 7);
}

// 测试用例5: 递增/递减，保持在最小和最大组大小之间
TEST_F(TestTableGroupCommit, NormalIncrementAndDecrement) {
  int64_t cur_size = 25;
  double percent = 0.7; // percent*2 > 0.5
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, true), 23);
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, false), 27);
}

// 测试用例6: 边界条件测试 - 超出最大组大小
TEST_F(TestTableGroupCommit, ExceedMaxGroupSize) {
  int64_t cur_size = DEFAULT_MAX_GROUP_SIZE - 1;
  double percent = 0.7; // percent*2 > 0.5
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, false), DEFAULT_MAX_GROUP_SIZE);
}

// 测试用例7: 边界条件测试 - 小于最小组大小
TEST_F(TestTableGroupCommit, BelowMinGroupSize) {
  int64_t cur_size = DEFAULT_MIN_GROUP_SIZE + 1;
  double percent = 0.7; // percent*2 > 0.5
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, true), DEFAULT_MIN_GROUP_SIZE);
}

// 测试用例8: 当前组大小等于最小组大小，递减时应保持不变
TEST_F(TestTableGroupCommit, CurrentEqualsMinSizeDecrease) {
  int64_t cur_size = DEFAULT_MIN_GROUP_SIZE;
  double percent = 0.1; // percent*2 < 0.5
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, true), DEFAULT_MIN_GROUP_SIZE);
}

// 测试用例9: 当前组大小等于最大组大小，递增时应保持不变
TEST_F(TestTableGroupCommit, CurrentEqualsMaxSizeIncrease) {
  int64_t cur_size = DEFAULT_MAX_GROUP_SIZE;
  double percent = 0.1; // percent*2 < 0.5
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, false), DEFAULT_MAX_GROUP_SIZE);
}

// 测试用例10: 百分比为负数，不变
TEST_F(TestTableGroupCommit, NegativePercentIncrease) {
  int64_t cur_size = 10;
  double percent = -0.1; // 负百分比
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, false), 10);
}

// 测试用例11: 百分比为负数，不变
TEST_F(TestTableGroupCommit, NegativePercentDecrease) {
  int64_t cur_size = 10;
  double percent = -0.1; // 负百分比
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, true), 10);
}

// 测试用例12: 百分比刚好为0.5，递增时应做出完全增加
TEST_F(TestTableGroupCommit, PercentExactlyHalfIncrease) {
  int64_t cur_size = 10;
  double percent = 0.5; // 刚好0.5
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, false), 12);
}

// 测试用例13: 百分比刚好为0.5，递减时应做出完全减少
TEST_F(TestTableGroupCommit, PercentExactlyHalfDecrease) {
  int64_t cur_size = 10;
  double percent = 0.5; // 刚好0.5
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, true), 8);
}

// 测试用例14: 百分比超过1，递减时应保持在最小组大小范围内
TEST_F(TestTableGroupCommit, PercentAboveOneDecrease) {
  int64_t cur_size = 10;
  double percent = 1.5; // percent*2 > 1
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, true), 8);
}

// 测试用例15: 当前组大小刚好为最大尺寸，递增时应保持不变
TEST_F(TestTableGroupCommit, CurrentSizeMaxNoIncrease) {
  int64_t cur_size = DEFAULT_MAX_GROUP_SIZE;
  double percent = 0.5; // 任意非零百分比
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, false), DEFAULT_MAX_GROUP_SIZE);
}

// 测试用例16: 当前组大小刚好为最小尺寸，递减时应不变
TEST_F(TestTableGroupCommit, CurrentSizeMinNoDecrease) {
  int64_t cur_size = DEFAULT_MIN_GROUP_SIZE;
  double percent = 0.5; // 任意非零百分比
  ASSERT_EQ(mgr_.group_size_and_ops_task_.get_new_group_size(cur_size, percent, true), DEFAULT_MIN_GROUP_SIZE);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_table_group.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
