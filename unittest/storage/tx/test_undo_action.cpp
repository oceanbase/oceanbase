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

#include "storage/tx/ob_trans_define.h"
#include <gtest/gtest.h>
#include "lib/oblog/ob_log.h"
namespace oceanbase
{
using namespace transaction;
namespace unittest
{
struct TestUndoAction : public ::testing::Test
{
  virtual void SetUp() {}
  virtual void TearDown() {}
};
TEST_F(TestUndoAction, valid)
{
  ObUndoAction a1(ObTxSEQ(100, 0), ObTxSEQ(1, 1));
  EXPECT_FALSE(a1.is_valid());
  ObUndoAction a2(ObTxSEQ(100, 1), ObTxSEQ(100, 1));
  EXPECT_FALSE(a2.is_valid());
  ObUndoAction a3(ObTxSEQ(100, 0), ObTxSEQ(100, 0));
  EXPECT_FALSE(a3.is_valid());
  ObUndoAction a4(ObTxSEQ(100, 0), ObTxSEQ(100, 1));
  EXPECT_FALSE(a4.is_valid());
  ObUndoAction a5(ObTxSEQ(100, 1), ObTxSEQ(100, 0));
  EXPECT_FALSE(a5.is_valid());
  ObUndoAction a6(ObTxSEQ(100, 1), ObTxSEQ(1, 0));
  EXPECT_FALSE(a6.is_valid());
  ObUndoAction a7(ObTxSEQ(100, 1), ObTxSEQ(1, 1));
  EXPECT_TRUE(a7.is_valid());
  ObUndoAction a8(ObTxSEQ(100, 0), ObTxSEQ(1, 0));
  EXPECT_TRUE(a8.is_valid());
}
TEST_F(TestUndoAction, contain)
{
  ObUndoAction a1(ObTxSEQ(100,1), ObTxSEQ(1, 1));
  ObUndoAction a2(ObTxSEQ(99,1), ObTxSEQ(1, 1));
  EXPECT_TRUE(a1.is_contain(a2));
  EXPECT_FALSE(a2.is_contain(a1));
  ObUndoAction a3(ObTxSEQ(100,0), ObTxSEQ(1, 0));
  ObUndoAction a4(ObTxSEQ(99,0), ObTxSEQ(1, 0));
  EXPECT_TRUE(a3.is_contain(a4));
  EXPECT_FALSE(a4.is_contain(a3));
  ObUndoAction a5(ObTxSEQ(100,2), ObTxSEQ(1, 2));
  EXPECT_FALSE(a5.is_contain(a1));
  EXPECT_FALSE(a5.is_contain(a2));
  EXPECT_FALSE(a1.is_contain(a5));
  EXPECT_TRUE(a3.is_contain(a5));
  EXPECT_FALSE(a4.is_contain(a5));
}

TEST_F(TestUndoAction, contain_point)
{
  ObUndoAction a1(ObTxSEQ(100,1), ObTxSEQ(1, 1));
  ObUndoAction a3(ObTxSEQ(100,0), ObTxSEQ(1, 0));
  EXPECT_TRUE(a3.is_contain(ObTxSEQ(50, 1)));
  EXPECT_TRUE(a1.is_contain(ObTxSEQ(50, 1)));
  EXPECT_FALSE(a1.is_contain(ObTxSEQ(50, 0)));
  EXPECT_FALSE(a1.is_contain(ObTxSEQ(50, 2)));
  EXPECT_TRUE(a3.is_contain(ObTxSEQ(50, 0)));
}
} // unittest
} //oceanbase
using namespace oceanbase;
using namespace transaction;
int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_undo_action.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
