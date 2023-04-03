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

#define USING_LOG_PREFIX SQL_PC
#include <iostream>
#include <gtest/gtest.h>
#include "sql/ob_sql_init.h"
#include "sql/plan_cache/ob_parse_node.h"
#include "sql/plan_cache/ob_plan_set.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

class ObParseNodeWrapperTest : public ::testing::Test
{
public:
  ObParseNodeWrapperTest();
  virtual ~ObParseNodeWrapperTest();
  int test_parse_node_wrapper();
  int init();
  void SetUp()
  {
    int ret = init();
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ObPlanCache plan_cache_;
};

int ObParseNodeWrapperTest::init()
{
  int ret = OB_SUCCESS;
  return ret;
}

ObParseNodeWrapperTest::ObParseNodeWrapperTest()
{
}

ObParseNodeWrapperTest::~ObParseNodeWrapperTest()
{
}

int ObParseNodeWrapperTest::test_parse_node_wrapper()
{
  int ret = OB_SUCCESS;
  _OB_LOG(INFO, "size of wrapper: %lu", sizeof(ObParseNode));
  ObParseNode *node = ObParseNode::alloc();
  EXPECT_TRUE(NULL != node);
  _OB_LOG(INFO, "hash of wrapper: %lu", node->hash());
  ObParseNode::free(node);
  ObPlanSet *plan_set = ObPlanSet::alloc();
  EXPECT_TRUE(NULL != plan_set);
  ObPlanSet::free(plan_set);
  return ret;
}

TEST_F(ObParseNodeWrapperTest, test_parse_node_wrapper)
{
  EXPECT_TRUE(OB_SUCCESS == test_parse_node_wrapper());
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("DEBUG");
  ::oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
