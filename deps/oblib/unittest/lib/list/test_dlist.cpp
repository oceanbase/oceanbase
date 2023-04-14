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

#define USING_LOG_PREFIX SHARE_PT

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#include "lib/list/ob_dlist.h"
#include "lib/allocator/page_arena.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgReferee;
using ::testing::Invoke;

namespace oceanbase
{
namespace common
{

class TestObDList : public ::testing::Test
{
public:
  TestObDList() {}

  virtual void SetUp() {}
  virtual void TearDown(){}
};

struct TestNode: public common::ObDLinkBase<TestNode>
{
  int64_t value_;
  OB_UNIS_VERSION(1);
};

OB_SERIALIZE_MEMBER(TestNode, value_);

TEST_F(TestObDList, encode_decode)
{
  common::ObDList<TestNode> list;
  TestNode node1;
  TestNode node2;
  node1.value_ = 1;
  node2.value_ = 2;

  ASSERT_TRUE(list.add_last(&node1));
  ASSERT_TRUE(list.add_last(&node2));

  int64_t buf_size = get_dlist_serialize_size(list);
  char *buf = static_cast<char*>(ob_malloc(buf_size, ObNewModIds::TEST));
  int64_t buf_len = buf_size;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialize_dlist(list, buf, buf_len, pos));
  ASSERT_EQ(buf_size, pos);
  LOG_INFO("print", K(buf_size));


  common::ObArenaAllocator allocator;
  common::ObDList<TestNode> new_list;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, deserialize_dlist(new_list, allocator, buf, buf_len, pos));

  TestNode *first_node = new_list.get_first();
  TestNode *second_node = first_node->get_next();
  ASSERT_TRUE(NULL != first_node);
  ASSERT_TRUE(NULL != second_node);
  ASSERT_EQ(1, first_node->value_);
  ASSERT_EQ(2, second_node->value_);
}



} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
