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

#include "gtest/gtest.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_define.h"
#include "logservice/libobcdc/src/ob_cdc_lightly_sorted_list.h"
#include "logservice/libobcdc/src/ob_log_trans_log.h"

#define USING_LOG_PREFIX

using namespace oceanbase;
using namespace common;
using namespace libobcdc;


namespace oceanbase
{
namespace unittest
{

struct TestNode
{
  TestNode(): data_(0), next_(NULL) {}
  TestNode(const int64_t data): data_(data), next_(NULL) {}

  void set_next(TestNode *node) {next_ = node;}
  TestNode *get_next() const {return next_;}
  bool operator==(const TestNode &other) {return data_ == other.data_;}
  bool operator<(const TestNode &other) {return data_ < other.data_;}

  TO_STRING_KV(K_(data), KPC_(next));

  int64_t data_;
  TestNode *next_;
};

TEST(SortedLightyList, unique_element_list)
{
  OBLOG_LOG(INFO, "test unique_element_list");
  SortedLightyList<TestNode> list(true);

  TestNode n1(1);
  TestNode n2(2);
  TestNode n3(3);
  TestNode n4(4);

  // 1. push by order
  list.reset_data();
  list.push(&n1);
  list.push(&n2);
  list.push(&n3);
  OBLOG_LOG(INFO, "push by order", K(list));
  ASSERT_EQ(3, list.count());
  ASSERT_TRUE(list.contains(&n1));
  ASSERT_TRUE(list.contains(&n2));
  ASSERT_TRUE(list.contains(&n3));
  ASSERT_FALSE(list.contains(&n4));

  // 2. push same data multi times
  list.reset_data();
  list.push(&n1);
  list.push(&n2);
  list.push(&n3);
  list.push(&n3);
  list.push(&n2);
  ASSERT_EQ(3, list.count());
  ASSERT_TRUE(list.contains(&n1));
  ASSERT_TRUE(list.contains(&n2));
  ASSERT_TRUE(list.contains(&n3));
  ASSERT_FALSE(list.contains(&n4));

  // 3. push node with same data multi times
  TestNode tt2(2);
  TestNode tt3(3);
  list.reset_data();
  list.push(&n1);
  list.push(&n2);
  list.push(&n3);
  list.push(&tt3);
  list.push(&tt2);
  OBLOG_LOG(INFO, "push node with same data multi times", K(list));
  ASSERT_EQ(3, list.count());
  ASSERT_TRUE(list.contains(&n1));
  ASSERT_TRUE(list.contains(&n2));
  ASSERT_TRUE(list.contains(&n3));
  ASSERT_FALSE(list.contains(&n4));

  // 4. push out of order
  list.reset_data();
  list.push(&n2);
  list.push(&n1); // push at head
  list.push(&n3); // push at tail
  OBLOG_LOG(INFO, "push out of order", K(list));
  ASSERT_EQ(3, list.count());
  ASSERT_TRUE(list.contains(&n1));
  ASSERT_TRUE(list.contains(&n2));
  ASSERT_TRUE(list.contains(&n3));
  ASSERT_FALSE(list.contains(&n4));

  // 5. push same data multi times(out of order)
  list.reset_data();
  list.push(&n2);
  list.push(&n1); // push at head
  list.push(&n3); // push at tail
  list.push(&n1);
  list.push(&n3);
  list.push(&n2);

  OBLOG_LOG(INFO, "push same data multi times(out of order)", K(list));
  ASSERT_EQ(3, list.count());
  ASSERT_TRUE(list.contains(&n1));
  ASSERT_TRUE(list.contains(&n2));
  ASSERT_TRUE(list.contains(&n3));
  ASSERT_FALSE(list.contains(&n4));

  // 6. push node with same data multi times(out of order)
  TestNode t1(1);
  TestNode t2(2);
  TestNode t3(3);
  list.reset_data();
  list.push(&n2);
  list.push(&n1); // push at head
  list.push(&n3); // push at tail
  list.push(&t1);
  list.push(&t3);
  list.push(&t2);

  OBLOG_LOG(INFO, "push node with same data multi times(out of order)", K(list));
  ASSERT_EQ(3, list.count());
  ASSERT_TRUE(list.contains(&n1));
  ASSERT_TRUE(list.contains(&n2));
  ASSERT_TRUE(list.contains(&n3));
  ASSERT_TRUE(list.contains(&t1));
  ASSERT_TRUE(list.contains(&t2));
  ASSERT_TRUE(list.contains(&t3));
  ASSERT_FALSE(list.contains(&n4));
}

TEST(SortedLightyList, non_unique_element_list)
{
  OBLOG_LOG(INFO, "test non_unique_element_list");
  SortedLightyList<TestNode> list(false);

  TestNode n1(1);
  TestNode n2(2);
  TestNode n3(3);
  TestNode n4(4);

  // 1. push by order
  list.reset_data();
  list.push(&n1);
  list.push(&n2);
  list.push(&n3);
  OBLOG_LOG(INFO, "push by order", K(list));
  ASSERT_EQ(3, list.count());
  ASSERT_TRUE(list.contains(&n1));
  ASSERT_TRUE(list.contains(&n2));
  ASSERT_TRUE(list.contains(&n3));
  ASSERT_FALSE(list.contains(&n4));

  // 2. push same data multi times
  list.reset_data();
  list.push(&n1);
  list.push(&n2);
  list.push(&n3);
  list.push(&n3); // will ignore the node already in list(which next_ptr is not null)
  list.push(&n2);
  OBLOG_LOG(INFO, "push same data multi times", K(list));
  ASSERT_EQ(3, list.count());
  TestNode t5(5);
  TestNode t3(3);
  list.push(&t5);
  list.push(&t3);
  ASSERT_EQ(5, list.count());
  ASSERT_TRUE(list.contains(&n1));
  ASSERT_TRUE(list.contains(&n2));
  ASSERT_TRUE(list.contains(&n3));
  ASSERT_FALSE(list.contains(&n4));
  ASSERT_TRUE(list.contains(&t5));

  // 3. push node with same data multi times
  TestNode tt2(2);
  TestNode tt3(3);
  list.reset_data();
  list.push(&n1);
  list.push(&n2);
  list.push(&n3);
  list.push(&tt3);
  list.push(&tt2);
  OBLOG_LOG(INFO, "push node with same data multi times", K(list));
  ASSERT_EQ(5, list.count());
  ASSERT_TRUE(list.contains(&n1));
  ASSERT_TRUE(list.contains(&n2));
  ASSERT_TRUE(list.contains(&n3));
  ASSERT_FALSE(list.contains(&n4));

  // 4. push out of order
  list.reset_data();
  list.push(&n2);
  list.push(&n1); // push at head
  list.push(&n3); // push at tail
  OBLOG_LOG(INFO, "push out of order", K(list));
  ASSERT_EQ(3, list.count());
  ASSERT_TRUE(list.contains(&n1));
  ASSERT_TRUE(list.contains(&n2));
  ASSERT_TRUE(list.contains(&n3));
  ASSERT_FALSE(list.contains(&n4));

  // 5. push same data multi times(out of order)
  list.reset_data();
  list.push(&n2);
  list.push(&n1); // push at head
  list.push(&n3); // push at tail
  list.push(&n1);
  list.push(&tt3);
  list.push(&tt2);

  OBLOG_LOG(INFO, "push same data multi times(out of order)", K(list));
  ASSERT_EQ(5, list.count());
  ASSERT_TRUE(list.contains(&n1));
  ASSERT_TRUE(list.contains(&n2));
  ASSERT_TRUE(list.contains(&t3));
  ASSERT_FALSE(list.contains(&n4));

  // 6. push node with same data multi times(out of order)
  TestNode t1(1);
  TestNode t2(2);
  TestNode ttt3(3);
  list.reset_data();
  list.push(&n2);
  list.push(&n1); // push at head
  list.push(&n3); // push at tail
  list.push(&t1);
  list.push(&ttt3);
  list.push(&t2);

  OBLOG_LOG(INFO, "push node with same data multi times(out of order)", K(list));
  ASSERT_EQ(6, list.count());
  ASSERT_TRUE(list.contains(&n1));
  ASSERT_TRUE(list.contains(&n2));
  ASSERT_TRUE(list.contains(&n3));
  ASSERT_TRUE(list.contains(&t1));
  ASSERT_TRUE(list.contains(&t2));
  ASSERT_TRUE(list.contains(&t3));
  ASSERT_FALSE(list.contains(&n4));
}

}
}

int main(int argc, char **argv)
{
  //ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_sorted_list.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}