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

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "share/ls/ob_ls_info.h"
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"



namespace oceanbase
{
using namespace unittest;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace common;

class TestLSReplica : public unittest::ObSimpleClusterTestBase
{
public:
  TestLSReplica() : unittest::ObSimpleClusterTestBase("test_ls_replica") {}
protected:
  ObLSReplica ls_replica_;
};

TEST_F(TestLSReplica, test_text2learnerlist)
{
  int ret = OB_SUCCESS;
  GlobalLearnerList learner_list;
  GlobalLearnerList learner_list2;
  bool learner_exists = false;

  const ObAddr addr1(ObAddr::IPV4, "127.0.0.1", 1000);
  const ObAddr addr2(ObAddr::IPV4, "127.0.0.1", 1001);
  const ObAddr addr3(ObAddr::IPV4, "127.0.0.2", 1000);
  const ObAddr addr4(ObAddr::IPV4, "127.0.0.2", 1001);

  // test learner with flag
  ObString string_to_parse = "127.0.0.1:1000:0:1,127.0.0.1:1001:0:1,127.0.0.2:1000:1:0";
  ObString string_to_parse2 = "127.0.0.1:1000:0:1";
  ret = ls_replica_.text2learner_list(to_cstring(string_to_parse), learner_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_replica_.text2learner_list(to_cstring(string_to_parse2), learner_list2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMember member1(addr1, 0);
  learner_exists = learner_list.contains(member1);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list2.contains(member1);
  ASSERT_EQ(false, learner_exists);

  member1.set_flag(-1);
  learner_exists = learner_list.contains(member1);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list2.contains(member1);
  ASSERT_EQ(false, learner_exists);

  member1.set_flag(1);
  learner_exists = learner_list.contains(member1);
  ASSERT_EQ(true, learner_exists);
  learner_exists = learner_list2.contains(member1);
  ASSERT_EQ(true, learner_exists);

  ObMember member2(addr2, 0);
  member2.set_flag(10);
  learner_exists = learner_list.contains(member2);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list2.contains(member2);
  ASSERT_EQ(false, learner_exists);

  ObMember member3(addr3, 0);
  member3.set_flag(0);
  learner_exists = learner_list.contains(member3);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list2.contains(member3);
  ASSERT_EQ(false, learner_exists);

  ObMember member4(addr4, 0);
  learner_exists = learner_list.contains(member4);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list2.contains(member4);
  ASSERT_EQ(false, learner_exists);

  ObMember member5(addr1, 0);
  learner_exists = learner_list.contains(member5);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list2.contains(member5);
  ASSERT_EQ(false, learner_exists);

  // test learner without flag
  GlobalLearnerList learner_list3;
  GlobalLearnerList learner_list4;

  ObString string_to_parse3 = "127.0.0.1:1000:0,127.0.0.1:1001:0,127.0.0.2:1000:0";
  ObString string_to_parse4 = "127.0.0.1:1000:0";

  ret = ls_replica_.text2learner_list(to_cstring(string_to_parse3), learner_list3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_replica_.text2learner_list(to_cstring(string_to_parse4), learner_list4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMember member6(addr1, 0);
  learner_exists = learner_list3.contains(member6);
  ASSERT_EQ(true, learner_exists);
  learner_exists = learner_list4.contains(member6);
  ASSERT_EQ(true, learner_exists);

  ObMember member7(addr1, 0);
  member7.set_flag(1);
  learner_exists = learner_list3.contains(member7);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list4.contains(member7);
  ASSERT_EQ(false, learner_exists);

  // test ipv6
  GlobalLearnerList learner_list5;
  GlobalLearnerList learner_list6;
  GlobalLearnerList learner_list7;
  GlobalLearnerList learner_list8;
  GlobalLearnerList learner_list9;
  GlobalLearnerList learner_list10;

  ObAddr addr7;
  ret = addr7.parse_from_string("[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:100");
  ASSERT_EQ(0, ret);

  ObAddr addr8;
  ret = addr8.parse_from_string("[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:101");
  ASSERT_EQ(0, ret);

  ObString string_to_parse5 = "[2610:00f8:0c34:67f9:0200:83ff:fe94:4c36]:100:-1,[2610:00f8:0c34:67f9:0200:83ff:fe94:4c36]:101:-1,[2610:00f8:0c34:67f9:0200:83ff:fe94:4c36]:102:-1";
  ObString string_to_parse6 = "[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:100:-1,[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:101:-1,[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:102:-1";
  ObString string_to_parse7 = "[2610:00f8:0c34:67f9:0200:83ff:fe94:4c36]:100:-1:1,[2610:00f8:0c34:67f9:0200:83ff:fe94:4c36]:101:-1:-1,[2610:00f8:0c34:67f9:0200:83ff:fe94:4c36]:102:-1:0";
  ObString string_to_parse8 = "[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:100:-1:0,[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:101:-1:-1,[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:102:-1:1";
  ObString string_to_parse9 = "[ABCD:EF01:2345:6789:ABCD:::6789]:100:-1:0";
  ObString string_to_parse10 = "127.0.0.1:100::";

  ret = ls_replica_.text2learner_list(to_cstring(string_to_parse5), learner_list5);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_replica_.text2learner_list(to_cstring(string_to_parse6), learner_list6);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_replica_.text2learner_list(to_cstring(string_to_parse7), learner_list7);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_replica_.text2learner_list(to_cstring(string_to_parse8), learner_list8);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ls_replica_.text2learner_list(to_cstring(string_to_parse9), learner_list9);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = ls_replica_.text2learner_list(to_cstring(string_to_parse10), learner_list10);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ObMember member8(addr7, -1);
  learner_exists = learner_list5.contains(member8);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list6.contains(member8);
  ASSERT_EQ(true, learner_exists);
  learner_exists = learner_list7.contains(member8);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list8.contains(member8);
  ASSERT_EQ(true, learner_exists);

  member8.set_flag(10);
  learner_exists = learner_list5.contains(member8);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list6.contains(member8);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list7.contains(member8);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list8.contains(member8);
  ASSERT_EQ(false, learner_exists);

  member8.set_flag(-1);
  learner_exists = learner_list5.contains(member8);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list6.contains(member8);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list7.contains(member8);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list8.contains(member8);
  ASSERT_EQ(false, learner_exists);

  ObMember member9(addr8, -1);
  learner_exists = learner_list5.contains(member9);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list6.contains(member9);
  ASSERT_EQ(true, learner_exists);
  learner_exists = learner_list7.contains(member9);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list8.contains(member9);
  ASSERT_EQ(false, learner_exists);

  member9.set_flag(10);
  learner_exists = learner_list5.contains(member9);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list6.contains(member9);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list7.contains(member9);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list8.contains(member9);
  ASSERT_EQ(false, learner_exists);

  ObMember member10(addr8, 1);
  learner_exists = learner_list5.contains(member10);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list6.contains(member10);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list7.contains(member10);
  ASSERT_EQ(false, learner_exists);
  learner_exists = learner_list8.contains(member10);
  ASSERT_EQ(false, learner_exists);
}
} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
