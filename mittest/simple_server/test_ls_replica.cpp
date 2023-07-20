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
  bool learner_exists = false;

  const ObAddr addr1(ObAddr::IPV4, "127.0.0.1", 1000);
  const ObAddr addr2(ObAddr::IPV4, "127.0.0.1", 1001);
  const ObAddr addr3(ObAddr::IPV4, "127.0.0.2", 1000);
  const ObAddr addr4(ObAddr::IPV4, "127.0.0.2", 1001);

  ObString string_to_parse = "127.0.0.1:1000:0:1,127.0.0.1:1001:0:1,127.0.0.2:1000:1:0";
  ret = ls_replica_.text2learner_list(to_cstring(string_to_parse), learner_list);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMember member1(addr1, 0);
  learner_exists = learner_list.contains(member1);
  ASSERT_EQ(false, learner_exists);

  member1.set_flag(-1);
  learner_exists = learner_list.contains(member1);
  ASSERT_EQ(false, learner_exists);

  member1.set_flag(1);
  learner_exists = learner_list.contains(member1);
  ASSERT_EQ(true, learner_exists);

  ObMember member2(addr2, 0);
  member2.set_flag(10);
  learner_exists = learner_list.contains(member2);
  ASSERT_EQ(false, learner_exists);

  ObMember member3(addr3, 0);
  member3.set_flag(0);
  learner_exists = learner_list.contains(member3);
  ASSERT_EQ(false, learner_exists);

  ObMember member4(addr4, 0);
  learner_exists = learner_list.contains(member4);
  ASSERT_EQ(false, learner_exists);

}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
