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

#include "clog/ob_log_broadcast_info_mgr.h"
#include <gtest/gtest.h>
#include "clog_mock_container/mock_log_membership_mgr.h"

namespace oceanbase {
using namespace common;
using namespace clog;
namespace unittest {
class TestMockObLogMembershipMgr : public MockObLogMembershipMgr {
public:
  void set_replica_num(const int64_t replica_num)
  {
    replica_num_ = replica_num;
  }
  int64_t get_replica_num() const
  {
    return replica_num_;
  }
  void set_member_list(const common::ObMemberList& member_list)
  {
    member_list_.deep_copy(member_list);
  }
  const common::ObMemberList& get_curr_member_list() const
  {
    return member_list_;
  }

private:
  int64_t replica_num_;
  common::ObMemberList member_list_;
};

class ObLogBroadcastInfoMgrTest : public testing::Test {
public:
  void SetUp();
  void TearDown();

  TestMockObLogMembershipMgr mm_;
  ObAddr server1_;
  ObAddr server2_;
  ObAddr server3_;
  ObAddr server4_;
  ObAddr server5_;
  ObMemberList member_list_;

  ObLogBroadcastInfoMgr broadcast_info_mgr_;
};

void ObLogBroadcastInfoMgrTest::SetUp()
{
  const int64_t replica_num = 5;

  common::ObPartitionKey partition_key(1099511627777, 1, 1);

  server1_.set_ip_addr("127.0.0.1", 1234);
  server2_.set_ip_addr("127.0.0.2", 1234);
  server3_.set_ip_addr("127.0.0.3", 1234);
  server4_.set_ip_addr("127.0.0.4", 1234);
  server5_.set_ip_addr("127.0.0.5", 1234);

  member_list_.add_server(server1_);
  member_list_.add_server(server2_);
  member_list_.add_server(server3_);
  member_list_.add_server(server4_);
  member_list_.add_server(server5_);

  mm_.set_replica_num(replica_num);
  mm_.set_member_list(member_list_);

  broadcast_info_mgr_.init(partition_key, &mm_);
}

void ObLogBroadcastInfoMgrTest::TearDown()
{
  broadcast_info_mgr_.destroy();
}

TEST_F(ObLogBroadcastInfoMgrTest, test1)
{
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server1_, REPLICA_TYPE_FULL, 10));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server2_, REPLICA_TYPE_FULL, 20));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server3_, REPLICA_TYPE_FULL, 30));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server4_, REPLICA_TYPE_LOGONLY, 5));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server5_, REPLICA_TYPE_LOGONLY, 50));

  uint64_t log_id = OB_INVALID_ID;
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.get_recyclable_log_id(log_id));
  EXPECT_EQ(10, log_id);
}

TEST_F(ObLogBroadcastInfoMgrTest, test2)
{
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server1_, REPLICA_TYPE_FULL, 10));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server2_, REPLICA_TYPE_FULL, 20));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server3_, REPLICA_TYPE_FULL, 30));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server4_, REPLICA_TYPE_FULL, 40));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server5_, REPLICA_TYPE_LOGONLY, 50));

  uint64_t log_id = OB_INVALID_ID;
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.get_recyclable_log_id(log_id));
  EXPECT_EQ(20, log_id);
}

TEST_F(ObLogBroadcastInfoMgrTest, test3)
{
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server1_, REPLICA_TYPE_FULL, 10));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server2_, REPLICA_TYPE_FULL, 20));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server3_, REPLICA_TYPE_LOGONLY, 30));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server4_, REPLICA_TYPE_LOGONLY, 40));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server5_, REPLICA_TYPE_LOGONLY, 50));

  uint64_t log_id = OB_INVALID_ID;
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.get_recyclable_log_id(log_id));
  EXPECT_EQ(10, log_id);
}

TEST_F(ObLogBroadcastInfoMgrTest, test4)
{
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server1_, REPLICA_TYPE_FULL, 10));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server2_, REPLICA_TYPE_FULL, 20));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server3_, REPLICA_TYPE_FULL, 30));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server4_, REPLICA_TYPE_FULL, 40));

  uint64_t log_id = OB_INVALID_ID;
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.get_recyclable_log_id(log_id));
  EXPECT_EQ(20, log_id);
}

TEST_F(ObLogBroadcastInfoMgrTest, test5)
{
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server1_, REPLICA_TYPE_FULL, 10));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server2_, REPLICA_TYPE_FULL, 20));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server3_, REPLICA_TYPE_FULL, 30));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server4_, REPLICA_TYPE_LOGONLY, 40));

  uint64_t log_id = OB_INVALID_ID;
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.get_recyclable_log_id(log_id));
  EXPECT_EQ(10, log_id);
}

TEST_F(ObLogBroadcastInfoMgrTest, test6)
{
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server1_, REPLICA_TYPE_FULL, 10));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server2_, REPLICA_TYPE_FULL, 20));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server3_, REPLICA_TYPE_LOGONLY, 30));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server4_, REPLICA_TYPE_LOGONLY, 40));

  uint64_t log_id = OB_INVALID_ID;
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.get_recyclable_log_id(log_id));
  EXPECT_EQ(0, log_id);
}

TEST_F(ObLogBroadcastInfoMgrTest, test7)
{
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server1_, REPLICA_TYPE_FULL, 10));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server2_, REPLICA_TYPE_FULL, 10));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server3_, REPLICA_TYPE_FULL, 10));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server4_, REPLICA_TYPE_FULL, 10));

  uint64_t log_id = OB_INVALID_ID;
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.get_recyclable_log_id(log_id));
  EXPECT_EQ(10, log_id);
}

TEST_F(ObLogBroadcastInfoMgrTest, test8)
{
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server1_, REPLICA_TYPE_FULL, 10));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server2_, REPLICA_TYPE_FULL, 20));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server2_, REPLICA_TYPE_FULL, 25));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server3_, REPLICA_TYPE_FULL, 30));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server4_, REPLICA_TYPE_FULL, 40));
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.update_broadcast_info(server5_, REPLICA_TYPE_LOGONLY, 50));

  uint64_t log_id = OB_INVALID_ID;
  EXPECT_EQ(OB_SUCCESS, broadcast_info_mgr_.get_recyclable_log_id(log_id));
  EXPECT_EQ(25, log_id);
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_ob_log_broadcast_info_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_ob_log_broadcast_info_mgr");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
