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
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/ob_define.h"
#include "share/rpc/ob_batch_rpc.h"
#include "lib/container/ob_array.h"
#include "election/ob_election_mgr.h"
#include "election/ob_election_priority.h"
#include "election/ob_election_cb.h"
#include "election/ob_election_time_def.h"
#include "common/ob_clock_generator.h"
#include "election/ob_election_rpc.h"
#include "election/ob_election_async_log.h"
#include "mock_ob_election_mgr.h"

namespace oceanbase {
namespace unittest {
using namespace election;
using namespace obrpc;
using namespace common;

static int64_t membership_version = 0;

static int64_t tenant_id_ = 1001;
static uint64_t table_id_ = tenant_id_ << 40;
static ObElectionMgr mgr_;

class TestObElectionMgr : public ::testing::Test {
public:
  TestObElectionMgr()
  {}
  ~TestObElectionMgr()
  {
    LIB_LOG(INFO, "TestObElectionMgr destroy finished");
  }
  virtual void SetUp()
  {
    init();
  }
  virtual void TearDown()
  {}

public:
  void init();

public:
  ObAddr self_;
  obrpc::ObBatchRpc batch_rpc_;
  MockEGPriorityGetter eg_cb_;
  int64_t replica_num_;
};

class MyObElectionCallback : public ObIElectionCallback {
public:
  MyObElectionCallback()
  {}
  ~MyObElectionCallback()
  {}
  int on_get_election_priority(ObElectionPriority& priority)
  {
    UNUSED(priority);
    return OB_SUCCESS;
  }
  int on_election_role_change()
  {
    return OB_SUCCESS;
  }
  int on_change_leader_retry(const common::ObAddr&, common::ObTsWindows&)
  {
    return OB_SUCCESS;
  }
};

void TestObElectionMgr::init()
{
  EXPECT_EQ(true, self_.set_ip_addr("127.0.0.1", 8021));
  replica_num_ = 3;
  LIB_LOG(INFO, "TestObElectionMgr init finished");
}

TEST_F(TestObElectionMgr, init_two_and_stop)
{
  EXPECT_EQ(OB_SUCCESS, mgr_.init(self_, &batch_rpc_, &eg_cb_));
  EXPECT_EQ(OB_INIT_TWICE, mgr_.init(self_, &batch_rpc_, &eg_cb_));
  EXPECT_EQ(OB_SUCCESS, mgr_.start());
  EXPECT_EQ(OB_SUCCESS, mgr_.stop());

  EXPECT_EQ(OB_SUCCESS, mgr_.stop());
}

TEST_F(TestObElectionMgr, add_partition)
{
  LIB_LOG(INFO, "begin test add_partition");
  ObIElection* unused = NULL;
  EXPECT_EQ(OB_SUCCESS, mgr_.start());

  int32_t max_part_cnt = 10;
  for (int32_t i = 1; i <= max_part_cnt; i++) {
    ObPartitionKey pkey(combine_id(tenant_id_, i), i, max_part_cnt);
    LIB_LOG(INFO, "extract tenant_id", K(pkey), "tenant_id", pkey.get_tenant_id());
    MyObElectionCallback cb;
    EXPECT_EQ(OB_SUCCESS, mgr_.add_partition(pkey, replica_num_, &cb, unused));
    EXPECT_EQ(OB_SUCCESS, mgr_.remove_partition(pkey));
  }
  LIB_LOG(INFO, "finished test add_partition");
}

TEST_F(TestObElectionMgr, set_and_get_candidate)
{
  ObMemberList mlist;
  ObAddr addr1, addr2, addr3, addr4;
  ObPartitionKey pkey(++table_id_, 1, 1);
  MyObElectionCallback cb;
  ObIElection* unused = NULL;

  EXPECT_EQ(OB_SUCCESS, mgr_.add_partition(pkey, replica_num_, &cb, unused));
  for (int i = 0; i < oceanbase::common::OB_MAX_MEMBER_NUMBER; i++) {
    ObAddr addr;
    EXPECT_EQ(true, addr.set_ip_addr("127.0.0.1", 8201 + i));
    ObMember member1(addr, 1);
    EXPECT_EQ(OB_SUCCESS, mlist.add_member(member1));
  }

  ObAddr addr;
  EXPECT_EQ(true, addr.set_ip_addr("127.0.0.1", 8201 + oceanbase::common::OB_MAX_MEMBER_NUMBER));
  ObMember member1(addr, 1);
  EXPECT_EQ(OB_SIZE_OVERFLOW, mlist.add_member(member1));
  EXPECT_EQ(OB_SUCCESS, mgr_.set_candidate(pkey, mlist.get_member_number(), mlist, ++membership_version));

  ObMemberList curr_mlist;
  EXPECT_EQ(OB_SUCCESS, mgr_.get_curr_candidate(pkey, curr_mlist));
  EXPECT_EQ(oceanbase::common::OB_MAX_MEMBER_NUMBER, curr_mlist.get_member_number());

  EXPECT_EQ(OB_SUCCESS, mgr_.remove_partition(pkey));
  EXPECT_EQ(OB_PARTITION_NOT_EXIST, mgr_.get_curr_candidate(pkey, curr_mlist));
}

TEST_F(TestObElectionMgr, start_partition)
{
  ObMemberList mlist;
  ObAddr leader;
  ObPartitionKey pkey(++table_id_, 1, 1);
  MyObElectionCallback cb;
  ObIElection* unused = NULL;

  EXPECT_EQ(true, leader.set_ip_addr("127.0.0.1", 8201));
  ObMember member1(leader, 1);
  EXPECT_EQ(OB_SUCCESS, mlist.add_member(member1));

  EXPECT_EQ(OB_SUCCESS, mgr_.add_partition(pkey, replica_num_, &cb, unused));
  EXPECT_EQ(OB_SUCCESS, mgr_.set_candidate(pkey, replica_num_, mlist, ++membership_version));
  EXPECT_EQ(OB_SUCCESS, mgr_.start_partition(pkey));
}

TEST_F(TestObElectionMgr, start_partition_with_leader)
{
  ObMemberList mlist;
  ObAddr leader;
  ObPartitionKey pkey(++table_id_, 1, 1);
  MyObElectionCallback cb;
  int64_t leader_epoch;
  ObIElection* unused = NULL;

  EXPECT_EQ(true, leader.set_ip_addr("127.0.0.1", 8201));
  ObMember member1(leader, 1);
  EXPECT_EQ(OB_SUCCESS, mlist.add_member(member1));

  EXPECT_EQ(OB_SUCCESS, mgr_.add_partition(pkey, replica_num_, &cb, unused));
  EXPECT_EQ(OB_SUCCESS, mgr_.set_candidate(pkey, replica_num_, mlist, ++membership_version));
  EXPECT_EQ(OB_SUCCESS, mgr_.start_partition(pkey, leader, ObClockGenerator::getClock(), leader_epoch));
}

TEST_F(TestObElectionMgr, remove_partition)
{
  ObMemberList mlist;
  ObAddr leader;
  ObPartitionKey pkey(++table_id_, 1, 1);
  MyObElectionCallback cb;
  ObIElection* unused = NULL;
  ObIElection* election1 = NULL;
  ObIElection* election2 = NULL;

  EXPECT_EQ(true, leader.set_ip_addr("127.0.0.1", 8201));
  ObMember member1(leader, 1);
  EXPECT_EQ(OB_SUCCESS, mlist.add_member(member1));
  EXPECT_EQ(OB_SUCCESS, mgr_.remove_partition(pkey));
  EXPECT_EQ(OB_SUCCESS, mgr_.add_partition(pkey, replica_num_, &cb, election1));
  EXPECT_EQ(OB_SUCCESS, mgr_.set_candidate(pkey, replica_num_, mlist, ++membership_version));
  EXPECT_EQ(OB_SUCCESS, mgr_.start_partition(pkey));
  EXPECT_NE(OB_SUCCESS, mgr_.add_partition(pkey, replica_num_, &cb, unused));
  EXPECT_EQ(OB_SUCCESS, mgr_.remove_partition(pkey));
  EXPECT_EQ(OB_SUCCESS, mgr_.add_partition(pkey, replica_num_, &cb, election2));
  EXPECT_EQ(OB_SUCCESS, mgr_.start_partition(pkey));
  int64_t release_count_before_revert = ATOMIC_LOAD(&election::ElectionAllocHandle::TOTAL_RELEASE_COUNT);
  mgr_.revert_election(election1);
  EXPECT_EQ(release_count_before_revert + 1, ATOMIC_LOAD(&election::ElectionAllocHandle::TOTAL_RELEASE_COUNT));
  mgr_.revert_election(election2);
  EXPECT_EQ(OB_SUCCESS, mgr_.remove_partition(pkey));
  EXPECT_EQ(release_count_before_revert + 2, ATOMIC_LOAD(&election::ElectionAllocHandle::TOTAL_RELEASE_COUNT));
}

TEST_F(TestObElectionMgr, stop_partition)
{
  ObMemberList mlist;
  ObAddr leader;
  ObPartitionKey pkey(++table_id_, 1, 1);
  MyObElectionCallback cb;
  ObIElection* unused = NULL;

  EXPECT_EQ(true, leader.set_ip_addr("127.0.0.1", 8201));
  ObMember member1(leader, 1);
  EXPECT_EQ(OB_SUCCESS, mlist.add_member(member1));

  EXPECT_EQ(OB_SUCCESS, mgr_.add_partition(pkey, replica_num_, &cb, unused));
  EXPECT_EQ(OB_SUCCESS, mgr_.set_candidate(pkey, replica_num_, mlist, ++membership_version));
  EXPECT_EQ(OB_SUCCESS, mgr_.start_partition(pkey));
  EXPECT_EQ(OB_SUCCESS, mgr_.stop_partition(pkey));
}

TEST_F(TestObElectionMgr, stop_partition_with_leader)
{
  ObMemberList mlist;
  ObAddr leader;
  ObPartitionKey pkey(++table_id_, 1, 1);
  MyObElectionCallback cb;
  int64_t leader_epoch;
  ObIElection* unused = NULL;

  EXPECT_EQ(true, leader.set_ip_addr("127.0.0.1", 8201));
  ObMember member1(leader, 1);
  EXPECT_EQ(OB_SUCCESS, mlist.add_member(member1));

  EXPECT_EQ(OB_SUCCESS, mgr_.add_partition(pkey, replica_num_, &cb, unused));
  EXPECT_EQ(OB_SUCCESS, mgr_.set_candidate(pkey, replica_num_, mlist, ++membership_version));
  EXPECT_EQ(OB_SUCCESS, mgr_.start_partition(pkey, leader, ObClockGenerator::getClock(), leader_epoch));
  EXPECT_EQ(OB_SUCCESS, mgr_.stop_partition(pkey));
}

TEST_F(TestObElectionMgr, inc_replica_num)
{
  ObMemberList mlist;
  ObAddr leader;
  ObPartitionKey pkey(++table_id_, 1, 1);
  MyObElectionCallback cb;
  int64_t leader_epoch;
  ObIElection* unused = NULL;

  EXPECT_EQ(true, leader.set_ip_addr("127.0.0.1", 8201));
  ObMember member1(leader, 1);
  EXPECT_EQ(OB_SUCCESS, mlist.add_member(member1));

  EXPECT_EQ(OB_SUCCESS, mgr_.add_partition(pkey, replica_num_, &cb, unused));
  EXPECT_EQ(OB_SUCCESS, mgr_.set_candidate(pkey, replica_num_, mlist, ++membership_version));
  EXPECT_EQ(OB_SUCCESS, mgr_.start_partition(pkey, leader, ObClockGenerator::getClock(), leader_epoch));
  EXPECT_EQ(OB_SUCCESS, mgr_.dec_replica_num(pkey));
  EXPECT_EQ(OB_SUCCESS, mgr_.dec_replica_num(pkey));
  EXPECT_EQ(OB_ERR_UNEXPECTED, mgr_.dec_replica_num(pkey));
  EXPECT_EQ(OB_ERR_UNEXPECTED, mgr_.dec_replica_num(pkey));
  EXPECT_EQ(OB_SUCCESS, mgr_.inc_replica_num(pkey));
  EXPECT_EQ(OB_SUCCESS, mgr_.inc_replica_num(pkey));
  EXPECT_EQ(OB_SUCCESS, mgr_.inc_replica_num(pkey));
  EXPECT_EQ(OB_SUCCESS, mgr_.inc_replica_num(pkey));
  EXPECT_EQ(OB_SUCCESS, mgr_.inc_replica_num(pkey));
  EXPECT_EQ(OB_SUCCESS, mgr_.inc_replica_num(pkey));
  EXPECT_EQ(OB_ERR_UNEXPECTED, mgr_.inc_replica_num(pkey));
  EXPECT_EQ(OB_ERR_UNEXPECTED, mgr_.inc_replica_num(pkey));
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  int ret = -1;

  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_election_mgr_lib.log", true, true);
  oceanbase::election::ObAsyncLog::getLogger().init("test_election_mgr.log", OB_LOG_LEVEL_INFO, true);
  EXPECT_EQ(OB_SUCCESS, ObTenantMutilAllocatorMgr::get_instance().init());

  if (OB_FAIL(oceanbase::common::ObClockGenerator::init())) {
    LIB_LOG(WARN, "clock generator init error.", K(ret));
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  (void)oceanbase::common::ObClockGenerator::destroy();

  return ret;
}
