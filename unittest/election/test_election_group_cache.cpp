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

#include "election/ob_election_group_mgr.h"
#include "election/ob_election_group_cache.h"
#include "election/ob_election.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include <gtest/gtest.h>
#include <iostream>
#include <vector>

namespace oceanbase {
namespace unittest {
using namespace election;
using namespace common;
using namespace std;

class TestObElectionGroupCache : public ::testing::Test {
public:
  const vector<ObAddr> ADDR = {ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 34500),
      ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 34501),
      ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 34502),
      ObAddr(ObAddr::VER::IPV4, "127.0.0.4", 34503),
      ObAddr(ObAddr::VER::IPV4, "127.0.0.5", 34504),
      ObAddr(ObAddr::VER::IPV4, "127.0.0.6", 34505),
      ObAddr(ObAddr::VER::IPV4, "127.0.0.7", 34506),
      ObAddr(ObAddr::VER::IPV4, "127.0.0.8", 34507)};

public:
  TestObElectionGroupCache()
  {}
  ~TestObElectionGroupCache()
  {}

  virtual void SetUp()
  {
    init();
  }
  virtual void TearDown()
  {}

  void init();
  void reset();
  void fill_partition_into_group(ObElectionGroupId eg_id);
  ObElectionGroupMgr mgr_;
  ObElectionGroupCache* election_group_cache_;
  ObTimeWheel tw_;
  vector<ObPartitionKey> filled_pkey_list;
};

void TestObElectionGroupCache::init()
{
  for (int i = 0; i < MAX_EG_PARTITION_NUM; ++i) {
    filled_pkey_list.push_back(ObPartitionKey(i, i, i));
  }
  tw_.init(1000, 1, "111");
  tw_.start();
  ::oceanbase::common::ObTenantMutilAllocatorMgr::get_instance().init();
  ASSERT_EQ(OB_SUCCESS,
      mgr_.init(ADDR[0],
          reinterpret_cast<ObIElectionRpc*>(0x01),
          &tw_,
          reinterpret_cast<ObIElectionMgr*>(0x01),
          reinterpret_cast<ObIElectionGroupPriorityGetter*>(0x01)));
  mgr_.start();
  ASSERT_EQ(OB_SUCCESS, mgr_.get_election_group_cache(election_group_cache_));
}

void TestObElectionGroupCache::fill_partition_into_group(ObElectionGroupId eg_id)
{
  int64_t not_used = 0;
  auto group = mgr_.get_election_group(eg_id);
  for (int i = 0; i < MAX_EG_PARTITION_NUM; ++i) {
    ASSERT_EQ(OB_SUCCESS,
        group->move_in_partition(filled_pkey_list[i], lease_t(1, std::numeric_limits<int64_t>::max()), 1, not_used));
  }
  mgr_.revert_election_group(group);
}

TEST_F(TestObElectionGroupCache, normal_test)
{
  uint64_t tenant_id = 1;

  ASSERT_EQ(0, election_group_cache_->get_group_num());
  ASSERT_EQ(0, election_group_cache_->get_queue_num());
  ASSERT_EQ(0, election_group_cache_->get_node_num());

  common::ObMemberList member;
  member.add_member(ObMember(ADDR[0], 1));
  member.add_member(ObMember(ADDR[1], 2));
  member.add_member(ObMember(ADDR[2], 3));
  ObElectionGroupKey key1(tenant_id, ADDR[0], 3, member);
  ObElectionGroupId eg_id1;
  ASSERT_EQ(OB_SUCCESS, election_group_cache_->get_or_create_eg_id(key1, eg_id1));

  ASSERT_EQ(true, eg_id1.is_valid());
  ASSERT_EQ(1, election_group_cache_->get_group_num());
  ASSERT_EQ(1, election_group_cache_->get_queue_num());
  ASSERT_EQ(1, election_group_cache_->get_node_num());

  ASSERT_EQ(OB_SUCCESS, election_group_cache_->get_or_create_eg_id(key1, eg_id1));

  ASSERT_EQ(true, eg_id1.is_valid());
  ASSERT_EQ(1, election_group_cache_->get_group_num());
  ASSERT_EQ(1, election_group_cache_->get_queue_num());
  ASSERT_EQ(1, election_group_cache_->get_node_num());

  common::ObMemberList member2;
  member2.add_member(ObMember(ADDR[2], 1));
  member2.add_member(ObMember(ADDR[0], 2));
  member2.add_member(ObMember(ADDR[1], 4));
  ObElectionGroupKey key2(tenant_id, ADDR[0], 3, member2);
  ObElectionGroupId eg_id2;
  ASSERT_EQ(OB_SUCCESS, election_group_cache_->get_or_create_eg_id(key2, eg_id2));

  ASSERT_EQ(eg_id1, eg_id2);
  ASSERT_EQ(1, election_group_cache_->get_group_num());
  ASSERT_EQ(1, election_group_cache_->get_queue_num());
  ASSERT_EQ(1, election_group_cache_->get_node_num());

  common::ObMemberList member3;
  member3.add_member(ObMember(ADDR[0], 1));
  member3.add_member(ObMember(ADDR[3], 2));
  member3.add_member(ObMember(ADDR[1], 4));
  ObElectionGroupKey key3(tenant_id, ADDR[0], 3, member3);
  ObElectionGroupId eg_id3;
  ASSERT_EQ(OB_SUCCESS, election_group_cache_->get_or_create_eg_id(key3, eg_id3));

  ASSERT_NE(eg_id2, eg_id3);
  ASSERT_EQ(2, election_group_cache_->get_group_num());
  ASSERT_EQ(2, election_group_cache_->get_queue_num());
  ASSERT_EQ(2, election_group_cache_->get_node_num());

  ASSERT_EQ(OB_SUCCESS, election_group_cache_->put_eg_id(key3, eg_id3));
  ASSERT_EQ(OB_SUCCESS, election_group_cache_->put_eg_id(key3, eg_id3));
  ObElectionGroupId eg_id4;
  ASSERT_EQ(OB_SUCCESS, election_group_cache_->get_or_create_eg_id(key3, eg_id4));

  ASSERT_EQ(eg_id3, eg_id4);
  ASSERT_EQ(2, election_group_cache_->get_group_num());
  ASSERT_EQ(2, election_group_cache_->get_queue_num());
  ASSERT_EQ(4, election_group_cache_->get_node_num());

  election_group_cache_->gc_invalid_queue();

  ASSERT_EQ(2, election_group_cache_->get_group_num());
  ASSERT_EQ(2, election_group_cache_->get_queue_num());
  ASSERT_EQ(4, election_group_cache_->get_node_num());

  fill_partition_into_group(eg_id1);

  ObElectionGroupId eg_id5;
  ASSERT_EQ(OB_SUCCESS, election_group_cache_->get_or_create_eg_id(key1, eg_id5));
  ASSERT_NE(eg_id1, eg_id5);

  ASSERT_EQ(3, election_group_cache_->get_group_num());
  ASSERT_EQ(2, election_group_cache_->get_queue_num());
  ASSERT_EQ(4, election_group_cache_->get_node_num());

  fill_partition_into_group(eg_id3);

  ObElectionGroupId eg_id6;
  ASSERT_EQ(OB_SUCCESS, election_group_cache_->get_or_create_eg_id(key3, eg_id6));
  ASSERT_NE(eg_id3, eg_id6);

  ASSERT_EQ(4, election_group_cache_->get_group_num());
  ASSERT_EQ(2, election_group_cache_->get_queue_num());
  ASSERT_EQ(2, election_group_cache_->get_node_num());

  ObIElectionGroup* group;

  group = mgr_.get_election_group(eg_id5);
  fill_partition_into_group(eg_id5);
  mgr_.revert_election_group(group);

  // gc
  election_group_cache_->gc_invalid_queue();

  ASSERT_EQ(4, election_group_cache_->get_group_num());
  ASSERT_EQ(1, election_group_cache_->get_queue_num());
  ASSERT_EQ(1, election_group_cache_->get_node_num());

  group = mgr_.get_election_group(eg_id6);
  fill_partition_into_group(eg_id6);
  mgr_.revert_election_group(group);
  election_group_cache_->gc_invalid_queue();
  ASSERT_EQ(4, election_group_cache_->get_group_num());
  ASSERT_EQ(0, election_group_cache_->get_queue_num());
  ASSERT_EQ(0, election_group_cache_->get_node_num());
}

TEST_F(TestObElectionGroupCache, put_egid)
{

  uint64_t tenant_id1 = 1;
  common::ObMemberList member1;
  member1.add_member(ObMember(ADDR[0], 1));
  member1.add_member(ObMember(ADDR[1], 2));
  member1.add_member(ObMember(ADDR[2], 3));
  ObElectionGroupKey key1(tenant_id1, ADDR[0], 3, member1);
  ObElectionGroupId eg_id1;
  ASSERT_EQ(OB_SUCCESS, election_group_cache_->get_or_create_eg_id(key1, eg_id1));

  ASSERT_EQ(true, eg_id1.is_valid());
  ASSERT_EQ(1, election_group_cache_->get_group_num());
  ASSERT_EQ(1, election_group_cache_->get_queue_num());
  ASSERT_EQ(1, election_group_cache_->get_node_num());

  ASSERT_EQ(OB_SUCCESS, election_group_cache_->put_eg_id(key1, eg_id1));
  ASSERT_EQ(OB_SUCCESS, election_group_cache_->put_eg_id(key1, eg_id1));

  ASSERT_EQ(1, election_group_cache_->get_group_num());
  ASSERT_EQ(1, election_group_cache_->get_queue_num());
  ASSERT_EQ(3, election_group_cache_->get_node_num());

  ObIElectionGroup* group;

  group = mgr_.get_election_group(eg_id1);
  fill_partition_into_group(eg_id1);
  mgr_.revert_election_group(group);
  election_group_cache_->gc_invalid_queue();

  ASSERT_EQ(1, election_group_cache_->get_group_num());
  ASSERT_EQ(0, election_group_cache_->get_queue_num());
  ASSERT_EQ(0, election_group_cache_->get_node_num());

  ASSERT_EQ(OB_SUCCESS, election_group_cache_->put_eg_id(key1, eg_id1));
  ASSERT_EQ(OB_SUCCESS, election_group_cache_->put_eg_id(key1, eg_id1));

  ASSERT_EQ(1, election_group_cache_->get_group_num());
  ASSERT_EQ(1, election_group_cache_->get_queue_num());
  ASSERT_EQ(2, election_group_cache_->get_node_num());

  ObElectionGroupId eg_id2;
  ASSERT_EQ(OB_SUCCESS, election_group_cache_->get_or_create_eg_id(key1, eg_id2));

  ASSERT_EQ(2, election_group_cache_->get_group_num());
  ASSERT_EQ(1, election_group_cache_->get_queue_num());
  ASSERT_EQ(1, election_group_cache_->get_node_num());

  ASSERT_NE(eg_id1, eg_id2);
}

TEST_F(TestObElectionGroupCache, defferent_key)
{

  uint64_t tenant_id1 = 1;
  common::ObMemberList member1;
  member1.add_member(ObMember(ADDR[0], 1));
  member1.add_member(ObMember(ADDR[1], 2));
  member1.add_member(ObMember(ADDR[2], 3));
  ObElectionGroupKey key1(tenant_id1, ADDR[0], 3, member1);
  ObElectionGroupId eg_id1;
  ASSERT_EQ(OB_SUCCESS, election_group_cache_->get_or_create_eg_id(key1, eg_id1));

  uint64_t tenant_id2 = 1;
  common::ObMemberList member2;
  member2.add_member(ObMember(ADDR[2], 1879));
  member2.add_member(ObMember(ADDR[0], 3454));
  member2.add_member(ObMember(ADDR[1], 6345654));
  ObElectionGroupKey key2(tenant_id2, ADDR[0], 3, member2);
  ObElectionGroupId eg_id2;
  ASSERT_EQ(OB_SUCCESS, election_group_cache_->get_or_create_eg_id(key2, eg_id2));

  ASSERT_EQ(eg_id1, eg_id2);
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  int ret = -1;

  oceanbase::election::ASYNC_LOG_INIT("test_election_groupcache.log", OB_LOG_LEVEL_INFO, true);

  if (OB_FAIL(oceanbase::common::ObClockGenerator::init())) {
    ELECT_LOG(WARN, "clock generator init error.", K(ret));
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }

  oceanbase::election::ASYNC_LOG_DESTROY();
  (void)oceanbase::common::ObClockGenerator::destroy();

  return ret;
}
