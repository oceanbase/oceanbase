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
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_replica_filter.h"
#include "fake_part_property_getter.h"

namespace oceanbase {
namespace share {
using namespace common;
using namespace host;

TEST(ObPartitionInfo, common)
{
  ObPartitionInfo partition;
  ASSERT_EQ(NULL, partition.get_allocator());
  ASSERT_EQ(OB_INVALID_ID, partition.get_table_id());
  ASSERT_EQ(OB_INVALID_INDEX, partition.get_partition_id());
  ModulePageAllocator allocator;
  partition.set_allocator(&allocator);
  partition.set_table_id(combine_id(1, 1));
  partition.set_partition_id(0);
  ASSERT_EQ(&allocator, partition.get_allocator());
  ASSERT_EQ(1UL, partition.get_tenant_id());
  ASSERT_EQ(combine_id(1, 1), partition.get_table_id());
  ASSERT_EQ(0, partition.get_partition_id());
  ASSERT_EQ(0, partition.replica_count());
  ASSERT_EQ(0, partition.get_replicas_v2().count());
  ObPartitionReplica replica;
  ASSERT_FALSE(replica.is_valid());
  replica.table_id_ = combine_id(1, 1);
  replica.partition_id_ = 0;
  replica.zone_ = "1";
  replica.partition_cnt_ = 1;
  ASSERT_FALSE(replica.is_valid());
  replica.server_.set_ip_addr("127.0.0.1", 80);
  ASSERT_TRUE(replica.is_valid());

  ASSERT_EQ(OB_SUCCESS, partition.add_replica(replica));
  replica.server_.set_ip_addr("127.0.0.1", 8080);
  ASSERT_EQ(OB_SUCCESS, partition.add_replica(replica));
  ASSERT_EQ(2, partition.replica_count());

  // set unit_id
  ObAddr server;
  const uint64_t unit_id = 10;
  server.set_ip_addr("127.0.0.1", 9999);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, partition.set_unit_id(server, unit_id));
  server = replica.server_;
  ASSERT_EQ(OB_SUCCESS, partition.set_unit_id(server, unit_id));
  const ObPartitionReplica* r = NULL;
  ASSERT_EQ(OB_SUCCESS, partition.find(server, r));
  ASSERT_EQ(unit_id, r->unit_id_);

  LOG_INFO("partition info", "partition", partition);
  char buf[1024 * 10];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, partition.serialize(buf, sizeof(buf), pos));
  ASSERT_GT(pos, 0);
  ASSERT_EQ(pos, partition.get_serialize_size());

  ObPartitionInfo p2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, p2.deserialize(buf, sizeof(buf), pos));
  ASSERT_GT(pos, 0);
  ASSERT_EQ(pos, partition.get_serialize_size());
}

TEST(ObPartitionInfo, add_replica)
{
  ObPartitionInfo partition;
  partition.set_table_id(combine_id(1, 1));
  partition.set_partition_id(0);
  ObPartitionReplica replica;
  replica.partition_cnt_ = 1;
  replica.table_id_ = combine_id(1, 1);
  replica.partition_id_ = 0;
  replica.zone_ = "1";
  replica.server_.set_ip_addr("127.0.0.1", 80);

  // insert
  ASSERT_EQ(OB_SUCCESS, partition.add_replica(replica));
  ASSERT_EQ(1, partition.replica_count());
  // replace
  ASSERT_EQ(OB_SUCCESS, partition.add_replica(replica));
  ASSERT_EQ(1, partition.replica_count());

  replica.zone_ = "2";
  replica.server_.set_ip_addr("8.8.8.8", 80);
  replica.data_version_ = 2;
  ASSERT_EQ(OB_SUCCESS, partition.add_replica(replica));
  ASSERT_EQ(2, partition.replica_count());

  replica.server_.set_port(8080);
  const ObPartitionReplica* r = NULL;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, partition.find(replica.server_, r));
  replica.server_.set_port(80);
  ASSERT_EQ(OB_SUCCESS, partition.find(replica.server_, r));
  ASSERT_TRUE(NULL != r);

  // not the same partition
  replica.partition_id_ = 1;
  ASSERT_NE(OB_SUCCESS, partition.add_replica(replica));

  // checksum error
  replica.partition_id_ = 0;
  replica.zone_ = "3";
  replica.server_.set_ip_addr("8.8.4.4", 80);
  replica.data_version_ = 2;
  replica.data_checksum_ = 1024;
  ASSERT_NE(OB_SUCCESS, partition.add_replica(replica));

  // pass checksum verify for different data version
  replica.data_version_ = 3;
  ASSERT_EQ(OB_SUCCESS, partition.add_replica(replica));
  ASSERT_EQ(3, partition.replica_count());
}

TEST(ObPartitionInfo, remove)
{
  ObPartitionInfo partition;
  partition.set_table_id(combine_id(1, 1));
  partition.set_partition_id(0);
  ObPartitionReplica replica;
  replica.table_id_ = combine_id(1, 1);
  replica.partition_id_ = 0;
  replica.zone_ = "1";
  replica.partition_cnt_ = 1;
  replica.server_.set_ip_addr("127.0.0.1", 80);

  ASSERT_EQ(OB_SUCCESS, partition.add_replica(replica));
  ASSERT_EQ(1, partition.replica_count());
  ASSERT_EQ(OB_SUCCESS, partition.remove(ObAddr(ObAddr::IPV4, "127.0.0.1", 8080)));
  ASSERT_EQ(1, partition.replica_count());
  ASSERT_EQ(OB_SUCCESS, partition.remove(ObAddr(ObAddr::IPV4, "127.0.0.1", 80)));
  ASSERT_EQ(0, partition.replica_count());
}

TEST(ObPartitionInfo, find)
{
  ObAddr s1;
  ObAddr s2;

  s1.set_ip_addr("127.0.0.1", 80);
  s2.set_ip_addr("127.0.0.1", 81);

  ObPartitionInfo partition;
  partition.set_table_id(combine_id(1, 1));
  partition.set_partition_id(0);
  ObPartitionReplica replica;
  replica.table_id_ = combine_id(1, 1);
  replica.partition_id_ = 0;
  replica.data_version_ = 1;
  replica.zone_ = "1";
  replica.partition_cnt_ = 1;
  replica.server_ = s1;
  ASSERT_EQ(OB_SUCCESS, partition.add_replica(replica));
  replica.zone_ = "2";
  replica.server_ = s2;
  ASSERT_EQ(OB_SUCCESS, partition.add_replica(replica));

  const ObPartitionReplica* leader = 0;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, partition.find_leader_v2(leader));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, partition.find_latest_leader(leader));

  ASSERT_EQ(OB_SUCCESS, partition.remove(s2));
  replica.to_leader_time_ = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, partition.add_replica(replica));
  ASSERT_EQ(OB_SUCCESS, partition.find_latest_leader(leader));
  ASSERT_EQ(s2, leader->server_);

  replica.server_ = s1;
  replica.role_ = LEADER;
  replica.zone_ = "1";
  replica.to_leader_time_ = ObTimeUtility::current_time() + 1;

  ASSERT_EQ(OB_SUCCESS, partition.remove(s1));
  ASSERT_EQ(OB_SUCCESS, partition.add_replica(replica));
  ASSERT_EQ(OB_SUCCESS, partition.find_leader_v2(leader));
  ASSERT_EQ(s1, leader->server_);
}

TEST(ObPartitionInfo, filter)
{
  ObPartitionInfo info;
  info.set_table_id(combine_id(1, 1));
  info.set_partition_id(0);
  ObPartitionReplica r;
  r.table_id_ = combine_id(1, 1);
  r.partition_id_ = 0;
  r.partition_cnt_ = 1;
  r.zone_ = "zone X";
  ObReplicaFilterHolder filters;

  ASSERT_EQ(OB_SUCCESS, filters.set_zone("1"));
  ASSERT_EQ(OB_SUCCESS, filters.set_version(2));

  r.server_ = A;
  ASSERT_EQ(OB_SUCCESS, info.add_replica(r));
  r.zone_ = "1";
  r.data_version_ = 2;
  r.server_ = B;
  ASSERT_EQ(OB_SUCCESS, info.add_replica(r));
  r.zone_ = "2";
  r.server_ = C;
  ASSERT_EQ(OB_SUCCESS, info.add_replica(r));

  ASSERT_EQ(3, info.replica_count());
  ASSERT_EQ(OB_SUCCESS, info.filter(filters));
  ASSERT_EQ(1, info.replica_count());

  filters.set_server(A);
  ASSERT_EQ(OB_SUCCESS, info.filter(filters));
  ASSERT_EQ(0, info.replica_count());

  ASSERT_EQ(OB_SUCCESS, info.filter(filters));
}

TEST(ObPartitionInfo, update_replica_status)
{
  ObAddr A(ObAddr::IPV4, "127.0.0.1", 80);
  ObAddr B(ObAddr::IPV4, "127.0.0.2", 80);
  ObAddr C(ObAddr::IPV4, "127.0.0.3", 80);
  ObAddr D(ObAddr::IPV4, "127.0.0.4", 80);
  ObAddr E(ObAddr::IPV4, "127.0.0.5", 80);

  ObPartitionInfo info;
  info.set_table_id(combine_id(1, 1));
  info.set_partition_id(0);

  info.update_replica_status();

  ObPartitionReplica std;
  std.table_id_ = combine_id(1, 1);
  std.partition_id_ = 0;
  std.zone_ = "1";
  std.partition_cnt_ = 1;
  std.role_ = FOLLOWER;
  std.unit_id_ = 1;
  std.data_version_ = 2;
  ASSERT_EQ(OB_SUCCESS,
      std.member_list_.push_back(ObPartitionReplica::Member(A, ::oceanbase::common::ObTimeUtility::current_time())));
  ASSERT_EQ(OB_SUCCESS,
      std.member_list_.push_back(ObPartitionReplica::Member(B, ::oceanbase::common::ObTimeUtility::current_time())));
  ASSERT_EQ(OB_SUCCESS,
      std.member_list_.push_back(ObPartitionReplica::Member(C, ::oceanbase::common::ObTimeUtility::current_time())));

  ObPartitionReplica r;
  r.assign(std);
  r.server_ = A;
  ASSERT_EQ(OB_SUCCESS, info.add_replica(r));
  r.server_ = B;
  ASSERT_EQ(OB_SUCCESS, info.add_replica(r));
  r.server_ = C;
  ASSERT_EQ(OB_SUCCESS, info.add_replica(r));
  r.server_ = D;
  ASSERT_EQ(OB_SUCCESS, info.add_replica(r));

#define SERVER_REPLICA(info, server)               \
  (*({                                             \
    const ObPartitionReplica* __r = NULL;          \
    ASSERT_EQ(OB_SUCCESS, info.find(server, __r)); \
    const_cast<ObPartitionReplica*>(__r);          \
  }))

  // no leader
  info.update_replica_status();
  FOREACH_CNT(iter, info.get_replicas_v2())
  {
    ASSERT_EQ(REPLICA_STATUS_NORMAL, iter->replica_status_);
  }

  // has valid to_leader_time
  SERVER_REPLICA(info, B).to_leader_time_ = ObTimeUtility::current_time();
  info.update_replica_status();

  ASSERT_EQ(REPLICA_STATUS_NORMAL, SERVER_REPLICA(info, A).replica_status_);
  ASSERT_EQ(REPLICA_STATUS_NORMAL, SERVER_REPLICA(info, B).replica_status_);
  ASSERT_EQ(REPLICA_STATUS_NORMAL, SERVER_REPLICA(info, C).replica_status_);
  ASSERT_EQ(REPLICA_STATUS_NORMAL, SERVER_REPLICA(info, D).replica_status_);

  // has leader replica
  SERVER_REPLICA(info, B).to_leader_time_ = 0;
  SERVER_REPLICA(info, C).role_ = LEADER;
  SERVER_REPLICA(info, D).replica_status_ = REPLICA_STATUS_NORMAL;
  info.update_replica_status();

  ASSERT_EQ(REPLICA_STATUS_NORMAL, SERVER_REPLICA(info, A).replica_status_);
  ASSERT_EQ(REPLICA_STATUS_NORMAL, SERVER_REPLICA(info, B).replica_status_);
  ASSERT_EQ(REPLICA_STATUS_NORMAL, SERVER_REPLICA(info, C).replica_status_);
  ASSERT_EQ(REPLICA_STATUS_OFFLINE, SERVER_REPLICA(info, D).replica_status_);

  SERVER_REPLICA(info, B).data_version_ = -1;
  info.update_replica_status();

  ASSERT_EQ(REPLICA_STATUS_FLAG, SERVER_REPLICA(info, B).replica_status_);
  ASSERT_EQ(REPLICA_STATUS_NORMAL, SERVER_REPLICA(info, C).replica_status_);
  ASSERT_EQ(REPLICA_STATUS_OFFLINE, SERVER_REPLICA(info, D).replica_status_);
}

TEST(ObPpartionReplica, member_list2text)
{
  ObAddr A(ObAddr::IPV4, "127.0.0.1", 80);
  ObAddr B(ObAddr::IPV4, "127.0.0.2", 80);
  ObAddr C(ObAddr::IPV4, "127.0.0.3", 80);

  ObPartitionReplica::MemberList member_list1;
  ObPartitionReplica::MemberList member_list2;

  ASSERT_EQ(OB_SUCCESS, member_list1.push_back(ObPartitionReplica::Member(A, 1)));
  ASSERT_EQ(OB_SUCCESS, member_list1.push_back(ObPartitionReplica::Member(B, 3)));
  ASSERT_EQ(OB_SUCCESS, member_list1.push_back(ObPartitionReplica::Member(B, 0)));
  char buf[1024];
  ASSERT_EQ(OB_SUCCESS, ObPartitionReplica::member_list2text(member_list1, buf, sizeof(buf)));
  LOG_INFO("member_list_text", K(buf));
  ASSERT_EQ(OB_SUCCESS, ObPartitionReplica::text2member_list(buf, member_list2));

  ASSERT_EQ(member_list1.count(), member_list2.count());
  for (int64_t i = 0; i < member_list1.count(); ++i) {
    ASSERT_EQ(member_list1.at(i), member_list2.at(i));
  }
}

TEST(ObPartitionReplica, fail_msg)
{
  int64_t timeout = 5;
  int64_t current = 10;
  // test failmsg:add,remove,get,get_last
  ObPartitionReplica::FailMsg fm;
  // case 1 : 10 elements
  for (int64_t i = 0; i < 10; ++i) {
    ASSERT_EQ(fm.add_failed_timestamp(i), OB_SUCCESS);
    // 0,1,2,3,4,5,6,7,8,9
  }
  for (int64_t i = 0; i < 10; ++i) {
    ASSERT_EQ(i, fm.get_failed_timestamp(i));
  }
  ASSERT_EQ(10, fm.get_failed_count());
  ASSERT_EQ(9, fm.get_last_fail_time());
  ASSERT_EQ(fm.remove_useless_timestamp(current, timeout), OB_SUCCESS);
  // 6,7,8,9
  ASSERT_EQ(6, fm.start_pos_);
  ASSERT_EQ(4, fm.get_failed_count());
  ASSERT_EQ(9, fm.get_last_fail_time());
  int64_t start = 6;
  for (int64_t i = 0; i < fm.count_; ++i) {
    ASSERT_EQ(start + i, fm.get_failed_timestamp(i));
  }

  // case2 : additional 20 elements
  for (int64_t i = 10; i < 30; ++i) {
    ASSERT_EQ(fm.add_failed_timestamp(i), OB_SUCCESS);
    // 14->29
  }
  ASSERT_EQ(14, fm.start_pos_);
  ASSERT_EQ(16, fm.get_failed_count());
  ASSERT_EQ(29, fm.get_last_fail_time());
  start = 14;
  for (int64_t i = 0; i < 16; ++i) {
    ASSERT_EQ(start + i, fm.get_failed_timestamp(i));
  }
  current = 30;
  ASSERT_EQ(fm.remove_useless_timestamp(current, timeout), OB_SUCCESS);
  // 16,17,18,19
  ASSERT_EQ(10, fm.start_pos_);
  ASSERT_EQ(4, fm.get_failed_count());
  ASSERT_EQ(29, fm.get_last_fail_time());
  start = 26;
  for (int64_t i = 0; i < fm.count_; ++i) {
    ASSERT_EQ(start + i, fm.get_failed_timestamp(i));
  }

  // case 3 remove all elements
  current = 50;
  ASSERT_EQ(fm.remove_useless_timestamp(current, timeout), OB_SUCCESS);
  ASSERT_EQ(14, fm.start_pos_);
  ASSERT_EQ(0, fm.get_failed_count());
  ASSERT_EQ(-1, fm.get_last_fail_time());

  // case 4: add additional 50 elements
  for (int64_t i = 0; i < 50; ++i) {
    ASSERT_EQ(fm.add_failed_timestamp(i), OB_SUCCESS);
    // 34->49
  }
  ASSERT_EQ(0, fm.start_pos_);
  ASSERT_EQ(16, fm.get_failed_count());
  ASSERT_EQ(49, fm.get_last_fail_time());
  start = 34;
  for (int64_t i = 0; i < 16; ++i) {
    ASSERT_EQ(start + i, fm.get_failed_timestamp(i));
  }
  // delete nothing
  current = 30;
  fm.remove_useless_timestamp(current, timeout);
  // 34->49
  ASSERT_EQ(0, fm.start_pos_);
  ASSERT_EQ(16, fm.get_failed_count());
  ASSERT_EQ(49, fm.get_last_fail_time());
  start = 34;
  for (int64_t i = 0; i < 16; ++i) {
    ASSERT_EQ(start + i, fm.get_failed_timestamp(i));
  }
}

TEST(ObPartitionReplica, fail_ms1)
{
  ObPartitionReplica::FailMsg fm;
  for (int64_t i = 0; i < 16; ++i) {
    ASSERT_EQ(fm.add_failed_timestamp(i), OB_SUCCESS);
    // 0,1,2,3,4,5,6,7,8,9
  }
  LOG_ERROR("test", K(fm));
  ASSERT_EQ(0, fm.last_fail_timestamp_[0]);
  ASSERT_EQ(16, fm.get_failed_count());
  ASSERT_EQ(15, fm.get_last_fail_time());
  ASSERT_EQ(0, fm.start_pos_);
  for (int64_t i = 0; i < 16; ++i) {
    ASSERT_EQ(i, fm.get_failed_timestamp(i));
  }

  fm.start_pos_ = -1;
  for (int64_t i = 0; i < 16; ++i) {
    ASSERT_EQ(-1, fm.get_failed_timestamp(i));
  }
  ASSERT_EQ(OB_ERR_UNEXPECTED, fm.add_failed_timestamp(16));
  ASSERT_EQ(OB_ERR_UNEXPECTED, fm.remove_useless_timestamp(1, 1));
  fm.start_pos_ = 17;
  for (int64_t i = 0; i < 16; ++i) {
    ASSERT_EQ(-1, fm.get_failed_timestamp(i));
  }
  ASSERT_EQ(OB_ERR_UNEXPECTED, fm.add_failed_timestamp(16));
  ASSERT_EQ(OB_ERR_UNEXPECTED, fm.remove_useless_timestamp(1, 1));
  fm.start_pos_ = 15;
  ASSERT_EQ(14, fm.get_failed_timestamp(fm.start_pos_));
  fm.start_pos_ = 16;
  ASSERT_EQ(-1, fm.get_failed_timestamp(0));
  fm.count_ = 0;
  ASSERT_EQ(-1, fm.get_failed_timestamp(0));
  fm.start_pos_ = 15;
  ASSERT_EQ(-1, fm.get_failed_timestamp(0));
  fm.count_ = 1;
  ASSERT_EQ(15, fm.get_failed_timestamp(0));
  ASSERT_EQ(OB_SUCCESS, fm.add_failed_timestamp(13));
  ASSERT_EQ(15, fm.get_failed_timestamp(0));
  ASSERT_EQ(13, fm.get_failed_timestamp(1));
  fm.start_pos_ = 0;
  ASSERT_EQ(OB_SUCCESS, fm.add_failed_timestamp(15));
  ASSERT_EQ(13, fm.get_failed_timestamp(0));
  ASSERT_EQ(1, fm.get_failed_timestamp(1));
  ASSERT_EQ(15, fm.get_failed_timestamp(2));

  fm.count_ = 16;
  ASSERT_EQ(OB_SUCCESS, fm.add_failed_timestamp(15));
  ASSERT_EQ(1, fm.start_pos_);
  ASSERT_EQ(16, fm.get_failed_count());
  fm.count_ = 0;
  ASSERT_EQ(OB_SUCCESS, fm.add_failed_timestamp(-1));
  ASSERT_EQ(1, fm.start_pos_);
  ASSERT_EQ(1, fm.get_failed_count());
}

TEST(ObPartitionReplica, fail_ms2)
{
  ObPartitionReplica::FailMsg fm;
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(fm.add_failed_timestamp(i), OB_SUCCESS);
    // 0,1,2,3,4,5
  }
  fm.task_type_ = 0;
  int64_t length = 0;
  {
    char buf[1024] = "";
    ObPartitionReplica::FailList faillist;
    ASSERT_EQ(OB_INVALID_ARGUMENT, ObPartitionReplica::fail_list2text(faillist, buf, length));
    length = 1;
    ASSERT_EQ(OB_SUCCESS, ObPartitionReplica::fail_list2text(faillist, buf, length));
    ASSERT_STREQ("", buf);
    ASSERT_EQ(OB_SUCCESS, faillist.push_back(fm));
    ASSERT_EQ(OB_SUCCESS, faillist.push_back(fm));
    ASSERT_EQ(OB_SUCCESS, faillist.push_back(fm));
    ASSERT_EQ(OB_SIZE_OVERFLOW, ObPartitionReplica::fail_list2text(faillist, buf, length));
    length = 800;
    ASSERT_EQ(OB_SUCCESS, ObPartitionReplica::fail_list2text(faillist, buf, length));
    ASSERT_STREQ("0.0.0.0:0,0,5,0,1,2,3,4;0.0.0.0:0,0,5,0,1,2,3,4;0.0.0.0:0,0,5,0,1,2,3,4", buf);
  }
  fm.start_pos_ = 3;
  fm.count_ = 2;
  {
    char buf[1024] = "";
    ObPartitionReplica::FailList faillist;
    length = 1;
    ASSERT_EQ(OB_SUCCESS, ObPartitionReplica::fail_list2text(faillist, buf, length));
    ASSERT_STREQ("", buf);
    ASSERT_EQ(OB_SUCCESS, faillist.push_back(fm));
    ASSERT_EQ(OB_SUCCESS, faillist.push_back(fm));
    ASSERT_EQ(OB_SUCCESS, faillist.push_back(fm));
    ASSERT_EQ(OB_SIZE_OVERFLOW, ObPartitionReplica::fail_list2text(faillist, buf, length));
    length = 800;
    ASSERT_EQ(OB_SUCCESS, ObPartitionReplica::fail_list2text(faillist, buf, length));
    ASSERT_STREQ("0.0.0.0:0,0,2,3,4;0.0.0.0:0,0,2,3,4;0.0.0.0:0,0,2,3,4", buf);
  }
  {
    char buf[] = "1.1.1.1:1,1,1,2;1.2.3.4:1,4,4,34,13,13,12";
    ObPartitionReplica::FailList faillist;
    ASSERT_EQ(OB_SUCCESS, ObPartitionReplica::text2fail_list(buf, faillist));
    ASSERT_EQ(34, faillist.at(1).get_failed_timestamp(0));
    ASSERT_EQ(13, faillist.at(1).get_failed_timestamp(1));
    ASSERT_EQ(13, faillist.at(1).get_failed_timestamp(2));
    ASSERT_EQ(12, faillist.at(1).get_failed_timestamp(3));
  }
  {
    char buf[] = "1.1.1.1:1,1,1,2;1.2.3.4:2,4,4,34,37,45,98";
    ObPartitionReplica::FailList faillist;
    ASSERT_EQ(OB_SUCCESS, ObPartitionReplica::text2fail_list(buf, faillist));
    LOG_INFO("faillist", K(faillist));
    ASSERT_EQ(2, faillist.count());
    ASSERT_EQ(1, faillist.at(0).count_);
    ASSERT_EQ(0, faillist.at(0).start_pos_);
    ASSERT_EQ(-1, faillist.at(0).get_failed_timestamp(1));
    ASSERT_EQ(34, faillist.at(1).get_failed_timestamp(0));
    ASSERT_EQ(4, faillist.at(1).count_);
    ASSERT_EQ(0, faillist.at(1).start_pos_);
    ASSERT_EQ(37, faillist.at(1).get_failed_timestamp(1));
    ASSERT_EQ(98, faillist.at(1).get_failed_timestamp(3));
  }
}

}  // end namespace share
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
