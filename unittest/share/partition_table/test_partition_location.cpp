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
#include "share/partition_table/ob_partition_location.h"

namespace oceanbase {
namespace share {
using namespace common;

TEST(ObPartitionLocation, common)
{
  ObPartitionLocation location;
  location.set_table_id(combine_id(1, 1));
  location.set_partition_id(0);
  location.set_partition_cnt(1);
  location.set_renew_time(ObTimeUtility::current_time());
  ObReplicaLocation replica_loc;

  ASSERT_FALSE(replica_loc.is_valid());

  const char* ip1 = "127.0.0.1";
  const char* ip2 = "127.0.0.2";
  const char* ip3 = "127.0.0.3";
  const int32_t port = 5555;

  const uint64_t table_id = 1;
  const int64_t partition_id = 1;
  const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();

  // add invalid replica location
  ASSERT_EQ(OB_INVALID_ARGUMENT, location.add(replica_loc));

  // add same replica location twice
  replica_loc.server_.set_ip_addr(ip1, port);
  ASSERT_EQ(OB_SUCCESS, location.add(replica_loc));
  ASSERT_EQ(OB_ERR_ALREADY_EXISTS, location.add(replica_loc));
  ASSERT_EQ(1, location.size());
  ASSERT_EQ(0, memcmp(&replica_loc, &location.get_replica_locations().at(0), sizeof(replica_loc)));

  // leader releated
  ObReplicaLocation tmp_replica_loc;
  ASSERT_EQ(OB_LOCATION_LEADER_NOT_EXIST, location.get_strong_leader(tmp_replica_loc));
  replica_loc.server_.set_ip_addr(ip2, port);
  replica_loc.role_ = LEADER;
  ASSERT_EQ(OB_SUCCESS, location.add(replica_loc));
  ASSERT_EQ(OB_SUCCESS, location.get_strong_leader(tmp_replica_loc));
  ASSERT_EQ(LEADER, tmp_replica_loc.role_);

  // del replica location
  ASSERT_EQ(2, location.size());
  ObAddr tmp_server;
  tmp_server.set_ip_addr(ip3, port);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, location.del(tmp_server));
  ASSERT_EQ(2, location.size());
  ObAddr leader_server;
  leader_server.set_ip_addr(ip2, port);
  ASSERT_EQ(OB_SUCCESS, location.del(leader_server));
  ASSERT_EQ(1, location.size());
  ASSERT_EQ(OB_LOCATION_LEADER_NOT_EXIST, location.get_strong_leader(tmp_replica_loc));

  // set get funcs
  location.set_table_id(table_id);
  location.set_partition_id(partition_id);
  location.set_renew_time(now);
  ASSERT_EQ(table_id, location.get_table_id());
  ASSERT_EQ(partition_id, location.get_partition_id());
  ASSERT_EQ(now, location.get_renew_time());

  // to_string and serialization/deserailization
  LOG_INFO("replica location", K(replica_loc));
  LOG_INFO("location", K(location));
  const int64_t OB_MAX_BUFFER_LEN = 1024;
  char buffer[OB_MAX_BUFFER_LEN];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, location.serialize(buffer, OB_MAX_BUFFER_LEN, pos));
  int64_t serialize_size = pos;
  ObPartitionLocation cmp_location;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, cmp_location.deserialize(buffer, serialize_size, pos));
  ASSERT_EQ(location.size(), cmp_location.size());
  for (int64_t i = 0; i < location.size(); ++i) {
    ASSERT_EQ(location.get_replica_locations().at(i).server_, cmp_location.get_replica_locations().at(i).server_);
    ASSERT_EQ(location.get_replica_locations().at(i).role_, cmp_location.get_replica_locations().at(i).role_);
    ASSERT_EQ(location.get_replica_locations().at(i).reserved_, cmp_location.get_replica_locations().at(i).reserved_);
  }
}

}  // end namespace share
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
