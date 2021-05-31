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

#include "clog/ob_log_define.h"
#include "common/ob_member_list.h"

#include <gtest/gtest.h>

namespace oceanbase {
using namespace common;
namespace unittest {
// TEST(ob_member_list_test, test1)
//{
//  const int64_t MEMBER_NUMBER = 3;
//  const int64_t TIMESTAMP = 1;
//  const int32_t BEGIN_PORT = 3000;
//  const int64_t BUF_LEN = 1000;
//
//  ObMemberList ob_member_list;
//  ob_member_list.reset();
//  common::ObAddr tmp_server;
//
//  tmp_server.set_ip_addr("127.0.0.1", BEGIN_PORT);
//  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, ob_member_list.remove_member(tmp_server));
//  for (int64_t i = 0; i < clog::OB_MAX_MEMBER_NUMBER; ++i) {
//    tmp_server.reset();
//    tmp_server.set_ip_addr("127.0.0.1", BEGIN_PORT + static_cast<int32_t>(i));
//    EXPECT_EQ(common::OB_SUCCESS, ob_member_list.add_member(tmp_server));
//  }
//  EXPECT_EQ(common::OB_SIZE_OVERFLOW, ob_member_list.add_member(tmp_server));
//  for (int64_t i = MEMBER_NUMBER; i < clog::OB_MAX_MEMBER_NUMBER; ++i) {
//    tmp_server.reset();
//    tmp_server.set_ip_addr("127.0.0.1", BEGIN_PORT + static_cast<int32_t>(i));
//    EXPECT_EQ(common::OB_SUCCESS, ob_member_list.remove_member(tmp_server));
//  }
//
//  for (int64_t i = 0; i < clog::OB_MAX_MEMBER_NUMBER; ++i) {
//    tmp_server.reset();
//    tmp_server.set_ip_addr("127.0.0.1", BEGIN_PORT + static_cast<int32_t>(i));
//    if (i < MEMBER_NUMBER) {
//      EXPECT_TRUE(ob_member_list.contains(tmp_server));
//    } else {
//      EXPECT_FALSE(ob_member_list.contains(tmp_server));
//    }
//  }
//
//  tmp_server.reset();
//  tmp_server.set_ip_addr("127.0.0.1", BEGIN_PORT + static_cast<int32_t>(0));
//  EXPECT_EQ(common::OB_ENTRY_EXIST, ob_member_list.add_member(tmp_server));
//
//  tmp_server.reset();
//  tmp_server.set_ip_addr("127.0.0.1", BEGIN_PORT + static_cast<int32_t>(MEMBER_NUMBER));
//  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, ob_member_list.remove_member(tmp_server));
//
//  tmp_server.reset();
//  tmp_server.set_ip_addr("127.0.0.1", BEGIN_PORT + static_cast<int32_t>(MEMBER_NUMBER - 2));
//  EXPECT_EQ(common::OB_SUCCESS, ob_member_list.remove_member(tmp_server));
//  EXPECT_EQ(MEMBER_NUMBER - 1, ob_member_list.get_member_number());
//
//  ob_member_list.set_mc_timestamp(TIMESTAMP);
//
//  char buf[BUF_LEN];
//  int64_t pos = 0;
//  ob_member_list.serialize(buf, BUF_LEN, pos);
//
//  ObMemberList ob_member_list2;
//  pos = 0;
//  EXPECT_EQ(common::OB_SUCCESS, ob_member_list2.deserialize(buf, BUF_LEN, pos));
//
//  ObMemberList ob_member_list3;
//  ob_member_list3.deep_copy(ob_member_list2);
//  EXPECT_EQ(33, ob_member_list3.get_serialize_size());
//  EXPECT_EQ(MEMBER_NUMBER - 1, ob_member_list3.get_member_number());
//
//  tmp_server.reset();
//  EXPECT_EQ(common::OB_SUCCESS, ob_member_list3.get_server_by_index(0, tmp_server));
//  EXPECT_STREQ("\"127.0.0.1:3000\"", tmp_server.to_cstring());
//
//  tmp_server.reset();
//  ob_member_list3.get_server_by_index(1, tmp_server);
//  EXPECT_STREQ("\"127.0.0.1:3002\"", tmp_server.to_cstring());
//
//  for (int64_t i = 2; i != clog::OB_MAX_MEMBER_NUMBER; ++i) {
//    tmp_server.reset();
//    EXPECT_EQ(common::OB_SIZE_OVERFLOW, ob_member_list3.get_server_by_index(i, tmp_server));
//    EXPECT_STREQ("\"0.0.0.0\"", tmp_server.to_cstring());
//  }
//  EXPECT_EQ(TIMESTAMP, ob_member_list3.get_mc_timestamp());
//}

TEST(ob_member_list_test, testif_equal)
{
  common::ObMemberList memberlist1;
  common::ObMemberList memberlist2;
  memberlist1.reset();
  memberlist2.reset();
  EXPECT_TRUE(memberlist1.member_addr_equal(memberlist2));  // test if both null is equal

  common::ObAddr addr1;
  common::ObAddr addr2;
  common::ObAddr addr3;
  common::ObAddr addr4;
  addr1.reset();
  addr2.reset();
  addr3.reset();
  addr4.reset();
  addr1.parse_from_cstring("127.0.0.1:8080");
  addr2.parse_from_cstring("127.0.0.2:8080");
  addr3.parse_from_cstring("127.0.0.3:8080");
  addr4.parse_from_cstring("127.0.0.4:8080");

  memberlist1.add_server(addr1);
  memberlist1.add_server(addr2);

  EXPECT_FALSE(memberlist1.member_addr_equal(memberlist2));  // test only on memberlist is null

  memberlist2.add_server(addr3);
  EXPECT_FALSE(memberlist1.member_addr_equal(memberlist2));  // test both the count and server addr are different

  memberlist2.add_server(addr4);
  EXPECT_FALSE(memberlist1.member_addr_equal(memberlist2));  // test all server are different

  memberlist1.add_server(addr3);
  memberlist1.add_server(addr4);
  EXPECT_FALSE(memberlist1.member_addr_equal(
      memberlist2));  // test both the count and server addr are different when no memberlist is null

  memberlist2.add_server(addr1);
  memberlist2.add_server(addr2);
  EXPECT_TRUE(memberlist1.member_addr_equal(memberlist2));  // not null. but equal
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_member_list.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest:test_member_list");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
