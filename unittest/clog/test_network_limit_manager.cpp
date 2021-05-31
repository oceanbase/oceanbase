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

#include "clog/ob_log_engine.h"

#include <gtest/gtest.h>

namespace oceanbase {
using namespace common;
using namespace clog;
namespace unittest {
TEST(test_network_limit_manager, test1)
{
  const int64_t ethernet_speed = 1000 * 1000;
  const int64_t ethernet_speed_tmp = ethernet_speed * 60 / 100;
  NetworkLimitManager network_limit_manager;
  EXPECT_EQ(OB_SUCCESS, network_limit_manager.init(ethernet_speed));
  ObAddr addr1(1, 80);
  ObAddr addr2(2, 81);
  ObAddr addr3(3, 82);
  ObAddr addr4(4, 83);
  int64_t network_limit = 0;
  EXPECT_EQ(OB_SUCCESS, network_limit_manager.get_limit(addr1, network_limit));
  EXPECT_EQ(ethernet_speed_tmp, network_limit);
  EXPECT_EQ(OB_SUCCESS, network_limit_manager.get_limit(addr1, network_limit));
  EXPECT_EQ(ethernet_speed_tmp, network_limit);
  EXPECT_EQ(OB_SUCCESS, network_limit_manager.get_limit(addr2, network_limit));
  EXPECT_EQ(ethernet_speed_tmp / 2, network_limit);
  EXPECT_EQ(OB_SUCCESS, network_limit_manager.get_limit(addr3, network_limit));
  EXPECT_EQ(ethernet_speed_tmp / 3, network_limit);

  ObMemberList member_list;
  member_list.add_server(addr1);
  EXPECT_EQ(OB_SUCCESS, network_limit_manager.get_limit(member_list, network_limit));
  EXPECT_EQ(ethernet_speed_tmp / 3, network_limit);
  member_list.add_server(addr4);
  EXPECT_EQ(OB_SUCCESS, network_limit_manager.get_limit(member_list, network_limit));
  EXPECT_EQ(ethernet_speed_tmp / 4, network_limit);

  EXPECT_EQ(OB_SUCCESS, network_limit_manager.try_limit(addr1, 100, 100000));
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_network_limit_manager.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest::test_network_limit_manager");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
