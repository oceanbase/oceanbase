/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_heartbeat_struct.h"

namespace oceanbase
{
namespace rootserver
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using namespace common;
class TestHeartbeatService : public ::testing::Test
{
public:
  TestHeartbeatService(){};
  virtual ~TestHeartbeatService(){};
  int clear_deleted_servers_in_all_servers_hb_info(
      ObArray<share::ObServerInfoInTable> &all_servers_info_in_table,
      hash::ObHashMap<common::ObAddr, share::ObServerHBInfo> &all_servers_hb_info);
  template <typename T>
  bool has_server_exist_in_array(
      const ObIArray<T> &array,
      const common::ObAddr &server,
      int64_t &idx);
};
int TestHeartbeatService::clear_deleted_servers_in_all_servers_hb_info(
      ObArray<share::ObServerInfoInTable>  &all_servers_info_in_table,
      hash::ObHashMap<common::ObAddr, share::ObServerHBInfo> &all_servers_hb_info)
{
  int ret = OB_SUCCESS;
  ObAddr server;
  hash::ObHashMap<ObAddr, share::ObServerHBInfo>::iterator iter = all_servers_hb_info.begin();

  while (OB_SUCC(ret) && iter != all_servers_hb_info.end()) {
    int64_t idx = OB_INVALID_INDEX_INT64;
    server.reset();
    server = iter->first;
    iter++;
    if (!has_server_exist_in_array(all_servers_info_in_table, server, idx)) {
      LOG_INFO("the server is deleted, it can be removed from all_servers_hb_info", K(server));
      if (OB_FAIL(all_servers_hb_info.erase_refactored(server))) {
        LOG_WARN("fail to remove the server from all_servers_hb_info", KR(ret), K(server));
      }
    }
  }

  return ret;
}
template <typename T>
bool TestHeartbeatService::has_server_exist_in_array(
    const ObIArray<T> &array,
    const common::ObAddr &server,
    int64_t &idx)
{
  bool bret = false;
  idx = OB_INVALID_INDEX_INT64;
  for (int64_t i = 0; i < array.count(); i++) {
    if (server == array.at(i).get_server()) {
      bret = true;
      idx = i;
      break;
    }
  }
  return bret;
}
TEST_F(TestHeartbeatService, EraseHBInfo)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<ObAddr, share::ObServerHBInfo> all_servers_hb_info;
  ObArray<share::ObServerInfoInTable> all_servers_info_in_table;
  share::ObServerHBInfo server_hb_info;
  share::ObServerInfoInTable server_info;
  const int64_t now = ObTimeUtility::current_time();
  ObAddr server1(ObAddr::IPV4, "127.0.0.1", 4444);
  ObAddr server2(ObAddr::IPV4, "127.0.0.1", 5555);
  ObAddr server3(ObAddr::IPV4, "127.0.0.1", 6666);
  ObAddr server4(ObAddr::IPV4, "127.0.0.1", 7777);
  ObZone zone("z1");
  ret = all_servers_hb_info.create(1024, ObModIds::OB_HASH_BUCKET);
  ASSERT_EQ(OB_SUCCESS, ret);
  // **************** case 1: server1 is deleting **************** //
  all_servers_info_in_table.reset();
  all_servers_hb_info.clear();
  server_info.reset();
  server_hb_info.reset();
  ASSERT_EQ(OB_SUCCESS, server_hb_info.init(server1, now, share::ObServerStatus::OB_HEARTBEAT_ALIVE));
  ASSERT_EQ(OB_SUCCESS, server_info.init(server1,1,zone, 30000, false, share::ObServerStatus::OB_SERVER_DELETING, "build_version", 0, 100, 0));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.set_refactored(server1, server_hb_info));
  server_hb_info.reset();
  server_info.reset();
  ASSERT_EQ(OB_SUCCESS, server_hb_info.init(server2, now, share::ObServerStatus::OB_HEARTBEAT_ALIVE));
  ASSERT_EQ(OB_SUCCESS, server_info.init(server2,2,zone, 30001, false, share::ObServerStatus::OB_SERVER_ACTIVE, "build_version", 0, 100, 0));
  ASSERT_EQ(OB_SUCCESS, all_servers_info_in_table.push_back(server_info));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.set_refactored(server2, server_hb_info));
  server_hb_info.reset();
  server_info.reset();
  ASSERT_EQ(OB_SUCCESS, server_hb_info.init(server3, now, share::ObServerStatus::OB_HEARTBEAT_ALIVE));
  ASSERT_EQ(OB_SUCCESS, server_info.init(server3,3,zone, 30002, false, share::ObServerStatus::OB_SERVER_ACTIVE, "build_version", 0, 100, 0));
  ASSERT_EQ(OB_SUCCESS, all_servers_info_in_table.push_back(server_info));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.set_refactored(server3, server_hb_info));
  server_hb_info.reset();
  server_info.reset();
  ASSERT_EQ(OB_SUCCESS, server_hb_info.init(server4, now, share::ObServerStatus::OB_HEARTBEAT_ALIVE));
  ASSERT_EQ(OB_SUCCESS, server_info.init(server4,4,zone, 30003, false, share::ObServerStatus::OB_SERVER_ACTIVE, "build_version", 0, 100, 0));
  ASSERT_EQ(OB_SUCCESS, all_servers_info_in_table.push_back(server_info));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.set_refactored(server4, server_hb_info));
  server_hb_info.reset();
  server_info.reset();

  clear_deleted_servers_in_all_servers_hb_info(all_servers_info_in_table, all_servers_hb_info);
  ASSERT_EQ(OB_HASH_NOT_EXIST, all_servers_hb_info.get_refactored(server1, server_hb_info));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.get_refactored(server2, server_hb_info));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.get_refactored(server3, server_hb_info));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.get_refactored(server4, server_hb_info));
  // **************** case 2: server2 and server3 is deleting **************** //
  all_servers_info_in_table.reset();
  all_servers_hb_info.clear();
  ASSERT_EQ(OB_SUCCESS, server_hb_info.init(server1, now, share::ObServerStatus::OB_HEARTBEAT_ALIVE));
  ASSERT_EQ(OB_SUCCESS, server_info.init(server1,1,zone, 30000, false, share::ObServerStatus::OB_SERVER_ACTIVE, "build_version", 0, 100, 0));
  ASSERT_EQ(OB_SUCCESS, all_servers_info_in_table.push_back(server_info));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.set_refactored(server1, server_hb_info));
  server_hb_info.reset();
  server_info.reset();
  ASSERT_EQ(OB_SUCCESS, server_hb_info.init(server2, now, share::ObServerStatus::OB_HEARTBEAT_ALIVE));
  ASSERT_EQ(OB_SUCCESS, server_info.init(server2,2,zone, 30001, false, share::ObServerStatus::OB_SERVER_DELETING, "build_version", 0, 100, 0));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.set_refactored(server2, server_hb_info));
  server_hb_info.reset();
  server_info.reset();
  ASSERT_EQ(OB_SUCCESS, server_hb_info.init(server3, now, share::ObServerStatus::OB_HEARTBEAT_ALIVE));
  ASSERT_EQ(OB_SUCCESS, server_info.init(server3,3,zone, 30002, false, share::ObServerStatus::OB_SERVER_DELETING, "build_version", 0, 100, 0));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.set_refactored(server3, server_hb_info));
  server_hb_info.reset();
  server_info.reset();
  ASSERT_EQ(OB_SUCCESS, server_hb_info.init(server4, now, share::ObServerStatus::OB_HEARTBEAT_ALIVE));
  ASSERT_EQ(OB_SUCCESS, server_info.init(server4,4,zone, 30003, false, share::ObServerStatus::OB_SERVER_ACTIVE, "build_version", 0, 100, 0));
  ASSERT_EQ(OB_SUCCESS, all_servers_info_in_table.push_back(server_info));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.set_refactored(server4, server_hb_info));
  server_hb_info.reset();
  server_info.reset();

  clear_deleted_servers_in_all_servers_hb_info(all_servers_info_in_table, all_servers_hb_info);
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.get_refactored(server1, server_hb_info));
  ASSERT_EQ(OB_HASH_NOT_EXIST, all_servers_hb_info.get_refactored(server2, server_hb_info));
  ASSERT_EQ(OB_HASH_NOT_EXIST, all_servers_hb_info.get_refactored(server3, server_hb_info));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.get_refactored(server4, server_hb_info));
  // **************** case 3: server4 is deleting **************** //
  all_servers_info_in_table.reset();
  all_servers_hb_info.clear();
  ASSERT_EQ(OB_SUCCESS, server_hb_info.init(server1, now, share::ObServerStatus::OB_HEARTBEAT_ALIVE));
  ASSERT_EQ(OB_SUCCESS, server_info.init(server1,1,zone, 30000, false, share::ObServerStatus::OB_SERVER_ACTIVE, "build_version", 0, 100, 0));
  ASSERT_EQ(OB_SUCCESS, all_servers_info_in_table.push_back(server_info));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.set_refactored(server1, server_hb_info));
  server_hb_info.reset();
  server_info.reset();
  ASSERT_EQ(OB_SUCCESS, server_hb_info.init(server2, now, share::ObServerStatus::OB_HEARTBEAT_ALIVE));
  ASSERT_EQ(OB_SUCCESS, server_info.init(server2,2,zone, 30001, false, share::ObServerStatus::OB_SERVER_ACTIVE, "build_version", 0, 100, 0));
  ASSERT_EQ(OB_SUCCESS, all_servers_info_in_table.push_back(server_info));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.set_refactored(server2, server_hb_info));
  server_hb_info.reset();
  server_info.reset();
  ASSERT_EQ(OB_SUCCESS, server_hb_info.init(server3, now, share::ObServerStatus::OB_HEARTBEAT_ALIVE));
  ASSERT_EQ(OB_SUCCESS, server_info.init(server3,3,zone, 30002, false, share::ObServerStatus::OB_SERVER_ACTIVE, "build_version", 0, 100, 0));
  ASSERT_EQ(OB_SUCCESS, all_servers_info_in_table.push_back(server_info));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.set_refactored(server3, server_hb_info));
  server_hb_info.reset();
  server_info.reset();
  ASSERT_EQ(OB_SUCCESS, server_hb_info.init(server4, now, share::ObServerStatus::OB_HEARTBEAT_ALIVE));
  ASSERT_EQ(OB_SUCCESS, server_info.init(server4,4,zone, 30003, false, share:: ObServerStatus::OB_SERVER_DELETING, "build_version", 0, 100, 0));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.set_refactored(server4, server_hb_info));
  server_hb_info.reset();
  server_info.reset();

  clear_deleted_servers_in_all_servers_hb_info(all_servers_info_in_table, all_servers_hb_info);
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.get_refactored(server1, server_hb_info));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.get_refactored(server2, server_hb_info));
  ASSERT_EQ(OB_SUCCESS, all_servers_hb_info.get_refactored(server3, server_hb_info));
  ASSERT_EQ(OB_HASH_NOT_EXIST, all_servers_hb_info.get_refactored(server4, server_hb_info));
}
} // rootservice
} // oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}