/**
 * Copyright (c) 2023 OceanBase
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
#include "lib/ob_define.h"
#include "rpc/obrpc/ob_net_keepalive.h"
#define private public
#include "logservice/palf/log_config_mgr.h"
#include "logservice/ob_arbitration_service.h"
#include "logservice/ob_net_keepalive_adapter.h"
#undef private

namespace oceanbase
{
using namespace obrpc;

namespace storage
{
}

namespace unittest
{
using namespace common;
using namespace logservice;
using namespace palf;

class MockNetKeepAliveAdapter : public logservice::IObNetKeepAliveAdapter
{
public:
  MockNetKeepAliveAdapter() {}
  ~MockNetKeepAliveAdapter() { }
  bool in_black_or_stopped(const common::ObAddr &server) override final {return false;}
  bool is_server_stopped(const common::ObAddr &server) override final {return false;}
  bool in_black(const common::ObAddr &server) override final {return false;}
  int get_last_resp_ts(const common::ObAddr &server, int64_t &last_resp_ts) { return OB_SUCCESS; }
};

const ObAddr addr1(ObAddr::IPV4, "127.0.0.1", 1000);
const ObAddr addr2(ObAddr::IPV4, "127.0.0.2", 1000);
const ObAddr addr3(ObAddr::IPV4, "127.0.0.3", 1000);
const ObAddr addr4(ObAddr::IPV4, "127.0.0.4", 1000);
const ObAddr addr5(ObAddr::IPV4, "127.0.0.5", 1000);
const ObAddr addr6(ObAddr::IPV4, "127.0.0.6", 1000);
const ObAddr addr7(ObAddr::IPV4, "127.0.0.7", 1000);
const ObAddr addr8(ObAddr::IPV4, "127.0.0.8", 1000);
const ObAddr addr9(ObAddr::IPV4, "127.0.0.9", 1000);
ObRegion region1("BEIJING");
ObRegion region2("SHANGHAI");
ObRegion default_region(DEFAULT_REGION_NAME);
ObIDC idc1("idc1");
ObIDC idc2("idc2");
ObIDC idc3("idc3");

class TestObArbitrationService : public ::testing::Test
{
public:
  TestObArbitrationService() { }
  ~TestObArbitrationService() { }
};


TEST_F(TestObArbitrationService, locality_allow_degrade_test)
{
  ObMemberList paxos_list;
  {
    // 2F, degrade 1, allow
    MockNetKeepAliveAdapter net_keepalive;
    ObArbitrationService::DoDegradeFunctor do_degrade_func(addr1, NULL, NULL, NULL);
    const int64_t palf_id = 1;
    const int64_t replica_num = 2;
    paxos_list.add_server(addr1);
    paxos_list.add_server(addr2);
    LogMemberStatusList dead_servers;
    common::GlobalLearnerList degraded_servers;
    EXPECT_EQ(OB_SUCCESS, dead_servers.push_back(LogMemberStatus(LogMemberAckInfo(ObMember(addr1, 1), 1, LSN(1000)))));
    EXPECT_TRUE(do_degrade_func.is_allow_degrade_(paxos_list, replica_num, degraded_servers, dead_servers));
  }
  {
    // 4F, degrade 3, not allow
    MockNetKeepAliveAdapter net_keepalive;
    ObArbitrationService::DoDegradeFunctor do_degrade_func(addr1, NULL, NULL, NULL);
    const int64_t palf_id = 1;
    paxos_list.add_server(addr3);
    paxos_list.add_server(addr4);
    const int64_t replica_num = 4;
    LogMemberStatusList dead_servers;
    common::GlobalLearnerList degraded_servers;
    EXPECT_EQ(OB_SUCCESS, dead_servers.push_back(LogMemberStatus(LogMemberAckInfo(ObMember(addr1, 1), 1, LSN(1000)))));
    EXPECT_EQ(OB_SUCCESS, dead_servers.push_back(LogMemberStatus(LogMemberAckInfo(ObMember(addr2, 1), 1, LSN(1000)))));
    EXPECT_EQ(OB_SUCCESS, dead_servers.push_back(LogMemberStatus(LogMemberAckInfo(ObMember(addr3, 1), 1, LSN(1000)))));
    EXPECT_FALSE(do_degrade_func.is_allow_degrade_(paxos_list, replica_num, degraded_servers, dead_servers));
  }
  {
    // 4F, degrade 1, not allow
    MockNetKeepAliveAdapter net_keepalive;
    ObArbitrationService::DoDegradeFunctor do_degrade_func(addr1, NULL, NULL, NULL);
    const int64_t palf_id = 1;
    const int64_t replica_num = 4;
    LogMemberStatusList dead_servers;
    common::GlobalLearnerList degraded_servers;
    EXPECT_EQ(OB_SUCCESS, dead_servers.push_back(LogMemberStatus(LogMemberAckInfo(ObMember(addr1, 1), 1, LSN(1000)))));
    EXPECT_FALSE(do_degrade_func.is_allow_degrade_(paxos_list, replica_num, degraded_servers, dead_servers));
  }
  {
    // 3F1A, degrade 1, not allow
    MockNetKeepAliveAdapter net_keepalive;
    ObArbitrationService::DoDegradeFunctor do_degrade_func(addr1, NULL, NULL, NULL);
    const int64_t palf_id = 1;
    paxos_list.remove_server(addr4);
    const int64_t replica_num = 3;
    LogMemberStatusList dead_servers;
    common::GlobalLearnerList degraded_servers;
    EXPECT_EQ(OB_SUCCESS, dead_servers.push_back(LogMemberStatus(LogMemberAckInfo(ObMember(addr3, 1), 1, LSN(1000)))));
    EXPECT_FALSE(do_degrade_func.is_allow_degrade_(paxos_list, replica_num, degraded_servers, dead_servers));
  }
  {
    // 4F1A, degrade 2(addr2, addr3), allow
    MockNetKeepAliveAdapter net_keepalive;
    ObArbitrationService::DoDegradeFunctor do_degrade_func(addr1, NULL, NULL, NULL);
    const int64_t palf_id = 1;
    paxos_list.add_server(addr4);
    const int64_t replica_num = 4;
    LogMemberStatusList dead_servers;
    common::GlobalLearnerList degraded_servers;
    EXPECT_EQ(OB_SUCCESS, dead_servers.push_back(LogMemberStatus(LogMemberAckInfo(ObMember(addr2, 1), 1, LSN(1000)))));
    EXPECT_EQ(OB_SUCCESS, dead_servers.push_back(LogMemberStatus(LogMemberAckInfo(ObMember(addr3, 1), 1, LSN(1000)))));
    EXPECT_TRUE(do_degrade_func.is_allow_degrade_(paxos_list, replica_num, degraded_servers, dead_servers));
  }
}

}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ob_arbitration_service.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_ob_arbitration_service");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
