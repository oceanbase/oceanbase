// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public
#include "logservice/leader_coordinator/ob_failure_detector.h"
#include "common/ob_role.h"
#include "lib/list/ob_dlist.h"
#include "logservice/leader_coordinator/failure_event.h"
#include "logservice/palf/election/interface/election_msg_handler.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include "logservice/palf/election/interface/election.h"
#include "logservice/palf/log_meta_info.h"
#define UNITTEST
#include "logservice/palf/election/utils/election_common_define.h"
#include "logservice/palf/election/algorithm/election_impl.h"
#include "logservice/leader_coordinator/election_priority_impl/election_priority_impl.h"
#include "share/ob_occam_timer.h"
#include "share/rc/ob_tenant_base.h"
#include "mock_logservice_container/mock_election_user.h"
#include <iostream>
#include <vector>
#include "logservice/leader_coordinator/table_accessor.h"

using namespace std;

#define SUCC_(stmt) ASSERT_EQ((stmt), OB_SUCCESS)
#define FAIL_(stmt) ASSERT_EQ((stmt), OB_FAIL)
#define TRUE_(stmt) ASSERT_EQ((stmt), true)
#define FALSE_(stmt) ASSERT_EQ((stmt), false)

namespace oceanbase
{
namespace unittest {

class TestPrioritySeperateBlackList : public ::testing::Test {
public:
  TestPrioritySeperateBlackList() {}
  ~TestPrioritySeperateBlackList() {}
  virtual void SetUp() {}
  virtual void TearDown() { }
};

TEST_F(TestPrioritySeperateBlackList, main)
{
  logservice::coordinator::LsElectionReferenceInfoRow row(1, share::ObLSID(1));
  row.row_for_table_.element<0>() = 1;
  row.row_for_table_.element<1>() = 1;
  row.row_for_table_.element<2>().assign("z4,z5;z3;z2,z1");
  row.row_for_table_.element<3>().assign("127.0.0.1:1080");
  row.row_for_table_.element<4>().assign("127.0.0.1:1080(MIGRATE);127.0.0.1:1081(MIGRATE)");
  row.convert_table_info_to_user_info_();
  ASSERT_EQ(row.row_for_user_.element<4>().count(), 2);
  ELECT_LOG(INFO, "print", K(row));
}

TEST_F(TestPrioritySeperateBlackList, test_set_user_row_for_specific_reason)
{
  // OB_ENTRY_EXIST
  {
    logservice::coordinator::LsElectionReferenceInfoRow row(1, share::ObLSID(1));
    row.row_for_table_.element<0>() = 1;
    row.row_for_table_.element<1>() = 1;
    row.row_for_table_.element<2>().assign("z4,z5;z3;z2,z1");
    row.row_for_table_.element<3>().assign("127.0.0.1:1080");
    row.row_for_table_.element<4>().assign("127.0.0.1:1080(MIGRATE)");
    row.convert_table_info_to_user_info_();
    ASSERT_EQ(OB_ENTRY_EXIST, row.set_user_row_for_specific_reason_(ObAddr(ObAddr::IPV4, "127.0.0.1", 1080),
        logservice::coordinator::InsertElectionBlacklistReason::MIGRATE));
    ASSERT_EQ(row.row_for_user_.element<4>().count(), 1);
    ELECT_LOG(INFO, "print", K(row));
  }
  // OB_SUCCESS
  {
    logservice::coordinator::LsElectionReferenceInfoRow row(1, share::ObLSID(1));
    row.row_for_table_.element<0>() = 1;
    row.row_for_table_.element<1>() = 1;
    row.row_for_table_.element<2>().assign("z4,z5;z3;z2,z1");
    row.row_for_table_.element<3>().assign("127.0.0.1:1080");
    row.row_for_table_.element<4>().assign("127.0.0.1:1080(MIGRATE);127.0.0.1:1081(MIGRATE);127.0.0.1:1082(SWITCH REPLICA)");
    row.convert_table_info_to_user_info_();
    const ObAddr new_addr = ObAddr(ObAddr::IPV4, "127.0.0.1", 1083);
    ASSERT_EQ(OB_SUCCESS, row.set_user_row_for_specific_reason_(new_addr,
        logservice::coordinator::InsertElectionBlacklistReason::MIGRATE));
    ASSERT_EQ(row.row_for_user_.element<4>().count(), 2);
    ASSERT_EQ(new_addr, row.row_for_user_.element<4>().at(1).element<0>());
    ELECT_LOG(INFO, "print", K(row));
  }
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_ob_election_priority_seperate_blacklist.log");
  oceanbase::palf::election::GLOBAL_INIT_ELECTION_MODULE();
  oceanbase::unittest::MockNetService::init();
  oceanbase::palf::election::INIT_TS = 0;
  oceanbase::common::ObClockGenerator::init();
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  oceanbase::share::ObTenantBase tenant_base(OB_SYS_TENANT_ID);
  tenant_base.init();
  oceanbase::share::ObTenantEnv::set_tenant(&tenant_base);
  logger.set_file_name("test_ob_election_priority_seperate_blacklist.log", false);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
