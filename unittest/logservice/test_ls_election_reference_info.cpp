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
#include "common/ob_range.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "logservice/leader_coordinator/table_accessor.h"
#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"
#include "logservice/leader_coordinator/ob_failure_detector.h"
#include "lib/net/ob_addr.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace palf;
using namespace logservice;
using namespace logservice::coordinator;
namespace unittest
{

class TestLsElectionReferenceInfoRow : public ::testing::Test
{
public:
  TestLsElectionReferenceInfoRow() {}
};

// TEST_F(TestLsElectionReferenceInfoRow, normal)
// {
//   LsElectionReferenceInfoRow row(1, ObLSID(1));
//   ObArray<ObArray<ObString>> zone_list_list;
//   ObArray<ObString> zone_list1, zone_list2, zone_list3;
//   zone_list1.push_back("z1");
//   zone_list2.push_back("z2");
//   zone_list2.push_back("z3");
//   zone_list3.push_back("z4");
//   zone_list3.push_back("z5");
//   zone_list_list.push_back(zone_list1);
//   zone_list_list.push_back(zone_list2);
//   zone_list_list.push_back(zone_list3);
//   ObArray<ObAddr> remove_member_list;
//   remove_member_list.push_back(ObAddr(ObAddr::VER::IPV4, "123.1.1.1", 1091));
//   remove_member_list.push_back(ObAddr(ObAddr::VER::IPV4, "123.1.1.1", 1092));
//   remove_member_list.push_back(ObAddr(ObAddr::VER::IPV4, "123.1.1.1", 1093));
//   ObArray<ObString> remove_reason;
//   remove_reason.push_back(ObString("reason1"));
//   remove_reason.push_back(ObString("reason1"));
//   remove_reason.push_back(ObString("reason1"));
//   ASSERT_EQ(OB_SUCCESS, row.set(ObLSID(1), zone_list_list, ObAddr(ObAddr::VER::IPV4, "123.1.1.1", 1090), remove_member_list, remove_reason));
//   COORDINATOR_LOG(INFO, "debug", K(row));
// }

}
}

int main(int argc, char **argv)
{
  system("rm -rf etc run log wallet store");
  system("rm -rf test_ls_election_reference_info.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_ls_election_reference_info.log", false, false);
  logger.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}