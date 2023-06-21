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
#include <gmock/gmock.h>
#define private public
#define protected public
#include "logservice/palf/election/interface/election_msg_handler.h"
#include "logservice/palf/log_config_mgr.h"
#include "logservice/palf/log_meta_info.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/string/ob_string_holder.h"
#include "logservice/leader_coordinator/ob_leader_coordinator.h"
#include "logservice/leader_coordinator/table_accessor.h"
#include "logservice/palf/election/utils/election_common_define.h"
#include "logservice/palf/election/algorithm/election_impl.h"
#include "logservice/leader_coordinator/election_priority_impl/election_priority_impl.h"
#include "logservice/mock_logservice_container/mock_election_user.h"
#include "share/ob_cluster_version.h"
#include "share/rc/ob_tenant_base.h"
#include <iostream>
#include "lib/container/ob_tuple.h"
#define MIT_TESTCASE_LABEL
#include "mtlenv/mock_tenant_module_env.h"
#include "observer/ob_server.h"

using namespace oceanbase::obrpc;
using namespace std;

#define SUCC_(stmt) ASSERT_EQ((stmt), OB_SUCCESS)
#define FAIL_(stmt) ASSERT_EQ((stmt), OB_FAIL)
#define TRUE_(stmt) ASSERT_EQ((stmt), true)
#define FALSE_(stmt) ASSERT_EQ((stmt), false)

namespace oceanbase {

namespace unittest {

using namespace common;
using namespace palf::election;
using namespace std;
using namespace logservice::coordinator;

class TestElectionPriority : public ::testing::Test {
public:
  TestElectionPriority() {}
  ~TestElectionPriority() {}
  static void SetUpTestCase() { ASSERT_EQ(OB_SUCCESS, storage::MockTenantModuleEnv::get_instance().init()); }
  static void TearDownTestCase() { storage::MockTenantModuleEnv::get_instance().destroy(); }
  void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  }
  // virtual void SetUp() { }
  // virtual void TearDown() { }
  static void test_normal()
  {
    ElectionPriorityImpl impl1, impl2;
    ASSERT_EQ(CLUSTER_VERSION_2100 > CLUSTER_VERSION_2000, true);
    impl1.priority_tuple_.element<0>().port_number_ = 1;
    impl1.priority_tuple_.element<0>().is_valid_ = true;
    impl2.priority_tuple_.element<0>().port_number_ = 2;
    impl2.priority_tuple_.element<0>().is_valid_ = true;
    int result = 0;
    ObStringHolder reason;
    oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_2100;
    ASSERT_EQ(OB_SUCCESS, impl1.compare_with(impl2, result, reason));
    ASSERT_EQ(result, -1);
    OB_LOG(DEBUG, "compare1", K(reason));
    result = 0;
    impl1.priority_tuple_.element<1>().is_valid_ = true;
    impl2.priority_tuple_.element<1>().is_valid_ = true;
    impl1.priority_tuple_.element<1>().is_manual_leader_ = true;
    oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_0_0_0;
    ASSERT_EQ(OB_SUCCESS, impl1.compare_with(impl2, result, reason));
    ASSERT_EQ(result, 1);
    OB_LOG(DEBUG, "compare2", K(reason));
    OB_LOG(DEBUG, "test_normal", K(reason));
  }
  static void test_serialize_and_deserialize()
  {
    OB_LOG(DEBUG, "print version", K(CLUSTER_VERSION_2000), K(CLUSTER_VERSION_4_0_0_0));
    ElectionPriorityImpl impl1, impl2;
    impl1.priority_tuple_.element<1>().is_in_blacklist_ = true;
    impl1.priority_tuple_.element<1>().is_valid_ = true;
    impl1.priority_tuple_.element<1>().is_manual_leader_ = true;
    int64_t pos = 0;
    char buffer[2048] = {0};
    ASSERT_EQ(OB_SUCCESS, impl1.serialize(buffer, 1024, pos));
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, impl2.deserialize(buffer, 1024, pos));
    int result = 0;
    ObStringHolder reason;
    oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_0_0_0;
    ASSERT_EQ(OB_SUCCESS, impl1.compare_with(impl2, result, reason));
    ASSERT_EQ(0, result);
    OB_LOG(DEBUG, "test_serialize_and_deserialize");

    ElectionAcceptResponseMsg msg(GCTX.self_addr(), palf::LogConfigVersion(), ElectionAcceptRequestMsg(
      1, GCTX.self_addr(), 1, 1, 1, 1, palf::LogConfigVersion()
    )), msg1(GCTX.self_addr(), palf::LogConfigVersion(), ElectionAcceptRequestMsg(
      1, GCTX.self_addr(), 1, 1, 1, 1, palf::LogConfigVersion()
    ));
    ASSERT_EQ(OB_SUCCESS, msg.set_accepted(12, &impl1));
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, msg.serialize(buffer, 1024, pos));
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, msg1.deserialize(buffer, 1024, pos));
    const char * msg_buffer = msg1.get_priority_buffer();
    const char * msg_buffer2 = msg.get_priority_buffer();
    OB_LOG(DEBUG, "start check serialize result");
    for (int64_t idx = 0; idx < 512; ++idx) {
      ASSERT_EQ(msg_buffer[idx], msg_buffer2[idx]);
    }
    OB_LOG(DEBUG, "end check serialize result");
    pos = 0;
    impl2.deserialize(msg_buffer, 512, pos);
    oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_0_0_0;
    ASSERT_EQ(OB_SUCCESS, impl1.compare_with(impl2, result, reason));
    ASSERT_EQ(0, result);
  }
  static void test_ob_string_holder()
  {
    ObStringHolder str1;
    str1 .assign("dsafgdsaf");
    char buffer[1024] = {0};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, str1.serialize(buffer, 1024, pos));
    ObStringHolder str2;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, str2.deserialize(buffer, 1024, pos));
    ASSERT_EQ(str1.get_ob_string(), str2.get_ob_string());
    OB_LOG(DEBUG, "test_ob_string_holder");
  }
  static void get_ls_reference_info_from_MTL()
  {
    auto p_coordinator =  MTL(logservice::coordinator::ObLeaderCoordinator*);
    {
      ObSpinLockGuard guard(p_coordinator->lock_);
      ObStringHolder holder, status;
      ASSERT_EQ(OB_SUCCESS, holder.assign("123"));
      ASSERT_EQ(OB_SUCCESS, p_coordinator->all_ls_election_reference_info_->push_back(LsElectionReferenceInfo(1, 2, true, ObTuple<bool, ObStringHolder>(true, std::move(holder)), false, false, false)));
    }
    ElectionPriorityImpl priority(share::ObLSID(1));
    ASSERT_EQ(OB_SUCCESS, priority.refresh());
    ASSERT_EQ(priority.priority_tuple_.element<1>().is_manual_leader_, true);
    ASSERT_EQ(priority.priority_tuple_.element<1>().in_blacklist_reason_.get_ob_string(), "123");
    OB_LOG(DEBUG, "get_ls_reference_info_from_MTL", K(priority));
  }
  static void test_ls_election_referenct_info_row()
  {
    LsElectionReferenceInfoRow row(1, share::ObLSID(1));
    ObStringHolder z1, z2, z3, z4, z5;
    ASSERT_EQ(OB_SUCCESS, z1.assign("z1"));
    ASSERT_EQ(OB_SUCCESS, z2.assign("z2"));
    ASSERT_EQ(OB_SUCCESS, z3.assign("z3"));
    ASSERT_EQ(OB_SUCCESS, z4.assign("z4"));
    ASSERT_EQ(OB_SUCCESS, z5.assign("z5"));
    ObArray<ObStringHolder> pri1, pri2, pri3;
    ASSERT_EQ(OB_SUCCESS, pri1.push_back(z1));
    ASSERT_EQ(OB_SUCCESS, pri1.push_back(z2));
    ASSERT_EQ(OB_SUCCESS, pri2.push_back(z3));
    ASSERT_EQ(OB_SUCCESS, pri3.push_back(z4));
    ASSERT_EQ(OB_SUCCESS, pri3.push_back(z5));
    ObArray<ObArray<ObStringHolder>> pri;
    ASSERT_EQ(OB_SUCCESS, pri.push_back(pri1));
    ASSERT_EQ(OB_SUCCESS, pri.push_back(pri2));
    ASSERT_EQ(OB_SUCCESS, pri.push_back(pri3));
    ASSERT_EQ(OB_SUCCESS, row.row_for_user_.element<2>().assign(pri));
    row.row_for_user_.element<3>() = ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1080);
    ObTuple<ObAddr, ObStringHolder> remove_member1, remove_member2;
    remove_member1.element<0>() = ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1080);
    ASSERT_EQ(OB_SUCCESS, remove_member1.element<1>().assign("reason1"));
    remove_member2.element<0>() = ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1081);
    ASSERT_EQ(OB_SUCCESS, remove_member2.element<1>().assign("reason2"));
    ASSERT_EQ(OB_SUCCESS, row.row_for_user_.element<4>().push_back(remove_member1));
    ASSERT_EQ(OB_SUCCESS, row.row_for_user_.element<4>().push_back(remove_member2));
    ASSERT_EQ(OB_SUCCESS, row.convert_user_info_to_table_info_());
    OB_LOG(DEBUG, "test_ls_election_referenct_info_row", K(row));
  }
};

TEST_F(TestElectionPriority, normal) {
  TestElectionPriority::test_normal();
}

TEST_F(TestElectionPriority, test_ob_string_holder) {
  TestElectionPriority::test_ob_string_holder();
}

TEST_F(TestElectionPriority, test_serialize_and_deserialize) {
  TestElectionPriority::test_serialize_and_deserialize();
}

// TEST_F(TestElectionPriority, get_ls_reference_info_from_MTL) {
//   // can't test get_ls_reference_info_from_MTL cause refresh() will report -4018 in ob_log_replay_service
//   TestElectionPriority::get_ls_reference_info_from_MTL();
// }

TEST_F(TestElectionPriority, test_ls_election_referenct_info_row) {
  TestElectionPriority::test_ls_election_referenct_info_row();
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_ob_election_priority.log");
  oceanbase::common::ObClockGenerator::init();
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_ob_election_priority.log", false, false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
