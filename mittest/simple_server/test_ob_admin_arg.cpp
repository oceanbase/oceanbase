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

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "rootserver/ob_admin_drtask_util.h" // ObAdminDRTaskUtil

namespace oceanbase
{
using namespace unittest;
using namespace rootserver;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace common;

class TestObAdminArg : public unittest::ObSimpleClusterTestBase
{
public:
   TestObAdminArg() : unittest::ObSimpleClusterTestBase("test_ob_admin_arg") {}
};

TEST_F(TestObAdminArg, test_argument)
{
  int ret = OB_SUCCESS;
  ObAdminCommandArg arg;
  const ObAdminDRTaskType task_type(ObAdminDRTaskType::REMOVE_REPLICA);

  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  share::ObLSID ls_id;
  ObReplicaType replica_type = REPLICA_TYPE_MAX;
  common::ObAddr data_source;
  common::ObAddr server_addr;
  int64_t orig_paxos_replica_number = 0;
  int64_t new_paxos_replica_number = 0;

  common::ObAddr server_to_compare1(ObAddr::VER::IPV4, "100.88.107.212", 2001);
  common::ObAddr server_to_compare2(ObAddr::VER::IPV4, "100.88.107.212", 2002);
  common::ObAddr server_to_compare3;
  ret = server_to_compare3.parse_from_string("[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:203");
  ASSERT_EQ(0, ret);
  common::ObAddr server_to_compare4;
  ret = server_to_compare4.parse_from_string("[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:204");
  ASSERT_EQ(0, ret);

  // test command is null
  ret = arg.init("", task_type);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  arg.reset();

  // test command is not null but invalid
  ret = arg.init("abc", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  arg.reset();

  // test only tenant_id is provided
  ret = arg.init("tenant_id=1002", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  arg.reset();

  ret = arg.init("  tenant_id=1002", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  arg.reset();

  ret = arg.init("tenant_id=1002  ", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  arg.reset();

  ret = arg.init(",tenant_id=1002,,", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  arg.reset();

  // test only ls_id is provided
  ret = arg.init("ls_id=1002", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, ls_id.id());
  arg.reset();

  ret = arg.init(" ls_id=1002", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, ls_id.id());
  arg.reset();

  ret = arg.init("ls_id=1002 ", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, ls_id.id());
  arg.reset();

  ret = arg.init(",,ls_id=1002,,", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  arg.reset();

  ret = arg.init("tenant_id=1002,ls_id=1001", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(1001, ls_id.id());
  arg.reset();

  ret = arg.init("ls_id=1001,tenant_id=1002", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(1001, ls_id.id());
  arg.reset();

  ret = arg.init("  ls_id=1001 ,tenant_id=1002 ", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(1001, ls_id.id());
  arg.reset();

  ret = arg.init("  ls_id=1001 ,  tenant_id=1002  ", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(1001, ls_id.id());
  arg.reset();

  ret = arg.init("  ls_id = 1001 ,  tenant_id = 1002  ", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(1001, ls_id.id());
  arg.reset();

  ret = arg.init("  LS_id = 1001 ,  tenaNT_id = 1002  ", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(1001, ls_id.id());
  arg.reset();

  // test server address IPV4
  // server address in format ip:port
  ret = arg.init("ls_id=1001,server=100.88.107.212:2002,tenant_id=1002", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(1001, ls_id.id());
  ASSERT_EQ(0, server_to_compare1 == server_addr);
  ASSERT_EQ(1, server_to_compare2 == server_addr);
  ASSERT_EQ(0, server_to_compare3 == server_addr);
  ASSERT_EQ(0, server_to_compare4 == server_addr);
  arg.reset();

  ret = arg.init("ls_id=1001,tenant_id=1002,server=100.88.107.212:2002", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(1001, ls_id.id());
  ASSERT_EQ(0, server_to_compare1 == server_addr);
  ASSERT_EQ(1, server_to_compare2 == server_addr);
  ASSERT_EQ(0, server_to_compare3 == server_addr);
  ASSERT_EQ(0, server_to_compare4 == server_addr);
  arg.reset();

  ret = arg.init("ls_id=1001,tenant_id=1002,replica_type=r,server=100.88.107.212:2002", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(1001, ls_id.id());
  ASSERT_EQ(0, server_to_compare1 == server_addr);
  ASSERT_EQ(1, server_to_compare2 == server_addr);
  ASSERT_EQ(0, server_to_compare3 == server_addr);
  ASSERT_EQ(0, server_to_compare4 == server_addr);
  arg.reset();

  // test server address IPV6
  // server address in format ip:port
  ObAddr server_addr_for_ipv6;
  ret = arg.init("ls_id=1001,tenant_id=1002,server=[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:203", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr_for_ipv6, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(1001, ls_id.id());
  ASSERT_EQ(0, server_to_compare1 == server_addr_for_ipv6);
  ASSERT_EQ(0, server_to_compare2 == server_addr_for_ipv6);
  ASSERT_EQ(1, server_to_compare3 == server_addr_for_ipv6);
  ASSERT_EQ(0, server_to_compare4 == server_addr_for_ipv6);
  arg.reset();

  // test data_source IPV4
  // data_source address in format ip:port
  ret = arg.init("ls_id=1001,tenant_id=1002,data_source=100.88.107.212:2001", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(1001, ls_id.id());
  ASSERT_EQ(1, server_to_compare1 == data_source);
  ASSERT_EQ(0, server_to_compare2 == data_source);
  ASSERT_EQ(0, server_to_compare3 == data_source);
  ASSERT_EQ(0, server_to_compare4 == data_source);
  arg.reset();

  // test data_source IPV4
  // data_source address in format ip:port:timstamp
  ret = arg.init("ls_id=1001,tenant_id=1002,data_source=100.88.107.212:2001:100", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  arg.reset();

  // test data_source IPV4
  // data_source address in format ip:port:timstamp:flag
  ret = arg.init("ls_id=1001,tenant_id=1002,data_source=100.88.107.212:2001:100:0", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  arg.reset();

  // test whole command
  ret = arg.init("tenant_id=1002,ls_id=1001,replica_type=readonly,data_source=100.88.107.212:2001,server=100.88.107.212:2002,orig_paxos_replica_number=2,new_paxos_replica_number=3", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(1001, ls_id.id());
  ASSERT_EQ(REPLICA_TYPE_READONLY, replica_type);
  ASSERT_EQ(1, server_to_compare1 == data_source);
  ASSERT_EQ(0, server_to_compare2 == data_source);
  ASSERT_EQ(0, server_to_compare3 == data_source);
  ASSERT_EQ(0, server_to_compare4 == data_source);
  ASSERT_EQ(0, server_to_compare1 == server_addr);
  ASSERT_EQ(1, server_to_compare2 == server_addr);
  ASSERT_EQ(0, server_to_compare3 == server_addr);
  ASSERT_EQ(0, server_to_compare4 == server_addr);
  ASSERT_EQ(2, orig_paxos_replica_number);
  ASSERT_EQ(3, new_paxos_replica_number);
  arg.reset();

  ret = arg.init(" tenant_id = 1002 , ls_id = 1001 , replica_type = readonly  , data_source = 100.88.107.212:2001 , server = 100.88.107.212:2002 , orig_paxos_replica_number = 2 , new_paxos_replica_number = 3 ", task_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(arg, tenant_id, ls_id, replica_type, data_source, server_addr, orig_paxos_replica_number, new_paxos_replica_number);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1002, tenant_id);
  ASSERT_EQ(1001, ls_id.id());
  ASSERT_EQ(REPLICA_TYPE_READONLY, replica_type);
  ASSERT_EQ(1, server_to_compare1 == data_source);
  ASSERT_EQ(0, server_to_compare2 == data_source);
  ASSERT_EQ(0, server_to_compare3 == data_source);
  ASSERT_EQ(0, server_to_compare4 == data_source);
  ASSERT_EQ(0, server_to_compare1 == server_addr);
  ASSERT_EQ(1, server_to_compare2 == server_addr);
  ASSERT_EQ(0, server_to_compare3 == server_addr);
  ASSERT_EQ(0, server_to_compare4 == server_addr);
  ASSERT_EQ(2, orig_paxos_replica_number);
  ASSERT_EQ(3, new_paxos_replica_number);
  arg.reset();

}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
