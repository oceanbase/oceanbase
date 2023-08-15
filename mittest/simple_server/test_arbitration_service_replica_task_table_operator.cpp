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
#include "share/arbitration_service/ob_arbitration_service_replica_task_table_operator.h"
#include "share/arbitration_service/ob_arbitration_service_replica_task_info.h"
#include "share/ob_define.h"                 //  for ObTaskID
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace common;

class TestArbitrationServiceReplicaTaskTableOperator : public unittest::ObSimpleClusterTestBase
{
public:
  TestArbitrationServiceReplicaTaskTableOperator() : unittest::ObSimpleClusterTestBase("test_arbitration_service_replica_task_table_operator") {}
protected:
  ObArbitrationServiceReplicaTaskTableOperator arb_service_replica_task_table_operator_;
};

TEST_F(TestArbitrationServiceReplicaTaskTableOperator, test_type)
{
  int ret = OB_SUCCESS;

  bool equal = false;
  int64_t create_time_us = ObTimeUtility::current_time();
  uint64_t tenant_id = 1002;
  share::ObLSID ls_id(1001);
  int64_t task_id = 1;
  ObString arbitration_service("127.0.0.1:1001");
  ObCurTraceId::init(GCONF.self_addr_);
  ObTaskId trace_id(*ObCurTraceId::get_trace_id());
  ObArbitrationServiceType addr_type(ObArbitrationServiceType::ADDR);
  ObString comment("generated for arbitraiton service");
  ObArbitrationServiceReplicaTaskInfo arbitration_service_replica_task_info;

  ObArbitrationServiceReplicaTaskType add_type(ObArbitrationServiceReplicaTaskType::ADD_REPLICA);
  ObArbitrationServiceReplicaTaskType remove_type(ObArbitrationServiceReplicaTaskType::REMOVE_REPLICA);
  ObArbitrationServiceReplicaTaskType invalid_type(ObArbitrationServiceReplicaTaskType::MAX_TYPE);

  // test normal task type
  arbitration_service_replica_task_info.reset();
  ret = arbitration_service_replica_task_info.build(
        create_time_us, tenant_id, ls_id, task_id, add_type, trace_id,
        arbitration_service, addr_type, comment);
  ASSERT_EQ(OB_SUCCESS, ret);

  arbitration_service_replica_task_info.reset();
  ret = arbitration_service_replica_task_info.build(
        create_time_us, tenant_id, ls_id, task_id, remove_type, trace_id,
        arbitration_service, addr_type, comment);
  ASSERT_EQ(OB_SUCCESS, ret);

  arbitration_service_replica_task_info.reset();
  ret = arbitration_service_replica_task_info.build(
        create_time_us, tenant_id, ls_id, task_id, invalid_type, trace_id,
        arbitration_service, addr_type, comment);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // test in mixed cases task type
  ObString mixed_add_type = "Add Replica";
  ObString mixed_remove_type = "Remove Replica";
  ObArbitrationServiceReplicaTaskType task_type_in_mixed_case;

  ret = task_type_in_mixed_case.parse_from_string(mixed_add_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  arbitration_service_replica_task_info.reset();
  ret = arbitration_service_replica_task_info.build(
        create_time_us, tenant_id, ls_id, task_id, task_type_in_mixed_case, trace_id,
        arbitration_service, addr_type, comment);
  ASSERT_EQ(OB_SUCCESS, ret);
  equal = false;
  equal = task_type_in_mixed_case == add_type;
  ASSERT_EQ(true, equal);

  ret = task_type_in_mixed_case.parse_from_string(mixed_remove_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  arbitration_service_replica_task_info.reset();
  ret = arbitration_service_replica_task_info.build(
        create_time_us, tenant_id, ls_id, task_id, task_type_in_mixed_case, trace_id,
        arbitration_service, addr_type, comment);
  ASSERT_EQ(OB_SUCCESS, ret);
  equal = false;
  equal = task_type_in_mixed_case == arbitration_service_replica_task_info.get_task_type();
  ASSERT_EQ(true, equal);
}


TEST_F(TestArbitrationServiceReplicaTaskTableOperator, test_info)
{
  int ret = OB_SUCCESS;

  bool equal = false;
  int64_t create_time_us = ObTimeUtility::current_time();
  uint64_t tenant_id = 1001;
  uint64_t invalid_tenant_id = OB_INVALID_ID;
  share::ObLSID ls_id(1);
  share::ObLSID invalid_ls_id(1001);
  int64_t task_id = 1;
  ObCurTraceId::init(GCONF.self_addr_);
  ObTaskId trace_id(*ObCurTraceId::get_trace_id());
  ObArbitrationServiceReplicaTaskType add_task_type(ObArbitrationServiceReplicaTaskType::ADD_REPLICA);
  ObArbitrationServiceReplicaTaskType remove_task_type(ObArbitrationServiceReplicaTaskType::REMOVE_REPLICA);
  ObArbitrationServiceReplicaTaskType invalid_task_type(ObArbitrationServiceReplicaTaskType::INVALID_TYPE);
  ObString arbitration_service("127.0.0.1:1001");
  ObString invalid_arbitration_service("11111");
  ObArbitrationServiceType addr_type(ObArbitrationServiceType::ADDR);
  ObArbitrationServiceType url_type(ObArbitrationServiceType::URL);
  ObArbitrationServiceType invalid_type(ObArbitrationServiceType::INVALID_ARBITRATION_SERVICE_TYPE);
  ObString comment("generated for arbitraiton service");
  ObString invalid_comment("");
  ObArbitrationServiceReplicaTaskInfo arbitration_service_replica_task_info;

  // 1. test task info build argument
  arbitration_service_replica_task_info.reset();
  ret = arbitration_service_replica_task_info.build(
        create_time_us, invalid_tenant_id, ls_id, task_id, add_task_type, trace_id,
        arbitration_service, addr_type, comment);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  arbitration_service_replica_task_info.reset();
  ret = arbitration_service_replica_task_info.build(
        create_time_us, tenant_id, invalid_ls_id, task_id, add_task_type, trace_id,
        arbitration_service, addr_type, comment);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  arbitration_service_replica_task_info.reset();
  ret = arbitration_service_replica_task_info.build(
        create_time_us, tenant_id, ls_id, task_id, add_task_type, trace_id,
        arbitration_service, addr_type, comment);
  ASSERT_EQ(OB_SUCCESS, ret);

  arbitration_service_replica_task_info.reset();
  ret = arbitration_service_replica_task_info.build(
        create_time_us, tenant_id, ls_id, task_id, remove_task_type, trace_id,
        arbitration_service, addr_type, comment);
  ASSERT_EQ(OB_SUCCESS, ret);

  arbitration_service_replica_task_info.reset();
  ret = arbitration_service_replica_task_info.build(
        create_time_us, tenant_id, ls_id, task_id, invalid_task_type, trace_id,
        arbitration_service, addr_type, comment);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  arbitration_service_replica_task_info.reset();
  ret = arbitration_service_replica_task_info.build(
        create_time_us, tenant_id, ls_id, task_id, add_task_type, trace_id,
        invalid_arbitration_service, addr_type, comment);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  arbitration_service_replica_task_info.reset();
  ret = arbitration_service_replica_task_info.build(
        create_time_us, tenant_id, ls_id, task_id, add_task_type, trace_id,
        arbitration_service, url_type, comment);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  arbitration_service_replica_task_info.reset();
  ret = arbitration_service_replica_task_info.build(
        create_time_us, tenant_id, ls_id, task_id, add_task_type, trace_id,
        arbitration_service, invalid_type, comment);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  arbitration_service_replica_task_info.reset();
  ret = arbitration_service_replica_task_info.build(
        create_time_us, tenant_id, ls_id, task_id, add_task_type, trace_id,
        arbitration_service, addr_type, invalid_comment);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // 2. test assign and is_equal
  ObArbitrationServiceReplicaTaskInfo arbitration_service_replica_task_info_to_compare;
  // 2.1 assign a invalid info
  arbitration_service_replica_task_info.reset();
  arbitration_service_replica_task_info_to_compare.reset();
  ret = arbitration_service_replica_task_info_to_compare.assign(arbitration_service_replica_task_info);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  // 2.2 assign a valid info
  arbitration_service_replica_task_info.reset();
  arbitration_service_replica_task_info_to_compare.reset();

  ret = arbitration_service_replica_task_info.build(
        create_time_us, tenant_id, ls_id, task_id, add_task_type, trace_id,
        arbitration_service, addr_type, comment);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arbitration_service_replica_task_info_to_compare.assign(arbitration_service_replica_task_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  equal = arbitration_service_replica_task_info_to_compare.is_equal(arbitration_service_replica_task_info);
  ASSERT_EQ(true, equal);
}

TEST_F(TestArbitrationServiceReplicaTaskTableOperator, test_operator)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();

  // 1. create initial member
  int64_t create_time_us = ObTimeUtility::current_time();
  uint64_t sys_tenant_id = 1;
  int64_t user_tenant_id = OB_INVALID_ID;
  ObSqlString sql;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign("CREATE RESOURCE UNIT unit_1 MAX_CPU 2, MEMORY_SIZE='2G'"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql.assign("CREATE RESOURCE POOL pool_1 UNIT = 'unit_1', UNIT_NUM = 1"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql.assign("CREATE TENANT mysql RESOURCE_POOL_LIST = ('pool_1') set ob_tcp_invited_nodes='%'"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ObMySQLProxy::MySQLResult res;
  ASSERT_EQ(OB_SUCCESS, sql.assign("SELECT tenant_id from __all_tenant WHERE tenant_name = 'mysql'"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
  sqlclient::ObMySQLResult *result = res.get_result();
  ASSERT_NE(nullptr, result);
  ASSERT_EQ(OB_SUCCESS, result->next());
  ASSERT_EQ(OB_SUCCESS, result->get_int("tenant_id", user_tenant_id));

  uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  uint64_t invalid_tenant_id = OB_INVALID_ID;
  share::ObLSID sys_ls_id(1);
  share::ObLSID user_ls_id(1001);
  share::ObLSID invalid_ls_id(0);

  ObCurTraceId::init(GCONF.self_addr_);
  ObString arbitration_service("127.0.0.1:1001");
  ObTaskId trace_id(*ObCurTraceId::get_trace_id());
  ObArbitrationServiceType addr_type(ObArbitrationServiceType::ArbitrationServiceType::ADDR);
  ObArbitrationServiceReplicaTaskType task_type(ObArbitrationServiceReplicaTaskType::ADD_REPLICA);
  ObString comment("generated for arbitraiton service");
  bool lock_line = true;
  bool equal = false;
  int64_t task_id = 1;
  ObArbitrationServiceReplicaTaskInfo arbitration_service_replica_task_info;
  ret = arbitration_service_replica_task_info.build(
        create_time_us, sys_tenant_id, sys_ls_id, task_id, task_type, trace_id,
        arbitration_service, addr_type, comment);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObArbitrationServiceReplicaTaskTableOperator::ObArbitrationServiceReplicaTaskInfoList task_infos;

  // 2. test get_all_tasks
  // 2.1 get with invalid argument
  task_infos.reset();
  ret = arb_service_replica_task_table_operator_.get_all_tasks(sql_proxy, OB_INVALID_ID, lock_line, task_infos);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  // 2.2 get from empty table
  task_infos.reset();
  ret = arb_service_replica_task_table_operator_.get_all_tasks(sql_proxy, sys_tenant_id, lock_line, task_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, 0 == task_infos.count());

  task_infos.reset();
  ret = arb_service_replica_task_table_operator_.get_all_tasks(sql_proxy, meta_tenant_id, lock_line, task_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, 0 == task_infos.count());

  task_infos.reset();
  ret = arb_service_replica_task_table_operator_.get_all_tasks(sql_proxy, user_tenant_id, lock_line, task_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, 0 == task_infos.count());

  // 3. insert info
  // 3.1 insert with invalid argument
  ObArbitrationServiceReplicaTaskInfo invalid_arbitration_service_replica_task_info;
  invalid_arbitration_service_replica_task_info.reset();
  ret = arb_service_replica_task_table_operator_.insert(sql_proxy, invalid_arbitration_service_replica_task_info);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  // 3.2 insert a valid info
  ret = arb_service_replica_task_table_operator_.insert(sql_proxy, arbitration_service_replica_task_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  task_infos.reset();
  ret = arb_service_replica_task_table_operator_.get_all_tasks(sql_proxy, meta_tenant_id, lock_line, task_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, 0 == task_infos.count());

  task_infos.reset();
  ret = arb_service_replica_task_table_operator_.get_all_tasks(sql_proxy, user_tenant_id, lock_line, task_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, 0 == task_infos.count());

  task_infos.reset();
  ret = arb_service_replica_task_table_operator_.get_all_tasks(sql_proxy, sys_tenant_id, lock_line, task_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, 1 == task_infos.count());
  equal = task_infos.at(0).is_equal(arbitration_service_replica_task_info);
  ASSERT_EQ(true, equal);
  // 3.3 insert a same task again
  ret = arb_service_replica_task_table_operator_.insert(sql_proxy, arbitration_service_replica_task_info);
  ASSERT_EQ(OB_ENTRY_EXIST, ret);

  // 4. remove info
  // 4.1 test remove with invalid argument
  ret = arb_service_replica_task_table_operator_.remove(sql_proxy, invalid_tenant_id, sys_ls_id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = arb_service_replica_task_table_operator_.remove(sql_proxy, sys_tenant_id, invalid_ls_id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = arb_service_replica_task_table_operator_.remove(sql_proxy, sys_tenant_id, user_ls_id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ret = arb_service_replica_task_table_operator_.remove(sql_proxy, meta_tenant_id, user_ls_id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // 4.2 remove a not exist info
  ret = arb_service_replica_task_table_operator_.remove(sql_proxy, user_tenant_id, sys_ls_id);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  task_infos.reset();
  ret = arb_service_replica_task_table_operator_.get_all_tasks(sql_proxy, sys_tenant_id, lock_line, task_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, 1 == task_infos.count());
  // 4.3 remove a info
  ret = arb_service_replica_task_table_operator_.remove(sql_proxy, sys_tenant_id, sys_ls_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  task_infos.reset();
  ret = arb_service_replica_task_table_operator_.get_all_tasks(sql_proxy, sys_tenant_id, lock_line, task_infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, 0 == task_infos.count());

  ret = arb_service_replica_task_table_operator_.insert_history(sql_proxy, arbitration_service_replica_task_info, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = arb_service_replica_task_table_operator_.insert_history(sql_proxy, arbitration_service_replica_task_info, 0);
  ASSERT_EQ(OB_ENTRY_EXIST, ret);

  ret = arb_service_replica_task_table_operator_.insert_history(sql_proxy, invalid_arbitration_service_replica_task_info, 0);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
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
