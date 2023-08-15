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

class TestChangeArbServiceStatus : public unittest::ObSimpleClusterTestBase
{
public:
  TestChangeArbServiceStatus() : unittest::ObSimpleClusterTestBase("test_change_arb_service_status") {}
};

TEST_F(TestChangeArbServiceStatus, test_change_arb_service_status)
{
  int ret = OB_SUCCESS;
  // 0. prepare initial members
  common::ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();
  int64_t affected_rows = 0;
  ObSqlString sql;
  int64_t tenant_id = OB_INVALID_ID;
  int64_t tmp_cnt = 0;

  // 1. create tenant without arbitration service
  ASSERT_EQ(OB_SUCCESS, sql.assign("create resource unit arbitration_unit max_cpu 2, memory_size '2G', log_disk_size='2G';"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql.assign("create resource pool arbitration_pool unit = 'arbitration_unit', unit_num = 1;"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql.assign("create tenant arbitration_tenant_1 resource_pool_list=('arbitration_pool');"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql.assign("select tenant_id "
                                   "from __all_tenant "
                                   "where tenant_name = 'arbitration_tenant_1';"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("tenant_id", tenant_id));
  }

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as cnt "
                                       "from __all_tenant "
                                       "where (tenant_id = %ld or tenant_id = %ld) "
                                       "and arbitration_service_status = 'DISABLED';", tenant_id, gen_meta_tenant_id(tenant_id)));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result1 = res1.get_result();
    ASSERT_NE(nullptr, result1);
    ASSERT_EQ(OB_SUCCESS, result1->next());
    ASSERT_EQ(OB_SUCCESS, result1->get_int("cnt", tmp_cnt));
    ASSERT_EQ(2, tmp_cnt);
  }

  // 2. enable tenant's arbitration service without arbitration service addr
  ASSERT_EQ(OB_SUCCESS, sql.assign("alter tenant arbitration_tenant_1 enable_arbitration_service = true;"));
  ASSERT_EQ(OB_OP_NOT_ALLOW, sql_proxy.write(sql.ptr(), affected_rows));

  // 3. enable tenant's arbitration service with arbitration service addr
  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system add arbitration service '127.0.0.1:10000';"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign("select count(*) as cnt from __all_arbitration_service;"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res2, sql.ptr()));
    sqlclient::ObMySQLResult *result2 = res2.get_result();
    ASSERT_NE(nullptr, result2);
    ASSERT_EQ(OB_SUCCESS, result2->next());
    ASSERT_EQ(OB_SUCCESS, result2->get_int("cnt", tmp_cnt));
    ASSERT_EQ(1, tmp_cnt);
  }

  // 1F not allow to enable arb service
  ASSERT_EQ(OB_SUCCESS, sql.assign("alter tenant arbitration_tenant_1 enable_arbitration_service = true;"));
  ASSERT_EQ(OB_OP_NOT_ALLOW, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as cnt "
                                       "from __all_tenant "
                                       "where (tenant_id = %ld or tenant_id = %ld) "
                                       "and arbitration_service_status = 'ENABLING';", tenant_id, gen_meta_tenant_id(tenant_id)));
  SMART_VAR(ObMySQLProxy::MySQLResult, res3) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res3, sql.ptr()));
    sqlclient::ObMySQLResult *result3 = res3.get_result();
    ASSERT_NE(nullptr, result3);
    ASSERT_EQ(OB_SUCCESS, result3->next());
    ASSERT_EQ(OB_SUCCESS, result3->get_int("cnt", tmp_cnt));
    ASSERT_EQ(0, tmp_cnt);
  }

  // 5. disable tenant's arbitration service when tenant already in disabled status
  ASSERT_EQ(OB_SUCCESS, sql.assign("alter tenant arbitration_tenant_1 enable_arbitration_service = false;"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as cnt "
                                       "from __all_tenant "
                                       "where (tenant_id = %ld or tenant_id = %ld) "
                                       "and arbitration_service_status = 'DISABLING';", tenant_id, gen_meta_tenant_id(tenant_id)));
  SMART_VAR(ObMySQLProxy::MySQLResult, res4) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res4, sql.ptr()));
    sqlclient::ObMySQLResult *result4 = res4.get_result();
    ASSERT_NE(nullptr, result4);
    ASSERT_EQ(OB_SUCCESS, result4->next());
    ASSERT_EQ(OB_SUCCESS, result4->get_int("cnt", tmp_cnt));
    ASSERT_EQ(0, tmp_cnt);
  }

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
