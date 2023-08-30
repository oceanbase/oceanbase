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
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_ls_id.h"
#include "share/unit/ob_unit_info.h"
#include "share/ls/ob_ls_status_operator.h"
#include "rootserver/ob_primary_ls_service.h"



namespace oceanbase
{
using namespace unittest;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace rootserver;
using namespace common;
class TestStandbyBalance : public unittest::ObSimpleClusterTestBase
{
public:
  TestStandbyBalance() : unittest::ObSimpleClusterTestBase("test_balance_operator") {}
protected:

  uint64_t tenant_id_;
};

TEST_F(TestStandbyBalance, BalanceLSGroup)
{
  int ret = OB_SUCCESS;
  tenant_id_ = 1;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema *tenant_schema = NULL;
  share::schema::ObTenantSchema tenant_schema1;
  if (OB_FAIL(get_curr_observer().get_schema_service().get_tenant_schema_guard(
          OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(OB_SYS_TENANT_ID, tenant_schema))) {
    LOG_WARN("failed to get tenant ids", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(tenant_schema1.assign(*tenant_schema))) {
    LOG_WARN("failed to assign tenant schema", KR(ret));
  } else {
    tenant_schema1.set_tenant_id(tenant_id_);
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ObZone z1("z1");
  int64_t affected_row = 0;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("sys", "oceanbase"));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  //2个unit group，四个日志流组
  uint64_t u1 = 1001;
  uint64_t u2 = 1002;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into __all_unit(unit_id, unit_group_id, resource_pool_id, zone, svr_ip, svr_port, migrate_from_svr_ip, migrate_from_svr_port, status) values(%lu, %lu, 1, 'z1', '127.0.0.1', 2882, ' ', 0, 'ACTIVE')", u1, u1));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_row));
  ASSERT_EQ(1, affected_row);

  uint64_t lg1 = 1001, lg2 = 1002, lg3 = 1003, lg4 = 1004;
  int64_t ls1 = 1001, ls2 = 1002, ls3 = 1003, ls4 = 1004;
  //4个日志流组都在一个ug上面
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into __all_ls_status(tenant_id, ls_id, status, ls_group_id , unit_group_id, primary_zone) values(%ld, %lu, 'NORMAL', %lu, %lu, '%s')", tenant_id_, ls1, lg1, u1, z1.ptr()));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_row));
  ASSERT_EQ(1, affected_row);
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into __all_ls_status(tenant_id, ls_id, status, ls_group_id , unit_group_id, primary_zone) values(%ld, %lu, 'NORMAL', %lu, %lu, '%s')", tenant_id_, ls2, lg2, u1, z1.ptr()));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_row));
  ASSERT_EQ(1, affected_row);
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into __all_ls_status(tenant_id, ls_id, status, ls_group_id , unit_group_id, primary_zone) values(%ld, %lu, 'NORMAL', %lu, %lu, '%s')", tenant_id_, ls3, lg3, u1, z1.ptr()));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_row));
  ASSERT_EQ(1, affected_row);
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into __all_ls_status(tenant_id, ls_id, status, ls_group_id , unit_group_id, primary_zone) values(%ld, %lu, 'NORMAL', %lu, %lu, '%s')", tenant_id_, ls4, lg4, u1, z1.ptr()));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(OB_SYS_TENANT_ID, sql.ptr(), affected_row));
  ASSERT_EQ(1, affected_row);
  ObTenantLSInfo tenant_stat(get_curr_observer().get_gctx().sql_proxy_, &tenant_schema1, tenant_id_);
  bool is_balanced = false;
  ASSERT_EQ(OB_SUCCESS, ObLSServiceHelper::balance_ls_group(true /*need_execute_balance*/, tenant_stat, is_balanced));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select ls_id, ls_group_id, unit_group_id from __all_ls_status where ls_id != 1 order by ls_id"));
  int64_t ls_id = 0;
  uint64_t ls_group_id = 0;
  uint64_t unit_group_id = 0;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    //1
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", ls_id));
    ASSERT_EQ(ls1, ls_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("ls_group_id", ls_group_id));
    ASSERT_EQ(lg1, ls_group_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("unit_group_id", unit_group_id));
    ASSERT_EQ(u1, unit_group_id);
    //2
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", ls_id));
    ASSERT_EQ(ls2, ls_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("ls_group_id", ls_group_id));
    ASSERT_EQ(lg2, ls_group_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("unit_group_id", unit_group_id));
    ASSERT_EQ(u1, unit_group_id);
    //3
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", ls_id));
    ASSERT_EQ(ls3, ls_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("ls_group_id", ls_group_id));
    ASSERT_EQ(lg3, ls_group_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("unit_group_id", unit_group_id));
    ASSERT_EQ(1, unit_group_id);
    //4
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", ls_id));
    ASSERT_EQ(ls4, ls_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("ls_group_id", ls_group_id));
    ASSERT_EQ(lg4, ls_group_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("unit_group_id", unit_group_id));
    ASSERT_EQ(1, unit_group_id);
  }

  //case 2: 2, 2 -> 2, 2, 1
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into __all_unit(unit_id, unit_group_id, resource_pool_id, zone, svr_ip, svr_port, migrate_from_svr_ip, migrate_from_svr_port, status) values(%lu, %lu, 1, 'z1', '127.0.0.1', 2882, ' ', 0, 'ACTIVE')", u2, u2));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_row));
  ASSERT_EQ(1, affected_row);
  ASSERT_EQ(OB_SUCCESS, ObLSServiceHelper::balance_ls_group(true /*need_execute_balance*/, tenant_stat, is_balanced));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select ls_id, ls_group_id, unit_group_id from __all_ls_status where ls_id != 1 order by ls_id"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    //1
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", ls_id));
    ASSERT_EQ(ls1, ls_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("ls_group_id", ls_group_id));
    ASSERT_EQ(lg1, ls_group_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("unit_group_id", unit_group_id));
    ASSERT_EQ(u1, unit_group_id);
    //2
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", ls_id));
    ASSERT_EQ(ls2, ls_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("ls_group_id", ls_group_id));
    ASSERT_EQ(lg2, ls_group_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("unit_group_id", unit_group_id));
    ASSERT_EQ(u1, unit_group_id);
    //3
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", ls_id));
    ASSERT_EQ(ls3, ls_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("ls_group_id", ls_group_id));
    ASSERT_EQ(lg3, ls_group_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("unit_group_id", unit_group_id));
    ASSERT_EQ(1, unit_group_id);
    //4
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", ls_id));
    ASSERT_EQ(ls4, ls_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("ls_group_id", ls_group_id));
    ASSERT_EQ(lg4, ls_group_id);
    ASSERT_EQ(OB_SUCCESS, result->get_uint("unit_group_id", unit_group_id));
    ASSERT_EQ(u2, unit_group_id);
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
