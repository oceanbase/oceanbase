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
#include "lib/string/ob_sql_string.h" // ObSqlString
#include "lib/mysqlclient/ob_mysql_proxy.h" // ObISqlClient, SMART_VAR
#include "observer/ob_sql_client_decorator.h" // ObSQLClientRetryWeak
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "rootserver/ob_root_utils.h"
#include "share/ob_server_table_operator.h"
#include "share/ob_zone_table_operation.h"
#define SQL_PROXY (get_curr_simple_server().get_observer().get_mysql_proxy())
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
class TestGetStoppedZoneList : public unittest::ObSimpleClusterTestBase
{
public:
  TestGetStoppedZoneList() : unittest::ObSimpleClusterTestBase("test_get_stopped_zone_list") {}
};
TEST_F(TestGetStoppedZoneList, GetStoppedZoneList)
{
  // empty zone z3 is stopped
  // server2 in z2 is stopped
  // stopped_zone_list should be z2, z3, stopped_server_list should be server2
  // have_other_stop_task is also tested
  ObServerInfoInTable server_info_in_table;
  ObAddr server2;
  ObServerTableOperator st_operator;
  int64_t affected_rows = 0;
  ObZone z2("z2");
  ObZone z3("z3");
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, st_operator.init(&SQL_PROXY));
  ASSERT_TRUE(server2.set_ip_addr("127.0.0.1", 11111));

  ASSERT_FALSE(rootserver::ObRootUtils::have_other_stop_task(GCONF.zone.str()));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system add zone z2"));
  ASSERT_EQ(OB_SUCCESS, SQL_PROXY.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  sql.reset();

  ASSERT_TRUE(rootserver::ObRootUtils::have_other_stop_task(GCONF.zone.str()));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system start zone z2"));
  ASSERT_EQ(OB_SUCCESS, SQL_PROXY.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));
  sql.reset();
  ASSERT_FALSE(rootserver::ObRootUtils::have_other_stop_task(GCONF.zone.str()));
  int ret = server_info_in_table.init(server2, 2, "z2", 15432, false, ObServerStatus::OB_SERVER_ACTIVE, "test_version", 5558888, 55555, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = st_operator.insert(SQL_PROXY, server_info_in_table);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(rootserver::ObRootUtils::have_other_stop_task(GCONF.zone.str()));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system add zone z3"));
  ASSERT_EQ(OB_SUCCESS, SQL_PROXY.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

  ObArray<ObZone> active_zone_list;
  ObArray<ObZone> inactive_zone_list;
  ASSERT_EQ(OB_SUCCESS, ObZoneTableOperation::get_active_zone_list(SQL_PROXY, active_zone_list));
  ASSERT_EQ(OB_SUCCESS, ObZoneTableOperation::get_inactive_zone_list(SQL_PROXY, inactive_zone_list));
  ASSERT_EQ(z3, inactive_zone_list.at(0));
  ASSERT_EQ(2, active_zone_list.count());

  ObArray<ObZone> stopped_zone_list;
  ObArray<ObAddr> stopped_server_list;
  ret = rootserver::ObRootUtils::get_stopped_zone_list(stopped_zone_list, stopped_server_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, stopped_server_list.count());
  ASSERT_EQ(server2, stopped_server_list.at(0));
  ASSERT_EQ(2, stopped_zone_list.count());
  ASSERT_TRUE(has_exist_in_array(stopped_zone_list, z2));
  ASSERT_TRUE(has_exist_in_array(stopped_zone_list, z3));
}
} // share
} // oceanbase
int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}