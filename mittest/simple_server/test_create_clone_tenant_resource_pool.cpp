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

class TestCreateCloneTenantResourcePool : public unittest::ObSimpleClusterTestBase
{
public:
  TestCreateCloneTenantResourcePool() : unittest::ObSimpleClusterTestBase("test_create_clone_tenant_resource_pool") {}
};

TEST_F(TestCreateCloneTenantResourcePool, test_create_tenant_resource_pool)
{
  int ret = OB_SUCCESS;
  // 0. prepare initial members
  common::ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();
  rootserver::ObRootService *root_service = get_curr_observer().get_gctx().root_service_;
  int64_t tmp_cnt = 0;

  // 1. create unit config
  int64_t affected_rows = 0;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign("create resource unit clone_tenant_unit max_cpu 2, memory_size '2G', log_disk_size='2G';"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  // 2. construct clone tenant arg
  ObString pool_name = "resource_pool_for_clone_tenant_1001";
  ObString unit_config_name = "clone_tenant_unit";
  uint64_t source_tenant_id = 1;
  uint64_t resource_pool_id = 2001;
  ObCloneResourcePoolArg clone_tenant_arg;
  ret = clone_tenant_arg.init(pool_name, unit_config_name, source_tenant_id, resource_pool_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. execute clone tenant
  ret = root_service->clone_resource_pool(clone_tenant_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 4. check __all_resource_pool inserted new rows
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as cnt "
                                       "from __all_resource_pool "
                                       "where name like '%%%.*s%%' "
                                       "and unit_config_id = (select unit_config_id "
                                                             "from __all_unit_config "
                                                             "where name like '%%clone_tenant_unit%%');",
                                        pool_name.length(), pool_name.ptr()));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", tmp_cnt));
    ASSERT_EQ(1, tmp_cnt);
  }

  // 5. check __all_unit inserted new rows
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as cnt "
                                      "from __all_unit "
                                      "where unit_group_id = 0 "
                                      "and resource_pool_id = %lu;", resource_pool_id));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", tmp_cnt));
    ASSERT_EQ(1, tmp_cnt);
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
