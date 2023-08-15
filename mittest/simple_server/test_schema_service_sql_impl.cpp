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
#include <gmock/gmock.h>

#define USING_LOG_PREFIX SHARE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "share/schema/ob_schema_service_sql_impl.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using namespace share::schema;
using namespace common;

static const int64_t TOTAL_NUM = 110;
static uint64_t g_tenant_id;
static ObSEArray<uint64_t, TOTAL_NUM> g_table_ids;

class TestSchemaServiceSqlImpl : public unittest::ObSimpleClusterTestBase
{
public:
  TestSchemaServiceSqlImpl() : unittest::ObSimpleClusterTestBase("test_schema_service_sql_impl") {}
  int batch_create_table(ObMySQLProxy &sql_proxy, const int64_t TOTAL_NUM, ObIArray<uint64_t> &table_ids);
};

int TestSchemaServiceSqlImpl::batch_create_table(
    ObMySQLProxy &sql_proxy,
    const int64_t TOTAL_NUM,
    ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  table_ids.reset();
  ObSqlString sql;
  // batch create table
  int64_t affected_rows = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < TOTAL_NUM; ++i) {
    sql.reset();
    if (OB_FAIL(sql.assign_fmt("create table t%ld(c1 int)", i))) {
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    }
  }
  // batch get table_id
  sql.reset();
  if (OB_FAIL(sql.assign_fmt("select table_id from oceanbase.__all_table where table_name in ("))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < TOTAL_NUM; ++i) {
      if (OB_FAIL(sql.append_fmt("%s't%ld'", 0 == i ? "" : ",", i))) {}
    }
    if (FAILEDx(sql.append_fmt(") order by table_id"))) {};
  }
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    if (OB_FAIL(table_ids.reserve(TOTAL_NUM))) {
    } else if (OB_UNLIKELY(!is_valid_tenant_id(g_tenant_id))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(sql_proxy.read(result, sql.ptr()))) {
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      sqlclient::ObMySQLResult &res = *result.get_result();
      uint64_t table_id = OB_INVALID_ID;
      while(OB_SUCC(ret) && OB_SUCC(res.next())) {
        EXTRACT_INT_FIELD_MYSQL(res, "table_id", table_id, uint64_t);
        if (OB_FAIL(table_ids.push_back(table_id))) {}
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to generate data", K(sql));
      }
    }
  }
  return ret;
}

TEST_F(TestSchemaServiceSqlImpl, prepare_data)
{
  g_tenant_id = OB_INVALID_TENANT_ID;

  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ASSERT_EQ(OB_SUCCESS, batch_create_table(sql_proxy, TOTAL_NUM, g_table_ids));
}

TEST_F(TestSchemaServiceSqlImpl, test_get_table_latest_schema_versions)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(TOTAL_NUM, g_table_ids.count());
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_TRUE(OB_NOT_NULL(GCTX.schema_service_));
  ObSchemaService *schema_service = GCTX.schema_service_->get_schema_service();
  ASSERT_TRUE(OB_NOT_NULL(schema_service));

  // 1.test batch get
  ObSEArray<ObTableLatestSchemaVersion, TOTAL_NUM> schema_versions;
  const int64_t small_num = ObSchemaServiceSQLImpl::MAX_IN_QUERY_PER_TIME - 1;
  const int64_t batch_num = ObSchemaServiceSQLImpl::MAX_IN_QUERY_PER_TIME;
  const int64_t big_num = TOTAL_NUM;
  ObSEArray<uint64_t, small_num> small_table_ids;
  ObSEArray<uint64_t, batch_num> batch_table_ids;
  for (int64_t i = 0; i < small_num; ++i) {
    ASSERT_EQ(OB_SUCCESS, small_table_ids.push_back(g_table_ids.at(i)));
  }
  for (int64_t i = 0; i < batch_num; ++i) {
    ASSERT_EQ(OB_SUCCESS, batch_table_ids.push_back(g_table_ids.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, schema_service->get_table_latest_schema_versions(inner_sql_proxy, g_tenant_id, small_table_ids, schema_versions));
  ASSERT_TRUE(schema_versions.count() == small_num);
  schema_versions.reset();
  ASSERT_EQ(OB_SUCCESS, schema_service->get_table_latest_schema_versions(inner_sql_proxy, g_tenant_id, batch_table_ids, schema_versions));
  ASSERT_TRUE(schema_versions.count() == batch_num);
  schema_versions.reset();
  ASSERT_EQ(OB_SUCCESS, schema_service->get_table_latest_schema_versions(inner_sql_proxy, g_tenant_id, g_table_ids, schema_versions));
  ASSERT_TRUE(schema_versions.count() == TOTAL_NUM);

  // 2.test table latest schema version
  int64_t t0_old_schema_version = schema_versions.at(0).get_schema_version();
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ObSqlString sql;
  int64_t affected_rows = 0;
  // 2.1 increase table schema version
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter table t0 add column c2 int"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ObArray<uint64_t> t0_table_id;
  ObArray<ObTableLatestSchemaVersion> t0_latest_schema_version;
  ASSERT_EQ(OB_SUCCESS, t0_table_id.push_back(g_table_ids.at(0)));
  ASSERT_EQ(OB_SUCCESS, schema_service->get_table_latest_schema_versions(inner_sql_proxy, g_tenant_id, t0_table_id, t0_latest_schema_version));
  ASSERT_TRUE(t0_latest_schema_version.count() == 1);
  ASSERT_TRUE(t0_latest_schema_version.at(0).get_schema_version() > t0_old_schema_version);
  ASSERT_TRUE(!t0_latest_schema_version.at(0).is_deleted());
  t0_old_schema_version = t0_latest_schema_version.at(0).get_schema_version();
  // 2.2 test is_deleted
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("drop table t0"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  t0_latest_schema_version.reset();
  ASSERT_EQ(OB_SUCCESS, schema_service->get_table_latest_schema_versions(inner_sql_proxy, g_tenant_id, t0_table_id, t0_latest_schema_version));
  ASSERT_TRUE(t0_latest_schema_version.count() == 1);
  ASSERT_TRUE(t0_latest_schema_version.at(0).get_schema_version() > t0_old_schema_version);
  ASSERT_TRUE(t0_latest_schema_version.at(0).is_deleted());
  // 2.3 schema version of other table has no change
  ObSEArray<ObTableLatestSchemaVersion, TOTAL_NUM> latest_schema_versions;
  ASSERT_EQ(OB_SUCCESS, schema_service->get_table_latest_schema_versions(inner_sql_proxy, g_tenant_id, g_table_ids, latest_schema_versions));
  ASSERT_TRUE(latest_schema_versions.count() == TOTAL_NUM);
  for (int64_t i = 0; i < TOTAL_NUM; ++i) {
    if (latest_schema_versions.at(i).get_table_id() != t0_table_id.at(0)) {
      ASSERT_TRUE(latest_schema_versions.at(i).get_table_id() == schema_versions.at(i).get_table_id());
      ASSERT_TRUE(latest_schema_versions.at(i).get_schema_version() == schema_versions.at(i).get_schema_version());
      ASSERT_TRUE(latest_schema_versions.at(i).is_deleted() == schema_versions.at(i).is_deleted());
    }
  }

  // 3.test error
  schema_versions.reset();
  ObArray<uint64_t> empty_table_ids;
  ObArray<uint64_t> invalid_table_ids;
  ObArray<uint64_t> dup_table_ids;
  ASSERT_EQ(OB_SUCCESS, invalid_table_ids.push_back(OB_INVALID_ID));
  ASSERT_EQ(OB_SUCCESS, dup_table_ids.push_back(g_table_ids.at(0)));
  ASSERT_EQ(OB_SUCCESS, dup_table_ids.push_back(g_table_ids.at(0)));

  ASSERT_EQ(OB_INVALID_ARGUMENT, schema_service->get_table_latest_schema_versions(inner_sql_proxy, 123, t0_table_id, schema_versions));
  ASSERT_EQ(OB_INVALID_ARGUMENT, schema_service->get_table_latest_schema_versions(inner_sql_proxy, g_tenant_id, empty_table_ids, schema_versions));
  ASSERT_EQ(OB_INVALID_ARGUMENT, schema_service->get_table_latest_schema_versions(inner_sql_proxy, g_tenant_id, invalid_table_ids, schema_versions));
  schema_versions.reset();
  ASSERT_EQ(OB_SUCCESS, schema_service->get_table_latest_schema_versions(inner_sql_proxy, g_tenant_id, dup_table_ids, schema_versions));
  ASSERT_TRUE(schema_versions.count() == 1);
  ASSERT_TRUE(dup_table_ids.at(0) == schema_versions.at(0).get_table_id());
}

} // namespace rootserver
} // namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
