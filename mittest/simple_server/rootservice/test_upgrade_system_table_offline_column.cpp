// owner: linxun.wf
// owner group: rs

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define USING_LOG_PREFIX RS

#include "env/ob_simple_cluster_test_base.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace
{

const int64_t UPGRADE_JOB_TIMEOUT_US = 180 * 1000 * 1000;
const int64_t POLL_INTERVAL_US = 100 * 1000;

int query_int(ObMySQLProxy &sql_proxy,
              const char *sql,
              const char *column_name,
              int64_t &value)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql_proxy.read(res, sql))) {
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
      } else if (OB_FAIL(result->get_int(column_name, value))) {
      }
    }
  }
  return ret;
}

}

class TestUpgradeSystemTableOfflineColumn : public ObSimpleClusterTestBase
{
};

TEST_F(TestUpgradeSystemTableOfflineColumn, reject_varchar_to_longtext)
{
  ObSchemaGetterGuard schema_guard;
  int ret = get_curr_observer().get_schema_service().get_tenant_schema_guard(
      OB_SYS_TENANT_ID, schema_guard);
  ASSERT_EQ(OB_SUCCESS, ret);

  // get original schema
  const ObTableSchema *installed_schema = nullptr;
  ret = schema_guard.get_table_schema(OB_SYS_TENANT_ID, OB_ALL_ROOTSERVICE_JOB_TID, installed_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, installed_schema);

  // confirm the column extra_info is varchar and length is MAX_ROOTSERVICE_JOB_EXTRA_INFO_LENGTH
  const ObColumnSchemaV2 *installed_column = installed_schema->get_column_schema("extra_info");
  ASSERT_NE(nullptr, installed_column);
  ASSERT_EQ(ObVarcharType, installed_column->get_data_type());
  ASSERT_EQ(MAX_ROOTSERVICE_JOB_EXTRA_INFO_LENGTH, installed_column->get_data_length());

  const int64_t schema_version = installed_schema->get_schema_version();
  const int64_t progressive_merge_round = installed_schema->get_progressive_merge_round();

  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  int64_t affected_rows = 0;
  ret = sql_proxy.write("alter system set enable_sys_table_ddl = true", affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t previous_job_id = 0;
  ret = query_int(
      sql_proxy,
      "select ifnull(max(job_id), 0) as job_id "
      "from oceanbase.__all_rootservice_job "
      "where job_type = 'UPGRADE_ALL' and tenant_id = 1",
      "job_id",
      previous_job_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSqlString finished_job_sql;
  ret = finished_job_sql.assign_fmt(
      "select count(*) as job_count "
      "from oceanbase.__all_rootservice_job "
      "where job_type = 'UPGRADE_ALL' and tenant_id = 1 "
      "and job_id > %ld and job_status in ('SUCCESS', 'FAILED')",
      previous_job_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSqlString failed_job_sql;
  ret = failed_job_sql.assign_fmt(
      "select count(*) as job_count "
      "from oceanbase.__all_rootservice_job "
      "where job_type = 'UPGRADE_ALL' and tenant_id = 1 "
      "and job_id > %ld and job_status = 'FAILED' "
      "and result_code = -4007",
      previous_job_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  const char *event_sql =
      "select count(*) as event_count "
      "from oceanbase.__all_rootservice_event_history "
      "where module = 'upgrade' "
      "and event = 'finish_upgrade_system_table' "
      "and name1 = 'tenant_id' and value1 = '1' "
      "and name3 = 'tmp_ret' and value3 = '-4007'";

  int64_t previous_event_count = 0;
  ret = query_int(sql_proxy, event_sql, "event_count", previous_event_count);
  ASSERT_EQ(OB_SUCCESS, ret);

  // open errsim
  ret = sql_proxy.write(
      "alter system set_tp "
      "tp_name = ERRSIM_UPGRADE_SYSTEM_TABLE_OFFLINE_COLUMN, "
      "error_code = 1, frequency = 1",
      affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);

  // run upgrade all
  int upgrade_ret = sql_proxy.write("alter system run upgrade job 'UPGRADE_ALL' tenant = sys", affected_rows);
  int wait_ret = OB_SUCCESS;
  int64_t finished_job_count = 0;
  if (OB_SUCCESS == upgrade_ret) {
    const int64_t deadline = ObTimeUtility::current_time() + UPGRADE_JOB_TIMEOUT_US;
    while (OB_SUCC(wait_ret)
           && 0 == finished_job_count
           && ObTimeUtility::current_time() < deadline) {
      wait_ret = query_int(sql_proxy, finished_job_sql.ptr(), "job_count", finished_job_count);
      if (OB_SUCC(wait_ret) && 0 == finished_job_count) {
        ob_usleep(POLL_INTERVAL_US);
      }
    }
    if (OB_SUCC(wait_ret) && 0 == finished_job_count) {
      wait_ret = OB_TIMEOUT;
    }
  }

  // close errsim
  const int disable_ret = sql_proxy.write(
      "alter system set_tp "
      "tp_name = ERRSIM_UPGRADE_SYSTEM_TABLE_OFFLINE_COLUMN, "
      "error_code = 0, frequency = 0",
      affected_rows);

  ASSERT_EQ(OB_SUCCESS, disable_ret);
  ASSERT_EQ(OB_SUCCESS, upgrade_ret);
  ASSERT_EQ(OB_SUCCESS, wait_ret);
  ASSERT_GT(finished_job_count, 0);

  // confirm the upgrade job is failed
  int64_t failed_job_count = 0;
  ret = query_int(sql_proxy, failed_job_sql.ptr(), "job_count", failed_job_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, failed_job_count);

  // confirm the upgrade event is generated
  int64_t event_count = previous_event_count;
  const int64_t event_deadline = ObTimeUtility::current_time() + 30 * 1000 * 1000;
  while (OB_SUCC(ret)
         && event_count == previous_event_count
         && ObTimeUtility::current_time() < event_deadline) {
    ret = query_int(sql_proxy, event_sql, "event_count", event_count);
    if (OB_SUCC(ret) && event_count == previous_event_count) {
      ob_usleep(POLL_INTERVAL_US);
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(event_count, previous_event_count);

  // confirm the schema is not changed
  ObSchemaGetterGuard after_schema_guard;
  ret = get_curr_observer().get_schema_service().get_tenant_schema_guard(OB_SYS_TENANT_ID, after_schema_guard);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObTableSchema *after_schema = nullptr;
  ret = after_schema_guard.get_table_schema(OB_SYS_TENANT_ID, OB_ALL_ROOTSERVICE_JOB_TID, after_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, after_schema);

  // confirm the column extra_info is varchar and length is MAX_ROOTSERVICE_JOB_EXTRA_INFO_LENGTH
  const ObColumnSchemaV2 *after_column = after_schema->get_column_schema("extra_info");
  ASSERT_NE(nullptr, after_column);
  ASSERT_EQ(ObVarcharType, after_column->get_data_type());
  ASSERT_EQ(MAX_ROOTSERVICE_JOB_EXTRA_INFO_LENGTH, after_column->get_data_length());
  ASSERT_EQ(schema_version, after_schema->get_schema_version());
  ASSERT_EQ(progressive_merge_round, after_schema->get_progressive_merge_round());
}

}
}

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
