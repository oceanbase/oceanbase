/**
 * Copyright (c) 2022 OceanBase
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
#include "lib/timezone/ob_time_convert.h"

namespace oceanbase
{
namespace share
{
using namespace schema;
using namespace common;

static const int64_t table_count = 6;
static const int64_t int_default_value = 1;
static const char *varchar_default_value = "aaa";
static const float float_default_value = 1.01;
static const double double_default_value = 2.111345;
static const char *timestamp_default_value = "2022-10-12 11:56:00.0";
static const char *datetime_default_value = "2022-10-12";

class TestProxyDefaultValue : public unittest::ObSimpleClusterTestBase
{
public:
  TestProxyDefaultValue() : unittest::ObSimpleClusterTestBase("test_proxy_default_value") {}
private:
};

TEST_F(TestProxyDefaultValue, test_mysql_common_data_types)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("use oceanbase;"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  // create table
  sql.reset();
  ret = sql.assign_fmt("create table t1(c1 int default %ld not null, c2 varchar(20) default '%s', "
      "c3 float default %f not null, c4 double default %lf not null, c5 timestamp default '%s', "
      "c6 datetime default '%s') partition by key(c1,c2,c3,c4,c5,c6)",
      int_default_value, varchar_default_value, float_default_value,
      double_default_value, timestamp_default_value, datetime_default_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  OB_LOG(INFO, "create_table succ");

  sql.reset();
  ret = sql.assign_fmt("select part_key_default_value from __all_virtual_proxy_partition_info "
      "where tenant_name = 'sys' and table_id = (select table_id from __all_virtual_table where table_name='t1')");
  ASSERT_EQ(OB_SUCCESS, ret);
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
  ASSERT_EQ(OB_SUCCESS, sql_proxy.read(result, OB_SYS_TENANT_ID, sql.ptr()));
    ASSERT_TRUE(OB_NOT_NULL(result.get_result()));
    sqlclient::ObMySQLResult &res = *result.get_result();
    int64_t index = 0;

    while (OB_SUCC(ret)) {
      if (OB_SUCC(res.next())) {
        ObString tmp_str;
        ObObj row;
        int64_t pos = 0;
        ret = res.get_varchar("part_key_default_value", tmp_str);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = row.deserialize(tmp_str.ptr(), tmp_str.length(), pos);
        ASSERT_EQ(OB_SUCCESS, ret);
        LOG_INFO("default value", K(index), K(row));
        switch(index) {
          case 0: {
            bool equal = int_default_value == row.get_int();
            ASSERT_TRUE(equal);
            break;
          }
          case 1: {
            bool equal = (0 == row.get_string().compare(varchar_default_value));
            ASSERT_TRUE(equal);
            break;
          }
          case 2: {
            bool equal = row.get_float() == float_default_value;
            ASSERT_TRUE(equal);
            break;
          }
          case 3: {
            bool equal = row.get_double() == double_default_value;
            ASSERT_TRUE(equal);
            break;
          }
          case 4: {
            int64_t timestamp = 0;
            ObTimeZoneInfo tz_info;
            char buf[50] = {0};
            ObString str;
            ObTimeConvertCtx cvrt_ctx(&tz_info, true);
            strcpy(buf, "+8:00");
            str.assign(buf, static_cast<int32_t>(strlen(buf)));
            tz_info.set_timezone(str);
            ret = ObTimeConverter::str_to_datetime(ObString(timestamp_default_value), cvrt_ctx, timestamp);
            ASSERT_EQ(OB_SUCCESS, ret);
            bool equal = row.get_timestamp() == timestamp;
            LOG_INFO("timestamp default value", K(row.get_timestamp()), K(timestamp));
            ASSERT_TRUE(equal);
            break;
          }
          case 5: {
            int64_t datetime = 0;
            ObTimeZoneInfo tz_info;
            char buf[50] = {0};
            ObString str;
            ObTimeConvertCtx cvrt_ctx(&tz_info, true);
            cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
            strcpy(buf, "+00:00");
            str.assign(buf, static_cast<int32_t>(strlen(buf)));
            tz_info.set_timezone(str);
            ret = ObTimeConverter::str_to_datetime(ObString(datetime_default_value), cvrt_ctx, datetime);
            ASSERT_EQ(OB_SUCCESS, ret);
            bool equal = row.get_datetime() == datetime;
            LOG_INFO("datetime default value", K(row.get_datetime()), K(datetime));
            ASSERT_TRUE(equal);
            break;
          }
          default: FAIL();
        }
        ++index;
      }
    } // end while
  }
}

TEST_F(TestProxyDefaultValue, test_oracle_common_data_types)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, create_tenant("oracle", "2G", "2G", true));
  // sys tenant sql_proxy
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  sql.assign_fmt("alter tenant oracle set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  sql.reset();
  sql.assign_fmt("alter tenant oracle set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF'");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  //init oracle_sql_proxy
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("oracle", "SYS", true));
  common::ObMySQLProxy &oracle_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  sql.reset();
  ret = sql.assign_fmt("create table ot1(c1 number default %ld not null, c2 varchar2(20) default '%s', "
      "c3 NVARCHAR2(10) default '%s' not null, c4 binary_double default %lf not null, c5 timestamp default '%s', "
      "c6 date default '%s') partition by hash(c1,c2,c3,c4,c5,c6)",
      int_default_value, varchar_default_value, varchar_default_value,
      double_default_value, timestamp_default_value, datetime_default_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, oracle_sql_proxy.write(sql.ptr(), affected_rows));
  OB_LOG(INFO, "create_table succ");

  // query in sys tenant
  sql.reset();
  ret = sql.assign_fmt("select part_key_default_value from __all_virtual_proxy_partition_info "
      "where tenant_name = 'oracle' and table_id = (select table_id from __all_virtual_table where table_name='ot1')");
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
  ASSERT_EQ(OB_SUCCESS, sql_proxy.read(result, OB_SYS_TENANT_ID, sql.ptr()));
    ASSERT_TRUE(OB_NOT_NULL(result.get_result()));
    sqlclient::ObMySQLResult &res = *result.get_result();
    int64_t index = 0;

    while (OB_SUCC(ret)) {
      if (OB_SUCC(res.next())) {
        ObString tmp_str;
        ObSqlString tmp_value;
        ObObj row;
        int64_t pos = 0;
        ret = res.get_varchar("part_key_default_value", tmp_str);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = row.deserialize(tmp_str.ptr(), tmp_str.length(), pos);
        ASSERT_EQ(OB_SUCCESS, ret);
        LOG_INFO("default value", K(index), K(row));
        switch(index) {
          case 0: {
            ASSERT_EQ(OB_SUCCESS, tmp_value.assign_fmt("%ld", int_default_value));
            bool equal = (0 == row.get_string().compare(tmp_value.ptr()));
            ASSERT_TRUE(equal);
            break;
          }
          case 1: {
            ASSERT_EQ(OB_SUCCESS, tmp_value.assign_fmt("'%s'", varchar_default_value));
            bool equal = (0 == row.get_string().compare(tmp_value.ptr()));
            ASSERT_TRUE(equal);
            break;
          }
          case 2: {
            ASSERT_EQ(OB_SUCCESS, tmp_value.assign_fmt("'%s'", varchar_default_value));
            bool equal = (0 == row.get_string().compare(tmp_value.ptr()));
            ASSERT_TRUE(equal);
            break;
          }
          case 3: {
            ASSERT_EQ(OB_SUCCESS, tmp_value.assign_fmt("%lf", double_default_value));
            bool equal = (0 == row.get_string().compare(tmp_value.ptr()));
            ASSERT_TRUE(equal);
            break;
          }
          case 4: {
            ASSERT_EQ(OB_SUCCESS, tmp_value.assign_fmt("'%s'", timestamp_default_value));
            bool equal = (0 == row.get_string().compare(tmp_value.ptr()));
            ASSERT_TRUE(equal);
            break;
          }
          case 5: {
            ASSERT_EQ(OB_SUCCESS, tmp_value.assign_fmt("'%s'", datetime_default_value));
            bool equal = (0 == row.get_string().compare(tmp_value.ptr()));
            ASSERT_TRUE(equal);
            break;
          }
          default: FAIL();
        }
        ++index;
      }
    } // end while
  }
}

TEST_F(TestProxyDefaultValue, test_default_value_is_null)
{
  int ret = OB_SUCCESS;
  // sys tenant sql_proxy
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  // oracle tenant sql_proxy
  common::ObMySQLProxy &oracle_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  // create table
  ObSqlString sql;
  int64_t affected_rows = 0;
  ret = sql.assign_fmt("create table t2 (c1 int default 1, c2 int, c3 int generated always as (c1 + 1) virtual) partition by key(c2, c3);");
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

  sql.reset();
  ret = sql.assign_fmt("create table ot2 (c1 number default 1, c2 number, c3 number generated as (c1 + 1) virtual, c4 int generated by default as identity not null) partition by hash(c2, c3, c4);");
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, oracle_sql_proxy.write(sql.ptr(), affected_rows));

  // query in sys tenant
  sql.reset();
  ret = sql.assign_fmt("select part_key_default_value from __all_virtual_proxy_partition_info "
      "where tenant_name = 'sys' and table_id = (select table_id from __all_virtual_table where table_name='t2')");
  ASSERT_EQ(OB_SUCCESS, ret);
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
  ASSERT_EQ(OB_SUCCESS, sql_proxy.read(result, OB_SYS_TENANT_ID, sql.ptr()));
    ASSERT_TRUE(OB_NOT_NULL(result.get_result()));
    sqlclient::ObMySQLResult &res = *result.get_result();
    int64_t index = 0;
    while (OB_SUCC(ret)) {
      if (OB_SUCC(res.next())) {
        ObString tmp_str;
        ret = res.get_varchar("part_key_default_value", tmp_str);
        ASSERT_EQ(OB_ERR_NULL_VALUE, ret);
      }
    }
  }

  sql.reset();
  ret = sql.assign_fmt("select part_key_default_value from __all_virtual_proxy_partition_info "
      "where tenant_name = 'oracle' and table_id = (select table_id from __all_virtual_table where table_name='ot2')");
  ASSERT_EQ(OB_SUCCESS, ret);
  SMART_VAR(ObMySQLProxy::MySQLResult, result1) {
    ret = sql_proxy.read(result1, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(OB_NOT_NULL(result1.get_result()));
    sqlclient::ObMySQLResult &res = *result1.get_result();
    int64_t index = 0;
    while (OB_SUCC(ret)) {
      if (OB_SUCC(res.next())) {
        ObString tmp_str;
        ret = res.get_varchar("part_key_default_value", tmp_str);
        ASSERT_EQ(OB_ERR_NULL_VALUE, ret);
      }
    }
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
