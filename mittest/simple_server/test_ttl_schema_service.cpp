/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define protected public
#define private public
#include "mittest/env/ob_simple_server_helper.h"
#include "env/ob_simple_cluster_test_base.h"
#include "share/table/ob_ttl_util.h"
#include "share/compaction_ttl/ob_compaction_ttl_util.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "unittest/storage/ob_truncate_info_helper.h"
#include "share/ob_cluster_version.h"
#include "common/ob_smart_var.h"
#include "rootserver/compaction_ttl/ob_compaction_ttl_service.h"
#include "simple_server/compaction_basic_func.h"
#include "storage/compaction/ob_index_block_micro_iterator.h"
#include "storage/ob_trans_version_skip_index_util.h"
#include "storage/compaction/ob_index_block_micro_iterator.h"
#include "lib/timezone/ob_time_convert.h"

namespace oceanbase
{
using namespace storage;
using namespace share;
using namespace common;
using namespace rootserver;
using namespace compaction;
namespace unittest
{

class ObTTLSchemaServiceTest : public ObSimpleClusterTestBase
{
public:
  ObTTLSchemaServiceTest()
    : ObSimpleClusterTestBase("test_ttl_schema_service"),
      tenant_id_(0)
  {
    ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    ObSqlString sql;
    int64_t affected_rows = 0;
    sql_proxy.write("drop database test", affected_rows);
    sql_proxy.write("create database test", affected_rows);
    sql_proxy.write("use test", affected_rows);
  }
  virtual void SetUp() override
  {
    bool tenant_exist = false;
    if (OB_SUCCESS != check_tenant_exist(tenant_exist) || !tenant_exist) {
      ObSimpleClusterTestBase::SetUp();
      ASSERT_EQ(OB_SUCCESS, create_tenant());
      ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
    }
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_));
  }

  // Helper method to test is_compaction_ttl_schema for a given table
  void test_is_compaction_ttl_schema(sqlclient::ObISQLConnection *conn,
                                     const uint64_t tenant_data_version,
                                     const char *table_name,
                                     bool expected_result) {
    int64_t table_id = 0;
    ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::get_table_id(*conn, table_name, false/*is_index*/, table_id));
    schema::ObSchemaGetterGuard schema_guard;
    const share::schema::ObTableSchema *table_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_table_schema(tenant_id_, schema_guard, table_id, table_schema));
    bool is_compaction_ttl = false;
    ASSERT_EQ(OB_SUCCESS, ObCompactionTTLUtil::is_compaction_ttl_schema(tenant_data_version, *table_schema, is_compaction_ttl));
    if (expected_result) {
      ASSERT_TRUE(is_compaction_ttl);
    } else {
      ASSERT_FALSE(is_compaction_ttl);
    }
  }

  uint64_t tenant_id_;

private:
  void insert_rows(const char *table_name, const int64_t rowkey_start, const int64_t row_count);
  void get_table_schema(
      sqlclient::ObISQLConnection *conn,
      const char *table_name,
      const bool is_index,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema *&table_schema);
  int check_schema_ttl(const share::schema::ObTableSchema *table_schema,
                       const ObString &expected_ttl_def,
                       const share::ObTTLDefinition::ObTTLType expected_ttl_type,
                       const bool expect_rowscn_flag,
                       const bool expect_has_ttl);
  void check_aux_lob_ttl(const share::schema::ObTableSchema &base_schema,
                         const ObString &expected_ttl_def,
                         const share::ObTTLDefinition::ObTTLType expected_ttl_type,
                         const bool expect_rowscn_flag,
                         const bool expect_has_ttl);
  void check_table_ttl(const char *table_name,
                       sqlclient::ObISQLConnection *conn,
                       const ObString &expected_ttl_def,
                       const share::ObTTLDefinition::ObTTLType expected_ttl_type,
                       const bool expect_rowscn_flag,
                       const bool expect_has_ttl,
                       const ObString index_name = ObString());
};
#define EXEC_FAIL(sql) ASSERT_NE(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows))
#define EXEC_SUCC(sql) ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows))
void ObTTLSchemaServiceTest::insert_rows(
  const char *table_name, const int64_t rowkey_start, const int64_t row_count)
{
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  for (int64_t i = 0; i < row_count; ++i) {
    sql.assign_fmt("insert into %s values(%ld, 'val%ld')", table_name, rowkey_start + i, i);
    EXEC_SUCC(sql);
  }
}

void ObTTLSchemaServiceTest::get_table_schema(
    sqlclient::ObISQLConnection *conn,
    const char *table_name,
    const bool is_index,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTableSchema *&table_schema)
{
  ASSERT_TRUE(nullptr != conn);
  int ret = OB_SUCCESS;
  table_schema = nullptr;
  int64_t table_id = 0;
  ret = CompactionBasicFunc::get_table_id(*conn, table_name, is_index, table_id);
  if (OB_SUCCESS != ret) {
    COMMON_LOG(WARN, "failed to get table id", K(ret), K(table_name), K(is_index));
  }
  ASSERT_TRUE(0 != table_id);
  ret = TruncateInfoHelper::get_table_schema(tenant_id_, schema_guard, table_id, table_schema);
  if (OB_SUCCESS != ret) {
    COMMON_LOG(WARN, "failed to get table schema", K(ret), K(table_id));
  }
  ASSERT_TRUE(nullptr != table_schema);
}

int ObTTLSchemaServiceTest::check_schema_ttl(
    const share::schema::ObTableSchema *table_schema,
    const ObString &expected_ttl_def,
    const share::ObTTLDefinition::ObTTLType expected_ttl_type,
    const bool expect_rowscn_flag,
    const bool expect_has_ttl)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "table schema is null", K(ret), K(expect_has_ttl), K(expect_rowscn_flag),
               K(expected_ttl_def), K(expected_ttl_type));
  } else {
    ObTTLFlag ttl_flag = table_schema->get_ttl_flag();
    if (expect_has_ttl) {
      if (table_schema->get_ttl_definition().empty()
          || expected_ttl_type != ttl_flag.ttl_type_
          || 0 != expected_ttl_def.compare(table_schema->get_ttl_definition())) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      if (!table_schema->get_ttl_definition().empty()
          || expected_ttl_type != ttl_flag.ttl_type_) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret) && expect_rowscn_flag != ttl_flag.had_rowscn_as_ttl_) {
      ret = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCCESS != ret) {
      COMMON_LOG(WARN, "check schema ttl failed", K(ret), "table_name", table_schema->get_table_name(),
                 K(expect_has_ttl), K(expect_rowscn_flag),
                 K(expected_ttl_def), K(expected_ttl_type), K(table_schema->get_ttl_definition()),
                 K(ttl_flag.ttl_type_), K(ttl_flag.had_rowscn_as_ttl_));
    }
  }
  return ret;
}

void ObTTLSchemaServiceTest::check_aux_lob_ttl(const share::schema::ObTableSchema &base_schema,
                                               const ObString &expected_ttl_def,
                                               const share::ObTTLDefinition::ObTTLType expected_ttl_type,
                                               const bool expect_rowscn_flag,
                                               const bool expect_has_ttl)
{
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *aux_schema = nullptr;
  aux_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_table_schema(
      tenant_id_, schema_guard, base_schema.get_aux_lob_meta_tid(), aux_schema));
  ASSERT_EQ(OB_SUCCESS, check_schema_ttl(aux_schema, expected_ttl_def, expected_ttl_type,
                                         expect_rowscn_flag, expect_has_ttl));
  aux_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_table_schema(
      tenant_id_, schema_guard, base_schema.get_aux_lob_piece_tid(), aux_schema));
  ASSERT_EQ(OB_SUCCESS, check_schema_ttl(aux_schema, expected_ttl_def, expected_ttl_type,
                                         expect_rowscn_flag, expect_has_ttl));
}

void ObTTLSchemaServiceTest::check_table_ttl(const char *table_name,
                       sqlclient::ObISQLConnection *conn,
                       const ObString &expected_ttl_def,
                       const share::ObTTLDefinition::ObTTLType expected_ttl_type,
                       const bool expect_rowscn_flag,
                       const bool expect_has_ttl,
                       const ObString index_name)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  get_table_schema(conn, table_name, false, schema_guard, table_schema);
  if (OB_NOT_NULL(table_schema)) {
    ASSERT_EQ(OB_SUCCESS, check_schema_ttl(table_schema, expected_ttl_def, expected_ttl_type,
                                           expect_rowscn_flag, expect_has_ttl));

    // index inherit check if provided
    if (!index_name.empty()) {
      const share::schema::ObTableSchema *idx_schema = nullptr;
      get_table_schema(conn, index_name.ptr(), true, schema_guard, idx_schema);
      ASSERT_EQ(OB_SUCCESS, check_schema_ttl(idx_schema, expected_ttl_def, expected_ttl_type,
                                             expect_rowscn_flag, expect_has_ttl));
    }

    check_aux_lob_ttl(*table_schema, expected_ttl_def, expected_ttl_type, expect_rowscn_flag, expect_has_ttl);
  } else {
    COMMON_LOG(WARN, "table schema is null", K(ret), K(table_name));
  }
}

TEST_F(ObTTLSchemaServiceTest, ttl_definition_ts_test)
{
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  ObSqlString sql;
  int64_t affected_rows = 0;

  // 1. Create a table with TTL
  sql.assign_fmt("create table ttl_test_table (c1 int primary key, c2 varchar(200), c3 int, index idx1(c3)) merge_engine=append_only TTL ora_rowscn + INTERVAL 1 year BY COMPACTION");
  EXEC_SUCC(sql);

  // 2. Get table_id and table_schema
  int64_t table_id = 0;
  ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::get_table_id(*conn, "ttl_test_table", false/*is_index*/, table_id));
  COMMON_LOG(INFO, "get table id", K(table_id));
  schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_table_schema(tenant_id_, schema_guard, table_id, table_schema));

  ASSERT_TRUE(table_schema->is_append_only_merge_engine());

  // Verify ttl_filter_us for YEAR: should be approximately (current_time - 1 year)
  int64_t current_time_us = ObTimeUtility::current_time();
  int64_t one_year_ago_us = 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_add_nmonth(current_time_us, -12, one_year_ago_us, true));

  // 3. Build ObSimpleTableTTLChecker and check ttl_definition_ts for YEAR
  ObSimpleTableTTLChecker ttl_checker;
  ASSERT_EQ(OB_SUCCESS, ttl_checker.init(*table_schema, table_schema->get_ttl_definition(), true));
  COMMON_LOG(INFO, "ttl_checker", K(ttl_checker.get_ttl_definition()), K(table_schema->get_ttl_definition()));
  int64_t ttl_filter_us = 0;
  ASSERT_EQ(OB_SUCCESS, ttl_checker.get_ttl_filter_us(ttl_filter_us));
  COMMON_LOG(INFO, "YEAR test", K(ttl_filter_us), K(one_year_ago_us), K(current_time_us), "diff", std::abs(ttl_filter_us - one_year_ago_us));
  ASSERT_TRUE(one_year_ago_us < ttl_filter_us);

  int64_t index_id = 0;
  ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::get_table_id(*conn, "ttl_test_table", true/*is_index*/, index_id));
  COMMON_LOG(INFO, "get index id", K(index_id));
  const share::schema::ObTableSchema *index_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_table_schema(tenant_id_, schema_guard, index_id, index_schema));

  ASSERT_TRUE(index_schema->has_ttl_definition());

  // 4. Test DAY TTL definition
  sql.assign_fmt("drop table ttl_test_table");
  EXEC_SUCC(sql);
  sql.assign_fmt("create table ttl_test_table (c1 int primary key, c2 varchar(200)) merge_engine=append_only TTL ora_rowscn + INTERVAL 1 day BY COMPACTION");
  EXEC_SUCC(sql);

  ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::get_table_id(*conn, "ttl_test_table", false/*is_index*/, table_id));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_table_schema(tenant_id_, schema_guard, table_id, table_schema));

  current_time_us = ObTimeUtility::current_time();
  int64_t one_day_ago_us = 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_add_nsecond(current_time_us, -86400000LL, 0, one_day_ago_us)); // -1 day in seconds

  ObSimpleTableTTLChecker ttl_checker_day;
  ASSERT_EQ(OB_SUCCESS, ttl_checker_day.init(*table_schema, table_schema->get_ttl_definition(), true));
  int64_t ttl_filter_us_day = 0;
  ASSERT_EQ(OB_SUCCESS, ttl_checker_day.get_ttl_filter_us(ttl_filter_us_day));

  COMMON_LOG(INFO, "DAY test", K(ttl_filter_us_day), K(one_day_ago_us), K(current_time_us), "diff", std::abs(ttl_filter_us_day - one_day_ago_us));
  ASSERT_TRUE(one_day_ago_us < ttl_filter_us_day);

  // 5. Test MONTH TTL definition
  sql.assign_fmt("drop table ttl_test_table");
  EXEC_SUCC(sql);
  sql.assign_fmt("create table ttl_test_table (c1 int primary key, c2 varchar(200)) merge_engine=append_only TTL ora_rowscn + INTERVAL 1 month BY COMPACTION");
  EXEC_SUCC(sql);

  ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::get_table_id(*conn, "ttl_test_table", false/*is_index*/, table_id));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_table_schema(tenant_id_, schema_guard, table_id, table_schema));

  current_time_us = ObTimeUtility::current_time();
  int64_t one_month_ago_us = 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_add_nmonth(current_time_us, -1, one_month_ago_us, true));

  ObSimpleTableTTLChecker ttl_checker_month;
  ASSERT_EQ(OB_SUCCESS, ttl_checker_month.init(*table_schema, table_schema->get_ttl_definition(), true));
  int64_t ttl_filter_us_month = 0;
  ASSERT_EQ(OB_SUCCESS, ttl_checker_month.get_ttl_filter_us(ttl_filter_us_month));

  COMMON_LOG(INFO, "MONTH test", K(ttl_filter_us_month), K(one_month_ago_us), K(current_time_us), "diff", std::abs(ttl_filter_us_month - one_month_ago_us));
  ASSERT_TRUE(one_month_ago_us < ttl_filter_us_month);

  sql.assign_fmt("drop table ttl_test_table");
  EXEC_SUCC(sql);
}

TEST_F(ObTTLSchemaServiceTest, vec_index_defense_test)
{
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  ObSqlString sql;
  int64_t affected_rows = 0;
  // case1: create table with vec_index and ttl
  sql.assign_fmt("create table vec_idx_with_ttl \
      (c1 int primary key, c2 vector(3), c3 varchar(200),\
      vector index idx1(c2) with (distance=l2, type=hnsw)) \
      TTL ora_rowscn + INTERVAL 1 hour BY COMPACTION");
  EXEC_FAIL(sql); // should fail

  // case2: create table with vec_index and alter table with ttl
  sql.assign_fmt("create table vec_idx_table1(c1 int, c2 sparsevector, primary key(c1), vector index idx1(c2) with (lib=vsag, type=sindi, distance=inner_product))");
  EXEC_SUCC(sql);

  sql.assign_fmt("alter table vec_idx_table1 TTL ora_rowscn + INTERVAL 1 hour BY COMPACTION");
  EXEC_FAIL(sql); // should fail

  // case3: create table with ttl and alter table add vec_index
  sql.assign_fmt("create table ttl_table3 \
    (c1 int primary key, c2 vector(3), c3 varchar(200))\
    TTL ora_rowscn + INTERVAL 1 hour BY COMPACTION");
  EXEC_FAIL(sql);

  sql.assign_fmt("alter table ttl_table4 add vector index idx1(c2) with (distance=l2, type=hnsw)");
  EXEC_FAIL(sql); // should fail
}

TEST_F(ObTTLSchemaServiceTest, append_only_merge_engine_defense_test)
{
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  ObSqlString sql;
  int64_t affected_rows = 0;
  // case1: create table with append_only merge engine and ttl
  sql.assign_fmt("create table append_only_table \
      (c1 int primary key, c2 varchar(200), c3 timestamp) \
      merge_engine=append_only \
      TTL c3 + INTERVAL 1 hour BY DELETING");
  EXEC_FAIL(sql); // should failed
  // case2: create table with append_only merge engine and ttl and has_merged_with_mds_info
  sql.assign_fmt("create table append_only_table \
      (c1 int primary key, c2 varchar(200), c3 timestamp) \
      merge_engine=append_only TTL ora_rowscn + INTERVAL 1 second BY COMPACTION");
  EXEC_SUCC(sql);
  sql.assign_fmt("alter table append_only_table TTL c3 + INTERVAL 1 hour BY DELETING");
  EXEC_FAIL(sql); // should fail
  sql.assign_fmt("alter table append_only_table TTL ora_rowscn + INTERVAL 1 hour BY COMPACTION");
  EXEC_SUCC(sql); // should success

  sql.assign_fmt("create table partial_update_ttl_table2 (c1 int primary key, c2 varchar(200), c3 int, index idx1(c3)) merge_engine=partial_update TTL ora_rowscn + INTERVAL 1 hour BY COMPACTION");
  EXEC_FAIL(sql); // should fail
}

TEST_F(ObTTLSchemaServiceTest, DISABLED_ttl_flag_and_inheritance)
{
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));

  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  ObSqlString sql;
  int64_t affected_rows = 0;
  ObArenaAllocator allocator("TTLCheck");

  const ObString c1_ttl_expr = ObString::make_string("c1 + INTERVAL 10 HOUR");
  const ObString c3_ttl_expr = ObString::make_string("c3 + INTERVAL 10 HOUR");
  const ObString rowscn_expr = ObString::make_string("ora_rowscn + INTERVAL 10 HOUR");

  sql.assign_fmt("drop table if exists deleting_ttl_table");
  EXEC_SUCC(sql);
  sql.assign_fmt("drop table if exists compaction_ttl_table");
  EXEC_SUCC(sql);

  // KV TTL table should mark ttl_type as KV and had_rowscn_as_ttl, and inherit to index/lob aux tables
  sql.assign_fmt("create table deleting_ttl_table "
                 "(c1 int primary key, c2 int, c3 timestamp, c4 text) merge_engine=delete_insert "
                 "TTL c3 + INTERVAL 10 HOUR BY DELETING");
  EXEC_SUCC(sql);
  sql.assign_fmt("create index idx_kv_ttl_c3 on deleting_ttl_table(c3)");
  EXEC_SUCC(sql);
  check_table_ttl("deleting_ttl_table",
                  conn,
                  c3_ttl_expr,
                  ObTTLDefinition::DELETING,
                  /*expect_rowscn_flag=*/false,
                  /*expect_has_ttl=*/true,
                  ObString::make_string("idx_kv_ttl_c3"));

  sql.assign_fmt("alter table deleting_ttl_table TTL ora_rowscn + INTERVAL 10 HOUR BY COMPACTION");
  EXEC_SUCC(sql);
  check_table_ttl("deleting_ttl_table",
                  conn,
                  rowscn_expr,
                  ObTTLDefinition::COMPACTION,
                  /*expect_rowscn_flag=*/true,
                  /*expect_has_ttl=*/true,
                  ObString::make_string("idx_kv_ttl_c3"));

  // SQL TTL table with rowscn, then remove TTL, then switch to KV TTL on a timestamp column, and finally SQL TTL on the same column
  sql.assign_fmt("create table compaction_ttl_table "
                 "(c1 timestamp primary key, c2 int, c3 int, c4 text) "
                 "merge_engine=delete_insert "
                 "TTL ora_rowscn + INTERVAL 10 HOUR BY COMPACTION");
  EXEC_SUCC(sql);
  sql.assign_fmt("create index idx_ttl_c2 on compaction_ttl_table(c2)");
  EXEC_SUCC(sql);
  check_table_ttl("compaction_ttl_table",
                  conn,
                  rowscn_expr,
                  ObTTLDefinition::COMPACTION,
                  /*expect_rowscn_flag=*/true,
                  /*expect_has_ttl=*/true,
                  ObString::make_string("idx_ttl_c2"));

  // remove TTL, ttl_definition should be empty, had_rowscn_as_ttl should stay true, ttl_type should be NONE or KV
  sql.assign_fmt("alter table compaction_ttl_table remove TTL");
  EXEC_SUCC(sql);
  check_table_ttl("compaction_ttl_table",
                  conn,
                  ObString(),
                  ObTTLDefinition::NONE,
                  /*expect_rowscn_flag=*/true,
                  /*expect_has_ttl=*/false,
                  ObString::make_string("idx_ttl_c2"));

  // switch to KV TTL on non-rowscn column, had_rowscn_as_ttl should turn false
  sql.assign_fmt("alter table compaction_ttl_table TTL(c1 + INTERVAL 10 HOUR)");
  EXEC_SUCC(sql);
  check_table_ttl("compaction_ttl_table",
                  conn,
                  c1_ttl_expr,
                  ObTTLDefinition::DELETING,
                  /*expect_rowscn_flag=*/true,
                  /*expect_has_ttl=*/true,
                  ObString::make_string("idx_ttl_c2"));

  // switch to KV TTL on non-rowscn column, had_rowscn_as_ttl should turn false
  sql.assign_fmt("alter table compaction_ttl_table TTL c1 + INTERVAL 10 HOUR BY DELETING");
  EXEC_SUCC(sql);
  check_table_ttl("compaction_ttl_table",
                  conn,
                  c1_ttl_expr,
                  ObTTLDefinition::DELETING,
                  /*expect_rowscn_flag=*/true,
                  /*expect_has_ttl=*/true,
                  ObString::make_string("idx_ttl_c2"));

  // switch to SQL TTL on non-rowscn column, inheritance should stay consistent
  sql.assign_fmt("alter table compaction_ttl_table TTL c1 + INTERVAL 10 HOUR BY COMPACTION");
  EXEC_FAIL(sql);
}

TEST_F(ObTTLSchemaServiceTest, is_compaction_ttl_schema_test)
{
  int ret = OB_SUCCESS;
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));

  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t tenant_data_version = 0;
  ASSERT_EQ(OB_SUCCESS, ObClusterVersion::get_instance().get_tenant_data_version(tenant_id_, tenant_data_version));

  // Case 1: Test partial_update merge engine table (should return false)
  sql.assign_fmt("create table partial_update_ttl_table (c1 int primary key, c2 varchar(200)) merge_engine=partial_update\
     TTL ora_rowscn + INTERVAL 1 hour BY COMPACTION");
  EXEC_FAIL(sql);

  // Case 2: Test TTL column is not rowscn column (should return False)
  sql.assign_fmt("create table non_rowscn_ttl_table (c1 int primary key, c2 timestamp) TTL c2 + INTERVAL 1 hour BY COMPACTION");
  EXEC_FAIL(sql);

  // Case 3: Test table with foreign key constraint
  // First create a referenced table
  sql.assign_fmt("create table ref_table (id int primary key, name varchar(100)) merge_engine=append_only TTL ora_rowscn + INTERVAL 1 hour BY COMPACTION");
  EXEC_SUCC(sql);

  // create table with foreign key and TTL should fail
  sql.assign_fmt("create table fk_ttl_table (c1 int primary key, c2 int, c3 varchar(200), foreign key(c2) references ref_table(id)) TTL ora_rowscn + INTERVAL 1 hour BY COMPACTION");
  EXEC_FAIL(sql);
  sql.assign_fmt("create table fk_ttl_table (c1 int primary key, c2 timestamp, c3 varchar(200), foreign key(c2) references ref_table(id)) TTL c2 + INTERVAL 1 hour BY COMPACTION");
  EXEC_FAIL(sql);

  // Case 4: Test valid compaction TTL table (should return true for comparison)
  sql.assign_fmt("create table valid_compaction_ttl_table (c1 int primary key, c2 varchar(200)) merge_engine=append_only TTL ora_rowscn + INTERVAL 1 hour BY COMPACTION");
  EXEC_SUCC(sql);

  test_is_compaction_ttl_schema(conn, tenant_data_version, "valid_compaction_ttl_table", true); // Should return true for valid compaction TTL table

  // Case 5: Test valid compaction TTL table (should return true for comparison)
  sql.assign_fmt("create table valid_compaction_ttl_table2 (c1 int primary key, c2 varchar(200)) merge_engine=delete_insert TTL ora_rowscn + INTERVAL 1 hour BY COMPACTION");
  EXEC_SUCC(sql);

  test_is_compaction_ttl_schema(conn, tenant_data_version, "valid_compaction_ttl_table2", true); // Should return true for valid compaction TTL table

  // Case 6: Test remove ttl definition for table with index
  const char *table_name = "valid_compaction_ttl_table3";
  int64_t index_id = 0;
  sql.assign_fmt("create table %s (c1 int primary key, c2 int, index idx(c2)) merge_engine=append_only TTL ora_rowscn + INTERVAL 1 hour BY COMPACTION", table_name);
  EXEC_SUCC(sql);

  test_is_compaction_ttl_schema(conn, tenant_data_version, table_name, true); // Should return true for valid compaction TTL table
  ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::get_table_id(*conn, table_name, true/*is_index*/, index_id));
  schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *index_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_table_schema(tenant_id_, schema_guard, index_id, index_schema));
  ASSERT_TRUE(index_schema->has_ttl_definition());
  ASSERT_TRUE(index_schema->is_append_only_merge_engine());

  sql.assign_fmt("alter table %s remove TTL", table_name);
  EXEC_SUCC(sql);
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_table_schema(tenant_id_, schema_guard, index_id, index_schema));
  ASSERT_FALSE(index_schema->has_ttl_definition());
  ASSERT_TRUE(index_schema->is_append_only_merge_engine());
}

TEST_F(ObTTLSchemaServiceTest, ttl_filter_ts_expire_check)
{
  int ret = OB_SUCCESS;
  const int64_t TTL_EXPIRE_SECONDS = 10; // TTL expire time in seconds
  const int64_t SLEEP_SECONDS = TTL_EXPIRE_SECONDS / 2;       // Sleep time in seconds
  const int64_t TEST_ROW_COUNT = 10;     // Number of rows per batch
  const int64_t rowkey_step = 1000000;
  int64_t rowkey_start = 0;
  // 1. Create table with TTL(ora_rowscn + INTERVAL 10 SECOND)
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  const char *table_name = "ttl_filter_us_test";
  ObSqlString sql;
  {
    int64_t affected_rows = 0;
    sql.assign_fmt("create table %s (c1 int primary key, c2 varchar(200)) TTL ora_rowscn + INTERVAL %ld SECOND BY COMPACTION merge_engine=append_only",
                  table_name, TTL_EXPIRE_SECONDS);
    EXEC_SUCC(sql);
  }

  // 2. Insert first batch of 10 rows
  insert_rows(table_name, rowkey_start, TEST_ROW_COUNT);

  // 3. Sleep 5 seconds
  sleep(SLEEP_SECONDS);

  // 4. Insert second batch of 10 rows
  insert_rows(table_name, rowkey_start + rowkey_step, TEST_ROW_COUNT);

  // 5. Sleep another 5 seconds
  sleep(SLEEP_SECONDS);

  // 6. Build ObSimpleTableTTLChecker and get ttl_filter_us
  int64_t table_id = 0;
  ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::get_table_id(*conn, table_name, false, table_id));
  schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_table_schema(tenant_id_, schema_guard, table_id, table_schema));
  ObSimpleTableTTLChecker ttl_checker;
  ObTTLFilterInfo ttl_filter_info;
  ASSERT_EQ(OB_SUCCESS, ttl_checker.init(*table_schema, table_schema->get_ttl_definition(), true));
  ASSERT_EQ(OB_SUCCESS, ObTTLFilterInfoHelper::generate_ttl_filter_info(*table_schema, ttl_filter_info));
  const int64_t ttl_filter_ts = ttl_filter_info.ttl_filter_value_;

  // 7. Query all ora_rowscn
  sql.assign_fmt("select c1, ora_rowscn from %s order by c1 asc", table_name);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = nullptr;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    result = res.get_result();
    ASSERT_TRUE(result != nullptr);
    while (OB_SUCCESS == (ret = result->next())) {
      int64_t ora_rowscn = 0;
      int64_t c1 = 0;
      ASSERT_EQ(OB_SUCCESS, result->get_int("c1", c1));
      ASSERT_EQ(OB_SUCCESS, result->get_int("ora_rowscn", ora_rowscn));
      if (c1 < rowkey_step) {
        ASSERT_LT(ora_rowscn, ttl_filter_ts);
        COMMON_LOG(INFO, "c1 < rowkey_step, ora_rowscn < ttl_filter_ts", K(c1), K(ora_rowscn), K(ttl_filter_ts));
      } else {
        ASSERT_GE(ora_rowscn, ttl_filter_ts);
        COMMON_LOG(INFO, "c1 >= rowkey_step, ora_rowscn >= ttl_filter_ts", K(c1), K(ora_rowscn), K(ttl_filter_ts));
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }
}

TEST_F(ObTTLSchemaServiceTest, report_ddl_create_snapshot_test)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(tenant_id_));
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  sqlclient::ObISQLConnection *conn = NULL;
  sqlclient::ObISQLConnection *sys_conn = NULL;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_EQ(OB_SUCCESS, sys_proxy.acquire(sys_conn));

  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t tenant_data_version = 0;
  ASSERT_EQ(OB_SUCCESS, ObClusterVersion::get_instance().get_tenant_data_version(tenant_id_, tenant_data_version));

  // Case 1: Test partial_update merge engine table (should return false)
  sql.assign_fmt("create table t1(a bigint, b bigint, c varchar(200))");
  EXEC_SUCC(sql);

  sql.assign_fmt("insert into t1 values(1, 1, 'val1')");
  EXEC_SUCC(sql);

  sql.assign_fmt("alter table t1 modify column c varchar(100)");
  EXEC_SUCC(sql);

  int64_t table_id = 0;
  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::get_table_id(*sys_conn, "t1", false/*is_index*/, table_id));
  ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::get_tablet_and_ls_id(*sys_conn, table_id, tablet_id, ls_id));

  // test ddl_create_snapshot
  sql.assign_fmt("select max(ddl_create_snapshot) as max_ddl_create_snapshot from oceanbase.%s where tablet_id = %ld", OB_ALL_VIRTUAL_TABLET_META_TABLE_TNAME, tablet_id.id());
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = nullptr;
    ASSERT_EQ(OB_SUCCESS, sys_proxy.read(res, sql.ptr()));
    result = res.get_result();
    ASSERT_TRUE(result != nullptr);
    while (OB_SUCCESS == (ret = result->next())) {
      int64_t max_ddl_create_snapshot = 0;
      ASSERT_EQ(OB_SUCCESS, result->get_int("max_ddl_create_snapshot", max_ddl_create_snapshot));
      ASSERT_TRUE(max_ddl_create_snapshot > 0);
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }

  // test min/max_merged_trans_version on mini sstable
  sql.assign_fmt("insert into t1 values(2, 2, 'val2')");
  EXEC_SUCC(sql);
  sql.assign_fmt("alter system minor freeze");
  EXEC_SUCC(sql);
  ASSERT_EQ(OB_SUCCESS, CompactionBasicFunc::check_minor_finish(*sys_conn, tenant_id_));
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle));
  ASSERT_TRUE(tablet_handle.is_valid());

  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTable *mini_sstable = nullptr;
  if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_ISNULL(mini_sstable = static_cast<ObSSTable*>(
    table_store_wrapper.get_member()->get_minor_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("minor sstable not exist", K(ret), KPC(table_store_wrapper.get_member()));
  } else {
    ASSERT_TRUE(mini_sstable->get_min_merged_trans_version() > 0);
    ASSERT_TRUE(mini_sstable->get_max_merged_trans_version() > 0);
    ASSERT_TRUE(mini_sstable->get_min_merged_trans_version() <= mini_sstable->get_max_merged_trans_version());
  }

  // test min/max_merged_trans_version on inc_major sstable
  sql.assign_fmt("set ob_query_timeout=600000000");
  EXEC_SUCC(sql);
  sql.assign_fmt("alter system set _enable_inc_major_direct_load=True");
  EXEC_SUCC(sql);
  sleep(3); // sleep to wait parameter change take effect
  sql.assign_fmt("insert /*+ direct(true, 0, 'inc_replace') enable_parallel_dml parallel(2) */ into t1 select random(), random(), randstr(30, random())from table(generator(100))");
  EXEC_SUCC(sql);
  ObSSTable *inc_major_sstable = nullptr;
  while (OB_SUCC(ret)) {
    ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle));
    ASSERT_TRUE(tablet_handle.is_valid());
    if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
      LOG_WARN("fail to fetch table store", K(ret));
    } else if (OB_NOT_NULL(inc_major_sstable = static_cast<ObSSTable*>(
        table_store_wrapper.get_member()->get_inc_major_sstables().get_boundary_table(true/*last*/)))) {
      break;
    } else {
      LOG_INFO("wait for inc major sstable", K(ret));
      sleep(3);
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  if (OB_NOT_NULL(inc_major_sstable)) {
    const int64_t inc_major_snapshot = inc_major_sstable->get_min_merged_trans_version();
    ASSERT_TRUE(inc_major_snapshot > 0);
    ASSERT_EQ(inc_major_snapshot, inc_major_sstable->get_max_merged_trans_version());
    ObIMacroBlockIterator *macro_block_iter = nullptr;
    ObIndexBlockMicroIterator micro_block_iter;
    const blocksstable::ObMicroBlock *micro_block = nullptr;
    ObDatumRange range;
    range.set_whole_range();
    ObMacroBlockDesc macro_desc;
    ObDataMacroBlockMeta block_meta;
    macro_desc.macro_meta_ = &block_meta;
    if (OB_FAIL(inc_major_sstable->scan_macro_block(
        range,
        tablet_handle.get_obj()->get_rowkey_read_info(),
        allocator,
        macro_block_iter,
        false, /* reverse scan */
        true, /* need micro info */
        true /* need secondary meta */))) {
      LOG_WARN("Fail to scan macro block", K(ret));
    }
    ObTransVersionSkipIndexInfo skip_index_info;
    const int64_t schema_rowkey_cnt = tablet_handle.get_obj()->get_rowkey_read_info().get_schema_rowkey_count();
    #define CHECK_MIN_VER(min_ver) \
    (expect_compat ? (0 != min_ver) : (0 == min_ver))
    while (OB_SUCC(ret) && OB_SUCC(macro_block_iter->get_next_macro_block(macro_desc))) {
      if (OB_ISNULL(macro_desc.macro_meta_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("null macro meta", K(ret), K(macro_desc.macro_meta_->val_));
      } else if (OB_FAIL(ObTransVersionSkipIndexReader::read_min_max_snapshot(
          macro_desc, schema_rowkey_cnt, skip_index_info))) {
        LOG_WARN("Failed to read min max snapshot", K(ret), K(macro_desc));
      } else {
        ASSERT_EQ(inc_major_snapshot, skip_index_info.min_snapshot_);
        ASSERT_EQ(inc_major_snapshot, skip_index_info.max_snapshot_);
        micro_block_iter.reset();
        if (OB_FAIL(micro_block_iter.init(
                    macro_desc,
                    tablet_handle.get_obj()->get_rowkey_read_info(),
                    macro_block_iter->get_micro_index_infos(),
                    macro_block_iter->get_micro_endkeys(),
                    static_cast<ObRowStoreType>(macro_desc.row_store_type_),
                    inc_major_sstable))) {
            LOG_WARN("Failed to init micro_block_iter", K(ret), K(macro_desc));
        } else {
          LOG_INFO("check macro", "macro_id", macro_desc.macro_meta_->val_.macro_id_, K(skip_index_info));
          while (OB_SUCC(ret) && OB_SUCC(micro_block_iter.next(micro_block))) {
            if (OB_ISNULL(micro_block)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("null micro block", K(ret), K(micro_block));
            } else if (OB_FAIL(ObTransVersionSkipIndexReader::read_min_max_snapshot(
                *micro_block, schema_rowkey_cnt, skip_index_info))) {
              LOG_WARN("Failed to read min max snapshot", K(ret), K(micro_block));
            } else {
              ASSERT_EQ(inc_major_snapshot, skip_index_info.min_snapshot_);
              ASSERT_EQ(inc_major_snapshot, skip_index_info.max_snapshot_);
              LOG_INFO("check micro", K(skip_index_info));
            }
          } // while
          ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
        }
      }
    } // while
    ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
    int64_t row_count = 0;
    sql.assign_fmt("select count(*) as val from t1 where ora_rowscn = %ld", inc_major_snapshot);
    if (OB_FAIL(SimpleServerHelper::select_int64(conn, sql.ptr(), row_count))) {
      COMMON_LOG(WARN, "failed to select row count", KR(ret), K(sql));
    }
    ASSERT_EQ(row_count, 100);
    LOG_INFO("check row count", K(row_count), K(inc_major_snapshot));
  } // if (OB_NOT_NULL(inc_major_sstable))
}

} // unittest
} // oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
