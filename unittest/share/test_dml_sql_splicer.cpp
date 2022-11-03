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
#include "lib/stat/ob_session_stat.h"
#include "share/ob_dml_sql_splicer.h"
#include <gtest/gtest.h>
#include "schema/db_initializer.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace share::schema;

TEST(ObDMLSqlSplicer, splice)
{
  ObDMLSqlSplicer splicer;
  ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("col1", 1));
  ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("col2", 2));
  ASSERT_EQ(OB_SUCCESS, splicer.add_column("col3", "str3"));
  ASSERT_EQ(OB_SUCCESS, splicer.add_column("col4", 4));
  ObSqlString sql;

  const char *out  = "INSERT INTO tname (col1, col2, col3, col4) VALUES (1, 2, 'str3', 4)";
  int ret = splicer.splice_insert_sql("tname", sql);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ(out, sql.ptr());

  out  = "INSERT IGNORE INTO tname (col1, col2, col3, col4) VALUES (1, 2, 'str3', 4)";
  ret = splicer.splice_insert_ignore_sql("tname", sql);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ(out, sql.ptr());

  out = "INSERT INTO tname (col1, col2, col3, col4) VALUES (1, 2, 'str3', 4)"
      " ON DUPLICATE KEY UPDATE col3 = 'str3', col4 = 4";
  ret = splicer.splice_insert_update_sql("tname", sql);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ(out, sql.ptr());

  out  = "REPLACE INTO tname (col1, col2, col3, col4) VALUES (1, 2, 'str3', 4)";
  ret = splicer.splice_replace_sql("tname", sql);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ(out, sql.ptr());

  out = "UPDATE tname SET col3 = 'str3', col4 = 4 WHERE col1 = 1 AND col2 = 2";
  ret = splicer.splice_update_sql("tname", sql);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ(out, sql.ptr());

  out = "SELECT 1 FROM tname WHERE col1 = 1 AND col2 = 2";
  ret = splicer.splice_select_1_sql("tname", sql);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ(out, sql.ptr());

  ret = splicer.splice_insert_sql(NULL, sql);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  splicer.reset();
  ret = splicer.splice_insert_sql("tname", sql);
  ASSERT_NE(OB_SUCCESS, ret);

  ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("c1", 1));
  ASSERT_EQ(OB_SUCCESS, splicer.add_column("c2", 2));
  ASSERT_NE(OB_SUCCESS, splicer.add_column(NULL, 3));
  ret = splicer.splice_insert_sql("tname", sql);
  ASSERT_EQ(OB_SUCCESS, ret);
};

TEST(ObDMLExecHelper, execute)
{
  DBInitializer db_initer;

  int ret = db_initer.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  const bool only_core_tables = false;
  ret = db_initer.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObDMLExecHelper exec(db_initer.get_sql_proxy(), OB_SYS_TENANT_ID);
  ObDMLSqlSplicer splicer;
  ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("zone", "HZ.BIG"));
  ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("name", "item1"));
  ASSERT_EQ(OB_SUCCESS, splicer.add_column("value", 1));
  ASSERT_EQ(OB_SUCCESS, splicer.add_column("info", "info"));
  ASSERT_EQ(OB_SUCCESS, splicer.add_gmt_modified());

  const char *tname = "__all_zone";
  int64_t affected_rows = 0;
  ret = exec.exec_insert(tname, splicer, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  ret = exec.exec_insert(tname, splicer, affected_rows);
  ASSERT_NE(OB_SUCCESS, ret);

  affected_rows = 0;
  ret = exec.exec_insert_ignore(tname, splicer, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, affected_rows);

  splicer.reset();
  ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("zone", "HZ.BIG2"));
  ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("name", "item1"));
  ASSERT_EQ(OB_SUCCESS, splicer.add_column("value", 1));
  ASSERT_EQ(OB_SUCCESS, splicer.add_column("info", "info"));
  ASSERT_EQ(OB_SUCCESS, splicer.add_gmt_modified());
  affected_rows = 0;
  ret = exec.exec_insert_ignore(tname, splicer, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  splicer.reset();
  ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("zone", "HZ.BIG"));
  ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("name", "item1"));
  ASSERT_EQ(OB_SUCCESS, splicer.add_column("value", 2));
  ASSERT_EQ(OB_SUCCESS, splicer.add_column("info", "info"));
  ret = exec.exec_replace(tname, splicer, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, affected_rows);

  splicer.reset();
  ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("zone", "HZ.BIG"));
  ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("name", "item2"));
  ASSERT_EQ(OB_SUCCESS, splicer.add_column("value", 2));
  ASSERT_EQ(OB_SUCCESS, splicer.add_column("info", "info"));
  ret = exec.exec_replace(tname, splicer, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  splicer.reset();
  ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("zone", "HZ.BIG"));
  ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("name", "item2"));
  ASSERT_EQ(OB_SUCCESS, splicer.add_column("value", 3));
  ASSERT_EQ(OB_SUCCESS, splicer.add_column("info", "info"));
  ret = exec.exec_insert_update(tname, splicer, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, affected_rows);
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
