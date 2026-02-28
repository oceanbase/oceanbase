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
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "lib/stat/ob_session_stat.h"
#include "share/ob_dml_sql_splicer.h"
#include "deps/oblib/src/lib/mysqlclient/ob_isql_client.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using ::testing::Invoke;
using ::testing::_;


#define ASSERT_SUCCESS(x) ASSERT_EQ(x, OB_SUCCESS)

class TestDMLSqlSplicer : public ObDMLSqlSplicer, public ::testing::Test
{
protected:
  virtual int64_t get_max_dml_num() const override { return max_dml_num_; }
  void set_max_dml_num(const int64_t max_dml_num) { max_dml_num_ = max_dml_num; }
private:
  int64_t max_dml_num_;
};

TEST_F(TestDMLSqlSplicer, batch_splice_in_array)
{
  set_max_dml_num(2);
  ObArray<ObSqlString> sqls;
  ASSERT_SUCCESS(add_column("a", 1));
  ASSERT_SUCCESS(add_column("b", 2));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(splice_batch_insert_in_array("all_test", sqls, "insert"));
  ASSERT_EQ(sqls.count(), 1);
  ASSERT_STRCASEEQ(sqls[0].ptr(), "insert into all_test (a, b) values (1, 2)");
  ASSERT_SUCCESS(add_column("a", 3));
  ASSERT_SUCCESS(add_column("b", 4));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(splice_batch_insert_in_array("all_test", sqls, "insert"));
  ASSERT_EQ(sqls.count(), 1);
  ASSERT_STRCASEEQ(sqls[0].ptr(), "insert into all_test (a, b) values (1, 2),(3, 4)");
  ASSERT_SUCCESS(add_column("a", 5));
  ASSERT_SUCCESS(add_column("b", 6));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(splice_batch_insert_in_array("all_test", sqls, "insert ignore"));
  ASSERT_EQ(sqls.count(), 2);
  ASSERT_STRCASEEQ(sqls[0].ptr(), "insert ignore into all_test (a, b) values (1, 2),(3, 4)");
  ASSERT_STRCASEEQ(sqls[1].ptr(), "insert ignore into all_test (a, b) values (5, 6)");
  reset();
  ASSERT_SUCCESS(add_column("a", 1));
  ASSERT_SUCCESS(add_column("b", 2));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(add_column("a", 3));
  ASSERT_SUCCESS(add_column("b", 4));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(set_default_columns("c", "0"));
  ASSERT_SUCCESS(splice_batch_insert_in_array("all_test", sqls, "insert ignore"));
  ASSERT_STRCASEEQ(sqls[0].ptr(), "insert ignore into all_test (a, b, c) values (1, 2, 0),(3, 4, 0)");
}

TEST_F(TestDMLSqlSplicer, batch_splice)
{
  set_max_dml_num(2);
  ObSqlString sql;
  ASSERT_SUCCESS(add_column("a", 1));
  ASSERT_SUCCESS(add_column("b", 2));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(add_column("a", 3));
  ASSERT_SUCCESS(add_column("b", 4));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(add_column("a", 5));
  ASSERT_SUCCESS(add_column("b", 6));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(splice_batch_insert_sql("all_test", sql));
  ASSERT_STRCASEEQ(sql.ptr(), "insert into all_test (a, b) values (1, 2),(3, 4),(5, 6)");
  ASSERT_SUCCESS(set_default_columns("c", "0"));
  ASSERT_SUCCESS(splice_batch_insert_sql("all_test", sql));
  ASSERT_STRCASEEQ(sql.ptr(), "insert into all_test (a, b, c) values (1, 2, 0),(3, 4, 0),(5, 6, 0)");
}

TEST_F(TestDMLSqlSplicer, long_string)
{
  set_max_dml_num(100);
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString target_sql;
  ObSqlString max_length_sql;
  ObSqlString max_length_sql_b;
  ObSqlString max_length_sql_2; // 1/2 of OB_MAX_SQL_LENGTH
  ObSqlString max_length_sql_3; // 1/3 of OB_MAX_SQL_LENGTH
  ObSqlString max_length_sql_4; // 1/4 of OB_MAX_SQL_LENGTH
  for (int64_t i = 0 ; i < OB_MAX_SQL_LENGTH; i++) {
    ASSERT_SUCCESS(max_length_sql.append("a"));
  }
  for (int64_t i = 0 ; i < OB_MAX_SQL_LENGTH; i++) {
    ASSERT_SUCCESS(max_length_sql_b.append("a"));
  }
  for (int64_t i = 0 ; i < OB_MAX_SQL_LENGTH / 2; i++) {
    ASSERT_SUCCESS(max_length_sql_2.append("a"));
  }
  for (int64_t i = 0 ; i < OB_MAX_SQL_LENGTH / 3; i++) {
    ASSERT_SUCCESS(max_length_sql_3.append("a"));
  }
  for (int64_t i = 0 ; i < OB_MAX_SQL_LENGTH / 4; i++) {
    ASSERT_SUCCESS(max_length_sql_4.append("a"));
  }
  ObArray<ObSqlString> sqls;
  ASSERT_SUCCESS(add_column("a", max_length_sql.string()));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(add_column("a", max_length_sql_b.string()));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(splice_batch_insert_in_array("all_test", sqls, "insert"));
  ASSERT_EQ(sqls.count(), 2);
  ASSERT_SUCCESS(target_sql.assign_fmt("insert into all_test (a) values ('%s')", max_length_sql.ptr()));
  ASSERT_STRCASEEQ(sqls[0].ptr(), target_sql.ptr());
  ASSERT_SUCCESS(target_sql.assign_fmt("insert into all_test (a) values ('%s')", max_length_sql_b.ptr()));
  ASSERT_STRCASEEQ(sqls[1].ptr(), target_sql.ptr());

  reset();
  ASSERT_SUCCESS(add_column("a", max_length_sql_2.string()));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(add_column("a", max_length_sql_2.string()));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(splice_batch_insert_in_array("all_test", sqls, "insert"));
  ASSERT_EQ(sqls.count(), 2);
  ASSERT_SUCCESS(target_sql.assign_fmt("insert into all_test (a) values ('%s')", max_length_sql_2.ptr()));
  ASSERT_STRCASEEQ(sqls[0].ptr(), target_sql.ptr());
  ASSERT_SUCCESS(target_sql.assign_fmt("insert into all_test (a) values ('%s')", max_length_sql_2.ptr()));
  ASSERT_STRCASEEQ(sqls[1].ptr(), target_sql.ptr());

  reset();
  ASSERT_SUCCESS(add_column("a", max_length_sql_3.string()));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(add_column("a", max_length_sql_3.string()));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(add_column("a", max_length_sql_3.string()));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(splice_batch_insert_in_array("all_test", sqls, "insert"));
  ASSERT_EQ(sqls.count(), 2);
  ASSERT_SUCCESS(target_sql.assign_fmt("insert into all_test (a) values ('%s'),('%s')", max_length_sql_3.ptr(), max_length_sql_3.ptr()));
  ASSERT_STRCASEEQ(sqls[0].ptr(), target_sql.ptr());
  ASSERT_SUCCESS(target_sql.assign_fmt("insert into all_test (a) values ('%s')", max_length_sql_3.ptr()));
  ASSERT_STRCASEEQ(sqls[1].ptr(), target_sql.ptr());

  reset();
  ASSERT_SUCCESS(add_column("a", max_length_sql_4.string()));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(add_column("a", max_length_sql_4.string()));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(add_column("a", max_length_sql_4.string()));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(add_column("a", max_length_sql_4.string()));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(splice_batch_insert_in_array("all_test", sqls, "insert"));
  ASSERT_EQ(sqls.count(), 2);
  ASSERT_SUCCESS(target_sql.assign_fmt("insert into all_test (a) values ('%s'),('%s'),('%s')", max_length_sql_4.ptr(), max_length_sql_4.ptr(), max_length_sql_4.ptr()));
  ASSERT_STRCASEEQ(sqls[0].ptr(), target_sql.ptr());
  ASSERT_SUCCESS(target_sql.assign_fmt("insert into all_test (a) values ('%s')", max_length_sql_4.ptr()));
  ASSERT_STRCASEEQ(sqls[1].ptr(), target_sql.ptr());

  set_max_dml_num(2);
  ASSERT_SUCCESS(splice_batch_insert_in_array("all_test", sqls, "insert"));
  ASSERT_EQ(sqls.count(), 2);
  ASSERT_SUCCESS(target_sql.assign_fmt("insert into all_test (a) values ('%s'),('%s')", max_length_sql_4.ptr(), max_length_sql_4.ptr()));
  ASSERT_STRCASEEQ(sqls[0].ptr(), target_sql.ptr());
  ASSERT_SUCCESS(target_sql.assign_fmt("insert into all_test (a) values ('%s'),('%s')", max_length_sql_4.ptr(), max_length_sql_4.ptr()));
  ASSERT_STRCASEEQ(sqls[1].ptr(), target_sql.ptr());
}

class MockSQLClient : public ObISQLClient
{
public:
  using ReadResult = ObISQLClient::ReadResult;
  int read(ReadResult &res, const int64_t cluster_id, const uint64_t tenant_id, const char *sql) override { return OB_OP_NOT_ALLOW; }
  int read(ReadResult &res, const uint64_t tenant_id, const char *sql) override { return OB_OP_NOT_ALLOW; }
  int read(ReadResult &res, const uint64_t tenant_id, const char *sql, const int32_t group_id) override { return OB_OP_NOT_ALLOW; }
  int escape(const char *from, const int64_t from_size, char *to, const int64_t to_size, int64_t &out_size) { return OB_OP_NOT_ALLOW; }

  MOCK_METHOD3(write, int(const uint64_t tenant_id, const char *sql, int64_t &affected_rows));
  int write(const uint64_t tenant_id, const char *sql,  const int32_t group_id, int64_t &affected_rows) override { return OB_OP_NOT_ALLOW; }

  sqlclient::ObISQLConnectionPool *get_pool() { return nullptr; }
  sqlclient::ObISQLConnection *get_connection() { return nullptr; }

  virtual bool is_oracle_mode() const { return false; }
};

TEST_F(TestDMLSqlSplicer, exec)
{
  set_max_dml_num(2);
  const char *table_name = "all_test";
  ObArray<ObSqlString> sqls;
  MockSQLClient mock_sql;
  bool has_error = false;
  int64_t index = 0;
  EXPECT_CALL(mock_sql, write(OB_SYS_TENANT_ID, _, _))
    .WillRepeatedly(Invoke([&](const uint64_t tenant_id, const char *sql, int64_t &affected_rows) -> int {
          int ret = OB_SUCCESS;
          if (index < 0 || index >= sqls.count()) {
            has_error = true;
          } else if (strcmp(sql, sqls[index].ptr()) != 0) {
            has_error = true;
          }
          affected_rows = 1;
          index++;
          return OB_SUCCESS;
      }));
  ObDMLExecHelper exec(mock_sql, OB_SYS_TENANT_ID);
  int64_t affected_rows = 1;

  ASSERT_SUCCESS(add_column("a", 1));
  ASSERT_SUCCESS(add_column("b", 2));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(add_column("a", 3));
  ASSERT_SUCCESS(add_column("b", 4));
  ASSERT_SUCCESS(finish_row());
  ASSERT_SUCCESS(add_column("a", 5));
  ASSERT_SUCCESS(add_column("b", 6));
  ASSERT_SUCCESS(finish_row());

  index = 0;
  affected_rows = 1;
  ASSERT_SUCCESS(splice_batch_insert_in_array(table_name, sqls, "insert ignore"));
  ASSERT_SUCCESS(exec.exec_batch_insert(table_name, *this, affected_rows, "insert ignore"));
  ASSERT_EQ(affected_rows, 2);
  ASSERT_FALSE(has_error);

  index = 0;
  affected_rows = 1;
  ASSERT_SUCCESS(splice_batch_insert_in_array(table_name, sqls, "insert"));
  ASSERT_SUCCESS(exec.exec_batch_insert(table_name, *this, affected_rows, "insert"));
  ASSERT_EQ(affected_rows, 2);
  ASSERT_FALSE(has_error);
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
