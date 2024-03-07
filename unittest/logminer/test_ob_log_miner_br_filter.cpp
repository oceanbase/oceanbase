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

#include "gtest/gtest.h"
#include "lib/oblog/ob_log.h"
#include "ob_log_miner_br_filter.h"
#include "ob_log_miner_filter_condition.h"
#include "ob_log_miner_br.h"
#include "ob_log_miner_test_utils.h"


using namespace oceanbase;

namespace oceanbase
{
namespace oblogminer
{

TEST(ob_log_miner_br_filter, ColumnBRFilterPlugin)
{
  ObArenaAllocator arena_alloc("FilterTest");
  ColumnBRFilterPlugin col_filter(&arena_alloc);
  ObLogMinerBR *br = nullptr;
  bool need_filter = false;
  const int buf_cnt = 10;
  binlogBuf *new_buf = static_cast<binlogBuf*>(arena_alloc.alloc(sizeof(binlogBuf) * buf_cnt));
  binlogBuf *old_buf = static_cast<binlogBuf*>(arena_alloc.alloc(sizeof(binlogBuf) * buf_cnt));
  for (int i = 0; i < 10; i++) {
    new_buf[i].buf = static_cast<char*>(arena_alloc.alloc(1024));
    new_buf[i].buf_size = 1024;
    new_buf[i].buf_used_size = 0;
    old_buf[i].buf = static_cast<char*>(arena_alloc.alloc(1024));
    old_buf[i].buf_size = 1024;
    old_buf[i].buf_used_size = 0;
  }
  EXPECT_EQ(OB_SUCCESS, col_filter.init("[]"));
  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "col1", "val1", "val2",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);
  col_filter.destroy();
  EXPECT_EQ(OB_INVALID_ARGUMENT, col_filter.init("[{}]"));
  col_filter.destroy();
  EXPECT_EQ(OB_INVALID_ARGUMENT, col_filter.init("[{\"table_name\":\"aaa\", \
  \"table_name\":\"aaa\"}]"));
  col_filter.destroy();
  EXPECT_EQ(OB_INVALID_ARGUMENT, col_filter.init("[{\"tenant_name\":\"aaa\", \
  \"table_name\":\"aaa\"}]"));
  col_filter.destroy();
  EXPECT_EQ(OB_INVALID_ARGUMENT, col_filter.init("[{\"database_name\":\"aaa\", \
  \"database_name\":\"aaa\"}]"));
  col_filter.destroy();
  EXPECT_EQ(OB_INVALID_ARGUMENT, col_filter.init("[{\"database_name\":\"t1.aaa\", \
  \"table_name\":\"aaa\"}]"));
  col_filter.destroy();
  EXPECT_EQ(OB_INVALID_ARGUMENT, col_filter.init("[{\"database_name\":\"t1.aaa\", \
  \"table_name\":\"aaa\", \"column_cond\":[{\"col1\": 3.1415926}]}]"));
  col_filter.destroy();
  EXPECT_EQ(OB_SUCCESS, col_filter.init("[{\"database_name\":\"t1.aaa\", \
  \"table_name\":\"aaa\", \"column_cond\":[{\"col1\": \"3.1415926\",\"col2\":\"abcde\"}]}]"));
  col_filter.destroy();
  EXPECT_EQ(OB_INVALID_ARGUMENT, col_filter.init("[{\"database_name\":\"t1.aaa\", \
  \"table_name\":\"aaa\", \"column_cond\":[{\"col1\": \"3.1415926\",\"col2\":\"abcde\"}]"));
  col_filter.destroy();
  EXPECT_EQ(OB_SUCCESS, col_filter.init("[{\"database_name\":\"t1.aaa\", \
  \"table_name\":\"aaa\", \"column_cond\":[{\"col1\": \"3.1415926\",\"col2\":\"abcde\"}, {\"col3\":null}]}]"));
  col_filter.destroy();
  EXPECT_EQ(OB_INVALID_ARGUMENT, col_filter.init("[\"table_name\":\"aaa\", "
      "\"column_cond\":[{\"col1\": \"3.1415926\",\"col2\":\"abcde\"},{\"col3\":null}]}]"));
  col_filter.destroy();
  EXPECT_EQ(OB_INVALID_ARGUMENT, col_filter.init("[\"column_cond\":[{\"col1\": \"3.1415926\",\"col2\":\"abcde\"},{\"col3\":null}]}]"));
  col_filter.destroy();

  EXPECT_EQ(OB_INVALID_ARGUMENT, col_filter.init("db1.t1|db2.t2|db3.t*"));
  col_filter.destroy();

  EXPECT_EQ(OB_INVALID_ARGUMENT, col_filter.init("db1.*|db2"));
  col_filter.destroy();

  EXPECT_EQ(OB_SUCCESS, col_filter.init("["
    "{\"database_name\":\"db1\",\"table_name\":\"table1\","
      "\"column_cond\":[{\"col1\":\"val1\"},{\"col2\":\"val2\"}]"
    "}"
  "]"));
  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "col1", "val1", "val2",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);
  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "col2", "val3", "val2",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);
  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 8, "col2", "val3", "val3",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING), "col1", nullptr, "val1",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);
  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db2", "table1", 8, "col2", "val3", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "col1", "val1", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);
  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table2", 8, "col2", "val3", "val3",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "col1", nullptr, "val1", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);
  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 8, "col2", "val3", "val3",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "col3", nullptr, "val4", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);
  br = build_logminer_br(new_buf, old_buf, EDDL, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table2", 0);
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);
  col_filter.destroy();

  EXPECT_EQ(OB_INVALID_ARGUMENT, col_filter.init("db*.t1|*_prod.*_history|db3.t*"));
  col_filter.destroy();

  EXPECT_EQ(OB_SUCCESS, col_filter.init(
    "[{\"database_name\":\"db1\", \"table_name\":\"table1\","
    "\"column_cond\":[{\"col1\":\"val1\", \"col2\":\"val2\"}]}]"));
  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db2", "non_spec_tbl", 3, "non_spec_col", "non_spec_val", nullptr);
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db0", "non_spec_tbl", 4, "non_spec_col", "non_spec_val", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db3", "non_spec_tbl", 4, "non_spec_col", "non_spec_val", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "non_spec_col", "non_spec_val", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db3", "tmp_tbl", 4, "non_spec_col", "non_spec_val", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "non_spec_col", "non_spec_val", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 8, "col1", "val2", "val2",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "col2", "val1", "val3", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 8, "col1", "val1", "val1",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "col2", "val1", "val3", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 8, "col1", "val10", "val100",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "col2", "val2", "val2", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 8, "col1", "val1", "val2",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "col2", "val1", "val2", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 8, "col1", nullptr, "val1",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "col2", nullptr, "val2", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 12, "col1", "val10", "val1",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "col2", "val20", "val2", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "col3", "val3", "val30", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);
  col_filter.destroy();

  // null value filter
  EXPECT_EQ(OB_SUCCESS, col_filter.init(
    "[{\"database_name\":\"db1\", \"table_name\":\"table1\","
    "\"column_cond\":[{\"col1\":null}]}]"));
  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db2", "non_spec_tbl", 4, "non_spec_col", "non_spec_val", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EUPDATE,lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "col1", "non_spec_val", "xx",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "col1", "non_spec_val", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "col1", nullptr, nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "col1", nullptr, nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "col1", nullptr, "xx",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);
  col_filter.destroy();

  // multi column filter with null
  EXPECT_EQ(OB_SUCCESS, col_filter.init(
    "[{\"database_name\":\"db1\", \"table_name\":\"table1\","
    "\"column_cond\":[{\"col1\":\"val1\", \"col2\":\"val2\"}, {\"col3\":null}]}]"));
  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db2", "non_spec_tbl", 4, "non_spec_col", "non_spec_val", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT,lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "non_spec_col", "non_spec_val", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "col3", nullptr, nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 8, "col1", "val1", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "col2", "val2", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "col1", "val1", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);
  col_filter.destroy();


  // multi column filter
  EXPECT_EQ(OB_SUCCESS, col_filter.init(
    "[{\"database_name\":\"db1\", \"table_name\":\"table1\","
    "\"column_cond\":[{\"col1\":\"val1\", \"col2\":\"val2\"}, {\"col3\":\"val3\", \"col4\":\"val4\"}]}]"));
  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db2", "non_spec_tbl", 4, "non_spec_col", "non_spec_val", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "col1", "val1", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 8, "col1", "val1", nullptr,
       static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
       "col2", "val2", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 8, "col1", "val1", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "col3", "val3", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, col_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);
  col_filter.destroy();
}

TEST(ob_log_miner_br_filter, OperationBRFilterPlugin)
{
  OperationBRFilterPlugin op_filter;
  ObArenaAllocator arena_alloc("OpFilterTest");
  const int64_t buf_cnt = 10;
  ObLogMinerBR *br = nullptr;
  bool need_filter = false;
  binlogBuf *new_buf = static_cast<binlogBuf*>(arena_alloc.alloc(sizeof(binlogBuf) * buf_cnt));
  binlogBuf *old_buf = static_cast<binlogBuf*>(arena_alloc.alloc(sizeof(binlogBuf) * buf_cnt));
  for (int i = 0; i < 10; i++) {
    new_buf[i].buf = static_cast<char*>(arena_alloc.alloc(1024));
    new_buf[i].buf_size = 1024;
    new_buf[i].buf_used_size = 0;
    old_buf[i].buf = static_cast<char*>(arena_alloc.alloc(1024));
    old_buf[i].buf_size = 1024;
    old_buf[i].buf_used_size = 0;
  }
  EXPECT_EQ(OB_INVALID_ARGUMENT, op_filter.init("aaa|bbb"));
  op_filter.destroy();
  EXPECT_EQ(OB_SUCCESS, op_filter.init("Insert"));
  op_filter.destroy();
  EXPECT_EQ(OB_INVALID_ARGUMENT, op_filter.init("lnsert"));
  op_filter.destroy();

  EXPECT_EQ(OB_ERR_UNEXPECTED, op_filter.init(nullptr));
  op_filter.destroy();


  EXPECT_EQ(OB_SUCCESS, op_filter.init("insert"));

  br = build_logminer_br(new_buf, old_buf, EBEGIN, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 0);
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 0);
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 0);
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 0);
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, HEARTBEAT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 0);
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  op_filter.destroy();

  EXPECT_EQ(OB_SUCCESS, op_filter.init("insert|update"));

  br = build_logminer_br(new_buf, old_buf, HEARTBEAT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 0);
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 0);
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 0);
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  op_filter.destroy();

  EXPECT_EQ(OB_SUCCESS, op_filter.init("insert|update|delete"));

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "col1", "val10", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 12, "col1", "val10", "val1",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "col2", "val20", "val2", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "col3", "val3", "val30", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 4, "col1", nullptr, "val1",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EBEGIN, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 0);
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, ECOMMIT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 0);
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, HEARTBEAT, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 0);
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EBEGIN, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 0);
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(false, need_filter);
  destroy_miner_br(br);

  br = build_logminer_br(new_buf, old_buf, EREPLACE, lib::Worker::CompatMode::MYSQL,
      "tenant1.db1", "table1", 0);
  EXPECT_EQ(OB_SUCCESS, op_filter.filter(*br, need_filter));
  EXPECT_EQ(true, need_filter);
  destroy_miner_br(br);

  op_filter.destroy();
}

}
}

int main(int argc, char **argv)
{
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  system("rm -f test_ob_log_miner_filter.log");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_log_miner_filter.log", true, false);
  logger.set_log_level("DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
