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

#include "lib/oblog/ob_log.h"
#include "ob_log_miner_test_utils.h"
#include "gtest/gtest.h"
#define private public
#include "ob_log_miner_record.h"
#undef private
namespace oceanbase
{
namespace oblogminer
{

TEST(test_ob_log_miner_record, InitObLogMinerRecord)
{
  ObArenaAllocator allocator("TestLogMnrRec");
  ObLogMinerBR *br = nullptr;
  ObLogMinerRecord record;
  const int buf_cnt = 10;
  binlogBuf *new_buf = static_cast<binlogBuf*>(allocator.alloc(sizeof(binlogBuf) * buf_cnt));
  binlogBuf *old_buf = static_cast<binlogBuf*>(allocator.alloc(sizeof(binlogBuf) * buf_cnt));
  for (int i = 0; i < 10; i++) {
    new_buf[i].buf = static_cast<char*>(allocator.alloc(1024));
    new_buf[i].buf_size = 1024;
    new_buf[i].buf_used_size = 0;
    old_buf[i].buf = static_cast<char*>(allocator.alloc(1024));
    old_buf[i].buf_size = 1024;
    old_buf[i].buf_used_size = 0;
  }
  record.set_allocator(&allocator);
  br = build_logminer_br(new_buf, old_buf, EINSERT,
      obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING, lib::Worker::CompatMode::MYSQL,
      "t1.db1", "tbl1", 3, "id", "1", nullptr);
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("t1", record.tenant_name_.ptr());
  EXPECT_STREQ("db1", record.database_name_.ptr());
  EXPECT_STREQ("tbl1", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO `db1`.`tbl1` (`id`) VALUES ('1');", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM `db1`.`tbl1` WHERE `id`='1' LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EUPDATE,
      obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", 6, "id", "1", "2", "name", "aaa", "bbb");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("UPDATE `db2`.`tbl2` SET `id`='1', `name`='aaa' WHERE `id`='2' AND `name`='bbb' LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("UPDATE `db2`.`tbl2` SET `id`='2', `name`='bbb' WHERE `id`='1' AND `name`='aaa' LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDELETE,
      obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", 6, "id", nullptr , "2", "name", nullptr, "bbb");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `id`='2' AND `name`='bbb' LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`id`, `name`) VALUES ('2', 'bbb');", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDDL,
      obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "", 3, "ddl_stmt_str", "CREATE TABLE T1(ID INT PRIMARY KEY);" , nullptr);
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("CREATE TABLE T1(ID INT PRIMARY KEY);", record.redo_stmt_.ptr());
  EXPECT_STREQ("/* NO SQL_UNDO GENERATED */", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDELETE,
      obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "gbk", 6,
      "id", nullptr , "2", "name", nullptr, "\xB0\xC2\xD0\xC7\xB1\xB4\xCB\xB9");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `id`='2' AND `name`='\xE5\xA5\xA5\xE6\x98\x9F\xE8\xB4\x9D\xE6\x96\xAF' LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`id`, `name`) VALUES ('2', '\xE5\xA5\xA5\xE6\x98\x9F\xE8\xB4\x9D\xE6\x96\xAF');", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDELETE,
      obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf16", 3, "name", nullptr, "\x59\x65\x66\x1F\x8D\x1D\x65\xAF");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `name`='\xE5\xA5\xA5\xE6\x98\x9F\xE8\xB4\x9D\xE6\x96\xAF' LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`name`) VALUES ('\xE5\xA5\xA5\xE6\x98\x9F\xE8\xB4\x9D\xE6\x96\xAF');", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDELETE,
      obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "latin1", 6, "id", nullptr , "2", "name", nullptr, "\x63\x69\xe0\x6f");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `id`='2' AND `name`='\x63\x69\xc3\xa0\x6f' LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`id`, `name`) VALUES ('2', '\x63\x69\xc3\xa0\x6f');", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDELETE,
      obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "gb18030", 6,
      "id", nullptr , "2", "name", nullptr, "\x95\xcb\x95\xcb\xc6\xe4\xd2\xf5\xa3\xac\xc6\xe4\xec\x59\xf2\xb3\xf2\xb3");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `id`='2' AND `name`='\xe6\x9b\x80\xe6\x9b\x80\xe5\x85\xb6\xe9\x98\xb4\xef\xbc\x8c\xe5\x85\xb6\xe9\x9d\x81\xe8\x99\xba\xe8\x99\xba' LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`id`, `name`) VALUES ('2', '\xe6\x9b\x80\xe6\x9b\x80\xe5\x85\xb6\xe9\x98\xb4\xef\xbc\x8c\xe5\x85\xb6\xe9\x9d\x81\xe8\x99\xba\xe8\x99\xba');", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDELETE,
      obmysql::EMySQLFieldType::MYSQL_TYPE_LONG, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "gb18030", 3,
      "id", nullptr , "2");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE \"id\"=2 and ROWNUM=1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"id\") VALUES (2);", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDELETE,
      obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "gb18030", 3,
      "id", nullptr , "2");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE \"id\"='2' and ROWNUM=1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"id\") VALUES ('2');", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EINSERT,
      obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "gb18030", 3,
      "id", "2" , nullptr);
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"id\") VALUES ('2');", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE \"id\"='2' and ROWNUM=1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // unsupport build undo stmt for lob type
  br = build_logminer_br(new_buf, old_buf, EINSERT,
      obmysql::EMySQLFieldType::MYSQL_TYPE_BLOB, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "gb18030", 3,
      "id", "2" , nullptr);
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"id\") VALUES ('2');", record.redo_stmt_.ptr());
  EXPECT_STREQ("/* NO SQL_UNDO GENERATED */", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EINSERT,
      obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_BLOB, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "utf8mb4", 3,
      "name", "ABCD" , nullptr);
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"name\") VALUES (HEXTORAW('41424344'));", record.redo_stmt_.ptr());
  EXPECT_STREQ("/* NO SQL_UNDO GENERATED */", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EINSERT,
      obmysql::EMySQLFieldType::MYSQL_TYPE_BLOB, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "binary", 3,
      "name", "ABCD" , nullptr);
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`name`) VALUES (UNHEX('41424344'));", record.redo_stmt_.ptr());
  EXPECT_STREQ("/* NO SQL_UNDO GENERATED */", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EINSERT,
      obmysql::EMySQLFieldType::MYSQL_TYPE_STRING, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 6,
      "name", "ABCD" , nullptr, "col2", "XXX", nullptr);
  br->get_br()->getTableMeta()->setUKs("name");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`name`, `col2`) VALUES ('ABCD', 'XXX');", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `name`='ABCD' LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EINSERT,
      obmysql::EMySQLFieldType::MYSQL_TYPE_STRING, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 6,
      "name", "ABCD" , nullptr, "col2", "XXX", nullptr);
  br->get_br()->getTableMeta()->setPKs("col2");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`name`, `col2`) VALUES ('ABCD', 'XXX');", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `col2`='XXX' LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EINSERT,
      obmysql::EMySQLFieldType::MYSQL_TYPE_STRING, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 9,
      "name", "ABCD" , nullptr, "col2", "XXX", nullptr, "col3", "val3");
  br->get_br()->getTableMeta()->setPKs("col2,col3");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`name`, `col2`, `col3`) VALUES ('ABCD', 'XXX', 'val3');", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `col2`='XXX' AND `col3`='val3' LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EINSERT,
      obmysql::EMySQLFieldType::MYSQL_TYPE_STRING, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 9,
      "name", "ABCD" , nullptr, "col2", "XXX", nullptr, "col3", "val3");
  br->get_br()->getTableMeta()->setUKs("col2,col3");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`name`, `col2`, `col3`) VALUES ('ABCD', 'XXX', 'val3');", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `col2`='XXX' AND `col3`='val3' LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EINSERT,
      obmysql::EMySQLFieldType::MYSQL_TYPE_LONG, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 9,
      "col1", "1", nullptr, "col2", "2", nullptr, "col3", "3");
  br->get_br()->getTableMeta()->setUKs("col2,col3");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`col1`, `col2`, `col3`) VALUES (1, 2, 3);", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `col2`=2 AND `col3`=3 LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();
}

}
}

int main(int argc, char **argv)
{
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  system("rm -f test_ob_log_miner_record.log");
  oceanbase::ObLogger &logger = oceanbase::ObLogger::get_logger();
  logger.set_file_name("test_ob_log_miner_record.log", true, false);
  logger.set_log_level("DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
