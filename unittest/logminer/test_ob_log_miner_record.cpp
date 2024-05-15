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
  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "t1.db1", "tbl1", 4, "id", "1", nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("t1", record.tenant_name_.ptr());
  EXPECT_STREQ("db1", record.database_name_.ptr());
  EXPECT_STREQ("tbl1", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO `db1`.`tbl1` (`id`) VALUES ('1');", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM `db1`.`tbl1` WHERE `id`='1' LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", 8, "id", "1", "2",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "name", "aaa", "bbb", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("UPDATE `db2`.`tbl2` SET `id`='1', `name`='aaa' WHERE `id`='2' AND `name`='bbb' LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("UPDATE `db2`.`tbl2` SET `id`='2', `name`='bbb' WHERE `id`='1' AND `name`='aaa' LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", 8, "id", nullptr , "2",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "name", nullptr, "bbb", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `id`='2' AND `name`='bbb' LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`id`, `name`) VALUES ('2', 'bbb');", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDDL, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "", 4, "ddl_stmt_str", "CREATE TABLE T1(ID INT PRIMARY KEY);" , nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("CREATE TABLE T1(ID INT PRIMARY KEY);", record.redo_stmt_.ptr());
  EXPECT_STREQ("/* NO SQL_UNDO GENERATED */", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "gbk", 8,
      "id", nullptr , "2",static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "name", nullptr, "\xB0\xC2\xD0\xC7\xB1\xB4\xCB\xB9",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `id`='2' AND `name`='\xE5\xA5\xA5\xE6\x98\x9F\xE8\xB4\x9D\xE6\x96\xAF' LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`id`, `name`) VALUES ('2', '\xE5\xA5\xA5\xE6\x98\x9F\xE8\xB4\x9D\xE6\x96\xAF');", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf16", 4, "name", nullptr, "\x59\x65\x66\x1F\x8D\x1D\x65\xAF",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `name`='\xE5\xA5\xA5\xE6\x98\x9F\xE8\xB4\x9D\xE6\x96\xAF' LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`name`) VALUES ('\xE5\xA5\xA5\xE6\x98\x9F\xE8\xB4\x9D\xE6\x96\xAF');", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "latin1", 8, "id", nullptr , "2",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "name", nullptr, "\x63\x69\xe0\x6f",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `id`='2' AND `name`='\x63\x69\xc3\xa0\x6f' LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`id`, `name`) VALUES ('2', '\x63\x69\xc3\xa0\x6f');", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "gb18030", 8,
      "id", nullptr , "2",static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING),
      "name", nullptr, "\x95\xcb\x95\xcb\xc6\xe4\xd2\xf5\xa3\xac\xc6\xe4\xec\x59\xf2\xb3\xf2\xb3",
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `id`='2' AND `name`='\xe6\x9b\x80\xe6\x9b\x80\xe5\x85\xb6\xe9\x98\xb4\xef\xbc\x8c\xe5\x85\xb6\xe9\x9d\x81\xe8\x99\xba\xe8\x99\xba' LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`id`, `name`) VALUES ('2', '\xe6\x9b\x80\xe6\x9b\x80\xe5\x85\xb6\xe9\x98\xb4\xef\xbc\x8c\xe5\x85\xb6\xe9\x9d\x81\xe8\x99\xba\xe8\x99\xba');", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "gb18030", 4,
      "id", nullptr , "2", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE \"id\"=2 and ROWNUM=1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"id\") VALUES (2);", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "gb18030", 4,
      "id", nullptr , "2", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE \"id\"='2' and ROWNUM=1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"id\") VALUES ('2');", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "gb18030", 4,
      "id", "2" , nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_VAR_STRING));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"id\") VALUES ('2');", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE \"id\"='2' and ROWNUM=1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "gb18030", 4,
      "id", "2" , nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_BLOB));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"id\") VALUES ('2');", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE 0=DBMS_LOB.COMPARE(\"id\", '2') and ROWNUM=1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "utf8mb4", 4,
      "name", "ABCD" , nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_BLOB));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"name\") VALUES (HEXTORAW('41424344'));", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE 0=DBMS_LOB.COMPARE(\"name\", HEXTORAW('41424344')) and ROWNUM=1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "binary", 4,
      "name", "ABCD" , nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_BLOB));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`name`) VALUES (UNHEX('41424344'));", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `name`=UNHEX('41424344') LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 8, "name", "ABCD" , nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_STRING),
      "col2", "XXX", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_STRING));
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

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 8, "name", "ABCD" , nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_STRING),
      "col2", "XXX", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_STRING));
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

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 12, "name", "ABCD" , nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_STRING),
      "col2", "XXX", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_STRING),
      "col3", "val3", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_STRING));
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

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 12, "name", "ABCD" , nullptr,
      static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_STRING),
      "col2", "XXX", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_STRING),
      "col3", "val3", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_STRING));
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

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 12,
      "col1", "1", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG),
      "col2", "2", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG),
      "col3", "3", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG));
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

  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 8,
      "col1", "1", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_BIT),
      "col2", "1836032", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_BIT));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`col1`, `col2`) VALUES (b'1', b'111000000010000000000');", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `col1`=b'1' AND `col2`=b'111000000010000000000' LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();
}

TEST(test_ob_log_miner_record, LobTypeInMySqlMode)
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


  // mysql multimode
  // insert without key
  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 8,
      "col1", "{\"key\": \"value\"}", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", "POINT(0 1)", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`col1`, `col2`) VALUES ('{\"key\": \"value\"}', ST_GeomFromText('POINT(0 1)'));",record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `col1`=cast('{\"key\": \"value\"}'as json) AND ST_Equals(`col2`, ST_GeomFromText('POINT(0 1)')) LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // insert with key
  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 12,
      "col1", "{\"key\": \"value\"}", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", "POINT(0 1)", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", "1", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG));
  br->get_br()->getTableMeta()->setUKs("col3");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`col1`, `col2`, `col3`) VALUES ('{\"key\": \"value\"}', ST_GeomFromText('POINT(0 1)'), 1);", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `col3`=1 LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // null value insert without key
  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 8,
      "col1", nullptr, nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", "POINT(0 1)", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`col1`, `col2`) VALUES (NULL, ST_GeomFromText('POINT(0 1)'));",record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `col1` IS NULL AND ST_Equals(`col2`, ST_GeomFromText('POINT(0 1)')) LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // null value insert with key
  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 12,
      "col1", "{\"key\": \"value\"}", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", nullptr, nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", "1", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG));
  br->get_br()->getTableMeta()->setUKs("col3");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`col1`, `col2`, `col3`) VALUES ('{\"key\": \"value\"}', NULL, 1);", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `col3`=1 LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // update without key
  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 8,
      "col1", "{\"key\": \"new\"}", "{\"key\": \"old\"}", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", "POINT(0 1)", "POINT(2 3)", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("UPDATE `db2`.`tbl2` SET `col1`='{\"key\": \"new\"}', `col2`=ST_GeomFromText('POINT(0 1)') WHERE `col1`=cast('{\"key\": \"old\"}'as json) AND ST_Equals(`col2`, ST_GeomFromText('POINT(2 3)')) LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("UPDATE `db2`.`tbl2` SET `col1`='{\"key\": \"old\"}', `col2`=ST_GeomFromText('POINT(2 3)') WHERE `col1`=cast('{\"key\": \"new\"}'as json) AND ST_Equals(`col2`, ST_GeomFromText('POINT(0 1)')) LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // update with key
  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 12,
      "col1", "{\"key\": \"new\"}", "{\"key\": \"old\"}", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", "POINT(0 1)", "POINT(2 3)", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", "1", "2", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG));
  br->get_br()->getTableMeta()->setUKs("col3");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("UPDATE `db2`.`tbl2` SET `col1`='{\"key\": \"new\"}', `col2`=ST_GeomFromText('POINT(0 1)'), `col3`=1 WHERE `col3`=2 LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("UPDATE `db2`.`tbl2` SET `col1`='{\"key\": \"old\"}', `col2`=ST_GeomFromText('POINT(2 3)'), `col3`=2 WHERE `col3`=1 LIMIT 1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // null value update without key
  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 8,
      "col1", "{\"key\": \"new\"}", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", "POINT(0 1)", "POINT(2 3)", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("UPDATE `db2`.`tbl2` SET `col1`='{\"key\": \"new\"}', `col2`=ST_GeomFromText('POINT(0 1)') WHERE `col1` IS NULL AND ST_Equals(`col2`, ST_GeomFromText('POINT(2 3)')) LIMIT 1;/* POTENTIALLY INACCURATE */",record.redo_stmt_.ptr());
  EXPECT_STREQ("UPDATE `db2`.`tbl2` SET `col1`=NULL, `col2`=ST_GeomFromText('POINT(2 3)') WHERE `col1`=cast('{\"key\": \"new\"}'as json) AND ST_Equals(`col2`, ST_GeomFromText('POINT(0 1)')) LIMIT 1;/* POTENTIALLY INACCURATE */", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // null value update with key
  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 12,
      "col1", "{\"key\": \"new\"}", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", "POINT(0 1)", "POINT(2 3)", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", "1", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG));
  br->get_br()->getTableMeta()->setUKs("col3");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("UPDATE `db2`.`tbl2` SET `col1`='{\"key\": \"new\"}', `col2`=ST_GeomFromText('POINT(0 1)'), `col3`=1 WHERE `col3` IS NULL LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("UPDATE `db2`.`tbl2` SET `col1`=NULL, `col2`=ST_GeomFromText('POINT(2 3)'), `col3`=NULL WHERE `col3`=1 LIMIT 1;/* POTENTIALLY INACCURATE */", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // delete without key
  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 8,
      "col1", nullptr, "{\"key\": \"old\"}", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", nullptr, "POINT(2 3)", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `col1`=cast('{\"key\": \"old\"}'as json) AND ST_Equals(`col2`, ST_GeomFromText('POINT(2 3)')) LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`col1`, `col2`) VALUES ('{\"key\": \"old\"}', ST_GeomFromText('POINT(2 3)'));", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // delete with key
  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 12,
      "col1", nullptr, "{\"key\": \"old\"}", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", nullptr, "POINT(2 3)", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", nullptr, "2", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG));
  br->get_br()->getTableMeta()->setUKs("col3");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `col3`=2 LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`col1`, `col2`, `col3`) VALUES ('{\"key\": \"old\"}', ST_GeomFromText('POINT(2 3)'), 2);", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // null value delete without key
  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 8,
      "col1", nullptr, nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", nullptr, "POINT(2 3)", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `col1` IS NULL AND ST_Equals(`col2`, ST_GeomFromText('POINT(2 3)')) LIMIT 1;/* POTENTIALLY INACCURATE */",record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`col1`, `col2`) VALUES (NULL, ST_GeomFromText('POINT(2 3)'));/* POTENTIALLY INACCURATE */", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // null value delete with key
  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::MYSQL,
      "tenant2.db2", "tbl2", "utf8mb4", 12,
      "col1", nullptr, nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", nullptr, "POINT(2 3)", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", nullptr, "1", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG));
  br->get_br()->getTableMeta()->setUKs("col3");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM `db2`.`tbl2` WHERE `col3`=1 LIMIT 1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO `db2`.`tbl2` (`col1`, `col2`, `col3`) VALUES (NULL, ST_GeomFromText('POINT(2 3)'), 1);/* POTENTIALLY INACCURATE */", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();
}

TEST(test_ob_log_miner_record, LobTypeInOracleMode)
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
  // multimode
  // insert without key
  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "utf8mb4", 16,
      "col1", "{\"key\": \"value\"}", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", "SRID=NULL;POINT(0 1)", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", "<a>abc</a>", nullptr, drcmsg_field_types::DRCMSG_TYPE_ORA_XML,
      "col4", "AABB1122", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_CLOB));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"col1\", \"col2\", \"col3\", \"col4\") VALUES ('{\"key\": \"value\"}', SDO_GEOMETRY('POINT(0 1)', NULL), '<a>abc</a>', 'AABB1122');",record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE JSON_EQUAL(\"col1\", '{\"key\": \"value\"}') AND \"col2\"=SDO_GEOMETRY('POINT(0 1)', NULL) AND \"col3\"='<a>abc</a>' AND 0=DBMS_LOB.COMPARE(\"col4\", 'AABB1122') and ROWNUM=1;/* POTENTIALLY INACCURATE */", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // insert with key
  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "utf8mb4", 20,
      "col1", "{\"key\": \"value\"}", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", "SRID=NULL;POINT(0 1)", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", "<a>abc</a>", nullptr, drcmsg_field_types::DRCMSG_TYPE_ORA_XML,
      "col4", "AABB1122", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_CLOB),
      "col5", "1", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG));
  br->get_br()->getTableMeta()->setUKs("col5");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"col1\", \"col2\", \"col3\", \"col4\", \"col5\") VALUES ('{\"key\": \"value\"}', SDO_GEOMETRY('POINT(0 1)', NULL), '<a>abc</a>', 'AABB1122', 1);", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE \"col5\"=1 and ROWNUM=1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // null value insert without key
  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "utf8mb4", 16,
      "col1", nullptr, nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col2", "SRID=NULL;POINT(0 1)", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", "<a>abc</a>", nullptr, drcmsg_field_types::DRCMSG_TYPE_ORA_XML,
      "col4", "AABB1122", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_CLOB));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"col1\", \"col2\", \"col3\", \"col4\") VALUES (NULL, SDO_GEOMETRY('POINT(0 1)', NULL), '<a>abc</a>', 'AABB1122');",record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE \"col1\" IS NULL AND \"col2\"=SDO_GEOMETRY('POINT(0 1)', NULL) AND \"col3\"='<a>abc</a>' AND 0=DBMS_LOB.COMPARE(\"col4\", 'AABB1122') and ROWNUM=1;/* POTENTIALLY INACCURATE */", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // null value insert with key
  br = build_logminer_br(new_buf, old_buf, EINSERT, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "utf8mb4", 20,
      "col1", "{\"key\": \"value\"}", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", nullptr, nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", "<a>abc</a>", nullptr, drcmsg_field_types::DRCMSG_TYPE_ORA_XML,
      "col4", "AABB1122", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_CLOB),
      "col5", "1", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG));
  br->get_br()->getTableMeta()->setUKs("col5");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"col1\", \"col2\", \"col3\", \"col4\", \"col5\") VALUES ('{\"key\": \"value\"}', NULL, '<a>abc</a>', 'AABB1122', 1);", record.redo_stmt_.ptr());
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE \"col5\"=1 and ROWNUM=1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // update without key
  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "utf8mb4", 16,
      "col1", "{\"key\": \"new\"}", "{\"key\": \"old\"}", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", "SRID=NULL;POINT(0 1)", "SRID=NULL;POINT(2 1)", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", "<a>abc</a>", "<a>cba</a>", drcmsg_field_types::DRCMSG_TYPE_ORA_XML,
      "col4", "AABB1122", "1122", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_CLOB));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("UPDATE \"db2\".\"tbl2\" SET \"col1\"='{\"key\": \"new\"}', \"col2\"=SDO_GEOMETRY('POINT(0 1)', NULL),"
      " \"col3\"='<a>abc</a>', \"col4\"='AABB1122' WHERE JSON_EQUAL(\"col1\", '{\"key\": \"old\"}') AND "
      "\"col2\"=SDO_GEOMETRY('POINT(2 1)', NULL) AND \"col3\"='<a>cba</a>' AND 0=DBMS_LOB.COMPARE(\"col4\", '1122')"
      " AND ROWNUM=1;/* POTENTIALLY INACCURATE */", record.redo_stmt_.ptr());
  EXPECT_STREQ("UPDATE \"db2\".\"tbl2\" SET \"col1\"='{\"key\": \"old\"}', \"col2\"=SDO_GEOMETRY('POINT(2 1)', NULL),"
      " \"col3\"='<a>cba</a>', \"col4\"='1122' WHERE JSON_EQUAL(\"col1\", '{\"key\": \"new\"}') AND "
      "\"col2\"=SDO_GEOMETRY('POINT(0 1)', NULL) AND \"col3\"='<a>abc</a>' AND 0=DBMS_LOB.COMPARE(\"col4\", 'AABB1122')"
      " AND ROWNUM=1;/* POTENTIALLY INACCURATE */", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // update with key
  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "utf8mb4", 20,
      "col1", "{\"key\": \"new\"}", "{\"key\": \"old\"}", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", "SRID=NULL;POINT(0 1)", "SRID=NULL;POINT(2 1)", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", "<a>abc</a>", "<a>cba</a>", drcmsg_field_types::DRCMSG_TYPE_ORA_XML,
      "col4", "AABB1122", "1122", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_CLOB),
      "col5", "1", "2", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG));
  br->get_br()->getTableMeta()->setUKs("col5");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("UPDATE \"db2\".\"tbl2\" SET \"col1\"='{\"key\": \"new\"}', \"col2\"=SDO_GEOMETRY('POINT(0 1)', NULL), \"col3\"='<a>abc</a>', \"col4\"='AABB1122', \"col5\"=1 WHERE \"col5\"=2 AND ROWNUM=1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("UPDATE \"db2\".\"tbl2\" SET \"col1\"='{\"key\": \"old\"}', \"col2\"=SDO_GEOMETRY('POINT(2 1)', NULL), \"col3\"='<a>cba</a>', \"col4\"='1122', \"col5\"=2 WHERE \"col5\"=1 AND ROWNUM=1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // null value update without key
  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "utf8mb4", 16,
      "col1", "{\"key\": \"new\"}", "{\"key\": \"old\"}", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", "SRID=NULL;POINT(0 1)", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", nullptr, nullptr, drcmsg_field_types::DRCMSG_TYPE_ORA_XML,
      "col4", "AABB1122", "1122", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_CLOB));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("UPDATE \"db2\".\"tbl2\" SET \"col1\"='{\"key\": \"new\"}', \"col2\"=SDO_GEOMETRY('POINT(0 1)', NULL),"
      " \"col3\"=NULL, \"col4\"='AABB1122' WHERE JSON_EQUAL(\"col1\", '{\"key\": \"old\"}') AND \"col2\" IS NULL AND "
      "\"col3\" IS NULL AND 0=DBMS_LOB.COMPARE(\"col4\", '1122') AND ROWNUM=1;/* POTENTIALLY INACCURATE */",record.redo_stmt_.ptr());
  EXPECT_STREQ("UPDATE \"db2\".\"tbl2\" SET \"col1\"='{\"key\": \"old\"}', \"col2\"=NULL, \"col3\"=NULL,"
      " \"col4\"='1122' WHERE JSON_EQUAL(\"col1\", '{\"key\": \"new\"}') AND \"col2\"=SDO_GEOMETRY('POINT(0 1)', NULL)"
      " AND \"col3\" IS NULL AND 0=DBMS_LOB.COMPARE(\"col4\", 'AABB1122') AND ROWNUM=1;/* POTENTIALLY INACCURATE */", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // null value update with key
  br = build_logminer_br(new_buf, old_buf, EUPDATE, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "utf8mb4", 20,
      "col1", "{\"key\": \"new\"}", "{\"key\": \"old\"}", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", "SRID=NULL;POINT(0 1)", "SRID=NULL;POINT(2 1)", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", "<a>abc</a>", "<a>cba</a>", drcmsg_field_types::DRCMSG_TYPE_ORA_XML,
      "col4", "AABB1122", "1122", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_CLOB),
      "col5", "1", nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG));
  br->get_br()->getTableMeta()->setUKs("col5");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("UPDATE \"db2\".\"tbl2\" SET \"col1\"='{\"key\": \"new\"}', \"col2\"=SDO_GEOMETRY('POINT(0 1)', NULL), \"col3\"='<a>abc</a>', \"col4\"='AABB1122', \"col5\"=1 WHERE \"col5\" IS NULL AND ROWNUM=1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("UPDATE \"db2\".\"tbl2\" SET \"col1\"='{\"key\": \"old\"}', \"col2\"=SDO_GEOMETRY('POINT(2 1)', NULL), \"col3\"='<a>cba</a>', \"col4\"='1122', \"col5\"=NULL WHERE \"col5\"=1 AND ROWNUM=1;", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // delete without key
  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "utf8mb4", 16,
      "col1", nullptr, "{\"key\": \"value\"}", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", nullptr, "SRID=NULL;POINT(0 1)", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", nullptr, "<a>abc</a>", drcmsg_field_types::DRCMSG_TYPE_ORA_XML,
      "col4", nullptr, "AABB1122", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_CLOB));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE JSON_EQUAL(\"col1\", '{\"key\": \"value\"}') AND \"col2\"=SDO_GEOMETRY('POINT(0 1)', NULL) AND \"col3\"='<a>abc</a>' AND 0=DBMS_LOB.COMPARE(\"col4\", 'AABB1122') and ROWNUM=1;/* POTENTIALLY INACCURATE */", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"col1\", \"col2\", \"col3\", \"col4\") VALUES ('{\"key\": \"value\"}', SDO_GEOMETRY('POINT(0 1)', NULL), '<a>abc</a>', 'AABB1122');", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // delete with key
  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "utf8mb4", 20,
      "col1", nullptr, "{\"key\": \"value\"}", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", nullptr, "SRID=NULL;POINT(0 1)", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", nullptr, "<a>abc</a>", drcmsg_field_types::DRCMSG_TYPE_ORA_XML,
      "col4", nullptr, "AABB1122", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_CLOB),
      "col5", nullptr, "2", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG));
  br->get_br()->getTableMeta()->setUKs("col5");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE \"col5\"=2 and ROWNUM=1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"col1\", \"col2\", \"col3\", \"col4\", \"col5\") VALUES ('{\"key\": \"value\"}', SDO_GEOMETRY('POINT(0 1)', NULL), '<a>abc</a>', 'AABB1122', 2);", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // null value delete without key
  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "utf8mb4", 16,
      "col1", nullptr, "{\"key\": \"value\"}", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", nullptr, nullptr, static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", nullptr, "<a>abc</a>", drcmsg_field_types::DRCMSG_TYPE_ORA_XML,
      "col4", nullptr, "AABB1122", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_CLOB));
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE JSON_EQUAL(\"col1\", '{\"key\": \"value\"}') AND \"col2\" IS NULL AND \"col3\"='<a>abc</a>' AND 0=DBMS_LOB.COMPARE(\"col4\", 'AABB1122') and ROWNUM=1;/* POTENTIALLY INACCURATE */",record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"col1\", \"col2\", \"col3\", \"col4\") VALUES ('{\"key\": \"value\"}', NULL, '<a>abc</a>', 'AABB1122');/* POTENTIALLY INACCURATE */", record.undo_stmt_.ptr());
  destroy_miner_br(br);
  record.destroy();

  // null value delete with key
  br = build_logminer_br(new_buf, old_buf, EDELETE, lib::Worker::CompatMode::ORACLE,
      "tenant2.db2", "tbl2", "utf8mb4", 20,
      "col1", nullptr, "{\"key\": \"value\"}", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_JSON),
      "col2", nullptr, "SRID=NULL;POINT(0 1)", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_GEOMETRY),
      "col3", nullptr, "<a>abc</a>", drcmsg_field_types::DRCMSG_TYPE_ORA_XML,
      "col4", nullptr, "AABB1122", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_ORA_CLOB),
      "col5", nullptr, "1", static_cast<int>(obmysql::EMySQLFieldType::MYSQL_TYPE_LONG));
  br->get_br()->getTableMeta()->setUKs("col5");
  EXPECT_EQ(OB_SUCCESS, record.init(*br));
  EXPECT_STREQ("tenant2", record.tenant_name_.ptr());
  EXPECT_STREQ("db2", record.database_name_.ptr());
  EXPECT_STREQ("tbl2", record.table_name_.ptr());
  EXPECT_EQ(OB_SUCCESS, record.build_stmts(*br));
  EXPECT_STREQ("DELETE FROM \"db2\".\"tbl2\" WHERE \"col5\"=1 and ROWNUM=1;", record.redo_stmt_.ptr());
  EXPECT_STREQ("INSERT INTO \"db2\".\"tbl2\" (\"col1\", \"col2\", \"col3\", \"col4\", \"col5\") VALUES ('{\"key\": \"value\"}', SDO_GEOMETRY('POINT(0 1)', NULL), '<a>abc</a>', 'AABB1122', 1);", record.undo_stmt_.ptr());
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
