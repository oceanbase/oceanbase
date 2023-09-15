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

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <time.h>
#include "share/ob_errno.h"
#include "share/restore/ob_import_arg.h"
#include "share/restore/ob_import_util.h"


using namespace oceanbase;
using namespace common;
using namespace share;

class ImportTableTest : public testing::Test
{
public:
  ImportTableTest() {}
  virtual ~ImportTableTest(){}
  virtual void SetUp() {};
  virtual void TearDown() {}
  virtual void TestBody() {}
};

TEST_F(ImportTableTest, test_import_database)
{
  ObImportDatabaseArray import_db_array;
  ObImportDatabaseItem db1(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"));
  ObImportDatabaseItem db2(OB_ORIGIN_AND_SENSITIVE, "db2", strlen("db2"));
  ObImportDatabaseItem db3(OB_ORIGIN_AND_SENSITIVE, "db3", strlen("db3"));

  ASSERT_EQ(OB_SUCCESS, import_db_array.add_item(db1));
  ASSERT_EQ(OB_SUCCESS, import_db_array.add_item(db2));
  ASSERT_EQ(OB_SUCCESS, import_db_array.add_item(db3));

  const char *format_str = "`db1`,`db2`,`db3`";
  char format_buff[1024];
  memset(format_buff, 0, 1024);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, import_db_array.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));

  char hex_format_buff[1024];
  pos = 0;
  memset(hex_format_buff, 0, 1024);
  ASSERT_EQ(OB_SUCCESS, import_db_array.hex_format_serialize(hex_format_buff, 1024, pos));
  ASSERT_EQ(pos, import_db_array.get_hex_format_serialize_size());

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, import_db_array.hex_format_deserialize(hex_format_buff, import_db_array.get_hex_format_serialize_size(), pos));
  memset(format_buff, 0, 1024);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, import_db_array.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));

  pos = 0;
  char buff[1024];
  ASSERT_EQ(OB_SUCCESS, import_db_array.serialize(buff, 1024, pos));
  ASSERT_EQ(import_db_array.get_serialize_size(), pos);

  pos = 0;
  ObImportDatabaseArray import_db_array_bak;
  ASSERT_EQ(OB_SUCCESS, import_db_array_bak.deserialize(buff, 1024, pos));
  memset(format_buff, 0, 1024);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, import_db_array_bak.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));
}

TEST_F(ImportTableTest, test_import_table)
{
  ObImportTableArray import_table_array;
  ObImportTableItem tb1(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"), "tb1", strlen("tb1"));
  ObImportTableItem tb2(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"), "tb2", strlen("tb2"));
  ObImportTableItem tb3(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"), "tb,3", strlen("tb,3"));

  ASSERT_EQ(OB_SUCCESS, import_table_array.add_item(tb1));
  ASSERT_EQ(OB_SUCCESS, import_table_array.add_item(tb2));
  ASSERT_EQ(OB_SUCCESS, import_table_array.add_item(tb3));

  const char *format_str = "`db1`.`tb1`,`db1`.`tb2`,`db1`.`tb,3`";
  char format_buff[1024];
  int64_t pos = 0;
  memset(format_buff, 0, 1024);
  ASSERT_EQ(OB_SUCCESS, import_table_array.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));

  char hex_format_buff[1024];
  pos = 0;
  memset(hex_format_buff, 0, 1024);
  ASSERT_EQ(OB_SUCCESS, import_table_array.hex_format_serialize(hex_format_buff, 1024, pos));
  ASSERT_EQ(pos, import_table_array.get_hex_format_serialize_size());

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, import_table_array.hex_format_deserialize(hex_format_buff, import_table_array.get_hex_format_serialize_size(), pos));
  memset(format_buff, 0, 1024);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, import_table_array.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));

  pos = 0;
  char buff[1024];
  ASSERT_EQ(OB_SUCCESS, import_table_array.serialize(buff, 1024, pos));
  ASSERT_EQ(import_table_array.get_serialize_size(), pos);

  pos = 0;
  ObImportTableArray import_table_array_bak;
  ASSERT_EQ(OB_SUCCESS, import_table_array_bak.deserialize(buff, 1024, pos));
  memset(format_buff, 0, 1024);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, import_table_array_bak.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));
}


TEST_F(ImportTableTest, test_import_partition)
{
  ObImportPartitionArray import_part_array;
  ObImportPartitionItem p1(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"), "tb1", strlen("tb1"), "p1", strlen("p1"));
  ObImportPartitionItem p2(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"), "tb2", strlen("tb2"), "p1", strlen("p1"));
  ObImportPartitionItem p3(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"), "tb,3", strlen("tb,3"), "p1", strlen("p1"));

  ASSERT_EQ(OB_SUCCESS, import_part_array.add_item(p1));
  ASSERT_EQ(OB_SUCCESS, import_part_array.add_item(p2));
  ASSERT_EQ(OB_SUCCESS, import_part_array.add_item(p3));

  const char *format_str = "`db1`.`tb1`:`p1`,`db1`.`tb2`:`p1`,`db1`.`tb,3`:`p1`";
  char format_buff[1024];
  int64_t pos = 0;
  memset(format_buff, 0, 1024);
  ASSERT_EQ(OB_SUCCESS, import_part_array.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));

  char hex_format_buff[1024];
  pos = 0;
  memset(hex_format_buff, 0, 1024);
  ASSERT_EQ(OB_SUCCESS, import_part_array.hex_format_serialize(hex_format_buff, 1024, pos));
  ASSERT_EQ(pos, import_part_array.get_hex_format_serialize_size());

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, import_part_array.hex_format_deserialize(hex_format_buff, import_part_array.get_hex_format_serialize_size(), pos));
  memset(format_buff, 0, 1024);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, import_part_array.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));

  pos = 0;
  char buff[1024];
  ASSERT_EQ(OB_SUCCESS, import_part_array.serialize(buff, 1024, pos));
  ASSERT_EQ(import_part_array.get_serialize_size(), pos);

  pos = 0;
  ObImportPartitionArray import_part_array_bak;
  ASSERT_EQ(OB_SUCCESS, import_part_array_bak.deserialize(buff, 1024, pos));
  memset(format_buff, 0, 1024);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, import_part_array_bak.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));
}


TEST_F(ImportTableTest, test_remap_database)
{
  ObRemapDatabaseArray remap_db_array;
  ObImportDatabaseItem src_db1(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"));
  ObImportDatabaseItem target_db1(OB_ORIGIN_AND_SENSITIVE, "db1_bak", strlen("db1_bak"));
  ObImportDatabaseItem src_db2(OB_ORIGIN_AND_SENSITIVE, "db2", strlen("db2"));
  ObImportDatabaseItem target_db2(OB_ORIGIN_AND_SENSITIVE, "db2_bak", strlen("db2_bak"));
  ObImportDatabaseItem src_db3(OB_ORIGIN_AND_SENSITIVE, "db3", strlen("db3"));
  ObImportDatabaseItem target_db3(OB_ORIGIN_AND_SENSITIVE, "db3_bak", strlen("db3_bak"));

  ObRemapDatabaseItem remap_db1;
  remap_db1.src_ = src_db1;
  remap_db1.target_ = target_db1;

  ObRemapDatabaseItem remap_db2;
  remap_db2.src_ = src_db2;
  remap_db2.target_ = target_db2;

  ObRemapDatabaseItem remap_db3;
  remap_db3.src_ = src_db3;
  remap_db3.target_ = target_db3;

  ASSERT_EQ(OB_SUCCESS, remap_db_array.add_item(remap_db1));
  ASSERT_EQ(OB_SUCCESS, remap_db_array.add_item(remap_db2));
  ASSERT_EQ(OB_SUCCESS, remap_db_array.add_item(remap_db3));

  const char *format_str = "`db1`:`db1_bak`,`db2`:`db2_bak`,`db3`:`db3_bak`";
  char format_buff[1024];
  memset(format_buff, 0, 1024);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, remap_db_array.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));

  char hex_format_buff[1024];
  memset(hex_format_buff, 0, 1024);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, remap_db_array.hex_format_serialize(hex_format_buff, 1024, pos));
  ASSERT_EQ(pos, remap_db_array.get_hex_format_serialize_size());

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, remap_db_array.hex_format_deserialize(hex_format_buff, remap_db_array.get_hex_format_serialize_size(), pos));
  memset(format_buff, 0, 1024);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, remap_db_array.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));

  pos = 0;
  char buff[1024];
  ASSERT_EQ(OB_SUCCESS, remap_db_array.serialize(buff, 1024, pos));

  pos = 0;
  ObRemapDatabaseArray remap_db_array_bak;
  ASSERT_EQ(OB_SUCCESS, remap_db_array_bak.deserialize(buff, 1024, pos));
  pos = 0;
  memset(format_buff, 0, 1024);
  ASSERT_EQ(OB_SUCCESS, remap_db_array_bak.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_buff));
}


TEST_F(ImportTableTest, test_remap_table)
{
  ObRemapTableArray remap_table_array;
  ObImportTableItem src_tb1(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"), "tb1", strlen("tb1"));
  ObImportTableItem target_tb1(OB_ORIGIN_AND_SENSITIVE, "db1_bak", strlen("db1_bak"), "tb1_bak", strlen("tb1_bak"));
  ObImportTableItem src_tb2(OB_ORIGIN_AND_SENSITIVE, "db2", strlen("db2"), "tb1", strlen("tb1"));
  ObImportTableItem target_tb2(OB_ORIGIN_AND_SENSITIVE, "db2_bak", strlen("db2_bak"), "tb1_bak", strlen("tb1_bak"));
  ObImportTableItem src_tb3(OB_ORIGIN_AND_SENSITIVE, "db3", strlen("db3"), "tb1,", strlen("tb1,"));
  ObImportTableItem target_tb3(OB_ORIGIN_AND_SENSITIVE, "db3_bak", strlen("db3_bak"), "tb1,_bak", strlen("tb1,_bak"));

  ObRemapTableItem remap_tb1;
  remap_tb1.src_ = src_tb1;
  remap_tb1.target_ = target_tb1;

  ObRemapTableItem remap_tb2;
  remap_tb2.src_ = src_tb2;
  remap_tb2.target_ = target_tb2;

  ObRemapTableItem remap_tb3;
  remap_tb3.src_ = src_tb3;
  remap_tb3.target_ = target_tb3;

  ASSERT_EQ(OB_SUCCESS, remap_table_array.add_item(remap_tb1));
  ASSERT_EQ(OB_SUCCESS, remap_table_array.add_item(remap_tb2));
  ASSERT_EQ(OB_SUCCESS, remap_table_array.add_item(remap_tb3));

  const char *format_str = "`db1`.`tb1`:`db1_bak`.`tb1_bak`,`db2`.`tb1`:`db2_bak`.`tb1_bak`,`db3`.`tb1,`:`db3_bak`.`tb1,_bak`";
  char format_buff[1024];
  memset(format_buff, 0, 1024);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, remap_table_array.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));

  char hex_format_buff[1024];
  memset(hex_format_buff, 0, 1024);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, remap_table_array.hex_format_serialize(hex_format_buff, 1024, pos));
  ASSERT_EQ(pos, remap_table_array.get_hex_format_serialize_size());

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, remap_table_array.hex_format_deserialize(hex_format_buff, remap_table_array.get_hex_format_serialize_size(), pos));
  memset(format_buff, 0, 1024);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, remap_table_array.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));

  pos = 0;
  char buff[1024];
  ASSERT_EQ(OB_SUCCESS, remap_table_array.serialize(buff, 1024, pos));

  pos = 0;
  ObRemapTableArray remap_table_array_bak;
  ASSERT_EQ(OB_SUCCESS, remap_table_array_bak.deserialize(buff, 1024, pos));
  pos = 0;
  memset(format_buff, 0, 1024);
  ASSERT_EQ(OB_SUCCESS, remap_table_array_bak.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_buff));
}



TEST_F(ImportTableTest, test_remap_partition)
{
  ObRemapPartitionArray remap_part_array;
  ObImportPartitionItem src_part1(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"), "tb1", strlen("tb1"), "p1", strlen("p1"));
  ObImportTableItem target_tb1(OB_ORIGIN_AND_SENSITIVE, "db1_bak", strlen("db1_bak"), "tb1_p1", strlen("tb1_p1"));
  ObImportPartitionItem src_part2(OB_ORIGIN_AND_SENSITIVE, "db2", strlen("db2"), "tb1", strlen("tb1"), "p1", strlen("p1"));
  ObImportTableItem target_tb2(OB_ORIGIN_AND_SENSITIVE, "db2_bak", strlen("db2_bak"), "tb1_p1", strlen("tb1_p1"));
  ObImportPartitionItem src_part3(OB_ORIGIN_AND_SENSITIVE, "db3", strlen("db3"), "tb1,", strlen("tb1,"), "p1", strlen("p1"));
  ObImportTableItem target_tb3(OB_ORIGIN_AND_SENSITIVE, "db3_bak", strlen("db3_bak"), "tb1,_p1", strlen("tb1,_p1"));

  ObRemapPartitionItem remap_p1;
  remap_p1.src_ = src_part1;
  remap_p1.target_ = target_tb1;

  ObRemapPartitionItem remap_p2;
  remap_p2.src_ = src_part2;
  remap_p2.target_ = target_tb2;

  ObRemapPartitionItem remap_p3;
  remap_p3.src_ = src_part3;
  remap_p3.target_ = target_tb3;

  ASSERT_EQ(OB_SUCCESS, remap_part_array.add_item(remap_p1));
  ASSERT_EQ(OB_SUCCESS, remap_part_array.add_item(remap_p2));
  ASSERT_EQ(OB_SUCCESS, remap_part_array.add_item(remap_p3));

  const char *format_str = "`db1`.`tb1`:`p1`:`db1_bak`.`tb1_p1`,`db2`.`tb1`:`p1`:`db2_bak`.`tb1_p1`,`db3`.`tb1,`:`p1`:`db3_bak`.`tb1,_p1`";
  char format_buff[1024];
  memset(format_buff, 0, 1024);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, remap_part_array.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));

  char hex_format_buff[1024];
  memset(hex_format_buff, 0, 1024);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, remap_part_array.hex_format_serialize(hex_format_buff, 1024, pos));
  ASSERT_EQ(pos, remap_part_array.get_hex_format_serialize_size());

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, remap_part_array.hex_format_deserialize(hex_format_buff, remap_part_array.get_hex_format_serialize_size(), pos));
  memset(format_buff, 0, 1024);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, remap_part_array.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_str));

  pos = 0;
  char buff[1024];
  ASSERT_EQ(OB_SUCCESS, remap_part_array.serialize(buff, 1024, pos));

  pos = 0;
  ObRemapPartitionArray remap_part_array_bak;
  ASSERT_EQ(OB_SUCCESS, remap_part_array_bak.deserialize(buff, 1024, pos));
  pos = 0;
  memset(format_buff, 0, 1024);
  ASSERT_EQ(OB_SUCCESS, remap_part_array_bak.format_serialize(format_buff, 1024, pos));
  ASSERT_EQ(0, strcmp(format_buff, format_buff));
}


TEST_F(ImportTableTest, test_import_conflict)
{
  ObImportArg arg;
  ObImportDatabaseItem db1(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"));
  ObImportDatabaseItem db2(OB_ORIGIN_AND_SENSITIVE, "db2", strlen("db2"));
  ObImportDatabaseItem db3(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"));

  ASSERT_EQ(OB_SUCCESS, arg.add_import_database(db1));
  ASSERT_EQ(OB_SUCCESS, arg.add_import_database(db2));
  ASSERT_EQ(OB_BACKUP_CONFLICT_VALUE, arg.add_import_database(db3));

  ObImportTableItem tb1(OB_ORIGIN_AND_SENSITIVE, "db3", strlen("db3"), "tb1", strlen("tb1"));
  ObImportTableItem tb2(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"), "tb2", strlen("tb2"));
  ASSERT_EQ(OB_SUCCESS, arg.add_import_table(tb1));
  ASSERT_EQ(OB_BACKUP_CONFLICT_VALUE, arg.add_import_table(tb2));
}

TEST_F(ImportTableTest, test_remap_conflict)
{
  ObImportArg arg;
  ObImportDatabaseItem src_db1(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"));
  ObImportDatabaseItem target_db1(OB_ORIGIN_AND_SENSITIVE, "db1_bak", strlen("db1_bak"));
  ObImportDatabaseItem src_db2(OB_ORIGIN_AND_SENSITIVE, "db2", strlen("db2"));
  ObImportDatabaseItem target_db2(OB_ORIGIN_AND_SENSITIVE, "db2_bak", strlen("db2_bak"));
  ObImportDatabaseItem src_db3(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"));
  ObImportDatabaseItem target_db3(OB_ORIGIN_AND_SENSITIVE, "db3_bak", strlen("db3_bak"));
  ObImportDatabaseItem src_db4(OB_ORIGIN_AND_SENSITIVE, "db4", strlen("db4"));
  ObImportDatabaseItem target_db4(OB_ORIGIN_AND_SENSITIVE, "db2_bak", strlen("db2_bak"));

  ObRemapDatabaseItem remap_db1;
  remap_db1.src_ = src_db1;
  remap_db1.target_ = target_db1;

  ObRemapDatabaseItem remap_db2;
  remap_db2.src_ = src_db2;
  remap_db2.target_ = target_db2;

  ObRemapDatabaseItem remap_db3;
  remap_db3.src_ = src_db3;
  remap_db3.target_ = target_db3;

  ObRemapDatabaseItem remap_db4;
  remap_db4.src_ = src_db4;
  remap_db4.target_ = target_db4;

  ASSERT_EQ(OB_SUCCESS, arg.add_remap_database(remap_db1));
  ASSERT_EQ(OB_SUCCESS, arg.add_remap_database(remap_db2));
  ASSERT_EQ(OB_BACKUP_CONFLICT_VALUE, arg.add_remap_database(remap_db3));
  ASSERT_EQ(OB_BACKUP_CONFLICT_VALUE, arg.add_remap_database(remap_db4));


  ObImportTableItem src_tb1(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"), "tb1", strlen("tb1"));
  ObImportTableItem target_tb1(OB_ORIGIN_AND_SENSITIVE, "db1_bak", strlen("db1_bak"), "tb1_bak", strlen("tb1_bak"));
  ObImportTableItem src_tb2(OB_ORIGIN_AND_SENSITIVE, "db1", strlen("db1"), "tb1", strlen("tb1"));
  ObImportTableItem target_tb2(OB_ORIGIN_AND_SENSITIVE, "db2_bak", strlen("db2_bak"), "tb1_bak", strlen("tb1_bak"));
  ObImportTableItem src_tb3(OB_ORIGIN_AND_SENSITIVE, "db3", strlen("db3"), "tb1", strlen("tb1"));
  ObImportTableItem target_tb3(OB_ORIGIN_AND_SENSITIVE, "db1_bak", strlen("db1_bak"), "tb1_bak", strlen("tb1_bak"));

  ObRemapTableItem remap_tb1;
  remap_tb1.src_ = src_tb1;
  remap_tb1.target_ = target_tb1;

  ObRemapTableItem remap_tb2;
  remap_tb2.src_ = src_tb2;
  remap_tb2.target_ = target_tb2;

  ObRemapTableItem remap_tb3;
  remap_tb3.src_ = src_tb3;
  remap_tb3.target_ = target_tb3;

  ASSERT_EQ(OB_SUCCESS, arg.add_remap_table(remap_tb1));
  ASSERT_EQ(OB_BACKUP_CONFLICT_VALUE, arg.add_remap_table(remap_tb2));
  ASSERT_EQ(OB_BACKUP_CONFLICT_VALUE, arg.add_remap_table(remap_tb3));
}

TEST_F(ImportTableTest, test_namecase_mode)
{
  ObImportArg arg;
  ObImportDatabaseItem db1(OB_LOWERCASE_AND_INSENSITIVE, "db1", strlen("db1"));
  ObImportDatabaseItem db2(OB_LOWERCASE_AND_INSENSITIVE, "db2", strlen("db2"));
  ObImportDatabaseItem db3(OB_LOWERCASE_AND_INSENSITIVE, "DB1", strlen("DB1"));

  ASSERT_EQ(OB_SUCCESS, arg.add_import_database(db1));
  ASSERT_EQ(OB_SUCCESS, arg.add_import_database(db2));
  ASSERT_EQ(OB_BACKUP_CONFLICT_VALUE, arg.add_import_database(db3));

  ObImportTableItem tb1(OB_LOWERCASE_AND_INSENSITIVE, "Db1", strlen("Db1"), "tb1", strlen("tb1"));
  ASSERT_EQ(OB_BACKUP_CONFLICT_VALUE, arg.add_import_table(tb1));


  ObImportDatabaseItem src_db1(OB_LOWERCASE_AND_INSENSITIVE, "db1", strlen("db1"));
  ObImportDatabaseItem target_db1(OB_LOWERCASE_AND_INSENSITIVE, "db1_bak", strlen("db1_bak"));
  ObImportDatabaseItem src_db2(OB_LOWERCASE_AND_INSENSITIVE, "db1", strlen("db1"));
  ObImportDatabaseItem target_db2(OB_LOWERCASE_AND_INSENSITIVE, "db1_Bak", strlen("db1_Bak"));

  ObRemapDatabaseItem remap_db1;
  remap_db1.src_ = src_db1;
  remap_db1.target_ = target_db1;

  ObRemapDatabaseItem remap_db2;
  remap_db2.src_ = src_db2;
  remap_db2.target_ = target_db2;

  ASSERT_EQ(OB_SUCCESS, arg.add_remap_database(remap_db1));
  ASSERT_EQ(OB_BACKUP_CONFLICT_VALUE, arg.add_remap_database(remap_db2));
}

TEST_F(ImportTableTest, test_check_aux_tenant)
{
  const ObString tenant_name_1("AUX_RECOVER$1694673215667468");
  bool is_recover_table_aux_tenant = false;
  ASSERT_EQ(OB_SUCCESS, ObImportTableUtil::check_is_recover_table_aux_tenant_name(tenant_name_1, is_recover_table_aux_tenant));
  ASSERT_EQ(true, is_recover_table_aux_tenant);
  const ObString tenant_name_2("AUX_RECOVER$1694673215667468aaa");
  ASSERT_EQ(OB_SUCCESS, ObImportTableUtil::check_is_recover_table_aux_tenant_name(tenant_name_2, is_recover_table_aux_tenant));
  ASSERT_EQ(false, is_recover_table_aux_tenant);
  const ObString tenant_name_3("AUX_RECOVER1694673215667468aaa");
  ASSERT_EQ(OB_SUCCESS, ObImportTableUtil::check_is_recover_table_aux_tenant_name(tenant_name_3, is_recover_table_aux_tenant));
  ASSERT_EQ(false, is_recover_table_aux_tenant);
  const ObString tenant_name_4("AUX_RECOVER$aaa");
  ASSERT_EQ(OB_SUCCESS, ObImportTableUtil::check_is_recover_table_aux_tenant_name(tenant_name_4, is_recover_table_aux_tenant));
  ASSERT_EQ(false, is_recover_table_aux_tenant);
}


int main(int argc, char **argv)
{
  system("rm -f test_import_table.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_import_table.log", true);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}