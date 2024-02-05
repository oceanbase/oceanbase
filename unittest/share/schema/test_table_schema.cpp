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

#include <gtest/gtest.h>
#define private public
#include "lib/allocator/ob_tc_malloc.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "ob_schema_test_utils.cpp"
namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{
static const int64_t BUF_SIZE = 1024*10;

//-------test for funcs-------//
TEST(ObTableSchemaTest, convert_column_type_to_str_test)
{
  ASSERT_EQ(0, memcmp(STR_COLUMN_TYPE_INT,
                      ObColumnSchemaV2::convert_column_type_to_str(ObIntType),
                      strlen(STR_COLUMN_TYPE_INT)));
  ASSERT_EQ(0, memcmp(STR_COLUMN_TYPE_VCHAR,
                      ObColumnSchemaV2::convert_column_type_to_str(ObVarcharType),
                      strlen(STR_COLUMN_TYPE_VCHAR)));
  ASSERT_EQ(0, memcmp(STR_COLUMN_TYPE_DATETIME,
                      ObColumnSchemaV2::convert_column_type_to_str(ObDateTimeType),
                      strlen(STR_COLUMN_TYPE_DATETIME)));
  ASSERT_EQ(0, memcmp(STR_COLUMN_TYPE_NUMBER,
                      ObColumnSchemaV2::convert_column_type_to_str(ObNumberType),
                      strlen(STR_COLUMN_TYPE_NUMBER)));
  ASSERT_EQ(0, memcmp(STR_COLUMN_TYPE_UNKNOWN,
                      ObColumnSchemaV2::convert_column_type_to_str(ObUnknownType),
                      strlen(STR_COLUMN_TYPE_UNKNOWN)));
}
TEST(ObTableSchemaTest, convert_str_to_column_type_test)
{
  ASSERT_EQ(ObIntType, ObColumnSchemaV2::convert_str_to_column_type(STR_COLUMN_TYPE_INT));
  ASSERT_EQ(ObVarcharType, ObColumnSchemaV2::convert_str_to_column_type(STR_COLUMN_TYPE_VCHAR));
  ASSERT_EQ(ObDateTimeType, ObColumnSchemaV2::convert_str_to_column_type(STR_COLUMN_TYPE_DATETIME));
  ASSERT_EQ(ObNumberType, ObColumnSchemaV2::convert_str_to_column_type(STR_COLUMN_TYPE_NUMBER));
  ASSERT_EQ(ObUnknownType, ObColumnSchemaV2::convert_str_to_column_type(STR_COLUMN_TYPE_UNKNOWN));
}

void fill_column_schema(ObColumnSchemaV2 &column, uint64_t id, const char *name,
                        uint64_t rowkey_pos = 1, uint64_t index_key_pos = 1,
                        uint64_t part_key_pos = 1, ObOrderType rowkey_order = ObOrderType::ASC)
{
  ObObj value;
  column.set_column_id(id);
  column.set_column_name(ObString::make_string(name));
  column.set_rowkey_position(rowkey_pos);
  column.set_order_in_rowkey(rowkey_order);
  column.set_tbl_part_key_pos(part_key_pos);
  column.set_index_position(index_key_pos);
  column.set_data_length(100);
  column.set_data_precision(1100);
  column.set_data_scale(88);
  column.set_nullable(false);
  column.set_charset_type(CHARSET_UTF8MB4);
  column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  column.set_data_type(ObIntType);
  value.set_int(100);
  column.set_orig_default_value(value);
  value.set_int(101);
  column.set_cur_default_value(value);
  column.set_comment("black gives me black eyes");
}

void fill_table_schema(ObTableSchema &table)
{
  ObArray<ObString> zone;
  zone.push_back(ObString::make_string("zone1"));
  zone.push_back(ObString::make_string("zone2"));
  table.set_tenant_id(3001);
  table.set_database_id(1);
  table.set_tablegroup_id(1);
  table.set_table_id(3001);
  table.set_max_used_column_id(16);
  table.set_rowkey_column_num(0);
  table.set_index_column_num(0);
  table.set_rowkey_split_pos(11);
  table.set_progressive_merge_num(11);
  table.set_compress_func_name(ObString::make_string("snappy_1.0"));
  table.set_autoinc_column_id(0);
  table.set_auto_increment(1);
  table.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table.set_def_type(TABLE_DEF_TYPE_USER);
  table.set_part_level(PARTITION_LEVEL_TWO);
  table.set_charset_type(CHARSET_UTF8MB4);
  table.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  table.set_create_mem_version(8888);
  table.set_last_modified_frozen_version(111);
  table.set_table_type(USER_TABLE);
  table.set_index_type(INDEX_TYPE_IS_NOT);
  table.set_index_status(INDEX_STATUS_AVAILABLE);
  table.set_data_table_id(0);
  table.set_is_use_bloomfilter(false);
  table.set_block_size(2097152);
  table.set_tablegroup_name("table group name 1");
  table.set_comment("This is a table");
  table.set_table_name("table_xxx");
  table.set_expire_info("expire: modify_time > 3000s");
  table.set_zone_list(zone);
  table.set_primary_zone(ObString::make_string("zone1"));
  table.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  table.get_part_option().set_part_expr (ObString::make_string("rand() mod 111"));
  table.get_part_option().set_part_num(100);
  table.get_sub_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  table.get_sub_part_option().set_part_expr (ObString::make_string("rand() mod 111"));
  table.get_sub_part_option().set_part_num(666);
}

void test_ob_table_schema_get_methods(void)
{
  int ret = OB_SUCCESS;
  ObTableSchema table;
  ObColumnSchemaV2 column;
  ASSERT_FALSE(table.is_valid());
  fill_table_schema(table);
  ASSERT_FALSE(table.is_valid());
  fill_column_schema(column, 1, "c1");
  ret = table.add_column(column);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3001, table.get_tenant_id());
  ASSERT_EQ(1, table.get_database_id());
  ASSERT_EQ(1, table.get_tablegroup_id());
  ASSERT_EQ(3001, table.get_table_id());
  ASSERT_EQ(0, table.get_index_tid_count());
  ASSERT_EQ(1, table.get_index_column_number());
  ASSERT_EQ(16, table.get_max_used_column_id());
  ASSERT_EQ(11, table.get_rowkey_split_pos());
  ASSERT_EQ(2097152, table.get_block_size());
  ASSERT_FALSE(table.is_use_bloomfilter());
  ASSERT_EQ(11, table.get_progressive_merge_num());
  ASSERT_EQ(0, table.get_autoinc_column_id());
  ASSERT_EQ(1, table.get_auto_increment());
  ASSERT_EQ(1, table.get_rowkey_column_num());
  ASSERT_EQ(1, table.get_index_column_num());
  ASSERT_TRUE(table.is_partitioned_table());
  ASSERT_EQ(1, table.get_partition_key_column_num());
  ASSERT_EQ(TABLE_LOAD_TYPE_IN_DISK, table.get_load_type());
  ASSERT_EQ(USER_TABLE, table.get_table_type());
  ASSERT_EQ(INDEX_TYPE_IS_NOT, table.get_index_type());
  ASSERT_EQ(TABLE_DEF_TYPE_USER, table.get_def_type());
  ASSERT_EQ(PARTITION_LEVEL_TWO, table.get_part_level());
  ASSERT_EQ(100 * 666, table.get_all_part_num());
  ASSERT_STREQ("table_xxx", table.get_table_name());
  ASSERT_STREQ("table_xxx", table.get_table_name_str().ptr());
  ASSERT_STREQ("snappy_1.0", table.get_compress_func_name());
  ASSERT_TRUE(table.is_compressed());
  ASSERT_EQ(CHARSET_UTF8MB4, table.get_charset_type());
  ASSERT_EQ(CS_TYPE_UTF8MB4_BIN, table.get_collation_type());
  ASSERT_EQ(0, table.get_data_table_id());
  ASSERT_EQ(8888, table.get_create_mem_version());
  ASSERT_EQ(INDEX_STATUS_AVAILABLE, table.get_index_status());
  ASSERT_EQ(111, table.get_last_modified_frozen_version());
  ASSERT_STREQ("expire: modify_time > 3000s", table.get_expire_info().ptr());
  ASSERT_STREQ("rand() mod 111", table.get_part_option().get_part_func_expr_str().ptr());
  ASSERT_STREQ("rand() mod 111", table.get_sub_part_option().get_part_func_expr_str().ptr());
  ASSERT_STREQ("zone1", table.get_primary_zone().ptr());
  ASSERT_TRUE(table.is_user_table());
  ASSERT_FALSE(table.is_sys_table());
  ASSERT_FALSE(table.is_view_table());
  ASSERT_FALSE(table.is_tmp_table());
  ASSERT_FALSE(table.is_index_table());
  //ASSERT_FALSE(table.is_local_index_table());
  ASSERT_FALSE(table.is_normal_index());
  ASSERT_FALSE(table.is_unique_index());
  ASSERT_TRUE(table.can_read_index());
  ASSERT_TRUE(table.has_partition());
}

void test_ob_table_column_operations(void)
{
  int ret = OB_SUCCESS;
  ObTableSchema table;
  ObColumnSchemaV2 columns[6];
  ASSERT_EQ(0, table.get_column_count());
  fill_column_schema(columns[0], 1, "c1", 1, 1, 1);
  fill_column_schema(columns[1], 2, "c2", 2, 2, 2);
  fill_column_schema(columns[2], 3, "c3", 0, 0, 0);
  fill_column_schema(columns[3], 4, "c4", 0, 0, 0);
  fill_column_schema(columns[4], 5, "c5", 0, 3, 0);
  fill_column_schema(columns[5], 6, "c6", 0, 0, 3);
  ret = table.add_column(columns[0]);
  table.set_max_used_column_id(columns[0].get_column_id());
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, table.get_column_count());
  ASSERT_EQ(1, table.get_index_column_num());
  ASSERT_EQ(1, table.get_rowkey_column_num());
  ASSERT_EQ(1, table.get_partition_key_column_num());
  ret = table.add_column(columns[1]);
  table.set_max_used_column_id(columns[1].get_column_id());
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, table.get_column_count());
  ASSERT_EQ(2, table.get_index_column_num());
  ASSERT_EQ(2, table.get_rowkey_column_num());
  ASSERT_EQ(2, table.get_partition_key_column_num());
  ret = table.add_column(columns[2]);
  table.set_max_used_column_id(columns[2].get_column_id());
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, table.get_column_count());
  ASSERT_EQ(2, table.get_index_column_num());
  ASSERT_EQ(2, table.get_rowkey_column_num());
  ASSERT_EQ(2, table.get_partition_key_column_num());
  ret = table.add_column(columns[3]);
  table.set_max_used_column_id(columns[3].get_column_id());
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, table.get_column_count());
  ASSERT_EQ(2, table.get_index_column_num());
  ASSERT_EQ(2, table.get_rowkey_column_num());
  ASSERT_EQ(2, table.get_partition_key_column_num());
  ret = table.add_column(columns[4]);
  table.set_max_used_column_id(columns[4].get_column_id());
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5, table.get_column_count());
  ASSERT_EQ(3, table.get_index_column_num());
  ASSERT_EQ(2, table.get_rowkey_column_num());
  ASSERT_EQ(2, table.get_partition_key_column_num());
  ret = table.add_column(columns[5]);
  table.set_max_used_column_id(columns[5].get_column_id());
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(6, table.get_column_count());
  ASSERT_EQ(3, table.get_index_column_num());
  ASSERT_EQ(2, table.get_rowkey_column_num());
  ASSERT_EQ(3, table.get_partition_key_column_num());

  //test delete column
  ret = table.delete_column(columns[0].get_column_name_str());
  ASSERT_NE(OB_SUCCESS, ret);
  ASSERT_EQ(6, table.get_column_count());
  ASSERT_EQ(3, table.get_index_column_num());
  ASSERT_EQ(2, table.get_rowkey_column_num());
  ASSERT_EQ(3, table.get_partition_key_column_num());

  ret = table.delete_column(columns[1].get_column_name_str());
  ASSERT_NE(OB_SUCCESS, ret);
  ASSERT_EQ(6, table.get_column_count());
  ASSERT_EQ(3, table.get_index_column_num());
  ASSERT_EQ(2, table.get_rowkey_column_num());
  ASSERT_EQ(3, table.get_partition_key_column_num());

  table.set_table_type(USER_VIEW);
  ret = table.delete_column(columns[2].get_column_name_str());
  ASSERT_NE(OB_SUCCESS, ret);
  table.set_table_type(USER_TABLE);
  ret = table.delete_column(columns[2].get_column_name_str());
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5, table.get_column_count());
  ASSERT_EQ(3, table.get_index_column_num());
  ASSERT_EQ(2, table.get_rowkey_column_num());
  ASSERT_EQ(3, table.get_partition_key_column_num());

  ret = table.delete_column(columns[3].get_column_name_str());
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, table.get_column_count());
  ASSERT_EQ(3, table.get_index_column_num());
  ASSERT_EQ(2, table.get_rowkey_column_num());
  ASSERT_EQ(3, table.get_partition_key_column_num());

  ret = table.delete_column("column_not_exist");
  ASSERT_NE(OB_SUCCESS, ret);
  ret = table.delete_column("");
  ASSERT_NE(OB_SUCCESS, ret);
  ret = table.delete_column(ObString::make_string("c100"));
  ASSERT_NE(OB_SUCCESS, ret);
  ret = table.delete_column(columns[4].get_column_name_str());
  ASSERT_NE(OB_SUCCESS, ret);
  ret = table.delete_column(columns[5].get_column_name_str());
  ASSERT_NE(OB_SUCCESS, ret);

  ObColumnSchemaV2 *table_column1 = table.get_column_schema(columns[0].get_column_id());
  ASSERT_NE(static_cast<ObColumnSchemaV2 *>(NULL), table_column1);
  ObSchemaTestUtils::expect_column_eq(&columns[0], table_column1);
  ObColumnSchemaV2 *table_column2 = table.get_column_schema(columns[1].get_column_id());
  ASSERT_NE(static_cast<ObColumnSchemaV2 *>(NULL), table_column2);
  ObSchemaTestUtils::expect_column_eq(&columns[1], table_column2);

  //test alter column
  ret = table.alter_column(columns[0]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table.alter_column(columns[4]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table.alter_column(columns[5]);
  ASSERT_EQ(OB_SUCCESS, ret);

  columns[2].set_column_id(7);
  ret = table.add_column(columns[2]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5, table.get_column_count());
  table.set_max_used_column_id(7);
  columns[3].set_column_id(8);
  ret = table.add_column(columns[3]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(6, table.get_column_count());
  table.set_max_used_column_id(8);

  columns[2].set_column_id(9);
  ret = table.alter_column(columns[2]);
  ASSERT_NE(OB_SUCCESS, ret);

  columns[2].set_column_id(7);
  columns[2].set_data_type(ObVarcharType);
  ret = table.alter_column(columns[2]);
  ASSERT_NE(OB_SUCCESS, ret);
  columns[2].set_data_type(ObIntType);
  ObColumnSchemaV2 *table_column3 = table.get_column_schema(columns[2].get_column_id());
  ASSERT_NE(static_cast<ObColumnSchemaV2 *>(NULL), table_column3);
  ObSchemaTestUtils::expect_column_eq(&columns[2], table_column3);

  fill_column_schema(columns[4], 9, "c9", 0, 1, 0);
  ret = table.add_column(columns[4]);
  ASSERT_EQ(OB_SUCCESS, ret);
  columns[4].set_data_type(ObVarcharType);
  ret = table.alter_column(columns[4]);
  ASSERT_NE(OB_SUCCESS, ret);
  columns[4].set_data_type(ObIntType);
  ObColumnSchemaV2 *table_column4 = table.get_column_schema(columns[4].get_column_id());
  ASSERT_NE(static_cast<ObColumnSchemaV2 *>(NULL), table_column4);
  ObSchemaTestUtils::expect_column_eq(&columns[4], table_column4);

  fill_column_schema(columns[5], 10, "c10", 0, 1, 0);
  columns[5].set_data_type(ObVarcharType);
  ret = table.add_column(columns[5]);
  ASSERT_EQ(OB_SUCCESS, ret);
  columns[5].set_data_length(columns[5].get_data_length() - 10);
  ret = table.alter_column(columns[5]);
  ASSERT_NE(OB_SUCCESS, ret);
  columns[5].set_data_length(columns[5].get_data_length() + 20);
  ret = table.alter_column(columns[5]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObColumnSchemaV2 *table_column5 = table.get_column_schema(columns[5].get_column_id());
  ASSERT_NE(static_cast<ObColumnSchemaV2 *>(NULL), table_column5);
  ObSchemaTestUtils::expect_column_eq(&columns[5], table_column5);

 // sys_table.init_as_sys_table();
  //uint64_t table_id_ = common::OB_INVALID_ID;
  //ObTableType table_type_ = SYSTEM_TABLE;
  //ObIndexType index_type_ = INDEX_TYPE_IS_NOT;
  //ObIndexUsingType index_using_type_ = USING_BTREE;
  //ObTableLoadType load_type_ = TABLE_LOAD_TYPE_IN_DISK;
  //ObTableDefType def_type_ = TABLE_DEF_TYPE_INTERNAL;
  //int64_t replica_num_ = common::OB_SAFE_COPY_COUNT;
  //int64_t create_mem_version_ = 1;
  //int64_t block_size_ = common::OB_DEFAULT_SSTABLE_BLOCK_SIZE;
  //common::ObCharsetType charset_type_ = ObCharset::get_default_charset();
  //common::ObCollationType collation_type_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
  //bool is_use_bloomfilter_ = false;
  //int64_t rowkey_split_pos_ = 0;
  //bool index_on_create_table_ = false;
  //int64_t progressive_merge_num_ = 0;
  //uint64_t autoinc_column_id_ = 0;
  //uint64_t auto_increment_ = 1;
  //deep_copy_str(common::OB_DEFAULT_COMPRESS_FUNC_NAME, compress_func_name_);

  //ASSERT_FALSE(sys_table.is_valid());
  //ret = sys_table.add_column(columns[0]);
  //ASSERT_EQ(OB_SUCCESS, ret);
  //columns[0].set_column_name("c9");
  //ret = sys_table.alter_column(columns[0]);
  //ASSERT_EQ(OB_SUCCESS, ret);

  ObTableSchema view_table;
  view_table.set_table_type(USER_VIEW);
  ret = view_table.add_column(columns[0]);
  ASSERT_EQ(OB_SUCCESS, ret);
  columns[0].set_column_name("c10");
  ret = view_table.alter_column(columns[0]);
  ASSERT_NE(OB_SUCCESS, ret);

  ObTableSchema tmp_table;
  tmp_table.set_table_type(TMP_TABLE);
  ret = tmp_table.add_column(columns[0]);
  ASSERT_EQ(OB_SUCCESS, ret);
  columns[0].set_column_name("c11");
  ret = tmp_table.alter_column(columns[0]);
  ASSERT_NE(OB_SUCCESS, ret);

  //test only one autoincrement constraint
  ObTableSchema auto_table;
  fill_table_schema(auto_table);
  fill_column_schema(columns[0], 5, "c1", 0, 0, 0);
  columns[0].set_autoincrement(true);
  fill_column_schema(columns[1], 6, "c2", 1, 1, 1);
  fill_column_schema(columns[2], 7, "c3", 0, 0, 0);
  columns[2].set_autoincrement(true);
  ret = auto_table.add_column(columns[0]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = auto_table.add_column(columns[1]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = auto_table.add_column(columns[2]);
  ASSERT_NE(OB_SUCCESS, ret);
  columns[0].set_autoincrement(false);
  ret = auto_table.alter_column(columns[0]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = auto_table.add_column(columns[2]);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void test_ob_rowkey_max_length(void)
{
  int ret = OB_SUCCESS;
  ObTableSchema table;
  ObColumnSchemaV2 columns[2];
  fill_table_schema(table);
  fill_column_schema(columns[0], 1, "c1", 1, 1, 1);
  fill_column_schema(columns[1], 2, "c2", 2, 2, 2);
  columns[0].set_data_type(ObVarcharType);
  columns[1].set_data_type(ObVarcharType);
  ret = table.add_column(columns[0]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table.add_column(columns[1]);
  ASSERT_EQ(OB_SUCCESS, ret);
  columns[1].set_data_length(OB_MAX_USER_ROW_KEY_LENGTH);
  ret = table.alter_column(columns[1]);
  ASSERT_NE(OB_SUCCESS, ret);
}

void test_ob_table_schema_is_valid(void)
{
  int ret = OB_SUCCESS;
  ObTableSchema table;
  ObColumnSchemaV2 column;
  ASSERT_FALSE(table.is_valid());
  fill_table_schema(table);
  fill_column_schema(column, 3, "c1");
  ret = table.add_column(column);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(table.is_valid());
  table.set_table_type(USER_INDEX);
  ASSERT_FALSE(table.is_valid());
  table.set_index_type(INDEX_TYPE_UNIQUE_LOCAL);
  ASSERT_TRUE(table.is_valid());
  table.set_index_type(INDEX_TYPE_IS_NOT);
  ASSERT_FALSE(table.is_valid());
  table.set_table_type(USER_VIEW);
  ASSERT_TRUE(table.is_valid());
  table.set_table_type(USER_TABLE);
  table.set_table_id(OB_MAX_SYS_VIEW_ID + 1);
  ASSERT_TRUE(table.is_valid());

  column.set_column_id(17);
  column.set_column_name("c2");
  ret = table.add_column(column);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(table.is_valid());
  table.set_max_used_column_id(17);
  ASSERT_FALSE(table.is_valid());

  ObTableSchema table2;
  fill_column_schema(column, 3, "c1");
  ret = table2.add_column(column);
  ASSERT_EQ(OB_SUCCESS, ret);
  fill_column_schema(column, 4, "c2", 2, 2, 2);
  ret = table2.add_column(column);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void test_ob_table_schema_get_column(void)
{
  int ret = OB_SUCCESS;
  ObTableSchema table;
  ObColumnSchemaV2 columns[2];
  ObColumnSchemaV2 *column1 = NULL;
  ObArray<ObColDesc> column_ids;
  fill_table_schema(table);
  fill_column_schema(columns[0], 1, "c1", 1, 1, 1, ObOrderType::ASC);
  fill_column_schema(columns[1], 2, "c2", 0, 0, 0, ObOrderType::DESC);
  ret = table.add_column(columns[0]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table.add_column(columns[1]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table.fill_column_collation_info();
  ASSERT_EQ(OB_SUCCESS, ret);
  column1 = table.get_column_schema(columns[0].get_column_name());
  ASSERT_NE(static_cast<ObColumnSchemaV2 *>(NULL), column1);
  ObSchemaTestUtils::expect_column_eq(&columns[0], column1);
  const ObColumnSchemaV2 *column2 = table.get_column_schema(columns[1].get_column_name());
  ASSERT_NE(static_cast<ObColumnSchemaV2 *>(NULL), column2);
  ObSchemaTestUtils::expect_column_eq(&columns[1], column2);
  column1 = table.get_column_schema_by_idx(22);
  ASSERT_EQ(static_cast<ObColumnSchemaV2 *>(NULL), column1);
  column1 = table.get_column_schema_by_idx(0);
  ASSERT_NE(static_cast<ObColumnSchemaV2 *>(NULL), column1);
  ObSchemaTestUtils::expect_column_eq(column1, &columns[0]);
  column2 = table.get_column_schema_by_idx(1);
  ASSERT_NE(static_cast<ObColumnSchemaV2 *>(NULL), column2);
  ObSchemaTestUtils::expect_column_eq(column2, &columns[1]);
  ret = table.get_column_ids(column_ids);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, column_ids.at(0).col_id_);
  ASSERT_EQ(2, column_ids.at(1).col_id_);
  ASSERT_EQ(ObOrderType::ASC, column_ids.at(0).col_order_);
  ASSERT_EQ(ObOrderType::DESC, column_ids.at(1).col_order_);
}

#define FILL_COLUMN_SCHEMA(idx, column, base_column_id, is_hidden) \
    do { \
      char column_name[128] = {0}; \
      snprintf(column_name, sizeof(column_name), "c%ld", idx); \
      uint64_t column_id = idx + base_column_id; \
      uint64_t rowkey_pos = idx > 63 ? 0 : idx + 1; \
      uint64_t index_key_pos = idx > 63 ? 0 : idx + 1; \
      uint64_t part_key_pos = idx > 63 ? 0 : idx + 1; \
      column.set_is_hidden(is_hidden); \
      fill_column_schema(column, column_id, column_name, rowkey_pos, index_key_pos, part_key_pos); \
    } while (0)

void test_ob_table_schema_get_column_idx(void)
{
  static const int64_t MAX_COLUMN_COUNT = OB_USER_ROW_MAX_COLUMNS_COUNT;
  static const uint64_t base_column_id = 16;
  static const int64_t hidden_column_mod = 3;

  ObTableSchema table;
  ObColumnSchemaV2 column_array[MAX_COLUMN_COUNT];
  fill_table_schema(table);

  // set Column
  for (int64_t idx = 0; idx < MAX_COLUMN_COUNT; idx++) {
    ObColumnSchemaV2 &column = column_array[idx];

    // partial colums are hidden
    bool is_hidden = (0 == (idx % hidden_column_mod));
    FILL_COLUMN_SCHEMA(idx, column, base_column_id, is_hidden);
    ASSERT_EQ(OB_SUCCESS, table.add_column(column));
  }

  ObColumnSchemaV2 *iter = column_array;
  int64_t ignore_hidden_column_idx = 0;

  // check get_column_idx
  for (int64_t idx = 0; idx < 10; idx++, iter++) {
    EXPECT_EQ(idx, table.get_column_idx(idx + base_column_id));

    // idx of non-hidden column is independent
    if (! iter->is_hidden()) {
      EXPECT_EQ(ignore_hidden_column_idx, table.get_column_idx(idx + base_column_id, true));
      ignore_hidden_column_idx++;
    }
  }

  // abnormal situations
  EXPECT_GT(0, table.get_column_idx(MAX_COLUMN_COUNT + base_column_id));
  EXPECT_GT(0, table.get_column_idx(MAX_COLUMN_COUNT + base_column_id, true));
  EXPECT_GT(0, table.get_column_idx(MAX_COLUMN_COUNT + base_column_id, false));
  EXPECT_GT(0, table.get_column_idx(-1 + base_column_id));
  EXPECT_GT(0, table.get_column_idx(-1 + base_column_id, true));
  EXPECT_GT(0, table.get_column_idx(-1 + base_column_id, false));
}

void test_ob_table_schema_construct_and_assign(void)
{
  int ret = OB_SUCCESS;
  ObTableSchema table1;
  ObTableSchema table2;
  ObColumnSchemaV2 columns[2];
  fill_column_schema(columns[0], 3, "c1", 1, 1, 1);
  fill_column_schema(columns[1], 5, "c2", 2, 2, 0);
  columns[0].set_data_type(ObVarcharType);
  columns[1].set_data_type(ObVarcharType);
  ret = table1.add_column(columns[0]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table1.add_column(columns[1]);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObColumnSchemaV2 *column1 = table1.get_column_schema("c1");
  //bool is_key = false;
  int64_t key_pos = 0;
  ASSERT_NE(static_cast<ObColumnSchemaV2 *>(NULL), column1);
  //ret = table1.check_column_is_partition_key(20, is_key);
  //ASSERT_EQ(OB_ERR_BAD_FIELD_ERROR, ret);
  key_pos = column1->get_tbl_part_key_pos();
  ASSERT_EQ(static_cast<int64_t>(1), key_pos);
  //ret = table1.check_column_is_partition_key(3, is_key);
  //ASSERT_TRUE(is_key);
  ret = table1.add_partition_key(ObString::make_string("c1"));
  ASSERT_EQ(OB_SUCCESS ,ret);
  ret = table1.add_partition_key(ObString::make_string("c1"));
  ASSERT_EQ(OB_SUCCESS ,ret);
  ret = table1.add_partition_key(ObString::make_string("c2"));
  ASSERT_EQ(OB_SUCCESS,ret);
  ret = table1.add_partition_key(ObString::make_string("c3"));
  ASSERT_EQ(OB_ERR_BAD_FIELD_ERROR ,ret);

  ret = table1.add_zone("zone3");
  ASSERT_EQ(OB_SUCCESS, ret);

  //test construct
  const int64_t size = table1.get_convert_size();
  char *buf = static_cast<char *>(ob_malloc(size));
  ASSERT_NE(static_cast<char *>(NULL), buf);
  ObDataBuffer data_buf(buf + sizeof(ObTableSchema), size - sizeof(ObTableSchema));
  ObTableSchema *new_schema = new(buf)ObTableSchema(&data_buf);
  ASSERT_NE(static_cast<ObTableSchema *>(NULL), new_schema);

  //test assign
  ret = (*new_schema).assign(table1);
  ASSERT_EQ(OB_SUCCESS ,ret);
  ObSchemaTestUtils::expect_table_eq(new_schema, &table1);
  ob_free(buf);
  buf = NULL;
}

void test_ob_table_schema_index_tid(void)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos_array;
  ObTableSchema table;
  ret = table.add_simple_index_info(ObAuxTableMetaInfo(4001, USER_TABLE, OB_INVALID_VERSION));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table.add_simple_index_info(ObAuxTableMetaInfo(4002, USER_TABLE, OB_INVALID_VERSION));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table.add_simple_index_info(ObAuxTableMetaInfo(4002, USER_TABLE, OB_INVALID_VERSION));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table.add_simple_index_info(ObAuxTableMetaInfo(4001, USER_TABLE, OB_INVALID_VERSION));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table.get_simple_index_infos(simple_index_infos_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, simple_index_infos_array.count());
  ASSERT_EQ(static_cast<uint64_t>(4001), simple_index_infos_array.at(0));
  ASSERT_EQ(static_cast<uint64_t>(4002), simple_index_infos_array.at(1));
}

void test_ob_table_schema_serialize(void)
{
  int ret = OB_SUCCESS;
  char buf[BUF_SIZE];
  int64_t pos = 0;
  int64_t data_len = 0;
  ObTableSchema src_table;
  ObTableSchema dst_table;
  ObColumnSchemaV2 columns[2];
  fill_table_schema(src_table);
  fill_column_schema(columns[0], 1, "c1", 1, 1, 1);
  fill_column_schema(columns[1], 2, "c2", 2, 2, 2);
  ret = src_table.add_column(columns[0]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = src_table.add_column(columns[1]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = src_table.serialize(buf, BUF_SIZE, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(pos, src_table.get_serialize_size());
  data_len = pos;
  pos = 0;
  ret = dst_table.deserialize(buf, data_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObSchemaTestUtils::expect_table_eq(&src_table, &dst_table);
}

void test_ob_shadow_rowkey_info(void)
{
  int ret = OB_SUCCESS;
  ObTableSchema table;
  ObColumnSchemaV2 columns[4];
  fill_table_schema(table);
  fill_column_schema(columns[0], 1, "c1", 1, 0, 0);
  fill_column_schema(columns[1], 2, "c2", 2, 0, 0);
  fill_column_schema(columns[2], OB_MIN_SHADOW_COLUMN_ID + 1, "shadow_pk_1", 3, 0, 0);
  fill_column_schema(columns[3], OB_MIN_SHADOW_COLUMN_ID + 2, "shadow_pk_2", 4, 0, 0);
  columns[0].set_data_type(ObVarcharType);
  columns[1].set_data_type(ObVarcharType);
  columns[2].set_data_type(ObVarcharType);
  columns[3].set_data_type(ObVarcharType);
  ret = table.add_column(columns[0]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table.add_column(columns[1]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table.add_column(columns[2]);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table.add_column(columns[3]);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObRowkeyInfo &shadow_rowkey_info = table.get_shadow_rowkey_info();
  ASSERT_EQ(2, table.get_shadow_rowkey_column_num());
  ASSERT_EQ(2, shadow_rowkey_info.get_size());
  for (int64_t i =0; i < shadow_rowkey_info.get_size(); ++i) {
    const ObRowkeyColumn *column = shadow_rowkey_info.get_column(i);
    ASSERT_NE(static_cast<ObRowkeyColumn*>(NULL), column);
    ASSERT_EQ(columns[2+i].get_column_id(), column->column_id_);
  }
}

TEST(ObTableSchemaTest, ob_table_schema_test)
{
  test_ob_table_schema_get_methods();
  test_ob_table_column_operations();
  test_ob_rowkey_max_length();
  test_ob_table_schema_is_valid();
  test_ob_table_schema_get_column();
  test_ob_table_schema_construct_and_assign();
  test_ob_table_schema_index_tid();
  test_ob_table_schema_serialize();
  test_ob_table_schema_get_column_idx();
  test_ob_shadow_rowkey_info();
}

TEST(ObTableSchemaTest, index_name)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  const uint64_t data_table_id = 3003;
  ObString index_name = ObString::make_string("haijing_index");
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObString index_table_name;
  ret = ObTableSchema::build_index_table_name(allocator, data_table_id,
      index_name, index_table_name);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(index_table_name, ObString::make_string("__idx_3003_haijing_index"));
  ASSERT_EQ(OB_SUCCESS, table_schema.set_table_name(index_table_name));
  table_schema.set_table_type(share::schema::USER_INDEX);
  ObString extracted_index_name;
  ASSERT_EQ(OB_SUCCESS, table_schema.get_index_name(extracted_index_name));
  ASSERT_EQ(index_name, extracted_index_name);
}

TEST(ObTableSchemaTest, seri)
{
  int ret = OB_SUCCESS;
  ObTableSchema table;
  ret = table.add_simple_index_info(ObAuxTableMetaInfo(4001, USER_TABLE, OB_INVALID_VERSION));
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t seri_size = table.get_serialize_size();
  ASSERT_NE(0, seri_size);
  static const int64_t buf_len = 1024;
  char buf[buf_len];
  int64_t pos = 0;
  ret = table.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(0, pos);
  ASSERT_EQ(seri_size, pos);
  pos = 0;
  ObTableSchema new_table;
  ret = new_table.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(table.get_index_tid_count(), new_table.get_index_tid_count());
}

TEST(ObTableSchemaTest, ob_col_desc_seri)
{
  int ret = OB_SUCCESS;
  uint64_t col_id = 16;
  ObObjMeta obj_meta;
  obj_meta.set_type(ObIntType);
  ObOrderType order = ObOrderType::DESC;
  ObColDesc col_desc;
  col_desc.col_id_ = col_id;
  col_desc.col_type_ = obj_meta;
  col_desc.col_order_ = order;
  int64_t seri_size = col_desc.get_serialize_size();
  ASSERT_NE(0, seri_size);
  static const int64_t buf_len = 1024;
  char buf[buf_len];
  int64_t pos = 0;
  ret = col_desc.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(0, pos);
  ASSERT_EQ(seri_size, pos);
  pos = 0;
  ObColDesc new_col_desc;
  ret = new_col_desc.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(new_col_desc.col_id_, col_desc.col_id_);
  ASSERT_EQ(new_col_desc.col_type_, col_desc.col_type_);
}

}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
