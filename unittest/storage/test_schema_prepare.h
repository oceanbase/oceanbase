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
#define protected public

#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"
#include "src/share/schema/ob_table_param.h"

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace unittest
{
class TestSchemaPrepare
{
public:
  static void prepare_schema(
    share::schema::ObTableSchema &table_schema,
    const int64_t rowkey_column_cnt = TEST_ROWKEY_COLUMN_CNT,
    const int64_t column_cnt = TEST_COLUMN_CNT,
    const int64_t micro_block_size = DEFAULT_MICRO_BLOCK_SIZE);

  static void add_all_and_each_column_group(
    ObIAllocator &allocator,
    share::schema::ObTableSchema &table_schema);
  static const int64_t TENANT_ID = 1;
  static const int64_t TABLE_ID = 7777;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 3;
  static const int64_t TEST_COLUMN_CNT = 6;
  static const int64_t DEFAULT_MICRO_BLOCK_SIZE = 16 * 1024;

};

void TestSchemaPrepare::prepare_schema(
  share::schema::ObTableSchema &table_schema,
  const int64_t rowkey_column_cnt,
  const int64_t column_cnt,
  const int64_t micro_block_size)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = TENANT_ID;
  const uint64_t table_id = TABLE_ID;
  ASSERT_TRUE(column_cnt >= rowkey_column_cnt);
  share::schema::ObColumnSchemaV2 column;

  //generate data table schema
  table_schema.reset();
  ret = table_schema.set_table_name("test_merge_multi_version");
  ASSERT_EQ(OB_SUCCESS, ret);
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(rowkey_column_cnt);
  table_schema.set_max_used_column_id(common::OB_APP_MIN_COLUMN_ID + column_cnt);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_pctfree(10);
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for(int64_t i = 0; i < column_cnt; ++i){
    ObObjType obj_type = ObIntType;
    const int64_t column_id = common::OB_APP_MIN_COLUMN_ID + i;

    if (i == 1) {
      obj_type = ObVarcharType;
    }
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(column_id);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    if (i < rowkey_column_cnt) {
      column.set_rowkey_position(i + 1);
    } else {
      column.set_rowkey_position(0);
    }

    share::schema::ObSkipIndexColumnAttr skip_idx_attr;
    if (!is_lob_storage(obj_type)) {
      skip_idx_attr.set_min_max();
      column.set_skip_index_attr(skip_idx_attr.get_packed_value());
    }
    COMMON_LOG(INFO, "add column", K(i), K(column));
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
  COMMON_LOG(INFO, "dump stable schema", LITERAL_K(TEST_ROWKEY_COLUMN_CNT), K(table_schema));
}

void TestSchemaPrepare::add_all_and_each_column_group(
  ObIAllocator &allocator,
  share::schema::ObTableSchema &table_schema)
{
  ObArray<share::schema::ObColDesc> col_ids;
  int64_t store_column_count;
  ASSERT_EQ(OB_SUCCESS, table_schema.get_store_column_ids(col_ids));
  ASSERT_EQ(OB_SUCCESS, table_schema.get_store_column_count(store_column_count));
  ASSERT_EQ(store_column_count, col_ids.count());
  ObRowStoreType row_store_type = FLAT_ROW_STORE;

  //add all_cg_schemas
  share::schema::ObColumnGroupSchema cg_1;
  cg_1.column_group_type_ = share::schema::ObColumnGroupType::ALL_COLUMN_GROUP;
  cg_1.column_group_id_ = store_column_count;
  cg_1.column_id_cnt_ = store_column_count;
  uint64_t *cg_1_ids = static_cast<uint64_t *>(allocator.alloc(store_column_count * sizeof(uint64_t)));
  for(int64_t i = 0; i < store_column_count; i++) {
    cg_1_ids[i] = col_ids.at(i).col_id_;
  }
  cg_1.column_id_arr_ = cg_1_ids;
  cg_1.column_group_name_ = "test_all";
  cg_1.row_store_type_ = row_store_type;
  ASSERT_EQ(OB_SUCCESS, table_schema.add_column_group(cg_1));

  //add single_cg_schema
  for(int64_t i = 0; i < store_column_count; i++) {
    share::schema::ObColumnGroupSchema cg_2;
    cg_2.column_group_type_ = share::schema::ObColumnGroupType::SINGLE_COLUMN_GROUP;
    cg_2.column_group_id_ = i;
    char c = i + '0';
    cg_2.column_group_name_ = &c;
    cg_2.column_id_cnt_ = 1;
    cg_2.column_id_arr_capacity_ = 1;
    uint64_t column_ids[1] = {col_ids.at(i).col_id_};
    cg_2.column_id_arr_ = column_ids;
    cg_2.row_store_type_ = row_store_type;
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column_group(cg_2));
  }

  allocator.free(cg_1_ids);
}

}
}