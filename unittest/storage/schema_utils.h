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

#pragma once

#define USING_LOG_PREFIX STORAGE

#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/charset/ob_charset.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_store_format.h"
#include "common/object/ob_obj_type.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace storage
{
class TestSchemaUtils
{
public:
  static void prepare_data_schema(share::schema::ObTableSchema &table_schema);
  static void prepare_index_schema(share::schema::ObTableSchema &index_schema);
private:
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 3;
  static const int64_t TEST_COLUMN_CNT = 6;
  static const uint64_t TEST_TENANT_ID = 1;
};

void TestSchemaUtils::prepare_data_schema(share::schema::ObTableSchema &table_schema)
{
  int ret = common::OB_SUCCESS;
  const uint64_t table_id = 219039915101;
  int64_t micro_block_size = 16 * 1024;
  share::schema::ObColumnSchemaV2 column;

  table_schema.reset();
  ret = table_schema.set_table_name("test_ls_tablet_service_data_table");
  ASSERT_EQ(common::OB_SUCCESS, ret);
  table_schema.set_schema_version(100);
  table_schema.set_tenant_id(TEST_TENANT_ID);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(common::ObRowStoreType::FLAT_ROW_STORE);
  //init column
  char name[common::OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  const int64_t column_ids[] = {16,17,20,21,22,23,24,29};
  for(int64_t i = 0; i < TEST_COLUMN_CNT; ++i){
    common::ObObjType obj_type = common::ObObjType::ObIntType;
    const int64_t column_id = column_ids[i];

    if (i == 1) {
      obj_type = common::ObObjType::ObVarcharType;
    }
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(column_id);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(common::OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(common::ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    if (i < TEST_ROWKEY_COLUMN_CNT) {
      column.set_rowkey_position(i + 1);
    } else {
      column.set_rowkey_position(0);
    }
    LOG_INFO("add column", K(i), K(column));
    ASSERT_EQ(common::OB_SUCCESS, table_schema.add_column(column));
  }
  LOG_INFO("dump data table schema", LITERAL_K(TEST_ROWKEY_COLUMN_CNT), K(table_schema));
}

void TestSchemaUtils::prepare_index_schema(share::schema::ObTableSchema &index_schema)
{
  int ret = common::OB_SUCCESS;
  const uint64_t index_table_id = 219019935101;
  const int64_t col_cnt = TEST_ROWKEY_COLUMN_CNT + 1;
  int64_t micro_block_size = 16 * 1024;
  share::schema::ObColumnSchemaV2 column;

  index_schema.reset();
  ret = index_schema.set_table_name("test_ls_tablet_service_index_table");
  ASSERT_EQ(common::OB_SUCCESS, ret);
  index_schema.set_tenant_id(TEST_TENANT_ID);
  index_schema.set_tablegroup_id(1);
  index_schema.set_database_id(1);
  index_schema.set_table_id(index_table_id);
  index_schema.set_rowkey_column_num(col_cnt);
  index_schema.set_max_used_column_id(col_cnt);
  index_schema.set_block_size(micro_block_size);
  index_schema.set_compress_func_name("none");
  index_schema.set_row_store_type(FLAT_ROW_STORE);

  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  const int64_t column_ids[] = {16,17,18,19};
  for (int64_t i = 0; i < col_cnt; ++i) {
    ObObjType obj_type = ObVarcharType;
    const int64_t column_id = column_ids[i];

    if (i != 2) {
      obj_type = ObIntType;
    }
    column.reset();
    column.set_table_id(index_table_id);
    column.set_column_id(column_id);
    if (0 == i) {
      int64_t i_name = 3;
      sprintf(name, "test%020ld", i_name);
    } else {
      sprintf(name, "test%020ld", i - 1);
    }
    ASSERT_EQ(common::OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(common::ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    column.set_rowkey_position(i + 1);
    LOG_INFO("add column", K(i), K(column));
    ASSERT_EQ(common::OB_SUCCESS, index_schema.add_column(column));
  }
  LOG_INFO("dump index table schema", LITERAL_K(TEST_ROWKEY_COLUMN_CNT), K(index_schema));
}
} // namespace storage
} // namespace oceanbase
