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

#ifndef OCEANBASE_BLOCKSSTABLE_OB_SCHEMA_GENERATOR_H_
#define OCEANBASE_BLOCKSSTABLE_OB_SCHEMA_GENERATOR_H_

#include "share/schema/ob_table_schema.h"
namespace oceanbase
{
namespace blocksstable
{
class ObSchemaGenerator
{
public:
  static inline int generate_table(
      const uint64_t table_id, const int64_t column_count,
      const int64_t rowkey_count,
      share::schema::ObTableSchema &table_schema);
private:
  static void set_column_type(
      const common::ObObjType obj_type, share::schema::ObColumnSchemaV2 &column);
};

int ObSchemaGenerator::generate_table(
    const uint64_t table_id, const int64_t column_count,
    const int64_t rowkey_count, share::schema::ObTableSchema &table_schema)
{
  int ret = common::OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  share::schema::ObColumnSchemaV2 column;
  table_schema.reset();
  table_schema.set_table_name("test_table");
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(combine_id(tenant_id, 1));
  table_schema.set_database_id(combine_id(tenant_id, 1));
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(rowkey_count);
  table_schema.set_max_used_column_id(column_count);
  table_schema.set_block_size(2L * 1024);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  int64_t rowkey_pos = 0;
  for(int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    if (OB_FAIL(column.set_column_name(name))) {
      STORAGE_LOG(WARN, "set_column_name failed", K(ret));
    } else {
      set_column_type(obj_type, column);
      column.set_data_length(1);
      if (obj_type >= common::ObIntType && rowkey_pos < rowkey_count) {
        ++rowkey_pos;
        column.set_rowkey_position(rowkey_pos);
      } else {
        column.set_rowkey_position(0);
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_schema.add_column(column))) {
        STORAGE_LOG(WARN, "add_column failed", K(ret), K(column));
      }
    }
  }

  return ret;
}

void ObSchemaGenerator::set_column_type(const common::ObObjType obj_type, share::schema::ObColumnSchemaV2 &column)
{
  ObObjMeta meta_type;
  meta_type.set_type(obj_type);
  column.set_meta_type(meta_type);
  if (ob_is_string_type(obj_type) && obj_type != ObHexStringType) {
    meta_type.set_collation_level(CS_LEVEL_IMPLICIT);
    meta_type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_meta_type(meta_type);
  }
}

}//end namespace blocksstable
}//end namespace oceanbase

#endif //OCEANBASE_BLOCKSSTABLE_OB_SCHEMA_GENERATE_H_
