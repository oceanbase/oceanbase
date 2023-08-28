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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_inner_table_schema.h"

#include "share/schema/ob_schema_macro_define.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_table_schema.h"
#include "share/scn.h"

namespace oceanbase
{
using namespace share::schema;
using namespace common;
namespace share
{

int ObInnerTableSchema::tenant_virtual_all_table_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_ALL_TABLE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_ALL_TABLE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_name", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_type", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLE_TYPE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("engine", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_ENGINE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("version", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("row_format", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      ROW_FORMAT_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rows", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("avg_row_length", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("data_length", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("max_data_length", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("index_length", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("data_free", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("auto_increment", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("create_time", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObTimestampType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(ObPreciseDateTime), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      false); //is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("update_time", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObTimestampType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(ObPreciseDateTime), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      false); //is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("check_time", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObTimestampType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(ObPreciseDateTime), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      false); //is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("collation", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_COLLATION_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("checksum", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_options", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_TABLE_STATUS_CREATE_OPTION_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_TABLE_COMMENT_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_table_column_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_TABLE_COLUMN_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_TABLE_COLUMN_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("field", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_COLUMN_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_VARCHAR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("collation", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_COLLATION_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("null", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      COLUMN_NULLABLE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("key", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      COLUMN_KEY_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("default", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      COLUMN_DEFAULT_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("extra", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      COLUMN_EXTRA_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("privileges", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_COLUMN_PRIVILEGE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_COLUMN_COMMENT_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_table_index_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_TABLE_INDEX_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_TABLE_INDEX_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj key_name_default;
    key_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("key_name", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_COLUMN_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      key_name_default,
      key_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj seq_in_index_default;
    seq_in_index_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("seq_in_index", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      seq_in_index_default,
      seq_in_index_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj table_schema_default;
    table_schema_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("table_schema", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_DATABASE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      table_schema_default,
      table_schema_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj table_default;
    table_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("table", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      table_default,
      table_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj non_unique_default;
    non_unique_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("non_unique", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      non_unique_default,
      non_unique_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj index_schema_default;
    index_schema_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("index_schema", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_DATABASE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      index_schema_default,
      index_schema_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj column_name_default;
    column_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("column_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_COLUMN_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      column_name_default,
      column_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("collation", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_COLLATION_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cardinality", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      INDEX_SUB_PART_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("packed", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      INDEX_PACKED_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj null_default;
    null_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("null", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      INDEX_NULL_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      null_default,
      null_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj index_type_default;
    index_type_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("index_type", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      INDEX_NULL_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      index_type_default,
      index_type_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_TABLE_COMMENT_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj index_comment_default;
    index_comment_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("index_comment", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_TABLE_COMMENT_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      index_comment_default,
      index_comment_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj is_visible_default;
    is_visible_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("is_visible", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_COLUMN_YES_NO_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      is_visible_default,
      is_visible_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("expression", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_DEFAULT_VALUE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_show_create_database_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_DATABASE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_database", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      DATABASE_DEFINE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_database_with_if_not_exists", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      DATABASE_DEFINE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_show_create_table_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_table", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObLongTextType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      0, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("character_set_client", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_CHARSET_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("collation_connection", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_CHARSET_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_session_variable_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_SESSION_VARIABLE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_SESSION_VARIABLE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ObObj variable_name_default;
    variable_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("variable_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_NAME_LEN, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      variable_name_default,
      variable_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("value", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_VALUE_LEN, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_privilege_grant_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("user_id", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("grants", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_GRANT_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_virtual_processlist_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_PROCESSLIST_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PROCESSLIST_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ObObj id_default;
    id_default.set_uint64(0);
    ADD_COLUMN_SCHEMA_T("id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      id_default,
      id_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj user_default;
    user_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("user", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_USERNAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      user_default,
      user_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TENANT_NAME_LENGTH_STORE, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj host_default;
    host_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("host", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_HOST_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      host_default,
      host_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("db", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_DATABASE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj command_default;
    command_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("command", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_COMMAND_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      command_default,
      command_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj sql_id_default;
    sql_id_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("sql_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_SQL_ID_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      sql_id_default,
      sql_id_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj time_default;
    time_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("time", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      time_default,
      time_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("state", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_SESSION_STATE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_COLUMN_VARCHAR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      1, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_IP_ADDR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_port", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      2, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sql_port", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("proxy_sessid", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("master_sessid", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("user_client_ip", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_IP_ADDR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("user_host", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_HOST_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trans_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("thread_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObUInt64Type, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(uint64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ssl_cipher", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_COMMAND_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj trace_id_default;
    trace_id_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("trace_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TRACE_ID_BUFFER_SIZE, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false, //is_autoincrement
      trace_id_default,
      trace_id_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trans_state", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TRANS_STATE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj total_time_default;
    total_time_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("total_time", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      total_time_default,
      total_time_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj retry_cnt_default;
    retry_cnt_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("retry_cnt", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      retry_cnt_default,
      retry_cnt_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj retry_info_default;
    retry_info_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("retry_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      retry_info_default,
      retry_info_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj action_default;
    action_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("action", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_VALUE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false, //is_autoincrement
      action_default,
      action_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj module_default;
    module_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("module", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_VALUE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false, //is_autoincrement
      module_default,
      module_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj client_info_default;
    client_info_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("client_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_VALUE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false, //is_autoincrement
      client_info_default,
      client_info_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sql_trace", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObTinyIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      1, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("plan_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("effective_tenant_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("level", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sample_percentage", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("record_policy", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      32, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lb_vid", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      20, //column_precision
      0, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lb_vip", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_IP_ADDR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lb_vport", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("in_bytes", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      20, //column_precision
      0, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("out_bytes", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      20, //column_precision
      0, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_num(1);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_LIST_COLUMNS);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("svr_ip, svr_port"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    } else if (OB_FAIL(table_schema.mock_list_partition_array())) {
      LOG_WARN("mock list partition array failed", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_warning_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_WARNING_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_WARNING_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("level", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      32, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("code", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("message", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      512, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ori_code", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sql_state", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      6, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_current_tenant_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_CURRENT_TENANT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_CURRENT_TENANT_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TENANT_NAME_LENGTH_STORE, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_stmt", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      TENANT_DEFINE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_database_status_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_DATABASE_STATUS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_DATABASE_STATUS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("db", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_DATABASE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      1, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_IP_ADDR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_port", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      2, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("read_only", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_num(1);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_LIST_COLUMNS);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("svr_ip, svr_port"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    } else if (OB_FAIL(table_schema.mock_list_partition_array())) {
      LOG_WARN("mock list partition array failed", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_tenant_status_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_TENANT_STATUS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_TENANT_STATUS_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TENANT_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      1, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_IP_ADDR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_port", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      2, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("read_only", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_num(1);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_LIST_COLUMNS);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("svr_ip, svr_port"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    } else if (OB_FAIL(table_schema.mock_list_partition_array())) {
      LOG_WARN("mock list partition array failed", K(ret));
    }
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_statname_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_STATNAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_STATNAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("stat_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("statistic#", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      64, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("display_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      64, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("class", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_event_name_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_EVENT_NAME_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_EVENT_NAME_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("event_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("event#", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      64, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("display_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      64, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("parameter1", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      64, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("parameter2", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      64, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("parameter3", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      64, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("wait_class_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("wait_class#", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("wait_class", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      64, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_global_variable_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ObObj variable_name_default;
    variable_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("variable_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_NAME_LEN, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      variable_name_default,
      variable_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("value", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_VALUE_LEN, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_show_tables_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_SHOW_TABLES_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_SHOW_TABLES_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_name", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_type", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLE_TYPE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_show_create_procedure_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ZERO);
  table_schema.set_charset_type(ObCharset::get_default_charset());
  table_schema.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_id", //column_name
      ++column_id, //column_id
      1, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_ROUTINE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_routine", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObLongTextType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      0, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("proc_type", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObIntType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(int64_t), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("character_set_client", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_CHARSET_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("collation_connection", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_CHARSET_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("collation_database", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_CHARSET_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sql_mode", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_CHARSET_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}


} // end namespace share
} // end namespace oceanbase
