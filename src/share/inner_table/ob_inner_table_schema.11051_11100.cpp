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

int ObInnerTableSchema::tenant_virtual_outline_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_OUTLINE_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_OUTLINE_TNAME))) {
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
    ADD_COLUMN_SCHEMA("database_id", //column_name
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
    ADD_COLUMN_SCHEMA("outline_id", //column_name
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
    ObObj database_name_default;
    database_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("database_name", //column_name
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
      database_name_default,
      database_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj outline_name_default;
    outline_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("outline_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_OUTLINE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      outline_name_default,
      outline_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("visible_signature", //column_name
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
    ADD_COLUMN_SCHEMA("sql_text", //column_name
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
    ADD_COLUMN_SCHEMA("outline_target", //column_name
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
    ADD_COLUMN_SCHEMA("outline_sql", //column_name
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
    ADD_COLUMN_SCHEMA("outline_content", //column_name
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
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::tenant_virtual_concurrent_limit_sql_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_TNAME))) {
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
    ADD_COLUMN_SCHEMA("database_id", //column_name
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
    ADD_COLUMN_SCHEMA("outline_id", //column_name
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
    ObObj database_name_default;
    database_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("database_name", //column_name
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
      database_name_default,
      database_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ObObj outline_name_default;
    outline_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("outline_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_OUTLINE_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      outline_name_default,
      outline_name_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("outline_content", //column_name
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
    ADD_COLUMN_SCHEMA("visible_signature", //column_name
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
    ADD_COLUMN_SCHEMA("sql_text", //column_name
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
    ObObj concurrent_num_default;
    concurrent_num_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("concurrent_num", //column_name
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
      concurrent_num_default,
      concurrent_num_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("limit_target", //column_name
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
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_virtual_tablet_sstable_macro_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_TABLET_SSTABLE_MACRO_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(7);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TABLET_SSTABLE_MACRO_INFO_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip", //column_name
      ++column_id, //column_id
      1, //rowkey_id
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
      2, //rowkey_id
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
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
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
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ls_id", //column_name
      ++column_id, //column_id
      4, //rowkey_id
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
    ADD_COLUMN_SCHEMA("tablet_id", //column_name
      ++column_id, //column_id
      5, //rowkey_id
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
    ADD_COLUMN_SCHEMA("end_log_scn", //column_name
      ++column_id, //column_id
      6, //rowkey_id
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
    ADD_COLUMN_SCHEMA("macro_idx_in_sstable", //column_name
      ++column_id, //column_id
      7, //rowkey_id
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
    ADD_COLUMN_SCHEMA("macro_logic_version", //column_name
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
    ADD_COLUMN_SCHEMA("macro_block_idx", //column_name
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
    ADD_COLUMN_SCHEMA("data_seq", //column_name
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
    ADD_COLUMN_SCHEMA("row_count", //column_name
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
    ADD_COLUMN_SCHEMA("original_size", //column_name
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
    ADD_COLUMN_SCHEMA("encoding_size", //column_name
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
    ADD_COLUMN_SCHEMA("compressed_size", //column_name
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
    ADD_COLUMN_SCHEMA("occupy_size", //column_name
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
    ADD_COLUMN_SCHEMA("micro_block_count", //column_name
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
    ADD_COLUMN_SCHEMA("data_checksum", //column_name
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
    ADD_COLUMN_SCHEMA("macro_range", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_RANGE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("macro_block_type", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_VALUE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("compressor_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_COMPRESSOR_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("row_store_type", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_COMPRESSOR_NAME_LENGTH, //column_length
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

int ObInnerTableSchema::all_virtual_proxy_partition_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TNAME))) {
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
    ADD_COLUMN_SCHEMA("tenant_name", //column_name
      ++column_id, //column_id
      1, //rowkey_id
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
    ADD_COLUMN_SCHEMA("table_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
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
    ADD_COLUMN_SCHEMA("part_level", //column_name
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
    ADD_COLUMN_SCHEMA("all_part_num", //column_name
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
    ADD_COLUMN_SCHEMA("template_num", //column_name
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
    ADD_COLUMN_SCHEMA("part_id_rule_ver", //column_name
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
    ADD_COLUMN_SCHEMA("part_type", //column_name
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
    ADD_COLUMN_SCHEMA("part_num", //column_name
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
    ADD_COLUMN_SCHEMA("is_column_type", //column_name
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
    ADD_COLUMN_SCHEMA("part_space", //column_name
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
    ADD_COLUMN_SCHEMA("part_expr", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_expr_bin", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_B_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_range_type", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_B_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_interval", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_interval_bin", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_B_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("interval_start", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("interval_start_bin", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_B_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_type", //column_name
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
    ADD_COLUMN_SCHEMA("sub_part_num", //column_name
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
    ADD_COLUMN_SCHEMA("is_sub_column_type", //column_name
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
    ADD_COLUMN_SCHEMA("sub_part_space", //column_name
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
    ADD_COLUMN_SCHEMA("sub_part_expr", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_expr_bin", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_B_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_range_type", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_B_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("def_sub_part_interval", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("def_sub_part_interval_bin", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_B_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("def_sub_interval_start", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("def_sub_interval_start_bin", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_B_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_key_num", //column_name
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
    ADD_COLUMN_SCHEMA("part_key_name", //column_name
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
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_key_type", //column_name
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
    ADD_COLUMN_SCHEMA("part_key_idx", //column_name
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
    ADD_COLUMN_SCHEMA("part_key_level", //column_name
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
    ADD_COLUMN_SCHEMA("part_key_extra", //column_name
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
    ADD_COLUMN_SCHEMA("part_key_collation_type", //column_name
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
    ADD_COLUMN_SCHEMA("part_key_rowkey_idx", //column_name
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
    ADD_COLUMN_SCHEMA("part_key_expr", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_key_length", //column_name
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
    ADD_COLUMN_SCHEMA("part_key_precision", //column_name
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
    ADD_COLUMN_SCHEMA("part_key_scale", //column_name
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
    ADD_COLUMN_SCHEMA("spare1", //column_name
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
    ADD_COLUMN_SCHEMA("spare2", //column_name
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
    ADD_COLUMN_SCHEMA("spare3", //column_name
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
    ADD_COLUMN_SCHEMA("spare4", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare5", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare6", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_key_default_value", //column_name
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
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_virtual_proxy_partition_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_PROXY_PARTITION_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PROXY_PARTITION_TNAME))) {
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
    ADD_COLUMN_SCHEMA("tenant_name", //column_name
      ++column_id, //column_id
      1, //rowkey_id
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
    ADD_COLUMN_SCHEMA("table_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
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
    ADD_COLUMN_SCHEMA("part_id", //column_name
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
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("status", //column_name
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
    ADD_COLUMN_SCHEMA("low_bound_val", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("low_bound_val_bin", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_B_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("high_bound_val", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("high_bound_val_bin", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_B_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_position", //column_name
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
    ADD_COLUMN_SCHEMA("tablet_id", //column_name
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
    ADD_COLUMN_SCHEMA("sub_part_num", //column_name
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
    ADD_COLUMN_SCHEMA("sub_part_space", //column_name
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
    ADD_COLUMN_SCHEMA("sub_part_interval", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_interval_bin", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_B_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_interval_start", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_interval_start_bin", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare1", //column_name
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
    ADD_COLUMN_SCHEMA("spare2", //column_name
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
    ADD_COLUMN_SCHEMA("spare3", //column_name
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
    ADD_COLUMN_SCHEMA("spare4", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare5", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare6", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
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

int ObInnerTableSchema::all_virtual_proxy_sub_partition_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TNAME))) {
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
    ADD_COLUMN_SCHEMA("tenant_name", //column_name
      ++column_id, //column_id
      1, //rowkey_id
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
    ADD_COLUMN_SCHEMA("table_id", //column_name
      ++column_id, //column_id
      2, //rowkey_id
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
    ADD_COLUMN_SCHEMA("part_id", //column_name
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
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_id", //column_name
      ++column_id, //column_id
      4, //rowkey_id
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
    ADD_COLUMN_SCHEMA("part_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("status", //column_name
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
    ADD_COLUMN_SCHEMA("low_bound_val", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("low_bound_val_bin", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_B_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("high_bound_val", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("high_bound_val_bin", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_B_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_position", //column_name
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
    ADD_COLUMN_SCHEMA("tablet_id", //column_name
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
    ADD_COLUMN_SCHEMA("spare1", //column_name
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
    ADD_COLUMN_SCHEMA("spare2", //column_name
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
    ADD_COLUMN_SCHEMA("spare3", //column_name
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
    ADD_COLUMN_SCHEMA("spare4", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare5", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare6", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_PARTITION_EXPR_LENGTH, //column_length
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

int ObInnerTableSchema::all_virtual_sys_task_status_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_SYS_TASK_STATUS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SYS_TASK_STATUS_TNAME))) {
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
    ADD_COLUMN_SCHEMA_TS("start_time", //column_name
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
    ADD_COLUMN_SCHEMA("task_type", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_SYS_TASK_TYPE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("task_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_TRACE_STAT_BUFFER_SIZE, //column_length
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
    ObObj comment_default;
    comment_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("comment", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TASK_COMMENT_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false, //is_autoincrement
      comment_default,
      comment_default); //default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_cancel", //column_name
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

int ObInnerTableSchema::all_virtual_macro_block_marker_status_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_MACRO_BLOCK_MARKER_STATUS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_MACRO_BLOCK_MARKER_STATUS_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip", //column_name
      ++column_id, //column_id
      1, //rowkey_id
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
      2, //rowkey_id
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
    ADD_COLUMN_SCHEMA("total_count", //column_name
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
    ADD_COLUMN_SCHEMA("reserved_count", //column_name
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
    ADD_COLUMN_SCHEMA("meta_block_count", //column_name
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
    ADD_COLUMN_SCHEMA("shared_meta_block_count", //column_name
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
    ADD_COLUMN_SCHEMA("tmp_file_count", //column_name
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
    ADD_COLUMN_SCHEMA("data_block_count", //column_name
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
    ADD_COLUMN_SCHEMA("shared_data_block_count", //column_name
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
    ADD_COLUMN_SCHEMA("disk_block_count", //column_name
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
    ADD_COLUMN_SCHEMA("bloomfilter_count", //column_name
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
    ADD_COLUMN_SCHEMA("hold_count", //column_name
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
    ADD_COLUMN_SCHEMA("pending_free_count", //column_name
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
    ADD_COLUMN_SCHEMA("free_count", //column_name
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
    ADD_COLUMN_SCHEMA("mark_cost_time", //column_name
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
    ADD_COLUMN_SCHEMA("sweep_cost_time", //column_name
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
    ADD_COLUMN_SCHEMA_TS("start_time", //column_name
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
    ADD_COLUMN_SCHEMA_TS("last_end_time", //column_name
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
    ADD_COLUMN_SCHEMA("mark_finished", //column_name
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
    ObObj comment_default;
    comment_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("comment", //column_name
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
      comment_default,
      comment_default); //default_value
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

int ObInnerTableSchema::all_virtual_io_stat_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_IO_STAT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_IO_STAT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("fd", //column_name
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
    ADD_COLUMN_SCHEMA("disk_type", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_DISK_TYPE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sys_io_up_limit_in_mb", //column_name
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
    ADD_COLUMN_SCHEMA("sys_io_bandwidth_in_mb", //column_name
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
    ADD_COLUMN_SCHEMA("sys_io_low_watermark_in_mb", //column_name
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
    ADD_COLUMN_SCHEMA("sys_io_high_watermark_in_mb", //column_name
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
    ADD_COLUMN_SCHEMA("io_bench_result", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_IO_BENCH_RESULT_LENGTH, //column_length
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

int ObInnerTableSchema::all_virtual_long_ops_status_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_LONG_OPS_STATUS_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_LONG_OPS_STATUS_TNAME))) {
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
    ADD_COLUMN_SCHEMA("sid", //column_name
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
    ADD_COLUMN_SCHEMA("op_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_LONG_OPS_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("target", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_LONG_OPS_TARGET_LENGTH, //column_length
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
      0, //part_key_pos
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
    ADD_COLUMN_SCHEMA("start_time", //column_name
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
    ADD_COLUMN_SCHEMA("finish_time", //column_name
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
    ADD_COLUMN_SCHEMA("elapsed_time", //column_name
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
    ADD_COLUMN_SCHEMA("remaining_time", //column_name
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
    ADD_COLUMN_SCHEMA("last_update_time", //column_name
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
    ADD_COLUMN_SCHEMA("percentage", //column_name
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
      MAX_LONG_OPS_MESSAGE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trace_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TRACE_ID_BUFFER_SIZE, //column_length
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

int ObInnerTableSchema::all_virtual_server_object_pool_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_SERVER_OBJECT_POOL_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SERVER_OBJECT_POOL_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip", //column_name
      ++column_id, //column_id
      1, //rowkey_id
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
      2, //rowkey_id
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
    ADD_COLUMN_SCHEMA("object_type", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_SYS_PARAM_VALUE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("arena_id", //column_name
      ++column_id, //column_id
      4, //rowkey_id
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
    ADD_COLUMN_SCHEMA("lock", //column_name
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
    ADD_COLUMN_SCHEMA("borrow_count", //column_name
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
    ADD_COLUMN_SCHEMA("return_count", //column_name
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
    ADD_COLUMN_SCHEMA("miss_count", //column_name
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
    ADD_COLUMN_SCHEMA("miss_return_count", //column_name
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
    ADD_COLUMN_SCHEMA("free_num", //column_name
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
    ADD_COLUMN_SCHEMA("last_borrow_ts", //column_name
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
    ADD_COLUMN_SCHEMA("last_return_ts", //column_name
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
    ADD_COLUMN_SCHEMA("last_miss_ts", //column_name
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
    ADD_COLUMN_SCHEMA("last_miss_return_ts", //column_name
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
    ADD_COLUMN_SCHEMA("next", //column_name
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

int ObInnerTableSchema::all_virtual_trans_lock_stat_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_TRANS_LOCK_STAT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TRANS_LOCK_STAT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("trans_id", //column_name
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
    ADD_COLUMN_SCHEMA("ls_id", //column_name
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
    ADD_COLUMN_SCHEMA("table_id", //column_name
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
    ADD_COLUMN_SCHEMA("tablet_id", //column_name
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
    ADD_COLUMN_SCHEMA("rowkey", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      512, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("session_id", //column_name
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
    ADD_COLUMN_SCHEMA("proxy_session_id", //column_name
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
    ADD_COLUMN_SCHEMA_TS("ctx_create_time", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObTimestampType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(ObPreciseDateTime), //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false, //is_autoincrement
      false); //is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("expired_time", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObTimestampType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(ObPreciseDateTime), //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false, //is_autoincrement
      false); //is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("time_after_recv", //column_name
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
    ADD_COLUMN_SCHEMA("row_lock_addr", //column_name
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

int ObInnerTableSchema::tenant_virtual_show_create_tablegroup_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_TNAME))) {
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
    ADD_COLUMN_SCHEMA("tablegroup_id", //column_name
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
    ADD_COLUMN_SCHEMA("tablegroup_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TABLEGROUP_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_tablegroup", //column_name
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
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(ENCODING_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_DYNAMIC_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);
  table_schema.set_tablet_id(0);

  table_schema.set_max_used_column_id(column_id);
  return ret;
}

int ObInnerTableSchema::all_virtual_server_blacklist_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_SERVER_BLACKLIST_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SERVER_BLACKLIST_TNAME))) {
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
    ADD_COLUMN_SCHEMA("dst_ip", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_IP_ADDR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("dst_port", //column_name
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
    ADD_COLUMN_SCHEMA("is_in_blacklist", //column_name
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

int ObInnerTableSchema::all_virtual_tenant_parameter_stat_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("zone", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_ZONE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_type", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      SERVER_TYPE_LENGTH, //column_length
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
    ADD_COLUMN_SCHEMA("name", //column_name
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
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("data_type", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_TYPE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
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
      false, //is_nullable
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
      OB_MAX_CONFIG_INFO_LEN, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("section", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_SECTION_LEN, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("scope", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_SCOPE_LEN, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("source", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_SOURCE_LEN, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("edit_level", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_EDIT_LEVEL_LEN, //column_length
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
      true, //is_nullable
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

int ObInnerTableSchema::all_virtual_server_schema_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_TNAME))) {
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
    ADD_COLUMN_SCHEMA("refreshed_schema_version", //column_name
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
    ADD_COLUMN_SCHEMA("received_schema_version", //column_name
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
    ADD_COLUMN_SCHEMA("schema_count", //column_name
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
    ADD_COLUMN_SCHEMA("schema_size", //column_name
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
    ADD_COLUMN_SCHEMA("min_sstable_schema_version", //column_name
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

int ObInnerTableSchema::all_virtual_memory_context_stat_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_MEMORY_CONTEXT_STAT_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_MEMORY_CONTEXT_STAT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("entity", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("p_entity", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      128, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("hold", //column_name
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
    ADD_COLUMN_SCHEMA("malloc_hold", //column_name
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
    ADD_COLUMN_SCHEMA("malloc_used", //column_name
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
    ADD_COLUMN_SCHEMA("arena_hold", //column_name
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
    ADD_COLUMN_SCHEMA("arena_used", //column_name
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
    ADD_COLUMN_SCHEMA("location", //column_name
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

int ObInnerTableSchema::all_virtual_dump_tenant_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_DUMP_TENANT_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_DUMP_TENANT_INFO_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip", //column_name
      ++column_id, //column_id
      1, //rowkey_id
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
      2, //rowkey_id
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
    ADD_COLUMN_SCHEMA("tenant_id", //column_name
      ++column_id, //column_id
      3, //rowkey_id
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
    ADD_COLUMN_SCHEMA("compat_mode", //column_name
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
    ADD_COLUMN_SCHEMA("unit_min_cpu", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObDoubleType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(double), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("unit_max_cpu", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObDoubleType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(double), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("slice", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObDoubleType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(double), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("remain_slice", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObDoubleType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      sizeof(double), //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("token_cnt", //column_name
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
    ADD_COLUMN_SCHEMA("ass_token_cnt", //column_name
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
    ADD_COLUMN_SCHEMA("lq_tokens", //column_name
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
    ADD_COLUMN_SCHEMA("used_lq_tokens", //column_name
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
    ADD_COLUMN_SCHEMA("stopped", //column_name
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
    ADD_COLUMN_SCHEMA("idle_us", //column_name
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
    ADD_COLUMN_SCHEMA("recv_hp_rpc_cnt", //column_name
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
    ADD_COLUMN_SCHEMA("recv_np_rpc_cnt", //column_name
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
    ADD_COLUMN_SCHEMA("recv_lp_rpc_cnt", //column_name
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
    ADD_COLUMN_SCHEMA("recv_mysql_cnt", //column_name
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
    ADD_COLUMN_SCHEMA("recv_task_cnt", //column_name
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
    ADD_COLUMN_SCHEMA("recv_large_req_cnt", //column_name
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
    ADD_COLUMN_SCHEMA("recv_large_queries", //column_name
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
    ADD_COLUMN_SCHEMA("actives", //column_name
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
    ADD_COLUMN_SCHEMA("workers", //column_name
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
    ADD_COLUMN_SCHEMA("lq_waiting_workers", //column_name
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
    ADD_COLUMN_SCHEMA("req_queue_total_size", //column_name
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
    ADD_COLUMN_SCHEMA("queue_0", //column_name
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
    ADD_COLUMN_SCHEMA("queue_1", //column_name
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
    ADD_COLUMN_SCHEMA("queue_2", //column_name
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
    ADD_COLUMN_SCHEMA("queue_3", //column_name
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
    ADD_COLUMN_SCHEMA("queue_4", //column_name
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
    ADD_COLUMN_SCHEMA("queue_5", //column_name
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
    ADD_COLUMN_SCHEMA("large_queued", //column_name
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

int ObInnerTableSchema::all_virtual_tenant_parameter_info_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_TENANT_PARAMETER_INFO_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(6);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_PARAMETER_INFO_TNAME))) {
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
    ADD_COLUMN_SCHEMA("zone", //column_name
      ++column_id, //column_id
      2, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      MAX_ZONE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_type", //column_name
      ++column_id, //column_id
      3, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      SERVER_TYPE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip", //column_name
      ++column_id, //column_id
      4, //rowkey_id
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
      5, //rowkey_id
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
    ADD_COLUMN_SCHEMA("name", //column_name
      ++column_id, //column_id
      6, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_NAME_LEN, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("data_type", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_TYPE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      true, //is_nullable
      false); //is_autoincrement
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
      false, //is_nullable
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
      OB_MAX_CONFIG_INFO_LEN, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("section", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_SECTION_LEN, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("scope", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_SCOPE_LEN, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("source", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_SOURCE_LEN, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("edit_level", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_CONFIG_EDIT_LEVEL_LEN, //column_length
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

int ObInnerTableSchema::all_virtual_audit_operation_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_AUDIT_OPERATION_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_AUDIT_OPERATION_TNAME))) {
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
    ADD_COLUMN_SCHEMA("operation_type", //column_name
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
    ADD_COLUMN_SCHEMA("operation_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_SYS_PARAM_NAME_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("audit_type_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_SYS_PARAM_NAME_LENGTH, //column_length
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

int ObInnerTableSchema::all_virtual_audit_action_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_AUDIT_ACTION_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_AUDIT_ACTION_TNAME))) {
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
    ADD_COLUMN_SCHEMA("action_id", //column_name
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
    ADD_COLUMN_SCHEMA("action_name", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_SYS_PARAM_NAME_LENGTH, //column_length
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

int ObInnerTableSchema::all_virtual_dag_warning_history_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  //generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  table_schema.set_table_id(OB_ALL_VIRTUAL_DAG_WARNING_HISTORY_TID);
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_DAG_WARNING_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("task_id", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MAX_TRACE_ID_BUFFER_SIZE, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("module", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_MODULE_NAME_LENGTH, //column_length
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
      OB_SYS_TASK_TYPE_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ret", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_RET_STR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("status", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_STATUS_STR_LENGTH, //column_length
      -1, //column_precision
      -1, //column_scale
      false, //is_nullable
      false); //is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("gmt_create", //column_name
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
    ADD_COLUMN_SCHEMA_TS("gmt_modified", //column_name
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
    ADD_COLUMN_SCHEMA("retry_cnt", //column_name
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
    ADD_COLUMN_SCHEMA("warning_info", //column_name
      ++column_id, //column_id
      0, //rowkey_id
      0, //index_id
      0, //part_key_pos
      ObVarcharType, //column_type
      CS_TYPE_INVALID, //column_collation_type
      OB_DAG_WARNING_INFO_LENGTH, //column_length
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


} // end namespace share
} // end namespace oceanbase
