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

namespace oceanbase {
using namespace share::schema;
using namespace common;
namespace share {

int ObInnerTableSchema::all_part_info_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_PART_INFO_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PART_INFO_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_type",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_num",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_space",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("new_part_num",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("new_part_space",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_type",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("def_sub_part_num",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObIntType,                         // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(int64_t),                   // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_expr",     // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,  // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_expr",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,   // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_interval",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,   // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("interval_start",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,    // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("new_part_interval",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,       // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("new_interval_start",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,        // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("def_sub_part_interval",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,           // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("def_sub_interval_start",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObVarcharType,                           // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,            // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        true,                                    // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("new_def_sub_part_interval",  // column_name
        ++column_id,                                // column_id
        0,                                          // rowkey_id
        0,                                          // index_id
        0,                                          // part_key_pos
        ObVarcharType,                              // column_type
        CS_TYPE_INVALID,                            // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,               // column_length
        -1,                                         // column_precision
        -1,                                         // column_scale
        true,                                       // is_nullable
        false);                                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("new_def_sub_interval_start",  // column_name
        ++column_id,                                 // column_id
        0,                                           // rowkey_id
        0,                                           // index_id
        0,                                           // part_key_pos
        ObVarcharType,                               // column_type
        CS_TYPE_INVALID,                             // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,                // column_length
        -1,                                          // column_precision
        -1,                                          // column_scale
        true,                                        // is_nullable
        false);                                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("block_size",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("replica_num",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("compress_func_name",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_MAX_COMPRESSOR_NAME_LENGTH,       // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare1",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare2",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare3",     // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_OLD_MAX_VARCHAR_LENGTH,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2(tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_part_info_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_PART_INFO_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PART_INFO_HISTORY_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        3,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_deleted",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_type",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_num",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_space",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("new_part_num",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("new_part_space",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_type",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("def_sub_part_num",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObIntType,                         // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(int64_t),                   // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_expr",     // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,  // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_expr",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,   // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_interval",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,   // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("interval_start",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,    // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("new_part_interval",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,       // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("new_interval_start",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,        // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("def_sub_part_interval",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,           // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("def_sub_interval_start",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObVarcharType,                           // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,            // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        true,                                    // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("new_def_sub_part_interval",  // column_name
        ++column_id,                                // column_id
        0,                                          // rowkey_id
        0,                                          // index_id
        0,                                          // part_key_pos
        ObVarcharType,                              // column_type
        CS_TYPE_INVALID,                            // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,               // column_length
        -1,                                         // column_precision
        -1,                                         // column_scale
        true,                                       // is_nullable
        false);                                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("new_def_sub_interval_start",  // column_name
        ++column_id,                                 // column_id
        0,                                           // rowkey_id
        0,                                           // index_id
        0,                                           // part_key_pos
        ObVarcharType,                               // column_type
        CS_TYPE_INVALID,                             // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,                // column_length
        -1,                                          // column_precision
        -1,                                          // column_scale
        true,                                        // is_nullable
        false);                                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("block_size",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("replica_num",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("compress_func_name",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_MAX_COMPRESSOR_NAME_LENGTH,       // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare1",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare2",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare3",     // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_OLD_MAX_VARCHAR_LENGTH,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2(tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_def_sub_part_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_DEF_SUB_PART_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DEF_SUB_PART_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_id",  // column_name
        ++column_id,                  // column_id
        3,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj sub_part_name_default;
    sub_part_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("sub_part_name",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_PARTITION_NAME_LENGTH,     // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        sub_part_name_default,
        sub_part_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("high_bound_val",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,    // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("b_high_bound_val",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_MAX_B_HIGH_BOUND_VAL_LENGTH,    // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("block_size",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("replica_num",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("compress_func_name",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_MAX_COMPRESSOR_NAME_LENGTH,       // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare1",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare2",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare3",     // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_OLD_MAX_VARCHAR_LENGTH,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment",          // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_PARTITION_COMMENT_LENGTH,  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("list_val",      // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,  // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("b_list_val",      // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_B_PARTITION_EXPR_LENGTH,  // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_idx",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj source_partition_id_default;
    source_partition_id_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("source_partition_id",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        MAX_VALUE_LENGTH,                       // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false,                                  // is_autoincrement
        source_partition_id_default,
        source_partition_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj mapping_pg_sub_part_id_default;
    mapping_pg_sub_part_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("mapping_pg_sub_part_id",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        true,                                      // is_nullable
        false,                                     // is_autoincrement
        mapping_pg_sub_part_id_default,
        mapping_pg_sub_part_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj tablespace_id_default;
    tablespace_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("tablespace_id",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        tablespace_id_default,
        tablespace_id_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2(tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_def_sub_part_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_DEF_SUB_PART_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DEF_SUB_PART_HISTORY_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_id",  // column_name
        ++column_id,                  // column_id
        3,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        4,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_deleted",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj sub_part_name_default;
    sub_part_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("sub_part_name",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_PARTITION_NAME_LENGTH,     // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false,                            // is_autoincrement
        sub_part_name_default,
        sub_part_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("high_bound_val",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,    // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("b_high_bound_val",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_MAX_B_HIGH_BOUND_VAL_LENGTH,    // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("block_size",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("replica_num",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("compress_func_name",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_MAX_COMPRESSOR_NAME_LENGTH,       // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare1",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare2",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("spare3",     // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_OLD_MAX_VARCHAR_LENGTH,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment",          // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_PARTITION_COMMENT_LENGTH,  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("list_val",      // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,  // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("b_list_val",      // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_B_PARTITION_EXPR_LENGTH,  // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_idx",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj source_partition_id_default;
    source_partition_id_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("source_partition_id",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        MAX_VALUE_LENGTH,                       // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false,                                  // is_autoincrement
        source_partition_id_default,
        source_partition_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj mapping_pg_sub_part_id_default;
    mapping_pg_sub_part_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("mapping_pg_sub_part_id",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        true,                                      // is_nullable
        false,                                     // is_autoincrement
        mapping_pg_sub_part_id_default,
        mapping_pg_sub_part_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj tablespace_id_default;
    tablespace_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("tablespace_id",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false,                            // is_autoincrement
        tablespace_id_default,
        tablespace_id_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2(tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_server_event_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SERVER_EVENT_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SERVER_EVENT_HISTORY_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_default;
    ObObj gmt_default_null;

    gmt_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        1,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        0,                                // column_length
        -1,                               // column_precision
        6,                                // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_default_null,
        gmt_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
        0,                       // index_id
        1,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        MAX_IP_ADDR_LENGTH,      // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_port",  // column_name
        ++column_id,               // column_id
        3,                         // rowkey_id
        0,                         // index_id
        2,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("module",             // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        MAX_ROOTSERVICE_EVENT_DESC_LENGTH,  // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("event",              // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        MAX_ROOTSERVICE_EVENT_DESC_LENGTH,  // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj name1_default;
    name1_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("name1",            // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        MAX_ROOTSERVICE_EVENT_NAME_LENGTH,  // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false,                              // is_autoincrement
        name1_default,
        name1_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj value1_default;
    value1_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("value1",            // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        MAX_ROOTSERVICE_EVENT_VALUE_LENGTH,  // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false,                               // is_autoincrement
        value1_default,
        value1_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj name2_default;
    name2_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("name2",            // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        MAX_ROOTSERVICE_EVENT_NAME_LENGTH,  // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false,                              // is_autoincrement
        name2_default,
        name2_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("value2",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObLongTextType,          // column_type
        CS_TYPE_INVALID,         // column_collation_type
        0,                       // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj name3_default;
    name3_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("name3",            // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        MAX_ROOTSERVICE_EVENT_NAME_LENGTH,  // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false,                              // is_autoincrement
        name3_default,
        name3_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj value3_default;
    value3_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("value3",            // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        MAX_ROOTSERVICE_EVENT_VALUE_LENGTH,  // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false,                               // is_autoincrement
        value3_default,
        value3_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj name4_default;
    name4_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("name4",            // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        MAX_ROOTSERVICE_EVENT_NAME_LENGTH,  // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false,                              // is_autoincrement
        name4_default,
        name4_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj value4_default;
    value4_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("value4",            // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        MAX_ROOTSERVICE_EVENT_VALUE_LENGTH,  // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false,                               // is_autoincrement
        value4_default,
        value4_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj name5_default;
    name5_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("name5",            // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        MAX_ROOTSERVICE_EVENT_NAME_LENGTH,  // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false,                              // is_autoincrement
        name5_default,
        name5_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj value5_default;
    value5_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("value5",            // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        MAX_ROOTSERVICE_EVENT_VALUE_LENGTH,  // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false,                               // is_autoincrement
        value5_default,
        value5_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj name6_default;
    name6_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("name6",            // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        MAX_ROOTSERVICE_EVENT_NAME_LENGTH,  // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false,                              // is_autoincrement
        name6_default,
        name6_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj value6_default;
    value6_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("value6",            // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        MAX_ROOTSERVICE_EVENT_VALUE_LENGTH,  // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false,                               // is_autoincrement
        value6_default,
        value6_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj extra_info_default;
    extra_info_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("extra_info",             // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObVarcharType,                            // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH,  // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        true,                                     // is_nullable
        false,                                    // is_autoincrement
        extra_info_default,
        extra_info_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2(svr_ip, svr_port)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_rootservice_job_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_ROOTSERVICE_JOB_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ROOTSERVICE_JOB_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("job_id",  // column_name
        ++column_id,             // column_id
        1,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("job_type",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        128,                       // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("job_status",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        128,                         // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("return_code",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj progress_default;
    progress_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("progress",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false,                       // is_autoincrement
        progress_default,
        progress_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_name",      // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_TENANT_NAME_LENGTH_STORE,  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_name",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,    // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_name",     // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_CORE_TALBE_NAME_LENGTH,  // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_id",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        MAX_IP_ADDR_LENGTH,      // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_port",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("unit_id",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObIntType,                // column_type
        CS_TYPE_INVALID,          // column_collation_type
        sizeof(int64_t),          // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        true,                     // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rs_svr_ip",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_IP_ADDR_LENGTH,         // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rs_svr_port",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sql_text",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObLongTextType,            // column_type
        CS_TYPE_INVALID,           // column_collation_type
        0,                         // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("extra_info",               // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObVarcharType,                            // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH,  // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        true,                                     // is_nullable
        false);                                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("resource_pool_id",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObIntType,                         // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(int64_t),                   // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tablegroup_id",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tablegroup_name",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_TABLEGROUP_NAME_LENGTH,    // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_unit_load_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_UNIT_LOAD_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_UNIT_LOAD_HISTORY_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_default;
    ObObj gmt_default_null;

    gmt_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        1,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        0,                                // column_length
        -1,                               // column_precision
        6,                                // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_default_null,
        gmt_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("zone",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObVarcharType,         // column_type
        CS_TYPE_INVALID,       // column_collation_type
        MAX_ZONE_LENGTH,       // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        MAX_IP_ADDR_LENGTH,      // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_port",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("unit_id",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObIntType,                // column_type
        CS_TYPE_INVALID,          // column_collation_type
        sizeof(int64_t),          // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("load",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObDoubleType,          // column_type
        CS_TYPE_INVALID,       // column_collation_type
        sizeof(double),        // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("disk_usage_rate",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObDoubleType,                     // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(double),                   // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("memory_usage_rate",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObDoubleType,                       // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(double),                     // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cpu_usage_rate",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObDoubleType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(double),                  // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("iops_usage_rate",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObDoubleType,                     // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(double),                   // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("disk_weight",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObDoubleType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(double),               // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("memory_weight",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObDoubleType,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(double),                 // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cpu_weight",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObDoubleType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(double),              // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("iops_weight",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObDoubleType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(double),               // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rs_svr_ip",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_IP_ADDR_LENGTH,         // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rs_svr_port",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_sys_variable_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SYS_VARIABLE_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SYS_VARIABLE_HISTORY_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("zone",  // column_name
        ++column_id,           // column_id
        2,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObVarcharType,         // column_type
        CS_TYPE_INVALID,       // column_collation_type
        MAX_ZONE_LENGTH,       // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj name_default;
    name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("name",  // column_name
        ++column_id,             // column_id
        3,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        OB_MAX_CONFIG_NAME_LEN,  // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false,                   // is_autoincrement
        name_default,
        name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        4,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_deleted",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("data_type",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("value",    // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_MAX_CONFIG_VALUE_LEN,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        true,                     // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("info",    // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        OB_MAX_CONFIG_INFO_LEN,  // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("flags",  // column_name
        ++column_id,            // column_id
        0,                      // rowkey_id
        0,                      // index_id
        0,                      // part_key_pos
        ObIntType,              // column_type
        CS_TYPE_INVALID,        // column_collation_type
        sizeof(int64_t),        // column_length
        -1,                     // column_precision
        -1,                     // column_scale
        false,                  // is_nullable
        false);                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj min_val_default;
    min_val_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("min_val",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_MAX_CONFIG_VALUE_LEN,    // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false,                      // is_autoincrement
        min_val_default,
        min_val_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj max_val_default;
    max_val_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("max_val",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_MAX_CONFIG_VALUE_LEN,    // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false,                      // is_autoincrement
        max_val_default,
        max_val_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_restore_job_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_RESTORE_JOB_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RESTORE_JOB_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("job_id",  // column_name
        ++column_id,             // column_id
        1,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_name",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_TENANT_NAME_LENGTH,    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("start_time",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_uri",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        2048,                        // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_end_time",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("recycle_end_time",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObIntType,                         // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(int64_t),                   // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("level",  // column_name
        ++column_id,            // column_id
        0,                      // rowkey_id
        0,                      // index_id
        0,                      // part_key_pos
        ObIntType,              // column_type
        CS_TYPE_INVALID,        // column_collation_type
        sizeof(int64_t),        // column_length
        -1,                     // column_precision
        -1,                     // column_scale
        false,                  // is_nullable
        false);                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("status",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_restore_task_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_RESTORE_TASK_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RESTORE_TASK_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_id",  // column_name
        ++column_id,                   // column_id
        3,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_table_id",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("index_map",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_OLD_MAX_VARCHAR_LENGTH,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("start_time",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("status",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("job_id",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_restore_job_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_RESTORE_JOB_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RESTORE_JOB_HISTORY_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("job_id",  // column_name
        ++column_id,             // column_id
        1,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_name",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_TENANT_NAME_LENGTH,    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("start_time",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_uri",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        2048,                        // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_end_time",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("recycle_end_time",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObIntType,                         // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(int64_t),                   // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("level",  // column_name
        ++column_id,            // column_id
        0,                      // rowkey_id
        0,                      // index_id
        0,                      // part_key_pos
        ObIntType,              // column_type
        CS_TYPE_INVALID,        // column_collation_type
        sizeof(int64_t),        // column_length
        -1,                     // column_precision
        -1,                     // column_scale
        false,                  // is_nullable
        false);                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("status",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_time_zone_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TIME_ZONE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TIME_ZONE_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj time_zone_id_default;
    time_zone_id_default.set_null();
    ADD_COLUMN_SCHEMA_T("Time_zone_id",  // column_name
        ++column_id,                     // column_id
        1,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        true,                            // is_autoincrement
        time_zone_id_default,
        time_zone_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj use_leap_seconds_default;
    use_leap_seconds_default.set_varchar(ObString::make_string("N"));
    ADD_COLUMN_SCHEMA_T("Use_leap_seconds",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        8,                                   // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false,                               // is_autoincrement
        use_leap_seconds_default,
        use_leap_seconds_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("Version",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObIntType,                // column_type
        CS_TYPE_INVALID,          // column_collation_type
        sizeof(int64_t),          // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        true,                     // is_nullable
        false);                   // is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_time_zone_name_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TIME_ZONE_NAME_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TIME_ZONE_NAME_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("Name",  // column_name
        ++column_id,           // column_id
        1,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObVarcharType,         // column_type
        CS_TYPE_INVALID,       // column_collation_type
        64,                    // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("Time_zone_id",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("Version",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObIntType,                // column_type
        CS_TYPE_INVALID,          // column_collation_type
        sizeof(int64_t),          // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        true,                     // is_nullable
        false);                   // is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_time_zone_transition_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TIME_ZONE_TRANSITION_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TIME_ZONE_TRANSITION_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("Time_zone_id",  // column_name
        ++column_id,                   // column_id
        1,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("Transition_time",  // column_name
        ++column_id,                      // column_id
        2,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("Transition_type_id",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObIntType,                           // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        sizeof(int64_t),                     // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("Version",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObIntType,                // column_type
        CS_TYPE_INVALID,          // column_collation_type
        sizeof(int64_t),          // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        true,                     // is_nullable
        false);                   // is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_time_zone_transition_type_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TIME_ZONE_TRANSITION_TYPE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TIME_ZONE_TRANSITION_TYPE_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("Time_zone_id",  // column_name
        ++column_id,                   // column_id
        1,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("Transition_type_id",  // column_name
        ++column_id,                         // column_id
        2,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObIntType,                           // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        sizeof(int64_t),                     // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj offset_default;
    offset_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("Offset",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false,                     // is_autoincrement
        offset_default,
        offset_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj is_dst_default;
    is_dst_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("Is_DST",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false,                     // is_autoincrement
        is_dst_default,
        is_dst_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj abbreviation_default;
    abbreviation_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("Abbreviation",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        8,                               // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        abbreviation_default,
        abbreviation_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("Version",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObIntType,                // column_type
        CS_TYPE_INVALID,          // column_collation_type
        sizeof(int64_t),          // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        true,                     // is_nullable
        false);                   // is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_ddl_id_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_DDL_ID_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DDL_ID_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ddl_id_str",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        OB_MAX_DDL_ID_STR_LENGTH,    // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ddl_stmt_str",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObLongTextType,                // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_foreign_key_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_FOREIGN_KEY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_FOREIGN_KEY_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("foreign_key_id",  // column_name
        ++column_id,                     // column_id
        2,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj foreign_key_name_default;
    foreign_key_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("foreign_key_name",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_MAX_CONSTRAINT_NAME_LENGTH,       // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false,                               // is_autoincrement
        foreign_key_name_default,
        foreign_key_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("child_table_id",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("parent_table_id",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("update_action",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("delete_action",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj enable_flag_default;
    enable_flag_default.set_tinyint(true);
    ADD_COLUMN_SCHEMA_T("enable_flag",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObTinyIntType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        1,                              // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        enable_flag_default,
        enable_flag_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj ref_cst_type_default;
    ref_cst_type_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("ref_cst_type",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        ref_cst_type_default,
        ref_cst_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj ref_cst_id_default;
    ref_cst_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("ref_cst_id",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false,                         // is_autoincrement
        ref_cst_id_default,
        ref_cst_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj validate_flag_default;
    validate_flag_default.set_tinyint(true);
    ADD_COLUMN_SCHEMA_T("validate_flag",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTinyIntType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        1,                                // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        validate_flag_default,
        validate_flag_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj rely_flag_default;
    rely_flag_default.set_tinyint(false);
    ADD_COLUMN_SCHEMA_T("rely_flag",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObTinyIntType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        1,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        rely_flag_default,
        rely_flag_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_foreign_key_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_FOREIGN_KEY_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_FOREIGN_KEY_HISTORY_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("foreign_key_id",  // column_name
        ++column_id,                     // column_id
        2,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        3,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_deleted",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj foreign_key_name_default;
    foreign_key_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("foreign_key_name",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_MAX_CONSTRAINT_NAME_LENGTH,       // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false,                               // is_autoincrement
        foreign_key_name_default,
        foreign_key_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("child_table_id",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("parent_table_id",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("update_action",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("delete_action",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj enable_flag_default;
    enable_flag_default.set_tinyint(true);
    ADD_COLUMN_SCHEMA_T("enable_flag",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObTinyIntType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        1,                              // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false,                          // is_autoincrement
        enable_flag_default,
        enable_flag_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj ref_cst_type_default;
    ref_cst_type_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("ref_cst_type",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false,                           // is_autoincrement
        ref_cst_type_default,
        ref_cst_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj ref_cst_id_default;
    ref_cst_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("ref_cst_id",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false,                         // is_autoincrement
        ref_cst_id_default,
        ref_cst_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj validate_flag_default;
    validate_flag_default.set_tinyint(true);
    ADD_COLUMN_SCHEMA_T("validate_flag",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTinyIntType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        1,                                // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false,                            // is_autoincrement
        validate_flag_default,
        validate_flag_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj rely_flag_default;
    rely_flag_default.set_tinyint(false);
    ADD_COLUMN_SCHEMA_T("rely_flag",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObTinyIntType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        1,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false,                        // is_autoincrement
        rely_flag_default,
        rely_flag_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_foreign_key_column_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_FOREIGN_KEY_COLUMN_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_FOREIGN_KEY_COLUMN_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("foreign_key_id",  // column_name
        ++column_id,                     // column_id
        2,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("child_column_id",  // column_name
        ++column_id,                      // column_id
        3,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("parent_column_id",  // column_name
        ++column_id,                       // column_id
        4,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObIntType,                         // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(int64_t),                   // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj position_default;
    position_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("position",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false,                       // is_autoincrement
        position_default,
        position_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_foreign_key_column_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_FOREIGN_KEY_COLUMN_HISTORY_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("foreign_key_id",  // column_name
        ++column_id,                     // column_id
        2,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("child_column_id",  // column_name
        ++column_id,                      // column_id
        3,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("parent_column_id",  // column_name
        ++column_id,                       // column_id
        4,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObIntType,                         // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(int64_t),                   // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        5,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_deleted",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj position_default;
    position_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("position",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false,                       // is_autoincrement
        position_default,
        position_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_synonym_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SYNONYM_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SYNONYM_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("synonym_id",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj synonym_name_default;
    synonym_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("synonym_name",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_SYNONYM_NAME_LENGTH,      // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        synonym_name_default,
        synonym_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj object_name_default;
    object_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("object_name",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_SYNONYM_NAME_LENGTH,     // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        object_name_default,
        object_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("object_database_id",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObIntType,                           // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        sizeof(int64_t),                     // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_synonym_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SYNONYM_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SYNONYM_HISTORY_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("synonym_id",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        3,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_deleted",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj synonym_name_default;
    synonym_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("synonym_name",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_SYNONYM_NAME_LENGTH,      // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false,                           // is_autoincrement
        synonym_name_default,
        synonym_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj object_name_default;
    object_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("object_name",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_SYNONYM_NAME_LENGTH,     // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false,                          // is_autoincrement
        object_name_default,
        object_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("object_database_id",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObIntType,                           // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        sizeof(int64_t),                     // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_sequence_v2_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SEQUENCE_V2_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SEQUENCE_V2_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sequence_key",  // column_name
        ++column_id,                   // column_id
        1,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("column_id",  // column_name
        ++column_id,                // column_id
        2,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sequence_name",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_SEQUENCE_NAME_LENGTH,    // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sequence_value",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObUInt64Type,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(uint64_t),                // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sync_value",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObUInt64Type,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(uint64_t),            // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_tenant_meta_table_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_META_TABLE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_META_TABLE_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_id",  // column_name
        ++column_id,                   // column_id
        3,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        4,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        MAX_IP_ADDR_LENGTH,      // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_port",  // column_name
        ++column_id,               // column_id
        5,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sql_port",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("unit_id",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObIntType,                // column_type
        CS_TYPE_INVALID,          // column_collation_type
        sizeof(int64_t),          // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_cnt",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("zone",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObVarcharType,         // column_type
        CS_TYPE_INVALID,       // column_collation_type
        MAX_ZONE_LENGTH,       // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("role",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObIntType,             // column_type
        CS_TYPE_INVALID,       // column_collation_type
        sizeof(int64_t),       // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("member_list",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        MAX_MEMBER_LIST_LENGTH,       // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("row_count",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("data_size",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("data_version",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("data_checksum",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("row_checksum",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("column_checksum",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        COLUMN_CHECKSUM_LENGTH,           // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj is_original_leader_default;
    is_original_leader_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("is_original_leader",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObIntType,                             // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        sizeof(int64_t),                       // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false,                                 // is_autoincrement
        is_original_leader_default,
        is_original_leader_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj is_previous_leader_default;
    is_previous_leader_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("is_previous_leader",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObIntType,                             // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        sizeof(int64_t),                       // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false,                                 // is_autoincrement
        is_previous_leader_default,
        is_previous_leader_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_time",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj rebuild_default;
    rebuild_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("rebuild",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false,                      // is_autoincrement
        rebuild_default,
        rebuild_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj replica_type_default;
    replica_type_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("replica_type",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        replica_type_default,
        replica_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj required_size_default;
    required_size_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("required_size",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        required_size_default,
        required_size_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj status_default;
    status_default.set_varchar(ObString::make_string("REPLICA_STATUS_NORMAL"));
    ADD_COLUMN_SCHEMA_T("status",   // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_REPLICA_STATUS_LENGTH,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false,                      // is_autoincrement
        status_default,
        status_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj is_restore_default;
    is_restore_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("is_restore",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false,                         // is_autoincrement
        is_restore_default,
        is_restore_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj partition_checksum_default;
    partition_checksum_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("partition_checksum",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObIntType,                             // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        sizeof(int64_t),                       // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false,                                 // is_autoincrement
        partition_checksum_default,
        partition_checksum_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj quorum_default;
    quorum_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("quorum",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false,                     // is_autoincrement
        quorum_default,
        quorum_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj fail_list_default;
    fail_list_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("fail_list",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_FAILLIST_LENGTH,       // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        fail_list_default,
        fail_list_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj recovery_timestamp_default;
    recovery_timestamp_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("recovery_timestamp",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObIntType,                             // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        sizeof(int64_t),                       // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false,                                 // is_autoincrement
        recovery_timestamp_default,
        recovery_timestamp_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj memstore_percent_default;
    memstore_percent_default.set_int(100);
    ADD_COLUMN_SCHEMA_T("memstore_percent",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObIntType,                           // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        sizeof(int64_t),                     // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false,                               // is_autoincrement
        memstore_percent_default,
        memstore_percent_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj data_file_id_default;
    data_file_id_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("data_file_id",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        data_file_id_default,
        data_file_id_default);  // default_value
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_index_wait_transaction_status_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_INDEX_WAIT_TRANSACTION_STATUS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_INDEX_WAIT_TRANSACTION_STATUS_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("index_table_id",  // column_name
        ++column_id,                     // column_id
        2,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_type",  // column_name
        ++column_id,               // column_id
        3,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_id",  // column_name
        ++column_id,                   // column_id
        4,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        MAX_IP_ADDR_LENGTH,      // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_port",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trans_status",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("snapshot_version",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObIntType,                         // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(int64_t),                   // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("frozen_version",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_index_schedule_task_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_INDEX_SCHEDULE_TASK_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_INDEX_SCHEDULE_TASK_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("index_table_id",  // column_name
        ++column_id,                     // column_id
        2,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_id",  // column_name
        ++column_id,                   // column_id
        3,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        MAX_IP_ADDR_LENGTH,      // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_port",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("frozen_version",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("snapshot_version",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObIntType,                         // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(int64_t),                   // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_index_checksum_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_INDEX_CHECKSUM_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(6);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_INDEX_CHECKSUM_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("execution_id",  // column_name
        ++column_id,                   // column_id
        1,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        2,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id",  // column_name
        ++column_id,               // column_id
        3,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_id",  // column_name
        ++column_id,                   // column_id
        4,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("column_id",  // column_name
        ++column_id,                // column_id
        5,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("task_id",  // column_name
        ++column_id,              // column_id
        6,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObIntType,                // column_type
        CS_TYPE_INVALID,          // column_collation_type
        sizeof(int64_t),          // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("checksum",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj checksum_method_default;
    checksum_method_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("checksum_method",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObIntType,                          // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(int64_t),                    // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false,                              // is_autoincrement
        checksum_method_default,
        checksum_method_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_routine_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_ROUTINE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ROUTINE_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_id",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_id",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_ROUTINE_NAME_LENGTH,    // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("overload",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("subprogram_id",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_type",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("flag",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObIntType,             // column_type
        CS_TYPE_INVALID,       // column_collation_type
        sizeof(int64_t),       // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("owner_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("priv_user",      // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_USER_NAME_LENGTH_STORE,  // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comp_flag",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("exec_env",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        OB_MAX_PROC_ENV_LENGTH,    // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_body",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObLongTextType,                // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment",    // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_TENANT_COMMENT_LENGTH,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("route_sql",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObLongTextType,             // column_type
        CS_TYPE_INVALID,            // column_collation_type
        0,                          // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_routine_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_ROUTINE_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ROUTINE_HISTORY_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_id",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        3,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_deleted",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_id",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_ROUTINE_NAME_LENGTH,    // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("overload",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("subprogram_id",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_type",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("flag",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObIntType,             // column_type
        CS_TYPE_INVALID,       // column_collation_type
        sizeof(int64_t),       // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        true,                  // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("owner_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("priv_user",      // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_USER_NAME_LENGTH_STORE,  // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comp_flag",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("exec_env",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        OB_MAX_PROC_ENV_LENGTH,    // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_body",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObLongTextType,                // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment",    // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_TENANT_COMMENT_LENGTH,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("route_sql",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObLongTextType,             // column_type
        CS_TYPE_INVALID,            // column_collation_type
        0,                          // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_routine_param_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_ROUTINE_PARAM_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ROUTINE_PARAM_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_id",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sequence",  // column_name
        ++column_id,               // column_id
        3,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("subprogram_id",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_position",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_level",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj param_name_default;
    param_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("param_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_COLUMN_NAME_LENGTH,     // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false,                         // is_autoincrement
        param_name_default,
        param_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_type",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_length",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_precision",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_scale",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_zero_fill",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_charset",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_coll_type",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("flag",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObIntType,             // column_type
        CS_TYPE_INVALID,       // column_collation_type
        sizeof(int64_t),       // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("default_value",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_DEFAULT_VALUE_LENGTH,    // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type_owner",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj type_name_default;
    type_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("type_name",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_COLUMN_NAME_LENGTH,    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false,                        // is_autoincrement
        type_name_default,
        type_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj type_subname_default;
    type_subname_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("type_subname",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_COLUMN_NAME_LENGTH,       // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false,                           // is_autoincrement
        type_subname_default,
        type_subname_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj extended_type_info_default;
    extended_type_info_default.set_varbinary(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("extended_type_info",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObVarcharType,                         // column_type
        CS_TYPE_BINARY,                        // column_collation_type
        OB_MAX_VARBINARY_LENGTH,               // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        true,                                  // is_nullable
        false,                                 // is_autoincrement
        extended_type_info_default,
        extended_type_info_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_routine_param_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_ROUTINE_PARAM_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ROUTINE_PARAM_HISTORY_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("routine_id",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sequence",  // column_name
        ++column_id,               // column_id
        3,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        4,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_deleted",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("subprogram_id",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_position",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_level",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj param_name_default;
    param_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("param_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_COLUMN_NAME_LENGTH,     // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false,                         // is_autoincrement
        param_name_default,
        param_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_type",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_length",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_precision",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_scale",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_zero_fill",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_charset",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("param_coll_type",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("flag",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObIntType,             // column_type
        CS_TYPE_INVALID,       // column_collation_type
        sizeof(int64_t),       // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        true,                  // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("default_value",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_DEFAULT_VALUE_LENGTH,    // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type_owner",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj type_name_default;
    type_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("type_name",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_COLUMN_NAME_LENGTH,    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false,                        // is_autoincrement
        type_name_default,
        type_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj type_subname_default;
    type_subname_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("type_subname",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_COLUMN_NAME_LENGTH,       // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false,                           // is_autoincrement
        type_subname_default,
        type_subname_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj extended_type_info_default;
    extended_type_info_default.set_varbinary(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("extended_type_info",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObVarcharType,                         // column_type
        CS_TYPE_BINARY,                        // column_collation_type
        OB_MAX_VARBINARY_LENGTH,               // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        true,                                  // is_nullable
        false,                                 // is_autoincrement
        extended_type_info_default,
        extended_type_info_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_table_stat_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TABLE_STAT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLE_STAT_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        1,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_id",  // column_name
        ++column_id,                   // column_id
        3,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("object_type",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("last_analyzed",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObTimestampType,                   // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(ObPreciseDateTime),         // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false,                             // is_autoincrement
        false);                            // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sstable_row_cnt",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObIntType,                        // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(int64_t),                  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sstable_avg_row_len",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObIntType,                            // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        sizeof(int64_t),                      // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("macro_blk_cnt",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("micro_blk_cnt",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("memtable_row_cnt",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObIntType,                         // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(int64_t),                   // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("memtable_avg_row_len",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObIntType,                             // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        sizeof(int64_t),                       // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (table_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_column_stat_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_COLUMN_STAT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_COLUMN_STAT_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        1,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_id",  // column_name
        ++column_id,                   // column_id
        3,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("column_id",  // column_name
        ++column_id,                // column_id
        4,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("object_type",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("last_analyzed",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObTimestampType,                   // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(ObPreciseDateTime),         // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false,                             // is_autoincrement
        false);                            // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("distinct_cnt",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("null_cnt",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("max_value",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_VALUE_LENGTH,           // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("b_max_value",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        MAX_VALUE_LENGTH,             // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("min_value",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_VALUE_LENGTH,           // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("b_min_value",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        MAX_VALUE_LENGTH,             // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("avg_len",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObIntType,                // column_type
        CS_TYPE_INVALID,          // column_collation_type
        sizeof(int64_t),          // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("distinct_cnt_synopsis",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        MAX_LLC_BITMAP_LENGTH,                  // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("distinct_cnt_synopsis_size",  // column_name
        ++column_id,                                 // column_id
        0,                                           // rowkey_id
        0,                                           // index_id
        0,                                           // part_key_pos
        ObIntType,                                   // column_type
        CS_TYPE_INVALID,                             // column_collation_type
        sizeof(int64_t),                             // column_length
        -1,                                          // column_precision
        -1,                                          // column_scale
        false,                                       // is_nullable
        false);                                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sample_size",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObDoubleType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(double),               // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("density",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObDoubleType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        sizeof(double),           // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("bucket_cnt",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("histogram_type",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (table_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_histogram_stat_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_HISTOGRAM_STAT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_HISTOGRAM_STAT_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_id",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        1,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_id",  // column_name
        ++column_id,                   // column_id
        3,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("column_id",  // column_name
        ++column_id,                // column_id
        4,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("endpoint_num",  // column_name
        ++column_id,                   // column_id
        5,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("object_type",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("endpoint_value",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        MAX_VALUE_LENGTH,                // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("b_endpoint_value",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        MAX_VALUE_LENGTH,                  // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("endpoint_repeat_cnt",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObIntType,                            // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        sizeof(int64_t),                      // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (table_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_package_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_PACKAGE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PACKAGE_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_id",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj package_name_default;
    package_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("package_name",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_PACKAGE_NAME_LENGTH,      // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        package_name_default,
        package_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObIntType,             // column_type
        CS_TYPE_INVALID,       // column_collation_type
        sizeof(int64_t),       // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("flag",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObIntType,             // column_type
        CS_TYPE_INVALID,       // column_collation_type
        sizeof(int64_t),       // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("owner_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comp_flag",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("exec_env",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        OB_MAX_PROC_ENV_LENGTH,    // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("source",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObLongTextType,          // column_type
        CS_TYPE_INVALID,         // column_collation_type
        0,                       // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment",    // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_TENANT_COMMENT_LENGTH,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("route_sql",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObLongTextType,             // column_type
        CS_TYPE_INVALID,            // column_collation_type
        0,                          // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_package_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_PACKAGE_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PACKAGE_HISTORY_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_id",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        3,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_deleted",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("database_id",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj package_name_default;
    package_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("package_name",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_PACKAGE_NAME_LENGTH,      // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false,                           // is_autoincrement
        package_name_default,
        package_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObIntType,             // column_type
        CS_TYPE_INVALID,       // column_collation_type
        sizeof(int64_t),       // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        true,                  // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("flag",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObIntType,             // column_type
        CS_TYPE_INVALID,       // column_collation_type
        sizeof(int64_t),       // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        true,                  // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("owner_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comp_flag",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("exec_env",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        OB_MAX_PROC_ENV_LENGTH,    // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("source",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObLongTextType,          // column_type
        CS_TYPE_INVALID,         // column_collation_type
        0,                       // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment",    // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_TENANT_COMMENT_LENGTH,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("route_sql",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObLongTextType,             // column_type
        CS_TYPE_INVALID,            // column_collation_type
        0,                          // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key_v2 (tenant_id)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(16);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_sql_execute_task_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SQL_EXECUTE_TASK_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(6);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SQL_EXECUTE_TASK_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("job_id",  // column_name
        ++column_id,             // column_id
        1,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("execution_id",  // column_name
        ++column_id,                   // column_id
        2,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sql_job_id",  // column_name
        ++column_id,                 // column_id
        3,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("task_id",  // column_name
        ++column_id,              // column_id
        4,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObIntType,                // column_type
        CS_TYPE_INVALID,          // column_collation_type
        sizeof(int64_t),          // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        5,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        MAX_IP_ADDR_LENGTH,      // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_port",  // column_name
        ++column_id,               // column_id
        6,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("slice_count",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("task_stat",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        64,                         // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("task_result",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("task_info",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_VALUE_LENGTH,           // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

int ObInnerTableSchema::all_index_build_stat_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_INDEX_BUILD_STAT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_INDEX_BUILD_STAT_TNAME))) {
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
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ObObj gmt_create_default;
    ObObj gmt_create_default_null;

    gmt_create_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_create_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_create",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_BINARY,                   // collation_type
        0,                                // column length
        -1,                               // column_precision
        6,                                // column_scale
        true,                             // is nullable
        false,                            // is_autoincrement
        false,                            // is_on_update_for_timestamp
        gmt_create_default_null,
        gmt_create_default)
  }

  if (OB_SUCC(ret)) {
    ObObj gmt_modified_default;
    ObObj gmt_modified_default_null;

    gmt_modified_default.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
    gmt_modified_default_null.set_null();
    ADD_COLUMN_SCHEMA_TS_T("gmt_modified",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_BINARY,                     // collation_type
        0,                                  // column length
        -1,                                 // column_precision
        6,                                  // column_scale
        true,                               // is nullable
        false,                              // is_autoincrement
        true,                               // is_on_update_for_timestamp
        gmt_modified_default_null,
        gmt_modified_default)
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("data_table_id",  // column_name
        ++column_id,                    // column_id
        2,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("index_table_id",  // column_name
        ++column_id,                     // column_id
        3,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("status",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("snapshot",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  table_schema.set_max_used_column_id(column_id);
  table_schema.get_part_option().set_max_used_part_id(table_schema.get_part_option().get_part_num() - 1);
  table_schema.get_part_option().set_partition_cnt_within_partition_table(
      OB_ALL_CORE_TABLE_TID == common::extract_pure_id(table_schema.get_table_id()) ? 1 : 0);
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
