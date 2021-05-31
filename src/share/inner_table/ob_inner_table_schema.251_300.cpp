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

int ObInnerTableSchema::all_tenant_profile_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_PROFILE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_PROFILE_TNAME))) {
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
    ADD_COLUMN_SCHEMA("profile_id",  // column_name
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
    ADD_COLUMN_SCHEMA("profile_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_SQL_LENGTH,             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("failed_login_attempts",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("password_lock_time",  // column_name
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
    ADD_COLUMN_SCHEMA("password_verify_function",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObVarcharType,                             // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        MAX_ORACLE_NAME_LENGTH,                    // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        true,                                      // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj password_life_time_default;
    password_life_time_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("password_life_time",  // column_name
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
        password_life_time_default,
        password_life_time_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj password_grace_time_default;
    password_grace_time_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("password_grace_time",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false,                                  // is_autoincrement
        password_grace_time_default,
        password_grace_time_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj password_reuse_time_default;
    password_reuse_time_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("password_reuse_time",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false,                                  // is_autoincrement
        password_reuse_time_default,
        password_reuse_time_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj password_reuse_max_default;
    password_reuse_max_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("password_reuse_max",  // column_name
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
        password_reuse_max_default,
        password_reuse_max_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj inactive_account_time_default;
    inactive_account_time_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("inactive_account_time",  // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObIntType,                                // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        sizeof(int64_t),                          // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        false,                                    // is_nullable
        false,                                    // is_autoincrement
        inactive_account_time_default,
        inactive_account_time_default);  // default_value
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

int ObInnerTableSchema::all_tenant_profile_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_PROFILE_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_PROFILE_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("profile_id",  // column_name
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
    ADD_COLUMN_SCHEMA("profile_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_SQL_LENGTH,             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("failed_login_attempts",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("password_lock_time",  // column_name
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
    ADD_COLUMN_SCHEMA("password_verify_function",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObVarcharType,                             // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        MAX_ORACLE_NAME_LENGTH,                    // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        true,                                      // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj password_life_time_default;
    password_life_time_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("password_life_time",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObIntType,                             // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        sizeof(int64_t),                       // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        true,                                  // is_nullable
        false,                                 // is_autoincrement
        password_life_time_default,
        password_life_time_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj password_grace_time_default;
    password_grace_time_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("password_grace_time",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false,                                  // is_autoincrement
        password_grace_time_default,
        password_grace_time_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj password_reuse_time_default;
    password_reuse_time_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("password_reuse_time",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false,                                  // is_autoincrement
        password_reuse_time_default,
        password_reuse_time_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj password_reuse_max_default;
    password_reuse_max_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("password_reuse_max",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObIntType,                             // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        sizeof(int64_t),                       // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        true,                                  // is_nullable
        false,                                 // is_autoincrement
        password_reuse_max_default,
        password_reuse_max_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj inactive_account_time_default;
    inactive_account_time_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("inactive_account_time",  // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObIntType,                                // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        sizeof(int64_t),                          // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        true,                                     // is_nullable
        false,                                    // is_autoincrement
        inactive_account_time_default,
        inactive_account_time_default);  // default_value
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

int ObInnerTableSchema::all_tenant_security_audit_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_SECURITY_AUDIT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_SECURITY_AUDIT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("audit_id",  // column_name
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
    ADD_COLUMN_SCHEMA("audit_type",  // column_name
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("owner_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObUInt64Type,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(uint64_t),          // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("operation_type",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObUInt64Type,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(uint64_t),                // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("in_success",  // column_name
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("in_failure",  // column_name
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

int ObInnerTableSchema::all_tenant_security_audit_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_SECURITY_AUDIT_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("audit_id",  // column_name
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
    ADD_COLUMN_SCHEMA("audit_type",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObUInt64Type,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(uint64_t),            // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("owner_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObUInt64Type,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(uint64_t),          // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("operation_type",  // column_name
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
    ADD_COLUMN_SCHEMA("in_success",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObUInt64Type,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(uint64_t),            // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("in_failure",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObUInt64Type,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(uint64_t),            // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
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

int ObInnerTableSchema::all_tenant_trigger_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_TRIGGER_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_TRIGGER_TNAME))) {
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
    ADD_COLUMN_SCHEMA("trigger_id",  // column_name
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
    ADD_COLUMN_SCHEMA("trigger_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_TRIGGER_NAME_LENGTH,    // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
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
    ADD_COLUMN_SCHEMA("trigger_type",  // column_name
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
    ADD_COLUMN_SCHEMA("trigger_events",  // column_name
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
    ADD_COLUMN_SCHEMA("timing_points",  // column_name
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
    ADD_COLUMN_SCHEMA("base_object_type",  // column_name
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
    ADD_COLUMN_SCHEMA("base_object_id",  // column_name
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
    ADD_COLUMN_SCHEMA("trigger_flags",  // column_name
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
    ADD_COLUMN_SCHEMA("update_columns",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_UPDATE_COLUMNS_LENGTH,    // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ref_old_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_TRIGGER_NAME_LENGTH,    // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ref_new_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_TRIGGER_NAME_LENGTH,    // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ref_parent_name",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_TRIGGER_NAME_LENGTH,       // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("when_condition",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_WHEN_CONDITION_LENGTH,    // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trigger_body",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_TRIGGER_BODY_LENGTH,    // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_spec_source",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        OB_MAX_TRIGGER_BODY_LENGTH,           // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_body_source",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        OB_MAX_TRIGGER_BODY_LENGTH,           // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_flag",  // column_name
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
    ADD_COLUMN_SCHEMA("package_comp_flag",  // column_name
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
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_exec_env",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_MAX_PROC_ENV_LENGTH,            // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sql_mode",  // column_name
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

int ObInnerTableSchema::all_tenant_trigger_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_TRIGGER_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_TRIGGER_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("trigger_id",  // column_name
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
    ADD_COLUMN_SCHEMA("trigger_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_TRIGGER_NAME_LENGTH,    // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
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
    ADD_COLUMN_SCHEMA("trigger_type",  // column_name
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
    ADD_COLUMN_SCHEMA("trigger_events",  // column_name
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
    ADD_COLUMN_SCHEMA("timing_points",  // column_name
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
    ADD_COLUMN_SCHEMA("base_object_type",  // column_name
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
    ADD_COLUMN_SCHEMA("base_object_id",  // column_name
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
    ADD_COLUMN_SCHEMA("trigger_flags",  // column_name
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
    ADD_COLUMN_SCHEMA("update_columns",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_UPDATE_COLUMNS_LENGTH,    // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ref_old_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_TRIGGER_NAME_LENGTH,    // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ref_new_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_TRIGGER_NAME_LENGTH,    // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ref_parent_name",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_TRIGGER_NAME_LENGTH,       // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("when_condition",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_WHEN_CONDITION_LENGTH,    // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trigger_body",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_TRIGGER_BODY_LENGTH,    // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_spec_source",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        OB_MAX_TRIGGER_BODY_LENGTH,           // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        true,                                 // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_body_source",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        OB_MAX_TRIGGER_BODY_LENGTH,           // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        true,                                 // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_flag",  // column_name
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
    ADD_COLUMN_SCHEMA("package_comp_flag",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObIntType,                          // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(int64_t),                    // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("package_exec_env",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_MAX_PROC_ENV_LENGTH,            // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sql_mode",  // column_name
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

int ObInnerTableSchema::all_seed_parameter_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SEED_PARAMETER_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SEED_PARAMETER_TNAME))) {
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
    ADD_COLUMN_SCHEMA("zone",  // column_name
        ++column_id,           // column_id
        1,                     // rowkey_id
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
    ADD_COLUMN_SCHEMA("svr_type",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        SERVER_TYPE_LENGTH,        // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        3,                       // rowkey_id
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
        4,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("name",    // column_name
        ++column_id,             // column_id
        5,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        OB_MAX_CONFIG_NAME_LEN,  // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("data_type",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_MAX_CONFIG_TYPE_LENGTH,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
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
        false,                    // is_nullable
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
    ADD_COLUMN_SCHEMA("section",    // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_MAX_CONFIG_SECTION_LEN,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("scope",    // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_MAX_CONFIG_SCOPE_LEN,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        true,                     // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("source",    // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        OB_MAX_CONFIG_SOURCE_LEN,  // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("edit_level",    // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_CONFIG_EDIT_LEVEL_LEN,  // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("config_version",  // column_name
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

int ObInnerTableSchema::all_tenant_sstable_column_checksum_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_SSTABLE_COLUMN_CHECKSUM_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(8);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_SSTABLE_COLUMN_CHECKSUM_TNAME))) {
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
    ADD_COLUMN_SCHEMA("index_id",  // column_name
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
    ADD_COLUMN_SCHEMA("sstable_type",  // column_name
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
    ADD_COLUMN_SCHEMA("column_id",  // column_name
        ++column_id,                // column_id
        6,                          // rowkey_id
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
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        7,                       // rowkey_id
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
        8,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("column_checksum",  // column_name
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
    ADD_COLUMN_SCHEMA("checksum_method",  // column_name
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
    ADD_COLUMN_SCHEMA("replica_type",  // column_name
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
    ADD_COLUMN_SCHEMA("major_version",  // column_name
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

int ObInnerTableSchema::all_tenant_security_audit_record_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_SECURITY_AUDIT_RECORD_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_SECURITY_AUDIT_RECORD_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA_TS("record_timestamp_us",  // column_name
        ++column_id,                             // column_id
        4,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObTimestampType,                         // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(ObPreciseDateTime),               // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false,                                   // is_autoincrement
        false);                                  // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("user_id",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObUInt64Type,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        sizeof(uint64_t),         // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("user_name",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_MAX_USER_NAME_LENGTH,    // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("effective_user_id",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObUInt64Type,                       // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(uint64_t),                   // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("effective_user_name",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        OB_MAX_USER_NAME_LENGTH,              // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("client_ip",  // column_name
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
    ADD_COLUMN_SCHEMA("user_client_ip",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        MAX_IP_ADDR_LENGTH,              // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("proxy_session_id",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObUInt64Type,                      // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(uint64_t),                  // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("session_id",  // column_name
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("entry_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObUInt64Type,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(uint64_t),          // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("statement_id",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObUInt64Type,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(uint64_t),              // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trans_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        512,                       // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("commit_version",  // column_name
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
    ADD_COLUMN_SCHEMA("trace_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        64,                        // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("db_id",  // column_name
        ++column_id,            // column_id
        0,                      // rowkey_id
        0,                      // index_id
        0,                      // part_key_pos
        ObUInt64Type,           // column_type
        CS_TYPE_INVALID,        // column_collation_type
        sizeof(uint64_t),       // column_length
        -1,                     // column_precision
        -1,                     // column_scale
        false,                  // is_nullable
        false);                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cur_db_id",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObUInt64Type,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(uint64_t),           // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("sql_timestamp_us",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObTimestampType,                      // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        sizeof(ObPreciseDateTime),            // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false,                                // is_autoincrement
        false);                               // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("audit_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObUInt64Type,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(uint64_t),          // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("audit_type",  // column_name
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("operation_type",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObUInt64Type,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(uint64_t),                // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("action_id",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObUInt64Type,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(uint64_t),           // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
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
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("obj_owner_name",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_USER_NAME_LENGTH,         // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("obj_name",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,  // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("new_obj_owner_name",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_MAX_USER_NAME_LENGTH,             // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("new_obj_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,      // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("auth_privileges",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,         // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("auth_grantee",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,      // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("logoff_logical_read",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObUInt64Type,                         // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        sizeof(uint64_t),                     // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("logoff_physical_read",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObUInt64Type,                          // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        sizeof(uint64_t),                      // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("logoff_logical_write",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObUInt64Type,                          // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        sizeof(uint64_t),                      // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("logoff_lock_count",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObUInt64Type,                       // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(uint64_t),                   // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("logoff_dead_lock",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        40,                                // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("logoff_cpu_time_us",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObUInt64Type,                        // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        sizeof(uint64_t),                    // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("logoff_exec_time_us",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObUInt64Type,                         // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        sizeof(uint64_t),                     // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("logoff_alive_time_us",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObUInt64Type,                          // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        sizeof(uint64_t),                      // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment_text",  // column_name
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
    ADD_COLUMN_SCHEMA("sql_bind",  // column_name
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

int ObInnerTableSchema::all_tenant_sysauth_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_SYSAUTH_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_SYSAUTH_TNAME))) {
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
    ADD_COLUMN_SCHEMA("grantee_id",  // column_name
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
    ADD_COLUMN_SCHEMA("priv_id",  // column_name
        ++column_id,              // column_id
        3,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("priv_option",  // column_name
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

int ObInnerTableSchema::all_tenant_sysauth_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_SYSAUTH_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_SYSAUTH_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("grantee_id",  // column_name
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
    ADD_COLUMN_SCHEMA("priv_id",  // column_name
        ++column_id,              // column_id
        3,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("priv_option",  // column_name
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

int ObInnerTableSchema::all_tenant_objauth_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_OBJAUTH_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(7);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OBJAUTH_TNAME))) {
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
    ADD_COLUMN_SCHEMA("obj_id",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("objtype",  // column_name
        ++column_id,              // column_id
        3,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("col_id",  // column_name
        ++column_id,             // column_id
        4,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("grantor_id",  // column_name
        ++column_id,                 // column_id
        5,                           // rowkey_id
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
    ADD_COLUMN_SCHEMA("grantee_id",  // column_name
        ++column_id,                 // column_id
        6,                           // rowkey_id
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
    ADD_COLUMN_SCHEMA("priv_id",  // column_name
        ++column_id,              // column_id
        7,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("priv_option",  // column_name
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

int ObInnerTableSchema::all_tenant_objauth_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_OBJAUTH_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(8);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OBJAUTH_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("obj_id",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("objtype",  // column_name
        ++column_id,              // column_id
        3,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("col_id",  // column_name
        ++column_id,             // column_id
        4,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("grantor_id",  // column_name
        ++column_id,                 // column_id
        5,                           // rowkey_id
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
    ADD_COLUMN_SCHEMA("grantee_id",  // column_name
        ++column_id,                 // column_id
        6,                           // rowkey_id
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
    ADD_COLUMN_SCHEMA("priv_id",  // column_name
        ++column_id,              // column_id
        7,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
        ++column_id,                     // column_id
        8,                               // rowkey_id
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
    ADD_COLUMN_SCHEMA("priv_option",  // column_name
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

int ObInnerTableSchema::all_tenant_backup_info_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_BACKUP_INFO_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_BACKUP_INFO_TNAME))) {
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
    ADD_COLUMN_SCHEMA("name",              // column_name
        ++column_id,                       // column_id
        2,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_INNER_TABLE_DEFAULT_KEY_LENTH,  // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("value",               // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_INNER_TABLE_DEFAULT_VALUE_LENTH,  // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
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

int ObInnerTableSchema::all_restore_info_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_RESTORE_INFO_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RESTORE_INFO_TNAME))) {
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
    ADD_COLUMN_SCHEMA("name",              // column_name
        ++column_id,                       // column_id
        2,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_INNER_TABLE_DEFAULT_KEY_LENTH,  // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("value",               // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_INNER_TABLE_DEFAULT_VALUE_LENTH,  // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
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

int ObInnerTableSchema::all_tenant_backup_log_archive_status_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_BACKUP_LOG_ARCHIVE_STATUS_TNAME))) {
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
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
        ++column_id,                  // column_id
        2,                            // rowkey_id
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
    ADD_COLUMN_SCHEMA("log_archive_round",  // column_name
        ++column_id,                        // column_id
        3,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObIntType,                          // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(int64_t),                    // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("min_first_time",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(ObPreciseDateTime),          // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false,                              // is_autoincrement
        false);                             // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("max_next_time",  // column_name
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
    ObObj input_bytes_default;
    input_bytes_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("input_bytes",  // column_name
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
        false,                          // is_autoincrement
        input_bytes_default,
        input_bytes_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj output_bytes_default;
    output_bytes_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("output_bytes",  // column_name
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
        output_bytes_default,
        output_bytes_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj deleted_input_bytes_default;
    deleted_input_bytes_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("deleted_input_bytes",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false,                                  // is_autoincrement
        deleted_input_bytes_default,
        deleted_input_bytes_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj deleted_output_bytes_default;
    deleted_output_bytes_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("deleted_output_bytes",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false,                                   // is_autoincrement
        deleted_output_bytes_default,
        deleted_output_bytes_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj pg_count_default;
    pg_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("pg_count",  // column_name
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
        pg_count_default,
        pg_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj status_default;
    status_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("status",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        OB_DEFAULT_STATUS_LENTH,   // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false,                     // is_autoincrement
        status_default,
        status_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj is_mount_file_created_default;
    is_mount_file_created_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("is_mount_file_created",  // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObIntType,                                // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        sizeof(int64_t),                          // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        false,                                    // is_nullable
        false,                                    // is_autoincrement
        is_mount_file_created_default,
        is_mount_file_created_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj compatible_default;
    compatible_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("compatible",  // column_name
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
        compatible_default,
        compatible_default);  // default_value
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

int ObInnerTableSchema::all_backup_log_archive_status_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_BACKUP_LOG_ARCHIVE_STATUS_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_BACKUP_LOG_ARCHIVE_STATUS_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
        ++column_id,                  // column_id
        2,                            // rowkey_id
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
    ADD_COLUMN_SCHEMA("log_archive_round",  // column_name
        ++column_id,                        // column_id
        3,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObIntType,                          // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(int64_t),                    // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("min_first_time",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObTimestampType,                    // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(ObPreciseDateTime),          // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false,                              // is_autoincrement
        false);                             // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("max_next_time",  // column_name
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
    ObObj input_bytes_default;
    input_bytes_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("input_bytes",  // column_name
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
        false,                          // is_autoincrement
        input_bytes_default,
        input_bytes_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj output_bytes_default;
    output_bytes_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("output_bytes",  // column_name
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
        output_bytes_default,
        output_bytes_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj deleted_input_bytes_default;
    deleted_input_bytes_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("deleted_input_bytes",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false,                                  // is_autoincrement
        deleted_input_bytes_default,
        deleted_input_bytes_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj deleted_output_bytes_default;
    deleted_output_bytes_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("deleted_output_bytes",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false,                                   // is_autoincrement
        deleted_output_bytes_default,
        deleted_output_bytes_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj pg_count_default;
    pg_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("pg_count",  // column_name
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
        pg_count_default,
        pg_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_dest",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_BACKUP_DEST_LENGTH,    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_mark_deleted",  // column_name
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
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj compatible_default;
    compatible_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("compatible",  // column_name
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
        compatible_default,
        compatible_default);  // default_value
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

int ObInnerTableSchema::all_tenant_backup_task_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_BACKUP_TASK_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_BACKUP_TASK_TNAME))) {
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
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
        ++column_id,                  // column_id
        2,                            // rowkey_id
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
    ADD_COLUMN_SCHEMA("backup_set_id",  // column_name
        ++column_id,                    // column_id
        3,                              // rowkey_id
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
    ADD_COLUMN_SCHEMA("backup_type",       // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_INNER_TABLE_BACKUP_TYPE_LENTH,  // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("device_type",          // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH,  // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
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
    ADD_COLUMN_SCHEMA("prev_full_backup_set_id",  // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObIntType,                                // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        sizeof(int64_t),                          // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        false,                                    // is_nullable
        false);                                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("prev_inc_backup_set_id",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("prev_backup_data_version",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        false,                                     // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("pg_count",  // column_name
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
    ADD_COLUMN_SCHEMA("macro_block_count",  // column_name
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
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("finish_pg_count",  // column_name
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
    ADD_COLUMN_SCHEMA("finish_macro_block_count",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        false,                                     // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("input_bytes",  // column_name
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
    ADD_COLUMN_SCHEMA("output_bytes",  // column_name
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
    ADD_COLUMN_SCHEMA_TS("start_time",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObTimestampType,                // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(ObPreciseDateTime),      // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        false);                         // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("end_time",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObTimestampType,              // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(ObPreciseDateTime),    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        false);                       // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("compatible",  // column_name
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
    ADD_COLUMN_SCHEMA("cluster_version",  // column_name
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
    ADD_COLUMN_SCHEMA("status",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_DEFAULT_STATUS_LENTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("result",  // column_name
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
    ADD_COLUMN_SCHEMA("cluster_id",  // column_name
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
    ADD_COLUMN_SCHEMA("backup_dest",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_BACKUP_DEST_LENGTH,    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_data_version",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObIntType,                            // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        sizeof(int64_t),                      // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        true,                                 // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_schema_version",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cluster_version_display",           // column_name
        ++column_id,                                       // column_id
        0,                                                 // rowkey_id
        0,                                                 // index_id
        0,                                                 // part_key_pos
        ObVarcharType,                                     // column_type
        CS_TYPE_INVALID,                                   // column_collation_type
        OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH,  // column_length
        -1,                                                // column_precision
        -1,                                                // column_scale
        true,                                              // is_nullable
        false);                                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_count",  // column_name
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
    ADD_COLUMN_SCHEMA("finish_partition_count",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        true,                                    // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj encryption_mode_default;
    encryption_mode_default.set_varchar(ObString::make_string("None"));
    ADD_COLUMN_SCHEMA_T("encryption_mode",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        OB_MAX_ENCRYPTION_MODE_LENGTH,      // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false,                              // is_autoincrement
        encryption_mode_default,
        encryption_mode_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj passwd_default;
    passwd_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("passwd",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        OB_MAX_PASSWORD_LENGTH,    // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false,                     // is_autoincrement
        passwd_default,
        passwd_default);  // default_value
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

int ObInnerTableSchema::all_backup_task_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_BACKUP_TASK_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_BACKUP_TASK_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
        ++column_id,                  // column_id
        2,                            // rowkey_id
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
    ADD_COLUMN_SCHEMA("backup_set_id",  // column_name
        ++column_id,                    // column_id
        3,                              // rowkey_id
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
    ADD_COLUMN_SCHEMA("backup_type",       // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_INNER_TABLE_BACKUP_TYPE_LENTH,  // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("device_type",          // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH,  // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
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
    ADD_COLUMN_SCHEMA("prev_full_backup_set_id",  // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObIntType,                                // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        sizeof(int64_t),                          // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        false,                                    // is_nullable
        false);                                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("prev_inc_backup_set_id",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("prev_backup_data_version",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        false,                                     // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("pg_count",  // column_name
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
    ADD_COLUMN_SCHEMA("macro_block_count",  // column_name
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
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("finish_pg_count",  // column_name
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
    ADD_COLUMN_SCHEMA("finish_macro_block_count",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        false,                                     // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("input_bytes",  // column_name
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
    ADD_COLUMN_SCHEMA("output_bytes",  // column_name
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
    ADD_COLUMN_SCHEMA_TS("start_time",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObTimestampType,                // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(ObPreciseDateTime),      // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        false);                         // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("end_time",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObTimestampType,              // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(ObPreciseDateTime),    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        false);                       // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("compatible",  // column_name
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
    ADD_COLUMN_SCHEMA("cluster_version",  // column_name
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
    ADD_COLUMN_SCHEMA("status",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_DEFAULT_STATUS_LENTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("result",  // column_name
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
    ADD_COLUMN_SCHEMA("cluster_id",  // column_name
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
    ADD_COLUMN_SCHEMA("backup_dest",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_BACKUP_DEST_LENGTH,    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_data_version",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObIntType,                            // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        sizeof(int64_t),                      // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        true,                                 // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_schema_version",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cluster_version_display",           // column_name
        ++column_id,                                       // column_id
        0,                                                 // rowkey_id
        0,                                                 // index_id
        0,                                                 // part_key_pos
        ObVarcharType,                                     // column_type
        CS_TYPE_INVALID,                                   // column_collation_type
        OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH,  // column_length
        -1,                                                // column_precision
        -1,                                                // column_scale
        true,                                              // is_nullable
        false);                                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_count",  // column_name
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
    ADD_COLUMN_SCHEMA("finish_partition_count",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        true,                                    // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_mark_deleted",  // column_name
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
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj encryption_mode_default;
    encryption_mode_default.set_varchar(ObString::make_string("None"));
    ADD_COLUMN_SCHEMA_T("encryption_mode",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        OB_MAX_ENCRYPTION_MODE_LENGTH,      // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false,                              // is_autoincrement
        encryption_mode_default,
        encryption_mode_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj passwd_default;
    passwd_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("passwd",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        OB_MAX_PASSWORD_LENGTH,    // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false,                     // is_autoincrement
        passwd_default,
        passwd_default);  // default_value
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

int ObInnerTableSchema::all_tenant_pg_backup_task_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_PG_BACKUP_TASK_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_PG_BACKUP_TASK_TNAME))) {
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
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
        ++column_id,                  // column_id
        4,                            // rowkey_id
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
    ADD_COLUMN_SCHEMA("backup_set_id",  // column_name
        ++column_id,                    // column_id
        5,                              // rowkey_id
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
    ADD_COLUMN_SCHEMA("backup_type",       // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_INNER_TABLE_BACKUP_TYPE_LENTH,  // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
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
    ADD_COLUMN_SCHEMA("partition_count",  // column_name
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
    ADD_COLUMN_SCHEMA("macro_block_count",  // column_name
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
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("finish_partition_count",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("finish_macro_block_count",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        false,                                     // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("input_bytes",  // column_name
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
    ADD_COLUMN_SCHEMA("output_bytes",  // column_name
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
    ADD_COLUMN_SCHEMA_TS("start_time",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObTimestampType,                // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(ObPreciseDateTime),      // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        false);                         // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("end_time",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObTimestampType,              // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(ObPreciseDateTime),    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        false);                       // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("retry_count",  // column_name
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
    ADD_COLUMN_SCHEMA("replica_role",  // column_name
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
    ADD_COLUMN_SCHEMA("replica_type",  // column_name
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
    ADD_COLUMN_SCHEMA("svr_ip",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_MAX_SERVER_ADDR_SIZE,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
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
    ADD_COLUMN_SCHEMA("status",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_DEFAULT_STATUS_LENTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("task_id",  // column_name
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
    ADD_COLUMN_SCHEMA("result",  // column_name
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
    ADD_COLUMN_SCHEMA("trace_id",     // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_TRACE_ID_BUFFER_SIZE,  // column_length
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

int ObInnerTableSchema::all_tenant_error_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_ERROR_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_ERROR_TNAME))) {
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
    ADD_COLUMN_SCHEMA("obj_id",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("obj_seq",  // column_name
        ++column_id,              // column_id
        3,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("obj_type",  // column_name
        ++column_id,               // column_id
        4,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("line",  // column_name
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
    ADD_COLUMN_SCHEMA("position",  // column_name
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
    ADD_COLUMN_SCHEMA("text_length",  // column_name
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
    ADD_COLUMN_SCHEMA("text",       // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_ORACLE_COMMENT_LENGTH,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("property",  // column_name
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
    ADD_COLUMN_SCHEMA("error_number",  // column_name
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

int ObInnerTableSchema::all_server_recovery_status_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SERVER_RECOVERY_STATUS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SERVER_RECOVERY_STATUS_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        1,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("rescue_svr_ip",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        MAX_IP_ADDR_LENGTH,             // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rescue_svr_port",  // column_name
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
    ADD_COLUMN_SCHEMA("rescue_progress",  // column_name
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

int ObInnerTableSchema::all_datafile_recovery_status_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_DATAFILE_RECOVERY_STATUS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DATAFILE_RECOVERY_STATUS_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        1,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        3,                          // rowkey_id
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
    ADD_COLUMN_SCHEMA("file_id",  // column_name
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
    ADD_COLUMN_SCHEMA("dest_svr_ip",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        MAX_IP_ADDR_LENGTH,           // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("dest_svr_port",  // column_name
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
    ADD_COLUMN_SCHEMA("dest_unit_id",  // column_name
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
    ObObj status_default;
    status_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("status",  // column_name
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
        status_default,
        status_default);  // default_value
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

int ObInnerTableSchema::all_tenant_backup_clean_info_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_BACKUP_CLEAN_INFO_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_BACKUP_CLEAN_INFO_TNAME))) {
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("start_time",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObTimestampType,                // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(ObPreciseDateTime),      // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        false);                         // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("end_time",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObTimestampType,              // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(ObPreciseDateTime),    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        false);                       // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
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
    ADD_COLUMN_SCHEMA("type",                     // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObVarcharType,                            // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        OB_INNER_TABLE_BACKUP_CLEAN_TYPE_LENGTH,  // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        false,                                    // is_nullable
        false);                                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("status",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_DEFAULT_STATUS_LENTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("parameter",                     // column_name
        ++column_id,                                   // column_id
        0,                                             // rowkey_id
        0,                                             // index_id
        0,                                             // part_key_pos
        ObVarcharType,                                 // column_type
        CS_TYPE_INVALID,                               // column_collation_type
        OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH,  // column_length
        -1,                                            // column_precision
        -1,                                            // column_scale
        false,                                         // is_nullable
        false);                                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("error_msg",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_MAX_ERROR_MSG_LEN,       // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment",   // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        MAX_TABLE_COMMENT_LENGTH,  // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("clog_gc_snapshot",  // column_name
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
    ADD_COLUMN_SCHEMA("result",  // column_name
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
    ADD_COLUMN_SCHEMA("copy_id",  // column_name
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

int ObInnerTableSchema::all_backup_clean_info_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_BACKUP_CLEAN_INFO_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_BACKUP_CLEAN_INFO_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("job_id",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA_TS("start_time",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObTimestampType,                // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(ObPreciseDateTime),      // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        false);                         // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("end_time",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObTimestampType,              // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(ObPreciseDateTime),    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        false);                       // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
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
    ADD_COLUMN_SCHEMA("type",                     // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObVarcharType,                            // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        OB_INNER_TABLE_BACKUP_CLEAN_TYPE_LENGTH,  // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        false,                                    // is_nullable
        false);                                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("status",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_DEFAULT_STATUS_LENTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("parameter",                     // column_name
        ++column_id,                                   // column_id
        0,                                             // rowkey_id
        0,                                             // index_id
        0,                                             // part_key_pos
        ObVarcharType,                                 // column_type
        CS_TYPE_INVALID,                               // column_collation_type
        OB_INNER_TABLE_BACKUP_CLEAN_PARAMETER_LENGTH,  // column_length
        -1,                                            // column_precision
        -1,                                            // column_scale
        false,                                         // is_nullable
        false);                                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("error_msg",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_MAX_ERROR_MSG_LEN,       // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment",   // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        MAX_TABLE_COMMENT_LENGTH,  // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("clog_gc_snapshot",  // column_name
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
    ADD_COLUMN_SCHEMA("result",  // column_name
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
    ADD_COLUMN_SCHEMA("copy_id",  // column_name
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

int ObInnerTableSchema::all_backup_task_clean_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_BACKUP_TASK_CLEAN_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_BACKUP_TASK_CLEAN_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
        ++column_id,                  // column_id
        2,                            // rowkey_id
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
    ADD_COLUMN_SCHEMA("backup_set_id",  // column_name
        ++column_id,                    // column_id
        3,                              // rowkey_id
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
    ADD_COLUMN_SCHEMA("job_id",  // column_name
        ++column_id,             // column_id
        4,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("backup_type",       // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_INNER_TABLE_BACKUP_TYPE_LENTH,  // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("device_type",          // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        OB_DEFAULT_OUTPUT_DEVICE_TYPE_LENTH,  // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
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
    ADD_COLUMN_SCHEMA("prev_full_backup_set_id",  // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObIntType,                                // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        sizeof(int64_t),                          // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        false,                                    // is_nullable
        false);                                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("prev_inc_backup_set_id",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("prev_backup_data_version",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        false,                                     // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("pg_count",  // column_name
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
    ADD_COLUMN_SCHEMA("macro_block_count",  // column_name
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
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("finish_pg_count",  // column_name
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
    ADD_COLUMN_SCHEMA("finish_macro_block_count",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        false,                                     // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("input_bytes",  // column_name
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
    ADD_COLUMN_SCHEMA("output_bytes",  // column_name
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
    ADD_COLUMN_SCHEMA_TS("start_time",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObTimestampType,                // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(ObPreciseDateTime),      // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        false);                         // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("end_time",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObTimestampType,              // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(ObPreciseDateTime),    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        false);                       // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("compatible",  // column_name
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
    ADD_COLUMN_SCHEMA("cluster_version",  // column_name
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
    ADD_COLUMN_SCHEMA("status",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_DEFAULT_STATUS_LENTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("result",  // column_name
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
    ADD_COLUMN_SCHEMA("cluster_id",  // column_name
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
    ADD_COLUMN_SCHEMA("backup_dest",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_BACKUP_DEST_LENGTH,    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_data_version",  // column_name
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
    ADD_COLUMN_SCHEMA("backup_schema_version",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cluster_version_display",           // column_name
        ++column_id,                                       // column_id
        0,                                                 // rowkey_id
        0,                                                 // index_id
        0,                                                 // part_key_pos
        ObVarcharType,                                     // column_type
        CS_TYPE_INVALID,                                   // column_collation_type
        OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH,  // column_length
        -1,                                                // column_precision
        -1,                                                // column_scale
        false,                                             // is_nullable
        false);                                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_count",  // column_name
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
    ADD_COLUMN_SCHEMA("finish_partition_count",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("copy_id",  // column_name
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

int ObInnerTableSchema::all_restore_progress_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_RESTORE_PROGRESS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RESTORE_PROGRESS_TNAME))) {
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
    ObObj external_job_id_default;
    external_job_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("external_job_id",  // column_name
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
        external_job_id_default,
        external_job_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj tenant_id_default;
    tenant_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("tenant_id",  // column_name
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
        false,                        // is_autoincrement
        tenant_id_default,
        tenant_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj tenant_name_default;
    tenant_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("tenant_name",    // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_TENANT_NAME_LENGTH_STORE,  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        tenant_name_default,
        tenant_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("status",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        64,                      // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("start_time",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObTimestampType,                // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(ObPreciseDateTime),      // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false,                          // is_autoincrement
        false);                         // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("completion_time",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObTimestampType,                     // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        sizeof(ObPreciseDateTime),           // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false,                               // is_autoincrement
        false);                              // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ObObj pg_count_default;
    pg_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("pg_count",  // column_name
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
        pg_count_default,
        pg_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj finish_pg_count_default;
    finish_pg_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("finish_pg_count",  // column_name
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
        finish_pg_count_default,
        finish_pg_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj partition_count_default;
    partition_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("partition_count",  // column_name
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
        partition_count_default,
        partition_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj finish_partition_count_default;
    finish_partition_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("finish_partition_count",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        false,                                     // is_nullable
        false,                                     // is_autoincrement
        finish_partition_count_default,
        finish_partition_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj macro_block_count_default;
    macro_block_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("macro_block_count",  // column_name
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
        false,                                // is_autoincrement
        macro_block_count_default,
        macro_block_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj finish_macro_block_count_default;
    finish_macro_block_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("finish_macro_block_count",  // column_name
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
        false,                                       // is_autoincrement
        finish_macro_block_count_default,
        finish_macro_block_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("restore_start_timestamp",  // column_name
        ++column_id,                                 // column_id
        0,                                           // rowkey_id
        0,                                           // index_id
        0,                                           // part_key_pos
        ObTimestampType,                             // column_type
        CS_TYPE_INVALID,                             // column_collation_type
        sizeof(ObPreciseDateTime),                   // column_length
        -1,                                          // column_precision
        -1,                                          // column_scale
        true,                                        // is_nullable
        false,                                       // is_autoincrement
        false);                                      // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("restore_finish_timestamp",  // column_name
        ++column_id,                                  // column_id
        0,                                            // rowkey_id
        0,                                            // index_id
        0,                                            // part_key_pos
        ObTimestampType,                              // column_type
        CS_TYPE_INVALID,                              // column_collation_type
        sizeof(ObPreciseDateTime),                    // column_length
        -1,                                           // column_precision
        -1,                                           // column_scale
        true,                                         // is_nullable
        false,                                        // is_autoincrement
        false);                                       // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("restore_current_timestamp",  // column_name
        ++column_id,                                   // column_id
        0,                                             // rowkey_id
        0,                                             // index_id
        0,                                             // part_key_pos
        ObTimestampType,                               // column_type
        CS_TYPE_INVALID,                               // column_collation_type
        sizeof(ObPreciseDateTime),                     // column_length
        -1,                                            // column_precision
        -1,                                            // column_scale
        true,                                          // is_nullable
        false,                                         // is_autoincrement
        false);                                        // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ObObj info_default;
    info_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("info",              // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_INNER_TABLE_DEFAULT_VALUE_LENTH,  // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false,                               // is_autoincrement
        info_default,
        info_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj backup_cluster_id_default;
    backup_cluster_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("backup_cluster_id",  // column_name
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
        false,                                // is_autoincrement
        backup_cluster_id_default,
        backup_cluster_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj backup_cluster_name_default;
    backup_cluster_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("backup_cluster_name",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        OB_MAX_CLUSTER_NAME_LENGTH,             // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false,                                  // is_autoincrement
        backup_cluster_name_default,
        backup_cluster_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj backup_tenant_id_default;
    backup_tenant_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("backup_tenant_id",  // column_name
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
        backup_tenant_id_default,
        backup_tenant_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj backup_tenant_name_default;
    backup_tenant_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("backup_tenant_name",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObVarcharType,                         // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        OB_MAX_TENANT_NAME_LENGTH_STORE,       // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false,                                 // is_autoincrement
        backup_tenant_name_default,
        backup_tenant_name_default);  // default_value
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

int ObInnerTableSchema::all_restore_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_RESTORE_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RESTORE_HISTORY_TNAME))) {
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
    ObObj external_job_id_default;
    external_job_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("external_job_id",  // column_name
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
        external_job_id_default,
        external_job_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj tenant_id_default;
    tenant_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("tenant_id",  // column_name
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
        false,                        // is_autoincrement
        tenant_id_default,
        tenant_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj tenant_name_default;
    tenant_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("tenant_name",    // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_TENANT_NAME_LENGTH_STORE,  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        tenant_name_default,
        tenant_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("status",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        64,                      // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("start_time",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObTimestampType,                // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(ObPreciseDateTime),      // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false,                          // is_autoincrement
        false);                         // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("completion_time",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObTimestampType,                     // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        sizeof(ObPreciseDateTime),           // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false,                               // is_autoincrement
        false);                              // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ObObj pg_count_default;
    pg_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("pg_count",  // column_name
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
        pg_count_default,
        pg_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj finish_pg_count_default;
    finish_pg_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("finish_pg_count",  // column_name
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
        finish_pg_count_default,
        finish_pg_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj partition_count_default;
    partition_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("partition_count",  // column_name
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
        partition_count_default,
        partition_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj finish_partition_count_default;
    finish_partition_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("finish_partition_count",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        false,                                     // is_nullable
        false,                                     // is_autoincrement
        finish_partition_count_default,
        finish_partition_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj macro_block_count_default;
    macro_block_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("macro_block_count",  // column_name
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
        false,                                // is_autoincrement
        macro_block_count_default,
        macro_block_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj finish_macro_block_count_default;
    finish_macro_block_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("finish_macro_block_count",  // column_name
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
        false,                                       // is_autoincrement
        finish_macro_block_count_default,
        finish_macro_block_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("restore_start_timestamp",  // column_name
        ++column_id,                                 // column_id
        0,                                           // rowkey_id
        0,                                           // index_id
        0,                                           // part_key_pos
        ObTimestampType,                             // column_type
        CS_TYPE_INVALID,                             // column_collation_type
        sizeof(ObPreciseDateTime),                   // column_length
        -1,                                          // column_precision
        -1,                                          // column_scale
        true,                                        // is_nullable
        false,                                       // is_autoincrement
        false);                                      // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("restore_finish_timestamp",  // column_name
        ++column_id,                                  // column_id
        0,                                            // rowkey_id
        0,                                            // index_id
        0,                                            // part_key_pos
        ObTimestampType,                              // column_type
        CS_TYPE_INVALID,                              // column_collation_type
        sizeof(ObPreciseDateTime),                    // column_length
        -1,                                           // column_precision
        -1,                                           // column_scale
        true,                                         // is_nullable
        false,                                        // is_autoincrement
        false);                                       // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("restore_current_timestamp",  // column_name
        ++column_id,                                   // column_id
        0,                                             // rowkey_id
        0,                                             // index_id
        0,                                             // part_key_pos
        ObTimestampType,                               // column_type
        CS_TYPE_INVALID,                               // column_collation_type
        sizeof(ObPreciseDateTime),                     // column_length
        -1,                                            // column_precision
        -1,                                            // column_scale
        true,                                          // is_nullable
        false,                                         // is_autoincrement
        false);                                        // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ObObj restore_data_version_default;
    restore_data_version_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("restore_data_version",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false,                                   // is_autoincrement
        restore_data_version_default,
        restore_data_version_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_dest",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_BACKUP_DEST_LENGTH,    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("restore_option",      // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_INNER_TABLE_DEFAULT_VALUE_LENTH,  // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj info_default;
    info_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("info",              // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_INNER_TABLE_DEFAULT_VALUE_LENTH,  // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false,                               // is_autoincrement
        info_default,
        info_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj backup_cluster_id_default;
    backup_cluster_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("backup_cluster_id",  // column_name
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
        false,                                // is_autoincrement
        backup_cluster_id_default,
        backup_cluster_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj backup_cluster_name_default;
    backup_cluster_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("backup_cluster_name",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        OB_MAX_CLUSTER_NAME_LENGTH,             // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false,                                  // is_autoincrement
        backup_cluster_name_default,
        backup_cluster_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj backup_tenant_id_default;
    backup_tenant_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("backup_tenant_id",  // column_name
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
        backup_tenant_id_default,
        backup_tenant_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj backup_tenant_name_default;
    backup_tenant_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("backup_tenant_name",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObVarcharType,                         // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        OB_MAX_TENANT_NAME_LENGTH_STORE,       // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false,                                 // is_autoincrement
        backup_tenant_name_default,
        backup_tenant_name_default);  // default_value
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

int ObInnerTableSchema::all_tenant_restore_pg_info_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_RESTORE_PG_INFO_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_RESTORE_PG_INFO_TNAME))) {
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
    ADD_COLUMN_SCHEMA("table_id",  // column_name
        ++column_id,               // column_id
        1,                         // rowkey_id
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
    ObObj macro_block_count_default;
    macro_block_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("macro_block_count",  // column_name
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
        false,                                // is_autoincrement
        macro_block_count_default,
        macro_block_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj finish_macro_block_count_default;
    finish_macro_block_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("finish_macro_block_count",  // column_name
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
        false,                                       // is_autoincrement
        finish_macro_block_count_default,
        finish_macro_block_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj partition_count_default;
    partition_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("partition_count",  // column_name
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
        partition_count_default,
        partition_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj finish_partition_count_default;
    finish_partition_count_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("finish_partition_count",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        false,                                     // is_nullable
        false,                                     // is_autoincrement
        finish_partition_count_default,
        finish_partition_count_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("restore_info",        // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_INNER_TABLE_DEFAULT_VALUE_LENTH,  // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
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

int ObInnerTableSchema::all_table_v2_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TABLE_V2_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLE_V2_HISTORY_TNAME))) {
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
    ObObj table_name_default;
    table_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("table_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,      // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false,                         // is_autoincrement
        table_name_default,
        table_name_default);  // default_value
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
    ADD_COLUMN_SCHEMA("table_type",  // column_name
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
    ADD_COLUMN_SCHEMA("load_type",  // column_name
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
    ADD_COLUMN_SCHEMA("def_type",  // column_name
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
    ADD_COLUMN_SCHEMA("rowkey_column_num",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObIntType,                          // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(int64_t),                    // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("index_column_num",  // column_name
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
    ADD_COLUMN_SCHEMA("max_used_column_id",  // column_name
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
    ADD_COLUMN_SCHEMA("autoinc_column_id",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObIntType,                          // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(int64_t),                    // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj auto_increment_default;
    auto_increment_default.set_uint64(1);
    ADD_COLUMN_SCHEMA_T("auto_increment",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObUInt64Type,                      // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(uint64_t),                  // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false,                             // is_autoincrement
        auto_increment_default,
        auto_increment_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("read_only",  // column_name
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
    ADD_COLUMN_SCHEMA("rowkey_split_pos",  // column_name
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
    ADD_COLUMN_SCHEMA("expire_condition",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_MAX_EXPIRE_INFO_STRING_LENGTH,  // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_use_bloomfilter",  // column_name
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
    ObObj comment_default;
    comment_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("comment",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_TABLE_COMMENT_LENGTH,   // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false,                      // is_autoincrement
        comment_default,
        comment_default);  // default_value
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
    ADD_COLUMN_SCHEMA("collation_type",  // column_name
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
    ADD_COLUMN_SCHEMA("data_table_id",  // column_name
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
    ADD_COLUMN_SCHEMA("index_status",  // column_name
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
    ADD_COLUMN_SCHEMA("progressive_merge_num",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("index_type",  // column_name
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
    ADD_COLUMN_SCHEMA("part_level",  // column_name
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
    ADD_COLUMN_SCHEMA("part_func_type",  // column_name
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
    ADD_COLUMN_SCHEMA("part_func_expr",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_PART_FUNC_EXPR_LENGTH,    // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
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
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_func_type",  // column_name
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
    ADD_COLUMN_SCHEMA("sub_part_func_expr",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_MAX_PART_FUNC_EXPR_LENGTH,        // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("sub_part_num",  // column_name
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
    ADD_COLUMN_SCHEMA("create_mem_version",  // column_name
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
    ADD_COLUMN_SCHEMA("view_definition",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObLongTextType,                   // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        0,                                // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("view_check_option",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObIntType,                          // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(int64_t),                    // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("view_is_updatable",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObIntType,                          // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(int64_t),                    // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("zone_list",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_ZONE_LIST_LENGTH,       // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("primary_zone",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        MAX_ZONE_LENGTH,               // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj index_using_type_default;
    index_using_type_default.set_int(USING_BTREE);
    ADD_COLUMN_SCHEMA_T("index_using_type",  // column_name
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
        false,                               // is_autoincrement
        index_using_type_default,
        index_using_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("parser_name",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_PARSER_NAME_LENGTH,    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj index_attributes_set_default;
    index_attributes_set_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("index_attributes_set",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        true,                                    // is_nullable
        false,                                   // is_autoincrement
        index_attributes_set_default,
        index_attributes_set_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj locality_default;
    locality_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("locality",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        MAX_LOCALITY_LENGTH,         // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false,                       // is_autoincrement
        locality_default,
        locality_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj tablet_size_default;
    tablet_size_default.set_int(OB_DEFAULT_TABLET_SIZE);
    ADD_COLUMN_SCHEMA_T("tablet_size",  // column_name
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
        false,                          // is_autoincrement
        tablet_size_default,
        tablet_size_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj pctfree_default;
    pctfree_default.set_int(OB_DEFAULT_PCTFREE);
    ADD_COLUMN_SCHEMA_T("pctfree",  // column_name
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
        false,                      // is_autoincrement
        pctfree_default,
        pctfree_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj previous_locality_default;
    previous_locality_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("previous_locality",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        MAX_LOCALITY_LENGTH,                  // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        true,                                 // is_nullable
        false,                                // is_autoincrement
        previous_locality_default,
        previous_locality_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj max_used_part_id_default;
    max_used_part_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("max_used_part_id",  // column_name
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
        false,                               // is_autoincrement
        max_used_part_id_default,
        max_used_part_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj partition_cnt_within_partition_table_default;
    partition_cnt_within_partition_table_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("partition_cnt_within_partition_table",  // column_name
        ++column_id,                                             // column_id
        0,                                                       // rowkey_id
        0,                                                       // index_id
        0,                                                       // part_key_pos
        ObIntType,                                               // column_type
        CS_TYPE_INVALID,                                         // column_collation_type
        sizeof(int64_t),                                         // column_length
        -1,                                                      // column_precision
        -1,                                                      // column_scale
        true,                                                    // is_nullable
        false,                                                   // is_autoincrement
        partition_cnt_within_partition_table_default,
        partition_cnt_within_partition_table_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj partition_status_default;
    partition_status_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("partition_status",  // column_name
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
        false,                               // is_autoincrement
        partition_status_default,
        partition_status_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj partition_schema_version_default;
    partition_schema_version_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("partition_schema_version",  // column_name
        ++column_id,                                 // column_id
        0,                                           // rowkey_id
        0,                                           // index_id
        0,                                           // part_key_pos
        ObIntType,                                   // column_type
        CS_TYPE_INVALID,                             // column_collation_type
        sizeof(int64_t),                             // column_length
        -1,                                          // column_precision
        -1,                                          // column_scale
        true,                                        // is_nullable
        false,                                       // is_autoincrement
        partition_schema_version_default,
        partition_schema_version_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("max_used_constraint_id",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        true,                                    // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj session_id_default;
    session_id_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("session_id",  // column_name
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
        session_id_default,
        session_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj pk_comment_default;
    pk_comment_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("pk_comment",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        MAX_TABLE_COMMENT_LENGTH,      // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false,                         // is_autoincrement
        pk_comment_default,
        pk_comment_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj sess_active_time_default;
    sess_active_time_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("sess_active_time",  // column_name
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
        false,                               // is_autoincrement
        sess_active_time_default,
        sess_active_time_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj row_store_type_default;
    row_store_type_default.set_varchar(ObString::make_string("FLAT_ROW_STORE"));
    ADD_COLUMN_SCHEMA_T("row_store_type",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_MAX_STORE_FORMAT_NAME_LENGTH,   // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false,                             // is_autoincrement
        row_store_type_default,
        row_store_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj store_format_default;
    store_format_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("store_format",   // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_STORE_FORMAT_NAME_LENGTH,  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false,                            // is_autoincrement
        store_format_default,
        store_format_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj duplicate_scope_default;
    duplicate_scope_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("duplicate_scope",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObIntType,                          // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(int64_t),                    // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false,                              // is_autoincrement
        duplicate_scope_default,
        duplicate_scope_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj binding_default;
    binding_default.set_tinyint(false);
    ADD_COLUMN_SCHEMA_T("binding",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObTinyIntType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        1,                          // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false,                      // is_autoincrement
        binding_default,
        binding_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj progressive_merge_round_default;
    progressive_merge_round_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("progressive_merge_round",  // column_name
        ++column_id,                                // column_id
        0,                                          // rowkey_id
        0,                                          // index_id
        0,                                          // part_key_pos
        ObIntType,                                  // column_type
        CS_TYPE_INVALID,                            // column_collation_type
        sizeof(int64_t),                            // column_length
        -1,                                         // column_precision
        -1,                                         // column_scale
        true,                                       // is_nullable
        false,                                      // is_autoincrement
        progressive_merge_round_default,
        progressive_merge_round_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj storage_format_version_default;
    storage_format_version_default.set_int(2);
    ADD_COLUMN_SCHEMA_T("storage_format_version",  // column_name
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
        storage_format_version_default,
        storage_format_version_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj table_mode_default;
    table_mode_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("table_mode",  // column_name
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
        table_mode_default,
        table_mode_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj encryption_default;
    encryption_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("encryption",   // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_ENCRYPTION_NAME_LENGTH,  // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false,                          // is_autoincrement
        encryption_default,
        encryption_default);  // default_value
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
    ObObj drop_schema_version_default;
    drop_schema_version_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("drop_schema_version",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false,                                  // is_autoincrement
        drop_schema_version_default,
        drop_schema_version_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj is_sub_part_template_default;
    is_sub_part_template_default.set_tinyint(true);
    ADD_COLUMN_SCHEMA_T("is_sub_part_template",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObTinyIntType,                           // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        1,                                       // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        true,                                    // is_nullable
        false,                                   // is_autoincrement
        is_sub_part_template_default,
        is_sub_part_template_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj dop_default;
    dop_default.set_int(1);
    ADD_COLUMN_SCHEMA_T("dop",  // column_name
        ++column_id,            // column_id
        0,                      // rowkey_id
        0,                      // index_id
        0,                      // part_key_pos
        ObIntType,              // column_type
        CS_TYPE_INVALID,        // column_collation_type
        sizeof(int64_t),        // column_length
        -1,                     // column_precision
        -1,                     // column_scale
        true,                   // is_nullable
        false,                  // is_autoincrement
        dop_default,
        dop_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj character_set_client_default;
    character_set_client_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("character_set_client",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        true,                                    // is_nullable
        false,                                   // is_autoincrement
        character_set_client_default,
        character_set_client_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj collation_connection_default;
    collation_connection_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("collation_connection",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        true,                                    // is_nullable
        false,                                   // is_autoincrement
        collation_connection_default,
        collation_connection_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj auto_part_size_default;
    auto_part_size_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("auto_part_size",  // column_name
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
        false,                             // is_autoincrement
        auto_part_size_default,
        auto_part_size_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj auto_part_default;
    auto_part_default.set_tinyint(false);
    ADD_COLUMN_SCHEMA_T("auto_part",  // column_name
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
        auto_part_default,
        auto_part_default);  // default_value
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

int ObInnerTableSchema::all_tenant_object_type_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_OBJECT_TYPE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OBJECT_TYPE_TNAME))) {
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
    ADD_COLUMN_SCHEMA("object_type_id",  // column_name
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
    ADD_COLUMN_SCHEMA("type",  // column_name
        ++column_id,           // column_id
        3,                     // rowkey_id
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
    ADD_COLUMN_SCHEMA("properties",  // column_name
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
    ADD_COLUMN_SCHEMA("charset_id",  // column_name
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
    ADD_COLUMN_SCHEMA("charset_form",  // column_name
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
    ADD_COLUMN_SCHEMA("length",  // column_name
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
    ADD_COLUMN_SCHEMA("number_precision",  // column_name
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
    ADD_COLUMN_SCHEMA("scale",  // column_name
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
    ADD_COLUMN_SCHEMA("zero_fill",  // column_name
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
    ADD_COLUMN_SCHEMA("coll_type",  // column_name
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
    ADD_COLUMN_SCHEMA("object_name",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_TABLE_TYPE_LENGTH,     // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
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

int ObInnerTableSchema::all_tenant_object_type_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_OBJECT_TYPE_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_OBJECT_TYPE_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("object_type_id",  // column_name
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
    ADD_COLUMN_SCHEMA("type",  // column_name
        ++column_id,           // column_id
        3,                     // rowkey_id
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
    ADD_COLUMN_SCHEMA("properties",  // column_name
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
    ADD_COLUMN_SCHEMA("charset_id",  // column_name
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
    ADD_COLUMN_SCHEMA("charset_form",  // column_name
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
    ADD_COLUMN_SCHEMA("length",  // column_name
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
    ADD_COLUMN_SCHEMA("number_precision",  // column_name
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
    ADD_COLUMN_SCHEMA("scale",  // column_name
        ++column_id,            // column_id
        0,                      // rowkey_id
        0,                      // index_id
        0,                      // part_key_pos
        ObIntType,              // column_type
        CS_TYPE_INVALID,        // column_collation_type
        sizeof(int64_t),        // column_length
        -1,                     // column_precision
        -1,                     // column_scale
        true,                   // is_nullable
        false);                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("zero_fill",  // column_name
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
    ADD_COLUMN_SCHEMA("coll_type",  // column_name
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
    ADD_COLUMN_SCHEMA("object_name",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_TABLE_TYPE_LENGTH,     // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
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

int ObInnerTableSchema::all_backup_validation_job_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_BACKUP_VALIDATION_JOB_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_BACKUP_VALIDATION_JOB_TNAME))) {
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
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
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
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
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
    ADD_COLUMN_SCHEMA("backup_set_id",  // column_name
        ++column_id,                    // column_id
        4,                              // rowkey_id
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
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("progress_percent",  // column_name
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
    ADD_COLUMN_SCHEMA("status",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_DEFAULT_STATUS_LENTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
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

int ObInnerTableSchema::all_backup_validation_job_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_BACKUP_VALIDATION_JOB_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_BACKUP_VALIDATION_JOB_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
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
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
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
    ADD_COLUMN_SCHEMA("backup_set_id",  // column_name
        ++column_id,                    // column_id
        4,                              // rowkey_id
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
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("progress_percent",  // column_name
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
    ADD_COLUMN_SCHEMA("status",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_DEFAULT_STATUS_LENTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
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

int ObInnerTableSchema::all_tenant_backup_validation_task_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_BACKUP_VALIDATION_TASK_TNAME))) {
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
    ADD_COLUMN_SCHEMA("job_id",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("task_id",  // column_name
        ++column_id,              // column_id
        3,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
        ++column_id,                  // column_id
        4,                            // rowkey_id
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
    ADD_COLUMN_SCHEMA("backup_set_id",  // column_name
        ++column_id,                    // column_id
        5,                              // rowkey_id
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
    ADD_COLUMN_SCHEMA("status",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_DEFAULT_STATUS_LENTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_dest",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_BACKUP_DEST_LENGTH,    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("start_time",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObTimestampType,                // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(ObPreciseDateTime),      // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        false);                         // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("end_time",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObTimestampType,              // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(ObPreciseDateTime),    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        false);                       // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("total_pg_count",  // column_name
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
    ADD_COLUMN_SCHEMA("finish_pg_count",  // column_name
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
    ADD_COLUMN_SCHEMA("total_partition_count",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("finish_partition_count",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("total_macro_block_count",  // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObIntType,                                // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        sizeof(int64_t),                          // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        false,                                    // is_nullable
        false);                                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("finish_macro_block_count",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        false,                                     // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("log_size",  // column_name
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
    ADD_COLUMN_SCHEMA("result",  // column_name
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
    ADD_COLUMN_SCHEMA("comment",   // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        MAX_TABLE_COMMENT_LENGTH,  // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
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

int ObInnerTableSchema::all_backup_validation_task_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_BACKUP_VALIDATION_TASK_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_BACKUP_VALIDATION_TASK_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("job_id",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("task_id",  // column_name
        ++column_id,              // column_id
        3,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
        ++column_id,                  // column_id
        4,                            // rowkey_id
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
    ADD_COLUMN_SCHEMA("backup_set_id",  // column_name
        ++column_id,                    // column_id
        5,                              // rowkey_id
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
    ADD_COLUMN_SCHEMA("status",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_DEFAULT_STATUS_LENTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_dest",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_BACKUP_DEST_LENGTH,    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("start_time",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObTimestampType,                // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(ObPreciseDateTime),      // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        false);                         // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("end_time",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObTimestampType,              // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(ObPreciseDateTime),    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        false);                       // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("total_pg_count",  // column_name
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
    ADD_COLUMN_SCHEMA("finish_pg_count",  // column_name
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
    ADD_COLUMN_SCHEMA("total_partition_count",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("finish_partition_count",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("total_macro_block_count",  // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObIntType,                                // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        sizeof(int64_t),                          // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        false,                                    // is_nullable
        false);                                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("finish_macro_block_count",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        false,                                     // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("log_size",  // column_name
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
    ADD_COLUMN_SCHEMA("result",  // column_name
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
    ADD_COLUMN_SCHEMA("comment",   // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        MAX_TABLE_COMMENT_LENGTH,  // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
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

int ObInnerTableSchema::all_tenant_pg_backup_validation_task_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(7);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_PG_BACKUP_VALIDATION_TASK_TNAME))) {
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
    ADD_COLUMN_SCHEMA("job_id",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("task_id",  // column_name
        ++column_id,              // column_id
        3,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
        ++column_id,                  // column_id
        4,                            // rowkey_id
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
    ADD_COLUMN_SCHEMA("backup_set_id",  // column_name
        ++column_id,                    // column_id
        5,                              // rowkey_id
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
    ADD_COLUMN_SCHEMA("table_id",  // column_name
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
    ADD_COLUMN_SCHEMA("partition_id",  // column_name
        ++column_id,                   // column_id
        7,                             // rowkey_id
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
    ADD_COLUMN_SCHEMA("archive_round",  // column_name
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
    ADD_COLUMN_SCHEMA("status",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_DEFAULT_STATUS_LENTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trace_id",     // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_TRACE_ID_BUFFER_SIZE,  // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
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
    ADD_COLUMN_SCHEMA("total_partition_count",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("finish_partition_count",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("total_macro_block_count",  // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObIntType,                                // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        sizeof(int64_t),                          // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        false,                                    // is_nullable
        false);                                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("finish_macro_block_count",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObIntType,                                 // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(int64_t),                           // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        false,                                     // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("log_info",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        64,                        // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("log_size",  // column_name
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
    ADD_COLUMN_SCHEMA("result",  // column_name
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
    ADD_COLUMN_SCHEMA("comment",   // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        MAX_TABLE_COMMENT_LENGTH,  // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
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

int ObInnerTableSchema::all_tenant_time_zone_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_TIME_ZONE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_TIME_ZONE_TNAME))) {
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
    ObObj tenant_id_default;
    tenant_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("tenant_id",  // column_name
        ++column_id,                  // column_id
        1,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        tenant_id_default,
        tenant_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj time_zone_id_default;
    time_zone_id_default.set_null();
    ADD_COLUMN_SCHEMA_T("time_zone_id",  // column_name
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
        false,                           // is_autoincrement
        time_zone_id_default,
        time_zone_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj use_leap_seconds_default;
    use_leap_seconds_default.set_varchar(ObString::make_string("N"));
    ADD_COLUMN_SCHEMA_T("use_leap_seconds",  // column_name
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
    ADD_COLUMN_SCHEMA("version",  // column_name
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

int ObInnerTableSchema::all_tenant_time_zone_name_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_TIME_ZONE_NAME_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_TIME_ZONE_NAME_TNAME))) {
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
    ObObj tenant_id_default;
    tenant_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("tenant_id",  // column_name
        ++column_id,                  // column_id
        1,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        tenant_id_default,
        tenant_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj name_default;
    name_default.set_null();
    ADD_COLUMN_SCHEMA_T("name",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        64,                      // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false,                   // is_autoincrement
        name_default,
        name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj time_zone_id_default;
    time_zone_id_default.set_null();
    ADD_COLUMN_SCHEMA_T("time_zone_id",  // column_name
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
        time_zone_id_default,
        time_zone_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("version",  // column_name
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

int ObInnerTableSchema::all_tenant_time_zone_transition_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_TIME_ZONE_TRANSITION_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_TIME_ZONE_TRANSITION_TNAME))) {
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
    ObObj tenant_id_default;
    tenant_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("tenant_id",  // column_name
        ++column_id,                  // column_id
        1,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        tenant_id_default,
        tenant_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj time_zone_id_default;
    time_zone_id_default.set_null();
    ADD_COLUMN_SCHEMA_T("time_zone_id",  // column_name
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
        false,                           // is_autoincrement
        time_zone_id_default,
        time_zone_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj transition_time_default;
    transition_time_default.set_null();
    ADD_COLUMN_SCHEMA_T("transition_time",  // column_name
        ++column_id,                        // column_id
        3,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObIntType,                          // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(int64_t),                    // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false,                              // is_autoincrement
        transition_time_default,
        transition_time_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj transition_type_id_default;
    transition_type_id_default.set_null();
    ADD_COLUMN_SCHEMA_T("transition_type_id",  // column_name
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
        transition_type_id_default,
        transition_type_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("version",  // column_name
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

int ObInnerTableSchema::all_tenant_time_zone_transition_type_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_TIME_ZONE_TRANSITION_TYPE_TNAME))) {
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
    ObObj tenant_id_default;
    tenant_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("tenant_id",  // column_name
        ++column_id,                  // column_id
        1,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObIntType,                    // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(int64_t),              // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        tenant_id_default,
        tenant_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj time_zone_id_default;
    time_zone_id_default.set_null();
    ADD_COLUMN_SCHEMA_T("time_zone_id",  // column_name
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
        false,                           // is_autoincrement
        time_zone_id_default,
        time_zone_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj transition_type_id_default;
    transition_type_id_default.set_null();
    ADD_COLUMN_SCHEMA_T("transition_type_id",  // column_name
        ++column_id,                           // column_id
        3,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObIntType,                             // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        sizeof(int64_t),                       // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false,                                 // is_autoincrement
        transition_type_id_default,
        transition_type_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj offset_default;
    offset_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("offset",  // column_name
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
    ADD_COLUMN_SCHEMA_T("is_dst",  // column_name
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
    ADD_COLUMN_SCHEMA_T("abbreviation",  // column_name
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
    ADD_COLUMN_SCHEMA("version",  // column_name
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

int ObInnerTableSchema::all_tenant_constraint_column_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_CONSTRAINT_COLUMN_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_CONSTRAINT_COLUMN_TNAME))) {
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
    ADD_COLUMN_SCHEMA("constraint_id",  // column_name
        ++column_id,                    // column_id
        3,                              // rowkey_id
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

int ObInnerTableSchema::all_tenant_constraint_column_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_CONSTRAINT_COLUMN_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("constraint_id",  // column_name
        ++column_id,                    // column_id
        3,                              // rowkey_id
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

int ObInnerTableSchema::all_tenant_global_transaction_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_GLOBAL_TRANSACTION_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME))) {
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
    ADD_COLUMN_SCHEMA("gtrid",  // column_name
        ++column_id,            // column_id
        1,                      // rowkey_id
        0,                      // index_id
        0,                      // part_key_pos
        ObVarcharType,          // column_type
        CS_TYPE_BINARY,         // column_collation_type
        128,                    // column_length
        -1,                     // column_precision
        -1,                     // column_scale
        false,                  // is_nullable
        false);                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("bqual",  // column_name
        ++column_id,            // column_id
        2,                      // rowkey_id
        0,                      // index_id
        0,                      // part_key_pos
        ObVarcharType,          // column_type
        CS_TYPE_BINARY,         // column_collation_type
        128,                    // column_length
        -1,                     // column_precision
        -1,                     // column_scale
        false,                  // is_nullable
        false);                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj format_id_default;
    format_id_default.set_int(1);
    ADD_COLUMN_SCHEMA_T("format_id",  // column_name
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
        false,                        // is_autoincrement
        format_id_default,
        format_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trans_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        512,                       // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("coordinator",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        128,                          // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("scheduler_ip",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_SERVER_ADDR_SIZE,       // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("scheduler_port",  // column_name
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
    ObObj is_readonly_default;
    is_readonly_default.set_tinyint(0);
    ADD_COLUMN_SCHEMA_T("is_readonly",  // column_name
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
        is_readonly_default,
        is_readonly_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("state",  // column_name
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
    ADD_COLUMN_SCHEMA("end_flag",  // column_name
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

int ObInnerTableSchema::all_tenant_dependency_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_DEPENDENCY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_DEPENDENCY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("dep_obj_type",  // column_name
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
    ADD_COLUMN_SCHEMA("dep_obj_id",  // column_name
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
    ADD_COLUMN_SCHEMA("dep_order",  // column_name
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
    ADD_COLUMN_SCHEMA("dep_timestamp",  // column_name
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
    ADD_COLUMN_SCHEMA("ref_obj_type",  // column_name
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
    ADD_COLUMN_SCHEMA("ref_obj_id",  // column_name
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
    ADD_COLUMN_SCHEMA("ref_timestamp",  // column_name
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
    ADD_COLUMN_SCHEMA("dep_obj_owner_id",  // column_name
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
    ADD_COLUMN_SCHEMA("property",  // column_name
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
    ADD_COLUMN_SCHEMA("dep_attrs",         // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_BINARY,                    // column_collation_type
        OB_MAX_ORACLE_RAW_SQL_COL_LENGTH,  // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("dep_reason",        // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_BINARY,                    // column_collation_type
        OB_MAX_ORACLE_RAW_SQL_COL_LENGTH,  // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ref_obj_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,      // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
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

int ObInnerTableSchema::all_backup_backupset_job_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_BACKUP_BACKUPSET_JOB_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_BACKUP_BACKUPSET_JOB_TNAME))) {
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
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
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
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
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
    ADD_COLUMN_SCHEMA("backup_set_id",  // column_name
        ++column_id,                    // column_id
        4,                              // rowkey_id
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
    ADD_COLUMN_SCHEMA("copy_id",  // column_name
        ++column_id,              // column_id
        5,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("backup_backupset_type",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        OB_INNER_TABLE_BACKUP_TYPE_LENTH,       // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
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
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("status",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_DEFAULT_STATUS_LENTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
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

int ObInnerTableSchema::all_backup_backupset_job_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_BACKUP_BACKUPSET_JOB_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_BACKUP_BACKUPSET_JOB_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
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
    ADD_COLUMN_SCHEMA("incarnation",  // column_name
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
    ADD_COLUMN_SCHEMA("backup_set_id",  // column_name
        ++column_id,                    // column_id
        4,                              // rowkey_id
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
    ADD_COLUMN_SCHEMA("copy_id",  // column_name
        ++column_id,              // column_id
        5,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("backup_backupset_type",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        OB_INNER_TABLE_BACKUP_TYPE_LENTH,       // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
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
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("status",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_DEFAULT_STATUS_LENTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
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

}  // end namespace share
}  // end namespace oceanbase
