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

int ObInnerTableSchema::all_virtual_tenant_tablespace_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_TABLESPACE_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TABLESPACE_ID",  // column_name
        ++column_id,                    // column_id
        2,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObNumberType,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        38,                             // column_length
        38,                             // column_precision
        0,                              // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TABLESPACE_NAME",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_UTF8MB4_BIN,              // column_collation_type
        MAX_ORACLE_NAME_LENGTH,           // column_length
        2,                                // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ENCRYPTION_NAME",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_UTF8MB4_BIN,              // column_collation_type
        MAX_ORACLE_NAME_LENGTH,           // column_length
        2,                                // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ENCRYPT_KEY",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_BINARY,               // column_collation_type
        OB_MAX_MASTER_KEY_LENGTH,     // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MASTER_KEY_ID",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObNumberType,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        38,                             // column_length
        38,                             // column_precision
        0,                              // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_profile_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_PROFILE_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PROFILE_ID",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PROFILE_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_SQL_LENGTH,             // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("FAILED_LOGIN_ATTEMPTS",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObNumberType,                           // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        38,                                     // column_length
        38,                                     // column_precision
        0,                                      // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PASSWORD_LOCK_TIME",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObNumberType,                        // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        38,                                  // column_length
        38,                                  // column_precision
        0,                                   // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PASSWORD_VERIFY_FUNCTION",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObVarcharType,                             // column_type
        CS_TYPE_UTF8MB4_BIN,                       // column_collation_type
        MAX_ORACLE_NAME_LENGTH,                    // column_length
        2,                                         // column_precision
        -1,                                        // column_scale
        true,                                      // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PASSWORD_LIFE_TIME",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObNumberType,                        // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        38,                                  // column_length
        38,                                  // column_precision
        0,                                   // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PASSWORD_GRACE_TIME",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObNumberType,                         // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        38,                                   // column_length
        38,                                   // column_precision
        0,                                    // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PASSWORD_REUSE_TIME",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObNumberType,                         // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        38,                                   // column_length
        38,                                   // column_precision
        0,                                    // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PASSWORD_REUSE_MAX",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObNumberType,                        // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        38,                                  // column_length
        38,                                  // column_precision
        0,                                   // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("INACTIVE_ACCOUNT_TIME",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObNumberType,                           // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        38,                                     // column_length
        38,                                     // column_precision
        0,                                      // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_role_grantee_map_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_ROLE_GRANTEE_MAP_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GRANTEE_ID",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ROLE_ID",  // column_name
        ++column_id,              // column_id
        3,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ADMIN_OPTION",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DISABLE_FLAG",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_table_privilege_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TABLE_PRIVILEGE_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("USER_ID",  // column_name
        ++column_id,              // column_id
        2,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DATABASE_NAME",  // column_name
        ++column_id,                    // column_id
        3,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,    // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TABLE_NAME",     // column_name
        ++column_id,                    // column_id
        4,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_CORE_TALBE_NAME_LENGTH,  // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_ALTER",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_CREATE",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_DELETE",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_DROP",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_GRANT_OPTION",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObNumberType,                       // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        38,                                 // column_length
        38,                                 // column_precision
        0,                                  // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_INSERT",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_UPDATE",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_SELECT",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_INDEX",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_CREATE_VIEW",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObNumberType,                      // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        38,                                // column_length
        38,                                // column_precision
        0,                                 // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_SHOW_VIEW",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_security_audit_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("AUDIT_ID",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("AUDIT_TYPE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OWNER_ID",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OPERATION_TYPE",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IN_SUCCESS",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IN_FAILURE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_security_audit_history_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(
      combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_HISTORY_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_HISTORY_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("AUDIT_ID",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SCHEMA_VERSION",  // column_name
        ++column_id,                     // column_id
        3,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IS_DELETED",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("AUDIT_TYPE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OWNER_ID",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OPERATION_TYPE",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IN_SUCCESS",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IN_FAILURE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_trigger_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_TRIGGER_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TRIGGER_ID",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TRIGGER_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_TRIGGER_NAME_LENGTH,    // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DATABASE_ID",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OWNER_ID",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SCHEMA_VERSION",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TRIGGER_TYPE",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TRIGGER_EVENTS",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TIMING_POINTS",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObNumberType,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        38,                             // column_length
        38,                             // column_precision
        0,                              // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("BASE_OBJECT_TYPE",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObNumberType,                      // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        38,                                // column_length
        38,                                // column_precision
        0,                                 // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("BASE_OBJECT_ID",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TRIGGER_FLAGS",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObNumberType,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        38,                             // column_length
        38,                             // column_precision
        0,                              // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("UPDATE_COLUMNS",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_UPDATE_COLUMNS_LENGTH,    // column_length
        2,                               // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("REF_OLD_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_TRIGGER_NAME_LENGTH,    // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("REF_NEW_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_TRIGGER_NAME_LENGTH,    // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("REF_PARENT_NAME",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_UTF8MB4_BIN,              // column_collation_type
        OB_MAX_TRIGGER_NAME_LENGTH,       // column_length
        2,                                // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("WHEN_CONDITION",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_WHEN_CONDITION_LENGTH,    // column_length
        2,                               // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TRIGGER_BODY",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_TRIGGER_BODY_LENGTH,    // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PACKAGE_SPEC_SOURCE",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_UTF8MB4_BIN,                  // column_collation_type
        OB_MAX_TRIGGER_BODY_LENGTH,           // column_length
        2,                                    // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PACKAGE_BODY_SOURCE",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_UTF8MB4_BIN,                  // column_collation_type
        OB_MAX_TRIGGER_BODY_LENGTH,           // column_length
        2,                                    // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PACKAGE_FLAG",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PACKAGE_COMP_FLAG",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObNumberType,                       // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        38,                                 // column_length
        38,                                 // column_precision
        0,                                  // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PACKAGE_EXEC_ENV",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_UTF8MB4_BIN,               // column_collation_type
        OB_MAX_PROC_ENV_LENGTH,            // column_length
        2,                                 // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SQL_MODE",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_security_audit_record_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(
      combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_SECURITY_AUDIT_RECORD_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SVR_IP",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_UTF8MB4_BIN,     // column_collation_type
        MAX_IP_ADDR_LENGTH,      // column_length
        2,                       // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SVR_PORT",  // column_name
        ++column_id,               // column_id
        3,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("RECORD_TIMESTAMP_US",  // column_name
        ++column_id,                          // column_id
        4,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObTimestampLTZType,                   // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        0,                                    // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("USER_ID",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("USER_NAME",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        OB_MAX_USER_NAME_LENGTH,    // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("EFFECTIVE_USER_ID",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObNumberType,                       // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        38,                                 // column_length
        38,                                 // column_precision
        0,                                  // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("EFFECTIVE_USER_NAME",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_UTF8MB4_BIN,                  // column_collation_type
        OB_MAX_USER_NAME_LENGTH,              // column_length
        2,                                    // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CLIENT_IP",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_IP_ADDR_LENGTH,         // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("USER_CLIENT_IP",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        MAX_IP_ADDR_LENGTH,              // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PROXY_SESSION_ID",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObNumberType,                      // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        38,                                // column_length
        38,                                // column_precision
        0,                                 // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SESSION_ID",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ENTRY_ID",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("STATEMENT_ID",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TRANS_ID",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        512,                       // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COMMIT_VERSION",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TRACE_ID",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        64,                        // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DB_ID",  // column_name
        ++column_id,            // column_id
        0,                      // rowkey_id
        0,                      // index_id
        0,                      // part_key_pos
        ObNumberType,           // column_type
        CS_TYPE_INVALID,        // column_collation_type
        38,                     // column_length
        38,                     // column_precision
        0,                      // column_scale
        false,                  // is_nullable
        false);                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CUR_DB_ID",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SQL_TIMESTAMP_US",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObTimestampLTZType,                // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        0,                                 // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("AUDIT_ID",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("AUDIT_TYPE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OPERATION_TYPE",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ACTION_ID",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("RETURN_CODE",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJ_OWNER_NAME",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_USER_NAME_LENGTH,         // column_length
        2,                               // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJ_NAME",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,  // column_length
        2,                         // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("NEW_OBJ_OWNER_NAME",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_UTF8MB4_BIN,                 // column_collation_type
        OB_MAX_USER_NAME_LENGTH,             // column_length
        2,                                   // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("NEW_OBJ_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,      // column_length
        2,                             // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("AUTH_PRIVILEGES",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_UTF8MB4_BIN,              // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,         // column_length
        2,                                // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("AUTH_GRANTEE",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,      // column_length
        2,                             // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LOGOFF_LOGICAL_READ",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObNumberType,                         // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        38,                                   // column_length
        38,                                   // column_precision
        0,                                    // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LOGOFF_PHYSICAL_READ",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObNumberType,                          // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        38,                                    // column_length
        38,                                    // column_precision
        0,                                     // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LOGOFF_LOGICAL_WRITE",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObNumberType,                          // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        38,                                    // column_length
        38,                                    // column_precision
        0,                                     // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LOGOFF_LOCK_COUNT",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObNumberType,                       // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        38,                                 // column_length
        38,                                 // column_precision
        0,                                  // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LOGOFF_DEAD_LOCK",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_UTF8MB4_BIN,               // column_collation_type
        40,                                // column_length
        2,                                 // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LOGOFF_CPU_TIME_US",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObNumberType,                        // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        38,                                  // column_length
        38,                                  // column_precision
        0,                                   // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LOGOFF_EXEC_TIME_US",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObNumberType,                         // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        38,                                   // column_length
        38,                                   // column_precision
        0,                                    // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LOGOFF_ALIVE_TIME_US",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObNumberType,                          // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        38,                                    // column_length
        38,                                    // column_precision
        0,                                     // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COMMENT_TEXT",  // column_name
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
    ADD_COLUMN_SCHEMA("SQL_BIND",  // column_name
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
    ADD_COLUMN_SCHEMA("SQL_TEXT",  // column_name
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
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_sysauth_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_SYSAUTH_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GRANTEE_ID",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_ID",  // column_name
        ++column_id,              // column_id
        3,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_OPTION",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_sysauth_history_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_SYSAUTH_HISTORY_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_SYSAUTH_HISTORY_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GRANTEE_ID",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_ID",  // column_name
        ++column_id,              // column_id
        3,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SCHEMA_VERSION",  // column_name
        ++column_id,                     // column_id
        4,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IS_DELETED",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_OPTION",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_objauth_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(7);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_OBJAUTH_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJ_ID",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObNumberType,            // column_type
        CS_TYPE_INVALID,         // column_collation_type
        38,                      // column_length
        38,                      // column_precision
        0,                       // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJTYPE",  // column_name
        ++column_id,              // column_id
        3,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COL_ID",  // column_name
        ++column_id,             // column_id
        4,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObNumberType,            // column_type
        CS_TYPE_INVALID,         // column_collation_type
        38,                      // column_length
        38,                      // column_precision
        0,                       // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GRANTOR_ID",  // column_name
        ++column_id,                 // column_id
        5,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GRANTEE_ID",  // column_name
        ++column_id,                 // column_id
        6,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_ID",  // column_name
        ++column_id,              // column_id
        7,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_OPTION",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_objauth_history_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_OBJAUTH_HISTORY_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(8);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_OBJAUTH_HISTORY_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJ_ID",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObNumberType,            // column_type
        CS_TYPE_INVALID,         // column_collation_type
        38,                      // column_length
        38,                      // column_precision
        0,                       // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJTYPE",  // column_name
        ++column_id,              // column_id
        3,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COL_ID",  // column_name
        ++column_id,             // column_id
        4,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObNumberType,            // column_type
        CS_TYPE_INVALID,         // column_collation_type
        38,                      // column_length
        38,                      // column_precision
        0,                       // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GRANTOR_ID",  // column_name
        ++column_id,                 // column_id
        5,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GRANTEE_ID",  // column_name
        ++column_id,                 // column_id
        6,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_ID",  // column_name
        ++column_id,              // column_id
        7,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SCHEMA_VERSION",  // column_name
        ++column_id,                     // column_id
        8,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IS_DELETED",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_OPTION",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_error_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_ERROR_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJ_ID",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObNumberType,            // column_type
        CS_TYPE_INVALID,         // column_collation_type
        38,                      // column_length
        38,                      // column_precision
        0,                       // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJ_SEQ",  // column_name
        ++column_id,              // column_id
        3,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJ_TYPE",  // column_name
        ++column_id,               // column_id
        4,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LINE",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObNumberType,          // column_type
        CS_TYPE_INVALID,       // column_collation_type
        38,                    // column_length
        38,                    // column_precision
        0,                     // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("POSITION",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TEXT_LENGTH",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TEXT",       // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_ORACLE_COMMENT_LENGTH,  // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PROPERTY",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ERROR_NUMBER",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SCHEMA_VERSION",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_def_sub_part_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_DEF_SUB_PART_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        1,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TABLE_ID",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SUB_PART_ID",  // column_name
        ++column_id,                  // column_id
        3,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SUB_PART_NAME",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_PARTITION_NAME_LENGTH,   // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SCHEMA_VERSION",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("HIGH_BOUND_VAL",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,    // column_length
        2,                               // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("B_HIGH_BOUND_VAL",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_UTF8MB4_BIN,               // column_collation_type
        OB_MAX_B_HIGH_BOUND_VAL_LENGTH,    // column_length
        2,                                 // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("BLOCK_SIZE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("REPLICA_NUM",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COMPRESS_FUNC_NAME",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_UTF8MB4_BIN,                 // column_collation_type
        OB_MAX_COMPRESSOR_NAME_LENGTH,       // column_length
        2,                                   // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SPARE1",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObNumberType,            // column_type
        CS_TYPE_INVALID,         // column_collation_type
        38,                      // column_length
        38,                      // column_precision
        0,                       // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SPARE2",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObNumberType,            // column_type
        CS_TYPE_INVALID,         // column_collation_type
        38,                      // column_length
        38,                      // column_precision
        0,                       // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SPARE3",     // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        OB_OLD_MAX_VARCHAR_LENGTH,  // column_length
        2,                          // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COMMENT",          // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_UTF8MB4_BIN,              // column_collation_type
        OB_MAX_PARTITION_COMMENT_LENGTH,  // column_length
        2,                                // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LIST_VAL",      // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,  // column_length
        2,                             // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("B_LIST_VAL",      // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_B_PARTITION_EXPR_LENGTH,  // column_length
        2,                               // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SUB_PART_IDX",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SOURCE_PARTITION_ID",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_UTF8MB4_BIN,                  // column_collation_type
        MAX_VALUE_LENGTH,                     // column_length
        2,                                    // column_precision
        -1,                                   // column_scale
        true,                                 // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MAPPING_PG_SUB_PART_ID",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObNumberType,                            // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        38,                                      // column_length
        38,                                      // column_precision
        0,                                       // column_scale
        true,                                    // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TABLESPACE_ID",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObNumberType,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        38,                             // column_length
        38,                             // column_precision
        0,                              // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_object_type_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_OBJECT_TYPE_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJECT_TYPE_ID",  // column_name
        ++column_id,                     // column_id
        2,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TYPE",  // column_name
        ++column_id,           // column_id
        3,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObNumberType,          // column_type
        CS_TYPE_INVALID,       // column_collation_type
        38,                    // column_length
        38,                    // column_precision
        0,                     // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SCHEMA_VERSION",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PROPERTIES",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CHARSET_ID",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CHARSET_FORM",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LENGTH",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObNumberType,            // column_type
        CS_TYPE_INVALID,         // column_collation_type
        38,                      // column_length
        38,                      // column_precision
        0,                       // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("NUMBER_PRECISION",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObNumberType,                      // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        38,                                // column_length
        38,                                // column_precision
        0,                                 // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SCALE",  // column_name
        ++column_id,            // column_id
        0,                      // rowkey_id
        0,                      // index_id
        0,                      // part_key_pos
        ObNumberType,           // column_type
        CS_TYPE_INVALID,        // column_collation_type
        38,                     // column_length
        38,                     // column_precision
        0,                      // column_scale
        false,                  // is_nullable
        false);                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ZERO_FILL",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COLL_TYPE",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DATABASE_ID",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("FLAG",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObNumberType,          // column_type
        CS_TYPE_INVALID,       // column_collation_type
        38,                    // column_length
        38,                    // column_precision
        0,                     // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OWNER_ID",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COMP_FLAG",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJECT_NAME",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        OB_MAX_TABLE_TYPE_LENGTH,     // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("EXEC_ENV",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        OB_MAX_PROC_ENV_LENGTH,    // column_length
        2,                         // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SOURCE",  // column_name
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
    ADD_COLUMN_SCHEMA("COMMENT",    // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_TENANT_COMMENT_LENGTH,  // column_length
        2,                          // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ROUTE_SQL",  // column_name
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
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_dblink_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_DBLINK_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DBLINK_ID",  // column_name
        ++column_id,                // column_id
        2,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DBLINK_NAME",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        OB_MAX_DBLINK_NAME_LENGTH,    // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OWNER_ID",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("HOST_IP",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_UTF8MB4_BIN,      // column_collation_type
        MAX_IP_ADDR_LENGTH,       // column_length
        2,                        // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("HOST_PORT",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CLUSTER_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_CLUSTER_NAME_LENGTH,    // column_length
        2,                             // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_NAME",      // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_UTF8MB4_BIN,              // column_collation_type
        OB_MAX_TENANT_NAME_LENGTH_STORE,  // column_length
        2,                                // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("USER_NAME",      // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_USER_NAME_LENGTH_STORE,  // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PASSWORD",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        OB_MAX_PASSWORD_LENGTH,    // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_dblink_history_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_DBLINK_HISTORY_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_DBLINK_HISTORY_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DBLINK_ID",  // column_name
        ++column_id,                // column_id
        2,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SCHEMA_VERSION",  // column_name
        ++column_id,                     // column_id
        3,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IS_DELETED",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DBLINK_NAME",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        OB_MAX_DBLINK_NAME_LENGTH,    // column_length
        2,                            // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OWNER_ID",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("HOST_IP",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_UTF8MB4_BIN,      // column_collation_type
        MAX_IP_ADDR_LENGTH,       // column_length
        2,                        // column_precision
        -1,                       // column_scale
        true,                     // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("HOST_PORT",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CLUSTER_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_CLUSTER_NAME_LENGTH,    // column_length
        2,                             // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_NAME",      // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_UTF8MB4_BIN,              // column_collation_type
        OB_MAX_TENANT_NAME_LENGTH_STORE,  // column_length
        2,                                // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("USER_NAME",      // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_USER_NAME_LENGTH_STORE,  // column_length
        2,                              // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PASSWORD",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        OB_MAX_PASSWORD_LENGTH,    // column_length
        2,                         // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_constraint_column_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_CONSTRAINT_COLUMN_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_CONSTRAINT_COLUMN_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TABLE_ID",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CONSTRAINT_ID",  // column_name
        ++column_id,                    // column_id
        3,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObNumberType,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        38,                             // column_length
        38,                             // column_precision
        0,                              // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COLUMN_ID",  // column_name
        ++column_id,                // column_id
        4,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SCHEMA_VERSION",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_dependency_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DEP_OBJ_TYPE",  // column_name
        ++column_id,                   // column_id
        2,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DEP_OBJ_ID",  // column_name
        ++column_id,                 // column_id
        3,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DEP_ORDER",  // column_name
        ++column_id,                // column_id
        4,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SCHEMA_VERSION",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObNumberType,                    // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        38,                              // column_length
        38,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DEP_TIMESTAMP",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObNumberType,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        38,                             // column_length
        38,                             // column_precision
        0,                              // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("REF_OBJ_TYPE",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("REF_OBJ_ID",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("REF_TIMESTAMP",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObNumberType,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        38,                             // column_length
        38,                             // column_precision
        0,                              // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DEP_OBJ_OWNER_ID",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObNumberType,                      // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        38,                                // column_length
        38,                                // column_precision
        0,                                 // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PROPERTY",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DEP_ATTRS",         // column_name
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
    ADD_COLUMN_SCHEMA("DEP_REASON",        // column_name
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
    ADD_COLUMN_SCHEMA("REF_OBJ_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,      // column_length
        2,                             // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_meta_table_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_META_TABLE_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_META_TABLE_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TABLE_ID",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PARTITION_ID",  // column_name
        ++column_id,                   // column_id
        3,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SVR_IP",  // column_name
        ++column_id,             // column_id
        4,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_UTF8MB4_BIN,     // column_collation_type
        MAX_IP_ADDR_LENGTH,      // column_length
        2,                       // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SVR_PORT",  // column_name
        ++column_id,               // column_id
        5,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SQL_PORT",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("UNIT_ID",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PARTITION_CNT",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObNumberType,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        38,                             // column_length
        38,                             // column_precision
        0,                              // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ZONE",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObVarcharType,         // column_type
        CS_TYPE_UTF8MB4_BIN,   // column_collation_type
        MAX_ZONE_LENGTH,       // column_length
        2,                     // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ROLE",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObNumberType,          // column_type
        CS_TYPE_INVALID,       // column_collation_type
        38,                    // column_length
        38,                    // column_precision
        0,                     // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MEMBER_LIST",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        MAX_MEMBER_LIST_LENGTH,       // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ROW_COUNT",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DATA_SIZE",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DATA_VERSION",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DATA_CHECKSUM",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObNumberType,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        38,                             // column_length
        38,                             // column_precision
        0,                              // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ROW_CHECKSUM",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COLUMN_CHECKSUM",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_UTF8MB4_BIN,              // column_collation_type
        COLUMN_CHECKSUM_LENGTH,           // column_length
        2,                                // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IS_ORIGINAL_LEADER",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObNumberType,                        // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        38,                                  // column_length
        38,                                  // column_precision
        0,                                   // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IS_PREVIOUS_LEADER",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObNumberType,                        // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        38,                                  // column_length
        38,                                  // column_precision
        0,                                   // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CREATE_TIME",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObNumberType,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        38,                           // column_length
        38,                           // column_precision
        0,                            // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("REBUILD",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("REPLICA_TYPE",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("REQUIRED_SIZE",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObNumberType,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        38,                             // column_length
        38,                             // column_precision
        0,                              // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("STATUS",     // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_REPLICA_STATUS_LENGTH,  // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IS_RESTORE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PARTITION_CHECKSUM",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObNumberType,                        // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        38,                                  // column_length
        38,                                  // column_precision
        0,                                   // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("QUORUM",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObNumberType,            // column_type
        CS_TYPE_INVALID,         // column_collation_type
        38,                      // column_length
        38,                      // column_precision
        0,                       // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("FAIL_LIST",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        OB_MAX_FAILLIST_LENGTH,     // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("RECOVERY_TIMESTAMP",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObNumberType,                        // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        38,                                  // column_length
        38,                                  // column_precision
        0,                                   // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MEMSTORE_PERCENT",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObNumberType,                      // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        38,                                // column_length
        38,                                // column_precision
        0,                                 // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DATA_FILE_ID",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_tenant_time_zone_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_TIME_ZONE_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_TIME_ZONE_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TIME_ZONE_ID",  // column_name
        ++column_id,                   // column_id
        2,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("USE_LEAP_SECONDS",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_UTF8MB4_BIN,               // column_collation_type
        8,                                 // column_length
        2,                                 // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("VERSION",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
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

int ObInnerTableSchema::all_virtual_tenant_time_zone_name_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("NAME",  // column_name
        ++column_id,           // column_id
        2,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObVarcharType,         // column_type
        CS_TYPE_UTF8MB4_BIN,   // column_collation_type
        64,                    // column_length
        2,                     // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TIME_ZONE_ID",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("VERSION",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
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

int ObInnerTableSchema::all_virtual_tenant_time_zone_transition_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(
      combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TIME_ZONE_ID",  // column_name
        ++column_id,                   // column_id
        2,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TRANSITION_TIME",  // column_name
        ++column_id,                      // column_id
        3,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObNumberType,                     // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        38,                               // column_length
        38,                               // column_precision
        0,                                // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TRANSITION_TYPE_ID",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObNumberType,                        // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        38,                                  // column_length
        38,                                  // column_precision
        0,                                   // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("VERSION",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
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

int ObInnerTableSchema::all_virtual_tenant_time_zone_transition_type_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(
      combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_TIME_ZONE_TRANSITION_TYPE_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TIME_ZONE_ID",  // column_name
        ++column_id,                   // column_id
        2,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        38,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TRANSITION_TYPE_ID",  // column_name
        ++column_id,                         // column_id
        3,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObNumberType,                        // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        38,                                  // column_length
        38,                                  // column_precision
        0,                                   // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OFFSET",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObNumberType,            // column_type
        CS_TYPE_INVALID,         // column_collation_type
        38,                      // column_length
        38,                      // column_precision
        0,                       // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IS_DST",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObNumberType,            // column_type
        CS_TYPE_INVALID,         // column_collation_type
        38,                      // column_length
        38,                      // column_precision
        0,                       // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ABBREVIATION",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        8,                             // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("VERSION",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
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

int ObInnerTableSchema::all_virtual_res_mgr_plan_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_RES_MGR_PLAN_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_RES_MGR_PLAN_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PLAN",  // column_name
        ++column_id,           // column_id
        2,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObVarcharType,         // column_type
        CS_TYPE_UTF8MB4_BIN,   // column_collation_type
        128,                   // column_length
        2,                     // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COMMENTS",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        2000,                      // column_length
        2,                         // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_res_mgr_directive_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_RES_MGR_DIRECTIVE_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PLAN",              // column_name
        ++column_id,                       // column_id
        2,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_UTF8MB4_BIN,               // column_collation_type
        OB_MAX_RESOURCE_PLAN_NAME_LENGTH,  // column_length
        2,                                 // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GROUP_OR_SUBPLAN",  // column_name
        ++column_id,                       // column_id
        3,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_UTF8MB4_BIN,               // column_collation_type
        OB_MAX_RESOURCE_PLAN_NAME_LENGTH,  // column_length
        2,                                 // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COMMENTS",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        2000,                      // column_length
        2,                         // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MGMT_P1",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObNumberType,             // column_type
        CS_TYPE_INVALID,          // column_collation_type
        38,                       // column_length
        38,                       // column_precision
        0,                        // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("UTILIZATION_LIMIT",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObNumberType,                       // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        38,                                 // column_length
        38,                                 // column_precision
        0,                                  // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_trans_lock_stat_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TRANS_LOCK_STAT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TRANS_LOCK_STAT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TRANS_ID",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        512,                       // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SVR_IP",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        1,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_UTF8MB4_BIN,     // column_collation_type
        MAX_IP_ADDR_LENGTH,      // column_length
        2,                       // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SVR_PORT",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        2,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PARTITION",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        64,                         // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TABLE_ID",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObNumberType,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        38,                        // column_length
        38,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ROWKEY",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_UTF8MB4_BIN,     // column_collation_type
        512,                     // column_length
        2,                       // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SESSION_ID",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        38,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PROXY_ID",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        512,                       // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CTX_CREATE_TIME",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampLTZType,               // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        0,                                // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("EXPIRED_TIME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ROW_LOCK_ADDR",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObNumberType,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        38,                             // column_length
        38,                             // column_precision
        0,                              // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("hash (SVR_IP, SVR_PORT)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(65536);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_HASH);
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

int ObInnerTableSchema::all_virtual_res_mgr_mapping_rule_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_RES_MGR_MAPPING_RULE_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_RES_MGR_MAPPING_RULE_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ATTRIBUTE",         // column_name
        ++column_id,                       // column_id
        2,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_UTF8MB4_BIN,               // column_collation_type
        OB_MAX_RESOURCE_PLAN_NAME_LENGTH,  // column_length
        2,                                 // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("VALUE",             // column_name
        ++column_id,                       // column_id
        3,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_BINARY,                    // column_collation_type
        OB_MAX_RESOURCE_PLAN_NAME_LENGTH,  // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CONSUMER_GROUP",    // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_UTF8MB4_BIN,               // column_collation_type
        OB_MAX_RESOURCE_PLAN_NAME_LENGTH,  // column_length
        2,                                 // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("STATUS",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObNumberType,            // column_type
        CS_TYPE_INVALID,         // column_collation_type
        38,                      // column_length
        38,                      // column_precision
        0,                       // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

int ObInnerTableSchema::all_virtual_res_mgr_consumer_group_real_agent_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_RES_MGR_CONSUMER_GROUP_REAL_AGENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_RES_MGR_CONSUMER_GROUP_REAL_AGENT_ORA_TNAME))) {
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
  table_schema.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  table_schema.set_create_mem_version(1);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        1,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        38,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CONSUMER_GROUP",    // column_name
        ++column_id,                       // column_id
        2,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_UTF8MB4_BIN,               // column_collation_type
        OB_MAX_RESOURCE_PLAN_NAME_LENGTH,  // column_length
        2,                                 // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CONSUMER_GROUP_ID",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObNumberType,                       // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        38,                                 // column_length
        38,                                 // column_precision
        0,                                  // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COMMENTS",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        2000,                      // column_length
        2,                         // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_CREATE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampLTZType,          // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("GMT_MODIFIED",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObTimestampLTZType,            // column_type
        CS_TYPE_INVALID,               // column_collation_type
        0,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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

}  // end namespace share
}  // end namespace oceanbase
