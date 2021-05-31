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

int ObInnerTableSchema::session_variables_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_INFORMATION_SCHEMA_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_SESSION_VARIABLES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_SESSION_VARIABLES_TNAME))) {
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
    ObObj variable_name_default;
    variable_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("VARIABLE_NAME",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_SYS_PARAM_NAME_LENGTH,     // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        variable_name_default,
        variable_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj variable_value_default;
    variable_value_default.set_null();
    ADD_COLUMN_SCHEMA_T("VARIABLE_VALUE",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_MAX_SYS_PARAM_VALUE_LENGTH,     // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false,                             // is_autoincrement
        variable_value_default,
        variable_value_default);  // default_value
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

int ObInnerTableSchema::table_privileges_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_INFORMATION_SCHEMA_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_TABLE_PRIVILEGES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TABLE_PRIVILEGES_TNAME))) {
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
    ObObj grantee_default;
    grantee_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("GRANTEE",      // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_INFOSCHEMA_GRANTEE_LEN,  // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        grantee_default,
        grantee_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj table_catalog_default;
    table_catalog_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("TABLE_CATALOG",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        MAX_TABLE_CATALOG_LENGTH,         // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        table_catalog_default,
        table_catalog_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj table_schema_default;
    table_schema_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("TABLE_SCHEMA",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,     // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        table_schema_default,
        table_schema_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj table_name_default;
    table_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("TABLE_NAME",         // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        OB_MAX_INFOSCHEMA_TABLE_NAME_LENGTH,  // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false,                                // is_autoincrement
        table_name_default,
        table_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj privilege_type_default;
    privilege_type_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("PRIVILEGE_TYPE",        // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObVarcharType,                           // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        MAX_INFOSCHEMA_COLUMN_PRIVILEGE_LENGTH,  // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false,                                   // is_autoincrement
        privilege_type_default,
        privilege_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj is_grantable_default;
    is_grantable_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("IS_GRANTABLE",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        MAX_COLUMN_YES_NO_LENGTH,        // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        is_grantable_default,
        is_grantable_default);  // default_value
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

int ObInnerTableSchema::user_privileges_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_INFORMATION_SCHEMA_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_PRIVILEGES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_PRIVILEGES_TNAME))) {
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
    ObObj grantee_default;
    grantee_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("GRANTEE",      // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_INFOSCHEMA_GRANTEE_LEN,  // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        grantee_default,
        grantee_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj table_catalog_default;
    table_catalog_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("TABLE_CATALOG",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        MAX_TABLE_CATALOG_LENGTH,         // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        table_catalog_default,
        table_catalog_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj privilege_type_default;
    privilege_type_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("PRIVILEGE_TYPE",        // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObVarcharType,                           // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        MAX_INFOSCHEMA_COLUMN_PRIVILEGE_LENGTH,  // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false,                                   // is_autoincrement
        privilege_type_default,
        privilege_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj is_grantable_default;
    is_grantable_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("IS_GRANTABLE",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        MAX_COLUMN_YES_NO_LENGTH,        // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        is_grantable_default,
        is_grantable_default);  // default_value
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

int ObInnerTableSchema::schema_privileges_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_INFORMATION_SCHEMA_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_SCHEMA_PRIVILEGES_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_SCHEMA_PRIVILEGES_TNAME))) {
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
    ObObj grantee_default;
    grantee_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("GRANTEE",      // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_INFOSCHEMA_GRANTEE_LEN,  // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        grantee_default,
        grantee_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj table_catalog_default;
    table_catalog_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("TABLE_CATALOG",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        MAX_TABLE_CATALOG_LENGTH,         // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        table_catalog_default,
        table_catalog_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj table_schema_default;
    table_schema_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("TABLE_SCHEMA",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,     // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        table_schema_default,
        table_schema_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj privilege_type_default;
    privilege_type_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("PRIVILEGE_TYPE",        // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObVarcharType,                           // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        MAX_INFOSCHEMA_COLUMN_PRIVILEGE_LENGTH,  // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false,                                   // is_autoincrement
        privilege_type_default,
        privilege_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj is_grantable_default;
    is_grantable_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("IS_GRANTABLE",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        MAX_COLUMN_YES_NO_LENGTH,        // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        is_grantable_default,
        is_grantable_default);  // default_value
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

int ObInnerTableSchema::table_constraints_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_INFORMATION_SCHEMA_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_TABLE_CONSTRAINTS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TABLE_CONSTRAINTS_TNAME))) {
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
    ObObj constraint_catalog_default;
    constraint_catalog_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("CONSTRAINT_CATALOG",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObVarcharType,                         // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        MAX_TABLE_CATALOG_LENGTH,              // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false,                                 // is_autoincrement
        constraint_catalog_default,
        constraint_catalog_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj constraint_schema_default;
    constraint_schema_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("CONSTRAINT_SCHEMA",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,          // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false,                                // is_autoincrement
        constraint_schema_default,
        constraint_schema_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj constraint_name_default;
    constraint_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("CONSTRAINT_NAME",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        OB_MAX_COLUMN_NAME_LENGTH,          // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false,                              // is_autoincrement
        constraint_name_default,
        constraint_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj table_schema_default;
    table_schema_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("TABLE_SCHEMA",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,     // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        table_schema_default,
        table_schema_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj table_name_default;
    table_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("TABLE_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,      // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false,                         // is_autoincrement
        table_name_default,
        table_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj constraint_type_default;
    constraint_type_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("CONSTRAINT_TYPE",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        INDEX_NULL_LENGTH,                  // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false,                              // is_autoincrement
        constraint_type_default,
        constraint_type_default);  // default_value
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

int ObInnerTableSchema::global_status_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_INFORMATION_SCHEMA_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_GLOBAL_STATUS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_GLOBAL_STATUS_TNAME))) {
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
    ObObj variable_name_default;
    variable_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("VARIABLE_NAME",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_SYS_PARAM_NAME_LENGTH,     // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        variable_name_default,
        variable_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj variable_value_default;
    variable_value_default.set_null();
    ADD_COLUMN_SCHEMA_T("VARIABLE_VALUE",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_MAX_SYS_PARAM_VALUE_LENGTH,     // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false,                             // is_autoincrement
        variable_value_default,
        variable_value_default);  // default_value
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

int ObInnerTableSchema::partitions_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_INFORMATION_SCHEMA_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_PARTITIONS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_PARTITIONS_TNAME))) {
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
    ObObj table_catalog_default;
    table_catalog_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("TABLE_CATALOG",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        MAX_TABLE_CATALOG_LENGTH,         // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        table_catalog_default,
        table_catalog_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj table_schema_default;
    table_schema_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("TABLE_SCHEMA",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,     // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        table_schema_default,
        table_schema_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj table_name_default;
    table_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("TABLE_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,      // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false,                         // is_autoincrement
        table_name_default,
        table_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PARTITION_NAME",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_PARTITION_NAME_LENGTH,    // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SUBPARTITION_NAME",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        OB_MAX_PARTITION_NAME_LENGTH,       // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PARTITION_ORDINAL_POSITION",  // column_name
        ++column_id,                                 // column_id
        0,                                           // rowkey_id
        0,                                           // index_id
        0,                                           // part_key_pos
        ObUInt64Type,                                // column_type
        CS_TYPE_INVALID,                             // column_collation_type
        sizeof(uint64_t),                            // column_length
        -1,                                          // column_precision
        -1,                                          // column_scale
        true,                                        // is_nullable
        false);                                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SUBPARTITION_ORDINAL_POSITION",  // column_name
        ++column_id,                                    // column_id
        0,                                              // rowkey_id
        0,                                              // index_id
        0,                                              // part_key_pos
        ObUInt64Type,                                   // column_type
        CS_TYPE_INVALID,                                // column_collation_type
        sizeof(uint64_t),                               // column_length
        -1,                                             // column_precision
        -1,                                             // column_scale
        true,                                           // is_nullable
        false);                                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PARTITION_METHOD",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_MAX_PARTITION_METHOD_LENGTH,    // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SUBPARTITION_METHOD",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        OB_MAX_PARTITION_METHOD_LENGTH,       // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        true,                                 // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PARTITION_EXPRESSION",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObVarcharType,                         // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        OB_MAX_PART_FUNC_EXPR_LENGTH,          // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        true,                                  // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SUBPARTITION_EXPRESSION",  // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObVarcharType,                            // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        OB_MAX_PART_FUNC_EXPR_LENGTH,             // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        true,                                     // is_nullable
        false);                                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PARTITION_DESCRIPTION",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        OB_MAX_PARTITION_DESCRIPTION_LENGTH,    // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj table_rows_default;
    table_rows_default.set_uint64(0);
    ADD_COLUMN_SCHEMA_T("TABLE_ROWS",  // column_name
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
        false,                         // is_autoincrement
        table_rows_default,
        table_rows_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj avg_row_length_default;
    avg_row_length_default.set_uint64(0);
    ADD_COLUMN_SCHEMA_T("AVG_ROW_LENGTH",  // column_name
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
        false,                             // is_autoincrement
        avg_row_length_default,
        avg_row_length_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj data_length_default;
    data_length_default.set_uint64(0);
    ADD_COLUMN_SCHEMA_T("DATA_LENGTH",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObUInt64Type,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(uint64_t),               // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        data_length_default,
        data_length_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MAX_DATA_LENGTH",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObUInt64Type,                     // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(uint64_t),                 // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj index_length_default;
    index_length_default.set_uint64(0);
    ADD_COLUMN_SCHEMA_T("INDEX_LENGTH",  // column_name
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
        false,                           // is_autoincrement
        index_length_default,
        index_length_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj data_free_default;
    data_free_default.set_uint64(0);
    ADD_COLUMN_SCHEMA_T("DATA_FREE",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObUInt64Type,                 // column_type
        CS_TYPE_INVALID,              // column_collation_type
        sizeof(uint64_t),             // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        data_free_default,
        data_free_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("CREATE_TIME",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObTimestampType,                 // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(ObPreciseDateTime),       // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false,                           // is_autoincrement
        false);                          // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("UPDATE_TIME",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObTimestampType,                 // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(ObPreciseDateTime),       // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false,                           // is_autoincrement
        false);                          // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("CHECK_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("CHECKSUM",  // column_name
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
    ObObj partition_comment_default;
    partition_comment_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("PARTITION_COMMENT",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        OB_MAX_PARTITION_COMMENT_LENGTH,      // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false,                                // is_autoincrement
        partition_comment_default,
        partition_comment_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj nodegroup_default;
    nodegroup_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("NODEGROUP",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_NODEGROUP_LENGTH,      // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        nodegroup_default,
        nodegroup_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TABLESPACE_NAME",  // column_name
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

int ObInnerTableSchema::session_status_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_INFORMATION_SCHEMA_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_SESSION_STATUS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_SESSION_STATUS_TNAME))) {
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
    ObObj variable_name_default;
    variable_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("VARIABLE_NAME",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_SYS_PARAM_NAME_LENGTH,     // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        variable_name_default,
        variable_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj variable_value_default;
    variable_value_default.set_null();
    ADD_COLUMN_SCHEMA_T("VARIABLE_VALUE",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        OB_MAX_SYS_PARAM_VALUE_LENGTH,     // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false,                             // is_autoincrement
        variable_value_default,
        variable_value_default);  // default_value
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

int ObInnerTableSchema::user_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_MYSQL_SCHEMA_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_USER_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_USER_TNAME))) {
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
    ADD_COLUMN_SCHEMA("host",     // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_MAX_HOST_NAME_LENGTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("user",           // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_USER_NAME_LENGTH_STORE,  // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("password",  // column_name
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
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("select_priv",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        1,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("insert_priv",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        1,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("update_priv",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        1,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("delete_priv",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        1,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_priv",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        1,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("drop_priv",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        1,                          // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("reload_priv",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        1,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("shutdown_priv",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        1,                              // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("process_priv",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        1,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("file_priv",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        1,                          // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("grant_priv",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        1,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("reference_priv",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        1,                               // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("index_priv",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        1,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("alter_priv",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        1,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("show_db_priv",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        1,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("super_priv",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        1,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_tmp_table_priv",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        1,                                      // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lock_tables_priv",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        1,                                 // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("execute_priv",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        1,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("repl_slave_priv",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        1,                                // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("repl_client_priv",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        1,                                 // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_view_priv",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        1,                                 // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("show_view_priv",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        1,                               // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_routine_priv",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        1,                                    // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("alter_routine_priv",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        1,                                   // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_user_priv",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        1,                                 // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("event_priv",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        1,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trigger_priv",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        1,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_tablespace_priv",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObVarcharType,                           // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        1,                                       // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj ssl_type_default;
    ssl_type_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("ssl_type",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        10,                          // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false,                       // is_autoincrement
        ssl_type_default,
        ssl_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj ssl_cipher_default;
    ssl_cipher_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("ssl_cipher",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        1024,                          // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false,                         // is_autoincrement
        ssl_cipher_default,
        ssl_cipher_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj x509_issuer_default;
    x509_issuer_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("x509_issuer",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        1024,                           // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        x509_issuer_default,
        x509_issuer_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj x509_subject_default;
    x509_subject_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("x509_subject",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        1024,                            // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        x509_subject_default,
        x509_subject_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj max_questions_default;
    max_questions_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("max_questions",  // column_name
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
        max_questions_default,
        max_questions_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj max_updates_default;
    max_updates_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("max_updates",  // column_name
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
        max_updates_default,
        max_updates_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj max_connections_default;
    max_connections_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("max_connections",  // column_name
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
        max_connections_default,
        max_connections_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj max_user_connections_default;
    max_user_connections_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("max_user_connections",  // column_name
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
        max_user_connections_default,
        max_user_connections_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("plugin",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        1024,                    // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("authentication_string",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        1024,                                   // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("password_expired",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        1,                                 // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
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

int ObInnerTableSchema::db_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_MYSQL_SCHEMA_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_DB_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_DB_TNAME))) {
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
    ADD_COLUMN_SCHEMA("host",     // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_MAX_HOST_NAME_LENGTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("db",           // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,  // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("user",           // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_USER_NAME_LENGTH_STORE,  // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("select_priv",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        1,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("insert_priv",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        1,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("update_priv",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        1,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("delete_priv",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        1,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_priv",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        1,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("drop_priv",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        1,                          // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("grant_priv",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        1,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("reference_priv",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        1,                               // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("index_priv",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        1,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("alter_priv",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        1,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_tmp_table_priv",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        1,                                      // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lock_tables_priv",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        1,                                 // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_view_priv",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        1,                                 // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("show_view_priv",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        1,                               // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("create_routine_priv",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        1,                                    // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("alter_routine_priv",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        1,                                   // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("execute_priv",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        1,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("event_priv",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        1,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trigger_priv",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        1,                             // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
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

int ObInnerTableSchema::all_virtual_server_memory_info_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SERVER_MEMORY_INFO_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SERVER_MEMORY_INFO_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        1,                       // rowkey_id
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
        2,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("server_memory_hold",  // column_name
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
    ADD_COLUMN_SCHEMA("server_memory_limit",  // column_name
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
    ADD_COLUMN_SCHEMA("system_reserved",  // column_name
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
    ADD_COLUMN_SCHEMA("active_memstore_used",  // column_name
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
    ADD_COLUMN_SCHEMA("total_memstore_used",  // column_name
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
    ADD_COLUMN_SCHEMA("major_freeze_trigger",  // column_name
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
    ADD_COLUMN_SCHEMA("memstore_limit",  // column_name
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
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("hash (addr_to_partition_id(svr_ip, svr_port))"))) {
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

int ObInnerTableSchema::all_virtual_partition_table_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PARTITION_TABLE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PARTITION_TABLE_TNAME))) {
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
    ObObj to_leader_time_default;
    to_leader_time_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("to_leader_time",  // column_name
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
        false,                             // is_autoincrement
        to_leader_time_default,
        to_leader_time_default);  // default_value
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

int ObInnerTableSchema::all_virtual_lock_wait_stat_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_LOCK_WAIT_STAT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_LOCK_WAIT_STAT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
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
        0,                         // rowkey_id
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
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("rowkey",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        512,                     // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("addr",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObUInt64Type,          // column_type
        CS_TYPE_INVALID,       // column_collation_type
        sizeof(uint64_t),      // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("need_wait",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObTinyIntType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        1,                          // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("recv_ts",  // column_name
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
    ADD_COLUMN_SCHEMA("lock_ts",  // column_name
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
    ADD_COLUMN_SCHEMA("abs_timeout",  // column_name
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
    ADD_COLUMN_SCHEMA("try_lock_times",  // column_name
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
    ADD_COLUMN_SCHEMA("time_after_recv",  // column_name
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
    ADD_COLUMN_SCHEMA("session_id",  // column_name
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
    ADD_COLUMN_SCHEMA("block_session_id",  // column_name
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
    ADD_COLUMN_SCHEMA("lock_mode",  // column_name
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
    ADD_COLUMN_SCHEMA("total_update_cnt",  // column_name
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
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("hash (addr_to_partition_id(svr_ip, svr_port))"))) {
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

int ObInnerTableSchema::all_virtual_partition_item_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PARTITION_ITEM_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PARTITION_ITEM_TNAME))) {
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
        false,                     // is_nullable
        false);                    // is_autoincrement
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
        false,                         // is_nullable
        false);                        // is_autoincrement
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
    ADD_COLUMN_SCHEMA("table_name",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,    // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_level",  // column_name
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
    ADD_COLUMN_SCHEMA("partition_num",  // column_name
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
    ADD_COLUMN_SCHEMA("partition_idx",  // column_name
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
    ADD_COLUMN_SCHEMA("part_func_type",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        5,                               // column_length
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
    ADD_COLUMN_SCHEMA("part_name",     // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_PARTITION_NAME_LENGTH,  // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("part_idx",  // column_name
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
    ADD_COLUMN_SCHEMA("part_id",  // column_name
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
    ADD_COLUMN_SCHEMA("part_high_bound",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,     // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("subpart_func_type",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        5,                                  // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("subpart_num",  // column_name
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
    ADD_COLUMN_SCHEMA("subpart_name",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_PARTITION_NAME_LENGTH,  // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("subpart_idx",  // column_name
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
    ADD_COLUMN_SCHEMA("subpart_id",  // column_name
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
    ADD_COLUMN_SCHEMA("subpart_high_bound",  // column_name
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

int ObInnerTableSchema::all_virtual_replica_task_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_REPLICA_TASK_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_REPLICA_TASK_TNAME))) {
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
        false,                     // is_nullable
        false);                    // is_autoincrement
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
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("src_ip",  // column_name
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
    ADD_COLUMN_SCHEMA("src_port",  // column_name
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
    ADD_COLUMN_SCHEMA("src_replica_type",  // column_name
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
    ADD_COLUMN_SCHEMA("region",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        MAX_REGION_LENGTH,       // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("dst_ip",  // column_name
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
    ADD_COLUMN_SCHEMA("dst_port",  // column_name
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
    ADD_COLUMN_SCHEMA("dst_replica_type",  // column_name
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
    ADD_COLUMN_SCHEMA("cmd_type",           // column_name
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
    ObObj comment_default;
    comment_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("comment",                // column_name
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
        comment_default,
        comment_default);  // default_value
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

int ObInnerTableSchema::all_virtual_replica_task_all_virtual_replica_task_i1_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID)));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_REPLICA_TASK_ALL_VIRTUAL_REPLICA_TASK_I1_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_REPLICA_TASK_ALL_VIRTUAL_REPLICA_TASK_I1_TNAME))) {
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
  table_schema.set_index_using_type(USING_HASH);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        column_id + 1,              // column_id
        1,                          // rowkey_id
        1,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_REPLICA_TASK_TID));

  table_schema.set_max_used_column_id(column_id + 1);
  return ret;
}

int ObInnerTableSchema::all_virtual_partition_location_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PARTITION_LOCATION_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PARTITION_LOCATION_TNAME))) {
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

int ObInnerTableSchema::proc_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_MYSQL_SCHEMA_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_PROC_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_PROC_TNAME))) {
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
    ADD_COLUMN_SCHEMA("db",           // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,  // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("name",        // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        OB_MAX_ROUTINE_NAME_LENGTH,  // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("type",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObVarcharType,         // column_type
        CS_TYPE_INVALID,       // column_collation_type
        10,                    // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("specific_name",        // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        OB_MAX_INFOSCHEMA_TABLE_NAME_LENGTH,  // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj language_default;
    language_default.set_varchar(ObString::make_string("SQL"));
    ADD_COLUMN_SCHEMA_T("language",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        4,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false,                       // is_autoincrement
        language_default,
        language_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj sql_data_access_default;
    sql_data_access_default.set_varchar(ObString::make_string("CONTAINS_SQL"));
    ADD_COLUMN_SCHEMA_T("sql_data_access",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        32,                                 // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false,                              // is_autoincrement
        sql_data_access_default,
        sql_data_access_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj is_deterministic_default;
    is_deterministic_default.set_varchar(ObString::make_string("NO"));
    ADD_COLUMN_SCHEMA_T("is_deterministic",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        4,                                   // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false,                               // is_autoincrement
        is_deterministic_default,
        is_deterministic_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj security_type_default;
    security_type_default.set_varchar(ObString::make_string("DEFINER"));
    ADD_COLUMN_SCHEMA_T("security_type",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        10,                               // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        security_type_default,
        security_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj param_list_default;
    param_list_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("param_list",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_VARCHAR_LENGTH,         // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false,                         // is_autoincrement
        param_list_default,
        param_list_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj returns_default;
    returns_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("returns",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_MAX_VARCHAR_LENGTH,      // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false,                      // is_autoincrement
        returns_default,
        returns_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj body_default;
    body_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("body",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        OB_MAX_VARCHAR_LENGTH,   // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false,                   // is_autoincrement
        body_default,
        body_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj definer_default;
    definer_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("definer",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        77,                         // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false,                      // is_autoincrement
        definer_default,
        definer_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("created",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTimestampType,             // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(ObPreciseDateTime),   // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false,                       // is_autoincrement
        false);                      // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("modified",  // column_name
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
    ObObj sql_mode_default;
    sql_mode_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("sql_mode",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        32,                          // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false,                       // is_autoincrement
        sql_mode_default,
        sql_mode_default);  // default_value
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
        OB_MAX_VARCHAR_LENGTH,      // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false,                      // is_autoincrement
        comment_default,
        comment_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("character_set_client",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObVarcharType,                         // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        MAX_CHARSET_LENGTH,                    // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("collation_connection",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObVarcharType,                         // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        MAX_CHARSET_LENGTH,                    // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("collation_database",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        MAX_CHARSET_LENGTH,                  // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("body_utf8",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_MAX_VARCHAR_LENGTH,      // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
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

int ObInnerTableSchema::tenant_virtual_collation_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_TENANT_VIRTUAL_COLLATION_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_COLLATION_TNAME))) {
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
    ObObj collation_default;
    collation_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("collation",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        MAX_COLLATION_LENGTH,         // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false,                        // is_autoincrement
        collation_default,
        collation_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj charset_default;
    charset_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("charset",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_CHARSET_LENGTH,         // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false,                      // is_autoincrement
        charset_default,
        charset_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj id_default;
    id_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("id",  // column_name
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
        false,                 // is_autoincrement
        id_default,
        id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj is_default_default;
    is_default_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("is_default",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        MAX_BOOL_STR_LENGTH,           // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false,                         // is_autoincrement
        is_default_default,
        is_default_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj is_compiled_default;
    is_compiled_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("is_compiled",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        MAX_BOOL_STR_LENGTH,            // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        is_compiled_default,
        is_compiled_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj sortlen_default;
    sortlen_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("sortlen",  // column_name
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
        sortlen_default,
        sortlen_default);  // default_value
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

int ObInnerTableSchema::tenant_virtual_charset_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_TENANT_VIRTUAL_CHARSET_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_CHARSET_TNAME))) {
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
    ObObj charset_default;
    charset_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("charset",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_CHARSET_LENGTH,         // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false,                      // is_autoincrement
        charset_default,
        charset_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj description_default;
    description_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("description",   // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        MAX_CHARSET_DESCRIPTION_LENGTH,  // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        description_default,
        description_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj default_collation_default;
    default_collation_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("default_collation",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        MAX_COLLATION_LENGTH,                 // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false,                                // is_autoincrement
        default_collation_default,
        default_collation_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj max_length_default;
    max_length_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("max_length",  // column_name
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
        max_length_default,
        max_length_default);  // default_value
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

int ObInnerTableSchema::all_virtual_tenant_memstore_allocator_info_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
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
        0,                         // rowkey_id
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
        false,                     // is_nullable
        false);                    // is_autoincrement
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
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("mt_base_version",  // column_name
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
    ADD_COLUMN_SCHEMA("retire_clock",  // column_name
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
    ADD_COLUMN_SCHEMA("mt_is_frozen",  // column_name
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
    ADD_COLUMN_SCHEMA("mt_protection_clock",  // column_name
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
    ADD_COLUMN_SCHEMA("mt_snapshot_version",  // column_name
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
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("hash (addr_to_partition_id(svr_ip, svr_port))"))) {
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

int ObInnerTableSchema::all_virtual_table_mgr_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TABLE_MGR_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TABLE_MGR_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
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
        0,                         // rowkey_id
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
        false,                       // is_nullable
        false);                      // is_autoincrement
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
        false,                     // is_nullable
        false);                    // is_autoincrement
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
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("index_id",  // column_name
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
    ADD_COLUMN_SCHEMA("base_version",  // column_name
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
    ADD_COLUMN_SCHEMA("multi_version_start",  // column_name
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
    ADD_COLUMN_SCHEMA("max_merged_version",  // column_name
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
    ADD_COLUMN_SCHEMA("upper_trans_version",  // column_name
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
    ADD_COLUMN_SCHEMA("start_log_ts",  // column_name
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
    ADD_COLUMN_SCHEMA("end_log_ts",  // column_name
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
    ADD_COLUMN_SCHEMA("max_log_ts",  // column_name
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
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("logical_data_version",  // column_name
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
    ADD_COLUMN_SCHEMA("size",  // column_name
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
    ADD_COLUMN_SCHEMA("compact_row",  // column_name
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
    ADD_COLUMN_SCHEMA("is_active",  // column_name
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
    ADD_COLUMN_SCHEMA("timestamp",  // column_name
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
    ADD_COLUMN_SCHEMA("ref",  // column_name
        ++column_id,          // column_id
        0,                    // rowkey_id
        0,                    // index_id
        0,                    // part_key_pos
        ObIntType,            // column_type
        CS_TYPE_INVALID,      // column_collation_type
        sizeof(int64_t),      // column_length
        -1,                   // column_precision
        -1,                   // column_scale
        false,                // is_nullable
        false);               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("write_ref",  // column_name
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
    ADD_COLUMN_SCHEMA("trx_count",  // column_name
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
    ADD_COLUMN_SCHEMA("pending_log_persisting_row_cnt",  // column_name
        ++column_id,                                     // column_id
        0,                                               // rowkey_id
        0,                                               // index_id
        0,                                               // part_key_pos
        ObIntType,                                       // column_type
        CS_TYPE_INVALID,                                 // column_collation_type
        sizeof(int64_t),                                 // column_length
        -1,                                              // column_precision
        -1,                                              // column_scale
        false,                                           // is_nullable
        false);                                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("contain_uncommitted_row",  // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObTinyIntType,                            // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        1,                                        // column_length
        -1,                                       // column_precision
        -1,                                       // column_scale
        false,                                    // is_nullable
        false);                                   // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("hash (addr_to_partition_id(svr_ip, svr_port))"))) {
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

int ObInnerTableSchema::all_virtual_meta_table_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_META_TABLE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_META_TABLE_TNAME))) {
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
    ADD_COLUMN_SCHEMA_TS("gmt_create",  // column_name
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
    ADD_COLUMN_SCHEMA_TS("gmt_modified",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(ObPreciseDateTime),        // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        false);                           // is_on_update_for_timestamp
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

int ObInnerTableSchema::all_virtual_freeze_info_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_FREEZE_INFO_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_FREEZE_INFO_TNAME))) {
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
    ADD_COLUMN_SCHEMA("frozen_version",  // column_name
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
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("frozen_timestamp",  // column_name
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
        true,                            // is_nullable
        false);                          // is_autoincrement
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

int ObInnerTableSchema::parameters_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_INFORMATION_SCHEMA_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_PARAMETERS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_PARAMETERS_TNAME))) {
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
    ObObj specific_catalog_default;
    specific_catalog_default.set_varchar(ObString::make_string("def"));
    ADD_COLUMN_SCHEMA_T("SPECIFIC_CATALOG",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        MAX_TABLE_CATALOG_LENGTH,            // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false,                               // is_autoincrement
        specific_catalog_default,
        specific_catalog_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj specific_schema_default;
    specific_schema_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("SPECIFIC_SCHEMA",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,        // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false,                              // is_autoincrement
        specific_schema_default,
        specific_schema_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj specific_name_default;
    specific_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("SPECIFIC_NAME",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_PARAMETERS_NAME_LENGTH,    // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        specific_name_default,
        specific_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj ordinal_position_default;
    ordinal_position_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("ORDINAL_POSITION",  // column_name
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
        ordinal_position_default,
        ordinal_position_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PARAMETER_MODE",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_PARAMETERS_NAME_LENGTH,   // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PARAMETER_NAME",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_DEFAULT_VALUE_LENGTH,     // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj data_type_default;
    data_type_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("DATA_TYPE",    // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_PARAMETERS_NAME_LENGTH,  // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false,                          // is_autoincrement
        data_type_default,
        data_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CHARACTER_MAXIMUM_LENGTH",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObUInt64Type,                              // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(uint64_t),                          // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        true,                                      // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CHARACTER_OCTET_LENGTH",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObUInt64Type,                            // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(uint64_t),                        // column_length
        -1,                                      // column_precision
        -1,                                      // column_scale
        true,                                    // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("NUMERIC_PRECISION",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObUInt64Type,                       // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(uint64_t),                   // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("NUMERIC_SCALE",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObUInt64Type,                   // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(uint64_t),               // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DATETIME_PRECISION",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObUInt64Type,                        // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        sizeof(uint64_t),                    // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CHARACTER_SET_NAME",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        MAX_CHARSET_LENGTH,                  // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COLLATION_NAME",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        MAX_COLLATION_LENGTH,            // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DTD_IDENTIFIER",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        OB_MAX_SYS_PARAM_NAME_LENGTH,    // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj routine_type_default;
    routine_type_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("ROUTINE_TYPE",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        9,                               // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        routine_type_default,
        routine_type_default);  // default_value
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

int ObInnerTableSchema::all_virtual_bad_block_table_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_BAD_BLOCK_TABLE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_BAD_BLOCK_TABLE_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
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
        0,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("disk_id",  // column_name
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
    ADD_COLUMN_SCHEMA("store_file_path",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        MAX_PATH_SIZE,                    // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("macro_block_index",  // column_name
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
    ADD_COLUMN_SCHEMA("error_type",  // column_name
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
    ADD_COLUMN_SCHEMA_TS("check_time",  // column_name
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
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("hash (addr_to_partition_id(svr_ip, svr_port))"))) {
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

int ObInnerTableSchema::all_virtual_px_worker_stat_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PX_WORKER_STAT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PX_WORKER_STAT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("session_id",  // column_name
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
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
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
        0,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("trace_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        OB_MAX_HOST_NAME_LENGTH,   // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("qc_id",  // column_name
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
    ADD_COLUMN_SCHEMA("sqc_id",  // column_name
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
    ADD_COLUMN_SCHEMA("worker_id",  // column_name
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
    ADD_COLUMN_SCHEMA("dfo_id",  // column_name
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
    ADD_COLUMN_SCHEMA("thread_id",  // column_name
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
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("hash (addr_to_partition_id(svr_ip, svr_port))"))) {
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

int ObInnerTableSchema::all_virtual_trans_audit_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TRANS_AUDIT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TRANS_AUDIT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
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
        0,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("pkey",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
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
    ADD_COLUMN_SCHEMA("session_id",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObIntType,                   // column_type
        CS_TYPE_INVALID,             // column_collation_type
        sizeof(int64_t),             // column_length
        20,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("proxy_id",  // column_name
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
    ADD_COLUMN_SCHEMA("trans_type",  // column_name
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
    ADD_COLUMN_SCHEMA_TS("ctx_create_time",  // column_name
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
    ADD_COLUMN_SCHEMA_TS("expired_time",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampType,                  // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        sizeof(ObPreciseDateTime),        // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false,                            // is_autoincrement
        false);                           // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trans_param",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        64,                           // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("total_sql_no",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObIntType,                     // column_type
        CS_TYPE_INVALID,               // column_collation_type
        sizeof(int64_t),               // column_length
        20,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("refer",  // column_name
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
    ADD_COLUMN_SCHEMA("prev_trans_arr",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        512,                             // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("next_trans_arr",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        512,                             // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ctx_addr",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        20,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ctx_type",  // column_name
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
    ADD_COLUMN_SCHEMA("trace_log",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        2048,                       // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
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
    ADD_COLUMN_SCHEMA("for_replay",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObTinyIntType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        1,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("hash (addr_to_partition_id(svr_ip, svr_port))"))) {
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

int ObInnerTableSchema::all_virtual_trans_sql_audit_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TRANS_SQL_AUDIT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TRANS_SQL_AUDIT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
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
        0,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("pkey",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
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
    ADD_COLUMN_SCHEMA("sql_no",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObIntType,               // column_type
        CS_TYPE_INVALID,         // column_collation_type
        sizeof(int64_t),         // column_length
        20,                      // column_precision
        0,                       // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
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
    ADD_COLUMN_SCHEMA("phy_plan_type",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        20,                             // column_precision
        0,                              // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("proxy_receive_us",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObIntType,                         // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(int64_t),                   // column_length
        20,                                // column_precision
        0,                                 // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("server_receive_us",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObIntType,                          // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(int64_t),                    // column_length
        20,                                 // column_precision
        0,                                  // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trans_receive_us",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObIntType,                         // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(int64_t),                   // column_length
        20,                                // column_precision
        0,                                 // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("trans_execute_us",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObIntType,                         // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(int64_t),                   // column_length
        20,                                // column_precision
        0,                                 // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("lock_for_read_cnt",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObIntType,                          // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(int64_t),                    // column_length
        20,                                 // column_precision
        0,                                  // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ctx_addr",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObIntType,                 // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(int64_t),           // column_length
        20,                        // column_precision
        0,                         // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("hash (addr_to_partition_id(svr_ip, svr_port))"))) {
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

int ObInnerTableSchema::all_virtual_weak_read_stat_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_WEAK_READ_STAT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_WEAK_READ_STAT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("tenant_id",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObIntType,                  // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(int64_t),            // column_length
        20,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
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
        0,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("server_version",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObIntType,                       // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(int64_t),                 // column_length
        20,                              // column_precision
        0,                               // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("server_version_delta",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObIntType,                             // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        sizeof(int64_t),                       // column_length
        20,                                    // column_precision
        0,                                     // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("local_cluster_version",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        20,                                     // column_precision
        0,                                      // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("local_cluster_version_delta",  // column_name
        ++column_id,                                  // column_id
        0,                                            // rowkey_id
        0,                                            // index_id
        0,                                            // part_key_pos
        ObIntType,                                    // column_type
        CS_TYPE_INVALID,                              // column_collation_type
        sizeof(int64_t),                              // column_length
        20,                                           // column_precision
        0,                                            // column_scale
        false,                                        // is_nullable
        false);                                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("total_part_count",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObIntType,                         // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        sizeof(int64_t),                   // column_length
        20,                                // column_precision
        0,                                 // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("valid_inner_part_count",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObIntType,                               // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        sizeof(int64_t),                         // column_length
        20,                                      // column_precision
        0,                                       // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("valid_user_part_count",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        20,                                     // column_precision
        0,                                      // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cluster_master_ip",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        MAX_IP_ADDR_LENGTH,                 // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cluster_master_port",  // column_name
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
    ADD_COLUMN_SCHEMA("cluster_heartbeat_post_tstamp",  // column_name
        ++column_id,                                    // column_id
        0,                                              // rowkey_id
        0,                                              // index_id
        0,                                              // part_key_pos
        ObIntType,                                      // column_type
        CS_TYPE_INVALID,                                // column_collation_type
        sizeof(int64_t),                                // column_length
        20,                                             // column_precision
        0,                                              // column_scale
        false,                                          // is_nullable
        false);                                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cluster_heartbeat_post_count",  // column_name
        ++column_id,                                   // column_id
        0,                                             // rowkey_id
        0,                                             // index_id
        0,                                             // part_key_pos
        ObIntType,                                     // column_type
        CS_TYPE_INVALID,                               // column_collation_type
        sizeof(int64_t),                               // column_length
        20,                                            // column_precision
        0,                                             // column_scale
        false,                                         // is_nullable
        false);                                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cluster_heartbeat_succ_tstamp",  // column_name
        ++column_id,                                    // column_id
        0,                                              // rowkey_id
        0,                                              // index_id
        0,                                              // part_key_pos
        ObIntType,                                      // column_type
        CS_TYPE_INVALID,                                // column_collation_type
        sizeof(int64_t),                                // column_length
        20,                                             // column_precision
        0,                                              // column_scale
        false,                                          // is_nullable
        false);                                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cluster_heartbeat_succ_count",  // column_name
        ++column_id,                                   // column_id
        0,                                             // rowkey_id
        0,                                             // index_id
        0,                                             // part_key_pos
        ObIntType,                                     // column_type
        CS_TYPE_INVALID,                               // column_collation_type
        sizeof(int64_t),                               // column_length
        20,                                            // column_precision
        0,                                             // column_scale
        false,                                         // is_nullable
        false);                                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("self_check_tstamp",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObIntType,                          // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        sizeof(int64_t),                    // column_length
        20,                                 // column_precision
        0,                                  // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("local_current_tstamp",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObIntType,                             // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        sizeof(int64_t),                       // column_length
        20,                                    // column_precision
        0,                                     // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("in_cluster_service",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObIntType,                           // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        sizeof(int64_t),                     // column_length
        20,                                  // column_precision
        0,                                   // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_cluster_service_master",  // column_name
        ++column_id,                                // column_id
        0,                                          // rowkey_id
        0,                                          // index_id
        0,                                          // part_key_pos
        ObIntType,                                  // column_type
        CS_TYPE_INVALID,                            // column_collation_type
        sizeof(int64_t),                            // column_length
        20,                                         // column_precision
        0,                                          // column_scale
        false,                                      // is_nullable
        false);                                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cluster_service_epoch",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        20,                                     // column_precision
        0,                                      // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cluster_total_server_count",  // column_name
        ++column_id,                                 // column_id
        0,                                           // rowkey_id
        0,                                           // index_id
        0,                                           // part_key_pos
        ObIntType,                                   // column_type
        CS_TYPE_INVALID,                             // column_collation_type
        sizeof(int64_t),                             // column_length
        20,                                          // column_precision
        0,                                           // column_scale
        false,                                       // is_nullable
        false);                                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cluster_skipped_server_count",  // column_name
        ++column_id,                                   // column_id
        0,                                             // rowkey_id
        0,                                             // index_id
        0,                                             // part_key_pos
        ObIntType,                                     // column_type
        CS_TYPE_INVALID,                               // column_collation_type
        sizeof(int64_t),                               // column_length
        20,                                            // column_precision
        0,                                             // column_scale
        false,                                         // is_nullable
        false);                                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cluster_version_gen_tstamp",  // column_name
        ++column_id,                                 // column_id
        0,                                           // rowkey_id
        0,                                           // index_id
        0,                                           // part_key_pos
        ObIntType,                                   // column_type
        CS_TYPE_INVALID,                             // column_collation_type
        sizeof(int64_t),                             // column_length
        20,                                          // column_precision
        0,                                           // column_scale
        false,                                       // is_nullable
        false);                                      // is_autoincrement
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
        20,                               // column_precision
        0,                                // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cluster_version_delta",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObIntType,                              // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        sizeof(int64_t),                        // column_length
        20,                                     // column_precision
        0,                                      // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("min_cluster_version",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObIntType,                            // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        sizeof(int64_t),                      // column_length
        20,                                   // column_precision
        0,                                    // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("max_cluster_version",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObIntType,                            // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        sizeof(int64_t),                      // column_length
        20,                                   // column_precision
        0,                                    // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("hash (addr_to_partition_id(svr_ip, svr_port))"))) {
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

}  // end namespace share
}  // end namespace oceanbase
