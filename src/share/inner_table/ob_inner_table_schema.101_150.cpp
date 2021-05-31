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

int ObInnerTableSchema::all_meta_table_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_META_TABLE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_META_TABLE_TNAME))) {
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
        true,                           // is_nullable
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
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_user_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_USER_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_USER_TNAME))) {
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
    ADD_COLUMN_SCHEMA("user_id",  // column_name
        ++column_id,              // column_id
        2,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("user_name",      // column_name
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
    ObObj host_default;
    host_default.set_varchar(ObString::make_string("%"));
    ADD_COLUMN_SCHEMA_T("host",   // column_name
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
        false,                    // is_autoincrement
        host_default,
        host_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("passwd",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        OB_MAX_PASSWORD_LENGTH,  // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("info",     // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_MAX_USER_INFO_LENGTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj priv_alter_default;
    priv_alter_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_alter",  // column_name
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
        priv_alter_default,
        priv_alter_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_default;
    priv_create_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create",  // column_name
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
        priv_create_default,
        priv_create_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_delete_default;
    priv_delete_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_delete",  // column_name
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
        priv_delete_default,
        priv_delete_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_drop_default;
    priv_drop_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_drop",  // column_name
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
        priv_drop_default,
        priv_drop_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_grant_option_default;
    priv_grant_option_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_grant_option",  // column_name
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
        priv_grant_option_default,
        priv_grant_option_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_insert_default;
    priv_insert_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_insert",  // column_name
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
        priv_insert_default,
        priv_insert_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_update_default;
    priv_update_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_update",  // column_name
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
        priv_update_default,
        priv_update_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_select_default;
    priv_select_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_select",  // column_name
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
        priv_select_default,
        priv_select_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_index_default;
    priv_index_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_index",  // column_name
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
        priv_index_default,
        priv_index_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_view_default;
    priv_create_view_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create_view",  // column_name
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
        priv_create_view_default,
        priv_create_view_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_show_view_default;
    priv_show_view_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_show_view",  // column_name
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
        priv_show_view_default,
        priv_show_view_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_show_db_default;
    priv_show_db_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_show_db",  // column_name
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
        priv_show_db_default,
        priv_show_db_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_user_default;
    priv_create_user_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create_user",  // column_name
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
        priv_create_user_default,
        priv_create_user_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_super_default;
    priv_super_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_super",  // column_name
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
        priv_super_default,
        priv_super_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_locked",  // column_name
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
    ObObj priv_process_default;
    priv_process_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_process",  // column_name
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
        priv_process_default,
        priv_process_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_synonym_default;
    priv_create_synonym_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create_synonym",  // column_name
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
        priv_create_synonym_default,
        priv_create_synonym_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj ssl_type_default;
    ssl_type_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("ssl_type",  // column_name
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
    ObObj type_default;
    type_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("type",  // column_name
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
        false,                   // is_autoincrement
        type_default,
        type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj profile_id_default;
    profile_id_default.set_int(OB_INVALID_ID);
    ADD_COLUMN_SCHEMA_T("profile_id",  // column_name
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
        profile_id_default,
        profile_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("password_last_changed",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObTimestampType,                           // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(ObPreciseDateTime),                 // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        true,                                      // is_nullable
        false,                                     // is_autoincrement
        false);                                    // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ObObj priv_file_default;
    priv_file_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_file",  // column_name
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
        priv_file_default,
        priv_file_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_alter_tenant_default;
    priv_alter_tenant_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_alter_tenant",  // column_name
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
        priv_alter_tenant_default,
        priv_alter_tenant_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_alter_system_default;
    priv_alter_system_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_alter_system",  // column_name
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
        priv_alter_system_default,
        priv_alter_system_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_resource_pool_default;
    priv_create_resource_pool_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create_resource_pool",  // column_name
        ++column_id,                                  // column_id
        0,                                            // rowkey_id
        0,                                            // index_id
        0,                                            // part_key_pos
        ObIntType,                                    // column_type
        CS_TYPE_INVALID,                              // column_collation_type
        sizeof(int64_t),                              // column_length
        -1,                                           // column_precision
        -1,                                           // column_scale
        false,                                        // is_nullable
        false,                                        // is_autoincrement
        priv_create_resource_pool_default,
        priv_create_resource_pool_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_resource_unit_default;
    priv_create_resource_unit_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create_resource_unit",  // column_name
        ++column_id,                                  // column_id
        0,                                            // rowkey_id
        0,                                            // index_id
        0,                                            // part_key_pos
        ObIntType,                                    // column_type
        CS_TYPE_INVALID,                              // column_collation_type
        sizeof(int64_t),                              // column_length
        -1,                                           // column_precision
        -1,                                           // column_scale
        false,                                        // is_nullable
        false,                                        // is_autoincrement
        priv_create_resource_unit_default,
        priv_create_resource_unit_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_user_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_USER_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_USER_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("user_id",  // column_name
        ++column_id,              // column_id
        2,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("user_name",      // column_name
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
    ObObj host_default;
    host_default.set_varchar(ObString::make_string("%"));
    ADD_COLUMN_SCHEMA_T("host",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_MAX_HOST_NAME_LENGTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        true,                     // is_nullable
        false,                    // is_autoincrement
        host_default,
        host_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("passwd",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        OB_MAX_PASSWORD_LENGTH,  // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("info",     // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_MAX_USER_INFO_LENGTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        true,                     // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj priv_alter_default;
    priv_alter_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_alter",  // column_name
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
        priv_alter_default,
        priv_alter_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_default;
    priv_create_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create",  // column_name
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
        priv_create_default,
        priv_create_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_delete_default;
    priv_delete_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_delete",  // column_name
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
        priv_delete_default,
        priv_delete_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_drop_default;
    priv_drop_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_drop",  // column_name
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
        false,                        // is_autoincrement
        priv_drop_default,
        priv_drop_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_grant_option_default;
    priv_grant_option_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_grant_option",  // column_name
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
        false,                                // is_autoincrement
        priv_grant_option_default,
        priv_grant_option_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_insert_default;
    priv_insert_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_insert",  // column_name
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
        priv_insert_default,
        priv_insert_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_update_default;
    priv_update_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_update",  // column_name
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
        priv_update_default,
        priv_update_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_select_default;
    priv_select_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_select",  // column_name
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
        priv_select_default,
        priv_select_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_index_default;
    priv_index_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_index",  // column_name
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
        priv_index_default,
        priv_index_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_view_default;
    priv_create_view_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create_view",  // column_name
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
        priv_create_view_default,
        priv_create_view_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_show_view_default;
    priv_show_view_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_show_view",  // column_name
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
        priv_show_view_default,
        priv_show_view_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_show_db_default;
    priv_show_db_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_show_db",  // column_name
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
        priv_show_db_default,
        priv_show_db_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_user_default;
    priv_create_user_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create_user",  // column_name
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
        priv_create_user_default,
        priv_create_user_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_super_default;
    priv_super_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_super",  // column_name
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
        priv_super_default,
        priv_super_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("is_locked",  // column_name
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
    ObObj priv_process_default;
    priv_process_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_process",  // column_name
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
        priv_process_default,
        priv_process_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_synonym_default;
    priv_create_synonym_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create_synonym",  // column_name
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
        priv_create_synonym_default,
        priv_create_synonym_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj ssl_type_default;
    ssl_type_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("ssl_type",  // column_name
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
        true,                          // is_nullable
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
        true,                           // is_nullable
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
        true,                            // is_nullable
        false,                           // is_autoincrement
        x509_subject_default,
        x509_subject_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj type_default;
    type_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("type",  // column_name
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
        false,                   // is_autoincrement
        type_default,
        type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj profile_id_default;
    profile_id_default.set_int(OB_INVALID_ID);
    ADD_COLUMN_SCHEMA_T("profile_id",  // column_name
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
        profile_id_default,
        profile_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA_TS("password_last_changed",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObTimestampType,                           // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        sizeof(ObPreciseDateTime),                 // column_length
        -1,                                        // column_precision
        -1,                                        // column_scale
        true,                                      // is_nullable
        false,                                     // is_autoincrement
        false);                                    // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ObObj priv_file_default;
    priv_file_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_file",  // column_name
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
        false,                        // is_autoincrement
        priv_file_default,
        priv_file_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_alter_tenant_default;
    priv_alter_tenant_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_alter_tenant",  // column_name
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
        false,                                // is_autoincrement
        priv_alter_tenant_default,
        priv_alter_tenant_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_alter_system_default;
    priv_alter_system_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_alter_system",  // column_name
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
        false,                                // is_autoincrement
        priv_alter_system_default,
        priv_alter_system_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_resource_pool_default;
    priv_create_resource_pool_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create_resource_pool",  // column_name
        ++column_id,                                  // column_id
        0,                                            // rowkey_id
        0,                                            // index_id
        0,                                            // part_key_pos
        ObIntType,                                    // column_type
        CS_TYPE_INVALID,                              // column_collation_type
        sizeof(int64_t),                              // column_length
        -1,                                           // column_precision
        -1,                                           // column_scale
        true,                                         // is_nullable
        false,                                        // is_autoincrement
        priv_create_resource_pool_default,
        priv_create_resource_pool_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_resource_unit_default;
    priv_create_resource_unit_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create_resource_unit",  // column_name
        ++column_id,                                  // column_id
        0,                                            // rowkey_id
        0,                                            // index_id
        0,                                            // part_key_pos
        ObIntType,                                    // column_type
        CS_TYPE_INVALID,                              // column_collation_type
        sizeof(int64_t),                              // column_length
        -1,                                           // column_precision
        -1,                                           // column_scale
        true,                                         // is_nullable
        false,                                        // is_autoincrement
        priv_create_resource_unit_default,
        priv_create_resource_unit_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_database_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_DATABASE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DATABASE_TNAME))) {
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
    ADD_COLUMN_SCHEMA("database_id",  // column_name
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
    ObObj database_name_default;
    database_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("database_name",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,      // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        database_name_default,
        database_name_default);  // default_value
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
        false,                        // is_nullable
        false);                       // is_autoincrement
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
        false,                      // is_nullable
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
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment",      // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        MAX_DATABASE_COMMENT_LENGTH,  // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
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
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj default_tablegroup_id_default;
    default_tablegroup_id_default.set_int(OB_INVALID_ID);
    ADD_COLUMN_SCHEMA_T("default_tablegroup_id",  // column_name
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
        default_tablegroup_id_default,
        default_tablegroup_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj in_recyclebin_default;
    in_recyclebin_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("in_recyclebin",  // column_name
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
        in_recyclebin_default,
        in_recyclebin_default);  // default_value
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
        false,                                  // is_nullable
        false,                                  // is_autoincrement
        drop_schema_version_default,
        drop_schema_version_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_database_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_DATABASE_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DATABASE_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("database_id",  // column_name
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
    ObObj database_name_default;
    database_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("database_name",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,      // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false,                            // is_autoincrement
        database_name_default,
        database_name_default);  // default_value
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
    ADD_COLUMN_SCHEMA("comment",      // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        MAX_DATABASE_COMMENT_LENGTH,  // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
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
    ObObj default_tablegroup_id_default;
    default_tablegroup_id_default.set_int(OB_INVALID_ID);
    ADD_COLUMN_SCHEMA_T("default_tablegroup_id",  // column_name
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
        default_tablegroup_id_default,
        default_tablegroup_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj in_recyclebin_default;
    in_recyclebin_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("in_recyclebin",  // column_name
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
        in_recyclebin_default,
        in_recyclebin_default);  // default_value
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
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_tablegroup_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TABLEGROUP_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLEGROUP_TNAME))) {
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
    ADD_COLUMN_SCHEMA("tablegroup_id",  // column_name
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
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment",        // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        MAX_TABLEGROUP_COMMENT_LENGTH,  // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
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
    ObObj part_level_default;
    part_level_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("part_level",  // column_name
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
        part_level_default,
        part_level_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj part_func_type_default;
    part_func_type_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("part_func_type",  // column_name
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
        part_func_type_default,
        part_func_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj part_func_expr_num_default;
    part_func_expr_num_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("part_func_expr_num",  // column_name
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
        part_func_expr_num_default,
        part_func_expr_num_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj part_num_default;
    part_num_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("part_num",  // column_name
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
        part_num_default,
        part_num_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj sub_part_func_type_default;
    sub_part_func_type_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("sub_part_func_type",  // column_name
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
        sub_part_func_type_default,
        sub_part_func_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj sub_part_func_expr_num_default;
    sub_part_func_expr_num_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("sub_part_func_expr_num",  // column_name
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
        sub_part_func_expr_num_default,
        sub_part_func_expr_num_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj sub_part_num_default;
    sub_part_num_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("sub_part_num",  // column_name
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
        sub_part_num_default,
        sub_part_num_default);  // default_value
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
        false,                               // is_nullable
        false,                               // is_autoincrement
        max_used_part_id_default,
        max_used_part_id_default);  // default_value
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
        false,                                // is_nullable
        false,                                // is_autoincrement
        previous_locality_default,
        previous_locality_default);  // default_value
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
        false,                                  // is_nullable
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
        false,                                   // is_nullable
        false,                                   // is_autoincrement
        is_sub_part_template_default,
        is_sub_part_template_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_tablegroup_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TABLEGROUP_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLEGROUP_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("tablegroup_id",  // column_name
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment",        // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        MAX_TABLEGROUP_COMMENT_LENGTH,  // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
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
    ObObj part_level_default;
    part_level_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("part_level",  // column_name
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
        part_level_default,
        part_level_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj part_func_type_default;
    part_func_type_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("part_func_type",  // column_name
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
        part_func_type_default,
        part_func_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj part_func_expr_num_default;
    part_func_expr_num_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("part_func_expr_num",  // column_name
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
        part_func_expr_num_default,
        part_func_expr_num_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj part_num_default;
    part_num_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("part_num",  // column_name
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
        part_num_default,
        part_num_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj sub_part_func_type_default;
    sub_part_func_type_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("sub_part_func_type",  // column_name
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
        sub_part_func_type_default,
        sub_part_func_type_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj sub_part_func_expr_num_default;
    sub_part_func_expr_num_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("sub_part_func_expr_num",  // column_name
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
        sub_part_func_expr_num_default,
        sub_part_func_expr_num_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj sub_part_num_default;
    sub_part_num_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("sub_part_num",  // column_name
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
        sub_part_num_default,
        sub_part_num_default);  // default_value
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
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_tenant_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_TNAME))) {
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
        false,                        // is_nullable
        false);                       // is_autoincrement
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
        false,                      // is_nullable
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
    ADD_COLUMN_SCHEMA("locked",  // column_name
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
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("info",       // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_MAX_TENANT_INFO_LENGTH,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
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
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj rewrite_merge_version_default;
    rewrite_merge_version_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("rewrite_merge_version",  // column_name
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
        rewrite_merge_version_default,
        rewrite_merge_version_default);  // default_value
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
        false,                       // is_nullable
        false,                       // is_autoincrement
        locality_default,
        locality_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj logonly_replica_num_default;
    logonly_replica_num_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("logonly_replica_num",  // column_name
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
        logonly_replica_num_default,
        logonly_replica_num_default);  // default_value
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
        false,                                // is_nullable
        false,                                // is_autoincrement
        previous_locality_default,
        previous_locality_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj storage_format_version_default;
    storage_format_version_default.set_int(0);
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
        false,                                     // is_nullable
        false,                                     // is_autoincrement
        storage_format_version_default,
        storage_format_version_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj storage_format_work_version_default;
    storage_format_work_version_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("storage_format_work_version",  // column_name
        ++column_id,                                    // column_id
        0,                                              // rowkey_id
        0,                                              // index_id
        0,                                              // part_key_pos
        ObIntType,                                      // column_type
        CS_TYPE_INVALID,                                // column_collation_type
        sizeof(int64_t),                                // column_length
        -1,                                             // column_precision
        -1,                                             // column_scale
        false,                                          // is_nullable
        false,                                          // is_autoincrement
        storage_format_work_version_default,
        storage_format_work_version_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj default_tablegroup_id_default;
    default_tablegroup_id_default.set_int(OB_INVALID_ID);
    ADD_COLUMN_SCHEMA_T("default_tablegroup_id",  // column_name
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
        default_tablegroup_id_default,
        default_tablegroup_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj compatibility_mode_default;
    compatibility_mode_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("compatibility_mode",  // column_name
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
        compatibility_mode_default,
        compatibility_mode_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj drop_tenant_time_default;
    drop_tenant_time_default.set_int(OB_INVALID_TIMESTAMP);
    ADD_COLUMN_SCHEMA_T("drop_tenant_time",  // column_name
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
        drop_tenant_time_default,
        drop_tenant_time_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj status_default;
    status_default.set_varchar(ObString::make_string("TENANT_STATUS_NORMAL"));
    ADD_COLUMN_SCHEMA_T("status",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        MAX_TENANT_STATUS_LENGTH,  // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false,                     // is_autoincrement
        status_default,
        status_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj in_recyclebin_default;
    in_recyclebin_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("in_recyclebin",  // column_name
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
        in_recyclebin_default,
        in_recyclebin_default);  // default_value
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

int ObInnerTableSchema::all_tenant_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("schema_version",  // column_name
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
    ADD_COLUMN_SCHEMA("locked",  // column_name
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
    ADD_COLUMN_SCHEMA("info",       // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_MAX_TENANT_INFO_LENGTH,  // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
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
    ObObj rewrite_merge_version_default;
    rewrite_merge_version_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("rewrite_merge_version",  // column_name
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
        rewrite_merge_version_default,
        rewrite_merge_version_default);  // default_value
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
    ObObj logonly_replica_num_default;
    logonly_replica_num_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("logonly_replica_num",  // column_name
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
        logonly_replica_num_default,
        logonly_replica_num_default);  // default_value
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
    ObObj storage_format_version_default;
    storage_format_version_default.set_int(0);
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
    ObObj storage_format_work_version_default;
    storage_format_work_version_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("storage_format_work_version",  // column_name
        ++column_id,                                    // column_id
        0,                                              // rowkey_id
        0,                                              // index_id
        0,                                              // part_key_pos
        ObIntType,                                      // column_type
        CS_TYPE_INVALID,                                // column_collation_type
        sizeof(int64_t),                                // column_length
        -1,                                             // column_precision
        -1,                                             // column_scale
        true,                                           // is_nullable
        false,                                          // is_autoincrement
        storage_format_work_version_default,
        storage_format_work_version_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj default_tablegroup_id_default;
    default_tablegroup_id_default.set_int(OB_INVALID_ID);
    ADD_COLUMN_SCHEMA_T("default_tablegroup_id",  // column_name
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
        default_tablegroup_id_default,
        default_tablegroup_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj compatibility_mode_default;
    compatibility_mode_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("compatibility_mode",  // column_name
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
        compatibility_mode_default,
        compatibility_mode_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj drop_tenant_time_default;
    drop_tenant_time_default.set_int(OB_INVALID_TIMESTAMP);
    ADD_COLUMN_SCHEMA_T("drop_tenant_time",  // column_name
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
        drop_tenant_time_default,
        drop_tenant_time_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj status_default;
    status_default.set_varchar(ObString::make_string("TENANT_STATUS_NORMAL"));
    ADD_COLUMN_SCHEMA_T("status",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        MAX_TENANT_STATUS_LENGTH,  // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false,                     // is_autoincrement
        status_default,
        status_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj in_recyclebin_default;
    in_recyclebin_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("in_recyclebin",  // column_name
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
        in_recyclebin_default,
        in_recyclebin_default);  // default_value
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

int ObInnerTableSchema::all_table_privilege_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TABLE_PRIVILEGE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLE_PRIVILEGE_TNAME))) {
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
    ADD_COLUMN_SCHEMA("user_id",  // column_name
        ++column_id,              // column_id
        2,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("database_name",  // column_name
        ++column_id,                    // column_id
        3,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,    // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_name",     // column_name
        ++column_id,                    // column_id
        4,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_CORE_TALBE_NAME_LENGTH,  // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj priv_alter_default;
    priv_alter_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_alter",  // column_name
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
        priv_alter_default,
        priv_alter_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_default;
    priv_create_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create",  // column_name
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
        priv_create_default,
        priv_create_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_delete_default;
    priv_delete_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_delete",  // column_name
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
        priv_delete_default,
        priv_delete_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_drop_default;
    priv_drop_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_drop",  // column_name
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
        priv_drop_default,
        priv_drop_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_grant_option_default;
    priv_grant_option_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_grant_option",  // column_name
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
        priv_grant_option_default,
        priv_grant_option_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_insert_default;
    priv_insert_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_insert",  // column_name
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
        priv_insert_default,
        priv_insert_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_update_default;
    priv_update_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_update",  // column_name
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
        priv_update_default,
        priv_update_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_select_default;
    priv_select_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_select",  // column_name
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
        priv_select_default,
        priv_select_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_index_default;
    priv_index_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_index",  // column_name
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
        priv_index_default,
        priv_index_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_view_default;
    priv_create_view_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create_view",  // column_name
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
        priv_create_view_default,
        priv_create_view_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_show_view_default;
    priv_show_view_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_show_view",  // column_name
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
        priv_show_view_default,
        priv_show_view_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_table_privilege_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TABLE_PRIVILEGE_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLE_PRIVILEGE_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("user_id",  // column_name
        ++column_id,              // column_id
        2,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("database_name",  // column_name
        ++column_id,                    // column_id
        3,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,    // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("table_name",     // column_name
        ++column_id,                    // column_id
        4,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_CORE_TALBE_NAME_LENGTH,  // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
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
    ObObj priv_alter_default;
    priv_alter_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_alter",  // column_name
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
        priv_alter_default,
        priv_alter_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_default;
    priv_create_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create",  // column_name
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
        priv_create_default,
        priv_create_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_delete_default;
    priv_delete_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_delete",  // column_name
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
        priv_delete_default,
        priv_delete_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_drop_default;
    priv_drop_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_drop",  // column_name
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
        false,                        // is_autoincrement
        priv_drop_default,
        priv_drop_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_grant_option_default;
    priv_grant_option_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_grant_option",  // column_name
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
        false,                                // is_autoincrement
        priv_grant_option_default,
        priv_grant_option_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_insert_default;
    priv_insert_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_insert",  // column_name
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
        priv_insert_default,
        priv_insert_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_update_default;
    priv_update_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_update",  // column_name
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
        priv_update_default,
        priv_update_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_select_default;
    priv_select_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_select",  // column_name
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
        priv_select_default,
        priv_select_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_index_default;
    priv_index_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_index",  // column_name
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
        priv_index_default,
        priv_index_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_view_default;
    priv_create_view_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create_view",  // column_name
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
        priv_create_view_default,
        priv_create_view_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_show_view_default;
    priv_show_view_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_show_view",  // column_name
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
        priv_show_view_default,
        priv_show_view_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_database_privilege_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_DATABASE_PRIVILEGE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DATABASE_PRIVILEGE_TNAME))) {
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
    ADD_COLUMN_SCHEMA("user_id",  // column_name
        ++column_id,              // column_id
        2,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("database_name",  // column_name
        ++column_id,                    // column_id
        3,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,    // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj priv_alter_default;
    priv_alter_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_alter",  // column_name
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
        priv_alter_default,
        priv_alter_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_default;
    priv_create_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create",  // column_name
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
        priv_create_default,
        priv_create_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_delete_default;
    priv_delete_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_delete",  // column_name
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
        priv_delete_default,
        priv_delete_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_drop_default;
    priv_drop_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_drop",  // column_name
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
        priv_drop_default,
        priv_drop_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_grant_option_default;
    priv_grant_option_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_grant_option",  // column_name
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
        priv_grant_option_default,
        priv_grant_option_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_insert_default;
    priv_insert_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_insert",  // column_name
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
        priv_insert_default,
        priv_insert_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_update_default;
    priv_update_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_update",  // column_name
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
        priv_update_default,
        priv_update_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_select_default;
    priv_select_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_select",  // column_name
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
        priv_select_default,
        priv_select_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_index_default;
    priv_index_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_index",  // column_name
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
        priv_index_default,
        priv_index_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_view_default;
    priv_create_view_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create_view",  // column_name
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
        priv_create_view_default,
        priv_create_view_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_show_view_default;
    priv_show_view_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_show_view",  // column_name
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
        priv_show_view_default,
        priv_show_view_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_database_privilege_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_DATABASE_PRIVILEGE_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DATABASE_PRIVILEGE_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("user_id",  // column_name
        ++column_id,              // column_id
        2,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("database_name",  // column_name
        ++column_id,                    // column_id
        3,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,    // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
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
    ObObj priv_alter_default;
    priv_alter_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_alter",  // column_name
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
        priv_alter_default,
        priv_alter_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_default;
    priv_create_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create",  // column_name
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
        priv_create_default,
        priv_create_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_delete_default;
    priv_delete_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_delete",  // column_name
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
        priv_delete_default,
        priv_delete_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_drop_default;
    priv_drop_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_drop",  // column_name
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
        false,                        // is_autoincrement
        priv_drop_default,
        priv_drop_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_grant_option_default;
    priv_grant_option_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_grant_option",  // column_name
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
        false,                                // is_autoincrement
        priv_grant_option_default,
        priv_grant_option_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_insert_default;
    priv_insert_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_insert",  // column_name
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
        priv_insert_default,
        priv_insert_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_update_default;
    priv_update_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_update",  // column_name
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
        priv_update_default,
        priv_update_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_select_default;
    priv_select_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_select",  // column_name
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
        priv_select_default,
        priv_select_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_index_default;
    priv_index_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_index",  // column_name
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
        priv_index_default,
        priv_index_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_create_view_default;
    priv_create_view_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_create_view",  // column_name
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
        priv_create_view_default,
        priv_create_view_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj priv_show_view_default;
    priv_show_view_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("priv_show_view",  // column_name
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
        priv_show_view_default,
        priv_show_view_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_table_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TABLE_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TABLE_HISTORY_TNAME))) {
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
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_column_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_COLUMN_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_COLUMN_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("column_id",  // column_name
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
    ObObj column_name_default;
    column_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("column_name",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_COLUMN_NAME_LENGTH,      // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false,                          // is_autoincrement
        column_name_default,
        column_name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj rowkey_position_default;
    rowkey_position_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("rowkey_position",  // column_name
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
        rowkey_position_default,
        rowkey_position_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("index_position",  // column_name
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
    ADD_COLUMN_SCHEMA("order_in_rowkey",  // column_name
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
    ADD_COLUMN_SCHEMA("partition_key_position",  // column_name
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
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("data_length",  // column_name
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
    ADD_COLUMN_SCHEMA("data_precision",  // column_name
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
    ADD_COLUMN_SCHEMA("data_scale",  // column_name
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
    ADD_COLUMN_SCHEMA("nullable",  // column_name
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
    ADD_COLUMN_SCHEMA("on_update_current_timestamp",  // column_name
        ++column_id,                                  // column_id
        0,                                            // rowkey_id
        0,                                            // index_id
        0,                                            // part_key_pos
        ObIntType,                                    // column_type
        CS_TYPE_INVALID,                              // column_collation_type
        sizeof(int64_t),                              // column_length
        -1,                                           // column_precision
        -1,                                           // column_scale
        true,                                         // is_nullable
        false);                                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("autoincrement",  // column_name
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
    ObObj is_hidden_default;
    is_hidden_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("is_hidden",  // column_name
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
        false,                        // is_autoincrement
        is_hidden_default,
        is_hidden_default);  // default_value
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
    ADD_COLUMN_SCHEMA("orig_default_value",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        OB_MAX_DEFAULT_VALUE_LENGTH,         // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cur_default_value",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        OB_MAX_DEFAULT_VALUE_LENGTH,        // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("comment",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObLongTextType,           // column_type
        CS_TYPE_INVALID,          // column_collation_type
        0,                        // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        true,                     // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj column_flags_default;
    column_flags_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("column_flags",  // column_name
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
        column_flags_default,
        column_flags_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj prev_column_id_default;
    prev_column_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("prev_column_id",  // column_name
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
        prev_column_id_default,
        prev_column_id_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("extended_type_info",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_BINARY,                      // column_collation_type
        OB_MAX_VARBINARY_LENGTH,             // column_length
        -1,                                  // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("orig_default_value_v2",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_BINARY,                         // column_collation_type
        OB_MAX_DEFAULT_VALUE_LENGTH,            // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cur_default_value_v2",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObVarcharType,                         // column_type
        CS_TYPE_BINARY,                        // column_collation_type
        OB_MAX_DEFAULT_VALUE_LENGTH,           // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        true,                                  // is_nullable
        false);                                // is_autoincrement
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_zone_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_ZONE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ZONE_TNAME))) {
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
    ADD_COLUMN_SCHEMA("name",  // column_name
        ++column_id,           // column_id
        2,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObVarcharType,         // column_type
        CS_TYPE_INVALID,       // column_collation_type
        TABLE_MAX_KEY_LENGTH,  // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("value",  // column_name
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
    ADD_COLUMN_SCHEMA("info",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObVarcharType,         // column_type
        CS_TYPE_INVALID,       // column_collation_type
        MAX_ZONE_INFO_LENGTH,  // column_length
        -1,                    // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
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

int ObInnerTableSchema::all_server_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SERVER_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SERVER_TNAME))) {
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
    ADD_COLUMN_SCHEMA("id",  // column_name
        ++column_id,         // column_id
        0,                   // rowkey_id
        0,                   // index_id
        0,                   // part_key_pos
        ObIntType,           // column_type
        CS_TYPE_INVALID,     // column_collation_type
        sizeof(int64_t),     // column_length
        -1,                  // column_precision
        -1,                  // column_scale
        false,               // is_nullable
        false);              // is_autoincrement
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
    ADD_COLUMN_SCHEMA("inner_port",  // column_name
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
    ADD_COLUMN_SCHEMA("with_rootserver",  // column_name
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
        OB_SERVER_STATUS_LENGTH,  // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("block_migrate_in_time",  // column_name
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
    ADD_COLUMN_SCHEMA("build_version",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_SERVER_VERSION_LENGTH,       // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj stop_time_default;
    stop_time_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("stop_time",  // column_name
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
        stop_time_default,
        stop_time_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("start_service_time",  // column_name
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
    ObObj first_sessid_default;
    first_sessid_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("first_sessid",  // column_name
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
        first_sessid_default,
        first_sessid_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj with_partition_default;
    with_partition_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("with_partition",  // column_name
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
        with_partition_default,
        with_partition_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj last_offline_time_default;
    last_offline_time_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("last_offline_time",  // column_name
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
        last_offline_time_default,
        last_offline_time_default);  // default_value
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

int ObInnerTableSchema::all_sys_parameter_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SYS_PARAMETER_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SYS_PARAMETER_TNAME))) {
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
    ADD_COLUMN_SCHEMA("value_strict",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_EXTRA_CONFIG_LENGTH,    // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
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
    ADD_COLUMN_SCHEMA("need_reboot",  // column_name
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
    ADD_COLUMN_SCHEMA("visible_level",    // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_CONFIG_VISIBLE_LEVEL_LEN,  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
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

int ObInnerTableSchema::tenant_parameter_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_TENANT_PARAMETER_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_PARAMETER_TNAME))) {
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

int ObInnerTableSchema::all_sys_variable_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SYS_VARIABLE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SYS_VARIABLE_TNAME))) {
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
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_sys_stat_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SYS_STAT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SYS_STAT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("name",    // column_name
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
        false);                  // is_autoincrement
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
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_column_statistic_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_COLUMN_STATISTIC_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_COLUMN_STATISTIC_TNAME))) {
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
    ADD_COLUMN_SCHEMA("num_distinct",  // column_name
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
    ADD_COLUMN_SCHEMA("num_null",  // column_name
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
    ADD_COLUMN_SCHEMA("llc_bitmap",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        MAX_LLC_BITMAP_LENGTH,       // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("llc_bitmap_size",  // column_name
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
    ADD_COLUMN_SCHEMA("last_rebuild_version",  // column_name
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
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (table_id)"))) {
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

int ObInnerTableSchema::all_unit_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_UNIT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_UNIT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("unit_id",  // column_name
        ++column_id,              // column_id
        1,                        // rowkey_id
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
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("group_id",  // column_name
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
    ADD_COLUMN_SCHEMA("migrate_from_svr_ip",  // column_name
        ++column_id,                          // column_id
        0,                                    // rowkey_id
        0,                                    // index_id
        0,                                    // part_key_pos
        ObVarcharType,                        // column_type
        CS_TYPE_INVALID,                      // column_collation_type
        MAX_IP_ADDR_LENGTH,                   // column_length
        -1,                                   // column_precision
        -1,                                   // column_scale
        false,                                // is_nullable
        false);                               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("migrate_from_svr_port",  // column_name
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
    ++column_id;  // for [discard]temporary_unit_svr_ip
  }

  if (OB_SUCC(ret)) {
    ++column_id;  // for [discard]temporary_unit_svr_port
  }

  if (OB_SUCC(ret)) {
    ObObj manual_migrate_default;
    manual_migrate_default.set_tinyint(0);
    ADD_COLUMN_SCHEMA_T("manual_migrate",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObTinyIntType,                     // column_type
        CS_TYPE_INVALID,                   // column_collation_type
        1,                                 // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false,                             // is_autoincrement
        manual_migrate_default,
        manual_migrate_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj status_default;
    status_default.set_varchar(ObString::make_string("ACTIVE"));
    ADD_COLUMN_SCHEMA_T("status",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        MAX_UNIT_STATUS_LENGTH,    // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false,                     // is_autoincrement
        status_default,
        status_default);  // default_value
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

int ObInnerTableSchema::all_unit_config_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_UNIT_CONFIG_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_UNIT_CONFIG_TNAME))) {
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
    ADD_COLUMN_SCHEMA("unit_config_id",  // column_name
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
    ADD_COLUMN_SCHEMA("name",    // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_INVALID,         // column_collation_type
        MAX_UNIT_CONFIG_LENGTH,  // column_length
        -1,                      // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("max_cpu",  // column_name
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
    ADD_COLUMN_SCHEMA("min_cpu",  // column_name
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
    ADD_COLUMN_SCHEMA("max_memory",  // column_name
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
    ADD_COLUMN_SCHEMA("min_memory",  // column_name
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
    ADD_COLUMN_SCHEMA("max_iops",  // column_name
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
    ADD_COLUMN_SCHEMA("min_iops",  // column_name
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
    ADD_COLUMN_SCHEMA("max_disk_size",  // column_name
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
    ADD_COLUMN_SCHEMA("max_session_num",  // column_name
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

int ObInnerTableSchema::all_resource_pool_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_RESOURCE_POOL_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RESOURCE_POOL_TNAME))) {
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
    ADD_COLUMN_SCHEMA("resource_pool_id",  // column_name
        ++column_id,                       // column_id
        1,                                 // rowkey_id
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
    ADD_COLUMN_SCHEMA("name",      // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        MAX_RESOURCE_POOL_LENGTH,  // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("unit_count",  // column_name
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
    ADD_COLUMN_SCHEMA("unit_config_id",  // column_name
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
    ObObj is_tenant_sys_pool_default;
    is_tenant_sys_pool_default.set_tinyint(false);
    ADD_COLUMN_SCHEMA_T("is_tenant_sys_pool",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObTinyIntType,                         // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        1,                                     // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false,                                 // is_autoincrement
        is_tenant_sys_pool_default,
        is_tenant_sys_pool_default);  // default_value
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

int ObInnerTableSchema::all_tenant_resource_usage_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_TENANT_RESOURCE_USAGE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_TENANT_RESOURCE_USAGE_TNAME))) {
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
    ADD_COLUMN_SCHEMA_TS("report_time",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObTimestampType,                 // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        sizeof(ObPreciseDateTime),       // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false,                           // is_autoincrement
        false);                          // is_on_update_for_timestamp
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cpu_quota",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObDoubleType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(double),             // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("cpu_quota_used",  // column_name
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
    ADD_COLUMN_SCHEMA("mem_quota",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObDoubleType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        sizeof(double),             // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("mem_quota_used",  // column_name
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
    ADD_COLUMN_SCHEMA("disk_quota",  // column_name
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
    ADD_COLUMN_SCHEMA("disk_quota_used",  // column_name
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
    ADD_COLUMN_SCHEMA("iops_quota",  // column_name
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
    ADD_COLUMN_SCHEMA("iops_quota_used",  // column_name
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

int ObInnerTableSchema::all_sequence_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SEQUENCE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SEQUENCE_TNAME))) {
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
    ADD_COLUMN_SCHEMA("sequence_key",  // column_name
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
    ADD_COLUMN_SCHEMA("column_id",  // column_name
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

  if (OB_SUCC(ret)) {
    ObObj migrated_default;
    migrated_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("migrated",  // column_name
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
        migrated_default,
        migrated_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_charset_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_CHARSET_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_CHARSET_TNAME))) {
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
        1,                          // rowkey_id
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

int ObInnerTableSchema::all_collation_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_COLLATION_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_COLLATION_TNAME))) {
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
        1,                            // rowkey_id
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

int ObInnerTableSchema::all_local_index_status_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_LOCAL_INDEX_STATUS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_LOCAL_INDEX_STATUS_TNAME))) {
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
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ret_code",  // column_name
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
    ObObj role_default;
    role_default.set_int(2);
    ADD_COLUMN_SCHEMA_T("role",  // column_name
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
        false,                   // is_autoincrement
        role_default,
        role_default);  // default_value
  }
  if (OB_SUCC(ret)) {
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (tenant_id)"))) {
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

int ObInnerTableSchema::all_dummy_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_DUMMY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_DUMMY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("key",  // column_name
        ++column_id,          // column_id
        1,                    // rowkey_id
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

int ObInnerTableSchema::all_frozen_map_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_FROZEN_MAP_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_FROZEN_MAP_TNAME))) {
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
        2,                                 // rowkey_id
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

int ObInnerTableSchema::all_clog_history_info_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_CLOG_HISTORY_INFO_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(7);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_CLOG_HISTORY_INFO_TNAME))) {
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
    ADD_COLUMN_SCHEMA("partition_idx",  // column_name
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
    ADD_COLUMN_SCHEMA("partition_cnt",  // column_name
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
    ADD_COLUMN_SCHEMA("start_log_id",  // column_name
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
    ADD_COLUMN_SCHEMA("start_log_timestamp",  // column_name
        ++column_id,                          // column_id
        5,                                    // rowkey_id
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
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        6,                       // rowkey_id
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
        7,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("end_log_id",  // column_name
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
    ADD_COLUMN_SCHEMA("end_log_timestamp",  // column_name
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
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("hash (addr_to_partition_id(svr_ip, svr_port))"))) {
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

int ObInnerTableSchema::all_clog_history_info_v2_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_CLOG_HISTORY_INFO_V2_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(7);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_CLOG_HISTORY_INFO_V2_TNAME))) {
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
        1,                         // part_key_pos
        ObUInt64Type,              // column_type
        CS_TYPE_INVALID,           // column_collation_type
        sizeof(uint64_t),          // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_idx",  // column_name
        ++column_id,                    // column_id
        2,                              // rowkey_id
        0,                              // index_id
        2,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("partition_cnt",  // column_name
        ++column_id,                    // column_id
        3,                              // rowkey_id
        0,                              // index_id
        3,                              // part_key_pos
        ObIntType,                      // column_type
        CS_TYPE_INVALID,                // column_collation_type
        sizeof(int64_t),                // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("start_log_id",  // column_name
        ++column_id,                   // column_id
        4,                             // rowkey_id
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
    ADD_COLUMN_SCHEMA("start_log_timestamp",  // column_name
        ++column_id,                          // column_id
        5,                                    // rowkey_id
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
    ADD_COLUMN_SCHEMA("svr_ip",  // column_name
        ++column_id,             // column_id
        6,                       // rowkey_id
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
        7,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("end_log_id",  // column_name
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
    ADD_COLUMN_SCHEMA("end_log_timestamp",  // column_name
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
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_KEY);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("key (table_id, partition_idx, partition_cnt)"))) {
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

int ObInnerTableSchema::all_rootservice_event_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_ROOTSERVICE_EVENT_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ROOTSERVICE_EVENT_HISTORY_TNAME))) {
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
    ObObj value2_default;
    value2_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("value2",            // column_name
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
        value2_default,
        value2_default);  // default_value
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
    ObObj rs_svr_ip_default;
    rs_svr_ip_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("rs_svr_ip",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        MAX_IP_ADDR_LENGTH,           // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false,                        // is_autoincrement
        rs_svr_ip_default,
        rs_svr_ip_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj rs_svr_port_default;
    rs_svr_port_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("rs_svr_port",  // column_name
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
        rs_svr_port_default,
        rs_svr_port_default);  // default_value
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

int ObInnerTableSchema::all_privilege_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_PRIVILEGE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PRIVILEGE_TNAME))) {
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
    ADD_COLUMN_SCHEMA("Privilege",    // column_name
        ++column_id,                  // column_id
        1,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        MAX_COLUMN_PRIVILEGE_LENGTH,  // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("Context",       // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        MAX_PRIVILEGE_CONTEXT_LENGTH,  // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("Comment",    // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        MAX_COLUMN_COMMENT_LENGTH,  // column_length
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

int ObInnerTableSchema::all_outline_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_OUTLINE_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_OUTLINE_TNAME))) {
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
    ADD_COLUMN_SCHEMA("outline_id",  // column_name
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
    ObObj name_default;
    name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("name",      // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        OB_MAX_OUTLINE_NAME_LENGTH,  // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false,                       // is_autoincrement
        name_default,
        name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj signature_default;
    signature_default.set_varbinary(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("signature",      // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_BINARY,                   // column_collation_type
        OB_MAX_OUTLINE_SIGNATURE_LENGTH,  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false,                            // is_autoincrement
        signature_default,
        signature_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("outline_content",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObLongTextType,                   // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        0,                                // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
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
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj owner_default;
    owner_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("owner",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_MAX_USERNAME_LENGTH,   // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false,                    // is_autoincrement
        owner_default,
        owner_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj used_default;
    used_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("used",  // column_name
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
        false,                   // is_autoincrement
        used_default,
        used_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj version_default;
    version_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("version",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_SERVER_VERSION_LENGTH,   // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false,                      // is_autoincrement
        version_default,
        version_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj compatible_default;
    compatible_default.set_int(1);
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

  if (OB_SUCC(ret)) {
    ObObj enabled_default;
    enabled_default.set_int(1);
    ADD_COLUMN_SCHEMA_T("enabled",  // column_name
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
        enabled_default,
        enabled_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj format_default;
    format_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("format",  // column_name
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
        format_default,
        format_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj outline_params_default;
    outline_params_default.set_varbinary(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("outline_params",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_BINARY,                    // column_collation_type
        OB_MAX_OUTLINE_PARAMS_LENGTH,      // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false,                             // is_autoincrement
        outline_params_default,
        outline_params_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("outline_target",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObLongTextType,                  // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        0,                               // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj sql_id_default;
    sql_id_default.set_varbinary(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("sql_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_BINARY,            // column_collation_type
        OB_MAX_SQL_ID_LENGTH,      // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false,                     // is_autoincrement
        sql_id_default,
        sql_id_default);  // default_value
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

int ObInnerTableSchema::all_outline_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_OUTLINE_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_OUTLINE_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("outline_id",  // column_name
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
    ObObj name_default;
    name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("name",      // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_INVALID,             // column_collation_type
        OB_MAX_OUTLINE_NAME_LENGTH,  // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false,                       // is_autoincrement
        name_default,
        name_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj signature_default;
    signature_default.set_varbinary(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("signature",      // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_BINARY,                   // column_collation_type
        OB_MAX_OUTLINE_SIGNATURE_LENGTH,  // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false,                            // is_autoincrement
        signature_default,
        signature_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("outline_content",  // column_name
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
    ObObj owner_default;
    owner_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("owner",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_INVALID,          // column_collation_type
        OB_MAX_USERNAME_LENGTH,   // column_length
        -1,                       // column_precision
        -1,                       // column_scale
        true,                     // is_nullable
        false,                    // is_autoincrement
        owner_default,
        owner_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj used_default;
    used_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("used",  // column_name
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
        false,                   // is_autoincrement
        used_default,
        used_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj version_default;
    version_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("version",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_INVALID,            // column_collation_type
        OB_SERVER_VERSION_LENGTH,   // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false,                      // is_autoincrement
        version_default,
        version_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj compatible_default;
    compatible_default.set_int(1);
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
        true,                          // is_nullable
        false,                         // is_autoincrement
        compatible_default,
        compatible_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj enabled_default;
    enabled_default.set_int(1);
    ADD_COLUMN_SCHEMA_T("enabled",  // column_name
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
        enabled_default,
        enabled_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj format_default;
    format_default.set_int(0);
    ADD_COLUMN_SCHEMA_T("format",  // column_name
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
        false,                     // is_autoincrement
        format_default,
        format_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj outline_params_default;
    outline_params_default.set_varbinary(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("outline_params",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_BINARY,                    // column_collation_type
        OB_MAX_OUTLINE_PARAMS_LENGTH,      // column_length
        -1,                                // column_precision
        -1,                                // column_scale
        true,                              // is_nullable
        false,                             // is_autoincrement
        outline_params_default,
        outline_params_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("outline_target",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObLongTextType,                  // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        0,                               // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj sql_id_default;
    sql_id_default.set_varbinary(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("sql_id",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_BINARY,            // column_collation_type
        OB_MAX_SQL_ID_LENGTH,      // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false,                     // is_autoincrement
        sql_id_default,
        sql_id_default);  // default_value
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

int ObInnerTableSchema::all_election_event_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_ELECTION_EVENT_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(6);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_ELECTION_EVENT_HISTORY_TNAME))) {
      LOG_ERROR("fail to set table_name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_compress_func_name(OB_DEFAULT_COMPRESS_FUNC_NAME))) {
      LOG_ERROR("fail to set compress_func_name", K(ret));
    }
  }
  table_schema.set_part_level(PARTITION_LEVEL_ONE);
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
    ADD_COLUMN_SCHEMA("table_id",  // column_name
        ++column_id,               // column_id
        2,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("partition_idx",  // column_name
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
    ADD_COLUMN_SCHEMA("partition_cnt",  // column_name
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
    ADD_COLUMN_SCHEMA("event",           // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_INVALID,                 // column_collation_type
        MAX_ELECTION_EVENT_DESC_LENGTH,  // column_length
        -1,                              // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ObObj leader_default;
    leader_default.set_varchar(ObString::make_string("0.0.0.0"));
    ADD_COLUMN_SCHEMA_T("leader",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_INVALID,           // column_collation_type
        OB_IP_PORT_STR_BUFF,       // column_length
        -1,                        // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false,                     // is_autoincrement
        leader_default,
        leader_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj info_default;
    info_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("info",                // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObVarcharType,                         // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        MAX_ELECTION_EVENT_EXTRA_INFO_LENGTH,  // column_length
        -1,                                    // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false,                                 // is_autoincrement
        info_default,
        info_default);  // default_value
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

int ObInnerTableSchema::all_recyclebin_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_RECYCLEBIN_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_RECYCLEBIN_TNAME))) {
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
    ADD_COLUMN_SCHEMA("object_name",  // column_name
        ++column_id,                  // column_id
        2,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_INVALID,              // column_collation_type
        OB_MAX_OBJECT_NAME_LENGTH,    // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
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
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("original_name",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_INVALID,                // column_collation_type
        OB_MAX_ORIGINAL_NANE_LENGTH,    // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
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

int ObInnerTableSchema::all_part_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_PART_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PART_TNAME))) {
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
    ADD_COLUMN_SCHEMA("part_id",  // column_name
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
    ObObj part_name_default;
    part_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("part_name",   // column_name
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
        false,                         // is_autoincrement
        part_name_default,
        part_name_default);  // default_value
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
    ADD_COLUMN_SCHEMA("sub_part_space",  // column_name
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
    ADD_COLUMN_SCHEMA("new_sub_part_num",  // column_name
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
    ADD_COLUMN_SCHEMA("new_sub_part_space",  // column_name
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
    ADD_COLUMN_SCHEMA("sub_part_interval",  // column_name
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
    ADD_COLUMN_SCHEMA("sub_interval_start",  // column_name
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
    ADD_COLUMN_SCHEMA("new_sub_part_interval",  // column_name
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
    ADD_COLUMN_SCHEMA("new_sub_interval_start",  // column_name
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
        true,                    // is_nullable
        false);                  // is_autoincrement
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
        true,                      // is_nullable
        false);                    // is_autoincrement
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
    ObObj mapping_pg_part_id_default;
    mapping_pg_part_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("mapping_pg_part_id",  // column_name
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
        mapping_pg_part_id_default,
        mapping_pg_part_id_default);  // default_value
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
        false,                                  // is_nullable
        false,                                  // is_autoincrement
        drop_schema_version_default,
        drop_schema_version_default);  // default_value
  }

  if (OB_SUCC(ret)) {
    ObObj max_used_sub_part_id_default;
    max_used_sub_part_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("max_used_sub_part_id",  // column_name
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
        max_used_sub_part_id_default,
        max_used_sub_part_id_default);  // default_value
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

int ObInnerTableSchema::all_part_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_PART_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_PART_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("part_id",  // column_name
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
    ObObj part_name_default;
    part_name_default.set_varchar(ObString::make_string(""));
    ADD_COLUMN_SCHEMA_T("part_name",   // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_INVALID,               // column_collation_type
        OB_MAX_PARTITION_NAME_LENGTH,  // column_length
        -1,                            // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false,                         // is_autoincrement
        part_name_default,
        part_name_default);  // default_value
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
    ADD_COLUMN_SCHEMA("sub_part_space",  // column_name
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
    ADD_COLUMN_SCHEMA("new_sub_part_num",  // column_name
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
    ADD_COLUMN_SCHEMA("new_sub_part_space",  // column_name
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
    ADD_COLUMN_SCHEMA("sub_part_interval",  // column_name
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
    ADD_COLUMN_SCHEMA("sub_interval_start",  // column_name
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
    ADD_COLUMN_SCHEMA("new_sub_part_interval",  // column_name
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
    ADD_COLUMN_SCHEMA("new_sub_interval_start",  // column_name
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
        true,                    // is_nullable
        false);                  // is_autoincrement
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
        true,                      // is_nullable
        false);                    // is_autoincrement
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
    ObObj mapping_pg_part_id_default;
    mapping_pg_part_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("mapping_pg_part_id",  // column_name
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
        mapping_pg_part_id_default,
        mapping_pg_part_id_default);  // default_value
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
    ObObj max_used_sub_part_id_default;
    max_used_sub_part_id_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("max_used_sub_part_id",  // column_name
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
        max_used_sub_part_id_default,
        max_used_sub_part_id_default);  // default_value
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

int ObInnerTableSchema::all_sub_part_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SUB_PART_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SUB_PART_TNAME))) {
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
    ADD_COLUMN_SCHEMA("part_id",  // column_name
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
    ADD_COLUMN_SCHEMA("sub_part_id",  // column_name
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
        true,                    // is_nullable
        false);                  // is_autoincrement
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
    ObObj sub_part_idx_default;
    sub_part_idx_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("sub_part_idx",  // column_name
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
        sub_part_idx_default,
        sub_part_idx_default);  // default_value
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
        false,                                  // is_nullable
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
        false,                                     // is_nullable
        false,                                     // is_autoincrement
        mapping_pg_sub_part_id_default,
        mapping_pg_sub_part_id_default);  // default_value
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
        false,                                  // is_nullable
        false,                                  // is_autoincrement
        drop_schema_version_default,
        drop_schema_version_default);  // default_value
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

int ObInnerTableSchema::all_sub_part_history_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_SUB_PART_HISTORY_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(SYSTEM_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_SUB_PART_HISTORY_TNAME))) {
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
    ADD_COLUMN_SCHEMA("part_id",  // column_name
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
    ADD_COLUMN_SCHEMA("sub_part_id",  // column_name
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
        true,                    // is_nullable
        false);                  // is_autoincrement
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
    ObObj sub_part_idx_default;
    sub_part_idx_default.set_int(-1);
    ADD_COLUMN_SCHEMA_T("sub_part_idx",  // column_name
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
        sub_part_idx_default,
        sub_part_idx_default);  // default_value
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

}  // end namespace share
}  // end namespace oceanbase
