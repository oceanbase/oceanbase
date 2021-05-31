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

int ObInnerTableSchema::all_virtual_reserved_table_mgr_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_RESERVED_TABLE_MGR_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(11);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_RESERVED_TABLE_MGR_TNAME))) {
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
    ADD_COLUMN_SCHEMA("svr_ip",   // column_name
        ++column_id,              // column_id
        1,                        // rowkey_id
        0,                        // index_id
        1,                        // part_key_pos
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
    ADD_COLUMN_SCHEMA("table_id",  // column_name
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
    ADD_COLUMN_SCHEMA("table_type",  // column_name
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
    ADD_COLUMN_SCHEMA("partition_id",  // column_name
        ++column_id,                   // column_id
        6,                             // rowkey_id
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
    ADD_COLUMN_SCHEMA("base_version",  // column_name
        ++column_id,                   // column_id
        8,                             // rowkey_id
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
        9,                                    // rowkey_id
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
        10,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("version",  // column_name
        ++column_id,              // column_id
        11,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("reserve_type",       // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        OB_MAX_RESERVED_POINT_TYPE_LENGTH,  // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("reserve_point_version",  // column_name
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

int ObInnerTableSchema::all_virtual_backupset_history_mgr_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_BACKUPSET_HISTORY_MGR_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_BACKUPSET_HISTORY_MGR_TNAME))) {
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("backup_recovery_window",  // column_name
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

int ObInnerTableSchema::all_virtual_backup_backupset_task_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_BACKUP_BACKUPSET_TASK_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_BACKUP_BACKUPSET_TASK_TNAME))) {
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
    ADD_COLUMN_SCHEMA("src_backup_dest",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_BACKUP_DEST_LENGTH,        // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("dst_backup_dest",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        OB_MAX_BACKUP_DEST_LENGTH,        // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("src_device_type",      // column_name
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
    ADD_COLUMN_SCHEMA("dst_device_type",      // column_name
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

int ObInnerTableSchema::all_virtual_pg_backup_backupset_task_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PG_BACKUP_BACKUPSET_TASK_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(7);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PG_BACKUP_BACKUPSET_TASK_TNAME))) {
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

int ObInnerTableSchema::all_virtual_backup_backup_log_archive_status_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_BACKUP_BACKUP_LOG_ARCHIVE_STATUS_TNAME))) {
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
    ADD_COLUMN_SCHEMA("copy_id",  // column_name
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

int ObInnerTableSchema::all_virtual_global_transaction_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_GLOBAL_TRANSACTION_TNAME))) {
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
    ADD_COLUMN_SCHEMA("gtrid",  // column_name
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
    ADD_COLUMN_SCHEMA("bqual",  // column_name
        ++column_id,            // column_id
        3,                      // rowkey_id
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
        4,                            // rowkey_id
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

}  // end namespace share
}  // end namespace oceanbase
