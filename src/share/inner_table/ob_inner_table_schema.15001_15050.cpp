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

int ObInnerTableSchema::all_virtual_table_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TABLE_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TABLE_AGENT_TNAME))) {
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TABLE_NAME",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_UTF8MB4_BIN,         // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,    // column_length
        2,                           // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
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
    ADD_COLUMN_SCHEMA("TABLE_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("LOAD_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("DEF_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("ROWKEY_COLUMN_NUM",  // column_name
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
    ADD_COLUMN_SCHEMA("INDEX_COLUMN_NUM",  // column_name
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
    ADD_COLUMN_SCHEMA("MAX_USED_COLUMN_ID",  // column_name
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
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("AUTOINC_COLUMN_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("AUTO_INCREMENT",  // column_name
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
    ADD_COLUMN_SCHEMA("READ_ONLY",  // column_name
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
    ADD_COLUMN_SCHEMA("ROWKEY_SPLIT_POS",  // column_name
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
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("EXPIRE_CONDITION",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_UTF8MB4_BIN,               // column_collation_type
        OB_MAX_EXPIRE_INFO_STRING_LENGTH,  // column_length
        2,                                 // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IS_USE_BLOOMFILTER",  // column_name
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
    ADD_COLUMN_SCHEMA("COMMENT",   // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        MAX_TABLE_COMMENT_LENGTH,  // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
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
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COLLATION_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("DATA_TABLE_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("INDEX_STATUS",  // column_name
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
    ADD_COLUMN_SCHEMA("TABLEGROUP_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("PROGRESSIVE_MERGE_NUM",  // column_name
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
    ADD_COLUMN_SCHEMA("INDEX_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("PART_LEVEL",  // column_name
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
    ADD_COLUMN_SCHEMA("PART_FUNC_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("PART_FUNC_EXPR",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_PART_FUNC_EXPR_LENGTH,    // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PART_NUM",  // column_name
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
    ADD_COLUMN_SCHEMA("SUB_PART_FUNC_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("SUB_PART_FUNC_EXPR",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_UTF8MB4_BIN,                 // column_collation_type
        OB_MAX_PART_FUNC_EXPR_LENGTH,        // column_length
        2,                                   // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SUB_PART_NUM",  // column_name
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
    ADD_COLUMN_SCHEMA("CREATE_MEM_VERSION",  // column_name
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
    ADD_COLUMN_SCHEMA("VIEW_DEFINITION",  // column_name
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
    ADD_COLUMN_SCHEMA("VIEW_CHECK_OPTION",  // column_name
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
    ADD_COLUMN_SCHEMA("VIEW_IS_UPDATABLE",  // column_name
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
    ADD_COLUMN_SCHEMA("ZONE_LIST",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_ZONE_LIST_LENGTH,       // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIMARY_ZONE",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        MAX_ZONE_LENGTH,               // column_length
        2,                             // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("INDEX_USING_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("PARSER_NAME",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        OB_MAX_PARSER_NAME_LENGTH,    // column_length
        2,                            // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("INDEX_ATTRIBUTES_SET",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObNumberType,                          // column_type
        CS_TYPE_INVALID,                       // column_collation_type
        38,                                    // column_length
        38,                                    // column_precision
        0,                                     // column_scale
        true,                                  // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LOCALITY",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        MAX_LOCALITY_LENGTH,       // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TABLET_SIZE",  // column_name
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
    ADD_COLUMN_SCHEMA("PCTFREE",  // column_name
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
    ADD_COLUMN_SCHEMA("PREVIOUS_LOCALITY",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_UTF8MB4_BIN,                // column_collation_type
        MAX_LOCALITY_LENGTH,                // column_length
        2,                                  // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MAX_USED_PART_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("PARTITION_CNT_WITHIN_PARTITION_TABLE",  // column_name
        ++column_id,                                           // column_id
        0,                                                     // rowkey_id
        0,                                                     // index_id
        0,                                                     // part_key_pos
        ObNumberType,                                          // column_type
        CS_TYPE_INVALID,                                       // column_collation_type
        38,                                                    // column_length
        38,                                                    // column_precision
        0,                                                     // column_scale
        false,                                                 // is_nullable
        false);                                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PARTITION_STATUS",  // column_name
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
    ADD_COLUMN_SCHEMA("PARTITION_SCHEMA_VERSION",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObNumberType,                              // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        38,                                        // column_length
        38,                                        // column_precision
        0,                                         // column_scale
        true,                                      // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MAX_USED_CONSTRAINT_ID",  // column_name
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
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PK_COMMENT",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_UTF8MB4_BIN,         // column_collation_type
        MAX_TABLE_COMMENT_LENGTH,    // column_length
        2,                           // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SESS_ACTIVE_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("ROW_STORE_TYPE",   // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_UTF8MB4_BIN,              // column_collation_type
        OB_MAX_STORE_FORMAT_NAME_LENGTH,  // column_length
        2,                                // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("STORE_FORMAT",     // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_UTF8MB4_BIN,              // column_collation_type
        OB_MAX_STORE_FORMAT_NAME_LENGTH,  // column_length
        2,                                // column_precision
        -1,                               // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DUPLICATE_SCOPE",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObNumberType,                     // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        38,                               // column_length
        38,                               // column_precision
        0,                                // column_scale
        true,                             // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("BINDING",  // column_name
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PROGRESSIVE_MERGE_ROUND",  // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObNumberType,                             // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        38,                                       // column_length
        38,                                       // column_precision
        0,                                        // column_scale
        true,                                     // is_nullable
        false);                                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("STORAGE_FORMAT_VERSION",  // column_name
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
    ADD_COLUMN_SCHEMA("TABLE_MODE",  // column_name
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
    ADD_COLUMN_SCHEMA("ENCRYPTION",     // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_ENCRYPTION_NAME_LENGTH,  // column_length
        2,                              // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
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
    ADD_COLUMN_SCHEMA("DROP_SCHEMA_VERSION",  // column_name
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
    ADD_COLUMN_SCHEMA("IS_SUB_PART_TEMPLATE",  // column_name
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
    ADD_COLUMN_SCHEMA("DOP",  // column_name
        ++column_id,          // column_id
        0,                    // rowkey_id
        0,                    // index_id
        0,                    // part_key_pos
        ObNumberType,         // column_type
        CS_TYPE_INVALID,      // column_collation_type
        38,                   // column_length
        38,                   // column_precision
        0,                    // column_scale
        false,                // is_nullable
        false);               // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CHARACTER_SET_CLIENT",  // column_name
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
    ADD_COLUMN_SCHEMA("COLLATION_CONNECTION",  // column_name
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
    ADD_COLUMN_SCHEMA("AUTO_PART_SIZE",  // column_name
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
    ADD_COLUMN_SCHEMA("AUTO_PART",  // column_name
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

int ObInnerTableSchema::all_virtual_column_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_COLUMN_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_COLUMN_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("COLUMN_ID",  // column_name
        ++column_id,                // column_id
        3,                          // rowkey_id
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COLUMN_NAME",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        OB_MAX_COLUMN_NAME_LENGTH,    // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ROWKEY_POSITION",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("INDEX_POSITION",  // column_name
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
    ADD_COLUMN_SCHEMA("ORDER_IN_ROWKEY",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("PARTITION_KEY_POSITION",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObNumberType,                            // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        38,                                      // column_length
        38,                                      // column_precision
        0,                                       // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DATA_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("DATA_LENGTH",  // column_name
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
    ADD_COLUMN_SCHEMA("DATA_PRECISION",  // column_name
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
    ADD_COLUMN_SCHEMA("DATA_SCALE",  // column_name
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
    ADD_COLUMN_SCHEMA("NULLABLE",  // column_name
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
    ADD_COLUMN_SCHEMA("ON_UPDATE_CURRENT_TIMESTAMP",  // column_name
        ++column_id,                                  // column_id
        0,                                            // rowkey_id
        0,                                            // index_id
        0,                                            // part_key_pos
        ObNumberType,                                 // column_type
        CS_TYPE_INVALID,                              // column_collation_type
        38,                                           // column_length
        38,                                           // column_precision
        0,                                            // column_scale
        false,                                        // is_nullable
        false);                                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("AUTOINCREMENT",  // column_name
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
    ADD_COLUMN_SCHEMA("IS_HIDDEN",  // column_name
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
    ADD_COLUMN_SCHEMA("COLLATION_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("ORIG_DEFAULT_VALUE",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_UTF8MB4_BIN,                 // column_collation_type
        OB_MAX_DEFAULT_VALUE_LENGTH,         // column_length
        2,                                   // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CUR_DEFAULT_VALUE",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_UTF8MB4_BIN,                // column_collation_type
        OB_MAX_DEFAULT_VALUE_LENGTH,        // column_length
        2,                                  // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COMMENT",  // column_name
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
    ADD_COLUMN_SCHEMA("COLUMN_FLAGS",  // column_name
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
    ADD_COLUMN_SCHEMA("PREV_COLUMN_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("EXTENDED_TYPE_INFO",  // column_name
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
    ADD_COLUMN_SCHEMA("ORIG_DEFAULT_VALUE_V2",  // column_name
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
    ADD_COLUMN_SCHEMA("CUR_DEFAULT_VALUE_V2",  // column_name
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

int ObInnerTableSchema::all_virtual_database_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_DATABASE_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_DATABASE_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("DATABASE_ID",  // column_name
        ++column_id,                  // column_id
        2,                            // rowkey_id
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DATABASE_NAME",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
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
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ZONE_LIST",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_ZONE_LIST_LENGTH,       // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIMARY_ZONE",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        MAX_ZONE_LENGTH,               // column_length
        2,                             // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COLLATION_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("COMMENT",      // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        MAX_DATABASE_COMMENT_LENGTH,  // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("READ_ONLY",  // column_name
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
    ADD_COLUMN_SCHEMA("DEFAULT_TABLEGROUP_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("IN_RECYCLEBIN",  // column_name
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
    ADD_COLUMN_SCHEMA("DROP_SCHEMA_VERSION",  // column_name
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

int ObInnerTableSchema::all_virtual_sequence_v2_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SEQUENCE_V2_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SEQUENCE_V2_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("SEQUENCE_KEY",  // column_name
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
    ADD_COLUMN_SCHEMA("COLUMN_ID",  // column_name
        ++column_id,                // column_id
        3,                          // rowkey_id
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SEQUENCE_NAME",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_SEQUENCE_NAME_LENGTH,    // column_length
        2,                              // column_precision
        -1,                             // column_scale
        true,                           // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SEQUENCE_VALUE",  // column_name
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
    ADD_COLUMN_SCHEMA("SYNC_VALUE",  // column_name
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

int ObInnerTableSchema::all_virtual_part_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PART_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PART_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("PART_ID",  // column_name
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PART_NAME",     // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_PARTITION_NAME_LENGTH,  // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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
    ADD_COLUMN_SCHEMA("SUB_PART_NUM",  // column_name
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
    ADD_COLUMN_SCHEMA("SUB_PART_SPACE",  // column_name
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
    ADD_COLUMN_SCHEMA("NEW_SUB_PART_NUM",  // column_name
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
    ADD_COLUMN_SCHEMA("NEW_SUB_PART_SPACE",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObNumberType,                        // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        38,                                  // column_length
        38,                                  // column_precision
        0,                                   // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SUB_PART_INTERVAL",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_UTF8MB4_BIN,                // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,       // column_length
        2,                                  // column_precision
        -1,                                 // column_scale
        true,                               // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SUB_INTERVAL_START",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_UTF8MB4_BIN,                 // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,        // column_length
        2,                                   // column_precision
        -1,                                  // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("NEW_SUB_PART_INTERVAL",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_UTF8MB4_BIN,                    // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,           // column_length
        2,                                      // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("NEW_SUB_INTERVAL_START",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObVarcharType,                           // column_type
        CS_TYPE_UTF8MB4_BIN,                     // column_collation_type
        OB_MAX_PARTITION_EXPR_LENGTH,            // column_length
        2,                                       // column_precision
        -1,                                      // column_scale
        true,                                    // is_nullable
        false);                                  // is_autoincrement
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
    ADD_COLUMN_SCHEMA("PART_IDX",  // column_name
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
    ADD_COLUMN_SCHEMA("MAPPING_PG_PART_ID",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObNumberType,                        // column_type
        CS_TYPE_INVALID,                     // column_collation_type
        38,                                  // column_length
        38,                                  // column_precision
        0,                                   // column_scale
        true,                                // is_nullable
        false);                              // is_autoincrement
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
    ADD_COLUMN_SCHEMA("DROP_SCHEMA_VERSION",  // column_name
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
    ADD_COLUMN_SCHEMA("MAX_USED_SUB_PART_ID",  // column_name
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

int ObInnerTableSchema::all_virtual_sub_part_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SUB_PART_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SUB_PART_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("PART_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("SUB_PART_ID",  // column_name
        ++column_id,                  // column_id
        4,                            // rowkey_id
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
        false,                         // is_nullable
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
        false,                                // is_nullable
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
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DROP_SCHEMA_VERSION",  // column_name
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

int ObInnerTableSchema::all_virtual_package_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PACKAGE_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PACKAGE_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("PACKAGE_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("PACKAGE_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_PACKAGE_NAME_LENGTH,    // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
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
    ADD_COLUMN_SCHEMA("TYPE",  // column_name
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

int ObInnerTableSchema::all_virtual_tenant_meta_table_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_META_TABLE_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_META_TABLE_AGENT_TNAME))) {
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

int ObInnerTableSchema::all_virtual_sql_audit_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SQL_AUDIT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SQL_AUDIT_ORA_TNAME))) {
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
    ADD_COLUMN_SCHEMA("SVR_IP",  // column_name
        ++column_id,             // column_id
        1,                       // rowkey_id
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
        2,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        ++column_id,                // column_id
        3,                          // rowkey_id
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
    ADD_COLUMN_SCHEMA("REQUEST_ID",  // column_name
        ++column_id,                 // column_id
        4,                           // rowkey_id
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
    ADD_COLUMN_SCHEMA("TRACE_ID",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        OB_MAX_HOST_NAME_LENGTH,   // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
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
    ADD_COLUMN_SCHEMA("CLIENT_PORT",  // column_name
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
    ADD_COLUMN_SCHEMA("TENANT_NAME",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        OB_MAX_TENANT_NAME_LENGTH,    // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("EFFECTIVE_TENANT_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("DB_NAME",      // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,  // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SQL_ID",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_UTF8MB4_BIN,     // column_collation_type
        OB_MAX_SQL_ID_LENGTH,    // column_length
        2,                       // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("QUERY_SQL",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObLongTextType,             // column_type
        CS_TYPE_INVALID,            // column_collation_type
        0,                          // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PLAN_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("AFFECTED_ROWS",  // column_name
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
    ADD_COLUMN_SCHEMA("RETURN_ROWS",  // column_name
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
    ADD_COLUMN_SCHEMA("RET_CODE",  // column_name
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
    ADD_COLUMN_SCHEMA("QC_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("DFO_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("SQC_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("WORKER_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("EVENT",          // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_WAIT_EVENT_NAME_LENGTH,  // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P1TEXT",          // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_WAIT_EVENT_PARAM_LENGTH,  // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P1",  // column_name
        ++column_id,         // column_id
        0,                   // rowkey_id
        0,                   // index_id
        0,                   // part_key_pos
        ObNumberType,        // column_type
        CS_TYPE_INVALID,     // column_collation_type
        38,                  // column_length
        38,                  // column_precision
        0,                   // column_scale
        false,               // is_nullable
        false);              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P2TEXT",          // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_WAIT_EVENT_PARAM_LENGTH,  // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P2",  // column_name
        ++column_id,         // column_id
        0,                   // rowkey_id
        0,                   // index_id
        0,                   // part_key_pos
        ObNumberType,        // column_type
        CS_TYPE_INVALID,     // column_collation_type
        38,                  // column_length
        38,                  // column_precision
        0,                   // column_scale
        false,               // is_nullable
        false);              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P3TEXT",          // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_WAIT_EVENT_PARAM_LENGTH,  // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P3",  // column_name
        ++column_id,         // column_id
        0,                   // rowkey_id
        0,                   // index_id
        0,                   // part_key_pos
        ObNumberType,        // column_type
        CS_TYPE_INVALID,     // column_collation_type
        38,                  // column_length
        38,                  // column_precision
        0,                   // column_scale
        false,               // is_nullable
        false);              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LEVEL",  // column_name
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
    ADD_COLUMN_SCHEMA("WAIT_CLASS_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("WAIT_CLASS#",  // column_name
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
    ADD_COLUMN_SCHEMA("WAIT_CLASS",      // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_WAIT_EVENT_PARAM_LENGTH,  // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("STATE",  // column_name
        ++column_id,            // column_id
        0,                      // rowkey_id
        0,                      // index_id
        0,                      // part_key_pos
        ObVarcharType,          // column_type
        CS_TYPE_UTF8MB4_BIN,    // column_collation_type
        19,                     // column_length
        2,                      // column_precision
        -1,                     // column_scale
        false,                  // is_nullable
        false);                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("WAIT_TIME_MICRO",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("TOTAL_WAIT_TIME_MICRO",  // column_name
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
    ADD_COLUMN_SCHEMA("TOTAL_WAITS",  // column_name
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
    ADD_COLUMN_SCHEMA("RPC_COUNT",  // column_name
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
    ADD_COLUMN_SCHEMA("PLAN_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("IS_INNER_SQL",  // column_name
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
    ADD_COLUMN_SCHEMA("IS_EXECUTOR_RPC",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("IS_HIT_PLAN",  // column_name
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
    ADD_COLUMN_SCHEMA("REQUEST_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("ELAPSED_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("NET_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("NET_WAIT_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("QUEUE_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("DECODE_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("GET_PLAN_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("EXECUTE_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("APPLICATION_WAIT_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("CONCURRENCY_WAIT_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("USER_IO_WAIT_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("SCHEDULE_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("ROW_CACHE_HIT",  // column_name
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
    ADD_COLUMN_SCHEMA("BLOOM_FILTER_CACHE_HIT",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObNumberType,                            // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        38,                                      // column_length
        38,                                      // column_precision
        0,                                       // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("BLOCK_CACHE_HIT",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("BLOCK_INDEX_CACHE_HIT",  // column_name
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
    ADD_COLUMN_SCHEMA("DISK_READS",  // column_name
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
    ADD_COLUMN_SCHEMA("EXECUTION_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("RETRY_CNT",  // column_name
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
    ADD_COLUMN_SCHEMA("TABLE_SCAN",  // column_name
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
    ADD_COLUMN_SCHEMA("CONSISTENCY_LEVEL",  // column_name
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
    ADD_COLUMN_SCHEMA("MEMSTORE_READ_ROW_COUNT",  // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObNumberType,                             // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        38,                                       // column_length
        38,                                       // column_precision
        0,                                        // column_scale
        false,                                    // is_nullable
        false);                                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SSSTORE_READ_ROW_COUNT",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObNumberType,                            // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        38,                                      // column_length
        38,                                      // column_precision
        0,                                       // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("REQUEST_MEMORY_USED",  // column_name
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
    ADD_COLUMN_SCHEMA("EXPECTED_WORKER_COUNT",  // column_name
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
    ADD_COLUMN_SCHEMA("USED_WORKER_COUNT",  // column_name
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
    ADD_COLUMN_SCHEMA("SCHED_INFO",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_UTF8MB4_BIN,         // column_collation_type
        16384,                       // column_length
        2,                           // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("FUSE_ROW_CACHE_HIT",  // column_name
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
    ADD_COLUMN_SCHEMA("PS_STMT_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("TRANSACTION_HASH",  // column_name
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
    ADD_COLUMN_SCHEMA("REQUEST_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("IS_BATCHED_MULTI_STMT",  // column_name
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
    ADD_COLUMN_SCHEMA("OB_TRACE_INFO",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        4096,                           // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PLAN_HASH",  // column_name
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
    ADD_COLUMN_SCHEMA("USER_GROUP",  // column_name
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
    ADD_COLUMN_SCHEMA("LOCK_FOR_READ_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("WAIT_TRX_MIGRATE_TIME",  // column_name
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
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("hash (SVR_IP, SVR_PORT)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(65536);
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

int ObInnerTableSchema::all_virtual_sql_audit_ora_all_virtual_sql_audit_i1_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SQL_AUDIT_ORA_ALL_VIRTUAL_SQL_AUDIT_I1_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SQL_AUDIT_ALL_VIRTUAL_SQL_AUDIT_I1_TNAME))) {
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
    table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
    if (OB_FAIL(table_schema.get_part_option().set_part_expr("hash (SVR_IP, SVR_PORT)"))) {
      LOG_WARN("set_part_expr failed", K(ret));
    }
    table_schema.get_part_option().set_part_num(65536);
    table_schema.set_part_level(PARTITION_LEVEL_ONE);
  }
  table_schema.set_index_using_type(USING_BTREE);
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_store_format(OB_STORE_FORMAT_COMPACT_MYSQL);
  table_schema.set_progressive_merge_round(1);
  table_schema.set_storage_format_version(3);

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        column_id + 3,              // column_id
        1,                          // rowkey_id
        1,                          // index_id
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
    ADD_COLUMN_SCHEMA("REQUEST_ID",  // column_name
        column_id + 4,               // column_id
        2,                           // rowkey_id
        2,                           // index_id
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
    ADD_COLUMN_SCHEMA("SVR_IP",  // column_name
        column_id + 1,           // column_id
        3,                       // rowkey_id
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
        column_id + 2,             // column_id
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
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SQL_AUDIT_ORA_TID));

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_virtual_plan_stat_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PLAN_STAT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PLAN_STAT_ORA_TNAME))) {
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
    ADD_COLUMN_SCHEMA("PLAN_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("SQL_ID",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_UTF8MB4_BIN,     // column_collation_type
        OB_MAX_SQL_ID_LENGTH,    // column_length
        2,                       // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("IS_BIND_SENSITIVE",  // column_name
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
    ADD_COLUMN_SCHEMA("IS_BIND_AWARE",  // column_name
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
    ADD_COLUMN_SCHEMA("STATEMENT",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObLongTextType,             // column_type
        CS_TYPE_INVALID,            // column_collation_type
        0,                          // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("QUERY_SQL",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObLongTextType,             // column_type
        CS_TYPE_INVALID,            // column_collation_type
        0,                          // column_length
        -1,                         // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SPECIAL_PARAMS",  // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_COMMAND_LENGTH,           // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PARAM_INFOS",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObLongTextType,               // column_type
        CS_TYPE_INVALID,              // column_collation_type
        0,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SYS_VARS",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        OB_MAX_COMMAND_LENGTH,     // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PLAN_HASH",  // column_name
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
    ADD_COLUMN_SCHEMA("FIRST_LOAD_TIME",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObTimestampLTZType,               // column_type
        CS_TYPE_INVALID,                  // column_collation_type
        0,                                // column_length
        -1,                               // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
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
    ADD_COLUMN_SCHEMA("MERGED_VERSION",  // column_name
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
    ADD_COLUMN_SCHEMA("LAST_ACTIVE_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("AVG_EXE_USEC",  // column_name
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
    ADD_COLUMN_SCHEMA("SLOWEST_EXE_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("SLOWEST_EXE_USEC",  // column_name
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
    ADD_COLUMN_SCHEMA("SLOW_COUNT",  // column_name
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
    ADD_COLUMN_SCHEMA("HIT_COUNT",  // column_name
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
    ADD_COLUMN_SCHEMA("PLAN_SIZE",  // column_name
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
    ADD_COLUMN_SCHEMA("EXECUTIONS",  // column_name
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
    ADD_COLUMN_SCHEMA("DISK_READS",  // column_name
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
    ADD_COLUMN_SCHEMA("DIRECT_WRITES",  // column_name
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
    ADD_COLUMN_SCHEMA("BUFFER_GETS",  // column_name
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
    ADD_COLUMN_SCHEMA("APPLICATION_WAIT_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("CONCURRENCY_WAIT_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("USER_IO_WAIT_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("ROWS_PROCESSED",  // column_name
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
    ADD_COLUMN_SCHEMA("ELAPSED_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("CPU_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("LARGE_QUERYS",  // column_name
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
    ADD_COLUMN_SCHEMA("DELAYED_LARGE_QUERYS",  // column_name
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
    ADD_COLUMN_SCHEMA("OUTLINE_VERSION",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("OUTLINE_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("OUTLINE_DATA",  // column_name
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
    ADD_COLUMN_SCHEMA("ACS_SEL_INFO",  // column_name
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
    ADD_COLUMN_SCHEMA("TABLE_SCAN",  // column_name
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
    ADD_COLUMN_SCHEMA("EVOLUTION",  // column_name
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
    ADD_COLUMN_SCHEMA("EVO_EXECUTIONS",  // column_name
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
    ADD_COLUMN_SCHEMA("EVO_CPU_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("TIMEOUT_COUNT",  // column_name
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
    ADD_COLUMN_SCHEMA("PS_STMT_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("DELAYED_PX_QUERYS",  // column_name
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
    ADD_COLUMN_SCHEMA("SESSID",  // column_name
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
    ADD_COLUMN_SCHEMA("TEMP_TABLES",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObLongTextType,               // column_type
        CS_TYPE_INVALID,              // column_collation_type
        0,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IS_USE_JIT",  // column_name
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
    ADD_COLUMN_SCHEMA("OBJECT_TYPE",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObLongTextType,               // column_type
        CS_TYPE_INVALID,              // column_collation_type
        0,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ENABLE_BF_CACHE",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("BF_FILTER_CNT",  // column_name
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
    ADD_COLUMN_SCHEMA("BF_ACCESS_CNT",  // column_name
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
    ADD_COLUMN_SCHEMA("ENABLE_ROW_CACHE",  // column_name
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
    ADD_COLUMN_SCHEMA("ROW_CACHE_HIT_CNT",  // column_name
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
    ADD_COLUMN_SCHEMA("ROW_CACHE_MISS_CNT",  // column_name
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
    ADD_COLUMN_SCHEMA("ENABLE_FUSE_ROW_CACHE",  // column_name
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
    ADD_COLUMN_SCHEMA("FUSE_ROW_CACHE_HIT_CNT",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObNumberType,                            // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        38,                                      // column_length
        38,                                      // column_precision
        0,                                       // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("FUSE_ROW_CACHE_MISS_CNT",  // column_name
        ++column_id,                              // column_id
        0,                                        // rowkey_id
        0,                                        // index_id
        0,                                        // part_key_pos
        ObNumberType,                             // column_type
        CS_TYPE_INVALID,                          // column_collation_type
        38,                                       // column_length
        38,                                       // column_precision
        0,                                        // column_scale
        false,                                    // is_nullable
        false);                                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("HINTS_INFO",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObLongTextType,              // column_type
        CS_TYPE_INVALID,             // column_collation_type
        0,                           // column_length
        -1,                          // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("HINTS_ALL_WORKED",  // column_name
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
    ADD_COLUMN_SCHEMA("PL_SCHEMA_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("IS_BATCHED_MULTI_STMT",  // column_name
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

int ObInnerTableSchema::all_virtual_sql_plan_statistics_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SQL_PLAN_STATISTICS_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SQL_PLAN_STATISTICS_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("PLAN_ID",  // column_name
        ++column_id,              // column_id
        4,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("OPERATION_ID",  // column_name
        ++column_id,                   // column_id
        5,                             // rowkey_id
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
    ADD_COLUMN_SCHEMA("EXECUTIONS",  // column_name
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
    ADD_COLUMN_SCHEMA("OUTPUT_ROWS",  // column_name
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
    ADD_COLUMN_SCHEMA("INPUT_ROWS",  // column_name
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
    ADD_COLUMN_SCHEMA("RESCAN_TIMES",  // column_name
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
    ADD_COLUMN_SCHEMA("BUFFER_GETS",  // column_name
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
    ADD_COLUMN_SCHEMA("DISK_READS",  // column_name
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
    ADD_COLUMN_SCHEMA("DISK_WRITES",  // column_name
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
    ADD_COLUMN_SCHEMA("ELAPSED_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("EXTEND_INFO1",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_MONITOR_INFO_LENGTH,    // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("EXTEND_INFO2",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_MONITOR_INFO_LENGTH,    // column_length
        2,                             // column_precision
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

int ObInnerTableSchema::all_virtual_plan_cache_plan_explain_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_ORA_TNAME))) {
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
        3,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("PLAN_ID",  // column_name
        ++column_id,              // column_id
        4,                        // rowkey_id
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
    ADD_COLUMN_SCHEMA("OPERATOR",     // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        OB_MAX_OPERATOR_NAME_LENGTH,  // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("NAME",             // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_UTF8MB4_BIN,              // column_collation_type
        OB_MAX_PLAN_EXPLAIN_NAME_LENGTH,  // column_length
        2,                                // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ROWS",  // column_name
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
    ADD_COLUMN_SCHEMA("COST",  // column_name
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
    ADD_COLUMN_SCHEMA("PROPERTY",         // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_UTF8MB4_BIN,              // column_collation_type
        OB_MAX_OPERATOR_PROPERTY_LENGTH,  // column_length
        2,                                // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PLAN_DEPTH",  // column_name
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
    ADD_COLUMN_SCHEMA("PLAN_LINE_ID",  // column_name
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

int ObInnerTableSchema::all_virtual_sequence_value_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SEQUENCE_VALUE_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SEQUENCE_VALUE_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("SEQUENCE_ID",  // column_name
        ++column_id,                  // column_id
        2,                            // rowkey_id
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("NEXT_VALUE",  // column_name
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

int ObInnerTableSchema::all_virtual_sequence_object_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SEQUENCE_OBJECT_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SEQUENCE_OBJECT_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("SEQUENCE_ID",  // column_name
        ++column_id,                  // column_id
        2,                            // rowkey_id
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
    ADD_COLUMN_SCHEMA("SEQUENCE_NAME",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_SEQUENCE_NAME_LENGTH,    // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MIN_VALUE",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        28,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MAX_VALUE",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObNumberType,               // column_type
        CS_TYPE_INVALID,            // column_collation_type
        38,                         // column_length
        28,                         // column_precision
        0,                          // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("INCREMENT_BY",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObNumberType,                  // column_type
        CS_TYPE_INVALID,               // column_collation_type
        38,                            // column_length
        28,                            // column_precision
        0,                             // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("START_WITH",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        28,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CACHE_SIZE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObNumberType,                // column_type
        CS_TYPE_INVALID,             // column_collation_type
        38,                          // column_length
        28,                          // column_precision
        0,                           // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ORDER_FLAG",  // column_name
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
    ADD_COLUMN_SCHEMA("CYCLE_FLAG",  // column_name
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

int ObInnerTableSchema::all_virtual_user_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_USER_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_USER_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("HOST",     // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_UTF8MB4_BIN,      // column_collation_type
        OB_MAX_HOST_NAME_LENGTH,  // column_length
        2,                        // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PASSWD",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_UTF8MB4_BIN,     // column_collation_type
        OB_MAX_PASSWORD_LENGTH,  // column_length
        2,                       // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("INFO",     // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_UTF8MB4_BIN,      // column_collation_type
        OB_MAX_USER_INFO_LENGTH,  // column_length
        2,                        // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
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
    ADD_COLUMN_SCHEMA("PRIV_SHOW_DB",  // column_name
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
    ADD_COLUMN_SCHEMA("PRIV_CREATE_USER",  // column_name
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
    ADD_COLUMN_SCHEMA("PRIV_SUPER",  // column_name
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
    ADD_COLUMN_SCHEMA("IS_LOCKED",  // column_name
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
    ADD_COLUMN_SCHEMA("PRIV_PROCESS",  // column_name
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
    ADD_COLUMN_SCHEMA("PRIV_CREATE_SYNONYM",  // column_name
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
    ADD_COLUMN_SCHEMA("SSL_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("SSL_CIPHER",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_UTF8MB4_BIN,         // column_collation_type
        1024,                        // column_length
        2,                           // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("X509_ISSUER",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        1024,                         // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("X509_SUBJECT",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        1024,                          // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TYPE",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObNumberType,          // column_type
        CS_TYPE_INVALID,       // column_collation_type
        38,                    // column_length
        38,                    // column_precision
        0,                     // column_scale
        true,                  // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PROFILE_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("PASSWORD_LAST_CHANGED",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObTimestampLTZType,                     // column_type
        CS_TYPE_INVALID,                        // column_collation_type
        0,                                      // column_length
        -1,                                     // column_precision
        -1,                                     // column_scale
        true,                                   // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_FILE",  // column_name
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
    ADD_COLUMN_SCHEMA("PRIV_ALTER_TENANT",  // column_name
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
    ADD_COLUMN_SCHEMA("PRIV_ALTER_SYSTEM",  // column_name
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
    ADD_COLUMN_SCHEMA("PRIV_CREATE_RESOURCE_POOL",  // column_name
        ++column_id,                                // column_id
        0,                                          // rowkey_id
        0,                                          // index_id
        0,                                          // part_key_pos
        ObNumberType,                               // column_type
        CS_TYPE_INVALID,                            // column_collation_type
        38,                                         // column_length
        38,                                         // column_precision
        0,                                          // column_scale
        false,                                      // is_nullable
        false);                                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIV_CREATE_RESOURCE_UNIT",  // column_name
        ++column_id,                                // column_id
        0,                                          // rowkey_id
        0,                                          // index_id
        0,                                          // part_key_pos
        ObNumberType,                               // column_type
        CS_TYPE_INVALID,                            // column_collation_type
        38,                                         // column_length
        38,                                         // column_precision
        0,                                          // column_scale
        false,                                      // is_nullable
        false);                                     // is_autoincrement
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

int ObInnerTableSchema::all_virtual_synonym_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SYNONYM_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SYNONYM_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("SYNONYM_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("SYNONYM_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_SYNONYM_NAME_LENGTH,    // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJECT_NAME",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        OB_MAX_SYNONYM_NAME_LENGTH,   // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJECT_DATABASE_ID",  // column_name
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

int ObInnerTableSchema::all_virtual_foreign_key_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_FOREIGN_KEY_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_FOREIGN_KEY_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("FOREIGN_KEY_ID",  // column_name
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("FOREIGN_KEY_NAME",  // column_name
        ++column_id,                       // column_id
        0,                                 // rowkey_id
        0,                                 // index_id
        0,                                 // part_key_pos
        ObVarcharType,                     // column_type
        CS_TYPE_UTF8MB4_BIN,               // column_collation_type
        OB_MAX_CONSTRAINT_NAME_LENGTH,     // column_length
        2,                                 // column_precision
        -1,                                // column_scale
        false,                             // is_nullable
        false);                            // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CHILD_TABLE_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("PARENT_TABLE_ID",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("UPDATE_ACTION",  // column_name
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
    ADD_COLUMN_SCHEMA("DELETE_ACTION",  // column_name
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
    ADD_COLUMN_SCHEMA("ENABLE_FLAG",  // column_name
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
    ADD_COLUMN_SCHEMA("REF_CST_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("REF_CST_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("VALIDATE_FLAG",  // column_name
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
    ADD_COLUMN_SCHEMA("RELY_FLAG",  // column_name
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

int ObInnerTableSchema::all_virtual_column_stat_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_COLUMN_STAT_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_COLUMN_STAT_AGENT_TNAME))) {
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJECT_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("LAST_ANALYZED",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObTimestampLTZType,             // column_type
        CS_TYPE_INVALID,                // column_collation_type
        0,                              // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DISTINCT_CNT",  // column_name
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
    ADD_COLUMN_SCHEMA("NULL_CNT",  // column_name
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
    ADD_COLUMN_SCHEMA("MAX_VALUE",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_VALUE_LENGTH,           // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("B_MAX_VALUE",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        MAX_VALUE_LENGTH,             // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MIN_VALUE",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_VALUE_LENGTH,           // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("B_MIN_VALUE",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        MAX_VALUE_LENGTH,             // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("AVG_LEN",  // column_name
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
    ADD_COLUMN_SCHEMA("DISTINCT_CNT_SYNOPSIS",  // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_UTF8MB4_BIN,                    // column_collation_type
        MAX_LLC_BITMAP_LENGTH,                  // column_length
        2,                                      // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DISTINCT_CNT_SYNOPSIS_SIZE",  // column_name
        ++column_id,                                 // column_id
        0,                                           // rowkey_id
        0,                                           // index_id
        0,                                           // part_key_pos
        ObNumberType,                                // column_type
        CS_TYPE_INVALID,                             // column_collation_type
        38,                                          // column_length
        38,                                          // column_precision
        0,                                           // column_scale
        false,                                       // is_nullable
        false);                                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SAMPLE_SIZE",  // column_name
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
    ADD_COLUMN_SCHEMA("DENSITY",  // column_name
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
    ADD_COLUMN_SCHEMA("BUCKET_CNT",  // column_name
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
    ADD_COLUMN_SCHEMA("HISTOGRAM_TYPE",  // column_name
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

int ObInnerTableSchema::all_virtual_column_statistic_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_COLUMN_STATISTIC_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_COLUMN_STATISTIC_AGENT_TNAME))) {
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("NUM_DISTINCT",  // column_name
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
    ADD_COLUMN_SCHEMA("NUM_NULL",  // column_name
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
    ADD_COLUMN_SCHEMA("MIN_VALUE",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_VALUE_LENGTH,           // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MAX_VALUE",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_VALUE_LENGTH,           // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LLC_BITMAP",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_UTF8MB4_BIN,         // column_collation_type
        MAX_LLC_BITMAP_LENGTH,       // column_length
        2,                           // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LLC_BITMAP_SIZE",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LAST_REBUILD_VERSION",  // column_name
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

int ObInnerTableSchema::all_virtual_partition_table_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PARTITION_TABLE_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PARTITION_TABLE_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("TO_LEADER_TIME",  // column_name
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

int ObInnerTableSchema::all_virtual_table_stat_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TABLE_STAT_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TABLE_STAT_AGENT_TNAME))) {
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OBJECT_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("LAST_ANALYZED",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObTimestampLTZType,             // column_type
        CS_TYPE_INVALID,                // column_collation_type
        0,                              // column_length
        -1,                             // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SSTABLE_ROW_CNT",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("SSTABLE_AVG_ROW_LEN",  // column_name
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
    ADD_COLUMN_SCHEMA("MACRO_BLK_CNT",  // column_name
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
    ADD_COLUMN_SCHEMA("MICRO_BLK_CNT",  // column_name
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
    ADD_COLUMN_SCHEMA("MEMTABLE_ROW_CNT",  // column_name
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
    ADD_COLUMN_SCHEMA("MEMTABLE_AVG_ROW_LEN",  // column_name
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

int ObInnerTableSchema::all_virtual_recyclebin_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_RECYCLEBIN_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_RECYCLEBIN_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("OBJECT_NAME",  // column_name
        ++column_id,                  // column_id
        2,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        OB_MAX_OBJECT_NAME_LENGTH,    // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
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
    ADD_COLUMN_SCHEMA("TABLEGROUP_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("ORIGINAL_NAME",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_ORIGINAL_NANE_LENGTH,    // column_length
        2,                              // column_precision
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

int ObInnerTableSchema::tenant_virtual_outline_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_TENANT_VIRTUAL_OUTLINE_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_OUTLINE_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("OUTLINE_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("DATABASE_NAME",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
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
    ADD_COLUMN_SCHEMA("OUTLINE_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_OUTLINE_NAME_LENGTH,    // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("VISIBLE_SIGNATURE",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObLongTextType,                     // column_type
        CS_TYPE_INVALID,                    // column_collation_type
        0,                                  // column_length
        -1,                                 // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
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
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OUTLINE_TARGET",  // column_name
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
    ADD_COLUMN_SCHEMA("OUTLINE_SQL",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObLongTextType,               // column_type
        CS_TYPE_INVALID,              // column_collation_type
        0,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
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

int ObInnerTableSchema::all_virtual_routine_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_ROUTINE_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_ROUTINE_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("ROUTINE_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("PACKAGE_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("ROUTINE_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_ROUTINE_NAME_LENGTH,    // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("OVERLOAD",  // column_name
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
    ADD_COLUMN_SCHEMA("SUBPROGRAM_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("ROUTINE_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("PRIV_USER",      // column_name
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
    ADD_COLUMN_SCHEMA("ROUTINE_BODY",  // column_name
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

int ObInnerTableSchema::all_virtual_tablegroup_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TABLEGROUP_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TABLEGROUP_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("TABLEGROUP_ID",  // column_name
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TABLEGROUP_NAME",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_UTF8MB4_BIN,              // column_collation_type
        OB_MAX_TABLEGROUP_NAME_LENGTH,    // column_length
        2,                                // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COMMENT",        // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        MAX_TABLEGROUP_COMMENT_LENGTH,  // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PRIMARY_ZONE",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        MAX_ZONE_LENGTH,               // column_length
        2,                             // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LOCALITY",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        MAX_LOCALITY_LENGTH,       // column_length
        2,                         // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PART_LEVEL",  // column_name
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
    ADD_COLUMN_SCHEMA("PART_FUNC_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("PART_FUNC_EXPR_NUM",  // column_name
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
    ADD_COLUMN_SCHEMA("PART_NUM",  // column_name
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
    ADD_COLUMN_SCHEMA("SUB_PART_FUNC_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("SUB_PART_FUNC_EXPR_NUM",  // column_name
        ++column_id,                             // column_id
        0,                                       // rowkey_id
        0,                                       // index_id
        0,                                       // part_key_pos
        ObNumberType,                            // column_type
        CS_TYPE_INVALID,                         // column_collation_type
        38,                                      // column_length
        38,                                      // column_precision
        0,                                       // column_scale
        false,                                   // is_nullable
        false);                                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SUB_PART_NUM",  // column_name
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
    ADD_COLUMN_SCHEMA("MAX_USED_PART_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("PARTITION_STATUS",  // column_name
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
    ADD_COLUMN_SCHEMA("PREVIOUS_LOCALITY",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_UTF8MB4_BIN,                // column_collation_type
        MAX_LOCALITY_LENGTH,                // column_length
        2,                                  // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PARTITION_SCHEMA_VERSION",  // column_name
        ++column_id,                               // column_id
        0,                                         // rowkey_id
        0,                                         // index_id
        0,                                         // part_key_pos
        ObNumberType,                              // column_type
        CS_TYPE_INVALID,                           // column_collation_type
        38,                                        // column_length
        38,                                        // column_precision
        0,                                         // column_scale
        true,                                      // is_nullable
        false);                                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("BINDING",  // column_name
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DROP_SCHEMA_VERSION",  // column_name
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
    ADD_COLUMN_SCHEMA("IS_SUB_PART_TEMPLATE",  // column_name
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

int ObInnerTableSchema::all_virtual_privilege_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PRIVILEGE_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PRIVILEGE_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("PRIVILEGE",    // column_name
        ++column_id,                  // column_id
        1,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        MAX_COLUMN_PRIVILEGE_LENGTH,  // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CONTEXT",       // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        MAX_PRIVILEGE_CONTEXT_LENGTH,  // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COMMENT",    // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_COLUMN_COMMENT_LENGTH,  // column_length
        2,                          // column_precision
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

int ObInnerTableSchema::all_virtual_sys_parameter_stat_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("SVR_TYPE",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        SERVER_TYPE_LENGTH,        // column_length
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
    ADD_COLUMN_SCHEMA("NAME",    // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_UTF8MB4_BIN,     // column_collation_type
        OB_MAX_CONFIG_NAME_LEN,  // column_length
        2,                       // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DATA_TYPE",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        OB_MAX_CONFIG_TYPE_LENGTH,  // column_length
        2,                          // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("VALUE",    // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_UTF8MB4_BIN,      // column_collation_type
        OB_MAX_CONFIG_VALUE_LEN,  // column_length
        2,                        // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("VALUE_STRICT",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_EXTRA_CONFIG_LENGTH,    // column_length
        2,                             // column_precision
        -1,                            // column_scale
        true,                          // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("INFO",    // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_UTF8MB4_BIN,     // column_collation_type
        OB_MAX_CONFIG_INFO_LEN,  // column_length
        2,                       // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("NEED_REBOOT",  // column_name
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
    ADD_COLUMN_SCHEMA("SECTION",    // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        OB_MAX_CONFIG_SECTION_LEN,  // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("VISIBLE_LEVEL",    // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
        0,                                // index_id
        0,                                // part_key_pos
        ObVarcharType,                    // column_type
        CS_TYPE_UTF8MB4_BIN,              // column_collation_type
        OB_MAX_CONFIG_VISIBLE_LEVEL_LEN,  // column_length
        2,                                // column_precision
        -1,                               // column_scale
        false,                            // is_nullable
        false);                           // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SCOPE",    // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_UTF8MB4_BIN,      // column_collation_type
        OB_MAX_CONFIG_SCOPE_LEN,  // column_length
        2,                        // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SOURCE",    // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        OB_MAX_CONFIG_SOURCE_LEN,  // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("EDIT_LEVEL",    // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_CONFIG_EDIT_LEVEL_LEN,  // column_length
        2,                             // column_precision
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

int ObInnerTableSchema::tenant_virtual_table_index_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_TENANT_VIRTUAL_TABLE_INDEX_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_TABLE_INDEX_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("TABLE_ID",  // column_name
        ++column_id,               // column_id
        1,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("KEY_NAME",   // column_name
        ++column_id,                // column_id
        2,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        OB_MAX_COLUMN_NAME_LENGTH,  // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SEQ_IN_INDEX",  // column_name
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
    ADD_COLUMN_SCHEMA("TABLE_SCHEMA",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,   // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TABLE",     // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,  // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("NON_UNIQUE",  // column_name
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
    ADD_COLUMN_SCHEMA("INDEX_SCHEMA",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,   // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COLUMN_NAME",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        OB_MAX_COLUMN_NAME_LENGTH,    // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COLLATION",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_COLLATION_LENGTH,       // column_length
        2,                          // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CARDINALITY",  // column_name
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
    ADD_COLUMN_SCHEMA("SUB_PART",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        INDEX_SUB_PART_LENGTH,     // column_length
        2,                         // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("PACKED",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_UTF8MB4_BIN,     // column_collation_type
        INDEX_PACKED_LENGTH,     // column_length
        2,                       // column_precision
        -1,                      // column_scale
        true,                    // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("NULL",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObVarcharType,         // column_type
        CS_TYPE_UTF8MB4_BIN,   // column_collation_type
        INDEX_NULL_LENGTH,     // column_length
        2,                     // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("INDEX_TYPE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_UTF8MB4_BIN,         // column_collation_type
        INDEX_NULL_LENGTH,           // column_length
        2,                           // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COMMENT",   // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        MAX_TABLE_COMMENT_LENGTH,  // column_length
        2,                         // column_precision
        -1,                        // column_scale
        true,                      // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("INDEX_COMMENT",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        MAX_TABLE_COMMENT_LENGTH,       // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IS_VISIBLE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_UTF8MB4_BIN,         // column_collation_type
        MAX_COLUMN_YES_NO_LENGTH,    // column_length
        2,                           // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
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

int ObInnerTableSchema::tenant_virtual_charset_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_TENANT_VIRTUAL_CHARSET_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_CHARSET_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("CHARSET",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_UTF8MB4_BIN,      // column_collation_type
        MAX_CHARSET_LENGTH,       // column_length
        2,                        // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DESCRIPTION",     // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        MAX_CHARSET_DESCRIPTION_LENGTH,  // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DEFAULT_COLLATION",  // column_name
        ++column_id,                        // column_id
        0,                                  // rowkey_id
        0,                                  // index_id
        0,                                  // part_key_pos
        ObVarcharType,                      // column_type
        CS_TYPE_UTF8MB4_BIN,                // column_collation_type
        MAX_COLLATION_LENGTH,               // column_length
        2,                                  // column_precision
        -1,                                 // column_scale
        false,                              // is_nullable
        false);                             // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MAX_LENGTH",  // column_name
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

int ObInnerTableSchema::tenant_virtual_all_table_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("DATABASE_ID",  // column_name
        ++column_id,                  // column_id
        1,                            // rowkey_id
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
    ADD_COLUMN_SCHEMA("TABLE_NAME",  // column_name
        ++column_id,                 // column_id
        2,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_UTF8MB4_BIN,         // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,    // column_length
        2,                           // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TABLE_TYPE",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_UTF8MB4_BIN,         // column_collation_type
        OB_MAX_TABLE_TYPE_LENGTH,    // column_length
        2,                           // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ENGINE",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_UTF8MB4_BIN,     // column_collation_type
        MAX_ENGINE_LENGTH,       // column_length
        2,                       // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
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
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ROW_FORMAT",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_UTF8MB4_BIN,         // column_collation_type
        ROW_FORMAT_LENGTH,           // column_length
        2,                           // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ROWS",  // column_name
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
    ADD_COLUMN_SCHEMA("AVG_ROW_LENGTH",  // column_name
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
    ADD_COLUMN_SCHEMA("DATA_LENGTH",  // column_name
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
    ADD_COLUMN_SCHEMA("MAX_DATA_LENGTH",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("INDEX_LENGTH",  // column_name
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
    ADD_COLUMN_SCHEMA("DATA_FREE",  // column_name
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
    ADD_COLUMN_SCHEMA("AUTO_INCREMENT",  // column_name
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
    ADD_COLUMN_SCHEMA("CREATE_TIME",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObTimestampLTZType,           // column_type
        CS_TYPE_INVALID,              // column_collation_type
        0,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("UPDATE_TIME",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObTimestampLTZType,           // column_type
        CS_TYPE_INVALID,              // column_collation_type
        0,                            // column_length
        -1,                           // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CHECK_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("COLLATION",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_COLLATION_LENGTH,       // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CHECKSUM",  // column_name
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
    ADD_COLUMN_SCHEMA("CREATE_OPTIONS",         // column_name
        ++column_id,                            // column_id
        0,                                      // rowkey_id
        0,                                      // index_id
        0,                                      // part_key_pos
        ObVarcharType,                          // column_type
        CS_TYPE_UTF8MB4_BIN,                    // column_collation_type
        MAX_TABLE_STATUS_CREATE_OPTION_LENGTH,  // column_length
        2,                                      // column_precision
        -1,                                     // column_scale
        false,                                  // is_nullable
        false);                                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COMMENT",   // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        MAX_TABLE_COMMENT_LENGTH,  // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
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

int ObInnerTableSchema::tenant_virtual_collation_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_TENANT_VIRTUAL_COLLATION_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_COLLATION_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("COLLATION",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_COLLATION_LENGTH,       // column_length
        2,                          // column_precision
        -1,                         // column_scale
        false,                      // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CHARSET",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_UTF8MB4_BIN,      // column_collation_type
        MAX_CHARSET_LENGTH,       // column_length
        2,                        // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ID",  // column_name
        ++column_id,         // column_id
        0,                   // rowkey_id
        0,                   // index_id
        0,                   // part_key_pos
        ObNumberType,        // column_type
        CS_TYPE_INVALID,     // column_collation_type
        38,                  // column_length
        38,                  // column_precision
        0,                   // column_scale
        false,               // is_nullable
        false);              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IS_DEFAULT",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_UTF8MB4_BIN,         // column_collation_type
        MAX_BOOL_STR_LENGTH,         // column_length
        2,                           // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("IS_COMPILED",  // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        MAX_BOOL_STR_LENGTH,          // column_length
        2,                            // column_precision
        -1,                           // column_scale
        false,                        // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SORTLEN",  // column_name
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

int ObInnerTableSchema::all_virtual_foreign_key_column_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_FOREIGN_KEY_COLUMN_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("FOREIGN_KEY_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("CHILD_COLUMN_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("PARENT_COLUMN_ID",  // column_name
        ++column_id,                       // column_id
        4,                                 // rowkey_id
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

int ObInnerTableSchema::all_virtual_server_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SERVER_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SERVER_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("SVR_IP",  // column_name
        ++column_id,             // column_id
        1,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("ID",  // column_name
        ++column_id,         // column_id
        0,                   // rowkey_id
        0,                   // index_id
        0,                   // part_key_pos
        ObNumberType,        // column_type
        CS_TYPE_INVALID,     // column_collation_type
        38,                  // column_length
        38,                  // column_precision
        0,                   // column_scale
        false,               // is_nullable
        false);              // is_autoincrement
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
    ADD_COLUMN_SCHEMA("INNER_PORT",  // column_name
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
    ADD_COLUMN_SCHEMA("WITH_ROOTSERVER",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("STATUS",   // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_UTF8MB4_BIN,      // column_collation_type
        OB_SERVER_STATUS_LENGTH,  // column_length
        2,                        // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("BLOCK_MIGRATE_IN_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("BUILD_VERSION",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_SERVER_VERSION_LENGTH,       // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("STOP_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("START_SERVICE_TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("FIRST_SESSID",  // column_name
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
    ADD_COLUMN_SCHEMA("WITH_PARTITION",  // column_name
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
    ADD_COLUMN_SCHEMA("LAST_OFFLINE_TIME",  // column_name
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

int ObInnerTableSchema::all_virtual_plan_cache_stat_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_TNAME))) {
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
        3,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("SQL_NUM",  // column_name
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
    ADD_COLUMN_SCHEMA("MEM_USED",  // column_name
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
    ADD_COLUMN_SCHEMA("MEM_HOLD",  // column_name
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
    ADD_COLUMN_SCHEMA("ACCESS_COUNT",  // column_name
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
    ADD_COLUMN_SCHEMA("HIT_COUNT",  // column_name
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
    ADD_COLUMN_SCHEMA("HIT_RATE",  // column_name
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
    ADD_COLUMN_SCHEMA("PLAN_NUM",  // column_name
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
    ADD_COLUMN_SCHEMA("MEM_LIMIT",  // column_name
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
    ADD_COLUMN_SCHEMA("HASH_BUCKET",  // column_name
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
    ADD_COLUMN_SCHEMA("STMTKEY_NUM",  // column_name
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
    ADD_COLUMN_SCHEMA("PC_REF_PLAN_LOCAL",  // column_name
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
    ADD_COLUMN_SCHEMA("PC_REF_PLAN_REMOTE",  // column_name
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
    ADD_COLUMN_SCHEMA("PC_REF_PLAN_DIST",  // column_name
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
    ADD_COLUMN_SCHEMA("PC_REF_PLAN_ARR",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("PC_REF_PLAN_STAT",  // column_name
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
    ADD_COLUMN_SCHEMA("PC_REF_PL",  // column_name
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
    ADD_COLUMN_SCHEMA("PC_REF_PL_STAT",  // column_name
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
    ADD_COLUMN_SCHEMA("PLAN_GEN",  // column_name
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
    ADD_COLUMN_SCHEMA("CLI_QUERY",  // column_name
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
    ADD_COLUMN_SCHEMA("OUTLINE_EXEC",  // column_name
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
    ADD_COLUMN_SCHEMA("PLAN_EXPLAIN",  // column_name
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
    ADD_COLUMN_SCHEMA("ASYN_BASELINE",  // column_name
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
    ADD_COLUMN_SCHEMA("LOAD_BASELINE",  // column_name
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
    ADD_COLUMN_SCHEMA("PS_EXEC",  // column_name
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
    ADD_COLUMN_SCHEMA("GV_SQL",  // column_name
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
    ADD_COLUMN_SCHEMA("PL_ANON",  // column_name
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
    ADD_COLUMN_SCHEMA("PL_ROUTINE",  // column_name
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
    ADD_COLUMN_SCHEMA("PACKAGE_VAR",  // column_name
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
    ADD_COLUMN_SCHEMA("PACKAGE_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("PACKAGE_SPEC",  // column_name
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
    ADD_COLUMN_SCHEMA("PACKAGE_BODY",  // column_name
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
    ADD_COLUMN_SCHEMA("PACKAGE_RESV",  // column_name
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
    ADD_COLUMN_SCHEMA("GET_PKG",  // column_name
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
    ADD_COLUMN_SCHEMA("INDEX_BUILDER",  // column_name
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
    ADD_COLUMN_SCHEMA("PCV_SET",  // column_name
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
    ADD_COLUMN_SCHEMA("PCV_RD",  // column_name
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
    ADD_COLUMN_SCHEMA("PCV_WR",  // column_name
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
    ADD_COLUMN_SCHEMA("PCV_GET_PLAN_KEY",  // column_name
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
    ADD_COLUMN_SCHEMA("PCV_GET_PL_KEY",  // column_name
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
    ADD_COLUMN_SCHEMA("PCV_EXPIRE_BY_USED",  // column_name
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
    ADD_COLUMN_SCHEMA("PCV_EXPIRE_BY_MEM",  // column_name
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

int ObInnerTableSchema::all_virtual_plan_cache_stat_ora_all_virtual_plan_cache_stat_i1_schema(
    ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(
      combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TNAME))) {
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        column_id + 1,              // column_id
        1,                          // rowkey_id
        1,                          // index_id
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
        column_id + 2,           // column_id
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
        column_id + 3,             // column_id
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
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PLAN_CACHE_STAT_ORA_TID));

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_virtual_processlist_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_PROCESSLIST_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_PROCESSLIST_ORA_TNAME))) {
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
    ADD_COLUMN_SCHEMA("ID",  // column_name
        ++column_id,         // column_id
        0,                   // rowkey_id
        0,                   // index_id
        0,                   // part_key_pos
        ObNumberType,        // column_type
        CS_TYPE_INVALID,     // column_collation_type
        38,                  // column_length
        38,                  // column_precision
        0,                   // column_scale
        false,               // is_nullable
        false);              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("USER",    // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_UTF8MB4_BIN,     // column_collation_type
        OB_MAX_USERNAME_LENGTH,  // column_length
        2,                       // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT",           // column_name
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
    ADD_COLUMN_SCHEMA("HOST",     // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_UTF8MB4_BIN,      // column_collation_type
        OB_MAX_HOST_NAME_LENGTH,  // column_length
        2,                        // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("DB",           // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        OB_MAX_DATABASE_NAME_LENGTH,  // column_length
        2,                            // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COMMAND",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_UTF8MB4_BIN,      // column_collation_type
        OB_MAX_COMMAND_LENGTH,    // column_length
        2,                        // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SQL_ID",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
        0,                       // index_id
        0,                       // part_key_pos
        ObVarcharType,           // column_type
        CS_TYPE_UTF8MB4_BIN,     // column_collation_type
        OB_MAX_SQL_ID_LENGTH,    // column_length
        2,                       // column_precision
        -1,                      // column_scale
        false,                   // is_nullable
        false);                  // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TIME",  // column_name
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
    ADD_COLUMN_SCHEMA("STATE",        // column_name
        ++column_id,                  // column_id
        0,                            // rowkey_id
        0,                            // index_id
        0,                            // part_key_pos
        ObVarcharType,                // column_type
        CS_TYPE_UTF8MB4_BIN,          // column_collation_type
        OB_MAX_SESSION_STATE_LENGTH,  // column_length
        2,                            // column_precision
        -1,                           // column_scale
        true,                         // is_nullable
        false);                       // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("INFO",       // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        MAX_COLUMN_VARCHAR_LENGTH,  // column_length
        2,                          // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
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
    ADD_COLUMN_SCHEMA("PROXY_SESSID",  // column_name
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
    ADD_COLUMN_SCHEMA("MASTER_SESSID",  // column_name
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
        true,                            // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("USER_HOST",  // column_name
        ++column_id,                // column_id
        0,                          // rowkey_id
        0,                          // index_id
        0,                          // part_key_pos
        ObVarcharType,              // column_type
        CS_TYPE_UTF8MB4_BIN,        // column_collation_type
        OB_MAX_HOST_NAME_LENGTH,    // column_length
        2,                          // column_precision
        -1,                         // column_scale
        true,                       // is_nullable
        false);                     // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TRANS_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("THREAD_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("SSL_CIPHER",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_UTF8MB4_BIN,         // column_collation_type
        OB_MAX_COMMAND_LENGTH,       // column_length
        2,                           // column_precision
        -1,                          // column_scale
        true,                        // is_nullable
        false);                      // is_autoincrement
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

int ObInnerTableSchema::all_virtual_session_wait_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SESSION_WAIT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SESSION_WAIT_ORA_TNAME))) {
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
    ADD_COLUMN_SCHEMA("SESSION_ID",  // column_name
        ++column_id,                 // column_id
        1,                           // rowkey_id
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
    ADD_COLUMN_SCHEMA("SVR_IP",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
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
        3,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("EVENT",          // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_WAIT_EVENT_NAME_LENGTH,  // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P1TEXT",          // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_WAIT_EVENT_PARAM_LENGTH,  // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P1",  // column_name
        ++column_id,         // column_id
        0,                   // rowkey_id
        0,                   // index_id
        0,                   // part_key_pos
        ObNumberType,        // column_type
        CS_TYPE_INVALID,     // column_collation_type
        38,                  // column_length
        38,                  // column_precision
        0,                   // column_scale
        false,               // is_nullable
        false);              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P2TEXT",          // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_WAIT_EVENT_PARAM_LENGTH,  // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P2",  // column_name
        ++column_id,         // column_id
        0,                   // rowkey_id
        0,                   // index_id
        0,                   // part_key_pos
        ObNumberType,        // column_type
        CS_TYPE_INVALID,     // column_collation_type
        38,                  // column_length
        38,                  // column_precision
        0,                   // column_scale
        false,               // is_nullable
        false);              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P3TEXT",          // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_WAIT_EVENT_PARAM_LENGTH,  // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P3",  // column_name
        ++column_id,         // column_id
        0,                   // rowkey_id
        0,                   // index_id
        0,                   // part_key_pos
        ObNumberType,        // column_type
        CS_TYPE_INVALID,     // column_collation_type
        38,                  // column_length
        38,                  // column_precision
        0,                   // column_scale
        false,               // is_nullable
        false);              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LEVEL",  // column_name
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
    ADD_COLUMN_SCHEMA("WAIT_CLASS_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("WAIT_CLASS#",  // column_name
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
    ADD_COLUMN_SCHEMA("WAIT_CLASS",      // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_WAIT_EVENT_PARAM_LENGTH,  // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("STATE",  // column_name
        ++column_id,            // column_id
        0,                      // rowkey_id
        0,                      // index_id
        0,                      // part_key_pos
        ObVarcharType,          // column_type
        CS_TYPE_UTF8MB4_BIN,    // column_collation_type
        19,                     // column_length
        2,                      // column_precision
        -1,                     // column_scale
        false,                  // is_nullable
        false);                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("WAIT_TIME_MICRO",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("TIME_REMAINING_MICRO",  // column_name
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
    ADD_COLUMN_SCHEMA("TIME_SINCE_LAST_WAIT_MICRO",  // column_name
        ++column_id,                                 // column_id
        0,                                           // rowkey_id
        0,                                           // index_id
        0,                                           // part_key_pos
        ObNumberType,                                // column_type
        CS_TYPE_INVALID,                             // column_collation_type
        38,                                          // column_length
        38,                                          // column_precision
        0,                                           // column_scale
        false,                                       // is_nullable
        false);                                      // is_autoincrement
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

int ObInnerTableSchema::all_virtual_session_wait_ora_all_virtual_session_wait_i1_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(
      combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SESSION_WAIT_ORA_ALL_VIRTUAL_SESSION_WAIT_I1_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SESSION_WAIT_ALL_VIRTUAL_SESSION_WAIT_I1_TNAME))) {
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SESSION_ID",  // column_name
        column_id + 1,               // column_id
        1,                           // rowkey_id
        1,                           // index_id
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
    ADD_COLUMN_SCHEMA("SVR_IP",  // column_name
        column_id + 2,           // column_id
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
        column_id + 3,             // column_id
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
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SESSION_WAIT_ORA_TID));

  table_schema.set_max_used_column_id(column_id + 3);
  return ret;
}

int ObInnerTableSchema::all_virtual_session_wait_history_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ORA_TNAME))) {
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
    ADD_COLUMN_SCHEMA("SESSION_ID",  // column_name
        ++column_id,                 // column_id
        1,                           // rowkey_id
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
    ADD_COLUMN_SCHEMA("SVR_IP",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
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
        3,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("SEQ#",  // column_name
        ++column_id,           // column_id
        4,                     // rowkey_id
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
    ADD_COLUMN_SCHEMA("EVENT#",  // column_name
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
    ADD_COLUMN_SCHEMA("EVENT",          // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_WAIT_EVENT_NAME_LENGTH,  // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P1TEXT",          // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_WAIT_EVENT_PARAM_LENGTH,  // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P1",  // column_name
        ++column_id,         // column_id
        0,                   // rowkey_id
        0,                   // index_id
        0,                   // part_key_pos
        ObNumberType,        // column_type
        CS_TYPE_INVALID,     // column_collation_type
        38,                  // column_length
        38,                  // column_precision
        0,                   // column_scale
        false,               // is_nullable
        false);              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P2TEXT",          // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_WAIT_EVENT_PARAM_LENGTH,  // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P2",  // column_name
        ++column_id,         // column_id
        0,                   // rowkey_id
        0,                   // index_id
        0,                   // part_key_pos
        ObNumberType,        // column_type
        CS_TYPE_INVALID,     // column_collation_type
        38,                  // column_length
        38,                  // column_precision
        0,                   // column_scale
        false,               // is_nullable
        false);              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P3TEXT",          // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_WAIT_EVENT_PARAM_LENGTH,  // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("P3",  // column_name
        ++column_id,         // column_id
        0,                   // rowkey_id
        0,                   // index_id
        0,                   // part_key_pos
        ObNumberType,        // column_type
        CS_TYPE_INVALID,     // column_collation_type
        38,                  // column_length
        38,                  // column_precision
        0,                   // column_scale
        false,               // is_nullable
        false);              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("LEVEL",  // column_name
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
    ADD_COLUMN_SCHEMA("WAIT_TIME_MICRO",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("TIME_SINCE_LAST_WAIT_MICRO",  // column_name
        ++column_id,                                 // column_id
        0,                                           // rowkey_id
        0,                                           // index_id
        0,                                           // part_key_pos
        ObNumberType,                                // column_type
        CS_TYPE_INVALID,                             // column_collation_type
        38,                                          // column_length
        38,                                          // column_precision
        0,                                           // column_scale
        false,                                       // is_nullable
        false);                                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("WAIT_TIME",  // column_name
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

int ObInnerTableSchema::all_virtual_session_wait_history_ora_all_virtual_session_wait_history_i1_schema(
    ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(
      combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ORA_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(
            OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_TNAME))) {
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SESSION_ID",  // column_name
        column_id + 1,               // column_id
        1,                           // rowkey_id
        1,                           // index_id
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
    ADD_COLUMN_SCHEMA("SVR_IP",  // column_name
        column_id + 2,           // column_id
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
        column_id + 3,             // column_id
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
    ADD_COLUMN_SCHEMA("SEQ#",  // column_name
        column_id + 4,         // column_id
        4,                     // rowkey_id
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
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_ORA_TID));

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_virtual_memory_info_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_MEMORY_INFO_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(5);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_MEMORY_INFO_ORA_TNAME))) {
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
        3,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("CTX_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("LABEL",  // column_name
        ++column_id,            // column_id
        5,                      // rowkey_id
        0,                      // index_id
        0,                      // part_key_pos
        ObVarcharType,          // column_type
        CS_TYPE_UTF8MB4_BIN,    // column_collation_type
        OB_MAX_CHAR_LENGTH,     // column_length
        2,                      // column_precision
        -1,                     // column_scale
        false,                  // is_nullable
        false);                 // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CTX_NAME",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        OB_MAX_CHAR_LENGTH,        // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MOD_TYPE",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        OB_MAX_CHAR_LENGTH,        // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("MOD_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("MOD_NAME",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        OB_MAX_CHAR_LENGTH,        // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("ZONE",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
        0,                     // index_id
        0,                     // part_key_pos
        ObVarcharType,         // column_type
        CS_TYPE_UTF8MB4_BIN,   // column_collation_type
        OB_MAX_CHAR_LENGTH,    // column_length
        2,                     // column_precision
        -1,                    // column_scale
        false,                 // is_nullable
        false);                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("HOLD",  // column_name
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
    ADD_COLUMN_SCHEMA("USED",  // column_name
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
    ADD_COLUMN_SCHEMA("COUNT",  // column_name
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
    ADD_COLUMN_SCHEMA("ALLOC_COUNT",  // column_name
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
    ADD_COLUMN_SCHEMA("FREE_COUNT",  // column_name
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

int ObInnerTableSchema::all_virtual_tenant_memstore_info_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_ORA_TNAME))) {
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
    ADD_COLUMN_SCHEMA("ACTIVE_MEMSTORE_USED",  // column_name
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
    ADD_COLUMN_SCHEMA("TOTAL_MEMSTORE_USED",  // column_name
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
    ADD_COLUMN_SCHEMA("MAJOR_FREEZE_TRIGGER",  // column_name
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
    ADD_COLUMN_SCHEMA("MEMSTORE_LIMIT",  // column_name
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
    ADD_COLUMN_SCHEMA("FREEZE_CNT",  // column_name
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

int ObInnerTableSchema::all_virtual_memstore_info_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_MEMSTORE_INFO_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_MEMSTORE_INFO_ORA_TNAME))) {
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
    ADD_COLUMN_SCHEMA("PARTITION_IDX",  // column_name
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
    ADD_COLUMN_SCHEMA("VERSION",  // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_UTF8MB4_BIN,      // column_collation_type
        MAX_VERSION_LENGTH,       // column_length
        2,                        // column_precision
        -1,                       // column_scale
        false,                    // is_nullable
        false);                   // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("BASE_VERSION",  // column_name
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
    ADD_COLUMN_SCHEMA("MULTI_VERSION_START",  // column_name
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
    ADD_COLUMN_SCHEMA("SNAPSHOT_VERSION",  // column_name
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
    ADD_COLUMN_SCHEMA("IS_ACTIVE",  // column_name
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
    ADD_COLUMN_SCHEMA("MEM_USED",  // column_name
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
    ADD_COLUMN_SCHEMA("HASH_ITEM_COUNT",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("HASH_MEM_USED",  // column_name
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
    ADD_COLUMN_SCHEMA("BTREE_ITEM_COUNT",  // column_name
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
    ADD_COLUMN_SCHEMA("BTREE_MEM_USED",  // column_name
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
    ADD_COLUMN_SCHEMA("INSERT_ROW_COUNT",  // column_name
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
    ADD_COLUMN_SCHEMA("UPDATE_ROW_COUNT",  // column_name
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
    ADD_COLUMN_SCHEMA("DELETE_ROW_COUNT",  // column_name
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
    ADD_COLUMN_SCHEMA("PURGE_ROW_COUNT",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("PURGE_QUEUE_COUNT",  // column_name
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

int ObInnerTableSchema::all_virtual_server_memory_info_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SERVER_MEMORY_INFO_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(2);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SERVER_MEMORY_INFO_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("SVR_IP",  // column_name
        ++column_id,             // column_id
        1,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("SERVER_MEMORY_HOLD",  // column_name
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
    ADD_COLUMN_SCHEMA("SERVER_MEMORY_LIMIT",  // column_name
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
    ADD_COLUMN_SCHEMA("SYSTEM_RESERVED",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("ACTIVE_MEMSTORE_USED",  // column_name
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
    ADD_COLUMN_SCHEMA("TOTAL_MEMSTORE_USED",  // column_name
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
    ADD_COLUMN_SCHEMA("MAJOR_FREEZE_TRIGGER",  // column_name
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
    ADD_COLUMN_SCHEMA("MEMSTORE_LIMIT",  // column_name
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

int ObInnerTableSchema::all_virtual_sesstat_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SESSTAT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SESSTAT_ORA_TNAME))) {
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
    ADD_COLUMN_SCHEMA("SESSION_ID",  // column_name
        ++column_id,                 // column_id
        1,                           // rowkey_id
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
    ADD_COLUMN_SCHEMA("SVR_IP",  // column_name
        ++column_id,             // column_id
        2,                       // rowkey_id
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
        3,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("STATISTIC#",  // column_name
        ++column_id,                 // column_id
        4,                           // rowkey_id
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
    ADD_COLUMN_SCHEMA("VALUE",  // column_name
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
    ADD_COLUMN_SCHEMA("CAN_VISIBLE",  // column_name
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

int ObInnerTableSchema::all_virtual_sesstat_ora_all_virtual_sesstat_i1_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SESSTAT_ORA_ALL_VIRTUAL_SESSTAT_I1_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SESSTAT_ALL_VIRTUAL_SESSTAT_I1_TNAME))) {
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SESSION_ID",  // column_name
        column_id + 1,               // column_id
        1,                           // rowkey_id
        1,                           // index_id
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
    ADD_COLUMN_SCHEMA("SVR_IP",  // column_name
        column_id + 2,           // column_id
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
        column_id + 3,             // column_id
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
    ADD_COLUMN_SCHEMA("STATISTIC#",  // column_name
        column_id + 4,               // column_id
        4,                           // rowkey_id
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
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SESSTAT_ORA_TID));

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_virtual_sysstat_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SYSSTAT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SYSSTAT_ORA_TNAME))) {
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
        3,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("STATISTIC#",  // column_name
        ++column_id,                 // column_id
        4,                           // rowkey_id
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
    ADD_COLUMN_SCHEMA("VALUE",  // column_name
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
    ADD_COLUMN_SCHEMA("STAT_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("NAME",  // column_name
        ++column_id,           // column_id
        0,                     // rowkey_id
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
    ADD_COLUMN_SCHEMA("CLASS",  // column_name
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
    ADD_COLUMN_SCHEMA("CAN_VISIBLE",  // column_name
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

int ObInnerTableSchema::all_virtual_sysstat_ora_all_virtual_sysstat_i1_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SYSSTAT_ORA_ALL_VIRTUAL_SYSSTAT_I1_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SYSSTAT_ALL_VIRTUAL_SYSSTAT_I1_TNAME))) {
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        column_id + 1,              // column_id
        1,                          // rowkey_id
        1,                          // index_id
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
        column_id + 2,           // column_id
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
        column_id + 3,             // column_id
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
    ADD_COLUMN_SCHEMA("STATISTIC#",  // column_name
        column_id + 4,               // column_id
        4,                           // rowkey_id
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
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SYSSTAT_ORA_TID));

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_virtual_system_event_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_TNAME))) {
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
        3,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("EVENT_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("EVENT",          // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_WAIT_EVENT_NAME_LENGTH,  // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("WAIT_CLASS_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("WAIT_CLASS#",  // column_name
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
    ADD_COLUMN_SCHEMA("WAIT_CLASS",      // column_name
        ++column_id,                     // column_id
        0,                               // rowkey_id
        0,                               // index_id
        0,                               // part_key_pos
        ObVarcharType,                   // column_type
        CS_TYPE_UTF8MB4_BIN,             // column_collation_type
        OB_MAX_WAIT_EVENT_PARAM_LENGTH,  // column_length
        2,                               // column_precision
        -1,                              // column_scale
        false,                           // is_nullable
        false);                          // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TOTAL_WAITS",  // column_name
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
    ADD_COLUMN_SCHEMA("TOTAL_TIMEOUTS",  // column_name
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
    ADD_COLUMN_SCHEMA("TIME_WAITED",  // column_name
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
    ADD_COLUMN_SCHEMA("MAX_WAIT",  // column_name
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
    ADD_COLUMN_SCHEMA("AVERAGE_WAIT",  // column_name
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
    ADD_COLUMN_SCHEMA("TIME_WAITED_MICRO",  // column_name
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

int ObInnerTableSchema::all_virtual_system_event_ora_all_virtual_system_event_i1_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(OB_INVALID_ID);
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(
      combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_ALL_VIRTUAL_SYSTEM_EVENT_I1_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(4);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_SYSTEM_EVENT_ALL_VIRTUAL_SYSTEM_EVENT_I1_TNAME))) {
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

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("TENANT_ID",  // column_name
        column_id + 1,              // column_id
        1,                          // rowkey_id
        1,                          // index_id
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
        column_id + 2,           // column_id
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
        column_id + 3,             // column_id
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
    ADD_COLUMN_SCHEMA("EVENT_ID",  // column_name
        column_id + 4,             // column_id
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
  table_schema.set_index_status(INDEX_STATUS_AVAILABLE);
  table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  table_schema.set_data_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SYSTEM_EVENT_ORA_TID));

  table_schema.set_max_used_column_id(column_id + 4);
  return ret;
}

int ObInnerTableSchema::all_virtual_tenant_memstore_allocator_info_agent_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_AGENT_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_ALL_VIRTUAL_TENANT_MEMSTORE_ALLOCATOR_INFO_AGENT_TNAME))) {
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
    ADD_COLUMN_SCHEMA("SVR_IP",  // column_name
        ++column_id,             // column_id
        0,                       // rowkey_id
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
    ADD_COLUMN_SCHEMA("PARTITION_ID",  // column_name
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
    ADD_COLUMN_SCHEMA("MT_BASE_VERSION",  // column_name
        ++column_id,                      // column_id
        0,                                // rowkey_id
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
    ADD_COLUMN_SCHEMA("RETIRE_CLOCK",  // column_name
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
    ADD_COLUMN_SCHEMA("MT_IS_FROZEN",  // column_name
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
    ADD_COLUMN_SCHEMA("MT_PROTECTION_CLOCK",  // column_name
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
    ADD_COLUMN_SCHEMA("MT_SNAPSHOT_VERSION",  // column_name
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

int ObInnerTableSchema::tenant_virtual_session_variable_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_TENANT_VIRTUAL_SESSION_VARIABLE_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_SESSION_VARIABLE_ORA_TNAME))) {
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
    ADD_COLUMN_SCHEMA("VARIABLE_NAME",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_CONFIG_NAME_LEN,         // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("VALUE",    // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_UTF8MB4_BIN,      // column_collation_type
        OB_MAX_CONFIG_VALUE_LEN,  // column_length
        2,                        // column_precision
        -1,                       // column_scale
        true,                     // is_nullable
        false);                   // is_autoincrement
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

int ObInnerTableSchema::tenant_virtual_global_variable_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(0);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_ORA_TNAME))) {
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
    ADD_COLUMN_SCHEMA("VARIABLE_NAME",  // column_name
        ++column_id,                    // column_id
        0,                              // rowkey_id
        0,                              // index_id
        0,                              // part_key_pos
        ObVarcharType,                  // column_type
        CS_TYPE_UTF8MB4_BIN,            // column_collation_type
        OB_MAX_CONFIG_NAME_LEN,         // column_length
        2,                              // column_precision
        -1,                             // column_scale
        false,                          // is_nullable
        false);                         // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("VALUE",    // column_name
        ++column_id,              // column_id
        0,                        // rowkey_id
        0,                        // index_id
        0,                        // part_key_pos
        ObVarcharType,            // column_type
        CS_TYPE_UTF8MB4_BIN,      // column_collation_type
        OB_MAX_CONFIG_VALUE_LEN,  // column_length
        2,                        // column_precision
        -1,                       // column_scale
        true,                     // is_nullable
        false);                   // is_autoincrement
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

int ObInnerTableSchema::tenant_virtual_show_create_table_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_ORA_TNAME))) {
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
    ADD_COLUMN_SCHEMA("TABLE_ID",  // column_name
        ++column_id,               // column_id
        1,                         // rowkey_id
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
    ADD_COLUMN_SCHEMA("TABLE_NAME",  // column_name
        ++column_id,                 // column_id
        0,                           // rowkey_id
        0,                           // index_id
        0,                           // part_key_pos
        ObVarcharType,               // column_type
        CS_TYPE_UTF8MB4_BIN,         // column_collation_type
        OB_MAX_TABLE_NAME_LENGTH,    // column_length
        2,                           // column_precision
        -1,                          // column_scale
        false,                       // is_nullable
        false);                      // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CREATE_TABLE",  // column_name
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
    ADD_COLUMN_SCHEMA("CHARACTER_SET_CLIENT",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObVarcharType,                         // column_type
        CS_TYPE_UTF8MB4_BIN,                   // column_collation_type
        MAX_CHARSET_LENGTH,                    // column_length
        2,                                     // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COLLATION_CONNECTION",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObVarcharType,                         // column_type
        CS_TYPE_UTF8MB4_BIN,                   // column_collation_type
        MAX_CHARSET_LENGTH,                    // column_length
        2,                                     // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
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

int ObInnerTableSchema::tenant_virtual_show_create_procedure_ora_schema(ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_APP_MIN_COLUMN_ID - 1;

  // generated fields:
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
  table_schema.set_database_id(combine_id(OB_SYS_TENANT_ID, OB_ORA_SYS_DATABASE_ID));
  table_schema.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_ORA_TID));
  table_schema.set_rowkey_split_pos(0);
  table_schema.set_is_use_bloomfilter(false);
  table_schema.set_progressive_merge_num(0);
  table_schema.set_rowkey_column_num(1);
  table_schema.set_load_type(TABLE_LOAD_TYPE_IN_DISK);
  table_schema.set_table_type(VIRTUAL_TABLE);
  table_schema.set_index_type(INDEX_TYPE_IS_NOT);
  table_schema.set_def_type(TABLE_DEF_TYPE_INTERNAL);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.set_table_name(OB_TENANT_VIRTUAL_SHOW_CREATE_PROCEDURE_ORA_TNAME))) {
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
    ADD_COLUMN_SCHEMA("ROUTINE_ID",  // column_name
        ++column_id,                 // column_id
        1,                           // rowkey_id
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
    ADD_COLUMN_SCHEMA("ROUTINE_NAME",  // column_name
        ++column_id,                   // column_id
        0,                             // rowkey_id
        0,                             // index_id
        0,                             // part_key_pos
        ObVarcharType,                 // column_type
        CS_TYPE_UTF8MB4_BIN,           // column_collation_type
        OB_MAX_ROUTINE_NAME_LENGTH,    // column_length
        2,                             // column_precision
        -1,                            // column_scale
        false,                         // is_nullable
        false);                        // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("CREATE_ROUTINE",  // column_name
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
    ADD_COLUMN_SCHEMA("PROC_TYPE",  // column_name
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
    ADD_COLUMN_SCHEMA("CHARACTER_SET_CLIENT",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObVarcharType,                         // column_type
        CS_TYPE_UTF8MB4_BIN,                   // column_collation_type
        MAX_CHARSET_LENGTH,                    // column_length
        2,                                     // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COLLATION_CONNECTION",  // column_name
        ++column_id,                           // column_id
        0,                                     // rowkey_id
        0,                                     // index_id
        0,                                     // part_key_pos
        ObVarcharType,                         // column_type
        CS_TYPE_UTF8MB4_BIN,                   // column_collation_type
        MAX_CHARSET_LENGTH,                    // column_length
        2,                                     // column_precision
        -1,                                    // column_scale
        false,                                 // is_nullable
        false);                                // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("COLLATION_DATABASE",  // column_name
        ++column_id,                         // column_id
        0,                                   // rowkey_id
        0,                                   // index_id
        0,                                   // part_key_pos
        ObVarcharType,                       // column_type
        CS_TYPE_UTF8MB4_BIN,                 // column_collation_type
        MAX_CHARSET_LENGTH,                  // column_length
        2,                                   // column_precision
        -1,                                  // column_scale
        false,                               // is_nullable
        false);                              // is_autoincrement
  }

  if (OB_SUCC(ret)) {
    ADD_COLUMN_SCHEMA("SQL_MODE",  // column_name
        ++column_id,               // column_id
        0,                         // rowkey_id
        0,                         // index_id
        0,                         // part_key_pos
        ObVarcharType,             // column_type
        CS_TYPE_UTF8MB4_BIN,       // column_collation_type
        MAX_CHARSET_LENGTH,        // column_length
        2,                         // column_precision
        -1,                        // column_scale
        false,                     // is_nullable
        false);                    // is_autoincrement
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
