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

#ifndef _OB_SCHEMA_MACRO_DEFINE_H_
#define _OB_SCHEMA_MACRO_DEFINE_H_

#include "common/rowkey/ob_rowkey_info.h"
#include "common/object/ob_object.h"
#include "lib/charset/ob_charset.h"
#include "share/schema/ob_table_schema.h"

#define ADD_COLUMN_SCHEMA(col_name, \
                          col_id, \
                          rowkey_position, \
                          index_position, \
                          partition_position, \
                          data_type, \
                          collation_type, \
                          data_len, \
                          data_precision, \
                          data_scale, \
                          nullable,\
                          is_autoincrement)          \
  { \
    ret = ADD_COLUMN_SCHEMA_FULL(table_schema, \
                           col_name, \
                           col_id, \
                           rowkey_position, \
                           index_position, \
                           partition_position, \
                           data_type, \
                           collation_type, \
                           data_len, \
                           data_precision, \
                           data_scale, \
                           oceanbase::common::ObOrderType::ASC, \
                           nullable,\
                           is_autoincrement); \
  }

#define ADD_COLUMN_SCHEMA_WITH_COLUMN_FLAGS(col_name, \
                          col_id, \
                          rowkey_position, \
                          index_position, \
                          partition_position, \
                          data_type, \
                          collation_type, \
                          data_len, \
                          data_precision, \
                          data_scale, \
                          nullable,\
                          is_autoincrement,\
                          is_hidden,\
                          is_storing_column)          \
  { \
    ret = ADD_COLUMN_SCHEMA_FULL(table_schema, \
                           col_name, \
                           col_id, \
                           rowkey_position, \
                           index_position, \
                           partition_position, \
                           data_type, \
                           collation_type, \
                           data_len, \
                           data_precision, \
                           data_scale, \
                           oceanbase::common::ObOrderType::ASC, \
                           nullable,\
                           is_autoincrement,\
                           is_hidden,\
                           is_storing_column); \
  }
  
#define ADD_COLUMN_SCHEMA_T(col_name, \
                            col_id, \
                            rowkey_position, \
                            index_position, \
                            partition_position, \
                            data_type, \
                            collation_type, \
                            data_len, \
                            data_precision, \
                            data_scale, \
                            nullable, \
                            is_autoincrement,   \
                            orig_default_value, \
                            cur_default_value) \
  { \
    ret = ADD_COLUMN_SCHEMA_WITH_DEFAULT_VALUE(table_schema, \
                                         col_name, \
                                         col_id, \
                                         rowkey_position, \
                                         index_position, \
                                         partition_position, \
                                         data_type, \
                                         collation_type, \
                                         data_len, \
                                         data_precision, \
                                         data_scale, \
                                         oceanbase::common::ObOrderType::ASC, \
                                         nullable, \
                                         is_autoincrement,   \
                                         orig_default_value, \
                                         cur_default_value); \
  }

#define ADD_COLUMN_SCHEMA_T_WITH_COLUMN_FLAGS(col_name, \
                                                   col_id, \
                                                   rowkey_position, \
                                                   index_position, \
                                                   partition_position, \
                                                   data_type, \
                                                   collation_type, \
                                                   data_len, \
                                                   data_precision, \
                                                   data_scale, \
                                                   nullable, \
                                                   is_autoincrement,   \
                                                   orig_default_value, \
                                                   cur_default_value, \
                                                   is_hidden, \
                                                   is_storing_column) \
  { \
    ret = ADD_COLUMN_SCHEMA_WITH_DEFAULT_VALUE(table_schema, \
                                         col_name, \
                                         col_id, \
                                         rowkey_position, \
                                         index_position, \
                                         partition_position, \
                                         data_type, \
                                         collation_type, \
                                         data_len, \
                                         data_precision, \
                                         data_scale, \
                                         oceanbase::common::ObOrderType::ASC, \
                                         nullable, \
                                         is_autoincrement,   \
                                         orig_default_value, \
                                         cur_default_value, \
                                         is_hidden, \
                                         is_storing_column); \
  }

#define ADD_COLUMN_SCHEMA_TS_T(col_name, \
                               col_id, \
                               rowkey_position, \
                               index_position, \
                               partition_position, \
                               data_type, \
                               collation_type, \
                               data_len, \
                               data_precision, \
                               data_scale, \
                               nullable, \
                               is_autoincrement,   \
                               is_on_update_for_timestamp, \
                               orig_default_value, \
                               cur_default_value) \
  { \
    ret = ADD_COLUMN_SCHEMA_TS_WITH_DEFAULT_VALUE(table_schema, \
                                            col_name, \
                                            col_id, \
                                            rowkey_position, \
                                            index_position, \
                                            partition_position, \
                                            data_type, \
                                            collation_type, \
                                            data_len, \
                                            data_precision, \
                                            data_scale, \
                                            oceanbase::common::ObOrderType::ASC, \
                                            nullable, \
                                            is_autoincrement,   \
                                            is_on_update_for_timestamp, \
                                            orig_default_value, \
                                            cur_default_value); \
  }
#define ADD_COLUMN_SCHEMA_TS_T_WITH_COLUMN_FLAGS(col_name, \
                                                      col_id, \
                                                      rowkey_position, \
                                                      index_position, \
                                                      partition_position, \
                                                      data_type, \
                                                      collation_type, \
                                                      data_len, \
                                                      data_precision, \
                                                      data_scale, \
                                                      nullable, \
                                                      is_autoincrement,   \
                                                      is_on_update_for_timestamp, \
                                                      orig_default_value, \
                                                      cur_default_value, \
                                                      is_hidden, \
                                                      is_storing_column) \
  { \
    ret = ADD_COLUMN_SCHEMA_TS_WITH_DEFAULT_VALUE(table_schema, \
                                            col_name, \
                                            col_id, \
                                            rowkey_position, \
                                            index_position, \
                                            partition_position, \
                                            data_type, \
                                            collation_type, \
                                            data_len, \
                                            data_precision, \
                                            data_scale, \
                                            oceanbase::common::ObOrderType::ASC, \
                                            nullable, \
                                            is_autoincrement,   \
                                            is_on_update_for_timestamp, \
                                            orig_default_value, \
                                            cur_default_value, \
                                            is_hidden, \
                                            is_storing_column); \
  }

#define ADD_COLUMN_SCHEMA_TS(col_name, \
                             col_id, \
                             rowkey_position, \
                             index_position, \
                             partition_position, \
                             data_type, \
                             collation_type, \
                             data_len, \
                             data_precision, \
                             data_scale, \
                             nullable,\
                             is_autoincrement, \
                             is_on_update_for_timestamp); \
  { \
    ret = ADD_COLUMN_SCHEMA_TS_FULL(table_schema, \
                              col_name, \
                              col_id, \
                              rowkey_position, \
                              index_position, \
                              partition_position, \
                              data_type, \
                              collation_type, \
                              data_len, \
                              data_precision, \
                              data_scale, \
                              oceanbase::common::ObOrderType::ASC, \
                              nullable,\
                              is_autoincrement, \
                              is_on_update_for_timestamp); \
  }
#define ADD_COLUMN_SCHEMA_TS_WITH_COLUMN_FLAGS(col_name, \
                                                    col_id, \
                                                    rowkey_position, \
                                                    index_position, \
                                                    partition_position, \
                                                    data_type, \
                                                    collation_type, \
                                                    data_len, \
                                                    data_precision, \
                                                    data_scale, \
                                                    nullable,\
                                                    is_autoincrement, \
                                                    is_on_update_for_timestamp, \
                                                    is_hidden, \
                                                    is_storing_column); \
  { \
    ret = ADD_COLUMN_SCHEMA_TS_FULL(table_schema, \
                              col_name, \
                              col_id, \
                              rowkey_position, \
                              index_position, \
                              partition_position, \
                              data_type, \
                              collation_type, \
                              data_len, \
                              data_precision, \
                              data_scale, \
                              oceanbase::common::ObOrderType::ASC, \
                              nullable,\
                              is_autoincrement, \
                              is_on_update_for_timestamp, \
                              is_hidden, \
                              is_storing_column); \
  } 

namespace oceanbase
{

namespace common
{

int ADD_COLUMN_SCHEMA_FULL(share::schema::ObTableSchema &table_schema,
                           const char *col_name,
                           const uint64_t col_id,
                           const int64_t rowkey_position,
                           const int64_t index_position,
                           const int64_t partition_position,
                           const common::ColumnType data_type,
                           const int collation_type,
                           const int64_t data_len,
                           const int16_t data_precision,
                           const int16_t data_scale,
                           const common::ObOrderType order_in_rowkey,
                           const bool nullable,
                           const bool is_autoincrement,
                           const bool is_hidden = false,
                           const bool is_storing_column = false);

int ADD_COLUMN_SCHEMA_WITH_DEFAULT_VALUE(share::schema::ObTableSchema &table_schema,
                                         const char *col_name,
                                         const uint64_t col_id,
                                         const int64_t rowkey_position,
                                         const int64_t index_position,
                                         const int64_t partition_position,
                                         const common::ColumnType data_type,
                                         const int collation_type,
                                         const int64_t data_len,
                                         const int16_t data_precision,
                                         const int16_t data_scale,
                                         const common::ObOrderType order,
                                         const bool nullable,
                                         const bool is_autoincrement,
                                         ObObj &orig_default_value,
                                         ObObj &cur_default_value,
                                         const bool is_hidden = false,
                                         const bool is_storing_column = false);

int ADD_COLUMN_SCHEMA_TS_WITH_DEFAULT_VALUE(share::schema::ObTableSchema &table_schema,
                                            const char *col_name,
                                            const uint64_t col_id,
                                            const int64_t rowkey_position,
                                            const int64_t index_position,
                                            const int64_t partition_position,
                                            const common::ColumnType data_type,
                                            const int collation_type,
                                            const int64_t data_len,
                                            const int16_t data_precision,
                                            const int16_t data_scale,
                                            const common::ObOrderType order,
                                            const bool nullable,
                                            const bool is_autoincrement,
                                            const bool is_on_update_for_timestamp,
                                            ObObj &orig_default_value,
                                            ObObj &cur_default_value,
                                            const bool is_hidden = false,
                                            const bool is_storing_column = false);

int ADD_COLUMN_SCHEMA_TS_FULL(share::schema::ObTableSchema &table_schema,
                              const char *col_name,
                              const uint64_t col_id,
                              const int64_t rowkey_position,
                              const int64_t index_position,
                              const int64_t partition_position,
                              const common::ColumnType data_type,
                              const int collation_type,
                              const int64_t data_len,
                              const int16_t data_precision,
                              const int16_t data_scale,
                              const common::ObOrderType order,
                              const bool nullable,
                              const bool is_autoincrement,
                              const bool is_on_update_for_timestamp,
                              const bool is_hidden = false,
                              const bool is_storing_column = false);

} // common
} // namespace oceanbase

#endif //_OB_SCHEMA_MACRO_DEFINE_H_
