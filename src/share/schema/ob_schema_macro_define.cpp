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

#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_macro_define.h"

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
                           const common::ObOrderType order,
                           const bool nullable,
                           const bool is_autoincrement,
                           const bool is_hidden,
                           const bool is_storing_column)
{
  int ret = OB_SUCCESS;
  share::schema::ObColumnSchemaV2 column;
  int64_t col_name_len = strlen(col_name);
  if(col_name_len <= OB_MAX_COLUMN_NAME_LENGTH) {
    ret = column.set_column_name(col_name);
  } else {
    ret = OB_SIZE_OVERFLOW;
    SHARE_SCHEMA_LOG(WARN, "col name is too long, ", K(col_name_len));
  }

  if (OB_SUCC(ret)) {
    if (data_len < INT32_MIN || data_len > INT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_SCHEMA_LOG(ERROR, "invalid data_len", K(ret), K(data_len));
    }
  }
 
  if(OB_SUCC(ret)) {
    ObObj orig_default_value;
    column.set_tenant_id(table_schema.get_tenant_id());
    column.set_table_id(table_schema.get_table_id());
    column.set_column_id(col_id);
    column.set_rowkey_position(rowkey_position);
    column.set_index_position(index_position);
    column.set_part_key_pos(partition_position);
    column.set_data_type(data_type);
    const ObAccuracy &default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY[data_type];
    if (ob_is_string_tc(data_type)) {
      column.set_data_length(static_cast<int32_t>(data_len));
      column.set_data_precision(data_precision);
    } else if (ob_is_text_tc(data_type) || ob_is_json_tc(data_type) || ob_is_geometry_tc(data_type)) {
      column.set_data_length(default_accuracy.get_length());
    } else if (ob_is_datetime_tc(data_type) || ob_is_time_tc(data_type)) {
      ObScale scale = -1 == data_scale ? default_accuracy.get_scale() :
      static_cast<ObScale>(data_scale);
      column.set_data_precision(static_cast<ObPrecision>(default_accuracy.get_precision()
                                                         + scale));
      column.set_data_scale(scale);
    } else {
      column.set_data_precision(default_accuracy.get_precision());
      column.set_data_scale(default_accuracy.get_scale());
      if (data_scale != -1) {
        column.set_data_scale(data_scale);
      }
      if (data_precision != -1) {
        column.set_data_precision(data_precision);
      }
    }
    column.set_nullable(nullable);
    column.set_autoincrement(is_autoincrement);
    column.set_is_hidden(is_hidden);
    if (CS_TYPE_INVALID == collation_type) {
      if (ob_is_string_type(data_type)) {
        column.set_charset_type(ObCharset::get_default_charset());
        column.set_collation_type(ObCharset::get_default_collation(
                                                                   ObCharset::get_default_charset()));
      } else if (ob_is_json(data_type)) {
        column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
      } else {
        column.set_collation_type(CS_TYPE_BINARY);
        column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
      }
    } else if (CS_TYPE_UTF8MB4_GENERAL_CI == collation_type)  {
      column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
    } else if (CS_TYPE_UTF8MB4_BIN == collation_type)  {
      column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
    } else if (CS_TYPE_BINARY == collation_type)  {
      column.set_collation_type(CS_TYPE_BINARY);
      column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
    } else {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "invalid collation type ", K(collation_type), K(ret));
    }
    if (is_storing_column) {
      column.add_column_flag(USER_SPECIFIED_STORING_COLUMN_FLAG);
    }
    column.set_order_in_rowkey(order);
    orig_default_value.set_null();
    ret = column.set_orig_default_value(orig_default_value);
    if (OB_FAIL(ret)) {
      SHARE_SCHEMA_LOG(WARN, "set orig default value failed", K(ret));
    }
  }
 
  if(OB_SUCC(ret)) {
    ret = table_schema.add_column(column);
    if (OB_FAIL(ret)) {
      SHARE_SCHEMA_LOG(WARN, "add column failed, ", K(ret)); 
    }
  }
  return ret;
}

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
                                         const bool is_hidden,
                                         const bool is_storing_column)
{
  int ret = OB_SUCCESS;
  share::schema::ObColumnSchemaV2 column;
  int64_t col_name_len = strlen(col_name);
  if(col_name_len <= OB_MAX_COLUMN_NAME_LENGTH) {
    ret = column.set_column_name(col_name);
  } else {
    ret = OB_SIZE_OVERFLOW;
    SHARE_SCHEMA_LOG(WARN, "col name is too long", K(col_name_len));
  }
 
  if(OB_SUCC(ret)) {
    column.set_tenant_id(table_schema.get_tenant_id());
    column.set_table_id(table_schema.get_table_id());
    column.set_column_id(col_id);
    column.set_rowkey_position(rowkey_position);
    column.set_index_position(index_position);
    column.set_tbl_part_key_pos(partition_position);
    column.set_data_type(data_type);
    const ObAccuracy &default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY[data_type];
    if (ob_is_string_tc(data_type)) {
      column.set_data_length(data_len);
    } else if (ob_is_text_tc(data_type) || ob_is_json_tc(data_type) || ob_is_geometry_tc(data_type)) {
      column.set_data_length(default_accuracy.get_length());
      if (DEFAULT_PRECISION_FOR_STRING != data_precision) {
        column.set_data_precision(data_precision);
      }
    } else if (ob_is_datetime_tc(data_type) || ob_is_time_tc(data_type)) {
      ObScale scale = -1 == data_scale ? default_accuracy.get_scale() :
      static_cast<ObScale>(data_scale);
      column.set_data_precision(static_cast<ObPrecision>(default_accuracy.get_precision()
                                                         + scale));
      column.set_data_scale(scale);
    } else {
      column.set_data_precision(default_accuracy.get_precision());
      column.set_data_scale(default_accuracy.get_scale());
      if (data_scale != -1) {
        column.set_data_scale(data_scale);
      }
      if (data_precision != -1) {
        column.set_data_precision(data_precision);
      }
    }
    column.set_nullable(nullable);
    column.set_autoincrement(is_autoincrement);
    column.set_is_hidden(is_hidden);
    if (CS_TYPE_INVALID == collation_type) {
      if (ob_is_string_type(data_type)) {
        column.set_charset_type(ObCharset::get_default_charset());
        column.set_collation_type(ObCharset::get_default_collation(
                                                                   ObCharset::get_default_charset()));
      } else if (ob_is_json(data_type)) {
        column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
      } else {
        column.set_collation_type(CS_TYPE_BINARY);
        column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
      }
    } else if (CS_TYPE_UTF8MB4_GENERAL_CI == collation_type) {
      column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
    } else if (CS_TYPE_UTF8MB4_BIN == collation_type) {
      column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
    } else if (CS_TYPE_BINARY == collation_type) {
      column.set_collation_type(CS_TYPE_BINARY);
      column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
    } else {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "invalid collation type ", K(collation_type), K(ret));
    }
    if (is_storing_column) {
      column.add_column_flag(USER_SPECIFIED_STORING_COLUMN_FLAG);
    }
    column.set_order_in_rowkey(order);
    orig_default_value.set_collation_type(column.get_collation_type());
    cur_default_value.set_collation_type(column.get_collation_type());
    if (OB_FAIL(column.set_orig_default_value(orig_default_value))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to set original default value, ", K(ret));
    } else if (OB_FAIL(column.set_cur_default_value(cur_default_value))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to set cur default value, ", K(ret));
    }
  }
 
  if(OB_SUCC(ret)) {
    ret = table_schema.add_column(column);
    if (OB_FAIL(ret)) {
      SHARE_SCHEMA_LOG(WARN, "add column failed, ", K(ret));
    }
  }
  return ret;
}

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
                                            const bool is_hidden,
                                            const bool is_storing_column)
{
  int ret = OB_SUCCESS;
  share::schema::ObColumnSchemaV2 column;
  int64_t col_name_len = strlen(col_name);
  if(col_name_len <= OB_MAX_COLUMN_NAME_LENGTH) {
    ret = column.set_column_name(col_name);
  } else {
    ret = OB_SIZE_OVERFLOW;
    SHARE_SCHEMA_LOG(WARN, "col name is too long", K(col_name_len));
  }
 
  if(OB_SUCC(ret)) {
    column.set_tenant_id(table_schema.get_tenant_id());
    column.set_table_id(table_schema.get_table_id());
    column.set_column_id(col_id);
    column.set_rowkey_position(rowkey_position);
    column.set_index_position(index_position);
    column.set_tbl_part_key_pos(partition_position);
    column.set_data_type(data_type);
    column.set_on_update_current_timestamp(is_on_update_for_timestamp);
    const ObAccuracy &default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY[data_type];
    if (ob_is_string_tc(data_type)) {
      column.set_data_length(data_len);
    } else if (ob_is_text_tc(data_type) || ob_is_json_tc(data_type) || ob_is_geometry_tc(data_type)) {
      column.set_data_length(default_accuracy.get_length());
    } else if (ob_is_datetime_tc(data_type) || ob_is_time_tc(data_type)) {
      ObScale scale = -1 == data_scale ? default_accuracy.get_scale() :
      static_cast<ObScale>(data_scale);
      column.set_data_precision(static_cast<ObPrecision>(default_accuracy.get_precision()
                                                         + scale));
      column.set_data_scale(scale);
    } else {
      column.set_data_precision(default_accuracy.get_precision());
      column.set_data_scale(default_accuracy.get_scale());
      if (data_scale != -1) {
        column.set_data_scale(data_scale);
      }
      if (data_precision != -1) {
        column.set_data_precision(data_precision);
      }
    }
    column.set_nullable(nullable);
    column.set_autoincrement(is_autoincrement);
    column.set_is_hidden(is_hidden);
    if (CS_TYPE_INVALID == collation_type) {
      if (ob_is_string_type(data_type)) {
        column.set_charset_type(ObCharset::get_default_charset());
        column.set_collation_type(ObCharset::get_default_collation(
                                                                   ObCharset::get_default_charset()));
      } else if (ob_is_json(data_type)) {
        column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
      } else {
        column.set_collation_type(CS_TYPE_BINARY);
        column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
      }
    } else if (CS_TYPE_UTF8MB4_GENERAL_CI == collation_type)  {
      column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
    } else if (CS_TYPE_UTF8MB4_BIN == collation_type)  {
      column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
    } else if (CS_TYPE_BINARY == collation_type)  {
      column.set_collation_type(CS_TYPE_BINARY);
      column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
    } else {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "invalid collation type ", K(collation_type), K(ret));
    }
    if (is_storing_column) {
      column.add_column_flag(USER_SPECIFIED_STORING_COLUMN_FLAG);
    }
    column.set_order_in_rowkey(order);
    orig_default_value.set_collation_type(column.get_collation_type());
    cur_default_value.set_collation_type(column.get_collation_type());
    if (OB_FAIL(column.set_orig_default_value(orig_default_value))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to set original default value, ", K(ret));
    } else if (OB_FAIL(column.set_cur_default_value(cur_default_value))) {
      SHARE_SCHEMA_LOG(WARN, "Fail to set cur default value, ", K(ret));
    }
  }
 
  if(OB_SUCC(ret)) {
    ret = table_schema.add_column(column);
    if (OB_FAIL(ret)) {
      SHARE_SCHEMA_LOG(WARN, "add column failed, ", K(ret));
    }
  }                                                                  
  return ret;
}

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
                              const bool is_hidden,
                              const bool is_storing_column)
{
  int ret = OB_SUCCESS;
  share::schema::ObColumnSchemaV2 column;
  int64_t col_name_len = strlen(col_name);
  if(col_name_len <= OB_MAX_COLUMN_NAME_LENGTH) {
    ret = column.set_column_name(col_name);
  } else {
    ret = OB_SIZE_OVERFLOW;
    SHARE_SCHEMA_LOG(WARN, "col name is too long, ", K(col_name_len));
  }
 
  if(OB_SUCC(ret)) {
    ObObj orig_default_value;
    column.set_tenant_id(table_schema.get_tenant_id());
    column.set_table_id(table_schema.get_table_id());
    column.set_column_id(col_id);
    column.set_rowkey_position(rowkey_position);
    column.set_index_position(index_position);
    column.set_part_key_pos(partition_position);
    column.set_data_type(data_type);
    column.set_on_update_current_timestamp(is_on_update_for_timestamp);
    const ObAccuracy &default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY[data_type];
    if (ob_is_string_tc(data_type)) {
      column.set_data_length(data_len);
    } else if (ob_is_text_tc(data_type) || ob_is_json_tc(data_type) || ob_is_geometry_tc(data_type)) {
      column.set_data_length(default_accuracy.get_length());
    } else if (ob_is_datetime_tc(data_type) || ob_is_time_tc(data_type)) {
      ObScale scale = -1 == data_scale ? default_accuracy.get_scale() :
      static_cast<ObScale>(data_scale);
      column.set_data_precision(static_cast<ObPrecision>(default_accuracy.get_precision()
                                                         + scale));
      column.set_data_scale(scale);
    } else {
      column.set_data_precision(default_accuracy.get_precision());
      column.set_data_scale(default_accuracy.get_scale());
      if (data_scale != -1) {
        column.set_data_scale(data_scale);
      }
      if (data_precision != -1) {
        column.set_data_precision(data_precision);
      }
    }
    column.set_nullable(nullable);
    column.set_autoincrement(is_autoincrement);
    column.set_is_hidden(is_hidden);
    if (CS_TYPE_INVALID == collation_type) {
      if (ob_is_string_type(data_type)) {
        column.set_charset_type(ObCharset::get_default_charset());
        column.set_collation_type(ObCharset::get_default_collation(
                                                                   ObCharset::get_default_charset()));
      } else if (ob_is_json(data_type)) {
        column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
      } else {
        column.set_collation_type(CS_TYPE_BINARY);
        column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
      }
    } else if (CS_TYPE_UTF8MB4_GENERAL_CI == collation_type) {
      column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
    } else if (CS_TYPE_UTF8MB4_BIN == collation_type) {
      column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
    } else if (CS_TYPE_BINARY == collation_type) {
      column.set_collation_type(CS_TYPE_BINARY);
      column.set_charset_type(ObCharset::charset_type_by_coll(column.get_collation_type()));
    } else {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "invalid collation type ", K(collation_type), K(ret));
    }
    if (is_storing_column) {
      column.add_column_flag(USER_SPECIFIED_STORING_COLUMN_FLAG);
    }
    column.set_order_in_rowkey(order);
    orig_default_value.set_null();
    ret = column.set_orig_default_value(orig_default_value);
    if (OB_FAIL(ret)) {
      SHARE_SCHEMA_LOG(WARN, "set orig default value failed", K(ret));
    }
  }
 
  if(OB_SUCC(ret)) {
    ret = table_schema.add_column(column);
    if (OB_FAIL(ret)) {
      SHARE_SCHEMA_LOG(WARN, "add column failed, ", K(ret)); 
    }
  }
  return ret;
}

} // common
} // oceanbase
