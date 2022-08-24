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

#ifndef OCEANBASE_OB_MYSQL_RESULT_H_
#define OCEANBASE_OB_MYSQL_RESULT_H_

#include "lib/string/ob_string.h"
#include "lib/number/ob_number_v2.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/ob_define.h"
#include "lib/rowid/ob_urowid.h"
#include "common/ob_smart_var.h"

#define COLUMN_MAP_BUCKET_NUM 107

#define EXTRACT_BOOL_FIELD_MYSQL_SKIP_RET(result, column_name, field)        \
  if (OB_SUCC(ret)) {                                                        \
    bool bool_value = 0;                                                     \
    if (OB_SUCCESS == (ret = (result).get_bool(column_name, bool_value))) {  \
      field = bool_value;                                                    \
    } else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) { \
      ret = OB_SUCCESS;                                                      \
      field = false;                                                         \
    } else {                                                                 \
      SQL_LOG(WARN, "fail to get column in row. ", K(column_name), K(ret));  \
    }                                                                        \
  }

#define EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, column_name, obj)             \
  if (OB_SUCC(ret)) {                                                                    \
    bool bool_value = 0;                                                                 \
    if (OB_SUCCESS == (ret = (result).get_bool(#column_name, bool_value))) {             \
      (obj).set_##column_name(bool_value);                                               \
    } else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {             \
      ret = OB_SUCCESS;                                                                  \
      (obj).set_##column_name(false);                                                    \
    } else {                                                                             \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }                                                                                    \
  }

#define EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL(result, column_name, obj)                      \
  if (OB_SUCC(ret)) {                                                                    \
    bool bool_value = 0;                                                                 \
    if (OB_SUCCESS == (ret = (result).get_bool(#column_name, bool_value))) {             \
      (obj).set_##column_name(bool_value);                                               \
    } else {                                                                             \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }                                                                                    \
  }

#define EXTRACT_BOOL_FIELD_MYSQL(result, column_name, field)                \
  if (OB_SUCC(ret)) {                                                       \
    bool bool_value = 0;                                                    \
    if (OB_SUCCESS != (ret = (result).get_bool(column_name, bool_value))) { \
      SQL_LOG(WARN, "fail to get column in row. ", K(column_name), K(ret)); \
    } else {                                                                \
      field = bool_value;                                                   \
    }                                                                       \
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in
// ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(                            \
    result, column_name, obj, skip_null_error, skip_column_error, default_value)         \
  if (OB_SUCC(ret)) {                                                                    \
    bool bool_value = 0;                                                                 \
    if (OB_SUCCESS == (ret = (result).get_bool(#column_name, bool_value))) {             \
      (obj).set_##column_name(bool_value);                                               \
    } else if (OB_ERR_NULL_VALUE == ret) {                                               \
      if (skip_null_error) {                                                             \
        SQL_LOG(INFO, "null value, ignore", "column_name", #column_name);                \
        (obj).set_##column_name(default_value);                                          \
        ret = OB_SUCCESS;                                                                \
      } else {                                                                           \
        SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret));                \
      }                                                                                  \
    } else if (OB_ERR_COLUMN_NOT_FOUND == ret) {                                         \
      if (skip_column_error) {                                                           \
        SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name);          \
        (obj).set_##column_name(default_value);                                          \
        ret = OB_SUCCESS;                                                                \
      } else {                                                                           \
        SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret));          \
      }                                                                                  \
    } else {                                                                             \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }                                                                                    \
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in
// ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(                                      \
    result, column_name, field, type, skip_null_error, skip_column_error, default_value) \
  if (OB_SUCC(ret)) {                                                                    \
    int64_t int_value = 0;                                                               \
    if (OB_SUCCESS == (ret = (result).get_int(column_name, int_value))) {                \
      field = static_cast<type>(int_value);                                              \
    } else if (OB_ERR_NULL_VALUE == ret) {                                               \
      if (skip_null_error) {                                                             \
        SQL_LOG(INFO, "null value, ignore", K(column_name));                             \
        field = static_cast<type>(default_value);                                        \
        ret = OB_SUCCESS;                                                                \
      } else {                                                                           \
        SQL_LOG(WARN, "null value", K(column_name), K(ret));                             \
      }                                                                                  \
    } else if (OB_ERR_COLUMN_NOT_FOUND == ret) {                                         \
      if (skip_column_error) {                                                           \
        SQL_LOG(INFO, "column not found, ignore", K(column_name));                       \
        field = static_cast<type>(default_value);                                        \
        ret = OB_SUCCESS;                                                                \
      } else {                                                                           \
        SQL_LOG(WARN, "column not found", K(column_name), K(ret));                       \
      }                                                                                  \
    } else {                                                                             \
      SQL_LOG(WARN, "fail to get column in row. ", K(column_name), K(ret));              \
    }                                                                                    \
  }

#define EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, column_name, field, type)   \
  if (OB_SUCC(ret)) {                                                        \
    int64_t int_value = 0;                                                   \
    if (OB_SUCCESS == (ret = (result).get_int(column_name, int_value))) {    \
      field = static_cast<type>(int_value);                                  \
    } else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) { \
      ret = OB_SUCCESS;                                                      \
      field = static_cast<type>(0);                                          \
    } else {                                                                 \
      SQL_LOG(WARN, "fail to get column in row. ", K(column_name), K(ret));  \
    }                                                                        \
  }

#define EXTRACT_INT_FIELD_MYSQL(result, column_name, field, type)                       \
  if (OB_SUCC(ret)) {                                                                   \
    int64_t int_value = 0;                                                              \
    if (OB_SUCCESS != (ret = (result).get_int(column_name, int_value))) {               \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", column_name, K(ret)); \
    } else {                                                                            \
      field = static_cast<type>(int_value);                                             \
    }                                                                                   \
  }

#define EXTRACT_UINT_FIELD_MYSQL(result, column_name, field, type)                      \
  if (OB_SUCC(ret)) {                                                                   \
    uint64_t int_value = 0;                                                             \
    if (OB_SUCCESS != (ret = (result).get_uint(column_name, int_value))) {              \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", column_name, K(ret)); \
    } else {                                                                            \
      field = static_cast<type>(int_value);                                             \
    }                                                                                   \
  }

#define EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, column_name, obj, type)                 \
  if (OB_SUCC(ret)) {                                                                    \
    int64_t int_value = 0;                                                               \
    if (OB_SUCCESS != (ret = (result).get_int(#column_name, int_value))) {               \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } else {                                                                             \
      (obj).set_##column_name(static_cast<type>(int_value));                             \
    }                                                                                    \
  }

#define EXTRACT_UINT_FIELD_TO_CLASS_MYSQL(result, column_name, obj, type)                \
  if (OB_SUCC(ret)) {                                                                    \
    uint64_t int_value = 0;                                                              \
    if (OB_SUCCESS != (ret = (result).get_uint(#column_name, int_value))) {              \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } else {                                                                             \
      (obj).set_##column_name(static_cast<type>(int_value));                             \
    }                                                                                    \
  }

#define EXTRACT_LAST_DDL_TIME_FIELD_TO_INT_MYSQL(result, column_name, obj, type)         \
  if (OB_SUCC(ret)) {                                                                    \
    int64_t int_value = 0;                                                               \
    if (OB_SUCCESS != (ret = (result).get_datetime(#column_name, int_value))) {          \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } else {                                                                             \
      (obj).set_##column_name(static_cast<type>(int_value));                             \
    }                                                                                    \
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in
// ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_UINT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(                            \
    result, column_name, obj, type, skip_null_error, skip_column_error, default_value)   \
  if (OB_SUCC(ret)) {                                                                    \
    uint64_t int_value = 0;                                                              \
    if (OB_SUCCESS == (ret = (result).get_uint(#column_name, int_value))) {              \
      (obj).set_##column_name(static_cast<type>(int_value));                             \
    } else if (OB_ERR_NULL_VALUE == ret) {                                               \
      if (skip_null_error) {                                                             \
        SQL_LOG(INFO, "null value, ignore", "column_name", #column_name);                \
        (obj).set_##column_name(static_cast<type>(default_value));                       \
        ret = OB_SUCCESS;                                                                \
      } else {                                                                           \
        SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret));                \
      }                                                                                  \
    } else if (OB_ERR_COLUMN_NOT_FOUND == ret) {                                         \
      if (skip_column_error) {                                                           \
        SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name);          \
        (obj).set_##column_name(static_cast<type>(default_value));                       \
        ret = OB_SUCCESS;                                                                \
      } else {                                                                           \
        SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret));          \
      }                                                                                  \
    } else {                                                                             \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }                                                                                    \
  }

#define EXTRACT_INT_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, column_name, obj, type)        \
  if (OB_SUCC(ret)) {                                                                    \
    int64_t int_value = 0;                                                               \
    if (OB_SUCCESS == (ret = (result).get_int(#column_name, int_value))) {               \
      (obj).set_##column_name(static_cast<type>(int_value));                             \
    } else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {             \
      ret = OB_SUCCESS;                                                                  \
      (obj).set_##column_name(static_cast<type>(0));                                     \
    } else {                                                                             \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }                                                                                    \
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in
// ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(                             \
    result, column_name, obj, type, skip_null_error, skip_column_error, default_value)   \
  if (OB_SUCC(ret)) {                                                                    \
    int64_t int_value = 0;                                                               \
    if (OB_SUCCESS == (ret = (result).get_int(#column_name, int_value))) {               \
      (obj).set_##column_name(static_cast<type>(int_value));                             \
    } else if (OB_ERR_NULL_VALUE == ret) {                                               \
      if (skip_null_error) {                                                             \
        SQL_LOG(INFO, "null value, ignore", "column_name", #column_name);                \
        (obj).set_##column_name(static_cast<type>(default_value));                       \
        ret = OB_SUCCESS;                                                                \
      } else {                                                                           \
        SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret));                \
      }                                                                                  \
    } else if (OB_ERR_COLUMN_NOT_FOUND == ret) {                                         \
      if (skip_column_error) {                                                           \
        SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name);          \
        (obj).set_##column_name(static_cast<type>(default_value));                       \
        ret = OB_SUCCESS;                                                                \
      } else {                                                                           \
        SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret));          \
      }                                                                                  \
    } else {                                                                             \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }                                                                                    \
  }

#define EXTRACT_DOUBLE_FIELD_MYSQL_SKIP_RET(result, column_name, field, type)   \
  if (OB_SUCC(ret)) {                                                           \
    double double_value = 0;                                                    \
    if (OB_SUCCESS == (ret = (result).get_double(column_name, double_value))) { \
      field = static_cast<type>(double_value);                                  \
    } else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {    \
      ret = OB_SUCCESS;                                                         \
      field = static_cast<type>(0);                                             \
    } else {                                                                    \
      SQL_LOG(WARN, "fail to get column in row. ", K(column_name), K(ret));     \
    }                                                                           \
  }

#define EXTRACT_DOUBLE_FIELD_MYSQL(result, column_name, field, type)                    \
  if (OB_SUCC(ret)) {                                                                   \
    double double_value = 0;                                                            \
    if (OB_SUCCESS != (ret = (result).get_double(column_name, double_value))) {         \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", column_name, K(ret)); \
    } else {                                                                            \
      field = static_cast<type>(double_value);                                          \
    }                                                                                   \
  }

#define EXTRACT_DOUBLE_FIELD_TO_CLASS_MYSQL(result, column_name, obj, type)              \
  if (OB_SUCC(ret)) {                                                                    \
    double double_value = 0;                                                             \
    if (OB_SUCCESS != (ret = (result).get_double(#column_name, double_value))) {         \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } else {                                                                             \
      (obj).set_##column_name(static_cast<type>(double_value));                          \
    }                                                                                    \
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in
// ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_DOUBLE_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(                          \
    result, column_name, obj, type, skip_null_error, skip_column_error, default_value)   \
  if (OB_SUCC(ret)) {                                                                    \
    double double_value = 0;                                                             \
    if (OB_SUCCESS == (ret = (result).get_double(#column_name, double_value))) {         \
      (obj).set_##column_name(static_cast<type>(double_value));                          \
    } else if (OB_ERR_NULL_VALUE == ret) {                                               \
      if (skip_null_error) {                                                             \
        SQL_LOG(INFO, "null value, ignore", "column_name", #column_name);                \
        (obj).set_##column_name(static_cast<type>(default_value));                       \
        ret = OB_SUCCESS;                                                                \
      } else {                                                                           \
        SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret));                \
      }                                                                                  \
    } else if (OB_ERR_COLUMN_NOT_FOUND == ret) {                                         \
      if (skip_column_error) {                                                           \
        SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name);          \
        (obj).set_##column_name(static_cast<type>(default_value));                       \
        ret = OB_SUCCESS;                                                                \
      } else {                                                                           \
        SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret));          \
      }                                                                                  \
    } else {                                                                             \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }                                                                                    \
  }

// TODO
// Now the ObNumber type interface is not perfect, the upper layer is ObSequenceSchema, the value of ObNumber type does
// not exceed int64_t, so the stack memory is used first ObNumber type is fully supported (by Yan Hua), which requires a
// lot of scene combing and allocator configuration, which will be replaced later
#define EXTRACT_NUMBER_FIELD_TO_CLASS_MYSQL(result, column_name, obj)                         \
  if (OB_SUCC(ret)) {                                                                         \
    common::number::ObNumber number_value;                                                    \
    char buffer[common::number::ObNumber::MAX_NUMBER_ALLOC_BUFF_SIZE];                        \
    ObDataBuffer data_buffer(buffer, sizeof(buffer));                                         \
    if (OB_SUCCESS != (ret = (result).get_number(#column_name, number_value, data_buffer))) { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret));      \
    } else if (OB_SUCCESS != (ret = (obj).set_##column_name(number_value))) {                 \
      SQL_LOG(WARN, "fail to set column to object. ", "column_name", #column_name, K(ret));   \
    }                                                                                         \
  }

#define EXTRACT_NUMBER_FIELD_MYSQL(result, column_name, number_value)                    \
  if (OB_SUCC(ret)) {                                                                    \
    if (OB_SUCCESS != (ret = (result).get_number(#column_name, number_value))) {         \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }                                                                                    \
  }

// TODO:
// The urowid type is not used in the internal performance stage, just define a macro, and then use the
// re-implementation code later
#define EXTRACT_UROWID_FIELD_TO_CLASS_MYSQL(result, column_name, obj) \
  if (OB_SUCC(ret)) {}

#define EXTRACT_UROWID_FIELD_MYSQL(result, column_name, urowid_data)                     \
  if (OB_SUCC(ret)) {                                                                    \
    if (OB_SUCCESS != (ret = (result).get_urowid(#column_name, urowid_data))) {          \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }                                                                                    \
  }

#define EXTRACT_LOB_FIELD_TO_CLASS_MYSQL(result, column_name, obj)                       \
  if (OB_SUCC(ret)) {                                                                    \
    ObLobLocator* lob_locator = NULL;                                                    \
    if (OB_SUCCESS != (ret = (result).get_lob_locator(#column_name, lob_locator))) {     \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } else if (OB_ISNULL(lob_locator)) {                                                 \
      obj.set_lob_locator(lob_locator);                                                  \
    }

#define EXTRACT_LOB_FIELD_MYSQL(result, column_name, lob_locator)                        \
  if (OB_SUCC(ret)) {                                                                    \
    if (OB_SUCCESS != (ret = (result).get_lob_locator(column_name, lob_locator))) {      \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }                                                                                    \
  }

#define EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, column_name, obj)                      \
  if (OB_SUCC(ret)) {                                                                       \
    common::ObString varchar_value;                                                         \
    if (OB_SUCCESS != (ret = (result).get_varchar(#column_name, varchar_value))) {          \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret));    \
    } else if (OB_SUCCESS != (ret = (obj).set_##column_name(varchar_value))) {              \
      SQL_LOG(WARN, "fail to set column to object. ", "column_name", #column_name, K(ret)); \
    }                                                                                       \
  }

#define EXTRACT_VARCHAR_FIELD_MYSQL(result, column_name, field)                  \
  if (OB_SUCC(ret)) {                                                            \
    if (OB_SUCCESS != (ret = (result).get_varchar(column_name, field))) {        \
      if (share::is_oracle_mode() && (OB_ERR_NULL_VALUE == ret)) {               \
        SQL_LOG(WARN, "varchar is null on oracle mode", K(ret), K(column_name)); \
        ret = OB_SUCCESS;                                                        \
      } else {                                                                   \
        SQL_LOG(WARN, "fail to get varchar. ", K(ret), K(column_name));          \
      }                                                                          \
    }                                                                            \
  }

#define EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, column_name, field)  \
  if (OB_SUCC(ret)) {                                                     \
    if (OB_SUCCESS != (ret = (result).get_varchar(column_name, field))) { \
      if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {   \
        ret = OB_SUCCESS;                                                 \
      } else {                                                            \
        SQL_LOG(WARN, "get varchar failed", K(ret));                      \
      }                                                                   \
    }                                                                     \
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in
// ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(                                   \
    result, column_name, field, skip_null_error, skip_column_error, default_value)        \
  if (OB_SUCC(ret)) {                                                                     \
    if (OB_SUCCESS != (ret = (result).get_varchar(column_name, field))) {                 \
      if (OB_ERR_NULL_VALUE == ret) {                                                     \
        if (skip_null_error) {                                                            \
          SQL_LOG(INFO, "null value, ignore", "column_name", #column_name);               \
          ret = OB_SUCCESS;                                                               \
        } else {                                                                          \
          SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret));               \
        }                                                                                 \
      } else if (OB_ERR_COLUMN_NOT_FOUND == ret) {                                        \
        if (skip_column_error) {                                                          \
          SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name, K(ret)); \
          field = default_value;                                                          \
          ret = OB_SUCCESS;                                                               \
        } else {                                                                          \
          SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret));         \
        }                                                                                 \
      } else {                                                                            \
        SQL_LOG(WARN, "get varchar failed", K(ret));                                      \
      }                                                                                   \
    }                                                                                     \
  }

#define EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, column_name, class_obj) \
  if (OB_SUCC(ret)) {                                                                 \
    ObString str_value;                                                               \
    if (OB_SUCCESS == (ret = (result).get_varchar(#column_name, str_value))) {        \
      if (OB_FAIL((class_obj).set_##column_name(str_value))) {                        \
        SQL_LOG(WARN, "fail to set value", KR(ret), K(str_value));                    \
      }                                                                               \
    } else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {          \
      ret = OB_SUCCESS;                                                               \
    } else {                                                                          \
      SQL_LOG(WARN, "fail to extract varchar field mysql. ", K(ret));                 \
    }                                                                                 \
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in
// ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(                       \
    result, column_name, class_obj, skip_null_error, skip_column_error, default_value) \
  if (OB_SUCC(ret)) {                                                                  \
    ObString str_value;                                                                \
    if (OB_SUCCESS == (ret = (result).get_varchar(#column_name, str_value))) {         \
      if (OB_FAIL((class_obj).set_##column_name(str_value))) {                         \
        SQL_LOG(WARN, "fail to set value", KR(ret), K(str_value));                     \
      }                                                                                \
    } else if (OB_ERR_NULL_VALUE == ret) {                                             \
      if (skip_null_error) {                                                           \
        SQL_LOG(INFO, "null value, ignore", "column_name", #column_name);              \
        ret = OB_SUCCESS;                                                              \
      } else {                                                                         \
        SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret));              \
      }                                                                                \
    } else if (OB_ERR_COLUMN_NOT_FOUND == ret) {                                       \
      if (skip_column_error) {                                                         \
        SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name);        \
        /*overwrite ret*/                                                              \
        if (OB_FAIL((class_obj).set_##column_name(default_value))) {                   \
          SQL_LOG(WARN, "fail to set value", KR(ret), K(default_value));               \
        }                                                                              \
      } else {                                                                         \
        SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret));        \
      }                                                                                \
    } else {                                                                           \
      SQL_LOG(WARN, "fail to extract varchar field mysql. ", K(ret));                  \
    }                                                                                  \
  }

#define EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(result, column_name, field, max_length, real_length)                      \
  if (OB_SUCC(ret)) {                                                                                                 \
    ObString str_value;                                                                                               \
    if (OB_SUCCESS == (ret = (result).get_varchar(column_name, str_value))) {                                         \
      if (str_value.length() >= max_length) {                                                                         \
        ret = OB_SIZE_OVERFLOW;                                                                                       \
        SQL_LOG(WARN, "field max length is not enough:", "max length", max_length, "str length", str_value.length()); \
      } else {                                                                                                        \
        MEMCPY(field, str_value.ptr(), str_value.length());                                                           \
        real_length = str_value.length();                                                                             \
        field[str_value.length()] = '\0';                                                                             \
      }                                                                                                               \
    } else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {                                          \
      ret = OB_SUCCESS;                                                                                               \
      real_length = 0;                                                                                                \
      field[0] = '\0';                                                                                                \
    } else {                                                                                                          \
      SQL_LOG(WARN, "fail to extract strbuf field mysql. ", K(ret));                                                  \
    }                                                                                                                 \
  }

#define EXTRACT_LONGTEXT_FIELD_MYSQL_WITH_ALLOCATOR_SKIP_RET(result, column_name, field, allocator, real_length) \
  if (OB_SUCC(ret)) {                                                                                            \
    ObString str_value;                                                                                          \
    if (OB_SUCCESS == (ret = (result).get_varchar(column_name, str_value))) {                                    \
      if (OB_ISNULL(field = static_cast<char*>((allocator).alloc(str_value.length() + 1)))) {                    \
        ret = OB_ALLOCATE_MEMORY_FAILED;                                                                         \
        SQL_LOG(WARN, "allocate memory failed", K(ret));                                                         \
      } else {                                                                                                   \
        MEMCPY(field, str_value.ptr(), str_value.length());                                                      \
        real_length = str_value.length();                                                                        \
        field[str_value.length()] = '\0';                                                                        \
      }                                                                                                          \
    } else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {                                     \
      ret = OB_SUCCESS;                                                                                          \
      real_length = 0;                                                                                           \
    } else {                                                                                                     \
      SQL_LOG(WARN, "fail to extract strbuf field mysql. ", K(ret));                                             \
    }                                                                                                            \
  }

#define EXTRACT_STRBUF_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, column_name, class_obj, max_length)                      \
  if (OB_SUCC(ret)) {                                                                                                 \
    ObString str_value;                                                                                               \
    if (OB_SUCCESS == (ret = (result).get_varchar(#column_name, str_value))) {                                        \
      if (str_value.length() >= max_length) {                                                                         \
        ret = OB_SIZE_OVERFLOW;                                                                                       \
        SQL_LOG(WARN, "field max length is not enough:", "max length", max_length, "str length", str_value.length()); \
      } else {                                                                                                        \
        ret = (class_obj).set_##column_name(str_value);                                                               \
      }                                                                                                               \
    } else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) {                                          \
      ret = OB_SUCCESS;                                                                                               \
    } else {                                                                                                          \
      SQL_LOG(WARN, "fail to extract strbuf field mysql. ", K(ret));                                                  \
    }                                                                                                                 \
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in
// ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_STRBUF_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(                                                       \
    result, column_name, class_obj, max_length, skip_null_error, skip_column_error, default_value)                    \
  if (OB_SUCC(ret)) {                                                                                                 \
    ObString str_value;                                                                                               \
    if (OB_SUCCESS == (ret = (result).get_varchar(#column_name, str_value))) {                                        \
      if (str_value.length() > max_length) {                                                                          \
        ret = OB_SIZE_OVERFLOW;                                                                                       \
        SQL_LOG(WARN, "field max length is not enough:", "max length", max_length, "str length", str_value.length()); \
      } else {                                                                                                        \
        ret = (class_obj).set_##column_name(str_value);                                                               \
      }                                                                                                               \
    } else if (OB_ERR_NULL_VALUE == ret) {                                                                            \
      if (skip_null_error) {                                                                                          \
        SQL_LOG(INFO, "null value, ignore", "column_name", #column_name);                                             \
        ret = OB_SUCCESS;                                                                                             \
      } else {                                                                                                        \
        SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret));                                             \
      }                                                                                                               \
    } else if (OB_ERR_COLUMN_NOT_FOUND == ret) {                                                                      \
      if (skip_column_error) {                                                                                        \
        SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name);                                       \
        (class_obj).set_##column_name(default_value);                                                                 \
        ret = OB_SUCCESS;                                                                                             \
      } else {                                                                                                        \
        SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret));                                       \
      }                                                                                                               \
    } else {                                                                                                          \
      SQL_LOG(WARN, "fail to extract strbuf field mysql. ", K(ret));                                                  \
    }                                                                                                                 \
  }

#define EXTRACT_STRBUF_FIELD_TO_CLASS_MYSQL(result, column_name, class_obj, max_length)                               \
  if (OB_SUCC(ret)) {                                                                                                 \
    ObString str_value;                                                                                               \
    if (OB_SUCCESS == (ret = (result).get_varchar(#column_name, str_value))) {                                        \
      if (str_value.length() > max_length) {                                                                          \
        ret = OB_SIZE_OVERFLOW;                                                                                       \
        SQL_LOG(WARN, "field max length is not enough:", "max length", max_length, "str length", str_value.length()); \
      } else {                                                                                                        \
        ret = (class_obj).set_##column_name(str_value);                                                               \
      }                                                                                                               \
    } else {                                                                                                          \
      SQL_LOG(DEBUG, "fail to extract strbuf field mysql. ", K(ret));                                                 \
    }                                                                                                                 \
  }

#define EXTRACT_STRBUF_FIELD_MYSQL(result, column_name, field, max_length, real_length)                               \
  if (OB_SUCC(ret)) {                                                                                                 \
    ObString str_value;                                                                                               \
    if (OB_SUCCESS == (ret = (result).get_varchar(column_name, str_value))) {                                         \
      if (str_value.length() >= max_length) {                                                                         \
        ret = OB_SIZE_OVERFLOW;                                                                                       \
        SQL_LOG(WARN, "field max length is not enough:", "max length", max_length, "str length", str_value.length()); \
      } else {                                                                                                        \
        MEMCPY(field, str_value.ptr(), str_value.length());                                                           \
        real_length = str_value.length();                                                                             \
        field[str_value.length()] = '\0';                                                                             \
      }                                                                                                               \
    } else {                                                                                                          \
      SQL_LOG(DEBUG, "fail to extract strbuf field mysql. ", K(ret));                                                 \
    }                                                                                                                 \
  }

/*
 * EXTRACT_NEW_DEFAULT_VALUE_FIELD_MYSQL is used to obtain the default value in __all_column,
 * which has been modified later. The default value may be obtained from cur_default_value or new_default_value.
 *  Compatibility processing is done here, and default_value_v2_version is used to distinguish between these two cases.
 */
#define EXTRACT_DEFAULT_VALUE_FIELD_MYSQL(                                                                \
    result, column_name, data_type, class_obj, is_cur_default_value, default_value_v2_version, tenant_id) \
  if (OB_SUCC(ret)) {                                                                                     \
    ObString str_value;                                                                                   \
    ObObj res_obj;                                                                                        \
    ret = (result).get_varchar(#column_name, str_value);                                                  \
                                                                                                          \
    if (OB_ERR_NULL_VALUE == ret) { /* without default value */                                           \
      if (!default_value_v2_version) {                                                                    \
        res_obj.set_null();                                                                               \
        ret = (class_obj).set_##column_name(res_obj);                                                     \
      } else {                                                                                            \
        ret = OB_SUCCESS;                                                                                 \
      }                                                                                                   \
    } else if (OB_ERR_COLUMN_NOT_FOUND == ret) {                                                          \
      SQL_LOG(WARN, "column not found, ignore", "column_name", #column_name);                             \
      ret = OB_SUCCESS;                                                                                   \
    } else if (OB_SUCC(ret)) {                                                                            \
      if (is_cur_default_value && column.is_default_expr_v2_column()) {                                   \
        res_obj.set_varchar(str_value);                                                                   \
        ret = (class_obj).set_##column_name(res_obj);                                                     \
      } else if (IS_DEFAULT_NOW_STR(data_type, str_value)) {                                              \
        res_obj.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);                                               \
        res_obj.set_scale(column.get_data_scale());                                                       \
        ret = (class_obj).set_##column_name(res_obj);                                                     \
      } else if (ob_is_string_type(data_type)) {                                                          \
        res_obj.set_string(data_type, str_value);                                                         \
        res_obj.meta_.set_collation_type(column.get_collation_type());                                    \
        /* will override the collaction level set in set_varchar */                                       \
        res_obj.meta_.set_collation_level(CS_LEVEL_IMPLICIT);                                             \
        if (ob_is_text_tc(data_type)) {                                                                   \
          res_obj.set_lob_inrow();                                                                        \
        }                                                                                                 \
        ret = (class_obj).set_##column_name(res_obj);                                                     \
      } else if (ob_is_bit_tc(data_type) || ob_is_enum_or_set_type(data_type)) {                          \
        ObObj def_obj;                                                                                    \
        def_obj.set_varchar(str_value);                                                                   \
        ObObj tmp_obj;                                                                                    \
        ObObj dest_obj;                                                                                   \
        ObArenaAllocator allocator(ObModIds::OB_SCHEMA);                                                  \
        ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, column.get_collation_type());                       \
        if (OB_FAIL(ObObjCaster::to_type(ObUInt64Type, cast_ctx, def_obj, tmp_obj))) {                    \
          SQL_LOG(WARN, "cast obj failed, ", "src type", def_obj.get_type());                             \
        } else {                                                                                          \
          if (ObBitType == data_type) {                                                                   \
            dest_obj.set_bit(tmp_obj.get_uint64());                                                       \
            dest_obj.set_scale(column.get_data_precision());                                              \
          } else if (ObEnumType == data_type) {                                                           \
            dest_obj.set_enum(tmp_obj.get_uint64());                                                      \
          } else { /*set type*/                                                                           \
            dest_obj.set_set(tmp_obj.get_uint64());                                                       \
          }                                                                                               \
          ret = (class_obj).set_##column_name(dest_obj);                                                  \
        }                                                                                                 \
      } else if (ob_is_json(data_type) && share::is_mysql_mode())                                         \
      { /* MySQL json does not support default value except null, */                                      \
        /* need this defensive to compatible a bug in old version */                                      \
        ObObj def_obj;                                                                                    \
        ObObj default_value;                                                                              \
        default_value.set_type(data_type);                                                                \
        ObArenaAllocator allocator(ObModIds::OB_SCHEMA);                                                  \
        ObObj dest_obj;                                                                                   \
        ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, column.get_collation_type());                       \
        if (OB_FAIL(default_value.build_not_strict_default_value())) {                                    \
          SQL_LOG(WARN, "failed to build not strict default json value", K(ret));                         \
        } else {                                                                                          \
          def_obj.set_json_value(data_type,                                                               \
                                 default_value.get_string().ptr(),                                        \
                                 default_value.get_string().length());                                    \
          if (OB_FAIL(ObObjCaster::to_type(data_type, cast_ctx, def_obj, dest_obj)))                      \
          {                                                                                               \
            SQL_LOG(WARN, "cast obj failed, ", "src type", def_obj.get_type(), "dest type", data_type);   \
          }                                                                                               \
          else                                                                                            \
          {                                                                                               \
            dest_obj.set_lob_inrow();                                                                     \
            dest_obj.meta_.set_collation_level(CS_LEVEL_IMPLICIT);                                        \
            ret = (class_obj).set_##column_name(dest_obj);                                                \
          }                                                                                               \
        }                                                                                                 \
      }                                                                                                   \
       else {                                                                                             \
        ObObj def_obj;                                                                                    \
        def_obj.set_varchar(str_value);                                                                   \
        ObArenaAllocator allocator(ObModIds::OB_SCHEMA);                                                  \
        ObObj dest_obj;                                                                                   \
        ObTimeZoneInfo tz_info;                                                                           \
        const ObDataTypeCastParams dtc_params(&tz_info);                                                  \
        ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NONE, column.get_collation_type());                \
        if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id, tz_info.get_tz_map_wrap()))) {                      \
          SQL_LOG(WARN, "get tenant timezone map failed", K(ret));                                        \
        } else if (OB_FAIL(ObObjCaster::to_type(data_type, cast_ctx, def_obj, dest_obj))) {               \
          SQL_LOG(WARN, "cast obj failed, ", "src type", def_obj.get_type(), "dest type", data_type);     \
        } else {                                                                                          \
          dest_obj.set_scale(column.get_data_scale());                                                    \
          ret = (class_obj).set_##column_name(dest_obj);                                                  \
        }                                                                                                 \
      }                                                                                                   \
    } else {                                                                                              \
      SQL_LOG(WARN, "fail to default value field mysql. ", K(ret));                                       \
    }                                                                                                     \
  }

#define EXTRACT_CREATE_TIME_FIELD_MYSQL(result, column_name, field, type) \
  EXTRACT_INT_FIELD_MYSQL(result, column_name, field, type)

#define EXTRACT_MODIFY_TIME_FIELD_MYSQL(result, column_name, field, type) \
  EXTRACT_INT_FIELD_MYSQL(result, column_name, field, type)

#define GET_COL_IGNORE_NULL(func, ...)                  \
  ({                                                    \
    {                                                   \
      if (OB_SUCC(ret)) {                               \
        if (OB_SUCCESS != (ret = func(__VA_ARGS__))) {  \
          if (OB_ERR_NULL_VALUE == ret) {               \
            ret = OB_SUCCESS;                           \
          } else {                                      \
            SQL_LOG(WARN, "get column failed", K(ret)); \
          }                                             \
        }                                               \
      }                                                 \
    }                                                   \
    ret;                                                \
  })

// for partition table use
// If the value of the column is NULL, or the column does not exist, take the default value default_value
// Inner_sql_result, core_proxy, ob_mysql_result_impl check out the error code that does not exist
// They are OB_SIZE_OVERFLOW, OB_ERR_NULL_VALUE and OB_INVALID_ARGUMENT
#define GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(func, idx, obj, default_value)                      \
  ({                                                                                               \
    {                                                                                              \
      if (OB_SUCC(ret)) {                                                                          \
        if (OB_SUCCESS != (ret = func(idx, obj))) {                                                \
          if (OB_ERR_NULL_VALUE == ret || OB_SIZE_OVERFLOW == ret || OB_INVALID_ARGUMENT == ret) { \
            SQL_LOG(WARN, "get column failed, so set default value", K(ret), "obj", #obj);         \
            ret = OB_SUCCESS;                                                                      \
            obj = default_value;                                                                   \
          } else {                                                                                 \
            SQL_LOG(WARN, "get column failed", K(ret));                                            \
          }                                                                                        \
        }                                                                                          \
      }                                                                                            \
    }                                                                                              \
    ret;                                                                                           \
  })

// for partition table use
// If the value of the column is NULL, or the column does not exist, take the default value default_value
// Inner_sql_result, core_proxy, ob_mysql_result_impl check out the error code that does not exist
// They are OB_SIZE_OVERFLOW, OB_ERR_NULL_VALUE and OB_INVALID_ARGUMENT
#define GET_TIMESTAMP_COL_BY_NAME_IGNORE_NULL_WITH_DEFAULT_VALUE(func, col_name, obj, default_value, tz_info) \
  ({                                                                                                          \
    {                                                                                                         \
      if (OB_SUCC(ret)) {                                                                                     \
        if (OB_SUCCESS != (ret = func(col_name, tz_info, obj))) {                                             \
          if (OB_ERR_NULL_VALUE == ret || OB_SIZE_OVERFLOW == ret || OB_INVALID_ARGUMENT == ret ||            \
              OB_ERR_COLUMN_NOT_FOUND == ret) {                                                               \
            ret = OB_SUCCESS;                                                                                 \
            obj = default_value;                                                                              \
          } else {                                                                                            \
            SQL_LOG(WARN, "get column failed", K(ret));                                                       \
          }                                                                                                   \
        }                                                                                                     \
      }                                                                                                       \
    }                                                                                                         \
    ret;                                                                                                      \
  })

// Used to construct the ID encoded with tenant_id
#define EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, column_name, obj, tenant_id) \
  if (OB_SUCC(ret)) {                                                                        \
    int64_t int_value = 0;                                                                   \
    if (OB_SUCCESS != (ret = (result).get_int(#column_name, int_value))) {                   \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret));     \
    } else {                                                                                 \
      if (OB_INVALID_ID == int_value || 0 == int_value || OB_SYS_TENANT_ID == tenant_id) {   \
        (obj).set_##column_name(int_value);                                                  \
      } else {                                                                               \
        (obj).set_##column_name(combine_id(tenant_id, int_value));                           \
      }                                                                                      \
    }                                                                                        \
  }

// Used to construct the ID encoded with tenant_id
#define EXTRACT_UINT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, column_name, obj, tenant_id) \
  if (OB_SUCC(ret)) {                                                                         \
    uint64_t uint_value = 0;                                                                  \
    if (OB_SUCCESS != (ret = (result).get_uint(#column_name, uint_value))) {                  \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret));      \
    } else {                                                                                  \
      if (OB_INVALID_ID == uint_value || 0 == uint_value || OB_SYS_TENANT_ID == tenant_id) {  \
        (obj).set_##column_name(uint_value);                                                  \
      } else {                                                                                \
        (obj).set_##column_name(combine_id(tenant_id, uint_value));                           \
      }                                                                                       \
    }                                                                                         \
  }

// Macro with default value, used to construct ID with tenant_id encoded
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in
// ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID_AND_DEFAULT_VALUE(                  \
    result, column_name, obj, tenant_id, skip_null_error, skip_column_error, default_value) \
  if (OB_SUCC(ret)) {                                                                       \
    int64_t int_value = 0;                                                                  \
    if (OB_SUCCESS == (ret = (result).get_int(#column_name, int_value))) {                  \
      if (OB_INVALID_ID == int_value || 0 == int_value || OB_SYS_TENANT_ID == tenant_id) {  \
        (obj).set_##column_name(int_value);                                                 \
      } else {                                                                              \
        (obj).set_##column_name(combine_id(tenant_id, int_value));                          \
      }                                                                                     \
    } else if (OB_ERR_NULL_VALUE == ret) {                                                  \
      if (skip_null_error) {                                                                \
        SQL_LOG(INFO, "null value, ignore", "column_name", #column_name);                   \
        (obj).set_##column_name(static_cast<uint64_t>(default_value));                      \
        ret = OB_SUCCESS;                                                                   \
      } else {                                                                              \
        SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret));                   \
      }                                                                                     \
    } else if (OB_ERR_COLUMN_NOT_FOUND == ret) {                                            \
      if (skip_column_error) {                                                              \
        SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name);             \
        (obj).set_##column_name(static_cast<uint64_t>(default_value));                      \
        ret = OB_SUCCESS;                                                                   \
      } else {                                                                              \
        SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret));             \
      }                                                                                     \
    } else {                                                                                \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret));    \
    }                                                                                       \
  }

#define EXTRACT_UINT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID_AND_DEFAULT_VALUE(                  \
    result, column_name, obj, tenant_id, skip_null_error, skip_column_error, default_value)  \
  if (OB_SUCC(ret)) {                                                                        \
    uint64_t uint_value = 0;                                                                 \
    if (OB_SUCCESS == (ret = (result).get_uint(#column_name, uint_value))) {                 \
      if (OB_INVALID_ID == uint_value || 0 == uint_value || OB_SYS_TENANT_ID == tenant_id) { \
        (obj).set_##column_name(uint_value);                                                 \
      } else {                                                                               \
        (obj).set_##column_name(combine_id(tenant_id, uint_value));                          \
      }                                                                                      \
    } else if (OB_ERR_NULL_VALUE == ret) {                                                   \
      if (skip_null_error) {                                                                 \
        SQL_LOG(INFO, "null value, ignore", "column_name", #column_name);                    \
        (obj).set_##column_name(static_cast<uint64_t>(default_value));                       \
        ret = OB_SUCCESS;                                                                    \
      } else {                                                                               \
        SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret));                    \
      }                                                                                      \
    } else if (OB_ERR_COLUMN_NOT_FOUND == ret) {                                             \
      if (skip_column_error) {                                                               \
        SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name);              \
        (obj).set_##column_name(static_cast<uint64_t>(default_value));                       \
        ret = OB_SUCCESS;                                                                    \
      } else {                                                                               \
        SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret));              \
      }                                                                                      \
    } else {                                                                                 \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret));     \
    }                                                                                        \
  }

// Macro with default value, used to construct ID with tenant_id encoded
#define EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, column_name, field, tenant_id)      \
  if (OB_SUCC(ret)) {                                                                      \
    int64_t int_value = 0;                                                                 \
    if (OB_SUCCESS != (ret = (result).get_int(column_name, int_value))) {                  \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", column_name, K(ret));    \
    } else {                                                                               \
      if (OB_INVALID_ID == int_value || 0 == int_value || OB_SYS_TENANT_ID == tenant_id) { \
        field = static_cast<uint64_t>(int_value);                                          \
      } else {                                                                             \
        field = combine_id(tenant_id, int_value);                                          \
      }                                                                                    \
    }                                                                                      \
  }
#define EXTRACT_UINT_FIELD_MYSQL_WITH_TENANT_ID(result, column_name, field, tenant_id)     \
  if (OB_SUCC(ret)) {                                                                      \
    uint64_t int_value = 0;                                                                \
    if (OB_SUCCESS != (ret = (result).get_uint(column_name, int_value))) {                 \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", column_name, K(ret));    \
    } else {                                                                               \
      if (OB_INVALID_ID == int_value || 0 == int_value || OB_SYS_TENANT_ID == tenant_id) { \
        field = static_cast<uint64_t>(int_value);                                          \
      } else {                                                                             \
        field = combine_id(tenant_id, int_value);                                          \
      }                                                                                    \
    }                                                                                      \
  }

namespace oceanbase {
namespace common {
class ObIAllocator;
class ObNewRow;
class ObLobLocator;
class ObTimeZoneInfo;
class ObOTimestampData;
class ObIntervalYMValue;
class ObIntervalDSValue;
namespace sqlclient {
class ObMySQLResult {
public:
  // see this for template virtual function
  DEFINE_ALLOCATOR_WRAPPER
  ObMySQLResult();
  virtual ~ObMySQLResult();
  // virtual int64_t get_row_count(void) const = 0;
  // virtual int64_t get_column_count(void) const = 0;
  /*
   * move result cursor to next row
   */
  virtual int close() = 0;
  virtual int next() = 0;

  // debug function
  virtual int print_info() const;
  /*
   * read int/str/TODO from result set
   * col_idx: indicate which column to read, [0, max_read_col)
   */
  virtual int get_int(const int64_t col_idx, int64_t& int_val) const = 0;
  virtual int get_uint(const int64_t col_idx, uint64_t& int_val) const = 0;
  virtual int get_datetime(const int64_t col_idx, int64_t& datetime) const = 0;
  virtual int get_date(const int64_t col_idx, int32_t& date) const = 0;
  virtual int get_time(const int64_t col_idx, int64_t& time) const = 0;
  virtual int get_year(const int64_t col_idx, uint8_t& year) const = 0;
  virtual int get_bool(const int64_t col_idx, bool& bool_val) const = 0;
  virtual int get_varchar(const int64_t col_idx, common::ObString& varchar_val) const = 0;
  virtual int get_raw(const int64_t col_idx, common::ObString& varchar_val) const = 0;
  virtual int get_float(const int64_t col_idx, float& float_val) const = 0;
  virtual int get_double(const int64_t col_idx, double& double_val) const = 0;
  virtual int get_timestamp(const int64_t col_idx, const common::ObTimeZoneInfo* tz_info, int64_t& int_val) const = 0;
  virtual int get_otimestamp_value(const int64_t col_idx, const common::ObTimeZoneInfo& tz_info,
      const common::ObObjType type, common::ObOTimestampData& otimestamp_val) const = 0;
  virtual int get_timestamp_tz(
      const int64_t col_idx, const common::ObTimeZoneInfo& tz_info, common::ObOTimestampData& otimestamp_val) const = 0;
  virtual int get_timestamp_ltz(
      const int64_t col_idx, const common::ObTimeZoneInfo& tz_info, common::ObOTimestampData& otimestamp_val) const = 0;
  virtual int get_timestamp_nano(
      const int64_t col_idx, const common::ObTimeZoneInfo& tz_info, common::ObOTimestampData& otimestamp_val) const = 0;
  virtual int get_number(const int64_t col_idx, common::number::ObNumber& nmb_val) const
  {
    UNUSED(col_idx);
    UNUSED(nmb_val);
    return common::OB_NOT_IMPLEMENT;
  }

  virtual int get_urowid(const int64_t col_idx, common::ObURowIDData& urowid_data) const
  {
    UNUSED(col_idx);
    UNUSED(urowid_data);
    return OB_NOT_IMPLEMENT;
  }
  virtual int get_lob_locator(const int64_t col_idx, ObLobLocator*& lob_locator) const
  {
    UNUSED(col_idx);
    UNUSED(lob_locator);
    return OB_NOT_IMPLEMENT;
  }
  virtual int get_type(const int64_t col_idx, ObObjMeta& type) const = 0;
  /// @note return OB_SUCCESS instead of OB_ERR_NULL_VALUE when obj is null
  virtual int get_obj(const int64_t col_idx, ObObj& obj, const common::ObTimeZoneInfo* tz_info = NULL,
      common::ObIAllocator* allocator = NULL) const = 0;

  template <class Allocator>
  int get_number(const int64_t col_idx, common::number::ObNumber& nmb_val, Allocator& allocator) const;

  template <class Allocator>
  int get_urowid(const int64_t col_idx, common::ObURowIDData& urowid_data, Allocator& allocator) const;
  template <class Allocator>
  int get_lob_locator(const int64_t col_idx, ObLobLocator*& lob_locator, Allocator& allocator) const;

  virtual int get_interval_ym(const int64_t col_idx, common::ObIntervalYMValue& int_val) const = 0;
  virtual int get_interval_ds(const int64_t col_idx, common::ObIntervalDSValue& int_val) const = 0;
  virtual int get_nvarchar2(const int64_t col_idx, common::ObString& nvarchar2_val) const = 0;
  virtual int get_nchar(const int64_t col_idx, common::ObString& nchar_val) const = 0;
  /*
   * read int/str/TODO from result set
   * col_name: indicate which column to read
   * @return  OB_INVALID_PARAM if col_name does not exsit
   */
  virtual int get_int(const char* col_name, int64_t& int_val) const = 0;
  virtual int get_uint(const char* col_name, uint64_t& int_val) const = 0;
  virtual int get_datetime(const char* col_name, int64_t& datetime) const = 0;
  virtual int get_date(const char* col_name, int32_t& date) const = 0;
  virtual int get_time(const char* col_name, int64_t& time) const = 0;
  virtual int get_year(const char* col_name, uint8_t& year) const = 0;
  virtual int get_bool(const char* col_name, bool& bool_val) const = 0;
  virtual int get_varchar(const char* col_name, common::ObString& varchar_val) const = 0;
  virtual int get_raw(const char* col_name, common::ObString& raw_val) const = 0;
  virtual int get_float(const char* col_name, float& float_val) const = 0;
  virtual int get_double(const char* col_name, double& double_val) const = 0;
  virtual int get_timestamp(const char* col_name, const common::ObTimeZoneInfo* tz_info, int64_t& int_val) const = 0;
  virtual int get_otimestamp_value(const char* col_name, const common::ObTimeZoneInfo& tz_info,
      const common::ObObjType type, common::ObOTimestampData& otimestamp_val) const = 0;
  virtual int get_timestamp_tz(
      const char* col_name, const common::ObTimeZoneInfo& tz_info, common::ObOTimestampData& otimestamp_val) const = 0;
  virtual int get_timestamp_ltz(
      const char* col_name, const common::ObTimeZoneInfo& tz_info, common::ObOTimestampData& otimestamp_val) const = 0;
  virtual int get_timestamp_nano(
      const char* col_name, const common::ObTimeZoneInfo& tz_info, common::ObOTimestampData& otimestamp_val) const = 0;
  virtual int get_number(const char* col_name, common::number::ObNumber& nmb_val) const
  {
    UNUSED(col_name);
    UNUSED(nmb_val);
    return common::OB_NOT_IMPLEMENT;
  }

  virtual int get_urowid(const char* col_name, common::ObURowIDData& urowid_data) const
  {
    UNUSED(col_name);
    UNUSED(urowid_data);
    return common::OB_NOT_IMPLEMENT;
  }
  virtual int get_lob_locator(const char* col_name, ObLobLocator*& lob_locator) const
  {
    UNUSED(col_name);
    UNUSED(lob_locator);
    return common::OB_NOT_IMPLEMENT;
  }
  virtual int get_type(const char* col_name, ObObjMeta& type) const = 0;
  virtual int get_obj(const char* col_name, ObObj& obj) const = 0;

  template <class Allocator>
  int get_number(const char* col_name, common::number::ObNumber& nmb_val, Allocator& allocator) const;

  template <class Allocator>
  int get_urowid(const char* col_name, common::ObURowIDData& urowid_data, Allocator& allocator) const;
  template <class Allocator>
  int get_lob_locator(const char* col_name, ObLobLocator*& lob_locator, Allocator& allocator) const;
  virtual int get_interval_ym(const char* col_name, common::ObIntervalYMValue& int_val) const = 0;
  virtual int get_interval_ds(const char* col_name, common::ObIntervalDSValue& int_val) const = 0;
  virtual int get_nvarchar2(const char* col_name, common::ObString& nvarchar2_val) const = 0;
  virtual int get_nchar(const char* col_name, common::ObString& nchar_val) const = 0;
  // single row get value
  int get_single_int(const int64_t row_idx, const int64_t col_idx, int64_t& int_val);

  virtual const ObNewRow* get_row() const
  {
    return NULL;
  }

protected:
  static const int64_t FAKE_TABLE_ID = 1;
  int varchar2datetime(const ObString& varchar, int64_t& datetime) const;

private:
  virtual int inner_get_number(
      const int64_t col_idx, common::number::ObNumber& nmb_val, IAllocator& allocator) const = 0;
  virtual int inner_get_number(
      const char* col_name, common::number::ObNumber& nmb_val, IAllocator& allocator) const = 0;

  virtual int inner_get_urowid(
      const char* col_name, common::ObURowIDData& urowid_data, common::ObIAllocator& allocator) const = 0;
  virtual int inner_get_urowid(
      const int64_t col_idx, common::ObURowIDData& urowid_data, common::ObIAllocator& allocator) const = 0;
  virtual int inner_get_lob_locator(
      const char* col_name, ObLobLocator*& lob_locator, common::ObIAllocator& allocator) const = 0;
  virtual int inner_get_lob_locator(
      const int64_t col_idx, ObLobLocator*& lob_locator, common::ObIAllocator& allocator) const = 0;
};

template <class Allocator>
int ObMySQLResult::get_number(const int64_t col_idx, common::number::ObNumber& nmb_val, Allocator& allocator) const
{
  TAllocator<Allocator> ta(allocator);
  return inner_get_number(col_idx, nmb_val, ta);
}

template <class Allocator>
int ObMySQLResult::get_number(const char* col_name, common::number::ObNumber& nmb_val, Allocator& allocator) const
{
  TAllocator<Allocator> ta(allocator);
  return inner_get_number(col_name, nmb_val, ta);
}

template <class Allocator>
int ObMySQLResult::get_urowid(const int64_t col_idx, common::ObURowIDData& urowid_data, Allocator& allocator) const
{
  TAllocator<Allocator> ta(allocator);
  return inner_get_urowid(col_idx, urowid_data, allocator);
}

template <class Allocator>
int ObMySQLResult::get_urowid(const char* col_name, common::ObURowIDData& urowid_data, Allocator& allocator) const
{
  TAllocator<Allocator> ta(allocator);
  return inner_get_urowid(col_name, urowid_data, allocator);
}

template <class Allocator>
int ObMySQLResult::get_lob_locator(const int64_t col_idx, ObLobLocator*& lob_locator, Allocator& allocator) const
{
  TAllocator<Allocator> ta(allocator);
  return inner_get_lob_locator(col_idx, lob_locator, allocator);
}

template <class Allocator>
int ObMySQLResult::get_lob_locator(const char* col_name, ObLobLocator*& lob_locator, Allocator& allocator) const
{
  TAllocator<Allocator> ta(allocator);
  return inner_get_lob_locator(col_name, lob_locator, allocator);
}
}  // namespace sqlclient
}  // namespace common
}  // namespace oceanbase
#endif  // OCEANBASE_OB_MYSQL_RESULT_H_
