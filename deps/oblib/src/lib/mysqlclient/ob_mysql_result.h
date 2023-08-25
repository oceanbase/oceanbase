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
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "common/object/ob_obj_type.h"
#include "common/object/ob_object.h"

#define COLUMN_MAP_BUCKET_NUM 107

#define EXTRACT_BOOL_FIELD_MYSQL_SKIP_RET(result, column_name, field) \
  if (OB_SUCC(ret)) \
  { \
    bool bool_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_bool(column_name, bool_value)))  \
    { \
      field = bool_value; \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
      field = false; \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", K(column_name), K(ret)); \
    }\
  }

#define EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, column_name, obj) \
  if (OB_SUCC(ret)) \
  { \
    bool bool_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_bool(#column_name, bool_value)))  \
    { \
      (obj).set_##column_name(bool_value); \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
      (obj).set_##column_name(false); \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }\
  }

#define EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL(result, column_name, obj) \
  if (OB_SUCC(ret)) \
  { \
    bool bool_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_bool(#column_name, bool_value)))  \
    { \
      (obj).set_##column_name(bool_value); \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }\
  }



#define EXTRACT_BOOL_FIELD_MYSQL(result, column_name, field) \
  if (OB_SUCC(ret)) \
  { \
    bool bool_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_bool(column_name, bool_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", K(column_name), K(ret)); \
    } \
    else \
    { \
      field = bool_value; \
    }\
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_BOOL_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, column_name, obj, skip_null_error, skip_column_error, default_value) \
  if (OB_SUCC(ret)) \
  { \
    bool bool_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_bool(#column_name, bool_value)))  \
    { \
      (obj).set_##column_name(bool_value); \
    } \
    else if (OB_ERR_NULL_VALUE == ret) \
		{ \
			if (skip_null_error) \
			{ \
				SQL_LOG(TRACE, "null value, ignore", "column_name", #column_name); \
        (obj).set_##column_name(default_value); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret)); \
			} \
		} \
    else if (OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
			if (skip_column_error) \
			{ \
				SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name); \
        (obj).set_##column_name(default_value); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret)); \
			} \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }\
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, column_name, field, type, skip_null_error, skip_column_error, default_value) \
  if (OB_SUCC(ret)) \
  { \
    int64_t int_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_int(column_name, int_value)))  \
    { \
      field = static_cast<type>(int_value); \
    } \
    else if (OB_ERR_NULL_VALUE == ret) \
    { \
      if (skip_null_error) \
      { \
        SQL_LOG(TRACE, "null value, ignore", K(column_name)); \
        field = static_cast<type>(default_value); \
        ret = OB_SUCCESS; \
      } \
      else \
      { \
        SQL_LOG(WARN, "null value", K(column_name), K(ret)); \
      } \
    } \
    else if (OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      if (skip_column_error) \
      { \
        SQL_LOG(INFO, "column not found, ignore", K(column_name)); \
        field = static_cast<type>(default_value); \
        ret = OB_SUCCESS; \
      } \
      else \
      { \
        SQL_LOG(WARN, "column not found", K(column_name), K(ret)); \
      } \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", K(column_name), K(ret)); \
    }\
  }

#define EXTRACT_UINT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, column_name, field, type, skip_null_error, skip_column_error, default_value) \
  if (OB_SUCC(ret)) \
  { \
    uint64_t int_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_uint(column_name, int_value)))  \
    { \
      field = static_cast<type>(int_value); \
    } \
    else if (OB_ERR_NULL_VALUE == ret) \
    { \
      if (skip_null_error) \
      { \
        SQL_LOG(TRACE, "null value, ignore", K(column_name)); \
        field = static_cast<type>(default_value); \
        ret = OB_SUCCESS; \
      } \
      else \
      { \
        SQL_LOG(WARN, "null value", K(column_name), K(ret)); \
      } \
    } \
    else if (OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      if (skip_column_error) \
      { \
        SQL_LOG(INFO, "column not found, ignore", K(column_name)); \
        field = static_cast<type>(default_value); \
        ret = OB_SUCCESS; \
      } \
      else \
      { \
        SQL_LOG(WARN, "column not found", K(column_name), K(ret)); \
      } \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", K(column_name), K(ret)); \
    }\
  }

#define EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, column_name, field, type) \
  if (OB_SUCC(ret)) \
  { \
    int64_t int_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_int(column_name, int_value))) \
    { \
      field = static_cast<type>(int_value); \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
      field = static_cast<type>(0); \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", K(column_name), K(ret)); \
    } \
  }

#define EXTRACT_DATETIME_FIELD_MYSQL_SKIP_RET(result, column_name, field, type) \
  if (OB_SUCC(ret)) \
  { \
    int64_t int_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_datetime(column_name, int_value))) \
    { \
      field = static_cast<type>(int_value); \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
      field = static_cast<type>(0); \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", K(column_name), K(ret)); \
    } \
  }

#define EXTRACT_INT_FIELD_MYSQL(result, column_name, field, type) \
  if (OB_SUCC(ret)) \
  { \
    int64_t int_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_int(column_name, int_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", column_name, K(ret)); \
    } \
    else \
    { \
      field = static_cast<type>(int_value); \
    }\
  }

#define EXTRACT_UINT_FIELD_MYSQL(result, column_name, field, type) \
  if (OB_SUCC(ret)) \
  { \
    uint64_t int_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_uint(column_name, int_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", column_name, K(ret)); \
    } \
    else \
    { \
      field = static_cast<type>(int_value); \
    }\
  }

#define EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, column_name, obj, type) \
  if (OB_SUCC(ret)) \
  { \
    int64_t int_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_int(#column_name, int_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else \
    { \
      (obj).set_##column_name(static_cast<type>(int_value)); \
    }\
  }
#define EXTRACT_INT_FIELD_TO_CLASS_VALUE_MYSQL(result, column_name, value_name, obj, type) \
  if (OB_SUCC(ret)) \
  { \
    int64_t int_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_int(#column_name, int_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else \
    { \
      (obj).set_##value_name(static_cast<type>(int_value)); \
    }\
  }

#define EXTRACT_UINT_FIELD_TO_CLASS_MYSQL(result, column_name, obj, type) \
  if (OB_SUCC(ret)) \
  { \
    uint64_t int_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_uint(#column_name, int_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else \
    { \
      (obj).set_##column_name(static_cast<type>(int_value)); \
    }\
  }

#define EXTRACT_LAST_DDL_TIME_FIELD_TO_INT_MYSQL(result, column_name, obj, type) \
  if (OB_SUCC(ret)) \
  { \
    int64_t int_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_datetime(#column_name, int_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else \
    { \
      (obj).set_##column_name(static_cast<type>(int_value)); \
    }\
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_UINT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, column_name, obj, type, skip_null_error, skip_column_error, default_value) \
  if (OB_SUCC(ret)) \
  { \
    uint64_t int_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_uint(#column_name, int_value)))  \
    { \
      (obj).set_##column_name(static_cast<type>(int_value)); \
    } \
    else if (OB_ERR_NULL_VALUE == ret) \
		{ \
			if (skip_null_error) \
			{ \
				SQL_LOG(TRACE, "null value, ignore", "column_name", #column_name); \
        (obj).set_##column_name(static_cast<type>(default_value)); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret)); \
			} \
		} \
    else if (OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
			if (skip_column_error) \
			{ \
				SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name); \
        (obj).set_##column_name(static_cast<type>(default_value)); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret)); \
			} \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }\
  }

#define EXTRACT_INT_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, column_name, obj, type) \
  if (OB_SUCC(ret)) \
  { \
    int64_t int_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_int(#column_name, int_value)))  \
    { \
      (obj).set_##column_name(static_cast<type>(int_value)); \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
      (obj).set_##column_name(static_cast<type>(0)); \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }\
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, column_name, obj, type, skip_null_error, skip_column_error, default_value) \
  if (OB_SUCC(ret)) \
  { \
    int64_t int_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_int(#column_name, int_value)))  \
    { \
      (obj).set_##column_name(static_cast<type>(int_value)); \
    } \
    else if (OB_ERR_NULL_VALUE == ret) \
		{ \
			if (skip_null_error) \
			{ \
				SQL_LOG(TRACE, "null value, ignore", "column_name", #column_name); \
        (obj).set_##column_name(static_cast<type>(default_value)); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret)); \
			} \
		} \
    else if (OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
			if (skip_column_error) \
			{ \
				SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name); \
        (obj).set_##column_name(static_cast<type>(default_value)); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret)); \
			} \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }\
  }

#define EXTRACT_DOUBLE_FIELD_MYSQL_SKIP_RET(result, column_name, field, type) \
  if (OB_SUCC(ret)) \
  { \
    double double_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_double(column_name, double_value))) \
    { \
      field = static_cast<type>(double_value); \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
      field = static_cast<type>(0); \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", K(column_name), K(ret)); \
    } \
  }

#define EXTRACT_DOUBLE_FIELD_MYSQL(result, column_name, field, type) \
  if (OB_SUCC(ret)) \
  { \
    double double_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_double(column_name, double_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", column_name, K(ret)); \
    } \
    else \
    { \
      field = static_cast<type>(double_value); \
    }\
  }

#define EXTRACT_DOUBLE_FIELD_TO_CLASS_MYSQL(result, column_name, obj, type) \
  if (OB_SUCC(ret)) \
  { \
    double double_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_double(#column_name, double_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else \
    { \
      (obj).set_##column_name(static_cast<type>(double_value)); \
    }\
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_DOUBLE_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, column_name, obj, type, skip_null_error, skip_column_error, default_value) \
  if (OB_SUCC(ret)) \
  { \
    double double_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_double(#column_name, double_value)))  \
    { \
      (obj).set_##column_name(static_cast<type>(double_value)); \
    } \
    else if (OB_ERR_NULL_VALUE == ret) \
		{ \
			if (skip_null_error) \
			{ \
				SQL_LOG(TRACE, "null value, ignore", "column_name", #column_name); \
        (obj).set_##column_name(static_cast<type>(default_value)); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret)); \
			} \
		} \
    else if (OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
			if (skip_column_error) \
			{ \
				SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name); \
        (obj).set_##column_name(static_cast<type>(default_value)); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret)); \
			} \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }\
  }

// TODO
// Now the ObNumber type interface is not perfect, the upper layer is ObSequenceSchema, the value of ObNumber type does not exceed int64_t, so the stack memory is used first
// ObNumber type is fully supported (by Yan Hua), which requires a lot of scene combing and allocator configuration, which will be replaced later
#define EXTRACT_NUMBER_FIELD_TO_CLASS_MYSQL(result, column_name, obj) \
  if (OB_SUCC(ret)) \
  { \
    common::number::ObNumber number_value; \
    char buffer[common::number::ObNumber::MAX_NUMBER_ALLOC_BUFF_SIZE]; \
    ObDataBuffer data_buffer(buffer, sizeof(buffer)); \
    if (OB_SUCCESS != (ret = (result).get_number(#column_name, number_value, data_buffer)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else if (OB_SUCCESS != (ret = (obj).set_##column_name(number_value))) \
    { \
      SQL_LOG(WARN, "fail to set column to object. ", "column_name", #column_name, K(ret)); \
    }\
  }

// for oracle dblink, all_object.object_id is number(38) type, but table_id_ in table_schema
// column_name's type inside obj schema is int64_t type, and the return value in resultset is number
#define EXTRACT_INT_FIELD_FROM_NUMBER_TO_CLASS_MYSQL(result, column_name, obj, type) \
  if (OB_SUCC(ret)) \
  { \
    common::number::ObNumber number_value; \
    char buffer[common::number::ObNumber::MAX_NUMBER_ALLOC_BUFF_SIZE]; \
    ObDataBuffer data_buffer(buffer, sizeof(buffer)); \
    int64_t context_val = -1; \
    if (OB_SUCCESS != (ret = (result).get_number(#column_name, number_value, data_buffer)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else if (!number_value.is_valid_int64(context_val)) { \
      SQL_LOG(WARN, "failed to get int64 from number", K(number_value), K(ret)); \
    } \
    else \
    { \
      (obj).set_##column_name(static_cast<type>(context_val)); \
    }\
  }

#define EXTRACT_INT_FIELD_FROM_NUMBER_TO_CLASS_MYSQL_WITH_TENAND_ID(result, column_name, obj, tenant_id) \
  if (OB_SUCC(ret)) \
  { \
    UNUSED(tenant_id); \
    common::number::ObNumber number_value; \
    char buffer[common::number::ObNumber::MAX_NUMBER_ALLOC_BUFF_SIZE]; \
    ObDataBuffer data_buffer(buffer, sizeof(buffer)); \
    int64_t context_val = -1; \
    if (OB_SUCCESS != (ret = (result).get_number(#column_name, number_value, data_buffer)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else if (!number_value.is_valid_int64(context_val)) { \
      SQL_LOG(WARN, "failed to get int64 from number", K(number_value), K(ret)); \
    } \
    else \
    { \
      (obj).set_##column_name(context_val); \
    }\
  }

#define EXTRACT_NUMBER_FIELD_MYSQL(result, column_name, number_value) \
  if (OB_SUCC(ret)) \
  { \
    if (OB_SUCCESS != (ret = (result).get_number(#column_name, number_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
  }

// TODO [zongmei.zzm]:
// The urowid type is not used in the internal performance stage, just define a macro, and then use the re-implementation code later
#define EXTRACT_UROWID_FIELD_TO_CLASS_MYSQL(result, column_name, obj) \
  if (OB_SUCC(ret)) {                                                 \
  }

#define EXTRACT_UROWID_FIELD_MYSQL(result, column_name, urowid_data) \
  if (OB_SUCC(ret)) {                                                \
    if (OB_SUCCESS !=                                                \
        (ret = (result).get_urowid(#column_name, urowid_data))) {    \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name",    \
              #column_name, K(ret));                                 \
    }                                                                \
  }

#define EXTRACT_LOB_FIELD_TO_CLASS_MYSQL(result, column_name, obj)   \
  if (OB_SUCC(ret)) {                                                \
    ObLobLocator *lob_locator = NULL;                                \
    if (OB_SUCCESS !=                                                \
        (ret = (result).get_lob_locator(#column_name, lob_locator))) {    \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name",    \
              #column_name, K(ret));                                 \
    } else if (OB_ISNULL(lob_locator)) {                             \
      obj.set_lob_locator(lob_locator);                              \
  }

#define EXTRACT_LOB_FIELD_MYSQL(result, column_name, lob_locator) \
  if (OB_SUCC(ret)) {                                                \
    if (OB_SUCCESS !=                                                \
        (ret = (result).get_lob_locator(column_name, lob_locator))) {    \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name",    \
              #column_name, K(ret));                                 \
    }                                                                \
  }

#define EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(result, column_name, obj) \
  if (OB_SUCC(ret)) \
  { \
    common::ObString varchar_value; \
    if (OB_SUCCESS != (ret = (result).get_varchar(#column_name, varchar_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else if (OB_SUCCESS != (ret = (obj).set_##column_name(varchar_value))) \
    { \
      SQL_LOG(WARN, "fail to set column to object. ", "column_name", #column_name, K(ret)); \
    }\
  }
#define EXTRACT_VARCHAR_FIELD_TO_CLASS_VALUE_MYSQL(result, column_name, value_name,  obj) \
  if (OB_SUCC(ret)) \
  { \
    common::ObString varchar_value; \
    if (OB_SUCCESS != (ret = (result).get_varchar(#column_name, varchar_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else if (OB_SUCCESS != (ret = (obj).set_##value_name(varchar_value))) \
    { \
      SQL_LOG(WARN, "fail to set column to object. ", "value_name", #value_name, K(ret)); \
    }\
  }

#define EXTRACT_VARCHAR_FIELD_MYSQL(result, column_name, field) \
  if (OB_SUCC(ret)) \
  { \
    if (OB_SUCCESS != (ret = (result).get_varchar(column_name, field))) \
    { \
      SQL_LOG(WARN, "fail to get varchar. ", K(ret), K(column_name)); \
    } \
  }

#define EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, column_name, field) \
  if (OB_SUCC(ret)) \
  { \
    if (OB_SUCCESS != (ret = (result).get_varchar(column_name, field))) \
    { \
      if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
      { \
        ret = OB_SUCCESS; \
      } \
      else \
      { \
        SQL_LOG(WARN, "get varchar failed", K(ret)); \
      } \
    } \
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, column_name, field, skip_null_error, skip_column_error, default_value) \
  if (OB_SUCC(ret)) \
  { \
    if (OB_SUCCESS != (ret = (result).get_varchar(column_name, field))) \
    { \
      if (OB_ERR_NULL_VALUE == ret) \
			{ \
				if (skip_null_error) \
				{ \
					SQL_LOG(TRACE, "null value, ignore", "column_name", #column_name); \
					ret = OB_SUCCESS; \
				} \
				else \
				{ \
					SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret)); \
				} \
			} \
      else if (OB_ERR_COLUMN_NOT_FOUND == ret) \
      { \
				if (skip_column_error) \
				{ \
					SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name, K(ret)); \
					field = default_value; \
					ret = OB_SUCCESS; \
				} \
				else \
				{ \
					SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret)); \
				} \
      } \
      else \
      { \
        SQL_LOG(WARN, "get varchar failed", K(ret)); \
      }\
    } \
  }

#define EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, column_name, class_obj) \
  if (OB_SUCC(ret)) \
  { \
    ObString str_value; \
    if (OB_SUCCESS == (ret = (result).get_varchar(#column_name, str_value))) \
    { \
      if (OB_FAIL((class_obj).set_##column_name(str_value))) \
      { \
        SQL_LOG(WARN, "fail to set value", KR(ret), K(str_value)); \
      } \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to extract varchar field mysql. ", K(ret)); \
    } \
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, column_name, class_obj, skip_null_error, skip_column_error, default_value) \
  if (OB_SUCC(ret)) \
  { \
    ObString str_value; \
    if (OB_SUCCESS == (ret = (result).get_varchar(#column_name, str_value))) \
    { \
      if (OB_FAIL((class_obj).set_##column_name(str_value))) \
      { \
        SQL_LOG(WARN, "fail to set value", KR(ret), K(str_value)); \
      } \
    } \
    else if (OB_ERR_NULL_VALUE == ret) \
    { \
			if (skip_null_error) \
			{ \
				SQL_LOG(TRACE, "null value, ignore", "column_name", #column_name); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret)); \
			} \
    } \
    else if (OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
			if (skip_column_error) \
			{ \
				SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name); \
        /*overwrite ret*/ \
        if (OB_FAIL((class_obj).set_##column_name(default_value))) \
        { \
          SQL_LOG(WARN, "fail to set value", KR(ret), K(default_value)); \
        } \
			} \
			else \
			{ \
				SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret)); \
			} \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to extract varchar field mysql. ", K(ret)); \
    } \
  }


#define EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(result, column_name, field, max_length, real_length) \
  if (OB_SUCC(ret)) \
  { \
    ObString str_value; \
    if (OB_SUCCESS == (ret = (result).get_varchar(column_name, str_value))) \
    { \
      if(str_value.length() >= max_length) \
      { \
        ret = OB_SIZE_OVERFLOW; \
        SQL_LOG(WARN, "field max length is not enough:", \
                  "max length", max_length, "str length", str_value.length()); \
      } \
      else \
      { \
        MEMCPY(field, str_value.ptr(), str_value.length()); \
        real_length = str_value.length(); \
        field[str_value.length()] = '\0'; \
      } \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
      real_length = 0; \
      field[0] = '\0'; \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to extract strbuf field mysql. ", K(ret)); \
    } \
  }


#define EXTRACT_STRBUF_FIELD_TO_CLASS_MYSQL_SKIP_RET(result, column_name, class_obj, max_length) \
  if (OB_SUCC(ret)) \
  { \
    ObString str_value; \
    if (OB_SUCCESS == (ret = (result).get_varchar(#column_name, str_value))) \
    { \
      if(str_value.length() >= max_length) \
      { \
        ret = OB_SIZE_OVERFLOW; \
        SQL_LOG(WARN, "field max length is not enough:", \
                  "max length", max_length, "str length", str_value.length()); \
      } \
      else \
      { \
        ret = (class_obj).set_##column_name(str_value); \
      } \
    } \
    else if (OB_ERR_NULL_VALUE == ret || OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
      ret = OB_SUCCESS; \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to extract strbuf field mysql. ", K(ret)); \
    } \
  }

// Macro with default value
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_STRBUF_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(result, column_name, class_obj, max_length, skip_null_error, skip_column_error, default_value) \
  if (OB_SUCC(ret)) \
  { \
    ObString str_value; \
    if (OB_SUCCESS == (ret = (result).get_varchar(#column_name, str_value))) \
    { \
      if(str_value.length() > max_length) \
      { \
        ret = OB_SIZE_OVERFLOW; \
        SQL_LOG(WARN, "field max length is not enough:", \
                  "max length", max_length, "str length", str_value.length()); \
      } \
      else \
      { \
        ret = (class_obj).set_##column_name(str_value); \
      } \
    } \
    else if (OB_ERR_NULL_VALUE == ret) \
    { \
			if (skip_null_error) \
			{ \
				SQL_LOG(TRACE, "null value, ignore", "column_name", #column_name); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret)); \
			} \
    } \
    else if (OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
			if (skip_column_error) \
			{ \
				SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name); \
        (class_obj).set_##column_name(default_value); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret)); \
			} \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to extract strbuf field mysql. ", K(ret)); \
    } \
  }

#define EXTRACT_STRBUF_FIELD_TO_CLASS_MYSQL(result, column_name, class_obj, max_length) \
  if (OB_SUCC(ret)) \
  { \
    ObString str_value; \
    if (OB_SUCCESS == (ret = (result).get_varchar(#column_name, str_value))) \
    { \
      if(str_value.length() > max_length) \
      { \
        ret = OB_SIZE_OVERFLOW; \
        SQL_LOG(WARN, "field max length is not enough:", \
                  "max length", max_length, "str length", str_value.length()); \
      } \
      else \
      { \
        ret = (class_obj).set_##column_name(str_value); \
      } \
    } \
    else \
    { \
      SQL_LOG(DEBUG, "fail to extract strbuf field mysql. ", K(ret)); \
    } \
  }


#define EXTRACT_STRBUF_FIELD_MYSQL(result, column_name, field, max_length, real_length) \
  if (OB_SUCC(ret)) \
  { \
    ObString str_value; \
    if (OB_SUCCESS == (ret = (result).get_varchar(column_name, str_value))) \
    { \
      if(str_value.length() >= max_length) \
      { \
        ret = OB_SIZE_OVERFLOW; \
        SQL_LOG(WARN, "field max length is not enough:", \
                  "max length", max_length, "str length", str_value.length()); \
      } \
      else \
      { \
        MEMCPY(field, str_value.ptr(), str_value.length()); \
        real_length = str_value.length(); \
        field[str_value.length()] = '\0'; \
      } \
    } \
    else \
    { \
      SQL_LOG(DEBUG, "fail to extract strbuf field mysql. ", K(ret)); \
    } \
  }

/** EXTRACT_NEW_DEFAULT_VALUE_FIELD_MYSQL用于获取__all_column中的默认值，后面做了修改，默认值可能
    从cur_default_value或者new_default_value获取，这里做了兼容处理，用default_value_v2_version做这两种情况的区分
*/
#define EXTRACT_DEFAULT_VALUE_FIELD_MYSQL(result, column_name, data_type, class_obj,is_cur_default_value, default_value_v2_version, tenant_id) \
  [&]() { /*+ The original macro use too much stack space, wrap to lambda to avoid it. */ \
  if (OB_SUCC(ret)) \
  { \
    ObString str_value; \
    ObObj res_obj; \
    ret = (result).get_varchar(#column_name, str_value); \
    \
    if (OB_ERR_NULL_VALUE == ret) \
    { /* without default value */                                     \
      if (!default_value_v2_version) {                                \
        res_obj.set_null();                                           \
        ret = (class_obj).set_##column_name(res_obj);                 \
      }                                                               \
      else {                                                          \
        ret = OB_SUCCESS;                                             \
      }                                                               \
    }                                                                 \
    else if (OB_ERR_COLUMN_NOT_FOUND == ret) \
    {                                                                   \
      SQL_LOG(WARN, "column not found, ignore", "column_name", #column_name); \
      ret = OB_SUCCESS;                                                 \
    }                                                                   \
    else if (OB_SUCC(ret)) \
    { /*big stack check ObSchemaRetrieveUtils::fill_column_schema*/  \
      ObObj def_obj;                                                 \
      ObArenaAllocator allocator(ObModIds::OB_SCHEMA);               \
      ObTimeZoneInfo tz_info;                                        \
      const ObDataTypeCastParams dtc_params(&tz_info);               \
      bool no_dtc_params = (ob_is_bit_tc(data_type) || ob_is_enum_or_set_type(data_type)); \
      ObCastCtx cast_ctx(&allocator, no_dtc_params ? NULL : &dtc_params, CM_NONE, column.get_collation_type());\
      if (is_cur_default_value && column.is_default_expr_v2_column())\
      { \
        res_obj.set_varchar(str_value); \
        ret = (class_obj).set_##column_name(res_obj); \
      } \
      else if (IS_DEFAULT_NOW_STR(data_type, str_value)) \
      { \
        res_obj.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG); \
        res_obj.set_scale(column.get_data_scale());\
        ret = (class_obj).set_##column_name(res_obj); \
      } \
      else if (column.is_generated_column()) { \
        res_obj.set_string(data_type, str_value); \
        res_obj.meta_.set_collation_type(CS_TYPE_UTF8MB4_BIN);  \
        res_obj.meta_.set_collation_level(CS_LEVEL_IMPLICIT); \
        ret = (class_obj).set_##column_name(res_obj); \
      } \
      else if (column.is_identity_column() || ob_is_string_type(data_type) || ob_is_geometry(data_type)) \
      { \
        res_obj.set_string(data_type, str_value); \
        res_obj.meta_.set_collation_type(column.get_collation_type());  \
        /* will override the collaction level set in set_varchar */ \
        res_obj.meta_.set_collation_level(CS_LEVEL_IMPLICIT); \
        /* only support full inrow data, all lobs from systable should be made inrow */ \
        if (res_obj.is_outrow_lob()) { \
          ret = OB_INVALID_ARGUMENT; \
          SQL_LOG(WARN, "outrow lob unsupported", "column_name", #column_name); \
        } \
        else { \
          if (ob_is_text_tc(data_type) || ob_is_geometry(data_type)) { res_obj.set_inrow(); } \
          ret = (class_obj).set_##column_name(res_obj); \
        } \
      }                                               \
      else { \
        if (ob_is_bit_tc(data_type) || ob_is_enum_or_set_type(data_type)) \
        {                                                                 \
          def_obj.set_varchar(str_value);                                 \
          ObObj tmp_obj;                                                  \
          if(OB_FAIL(ObObjCaster::to_type(ObUInt64Type, cast_ctx, def_obj, tmp_obj)))  \
          {                                                                            \
            SQL_LOG(WARN, "cast obj failed, ", "src type", def_obj.get_type());        \
          }                                                               \
          else                                                            \
          {                                                               \
            if (ObBitType == data_type) {                                 \
              res_obj.set_bit(tmp_obj.get_uint64());                      \
              res_obj.set_scale(column.get_data_precision());             \
            } else if (ObEnumType == data_type) {                         \
              res_obj.set_enum(tmp_obj.get_uint64());                     \
            } else {/*set type*/                                          \
              res_obj.set_set(tmp_obj.get_uint64());                      \
            }                                                             \
            ret = (class_obj).set_##column_name(res_obj);                 \
          }                                                               \
        }                                                                 \
        else if (ob_is_json(data_type))                                                   \
        {                                                                                 \
          def_obj.set_type(data_type);                                                    \
          if (is_mysql_mode()) {                                                          \
            if (OB_FAIL(def_obj.build_not_strict_default_value())) {                      \
              SQL_LOG(WARN, "failed to build not strict default json value", K(ret));     \
            } else {                                                                      \
              res_obj.set_json_value(data_type,  def_obj.get_string().ptr(),              \
                                      def_obj.get_string().length());                     \
            }                                                                             \
          } else {                                                                        \
            def_obj.set_json_value(data_type, str_value.ptr(), str_value.length());       \
            if (OB_FAIL(ObObjCaster::to_type(data_type, cast_ctx, def_obj, res_obj))) {   \
              SQL_LOG(WARN, "cast obj failed, ", "src type", def_obj.get_type(), "dest type", data_type); \
            } else {                                                                      \
              res_obj.set_inrow();                                                        \
            }                                                                             \
          }                                                                               \
          if (OB_SUCC(ret)) {                                                             \
            res_obj.meta_.set_collation_level(CS_LEVEL_IMPLICIT);                         \
            ret = (class_obj).set_##column_name(res_obj);                                 \
          }                                                                               \
        }                                                                                 \
        else \
        { \
          def_obj.set_varchar(str_value); \
          if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id, tz_info.get_tz_map_wrap())))  \
          {         \
            SQL_LOG(WARN, "get tenant timezone map failed", K(ret));    \
          }         \
          else if (OB_FAIL(ObObjCaster::to_type(data_type, cast_ctx, def_obj, res_obj))) \
          { \
            SQL_LOG(WARN, "cast obj failed, ", "src type", def_obj.get_type(), "dest type", data_type); \
          } \
          else \
          { \
            res_obj.set_scale(column.get_data_scale());\
            ret = (class_obj).set_##column_name(res_obj); \
          } \
        } \
      } \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to default value field mysql. ", K(ret)); \
    } \
  } \
  } ()

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
            SQL_LOG(WARN, "get column failed", K(ret));      \
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
#define GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(func, idx, obj, default_value) \
  ({                                                                                  \
    {                                                                                 \
      if (OB_SUCC(ret)) {                                                             \
        if (OB_SUCCESS != (ret = func(idx, obj))) {                                   \
          if (OB_ERR_NULL_VALUE == ret                                                \
              || OB_SIZE_OVERFLOW == ret                                              \
              || OB_INVALID_ARGUMENT == ret) {                                        \
            SQL_LOG(TRACE, "get column failed, so set default value", K(ret), "obj", #obj); \
            ret = OB_SUCCESS;                                                         \
            obj = default_value;                                                      \
          } else {                                                                    \
            SQL_LOG(WARN, "get column failed", K(ret));                                    \
          }                                                                           \
        }                                                                             \
      }                                                                               \
    }                                                                                 \
    ret;                                                                              \
  })

// for partition table use
// If the value of the column is NULL, or the column does not exist, take the default value default_value
// Inner_sql_result, core_proxy, ob_mysql_result_impl check out the error code that does not exist
// They are OB_SIZE_OVERFLOW, OB_ERR_NULL_VALUE and OB_INVALID_ARGUMENT
#define GET_TIMESTAMP_COL_BY_NAME_IGNORE_NULL_WITH_DEFAULT_VALUE(func, col_name, obj, default_value, tz_info) \
  ({                                                                                  \
    {                                                                                 \
      if (OB_SUCC(ret)) {                                                             \
        if (OB_SUCCESS != (ret = func(col_name, tz_info, obj))) {                     \
          if (OB_ERR_NULL_VALUE == ret                                                \
              || OB_SIZE_OVERFLOW == ret                                              \
              || OB_INVALID_ARGUMENT == ret                                           \
              || OB_ERR_COLUMN_NOT_FOUND == ret) {                                    \
            ret = OB_SUCCESS;                                                         \
            obj = default_value;                                                      \
          } else {                                                                    \
            SQL_LOG(WARN, "get column failed", K(ret));                                    \
          }                                                                           \
        }                                                                             \
      }                                                                               \
    }                                                                                 \
    ret;                                                                              \
  })

// Used to construct the ID encoded with tenant_id
#define EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, column_name, obj, tenant_id) \
  if (OB_SUCC(ret)) \
  { \
    UNUSED(tenant_id); \
    int64_t int_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_int(#column_name, int_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else \
    { \
      (obj).set_##column_name(int_value); \
    }\
  }

// Used to construct the ID encoded with tenant_id
#define EXTRACT_UINT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID(result, column_name, obj, tenant_id) \
  if (OB_SUCC(ret)) \
  { \
    UNUSED(tenant_id); \
    uint64_t uint_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_uint(#column_name, uint_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    } \
    else \
    { \
      (obj).set_##column_name(uint_value); \
    }\
  }

// Macro with default value, used to construct ID with tenant_id encoded
// 1. skip_null_error: indicates whether to ignore NULL values
// 2. skip_column_error: indicates whether to ignore column errors, and pass in ObSchemaService::g_ignore_column_retrieve_error_
// 3. default_value: indicates the default value passed in
#define EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID_AND_DEFAULT_VALUE(result, column_name, obj, tenant_id, skip_null_error, skip_column_error, default_value) \
  if (OB_SUCC(ret)) \
  { \
    UNUSED(tenant_id); \
    int64_t int_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_int(#column_name, int_value)))  \
    { \
      (obj).set_##column_name(int_value); \
    } \
    else if (OB_ERR_NULL_VALUE == ret) \
		{ \
			if (skip_null_error) \
			{ \
				SQL_LOG(TRACE, "null value, ignore", "column_name", #column_name); \
        (obj).set_##column_name(static_cast<uint64_t>(default_value)); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret)); \
			} \
		} \
    else if (OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
			if (skip_column_error) \
			{ \
				SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name); \
        (obj).set_##column_name(static_cast<uint64_t>(default_value)); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret)); \
			} \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }\
  }

#define EXTRACT_UINT_FIELD_TO_CLASS_MYSQL_WITH_TENANT_ID_AND_DEFAULT_VALUE(result, column_name, obj, tenant_id, skip_null_error, skip_column_error, default_value) \
  if (OB_SUCC(ret)) \
  { \
    UNUSED(tenant_id); \
    uint64_t uint_value = 0; \
    if (OB_SUCCESS == (ret = (result).get_uint(#column_name, uint_value)))  \
    { \
      (obj).set_##column_name(uint_value); \
    } \
    else if (OB_ERR_NULL_VALUE == ret) \
		{ \
			if (skip_null_error) \
			{ \
				SQL_LOG(TRACE, "null value, ignore", "column_name", #column_name); \
        (obj).set_##column_name(static_cast<uint64_t>(default_value)); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "null value", "column_name", #column_name, K(ret)); \
			} \
		} \
    else if (OB_ERR_COLUMN_NOT_FOUND == ret) \
    { \
			if (skip_column_error) \
			{ \
				SQL_LOG(INFO, "column not found, ignore", "column_name", #column_name); \
        (obj).set_##column_name(static_cast<uint64_t>(default_value)); \
				ret = OB_SUCCESS; \
			} \
			else \
			{ \
				SQL_LOG(WARN, "column not found", "column_name", #column_name, K(ret)); \
			} \
    } \
    else \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", #column_name, K(ret)); \
    }\
  }

// Macro with default value, used to construct ID with tenant_id encoded
#define EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(result, column_name, field, tenant_id) \
  if (OB_SUCC(ret)) \
  { \
    UNUSED(tenant_id); \
    int64_t int_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_int(column_name, int_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", column_name, K(ret)); \
    } \
    else \
    { \
      field = static_cast<uint64_t>(int_value); \
    }\
  }
#define EXTRACT_UINT_FIELD_MYSQL_WITH_TENANT_ID(result, column_name, field, tenant_id) \
  if (OB_SUCC(ret)) \
  { \
    UNUSED(tenant_id); \
    uint64_t int_value = 0; \
    if (OB_SUCCESS != (ret = (result).get_uint(column_name, int_value)))  \
    { \
      SQL_LOG(WARN, "fail to get column in row. ", "column_name", column_name, K(ret)); \
    } \
    else \
    { \
      field = static_cast<uint64_t>(int_value); \
    }\
  }

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObNewRow;
struct ObLobLocator;
namespace sqlclient
{
class ObMySQLResult
{
public:
  //see this for template virtual function
  DEFINE_ALLOCATOR_WRAPPER
  ObMySQLResult();
  virtual ~ObMySQLResult();
  //virtual int64_t get_row_count(void) const = 0;
  virtual int64_t get_column_count() const = 0;
  /*
   * move result cursor to next row
   */
  virtual int close() = 0;
  virtual int next() = 0;

  //debug function
  virtual int print_info() const;
  /*
   * read int/str/TODO from result set
   * col_idx: indicate which column to read, [0, max_read_col)
   */
  virtual int get_int(const int64_t col_idx, int64_t &int_val) const = 0;
  virtual int get_uint(const int64_t col_idx, uint64_t &int_val) const = 0;
  virtual int get_datetime(const int64_t col_idx, int64_t &datetime) const = 0;
  virtual int get_date(const int64_t col_idx, int32_t &date) const = 0;
  virtual int get_time(const int64_t col_idx, int64_t &time) const = 0;
  virtual int get_year(const int64_t col_idx, uint8_t &year) const = 0;
  virtual int get_bool(const int64_t col_idx, bool &bool_val) const = 0;
  virtual int get_varchar(const int64_t col_idx, common::ObString &varchar_val) const = 0;
  virtual int get_raw(const int64_t col_idx, common::ObString &varchar_val) const = 0;
  virtual int get_float(const int64_t col_idx, float &float_val) const = 0;
  virtual int get_double(const int64_t col_idx, double &double_val) const = 0;
  virtual int get_timestamp(const int64_t col_idx, const common::ObTimeZoneInfo *tz_info, int64_t &int_val) const = 0;
  virtual int get_otimestamp_value(const int64_t col_idx,
                                   const common::ObTimeZoneInfo &tz_info,
                                   const common::ObObjType type,
                                   common::ObOTimestampData &otimestamp_val) const = 0;
  virtual int get_timestamp_tz(const int64_t col_idx,
                               const common::ObTimeZoneInfo &tz_info,
                               common::ObOTimestampData &otimestamp_val) const = 0;
  virtual int get_timestamp_ltz(const int64_t col_idx,
                                const common::ObTimeZoneInfo &tz_info,
                                common::ObOTimestampData &otimestamp_val) const = 0;
  virtual int get_timestamp_nano(const int64_t col_idx,
                                 const common::ObTimeZoneInfo &tz_info,
                                 common::ObOTimestampData &otimestamp_val) const = 0;
  virtual int get_number(const int64_t col_idx, common::number::ObNumber &nmb_val) const
  {
    UNUSED(col_idx); UNUSED(nmb_val); return common::OB_NOT_IMPLEMENT;
  }

  virtual int get_urowid(const int64_t col_idx, common::ObURowIDData &urowid_data) const
  {
    UNUSED(col_idx); UNUSED(urowid_data); return OB_NOT_IMPLEMENT;
  }
  virtual int get_lob_locator(const int64_t col_idx, ObLobLocator *&lob_locator) const
  {
    UNUSED(col_idx); UNUSED(lob_locator); return OB_NOT_IMPLEMENT;
  }
  virtual int get_type(const int64_t col_idx, ObObjMeta &type) const = 0;
  virtual int get_col_meta(const int64_t col_idx, bool old_max_length,
                           oceanbase::common::ObString &name, ObDataType &data_type) const = 0;
  int format_precision_scale_length(int16_t &precision, int16_t &scale, int32_t &length,
                                     oceanbase::common::ObObjType ob_type, oceanbase::common::ObCollationType cs_type,
                                     DblinkDriverProto link_type, bool old_max_length) const;
  /// @note return OB_SUCCESS instead of OB_ERR_NULL_VALUE when obj is null
  virtual int get_obj(const int64_t col_idx, ObObj &obj,
                      const common::ObTimeZoneInfo *tz_info = NULL,
                      common::ObIAllocator *allocator = NULL) const = 0;

  template <class Allocator>
  int get_number(const int64_t col_idx, common::number::ObNumber &nmb_val,
                 Allocator &allocator) const;

  template <class Allocator>
  int get_urowid(const int64_t col_idx, common::ObURowIDData &urowid_data,
                 Allocator &allocator) const;
  template <class Allocator>
  int get_lob_locator(const int64_t col_idx, ObLobLocator *&lob_locator,
                 Allocator &allocator) const;

  virtual int get_interval_ym(const int64_t col_idx, common::ObIntervalYMValue &int_val) const = 0;
  virtual int get_interval_ds(const int64_t col_idx, common::ObIntervalDSValue &int_val) const = 0;
  virtual int get_nvarchar2(const int64_t col_idx, common::ObString &nvarchar2_val) const = 0;
  virtual int get_nchar(const int64_t col_idx, common::ObString &nchar_val) const = 0;
  /*
  * read int/str/TODO from result set
  * col_name: indicate which column to read
  * @return  OB_INVALID_PARAM if col_name does not exsit
  */
  virtual int get_int(const char *col_name, int64_t &int_val) const = 0;
  virtual int get_uint(const char *col_name, uint64_t &int_val) const = 0;
  virtual int get_datetime(const char *col_name, int64_t &datetime) const = 0;
  virtual int get_date(const char *col_name, int32_t &date) const = 0;
  virtual int get_time(const char *col_name, int64_t &time) const = 0;
  virtual int get_year(const char *col_name, uint8_t &year) const = 0;
  virtual int get_bool(const char *col_name, bool &bool_val) const = 0;
  virtual int get_varchar(const char *col_name, common::ObString &varchar_val) const = 0;
  virtual int get_raw(const char *col_name, common::ObString &raw_val) const = 0;
  virtual int get_float(const char *col_name, float &float_val) const = 0;
  virtual int get_double(const char *col_name, double &double_val) const = 0;
  virtual int get_timestamp(const char *col_name, const common::ObTimeZoneInfo *tz_info, int64_t &int_val) const = 0;
  virtual int get_otimestamp_value(const char *col_name,
                                   const common::ObTimeZoneInfo &tz_info,
                                   const common::ObObjType type,
                                   common::ObOTimestampData &otimestamp_val) const = 0;
  virtual int get_timestamp_tz(const char *col_name,
                               const common::ObTimeZoneInfo &tz_info,
                               common::ObOTimestampData &otimestamp_val) const = 0;
  virtual int get_timestamp_ltz(const char *col_name,
                                const common::ObTimeZoneInfo &tz_info,
                                common::ObOTimestampData &otimestamp_val) const = 0;
  virtual int get_timestamp_nano(const char *col_name,
                                 const common::ObTimeZoneInfo &tz_info,
                                 common::ObOTimestampData &otimestamp_val) const = 0;
  virtual int get_number(const char *col_name, common::number::ObNumber &nmb_val) const
  {
    UNUSED(col_name); UNUSED(nmb_val); return common::OB_NOT_IMPLEMENT;
  }

  virtual int get_urowid(const char *col_name, common::ObURowIDData &urowid_data) const
  {
    UNUSED(col_name); UNUSED(urowid_data); return common::OB_NOT_IMPLEMENT;
  }
  virtual int get_lob_locator(const char *col_name, ObLobLocator *&lob_locator) const
  {
    UNUSED(col_name); UNUSED(lob_locator); return common::OB_NOT_IMPLEMENT;
  }
  virtual int get_type(const char* col_name, ObObjMeta &type) const = 0;
  virtual int get_obj(const char* col_name, ObObj &obj) const = 0;

  template <class Allocator>
  int get_number(const char *col_name, common::number::ObNumber &nmb_val, Allocator &allocator) const;

  template<class Allocator>
  int get_urowid(const char *col_name,
                 common::ObURowIDData &urowid_data, Allocator &allocator) const;
  template<class Allocator>
  int get_lob_locator(const char *col_name,
                 ObLobLocator *&lob_locator, Allocator &allocator) const;
  virtual int get_interval_ym(const char *col_name, common::ObIntervalYMValue &int_val) const = 0;
  virtual int get_interval_ds(const char *col_name, common::ObIntervalDSValue &int_val) const = 0;
  virtual int get_nvarchar2(const char *col_name, common::ObString &nvarchar2_val) const = 0;
  virtual int get_nchar(const char *col_name, common::ObString &nchar_val) const = 0;
  // single row get value
  int get_single_int(const int64_t row_idx, const int64_t col_idx, int64_t &int_val);

  virtual const ObNewRow *get_row() const { return NULL; }
  virtual int set_expected_charset_id(uint16_t charset_id, uint16_t ncharset_id)
  { UNUSEDx(charset_id, ncharset_id); return OB_SUCCESS; }
protected:
  static const int64_t FAKE_TABLE_ID = 1;
  int varchar2datetime(const ObString &varchar, int64_t &datetime) const;
private:
  virtual int inner_get_number(const int64_t col_idx, common::number::ObNumber &nmb_val,
                               IAllocator &allocator) const = 0;
  virtual int inner_get_number(const char *col_name, common::number::ObNumber &nmb_val,
                               IAllocator &allocator) const = 0;

  virtual int inner_get_urowid(const char *col_name, common::ObURowIDData &urowid_data,
                               common::ObIAllocator &allocator) const = 0;
  virtual int inner_get_urowid(const int64_t col_idx, common::ObURowIDData &urowid_data,
                               common::ObIAllocator &allocator) const = 0;
  virtual int inner_get_lob_locator(const char *col_name, ObLobLocator *&lob_locator,
                               common::ObIAllocator &allocator) const = 0;
  virtual int inner_get_lob_locator(const int64_t col_idx, ObLobLocator *&lob_locator,
                               common::ObIAllocator &allocator) const = 0;
};

template <class Allocator>
int ObMySQLResult::get_number(const int64_t col_idx, common::number::ObNumber &nmb_val,
                              Allocator &allocator) const
{
  TAllocator<Allocator> ta(allocator);
  return inner_get_number(col_idx, nmb_val, ta);
}

template <class Allocator>
int ObMySQLResult::get_number(const char *col_name, common::number::ObNumber &nmb_val,
                              Allocator &allocator) const
{
  TAllocator<Allocator> ta(allocator);
  return inner_get_number(col_name, nmb_val, ta);
}

template <class Allocator>
int ObMySQLResult::get_urowid(const int64_t col_idx, common::ObURowIDData &urowid_data,
                              Allocator &allocator) const
{
  TAllocator<Allocator> ta(allocator);
  return inner_get_urowid(col_idx, urowid_data, allocator);
}

template <class Allocator>
int ObMySQLResult::get_urowid(const char* col_name, common::ObURowIDData &urowid_data,
                              Allocator &allocator) const
{
  TAllocator<Allocator> ta(allocator);
  return inner_get_urowid(col_name, urowid_data, allocator);
}

template <class Allocator>
int ObMySQLResult::get_lob_locator(const int64_t col_idx, ObLobLocator *&lob_locator,
                              Allocator &allocator) const
{
  TAllocator<Allocator> ta(allocator);
  return inner_get_lob_locator(col_idx, lob_locator, allocator);
}

template <class Allocator>
int ObMySQLResult::get_lob_locator(const char* col_name, ObLobLocator *&lob_locator,
                              Allocator &allocator) const
{
  TAllocator<Allocator> ta(allocator);
  return inner_get_lob_locator(col_name, lob_locator, allocator);
}
}
}
}
#endif //OCEANBASE_OB_MYSQL_RESULT_H_
