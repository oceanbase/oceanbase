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

#define USING_LOG_PREFIX COMMON

#include "common/sql_mode/ob_sql_mode_utils.h"

#include <string.h>
#include "lib/charset/ob_charset.h"
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_allocator.h"
#include "common/object/ob_object.h"
#include "common/sql_mode/ob_sql_mode.h"
namespace oceanbase {
namespace common {

typedef struct {
  ObSQLMode int_val;
  const char* str_val;
} ObSqlModeMap;

ObSqlModeMap SQL_MODE_MAP[] = {{SMO_REAL_AS_FLOAT, STR_REAL_AS_FLOAT},
    {SMO_PIPES_AS_CONCAT, STR_PIPES_AS_CONCAT},
    {SMO_ANSI_QUOTES, STR_ANSI_QUOTES},
    {SMO_IGNORE_SPACE, STR_IGNORE_SPACE},
    {SMO_NOT_USED, STR_NOT_USED},
    {SMO_ONLY_FULL_GROUP_BY, STR_ONLY_FULL_GROUP_BY},
    {SMO_NO_UNSIGNED_SUBTRACTION, STR_NO_UNSIGNED_SUBTRACTION},
    {SMO_NO_DIR_IN_CREATE, STR_NO_DIR_IN_CREATE},
    {SMO_NO_KEY_OPTIONS, STR_NO_KEY_OPTIONS},
    {SMO_NO_TABLE_OPTIONS, STR_NO_TABLE_OPTIONS},
    {SMO_NO_FIELD_OPTIONS, STR_NO_FIELD_OPTIONS},
    {SMO_NO_AUTO_VALUE_ON_ZERO, STR_NO_AUTO_VALUE_ON_ZERO},
    {SMO_NO_BACKSLASH_ESCAPES, STR_NO_BACKSLASH_ESCAPES},
    {SMO_STRICT_TRANS_TABLES, STR_STRICT_TRANS_TABLES},
    {SMO_STRICT_ALL_TABLES, STR_STRICT_ALL_TABLES},
    {SMO_NO_ZERO_IN_DATE, STR_NO_ZERO_IN_DATE},
    {SMO_NO_ZERO_DATE, STR_NO_ZERO_DATE},
    {SMO_ALLOW_INVALID_DATES, STR_ALLOW_INVALID_DATES},
    {SMO_ERROR_FOR_DIVISION_BY_ZERO, STR_ERROR_FOR_DIVISION_BY_ZERO},
    {SMO_NO_AUTO_CREATE_USER, STR_NO_AUTO_CREATE_USER},
    {SMO_HIGH_NOT_PRECEDENCE, STR_HIGH_NOT_PRECEDENCE},
    {SMO_NO_ENGINE_SUBSTITUTION, STR_NO_ENGINE_SUBSTITUTION},
    {SMO_PAD_CHAR_TO_FULL_LENGTH, STR_PAD_CHAR_TO_FULL_LENGTH},
    {SMO_TRADITIONAL, STR_COMBINE_TRADITIONAL},  // Search in order, ensure that combine_str is in front
    {SMO_TRADITIONAL, STR_TRADITIONAL},
    {SMO_ANSI, STR_COMBINE_ANSI},
    {SMO_ANSI, STR_ANSI},
    {SMO_ERROR_ON_RESOLVE_CAST, STR_ERROR_ON_RESOLVE_CAST},
    {0, NULL}};

ObSQLMode SUPPORT_MODE = SMO_STRICT_ALL_TABLES | SMO_STRICT_TRANS_TABLES | SMO_PAD_CHAR_TO_FULL_LENGTH |
                         SMO_ONLY_FULL_GROUP_BY | SMO_NO_AUTO_VALUE_ON_ZERO | SMO_PIPES_AS_CONCAT |
                         SMO_HIGH_NOT_PRECEDENCE | SMO_ERROR_ON_RESOLVE_CAST;

bool is_sql_mode_supported(ObSQLMode mode)
{
  return 0 == (mode & ~SUPPORT_MODE);
}

#define MAX_MODE_STR_BUF_LEN 512
int ob_str_to_sql_mode(const ObString& str, ObSQLMode& mode)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  if (OB_UNLIKELY(str.length() >= MAX_MODE_STR_BUF_LEN)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("sql mode string is too long", K(str), K(ret));
  } else if (OB_ISNULL(buf = strndupa(str.ptr(), str.length()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc memory", K(ret));
  } else {
    char* value = NULL;
    char* saveptr = NULL;
    ObSQLMode tmp_mode = 0;
    for (value = strtok_r(buf, ", ", &saveptr); NULL != value && OB_SUCC(ret); value = strtok_r(NULL, ", ", &saveptr)) {
      uint64_t i = 0;
      for (; NULL != SQL_MODE_MAP[i].str_val && OB_SUCC(ret); ++i) {
        // there is no need to use ObCharset::strcmp, because all valid values are comprised of
        // ascii character, we can use C string functions instead.
        // besides, we are sure that these two strings are both '\0' terminated, so strcasecmp().
        if (0 == STRCASECMP(value, SQL_MODE_MAP[i].str_val)) {
          tmp_mode |= SQL_MODE_MAP[i].int_val;
          if (is_sql_mode_supported(SQL_MODE_MAP[i].int_val)) {
          } else {
            LOG_WARN("invalid sql_mode, not supported", K(SQL_MODE_MAP[i].str_val));
            ret = OB_NOT_SUPPORTED;
          }
          break;
        }
      }
      if (OB_ISNULL(SQL_MODE_MAP[i].str_val) && OB_SUCC(ret)) {
        ret = OB_ERR_WRONG_VALUE_FOR_VAR;
        LOG_WARN("failed to set sql_mode", K(value), K(ret));
      }
    }  // for
    if (OB_SUCC(ret)) {
      mode = tmp_mode;
    }
  }
  return ret;
}

int ob_sql_mode_to_str(const ObObj& int_val, ObObj& str_val, ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "allocator is NULL", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator->alloc(MAX_MODE_STR_BUF_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc memory", K(ret));
  } else {
    uint64_t uint64_val = 0;
    if (ObIntType == int_val.get_type()) {
      uint64_val = static_cast<uint64_t>(int_val.get_int());
    } else if (ObUInt64Type == int_val.get_type()) {
      uint64_val = int_val.get_uint64();
    } else {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid sql mode value type", K(ret), K(int_val));
    }
    if (OB_FAIL(ret)) {
    } else {
      char* end_ptr = buf;
      for (int64_t i = 0; NULL != SQL_MODE_MAP[i].str_val && OB_SUCC(ret); ++i) {
        if ((uint64_val & SQL_MODE_MAP[i].int_val) != 0) {
          if (!is_sql_mode_supported(SQL_MODE_MAP[i].int_val)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("invalid sql_mode, not supported", K(SQL_MODE_MAP[i].int_val));
          } else {
            snprintf(end_ptr, MAX_MODE_STR_BUF_LEN, "%s%c", SQL_MODE_MAP[i].str_val, ',');
            end_ptr += strlen(SQL_MODE_MAP[i].str_val) + 1;
          }
        }
      }  // end for
      if (OB_FAIL(ret)) {
      } else if (end_ptr == buf) {
        str_val.set_varchar(ObString(""));
      } else {
        ObString value_str;
        value_str.assign_ptr(buf, static_cast<int32_t>(end_ptr - buf - 1));
        str_val.set_varchar(value_str);
      }
    }
  }
  return ret;
}
}  // namespace common
}  // namespace oceanbase
