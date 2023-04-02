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

#ifndef OB_SQL_MODE_UTILS_H_
#define OB_SQL_MODE_UTILS_H_

#include "lib/string/ob_string.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "common/object/ob_object.h"
namespace oceanbase
{
namespace common
{
bool is_sql_mode_supported(ObSQLMode mode);

int ob_str_to_sql_mode(const ObString &str, ObSQLMode &mode);

int ob_sql_mode_to_str(const ObObj &int_val, ObObj &str_val, ObIAllocator *allocator);
inline ObSQLMode ob_compatibility_mode_to_sql_mode(ObCompatibilityMode comp_mode)
{
  return ObCompatibilityMode::ORACLE_MODE == comp_mode ? SMO_ORACLE : 0;
}

inline ObCompatibilityMode ob_sql_mode_to_compatibility_mode(ObSQLMode sql_mode)
{
  return 0 == (sql_mode & SMO_ORACLE) ? MYSQL_MODE : ORACLE_MODE;
}

inline bool is_strict_mode(ObSQLMode mode)
{
  return ((SMO_STRICT_ALL_TABLES & mode) || (SMO_STRICT_TRANS_TABLES & mode) || (SMO_ORACLE & mode));
}
inline bool is_pad_char_to_full_length(ObSQLMode mode)
{
  return (SMO_PAD_CHAR_TO_FULL_LENGTH & mode);
}
inline bool is_no_zero_date(ObSQLMode mode)
{
  return (SMO_NO_ZERO_DATE & mode);
}
inline bool is_no_zero_in_date(ObSQLMode mode)
{
  return (SMO_NO_ZERO_IN_DATE & mode);
}
inline bool is_no_unsigned_subtraction(ObSQLMode mode)
{
  return (SMO_NO_UNSIGNED_SUBTRACTION & mode);
} 
inline bool is_no_key_options(ObSQLMode mode)
{
  return (SMO_NO_KEY_OPTIONS & mode);
}
inline bool is_no_field_options(ObSQLMode mode)
{
  return (SMO_NO_FIELD_OPTIONS & mode);
}
inline bool is_no_table_options(ObSQLMode mode)
{
  return (SMO_NO_TABLE_OPTIONS & mode);
}
inline bool is_allow_invalid_dates(ObSQLMode mode)
{
  return (SMO_ALLOW_INVALID_DATES & mode);
}
inline bool is_no_auto_create_user(ObSQLMode mode)
{
  return (SMO_NO_AUTO_CREATE_USER & mode);
}
inline bool is_error_for_division_by_zero(ObSQLMode mode)
{
  return (SMO_ERROR_FOR_DIVISION_BY_ZERO & mode);
}
inline bool is_mysql_compatible(ObCompatibilityMode mode)
{
  return OCEANBASE_MODE == mode || MYSQL_MODE == mode;
}
inline bool is_oracle_compatible(ObCompatibilityMode mode)
{
  return ORACLE_MODE == mode;
}
inline bool is_mysql_compatible(ObSQLMode mode)
{
  return is_mysql_compatible(ob_sql_mode_to_compatibility_mode(mode));
}
inline bool is_oracle_compatible(ObSQLMode mode)
{
  return is_oracle_compatible(ob_sql_mode_to_compatibility_mode(mode));
}
inline ObCompatibilityMode get_compatibility_mode()
{
  return lib::is_oracle_mode() ? ObCompatibilityMode::ORACLE_MODE : ObCompatibilityMode::MYSQL_MODE;
}

inline bool is_only_full_group_by_on(ObSQLMode mode)
{
  return (SMO_ONLY_FULL_GROUP_BY & mode) || lib::is_oracle_mode();
}

inline bool is_time_truncate_fractional(ObSQLMode mode)
{
  return (SMO_TIME_TRUNCATE_FRACTIONAL & mode);
}

}
}

#endif /* OB_SQL_MODE_UTILS_H_ */
