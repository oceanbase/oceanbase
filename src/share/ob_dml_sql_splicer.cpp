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

#define USING_LOG_PREFIX SHARE

#include "share/ob_dml_sql_splicer.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "deps/oblib/src/lib/literals/ob_literals.h"

namespace oceanbase
{
namespace share
{
using namespace common;

const char *const ObDMLSqlSplicer::NULL_VALUE = NULL;

void ObDMLSqlSplicer::reset()
{
  values_.reset();
  columns_.reset();
  extra_condition_.reset();
  rows_end_pos_.reset();
  default_column_header_.reset();
  default_column_value_.reset();
}

int ObDMLSqlSplicer::append_uint64_value(const uint64_t value, bool &is_null)
{
  is_null = false;
  return values_.append_fmt("%lu", value);
}

int ObDMLSqlSplicer::append_value(const uint64_t value, bool &is_null)
{
  return append_value(static_cast<int64_t>(value), is_null);
  //is_null = false;
  //return values_.append_fmt("%lu", value);
}

int ObDMLSqlSplicer::append_value(const int64_t value, bool &is_null)
{
  is_null = false;
  return values_.append_fmt("%ld", value);
}

int ObDMLSqlSplicer::append_value(const uint32_t value, bool &is_null)
{
  is_null = false;
  return values_.append_fmt("%u", value);
}

int ObDMLSqlSplicer::append_value(const int32_t value, bool &is_null)
{
  is_null = false;
  return values_.append_fmt("%d", value);
}

int ObDMLSqlSplicer::append_value(const uint16_t value, bool &is_null)
{
  is_null = false;
  return values_.append_fmt("%u", value);
}

int ObDMLSqlSplicer::append_value(const int16_t value, bool &is_null)
{
  is_null = false;
  return values_.append_fmt("%d", value);
}

int ObDMLSqlSplicer::append_value(const uint8_t value, bool &is_null)
{
  is_null = false;
  return values_.append_fmt("%u", value);
}

int ObDMLSqlSplicer::append_value(const int8_t value, bool &is_null)
{
  is_null = false;
  return values_.append_fmt("%d", value);
}

int ObDMLSqlSplicer::append_value(const bool value, bool &is_null)
{
  is_null = false;
  return values_.append_fmt("%d", value);
}

int ObDMLSqlSplicer::append_value(const double value, bool &is_null)
{
  is_null = false;
  return values_.append_fmt("%lf", value);
}

int ObDMLSqlSplicer::append_value(const char *str, bool &is_null)
{
  is_null = (NULL == str);
  int ret = OB_SUCCESS;
  if (!is_null) {
    if (OB_FAIL(values_.append_fmt(mode_ == NAKED_VALUE_MODE ? "%s" : "'%s'", str))) {
      LOG_WARN("append string to values failed", K(ret), K(str));
    }
  }
  return ret;
}

int ObDMLSqlSplicer::append_value(const common::number::ObNumber &nmb, bool &is_null)
{
  is_null = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(values_.append_fmt("%s", nmb.format()))) {
    LOG_WARN("append number to values failed", K(ret), K(nmb));
  }
  return ret;
}

int ObDMLSqlSplicer::append_value(char *str, bool &is_null)
{
  return append_value(static_cast<const char *>(str), is_null);
}

int ObDMLSqlSplicer::append_value(const ObString &str, bool &is_null)
{
  is_null = (NULL == str.ptr());
  int ret = OB_SUCCESS;
  if (!is_null) {
    if (OB_FAIL(values_.append_fmt(mode_ == NAKED_VALUE_MODE ? "%.*s" : "'%.*s'",
        str.length(), str.ptr()))) {
      LOG_WARN("append string to values failed", K(ret), K(str));
    }
  }
  return ret;
}

int ObDMLSqlSplicer::append_value(const ObHexEscapeSqlStr &escape_str, bool &is_null)
{
  int ret = OB_SUCCESS;
  if (NAKED_VALUE_MODE == mode_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("add hex escaped string value in naked mode not supported", K(ret), K(escape_str));
  } else {
    is_null = (NULL == escape_str.str().ptr());
    if (!is_null) {
      if (OB_FAIL(sql_append_hex_escape_str(escape_str.str(), values_))) {
        LOG_WARN("sql_append_hex_escape_str failed", K(escape_str), K(ret));
      } else {
        is_hex_value_ = true;
      }
    }
  }
  return ret;
}

int ObDMLSqlSplicer::append_value(const ObRealUInt64 &value, bool &is_null)
{
  is_null = false;
  return values_.append_fmt("%lu", value.value());
}

int ObDMLSqlSplicer::append_value(const ObObj &obj, bool &is_null)
{
  int ret = OB_SUCCESS;
  switch(obj.get_type()) {
    case ObNullType:
      is_null = true;
      break;
    case ObTinyIntType:
      ret = append_value(obj.get_tinyint(), is_null);
      break;
    case ObSmallIntType:
      ret = append_value(obj.get_smallint(), is_null);
      break;
    case ObMediumIntType:
      ret = append_value(obj.get_mediumint(), is_null);
      break;
    case ObInt32Type:
      ret = append_value(obj.get_int32(), is_null);
      break;
    case ObIntType:
      ret = append_value(obj.get_int(), is_null);
      break;
    case ObUTinyIntType:
      ret = append_value(obj.get_utinyint(), is_null);
      break;
    case ObUSmallIntType:
      ret = append_value(obj.get_usmallint(), is_null);
      break;
    case ObUMediumIntType:
      ret = append_value(obj.get_umediumint(), is_null);
      break;
    case ObUInt32Type:
      ret = append_value(obj.get_uint32(), is_null);
      break;
    case ObUInt64Type:
      ret = append_value(obj.get_uint64(), is_null);
      break;
    case ObDoubleType:
      ret = append_value(obj.get_double(), is_null);
      break;
    case ObUDoubleType:
      ret = append_value(obj.get_udouble(), is_null);
      break;
    case ObVarcharType:
      if (obj.get_collation_type() == CS_TYPE_BINARY) {
        // varbinary
        ret = append_value(ObHexEscapeSqlStr(obj.get_string()), is_null);
      } else {
        // varchar
        ret = append_value(obj.get_string(), is_null);
      }
      break;
    case ObCharType:
      if (obj.get_collation_type() == CS_TYPE_BINARY) {
        // binary
        ret = append_value(ObHexEscapeSqlStr(obj.get_string()), is_null);
      } else {
        // char
        ret = append_value(obj.get_string(), is_null);
      }
      break;
    case ObFloatType:
    case ObUFloatType:
    case ObNumberType:
    case ObUNumberType:
    case ObDateTimeType:
    case ObTimestampType:
    case ObDateType:
    case ObTimeType:
    case ObYearType:
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported object type", "obj_type", obj.get_type());
      break;
  }
  return ret;
}

int ObDMLSqlSplicer::add_pk_column(const bool is_null, const char *col_name)
{
  const bool is_pk = true;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column name", K(ret), KP(col_name));
  } else if (OB_FAIL(add_column(is_pk, is_null, col_name))) {
    LOG_WARN("add column failed", K(ret), K(is_pk), K(is_null), K(col_name));
  }
  return ret;
}

int ObDMLSqlSplicer::add_column(const bool is_null, const char *col_name)
{
  const bool is_pk = false;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column name", K(ret), KP(col_name));
  } else if (OB_FAIL(add_column(is_pk, is_null, col_name))) {
    LOG_WARN("add column failed", K(ret), K(is_pk), K(is_null), K(col_name));
  }
  return ret;
}

int ObDMLSqlSplicer::add_column(
    const bool is_primary_key, const bool is_null, const char *col_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column name", K(ret), KP(col_name));
  } else {
    Column col;
    col.primary_key_ = is_primary_key;
    col.is_null_ = is_null;
    col.name_ = col_name;
    col.value_end_pos_ = values_.length();
    col.is_hex_value_ = is_hex_value_;
    // reset it
    is_hex_value_ = false;
    if (OB_FAIL(columns_.push_back(col))) {
      LOG_WARN("push column failed", K(ret), K(col_name));
    }
  }
  return ret;
}

int ObDMLSqlSplicer::add_uint64_pk_column(const char *col_name, const uint64_t value)
{
  const bool is_pk = true;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column name", K(ret), KP(col_name));
  } else if (OB_FAIL(add_uint64_column(is_pk, col_name, value))) {
    LOG_WARN("add column failed", K(ret), K(is_pk), K(value), K(col_name));
  }
  return ret;
}

int ObDMLSqlSplicer::add_uint64_column(const char *col_name, const uint64_t value)
{
  const bool is_pk = false;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column name", K(ret), KP(col_name));
  } else if (OB_FAIL(add_uint64_column(is_pk, col_name, value))) {
    LOG_WARN("add column failed", K(ret), K(is_pk), K(value), K(col_name));
  }
  return ret;
}

int ObDMLSqlSplicer::add_uint64_column(
    const bool is_primary_key, const char *col_name, const uint64_t value)
{
  int ret = OB_SUCCESS;
  bool is_null = false;
  if (OB_ISNULL(col_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column name", K(ret), KP(col_name));
  } else if (OB_FAIL(append_uint64_value(value, is_null))) {
    LOG_WARN("append value failed", K(ret), K(value));
  } else if (OB_FAIL(add_column(is_primary_key, is_null, col_name))) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

int ObDMLSqlSplicer::add_time_column(const char *col_name,
                                     const int64_t now,
                                     bool is_pk/*default false*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column name", K(ret), KP(col_name));
  } else {
    if (now > 0) {
      if (OB_FAIL(values_.append_fmt("usec_to_time(%ld)", now))) {
        LOG_WARN("append value failed", K(ret));
      }
    } else {
      if (OB_FAIL(values_.append_fmt("now(6)"))) {
        LOG_WARN("append value failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    const bool is_primary_key = is_pk;
    const bool is_null = false;
    if (OB_FAIL(add_column(is_primary_key, is_null, col_name))) {
      LOG_WARN("add column failed", K(ret), K(is_primary_key), K(is_null), K(col_name));
    }
  }
  return ret;
}

int ObDMLSqlSplicer::add_raw_time_column(const char *col_name, const int64_t now)
{
  int ret = OB_SUCCESS;
  const bool is_primary_key = false;
  const bool is_null = false;

  if (OB_ISNULL(col_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column name", K(ret), KP(col_name));
  } else if (OB_FAIL(values_.append_fmt("usec_to_time(%ld)", now))) {
    LOG_WARN("append value failed", K(ret));
  } else if (OB_FAIL(add_column(is_primary_key, is_null, col_name))) {
    LOG_WARN("add column failed", K(ret), K(is_primary_key), K(is_null), K(col_name));
  }
  return ret;

}

int ObDMLSqlSplicer::splice_insert(const char *table_name, const char *head,
    ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (NULL == table_name || NULL == head) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name), KP(head));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else {
    if (OB_FAIL(sql.assign_fmt("%s INTO %s (", head, table_name))) {
      LOG_WARN("assign sql failed", K(ret));
    } else if (OB_FAIL(splice_column(", ", ColSet::ALL, ValSet::ONLY_COL_NAME, sql))) {
      LOG_WARN("add column name failed", K(ret));
    } else if (!default_column_header_.empty() && OB_FAIL(sql.append_fmt(", %s", default_column_header_.ptr()))) {
      LOG_WARN("failed to append default_column_header_", KR(ret), K(default_column_header_));
    } else if (OB_FAIL(sql.append(") VALUES ("))) {
      LOG_WARN("append sql failed", K(ret));
    } else if (OB_FAIL(splice_column(", ", ColSet::ALL, ValSet::ONLY_VALUE, sql))) {
      LOG_WARN("add values failed", K(ret));
    } else if (!default_column_value_.empty() && OB_FAIL(sql.append_fmt(", %s", default_column_value_.ptr()))) {
      LOG_WARN("failed to append default_column_value_", KR(ret), K(default_column_value_));
    } else if (OB_FAIL(sql.append(")"))) {
      LOG_WARN("append sql failed", K(ret));
    }
  }
  return ret;
}

int ObDMLSqlSplicer::splice_column(const char *sep,
    const ColSet::Type col_set, const ValSet::Type val_set, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  bool first = true;
  int64_t start_pos = 0;
  if (NULL == sep) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sep));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
    const Column &col = columns_.at(i);
    if (ColSet::ALL == col_set
        || (ColSet::ONLY_PK == col_set && col.primary_key_)
        || (ColSet::FILTER_PK == col_set && !col.primary_key_)) {
      if (!first) {
        if (OB_FAIL(sql.append(sep))) {
          LOG_WARN("append sql failed");
        }
      }
      if (OB_SUCC(ret)) {
        switch (val_set) {
          case ValSet::ONLY_COL_NAME: {
            ret = sql.append(col.name_);
            break;
          }
          case ValSet::ONLY_VALUE: {
            if (col.is_null_) {
              ret = sql.append("NULL");
            } else {
              ret = sql.append(ObString(
                  col.value_end_pos_ - start_pos, values_.ptr() + start_pos));
            }
            break;
          }
          case ValSet::ALL : {
            if (col.is_null_) {
              ret = sql.append_fmt("%s = NULL", col.name_);
            } else {
              ret = sql.append_fmt("%s = %.*s", col.name_,
                  static_cast<int32_t>(col.value_end_pos_ - start_pos),
                  values_.ptr() + start_pos);
            }
            break;
          }
          default : {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected value", K(ret), K(val_set));
          }
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("append column failed", K(ret));
        }
      }
      first = false;
    }
    start_pos = col.value_end_pos_;
  }
  return ret;
}

int ObDMLSqlSplicer::splice_insert_sql_without_plancache(const char *table_name, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_insert(table_name, "INSERT /*+use_plan_cache(none)*/", sql))) {
    LOG_WARN("splice insert failed", K(ret), K(table_name));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_insert_sql(const char *table_name, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_insert(table_name, "INSERT", sql))) {
    LOG_WARN("splice insert failed", K(ret), K(table_name));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_insert_ignore_sql(const char *table_name, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_insert(table_name, "INSERT IGNORE", sql))) {
    LOG_WARN("splice insert failed", K(ret), K(table_name));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_insert_update_sql(const char *table_name, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_insert_sql(table_name, sql))) {
    LOG_WARN("splice insert sql failed", K(ret), K(table_name));
  } else if (OB_FAIL(sql.append(" ON DUPLICATE KEY UPDATE "))) {
    LOG_WARN("append sql failed", K(ret));
  } else if (OB_FAIL(splice_column(", ", ColSet::FILTER_PK, ValSet::ALL, sql))) {
    LOG_WARN("splice column failed", K(ret));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_replace_sql(const char *table_name, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_insert(table_name, "REPLACE", sql))) {
    LOG_WARN("splice insert failed", K(ret), K(table_name));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_delete_sql(const char *table_name, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE ", table_name))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(splice_column(" AND ", ColSet::ONLY_PK, ValSet::ALL, sql))) {
    LOG_WARN("splice where condition failed", K(ret));
  } else {
    if (!extra_condition_.empty()) {
      if (OB_FAIL(sql.append_fmt(" AND %.*s",
          static_cast<int32_t>(extra_condition_.length()), extra_condition_.ptr()))) {
        LOG_WARN("add extra condition failed", K(ret), K_(extra_condition));
      }
    }
  }

  return ret;
}

int ObDMLSqlSplicer::splice_column_names(common::ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid column count", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_column(",", ColSet::ALL, ValSet::ONLY_COL_NAME, sql))) {
    LOG_WARN("add column names failed", K(ret));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_values(ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid column count", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_column(", ", ColSet::ALL, ValSet::ONLY_VALUE, sql))) {
    LOG_WARN("add values failed", K(ret));
  }
  return ret;
}

// "pk1=v1, pk2=v2, c3=v3, c4=v4"
int ObDMLSqlSplicer::splice_assignments(common::ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid column count", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_column(", ", ColSet::ALL, ValSet::ALL, sql))) {
    LOG_WARN("add values failed", K(ret));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_predicates(ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid column count", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_column(" AND ", ColSet::ALL, ValSet::ALL, sql))) {
    LOG_WARN("add values failed", K(ret));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_update_sql(const char *table_name, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET ", table_name))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(splice_column(", ", ColSet::FILTER_PK, ValSet::ALL, sql))) {
    LOG_WARN("splice set columns failed", K(ret));
  } else if (OB_FAIL(sql.append(" WHERE "))) {
    LOG_WARN("append sql failed", K(ret));
  } else if (OB_FAIL(splice_column(" AND ", ColSet::ONLY_PK, ValSet::ALL, sql))) {
    LOG_WARN("splice where condition failed", K(ret));
  } else {
    if (!extra_condition_.empty()) {
      if (OB_FAIL(sql.append_fmt(" AND %.*s",
          static_cast<int32_t>(extra_condition_.length()), extra_condition_.ptr()))) {
        LOG_WARN("add extra condition failed", K(ret), K_(extra_condition));
      }
    }
  }
  return ret;
}

int ObPTSqlSplicer::splice_batch_insert_update_replica_sql(
    const char *table_name,
    const bool with_role,
    ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  ObArray<ObString> all_names;
  ObArray<int64_t> rows_matrix;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_batch_insert(table_name, "INSERT", sql, all_names, rows_matrix))) {
    LOG_WARN("splice insert sql failed", K(ret), K(table_name));
  } else if (OB_FAIL(sql.append(" ON DUPLICATE KEY UPDATE "))) {
    LOG_WARN("append sql failed", K(ret));
  } else if (OB_FAIL(splice_batch_insert_update_replica_column(
          with_role, ObString::make_string(","), all_names, sql))) {
    LOG_WARN("failed to splice on dup", K(ret));
  }
  return ret;
}

int ObPTSqlSplicer::splice_insert_update_replica_sql(const char *table_name, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_insert_sql(table_name, sql))) {
    LOG_WARN("splice insert sql failed", K(ret), K(table_name));
  } else if (OB_FAIL(sql.append(" ON DUPLICATE KEY UPDATE "))) {
    LOG_WARN("append sql failed", K(ret));
  } else if (OB_FAIL(splice_insert_update_replica_column(", ", sql))) {
    LOG_WARN("splice column failed", K(ret));
  }
  return ret;
}

int ObPTSqlSplicer::splice_batch_insert_update_replica_column(
    const bool with_role,
    const ObString &sep,
    const ObIArray<ObString> &names,
    ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  int64_t N = names.count();
  bool first = true;
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    const ObString &name = names.at(i);
    if (0 == name.case_compare("tenant_id")
        || 0 == name.case_compare("table_id")
        || 0 == name.case_compare("partition_id")
        || 0 == name.case_compare("svr_ip")
        || 0 == name.case_compare("svr_port")
        || 0 == name.case_compare("unit_id")
        || (0 == name.case_compare("role") && !with_role)
        || 0 == name.case_compare("is_previous_leader")) {
      // Skip primary_key, unit_id, is_previous_leader, and keep it consistent with the original batch report logic
    } else {
      if (!first && OB_FAIL(sql.append(sep))) {
        LOG_WARN("failed to append sep", K(ret));
      } else if (OB_FAIL(sql.append_fmt("%.*s=VALUES(%.*s)", name.length(), name.ptr(), name.length(), name.ptr()))) {
        LOG_WARN("failed to append str", K(ret));
      }
      first = false;
    }
  } // end for
  return ret;
}

int ObPTSqlSplicer::splice_insert_update_replica_column(const char *sep, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  bool first = true;
  int64_t start_pos = 0;
  if (NULL == sep) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sep));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
    const Column &col = columns_.at(i);
    if (col.primary_key_ ||
        0 == strncmp(col.name_, "unit_id", strlen("unit_id")) ||
        0 == strncmp(col.name_, "is_previous_leader", strlen("is_previous_leader"))) {
      //do nothing
    } else {
      if (!first) {
        if (OB_FAIL(sql.append(sep))) {
          LOG_WARN("append sql failed");
        }
      }
      if (OB_SUCC(ret)) {
        if (col.is_null_) {
          ret = sql.append_fmt("%s = NULL", col.name_);
        } else {
          ret = sql.append_fmt("%s = %.*s", col.name_,
              static_cast<int32_t>(col.value_end_pos_ - start_pos),
              values_.ptr() + start_pos);
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("append column failed", K(ret));
        }
      }
      first = false;
    }
    start_pos = col.value_end_pos_;
  }
  return ret;
}

int ObDMLSqlSplicer::splice_select_1_sql(const char *table_name, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(sql.assign_fmt("SELECT 1 FROM %s WHERE ", table_name))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(splice_column(" AND ", ColSet::ONLY_PK, ValSet::ALL, sql))) {
    LOG_WARN("splice column failed", K(ret));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_core_cells(ObCoreTableStoreCell &kv_proxy,
      common::ObIArray<ObCoreTableProxy::UpdateCell> &cells)
{
  int ret = OB_SUCCESS;
  cells.reuse();
  int64_t start_pos = 0;
  ObCoreTableProxy::Cell cell;
  ObCoreTableProxy::UpdateCell ucell;
  if (columns_.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column count", K(ret), "column_count", columns_.count());
  }

  FOREACH_X(col, columns_, OB_SUCCESS == ret) {
    ucell.is_filter_cell_ = col->primary_key_;
    cell.name_ = ObString::make_string(col->name_);
    if (col->is_null_) {
      cell.value_.reset();
    } else {
      cell.value_ = ObString(0, static_cast<int32_t>(col->value_end_pos_ - start_pos),
          values_.ptr() + start_pos);
    }
    cell.is_hex_value_ = col->is_hex_value_;
    if (OB_FAIL(kv_proxy.store_cell(cell, ucell.cell_))) {
      LOG_WARN("store cell failed");
    } else if (OB_FAIL(cells.push_back(ucell))) {
      LOG_WARN("add update cell failed", K(ret), K(ucell));
    }
    start_pos = col->value_end_pos_;
  }
  return ret;
}
//////////////// for batch DMLs
int ObDMLSqlSplicer::finish_row()
{
  int ret = OB_SUCCESS;
  int64_t N = rows_end_pos_.count();
  int64_t last_pos = columns_.count() - 1;
  if (0 >= N) {
    if (last_pos < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("no cells in the row", K(ret), K(last_pos));
    } else if (OB_FAIL(rows_end_pos_.push_back(last_pos))) {
      LOG_WARN("failed to push back", K(ret), K(last_pos));
    }
  } else {
    int64_t last_row_end_pos = rows_end_pos_.at(N-1);
    if (last_pos <= last_row_end_pos) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("no cells in the row", K(ret), K(last_pos), K(last_row_end_pos));
    } else if (OB_FAIL(rows_end_pos_.push_back(last_pos))) {
      LOG_WARN("failed to push back", K(ret), K(last_pos), K(last_row_end_pos));
    }
  }
  LOG_DEBUG("end of row", K(ret), K(N), K(last_pos));
  return ret;
}

int ObDMLSqlSplicer::build_rows_matrix(ObIArray<ObString> &all_names, ObIArray<int64_t> &rows_matrix) const
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<ObString, int64_t, hash::NoPthreadDefendMode> name_idx_map;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(name_idx_map.create(512, ObModIds::OB_HASH_BUCKET))) {
      LOG_WARN("failed to init hashmap", K(ret));
    }
  }

  ObArray<int64_t> name_idx_array;
  int64_t cell_count = columns_.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < cell_count; ++i)
  {
    const Column &col = columns_.at(i);
    ObString cname = ObString::make_string(col.name_);
    int64_t name_idx = 0;
    if (OB_FAIL(name_idx_map.get_refactored(cname, name_idx))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        // found a new column name
        if (OB_FAIL(all_names.push_back(cname))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(name_idx_map.set_refactored(cname, all_names.count()-1))) {
          LOG_WARN("failed to set hashmap", K(ret));
        } else {
          name_idx = all_names.count()-1;
        }
      } else {
        LOG_WARN("failed to get name from map", K(ret));
      }
    } else {
      // column name already exists
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(name_idx_array.push_back(name_idx))) {
        LOG_WARN("failed to push back name idx", K(ret));
      }
    }
  } // end for
  ObArray<int64_t> name_idx_to_pos;
  int64_t row_count = rows_end_pos_.count();
  if (FAILEDx(name_idx_to_pos.prepare_allocate(all_names.count()))) {
    LOG_WARN("failed to prepare_allocate", KR(ret), K(all_names.count()));
  } else if (OB_FAIL(rows_matrix.reserve(row_count * all_names.count()))) {
    LOG_WARN("failed to reserve rows_matrix", KR(ret), K(row_count), K(all_names.count()));
  }
  int64_t last_pos = 0;
  for (int64_t i = 0; OB_SUCCESS == ret && i < row_count; ++i)   // for each row
  {
    int64_t row_end_pos = rows_end_pos_.at(i);
    if (row_end_pos >= columns_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("index out of range", K(ret), K(row_end_pos), "count", columns_.count());
    }
    for (int64_t j = 0; j < name_idx_to_pos.count(); j++) {
      name_idx_to_pos[j] = -1;
    }
    for (int64_t pos = last_pos; OB_SUCCESS == ret && pos <= row_end_pos; ++pos) {
      // build map: name_idx -> pos
      if (OB_UNLIKELY(pos >= name_idx_array.count() || name_idx_array[pos] >= name_idx_to_pos.count()
          || name_idx_array[pos] < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index out of range", KR(ret), K(pos));
      } else {
        name_idx_to_pos[name_idx_array[pos]] = pos;
      }
    }
    last_pos = row_end_pos + 1;
    // build row
    int64_t column_count = all_names.count();
    for (int64_t j = 0; OB_SUCCESS == ret && j < column_count; ++j)
    {
      if (OB_UNLIKELY(j >= name_idx_to_pos.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index out of range", KR(ret), K(j), "count", name_idx_to_pos.count(), K(column_count));
      } else if (OB_FAIL(rows_matrix.push_back(name_idx_to_pos[j]))) {
        LOG_WARN("failed to push back to rows matrix", K(ret));
      }
    } // end for
  } // end for
#ifndef NDEBUG
  // sanity check
  if (OB_SUCC(ret)) {
    int64_t column_count = all_names.count();
    int64_t row_count = rows_end_pos_.count();
    int64_t matrix_size = rows_matrix.count();
    if (matrix_size != row_count * column_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("wrong matrix size", K(matrix_size), K(column_count), K(row_count));
    } else {
      for (int64_t i = 0; OB_SUCCESS == ret && i < row_count; ++i)
      {
        for (int64_t j = 0; OB_SUCCESS == ret && j < column_count; ++j)
        {
          int64_t cell_pos = rows_matrix.at(j+i*column_count);
          if (-1 != cell_pos) {
            if (cell_pos >= columns_.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("cell pos out of range", K(cell_pos));
            } else if (all_names.at(j) != ObString::make_string(columns_.at(cell_pos).name_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("cell name not match", K(i), K(j), K(cell_pos));
            }
          }
        } // end for
      } // end for
    }
  }
#endif
  return ret;
}

static int join_strings(const ObString &sep, const ObIArray<ObString> &names, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t N = names.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    if (0 != i) {
      if (OB_FAIL(sql.append(sep))) {
        LOG_WARN("failed to append sep", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append(names.at(i)))) {
        LOG_WARN("failed to append str", K(ret));
      }
    }
  } // end for
  return ret;
}

// add single row to sql
// (1, 2, 3)
int ObDMLSqlSplicer::construct_insert_row(const common::ObArray<common::ObString> &all_names,
    const int64_t row_id, const common::ObArray<int64_t> &rows_matrix, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  const int64_t column_count = all_names.count();
  const int64_t matrix_size = rows_matrix.count();
  const int64_t begin_idx = row_id * column_count;
  if (begin_idx >= matrix_size || begin_idx + column_count > matrix_size || begin_idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index out of bound", KR(ret), K(begin_idx), K(column_count), K(matrix_size));
  } else if (OB_FAIL(sql.append("("))) {
    LOG_WARN("failed to append", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; i++) {
      int64_t cell_pos = rows_matrix.at(i + begin_idx);
      if (i != 0 && OB_FAIL(sql.append(", "))) {
        LOG_WARN("failed to append", KR(ret));
      } else if (cell_pos != -1) {
        if (cell_pos >= columns_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("cell pos out of range", K(cell_pos));
        } else if (all_names.at(i) != ObString::make_string(columns_.at(cell_pos).name_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("cell name not match", K(row_id), K(i), K(cell_pos));
        } else {
          const Column &col = columns_.at(cell_pos);
          int64_t start_pos = 0;
          if (cell_pos >= 1) {
            start_pos = columns_.at(cell_pos - 1).value_end_pos_;
          }
          if (col.is_null_) {
            if (OB_FAIL(sql.append("NULL"))) {
              LOG_WARN("failed to append NULL", KR(ret));
            }
          } else {
            if (OB_FAIL(sql.append(ObString(col.value_end_pos_ - start_pos, values_.ptr() + start_pos)))) {
              LOG_WARN("failed to append value", KR(ret));
            }
          }
        }
      } else if (OB_FAIL(sql.append("NULL"))) {
        LOG_WARN("failed to append NULL", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!default_column_value_.empty() && OB_FAIL(sql.append_fmt(", %s", default_column_value_.ptr()))) {
      LOG_WARN("failed to append default_column_value_", KR(ret), K(default_column_value_));
    } else if (OB_FAIL(sql.append(")"))) {
      LOG_WARN("failed to append", KR(ret));
    }
  }
  return ret;
}

// [(1,2,3), (4,5,6)], [(7,8,9), (10,11,12)], [(13,14,15)]
int ObDMLSqlSplicer::splice_rows_matrix_in_array(const common::ObArray<common::ObString> &all_names,
    const common::ObArray<int64_t> &rows_matrix, const int64_t rows_in_sql,
    const int64_t max_sql_length, const ObSqlString &header, ObIArray<common::ObSqlString> &sqls) const
{
  int ret = OB_SUCCESS;
  const int64_t column_count = all_names.count();
  const int64_t row_count = rows_end_pos_.count();
  const int64_t matrix_size = rows_matrix.count();
  sqls.reset();
  if (matrix_size != row_count * column_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong matrix size", K(matrix_size), K(column_count), K(row_count));
  } else {
    ObSqlString sql;
    ObSqlString tmp_sql;
    int64_t current_row_count = 0;
    for (int64_t i = 0; i < row_count && OB_SUCC(ret); i++) {
      tmp_sql.reuse();
      if (0 == current_row_count && OB_FAIL(sql.assign(header))) {
        LOG_WARN("failed to assign header", KR(ret), K(header));
      } else if (OB_FAIL(construct_insert_row(all_names, i, rows_matrix, tmp_sql))) {
        LOG_WARN("failed to construct_insert_row", KR(ret), K(i));
      } else {
        int64_t next_sql_length = tmp_sql.length() + 1 /*length of ","*/ + sql.length();
        if (next_sql_length <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sql length overflow", KR(ret), K(next_sql_length), K(tmp_sql), K(sql));
        } else if (current_row_count != 0 && next_sql_length > max_sql_length) {
          // sql length exceed, append sql to sqls and reset
          if (OB_FAIL(sqls.push_back(sql))) {
            LOG_WARN("failed to push_back sql", KR(ret));
          } else if (OB_FAIL(sql.assign(header))) {
            LOG_WARN("failed to assign header", KR(ret), K(header));
          } else {
            current_row_count = 0;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (current_row_count != 0 && OB_FAIL(sql.append(","))) {
          LOG_WARN("append sql failed", K(ret));
        } else if (OB_FAIL(sql.append(tmp_sql.string()))) {
          LOG_WARN("failed to append tmp_sql", KR(ret), K(tmp_sql));
        } else {
          current_row_count++;
          if (current_row_count == rows_in_sql) {
            if (OB_FAIL(sqls.push_back(sql))) {
              LOG_WARN("failed to push_back sql", KR(ret));
            } else {
              current_row_count = 0;
            }
          }
        }
      }
    }
    if (current_row_count != 0) {
      if (FAILEDx(sqls.push_back(sql))) {
        LOG_WARN("failed to push_back sql", KR(ret));
      }
    }
  }
  return ret;
}

// (1,2,3), (4,5,6)
int ObDMLSqlSplicer::splice_rows_matrix(const ObArray<ObString> &all_names,
    const common::ObSqlString &sql_header,
    const ObArray<int64_t> &rows_matrix,
    ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  ObArray<ObSqlString> sqls;
  const int64_t row_count = rows_end_pos_.count();
  if (OB_FAIL(splice_rows_matrix_in_array(all_names, rows_matrix, row_count,
          INT64_MAX/*max_sql_length*/, sql_header, sqls))) {
    LOG_WARN("failed to splice rows matrix in array", KR(ret), K(all_names), K(row_count),
        K(sql_header), K(rows_matrix));
  } else if (sqls.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sqls count not match", KR(ret), K(sqls.count()));
  } else if (OB_FAIL(sql.assign(sqls.at(0).string()))) {
    LOG_WARN("failed to assign sql", KR(ret));
  }
  return ret;
}

int ObDMLSqlSplicer::set_default_columns(const char *header, const char *value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(default_column_header_.assign(header))) {
    LOG_WARN("failed to assign default column header", KR(ret), K(header));
  } else if (OB_FAIL(default_column_value_.assign(value))) {
    LOG_WARN("failed to assign default column value", KR(ret), K(value));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_batch_insert_header(const char *table_name, const char *head,
    const ObArray<ObString> &all_names, ObSqlString &sql_header) const
{
  int ret = OB_SUCCESS;
  if (NULL == table_name || NULL == head) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name), KP(head));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(sql_header.assign_fmt("%s INTO %s (", head, table_name))) {
    LOG_WARN("assign sql_header failed", K(ret));
  } else if (OB_FAIL(join_strings(", ", all_names, sql_header))) {
    LOG_WARN("add column name failed", K(ret));
  } else if (!default_column_header_.empty() && OB_FAIL(sql_header.append_fmt(", %s", default_column_header_.ptr()))) {
    LOG_WARN("failed to append default_column_header_", KR(ret), K(default_column_header_));
  } else if (OB_FAIL(sql_header.append(") VALUES "))) {
    LOG_WARN("append sql_header failed", K(ret));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_batch_insert(const char *table_name, const char *head, ObSqlString &sql,
                                         ObArray<ObString> &all_names, ObArray<int64_t> &rows_matrix) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql_header;
  if (NULL == table_name || NULL == head) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name), KP(head));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(build_rows_matrix(all_names, rows_matrix))) {
    LOG_WARN("failed to build matrix", K(ret));
  } else if (OB_FAIL(splice_batch_insert_header(table_name, head, all_names, sql_header))) {
    LOG_WARN("failed to splice batch insert header", KR(ret), K(table_name), K(head));
  } else if (OB_FAIL(splice_rows_matrix(all_names, sql_header, rows_matrix, sql))) {
    LOG_WARN("failed to splice row matrix", K(ret));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_batch_insert_in_array(const char *table_name,
    common::ObIArray<common::ObSqlString> &sqls, const char *insert_header) const
{
  int ret = OB_SUCCESS;
  ObArray<ObString> all_names;
  ObArray<int64_t> rows_matrix;
  ObSqlString header;
  if (OB_ISNULL(table_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(table_name));
  } else if (OB_FAIL(build_rows_matrix(all_names, rows_matrix))) {
    LOG_WARN("failed to build rows matrix", KR(ret));
  } else if (OB_FAIL(splice_batch_insert_header(table_name, insert_header, all_names, header))) {
    LOG_WARN("failed to splice batch insert header", KR(ret), KP(table_name));
  } else if (OB_FAIL(splice_rows_matrix_in_array(all_names, rows_matrix, get_max_dml_num(), OB_MAX_SQL_LENGTH, header, sqls))) {
    LOG_WARN("failed to splice rows matrix in array", KR(ret), K(all_names), K(header));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_batch_insert_sql(const char *table_name, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  ObArray<ObString> all_names;
  ObArray<int64_t> rows_matrix;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_batch_insert(table_name, "INSERT", sql, all_names, rows_matrix))) {
    LOG_WARN("splice insert failed", K(ret), K(table_name));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_batch_insert_ignore_sql(const char *table_name, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  ObArray<ObString> all_names;
  ObArray<int64_t> rows_matrix;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_batch_insert(table_name, "INSERT IGNORE", sql, all_names, rows_matrix))) {
    LOG_WARN("splice insert failed", K(ret), K(table_name));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_batch_replace_sql_without_plancache(const char *table_name, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  ObArray<ObString> all_names;
  ObArray<int64_t> rows_matrix;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_batch_insert(table_name, "REPLACE /*+use_plan_cache(none)*/", sql, all_names, rows_matrix))) {
    LOG_WARN("splice insert failed", K(ret), K(table_name));
  }
  return ret;
}

int ObDMLSqlSplicer::splice_batch_replace_sql(const char *table_name, ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  ObArray<ObString> all_names;
  ObArray<int64_t> rows_matrix;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_batch_insert(table_name, "REPLACE", sql, all_names, rows_matrix))) {
    LOG_WARN("splice insert failed", K(ret), K(table_name));
  }
  return ret;
}

// @todo optimize, we could only have non-primary columns here
static int splice_on_duplicate_key_update(const ObString &sep, const ObIArray<ObString> &names, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t N = names.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    if (0 != i) {
      if (OB_FAIL(sql.append(sep))) {
        LOG_WARN("failed to append sep", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const ObString &name = names.at(i);
      if (OB_FAIL(sql.append_fmt("%.*s=VALUES(%.*s)", name.length(), name.ptr(), name.length(), name.ptr()))) {
        LOG_WARN("failed to append str", K(ret));
      }
    }
  } // end for
  return ret;
}

int ObDMLSqlSplicer::splice_batch_insert_update_sql(const char *table_name, common::ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  ObArray<ObString> all_names;
  ObArray<int64_t> rows_matrix;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(splice_batch_insert(table_name, "INSERT", sql, all_names, rows_matrix))) {
    LOG_WARN("splice insert sql failed", K(ret), K(table_name));
  } else if (OB_FAIL(sql.append(" ON DUPLICATE KEY UPDATE "))) {
    LOG_WARN("append sql failed", K(ret));
  } else if (OB_FAIL(splice_on_duplicate_key_update(ObString::make_string(","), all_names, sql))) {
    LOG_WARN("failed to splice on dup", K(ret));
  }
  return ret;
}

// (c1, c2, c3) IN ((1, 2, 3), (4, 5, 6))
int ObDMLSqlSplicer::splice_batch_predicates(const ObArray<ObString> &all_names, const ObArray<int64_t> &rows_matrix, common::ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  int64_t column_count = all_names.count();
  // part1: (c1, c2, c3)
  if (OB_FAIL(sql.append("("))) {
    LOG_WARN("append sql failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCCESS == ret && i < column_count; ++i) {
    if (0 != i) {
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append(","))) {
          LOG_WARN("append sql failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      const ObString &name = all_names.at(i);
      if (OB_FAIL(sql.append_fmt("%.*s", name.length(), name.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }  // end for
  if (OB_SUCC(ret)) {
    ObSqlString sql_result;
    if (OB_FAIL(sql.append(") IN ("))) {
      LOG_WARN("append sql failed", K(ret));
    }
    // part2: (1, 2, 3), (4, 5, 6)
    else if (OB_FAIL(splice_rows_matrix(all_names, sql, rows_matrix, sql_result))) {
      LOG_WARN("failed to splice row matrix", K(ret));
    } else if (OB_FAIL(sql_result.append(")"))) {
      LOG_WARN("append sql failed", K(ret));
    } else if (OB_FAIL(sql.assign(sql_result))) {
      LOG_WARN("failed to assign sql_result", KR(ret), K(sql_result), K(sql));
    }
  }
  return ret;
}

int ObDMLSqlSplicer::splice_batch_delete_sql(const char *table_name, common::ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  ObArray<ObString> all_names;
  ObArray<int64_t> rows_matrix;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(build_rows_matrix(all_names, rows_matrix))) {
    LOG_WARN("failed to build matrix", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE ", table_name))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(splice_batch_predicates(all_names, rows_matrix, sql))) {
    LOG_WARN("failed to splice batch predicates", K(ret));
  } else if (!extra_condition_.empty()) {
    if (OB_FAIL(sql.append_fmt(" AND %.*s",
                               static_cast<int32_t>(extra_condition_.length()), extra_condition_.ptr()))) {
      LOG_WARN("add extra condition failed", K(ret), K_(extra_condition));
    }
  }
  return ret;
}

int ObDMLSqlSplicer::splice_batch_predicates_sql(common::ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  ObArray<ObString> all_names;
  ObArray<int64_t> rows_matrix;
  if (columns_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_count is invalid", K(ret), "column_count", columns_.count());
  } else if (OB_FAIL(build_rows_matrix(all_names, rows_matrix))) {
    LOG_WARN("failed to build matrix", K(ret));
  } else if (OB_FAIL(splice_batch_predicates(all_names, rows_matrix, sql))) {
    LOG_WARN("failed to splice batch predicates", K(ret));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
#define DEF_DML_EXECUTE(dml) \
  int ObDMLExecHelper::exec_##dml(const char *table_name,                            \
      const ObDMLSqlSplicer &splicer, int64_t &affected_rows)                        \
  {                                                                                  \
    int ret = OB_SUCCESS;                                                            \
    ObSqlString sql;                                                                 \
    if (NULL == table_name) {                                                        \
      ret = OB_INVALID_ARGUMENT;                                                     \
      LOG_WARN("invalid argument", K(ret), KP(table_name));                          \
    } else if (OB_FAIL(splicer.splice_##dml##_sql(table_name, sql))) {               \
      LOG_WARN("splice sql failed", K(ret));                                         \
    } else if (OB_FAIL(sql_client_.write(tenant_id_, sql.ptr(), affected_rows))) {   \
      LOG_WARN("execute sql failed", K(ret), K(sql));                                \
    }  \
    return ret;                                                                      \
  }

DEF_DML_EXECUTE(insert);
DEF_DML_EXECUTE(update);
DEF_DML_EXECUTE(delete);
DEF_DML_EXECUTE(replace);
DEF_DML_EXECUTE(insert_update);

#undef DEF_DML_EXECUTE

int ObDMLExecHelper::exec_batch_insert(const char *table_name,
      const ObDMLSqlSplicer &splicer, int64_t &affected_rows, const char *insert_header)
{
  int ret = OB_SUCCESS;
  ObArray<ObSqlString> sqls;
  affected_rows = 0;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (OB_FAIL(splicer.splice_batch_insert_in_array(table_name, sqls, insert_header))) {
    LOG_WARN("splice sql failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sqls.count(); i++) {
      int64_t tmp_affected_rows = 0;
      if (OB_FAIL(sql_client_.write(tenant_id_, sqls.at(i).ptr(), tmp_affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(sqls.at(i)));
      } else {
        affected_rows += tmp_affected_rows;
      }
    }
  }
  return ret;
}

int ObDMLExecHelper::exec_insert_ignore(const char *table_name,
    const ObDMLSqlSplicer &splicer, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  ObSqlString sql;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (OB_FAIL(check_row_exist(table_name, splicer, exist))) {
    LOG_WARN("check_row_exist failed", K(table_name), K(ret));
  } else {
    sql.reset();
    if (exist) {
      // do nothing
    } else {
      if (OB_FAIL(splicer.splice_insert_sql(table_name, sql))) {
        LOG_WARN("splice sql failed", K(ret), K(exist));
      } else if (OB_FAIL(sql_client_.write(tenant_id_, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      }
    }
  }
  return ret;
}

int ObDMLExecHelper::check_row_exist(const char *table_name,
    const ObDMLSqlSplicer &splicer, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  ObSqlString sql;
  if (NULL == table_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_name));
  } else if (OB_FAIL(splicer.splice_select_1_sql(table_name, sql))) {
    LOG_WARN("splice select sql failed", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(sql_client_.read(res, tenant_id_, sql.ptr()))) {
        LOG_WARN("execute query failed", K(ret), K(sql));
      } else {
        if (res.get_result() != NULL && OB_SUCCESS == (ret = res.get_result()->next())) {
          exist = true;
        }
        if (OB_FAIL(ret)) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            exist = false;
          } else {
            LOG_WARN("next failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDMLSqlSplicer::add_long_double_column(const char *col_name, const double value)
{
  int ret = OB_SUCCESS;
  const bool is_primary_key = false;
  const bool is_null = false;
  if (OB_ISNULL(col_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column name", K(ret), KP(col_name));
  } else if (OB_FAIL(values_.append_fmt("%.17g", value))) {
    LOG_WARN("append value failed", K(ret));
  } else if (OB_FAIL(add_column(is_primary_key, is_null, col_name))) {
    LOG_WARN("add column failed", K(ret), K(is_primary_key), K(is_null), K(col_name));
  }
  return ret;
}

int ObDMLSqlSplicer::add_function_call(const char *col_name, const char *func_call)
{
  int ret = OB_SUCCESS;
  const bool is_primary_key = false;
  const bool is_null = false;
  if (OB_ISNULL(col_name) || OB_ISNULL(func_call)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column name", K(ret), KP(col_name));
  } else if (OB_FAIL(values_.append(func_call))) {
    LOG_WARN("append value failed", K(ret));
  } else if (OB_FAIL(add_column(is_primary_key, is_null, col_name))) {
    LOG_WARN("add column failed", K(ret), K(is_primary_key), K(is_null), K(col_name));
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
