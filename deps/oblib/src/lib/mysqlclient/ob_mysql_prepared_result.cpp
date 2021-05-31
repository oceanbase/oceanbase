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

#define USING_LOG_PREFIX LIB_MYSQLC
#include <mariadb/mysql.h>
#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/mysqlclient/ob_mysql_prepared_result.h"
#include "lib/mysqlclient/ob_mysql_prepared_statement.h"

namespace oceanbase {
namespace common {
namespace sqlclient {
ObMySQLPreparedResult::ObMySQLPreparedResult(ObMySQLPreparedStatement& stmt)
    : stmt_(stmt), alloc_(stmt.get_allocator()), result_column_count_(0), bind_(NULL)
{}

ObMySQLPreparedResult::~ObMySQLPreparedResult()
{}

int ObMySQLPreparedResult::init()
{
  int ret = OB_SUCCESS;
  MYSQL_STMT* stmt = NULL;
  if (OB_ISNULL(stmt = stmt_.get_stmt_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stmt handler is null", K(ret));
  } else if (FALSE_IT(result_column_count_ = mysql_stmt_field_count(stmt))) {
    // impossible
  } else if (result_column_count_ < 0) {
    ret = OB_ERR_SQL_CLIENT;
  } else if (result_column_count_ == 0) {
    // insert or replace that do not produce result sets
  } else if (OB_ISNULL(
                 bind_ = reinterpret_cast<MYSQL_BIND*>(alloc_.alloc(sizeof(MYSQL_BIND) * result_column_count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("out of memory, alloc mem for mysql bind error", K(ret));
  } else {
    // LOG_INFO("statemen field count=%d", result_column_count_);
  }
  return ret;
}

int ObMySQLPreparedResult::bind_result_param()
{
  int ret = OB_SUCCESS;
  MYSQL_STMT* stmt = NULL;
  if (OB_ISNULL(stmt = stmt_.get_stmt_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stmt handler is null", K(ret));
  } else if (OB_UNLIKELY(0 != mysql_stmt_bind_result(stmt, bind_))) {
    ret = OB_ERR_SQL_CLIENT;
    LOG_WARN("fail to bind result param", K(ret));
  }
  return ret;
}

void ObMySQLPreparedResult::close()
{
  result_column_count_ = 0;
  alloc_.free(bind_);
  bind_ = NULL;
}

int ObMySQLPreparedResult::next()
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;
  MYSQL_STMT* stmt = NULL;
  if (OB_ISNULL(stmt = stmt_.get_stmt_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stmt handler is null", K(ret));
  } else {
    LOG_INFO("mysql stmt", K(stmt));
    tmp_ret = mysql_stmt_fetch(stmt);
    if (OB_LIKELY(0 == tmp_ret)) {
      ret = OB_SUCCESS;
    } else if (MYSQL_NO_DATA == tmp_ret) {
      ret = OB_ITER_END;
    } else if (MYSQL_DATA_TRUNCATED == tmp_ret) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("SQL result truncated", K(ret));
    } else {
      ret = OB_ERR_SQL_CLIENT;
      LOG_WARN("fail to fetch next row", K(tmp_ret), K(ret));
    }
  }
  return ret;
}

int ObMySQLPreparedResult::bind_result(
    const int64_t col_idx, enum_field_types buffer_type, char* out_buf, const int64_t buf_len, unsigned long& res_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bind_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("result not init. call init() first", K(ret));
  } else if (OB_LIKELY(col_idx >= 0) && OB_LIKELY(col_idx < result_column_count_)) {
    bind_[col_idx].buffer_type = buffer_type;
    bind_[col_idx].buffer = out_buf;
    bind_[col_idx].buffer_length = buf_len;
    bind_[col_idx].is_null = NULL;
    bind_[col_idx].length = &res_len;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index", K(col_idx), K(result_column_count_));
  }
  return ret;
}

int ObMySQLPreparedResult::get_int(const int64_t col_idx, int64_t& int_val) const
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(col_idx >= 0) && OB_LIKELY(col_idx < result_column_count_)) {
    // currently not support auto type convertion
    if (MYSQL_TYPE_LONGLONG == bind_[col_idx].buffer_type) {
      int_val = *(reinterpret_cast<int64_t*>(bind_[col_idx].buffer));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid type, forget to call bind_type_and_buf() first",
          "real type",
          bind_[col_idx].buffer_type,
          "expected type",
          MYSQL_TYPE_LONGLONG,
          K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index", K(col_idx), K(result_column_count_));
  }
  return ret;
}

int ObMySQLPreparedResult::get_varchar(const int64_t col_idx, ObString& varchar_val) const
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(col_idx >= 0) && OB_LIKELY(col_idx < result_column_count_)) {
    // currently not support auto type convertion
    if (MYSQL_TYPE_VAR_STRING == bind_[col_idx].buffer_type) {
      varchar_val.assign(static_cast<char*>(bind_[col_idx].buffer), static_cast<int32_t>(*bind_[col_idx].length));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid type, forget to call bind_type_and_buf() first",
          "real type",
          bind_[col_idx].buffer_type,
          "expected type",
          MYSQL_TYPE_VAR_STRING,
          K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index", K(col_idx), K(result_column_count_));
  }
  return ret;
}

}  // end namespace sqlclient
}  // end namespace common
}  // end namespace oceanbase
