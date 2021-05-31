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
#include "lib/string/ob_string.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_prepared_param.h"
#include "lib/mysqlclient/ob_mysql_prepared_result.h"
#include "lib/mysqlclient/ob_mysql_prepared_statement.h"

namespace oceanbase {
namespace common {
namespace sqlclient {
ObMySQLPreparedStatement::ObMySQLPreparedStatement()
    : conn_(NULL),
      arena_allocator_(ObModIds::MYSQL_CLIENT_CACHE),
      alloc_(&arena_allocator_),
      param_(*this),
      result_(*this),
      stmt_param_count_(0),
      stmt_(NULL)
{}

ObMySQLPreparedStatement::~ObMySQLPreparedStatement()
{}

ObIAllocator& ObMySQLPreparedStatement::get_allocator()
{
  return *alloc_;
}

MYSQL_STMT* ObMySQLPreparedStatement::get_stmt_handler()
{
  return stmt_;
}

MYSQL* ObMySQLPreparedStatement::get_conn_handler()
{
  return conn_->get_handler();
}

ObMySQLConnection* ObMySQLPreparedStatement::get_connection()
{
  return conn_;
}

int ObMySQLPreparedStatement::init(ObMySQLConnection& conn, const char* sql)
{
  int ret = OB_SUCCESS;
  // will be used by param_ and result_
  conn_ = &conn;

  if (OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", K(sql), K(ret));
  } else if (OB_ISNULL(stmt_ = mysql_stmt_init(conn_->get_handler()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to init stmt", K(ret));
  } else if (0 != mysql_stmt_prepare(stmt_, sql, STRLEN(sql))) {
    ret = -mysql_errno(conn_->get_handler());
    LOG_WARN("fail to prepare stmt", "info", mysql_error(conn_->get_handler()), K(ret));
  } else if (OB_FAIL(param_.init())) {
    LOG_WARN("fail to init prepared result", K(ret));
  } else if (OB_FAIL(result_.init())) {
    LOG_WARN("fail to init prepared result", K(ret));
  } else {
    LOG_INFO("conn_handler", "handler", conn_->get_handler(), K_(stmt));
  }
  return ret;
}

int ObMySQLPreparedStatement::close()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stmt handler is null", K(ret));
  } else if (0 != mysql_stmt_close(stmt_)) {
    ret = -mysql_errno(conn_->get_handler());
    LOG_WARN("fail to close stmt", "info", mysql_error(conn_->get_handler()), K(ret));
  }
  stmt_param_count_ = 0;
  return ret;
}

int ObMySQLPreparedStatement::execute_update()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stmt handler is null", K(ret));
  } else if (OB_FAIL(param_.bind_param())) {
    LOG_WARN("fail to bind prepared input param", K(ret));
  } else if (0 != mysql_stmt_execute(stmt_)) {
    ret = -mysql_stmt_errno(stmt_);
    LOG_WARN("fail to execute stmt", "info", mysql_stmt_error(stmt_), K(ret));
  }
  return ret;
}

ObMySQLPreparedResult* ObMySQLPreparedStatement::execute_query()
{
  ObMySQLPreparedResult* result = NULL;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stmt handler is null", K(ret));
  } else if (OB_FAIL(param_.bind_param())) {
    LOG_WARN("fail to bind prepared input param", K(ret));
  } else if (OB_FAIL(result_.bind_result_param())) {
    LOG_WARN("bind result param fail", K(ret));
  } else if (0 != mysql_stmt_execute(stmt_)) {
    ret = -mysql_stmt_errno(stmt_);
    LOG_WARN("fail to execute stmt", "info", mysql_stmt_error(stmt_), K(ret));
  } else if (0 != mysql_stmt_store_result(stmt_)) {
    ret = -mysql_stmt_errno(stmt_);
    LOG_WARN("fail to store prepared result", "info", mysql_stmt_error(stmt_), K(ret));
  } else {
    result = &result_;
  }
  conn_->set_last_error(ret);
  return result;
}

int ObMySQLPreparedStatement::set_int(const int64_t col_idx, const int64_t int_val)
{
  // TODO
  UNUSED(col_idx);
  UNUSED(int_val);
  LOG_WARN("call not implemented function.");
  return OB_NOT_IMPLEMENT;
}

int ObMySQLPreparedStatement::set_varchar(const int64_t col_idx, const ObString& varchar_val)
{
  // TODO
  UNUSED(col_idx);
  UNUSED(varchar_val);
  LOG_WARN("call not implemented function.");
  return OB_NOT_IMPLEMENT;
}

int ObMySQLPreparedStatement::bind_param_int(const int64_t col_idx, int64_t* in_int_val)
{
  int ret = OB_SUCCESS;
  unsigned long res_len = 0;
  if (OB_FAIL(param_.bind_param(
          col_idx, MYSQL_TYPE_LONGLONG, reinterpret_cast<char*>(in_int_val), sizeof(int64_t), res_len))) {
    LOG_WARN("fail to bind int result", K(col_idx), K(ret));
  }
  return ret;
}

int ObMySQLPreparedStatement::bind_param_varchar(const int64_t col_idx, char* in_str_val, unsigned long& in_str_len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(param_.bind_param(col_idx, MYSQL_TYPE_VAR_STRING, in_str_val, 0, in_str_len))) {
    LOG_WARN("fail to bind int result", K(col_idx), K(ret));
  }
  return ret;
}

int ObMySQLPreparedStatement::bind_result_int(const int64_t col_idx, int64_t* out_buf)
{
  int ret = OB_SUCCESS;
  unsigned long res_len = 0;
  if (OB_FAIL(result_.bind_result(
          col_idx, MYSQL_TYPE_LONGLONG, reinterpret_cast<char*>(out_buf), sizeof(int64_t), res_len))) {
    LOG_WARN("fail to bind int result", K(col_idx), K(ret));
  }
  return ret;
}

int ObMySQLPreparedStatement::bind_result_varchar(
    const int64_t col_idx, char* out_buf, const int buf_len, unsigned long& res_len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(result_.bind_result(col_idx, MYSQL_TYPE_VAR_STRING, out_buf, buf_len, res_len))) {
    LOG_WARN("fail to bind int result", K(col_idx), K(ret));
  }
  return ret;
}

}  // namespace sqlclient
}  // end namespace common
}  // end namespace oceanbase
