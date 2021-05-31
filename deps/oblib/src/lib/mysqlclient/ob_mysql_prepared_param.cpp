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
#include "lib/mysqlclient/ob_mysql_prepared_statement.h"
#include "lib/mysqlclient/ob_mysql_prepared_param.h"

namespace oceanbase {
namespace common {
namespace sqlclient {
ObMySQLPreparedParam::ObMySQLPreparedParam(ObMySQLPreparedStatement& stmt)
    : stmt_(stmt), alloc_(stmt.get_allocator()), param_count_(0), bind_(NULL)
{}

ObMySQLPreparedParam::~ObMySQLPreparedParam()
{}

int ObMySQLPreparedParam::init()
{
  int ret = OB_SUCCESS;
  MYSQL_STMT* stmt = NULL;
  if (OB_ISNULL(stmt = stmt_.get_stmt_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt handler is null", K(ret));
  } else if (FALSE_IT(param_count_ = static_cast<int>(mysql_stmt_param_count(stmt)))) {
    // impossible
  } else if (param_count_ < 0) {
    ret = OB_ERR_SQL_CLIENT;
  } else if (0 == param_count_) {
    // insert or replace that do not produce result sets
  } else if (OB_ISNULL(bind_ = reinterpret_cast<MYSQL_BIND*>(alloc_.alloc(sizeof(MYSQL_BIND) * param_count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("out of memory, alloc mem for mysql_bind error", K(ret));
  } else {
    LOG_DEBUG("statement field", K(param_count_));
  }
  return ret;
}

int ObMySQLPreparedParam::bind_param()
{
  int ret = OB_SUCCESS;
  MYSQL_STMT* stmt = NULL;
  if (OB_ISNULL(stmt = stmt_.get_stmt_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt handler is null", K(ret));
  } else if (OB_UNLIKELY(0 != mysql_stmt_bind_param(stmt, bind_))) {
    ret = OB_ERR_SQL_CLIENT;
    LOG_WARN("fail to bind param", K(ret));
  }
  return ret;
}

void ObMySQLPreparedParam::close()
{
  if (OB_LIKELY(NULL != bind_)) {
    alloc_.free(bind_);
    bind_ = NULL;
  }
}

int ObMySQLPreparedParam::bind_param(
    const int64_t col_idx, enum_field_types buffer_type, char* out_buf, const int64_t buf_len, unsigned long& res_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bind_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("result not init. call init() first", K(ret));
  } else if (OB_LIKELY(col_idx >= 0) && OB_LIKELY(col_idx < param_count_)) {
    bind_[col_idx].buffer_type = buffer_type;
    bind_[col_idx].buffer = out_buf;
    bind_[col_idx].buffer_length = buf_len;
    bind_[col_idx].is_null = NULL;
    bind_[col_idx].length = &res_len;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index", K(col_idx), K(param_count_));
  }
  return ret;
}

}  // end namespace sqlclient
}  // end namespace common
}  // end namespace oceanbase
