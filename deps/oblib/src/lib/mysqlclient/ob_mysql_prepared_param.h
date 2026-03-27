/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __OB_COMMON_SQLCLIENT_MYSQL_PREPARED_PARAM__
#define __OB_COMMON_SQLCLIENT_MYSQL_PREPARED_PARAM__

#include <mysql.h>
#include "lib/string/ob_string.h"
#include "lib/mysqlclient/ob_isql_connection.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
// #include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_result.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
namespace sqlclient
{
class ObBindParam;
class ObMySQLPreparedStatement;
class ObMySQLPreparedParam
{
friend ObMySQLPreparedStatement;
public:
  explicit ObMySQLPreparedParam(ObMySQLPreparedStatement &stmt);
  ~ObMySQLPreparedParam();
  int init();
  int bind_param();
  void close();
  int bind_param(ObBindParam &param);
  int64_t get_stmt_param_count() const { return param_count_; }

private:
  ObMySQLPreparedStatement &stmt_;
  common::ObIAllocator *alloc_;
  int64_t param_count_;
  MYSQL_BIND *bind_;
};
}
}
}
#endif
