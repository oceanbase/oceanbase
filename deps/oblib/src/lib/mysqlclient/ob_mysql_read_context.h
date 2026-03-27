/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_MYSQLCLIENT_OB_MYSQL_READ_CONTEXT_H_
#define OCEANBASE_MYSQLCLIENT_OB_MYSQL_READ_CONTEXT_H_

#include "lib/mysqlclient/ob_isql_result_handler.h"
#include "lib/mysqlclient/ob_mysql_statement.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObMySQLResult;

class ObMySQLReadContext : public ObISQLResultHandler
{
public:
  ObMySQLReadContext() : result_(NULL), stmt_() {}
  virtual ~ObMySQLReadContext() {}

  virtual ObMySQLResult *mysql_result() { return result_; }

public:
  ObMySQLResult *result_;
  ObMySQLStatement stmt_;
};

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_MYSQLCLIENT_OB_MYSQL_READ_CONTEXT_H_
