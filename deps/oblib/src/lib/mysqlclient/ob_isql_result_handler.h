/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_MYSQLCLIENT_OB_ISQL_RESULT_HANDLER_H_
#define OCEANBASE_MYSQLCLIENT_OB_ISQL_RESULT_HANDLER_H_

namespace oceanbase
{
namespace common
{
namespace sqlclient
{

class ObMySQLResult;

class ObISQLResultHandler
{
public:
  ObISQLResultHandler() {}
  virtual ~ObISQLResultHandler() {}

  virtual ObMySQLResult *mysql_result() = 0;
};

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase


#endif // OCEANBASE_MYSQLCLIENT_OB_ISQL_RESULT_HANDLER_H_
