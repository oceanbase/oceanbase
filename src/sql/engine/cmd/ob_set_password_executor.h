/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_SET_PASSWORD_EXECUTOR_
#define OCEANBASE_SQL_SET_PASSWORD_EXECUTOR_

namespace oceanbase
{
namespace common
{
class ObString;
class ObSqlString;
}
namespace obrpc
{
class ObCommonRpcProxy;
}

namespace sql
{
class ObExecContext;
class ObSetPasswordStmt;

class ObSetPasswordExecutor
{
public:
  ObSetPasswordExecutor();
  virtual ~ObSetPasswordExecutor();
  int execute(ObExecContext &ctx, ObSetPasswordStmt &stmt);
};

}
}
#endif /* __OB_SQL_SET_PASSWORD_EXECUTOR_H__ */
//// end of header file

