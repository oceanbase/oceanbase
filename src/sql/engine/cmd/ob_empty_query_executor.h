/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EMPTY_QUERY_EXECUTOR_H__
#define OCEANBASE_SQL_ENGINE_EMPTY_QUERY_EXECUTOR_H__

#include "share/ob_define.h"
namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObEmptyQueryStmt;

class ObEmptyQueryExecutor
{
public:
  ObEmptyQueryExecutor() {}
  virtual ~ObEmptyQueryExecutor() {}
  int execute(ObExecContext &ctx, ObEmptyQueryStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObEmptyQueryExecutor);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EMPTY_QUERY_EXECUTOR_H__ */
