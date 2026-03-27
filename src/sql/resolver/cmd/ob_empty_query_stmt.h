/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_EMPTY_QUERY_STMT_H_
#define OCEANBASE_SQL_RESOLVER_EMPTY_QUERY_STMT_H_

#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/cmd/ob_cmd_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObEmptyQueryStmt : public ObCMDStmt
{
public:
  ObEmptyQueryStmt() : ObCMDStmt(stmt::T_EMPTY_QUERY)
  {
  }

  virtual ~ObEmptyQueryStmt()
  {
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObEmptyQueryStmt);
};

} // sql
} // oceanbase
#endif /*OCEANBASE_SQL_RESOLVER_EMPTY_QUERY_STMT_H_*/
