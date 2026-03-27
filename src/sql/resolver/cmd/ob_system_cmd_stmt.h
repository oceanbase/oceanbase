/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_SYSTEM_CMD_STMT_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_SYSTEM_CMD_STMT_

#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/ob_cmd.h"
namespace oceanbase
{
namespace sql
{
class ObSystemCmdStmt : public ObStmt, public ObICmd
{
public:
  ObSystemCmdStmt(common::ObIAllocator* name_pool, stmt::StmtType type)
  : ObStmt(name_pool, type)
  {}
  explicit ObSystemCmdStmt(stmt::StmtType type) : ObStmt(type)
  {}
  virtual ~ObSystemCmdStmt() {}
  virtual int get_cmd_type() const { return get_stmt_type(); }
private:
  DISALLOW_COPY_AND_ASSIGN(ObSystemCmdStmt);
};
} // namespace sql
} // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_CMD_OB_SYSTEM_CMD_STMT_ */
