/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_TCL_OB_TCL_STMT_H_
#define OCEANBASE_SRC_SQL_RESOLVER_TCL_OB_TCL_STMT_H_ 1
#include "share/ob_rpc_struct.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/ob_cmd.h"
namespace oceanbase
{
namespace sql
{
class ObTCLStmt : public ObStmt, public ObICmd
{
public:
  ObTCLStmt(common::ObIAllocator *name_pool, stmt::StmtType type)
  : ObStmt(name_pool, type)
  {
  }
  explicit ObTCLStmt(stmt::StmtType type): ObStmt(type)
  {
  }
  virtual ~ObTCLStmt() {}
  virtual int get_cmd_type() const { return get_stmt_type(); }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTCLStmt);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_TCL_OB_TCL_STMT_H_ */
