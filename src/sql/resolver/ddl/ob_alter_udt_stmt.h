/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_UDT_STMT_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_UDT_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObAlterUDTStmt : public ObDDLStmt
{
public:
  explicit ObAlterUDTStmt(common::ObIAllocator *name_pool)
      : ObDDLStmt(name_pool, stmt::T_ALTER_TYPE),
        alter_udt_arg_() {}
    ObAlterUDTStmt()
      : ObDDLStmt(stmt::T_ALTER_TYPE),
        alter_udt_arg_() {}
  virtual ~ObAlterUDTStmt() {}
  obrpc::ObAlterUDTArg &get_alter_udt_arg() { return alter_udt_arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return alter_udt_arg_; }
private:
  obrpc::ObAlterUDTArg alter_udt_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObAlterUDTStmt);
};
}
}

#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_UDT_STMT_H_ */
