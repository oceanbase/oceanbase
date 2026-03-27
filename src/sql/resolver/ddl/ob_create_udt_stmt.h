/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_UDT_STMT_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_UDT_STMT_H_

#include "lib/allocator/ob_allocator.h"
#include "share/ob_rpc_struct.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObCreateUDTStmt : public ObDDLStmt
{
public:
  explicit ObCreateUDTStmt(common::ObIAllocator *name_pool)
  : ObDDLStmt(name_pool, stmt::T_CREATE_TYPE)
  {}
  ObCreateUDTStmt() : ObDDLStmt(stmt::T_CREATE_TYPE) {}
  virtual ~ObCreateUDTStmt() {}
  obrpc::ObCreateUDTArg &get_udt_arg() { return udt_arg_; }
  const obrpc::ObCreateUDTArg &get_udt_arg() const { return udt_arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return udt_arg_; }
  TO_STRING_KV(K_(udt_arg));
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateUDTStmt);
private:
  obrpc::ObCreateUDTArg udt_arg_;
};

}
}

#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_UDT_STMT_H_ */
