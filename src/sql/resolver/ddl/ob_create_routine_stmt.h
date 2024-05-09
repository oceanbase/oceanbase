/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_PROCEDURE_STMT_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_PROCEDURE_STMT_H_

#include "lib/allocator/ob_allocator.h"
#include "share/ob_rpc_struct.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObCreateRoutineStmt : public ObDDLStmt
{
public:
  explicit ObCreateRoutineStmt(common::ObIAllocator *name_pool)
      : ObDDLStmt(name_pool, stmt::T_CREATE_ROUTINE) {}
  ObCreateRoutineStmt() : ObDDLStmt(stmt::T_CREATE_ROUTINE) {}
  virtual ~ObCreateRoutineStmt() {}

  obrpc::ObCreateRoutineArg &get_routine_arg() { return routine_arg_; }
  const obrpc::ObCreateRoutineArg &get_routine_arg() const { return routine_arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return routine_arg_; }
  VIRTUAL_TO_STRING_KV(K_(routine_arg));

protected:
  explicit ObCreateRoutineStmt(common::ObIAllocator *name_pool, stmt::StmtType stmt_type)
      : ObDDLStmt(name_pool, stmt_type) {}
  explicit ObCreateRoutineStmt(stmt::StmtType stmt_type)
      : ObDDLStmt(stmt_type) {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateRoutineStmt);
  obrpc::ObCreateRoutineArg routine_arg_;
};

}
}

#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_PROCEDURE_STMT_H_ */
