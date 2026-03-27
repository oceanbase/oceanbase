/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_PROCEDURE_STMT_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_PROCEDURE_STMT_H_

#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_table_schema.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObDropRoutineStmt : public ObDDLStmt
{
public:
  explicit ObDropRoutineStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_DROP_ROUTINE) {}
  ObDropRoutineStmt()
    : ObDDLStmt(stmt::T_DROP_ROUTINE) {}
  virtual ~ObDropRoutineStmt() {}

  obrpc::ObDropRoutineArg &get_routine_arg() { return routine_arg_; }
  const obrpc::ObDropRoutineArg &get_routine_arg() const { return routine_arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return routine_arg_; }
  TO_STRING_KV(K_(routine_arg));
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropRoutineStmt);
private:
  obrpc::ObDropRoutineArg routine_arg_;
};

}
}


#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_PROCEDURE_STMT_H_ */
