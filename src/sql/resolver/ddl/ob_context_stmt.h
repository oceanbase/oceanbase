/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_RESOLVER_DDL_CONTEXT_STMT_H_
#define OCEANBASE_SQL_OB_RESOLVER_DDL_CONTEXT_STMT_H_

#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_create_context_resolver.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObContextDDLStmt : public ObDDLStmt
{
public:
  explicit ObContextDDLStmt(common::ObIAllocator *name_pool, stmt::StmtType type) :
      ObDDLStmt(name_pool, type),
      arg_()
  {
    arg_.set_stmt_type(type);
  }
  ObContextDDLStmt(stmt::StmtType type) :
      ObDDLStmt(type),
      arg_()
  {
    arg_.set_stmt_type(type);
  }
  virtual ~ObContextDDLStmt() = default;
  virtual void print(FILE *fp, int32_t level, int32_t index = 0)
  {
    UNUSED(index);
    UNUSED(fp);
    UNUSED(level);
  }
  void set_context_id(uint64_t context_id)
  {
    arg_.set_context_id(context_id);
  }
  void set_context_namespace(const common::ObString &ctx_namespace)
  {
    arg_.set_namespace(ctx_namespace);
  }
  void set_database_name(const common::ObString &db_name)
  {
    arg_.set_schema_name(db_name);
  }
  void set_tenant_id(uint64_t tenant_id)
  {
    arg_.set_tenant_id(tenant_id);
  }
  void set_context_type(ObContextType type)
  {
    arg_.set_context_type(type);
  }
  void set_package_name(const common::ObString &package_name)
  {
    arg_.set_package_name(package_name);
  }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_; }
  obrpc::ObContextDDLArg &get_arg() { return arg_; }
private:
  obrpc::ObContextDDLArg arg_;
  DISALLOW_COPY_AND_ASSIGN(ObContextDDLStmt);
};

class ObCreateContextStmt : public ObContextDDLStmt
{
public:
  explicit ObCreateContextStmt(common::ObIAllocator *name_pool) :
      ObContextDDLStmt(name_pool, stmt::T_CREATE_CONTEXT)
  {
  }
  ObCreateContextStmt() :
      ObContextDDLStmt(stmt::T_CREATE_CONTEXT)
  {
  }
  virtual ~ObCreateContextStmt() = default;
};

class ObDropContextStmt : public ObContextDDLStmt
{
public:
  explicit ObDropContextStmt(common::ObIAllocator *name_pool) :
      ObContextDDLStmt(name_pool, stmt::T_DROP_CONTEXT)
  {
  }
  ObDropContextStmt() :
      ObContextDDLStmt(stmt::T_DROP_CONTEXT)
  {
  }
  virtual ~ObDropContextStmt() = default;
};


} /* sql */
} /* oceanbase */
#endif //OCEANBASE_SQL_OB_RESOLVER_DDL_CONTEXT_STMT_H_
