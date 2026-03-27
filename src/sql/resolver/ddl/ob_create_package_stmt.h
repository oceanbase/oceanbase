/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_PACKAGE_STMT_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_PACKAGE_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObCreatePackageStmt : public ObDDLStmt
{
public:
  explicit ObCreatePackageStmt(common::ObIAllocator *name_pool)
      : ObDDLStmt(name_pool, stmt::T_CREATE_PACKAGE),
        create_package_arg_() {}
  ObCreatePackageStmt()
      : ObDDLStmt(stmt::T_CREATE_PACKAGE),
        create_package_arg_() {}
  virtual ~ObCreatePackageStmt() {}
  obrpc::ObCreatePackageArg &get_create_package_arg() { return create_package_arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return create_package_arg_; }
private:
  obrpc::ObCreatePackageArg create_package_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObCreatePackageStmt);
};
} //namespace sql
} //namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_PACKAGE_STMT_H_ */
