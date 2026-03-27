/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_PACKAGE_STMT_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_PACKAGE_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObDropPackageStmt : public ObDDLStmt
{
public:
  explicit ObDropPackageStmt(common::ObIAllocator *name_pool)
      : ObDDLStmt(name_pool, stmt::T_DROP_PACKAGE),
        drop_package_arg_() {}
  ObDropPackageStmt()
      : ObDDLStmt(stmt::T_DROP_PACKAGE),
        drop_package_arg_() {}
  virtual ~ObDropPackageStmt() {}
  obrpc::ObDropPackageArg &get_drop_package_arg() { return drop_package_arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return drop_package_arg_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropPackageStmt);

  obrpc::ObDropPackageArg drop_package_arg_;
};
} //namespace sql
} //namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_DROP_PACKAGE_STMT_H_ */
