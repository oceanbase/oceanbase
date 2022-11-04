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
