/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DROP_DIRECTORY_STMT_H_
#define OCEANBASE_SQL_OB_DROP_DIRECTORY_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObDropDirectoryStmt : public ObDDLStmt
{
public:
  ObDropDirectoryStmt();
  explicit ObDropDirectoryStmt(common::ObIAllocator *name_pool);
  virtual ~ObDropDirectoryStmt();

  virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_; }
  virtual bool cause_implicit_commit() const { return true; }

  obrpc::ObDropDirectoryArg &get_drop_directory_arg() { return arg_; }
  
  void set_tenant_id(const uint64_t id) { arg_.tenant_id_ = id; }
  void set_directory_name(const common::ObString &name) { arg_.directory_name_ = name; }

  TO_STRING_KV(K_(arg));
private:
  obrpc::ObDropDirectoryArg arg_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropDirectoryStmt);
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_DROP_DIRECTORY_STMT_H_
