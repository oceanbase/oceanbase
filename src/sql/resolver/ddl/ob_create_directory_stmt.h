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

#ifndef OCEANBASE_SQL_OB_CREATE_DIRECTORY_STMT_H_
#define OCEANBASE_SQL_OB_CREATE_DIRECTORY_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObCreateDirectoryStmt : public ObDDLStmt
{
public:
  ObCreateDirectoryStmt();
  explicit ObCreateDirectoryStmt(common::ObIAllocator *name_pool);
  virtual ~ObCreateDirectoryStmt();

  virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_; }
  virtual bool cause_implicit_commit() const { return true; }

  obrpc::ObCreateDirectoryArg &get_create_directory_arg() { return arg_; }

  void set_tenant_id(const uint64_t tenant_id) { arg_.schema_.set_tenant_id(tenant_id); }
  void set_user_id(const uint64_t user_id) { arg_.user_id_ = user_id; }
  void set_or_replace(bool or_replace) { arg_.or_replace_ = or_replace; }
  int set_directory_name(const common::ObString &name) { return arg_.schema_.set_directory_name(name); }
  int set_directory_path(const common::ObString &path) { return arg_.schema_.set_directory_path(path); }

  bool is_or_replace() const {return arg_.or_replace_;}

  TO_STRING_KV(K_(arg));
private:
  obrpc::ObCreateDirectoryArg arg_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateDirectoryStmt);
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_CREATE_DIRECTORY_STMT_H_
