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
