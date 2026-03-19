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

#ifndef OCEANBASE_SQL_OB_ALTER_VIEW_STMT_
#define OCEANBASE_SQL_OB_ALTER_VIEW_STMT_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObAlterViewStmt : public ObDDLStmt
{
public:
  ObAlterViewStmt()
    : ObDDLStmt(stmt::T_ALTER_VIEW),
      alter_view_arg_()
  {}
  explicit ObAlterViewStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_ALTER_VIEW),
      alter_view_arg_()
  {}
  virtual ~ObAlterViewStmt() {}
  obrpc::ObAlterViewArg &get_alter_view_arg() { return alter_view_arg_; }
  const obrpc::ObAlterViewArg &get_alter_view_arg() const { return alter_view_arg_; }
  const common::ObString &get_database_name() const { return alter_view_arg_.database_name_; }
  const common::ObString &get_view_name() const { return alter_view_arg_.view_name_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() override { return alter_view_arg_; }
  virtual bool cause_implicit_commit() const override { return true; }
  TO_STRING_KV(K_(alter_view_arg));

private:
  obrpc::ObAlterViewArg alter_view_arg_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_ALTER_VIEW_STMT_
