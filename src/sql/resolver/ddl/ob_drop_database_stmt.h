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

#ifndef OCEANBASE_SQL_OB_DROP_DATABASE_STMT_H_
#define OCEANBASE_SQL_OB_DROP_DATABASE_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObDropDatabaseStmt : public ObDDLStmt
{
public:
  ObDropDatabaseStmt(common::ObIAllocator *name_pool)
      :ObDDLStmt(name_pool, stmt::T_DROP_DATABASE),
      drop_database_arg_(),
      server_charset_(),
      server_collation_()
  {
  }
  ObDropDatabaseStmt() : ObDDLStmt(stmt::T_DROP_DATABASE),
                         drop_database_arg_(),
                         server_charset_(),
                         server_collation_()
  {
  }
  virtual ~ObDropDatabaseStmt()
  {
  }

  void set_tenant_id(const int64_t tenant_id)
  {
    drop_database_arg_.tenant_id_ = tenant_id;
  }

  void set_if_exist(const bool if_exist)
  {
    drop_database_arg_.if_exist_ = if_exist;
  }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return drop_database_arg_; }

  TO_STRING_KV(K_(drop_database_arg));
  inline obrpc::ObDropDatabaseArg &get_drop_database_arg();
  virtual bool cause_implicit_commit() const { return true; }

  inline void set_database_name(const common::ObString &database_name);
  const common::ObString& get_database_name() const
  { return drop_database_arg_.database_name_; }
  const common::ObString& get_server_charset() const { return server_charset_; }
  void set_server_charset(const common::ObString &server_charset)
  { server_charset_ = server_charset; }
  const common::ObString& get_server_collation() const { return server_collation_; }
  void set_server_collation(const common::ObString &server_collation)
  { server_collation_ = server_collation; }
  void set_to_recyclebin(const bool to_recyclebin)
  { drop_database_arg_.to_recyclebin_ = to_recyclebin; }
private:
  obrpc::ObDropDatabaseArg drop_database_arg_;
  common::ObString server_charset_;
  common::ObString server_collation_;
};

inline obrpc::ObDropDatabaseArg &ObDropDatabaseStmt::get_drop_database_arg()
{
  return drop_database_arg_;
}

inline void ObDropDatabaseStmt::set_database_name(const common::ObString &database_name)
{
  drop_database_arg_.database_name_ = database_name;
}

} //namespace sql
} //namespace oceanbase

#endif //OCEANBASE_SQL_OB_DROP_DATABASE_STMT_H_
