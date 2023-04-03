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

#ifndef OCEANBASE_SQL_OB_USE_DATABASE_STMT_H_
#define OCEANBASE_SQL_OB_USE_DATABASE_STMT_H_

#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObUseDatabaseStmt : public ObDDLStmt
{
public:
  ObUseDatabaseStmt() : ObDDLStmt(stmt::T_USE_DATABASE),
    db_id_(common::OB_INVALID),
    db_name_(),
    db_charset_(),
    db_collation_(),
    db_priv_set_()
  {
  }

  explicit ObUseDatabaseStmt(common::ObIAllocator *name_pool)
      : ObDDLStmt(name_pool, stmt::T_USE_DATABASE),
      db_id_(common::OB_INVALID),
      db_name_(),
      db_charset_(),
      db_collation_(),
      db_priv_set_()
  {
  }

  virtual ~ObUseDatabaseStmt()
  {}
  void set_db_name(const common::ObString &db_name)
  { db_name_.assign_ptr(db_name.ptr(), db_name.length()); }
  void set_db_id(const int64_t db_id) { db_id_ = db_id; }
  const common::ObString& get_db_name() const { return db_name_; }
  int64_t get_db_id() const { return db_id_; }
  void set_db_id(uint64_t db_id) { db_id_ = db_id; }
  const common::ObString& get_db_charset() const { return db_charset_; }
  void set_db_charset(const common::ObString &db_charset) { db_charset_ = db_charset; }
  const common::ObString& get_db_collation() const { return db_collation_; }
  void set_db_collation(const common::ObString &db_collation) { db_collation_ = db_collation; }
  const ObPrivSet& get_db_priv_set() const { return db_priv_set_; }
  void set_db_priv_set(const ObPrivSet &db_priv_set) { db_priv_set_ = db_priv_set; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return use_database_arg_; }
  virtual bool cause_implicit_commit() const { return false; }
private:
  int64_t db_id_;
  common::ObString db_name_;
  common::ObString db_charset_;
  common::ObString db_collation_;
  ObPrivSet db_priv_set_;
  obrpc::ObUseDatabaseArg use_database_arg_; // 用于返回exec_tenant_id_
};
} //namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_USE_DATABASE_STMT_H_
