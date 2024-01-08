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

#ifndef OCEANBASE_SQL_OB_CREATE_OUTLINE_STMT_H_
#define OCEANBASE_SQL_OB_CREATE_OUTLINE_STMT_H_

#include "lib/string/ob_string.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObCreateOutlineStmt : public ObDDLStmt
{
public:
  ObCreateOutlineStmt() :
      ObDDLStmt(stmt::T_CREATE_OUTLINE),
      create_outline_arg_(),
      max_concurrent_(-1),
      outline_stmt_(NULL)
  {}
  ~ObCreateOutlineStmt() { }
  void set_replace() { create_outline_arg_.or_replace_ = true; }
  void set_database_name(const common::ObString &database_name) { create_outline_arg_.db_name_ = database_name; }
  void set_owner(const common::ObString &user_name) { create_outline_arg_.outline_info_.set_owner(user_name); }
  void set_owner_id(const uint64_t owner_id) { create_outline_arg_.outline_info_.set_owner_id(owner_id); }
  void set_server_version(const common::ObString &version) { create_outline_arg_.outline_info_.set_version(version); }
  void set_outline_name(const common::ObString &outline_name) { create_outline_arg_.outline_info_.set_name(outline_name); }
  void set_outline_sql(const common::ObString &outline_sql) { create_outline_arg_.outline_info_.set_sql_text(outline_sql);}
  const common::ObString &get_outline_sql() const { return create_outline_arg_.outline_info_.get_sql_text_str(); }
  common::ObString &get_outline_sql() { return create_outline_arg_.outline_info_.get_sql_text_str(); }
  void set_outline_stmt(ObStmt *stmt) { outline_stmt_ = stmt; }
  void set_max_concurrent(int64_t max_concurrent) { max_concurrent_ = max_concurrent; }
  int64_t get_max_concurrent() { return max_concurrent_; }
  ObStmt *&get_outline_stmt() { return outline_stmt_; }
  void set_target_sql(const common::ObString &target) { create_outline_arg_.outline_info_.set_outline_target(target);}
  const common::ObString &get_target_sql() const { return create_outline_arg_.outline_info_.get_outline_target_str(); }
  common::ObString &get_target_sql() { return create_outline_arg_.outline_info_.get_outline_target_str(); }
  obrpc::ObCreateOutlineArg &get_create_outline_arg() { return create_outline_arg_; }
  common::ObString &get_hint() { return hint_; }
  common::ObString &get_sql_id() { return sql_id_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return create_outline_arg_; }
  TO_STRING_KV(K_(create_outline_arg),
               K_(outline_stmt));
private:
  obrpc::ObCreateOutlineArg create_outline_arg_; //这个是需要在执行的过程中填充的，
  common::ObString sql_id_; //给定sql_id的情况
  common::ObString hint_; //给定的hint是什么
  int64_t max_concurrent_;
  ObStmt *outline_stmt_;//the stmt for outline, 通过这个值是否为null来判断是那种情况
  DISALLOW_COPY_AND_ASSIGN(ObCreateOutlineStmt);
};
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_CREATE_OUTLINE_STMT_H_
