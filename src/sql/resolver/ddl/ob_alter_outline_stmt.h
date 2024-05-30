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

#ifndef OCEANBASE_SQL_OB_ALTER_OUTLINE_STMT_H_
#define OCEANBASE_SQL_OB_ALTER_OUTLINE_STMT_H_

#include "lib/string/ob_string.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObAlterOutlineStmt : public ObDDLStmt
{
public:
  ObAlterOutlineStmt() :
      ObDDLStmt(stmt::T_ALTER_OUTLINE),
      alter_outline_arg_(),
      outline_stmt_(NULL)
  {}
  virtual ~ObAlterOutlineStmt() { }
  void set_database_name(const common::ObString &database_name)
  { alter_outline_arg_.db_name_ = database_name; }
  void set_outline_name(const common::ObString &outline_name)
  { alter_outline_arg_.alter_outline_info_.set_name(outline_name); }
  void set_outline_sql(const common::ObString &outline_sql)
  { alter_outline_arg_.alter_outline_info_.set_sql_text(outline_sql);}
  void set_outline_stmt(ObStmt *stmt) { outline_stmt_ = stmt; }

  const common::ObString &get_outline_sql() const { return alter_outline_arg_.alter_outline_info_.get_sql_text_str(); }
  common::ObString &get_outline_sql() { return alter_outline_arg_.alter_outline_info_.get_sql_text_str(); }
  const common::ObString &get_target_sql() const { return alter_outline_arg_.alter_outline_info_.get_outline_target_str(); }
  common::ObString &get_target_sql() { return alter_outline_arg_.alter_outline_info_.get_outline_target_str(); }
  ObStmt *&get_outline_stmt() { return outline_stmt_; }
  obrpc::ObAlterOutlineArg &get_alter_outline_arg() { return alter_outline_arg_; }
  const obrpc::ObAlterOutlineArg &get_alter_outline_arg() const { return alter_outline_arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return alter_outline_arg_; }
  TO_STRING_KV(K_(alter_outline_arg),
               K_(outline_stmt));
private:
  obrpc::ObAlterOutlineArg alter_outline_arg_;
  ObStmt *outline_stmt_;//the stmt for outline
  DISALLOW_COPY_AND_ASSIGN(ObAlterOutlineStmt);
};
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_ALTER_OUTLINE_STMT_H_
