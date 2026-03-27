/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALTER_USER_PROFILE_STMT_H_
#define OB_ALTER_USER_PROFILE_STMT_H_
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "lib/string/ob_strings.h"
namespace oceanbase
{
namespace sql
{
class ObAlterUserProfileStmt: public ObDDLStmt
{
public:
  ObAlterUserProfileStmt();
  explicit ObAlterUserProfileStmt(common::ObIAllocator *name_pool);
  virtual ~ObAlterUserProfileStmt();

  obrpc::ObAlterUserProfileArg &get_ddl_arg() { return arg_; }
  // function members
  TO_STRING_KV(K_(stmt_type), K_(arg));
  enum {SET_ROLE = 1 << 0, SET_DEFAULT_ROLE = 1 << 1};
  void set_set_role_flag(int set_role_flag) { set_role_flag_ = set_role_flag; }
  int get_set_role_flag() const { return set_role_flag_; }
  virtual bool cause_implicit_commit() const { return !(lib::is_mysql_mode() && get_set_role_flag() == SET_ROLE); }
private:
  // data members
  obrpc::ObAlterUserProfileArg arg_;
  int set_role_flag_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterUserProfileStmt);
};
} // end namespace sql
} // end namespace oceanbase

#endif //OB_ALTER_USER_PROFILE_STMT_H_
