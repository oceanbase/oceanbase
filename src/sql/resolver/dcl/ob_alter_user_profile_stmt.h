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
  void set_set_role_flag(int set_role_flag) { set_role_flag_ = set_role_flag; }
  int get_set_role_flag() { return set_role_flag_; }
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
