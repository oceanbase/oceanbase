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

#ifndef OCEANBASE_SQL_RESOLVER_DCL_OB_LOCK_USER_STMT_
#define OCEANBASE_SQL_RESOLVER_DCL_OB_LOCK_USER_STMT_
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "lib/string/ob_strings.h"
namespace oceanbase
{
namespace sql
{
class ObLockUserStmt: public ObDDLStmt
{
public:
  ObLockUserStmt();
  explicit ObLockUserStmt(common::ObIAllocator *name_pool);
  virtual ~ObLockUserStmt();
  int add_user(const common::ObString &user_name, const common::ObString &host_name);
  const common::ObStrings *get_users() const { return &user_; }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_locked(bool locked) { locked_ = locked; }
  bool is_locked() const { return locked_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return lock_user_arg_; }
  // function members
  TO_STRING_KV(K_(stmt_type), K_(tenant_id), K_(user), K_(locked));
  virtual bool cause_implicit_commit() const { return true; }

private:
  // data members
  uint64_t tenant_id_;
  common::ObStrings user_;//user1,host1; usr2,host2;...
  bool locked_;
  obrpc::ObLockUserArg lock_user_arg_; // 用于返回exec_tenant_id_
private:
  DISALLOW_COPY_AND_ASSIGN(ObLockUserStmt);
};
} // end namespace sql
} // end namespace oceanbase

#endif //OCEANBASE_SQL_RESOLVER_DCL_OB_LOCK_USER_STMT_
