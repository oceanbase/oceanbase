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

#ifndef OCEANBASE_SQL_RESOLVER_DCL_OB_DROP_USE_STMT_
#define OCEANBASE_SQL_RESOLVER_DCL_OB_DROP_USE_STMT_
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "lib/string/ob_strings.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{
class ObDropUserStmt: public ObDDLStmt
{
public:
  explicit ObDropUserStmt(common::ObIAllocator *name_pool);
  ObDropUserStmt();
  virtual ~ObDropUserStmt();
  int add_user(const common::ObString &user_name, const common::ObString &host_name);
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; };
  void set_if_exists(const bool if_exists) { if_exists_ = if_exists; }
  const common::ObStrings *get_users() const { return &users_; };
  bool get_if_exists() const { return if_exists_; };
  uint64_t get_tenant_id() const { return tenant_id_; };
  virtual bool cause_implicit_commit() const { return true; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return drop_user_arg_; }
  DECLARE_VIRTUAL_TO_STRING;
private:
  // data members
  common::ObStrings users_;//user1,host1; usr2,host2;...
  uint64_t tenant_id_;
  bool if_exists_;
  obrpc::ObDropUserArg drop_user_arg_; // 用于返回exec_tenant_id_
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropUserStmt);
};
} // end namespace sql
} // end namespace oceanbase

#endif //OCEANBAS_SQL_RESOLVER_DCL_OB_DROP_USER_STMT_
