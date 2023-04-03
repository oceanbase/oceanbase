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

#ifndef OCEANBASE_SQL_RESOLVER_DCL_OB_RENAME_USE_STMT_
#define OCEANBASE_SQL_RESOLVER_DCL_OB_RENAME_USE_STMT_
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "lib/string/ob_strings.h"
namespace oceanbase
{
namespace sql
{
class ObRenameUserStmt: public ObDDLStmt
{
public:
  explicit ObRenameUserStmt(common::ObIAllocator *name_pool);
  ObRenameUserStmt();
  virtual ~ObRenameUserStmt();
  int add_rename_info(const common::ObString &from_user, const common::ObString &from_host,
                      const common::ObString &to_user, const common::ObString &to_host);
  const common::ObStrings *get_rename_infos() const;
  inline uint64_t get_tenant_id() { return tenant_id_; }
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  virtual bool cause_implicit_commit() const { return true; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return rename_user_arg_; }
  DECLARE_VIRTUAL_TO_STRING;
private:
  // data members
  common::ObStrings rename_infos_; // (from1_user, from1_host, to1_user, to1_host), (from2_user, from2_host, to2_user, to2_host)
  uint64_t tenant_id_;
  obrpc::ObRenameUserArg rename_user_arg_; // 用于返回exec_tenant_id_
private:
  DISALLOW_COPY_AND_ASSIGN(ObRenameUserStmt);
};
} // end namespace sql
} // end namespace oceanbase

#endif //OCEANBASE_SQL_RESOLVER_DCL_OB_RENAME_USER_STMT_
