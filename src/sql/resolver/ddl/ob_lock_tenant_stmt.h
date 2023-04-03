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

#ifndef OCEANBASE_SQL_OB_LOCK_TENANT_STMT_H_
#define OCEANBASE_SQL_OB_LOCK_TENANT_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObLockTenantStmt : public ObDDLStmt
{
public:
  explicit ObLockTenantStmt(common::ObIAllocator *name_pool);
  ObLockTenantStmt();
  virtual ~ObLockTenantStmt();
  inline obrpc::ObLockTenantArg &get_lock_tenant_arg();
  const common::ObString &get_tenant_name() const { return lock_tenant_arg_.tenant_name_; }
  virtual void print(FILE *fp, int32_t level, int32_t index = 0);

  void set_tenant_name(const common::ObString &tenant_name);
  void set_locked(const bool is_locked);
  virtual obrpc::ObDDLArg &get_ddl_arg() { return lock_tenant_arg_; }
private:
  obrpc::ObLockTenantArg lock_tenant_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObLockTenantStmt);
};

inline obrpc::ObLockTenantArg &ObLockTenantStmt::get_lock_tenant_arg()
{
  return lock_tenant_arg_;
}

} /* sql */
} /* oceanbase */

#endif //OCEANBASE_SQL_OB_LOCK_TENANT_STMT_H_
