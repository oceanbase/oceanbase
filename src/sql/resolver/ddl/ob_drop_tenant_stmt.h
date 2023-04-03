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

#ifndef OCEANBASE_SQL_OB_DROP_TENANT_STMT_H_
#define OCEANBASE_SQL_OB_DROP_TENANT_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObDropTenantStmt : public ObDDLStmt
{
public:
  explicit ObDropTenantStmt(common::ObIAllocator *name_pool);
  ObDropTenantStmt();
  virtual ~ObDropTenantStmt();
  inline obrpc::ObDropTenantArg &get_drop_tenant_arg();
  const common::ObString &get_tenant_name() const { return drop_tenant_arg_.tenant_name_; }
  virtual void print(FILE *fp, int32_t level, int32_t index = 0);
  virtual bool cause_implicit_commit() const { return true; }

  void set_if_exist(const bool if_exist)
  {
    drop_tenant_arg_.if_exist_ = if_exist;
  }
  void set_tenant_name(const common::ObString &tenant_name);
  void set_delay_to_drop(const bool delay_to_drop);
  void set_open_recyclebin(const bool open_recyclebin);
  virtual obrpc::ObDDLArg &get_ddl_arg() { return drop_tenant_arg_; }
private:
  obrpc::ObDropTenantArg drop_tenant_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObDropTenantStmt);
};

inline obrpc::ObDropTenantArg &ObDropTenantStmt::get_drop_tenant_arg()
{
  return drop_tenant_arg_;
}

} /* sql */
} /* oceanbase */

#endif //OCEANBASE_SQL_OB_DROP_TENANT_STMT_H_
