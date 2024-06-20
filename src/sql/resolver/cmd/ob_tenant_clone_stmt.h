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

#ifndef OCEANBASE_SQL_OB_CLONE_TENANT_STMT_H_
#define OCEANBASE_SQL_OB_CLONE_TENANT_STMT_H_

#include "sql/resolver/cmd/ob_cmd_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObCloneTenantStmt : public ObCMDStmt
{
public:
  explicit ObCloneTenantStmt(common::ObIAllocator *name_pool)
    : ObCMDStmt(name_pool, stmt::T_CLONE_TENANT),
      clone_tenant_arg_(),
      if_not_exists_(false) {}
  ObCloneTenantStmt()
    : ObCMDStmt(stmt::T_CLONE_TENANT),
      clone_tenant_arg_(),
      if_not_exists_(false) {}
  virtual ~ObCloneTenantStmt() {}
  inline obrpc::ObCloneTenantArg &get_clone_tenant_arg() { return clone_tenant_arg_; }
  inline bool get_if_not_exists() const { return if_not_exists_; }
  int init(const ObString &new_tenant_name,
           const ObString &source_tenant_name,
           const ObString &tenant_snapshot_name,
           const ObString &resource_pool_name,
           const ObString &unit_config_name)
  {
    return clone_tenant_arg_.init(new_tenant_name,
                                  source_tenant_name,
                                  tenant_snapshot_name,
                                  resource_pool_name,
                                  unit_config_name);
  }
  void set_if_not_exists(const bool is_exist)
  {
    if_not_exists_ = is_exist;
  }
private:
  obrpc::ObCloneTenantArg clone_tenant_arg_;
  bool if_not_exists_;
  DISALLOW_COPY_AND_ASSIGN(ObCloneTenantStmt);
};

} /* sql */
} /* oceanbase */

#endif //OCEANBASE_SQL_OB_CLONE_TENANT_STMT_H_
