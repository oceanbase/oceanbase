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

#ifndef OCEANBASE_SQL_OB_CREATE_TENANT_STMT_H_
#define OCEANBASE_SQL_OB_CREATE_TENANT_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObCreateTenantStmt : public ObDDLStmt
{
public:
  explicit ObCreateTenantStmt(common::ObIAllocator *name_pool);
  ObCreateTenantStmt();
  virtual ~ObCreateTenantStmt();
  inline obrpc::ObCreateTenantArg &get_create_tenant_arg();
  virtual void print(FILE *fp, int32_t level, int32_t index = 0);

  void set_tenant_name(const common::ObString &tenant_name);
  int add_resource_pool(const common::ObString &res);
  int set_resource_pool(const common::ObIArray<common::ObString> &res);
  int add_zone(const common::ObString &zone);
  int set_comment(const common::ObString &commont);
  int set_locality(const common::ObString &locality);
  void set_primary_zone(const common::ObString &zone);
  void set_if_not_exist(const bool is_exist);
  void set_charset_type(const common::ObCharsetType type);
  void set_collation_type(const common::ObCollationType type);
  void set_enable_arbitration_service(const bool enable_arbitration_service);
  void set_read_only(const bool read_only)
  {
    create_tenant_arg_.tenant_schema_.set_read_only(read_only);
  }
  virtual bool cause_implicit_commit() const { return true; }
  int add_sys_var_node(const ObVariableSetStmt::VariableSetNode &node) { return sys_var_nodes_.push_back(node); }
  const common::ObIArray<ObVariableSetStmt::VariableSetNode> &get_sys_var_nodes() const {return sys_var_nodes_;}
  int assign_variable_nodes(const common::ObIArray<ObVariableSetStmt::VariableSetNode> &other);
  int set_default_tablegroup_name(const common::ObString &tablegroup_name);
  virtual obrpc::ObDDLArg &get_ddl_arg() { return create_tenant_arg_; }
  void set_create_standby_tenant();
  void set_log_restore_source(const common::ObString &log_restore_source);
private:
  obrpc::ObCreateTenantArg create_tenant_arg_;
  common::ObArray<ObVariableSetStmt::VariableSetNode, common::ModulePageAllocator, true> sys_var_nodes_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateTenantStmt);
};

inline obrpc::ObCreateTenantArg &ObCreateTenantStmt::get_create_tenant_arg()
{
  return create_tenant_arg_;
}

inline int ObCreateTenantStmt::assign_variable_nodes(const common::ObIArray<ObVariableSetStmt::VariableSetNode> &other)
{
  return sys_var_nodes_.assign(other);
}

} /* sql */
} /* oceanbase */
#endif //OCEANBASE_SQL_OB_CREATE_TENANT_STMT_H_
