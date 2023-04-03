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

#ifndef OCEANBASE_SQL_OB_MODIFY_TENANT_STMT_H_
#define OCEANBASE_SQL_OB_MODIFY_TENANT_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObModifyTenantSpecialOption final
{
public:
  ObModifyTenantSpecialOption();
  ~ObModifyTenantSpecialOption() = default;
  void set_progressive_merge_num(const int64_t progressive_merge_num) { progressive_merge_num_ = progressive_merge_num; }
  int64_t get_progressive_merge_num() const { return progressive_merge_num_; }
private:
  int64_t progressive_merge_num_;
};

class ObModifyTenantStmt : public ObDDLStmt
{
public:
  explicit ObModifyTenantStmt(common::ObIAllocator *name_pool);
  ObModifyTenantStmt();
  virtual ~ObModifyTenantStmt();
  inline obrpc::ObModifyTenantArg &get_modify_tenant_arg();
  virtual void print(FILE *fp, int32_t level, int32_t index = 0);

  void set_tenant_name(const common::ObString &tenant_name);
  void set_new_tenant_name(const common::ObString &new_tenant_name);
  int add_resource_pool(const common::ObString &res);
  int add_zone(const common::ObString &zone);
  int set_comment(const common::ObString &comment);
  int set_locality(const common::ObString &locality);
  void set_primary_zone(const common::ObString &zone);
  void set_charset_type(const common::ObCharsetType type);
  void set_enable_arbitration_service(const bool enable_arbitration_service);
  void set_collation_type(const common::ObCollationType type);
  inline void set_read_only(const bool read_only)
  {
    modify_tenant_arg_.tenant_schema_.set_read_only(read_only);
  }
  void set_progressive_merge_num(const int64_t progressive_merge_num)
  {
    special_option_.set_progressive_merge_num(progressive_merge_num);
  }
  int64_t get_progressive_merge_num()
  {
    return special_option_.get_progressive_merge_num();
  }

  inline void set_alter_option_set(const common::ObBitSet<> &alter_option_set);
  virtual bool cause_implicit_commit() const { return true; }
  inline const common::ObString &get_tenant_name() const;
  inline const common::ObString &get_new_tenant_name() const;
  void set_for_current_tenant(bool is_current_tenant)
  { for_current_tenant_ = is_current_tenant; }
  bool is_for_current_tenant() const
  { return for_current_tenant_; }
  int check_normal_tenant_can_do(bool &normal_can_do) const
  { return modify_tenant_arg_.check_normal_tenant_can_do(normal_can_do); }
  int add_sys_var_node(const ObVariableSetStmt::VariableSetNode &node) { return sys_var_nodes_.push_back(node); }
  const common::ObIArray<ObVariableSetStmt::VariableSetNode> &get_sys_var_nodes() const {return sys_var_nodes_;}
  int assign_variable_nodes(const common::ObIArray<ObVariableSetStmt::VariableSetNode> &other);
  int set_default_tablegroup_name(const common::ObString &tablegroup_name);

  virtual obrpc::ObDDLArg &get_ddl_arg() { return modify_tenant_arg_; }
  TO_STRING_KV(K_(modify_tenant_arg));
private:
  bool for_current_tenant_;
  obrpc::ObModifyTenantArg modify_tenant_arg_;
  common::ObArray<ObVariableSetStmt::VariableSetNode, common::ModulePageAllocator, true> sys_var_nodes_;
  ObModifyTenantSpecialOption special_option_;
  DISALLOW_COPY_AND_ASSIGN(ObModifyTenantStmt);
};

inline obrpc::ObModifyTenantArg &ObModifyTenantStmt::get_modify_tenant_arg()
{
  return modify_tenant_arg_;
}

inline void ObModifyTenantStmt::set_alter_option_set(const common::ObBitSet<> &alter_option_set)
{
  //copy
  modify_tenant_arg_.alter_option_bitset_ = alter_option_set;
}

inline const common::ObString &ObModifyTenantStmt::get_tenant_name() const
{
  return modify_tenant_arg_.tenant_schema_.get_tenant_name_str();
}

inline const common::ObString &ObModifyTenantStmt::get_new_tenant_name() const
{
  return modify_tenant_arg_.new_tenant_name_;
}

inline int ObModifyTenantStmt::assign_variable_nodes(const common::ObIArray<ObVariableSetStmt::VariableSetNode> &other)
{
  return sys_var_nodes_.assign(other);
}
} /* sql */
} /* oceanbase */

#endif //OCEANBASE_SQL_OB_MODIFY_TENANT_STMT_H_
