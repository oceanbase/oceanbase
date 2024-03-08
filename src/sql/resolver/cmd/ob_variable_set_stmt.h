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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_VARIABLE_SET_STMT_
#define OCEANBASE_SQL_RESOLVER_CMD_VARIABLE_SET_STMT_

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "share/system_variable/ob_system_variable.h"
#include "sql/resolver/cmd/ob_set_names_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObVariableSetStmt : public ObDDLStmt
{
public:
  class VariableSetNode
  {
  public:
    VariableSetNode() : variable_name_(),
                        is_system_variable_(false),
                        set_scope_(share::ObSetVar::SET_SCOPE_NEXT_TRANS),
                        value_expr_(NULL),
                        is_set_default_(false),
                        set_names_stmt_(NULL)
    {}
    virtual ~VariableSetNode() {}
    TO_STRING_KV(K_(variable_name), K_(is_system_variable),
                 K_(set_scope), K_(value_expr), K_(is_set_default));

    common::ObString variable_name_;
    bool is_system_variable_;
    share::ObSetVar::SetScopeType set_scope_;
    ObRawExpr *value_expr_;
    bool is_set_default_;
    ObSetNamesStmt *set_names_stmt_;
  };

  ObVariableSetStmt() : ObDDLStmt(stmt::T_VARIABLE_SET),
                        actual_tenant_id_(common::OB_INVALID_ID),
                        variable_nodes_(),
                        has_global_variable_(false)
  {}
  virtual ~ObVariableSetStmt() {}

  inline void set_actual_tenant_id(uint64_t actual_tenant_id) { actual_tenant_id_ = actual_tenant_id; }
  inline uint64_t get_actual_tenant_id() const { return actual_tenant_id_; }
  inline int add_variable_node(const VariableSetNode &node);
  inline int64_t get_variables_size() const;
  int get_variable_node(int64_t index, VariableSetNode &node) const;
  inline void set_has_global_variable(bool has_global_variable)
  { has_global_variable_ = has_global_variable; }
  inline bool has_global_variable() const { return has_global_variable_; }

  virtual int get_cmd_type() const { return get_stmt_type(); }
  virtual bool cause_implicit_commit() const {
    /* when VariableSet is SET AUTOCOMMIT=? statement,
     * and autocommit turns from 0 to 1,
     * it cause implicit commit in its executor.
     * Other set statement do not trigger implicit commit.
     * If it does, SET AUTOCOMMIT should use another new stmt
     */
    return false;
  }

  const common::ObIArray<VariableSetNode> &get_variable_nodes() const
  { return variable_nodes_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return modify_sysvar_arg_; }
  TO_STRING_KV(K_(actual_tenant_id), K_(variable_nodes));
private:
  uint64_t actual_tenant_id_;
  common::ObArray<VariableSetNode, common::ModulePageAllocator, true> variable_nodes_;
  bool has_global_variable_;
  obrpc::ObModifySysVarArg modify_sysvar_arg_; // 用于返回exec_tenant_id_
  DISALLOW_COPY_AND_ASSIGN(ObVariableSetStmt);
};

inline int ObVariableSetStmt::add_variable_node(const VariableSetNode &node)
{
  return variable_nodes_.push_back(node);
}

inline int64_t ObVariableSetStmt::get_variables_size() const
{
  return variable_nodes_.count();
}
}//end of namespace sql
}//end of namespace oceanbase

#endif //OCEANBASE_SQL_RESOLVER_CMD_VARIABLE_SET_STMT_
