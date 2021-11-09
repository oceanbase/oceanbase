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

namespace oceanbase {
namespace sql {
class ObVariableSetStmt : public ObDDLStmt {
public:
  class VariableSetNode {
  public:
    VariableSetNode()
        : variable_name_(),
          is_system_variable_(false),
          set_scope_(share::ObSetVar::SET_SCOPE_NEXT_TRANS),
          value_expr_(NULL),
          is_set_default_(false)
    {}
    virtual ~VariableSetNode()
    {}
    TO_STRING_KV(K_(variable_name), K_(is_system_variable), K_(set_scope), K_(value_expr), K_(is_set_default));

    common::ObString variable_name_;
    bool is_system_variable_;
    share::ObSetVar::SetScopeType set_scope_;
    ObRawExpr* value_expr_;
    bool is_set_default_;
  };
  class NamesSetNode {
  public:
    NamesSetNode()
        : is_set_names_(true),
          is_default_charset_(true),
          is_default_collation_(true),
          charset_(),
          collation_()
    {}
    virtual ~NamesSetNode()
    {}
    TO_STRING_KV(K_(is_set_names), K_(is_default_charset), K_(is_default_collation), K_(charset), K_(collation));

    bool is_set_names_;  // SET NAMES or SET CHARSET?
    bool is_default_charset_;
    bool is_default_collation_;
    common::ObString charset_;
    common::ObString collation_;
  };

  class VariableNamesSetNode {
  public:
    VariableNamesSetNode()
      : is_set_variable_(true),
        var_set_node_(),
        names_set_node_()
    {}
    VariableNamesSetNode(bool is_set_variable, const VariableSetNode &var_node,
          const NamesSetNode &names_node)
        : is_set_variable_(is_set_variable),
          var_set_node_(var_node),
          names_set_node_(names_node)
    {}
    virtual ~VariableNamesSetNode()
    {}
    TO_STRING_KV(K_(is_set_variable), K_(var_set_node), K_(names_set_node));

    bool is_set_variable_; // set variables or set names
    VariableSetNode var_set_node_;
    NamesSetNode names_set_node_;
  };

  ObVariableSetStmt()
      : ObDDLStmt(stmt::T_VARIABLE_SET),
        actual_tenant_id_(common::OB_INVALID_ID),
        variable_nodes_(),
        has_global_variable_(false)
  {}
  virtual ~ObVariableSetStmt()
  {}

  static inline VariableNamesSetNode make_variable_name_node(VariableSetNode& var_set_node) {
    return VariableNamesSetNode(true, var_set_node, NamesSetNode());
  }
  static inline VariableNamesSetNode make_variable_name_node(NamesSetNode& names_set_node) {
    return VariableNamesSetNode(false, VariableSetNode(), names_set_node);
  }
  inline void set_actual_tenant_id(uint64_t actual_tenant_id)
  {
    actual_tenant_id_ = actual_tenant_id;
  }
  inline uint64_t get_actual_tenant_id() const
  {
    return actual_tenant_id_;
  }
  inline int add_variable_node(const VariableNamesSetNode& node);
  inline int64_t get_variables_size() const;
  int get_variable_node(int64_t index, VariableNamesSetNode& node) const;
  inline void set_has_global_variable(bool has_global_variable)
  {
    has_global_variable_ = has_global_variable;
  }
  inline bool has_global_variable() const
  {
    return has_global_variable_;
  }

  virtual int get_cmd_type() const
  {
    return get_stmt_type();
  }
  virtual bool cause_implicit_commit() const
  {
    /* when VariableSet is SET AUTOCOMMIT=? statement,
     * and autocommit turns from 0 to 1,
     * it cause implicit commit in its executor.
     * Other set statement do not trigger implicit commit.
     * If it does, SET AUTOCOMMIT should use another new stmt
     */
    return false;
  }

  const common::ObIArray<VariableNamesSetNode>& get_variable_nodes() const
  {
    return variable_nodes_;
  }
  virtual obrpc::ObDDLArg& get_ddl_arg()
  {
    return modify_sysvar_arg_;
  }
  TO_STRING_KV(K_(actual_tenant_id), K_(variable_nodes));

private:
  uint64_t actual_tenant_id_;
  common::ObArray<VariableNamesSetNode, common::ModulePageAllocator, true> variable_nodes_;
  bool has_global_variable_;
  obrpc::ObModifySysVarArg modify_sysvar_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObVariableSetStmt);
};

inline int ObVariableSetStmt::add_variable_node(const VariableNamesSetNode& node)
{
  return variable_nodes_.push_back(node);
}

inline int64_t ObVariableSetStmt::get_variables_size() const
{
  return variable_nodes_.count();
}
}  // end of namespace sql
}  // end of namespace oceanbase

#endif  // OCEANBASE_SQL_RESOLVER_CMD_VARIABLE_SET_STMT_
