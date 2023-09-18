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

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_modify_tenant_resolver.h"
#include "sql/resolver/ddl/ob_modify_tenant_stmt.h"
#include "sql/resolver/ddl/ob_tenant_resolver.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/cmd/ob_variable_set_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

/**
 *  MODIFY TENANT tenant_name MODIFY
 *      (modify_resource_definition,...)
 *
 *  modify_resource_definition:
 * TODO: (xiaochu.yh) add detail res definition here
 */

ObModifyTenantResolver::ObModifyTenantResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObModifyTenantResolver::~ObModifyTenantResolver()
{
}

int ObModifyTenantResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObModifyTenantStmt *modify_tenant_stmt = NULL;

  if (OB_UNLIKELY(T_MODIFY_TENANT != parse_tree.type_)
      || OB_UNLIKELY(4 != parse_tree.num_child_)
      || OB_ISNULL(parse_tree.children_)
      || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", "num_child", parse_tree.num_child_, "children", parse_tree.children_, K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (modify_tenant_stmt = create_stmt<ObModifyTenantStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create stmt");
    } else {
      stmt_ = modify_tenant_stmt;
    }
  }

  /* tenant name */
  ObString tenant_name;
  if (OB_SUCC(ret)) {
    if (nullptr == parse_tree.children_[0]) {
      tenant_name = ObString::make_string("all");
    } else if (OB_UNLIKELY(T_IDENT != parse_tree.children_[0]->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid parse_tree", K(ret));
    } else {
      tenant_name.assign_ptr((char *)(parse_tree.children_[0]->str_value_),
                             static_cast<int32_t>(parse_tree.children_[0]->str_len_));
    }
    if (OB_SUCC(ret)) {
      modify_tenant_stmt->set_tenant_name(tenant_name);
      modify_tenant_stmt->set_for_current_tenant(0 == session_info_->get_tenant_name().compare(
              modify_tenant_stmt->get_tenant_name()));
    }
  }

  /* tenant options */
  if (OB_SUCC(ret)) {
    if (NULL != parse_tree.children_[1]) {
      if (OB_UNLIKELY(T_TENANT_OPTION_LIST != parse_tree.children_[1]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid parse_tree", K(ret));
      } else {
        ObTenantResolver<ObModifyTenantStmt> resolver;
        if (OB_FAIL(resolver.resolve_tenant_options(modify_tenant_stmt,
                                                    parse_tree.children_[1],
                                                    session_info_,
                                                    *allocator_))) {
          LOG_WARN("resolve tenant option failed", K(ret));
        } else if (session_info_->get_in_transaction() && resolver.is_modify_read_only()) {
          ret = OB_ERR_LOCK_OR_ACTIVE_TRANSACTION;

          LOG_WARN("Can't execute the given command because "
                   "you have active locked tables or an active transaction");
        } else {
          modify_tenant_stmt->set_alter_option_set(resolver.get_alter_option_bitset());
        }
        if (OB_SUCC(ret)) {
          if (resolver.get_alter_option_bitset().has_member(obrpc::ObModifyTenantArg::PROGRESSIVE_MERGE_NUM) && resolver.get_alter_option_bitset().num_members() > 1) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("modify progressive merge num must be done seprately", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify both progeressive merge number and alter option");
          } else if (resolver.get_alter_option_bitset().has_member(obrpc::ObModifyTenantArg::ENABLE_EXTENDED_ROWID) && resolver.get_alter_option_bitset().num_members() > 1) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("enable extended rowid must be done seprately", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Modify both enable extended rowid option and alter option");
          }
        }
      }
    }
  }

  /* sys_var options */
  if (OB_SUCC(ret)) {
    if (NULL != parse_tree.children_[2]) {
      if (OB_UNLIKELY(T_VARIABLE_SET != parse_tree.children_[2]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse_tree", "parse_tree_type", parse_tree.children_[2]->type_, K(ret));
      } else {
        ObVariableSetResolver var_set_resolver(params_);
        if (OB_FAIL(var_set_resolver.resolve(*(parse_tree.children_[2])))) {
          LOG_WARN("failed to resolver sys var set options", K(ret));
        } else if (OB_ISNULL(var_set_resolver.get_basic_stmt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get_basic_stmt", K(ret));
        } else {
          ObVariableSetStmt *stmt = static_cast<ObVariableSetStmt *>(var_set_resolver.get_basic_stmt());
          if (OB_FAIL(modify_tenant_stmt->assign_variable_nodes(stmt->get_variable_nodes()))) {
            LOG_WARN("failed to assign_variable_nodes", K(ret));
          }
        }
      }
    }
  }

  /* tenant rename */
  if (OB_SUCC(ret)) {
    ObString new_tenant_name;
    if (NULL != parse_tree.children_[3]) {
      if (OB_UNLIKELY(T_IDENT != parse_tree.children_[3]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse_tree", K(ret));
      } else if (0 != session_info_->get_tenant_name().case_compare("sys")) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "normal tenants rename tenant name");
      } else if (0 == modify_tenant_stmt->get_tenant_name().case_compare("sys")) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "rename sys tenant name");
      } else {
        new_tenant_name.assign_ptr(
            (char *)(parse_tree.children_[3]->str_value_),
            static_cast<int32_t>(parse_tree.children_[3]->str_len_));
        if (OB_FAIL(ObResolverUtils::check_not_supported_tenant_name(new_tenant_name))) {
          LOG_WARN("unsupported tenant name", KR(ret), K(new_tenant_name));
        }
      }
    } else {
      new_tenant_name.reset();
    }
    modify_tenant_stmt->set_new_tenant_name(new_tenant_name);
  }

  return ret;
}

} /* sql */
} /* oceanbase */
