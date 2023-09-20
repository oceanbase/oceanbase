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

#include "sql/resolver/ddl/ob_create_tenant_resolver.h"
#include "sql/resolver/ddl/ob_create_tenant_stmt.h"
#include "sql/resolver/ddl/ob_tenant_resolver.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/cmd/ob_variable_set_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

/**
 *  CREATE TENANT [IF NOT EXISTS] tenant_name
 *      (create_resource_definition,...)
 *
 *  create_resource_definition:
 * TODO: (xiaochu.yh) add detail res definition here
 */

ObCreateTenantResolver::ObCreateTenantResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
}

ObCreateTenantResolver::~ObCreateTenantResolver()
{
}

int ObCreateTenantResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObCreateTenantStmt *mystmt = NULL;

  if (OB_UNLIKELY(T_CREATE_TENANT != parse_tree.type_)
      || OB_ISNULL(parse_tree.children_)
      || OB_UNLIKELY(4 != parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(parse_tree.type_), K(parse_tree.num_child_), K(parse_tree.children_), K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (mystmt = create_stmt<ObCreateTenantStmt>()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create select stmt");
    } else {
      stmt_ = mystmt;
    }
  }

  /* [if not exists] */
  if (OB_SUCC(ret)) {
    if (NULL != parse_tree.children_[0]) {
      if (OB_UNLIKELY(T_IF_NOT_EXISTS != parse_tree.children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid parse_tree", K(ret));
      } else {
        mystmt->set_if_not_exist(true);
      }
    } else {
      mystmt->set_if_not_exist(false);
    }
  }
  /* tenant name */
  if (OB_SUCC(ret)) {
    ObTenantResolver<ObCreateTenantStmt> resolver;
    if (OB_FAIL(resolver.resolve_tenant_name(mystmt, parse_tree.children_[1]))) {
      LOG_WARN("resolve tenant name failed", KR(ret));
    } else {
      const ObString &tenant_name = mystmt->get_create_tenant_arg().tenant_schema_.get_tenant_name_str();
      if (OB_FAIL(ObResolverUtils::check_not_supported_tenant_name(tenant_name))) {
        LOG_WARN("unsupported tenant name", KR(ret), K(tenant_name));
      }
    }
  }

  /* tenant options */
  if (OB_SUCC(ret)) {
    if (NULL != parse_tree.children_[2]) {
      if (OB_UNLIKELY(T_TENANT_OPTION_LIST != parse_tree.children_[2]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid option node type", K(parse_tree.children_[2]->type_), K(ret));
      } else {
        ObTenantResolver<ObCreateTenantStmt> resolver;
        ret = resolver.resolve_tenant_options(mystmt, parse_tree.children_[2], session_info_, *allocator_);
      }
    }
  }

  /* sys_var options */
  if (OB_SUCC(ret)) {
    if (NULL != parse_tree.children_[3]) {
      if (OB_UNLIKELY(T_VARIABLE_SET != parse_tree.children_[3]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse_tree", "parse_tree_type", parse_tree.children_[3]->type_, K(ret));
      } else {
        ObVariableSetResolver var_set_resolver(params_);
        if (OB_FAIL(var_set_resolver.resolve(*(parse_tree.children_[3])))) {
          LOG_WARN("failed to resolver sys var set options", K(ret));
        } else if (OB_ISNULL(var_set_resolver.get_basic_stmt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get_basic_stmt", K(ret));
        } else {
          ObVariableSetStmt *stmt = static_cast<ObVariableSetStmt *>(var_set_resolver.get_basic_stmt());
          if (OB_FAIL(mystmt->assign_variable_nodes(stmt->get_variable_nodes()))) {
            LOG_WARN("failed to assign_variable_nodes", K(ret));
          }
        }

      }
    }
  }

  bool is_oracle_mode = false;
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < mystmt->get_sys_var_nodes().count(); i++) {
      const ObVariableSetStmt::VariableSetNode &node = mystmt->get_sys_var_nodes().at(i);
      if (0 == node.variable_name_.case_compare("ob_compatibility_mode")) {
        ObConstRawExpr *const_expr = static_cast<ObConstRawExpr*>(node.value_expr_);
        if (nullptr == const_expr) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("const expr is null", K(ret));
        } else {
          ObObj value = const_expr->get_value();
          ObString str;
          if (OB_FAIL(value.get_string(str))) {
            LOG_WARN("get string failed", K(ret));
          } else if (0 == str.case_compare("oracle")) {
            is_oracle_mode = true;
          }
        }
      }
    }
  }

  /* 对于tenant的charset和collation属性，是依赖于模式的，所以需要确定好模式后再处理
   * MySQL依赖于charset和collation属性，这里都会设置
   * 对于Oracle租户，创建租户时只允许设置charset属性，因为Oracle其实没有collation概念，Oracle的
   * 比较行为依赖于nls_comp和nls_sort两个参数，这两个默认行为是BINARY比较
   */
  if (OB_SUCC(ret)) {
    ObCollationType collation_type = mystmt->get_create_tenant_arg().tenant_schema_.get_collation_type();
    ObCharsetType charset_type = mystmt->get_create_tenant_arg().tenant_schema_.get_charset_type();
    if (is_oracle_mode) {
      if (CS_TYPE_BINARY == collation_type || CHARSET_BINARY == charset_type) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cant't set collation for oracle mode", K(ret));
      } else if (CHARSET_INVALID == charset_type && CS_TYPE_INVALID == collation_type) {
        charset_type = ObCharset::get_default_charset();
        collation_type = ObCharset::get_default_collation_oracle(charset_type);
      } else if (CS_TYPE_INVALID == collation_type) {
        collation_type = ObCharset::get_default_collation_oracle(charset_type);
      } else if (CHARSET_INVALID == charset_type) {
        charset_type = ObCharset::charset_type_by_coll(collation_type);
      } else {
        //这里需要考虑到charset_type已经是一个有效的字符集时，需要重新设置对应的collation_type
        //bug:
        collation_type = ObCharset::get_default_collation_oracle(charset_type);
      }

      if (OB_SUCC(ret)) {
        if (CHARSET_UTF16 == charset_type) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Use utf16 as database charset");
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(collation_type != ObCharset::get_default_collation_oracle(charset_type))) {
          ret = OB_ERR_CHARACTER_SET_MISMATCH;
          LOG_WARN("collation isn't corresponding to charset in oracle mode", K(ret), 
                   K(collation_type), K(ObCharset::get_default_collation_oracle(charset_type)));
        }
      }
    } else {
      if (collation_type == CS_TYPE_INVALID
          && charset_type == CHARSET_INVALID) {
        charset_type = ObCharset::get_default_charset();
        collation_type = ObCharset::get_default_collation(charset_type);
      } else if (OB_FAIL(common::ObCharset::check_and_fill_info(charset_type, collation_type))) {
        SQL_LOG(WARN, "fail to check charset collation", K(ret));
      }
    }
    // 这里需要检查对应的字符集和collation是否是匹配并且有效的，如果不匹配直接在resolver阶段抛出错误
    if (!ObCharset::is_valid_collation(charset_type, collation_type)) {
      ret = OB_ERR_COLLATION_MISMATCH;
      LOG_WARN("invalid collation info", K(charset_type), K(collation_type));
    } else {
      mystmt->set_collation_type(collation_type);
      mystmt->set_charset_type(charset_type);
    }
  }

  return ret;
}


} /* sql */
} /* oceanbase */
