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
#include "sql/resolver/prepare/ob_execute_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObExecuteResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObExecuteStmt *execute_stmt = NULL;
  if (parse_tree.num_child_ != 2 || OB_ISNULL(session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "num_child", parse_tree.num_child_, K(allocator_), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(execute_stmt = create_stmt<ObExecuteStmt>())) {
      ret = OB_SQL_RESOLVER_NO_MEMORY;
      LOG_WARN("failed to create execute stmt", K(ret));
    } else {
      stmt_ = execute_stmt;
    }
  }
  if (OB_SUCC(ret)) {
    //resolver stmt name
    if (OB_ISNULL(parse_tree.children_[0])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(parse_tree.children_[0]), K(ret));
    } else {
      ObString stmt_name;
      ObString name(parse_tree.children_[0]->str_len_, parse_tree.children_[0]->str_value_);
      ObPsSessionInfo *ps_session_info = NULL;
      ObPsStmtId ps_id = OB_INVALID_ID;
      stmt::StmtType ps_type = stmt::T_NONE;
      if (OB_FAIL(ob_simple_low_to_up(*params_.allocator_, name, stmt_name))) {
        LOG_WARN("failed to write string", K(ret));
      } else if(OB_FAIL(session_info_->get_prepare_id(stmt_name, ps_id))) {
        LOG_WARN("failed to get prepare id", K(ret));
      } else if (OB_FAIL(session_info_->get_ps_session_info(ps_id, ps_session_info))) {
        LOG_WARN("failed to get ps session info", K(ret));
      } else if (OB_ISNULL(ps_session_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ps session info is NULL", K(stmt_name), K(ps_id), K(ret));
      } else {
        ps_type = ps_session_info->get_stmt_type();
        execute_stmt->set_prepare_id(ps_id);
        execute_stmt->set_prepare_type(ps_type);
      }
    }
  }
  //resolver variable
  if(OB_SUCC(ret)) {
    if (NULL == parse_tree.children_[1]) {
      //do nothing
    } else if (parse_tree.children_[1]->type_ != T_ARGUMENT_LIST) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(parse_tree.children_[1]->type_), K(ret));
    } else {
      ParseNode *arguments = parse_tree.children_[1];
      for(int32_t i = 0; OB_SUCC(ret) && i < arguments->num_child_; ++i) {
        if (OB_ISNULL(arguments->children_[i])) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(arguments->children_[i]), K(ret));
        } else {
          ObRawExpr *param_expr = NULL;
          if (OB_FAIL(ObResolverUtils::resolve_const_expr(params_, *arguments->children_[i], param_expr, NULL))) {
            LOG_WARN("failed to resolve const expr", K(ret));
          } else if (OB_FAIL(execute_stmt->add_param(param_expr))) {
            LOG_WARN("failed to add param", K(ret));
          } else { /*do nothing*/ }
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
