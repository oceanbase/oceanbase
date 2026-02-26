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
#include "sql/resolver/dml/ob_standard_group_checker.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
int ObStandardGroupChecker::init(const ObSelectStmt* select_stmt,
                                 ObSQLSessionInfo *session_info,
                                 ObSchemaChecker *schema_checker)
{
  int ret = OB_SUCCESS;
  select_stmt_ = select_stmt;
  session_info_ = session_info;
  schema_checker_ = schema_checker;
  return ret;
}

int ObStandardGroupChecker::add_group_by_expr(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group by expr is null", K(ret));
  } else if (OB_FAIL(group_by_exprs_.push_back(expr))) {
    LOG_WARN("add expr to settled exprs failed", K(ret));
  } else {
    //this stmt has group
    set_has_group(true);
    is_scalar_aggr_ = false;
  }
  return ret;
}

int ObStandardGroupChecker::add_unsettled_expr(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null");
  } else if (expr->has_generalized_column()) {
    if (expr->has_flag(CNT_AGG)) {
      set_has_group(true);
    }
    if (OB_FAIL(unsettled_exprs_.push_back(expr))) {
      LOG_WARN("add unsettled exprs failed", K(ret));
    }
  }
  return ret;
}

int ObStandardGroupChecker::check_only_full_group_by()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt_), K(session_info_));
  } else if (!has_group_) {
    // do nothing
  } else {
    ObArenaAllocator alloc("CheckUnique", OB_MALLOC_NORMAL_BLOCK_SIZE,
                            session_info_->get_effective_tenant_id(),
                            ObCtxIds::DEFAULT_CTX_ID);
    ObRawExprFactory expr_factory(alloc);
    ObFdItemFactory fd_item_factory(alloc);
    ObStandardGroupVisitor visitor(this);
    if (OB_FAIL(deduce_settled_exprs(&alloc,
                                     &expr_factory,
                                     &fd_item_factory))) {
      LOG_WARN("failed to deduce settled exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < unsettled_exprs_.count(); ++i) {
      ObRawExpr *unsettled_expr = unsettled_exprs_.at(i);
      bool is_valid = false;
      if (OB_FAIL(expr_exists_in_group_by(unsettled_expr, is_valid))) {
        LOG_WARN("failed to check column in settled columns", K(ret));
      } else if (is_valid) {
        // expr exists in group by columns
      } else if (OB_FAIL(unsettled_expr->preorder_accept(visitor))) {
        LOG_WARN("failed to check unsettled expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      expr_factory.destory();
      fd_item_factory.destory();
    }
  }
  return ret;
}

int ObStandardGroupChecker::expr_exists_in_group_by(ObRawExpr *expr,
                                                    bool &is_existed)
{
  int ret = OB_SUCCESS;
  int tmp_ret;
  is_existed = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (ObOptimizerUtil::find_equal_expr(group_by_exprs_, expr)) {
    is_existed = true;
  }
  return ret;
}

int ObStandardGroupChecker::deduce_settled_exprs(ObArenaAllocator *alloc,
                                                 ObRawExprFactory *expr_factory,
                                                 ObFdItemFactory *fd_item_factory)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt_) || OB_ISNULL(session_info_)
      || OB_ISNULL(alloc) || OB_ISNULL(expr_factory) || OB_ISNULL(fd_item_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt_), K(session_info_), K(alloc), K(expr_factory), K(fd_item_factory));
  } else {
    ObTransformUtils::UniqueCheckHelper check_helper;
    check_helper.alloc_ = alloc;
    check_helper.fd_factory_ = fd_item_factory;
    check_helper.expr_factory_ = expr_factory;
    check_helper.schema_checker_ = schema_checker_;
    check_helper.session_info_ = session_info_;
    ObSEArray<TableItem*, 8> from_tables;
    ObTransformUtils::UniqueCheckInfo unique_info;
    if (OB_FAIL(select_stmt_->get_from_tables(from_tables))) {
      LOG_WARN("failed to get from tables", K(ret));
    } else if (OB_FAIL(ObTransformUtils::compute_tables_property(select_stmt_,
                                                                 check_helper,
                                                                 from_tables,
                                                                 select_stmt_->get_condition_exprs(),
                                                                 unique_info))) {
      LOG_WARN("failed to compute tables property", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::append_exprs_no_dup(settled_exprs_, unique_info.const_exprs_))) {
      LOG_WARN("failed to append const exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::append_exprs_no_dup(settled_exprs_, group_by_exprs_))) {
      LOG_WARN("failed to append group by exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::deduce_determined_exprs(settled_exprs_,
                                                                select_stmt_,
                                                                unique_info.fd_sets_,
                                                                unique_info.equal_sets_,
                                                                unique_info.const_exprs_))) {
      LOG_WARN("failed to deduce determined exprs", K(ret));
    }
  }
  return ret;
}

int ObStandardGroupChecker::check_unsettled_column(const ObColumnRefRawExpr *unsettled_column)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  bool is_determined = false;
  if (OB_ISNULL(unsettled_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsettled column is null", K(ret));
  } else if (ObOptimizerUtil::find_equal_expr(settled_exprs_, unsettled_column)) {
    // unsettled_column is in group by column or is a const expr
  } else {
    ret = is_scalar_aggr_ ? OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS : OB_ERR_WRONG_FIELD_WITH_GROUP;
    if (OB_ERR_WRONG_FIELD_WITH_GROUP == ret) {
      ObString column_name = concat_qualified_name(unsettled_column->get_database_name(),
                                                   unsettled_column->get_table_name(),
                                                   unsettled_column->get_column_name());
      LOG_USER_ERROR(OB_ERR_WRONG_FIELD_WITH_GROUP, column_name.length(), column_name.ptr());
    }
  }
  return ret;
}

int ObStandardGroupVisitor::visit(ObColumnRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("standard group checker is null", K(ret));
  } else if (is_in_subquery_) {
    // do nothing
  } else if (OB_FAIL(checker_->check_unsettled_column(&expr))) {
    LOG_WARN("failed to visit column ref expr", K(ret));
  }
  return ret;
}

int ObStandardGroupVisitor::visit(ObSysFunRawExpr &expr)
{
  if (T_FUN_SYS_ANY_VALUE == expr.get_expr_type()) {
    skip_expr_ = &expr;
  }
  return OB_SUCCESS;
}

int ObStandardGroupVisitor::visit(ObAggFunRawExpr &expr)
{
  skip_expr_ = &expr;
  return OB_SUCCESS;
}

int ObStandardGroupVisitor::visit(ObExecParamRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObStandardGroupVisitor exec_param_visitor(checker_);
  if (OB_ISNULL(expr.get_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref expr is invalid", K(ret));
  } else if (OB_FAIL(expr.get_ref_expr()->preorder_accept(exec_param_visitor))) {
    LOG_WARN("failed to visit child", K(ret));
  } else {
    skip_expr_ = &expr;
  }
  return ret;
}

int ObStandardGroupVisitor::visit(ObQueryRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *ref_stmt = expr.get_ref_stmt();
  ObSEArray<ObRawExpr*, 8> relation_exprs;
  ObStandardGroupVisitor sub_query_visitor(checker_, true);
  if (OB_ISNULL(ref_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref stmt is null", K(ret), K(expr));
  } else if (OB_FAIL(ref_stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    if (OB_ISNULL(relation_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("relation expr is null", K(ret), K(i));
    } else if (!relation_exprs.at(i)->has_flag(CNT_DYNAMIC_PARAM)
               && !relation_exprs.at(i)->has_flag(CNT_SUB_QUERY)) {
      // do nothing
    } else if (OB_FAIL(relation_exprs.at(i)->preorder_accept(sub_query_visitor))) {
      LOG_WARN("failed to check unsettled expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    skip_expr_ = &expr;
  }
  return ret;
}

bool ObStandardGroupVisitor::skip_child(ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  bool bret = false;
  if (skip_expr_ == &expr) {
    bret = true;
  } else if (OB_ISNULL(checker_)) {
    // should never reach here
  } else if (OB_FAIL(checker_->expr_exists_in_group_by(&expr, bret))) {
    bret = false;
    LOG_WARN("failed to check column in settled columns", K(ret));
  }
  return bret;
}

}  // namespace sql
}  // namespace oceanbase