/**
 * Copyright (c) 2023 OceanBase
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
#include "sql/rewrite/ob_transform_utils.h"
#include "ob_stmt_expr_visitor.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

void ObStmtExprVisitor::remove_scope(const ObIArray<DmlStmtScope> &scopes)
{
  for (int64_t i = 0; i < scopes.count(); ++i) {
    remove_scope(scopes.at(i));
  }
}

void ObStmtExprVisitor::add_scope(const ObIArray<DmlStmtScope> &scopes)
{
  for (int64_t i = 0; i < scopes.count(); ++i) {
    add_scope(scopes.at(i));
  }
}

int ObStmtExprGetter::do_visit(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (NULL == checker_) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = checker_->add_expr(expr);
  }
  return ret;
}

int ObStmtExprReplacer::add_skip_expr(const ObRawExpr *skip_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(skip_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret), KP(skip_expr));
  } else if (!skip_exprs_.created()) {
    if (OB_FAIL(skip_exprs_.create(8))) {
      LOG_WARN("failed to create expr set", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int tmp_ret = skip_exprs_.exist_refactored(reinterpret_cast<uint64_t>(skip_expr));
    if (OB_HASH_EXIST == tmp_ret) {
    } else if (OB_HASH_NOT_EXIST == tmp_ret) {
      if (OB_FAIL(skip_exprs_.set_refactored(reinterpret_cast<uint64_t>(skip_expr)))) {
        LOG_WARN("failed to add replace expr into set", K(ret));
      }
    } else {
      ret = tmp_ret;
      LOG_WARN("failed to get expr from set", K(ret));
    }
  }
  return ret;
}

int ObStmtExprReplacer::add_replace_exprs(const ObIArray<ObRawExpr *> &from_exprs,
                                          const ObIArray<ObRawExpr *> &to_exprs,
                                          const ObIArray<ObRawExpr *> *skip_exprs /*default NULL*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replacer_.add_replace_exprs(from_exprs, to_exprs))) {
    LOG_WARN("failed to add replace exprs", K(ret));
  } else if (NULL != skip_exprs) {
    for (int64_t i = 0; OB_SUCC(ret) && i < skip_exprs->count(); ++i) {
      if (OB_FAIL(add_skip_expr(skip_exprs->at(i)))) {
        LOG_WARN("failed to add skip expr", K(ret));
      }
    }
  }
  return ret;
}

int ObStmtExprReplacer::check_expr_need_skip(const ObRawExpr *expr, bool &need_skip)
{
  int ret = OB_SUCCESS;
  need_skip = false;
  uint64_t key = reinterpret_cast<uint64_t>(expr);
  if (skip_exprs_.created()) {
    int tmp_ret = skip_exprs_.exist_refactored(key);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
    } else if (OB_HASH_EXIST == tmp_ret) {
      need_skip = true;
    } else {
      ret = tmp_ret;
      LOG_WARN("failed to get expr from hash map", K(ret));
    }
  }
  return ret;
}

int ObStmtExprReplacer::do_visit(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  bool need_skip = false;
  if (OB_FAIL(check_expr_need_skip(expr, need_skip))) {
    LOG_WARN("failed to check expr need skip", K(ret), K(expr));
  } else if (need_skip) {
  } else if (OB_FAIL(replacer_.replace(expr))) {
    LOG_WARN("failed to replace", K(ret), K(expr));
  } else { /* do nothing */ }
  return ret;
}

int ObStmtExprCopier::do_visit(ObRawExpr *&expr)
{
  return copier_.copy_on_replace(expr, expr);
}

int ObSharedExprChecker::init(ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<uint64_t> stmt_expr_set;
  if (OB_FAIL(shared_expr_set_.create(64))) {
    LOG_WARN("failed to create expr set", K(ret));
  } else if (OB_FAIL(stmt_expr_set.create(64))) {
    LOG_WARN("failed to create expr set", K(ret));
  } else {
    stmt_expr_set_ = &stmt_expr_set;
    set_relation_scope();
    if (OB_FAIL(stmt.iterate_stmt_expr(*this))) {
      LOG_WARN("failed to iterate stmt expr", K(ret));
    }
    stmt_expr_set_ = NULL;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt_expr_set.destroy())) {
      LOG_WARN("failed to destroy stmt expr set", K(ret));
    }
  }
  return ret;
}

int ObSharedExprChecker::destroy()
{
  int ret = OB_SUCCESS;
  if (shared_expr_set_.created() && OB_FAIL(shared_expr_set_.destroy())) {
    LOG_WARN("destroy hash set failed", K(ret));
  }
  return ret;
}

int ObSharedExprChecker::is_shared_expr(const ObRawExpr *expr, bool &is_shared) const
{
  int ret = OB_SUCCESS;
  is_shared = false;
  if (OB_ISNULL(expr)) {
    /* do nothing */
  } else if (T_FUN_COLUMN_CONV == expr->get_expr_type()
             || expr->is_column_ref_expr()) {
    /* do nothing */
  } else if (shared_expr_set_.created()) {
    uint64_t key = reinterpret_cast<uint64_t>(expr);
    int tmp_ret = shared_expr_set_.exist_refactored(key);
    if (OB_HASH_EXIST == tmp_ret) {
      is_shared = true;
    } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != tmp_ret)) {
      ret = tmp_ret;
      LOG_WARN("failed to check hash set exists", K(ret));
    }
  }
  return ret;
}

int ObSharedExprChecker::do_visit(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  bool is_shared = false;
  uint64_t key = reinterpret_cast<uint64_t>(expr);
  if (OB_ISNULL(expr) || OB_ISNULL(stmt_expr_set_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr), K(stmt_expr_set_));
  } else if (expr->has_flag(CNT_COLUMN) ||
              expr->has_flag(CNT_AGG) ||
              expr->has_flag(CNT_SET_OP) ||
              expr->has_flag(CNT_WINDOW_FUNC) ||
              expr->has_flag(CNT_DYNAMIC_PARAM) ||
              expr->has_flag(CNT_SUB_QUERY)) {
    bool need_check_in_share = T_FUN_COLUMN_CONV == expr->get_expr_type();
    if (OB_FAIL(stmt_expr_set_->set_refactored(key, 0))) {
      if (OB_HASH_EXIST == ret) {
        is_shared = true;
        if (OB_FAIL(shared_expr_set_.set_refactored(key))) {
          LOG_WARN("failed to add expr into set", K(ret));
        }
      } else {
        LOG_WARN("failed to add expr into set", K(ret));
      }
    }
    if (OB_SUCC(ret) && (!is_shared || need_check_in_share)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
        if (OB_FAIL(SMART_CALL(do_visit(expr->get_param_expr(i))))) {
          LOG_WARN("failed to visit first", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObStmtExecParamFormatter::do_visit(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  if (OB_FAIL(do_formalize_exec_param(expr, is_happened))) {
    LOG_WARN("failed to add exec param reference", K(ret));
  } else if (!is_happened) {
    //do nothing
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (OB_FAIL(expr->extract_info())) {
    LOG_WARN("failed to extract info", K(ret));
  }
  return ret;
}

int ObStmtExecParamFormatter::do_formalize_exec_param(ObRawExpr *&expr, bool &is_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (expr->is_exec_param_expr()) {
    ObExecParamRawExpr *exec_param = static_cast<ObExecParamRawExpr *>(expr);
    ObRawExpr *ref_expr = exec_param->get_ref_expr();
    if (OB_ISNULL(ref_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref expr is null", K(ret));
    } else if (OB_FAIL(SMART_CALL(do_formalize_exec_param(ref_expr, is_happened)))) {
      LOG_WARN("failed to remove const exec param", K(ret));
    } else if (ref_expr->is_const_expr() &&
               !ref_expr->has_flag(CNT_ONETIME)) {
      // if the ref expr is a const expr but is also a onetime expr
      // then it is no need to remove it
      expr = ref_expr;
      is_happened = true;
    } else {
      exec_param->set_explicited_reference();
      exec_param->set_ref_expr(ref_expr);
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(do_formalize_exec_param(expr->get_param_expr(i),
                                                     is_happened)))) {
        LOG_WARN("failed to remove const exec param", K(ret));
      }
    }
  }
  return ret;
}

int ObStmtExprChecker::do_visit(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_expr(expr))) {
    LOG_WARN("failed to check expr", K(ret), KPC(expr));
  }
  return ret;
}

int ObStmtExprChecker::check_expr(const ObRawExpr *expr) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(check_const_flag(expr))) {
    LOG_WARN("failed to check const flag", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(SMART_CALL(check_expr(expr->get_param_expr(i))))) {
      LOG_WARN("failed to check param expr", K(ret));
    }
  }
  return ret;
}

int ObStmtExprChecker::check_const_flag(const ObRawExpr *expr) const
{
  int ret = OB_SUCCESS;
  bool expect_is_const = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && expect_is_const && i < expr->get_param_count(); ++i) {
    const ObRawExpr *param_expr = expr->get_param_expr(i);
    if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret), K(param_expr));
    } else {
      expect_is_const = param_expr->is_const_expr();
    }
  }
  if (OB_SUCC(ret) && expect_is_const) {
    if (OB_FAIL(expr->is_const_inherit_expr(expect_is_const))) {
      LOG_WARN("failed to check expr is const inherit", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(expr->is_const_expr() != expect_is_const)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr const flag is not match", K(ret), K(expect_is_const), KPC(expr));
  }
  return ret;
}
