// (C) Copyright 2021 Alibaba Inc. All Rights Reserved.
//  Authors:
//    link.zt <>
//  Normalizer:
//
//
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

int ObStmtExprReplacer::add_replace_exprs(const ObIArray<ObRawExpr *> &from_exprs,
                                          const ObIArray<ObRawExpr *> &to_exprs)
{
  return replacer.add_replace_exprs(from_exprs, to_exprs);
}

int ObStmtExprReplacer::do_visit(ObRawExpr *&expr)
{
  return replacer.replace(expr);
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
      LOG_WARN("failed to detroy stmt expr set", K(ret));
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
