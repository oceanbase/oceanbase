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
#include "common/ob_smart_call.h"
#include "sql/resolver/expr/ob_raw_expr_copier.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObIRawExprCopier::copy(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *old_expr = expr;
  if (OB_FAIL(copy(old_expr, expr))) {
    LOG_WARN("failed to copy expr", K(ret));
  }
  return ret;
}

int ObIRawExprCopier::copy(const ObRawExpr *expr,
                           ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = NULL;
  if (OB_LIKELY(expr != NULL)) {
    if (OB_FAIL(check_need_copy(expr, new_expr))) {
      LOG_WARN("failed to check need copy", K(ret));
    } else if (new_expr != NULL) {
      // do nothing
    } else if (OB_FAIL(do_copy_expr(expr, new_expr))) {
      LOG_WARN("failed to copy expr", K(ret));
    }
  }
  return ret;
}

int ObPLExprCopier::copy_expr(ObRawExprFactory &expr_factory,
                              const ObRawExpr *old_expr,
                              ObRawExpr *&new_expr)
{
  ObPLExprCopier copier(expr_factory);
  return copier.copy(old_expr, new_expr);
}

int ObPLExprCopier::check_need_copy(const ObRawExpr *old_expr,
                                    ObRawExpr *&new_expr)
{
  UNUSED(old_expr);
  new_expr = NULL;
  return OB_SUCCESS;
}

int ObPLExprCopier::do_copy_expr(const ObRawExpr *old_expr,
                                 ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(old_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old expr is null", K(ret));
  } else if (OB_FAIL(expr_factory_.create_raw_expr(old_expr->get_expr_class(),
                                                   old_expr->get_expr_type(),
                                                   new_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_FAIL(new_expr->deep_copy(*this, *old_expr))) {
    LOG_WARN("failed to assign old expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(copy(new_expr->get_param_expr(i))))) {
        LOG_WARN("failed to copy param expr", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprCopier::check_need_copy(const ObRawExpr *old_expr,
                                     ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  int tmp = OB_SUCCESS;
  uint64_t key = reinterpret_cast<uint64_t>(old_expr);
  uint64_t val = 0;
  new_expr = NULL;
  if (OB_UNLIKELY(!copied_exprs_.created())) {
    // do nothing
  } else if (OB_HASH_EXIST == (tmp = new_exprs_.exist_refactored(key))) {
    // the old expr is copied in the current context
    new_expr = const_cast<ObRawExpr *>(old_expr);
  } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != tmp)) {
    ret = tmp;
    LOG_WARN("get expr from hash map failed", K(ret));
  } else if (OB_SUCCESS == (tmp = copied_exprs_.get_refactored(key, val))) {
    new_expr = reinterpret_cast<ObRawExpr *>(val);
  } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != tmp)) {
    ret = tmp;
    LOG_WARN("get expr from hash map failed", K(ret));
  }
  if (OB_SUCC(ret) && OB_ISNULL(new_expr) &&
      old_expr->is_exec_param_expr() &&
      !static_cast<const ObExecParamRawExpr *>(old_expr)->is_onetime()) {
    // TODO link.zt skip the copy of exec param expr
    // let the query ref raw expr to copy the expr
    // to be improved
    new_expr = const_cast<ObRawExpr *>(old_expr);
  }
  return ret;
}

int ObRawExprCopier::copy_expr_node(const ObRawExpr *expr,
                                    ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *tmp = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (ObRawExprUtils::find_expr(uncopy_expr_nodes_, expr)) {
    new_expr = const_cast<ObRawExpr *>(expr);
  } else if (OB_FAIL(expr_factory_.create_raw_expr(expr->get_expr_class(),
                                                   expr->get_expr_type(),
                                                   tmp))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_FAIL(tmp->deep_copy(*this, *expr))) {
    LOG_WARN("failed to assign old expr", K(ret));
  } else if (OB_FAIL(add_expr(expr, tmp))) {
    LOG_WARN("failed to add expr", K(ret));
  } else {
    new_expr = tmp;
  }
  return ret;
}

int ObRawExprCopier::copy_expr(ObRawExprFactory &expr_factory,
                               const ObRawExpr *old_expr,
                               ObRawExpr *&new_expr)
{
  ObRawExprCopier copier(expr_factory);
  return copier.copy(old_expr, new_expr);
}

int ObRawExprCopier::copy_expr_node(ObRawExprFactory &expr_factory,
                                    const ObRawExpr *old_expr,
                                    ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *tmp = NULL;
  ObRawExprCopier copier(expr_factory);
  if (OB_ISNULL(old_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(old_expr->get_expr_class(),
                                                  old_expr->get_expr_type(),
                                                  tmp))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_FAIL(tmp->deep_copy(copier, *old_expr))) {
    LOG_WARN("failed to assign old expr", K(ret));
  } else {
    new_expr = tmp;
  }
  return ret;
}

int ObRawExprCopier::add_expr(const ObRawExpr *from,
                              const ObRawExpr *to)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(from) || OB_ISNULL(to)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input exprs are invalid", K(ret), K(from), K(to));
  } else if (OB_UNLIKELY(!copied_exprs_.created())) {
    if (OB_FAIL(copied_exprs_.create(64, ObModIds::OB_SQL_COMPILE))) {
      LOG_WARN("failed to create expr map", K(ret));
    } else if (OB_FAIL(new_exprs_.create(64))) {
      LOG_WARN("failed to create expr set", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(copied_exprs_.set_refactored(reinterpret_cast<uint64_t>(from),
                                             reinterpret_cast<uint64_t>(to)))) {
      LOG_WARN("faield to add copied expr into map", K(ret));
    } else if (OB_FAIL(new_exprs_.set_refactored(reinterpret_cast<uint64_t>(to)))) {
      LOG_WARN("failed to add copied expr into set", K(ret));
    }
  }
  return ret;
}

int ObRawExprCopier::add_expr(const ObIArray<ObRawExpr *> &from_exprs,
                              const ObIArray<ObRawExpr *> &to_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(from_exprs.count() != to_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array size does not match",
             K(ret), K(from_exprs.count()), K(to_exprs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < from_exprs.count(); ++i) {
    if (is_existed(from_exprs.at(i))) {
      // do nothing
    } else if (OB_FAIL(add_expr(from_exprs.at(i), to_exprs.at(i)))) {
      LOG_WARN("failed to add expr", K(ret));
    }
  }
  return ret;
}

bool ObRawExprCopier::is_existed(const ObRawExpr *target) const
{
  bool bret = false;
  if (OB_LIKELY(copied_exprs_.created())) {
    uint64_t key = reinterpret_cast<uint64_t>(target);
    uint64_t val = 0;
    if (OB_SUCCESS == copied_exprs_.get_refactored(key, val)) {
      bret = true;
    }
  }
  return bret;
}

int ObRawExprCopier::do_copy_expr(const ObRawExpr *old_expr,
                                  ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = NULL;
  if (OB_ISNULL(old_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old expr is null", K(ret));
  } else if (OB_FAIL(copy_expr_node(old_expr, new_expr))) {
    LOG_WARN("failed to copy expr node", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(copy(new_expr->get_param_expr(i))))) {
        LOG_WARN("failed to copy param expr", K(ret));
      }
    }
  }
  return ret;
}

/**
 * @brief ObRawExprCopier::copy_on_replace
 * @param from_expr : the input expr to be replaced
 * @param to_expr : the output expr generaetd by replacing the input expr
 * @param replacer : tells how to replace a expr node
 * @return
 */
int ObRawExprCopier::copy_on_replace(ObRawExpr *from_expr,
                                     ObRawExpr *&to_expr,
                                     ObIRawExprReplacer *replacer /*= NULL*/)
{
  int ret = OB_SUCCESS;
  ObRawExpr *tmp = NULL;
  to_expr = NULL;
  if (OB_ISNULL(from_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input expr is null", K(ret));
  } else if (OB_FAIL(check_need_copy(from_expr, tmp))) {
    LOG_WARN("failed to check need copy", K(ret));
  } else if (NULL != tmp) {
    // the base_expr is already re-created
    to_expr = tmp;
  } else if (NULL != replacer) {
    if (OB_FAIL(replacer->generate_new_expr(expr_factory_, from_expr, tmp))) {
        LOG_WARN("failed to generate new expr", K(ret));
    } else if (NULL != tmp) {
      if (OB_FAIL(add_expr(from_expr, tmp))) {
        LOG_WARN("failed to add expr into replace map", K(ret));
      } else {
        to_expr = tmp;
      }
    }
  }

  if (OB_SUCC(ret) && NULL == to_expr) {
    to_expr = from_expr;
    for (int64_t i = 0; OB_SUCC(ret) && i < to_expr->get_param_count(); ++i) {
      ObRawExpr *param = from_expr->get_param_expr(i);
      ObRawExpr *new_param = NULL;
      if (OB_FAIL(SMART_CALL(copy_on_replace(param, new_param, replacer)))) {
        LOG_WARN("failed to static replace expr", K(ret));
      } else if (param == new_param) {
        // do noting if the param does not change, or the expr is marked as uncopy.
      } else if (from_expr == to_expr &&
                 OB_FAIL(copy_expr_node(from_expr, to_expr))) {
        // the param is changed, create a copy of the from_expr
        // and then make modifications on the copy (to_expr).
        LOG_WARN("failed to copy expr node", K(ret));
      } else {
        to_expr->get_param_expr(i) = new_param;
      }
    }
  }
  return ret;
}

int ObRawExprCopier::add_skipped_expr(const ObRawExpr *target, bool include_child)
{
  int ret = OB_SUCCESS;
  if (include_child) {
    ret = add_expr(target, target);
  } else {
    ret = uncopy_expr_nodes_.push_back(target);
  }
  return ret;
}

int ObRawExprCopier::add_replaced_expr(const ObRawExpr *from_expr,
                                       const ObRawExpr *to_expr)
{
  return add_expr(from_expr, to_expr);
}

int ObRawExprCopier::add_replaced_expr(const ObIArray<ObRawExpr *> &from_exprs,
                                       const ObIArray<ObRawExpr *> &to_exprs)
{
  return add_expr(from_exprs, to_exprs);
}

int ObRawExprCopier::get_copied_exprs(ObIArray<std::pair<ObRawExpr *, ObRawExpr *>> &from_to_exprs)
{
  int ret = OB_SUCCESS;
  for (auto it = copied_exprs_.begin(); OB_SUCC(ret) && it != copied_exprs_.end(); ++it) {
    if (OB_FAIL(from_to_exprs.push_back(
                  std::pair<ObRawExpr *, ObRawExpr *>(reinterpret_cast<ObRawExpr*>(it->first),
                                                      reinterpret_cast<ObRawExpr*>(it->second))))) {
      LOG_WARN("failed to push back from to expr", K(ret));
    }
  }
  return ret;
}
