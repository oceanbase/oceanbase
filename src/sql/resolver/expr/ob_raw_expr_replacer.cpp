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
#include "ob_raw_expr_replacer.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
ObRawExprReplacer::ObRawExprReplacer()
  : replace_happened_(false), skip_bool_param_mysql_(false)
{}

ObRawExprReplacer::~ObRawExprReplacer()
{}

void ObRawExprReplacer::destroy()
{
  if (to_exprs_.created()) {
    to_exprs_.destroy();
  }
  if (expr_replace_map_.created()) {
    expr_replace_map_.destroy();
  }
}

int ObRawExprReplacer::replace(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *new_expr = NULL;
  bool need_replace = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (OB_FAIL(check_need_replace(expr, new_expr, need_replace))) {
    LOG_WARN("failed to check need replace", K(ret));
  } else if (need_replace) {
    expr = new_expr;
    replace_happened_ = true;
  } else if (OB_FAIL(expr->preorder_accept(*this))) {
    LOG_WARN("failed to preorder accept expr", K(ret));
  }
  return ret;
}

int ObRawExprReplacer::visit(ObConstRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObExecParamRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObVarRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObOpPseudoColumnRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObQueryRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  bool skip_expr = false;
  if (OB_FAIL(check_skip_expr(expr, skip_expr))) {
    LOG_WARN("failed to check skip expr");
  } else if (!skip_expr) {
    ObRawExpr *new_expr = NULL;
    bool need_replace = false;
    int64_t count = expr.get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(check_need_replace(expr.get_param_expr(i), new_expr, need_replace))) {
        LOG_WARN("failed to check need replace", K(ret));
      } else if (need_replace) {
        expr.get_param_expr(i) = new_expr;
        replace_happened_ = true;
      }
    }
  }
  return ret;
}

int ObRawExprReplacer::visit(ObPlQueryRefRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObColumnRefRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  bool skip_expr = false;
  if (OB_FAIL(check_skip_expr(expr, skip_expr))) {
    LOG_WARN("failed to check skip expr");
  } else if (!skip_expr) {
    ObRawExpr *new_expr = NULL;
    bool need_replace = false;
    int64_t count = expr.get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(check_need_replace(expr.get_param_expr(i), new_expr, need_replace))) {
        LOG_WARN("failed to check need replace", K(ret));
      } else if (is_skip_bool_param_mysql()) {
        if (T_OP_IS == expr.get_expr_type() && i == 1) {
          //do nothing
        } else if (T_OP_IS_NOT == expr.get_expr_type() && i == 1) {
          //do nothing
        } else if (need_replace) {
          ret = expr.replace_param_expr(i, new_expr);
          replace_happened_ = true;
        }
      } else if (need_replace) {
        ret = expr.replace_param_expr(i, new_expr);
        replace_happened_ = true;
      }
    }
  }
  return ret;
}

int ObRawExprReplacer::visit(ObAliasRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  bool skip_expr = false;
  if (OB_FAIL(check_skip_expr(expr, skip_expr))) {
    LOG_WARN("failed to check skip expr");
  } else if (!skip_expr) {
    ObRawExpr *new_expr = NULL;
    bool need_replace = false;
    if (OB_FAIL(check_need_replace(expr.get_ref_expr(), new_expr, need_replace))) {
      LOG_WARN("failed to check need replace", K(ret));
    } else if (need_replace) {
      expr.set_ref_expr(new_expr);
      replace_happened_ = true;
    }
  }
  return ret;
}

int ObRawExprReplacer::visit(ObPseudoColumnRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObCaseOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  bool skip_expr = false;
  if (expr.get_when_expr_size() != expr.get_then_expr_size()) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(check_skip_expr(expr, skip_expr))) {
    LOG_WARN("failed to check skip expr");
  } else if (!skip_expr) {
    ObRawExpr *new_expr = NULL;
    bool need_replace = false;
    if (OB_FAIL(check_need_replace(expr.get_arg_param_expr(), new_expr, need_replace))) {
      LOG_WARN("failed to check need replace", K(ret));
    } else if (need_replace) {
      expr.set_arg_param_expr(new_expr);
      replace_happened_ = true;
    }

    int64_t count = expr.get_when_expr_size();
    for (int32_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(check_need_replace(expr.get_when_param_expr(i), new_expr, need_replace))) {
        LOG_WARN("failed to check need replace", K(ret));
      } else if (need_replace) {
        expr.replace_when_param_expr(i, new_expr);
        replace_happened_ = true;
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(check_need_replace(expr.get_then_param_expr(i), new_expr, need_replace))) {
        LOG_WARN("failed to check need replace", K(ret));
      } else if (need_replace) {
        expr.replace_then_param_expr(i, new_expr);
        replace_happened_ = true;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_need_replace(expr.get_default_param_expr(), new_expr, need_replace))) {
        LOG_WARN("failed to check need replace", K(ret));
      } else if (need_replace) {
        expr.set_default_param_expr(new_expr);
        replace_happened_ = true;
      }
    }
  }
  return ret;
}

int ObRawExprReplacer::visit(ObAggFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  bool skip_expr = false;
  if (OB_FAIL(check_skip_expr(expr, skip_expr))) {
    LOG_WARN("failed to check skip expr");
  } else if (!skip_expr) {
    ObRawExpr *new_expr = NULL;
    bool need_replace = false;
    ObIArray<ObRawExpr*> &real_param_exprs = expr.get_real_param_exprs_for_update();
    for (int64_t i = 0; OB_SUCC(ret) && i < real_param_exprs.count(); ++i) {
      if (OB_FAIL(check_need_replace(real_param_exprs.at(i), new_expr, need_replace))) {
        LOG_WARN("failed to check need replace", K(ret));
      } else if (need_replace) {
        real_param_exprs.at(i) = new_expr;
        replace_happened_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      ObIArray<OrderItem> &order_items = expr.get_order_items_for_update();
      for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
        if (OB_FAIL(check_need_replace(order_items.at(i).expr_, new_expr, need_replace))) {
          LOG_WARN("failed to check need replace", K(ret));
        } else if (need_replace) {
          order_items.at(i).expr_ = new_expr;
          replace_happened_ = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_need_replace(expr.get_pl_agg_udf_expr(), new_expr, need_replace))) {
        LOG_WARN("failed to check need replace", K(ret));
      } else if (need_replace) {
        expr.set_pl_agg_udf_expr(new_expr);
        replace_happened_ = true;
      }
    }
  }
  return ret;
}

int ObRawExprReplacer::visit(ObSysFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  bool skip_expr = false;
  if (OB_FAIL(check_skip_expr(expr, skip_expr))) {
    LOG_WARN("failed to check skip expr");
  } else if (!skip_expr) {
    ObRawExpr *new_expr = NULL;
    bool need_replace = false;
    int64_t count = expr.get_param_count();
    for (int32_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(check_need_replace(expr.get_param_expr(i), new_expr, need_replace))) {
        LOG_WARN("failed to check need replace", K(ret));
      } else if (need_replace) {
        ret = expr.replace_param_expr(i, new_expr);
        replace_happened_ = true;
      }
    }
  }
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObSetOpRawExpr &expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObRawExprReplacer::visit(ObWinFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  bool skip_expr = false;
  if (OB_FAIL(check_skip_expr(expr, skip_expr))) {
    LOG_WARN("failed to check skip expr");
  } else if (!skip_expr) {
    ObRawExpr *new_expr = NULL;
    bool need_replace = false;
    int64_t count = expr.get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(check_need_replace(expr.get_param_expr(i), new_expr, need_replace))) {
        LOG_WARN("failed to check need replace", K(ret));
      } else if (need_replace) {
        expr.get_param_expr(i) = new_expr;
        replace_happened_ = true;
      }
    }
  }
  return ret;
}

bool ObRawExprReplacer::skip_child(ObRawExpr &expr)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_skip_expr(expr, bret))) {
    LOG_WARN("failed to check skip expr", K(ret));
  }
  return bret;
}

int ObRawExprReplacer::add_replace_expr(ObRawExpr *from_expr,
                                        ObRawExpr *to_expr,
                                        bool overwrite /*false*/)
{
  int ret = OB_SUCCESS;
  bool is_existed = false;
  if (OB_ISNULL(from_expr) || OB_ISNULL(to_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", KP(from_expr), KP(to_expr), K(ret));
  } else if (OB_FAIL(try_init_expr_map(DEFAULT_BUCKET_SIZE))) {
    LOG_WARN("failed to init expr map", K(ret));
  } else if (OB_FAIL(check_from_expr_existed(from_expr, to_expr, overwrite, is_existed))) {
    LOG_WARN("failed to check from expr existed", K(ret));
  } else if (is_existed) {
    // do not add duplicated replace expr
  } else if (OB_FAIL(expr_replace_map_.set_refactored(reinterpret_cast<uint64_t>(from_expr),
                                                      reinterpret_cast<uint64_t>(to_expr),
                                                      overwrite))) {
    LOG_WARN("failed to add replace expr into map", K(ret));
  } else if (OB_FAIL(to_exprs_.set_refactored(reinterpret_cast<uint64_t>(to_expr)))) {
    LOG_WARN("failed to add replace expr into set", K(ret));
  }
  return ret;
}

int ObRawExprReplacer::add_replace_exprs(const ObIArray<ObRawExpr *> &from_exprs,
                                         const ObIArray<ObRawExpr *> &to_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(from_exprs.count() != to_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr size mismatch", K(from_exprs.count()), K(to_exprs.count()), K(ret));
  } else if (OB_FAIL(try_init_expr_map(from_exprs.count()))) {
    LOG_WARN("failed to init expr map", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < from_exprs.count(); ++i) {
    if (OB_FAIL(add_replace_expr(from_exprs.at(i), to_exprs.at(i)))) {
      LOG_WARN("failed to add replace expr", K(ret));
    }
  }
  return ret;
}

int ObRawExprReplacer::add_replace_exprs(
    const ObIArray<std::pair<ObRawExpr *, ObRawExpr *>> &to_replace_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_init_expr_map(to_replace_exprs.count()))) {
    LOG_WARN("failed to init expr map", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < to_replace_exprs.count(); ++i) {
    if (OB_FAIL(add_replace_expr(to_replace_exprs.at(i).first, to_replace_exprs.at(i).second))) {
      LOG_WARN("failed to add replace expr", K(ret));
    }
  }
  return ret;
}

int ObRawExprReplacer::append_replace_exprs(const ObRawExprReplacer &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_init_expr_map(other.expr_replace_map_.size()))) {
    LOG_WARN("failed to init expr map", K(ret));
  }
  for (auto it = other.expr_replace_map_.begin();
       OB_SUCC(ret) && it != other.expr_replace_map_.end(); ++it) {
    if (OB_FAIL(add_replace_expr(reinterpret_cast<ObRawExpr *>(it->first),
                                 reinterpret_cast<ObRawExpr *>(it->second)))) {
      LOG_WARN("failed to push back from to expr", K(ret));
    }
  }
  return ret;
}

int ObRawExprReplacer::try_init_expr_map(int64_t bucket_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!expr_replace_map_.created())) {
    bucket_size = MAX(bucket_size, DEFAULT_BUCKET_SIZE);
    if (OB_FAIL(expr_replace_map_.create(bucket_size, ObModIds::OB_SQL_COMPILE))) {
      LOG_WARN("failed to create expr map", K(ret));
    } else if (OB_FAIL(to_exprs_.create(bucket_size))) {
      LOG_WARN("failed to create expr set", K(ret));
    }
  }
  return ret;
}

int ObRawExprReplacer::check_from_expr_existed(const ObRawExpr *from_expr,
                                               const ObRawExpr *to_expr,
                                               const bool overwrite,
                                               bool &is_existed)
{
  int ret = OB_SUCCESS;
  ObRawExpr *old_expr = NULL;
  if (OB_FAIL(check_need_replace(from_expr, old_expr, is_existed))) {
    LOG_WARN("failed to check need replace", K(ret));
  } else if (!is_existed) {
    // do nothing
  } else if (OB_UNLIKELY(old_expr != to_expr)) {
    if (overwrite) {
      is_existed = false;
    }
  }
  return ret;
}

int ObRawExprReplacer::check_skip_expr(const ObRawExpr &expr, bool &skip_expr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  skip_expr = false;
  uint64_t key = reinterpret_cast<uint64_t>(&expr);
  if (OB_UNLIKELY(!to_exprs_.created())) {
    skip_expr = true;
  } else if (OB_HASH_EXIST == (tmp_ret = to_exprs_.exist_refactored(key))) {
    skip_expr = true;
  } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != tmp_ret)) {
    ret = tmp_ret;
    LOG_WARN("failed to get expr from set", K(ret));
  }
  return ret;
}

int ObRawExprReplacer::check_need_replace(const ObRawExpr *old_expr,
                                          ObRawExpr *&new_expr,
                                          bool &need_replace)
{
  int ret = OB_SUCCESS;
  uint64_t key = reinterpret_cast<uint64_t>(old_expr);
  uint64_t val = 0;
  need_replace = false;
  if (OB_UNLIKELY(!expr_replace_map_.created())) {
    // do nothing
  } else if (OB_FAIL(expr_replace_map_.get_refactored(key, val))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get expr from hash map", K(ret));
    }
  } else {
    need_replace = true;
    new_expr = reinterpret_cast<ObRawExpr *>(val);
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase
