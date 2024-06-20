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

#define USING_LOG_PREFIX SQL_REWRITE

#include "ob_transform_count_to_exists.h"
#include "share/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

int ObTransformCountToExists::TransParam::assign(const TransParam &other)
{
  int ret = OB_SUCCESS;
  subquery_expr_ = other.subquery_expr_;
  const_expr_ = other.const_expr_;
  count_param_ = other.count_param_;
  source_expr_ = other.source_expr_;
  target_expr_ = other.target_expr_;
  need_add_constraint_ = other.need_add_constraint_;
  trans_type_ = other.trans_type_;
  return ret;
}

int ObTransformCountToExists::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                 ObDMLStmt *&stmt,
                                                 bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  ObSEArray<TransParam, 8> trans_params;
  ObSEArray<ObRawExpr *, 8> cond_exprs;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(cond_exprs.assign(stmt->get_condition_exprs()))) {
    LOG_WARN("failed to assign where condition exprs", K(ret));
  } else if (OB_FAIL(collect_trans_params(stmt, cond_exprs, trans_params))) {
    LOG_WARN("failed to transform count to exists", K(ret));
  } else if (trans_params.empty()) {
    // do nothing
  } else if (OB_FAIL(do_transform(stmt, trans_params))) {
    LOG_WARN("failed to transform subquery", K(ret));
  } else {
    trans_happened = true;
    if (OB_FAIL(add_transform_hint(*stmt, &trans_params))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

int ObTransformCountToExists::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObIArray<TransParam> *org_trans_params = NULL;
  ObCountToExistsHint *hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(trans_params) ||
      OB_FALSE_IT(org_trans_params = static_cast<ObIArray<TransParam>*>(trans_params)) ||
      OB_UNLIKELY(org_trans_params->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(trans_params), K(org_trans_params));
  } else if (OB_FAIL(ctx_->add_used_trans_hint(get_hint(stmt.get_stmt_hint())))) {
    LOG_WARN("failed to add used trans hint", K(ret));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_COUNT_TO_EXISTS, hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_ISNULL(hint)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
    LOG_WARN("failed to push back hint", K(ret));
  } else {
    hint->set_qb_name(ctx_->src_qb_name_);
    for (int64_t i = 0; OB_SUCC(ret) && i < org_trans_params->count(); ++i) {
      ObQueryRefRawExpr *query_expr = org_trans_params->at(i).subquery_expr_;
      ObString qb_name;
      if (OB_ISNULL(query_expr) || OB_ISNULL(query_expr->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(query_expr));
      } else if (OB_FAIL(query_expr->get_ref_stmt()->get_qb_name(qb_name))) {
        LOG_WARN("failed to get qb name", K(ret));
      } else if (OB_FAIL(hint->get_qb_names().push_back(qb_name))) {
        LOG_WARN("failed to push back qb name", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformCountToExists::collect_trans_params(ObDMLStmt *stmt,
                                                   const ObIArray<ObRawExpr *> &cond_exprs,
                                                   ObIArray<TransParam> &trans_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  } else if (OB_ISNULL(stmt) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ctx_->expr_factory_), K(ctx_->session_info_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cond_exprs.count(); ++i) {
      ObRawExpr *expr = cond_exprs.at(i);
      bool is_valid = false;
      TransParam trans_param;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(check_trans_valid(stmt, expr, trans_param, is_valid))) {
        LOG_WARN("failed to check transform valid", K(ret));
      } else if (!is_valid) {
        // do nothing
      } else if (OB_ISNULL(trans_param.subquery_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(trans_param));
      } else {
        trans_param.subquery_expr_->set_is_set(true);
        ObRawExpr *exists_expr = NULL;
        // build exists / not exists expr
        // TRANS_INVALID never reach here
        if (trans_param.trans_type_ == TRANS_EXISTS) {
          if (OB_FAIL(ObRawExprUtils::build_exists_expr(*ctx_->expr_factory_,
                                                        ctx_->session_info_,
                                                        T_OP_EXISTS,
                                                        trans_param.subquery_expr_,
                                                        exists_expr))) {
            LOG_WARN("failed to build exists expr", K(ret));
          }
        } else if (trans_param.trans_type_ == TRANS_NOT_EXISTS) {
          if (OB_FAIL(ObRawExprUtils::build_exists_expr(*ctx_->expr_factory_,
                                                        ctx_->session_info_,
                                                        T_OP_NOT_EXISTS,
                                                        trans_param.subquery_expr_,
                                                        exists_expr))) {
            LOG_WARN("failed to build not-exists expr", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(exists_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          trans_param.source_expr_ = expr;
          trans_param.target_expr_ = exists_expr;
          ret = trans_params.push_back(trans_param);
        }
      }
    }
  }
  return ret;
}

int ObTransformCountToExists::check_trans_valid(ObDMLStmt *stmt, ObRawExpr *expr, TransParam &trans_param, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObRawExpr *subquery_param = NULL;
  ObRawExpr *val_param = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!expr->has_flag(CNT_SUB_QUERY)) {
    // do nothing
  } else if (OB_FAIL(get_trans_type(expr, val_param, subquery_param, trans_param.trans_type_))) {
    LOG_WARN("failed to get transform type", K(ret));
  } else if (OB_ISNULL(val_param) || OB_ISNULL(subquery_param) ||
             trans_param.trans_type_ == TRANS_INVALID) {
    // do nothing
  } else {
    OPT_TRACE("try", expr);
    ObQueryRefRawExpr *tmp_subquery_expr = static_cast<ObQueryRefRawExpr *>(subquery_param);
    ObSelectStmt *tmp_subquery = NULL;
    ObRawExpr *sel_expr = NULL;
    bool is_sel_expr_valid = false;
    bool is_const_expr_valid = false;
    bool has_rownum = false;
    if (OB_ISNULL(tmp_subquery = tmp_subquery_expr->get_ref_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_ISNULL(sel_expr = tmp_subquery->get_select_item(0).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null expr", K(ret));
    } else if (OB_FAIL(tmp_subquery->has_rownum(has_rownum))) {
      LOG_WARN("failed to check if subquery has rownum", K(ret));
    } else if (has_rownum) {
      // do nothing
    } else if (tmp_subquery_expr->get_ref_count() > 1 ||
               tmp_subquery->has_having() || !tmp_subquery->is_scala_group_by() ||
               tmp_subquery->has_order_by() || tmp_subquery->has_limit() ||
               tmp_subquery->has_window_function_filter()) {
      // only scalar group by subquery without having and referred by once can be transformed
      OPT_TRACE("only scalar group by subquery without having and referred by once can be transformed");
    } else if (OB_FAIL(check_sel_expr_valid(sel_expr,
                                            trans_param.count_param_,
                                            is_sel_expr_valid))) {
      LOG_WARN("failed to check select expr validity", K(ret));
    } else if (!is_sel_expr_valid) {
    } else if (OB_FAIL(check_value_zero(val_param,
                                        trans_param.need_add_constraint_,
                                        is_const_expr_valid))) {
      LOG_WARN("failed to check value zero", K(ret));
    } else if (!is_const_expr_valid) {
      // do nothing
    } else if (OB_FAIL(check_hint_valid(*stmt, *tmp_subquery, is_valid))) {
      LOG_WARN("failed to check hint valid", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else {
      trans_param.subquery_expr_ = tmp_subquery_expr;
      trans_param.const_expr_ = val_param;
    }
  }
  return ret;
}

int ObTransformCountToExists::check_hint_valid(ObDMLStmt &stmt, ObSelectStmt &subquery, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObString subquery_qb_name;
  const ObCountToExistsHint *hint = static_cast<const ObCountToExistsHint*>(get_hint(stmt.get_stmt_hint()));
  if (NULL == hint) { // do transform by default
    is_valid = true;
  } else if (OB_FAIL(subquery.get_qb_name(subquery_qb_name))) {
    LOG_WARN("failed to get qb name", K(ret));
  } else {
    is_valid = hint->enable_count_to_exists(subquery_qb_name);
    if (!is_valid) {
      OPT_TRACE("hint disable transform");
    }
    LOG_TRACE("succeed to check count_to_exists hint valid", K(is_valid),
                                                             K(subquery_qb_name), K(*hint));
  }
  return ret;
}

/**
 * @brief 
 *  0 < subquery count(*) --> exists
 *  0 > subquery count(*) --> invalid
 *  0 <= subquery count(*) -> invalid
 *  0 >= subquery count(*) -> not exists
 *  0 == subquery count(*) -> not exists
 *  subquery count(*) > 0 --> exists
 *  subquery count(*) >= 0 -> invalid
 *  subquery count(*) < 0 --> invalid
 *  subquery count(*) <= 0 -> not exists
 *  subquery count(*) == 0 -> not exists
 * @param expr 
 * @param val_param 
 * @param subquery_param 
 * @param trans_type 
 * @return int 
 */
int ObTransformCountToExists::get_trans_type(ObRawExpr *expr, ObRawExpr *&val_param,
                                             ObRawExpr *&subquery_param, TransType &trans_type)
{
  int ret = OB_SUCCESS;
  val_param = NULL;
  subquery_param = NULL;
  trans_type = TRANS_INVALID;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (expr->get_expr_type() == T_OP_GT || expr->get_expr_type() == T_OP_LT ||
             expr->get_expr_type() == T_OP_GE || expr->get_expr_type() == T_OP_LE ||
             expr->get_expr_type() == T_OP_EQ) {
    ObRawExpr *first_param = expr->get_param_expr(0);
    ObRawExpr *second_param = expr->get_param_expr(1);
    if (OB_ISNULL(first_param) || OB_ISNULL(second_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(first_param), K(second_param));
    } else if (first_param->is_static_scalar_const_expr() && second_param->is_query_ref_expr()) {
      val_param = first_param;
      subquery_param = second_param;
      switch (expr->get_expr_type()) {
        case T_OP_GE:
        case T_OP_EQ:
          trans_type = TRANS_NOT_EXISTS;
          break;
        case T_OP_LT:
          trans_type = TRANS_EXISTS;
          break;
        default:
          break;
      }
    } else if (first_param->is_query_ref_expr() && second_param->is_static_scalar_const_expr()) {
      subquery_param = first_param;
      val_param = second_param;
      switch (expr->get_expr_type()) {
        case T_OP_LE:
        case T_OP_EQ:
          trans_type = TRANS_NOT_EXISTS;
          break;
        case T_OP_GT:
          trans_type = TRANS_EXISTS;
          break;
        default:
          break;
      }
    }
  }
  return ret;
}

int ObTransformCountToExists::check_sel_expr_valid(ObRawExpr *select_expr,
                                                   ObRawExpr *&count_param,
                                                   bool &is_sel_expr_valid)
{
  int ret = OB_SUCCESS;
  is_sel_expr_valid = false;
  if (OB_ISNULL(select_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (select_expr->get_expr_type() == T_FUN_COUNT) {
    ObRawExpr *param = NULL;
    if (select_expr->get_param_count() == 0) {
      // count(*)
      is_sel_expr_valid = true;
    } else if (OB_ISNULL(param = select_expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!param->has_flag(CNT_SUB_QUERY)) {
      is_sel_expr_valid = true;
      count_param = param;
    }
  }
  if (OB_SUCC(ret) && !is_sel_expr_valid) {
    OPT_TRACE("select expr is not valid", select_expr);
  }
  return ret;
}
int ObTransformCountToExists::check_value_zero(ObRawExpr *expr,
                                               bool &need_add_constraint,
                                               bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObObj value;
  bool got_result = false;
  is_valid = false;
  need_add_constraint = false;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr), K(ctx_));
  } else if (OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_->exec_ctx_));
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                               expr,
                                                               value,
                                                               got_result,
                                                               *ctx_->allocator_))) {
    LOG_WARN("failed to calc const or calculable expr value", K(ret));
  } else if (got_result) {
    int64_t const_value = OB_INVALID_ID;
    number::ObNumber number;
    if (value.is_integer_type()) {
      const_value = value.get_int();
    } else if (value.is_number()) {
      if (OB_FAIL(value.get_number(number))) {
        LOG_WARN("failed to get number", K(ret));
      } else if (OB_UNLIKELY(!number.is_valid_int64(const_value))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("number is not valid int64", K(ret), K(value), K(number));
      }
    }
    if (OB_SUCC(ret) && const_value == 0) {
      need_add_constraint = expr->is_static_const_expr();
      is_valid = true;
    }
  }
  if (OB_SUCC(ret) && !is_valid) {
    OPT_TRACE("const expr is not zero", expr);
  }
  return ret;
}

int ObTransformCountToExists::do_transform(ObDMLStmt *stmt,
                                           const ObIArray<TransParam> &trans_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ctx_));
  } else if (OB_ISNULL(ctx_->allocator_) ||
             OB_ISNULL(ctx_->expr_factory_) ||
             OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_->allocator_),
                                    K(ctx_->expr_factory_), K(ctx_->session_info_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_params.count(); ++i) {
      TransParam trans_param = trans_params.at(i);
      ObSelectStmt *subquery = NULL;
      if (OB_ISNULL(trans_param.subquery_expr_) || OB_ISNULL(trans_param.const_expr_) ||
          OB_ISNULL(subquery = trans_param.subquery_expr_->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        if (OB_NOT_NULL(trans_param.count_param_)) {
          // select * from t1 where (select count(to_char(c1, '')) from t1) > 0;
          // ->
          // select * from t1 where exists (select 1 from t1 where to_char(c1, '') is not null);
          ObRawExpr *not_null_cond = NULL;
          bool is_not_null = true;
          ObArray<ObRawExpr *> constraints;
          if (OB_FAIL(ObTransformUtils::is_expr_not_null(ctx_, subquery, trans_param.count_param_,
                                                         NULLABLE_SCOPE::NS_WHERE,
                                                         is_not_null,
                                                         &constraints))) {
            LOG_WARN("failed to check whether expr is nullable", K(ret));
          } else if (is_not_null) {
            if (OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, constraints))) {
              LOG_WARN("failed to add param not null constraint", K(ret));
            }
          } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*ctx_->expr_factory_,
                                                                    trans_param.count_param_,
                                                                    true,
                                                                    not_null_cond))) {
            LOG_WARN("failed to build is not null expr", K(ret));
          } else if (OB_FAIL(subquery->get_condition_exprs().push_back(not_null_cond))) {
            LOG_WARN("failed to append not null cond expr", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FALSE_IT(subquery->get_select_items().reuse())) {
        } else if (OB_FALSE_IT(subquery->get_aggr_items().reuse())) {
        } else if (OB_FAIL(ObTransformUtils::create_dummy_select_item(*subquery, ctx_))) {
          LOG_WARN("failed to create dummy select item", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_condition_exprs(),
                                                        trans_param.source_expr_))) {
          LOG_WARN("failed to remove count subquery expr", K(ret));
        } else if (OB_FAIL(stmt->get_condition_exprs().push_back(trans_param.target_expr_))) {
          LOG_WARN("failed to push back exists expr", K(ret));
        } else if (OB_FAIL(subquery->formalize_stmt(ctx_->session_info_))) {
          LOG_WARN("failed to formalize stmt", K(ret));
        } else if (trans_param.need_add_constraint_ &&
                   OB_FAIL(ObTransformUtils::add_const_param_constraints(trans_param.const_expr_, ctx_))) {
          LOG_WARN("failed to add const param constraints", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}
} //namespace sql
} //namespace oceanbase