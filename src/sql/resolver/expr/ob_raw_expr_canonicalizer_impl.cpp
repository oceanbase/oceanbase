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
#include "sql/resolver/expr/ob_raw_expr_canonicalizer_impl.h"
#include "lib/json/ob_json_print_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "sql/ob_sql_context.h"
#include "common/ob_smart_call.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
ObRawExprCanonicalizerImpl::ObRawExprCanonicalizerImpl(ObExprResolveContext &ctx)
    : ctx_(ctx)
{}

int ObRawExprCanonicalizerImpl::canonicalize(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(push_not(expr))) {
    LOG_WARN("push not expr failed", K(ret));
  } else if (OB_FAIL(cluster_and_or(expr))) {
    LOG_WARN("cluster and or failed", K(ret));
  } else if (OB_FAIL(pull_similar_expr(expr))) {
    LOG_WARN("pull similar expr failed", K(ret));
  } else if (OB_FAIL(expr->extract_info())) {
    LOG_WARN("failed to extract info", K(ret), K(*expr));
  }
  return ret;
}

/**
 * @brief ObRawExprCanonicalizerImpl::cluster_and_or
 *   and(and(a,b),c) => and (a,b,c)
 *   or(or(a,b),c) => or(a,b,c)
 * @param expr
 * @return
 */
int ObRawExprCanonicalizerImpl::cluster_and_or(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(SMART_CALL(cluster_and_or(expr->get_param_expr(i))))) {
      LOG_WARN("failed to cluster and or", K(ret));
    }
  }
  if (OB_SUCC(ret) && (expr->get_expr_type() == T_OP_AND ||
                       expr->get_expr_type() == T_OP_OR)) {
    ObSEArray<ObRawExpr *, 4> new_param_exprs;
    const ObItemType expr_type = expr->get_expr_type();
    bool is_valid = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ObRawExpr *param = NULL;
      if (OB_ISNULL(param = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else if (param->get_expr_type() == expr_type) {
        is_valid = true;
        if (OB_FAIL(append(new_param_exprs,
                           static_cast<ObOpRawExpr *>(param)->get_param_exprs()))) {
          LOG_WARN("failed to append param exprs", K(ret));
        }
      } else if (OB_FAIL(new_param_exprs.push_back(param))) {
        LOG_WARN("failed to push back param", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      ObOpRawExpr *op_expr = static_cast<ObOpRawExpr *>(expr);
      if (OB_FAIL(op_expr->get_param_exprs().assign(new_param_exprs))) {
        LOG_WARN("failed to assign new param exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprCanonicalizerImpl::pull_similar_expr(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(SMART_CALL(pull_similar_expr(expr->get_param_expr(i))))) {
      LOG_WARN("failed to pull similar expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && (expr->get_expr_type() == T_OP_OR ||
                       expr->get_expr_type() == T_OP_AND)) {
    // A OR A => A, A and A => A
    if (OB_FAIL(remove_duplicate_conds(expr))) {
      LOG_WARN("failed to remove duplicate condition", K(ret));
    } else if (expr->get_expr_type() == T_OP_OR) {
      if (OB_FAIL(pull_and_factor(expr))) {
        LOG_WARN("failed to pull and factor", K(ret));
      }
    } else if (expr->get_expr_type() == T_OP_AND) {
      if (OB_FAIL(pull_parallel_expr(expr))) {
        LOG_WARN("failed to pull parallel expr", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprCanonicalizerImpl::remove_duplicate_conds(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op expr is null", K(ret), K(expr));
  } else if (expr->has_flag(CNT_OUTER_JOIN_SYMBOL)) {
    // do nothing
  } else if (expr->get_expr_type() == T_OP_OR ||
             expr->get_expr_type() == T_OP_AND) {
    ObOpRawExpr *op_expr = static_cast<ObOpRawExpr *>(expr);
    ObSEArray<ObRawExpr *, 4> param_conds;
    for (int64_t i = 0; OB_SUCC(ret) && i < op_expr->get_param_count(); ++i) {
      if (OB_FAIL(add_var_to_array_no_dup(param_conds, op_expr->get_param_expr(i)))) {
        LOG_WARN("failed to append array no dup", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (param_conds.count() == op_expr->get_param_count()) {
        // do nothing
      } else if (param_conds.count() == 1) {
        expr = param_conds.at(0);
      } else if (OB_FAIL(op_expr->get_param_exprs().assign(param_conds))) {
        LOG_WARN("failed to assign param conditions", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprCanonicalizerImpl::pull_and_factor(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != expr) && T_OP_OR == expr->get_expr_type()) {
    ObRawExpr *short_expr = NULL;
    int64_t factor_num = INT64_MAX;
    // continue processing when one 'and'(ObOpRawExpr) exist at least
    bool do_handle = false;
    bool expr_copied = false;
    ObOpRawExpr *m_expr = static_cast<ObOpRawExpr *>(expr);
    for (int64_t i = 0; OB_SUCC(ret) && i < m_expr->get_param_count(); ++i) {
      const ObRawExpr *and_expr = m_expr->get_param_expr(i);
      // at least one 'and' expr
      if (OB_ISNULL(and_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("and_expr is null");
      } else {
        if (and_expr->get_param_count() > 1) {
          do_handle = true;
        }
        if (T_OP_AND != and_expr->get_expr_type()) {
          short_expr = m_expr->get_param_expr(i);
          factor_num = 1;
        } else if (and_expr->get_param_count() < factor_num) {
          short_expr = m_expr->get_param_expr(i);
          factor_num = and_expr->get_param_count();
        }
        // find 'and' expr and short shortest expr
        // do not need to go on
        if (do_handle && 1 == factor_num) {
          break;
        }
      }
    }
    if (OB_SUCC(ret) && do_handle) {
      bool is_match = false;
      ObQueryCtx *query_ctx = NULL;
      ObStmtCompareContext stmt_compare_context;
      ObArray<ObRawExpr *> candidate_factors;
      if (OB_ISNULL(short_expr) ||
          OB_ISNULL(ctx_.stmt_) ||
          OB_ISNULL(query_ctx = ctx_.stmt_->get_query_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(short_expr),
            K(ctx_.stmt_), K(query_ctx));
      } else if (T_OP_AND == short_expr->get_expr_type()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < short_expr->get_param_count(); ++i) {
          if (OB_FAIL(candidate_factors.push_back(short_expr->get_param_expr(i)))) {
            LOG_WARN("construct candidate factors failed", K(ret));
          }
        }
      } else if (OB_FAIL(candidate_factors.push_back(short_expr))) {
        LOG_WARN("cons candidate factors failed", K(ret));
      }
      if (OB_SUCC(ret)) {
        ObOpRawExpr *new_and = NULL;
        stmt_compare_context.init(&query_ctx->calculable_items_);
        if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_AND, new_and))) {
          LOG_WARN("alloc ObOpRawExpr failed", K(ret));
        } else if (OB_ISNULL(new_and)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("new_and is null");
        } else {
          new_and->set_data_type(ObTinyIntType);
        }
        bool need_or = true;
        ObSEArray<int64_t, 4> idxs;
        ObSEArray<ObPCParamEqualInfo, 8> equal_params;
        for (int64_t i = 0; OB_SUCC(ret) && need_or && i < candidate_factors.count(); ++i) {
          idxs.reset();
          equal_params.reset();
          if (OB_ISNULL(candidate_factors.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("candidate factor param expr is null", K(i));
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < m_expr->get_param_count(); ++j) {
            ObOpRawExpr *and_expr = NULL;
            // 1. ignore NULL 'or expr'
            if (OB_ISNULL(m_expr->get_param_expr(j))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("m_expr param is null", K(j), K(*m_expr));
            }
            // 2. seem whole none-'or expr' as integrity
            //     E.g.  A or (A and B) ==> A
            else if (T_OP_AND != m_expr->get_param_expr(j)->get_expr_type()) {
              if (OB_FAIL(ObStmtComparer::is_same_condition(candidate_factors.at(i),
                                                            m_expr->get_param_expr(j),
                                                            stmt_compare_context,
                                                            is_match))) {
                LOG_WARN("failed to check is condition equal", K(ret));
              } else if (!is_match) {
                /*do nothing*/
              } else if (m_expr->has_flag(CNT_OUTER_JOIN_SYMBOL)) {
                /* don't eliminate or expr in (+), to keep the error information compatible with oracle
                *  e,g,. select * from t1,t2 where t1.c1(+) = t2.c1 or t1.c1(+) = t2.c1 should raise ORA-1719 
                */
              } else if (OB_FAIL(append(equal_params,
                                        stmt_compare_context.equal_param_info_))) {
                LOG_WARN("failed to append expr", K(ret));
              } else if (OB_FAIL(idxs.push_back(-1 /*and factor index*/))) {
                LOG_WARN("failed to push back pos", K(ret));
              } else { /*do nothing*/ }
            }
            // 3. find factor in and list
            else { // and_expr != NULL
              and_expr = static_cast<ObOpRawExpr *>(m_expr->get_param_expr(j));
              for (int64_t k = 0; OB_SUCC(ret) && k < and_expr->get_param_count(); ++k) {
                if (OB_FAIL(ObStmtComparer::is_same_condition(candidate_factors.at(i),
                                                              and_expr->get_param_expr(k),
                                                              stmt_compare_context,
                                                              is_match))) {
                  LOG_WARN("failed to check is condition equal", K(ret));
                } else if (!is_match) {
                  /*do nothing*/
                } else if (OB_FAIL(append(equal_params,
                                          stmt_compare_context.equal_param_info_))) {
                  LOG_WARN("failed to append expr", K(ret));
                } else if (OB_FAIL(idxs.push_back(k))) {
                  LOG_WARN("failed to push back pos", K(ret));
                } else {
                  break;
                }
              }
            }
          }
          /* is and factor */
          if (OB_SUCC(ret) && idxs.count() == m_expr->get_param_count()) {
            // 1. add and factor to new and expr
            if (OB_FAIL(new_and->add_param_expr(candidate_factors.at(i)))) {
              LOG_WARN("failed to add param expr", K(ret));
            }
            // 2. remove from or
            if (OB_SUCC(ret) && !expr_copied) {
              // deep copy T_OP_OR and T_OP_AND to make expr unshared
              ObRawExprCopier expr_copier(ctx_.expr_factory_);
              ObOpRawExpr *new_or_expr = NULL;
              ObArray<ObRawExpr *> new_candidate_factors;
              if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_OR, new_or_expr))) {
                LOG_WARN("alloc ObOpRawExpr failed", K(ret));
              } else if (OB_ISNULL(new_or_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("new_or_expr is null");
              }
              for (int64_t j = 0; OB_SUCC(ret) && j < m_expr->get_param_count(); ++j) {
                ObRawExpr *new_or_param = m_expr->get_param_expr(j);
                if (T_OP_AND == new_or_param->get_expr_type() &&
                    OB_FAIL(expr_copier.copy_expr_node(new_or_param, new_or_param))) {
                  LOG_WARN("failed to copy expr node", K(ret));
                } else if (OB_FAIL(new_or_expr->add_param_expr(new_or_param))) {
                  LOG_WARN("failed to add param expr", K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                m_expr = new_or_expr;
                expr = new_or_expr;
                expr_copied = true;
              }
            }
            for (int64_t j = 0; OB_SUCC(ret) && j < idxs.count(); ++j) {
              ObOpRawExpr *and_expr = NULL;
              if (OB_ISNULL(m_expr->get_param_expr(j))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("m_expr is null");
              } else if (idxs.at(j) < 0) {
                // whole or expr is and factor
                // no need to consider OR condition any more
                // E.g.  A or (A and B) ==> A
                need_or = false;
              } else if (T_OP_AND != m_expr->get_param_expr(j)->get_expr_type()) {
                // only 1 factor, but not and factor. idxs.count() == m_expr->get_param_count() can not be satisfied
                ret = OB_ERR_UNEXPECTED;
              } else {
                // must be and factor in and list
                and_expr = static_cast<ObOpRawExpr *>(m_expr->get_param_expr(j));
                if (OB_FAIL(and_expr->remove_param_expr(idxs.at(j)))) {
                  LOG_WARN("remove param expr failed", K(ret));
                } else if (1 == and_expr->get_param_count()) {
                  m_expr->get_param_expr(j) = and_expr->get_param_expr(0);
                }
              }
            }
            //这里会导致m_expr的树形结构发生变化，所以and or关系也有可能发生变化，所以需要做一次相邻 and or的提升
            if (OB_SUCC(ret)) {
              if (OB_FAIL(append(query_ctx->all_equal_param_constraints_, equal_params))) {
                LOG_WARN("failed to append expr", K(ret));
              } else {
                ret = cluster_and_or(expr);
                LOG_DEBUG("succeed to extract common expression", K(*expr), K(equal_params));
              }
            }
          }
        }
        // and factor is found
        if (OB_SUCC(ret) && new_and->get_param_count() > 0) {
          if (need_or) {
            if (OB_FAIL(new_and->add_param_expr(m_expr))) {
              LOG_WARN("failed to add param expr", K(ret));
            } else {
              expr = new_and;
            }
          } else if (1 == new_and->get_param_count() && !expr->has_flag(CNT_OUTER_JOIN_SYMBOL)) {
            ObRawExpr *const_false = NULL;
            ObSEArray<ObRawExpr*,2> new_exprs;
            if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(&ctx_.expr_factory_, const_false, false))) {
              LOG_WARN("failed to build const expr", K(ret));
            } else if (OB_FAIL(new_exprs.push_back(const_false))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else if (OB_FAIL(new_exprs.push_back(new_and->get_param_expr(0)))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else if (OB_FAIL(ObRawExprUtils::build_or_exprs(ctx_.expr_factory_, new_exprs, expr))) {
              LOG_WARN("failed to build or expr", K(ret));
            }
          } else {
            expr = new_and;
          }
        }
      }
    }
  }
  return ret;
}

int ObRawExprCanonicalizerImpl::pull_parallel_expr(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (expr && (expr->get_expr_type() == T_OP_AND || expr->get_expr_type() == T_OP_OR)) {
    bool has_sub = false;
    ObRawExpr::ExprClass expr_class = expr->get_expr_class();
    if (ObRawExpr::EXPR_OPERATOR != expr_class){
      LOG_WARN("Unexpected expression type", K(expr_class));
    } else {
      ObOpRawExpr *parent_expr = static_cast<ObOpRawExpr *>(expr);
      for (int64_t i = 0; OB_SUCC(ret) && i < parent_expr->get_param_count(); ++i) {
        ObRawExpr *sub_expr = parent_expr->get_param_expr(i);
        if (sub_expr->get_expr_type() == parent_expr->get_expr_type()) {
          if (OB_FAIL(SMART_CALL(pull_parallel_expr(sub_expr)))) {
            LOG_WARN("Cluster AND or OR expression failed");
          } else {
            has_sub = true;
          }
        }
      }
      if (OB_SUCC(ret) && has_sub) {
        ObOpRawExpr* tmp = nullptr;
        if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(parent_expr->get_expr_type(), tmp))) {
          LOG_WARN("failed to create raw expr", K(ret));
        } else if (OB_ISNULL(tmp)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null expr", K(ret));
        } else if (OB_FAIL(tmp->assign(*parent_expr))) {
          LOG_WARN("failed to assign expr", K(ret));
        } else {
          parent_expr->clear_child();
          for (int64_t i = 0; OB_SUCC(ret) && i < tmp->get_param_count(); ++i) {
            ObRawExpr *sub_expr = tmp->get_param_expr(i);
            if (OB_ISNULL(sub_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("sub_expr is null", K(i));
            } else if (sub_expr->get_expr_type() == parent_expr->get_expr_type()) {
              ObRawExpr::ExprClass sub_expr_class = sub_expr->get_expr_class();
              if (ObRawExpr::EXPR_OPERATOR != sub_expr_class) {
                LOG_WARN("Unexpect Expression class", K(sub_expr_class));
              } else {
                ObOpRawExpr *child_expr = static_cast<ObOpRawExpr *>(sub_expr);
                for (int64_t j = 0; OB_SUCC(ret) && j < child_expr->get_param_count(); j++) {
                  if (OB_FAIL(parent_expr->add_param_expr(child_expr->get_param_expr(j)))) {
                    LOG_WARN("Pull AND or OR expression failed", K(ret));
                  }
                }
              }
            } else {
              if (OB_FAIL(parent_expr->add_param_expr(sub_expr))) {
                LOG_WARN("Pull AND or OR expression failed");
              }
            }
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(tmp)) {
          tmp->reset();
        }
      }
      if (OB_SUCC(ret)) {
        if (expr->get_expr_type() == T_OP_AND && expr->get_param_count() == 1) {
          expr = expr->get_param_expr(0);
        }
      }
    }
  }
  return ret;
}

int ObRawExprCanonicalizerImpl::push_not(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (expr != NULL && expr->has_flag(CNT_NOT)) {
    if (expr->has_flag(IS_NOT)) {
      if (OB_FAIL(do_push_not(expr))) {
        LOG_WARN("failed to do push not expr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(push_not(expr->get_param_expr(i))))) {
        LOG_WARN("failed to push not expr", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprCanonicalizerImpl::do_push_not(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *child = NULL;
  ObItemType opp_type = T_MIN_OP;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (!expr->is_op_expr() || expr->get_expr_type() != T_OP_NOT) {
    // do nothing
  } else if (OB_ISNULL(child = expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child expr is null", K(ret));
  } else if (!child->is_op_expr()) {
    // do nothing
  } else if (child->get_expr_type() == T_OP_NOT) {
    ObRawExpr *param = NULL;
    if (OB_ISNULL(param = child->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param is null", K(ret));
    } else if (!IS_BOOL_OP(param->get_expr_type())) {
      // do nothing
    } else if (OB_FAIL(push_not(param))) {
      LOG_WARN("failed to push not", K(ret));
    } else {
      expr = param;
    }
  } else if (child->get_expr_type() == T_OP_OR ||
             child->get_expr_type() == T_OP_AND) {
    ObItemType new_type = (child->get_expr_type() == T_OP_OR) ? T_OP_AND : T_OP_OR;
    ObOpRawExpr *new_expr = NULL;
    if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(new_type, new_expr))) {
      LOG_WARN("failed to create raw expr", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child->get_param_count(); ++i) {
      ObOpRawExpr *not_expr = NULL;
      if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_NOT, not_expr))) {
        LOG_WARN("failed to create not expr", K(ret));
      } else if (not_expr->add_param_expr(child->get_param_expr(i))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else if (OB_FAIL(new_expr->add_param_expr(not_expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else if (OB_FAIL(not_expr->add_flag(IS_NOT))) {
        LOG_WARN("failed to add not flag", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      expr = new_expr;
    }
  } else if ((opp_type = get_opposite_op(child->get_expr_type())) != T_MIN_OP) {
    ObItemType new_type = opp_type;
    ObOpRawExpr *new_expr = NULL;
    if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(new_type, new_expr))) {
      LOG_WARN("failed to create raw expr", K(ret));
    } else if (OB_FAIL(new_expr->get_param_exprs().assign(
                         static_cast<ObOpRawExpr*>(child)->get_param_exprs()))) {
      LOG_WARN("failed to assign param exprs", K(ret));
    } else {
      expr = new_expr;
    }
  }
  return ret;
}

ObRawExprCanonicalizerImpl::ObOppositeOpPair ObRawExprCanonicalizerImpl::OPPOSITE_PAIRS[] =
{
  // T_MIN_OP means no opposite operator
  {T_OP_EQ, T_OP_NE},
  {T_OP_LE, T_OP_GT},
  {T_OP_LT, T_OP_GE},
  {T_OP_GE, T_OP_LT},
  {T_OP_GT, T_OP_LE},
  {T_OP_NE, T_OP_EQ},
  {T_OP_IS, T_OP_IS_NOT},
  {T_OP_IS_NOT, T_OP_IS},
  {T_OP_BTW, T_OP_NOT_BTW},
  {T_OP_NOT_BTW, T_OP_BTW},
  {T_OP_NOT, T_MIN_OP},
  {T_OP_AND, T_MIN_OP},
  {T_OP_OR, T_MIN_OP},
  {T_OP_IN, T_OP_NOT_IN},
  {T_OP_NOT_IN, T_OP_IN},
  {T_OP_NSEQ, T_MIN_OP},
};

ObItemType ObRawExprCanonicalizerImpl::get_opposite_op(ObItemType type)
{
  ObItemType ret = T_MIN_OP;
  int32_t low = 0;
  int32_t mid = 0;
  int32_t high = ARRAYSIZEOF(OPPOSITE_PAIRS) - 1;
  while (low <= high) {
    mid = low + (high - low) / 2;
    if (OPPOSITE_PAIRS[mid].original_ == type) {
      ret = OPPOSITE_PAIRS[mid].opposite_;
      break;
    } else if (OPPOSITE_PAIRS[mid].original_ > type) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
