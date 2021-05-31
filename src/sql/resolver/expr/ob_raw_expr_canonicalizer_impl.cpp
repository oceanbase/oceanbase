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
namespace oceanbase {
using namespace common;
namespace sql {
ObRawExprCanonicalizerImpl::ObRawExprCanonicalizerImpl(ObExprResolveContext& ctx) : ctx_(ctx)
{}

int ObRawExprCanonicalizerImpl::canonicalize(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(push_not(expr))) {
    LOG_WARN("push not expr failed", K(ret));
  } else if (OB_FAIL(cluster_and_or(expr))) {
    LOG_WARN("cluster and or failed", K(ret));
  } else if (OB_FAIL(pull_similar_expr(expr))) {
    LOG_WARN("pull similar expr failed", K(ret));
  }
  return ret;
}

int ObRawExprCanonicalizerImpl::cluster_and_or(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  bool is_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_overflow))) {
    LOG_ERROR("fail to check stack overflow", K(ret));
  } else if (is_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else if (OB_NOT_NULL(expr)) {
    if (expr->get_expr_type() == T_OP_AND || expr->get_expr_type() == T_OP_OR) {
      ObRawExpr::ExprClass expr_class = expr->get_expr_class();
      if (ObRawExpr::EXPR_OPERATOR == expr_class) {
        ObOpRawExpr* parent_expr = static_cast<ObOpRawExpr*>(expr);
        ObOpRawExpr* tmp = NULL;
        if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(parent_expr->get_expr_type(), tmp))) {
          LOG_WARN("create op raw expr failed", KPC(parent_expr));
        } else if (OB_FAIL(tmp->assign(*parent_expr))) {
          LOG_WARN("assign parent expr failed", K(ret));
        }
        parent_expr->clear_child();
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp->get_param_count(); i++) {
          ObRawExpr* sub_expr = tmp->get_param_expr(i);
          if (OB_ISNULL(sub_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sub expr is null");
          } else if (OB_FAIL(SMART_CALL(cluster_and_or(sub_expr)))) {
            LOG_WARN("Cluster AND or OR expression failed");
          } else if (sub_expr->get_expr_type() == parent_expr->get_expr_type()) {
            // and(and(a,b),c) -> and(a,b,c)
            ObRawExpr::ExprClass sub_expr_class = sub_expr->get_expr_class();
            if (ObRawExpr::EXPR_OPERATOR == sub_expr_class) {
              ObOpRawExpr* child_expr = static_cast<ObOpRawExpr*>(sub_expr);
              for (int64_t j = 0; OB_SUCC(ret) && j < child_expr->get_param_count(); ++j) {
                if (OB_FAIL(parent_expr->add_param_expr(child_expr->get_param_expr(j)))) {
                  LOG_WARN("Cluster AND or OR expression failed", K(ret));
                }
              }
              if (child_expr != NULL) {
                child_expr->reset();
              }
            } else {
              LOG_WARN("expression type mismatch , expect ObOpRawExpr", K(sub_expr_class));
            }
          } else {
            if (OB_FAIL(parent_expr->add_param_expr(sub_expr))) {
              LOG_WARN("failed to add param expr", K(ret));
            }
          }
        }  // end for
        if (tmp != NULL) {
          tmp->reset();
        }
      } else {
        LOG_WARN("expression type mismatch , expect ObOpRawExpr", K(expr_class));
      }
    } else {
      ret = process_children(expr, &ObRawExprCanonicalizerImpl::cluster_and_or);
    }
  }
  return ret;
}

int ObRawExprCanonicalizerImpl::pull_similar_expr(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (expr != NULL) {
    if (expr->get_expr_type() == T_OP_OR) {
      ObOpRawExpr* m_expr = static_cast<ObOpRawExpr*>(expr);
      for (int64_t i = 0; OB_SUCC(ret) && i < m_expr->get_param_count(); ++i) {
        if (OB_FAIL(SMART_CALL(pull_similar_expr(m_expr->get_param_expr(i))))) {
          LOG_WARN("pull child similar expr failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(pull_and_factor(expr))) {
        LOG_WARN("pull and facotr failed", K(ret));
      }
    } else if (expr->get_expr_type() == T_OP_AND) {
      ObOpRawExpr* m_expr = static_cast<ObOpRawExpr*>(expr);
      for (int64_t i = 0; OB_SUCC(ret) && i < m_expr->get_param_count(); ++i) {
        if (OB_FAIL(SMART_CALL(pull_similar_expr(m_expr->get_param_expr(i))))) {
          LOG_WARN("pull child similar expr failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(pull_parallel_expr(expr))) {
        // In fact:
        // If cluster_and_or is invoked first, we only need to check if direct child is AND expression
        LOG_WARN("pull parallel expr failed", K(ret));
      }
    } else {
      ret = process_children(expr, &ObRawExprCanonicalizerImpl::pull_similar_expr);
    }
  }
  return ret;
}

int ObRawExprCanonicalizerImpl::pull_and_factor(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (expr && T_OP_OR == expr->get_expr_type()) {
    ObRawExpr* short_expr = NULL;
    int64_t factor_num = INT64_MAX;
    // continue processing when one 'and'(ObOpRawExpr) exist at least
    bool do_handle = false;
    ObOpRawExpr* m_expr = static_cast<ObOpRawExpr*>(expr);
    for (int64_t i = 0; OB_SUCC(ret) && i < m_expr->get_param_count(); ++i) {
      const ObRawExpr* and_expr = m_expr->get_param_expr(i);
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
      ObQueryCtx* query_ctx = NULL;
      ObStmtCompareContext stmt_compare_context;
      ObArray<ObRawExpr*> candidate_factors;
      if (OB_ISNULL(short_expr) || OB_ISNULL(ctx_.stmt_) || OB_ISNULL(query_ctx = ctx_.stmt_->get_query_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(short_expr), K(ctx_.stmt_), K(query_ctx));
      } else if (OB_FAIL(stmt_compare_context.init(query_ctx))) {
        LOG_WARN("failed to init query ctx", K(ret));
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
        ObOpRawExpr* new_and = NULL;
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
            ObOpRawExpr* and_expr = NULL;
            // 1. ignore NULL 'or expr'
            if (OB_ISNULL(m_expr->get_param_expr(j))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("m_expr param is null", K(j), K(*m_expr));
            }
            // 2. seem whole none-'or expr' as integrity
            //     E.g.  A or (A and B) ==> A
            else if (T_OP_AND != m_expr->get_param_expr(j)->get_expr_type()) {
              if (OB_FAIL(ObStmtComparer::is_same_condition(
                      candidate_factors.at(i), m_expr->get_param_expr(j), stmt_compare_context, is_match))) {
                LOG_WARN("failed to check is condition equal", K(ret));
              } else if (!is_match) {
                /*do nothing*/
              } else if (OB_FAIL(append(equal_params, stmt_compare_context.equal_param_info_))) {
                LOG_WARN("failed to append expr", K(ret));
              } else if (OB_FAIL(idxs.push_back(-1 /*and factor index*/))) {
                LOG_WARN("failed to push back pos", K(ret));
              } else { /*do nothing*/
              }
            }
            // 3. find factor in and list
            else {  // and_expr != NULL
              and_expr = static_cast<ObOpRawExpr*>(m_expr->get_param_expr(j));
              for (int64_t k = 0; OB_SUCC(ret) && k < and_expr->get_param_count(); ++k) {
                if (OB_FAIL(ObStmtComparer::is_same_condition(
                        candidate_factors.at(i), and_expr->get_param_expr(k), stmt_compare_context, is_match))) {
                  LOG_WARN("failed to check is condition equal", K(ret));
                } else if (!is_match) {
                  /*do nothing*/
                } else if (OB_FAIL(append(equal_params, stmt_compare_context.equal_param_info_))) {
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
            for (int64_t j = 0; OB_SUCC(ret) && j < idxs.count(); ++j) {
              ObOpRawExpr* and_expr = NULL;
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
                and_expr = static_cast<ObOpRawExpr*>(m_expr->get_param_expr(j));
                if (OB_FAIL(and_expr->remove_param_expr(idxs.at(j)))) {
                  LOG_WARN("remove param expr failed", K(ret));
                } else if (1 == and_expr->get_param_count()) {
                  m_expr->get_param_expr(j) = and_expr->get_param_expr(0);
                }
              }
            }
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
          } else if (1 == new_and->get_param_count()) {
            expr = new_and->get_param_expr(0);
          } else {
            expr = new_and;
          }
        }
      }
    }
  }
  return ret;
}

int ObRawExprCanonicalizerImpl::pull_parallel_expr(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (expr && (expr->get_expr_type() == T_OP_AND || expr->get_expr_type() == T_OP_OR)) {
    bool has_sub = false;
    ObRawExpr::ExprClass expr_class = expr->get_expr_class();
    if (ObRawExpr::EXPR_OPERATOR != expr_class) {
      LOG_WARN("Unexpected expression type", K(expr_class));
    } else {
      ObOpRawExpr* parent_expr = static_cast<ObOpRawExpr*>(expr);
      for (int64_t i = 0; OB_SUCC(ret) && i < parent_expr->get_param_count(); ++i) {
        ObRawExpr* sub_expr = parent_expr->get_param_expr(i);
        if (sub_expr->get_expr_type() == parent_expr->get_expr_type()) {
          if (OB_FAIL(SMART_CALL(pull_parallel_expr(sub_expr)))) {
            LOG_WARN("Cluster AND or OR expression failed");
          } else {
            has_sub = true;
          }
        }
      }
      if (OB_SUCC(ret) && has_sub) {
        ObOpRawExpr tmp;
        ret = tmp.assign(*parent_expr);  // ret will be checked in the following loop
        parent_expr->clear_child();
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp.get_param_count(); ++i) {
          ObRawExpr* sub_expr = tmp.get_param_expr(i);
          if (OB_ISNULL(sub_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sub_expr is null", K(i));
          } else if (sub_expr->get_expr_type() == parent_expr->get_expr_type()) {
            ObRawExpr::ExprClass sub_expr_class = sub_expr->get_expr_class();
            if (ObRawExpr::EXPR_OPERATOR != sub_expr_class) {
              LOG_WARN("Unexpect Expression class", K(sub_expr_class));
            } else {
              ObOpRawExpr* child_expr = static_cast<ObOpRawExpr*>(sub_expr);
              for (int64_t j = 0; OB_SUCC(ret) && j < child_expr->get_param_count(); j++) {
                if (OB_FAIL(parent_expr->add_param_expr(child_expr->get_param_expr(j)))) {
                  LOG_WARN("Pull AND or OR expression failed", K(ret));
                }
              }
              child_expr->reset();
            }
          } else {
            if (OB_FAIL(parent_expr->add_param_expr(sub_expr))) {
              LOG_WARN("Pull AND or OR expression failed");
            }
          }
        }
        if (OB_SUCC(ret)) {
          tmp.reset();
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

int ObRawExprCanonicalizerImpl::push_not(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (expr != NULL && expr->has_flag(CNT_NOT)) {
    if (expr->has_flag(IS_NOT)) {
      ret = do_push_not(expr);
    } else if (OB_FAIL(process_children(expr, &ObRawExprCanonicalizerImpl::push_not))) {
      LOG_WARN("Push NOT expression failed", K(ret));
    }
  }
  return ret;
}

int ObRawExprCanonicalizerImpl::do_push_not(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (expr->get_expr_type() != T_OP_NOT) {
    // ignore
  } else {
    ObItemType opp_ty = T_MIN_OP;
    ObRawExpr::ExprClass expr_class = expr->get_expr_class();
    if (ObRawExpr::EXPR_OPERATOR != expr_class) {
      LOG_WARN("Unexpected expression type", K(expr_class));
    } else {
      ObOpRawExpr* not_expr = static_cast<ObOpRawExpr*>(expr);
      ObRawExpr* child_expr = not_expr->get_param_expr(0);
      if (OB_ISNULL(child_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child expr is null");
      } else if (child_expr->get_expr_type() == T_OP_NOT && ObRawExpr::EXPR_OPERATOR == child_expr->get_expr_class()) {
        // not(not(X)) => X
        // X must be a boolean operator
        if (OB_ISNULL(child_expr->get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null", K(ret));
        } else if (IS_BOOL_OP(child_expr->get_param_expr(0)->get_expr_type())) {
          expr = child_expr->get_param_expr(0);
          not_expr->reset();
          child_expr->reset();
          ret = push_not(expr);
        }
      } else if (ObRawExpr::EXPR_OPERATOR == child_expr->get_expr_class() &&
                 (child_expr->get_expr_type() == T_OP_AND || child_expr->get_expr_type() == T_OP_OR)) {
        // not(and(A, B)) => or(not(A), not(B))
        // not(or(A, B)) => and(not(A), not(B))
        child_expr->set_expr_type(child_expr->get_expr_type() == T_OP_AND ? T_OP_OR : T_OP_AND);
        // child_expr->free_op(); @todo
        expr = child_expr;
        ObOpRawExpr* m_expr = static_cast<ObOpRawExpr*>(child_expr);  // and, or
        ObOpRawExpr tmp;
        ret = tmp.assign(*m_expr);  // copy old children
        if (OB_SUCC(ret)) {
          not_expr->reset();
          m_expr->clear_child();
          // reuse not
          not_expr->set_expr_type(T_OP_NOT);
          not_expr->set_param_expr(tmp.get_param_expr(0));
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to assigin expr", K(ret));
        } else if (OB_FAIL(not_expr->add_flag(IS_NOT))) {
          LOG_WARN("failed to add flag IS_NOT", K(ret));
        } else if (OB_FAIL(m_expr->add_param_expr(not_expr))) {
          LOG_WARN("failed to add param expr", K(ret));
        } else if (OB_FAIL(SMART_CALL(do_push_not(m_expr->get_param_expr(0))))) {
          LOG_WARN("failed to do push not", K(ret));
        }
        for (int64_t i = 1; OB_SUCC(ret) && i < tmp.get_param_count(); ++i) {
          ObOpRawExpr* another = NULL;
          if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_NOT, another))) {
            LOG_WARN("create ObOpRawExpr failed", K(ret));
          } else if (OB_ISNULL(another)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is null");
          } else {
            another->set_param_expr(tmp.get_param_expr(i));
            if (OB_FAIL(another->add_flag(IS_NOT))) {
              LOG_WARN("failed to add flag IS_NOT", K(ret));
            } else if (OB_FAIL(m_expr->add_param_expr(another))) {
              LOG_WARN("add param expression failed", K(ret));
            } else if (OB_FAIL(SMART_CALL(do_push_not(m_expr->get_param_expr(i))))) {
              LOG_WARN("param do push not failed", K(ret));
            }
          }
        }
      } else if ((opp_ty = get_opposite_op(child_expr->get_expr_type())) != T_MIN_OP) {
        // not(>(a,b)) to <=(a,b)
        expr = child_expr;
        child_expr->set_expr_type(opp_ty);
        not_expr->reset();
      } else {
        // can not push down any more
      }
    }
  }
  return ret;
}

ObRawExprCanonicalizerImpl::ObOppositeOpPair ObRawExprCanonicalizerImpl::OPPOSITE_PAIRS[] = {
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

int ObRawExprCanonicalizerImpl::process_children(ObRawExpr*& expr, process_action trave_action)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
    if (OB_FAIL((this->*trave_action)(expr->get_param_expr(i)))) {
      LOG_WARN("Trave OpRawExpr failed", K(ret));
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
