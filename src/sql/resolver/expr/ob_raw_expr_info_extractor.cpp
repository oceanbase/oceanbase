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
#include "sql/resolver/expr/ob_raw_expr_info_extractor.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

int ObRawExprInfoExtractor::analyze(ObRawExpr &expr)
{
  return expr.postorder_accept(*this);
}

int ObRawExprInfoExtractor::visit(ObPlQueryRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clear_info(expr))) {
    LOG_WARN("fail to clear info", K(ret));
  } else if (OB_FAIL(pull_info(expr))) {
    LOG_WARN("failed to pull info from child", K(ret));
  }
  return ret;
}

int ObRawExprInfoExtractor::visit(ObConstRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObItemType type = expr.get_expr_type();
  switch (type) {
  case T_SYSTEM_VARIABLE:
  case T_QUESTIONMARK: {
    if (OB_FAIL(expr.add_flag(IS_STATIC_PARAM))) {
      // we need to calculate the variable value before entering optimizer
      LOG_WARN("failed to add flag IS_STATIC_PARAM", K(ret));
    }
    break;
  }
  case T_ENUM:
  case T_SET: {
    if (OB_FAIL(expr.add_flag(IS_ENUM_OR_SET))) {
      LOG_WARN("failed to add flag IS_ENUM_OR_SET", K(ret));
    }
    break;
  }
  default:
    break;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr.add_flag(IS_CONST))) {
      LOG_WARN("failed to add flag IS_CONST", K(ret));
    }
  }
  return ret;
}

int ObRawExprInfoExtractor::visit(ObVarRawExpr &expr)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObRawExprInfoExtractor::visit(ObOpPseudoColumnRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.add_flag(IS_OP_PSEUDO_COLUMN))) {
    LOG_WARN("add flag failed", K(ret));
  }
  return ret;
}

int ObRawExprInfoExtractor::visit(ObQueryRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clear_info(expr))) {
    LOG_WARN("fail to clear info", K(ret));
  } else if (OB_FAIL(pull_info(expr))) {
    LOG_WARN("failed to pull info from child", K(ret));
  } else if (OB_FAIL(expr.add_flag(IS_SUB_QUERY))) {
    LOG_WARN("failed to add is sub query flag", K(ret));
  } else if (ob_is_enumset_tc(expr.get_data_type()) && OB_FAIL(expr.add_flag(IS_ENUM_OR_SET))) {
    LOG_WARN("failed to add flag IS_ENUM_OR_SET", K(expr), K(ret));
  }
  return ret;
}

int ObRawExprInfoExtractor::visit(ObExecParamRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.add_flag(IS_CONST))) {
    LOG_WARN("failed to add is const", K(ret));
  } else if (OB_FAIL(expr.add_flag(IS_DYNAMIC_PARAM))) {
    LOG_WARN("failed to add is exec param", K(ret));
  } else if (!expr.is_onetime()) {
    // do nothing
  } else if (OB_FAIL(expr.add_flag(IS_ONETIME))) {
    LOG_WARN("failed to add is onetime", K(ret));
  }
  if (OB_SUCC(ret) && expr.get_ref_expr()->has_enum_set_column()) {
    OZ(expr.add_flag(CNT_ENUM_OR_SET));
    OZ(expr.set_enum_set_values(expr.get_ref_expr()->get_enum_set_values()));
  }
  return ret;
}

int ObRawExprInfoExtractor::visit(ObColumnRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.add_flag(IS_COLUMN))) {
    LOG_WARN("failed to add flag IS_COLUMN", K(expr), K(ret));
  } else if (ob_is_enumset_tc(expr.get_data_type()) && OB_FAIL(expr.add_flag(IS_ENUM_OR_SET))) {
    LOG_WARN("failed to add flag IS_ENUM_OR_SET", K(expr), K(ret));
  } else {/*do nothing*/}
  return ret;
}


#define CONST_ACTION(expr) \
  do { \
    if ((expr)->has_flag(IS_CONST) || (expr)->has_flag(IS_CONST_EXPR)) { \
      if ((expr)->has_flag(IS_CONST_EXPR)) { \
        cnt_const_expr = true;  \
      } \
    } \
    else { \
      is_const_expr = false; \
    } \
  } while(0)

int ObRawExprInfoExtractor::clear_info(ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  ObExprInfo &expr_info = expr.get_expr_info();
  bool is_implicit_cast = expr_info.has_member(IS_OP_OPERAND_IMPLICIT_CAST);
  bool is_self_param = expr_info.has_member(IS_UDT_UDF_SELF_PARAM);
  expr_info.reset();
  if (is_implicit_cast) {
    OZ(expr_info.add_member(IS_OP_OPERAND_IMPLICIT_CAST));
  }
  if (is_self_param) {
    OZ(expr_info.add_member(IS_UDT_UDF_SELF_PARAM));
  }
  return ret;
}

int ObRawExprInfoExtractor::pull_info(ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); i++) {
    ObRawExpr *param_expr = expr.get_param_expr(i);
    if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(i));
    } else if (OB_FAIL(expr.add_child_flags(param_expr->get_expr_info()))) {
      LOG_WARN("fail to add child flags", K(ret));
    }
  }
  return ret;
}
// examples:
// IS_CONST: 10
// IS_CONST_EXPR: 10+10
// CNT_CONST_EXPR: 10+10+c1
int ObRawExprInfoExtractor::add_const(ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  bool cnt_const_expr = false;
  bool is_const_expr = true;
  for (int64_t i = 0; i < expr.get_param_count(); i++) {
    ObRawExpr *param_expr = expr.get_param_expr(i);
    CONST_ACTION(param_expr);
  }
  if (is_const_expr) {
    bool is_const_inherit = true;
    if (OB_FAIL(expr.is_const_inherit_expr(is_const_inherit))) {
      LOG_WARN("failed to check expr is const in");
    } else {
      is_const_expr = is_const_inherit;
    }
  }
  if (is_const_expr) {
    if (OB_FAIL(expr.add_flag(IS_CONST_EXPR))) {
      LOG_WARN("failed to add flag IS_CONST_EXPR", K(ret));
    }
  } else if (cnt_const_expr) {
    if (OB_FAIL(expr.add_flag(CNT_CONST_EXPR))) {
      LOG_WARN("failed to add flag CNT_CONST_EXPR", K(ret));
    }
  }
  return ret;
}

int ObRawExprInfoExtractor::visit(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  const bool is_inner_added = expr.has_flag(IS_INNER_ADDED_EXPR);
  if (OB_FAIL(clear_info(expr))) {
    LOG_WARN("fail to clear info", K(ret));
  } else if (OB_FAIL(pull_info(expr))) {
    LOG_WARN("fail to add pull info", K(ret));
  } else if (is_inner_added && OB_FAIL(expr.add_flag(IS_INNER_ADDED_EXPR))) {
    LOG_WARN("add flag failed", K(ret));
  } else if (T_OP_ORACLE_OUTER_JOIN_SYMBOL == expr.get_expr_type() &&
             OB_FAIL(expr.add_flag(IS_OUTER_JOIN_SYMBOL))) {
    LOG_WARN("failed to add flag IS_CONST_EXPR", K(ret));
  } else if (OB_FAIL(add_const(expr))) {
    LOG_WARN("fail to add const", K(ret));
  } else if (1 == expr.get_param_count()) {
    // unary operator
    switch(expr.get_expr_type()) {
      case T_OP_NOT:
        if (OB_FAIL(expr.add_flag(IS_NOT))) {
          LOG_WARN("failed to add flag IS_NOT", K(ret));
        }
        break;
      case T_OP_PRIOR:
        if (OB_FAIL(expr.add_flag(IS_PRIOR))) {
          LOG_WARN("failed to add flag IS_PRIOR", K(ret));
        }
        break;
      case T_OP_CONNECT_BY_ROOT:
        if (OB_FAIL(expr.add_flag(IS_CONNECT_BY_ROOT))) {
          LOG_WARN("failed to add flag IS_CONNECT_BY_ROOT", K(ret));
        }
        break;
      default:
        break;
    }
  } else if (2 == expr.get_param_count()) {
    // binary operator
    ObRawExpr *param_expr1 = expr.get_param_expr(0);
    ObRawExpr *param_expr2 = expr.get_param_expr(1);
    if (OB_ISNULL(param_expr1) || OB_ISNULL(param_expr2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(param_expr1), K(param_expr2));
    } else if ((expr.get_expr_type() == T_OP_EQ) || (expr.get_expr_type() == T_OP_NSEQ)) {
      if (param_expr1->has_flag(CNT_COLUMN) && param_expr2->has_flag(CNT_COLUMN) &&
          !param_expr1->get_relation_ids().overlap(param_expr2->get_relation_ids())) {
        if (OB_FAIL(expr.add_flag(IS_JOIN_COND))) {
          LOG_WARN("failed to add flag IS_JOIN_COND", K(ret));
        }
      } else if ((param_expr1->has_flag(IS_COLUMN) &&
                  param_expr2->is_const_expr())
                  || (param_expr2->has_flag(IS_COLUMN) &&
                      param_expr1->is_const_expr())) {
        if (OB_FAIL(expr.add_flag(IS_SIMPLE_COND))) {
          LOG_WARN("failed to add flag IS_SIMPLE_COND", K(ret));
        }
      } else if ((param_expr1->has_flag(IS_ROWID) &&
                  param_expr2->is_const_expr())
                  || (param_expr2->has_flag(IS_ROWID) &&
                      param_expr1->is_const_expr())) {
        if (OB_FAIL(expr.add_flag(IS_ROWID_SIMPLE_COND))) {
          LOG_WARN("failed to add flag IS_ROWID_SIMPLE_COND", K(ret));
        }
      }
    } else if (IS_RANGE_CMP_OP(expr.get_expr_type())) {
      if ((param_expr1->has_flag(IS_COLUMN) &&
            param_expr2->is_const_expr())
          || (param_expr2->has_flag(IS_COLUMN) &&
              param_expr1->is_const_expr())) {
        if (OB_FAIL(expr.add_flag(IS_RANGE_COND))) {
          LOG_WARN("failed to add flag IS_RANGE_COND", K(ret));
        }
      } else if ((param_expr1->has_flag(IS_ROWID) &&
                  param_expr2->is_const_expr()) ||
                  (param_expr2->has_flag(IS_ROWID) &&
                  param_expr1->is_const_expr())) {
        if (OB_FAIL(expr.add_flag(IS_ROWID_RANGE_COND))) {
          LOG_WARN("failed to add flag IS_ROWID_RANGE_COND", K(ret));
        }
      }
    } else if (expr.get_expr_type() == T_OP_IN || expr.get_expr_type() == T_OP_NOT_IN) {
      if (OB_FAIL(expr.add_flag(IS_IN))) {
        LOG_WARN("failed to add flag IS_IN", K(ret));
      }
    } else if (expr.get_expr_type() == T_OP_ASSIGN) {
      if (OB_FAIL(expr.add_flag(IS_ASSIGN_EXPR))) {
        LOG_WARN("failed to add flag IS_ASSIGN_EXPR", K(ret));
      }
    } else if (T_OP_IS == expr.get_expr_type()) {
      if (OB_FAIL(expr.add_flag(IS_IS_EXPR))) {
        LOG_WARN("failed to add flag IS_IS_EXPR", K(ret));
      }
    }
  } else if (3 == expr.get_param_count()) {
    // triple operator
    ObRawExpr *param_expr1 = expr.get_param_expr(0);
    ObRawExpr *param_expr2 = expr.get_param_expr(1);
    ObRawExpr *param_expr3 = expr.get_param_expr(2);
    if (OB_ISNULL(param_expr1) || OB_ISNULL(param_expr2) || OB_ISNULL(param_expr3)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(param_expr1), K(param_expr2), K(param_expr3));
    } else if (T_OP_BTW == expr.get_expr_type()) {
      if (param_expr1->has_flag(IS_COLUMN)
          && param_expr2->is_const_expr()
          && param_expr3->is_const_expr()) {
        if (OB_FAIL(expr.add_flag(IS_RANGE_COND))) {
          LOG_WARN("failed to add flag IS_RANGE_COND", K(ret));
        }
      }
    } else {}
  }
  if (OB_SUCC(ret) && expr.get_expr_type() == T_OP_OR) {
    if (OB_FAIL(expr.add_flag(IS_OR))) {
      LOG_WARN("failed to add flag IS_OR", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(visit_subquery_node(expr))) {
    LOG_WARN("visit subquery node failed", K(ret));
  }
  if (OB_SUCC(ret) && expr.get_expr_type() == T_OBJ_ACCESS_REF) {
    if (OB_FAIL(expr.add_flag(CNT_OBJ_ACCESS_EXPR))) {
      LOG_WARN("failed to add flag IS_OR", K(ret));
    }
  }
  return ret;
}

int ObRawExprInfoExtractor::visit_subquery_node(ObOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.has_flag(CNT_SUB_QUERY)) {
    if (IS_COMPARISON_OP(expr.get_expr_type())) {
      //二元操作符，需要探测右边操作符的T_ALL/T_ANY等节点，并变换
      //T_ALL/T_ANY是为了方便resolve增加的无用的ObOpRawExpr节点，
      //在后面其实直接可以用IS_WITH_ALL, IS_WITH_ANY flag来表示其信息，所以在这里将这两种节点去掉
      ObRawExpr *left_expr = NULL;
      ObRawExpr *right_expr = NULL;
      if (OB_UNLIKELY(expr.get_param_count() != 2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr param count is invalid", K(expr.get_param_count()));
      } else if (OB_ISNULL(left_expr = expr.get_param_expr(0))
          || OB_ISNULL(right_expr = expr.get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(left_expr), K(right_expr));
      } else if (right_expr->get_expr_type() == T_ALL || right_expr->get_expr_type() == T_ANY) {
        ObSubQueryKey key_flag = (right_expr->get_expr_type() == T_ALL) ? T_WITH_ALL : T_WITH_ANY;
        expr.set_subquery_key(key_flag);
        //去除T_ALL or T_ANY的无用ObOpRawExpr节点
        if (OB_FAIL(expr.replace_param_expr(1, right_expr->get_param_expr(0)))) {
          LOG_WARN("replace right expr failed", K(ret));
        } else {
          right_expr = expr.get_param_expr(1);
        }
      }
      if (OB_SUCCESS == ret && left_expr->has_flag(IS_SUB_QUERY)) {
        ObQueryRefRawExpr *left_ref = static_cast<ObQueryRefRawExpr*>(left_expr);
        if (OB_UNLIKELY(left_ref->is_set())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("left expr is set");
        } else if (left_ref->get_output_column() > 1 &&
                   IS_COMMON_COMPARISON_OP(expr.get_expr_type())) {
          //左边子查询结果只可能是标量或者向量，如果是标量，不需要做operator转换，普通的比较表达式也能够处理子查询的情况
          expr.set_expr_type(get_subquery_comparison_type(expr.get_expr_type()));
        }
      }
      if (OB_SUCCESS == ret && right_expr->has_flag(IS_SUB_QUERY)) {
        //操作符也需要添加上ALL/ANY等flag，方便判断
        ObQueryRefRawExpr *right_ref = static_cast<ObQueryRefRawExpr*>(right_expr);
        if ((right_ref->get_output_column() > 1 || right_ref->is_set()) &&
            IS_COMMON_COMPARISON_OP(expr.get_expr_type())) {
          //子查询的结果是向量或者集合，那么必须将比较操作符转换为对应的subquery expr operator
          expr.set_expr_type(get_subquery_comparison_type(expr.get_expr_type()));
        }
        if (expr.get_subquery_key() == T_WITH_ALL) {
          if (OB_FAIL(expr.add_flag(IS_WITH_ALL))) {
            LOG_WARN("failed to add flag IS_WITH_ALL", K(ret));
          }
        } else if (expr.get_subquery_key() == T_WITH_ANY) {
          if (OB_FAIL(expr.add_flag(IS_WITH_ANY))) {
            LOG_WARN("failed to add flag IS_WITH_ANY", K(ret));
          }
        } else {
          if (OB_FAIL(expr.add_flag(IS_WITH_SUBQUERY))) {
            LOG_WARN("failed to add flag IS_WITH_SUBQUERY", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

ObItemType ObRawExprInfoExtractor::get_subquery_comparison_type(ObItemType cmp_type) const
{
  ObItemType ret = cmp_type;
  switch (cmp_type) {
    case T_OP_EQ:
      ret = T_OP_SQ_EQ;
      break;
    case T_OP_NSEQ:
      ret = T_OP_SQ_NSEQ;
      break;
    case T_OP_LE:
      ret = T_OP_SQ_LE;
      break;
    case T_OP_LT:
      ret = T_OP_SQ_LT;
      break;
    case T_OP_GE:
      ret = T_OP_SQ_GE;
      break;
    case T_OP_GT:
      ret = T_OP_SQ_GT;
      break;
    case T_OP_NE:
      ret = T_OP_SQ_NE;
      break;
    default:
      ret = cmp_type;
      break;
  }
  return ret;
}

int ObRawExprInfoExtractor::visit(ObCaseOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clear_info(expr))) {
    LOG_WARN("fail to clear info", K(ret));
  } else if (OB_FAIL(pull_info(expr))) {
    LOG_WARN("fail to add pull info", K(ret));
  } else if (OB_FAIL(add_const(expr))) {
    LOG_WARN("fail to add const", K(expr), K(ret));
  } else {}
  return ret;
}

int ObRawExprInfoExtractor::visit(ObAggFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  const bool is_inner_added = expr.has_flag(IS_INNER_ADDED_EXPR);
  if (OB_FAIL(clear_info(expr))) {
    LOG_WARN("fail to clear info", K(ret));
  } else if (OB_FAIL(pull_info(expr))) {
    LOG_WARN("fail to add pull info", K(ret));
  } else if (OB_FAIL(expr.add_flag(IS_AGG))) {
    LOG_WARN("failed to add flag IS_AGG", K(ret));
  } else if (is_inner_added && OB_FAIL(expr.add_flag(IS_INNER_ADDED_EXPR))) {
    LOG_WARN("failed to add inner added expr flag", K(ret));
  } else { }
  return ret;
}

int ObRawExprInfoExtractor::visit(ObSysFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  //bug:
  const bool is_inner_added = expr.has_flag(IS_INNER_ADDED_EXPR);
  if (OB_FAIL(clear_info(expr))) {
    LOG_WARN("fail to clear info", K(ret));
  } else if (OB_FAIL(pull_info(expr))) {
    LOG_WARN("fail to add pull info", K(ret));
  } else if (OB_FAIL(add_const(expr))) {
    LOG_WARN("fail to add const", K(expr), K(ret));
  } else if (OB_FAIL(expr.add_flag(IS_FUNC))) {
    LOG_WARN("failed to add flag IS_FUNC", K(ret));
  } else if (is_inner_added && OB_FAIL(expr.add_flag(IS_INNER_ADDED_EXPR))) {
    LOG_WARN("add flag failed", K(ret));
  } else if (ob_is_enumset_tc(expr.get_data_type()) && OB_FAIL(expr.add_flag(IS_ENUM_OR_SET))) {
    LOG_WARN("add enum or set flag failed", K(ret));
  } else {
    // these functions should not be calculated first
    if (T_FUN_SYS_AUTOINC_NEXTVAL == expr.get_expr_type()
        || T_FUN_SYS_DOC_ID == expr.get_expr_type()
        || T_FUN_SYS_TABLET_AUTOINC_NEXTVAL == expr.get_expr_type()
        || T_FUN_SYS_SLEEP == expr.get_expr_type()
        || (T_FUN_SYS_LAST_INSERT_ID == expr.get_expr_type() && expr.get_param_count() > 0)
        || T_FUN_SYS_PART_ID == expr.get_expr_type()
        || T_OP_GET_PACKAGE_VAR == expr.get_expr_type()
        || T_OP_GET_SUBPROGRAM_VAR == expr.get_expr_type()
        || (T_FUN_SYS_SYSDATE == expr.get_expr_type() && !lib::is_oracle_mode())
        || T_FUN_NORMAL_UDF == expr.get_expr_type()
        || T_FUN_SYS_GENERATOR == expr.get_expr_type()
        || T_FUN_SYS_LAST_REFRESH_SCN == expr.get_expr_type()
        || (T_FUN_UDF == expr.get_expr_type()
            && !static_cast<ObUDFRawExpr&>(expr).is_deterministic())
        || T_FUN_SYS_GET_LOCK == expr.get_expr_type()
        || T_FUN_SYS_IS_FREE_LOCK == expr.get_expr_type()
        || T_FUN_SYS_IS_USED_LOCK == expr.get_expr_type()
        || T_FUN_SYS_RELEASE_LOCK == expr.get_expr_type()
        || T_FUN_SYS_RELEASE_ALL_LOCKS == expr.get_expr_type()) {
      if (OB_FAIL(expr.add_flag(IS_STATE_FUNC))) {
        LOG_WARN("failed to add flag IS_STATE_FUNC", K(ret));
      }
    } else if (T_FUN_SYS_VALUES == expr.get_expr_type()) {
      if (OB_FAIL(expr.add_flag(IS_VALUES))) {
        LOG_WARN("failed to add flag IS_VALUES", K(ret));
      }
    } else if ((T_FUN_SYS_RAND == expr.get_expr_type() || T_FUN_SYS_RANDOM == expr.get_expr_type()) &&
               !expr.has_flag(CNT_COLUMN)) {
      if (OB_FAIL(expr.add_flag(IS_RAND_FUNC))) {
        LOG_WARN("failed to add flag IS_RAND_FUNC", K(ret));
      }
    } else if (T_FUN_SYS_GUID == expr.get_expr_type() || T_FUN_SYS_UUID == expr.get_expr_type() 
              || T_FUN_SYS_UUID_SHORT == expr.get_expr_type()) {
      if (OB_FAIL(expr.add_flag(IS_RAND_FUNC))) {
        LOG_WARN("failed to add flag IS_RAND_FUNC", K(ret));
      }
    } else if (T_FUN_SYS_ROWNUM == expr.get_expr_type()) {
      if (OB_FAIL(expr.add_flag(IS_ROWNUM))) {
        LOG_WARN("failed to add flag IS_ROWNUM", K(ret));
      }
    } else if (T_FUN_SYS_SEQ_NEXTVAL == expr.get_expr_type()) {
      if (OB_FAIL(expr.add_flag(IS_SEQ_EXPR))) {
        LOG_WARN("failed to add flag IS_SEQ_EXPR", K(ret));
      }
    } else if (T_FUN_SYS_CALC_UROWID == expr.get_expr_type()) {
      if (OB_FAIL(expr.add_flag(IS_ROWID))) {
        LOG_WARN("failed to add flag IS_ROWID", K(ret));
      }
    } else if (T_FUN_SYS_CONNECT_BY_PATH == expr.get_expr_type()) {
      if (OB_FAIL(expr.add_flag(IS_SYS_CONNECT_BY_PATH))) {
        LOG_WARN("failed to add flag IS_SYS_CONNECT_BY_PATH", K(ret));
      }
    } else if (T_FUN_NORMAL_UDF == expr.get_expr_type()
               || T_FUN_AGG_UDF == expr.get_expr_type()) {
      /*
       * it seems we have no choice but to set the udf uncalculable.
       * we can not say a udf expr is const or not util we finish the xxx_init() function.
       * but we do the xxx_init() at the expr deduce type stage which was done after we
       * extractor info from expr.
       * */
      if (OB_FAIL(expr.add_flag(IS_SO_UDF_EXPR))) {
        LOG_WARN("failed to add flag IS_SO_UDF_EXPR", K(ret));
      }
    } else if (T_FUN_SYS_REMOVE_CONST == expr.get_expr_type()) {
      OZ(expr.add_flag(CNT_VOLATILE_CONST));
    } else if (T_FUN_SYS_WRAPPER_INNER == expr.get_expr_type()) {
      OZ(expr.add_flag(CNT_VOLATILE_CONST));
    }
    if (OB_SUCC(ret) && T_FUN_UDF == expr.get_expr_type()) {
      if (OB_FAIL(expr.add_flag(IS_PL_UDF))) {
        LOG_WARN("failed to add flag IS_STATE_FUNC", K(ret));
      }
    }
    if (OB_SUCC(ret) && T_OP_GET_USER_VAR == expr.get_expr_type() && !expr.is_const_expr()) {
      if (OB_FAIL(expr.add_flag(IS_DYNAMIC_USER_VARIABLE))) {
        LOG_WARN("failed to add flag IS_DYNAMIC_USER_VARIABLE", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (T_FUN_SYS_CUR_TIMESTAMP == expr.get_expr_type()
          || T_FUN_SYS_CUR_TIME == expr.get_expr_type()
          || T_FUN_SYS_CUR_DATE == expr.get_expr_type()
          || T_FUN_SYS_UTC_TIME == expr.get_expr_type()
          || T_FUN_SYS_UTC_TIMESTAMP == expr.get_expr_type()
          || T_FUN_SYS_UTC_DATE == expr.get_expr_type()
          || T_FUN_SYS_LOCALTIMESTAMP == expr.get_expr_type()
          || (T_FUN_SYS_SYSDATE == expr.get_expr_type() && lib::is_oracle_mode())
          || T_FUN_SYS_SYSTIMESTAMP == expr.get_expr_type()
          || (T_FUN_SYS_UNIX_TIMESTAMP == expr.get_expr_type()
              && 0 == expr.get_param_exprs().count())) { // check if has argument
        if (OB_FAIL(expr.add_flag(IS_CUR_TIME))) { //需要在plan执行前取系统当前时间
          LOG_WARN("failed to add flag IS_CUR_TIME", K(ret));
        }
      } else if (T_FUN_SYS_DEFAULT == expr.get_expr_type()) {
        if (OB_FAIL(expr.add_flag(IS_DEFAULT))) {
          LOG_WARN("failed to add flag IS_DEFAULT", K(ret));
        }
      } else if (T_FUN_SYS_LAST_INSERT_ID == expr.get_expr_type()) {
        if (OB_FAIL(expr.add_flag(IS_LAST_INSERT_ID))) {
          LOG_WARN("failed to add flag IS_LAST_INSERT_ID", K(ret));
        }
      } else {}
    }

  }
  return ret;
}

int ObRawExprInfoExtractor::visit(ObSetOpRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clear_info(expr))) {
    LOG_WARN("fail to clear info", K(ret));
  } else if (OB_FAIL(expr.add_flag(IS_SET_OP))) {
    LOG_WARN("failed to add flag IS_SET_OP", K(ret));
  }
  return ret;
}

int ObRawExprInfoExtractor::visit(ObAliasRefRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clear_info(expr))) {
    LOG_WARN("fail to clear info", K(ret));
  } else if (OB_FAIL(expr.add_flag(IS_ALIAS))) {
    LOG_WARN("failed to add flag", K(ret));
  } else if (OB_ISNULL(expr.get_ref_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref expr is null");
  } else if (expr.is_ref_query_output()) {
    if (OB_FAIL(expr.add_flag(CNT_SUB_QUERY))) {
      LOG_WARN("failed to add expr flag", K(ret));
    }
  } else if (OB_FAIL(expr.add_child_flags(expr.get_ref_expr()->get_expr_info()))) {
    LOG_WARN("add child flags to expr failed", K(ret));
  }
  return ret;
}

int ObRawExprInfoExtractor::visit(ObWinFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clear_info(expr))) {
    LOG_WARN("fail to clear info", K(ret));
  } else if (OB_FAIL(pull_info(expr))) {
    LOG_WARN("pull match info failed", K(ret));
  } else if (OB_FAIL(expr.add_flag(IS_WINDOW_FUNC))) {
    LOG_WARN("add flag failed", K(ret));
  } else if (OB_FAIL(expr.add_flag(CNT_WINDOW_FUNC))) {
    LOG_WARN("add flag failed", K(ret));
  }
  return ret;
}

int ObRawExprInfoExtractor::visit(ObPseudoColumnRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clear_info(expr))) {
    LOG_WARN("fail to clear info", K(ret));
  } else if (OB_FAIL(expr.add_flag(IS_PSEUDO_COLUMN))) {
    LOG_WARN("add flag fail", K(ret));
  } else if (T_LEVEL == expr.get_expr_type()) {
    if (OB_FAIL(expr.add_flag(IS_LEVEL))) {
        LOG_WARN("failed to add flag IS_LEVEL", K(ret));
    }
  }else if (T_CONNECT_BY_ISLEAF == expr.get_expr_type()) {
    if (OB_FAIL(expr.add_flag(IS_CONNECT_BY_ISLEAF))) {
        LOG_WARN("failed to add flag IS_CONNECT_BY_ISLEAF", K(ret));
    }
  }else if (T_CONNECT_BY_ISCYCLE == expr.get_expr_type()) {
    if (OB_FAIL(expr.add_flag(IS_CONNECT_BY_ISCYCLE))) {
        LOG_WARN("failed to add flag IS_CONNECT_BY_ISCYCLE", K(ret));
    }
  } else if (T_ORA_ROWSCN == expr.get_expr_type()) {
    if (OB_FAIL(expr.add_flag(IS_ORA_ROWSCN_EXPR))) {
        LOG_WARN("failed to add flag IS_ORA_ROWSCN_EXPR", K(ret));
    }
  }
  return ret;
}

int ObRawExprInfoExtractor::visit(ObMatchFunRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clear_info(expr))) {
    LOG_WARN("failed to clear info", K(ret));
  } else if (OB_FAIL(pull_info(expr))) {
    LOG_WARN("pull match against info failed", K(ret));
  } else if (OB_FAIL(expr.add_flag(IS_MATCH_EXPR))) {
    LOG_WARN("add flag to match against failed", K(ret));
  }
  return ret;
}


}  // namespace sql
}  // namespace oceanbase
