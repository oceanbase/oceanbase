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

#define USING_LOG_PREFIX SERVER
#include "ob_request_base.h"
#define IS_LOGICAL_OPERATOR(type) ((type) == T_OP_AND || (type) == T_OP_OR || (type) == T_OP_XOR)
#define IS_SIMPLIFIABLE_OPERATOR(type) (IS_LOGICAL_OPERATOR(type) || \
                                        (type) == T_OP_ADD || \
                                        (type) == T_OP_MINUS || \
                                        (type) == T_OP_MUL || \
                                        (type) == T_OP_DIV || \
                                        (type) == T_OP_MOD)

namespace oceanbase
{
using namespace sql;
namespace share
{

int get_op_precedence(ObItemType op_type) {
  switch (op_type) {
    case T_OP_OR:
      return PREC_OR;
    case T_OP_XOR:
      return PREC_XOR;
    case T_OP_AND:
      return PREC_AND;
    case T_OP_BIT_OR:
    case T_OP_BIT_AND:
    case T_OP_BIT_XOR:
    case T_OP_BIT_LEFT_SHIFT:
    case T_OP_BIT_RIGHT_SHIFT:
      return PREC_BIT;
    case T_OP_EQ:
    case T_OP_NSEQ:
    case T_OP_LE:
    case T_OP_LT:
    case T_OP_GE:
    case T_OP_GT:
    case T_OP_NE:
    case T_OP_LIKE:
    case T_OP_IN:
    case T_OP_IS:
    case T_OP_IS_NOT:
      return PREC_COMP;
    case T_OP_ADD:
    case T_OP_MINUS:
      return PREC_ADD;
    case T_OP_MUL:
    case T_OP_DIV:
    case T_OP_MOD:
      return PREC_MUL;
    case T_OP_POW:
      return PREC_POW;
    case T_OP_NOT:
    case T_OP_BIT_NEG:
    case T_OP_NEG:
    case T_OP_POS:
      return PREC_UNARY;
    default:
      return 0;
  }
}

bool is_atomic_expression(ObItemType op_type) {
  switch (op_type) {
    case T_OP_EQ:
    case T_OP_NSEQ:
    case T_OP_LE:
    case T_OP_LT:
    case T_OP_GE:
    case T_OP_GT:
    case T_OP_NE:
    case T_OP_LIKE:
    case T_OP_IN:
    case T_OP_IS:
    case T_OP_IS_NOT:
    case T_OP_ADD:
    case T_OP_MINUS:
    case T_OP_MUL:
    case T_OP_DIV:
    case T_OP_MOD:
    case T_OP_POW:
    case T_OP_BIT_OR:
    case T_OP_BIT_AND:
    case T_OP_BIT_XOR:
    case T_OP_BIT_LEFT_SHIFT:
    case T_OP_BIT_RIGHT_SHIFT:
    case T_OP_BIT_NEG:
    case T_OP_NEG:
    case T_OP_POS:
      return true;
    case T_OP_OR:
    case T_OP_XOR:
    case T_OP_AND:
    case T_OP_NOT:
    default:
      return false;
  }
}

bool ObReqOpExpr::has_multi_params_recursive() const {
  if (params.count() > 1) {
    return true;
  }
  for (uint64_t i = 0; i < params.count(); i++) {
    ObReqOpExpr *op_expr = dynamic_cast<ObReqOpExpr*>(params.at(i));
    if (OB_NOT_NULL(op_expr) && op_expr->has_multi_params_recursive()) {
      return true;
    }
  }
  return false;
}

int ObReqOpExpr::need_parentheses_by_associativity(ObItemType parent_type, ObItemType child_type, int child_index, bool &need_parentheses) const {
  int ret = OB_SUCCESS;
  int parent_prec = get_op_precedence(parent_type);
  int child_prec = get_op_precedence(child_type);
  if (parent_prec > 0 && child_prec > 0) {
    if (parent_prec > child_prec) {
      need_parentheses = true;
    } else if (parent_prec < child_prec) {
      need_parentheses = false;
    } else if (need_right_operand_parentheses(parent_type, child_index)) {
      need_parentheses = true;
    } else {
      need_parentheses = !is_left_associative(parent_type);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected op type", K(ret), K(parent_type), K(child_type));
    need_parentheses = true;
  }
  return ret;
}

int ObReqOpExpr::need_parentheses_for_child(const ObReqOpExpr &child_expr, int child_index, bool &need_parentheses) const {
  int ret = OB_SUCCESS;
  ObItemType child_type = child_expr.get_op_type();
  if (child_type == T_OP_NOT) {
    need_parentheses = false;
  } else {
    ObItemType parent_type = get_op_type();
    // child is a single expression
    if (child_expr.params.count() == 1) {
      ret = need_parentheses_for_sub_expr(child_expr, child_index, parent_type, params.count(), need_parentheses);
    }
    // only one child
    else if (params.count() == 1) {
      if (parent_type == T_OP_NOT) {
        need_parentheses = (child_type != T_OP_NOT);
      } else {
        need_parentheses = false;
      }
    }
    // just for better readability
    else if (parent_type == T_OP_OR && child_type == T_OP_AND) {
      need_parentheses = true;
    }
    // more than one child
    else {
      if (OB_FAIL(need_parentheses_by_associativity(parent_type, child_type, child_index, need_parentheses))) {
        LOG_WARN("fail to get need parentheses", K(ret));
      }
    }
  }
  return ret;
}

int ObReqOpExpr::need_parentheses_for_sub_expr(const ObReqOpExpr &expr, int expr_index, ObItemType root_type, int root_param_count, bool &need_parentheses) const {
  int ret = OB_SUCCESS;
  if (expr.params.count() > 1) {
    if (root_param_count == 1 && IS_LOGICAL_OPERATOR(root_type)) {
      need_parentheses = false;
    } else {
      switch (root_type) {
        case T_OP_NOT:
          need_parentheses = !is_atomic_expression(expr.get_op_type());
          break;
        case T_OP_AND:
          need_parentheses = expr.get_op_type() == T_OP_OR || expr.get_op_type() == T_OP_XOR;
          break;
        case T_OP_OR:
          need_parentheses = expr.get_op_type() == T_OP_AND;
          break;
        case T_OP_XOR:
          need_parentheses = expr.get_op_type() == T_OP_AND || expr.get_op_type() == T_OP_OR;
          break;
        default:
          if (OB_FAIL(need_parentheses_by_associativity(root_type, expr.get_op_type(), expr_index, need_parentheses))) {
            LOG_WARN("fail to get need parentheses", K(ret));
          }
          break;
      }
    }
  } else if (expr.params.count() == 1) {
    ObItemType expr_type = expr.get_op_type();
    bool is_and_or_xor = IS_LOGICAL_OPERATOR(expr_type);
    ObReqOpExpr *grand_child = dynamic_cast<ObReqOpExpr*>(expr.params.at(0));
    if (OB_NOT_NULL(grand_child)) {
      if (is_and_or_xor && grand_child->get_op_type() == T_OP_NOT) {
        need_parentheses = false;
      } else {
        ObItemType top_type = is_and_or_xor ? root_type : expr_type;
        int top_param_count = is_and_or_xor ? root_param_count : 1;
        int grand_child_index = is_and_or_xor ? expr_index : 0;
        ret = need_parentheses_for_sub_expr(*grand_child, grand_child_index, top_type, top_param_count, need_parentheses);
      }
    } else {
      need_parentheses = false;
    }
  } else {
    need_parentheses = false;
  }
  return ret;
}

void ObReqOpExpr::simplify_recursive() {
  for (uint64_t i = 0; i < params.count(); i++) {
    ObReqOpExpr *op_expr = dynamic_cast<ObReqOpExpr*>(params.at(i));
    if (OB_NOT_NULL(op_expr)) {
      op_expr->simplify_recursive();
      if (op_expr->params.count() == 1 && IS_SIMPLIFIABLE_OPERATOR(op_expr->get_op_type())) {
        params.at(i) = op_expr->params.at(0);
      }
    }
  }
}

bool ObReqOpExpr::need_right_operand_parentheses(ObItemType parent_type, int child_index) const {
  switch (parent_type) {
    case T_OP_MUL:
    case T_OP_DIV:
    case T_OP_MOD:
    case T_OP_POW:
    case T_OP_BIT_LEFT_SHIFT:
    case T_OP_BIT_RIGHT_SHIFT:
      return (child_index > 0);
    default:
      return false;
  }
}

bool is_left_associative(ObItemType op_type) {
  switch (op_type) {
    case T_OP_OR:
    case T_OP_XOR:
    case T_OP_AND:
    case T_OP_ADD:
    case T_OP_MINUS:
    case T_OP_MUL:
    case T_OP_DIV:
    case T_OP_MOD:
    case T_OP_BIT_OR:
    case T_OP_BIT_AND:
    case T_OP_BIT_XOR:
    case T_OP_BIT_LEFT_SHIFT:
    case T_OP_BIT_RIGHT_SHIFT:
      return true;
    case T_OP_POW:
    case T_OP_NOT:
    case T_OP_BIT_NEG:
    case T_OP_NEG:
    case T_OP_POS:
      return false;
    default:
      return true;
  }
}

int ObReqExpr::translate_alias(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_)
{
  int ret = OB_SUCCESS;
  if (!alias_name.empty()) {
    DATA_PRINTF(" as ");
    PRINT_IDENT(alias_name);
  }
  return ret;
}

int ObReqExpr::translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope/*= FIELD_LIST_SCOPE*/, bool need_alias /*= true*/)
{
  int ret = OB_SUCCESS;
  PRINT_IDENT(expr_name);
  for (int i = 0; i < params.count() && OB_SUCC(ret); i++) {
    ObReqExpr *param = params.at(i);
    if (i == 0) {
      DATA_PRINTF("(");
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(param->translate_expr(print_params_, buf_, buf_len_, pos_, scope, false))) {
      LOG_WARN("fail to translate expr", K(ret));
    } else if (i + 1 < params.count()) {
      DATA_PRINTF(", ");
    } else {
      DATA_PRINTF(")");
    }
  }
  if (OB_SUCC(ret) && need_alias && translate_alias(print_params_, buf_, buf_len_, pos_)) {
    LOG_WARN("fail to translate expr alias", K(ret));
  }
  return ret;
}

int ObReqColumnExpr::translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope/*= FIELD_LIST_SCOPE*/, bool need_alias /*= true*/)
{
  int ret = OB_SUCCESS;
  if (!table_name.empty()) {
    PRINT_IDENT(table_name);
    DATA_PRINTF(".");
  }
  PRINT_IDENT(expr_name);
  return ret;
}

int ObReqConstExpr::translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope/*= FIELD_LIST_SCOPE*/, bool need_alias /*= true*/)
{
  int ret = OB_SUCCESS;
  if (is_numeric_) {
    PRINT_IDENT(expr_name);
  } else if (var_type_ == ObNullType) {
    PRINT_IDENT("NULL");
  } else {
    DATA_PRINTF("'");
    PRINT_IDENT(expr_name);
    DATA_PRINTF("'");
  }
  if (OB_SUCC(ret) && need_alias && translate_alias(print_params_, buf_, buf_len_, pos_)) {
    LOG_WARN("fail to translate expr alias", K(ret));
  }
  return ret;
}

int ObReqMatchExpr::translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope/*= FIELD_LIST_SCOPE*/, bool need_alias /*= true*/)
{
  int ret = OB_SUCCESS;
  DATA_PRINTF("match(");
  int i = 0;
  // last param is query text
  for (; i + 1 < params.count() && OB_SUCC(ret); i++) {
    ObReqExpr *param = params.at(i);
    if (OB_FAIL(param->translate_expr(print_params_, buf_, buf_len_, pos_, scope, false))) {
      LOG_WARN("fail to translate expr", K(ret));
    } else if (i + 2 < params.count()) {
      DATA_PRINTF(", ");
    }
  }
  if (OB_SUCC(ret)) {
    DATA_PRINTF(") against(");
    ObReqExpr *param = params.at(i);
    if (OB_FAIL(param->translate_expr(print_params_, buf_, buf_len_, pos_, scope, false))) {
      LOG_WARN("fail to translate expr", K(ret));
    } else {
      switch (score_type_) {
        case SCORE_TYPE_BEST_FIELDS:
        case SCORE_TYPE_MOST_FIELDS:
        case SCORE_TYPE_CROSS_FIELDS:
          DATA_PRINTF(" in natural language mode)");
          break;
        case SCORE_TYPE_PHRASE:
          DATA_PRINTF(" in match phrase mode)");
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported score type", K(score_type_));
          break;
      }
    }
  }
  if (OB_SUCC(ret) && need_alias && translate_alias(print_params_, buf_, buf_len_, pos_)) {
    LOG_WARN("fail to translate expr alias", K(ret));
  }
  return ret;
}

int ObReqWindowFunExpr::translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope/*= FIELD_LIST_SCOPE*/, bool need_alias /*= true*/)
{
  int ret = OB_SUCCESS;
  PRINT_IDENT(expr_name);
  if (OB_FAIL(ret)) {
  } else {
    DATA_PRINTF("()");
    if (order_items_.count() > 0) {
      DATA_PRINTF(" over( ORDER BY ");
    }
    for (int i = 0; i < order_items_.count() && OB_SUCC(ret); i++) {
      OrderInfo *order_info = order_items_.at(i);
      if (OB_FAIL(order_info->translate(print_params_, buf_, buf_len_, pos_, ORDER_SCOPE))) {
        LOG_WARN("fail to translate expr", K(ret));
      } else if (i + 1 < order_items_.count()) {
        DATA_PRINTF(", ");
      }
    }
    if (order_items_.count() > 0) {
      DATA_PRINTF(")");
    }
  }
  if (OB_SUCC(ret) && need_alias && translate_alias(print_params_, buf_, buf_len_, pos_)) {
    LOG_WARN("fail to translate expr alias", K(ret));
  }
  return ret;
}

int ObReqWindowFunExpr::construct_window_fun_expr(ObIAllocator &alloc, OrderInfo *order_info, const ObString &expr_name, const ObString &alias, ObReqWindowFunExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(order_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_expr or query_expr is null", K(ret));
  } else if (OB_ISNULL(expr = OB_NEWx(ObReqWindowFunExpr, &alloc, expr_name, alias))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create match expr", K(ret));
  } else if (OB_FAIL(expr->order_items_.push_back(order_info))) {
    LOG_WARN("fail to push query order item", K(ret));
  }
  return ret;
}

int OrderInfo::translate(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope)
{
  int ret = OB_SUCCESS;
  if (order_item->alias_name.empty()) {
    if (OB_FAIL(order_item->translate_expr(print_params_, buf_, buf_len_, pos_, ORDER_SCOPE))) {
      LOG_WARN("fail to translate expr", K(ret));
    }
  } else {
    PRINT_IDENT(order_item->alias_name);
  }
  if (OB_FAIL(ret)) {
  } else if (ascent == false) {
    DATA_PRINTF(" DESC");
  }
  return ret;
}

int ObReqOpExpr::get_op_string(ObString &op_str)
{
  int ret = OB_SUCCESS;
  switch (op_type_) {
    case T_OP_ADD:
      op_str = "+";
      break;
    case T_OP_MINUS:
      op_str = "-";
      break;
    case T_OP_MUL:
      op_str = "*";
      break;
    case T_OP_DIV:
      op_str = "/";
      break;
    case T_OP_LE:
      op_str = "<=";
      break;
    case T_OP_LT:
      op_str = "<";
      break;
    case T_OP_EQ:
      op_str = "=";
      break;
    case T_OP_NSEQ:
      op_str = "<=>";
      break;
    case T_OP_GE:
      op_str = ">=";
      break;
    case T_OP_GT:
      op_str = ">";
      break;
    case T_OP_NE:
      op_str = "!=";
      break;
    case T_OP_AND:
      op_str = "AND";
      break;
    case T_OP_OR:
      op_str = "OR";
      break;
    case T_OP_NOT:
      op_str = "NOT";
      break;
    case T_OP_IS_NOT:
      op_str = "IS NOT";
      break;
    case T_OP_IN:
      op_str = "IN";
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported op type", K(ret), K(op_type_));
  }
  return ret;
}

int ObReqOpExpr::translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope/*= FIELD_LIST_SCOPE*/, bool need_alias /*= true*/)
{
  int ret = OB_SUCCESS;
  ObString symbol;

  if (op_type_ == T_OP_IN) {
    if (OB_FAIL(translate_in_expr(print_params_, buf_, buf_len_, pos_, scope, need_alias))) {
      LOG_WARN("fail to translate IN expr", K(ret));
    }
  } else {
    if (OB_FAIL(get_op_string(symbol))) {
      LOG_WARN("fail to get op string", K(ret));
    } else if (need_parentheses_) {
      DATA_PRINTF("(");
    }
    if (op_type_ == T_OP_NOT) {
      DATA_PRINTF("%.*s ", LEN_AND_PTR(symbol));
    }
    for (int i = 0; i < params.count() && OB_SUCC(ret); i++) {
      ObReqExpr *param = params.at(i);
      ObReqOpExpr *child_op_expr = dynamic_cast<ObReqOpExpr*>(param);
      if (OB_NOT_NULL(child_op_expr)) {
        bool need_parentheses = false;
        if (OB_SUCC(need_parentheses_for_child(*child_op_expr, i, need_parentheses))) {
          child_op_expr->need_parentheses_ = need_parentheses;
        } else {
          LOG_WARN("fail to get need parentheses", K(ret));
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(child_op_expr) && child_op_expr->get_op_type() == T_OP_IN) {
        if (OB_FAIL(child_op_expr->translate_in_expr(print_params_, buf_, buf_len_, pos_, scope, false))) {
          LOG_WARN("fail to translate IN expr", K(ret));
        }
      } else if (OB_FAIL(param->translate_expr(print_params_, buf_, buf_len_, pos_, scope, false))) {
        LOG_WARN("fail to translate expr", K(ret));
      }
      if (OB_SUCC(ret) && i + 1 < params.count()) {
        if (op_type_ == T_OP_NOT) {
          DATA_PRINTF(" AND ");
        } else {
          DATA_PRINTF(" %.*s ", LEN_AND_PTR(symbol));
        }
      }
    }
    if (OB_SUCC(ret) && need_parentheses_) {
      DATA_PRINTF(")");
    }
    if (OB_SUCC(ret) && need_alias && translate_alias(print_params_, buf_, buf_len_, pos_)) {
      LOG_WARN("fail to translate expr alias", K(ret));
    }
  }
  return ret;
}

int ObReqOpExpr::translate_in_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope/*= FIELD_LIST_SCOPE*/, bool need_alias /*= true*/)
{
  int ret = OB_SUCCESS;
  if (need_parentheses_) {
    DATA_PRINTF("(");
  }
  if (params.count() >= 1) {
    if (OB_FAIL(params.at(0)->translate_expr(print_params_, buf_, buf_len_, pos_, scope, false))) {
      LOG_WARN("fail to translate IN column expr", K(ret));
    } else {
      DATA_PRINTF(" IN (");
      for (int i = 1; i < params.count() && OB_SUCC(ret); i++) {
        if (i > 1) {
          DATA_PRINTF(", ");
        }
        if (OB_FAIL(params.at(i)->translate_expr(print_params_, buf_, buf_len_, pos_, scope, false))) {
          LOG_WARN("fail to translate IN value expr", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        DATA_PRINTF(")");
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("IN expr must have at least two values", K(ret));
  }
  if (OB_SUCC(ret) && need_parentheses_) {
    DATA_PRINTF(")");
  }
  if (OB_SUCC(ret) && need_alias && translate_alias(print_params_, buf_, buf_len_, pos_)) {
    LOG_WARN("fail to translate expr alias", K(ret));
  }
  return ret;
}

int ObReqCaseWhenExpr::translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope/*= FIELD_LIST_SCOPE*/, bool need_alias /*= true*/)
{
  int ret = OB_SUCCESS;
  DATA_PRINTF("(case");
  if (OB_SUCC(ret)) {
    if (arg_expr_ != NULL) {
      DATA_PRINTF(" ");
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(arg_expr_->translate_expr(print_params_, buf_, buf_len_, pos_, scope, false))) {
        LOG_WARN("fail to translate expr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < when_exprs_.count(); i++) {
      DATA_PRINTF(" when ");
      ObReqExpr *when_param = when_exprs_.at(i);
      ObReqExpr *then_param = then_exprs_.at(i);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(when_param->translate_expr(print_params_, buf_, buf_len_, pos_, scope, false))) {
        LOG_WARN("fail to translate expr", K(ret));
      } else {
        DATA_PRINTF(" then ");
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(then_param->translate_expr(print_params_, buf_, buf_len_, pos_, scope, false))) {
          LOG_WARN("fail to translate expr", K(ret));
        }
      }
    }
    DATA_PRINTF(" else");
    if (OB_SUCC(ret)) {
      if (NULL != default_expr_) {
        DATA_PRINTF(" ");
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(default_expr_->translate_expr(print_params_, buf_, buf_len_, pos_, scope, false))) {
          LOG_WARN("fail to translate expr", K(ret));
        }
      }
    }
    DATA_PRINTF(" end)");
  }
  return ret;
}

int ObReqOpExpr::init(ObReqExpr *l_para, ObReqExpr *r_para, ObItemType type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(params.push_back(l_para))) {
    LOG_WARN("fail to push back sub query", K(ret));
  } else if (OB_FAIL(params.push_back(r_para))) {
    LOG_WARN("fail to push back sub query", K(ret));
  } else if (OB_FAIL(set_op_name())) {
    LOG_WARN("fail to set op name", K(ret));
  }
  return ret;
}

int ObReqOpExpr::set_op_name()
{
  int ret = OB_SUCCESS;
  ObString op_str;
  if (OB_FAIL(get_op_string(op_str))) {
    LOG_WARN("fail to get op string", K(ret));
  } else {
    expr_name = op_str;
  }
  return ret;
}

void ObReqJoinedTable::init(ObReqTable *left_table, ObReqTable *right_table, ObReqExpr *condition, ObReqJoinType joined_type)
{
  table_type_ = ReqTableType::JOINED_TABLE;
  joined_type_ = joined_type;
  left_table_ = left_table;
  right_table_ = right_table;
  condition_ = condition;
}

int ObReqExpr::construct_expr(ObReqExpr *&expr, ObIAllocator &alloc, const ObString &expr_name, const ObString &alias_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr = OB_NEWx(ObReqExpr, &alloc, expr_name, alias_name))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create expr", K(ret));
  }
  return ret;
}

int ObReqExpr::construct_expr(ObReqExpr *&expr, ObIAllocator &alloc, const ObString &expr_name, ObReqExpr *param, const ObString &alias_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr = OB_NEWx(ObReqExpr, &alloc, expr_name, alias_name))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create expr", K(ret));
  } else if (OB_FAIL(expr->params.push_back(param))) {
    LOG_WARN("fail to push back param", K(ret));
  }
  return ret;
}

int ObReqExpr::construct_expr(ObReqExpr *&expr, ObIAllocator &alloc, const ObString &expr_name, ObReqExpr *param1, ObReqExpr *param2, const ObString &alias_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr = OB_NEWx(ObReqExpr, &alloc, expr_name, alias_name))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create expr", K(ret));
  } else if (OB_FAIL(expr->params.push_back(param1))) {
    LOG_WARN("fail to push back param1", K(ret));
  } else if (OB_FAIL(expr->params.push_back(param2))) {
    LOG_WARN("fail to push back param2", K(ret));
  }
  return ret;
}

int ObReqExpr::construct_expr(ObReqExpr *&expr, ObIAllocator &alloc, const ObString &expr_name, const common::ObIArray<ObReqExpr *> &params, const ObString &alias_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr = OB_NEWx(ObReqExpr, &alloc, expr_name, alias_name))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create expr", K(ret));
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
      if (OB_FAIL(expr->params.push_back(params.at(i)))) {
        LOG_WARN("fail to push back param", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObReqColumnExpr::construct_column_expr(ObReqColumnExpr *&expr, ObIAllocator &alloc, const ObString &expr_name, double weight)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr = OB_NEWx(ObReqColumnExpr, &alloc, expr_name, ObString(), weight))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create column expr", K(ret));
  }
  return ret;
}

int ObReqColumnExpr::construct_column_expr(ObReqColumnExpr *&expr, ObIAllocator &alloc, const ObString &expr_name, const ObString &table_name, double weight)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr = OB_NEWx(ObReqColumnExpr, &alloc, expr_name, table_name, weight))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create column expr", K(ret));
  }
  return ret;
}

int ObReqConstExpr::construct_const_expr(ObReqConstExpr *&expr, ObIAllocator &alloc, const ObString &expr_name, ObObjType var_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr = OB_NEWx(ObReqConstExpr, &alloc, var_type, expr_name))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create const expr", K(ret));
  } else {
    expr->set_numeric();
  }
  return ret;
}

int ObReqConstExpr::construct_const_numeric_expr(ObReqConstExpr *&expr, ObIAllocator &alloc, double num_value, ObObjType var_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr = OB_NEWx(ObReqConstExpr, &alloc, var_type, ObString()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create const expr", K(ret));
  } else if (OB_FAIL(expr->set_numeric(alloc, num_value, var_type))) {
    LOG_WARN("fail to set numeric properties", K(ret));
  }
  return ret;
}

int ObReqConstExpr::set_numeric(ObIAllocator &alloc, double numeric_value, ObObjType var_type)
{
  int ret = OB_SUCCESS;
  if (!ob_is_numeric_type(var_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("var_type is not numeric type", K(ret), K(var_type));
  } else {
    char buf[32];
    int64_t len = 0;
    switch (var_type) {
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType:
        len = snprintf(buf, sizeof(buf), "%ld", static_cast<int64_t>(numeric_value));
        break;
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type:
        len = snprintf(buf, sizeof(buf), "%lu", static_cast<uint64_t>(numeric_value));
        break;
      case ObFloatType:
        len = snprintf(buf, sizeof(buf), "%.7g", numeric_value);
        break;
      case ObDoubleType:
      case ObNumberType:
      case ObNumberFloatType:
      default:
        len = snprintf(buf, sizeof(buf), "%.15g", numeric_value);
        break;
    }
    char *num_str = static_cast<char *>(alloc.alloc(len));
    if (OB_ISNULL(num_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for numeric string", K(ret), K(len));
    } else {
      MEMCPY(num_str, buf, len);
      expr_name.assign_ptr(num_str, static_cast<int32_t>(len));
      is_numeric_ = true;
      numeric_value_ = numeric_value;
      var_type_ = var_type;
    }
  }
  return ret;
}

int ObReqMatchExpr::construct_match_expr(ObReqMatchExpr *&expr, ObIAllocator &alloc, ObReqExpr *field_expr, ObReqExpr *query_expr, ObEsScoreType score_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(field_expr) || OB_ISNULL(query_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_expr or query_expr is null", K(ret));
  } else if (OB_ISNULL(expr = OB_NEWx(ObReqMatchExpr, &alloc, score_type))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create match expr", K(ret));
  } else if (OB_FAIL(expr->params.push_back(field_expr))) {
    LOG_WARN("fail to add field to match expr", K(ret));
  } else if (OB_FAIL(expr->params.push_back(query_expr))) {
    LOG_WARN("fail to add query to match expr", K(ret));
  }
  return ret;
}

int ObReqCaseWhenExpr::construct_case_when_expr(ObReqCaseWhenExpr *&expr, ObIAllocator &alloc,
                                                const common::ObIArray<ObReqExpr *> &when_exprs,
                                                const common::ObIArray<ObReqExpr *> &then_exprs,
                                                ObReqExpr *default_expr,
                                                ObReqExpr *arg_expr)
{
  int ret = OB_SUCCESS;
  if (when_exprs.count() != then_exprs.count() || when_exprs.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("when_exprs and then_exprs count mismatch", K(ret), K(when_exprs.count()), K(then_exprs.count()));
  } else if (OB_ISNULL(expr = OB_NEWx(ObReqCaseWhenExpr, &alloc, arg_expr, when_exprs, then_exprs, default_expr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create case when expr", K(ret));
  }
  return ret;
}

int ObReqCaseWhenExpr::construct_case_when_expr(ObReqCaseWhenExpr *&expr, ObIAllocator &alloc,
                                                ObReqExpr *when_expr,
                                                ObReqExpr *then_expr,
                                                ObReqExpr *default_expr,
                                                ObReqExpr *arg_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(when_expr) || OB_ISNULL(then_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("when_expr or then_expr is null", K(ret));
  } else if (OB_ISNULL(expr = OB_NEWx(ObReqCaseWhenExpr, &alloc, default_expr, when_expr, then_expr, arg_expr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create case when expr", K(ret));
  }
  return ret;
}

int ObReqOpExpr::construct_binary_op_expr(ObReqOpExpr *&expr, ObIAllocator &alloc, ObItemType type, ObReqExpr *l_param, ObReqExpr *r_param, const ObString &alias_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(l_param) || OB_ISNULL(r_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is null", K(ret), K(l_param), K(r_param));
  } else if (OB_ISNULL(expr = OB_NEWx(ObReqOpExpr, &alloc, type))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(expr->init(l_param, r_param, type))) {
    LOG_WARN("fail to init op expr", K(ret));
  } else if (!alias_name.empty()) {
    expr->set_alias(alias_name);
  }
  return ret;
}

int ObReqOpExpr::construct_unary_op_expr(ObReqOpExpr *&expr, ObIAllocator &alloc, ObItemType type, ObReqExpr *param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is null", K(ret), K(param));
  } else if (OB_ISNULL(expr = OB_NEWx(ObReqOpExpr, &alloc, type))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create unary op expr", K(ret));
  } else if (OB_FAIL(expr->params.push_back(param))) {
    LOG_WARN("fail to push back param", K(ret));
  } else if (OB_FAIL(expr->set_op_name())) {
    LOG_WARN("fail to set op name", K(ret));
  }
  return ret;
}

int ObReqOpExpr::construct_op_expr(ObReqOpExpr *&expr, ObIAllocator &alloc, ObItemType type, const common::ObIArray<ObReqExpr *> &params)
{
  int ret = OB_SUCCESS;
  if (params.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params array is empty", K(ret));
  } else if (OB_ISNULL(expr = OB_NEWx(ObReqOpExpr, &alloc, type))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create op expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
      if (OB_FAIL(expr->params.push_back(params.at(i)))) {
        LOG_WARN("fail to push back param", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(expr->set_op_name())) {
      LOG_WARN("fail to set op name", K(ret));
    }
  }
  return ret;
}

int ObReqOpExpr::construct_in_expr(ObIAllocator &alloc, ObReqColumnExpr *col_expr, common::ObIArray<ObReqConstExpr *> &value_exprs, ObReqOpExpr *&in_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_expr) || value_exprs.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(in_expr = OB_NEWx(ObReqOpExpr, &alloc, T_OP_IN))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create in expr", K(ret));
  } else if (OB_ISNULL(col_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpectd null ptr", K(ret));
  } else if (OB_FAIL(in_expr->params.push_back(col_expr))) {
    LOG_WARN("fail to push in expr param", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < value_exprs.count(); i++) {
      if (OB_ISNULL(value_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpectd null ptr", K(ret));
      } else if (OB_FAIL(in_expr->params.push_back(value_exprs.at(i)))) {
        LOG_WARN("fail to push in expr param", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase