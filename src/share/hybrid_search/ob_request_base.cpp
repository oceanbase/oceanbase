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
#include "sql/printer/ob_raw_expr_printer.h"


namespace oceanbase
{
using namespace sql;
namespace share
{

int get_op_precedence(ObItemType op_type) {
  switch (op_type) {
    case T_OP_OR:
    case T_OP_XOR:
      return PREC_OR_XOR;
    case T_OP_AND:
      return PREC_AND;
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
    case T_OP_BIT_OR:
    case T_OP_BIT_AND:
    case T_OP_BIT_XOR:
    case T_OP_BIT_LEFT_SHIFT:
    case T_OP_BIT_RIGHT_SHIFT:
      return PREC_BIT;
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
  if (ob_is_numeric_type(var_type_) || var_type_ == ObNullType) {
    PRINT_IDENT(expr_name);
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
          DATA_PRINTF(" in boolean mode)");
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

int ObReqOpExpr::get_op_string(ObString &op_str, bool &need_parentheses)
{
  int ret = OB_SUCCESS;
  need_parentheses = false;
  switch (op_type_) {
    case T_OP_ADD:
      op_str = "+";
      need_parentheses = true;
      break;
    case T_OP_MINUS:
      op_str = "-";
      need_parentheses = true;
      break;
    case T_OP_MUL:
      op_str = "*";
      need_parentheses = true;
      break;
    case T_OP_DIV:
      op_str = "/";
      need_parentheses = true;
      break;
    case T_OP_LE:
      op_str = "<=";
      break;
    case T_OP_LT:
      op_str = "<";
      break;
    case T_OP_EQ:
      op_str = "=";
      need_parentheses = true;
      break;
    case T_OP_NSEQ:
      op_str = "<=>";
      break;
    case T_OP_GE:
      op_str = ">=";
      break;
    case T_OP_GT:
      op_str = ">";
      need_parentheses = true;
      break;
    case T_OP_NE:
      op_str = "!=";
      break;
    case T_OP_AND:
      op_str = "AND";
      need_parentheses = true;
      break;
    case T_OP_OR:
      op_str = "OR";
      need_parentheses = true;
      break;
    case T_OP_NOT:
      op_str = "NOT";
      need_parentheses = true;
      break;
    case T_OP_IS_NOT:
      op_str = "IS NOT";
      need_parentheses = true;
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
  bool need_parentheses = false;
  if (OB_FAIL(get_op_string(symbol, need_parentheses))) {
    LOG_WARN("fail to get op string", K(ret));
  } else if (need_parentheses_ && need_parentheses) {
    DATA_PRINTF("(");
  }
  if (op_type_ == T_OP_NOT) {
    DATA_PRINTF("%.*s ", LEN_AND_PTR(symbol));
    if (params.count() > 1) {
      DATA_PRINTF("(");
    }
  }
  for (int i = 0; i < params.count() && OB_SUCC(ret); i++) {
    ObReqExpr *param = params.at(i);
    ObReqOpExpr *op_expr = dynamic_cast<ObReqOpExpr*>(param);
    if (op_expr != nullptr) {
      if (op_type_ == T_OP_OR && op_expr->get_op_type() == T_OP_AND) {
        op_expr->need_parentheses_ = true;
      } else {
        int current_prec = get_op_precedence(op_type_);
        int param_prec = get_op_precedence(op_expr->get_op_type());
        if (current_prec > 0 && param_prec > 0) {
          if (current_prec > param_prec) {
            op_expr->need_parentheses_ = true;
          }
        }
      }
    }
    if (OB_FAIL(param->translate_expr(print_params_, buf_, buf_len_, pos_, scope, false))) {
      LOG_WARN("fail to translate expr", K(ret));
    } else if (i + 1 < params.count()) {
      if (op_type_ == T_OP_NOT) {
        DATA_PRINTF(" AND ");
      } else {
        DATA_PRINTF(" %.*s ", LEN_AND_PTR(symbol));
      }
    }
  }
  if (op_type_ == T_OP_NOT && params.count() > 1) {
    DATA_PRINTF(")");
  }
  if (OB_SUCC(ret) && need_parentheses_ && need_parentheses) {
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
  } else {
    op_type_ = type;
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

}  // namespace share
}  // namespace oceanbase