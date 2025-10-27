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
#include "ob_query_translator.h"

namespace oceanbase
{
using namespace sql;
namespace share
{

int ObQueryTranslator::translate()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(translate_select())) {
    LOG_WARN("fail to translate select items", K(ret));
  } else if (OB_FAIL(translate_from())) {
    LOG_WARN("fail to translate from items", K(ret));
  } else if (OB_FAIL(translate_where())) {
    LOG_WARN("fail to translate where items", K(ret));
  } else if (OB_FAIL(translate_group_by())) {
    LOG_WARN("fail to translate group by items", K(ret));
  } else if (OB_FAIL(translate_order_by())) {
    LOG_WARN("fail to translate order by items", K(ret));
  } else if (req_->has_vec_approx()) {
    DATA_PRINTF(" APPROXIMATE");
  }
  if (OB_SUCC(ret) && translate_limit()) {
    LOG_WARN("fail to translate limit items", K(ret));
  }

  return ret;
}

int ObRequestTranslator::translate_hints()
{
  int ret = OB_SUCCESS;
  const ObQueryReqFromJson *query_req = static_cast<const ObQueryReqFromJson*>(req_);
  int hint_count = query_req->req_hints_.count();
  if (hint_count > 0 || query_req->match_idxs_.count() >= 3) {
    DATA_PRINTF("/*+ ");
  }
  for (int i = 0; i < query_req->req_hints_.count() && OB_SUCC(ret); i++) {
    PRINT_IDENT(query_req->req_hints_.at(i));
    DATA_PRINTF(" ");
  }
  if (OB_SUCC(ret) && query_req->match_idxs_.count() >= 3) {
    DATA_PRINTF("union_merge( ");
    for (int i = 0; i < query_req->match_idxs_.count() && OB_SUCC(ret); i++) {
      PRINT_IDENT(query_req->match_idxs_.at(i));
      if (i + 1 < query_req->match_idxs_.count()) {
        DATA_PRINTF(" ");
      } else {
        DATA_PRINTF(")");
      }
    }
  }
  if (hint_count > 0 || query_req->match_idxs_.count() >= 3) {
    DATA_PRINTF("*/");
  }

  return ret;
}

int ObQueryTranslator::translate_select()
{
  int ret = OB_SUCCESS;
  const ObQueryReqFromJson *query_req = static_cast<const ObQueryReqFromJson*>(req_);
  for (int i = 0; i < query_req->score_items_.count(); i++) {
    ObReqOpExpr *op_expr = dynamic_cast<ObReqOpExpr*>(query_req->score_items_.at(i));
    if (OB_NOT_NULL(op_expr)) {
      op_expr->simplify_recursive();
    }
  }
  int item_count = query_req->select_items_.count();
  int score_count = query_req->score_items_.count();
  DATA_PRINTF("SELECT ");
  if (OB_FAIL(translate_hints())) {
    LOG_WARN("fail to hints", K(ret));
  } else if (query_req->output_all_columns_) {
    DATA_PRINTF("*");
  }
  if (OB_SUCC(ret) && item_count > 0 && query_req->output_all_columns_) {
    DATA_PRINTF(", ");
  }
  for (int i = 0; i < item_count && OB_SUCC(ret); i++) {
    ObReqExpr *expr = query_req->select_items_.at(i);
    if (OB_FAIL(expr->translate_expr(print_params_, buf_, buf_len_, pos_, FIELD_LIST_SCOPE))) {
      LOG_WARN("fail to translate expr", K(ret));
    } else if (i + 1 < item_count) {
      DATA_PRINTF(", ");
    }
  }
  if (OB_SUCC(ret) && (item_count > 0 || query_req->output_all_columns_) && score_count > 0) {
    DATA_PRINTF(", ");
  }
  for (int i = 0; i < score_count && OB_SUCC(ret); i++) {
    ObReqExpr *expr = query_req->score_items_.at(i);
    if (i == 0 && score_count > 1) {
       DATA_PRINTF("(");
    } else if (score_count == 1) {
      ObReqOpExpr *op_expr = dynamic_cast<ObReqOpExpr*>(expr);
      if (OB_NOT_NULL(op_expr) && op_expr->has_multi_params_recursive()) {
        op_expr->need_parentheses_ = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(expr->translate_expr(print_params_, buf_, buf_len_, pos_, FIELD_LIST_SCOPE, false))) {
      LOG_WARN("fail to translate expr", K(ret));
    } else if (i + 1 < score_count) {
      DATA_PRINTF(" + ");
    } else {
      if (score_count > 1) {
        DATA_PRINTF(")");
      }
      if (OB_FAIL(ret)) {
      } else if (score_count == 1 && expr->expr_name == "_score" &&
                 query_req->score_alias_.empty()) {
        // do nothing
      } else if (query_req->score_alias_.empty()) {
        DATA_PRINTF(" as _score");
      } else {
        DATA_PRINTF(" as ");
        PRINT_IDENT(query_req->score_alias_);
      }
    }
  }
  return ret;
}

int ObQueryTranslator::translate_order(const OrderInfo *order_info)
{
  int ret = OB_SUCCESS;
  if (order_info->order_item->alias_name.empty()) {
    if (OB_FAIL(order_info->order_item->translate_expr(print_params_, buf_, buf_len_, pos_, ORDER_SCOPE))) {
      LOG_WARN("fail to translate expr", K(ret));
    }
  } else {
    PRINT_IDENT(order_info->order_item->alias_name);
  }
  if (OB_FAIL(ret)) {
  } else if (order_info->ascent == false) {
    DATA_PRINTF(" DESC");
  }
  return ret;
}

int ObQueryTranslator::translate_order_by()
{
  int ret = OB_SUCCESS;
  if (req_->order_items_.count() > 0) {
    DATA_PRINTF(" ORDER BY ");
  }
  for (int i = 0; i < req_->order_items_.count() && OB_SUCC(ret); i++) {
    OrderInfo *order_info = req_->order_items_.at(i);
    if (OB_FAIL(order_info->translate(print_params_, buf_, buf_len_, pos_, ORDER_SCOPE))) {
      LOG_WARN("fail to translate expr", K(ret));
    } else if (i + 1 < req_->order_items_.count()) {
      DATA_PRINTF(", ");
    }
  }
  return ret;
}

int ObQueryTranslator::translate_group_by()
{
  int ret = OB_SUCCESS;
  const ObQueryReqFromJson *query_req = static_cast<const ObQueryReqFromJson*>(req_);
  if (query_req->group_items_.count() > 0) {
    DATA_PRINTF(" GROUP BY ");
  }
  for (int i = 0; i < query_req->group_items_.count() && OB_SUCC(ret); i++) {
    ObReqExpr *item_expr = query_req->group_items_.at(i);
    if (OB_FAIL(item_expr->translate_expr(print_params_, buf_, buf_len_, pos_, FIELD_LIST_SCOPE))) {
      LOG_WARN("fail to translate expr", K(ret));
    } else if (i + 1 < query_req->group_items_.count()) {
      DATA_PRINTF(", ");
    }
  }
  return ret;
}

int ObRequestTranslator::translate_table(const ObReqTable *table)
{
  int ret = OB_SUCCESS;
  if (table->table_type_ == ReqTableType::BASE_TABLE) {
    if (!table->database_name_.empty()) {
      PRINT_IDENT(table->database_name_);
      DATA_PRINTF(".");
    }
    PRINT_IDENT(table->table_name_);
  } else if (table->table_type_ == ReqTableType::SUB_QUERY) {
    ObQueryReqFromJson *ref_query = static_cast<ObQueryReqFromJson *>(table->ref_query_);
    int64_t res_len = 0;
    DATA_PRINTF("(");
    if (OB_FAIL(ref_query->translate((buf_ + *pos_), (buf_len_ - *pos_), res_len))) {
      LOG_WARN("failed to translate ref query", K(ret), K(*pos_), K(buf_len_));
    } else {
      (*pos_) += res_len;
      DATA_PRINTF(") ");
      PRINT_IDENT(table->alias_name_);
    }
  } else if (table->table_type_ == ReqTableType::MULTI_SET) {
    const ObMultiSetTable *mul_tab = static_cast<const ObMultiSetTable *>(table);
    DATA_PRINTF("(");
    for (uint64_t i = 0; i < mul_tab->sub_queries_.count() && OB_SUCC(ret); i++) {
      if (i > 0) {
        if (mul_tab->joined_type_ == ObReqJoinType::UNION_ALL) {
          DATA_PRINTF(" UNION ALL ");
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected join type", K(ret), K(mul_tab->joined_type_));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(translate_table(mul_tab->sub_queries_.at(i)))) {
        LOG_WARN("left_table translate failed", K(ret));
      }
    }
    DATA_PRINTF(")");
  } else if (table->table_type_ == ReqTableType::JOINED_TABLE) {
    const ObReqJoinedTable *jt = static_cast<const ObReqJoinedTable *>(table);
    DATA_PRINTF("(");
    if (OB_ISNULL(jt->left_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left_table should not be NULL", K(ret));
    } else if (OB_FAIL(translate_table(jt->left_table_))) {
      LOG_WARN("left_table translate failed", K(ret));
    } else {
      ObString join_type;
      switch (jt->joined_type_) {
        case FULL_OUTER_JOIN :
          join_type = "full join";
          break;
        case LEFT_OUTER_JOIN :
          join_type = "left join";
          break;
        case RIGHT_OUTER_JOIN :
          join_type = "right join";
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unknown join type", K(ret), K(jt->joined_type_));
          break;
      }
      if (OB_SUCC(ret)) {
        DATA_PRINTF(" %.*s ", LEN_AND_PTR(join_type));
        if (OB_FAIL(translate_table(jt->right_table_))) {
          LOG_WARN("left_table translate failed", K(ret));
        } else {
          DATA_PRINTF(" on ");
          if (OB_FAIL(jt->condition_->translate_expr(print_params_, buf_, buf_len_, pos_, ON_SCOPE))) {
            LOG_WARN("translate join condition failed", K(ret));
          } else {
            DATA_PRINTF(")");
          }
        }
      }
    }

  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported table type", K(ret), K(table->table_type_));
  }

  return ret;
}

int ObRequestTranslator::translate_from()
{
  int ret = OB_SUCCESS;
  DATA_PRINTF(" FROM ");
  for (int i = 0; i < req_->from_items_.count() && OB_SUCC(ret); i++) {
    ObReqTable *table = req_->from_items_.at(i);
    if (OB_FAIL(translate_table(table))) {
      LOG_WARN("fail to translate table", K(ret), K(i));
    } else if (i + 1 < req_->from_items_.count()) {
      DATA_PRINTF(", ");
    }
  }
  return ret;
}

int ObRequestTranslator::translate_where()
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < req_->condition_items_.count(); i++) {
    ObReqOpExpr *op_expr = dynamic_cast<ObReqOpExpr*>(req_->condition_items_.at(i));
    if (OB_NOT_NULL(op_expr)) {
      op_expr->simplify_recursive();
    }
  }
  if (req_->condition_items_.count() > 0) {
    DATA_PRINTF(" WHERE ");
  }
  for (int i = 0; i < req_->condition_items_.count() && OB_SUCC(ret); i++) {
    ObReqExpr *expr = req_->condition_items_.at(i);
    if (ObReqOpExpr* op_expr = dynamic_cast<ObReqOpExpr*>(expr)) {
      if (req_->condition_items_.count() == 1) {
        op_expr->need_parentheses_ = false;
      }
    }
    if (OB_FAIL(expr->translate_expr(print_params_, buf_, buf_len_, pos_, WHERE_SCOPE, false))) {
      LOG_WARN("fail to translate expr", K(ret));
    }
  }
  return ret;
}

int ObRequestTranslator::translate_limit()
{
  int ret = OB_SUCCESS;
  if (req_->limit_item_ != NULL)  {
    DATA_PRINTF(" LIMIT ");
    if (req_->offset_item_ != NULL) {
      if (OB_FAIL(req_->offset_item_->translate_expr(print_params_, buf_, buf_len_, pos_, LIMIT_SCOPE)) ) {
        LOG_WARN("fail to translate expr", K(ret));
      } else {
        DATA_PRINTF(", ");
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(req_->limit_item_->translate_expr(print_params_, buf_, buf_len_, pos_, LIMIT_SCOPE)) ) {
      LOG_WARN("fail to translate expr", K(ret));
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase