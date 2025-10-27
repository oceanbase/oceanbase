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
#include "ob_query_request.h"
#include "ob_query_translator.h"

namespace oceanbase
{
namespace share
{

int ObQueryReqFromJson::translate(char *buf, int64_t buf_len, int64_t &res_len)
{
  int ret = OB_SUCCESS;
  ObQueryTranslator translator(buf, buf_len, &res_len, this);
  if (OB_FAIL(translator.translate())) {
    LOG_WARN("fail to translate request to sql", K(ret));
  }
  return ret;
}

int ObQueryReqFromJson::add_score_item(ObIAllocator &alloc, ObReqExpr *score_item)
{
  int ret = OB_SUCCESS;
  int count = score_items_.count();
  if (count == 0) {
    if (OB_FAIL(score_items_.push_back(score_item))) {
      LOG_WARN("fail to append score item", K(ret));
    } else if (score_item->alias_name.empty()) {
      score_item->set_alias("_score");
    }
  } else {
    ObReqOpExpr* op_expr = NULL;
    if (score_items_.at(count - 1)->get_expr_type() == ObReqExprType::OP_EXPR) {
      op_expr = static_cast<ObReqOpExpr*>(score_items_.at(count - 1));
    }
    if (OB_NOT_NULL(op_expr) && op_expr->get_op_type() == T_OP_ADD) {
      if (OB_FAIL(op_expr->params.push_back(score_item))) {
        LOG_WARN("fail to append param", K(ret));
      }
    } else {
      ObReqOpExpr *add_expr = NULL;
      ObString empty_str;
      if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(add_expr, alloc, T_OP_ADD, score_items_.at(count - 1), score_item, "_score"))) {
        LOG_WARN("fail to create op expr", K(ret));
      } else if (score_items_.at(count - 1)->alias_name.compare("_score") == 0 &&
                 FALSE_IT(score_items_.at(count - 1)->set_alias(empty_str))) {
      } else if (FALSE_IT(score_items_.pop_back())) {
      } else if (OB_FAIL(score_items_.push_back(add_expr))) {
        LOG_WARN("fail to append expr", K(ret));
      }
    }
  }
  return ret;
}
}  // namespace share
}  // namespace oceanbase