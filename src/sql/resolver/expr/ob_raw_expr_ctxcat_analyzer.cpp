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
#include "sql/resolver/expr/ob_raw_expr_ctxcat_analyzer.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

namespace oceanbase {
using namespace common;
namespace sql {
/*int ObRawExprCtxCatAnalyzer::splite_search_keywords(ObFunMatchAgainst &expr)
{
  int ret = OB_SUCCESS;
  //1. create search expr tree
  ObRawExpr *search_tree = NULL;
  ObString keyword_text;
  const ObRawExpr *search_key = expr.get_search_key();
  ObArray<ObString> keywords;
  if (OB_ISNULL(search_key)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("search_key is null");
  } else if (!search_key->is_const_expr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("search_key isn't const expr");
  } else if (OB_FAIL(static_cast<const ObConstRawExpr*>(search_key)->get_value().get_string(keyword_text))) {
    LOG_WARN("get keyword text failed", K(ret));
  } else if (OB_FAIL(create_search_expr_tree(keyword_text, expr.get_mode_flag(), search_tree, keywords))) {
    LOG_WARN("create search expr tree failed", K(ret));
  } else {
    expr.set_search_tree(search_tree);
  }
  //2. add fulltext column filter
  for (int64_t i = 0; OB_SUCC(ret) && i < keywords.count(); ++i) {
    ObString &keyword = keywords.at(i);
    ObObj tmp = static_cast<const ObConstRawExpr*>(search_key)->get_value();
    ObRawExpr *fulltext_key = NULL;
    tmp.set_varchar(keyword);
    ObConstRawExpr *keyword_expr = NULL;
    if (OB_FAIL(expr_factory_.create_raw_expr(T_VARCHAR, keyword_expr))) {
      LOG_WARN("create keyword expr failed", K(ret));
    } else if (OB_ISNULL(keyword_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("keyword_expr is null");
    } else {
      keyword_expr->set_value(tmp);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObRawExprUtils::create_equal_expr(expr_factory_, session_info_, keyword_expr,
                                                    expr.get_real_column(), fulltext_key))) {
        LOG_WARN("create equal expr failed", K(ret));
      } else if (OB_FAIL(expr.add_fulltext_key(fulltext_key))) {
        LOG_WARN("add fulltext key to match against failed", K(ret));
      }
    }
  }
  return ret;
}*/

int ObRawExprCtxCatAnalyzer::create_fulltext_filter(ObFunMatchAgainst& expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr* fulltext_filter = NULL;
  if (OB_FAIL(ObRawExprUtils::create_equal_expr(
          expr_factory_, session_info_, expr.get_search_key(), expr.get_real_column(), fulltext_filter))) {
    LOG_WARN("create equal expr failed", K(ret));
  } else {
    expr.set_fulltext_filter(fulltext_filter);
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
