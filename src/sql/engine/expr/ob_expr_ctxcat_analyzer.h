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

#ifndef OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_CTXCAT_ANALYZER_H_
#define OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_CTXCAT_ANALYZER_H_
#include "sql/ob_sql_define.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_iter_expr_index_scan.h"
#include "lib/charset/ob_ctype.h"
namespace oceanbase {
namespace sql {
class ObExprOperatorFactory;
class ObExprCtxCatAnalyzer {
public:
  ObExprCtxCatAnalyzer(ObExprOperatorFactory& expr_factory) : expr_factory_(expr_factory)
  {}
  int parse_search_keywords(common::ObObj& keyword_text, ObMatchAgainstMode mode_flag, ObIterExprOperator*& search_tree,
      common::ObIArray<common::ObObj>& keywords);

private:
  int create_set_op_tree(
      common::ObIArray<ObIndexScanIterExpr*>& set_keys, ObItemType set_op_type, ObIterExprOperator*& search_tree);
  int ft_get_simple_words(
      common::ObObj& keyword_text, ObIterExprOperator*& search_tree, common::ObIArray<common::ObObj>& keywords);
  int ft_get_boolean_words(
      common::ObObj& keyword_text, ObIterExprOperator*& search_tree, common::ObIArray<common::ObObj>& keywords);

private:
  ObExprOperatorFactory& expr_factory_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_CTXCAT_ANALYZER_H_ */
