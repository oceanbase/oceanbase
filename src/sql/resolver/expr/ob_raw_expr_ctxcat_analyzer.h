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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_EXPR_OB_RAW_EXPR_CTXCAT_ANALYZER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_EXPR_OB_RAW_EXPR_CTXCAT_ANALYZER_H_
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "sql/parser/ob_item_type.h"
#include "sql/resolver/expr/ob_raw_expr.h"
namespace oceanbase {
namespace sql {
class ObSQLSessionInfo;
class ObRawExprCtxCatAnalyzer {
public:
  ObRawExprCtxCatAnalyzer(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info)
      : expr_factory_(expr_factory), session_info_(session_info)
  {}
  //  int splite_search_keywords(ObFunMatchAgainst &expr);
  int create_fulltext_filter(ObFunMatchAgainst& expr);

private:
  ObRawExprFactory& expr_factory_;
  ObSQLSessionInfo* session_info_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_EXPR_OB_RAW_EXPR_CTXCAT_ANALYZER_H_ */
