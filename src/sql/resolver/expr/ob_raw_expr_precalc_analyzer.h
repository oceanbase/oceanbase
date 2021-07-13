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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_EXPR_OB_RAW_EXPR_PRECALC_ANALYZER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_EXPR_OB_RAW_EXPR_PRECALC_ANALYZER_H_
#include "sql/parser/ob_item_type.h"
#include "common/object/ob_obj_type.h"
namespace oceanbase {
namespace sql {
class ObRawExpr;
class ObDMLStmt;
class ObRawExprFactory;
class ObSQLSessionInfo;
class ObRawExprPrecalcAnalyzer {
public:
  ObRawExprPrecalcAnalyzer(ObRawExprFactory& expr_factory, ObSQLSessionInfo* my_session)
      : expr_factory_(expr_factory), my_session_(my_session)
  {}

  int analyze_all_expr(ObDMLStmt& stmt);
  int analyze_expr_tree(ObRawExpr* expr, ObDMLStmt& stmt, const bool filter_expr = false);

private:
  int pre_cast_const_expr(ObRawExpr* expr);
  int pre_cast_recursively(ObRawExpr* expr);
  int extract_calculable_expr(ObRawExpr*& expr, ObDMLStmt& stmt, const bool filter_expr = false);
  int extract_calculable_expr_recursively(ObRawExpr*& expr, ObDMLStmt& stmt, const bool filter_expr = false);
  bool can_be_pre_cast(ObItemType expr_type) const;
  bool calculation_need_cast(common::ObObjType param_type, common::ObObjType calc_type) const;
  bool is_filtered(ObRawExpr& expr);

private:
  const static ObItemType EXPR_FILTER_LIST[];
  ObRawExprFactory& expr_factory_;
  ObSQLSessionInfo* my_session_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_EXPR_OB_RAW_EXPR_PRECALC_ANALYZER_H_ */
