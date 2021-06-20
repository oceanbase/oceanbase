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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TIMEZONE_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TIMEZONE_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObExprBaseTimezone : public ObFuncExprOperator {
public:
  explicit ObExprBaseTimezone(
      common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, const bool is_sessiontimezone);
  virtual ~ObExprBaseTimezone()
  {}
  virtual int calc_result_type0(ObExprResType& type, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result0(common::ObObj& result, common::ObExprCtx& expr_ctx) const = 0;

private:
  bool is_sessiontimezone_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprBaseTimezone);
};

class ObExprSessiontimezone : public ObExprBaseTimezone {
public:
  explicit ObExprSessiontimezone(common::ObIAllocator& alloc);
  virtual ~ObExprSessiontimezone()
  {}
  virtual int calc_result0(common::ObObj& result, common::ObExprCtx& expr_ctx) const;
  static int eval_session_timezone(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSessiontimezone);
};

class ObExprDbtimezone : public ObExprBaseTimezone {
public:
  explicit ObExprDbtimezone(common::ObIAllocator& alloc);
  virtual ~ObExprDbtimezone()
  {}
  virtual int calc_result0(common::ObObj& result, common::ObExprCtx& expr_ctx) const;
  static int eval_db_timezone(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDbtimezone);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TIMEZONE_ */
