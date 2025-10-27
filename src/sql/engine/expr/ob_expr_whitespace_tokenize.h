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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_WHITESPACE_TOKENIZE_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_WHITESPACE_TOKENIZE_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {

class ObExprWhitespaceTokenize: public ObStringExprOperator
{
public:
  explicit ObExprWhitespaceTokenize(common::ObIAllocator& alloc);
  virtual ~ObExprWhitespaceTokenize() = default;

  virtual int calc_result_type1(ObExprResType& type, ObExprResType &text, common::ObExprTypeCtx& type_ctx) const override;
  static int eval_tokenize(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
private:
  static int tokenize(ObString &output, const ObString &text, common::ObIAllocator &allocator);
  DISABLE_COPY_ASSIGN(ObExprWhitespaceTokenize);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_WHITESPACE_TOKENIZE_ */