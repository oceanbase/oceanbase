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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REGEXP_LIKE_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REGEXP_LIKE_

#include "lib/string/ob_string.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"

namespace oceanbase {
namespace sql {
class ObExprRegexpLike : public ObFuncExprOperator {
public:
  explicit ObExprRegexpLike(common::ObIAllocator& alloc);
  virtual ~ObExprRegexpLike();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs, int64_t param_num, common::ObExprCtx& expr_ctx) const;

  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;

  static int eval_regexp_like(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  static int regexp_like(bool& match, const common::ObString& text, const common::ObString& pattern, int64_t position,
      int64_t occurrence, const common::ObCollationType calc_cs_type, const common::ObString& match_param,
      bool has_null_argument, ObExprRegexContext* regexp_ptr, common::ObExprStringBuf& string_buf);

  int calc(common::ObObj& result, const common::ObString& text, const common::ObString& pattern, int64_t position,
      int64_t occurrence, const common::ObString& match_param, bool has_null_argument, ObExprRegexContext* regexp_ptr,
      common::ObExprStringBuf& string_buf) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRegexpLike);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REGEXP_SUBSTR_ */
