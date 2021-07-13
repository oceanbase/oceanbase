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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REGEXP_COUNT_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REGEXP_COUNT_

#include "lib/string/ob_string.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"

namespace oceanbase {
namespace sql {
class ObExprRegexpCount : public ObFuncExprOperator {
public:
  explicit ObExprRegexpCount(common::ObIAllocator& alloc);
  virtual ~ObExprRegexpCount();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs, int64_t param_num, common::ObExprCtx& expr_ctx) const;

  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;

  static int eval_regexp_count(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

  static int get_regexp_flags(
      const common::ObCollationType calc_cs_type, const common::ObString& match_param, int& flags, int& multi_flag);

private:
  static int calc(int64_t& ret_count, const common::ObString& text, const common::ObString& pattern, int64_t position,
      const common::ObCollationType calc_cs_type, const common::ObString& match_param, int64_t subexpr,
      bool has_null_argument, ObExprRegexContext* regexp_ptr, common::ObExprStringBuf& string_buf);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRegexpCount);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REGEXP_SUBSTR_ */
