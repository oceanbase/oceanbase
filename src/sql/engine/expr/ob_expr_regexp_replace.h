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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REGEXP_REPLACE_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REGEXP_REPLACE_

#include "lib/string/ob_string.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObExprRegexpReplace : public ObStringExprOperator {
public:
  explicit ObExprRegexpReplace(common::ObIAllocator& alloc);
  virtual ~ObExprRegexpReplace();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs, int64_t param_num, common::ObExprCtx& expr_ctx) const;

  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;

  static int eval_regexp_replace(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  static int calc(common::ObString& ret_str, const common::ObString& text, const common::ObString& pattern,
      const common::ObString& replacement_string, int64_t position, int64_t occurrence,
      const common::ObCollationType cs_type, const common::ObString& match_param, int null_argument_idx,
      ObExprRegexContext* regexp_ptr, common::ObExprStringBuf& string_buf);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRegexpReplace);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REGEXP_SUBSTR_ */
