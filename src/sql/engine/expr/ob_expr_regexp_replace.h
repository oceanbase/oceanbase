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

namespace oceanbase
{
namespace sql
{
class ObExprRegexpReplace : public ObStringExprOperator
{
public:
  explicit ObExprRegexpReplace(common::ObIAllocator &alloc);
  virtual ~ObExprRegexpReplace();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
  virtual int is_valid_for_generated_column(const ObRawExpr*expr,
                                            const common::ObIArray<ObRawExpr *> &exprs,
                                            bool &is_valid) const override;
  static int eval_regexp_replace(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int eval_regexp_replace_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                        const EvalBound &bound);

private:
  template <typename TextVec, typename ResVec>
  static int vector_regexp_replace_convert(const ObExpr &expr, ObEvalCtx &ctx,
                                           const ObBitVector &skip, const EvalBound &bound,
                                           ObString res_replace, bool is_no_pattern_to_replace,
                                           ObCollationType res_coll_type,
                                           ObExprStrResAlloc &out_alloc, ObIAllocator &tmp_alloc,
                                           const int64_t idx);

  template <typename TextVec, typename ResVec>
  static int vector_regexp_replace(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                   const EvalBound &bound);
  DISALLOW_COPY_AND_ASSIGN(ObExprRegexpReplace);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REGEXP_SUBSTR_ */
