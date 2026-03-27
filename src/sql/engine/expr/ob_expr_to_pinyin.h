/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TO_PINYIN_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TO_PINYIN_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_to_pinyin_tab.h"

namespace oceanbase
{
namespace sql
{
class ObExprToPinyin : public ObFuncExprOperator
{
public:
  explicit ObExprToPinyin(common::ObIAllocator &alloc);
  virtual ~ObExprToPinyin();

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  static int eval_to_pinyin(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            ObDatum &expr_datum);
  static int eval_to_pinyin_batch(
      const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  static int eval_to_pinyin_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  static int calc_convert_mode(const ObString &convert_option,
                               ModeOption &convert_mode);

  static int calc_result_length(const ObExprResType &type,
                                int64_t &res_len);
  template <typename ArgVec, typename ResVec>
  static int to_pinyin_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  DISALLOW_COPY_AND_ASSIGN(ObExprToPinyin);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TO_PINYIN_ */
