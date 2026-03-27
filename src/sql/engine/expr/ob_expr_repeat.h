/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REPEAT_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REPEAT_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRepeat : public ObStringExprOperator
{
public:
  explicit  ObExprRepeat(common::ObIAllocator &alloc);
  virtual ~ObExprRepeat();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &text,
                                ObExprResType &count,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &result,
                  const common::ObObj &text,
                  const common::ObObj &count,
                  common::ObIAllocator *allocator,
                  const common::ObObjType res_type,
                  const int64_t max_result_size);
  static int calc(common::ObObj &result,
                  const common::ObObjType type,
                  const common::ObString &text,
                  const int64_t count,
                  common::ObIAllocator *allocator,
                  const int64_t max_result_size);

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_repeat(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  static int eval_repeat_vector(VECTOR_EVAL_FUNC_ARG_DECL);

  static int repeat(common::ObString &output,
                    bool &is_null,
                    const common::ObString &input,
                    const int64_t count,
                    common::ObIAllocator &alloc,
                    const int64_t max_result_size);
  static int repeat_text(ObObjType res_type,
                         bool has_lob_header,
                         ObString &output,
                         bool &is_null,
                         const ObString &text,
                         const int64_t count,
                         ObIAllocator &allocator,
                         const int64_t max_result_size);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  template <typename Arg0Vec, typename Arg1Vec, typename ResVec>
  static int repeat_vector(VECTOR_EVAL_FUNC_ARG_DECL);

  static const int64_t MEM_WARN_THRESHOLD = 100 * 1024 * 1024; // 100M
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprRepeat);
};

}
}
#endif //OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REPEAT_
