/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_OB_EXPR_FROM_BASE64_
#define OCEANBASE_SQL_ENGINE_OB_EXPR_FROM_BASE64_

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"

namespace oceanbase
{
namespace sql
{
class ObExprFromBase64 : public ObStringExprOperator {
public:
  explicit ObExprFromBase64(common::ObIAllocator &alloc);
  virtual ~ObExprFromBase64();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &str,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const;
  static int eval_from_base64(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_from_base64_batch(const ObExpr &expr, ObEvalCtx &ctx,
                              const ObBitVector &skip,
                              const int64_t batch_size);
  static int eval_from_base64_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  template<typename ArgVec, typename ResVec, bool isText>
  static int vector_from_base64(VECTOR_EVAL_FUNC_ARG_DECL);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprFromBase64);

  static int calc(common::ObObj &result,
                  const common::ObObj &obj1,
                  common::ObIAllocator *allocator);
  static const int64_t NCHAR_PER_BASE64 = 4;
  static const int64_t NCHAR_PER_BASE64_GROUP = 3;
  static inline ObLength base64_needed_decoded_length(ObLength length_of_encoded_data)
  {
    return (ObLength) ((length_of_encoded_data * NCHAR_PER_BASE64_GROUP + NCHAR_PER_BASE64 - 1 ) / NCHAR_PER_BASE64);
  }
};
}
}

#endif //OCEANBASE_SQL_ENGINE_OB_EXPR_FROM_BASE64_
