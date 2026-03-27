/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CRC32_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CRC32_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObExprCrc32 : public ObFuncExprOperator {
public:
  explicit ObExprCrc32(common::ObIAllocator& alloc);
  virtual ~ObExprCrc32(){};

  virtual int calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const;

  virtual int cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const;
  static int calc_crc32_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);
  static int calc_crc32_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL);

  template <typename ArgVec, typename ResVec>
  static int vector_crc32(VECTOR_EVAL_FUNC_ARG_DECL);

  static const uint32_t Crc32Lookup[16][256];

  static uint32_t crc32_16bytes(uint32_t previousCrc32, unsigned char *data, size_t length);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCrc32);
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CRC32_
