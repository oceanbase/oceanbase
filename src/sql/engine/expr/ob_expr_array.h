/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY
#define OCEANBASE_SQL_OB_EXPR_ARRAY

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/udt/ob_array_utils.h"


namespace oceanbase
{
namespace sql
{
class ObExprArray : public ObFuncExprOperator
{
public:
  explicit ObExprArray(common::ObIAllocator &alloc);
  explicit ObExprArray(common::ObIAllocator &alloc, ObExprOperatorType type,
                                const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprArray();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_array(const ObExpr &expr,
                        ObEvalCtx &ctx,
                        ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:

  static int add_elem_to_nested_array(ObIAllocator &tmp_allocator, ObEvalCtx &ctx, uint16_t subschema_id,
                                      ObString &raw_bin, ObArrayNested *nest_array);
  DISALLOW_COPY_AND_ASSIGN(ObExprArray);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY