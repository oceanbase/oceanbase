/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_TYPE_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_TYPE_H_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonType : public ObFuncExprOperator
{
public:
  explicit ObExprJsonType(common::ObIAllocator &alloc);
  virtual ~ObExprJsonType();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  static uint32_t opaque_index(ObObjType field_type);
  static int eval_json_type(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc(ObEvalCtx &ctx, const ObDatum &data, ObDatumMeta meta, bool has_lob_header,
                  ObIAllocator *allocator, uint32_t &type_idx, bool &is_null);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonType);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_TYPE_H_