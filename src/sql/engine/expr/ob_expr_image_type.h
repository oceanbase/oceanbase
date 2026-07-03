/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_OB_EXPR_IMAGE_TYPE_H_
#define _OCEANBASE_SQL_OB_EXPR_IMAGE_TYPE_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

// image_type(expr): detect image type from binary data (varchar/varbinary/blob/text), return type name
// Supported formats: JPEG, PNG, GIF, WEBP, BMP, TIFF, ICO, etc.
class ObExprImageType : public ObStringExprOperator
{
public:
  explicit ObExprImageType(common::ObIAllocator &alloc);
  virtual ~ObExprImageType();

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &img,
                                common::ObExprTypeCtx &type_ctx) const override;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

  static int eval_image_type(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprImageType);
};

} // end namespace sql
} // end namespace oceanbase

#endif // _OCEANBASE_SQL_OB_EXPR_IMAGE_TYPE_H_
