/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_NULLIF_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_NULLIF_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprNullif : public ObFuncExprOperator
{
public:
  explicit  ObExprNullif(common::ObIAllocator &alloc);
  virtual ~ObExprNullif() {};

  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int cast_param(const ObExpr &src_expr, ObEvalCtx &ctx,
                        const ObDatumMeta &dst_meta,
                        const ObCastMode &cm, ObIAllocator &allocator,
                        ObDatum &res_datum);
  static int cast_result(const ObExpr &src_expr, const ObExpr &dst_expr, ObEvalCtx &ctx,
                         const ObCastMode &cm, ObDatum &expr_datum);
  static int eval_nullif(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_nullif_enumset(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  int set_extra_info(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                     ObSQLMode sql_mode, ObExpr &rt_expr) const;
  void set_first_param_flag(bool flag) { first_param_can_be_null_ = flag; }
  DECLARE_SET_LOCAL_SESSION_VARS;
protected:
  bool first_param_can_be_null_;
private:
  int deduce_type(ObExprResType &type,
                  ObExprResType &type1,
                  ObExprResType &type2,
                  common::ObExprTypeCtx &type_ctx) const;
  int se_deduce_type(ObExprResType &type,
                     ObExprResType &cmp_type,
                     ObExprResType &type1,
                     ObExprResType &type2,
                     common::ObExprTypeCtx &type_ctx) const;
  DISALLOW_COPY_AND_ASSIGN(ObExprNullif);
};
} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_NULLIF_

