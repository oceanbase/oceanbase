/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ORACLE_NULLIF_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ORACLE_NULLIF_

#include "sql/engine/expr/ob_expr_nullif.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {
class ObExprOracleNullif : public ObExprNullif
{
 public:
  explicit ObExprOracleNullif(common::ObIAllocator &alloc);
  virtual ~ObExprOracleNullif(){};

  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_nullif(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_nullif_not_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
 private:
  DISALLOW_COPY_AND_ASSIGN(ObExprOracleNullif);

  bool is_same_type(const ObExprResType &type1, const ObExprResType &type2) const;

  bool is_numberic_type(const ObObjOType &otype1, const ObObjOType &otype2) const;

  // type1与type2都是oracle的数值类型（smallint，int，float，number，binary_float，binary_double)
  void calc_numberic_type(ObExprResType &type,
                          ObExprResType &type1,
                          ObExprResType &type2) const;

  bool is_string_type(const ObObjOType &otype1, const ObObjOType &otype2) const;

  bool is_time_type(const ObObjOType &otype1, const ObObjOType &otype2) const;
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ORACLE_NULLIF_
