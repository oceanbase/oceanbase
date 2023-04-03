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
