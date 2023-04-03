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

#ifndef OCEANBASE_SQL_ENGINE_OB_SQL_EXPR_FUNC_ROUND_
#define OCEANBASE_SQL_ENGINE_OB_SQL_EXPR_FUNC_ROUND_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
#define ROUND_MIN_SCALE -30
#define ROUND_MAX_SCALE 30
class ObExprFuncRound : public ObFuncExprOperator
{
public:
  explicit  ObExprFuncRound(common::ObIAllocator &alloc);
  virtual ~ObExprFuncRound();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }

  static int calc_round_expr_numeric1_batch(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const int64_t batch_size);

  static int calc_round_expr_numeric2_batch(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const int64_t batch_size);

  static int calc_round_expr_datetime1_batch(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const int64_t batch_size);

  static int calc_round_expr_datetime2_batch(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const int64_t batch_size);
private:
  // engine 3.0
  int se_deduce_type(ObExprResType &type,
                     ObExprResType *params,
                     int64_t param_num,
                     common::ObExprTypeCtx &type_ctx) const;
  static int set_res_scale_prec(common::ObExprTypeCtx &type_ctx, ObExprResType *params,
                                int64_t param_num, const common::ObObjType &res_type,
                                ObExprResType &type);
  static int set_res_and_calc_type(ObExprResType *params, int64_t param_num,
                                   common::ObObjType &res_type);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprFuncRound);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_OB_SQL_EXPR_FUNC_ROUND_
