/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_FUNC_NORMAL_H_
#define OCEANBASE_SQL_OB_EXPR_FUNC_NORMAL_H_

#include <random>
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprNormal : public ObFuncExprOperator
{
	class ObExprNormalCtx: public ObExprOperatorCtx
	{
	public:
		ObExprNormalCtx() = default;
    ~ObExprNormalCtx() = default;
    int initialize(ObEvalCtx &ctx, const ObExpr &expr);
    int generate_next_value(int64_t sample, double &result);
  private:
    std::normal_distribution<double> normal_dist_;
    std::mt19937_64 gen_; // map continuous small number to large sparse space
  };
public:
	explicit ObExprNormal(common::ObIAllocator &alloc);
	virtual ~ObExprNormal();
  virtual int calc_result_type3(ObExprResType &result_type,
                                ObExprResType &mean,
                                ObExprResType &stddev,
                                ObExprResType &rand_expr,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual bool need_rt_ctx() const override { return true; }
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_next_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
	DISALLOW_COPY_AND_ASSIGN(ObExprNormal);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* OCEANBASE_SQL_OB_EXPR_FUNC_NORMAL_H_ */
