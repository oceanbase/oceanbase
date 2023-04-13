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

#ifndef OCEANBASE_SQL_OB_EXPR_FUNC_UNIFORM_H_
#define OCEANBASE_SQL_OB_EXPR_FUNC_UNIFORM_H_

#include <random>
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprUniform : public ObFuncExprOperator
{
	class ObExprUniformIntCtx: public ObExprOperatorCtx
	{
	public:
		ObExprUniformIntCtx() = default;
    ~ObExprUniformIntCtx() = default;
    int initialize(ObEvalCtx &ctx, const ObExpr &expr);
    int generate_next_value(int64_t sample, int64_t &res);
  private:
    std::uniform_int_distribution<int64_t> int_dist_;
    std::mt19937_64 gen_; // map continuous small number to large sparse space
  };
	class ObExprUniformRealCtx: public ObExprOperatorCtx
	{
	public:
		ObExprUniformRealCtx() = default;
    ~ObExprUniformRealCtx() = default;
    int initialize(ObEvalCtx &ctx, const ObExpr &expr);
    int generate_next_value(int64_t sample, double &res);
  private:
    std::uniform_real_distribution<double> real_dist_;
    std::mt19937_64 gen_; // map continuous small number to large sparse space
  };
public:
	explicit ObExprUniform(common::ObIAllocator &alloc);
	virtual ~ObExprUniform();
  virtual int calc_result_type3(ObExprResType &result_type,
                                ObExprResType &exponent,
                                ObExprResType &size,
                                ObExprResType &rand_expr,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual bool need_rt_ctx() const override { return true; }
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_next_int_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  static int eval_next_real_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  static int eval_next_number_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
	DISALLOW_COPY_AND_ASSIGN(ObExprUniform);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* OCEANBASE_SQL_OB_EXPR_FUNC_UNIFORM_H_ */
