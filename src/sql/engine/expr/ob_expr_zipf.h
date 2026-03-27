/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_FUNC_ZIPF_H_
#define OCEANBASE_SQL_OB_EXPR_FUNC_ZIPF_H_

#include <random>
#include "sql/engine/expr/ob_expr_operator.h"
#include "src/sql/ob_sql_define.h"

namespace oceanbase
{
namespace sql
{
class ObExprZipf : public ObFuncExprOperator
{
	class ObExprZipfCtx: public ObExprOperatorCtx
	{
	public:
		ObExprZipfCtx() = default;
    ~ObExprZipfCtx() = default;
    int initialize(ObEvalCtx &ctx, const ObExpr &expr);
    int generate_next_value(int64_t seed, int64_t &result);
  private:
    sql::ObTMArray<double> probe_cp_; // cumulative probability array
    std::mt19937_64 gen_; // map continuous small number to large sparse space
  };
public:
	explicit ObExprZipf(common::ObIAllocator &alloc);
	virtual ~ObExprZipf();
  virtual int calc_result_type3(ObExprResType &result_type,
                                ObExprResType &exponent,
                                ObExprResType &size,
                                ObExprResType &rand_expr,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual bool need_rt_ctx() const override { return true; }
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_next_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
	DISALLOW_COPY_AND_ASSIGN(ObExprZipf);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* OCEANBASE_SQL_OB_EXPR_FUNC_ZIPF_H_ */
