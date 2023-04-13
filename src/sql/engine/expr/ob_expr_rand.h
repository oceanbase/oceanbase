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

#ifndef OCEANBASE_SQL_OB_EXPR_FUNC_RAND_H_
#define OCEANBASE_SQL_OB_EXPR_FUNC_RAND_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRand: public ObFuncExprOperator
{
  OB_UNIS_VERSION(1);
	class ObExprRandCtx: public ObExprOperatorCtx
	{
	public:
		ObExprRandCtx();
		virtual ~ObExprRandCtx();
		void set_seed(uint32_t seed);
		void get_next_random(double &res);
	private:
		static const uint64_t max_value_;
		uint64_t seed1_;
		uint64_t seed2_;
	};
public:
	explicit ObExprRand(common::ObIAllocator &alloc);
	virtual ~ObExprRand();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
	inline void set_seed_const(bool is_seed_const);

  // engine 3.0
  virtual bool need_rt_ctx() const override { return true; }
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int calc_random_expr_const_seed(const ObExpr &expr, ObEvalCtx &ctx,
                                         ObDatum &res_datum);
  static int calc_random_expr_nonconst_seed(const ObExpr &expr, ObEvalCtx &ctx,
                                            ObDatum &res_datum);
public:
  virtual int assign(const ObExprOperator &other) override;
private:
	bool is_seed_const_;
	// disallow copy
	DISALLOW_COPY_AND_ASSIGN(ObExprRand);
};

inline void ObExprRand::set_seed_const(bool is_seed_const)
{
	is_seed_const_ = is_seed_const;
}
} /* namespace sql */
} /* namespace oceanbase */

#endif /* OCEANBASE_SQL_OB_EXPR_FUNC_RANDOM_H_ */
