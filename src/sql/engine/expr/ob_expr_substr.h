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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_SUBSTR_
#define OCEANBASE_SQL_ENGINE_EXPR_SUBSTR_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprSubstr : public ObStringExprOperator
{
public:
  explicit  ObExprSubstr(common::ObIAllocator &alloc);
  virtual ~ObExprSubstr();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  static int substr(common::ObString &output,
                    const common::ObString &input,
                    const int64_t pos,
                    const int64_t len,
                    common::ObCollationType cs_type,
                    const bool do_ascii_optimize_check);

  static int calc(common::ObObj &result,
                  const common::ObString &text,
                  const int64_t start_pos,
                  const int64_t length,
                  common::ObCollationType cs_type,
                  const bool is_clob);

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_substr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int eval_substr_batch(
             const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);

private:
  int calc_result_length(ObExprResType *types_array,
                         int64_t param_num,
                         common::ObCollationType cs_type,
                         int64_t &res_len) const;
  int calc_result_length_oracle(const ObExprResType *types_array,
                                int64_t param_num,
                                const ObExprResType &result_type,
                                int64_t &res_len) const;
  int calc_result2_for_mysql(common::ObObj &result,
                             const common::ObObj &text,
                             const common::ObObj &start_pos,
                             common::ObExprCtx &expr_ctx) const;
  int calc_result2_for_oracle(common::ObObj &result,
                              const common::ObObj &text,
                              const common::ObObj &start_pos,
                              common::ObExprCtx &expr_ctx) const;
  int calc_result3_for_mysql(common::ObObj &result,
                             const common::ObObj &text,
                             const common::ObObj &start_pos,
                             const common::ObObj &length,
                             common::ObExprCtx &expr_ctx) const;
  int calc_result3_for_oracle(common::ObObj &result,
                              const common::ObObj &text,
                              const common::ObObj &start_pos,
                              const common::ObObj &length,
                              common::ObExprCtx &expr_ctx) const;
  int cast_param_type_for_mysql(const common::ObObj& in,
                                common::ObExprCtx& expr_ctx,
                                common::ObObj& out) const;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprSubstr);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_SUBSTR_ */
