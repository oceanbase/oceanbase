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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_ESTIMATE_NDV_H_
#define OCEANBASE_SQL_ENGINE_EXPR_ESTIMATE_NDV_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObExprEstimateNdv : public ObFuncExprOperator {
public:
  explicit ObExprEstimateNdv(common::ObIAllocator &alloc);
  virtual ~ObExprEstimateNdv();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  static void llc_estimate_ndv(int64_t &result, const common::ObString &bitmap_str);
  static int llc_estimate_ndv(double &estimate_ndv, const common::ObString &bitmap_buf);
  // 计算value的leading zeros。在HyperLogLogCount中，一个hash值的前面若干位要用来做分桶，
  // 这里的传入参数value是通过左移移除掉分桶部分后的部分，它的实际有效位数是高bit_width位。
  static uint64_t llc_leading_zeros(uint64_t value, uint64_t bit_width);
  static bool llc_is_num_buckets_valid(int64_t num_buckets);
  // for engine 3.0
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int calc_estimate_ndv_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                    ObDatum &res_datum);
private:
  // 计算HyperLogLogCount中的 alpha * m^2 的函数。计算涉及变量m(uint64_t)
  // 转double的步骤，调用者需要考虑可能的精度损失（目前m通常不超过4096，无损失）。
  static inline double llc_alpha_times_m_square(const uint64_t m);
  // 根据Google的HLLC论文桶数至少取2^4(16)个，至多取2^16(65536)个。
  static const int LLC_NUM_BUCKETS_MIN = (1 << 4);
  static const int LLC_NUM_BUCKETS_MAX = (1 << 16);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprEstimateNdv);
};
} /* namespace sql */
} /* namespace oceanbase */



#endif /* OCEANBASE_SQL_ENGINE_EXPR_ESTIMATE_NDV_H_ */
