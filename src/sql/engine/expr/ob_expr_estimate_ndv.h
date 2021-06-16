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
  explicit ObExprEstimateNdv(common::ObIAllocator& alloc);
  virtual ~ObExprEstimateNdv();
  virtual int calc_result_type1(
      ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const override;
  virtual int calc_result1(common::ObObj& result, const common::ObObj& obj, common::ObExprCtx& expr_ctx) const override;
  static int llc_estimate_ndv(common::ObObj& result, const common::ObObj& obj, common::ObExprCtx& expr_ctx);
  static void llc_estimate_ndv(int64_t& result, const common::ObString& bitmap_str);
  static int llc_estimate_ndv(double& estimate_ndv, const common::ObString& bitmap_buf);
  // high several bits of hash value are used to store bucket_id, the param value must
  // remove these bits by left shift, the count of valid bits after removing is bit_width.
  static uint64_t llc_leading_zeros(uint64_t value, uint64_t bit_width);
  static bool llc_is_num_buckets_valid(int64_t num_buckets);
  // for engine 3.0
  virtual int cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_estimate_ndv_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);

private:
  static inline double llc_alpha_times_m_square(const uint64_t m);
  // the count of buckets should be between 16 and 65536, according to Google's HLLC paper.
  static const int LLC_NUM_BUCKETS_MIN = (1 << 4);
  static const int LLC_NUM_BUCKETS_MAX = (1 << 16);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprEstimateNdv);
};
} /* namespace sql */
} /* namespace oceanbase */

#endif /* OCEANBASE_SQL_ENGINE_EXPR_ESTIMATE_NDV_H_ */
