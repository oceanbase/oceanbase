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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_EDIT_DISTANCE_
#define OCEANBASE_SQL_ENGINE_EXPR_EDIT_DISTANCE_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprEditDistance : public ObFuncExprOperator {
public:
  explicit ObExprEditDistance(common::ObIAllocator &alloc);
  virtual ~ObExprEditDistance();

  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

  static int calc_edit_distance(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_edit_distance_vector(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       const ObBitVector &skip,
                                       const EvalBound &bound);

  static int compute_levenshtein(const char * __restrict s1,
                                 int64_t len1,
                                 const char * __restrict s2,
                                 int64_t len2,
                                 ObIArray<uint32_t> &dists,
                                 uint64_t &distance);

private:
  template <typename ArgVec1, typename ArgVec2, typename ResVec>
  static int calc_edit_distance_vector_dispatch(const ObExpr &expr,
                                                ObEvalCtx &ctx,
                                                const ObBitVector &skip,
                                                const EvalBound &bound);

  DISALLOW_COPY_AND_ASSIGN(ObExprEditDistance);
};

class ObExprEditDistanceUTF8 : public ObFuncExprOperator {
public:
  struct WCharCollector {  // Helper struct to collect wide characters
    common::ObIArray<ob_wc_t> &wchars;

    WCharCollector(common::ObIArray<ob_wc_t> &w) : wchars(w) {}

    int operator()(const common::ObString &str, ob_wc_t wc);
  };

  explicit ObExprEditDistanceUTF8(common::ObIAllocator &alloc);
  virtual ~ObExprEditDistanceUTF8();

  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

  static int calc_edit_distance_utf8(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_edit_distance_utf8_vector(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            const ObBitVector &skip,
                                            const EvalBound &bound);

  static int compute_levenshtein_utf8(const char * __restrict s1,
                                      int64_t byte_len1,
                                      ObCollationType cs_type_str1,
                                      const char * __restrict s2,
                                      int64_t byte_len2,
                                      ObCollationType cs_type_str2,
                                      ObIArray<uint32_t> &dists,
                                      uint64_t &distance);

private:
  template <typename ArgVec1, typename ArgVec2, typename ResVec>
  static int calc_edit_distance_utf8_vector_dispatch(const ObExpr &expr,
                                                     ObEvalCtx &ctx,
                                                     const ObBitVector &skip,
                                                     const EvalBound &bound);

  DISALLOW_COPY_AND_ASSIGN(ObExprEditDistanceUTF8);
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_EXPR_EDIT_DISTANCE_
