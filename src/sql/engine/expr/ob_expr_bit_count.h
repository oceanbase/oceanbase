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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_BIT_COUNT_H_
#define OCEANBASE_SQL_ENGINE_EXPR_BIT_COUNT_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprBitCount: public ObBitwiseExprOperator {
public:
	explicit ObExprBitCount(common::ObIAllocator &alloc);
	virtual ~ObExprBitCount();
  // for static typing engine
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const override;
  static int calc_bitcount_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                ObDatum& res_datum);

  static int calc_bitcount_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL);

  template <typename ArgVec, typename ResVec>
  static int vector_bitcount(VECTOR_EVAL_FUNC_ARG_DECL);

  template <typename ArgVec, typename ResVec, bool isFixed>
  static int vector_bitcount_int_specific(VECTOR_EVAL_FUNC_ARG_DECL);



  DECLARE_SET_LOCAL_SESSION_VARS;
private:
	static const uint8_t char_to_num_bits[256];


  OB_INLINE static uint64_t calc_table_look_up(uint64_t &uint_val) {
   return static_cast<uint64_t>(
                  char_to_num_bits[static_cast<uint8_t>(uint_val)]
                + char_to_num_bits[static_cast<uint8_t>(uint_val >> 8)]
                + char_to_num_bits[static_cast<uint8_t>(uint_val >> 16)]
                + char_to_num_bits[static_cast<uint8_t>(uint_val >> 24)]
                + char_to_num_bits[static_cast<uint8_t>(uint_val >> 32)]
                + char_to_num_bits[static_cast<uint8_t>(uint_val >> 40)]
                + char_to_num_bits[static_cast<uint8_t>(uint_val >> 48)]
                + char_to_num_bits[static_cast<uint8_t>(uint_val >> 56)]);

                // __m128i sum = _mm_sad_epu8(result, _mm_setzero_si128());
  }

	DISALLOW_COPY_AND_ASSIGN(ObExprBitCount);
};
} /* namespace sql */
} /* namespace oceanbase */

#endif /* OCEANBASE_SQL_ENGINE_EXPR_BIT_COUNT_H_ */
