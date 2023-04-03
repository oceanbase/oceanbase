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

#ifndef _OB_EXPR_MINUS_H_
#define _OB_EXPR_MINUS_H_
#include <math.h>
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprCGCtx;
class ObExprMinus: public ObArithExprOperator
{
public:
  ObExprMinus();
  explicit  ObExprMinus(common::ObIAllocator &alloc, ObExprOperatorType type = T_OP_MINUS);
  virtual ~ObExprMinus() {};
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  static int minus_datetime(common::ObObj &res,
                            const common::ObObj &left,
                            const common::ObObj &right,
                            common::ObIAllocator *allocator,
                            common::ObScale scale);
  static int calc(ObDatum &res, const ObDatum &left, const ObDatum &right,
                      const ObDatumMeta &left_meta, const ObDatumMeta &right_meta,
                      ObIAllocator *allocator, uint64_t tenant_id);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  OB_INLINE static int minus_int(common::ObObj &res,
                       const common::ObObj &left,
                       const common::ObObj &right,
                       common::ObIAllocator *allocator,
                       common::ObScale scale);
  OB_INLINE static int minus_uint(common::ObObj &res,
                        const common::ObObj &left,
                        const common::ObObj &right,
                        common::ObIAllocator *allocator,
                        common::ObScale scale);
  static int minus_float(common::ObObj &res,
                         const common::ObObj &left,
                         const common::ObObj &right,
                         common::ObIAllocator *allocator,
                         common::ObScale scale);
  static int minus_double(common::ObObj &res,
                          const common::ObObj &left,
                          const common::ObObj &right,
                          common::ObIAllocator *allocator,
                          common::ObScale scale);
  static int minus_double_no_overflow(common::ObObj &res,
                                      const common::ObObj &left,
                                      const common::ObObj &right,
                                      common::ObIAllocator *allocator,
                                      common::ObScale scale);
  static int minus_number(common::ObObj &res,
                          const common::ObObj &left,
                          const common::ObObj &right,
                          common::ObIAllocator *allocator,
                          common::ObScale scale);
  static int calc_datetime_minus(common::ObObj &res,
                                 const common::ObObj &left,
                                 const common::ObObj &right,
                                 common::ObExprCtx &expr_ctx,
                                 common::ObScale calc_scale);
  static int calc_timestamp_minus(common::ObObj &res,
                                  const common::ObObj &left,
                                  const common::ObObj &right,
                                  const common::ObTimeZoneInfo *tz_info);


  DISALLOW_COPY_AND_ASSIGN(ObExprMinus);

public:
  static int minus_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int minus_int_int(EVAL_FUNC_ARG_DECL);
  static int minus_int_int_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_int_uint(EVAL_FUNC_ARG_DECL);
  static int minus_int_uint_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_uint_uint(EVAL_FUNC_ARG_DECL);
  static int minus_uint_uint_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_uint_int(EVAL_FUNC_ARG_DECL);
  static int minus_uint_int_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_float_float(EVAL_FUNC_ARG_DECL);
  static int minus_float_float_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_double_double(EVAL_FUNC_ARG_DECL);
  static int minus_double_double_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_number_number(EVAL_FUNC_ARG_DECL);
  static int minus_number_number_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_intervalym_intervalym(EVAL_FUNC_ARG_DECL);
  static int minus_intervalym_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_intervalds_intervalds(EVAL_FUNC_ARG_DECL);
  static int minus_intervalds_intervalds_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_datetime_intervalym(EVAL_FUNC_ARG_DECL);
  static int minus_datetime_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_datetime_intervalds(EVAL_FUNC_ARG_DECL);
  static int minus_datetime_intervalds_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_timestamptz_intervalym(EVAL_FUNC_ARG_DECL);
  static int minus_timestamptz_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_timestampltz_intervalym(EVAL_FUNC_ARG_DECL);
  static int minus_timestampltz_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_timestampnano_intervalym(EVAL_FUNC_ARG_DECL);
  static int minus_timestampnano_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_timestamptz_intervalds(EVAL_FUNC_ARG_DECL);
  static int minus_timestamptz_intervalds_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_timestamp_tiny_intervalds(EVAL_FUNC_ARG_DECL);
  static int minus_timestamp_tiny_intervalds_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_timestamp_timestamp(EVAL_FUNC_ARG_DECL);
  static int minus_timestamp_timestamp_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_datetime_number(EVAL_FUNC_ARG_DECL);
  static int minus_datetime_number_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_datetime_datetime_oracle(EVAL_FUNC_ARG_DECL);
  static int minus_datetime_datetime_oracle_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int minus_datetime_datetime(EVAL_FUNC_ARG_DECL);
  static int minus_datetime_datetime_batch(BATCH_EVAL_FUNC_ARG_DECL);

public:
  OB_INLINE static bool is_int_int_out_of_range(int64_t val1, int64_t val2, int64_t res)
  {
    // top digit:
    // 0 - 0     : safe
    // 1 - 1     : safe
    // 0 - 1 = 1 : overflow
    // 0 - 1 = 0 : safe
    // 1 - 0 = 1 : safe
    // 1 - 0 = 0 : underflow
    return (val1 >> SHIFT_OFFSET) != (res >>SHIFT_OFFSET) &&
           (val2 >> SHIFT_OFFSET) == (res >> SHIFT_OFFSET);
  }
  OB_INLINE static bool is_int_uint_out_of_range(int64_t val1, uint64_t val2, uint64_t res)
  {
    // top digit:
    // 0 - 0 = 0 : safe
    // 0 - 0 = 1 : underflow
    // 0 - 1     : underflow
    // 1 - 0     : underflow
    // 1 - 1     : underflow
    return !(0 == (val1 >> SHIFT_OFFSET) &&
             0 == (val2 >> SHIFT_OFFSET) &&
             0 == (res >> SHIFT_OFFSET));
  }
  OB_INLINE static bool is_uint_int_out_of_range(uint64_t val1, int64_t val2, uint64_t res)
  {
    // top digit:
    // 0 - 1     : safe
    // 1 - 1 = 0 : overflow
    // 1 - 1 = 1 : safe
    // 0 - 0 = 0 : safe
    // 0 - 0 = 1 : underflow
    // 0 - 1     : safe
    return (val1 >> SHIFT_OFFSET) == (static_cast<uint64_t>(val2) >> SHIFT_OFFSET) &&
           (val1 >> SHIFT_OFFSET) != (res >> SHIFT_OFFSET);
  }
  OB_INLINE static bool is_uint_uint_out_of_range(uint64_t val1, uint64_t val2, uint64_t res)
  {
    UNUSED(res);
    return val1 < val2;
  }
private:
  static ObArithFunc minus_funcs_[common::ObMaxTC];
  static ObArithFunc agg_minus_funcs_[common::ObMaxTC];
  static const int64_t SHIFT_OFFSET = 63;
};

// Minus expr for aggregation, different with ObExprMinus:
//  No overflow check for double type.
class ObExprAggMinus : public ObExprMinus
{
public:
  explicit ObExprAggMinus(common::ObIAllocator &alloc)
      : ObExprMinus(alloc, T_OP_AGG_MINUS)
  {
  }
};

}
}
#endif  /* _OB_EXPR_MINUS_H_ */
