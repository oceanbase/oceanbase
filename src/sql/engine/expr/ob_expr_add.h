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

#ifndef _OB_EXPR_ADD_H_
#define _OB_EXPR_ADD_H_

#include <float.h>
#include <math.h>
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObRawExpr;
class ObExprCGCtx;

class ObExprAdd : public ObArithExprOperator {
public:
  ObExprAdd();
  explicit ObExprAdd(common::ObIAllocator& alloc, ObExprOperatorType type = T_OP_ADD);
  virtual ~ObExprAdd(){};
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result2(
      common::ObObj& result, const common::ObObj& left, const common::ObObj& right, common::ObExprCtx& expr_ctx) const;
  static int calc(common::ObObj& res, const common::ObObj& ojb1, const common::ObObj& obj2,
      common::ObIAllocator* allocator, common::ObScale scale);
  static int calc(common::ObObj& res, const common::ObObj& ojb1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx,
      common::ObScale scale);
  // add for aggregate function.
  static int calc_for_agg(common::ObObj& res, const common::ObObj& ojb1, const common::ObObj& obj2,
      common::ObExprCtx& expr_ctx, common::ObScale scale);

  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;

public:
  // very very effective implementation
  // if false is returned, the addition of multiplication will be stored in res
  template <typename T1, typename T2, typename T3>
  OB_INLINE static bool is_add_out_of_range(T1 val1, T2 val2, T3& res)
  {
    return __builtin_add_overflow(val1, val2, &res);
  }
  OB_INLINE static bool is_int_int_out_of_range(int64_t val1, int64_t val2, int64_t res)
  {
    // top digit:
    // 0 + 0 = 0 : safe.
    // 0 + 0 = 1 : overflow.
    // 0 + 1     : safe.
    // 1 + 0     : safe.
    // 1 + 1 = 0 : underflow.
    // 1 + 1 = 1 : safe.
    return (val1 >> SHIFT_OFFSET) != (res >> SHIFT_OFFSET) && (val2 >> SHIFT_OFFSET) != (res >> SHIFT_OFFSET);
  }
  OB_INLINE static bool is_int_uint_out_of_range(int64_t val1, uint64_t val2, uint64_t res)
  {
    // top digit:
    // 0 + 0     : safe.
    // 0 + 1 = 0 : overflow.
    // 0 + 1 = 1 : safe.
    // 1 + 0 = 0 : safe.
    // 1 + 0 = 1 : underflow.
    // 1 + 1     : safe.
    return (static_cast<uint64_t>(val1) >> SHIFT_OFFSET) == (res >> SHIFT_OFFSET) &&
           (val2 >> SHIFT_OFFSET) != (res >> SHIFT_OFFSET);
  }
  OB_INLINE static bool is_uint_uint_out_of_range(uint64_t val1, uint64_t val2, uint64_t res)
  {
    // top digit:
    // 0 + 0     : safe.
    // 0 + 1 = 0 : overflow.
    // 0 + 1 = 1 : safe.
    // 1 + 0 = 0 : overflow.
    // 1 + 0 = 1 : safe.
    // 1 + 1     : overflow.
    return (val1 >> SHIFT_OFFSET) + (val2 >> SHIFT_OFFSET) > (res >> SHIFT_OFFSET);
  }
  static int add_datetime(common::ObObj& res, const common::ObObj& left, const common::ObObj& right,
      common::ObIAllocator* allocator, common::ObScale scale);

private:
  static int add_int(common::ObObj& res, const common::ObObj& left, const common::ObObj& right,
      common::ObIAllocator* allocator, common::ObScale scale);
  static int add_uint(common::ObObj& res, const common::ObObj& left, const common::ObObj& right,
      common::ObIAllocator* allocator, common::ObScale scale);
  static int add_float(common::ObObj& res, const common::ObObj& left, const common::ObObj& right,
      common::ObIAllocator* allocator, common::ObScale scale);
  static int add_double(common::ObObj& res, const common::ObObj& left, const common::ObObj& right,
      common::ObIAllocator* allocator, common::ObScale scale);
  static int add_double_no_overflow(common::ObObj& res, const common::ObObj& left, const common::ObObj& right,
      common::ObIAllocator* allocator, common::ObScale scale);
  static int add_number(common::ObObj& res, const common::ObObj& left, const common::ObObj& right,
      common::ObIAllocator* allocator, common::ObScale scale);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprAdd);

public:
  static int add_null(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int add_int_int(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int add_int_uint(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int add_uint_uint(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int add_uint_int(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int add_float_float(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int add_double_double(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int add_number_number(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

  static int add_intervalym_intervalym(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int add_intervalym_datetime_common(
      const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool interval_left);
  OB_INLINE static int add_intervalym_datetime(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_intervalym_datetime_common(expr, ctx, expr_datum, true);
  }
  OB_INLINE static int add_datetime_intervalym(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_intervalym_datetime_common(expr, ctx, expr_datum, false);
  }

  static int add_intervalym_timestamptz_common(
      const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool interval_left);
  OB_INLINE static int add_intervalym_timestamptz(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_intervalym_timestamptz_common(expr, ctx, expr_datum, true);
  }
  OB_INLINE static int add_timestamptz_intervalym(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_intervalym_timestamptz_common(expr, ctx, expr_datum, false);
  }

  static int add_intervalym_timestampltz_common(
      const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool interval_left);
  OB_INLINE static int add_intervalym_timestampltz(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_intervalym_timestampltz_common(expr, ctx, expr_datum, true);
  }
  OB_INLINE static int add_timestampltz_intervalym(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_intervalym_timestampltz_common(expr, ctx, expr_datum, false);
  }

  static int add_intervalym_timestampnano_common(
      const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool interval_left);
  OB_INLINE static int add_intervalym_timestampnano(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_intervalym_timestampnano_common(expr, ctx, expr_datum, true);
  }
  OB_INLINE static int add_timestampnano_intervalym(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_intervalym_timestampnano_common(expr, ctx, expr_datum, false);
  }

  static int add_intervalds_intervalds(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int add_intervalds_datetime_common(
      const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool interval_left);
  OB_INLINE static int add_intervalds_datetime(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_intervalds_datetime_common(expr, ctx, expr_datum, true);
  }
  OB_INLINE static int add_datetime_intervalds(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_intervalds_datetime_common(expr, ctx, expr_datum, false);
  }

  static int add_intervalds_timestamptz_common(
      const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool interval_left);
  OB_INLINE static int add_intervalds_timestamptz(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_intervalds_timestamptz_common(expr, ctx, expr_datum, true);
  }
  OB_INLINE static int add_timestamptz_intervalds(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_intervalds_timestamptz_common(expr, ctx, expr_datum, false);
  }

  static int add_intervalds_timestamp_tiny_common(
      const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool interval_left);
  OB_INLINE static int add_intervalds_timestamp_tiny(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_intervalds_timestamp_tiny_common(expr, ctx, expr_datum, true);
  }
  OB_INLINE static int add_timestamp_tiny_intervalds(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_intervalds_timestamp_tiny_common(expr, ctx, expr_datum, false);
  }

  static int add_number_datetime_common(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool number_left);
  OB_INLINE static int add_number_datetime(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_number_datetime_common(expr, ctx, expr_datum, true);
  }
  OB_INLINE static int add_datetime_number(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
  {
    return add_number_datetime_common(expr, ctx, expr_datum, false);
  }
  static int add_datetime_datetime(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  static ObArithFunc add_funcs_[common::ObMaxTC];
  static ObArithFunc agg_add_funcs_[common::ObMaxTC];
  static const int64_t SHIFT_OFFSET = 63;
};

// Add expr for aggregation, different with ObExprAdd:
//  No overflow check for float/double type.
class ObExprAggAdd : public ObExprAdd {
public:
  explicit ObExprAggAdd(common::ObIAllocator& alloc) : ObExprAdd(alloc, T_OP_AGG_ADD)
  {}
};

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_EXPR_ADD_H_ */
