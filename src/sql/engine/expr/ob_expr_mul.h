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

#ifndef _OB_EXPR_MUL_H_
#define _OB_EXPR_MUL_H_

#include <cmath>
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
using namespace common::number;
class ObExprMul: public ObArithExprOperator
{
public:
  ObExprMul();
  explicit  ObExprMul(common::ObIAllocator &alloc, ObExprOperatorType type = T_OP_MUL);
  virtual ~ObExprMul() {};
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &res,
                  const common::ObObj &ojb1,
                  const common::ObObj &obj2,
                  common::ObIAllocator *allocator,
                  common::ObScale scale);
  static int calc(common::ObObj &res,
                  const common::ObObj &ojb1,
                  const common::ObObj &obj2,
                  common::ObExprCtx &expr_ctx,
                  common::ObScale scale);

  static int mul_int_int(EVAL_FUNC_ARG_DECL);
  static int mul_int_int_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int mul_int_uint(EVAL_FUNC_ARG_DECL);
  static int mul_int_uint_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int mul_uint_int(EVAL_FUNC_ARG_DECL);
  static int mul_uint_int_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int mul_uint_uint(EVAL_FUNC_ARG_DECL);
  static int mul_uint_uint_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int mul_float(EVAL_FUNC_ARG_DECL);
  static int mul_float_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int mul_double(EVAL_FUNC_ARG_DECL);
  static int mul_double_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int mul_number(EVAL_FUNC_ARG_DECL);
  static int mul_number_batch(BATCH_EVAL_FUNC_ARG_DECL);

  static int mul_intervalym_number_common(EVAL_FUNC_ARG_DECL, const bool number_left);
  static int mul_intervalds_number_common(EVAL_FUNC_ARG_DECL, const bool number_left);
  OB_INLINE static int mul_intervalym_number(EVAL_FUNC_ARG_DECL)
  {
    return mul_intervalym_number_common(EVAL_FUNC_ARG_LIST, false);
  }
  static int mul_intervalym_number_batch(BATCH_EVAL_FUNC_ARG_DECL);

  OB_INLINE static int mul_intervalds_number(EVAL_FUNC_ARG_DECL)
  {
    return mul_intervalds_number_common(EVAL_FUNC_ARG_LIST, false);
  }
  static int mul_intervalds_number_batch(BATCH_EVAL_FUNC_ARG_DECL);

  OB_INLINE static int mul_number_intervalym(EVAL_FUNC_ARG_DECL)
  {
    return mul_intervalym_number_common(EVAL_FUNC_ARG_LIST, true);
  }
  static int mul_number_intervalym_batch(BATCH_EVAL_FUNC_ARG_DECL);

  OB_INLINE static int mul_number_intervalds(EVAL_FUNC_ARG_DECL)
  {
    return mul_intervalds_number_common(EVAL_FUNC_ARG_LIST, true);
  }
  static int mul_number_intervalds_batch(BATCH_EVAL_FUNC_ARG_DECL);

  // temporary used, remove after all expr converted
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int mul_int(common::ObObj &res,
                     const common::ObObj &left,
                     const common::ObObj &right,
                     common::ObIAllocator *allocator,
                     common::ObScale scale);
  static int mul_uint(common::ObObj &res,
                      const common::ObObj &left,
                      const common::ObObj &right,
                      common::ObIAllocator *allocator,
                      common::ObScale scale);
  static int mul_double(common::ObObj &res,
                        const common::ObObj &left,
                        const common::ObObj &right,
                        common::ObIAllocator *allocator,
                        common::ObScale scale);
  static int mul_double_no_overflow(common::ObObj &res,
                                    const common::ObObj &left,
                                    const common::ObObj &right,
                                    common::ObIAllocator *allocator,
                                    common::ObScale scale);
  static int mul_float(common::ObObj &res,
                       const common::ObObj &left,
                       const common::ObObj &right,
                       common::ObIAllocator *allocator,
                       common::ObScale scale);
  static int mul_number(common::ObObj &res,
                        const common::ObObj &left,
                        const common::ObObj &right,
                        common::ObIAllocator *allocator,
                        common::ObScale scale);
  static int mul_interval(common::ObObj &res,
                          const common::ObObj &left,
                          const common::ObObj &right,
                          common::ObIAllocator *allocator,
                          common::ObScale scale);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMul);
public:
  //very very effective implementation
  //if false is returned, the result of multiplication will be stored in res
  template<typename T1, typename T2, typename T3>
  OB_INLINE static bool is_mul_out_of_range(T1 val1, T2 val2, T3 &res)
  {
    return __builtin_mul_overflow(val1, val2, &res);
  }
  OB_INLINE static bool is_int_uint_mul_out_of_range(int64_t val1, uint64_t val2)
  {
    bool overflow = false;
    if ((val1 < 0) && (0 != val2)) {
      overflow = true;
    } else {
      overflow = is_uint_uint_mul_out_of_range(static_cast<uint64_t>(val1), val2);
    }
    return overflow;
  }
  OB_INLINE static bool is_uint_uint_mul_out_of_range(uint64_t val1, uint64_t val2)
  {
    int overflow = false;
    unsigned long long res;
    overflow = __builtin_umulll_overflow(val1, val2, &res);
    return overflow;
  }
private:
  static ObArithFunc mul_funcs_[common::ObMaxTC];
  static ObArithFunc agg_mul_funcs_[common::ObMaxTC];
  static const int64_t SHIFT_OFFSET = 32;
};


// Mul expr for aggregation, different with ObExprMul:
//  No overflow check for double type.
class ObExprAggMul: public ObExprMul
{
public:
  explicit ObExprAggMul(common::ObIAllocator &alloc)
      : ObExprMul(alloc, T_OP_AGG_MUL)
  {
  }
};

}
}

#endif  /* _OB_NAME_NUL_H_*/
