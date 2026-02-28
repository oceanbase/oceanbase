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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_DATE_TRUNC_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_DATE_TRUNC_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
namespace sql
{

// Time unit enum type for template parameters
enum class DateTruncTimeUnit : int8_t {
  MICROSECONDS = 0,
  MILLISECONDS,
  SECOND,
  MINUTE,
  HOUR,
  DAY,
  WEEK,
  MONTH,
  QUARTER,
  YEAR,
  DECADE,
  CENTURY,
  MILLENNIUM,
  INVALID = -1
};

class ObExprDateTrunc : public ObFuncExprOperator
{
public:
  explicit  ObExprDateTrunc(common::ObIAllocator &alloc);
  virtual ~ObExprDateTrunc();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_date_trunc(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  static int eval_date_trunc_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);

private :
  // Template version of process_time_param
  template <ObObjType PARAM_TYPE>
  static int process_time_param(const char *param_ptr,
                                ObSolidifiedVarsGetter &helper,
                                ObTime &ob_time);
  // Runtime dispatch function for eval_date_trunc
  static int process_time_param_dispatch(const ObObjType param_type,
                                         const char *param_ptr,
                                         ObSolidifiedVarsGetter &helper,
                                         ObTime &ob_time);
  // Convert ObString to DateTruncTimeUnit enum
  static DateTruncTimeUnit parse_time_unit(const ObString &time_unit);
  // Runtime dispatch function for eval_date_trunc
  static int do_date_trunc_dispatch(const ObString &time_unit, ObTime &ob_time);
  // Template version: accepts compile-time time_unit parameter
  template <DateTruncTimeUnit TIME_UNIT>
  static int do_date_trunc_template(ObTime &ob_time);
  // Template function that directly operates on ObMySQLDateTime to avoid ObTime conversion
  template <DateTruncTimeUnit TIME_UNIT>
  static int do_date_trunc_mysql_datetime_template(ObMySQLDateTime &mysql_datetime);
  // Template function that directly operates on ObMySQLDate to avoid ObTime conversion
  template <DateTruncTimeUnit TIME_UNIT>
  static int do_date_trunc_mysql_date_template(ObMySQLDate &mysql_date);
  static int set_time_res(const ObObjType res_type, ObTime &ob_time, ObDatum &expr_datum);

  // Template function for vectorized processing
  template <bool IS_TIME_UNIT_CONST, ObObjType DATETIME_TYPE>
  static int eval_date_trunc_vector_template(const ObExpr &expr, ObEvalCtx &ctx,
                                           const ObBitVector &skip, const EvalBound &bound);


  // Dispatch function: call corresponding template specialization based on time_unit string value
  template <ObObjType DATETIME_TYPE>
  static int eval_date_trunc_vector_const_unit_dispatch(const ObExpr &expr, ObEvalCtx &ctx,
                                                        const ObBitVector &skip, const EvalBound &bound,
                                                        const ObString &time_unit);

  // L1 dispatch function: dispatch based on DATETIME_TYPE, datetime_format, res_format
  template <ObObjType DATETIME_TYPE>
  static int eval_date_trunc_vector_const_unit_dispatch_l1(const ObExpr &expr, ObEvalCtx &ctx,
                                                           const ObBitVector &skip, const EvalBound &bound,
                                                           DateTruncTimeUnit unit);

  // Helper function: dispatch based on format, TIME_UNIT as template parameter
  template <ObObjType DATETIME_TYPE, DateTruncTimeUnit TIME_UNIT>
  static int eval_date_trunc_vector_const_unit_dispatch_l2(
      const ObExpr &expr, ObEvalCtx &ctx,
      const ObBitVector &skip, const EvalBound &bound,
      VectorFormat datetime_format, VectorFormat res_format,
      VecValueTypeClass datetime_vec_tc, VecValueTypeClass res_vec_tc);

  // Constant time_unit implementation with format template parameters (for same format cases)
  template <ObObjType DATETIME_TYPE, DateTruncTimeUnit TIME_UNIT, typename DATETIME_VECTOR, typename RES_VECTOR>
  static int eval_date_trunc_vector_const_unit_with_format(const ObExpr &expr, ObEvalCtx &ctx,
                                                           const ObBitVector &skip, const EvalBound &bound);

  // Template specialization for variable time_unit
  template <ObObjType DATETIME_TYPE>
  static int eval_date_trunc_vector_var_unit(const ObExpr &expr, ObEvalCtx &ctx,
                                           const ObBitVector &skip, const EvalBound &bound);

  // Check for ZERO_DATE in the bound and set corresponding res_vec to NULL
  template <ObObjType DATETIME_TYPE, typename ARG_VECTOR, typename RES_VECTOR>
  static int check_zero_date(const ObBitVector &skip,
                             ObBitVector &eval_flags,
                             const EvalBound &bound,
                             ARG_VECTOR *arg_vec,
                             RES_VECTOR *res_vec);

  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprDateTrunc);
};

}
}


#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_DATE_TRUNC_H_ */
