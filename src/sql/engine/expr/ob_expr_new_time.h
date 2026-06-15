/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_NEW_TIME_H_
#define OCEANBASE_SQL_OB_EXPR_NEW_TIME_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
/**
 * @brief Oracle NEW_TIME function implementation
 *
 * NEW_TIME(date, from_tz, to_tz)
 * Returns the date and time in time zone to_tz when date and time in time zone from_tz are date.
 * The return type is always DATE, regardless of the data type of date.
 *
 * Supported timezone abbreviations:
 * AST, ADT: Atlantic Standard or Daylight Time
 * BST, BDT: Bering Standard or Daylight Time
 * CST, CDT: Central Standard or Daylight Time
 * EST, EDT: Eastern Standard or Daylight Time
 * GMT: Greenwich Mean Time
 * HST, HDT: Alaska-Hawaii Standard Time or Daylight Time
 * MST, MDT: Mountain Standard or Daylight Time
 * NST: Newfoundland Standard Time
 * PST, PDT: Pacific Standard or Daylight Time
 * YST, YDT: Yukon Standard or Daylight Time
 */
class ObExprNewTime : public ObFuncExprOperator
{
public:
  explicit ObExprNewTime(common::ObIAllocator &alloc);
  virtual ~ObExprNewTime() {}

  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &input1,
                                ObExprResType &input2,
                                ObExprResType &input3,
                                common::ObExprTypeCtx &type_ctx) const override;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &expr) const override;

  static int eval_new_time(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

  static int eval_new_time_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                   const ObBitVector &skip, const EvalBound &bound);

private:
  // Disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprNewTime);

  static int get_timezone_offset(const ObString &tz_abbr, int32_t &offset);

  static int calc_new_time(int64_t timestamp_data,
                           const ObString &from_tz,
                           const ObString &to_tz,
                           ObDatum &result);

  // Helper function to apply timezone offset and set result
  template <typename ResVec, typename IN_TYPE>
  static OB_INLINE int apply_timezone_offset_and_set_result(IN_TYPE timestamp_val,
                                                              int32_t from_offset,
                                                              int32_t to_offset,
                                                              ResVec *res_vec,
                                                              int64_t idx);

  // Template functions for vectorized execution
  template <typename ArgVec, typename ResVec, typename IN_TYPE>
  static int new_time_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);

  template <typename ArgVec, typename ResVec, typename IN_TYPE>
  static int new_time_vector_const(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_EXPR_NEW_TIME_H_
