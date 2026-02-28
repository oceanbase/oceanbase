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

#ifndef _OB_SQL_EXPR_DATE_ADD_CK_H_
#define _OB_SQL_EXPR_DATE_ADD_CK_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

/**
 * ClickHouse-compatible DATE_ADD/DATE_SUB expression base class
 *
 * Supported OceanBase types (no MySQL-specific types):
 * - date: date without time (ObDateType)
 * - datetime: datetime with precision 0-6 (ObDateTimeType with scale)
 * - timestamp: timestamp type (ObTimestampType)
 *
 * Key differences from MySQL mode:
 * 1. Return type precision promotion based on interval unit (e.g., MICROSECOND -> datetime(6))
 * 2. Support for MILLISECOND and NANOSECOND units
 * 3. date + sub-second units must error
 * 4. Interval values are always truncated (floor), never rounded
 * 5. String input is treated as datetime with scale 3 (assumes millisecond precision)
 *
 * Note: OceanBase datetime precision range is 0-6, ClickHouse DateTime64 supports 0-9.
 *       Nanosecond precision (9 digits) is mapped to microsecond precision (6 digits).
 */
class ObExprDateAdjustClickhouse : public ObFuncExprOperator
{
public:
  ObExprDateAdjustClickhouse(common::ObIAllocator &alloc,
                             ObExprOperatorType type,
                             const char *name,
                             int32_t param_num,
                             int32_t dimension = NOT_ROW_DIMENSION);

  virtual ~ObExprDateAdjustClickhouse();

  /**
   * Type inference for ClickHouse mode
   *
   * Precision promotion rules (unit-driven, input precision is base):
   * 1. date + sub-second unit (MILLISECOND, MICROSECOND, NANOSECOND) -> ERROR
   * 2. datetime + MILLISECOND -> datetime(3)
   * 3. datetime + MICROSECOND -> datetime(6)
   * 4. datetime + NANOSECOND -> datetime(6)  // Note: OB max precision is 6
   * 5. datetime(N) + sub-second unit -> datetime(max(N, unit_required_precision))
   * 6. date + time unit (HOUR, MINUTE, SECOND) -> datetime(0)
   * 7. date + date unit (DAY, WEEK, MONTH, QUARTER, YEAR) -> date
   * 8. string + any unit -> base_scale=3 (assumes millisecond precision)
   *    - string + MICROSECOND/NANOSECOND -> datetime(6) (max(3,6)=6)
   *    - string + MILLISECOND/SECOND/MINUTE/HOUR/DAY -> datetime(3) (max(3,3)=3 or max(3,0)=3)
   *
   * Note: Interval value precision is NOT considered, only unit type matters
   */
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &date,
                                ObExprResType &interval,
                                ObExprResType &unit,
                                common::ObExprTypeCtx &type_ctx) const;

  /**
   * Calculation function for ClickHouse mode
   *
   * Key behaviors:
   * 1. All interval values are truncated (floor), not rounded
   * 2. Set calc_type to datetime for first parameter for simplified computation
   * 3. Support MILLISECOND and NANOSECOND conversion
   */
  static int calc_date_adjust_ck(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum, bool is_add);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDateAdjustClickhouse);
};

class ObExprDateAddClickhouse : public ObExprDateAdjustClickhouse
{
public:
  explicit ObExprDateAddClickhouse(common::ObIAllocator &alloc);
  virtual ~ObExprDateAddClickhouse() {};

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_date_add_ck(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_date_add_ck_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                     const ObBitVector &skip, const EvalBound &bound);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDateAddClickhouse);
};

class ObExprDateSubClickhouse : public ObExprDateAdjustClickhouse
{
public:
  explicit ObExprDateSubClickhouse(common::ObIAllocator &alloc);
  virtual ~ObExprDateSubClickhouse() {};

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_date_sub_ck(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_date_sub_ck_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                     const ObBitVector &skip, const EvalBound &bound);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDateSubClickhouse);
};

} // namespace sql
} // namespace oceanbase

#endif // _OB_SQL_EXPR_DATE_ADD_CK_H_
