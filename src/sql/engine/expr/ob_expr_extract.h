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

#ifndef OCEANBASE_SQL_OB_EXPR_EXTRACT_H_
#define OCEANBASE_SQL_OB_EXPR_EXTRACT_H_

#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObPhysicalPlanCtx;
class ObExprExtract : public ObFuncExprOperator
{
public:
  explicit  ObExprExtract(common::ObIAllocator &alloc);
  virtual ~ObExprExtract();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &date_unit,
                                ObExprResType &date,
                                common::ObExprTypeCtx &type_ctx) const;

  template<typename T_ARG, typename T_RES>
  static int calc_oracle(
      const ObSQLSessionInfo *session,
      ObEvalCtx &ctx,
      const ObObjType obj_type,
      const T_ARG &obj,
      const ObDateUnitType &extract_unit,
      T_RES &result);

  template<typename T_ARG, typename T_RES>
  static int calc(ObObjType date_type,
      const T_ARG &date,
      const ObDateUnitType extract_field,
      const ObScale scale,
      const ObCastMode cast_mode,
      const ObTimeZoneInfo *tz_info,
      const int64_t cur_ts_value,
      const ObDateSqlMode date_sql_mode,
      bool has_lob_header,
      T_RES &result);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_extract_oracle(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_extract_mysql(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  // for static engine batch
  static int calc_extract_oracle_batch(
      const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  static int calc_extract_mysql_batch(
      const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  static int calc_extract_oracle_vector(
      const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
  static int calc_extract_mysql_vector(
      const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);

private:
  int set_result_type_oracle(common::ObExprTypeCtx &type_ctx,
                             const ObExprResType &date_unit, 
                             ObExprResType &res_type) const;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprExtract);

};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_EXTRACT_H_
