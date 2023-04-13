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

#ifndef _OCEANBASE_SQL_OB_EXPR_TIME_STMAP_DIFF_H_
#define _OCEANBASE_SQL_OB_EXPR_TIME_STMAP_DIFF_H_
#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprTimeStampDiff : public ObFuncExprOperator
{
public:
  explicit  ObExprTimeStampDiff(common::ObIAllocator &alloc);
  virtual ~ObExprTimeStampDiff();
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &unit,
                                ObExprResType &left,
                                ObExprResType &right,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual common::ObCastMode get_cast_mode() const { return CM_NULL_ON_WARN;}

  static int eval_timestamp_diff(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  static int calc(int64_t &res, bool &is_null, int64_t unit_value, int64_t usec_left,
                  int64_t usec_right, const common::ObTimeZoneInfo *tz_info);
  static int calc_month_diff(const int64_t &left,
                             const int64_t &right,
                             const common::ObTimeZoneInfo *tz_info,
                             int64_t &diff);
  static int adjust_sub_one(const common::ObTime *p_min, const common::ObTime *p_max, int64_t &bias);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTimeStampDiff);
};

inline int ObExprTimeStampDiff::calc_result_type3(ObExprResType &type,
                                            ObExprResType &unit,
                                            ObExprResType &left,
                                            ObExprResType &right,
                                            common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(unit);
  type.set_int();
  type.set_scale(common::DEFAULT_SCALE_FOR_INTEGER);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  //set calc type
  left.set_calc_type(common::ObDateTimeType);
  right.set_calc_type(common::ObDateTimeType);
  return common::OB_SUCCESS;
}
} //sql
} //oceanbase
#endif //_OCEANBASE_SQL_OB_EXPR_TIME_STMAP_DIFF_H_
