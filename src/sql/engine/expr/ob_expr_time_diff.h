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

#ifndef _OCEANBASE_SQL_OB_EXPR_TIME_DIFF_H_
#define _OCEANBASE_SQL_OB_EXPR_TIME_DIFF_H_
#include "lib/timezone/ob_time_convert.h"
#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprTimeDiff : public ObFuncExprOperator
{
public:
  explicit  ObExprTimeDiff(common::ObIAllocator &alloc);
  virtual ~ObExprTimeDiff();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &left,
                                ObExprResType &right,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &result, const common::ObObj &left,
                  const common::ObObj &right, common::ObCastCtx &cast_ctx);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_timediff(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  static int get_diff_value(const common::ObObj &obj1,
                            const common::ObObj &obj2,
                            const common::ObTimeZoneInfo *tz_info,
                            int64_t &diff);
  static int get_diff_value_with_ob_time(common::ObTime &ot1,
                                         common::ObTime &ot2,
                                         const common::ObTimeZoneInfo *tz_info,
                                         int64_t &diff);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTimeDiff);

};

inline int ObExprTimeDiff::calc_result_type2(ObExprResType &type,
                                            ObExprResType &left,
                                            ObExprResType &right,
                                            common::ObExprTypeCtx &type_ctx) const
{
  //TODO<xiaoni> actually ,the source of the objs will bring up different results
  //if obj is constant and of string type.
  //e.g. select timediff("12:34:56.123", "13:33:22.34567");
  //then the scale of result type will be 5 (the larger one)
  //if obj comes from a column of a table , then the scale of result type will be 6
  //no matter what the scale of objs is.
  //e.g. select timediff(tableA.left,tableB.right) from tableA,tableB
  //when left and right are both STRING type(varchar)
  //the scale of result type may be 6, always.
  int ret = common::OB_SUCCESS;
  common::ObScale scale = static_cast<common::ObScale>(common::max(left.get_scale(), right.get_scale()));
  if (scale > common::MAX_SCALE_FOR_TEMPORAL) {
    scale = common::MAX_SCALE_FOR_TEMPORAL;
  }

  if (ob_is_enumset_tc(left.get_type())) {
    left.set_calc_type(common::ObVarcharType);
    scale = common::MAX_SCALE_FOR_TEMPORAL;
  } else if (ob_is_real_type(left.get_type())) {
    left.set_calc_type(common::ObNumberType);
  }
  if (ob_is_enumset_tc(right.get_type())) {
    right.set_calc_type(common::ObVarcharType);
    scale = common::MAX_SCALE_FOR_TEMPORAL;
  }
  type.set_time();
  type.set_scale(scale);
  UNUSED(type_ctx);
  return ret;
}
} //sql
} //oceanbase
#endif //_OCEANBASE_SQL_OB_EXPR_TIME_DIFF_H_
