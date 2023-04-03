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

#ifndef OCEANBASE_SQL_OB_EXPR_CONVERT_TZ_H_
#define OCEANBASE_SQL_OB_EXPR_CONVERT_TZ_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprConvertTZ : public ObFuncExprOperator{
public:
  explicit ObExprConvertTZ(common::ObIAllocator &alloc);
  virtual ~ObExprConvertTZ(){}
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &input1,
                                ObExprResType &input2,
                                ObExprResType &input3,
                                common::ObExprTypeCtx &type_ctx)const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &expr) const override;
  static int eval_convert_tz(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int find_time_zone_pos(const ObString &tz_name,
                                        const ObTimeZoneInfo &tz_info,
                                        ObTimeZoneInfoPos &tz_info_pos);
  template <typename T>
  static int calc_convert_tz(int64_t timestamp_data, const ObString &tz_str_s,//source time zone (input2)
                        const ObString &tz_str_d,//destination time zone (input3)
                        ObSQLSessionInfo *session,
                        T &result);
  static int calc(int64_t &timestamp_data, const ObTimeZoneInfoPos &tz_info_pos,
                 const bool input_utc_time);
  static int parse_string(int64_t &timestamp_data, const ObString &tz_str,
                          ObSQLSessionInfo *session, const bool input_utc_time);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprConvertTZ);

};


} //sql
} //oceanbasel
#endif //OCEANBASE_SQL_OB_EXPR_CONVERT_TZ_H_
