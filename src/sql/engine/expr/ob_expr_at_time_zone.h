/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_AT_TIME_ZONE_H_
#define OCEANBASE_SQL_OB_EXPR_AT_TIME_ZONE_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprAtTimeZoneBase : public ObFuncExprOperator
{
public:
  explicit ObExprAtTimeZoneBase(common::ObIAllocator &alloc, ObExprOperatorType type,
                                const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprAtTimeZoneBase() {}
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &expr) const override;
  static int calc(common::ObOTimestampData &timestamp_data,
                  const common::ObTimeZoneInfoPos &tz_info_pos);
};

class ObExprAtTimeZone : public ObExprAtTimeZoneBase
{
public:
  ObExprAtTimeZone();
  explicit ObExprAtTimeZone(common::ObIAllocator &alloc);
  virtual ~ObExprAtTimeZone() {}
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &input1,
                                ObExprResType &input2,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc_at_time_zone(const common::ObString &tz_str,
                               common::ObOTimestampData &timestamp_data,
                               ObSQLSessionInfo *session);
  static int eval_at_time_zone(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int find_time_zone_pos(const common::ObString &tz_name,
                         const common::ObTimeZoneInfo &tz_info,
                         common::ObTimeZoneInfoPos &tz_info_pos);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprAtTimeZone);
};

class ObExprAtLocal : public ObExprAtTimeZoneBase
{
public:
  ObExprAtLocal();
  explicit ObExprAtLocal(common::ObIAllocator &alloc);
  virtual ~ObExprAtLocal() {}
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &input1,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc_at_local(ObSQLSessionInfo *session, common::ObOTimestampData &timestamp_data);
  static int eval_at_local(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprAtLocal);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_AT_TIME_ZONE_H_
