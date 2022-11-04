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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_DAY_OF_FUNC_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_DAY_OF_FUNC_H_
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_time.h"

namespace oceanbase
{
namespace sql
{

class ObExprDayOfMonth: public ObExprTimeBase
{
public:
  ObExprDayOfMonth();
  explicit ObExprDayOfMonth(common::ObIAllocator &alloc);
  virtual ~ObExprDayOfMonth();
  static int calc_dayofmonth(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDayOfMonth);
};

class ObExprDay: public ObExprTimeBase
{
public:
  ObExprDay();
  explicit ObExprDay(common::ObIAllocator &alloc);
  virtual ~ObExprDay();
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDay);
};

class ObExprDayOfWeek: public ObExprTimeBase
{
public:
  ObExprDayOfWeek();
  explicit ObExprDayOfWeek(common::ObIAllocator &alloc);
  virtual ~ObExprDayOfWeek();
  static int calc_dayofweek(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDayOfWeek);
};

class ObExprDayOfYear: public ObExprTimeBase
{
public:
  ObExprDayOfYear();
  explicit ObExprDayOfYear(common::ObIAllocator &alloc);
  virtual ~ObExprDayOfYear();
  static int calc_dayofyear(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDayOfYear);
};

class ObExprToSeconds: public ObFuncExprOperator
{
public:
  ObExprToSeconds();
  explicit ObExprToSeconds(common::ObIAllocator &alloc);
  virtual ~ObExprToSeconds();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &date,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_toseconds(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToSeconds);
};

inline int ObExprToSeconds::calc_result_type1(ObExprResType &type,
                                              ObExprResType &date,
                                              common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(date);
  type.set_int();
  type.set_scale(common::DEFAULT_SCALE_FOR_INTEGER);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  if (ob_is_enumset_tc(date.get_type())) {
    date.set_calc_type(common::ObVarcharType);
  }
  return common::OB_SUCCESS;
}

class ObExprSecToTime: public ObFuncExprOperator
{
public:
  ObExprSecToTime();
  explicit ObExprSecToTime(common::ObIAllocator &alloc);
  virtual ~ObExprSecToTime();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &date,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_sectotime(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSecToTime);
};

inline int ObExprSecToTime::calc_result_type1(ObExprResType &type,
                                              ObExprResType &sec,
                                              common::ObExprTypeCtx &type_ctx) const
{
  type.set_time();
  type.set_scale((0 <= sec.get_scale() && sec.get_scale() <= 6) ? sec.get_scale() : common::MAX_SCALE_FOR_TEMPORAL);
  //set calc type
  sec.set_calc_type(common::ObNumberType);
  UNUSED(type_ctx);
  return common::OB_SUCCESS;
}

class ObExprTimeToSec: public ObFuncExprOperator
{
public:
  ObExprTimeToSec();
  explicit ObExprTimeToSec(common::ObIAllocator &alloc);
  virtual ~ObExprTimeToSec();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &date,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_timetosec(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprTimeToSec);
};

inline int ObExprTimeToSec::calc_result_type1(ObExprResType &type,
                                              ObExprResType &time,
                                              common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_int();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);
  time.set_calc_type(common::ObTimeType);
  return common::OB_SUCCESS;
}

class ObExprSubAddtime: public ObFuncExprOperator
{
public:
  ObExprSubAddtime(common::ObIAllocator &alloc, ObExprOperatorType type, const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprSubAddtime() {};
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &date_arg,
                                ObExprResType &time_arg,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_result2(common::ObObj &result,
                           const common::ObObj &date_arg,
                           const common::ObObj &time_arg,
                           common::ObExprCtx &expr_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual common::ObCastMode get_cast_mode() const override { return CM_NULL_ON_WARN; }
  static int subaddtime_common(const ObExpr &expr, ObEvalCtx &ctx,
                            ObDatum &expr_datum,
                            bool &null_res,
                            common::ObDatum *&date_arg,
                            common::ObDatum *&time_arg,
                            int64_t &time_val);
  static int subaddtime_datetime(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int subaddtime_varchar(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSubAddtime);
};

class ObExprSubtime: public ObExprSubAddtime
{
public:
  explicit ObExprSubtime(common::ObIAllocator &alloc);
  virtual ~ObExprSubtime() {};
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSubtime);
};

class ObExprAddtime: public ObExprSubAddtime
{ 
public:
  explicit ObExprAddtime(common::ObIAllocator &alloc);
  virtual ~ObExprAddtime() {};
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprAddtime);
};

class ObExprDayName: public ObExprTimeBase
{
public:
  ObExprDayName();
  explicit ObExprDayName(common::ObIAllocator &alloc);
  virtual ~ObExprDayName();
  virtual int calc_result_type1(ObExprResType &type,
                               ObExprResType &type1,
                               common::ObExprTypeCtx &type_ctx) const;
  static int calc_dayname(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDayName);
};

}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_DAY_OF_FUNC_H_ */
