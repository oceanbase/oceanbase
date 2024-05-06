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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_TIME_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_TIME_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprTime : public ObFuncExprOperator
{
public:
  explicit ObExprTime(common::ObIAllocator &alloc);
  virtual ~ObExprTime();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual common::ObCastMode get_cast_mode() const override { return CM_NULL_ON_WARN; }
  static int calc_time(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTime);
};

class ObExprTimeBase : public ObFuncExprOperator
{
public:
  explicit ObExprTimeBase(common::ObIAllocator &alloc, int32_t date_type, ObExprOperatorType type,
                          const char *name, ObValidForGeneratedColFlag valid_for_generated_col);
  virtual ~ObExprTimeBase();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum,
                  int32_t type, bool with_date, bool is_allow_incomplete_dates = false);
  virtual int is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const;
  DECLARE_SET_LOCAL_SESSION_VARS;
private :
  int32_t dt_type_;
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTimeBase);
};
inline int ObExprTimeBase::calc_result_type1(ObExprResType &type,
                                           ObExprResType &type1,
                                           common::ObExprTypeCtx &type_ctx) const
{
  type.set_int32();
  type.set_precision(4);
  type.set_scale(0);
  common::ObObjTypeClass tc1 = ob_obj_type_class(type1.get_type());
  if (ob_is_enum_or_set_type(type1.get_type()) || ob_is_string_type(type1.get_type())) {
    type1.set_calc_type_default_varchar();
  } else if ((common::ObFloatTC == tc1) || (common::ObDoubleTC == tc1)) {
    type1.set_calc_type(common::ObNumberType);
  }
  return common::OB_SUCCESS;
}

class ObExprHour : public ObExprTimeBase
{
public:
  explicit ObExprHour(common::ObIAllocator &alloc);
  virtual ~ObExprHour();
  static int calc_hour(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprHour);
};

class ObExprMinute: public ObExprTimeBase
{
public:
  ObExprMinute();
  explicit ObExprMinute(common::ObIAllocator &alloc);
  virtual ~ObExprMinute();
  static int calc_minute(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMinute);
};

class ObExprSecond: public ObExprTimeBase
{
public:
  ObExprSecond();
  explicit ObExprSecond(common::ObIAllocator &alloc);
  virtual ~ObExprSecond();
  static int calc_second(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSecond);
};

class ObExprMicrosecond: public ObExprTimeBase
{
public:
  ObExprMicrosecond();
  explicit ObExprMicrosecond(common::ObIAllocator &alloc);
  virtual ~ObExprMicrosecond();
  static int calc_microsecond(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMicrosecond);
};

class ObExprYear: public ObExprTimeBase
{
public:
  ObExprYear();
  explicit ObExprYear(common::ObIAllocator &alloc);
  virtual ~ObExprYear();
  static int calc_year(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprYear);
};

class ObExprMonth: public ObExprTimeBase
{
public:
  ObExprMonth();
  explicit ObExprMonth(common::ObIAllocator &alloc);
  virtual ~ObExprMonth();
  static int calc_month(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMonth);
};

class ObExprMonthName: public ObExprTimeBase
{
public:
  ObExprMonthName();
  explicit ObExprMonthName(common::ObIAllocator &alloc);
  virtual ~ObExprMonthName();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc_month_name(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static const char* get_month_name(int month);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMonthName);
};

}
}


#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_TIME_H_ */
