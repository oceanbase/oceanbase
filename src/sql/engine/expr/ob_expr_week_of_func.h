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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_WEEK_OF_FUNC_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_WEEK_OF_FUNC_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

// WEEKOFYEAR(date)
// Returns the calendar week of the date
// --as a number in the range from 1 to 53.
// WEEKOFYEAR() is a compatibility function 
// --that is equivalent to WEEK(date,3).
class ObExprWeekOfYear : public ObFuncExprOperator {
public:
  ObExprWeekOfYear();
  explicit ObExprWeekOfYear(common::ObIAllocator& alloc);
  virtual ~ObExprWeekOfYear();
  virtual int calc_result_type1(ObExprResType& type, 
                                ObExprResType& date,
                                common::ObExprTypeCtx& type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_weekofyear(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const;
  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprWeekOfYear);
};

inline int ObExprWeekOfYear::calc_result_type1(ObExprResType& type,
    ObExprResType& date, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(date);
  type.set_int();
  type.set_scale(common::DEFAULT_SCALE_FOR_INTEGER);
  type.set_precision(common::ObAccuracy
      ::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  if (ob_is_enumset_tc(date.get_type())) {
    date.set_calc_type(common::ObVarcharType);
  }
  return common::OB_SUCCESS;
}

// WEEKDAY(date)
// Returns the weekday index for date
// --(0 = Monday, 1 = Tuesday, … 6 = Sunday).
class ObExprWeekDay : public ObFuncExprOperator {
public:
  ObExprWeekDay();
  explicit ObExprWeekDay(common::ObIAllocator& alloc);
  virtual ~ObExprWeekDay();
  virtual int calc_result_type1(ObExprResType& type, 
                                ObExprResType& date,
                                common::ObExprTypeCtx& type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_weekday(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const;
  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprWeekDay);
};
inline int ObExprWeekDay::calc_result_type1(ObExprResType& type,
    ObExprResType& date, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(date);
  type.set_int();
  type.set_scale(common::DEFAULT_SCALE_FOR_INTEGER);
  type.set_precision(common::ObAccuracy
      ::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  if (ob_is_enumset_tc(date.get_type())) {
    date.set_calc_type(common::ObVarcharType);
  }
  return common::OB_SUCCESS;
}

// YEARWEEK(date), YEARWEEK(date,mode)
// Returns year and week for a date.
// example: select yearweek('2018-07-04',6) from dual;
//-----------> 201827
class ObExprYearWeek : public ObFuncExprOperator {
public:
  ObExprYearWeek();
  explicit ObExprYearWeek(common::ObIAllocator& alloc);
  virtual ~ObExprYearWeek();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num, 
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_yearweek(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  virtual int is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const;
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprYearWeek);
};

//WEEK(date), WEEK(date, mode) returns the week number for date. The two-argument form of WEEK() enables you to specify whether
//the week starts on Sunday or Monday and whether the return value should be in the range from 0 to 53 or from 1 to 53
//e.g., SELECT WEEK('2008-02-20',1); --> 8
//      SELECT WEEK('2008-02-20'); SELECT WEEK('2008-02-20', 0); --> 7
class ObExprWeek : public ObFuncExprOperator {
public:
  ObExprWeek();
  explicit ObExprWeek(common::ObIAllocator& alloc);
  virtual ~ObExprWeek();
  virtual int calc_result_typeN(ObExprResType& type, 
                                ObExprResType* types,
                                int64_t params_count,
                                common::ObExprTypeCtx& type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_week(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const;
  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprWeek);
};

}
}
#endif
