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

#ifndef _OB_SQL_EXPR_DATE_ADD_H_
#define _OB_SQL_EXPR_DATE_ADD_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{

class ObExprDateAdjust : public ObFuncExprOperator
{
public:
  ObExprDateAdjust(common::ObIAllocator &alloc,
                   ObExprOperatorType type,
                   const char *name,
                   int32_t param_num,
                   int32_t dimension = NOT_ROW_DIMENSION);

  virtual ~ObExprDateAdjust();
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &date,
                                ObExprResType &interval,
                                ObExprResType &unit,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc_date_adjust(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum, bool is_add);
  virtual int is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDateAdjust);
};

class ObExprDateAdd : public ObExprDateAdjust
{
public:
  explicit  ObExprDateAdd(common::ObIAllocator &alloc);
  virtual ~ObExprDateAdd() {};
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_date_add(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDateAdd);
};

class ObExprDateSub : public ObExprDateAdjust
{
public:
  explicit  ObExprDateSub(common::ObIAllocator &alloc);
  virtual ~ObExprDateSub() {};
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_date_sub(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDateSub);
};

class ObExprAddMonths : public ObFuncExprOperator
{
public:
  explicit ObExprAddMonths(common::ObIAllocator &alloc);
  virtual ~ObExprAddMonths() {}
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_add_months(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprAddMonths);
};

class ObExprLastDay : public ObFuncExprOperator
{
public:
  explicit ObExprLastDay(common::ObIAllocator &alloc);
  virtual ~ObExprLastDay() {}
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_last_day(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprLastDay);
};


class ObExprNextDay : public ObFuncExprOperator
{
public:
  explicit ObExprNextDay(common::ObIAllocator &alloc);
  virtual ~ObExprNextDay() {}
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_next_day(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprNextDay);
};

} //sql
} //oceanbase
#endif //_OCEANBASE_SQL_OB_EXPR_DATE_ADD_H_
