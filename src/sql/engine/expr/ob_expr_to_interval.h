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

#ifndef OB_EXPR_TO_INTERVAL_H
#define OB_EXPR_TO_INTERVAL_H


#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{

class ObExprToYMInterval : public ObFuncExprOperator
{
public:
  explicit  ObExprToYMInterval(common::ObIAllocator &alloc);
  virtual ~ObExprToYMInterval();
  int calc_result_type1(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_to_yminterval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToYMInterval);
};

class ObExprToDSInterval : public ObFuncExprOperator
{
public:
  explicit  ObExprToDSInterval(common::ObIAllocator &alloc);
  virtual ~ObExprToDSInterval();
  int calc_result_type1(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_to_dsinterval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToDSInterval);
};

class ObExprNumToYMInterval : public ObFuncExprOperator
{
public:
  explicit  ObExprNumToYMInterval(common::ObIAllocator &alloc);
  virtual ~ObExprNumToYMInterval();
  int calc_result_type2(ObExprResType &type, ObExprResType &type1, ObExprResType &type2, common::ObExprTypeCtx &type_ctx) const;
  template <class T>
  static int calc_result_common(const T &obj1,
                                const T &obj2,
                                common::ObIAllocator &calc_buf,
                                common::ObIntervalYMValue &ym_value);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_num_to_yminterval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprNumToYMInterval);
};

class ObExprNumToDSInterval : public ObFuncExprOperator
{
public:
  explicit  ObExprNumToDSInterval(common::ObIAllocator &alloc);
  virtual ~ObExprNumToDSInterval();
  int calc_result_type2(ObExprResType &type, ObExprResType &type1, ObExprResType &type2, common::ObExprTypeCtx &type_ctx) const;
  template <class R, class T>
  static int calc_result_common(R &result,
                                const T &obj1,
                                const T &obj2,
                                common::ObIAllocator &calc_buf);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_num_to_dsinterval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprNumToDSInterval);
};

}
}


#endif // OB_EXPR_TO_INTERVAL_H
