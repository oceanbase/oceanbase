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

#ifndef _OB_EXPR_IS_H
#define _OB_EXPR_IS_H 1
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObConstRawExpr;
class ObExprIsBase: public ObRelationalExprOperator
{
public:
   // speicial float/double value
  enum Ieee754
  {
    INFINITE_VALUE = 0,
    NAN_VALUE = 1,
  };

  explicit  ObExprIsBase(common::ObIAllocator &alloc,
                         ObExprOperatorType type,
                         const char *name);
  virtual ~ObExprIsBase() {};

  int cg_expr_internal(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr, const ObConstRawExpr *&const_param2) const;
  int cg_result_type_class(common::ObObjType type, ObExpr::EvalFunc &eval_func,
                          bool is_not, bool is_true) const;
  int calc_result_type2(ObExprResType &type,
                        ObExprResType &type1,
                        ObExprResType &type2,
                        common::ObExprTypeCtx &type_ctx) const;

  template <typename T>
  static int is_zero(T number, const uint32_t len);
  static int is_infinite_nan(const common::ObObjType, common::ObDatum *, bool &, Ieee754);
  private:
  // types and constants
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprIsBase);
  // function members
private:
  // data members
};

class ObExprIs: public ObExprIsBase
{
  public:
//  ObExprIs() {}
  explicit  ObExprIs(common::ObIAllocator &alloc)
     : ObExprIsBase(alloc, T_OP_IS, N_IS) {};
  virtual ~ObExprIs() {};

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                  const ObRawExpr &raw_expr,
                  ObExpr &rt_expr) const override;

  // keep this function for compatibility with server before 4.1
  static int calc_is_date_int_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  static int calc_is_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int int_is_true(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int int_is_false(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int json_is_true(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int json_is_false(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int float_is_true(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int float_is_false(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int double_is_true(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int double_is_false(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int number_is_true(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int number_is_false(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_is_infinite(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_is_nan(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  static int calc_collection_is_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int decimal_int_is_true(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int decimal_int_is_false(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  // types and constants
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprIs);
  // function members
private:
  // data members
};

class ObExprIsNot: public ObExprIsBase
{
public:
  explicit  ObExprIsNot(common::ObIAllocator &alloc)
     : ObExprIsBase(alloc, T_OP_IS_NOT, N_IS_NOT) {};
  virtual ~ObExprIsNot() {};

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                  const ObRawExpr &raw_expr,
                  ObExpr &rt_expr) const override;
  static int calc_is_not_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int int_is_not_true(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int int_is_not_false(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int json_is_not_true(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int json_is_not_false(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int float_is_not_true(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int float_is_not_false(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int double_is_not_true(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int double_is_not_false(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int number_is_not_true(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int number_is_not_false(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_is_not_infinite(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_is_not_nan(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  static int calc_collection_is_not_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  static int calc_batch_is_not_null(const ObExpr &expr, ObEvalCtx &ctx,
                                    const ObBitVector &skip, const int64_t batch_size);
  static int decimal_int_is_not_true(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int decimal_int_is_not_false(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  // types and constants
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprIsNot);
  // function members
private:
  // data members
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_EXPR_IS_H */
