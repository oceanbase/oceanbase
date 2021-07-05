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
namespace oceanbase {
namespace sql {
class ObConstRawExpr;
class ObExprIsBase : public ObRelationalExprOperator {
public:
  //  ObExprIsBase();
  explicit ObExprIsBase(common::ObIAllocator& alloc, ObExprOperatorType type, const char* name);
  virtual ~ObExprIsBase(){};

  int calc_result3(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      const common::ObObj& obj3, common::ObExprCtx& expr_ctx) const;

  int calc_with_int_internal(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      common::ObCastCtx& cast_ctx, bool is_not /* True if IS_NOT */) const;
  int cg_expr_internal(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr,
      const ObConstRawExpr*& const_param2, const ObConstRawExpr*& const_param3) const;
  int cg_result_type_class(common::ObObjType type, ObExpr::EvalFunc& eval_func, bool is_not, bool is_true) const;
  int calc(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, const common::ObObj& obj3,
      common::ObCastCtx& cast_ctx) const;

  int calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprResType& type3,
      common::ObExprTypeCtx& type_ctx) const;

  virtual int calc_with_null(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      const common::ObObj& obj3, common::ObCastCtx& cast_ctx) const = 0;
  virtual int calc_with_int(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      common::ObCastCtx& cast_ctx) const = 0;
  template <typename T>
  static int is_zero(T number);

private:
  // types and constants
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprIsBase);
  // function members
private:
  // data members
};

class ObExprIs : public ObExprIsBase {
public:
  //  ObExprIs() {}
  explicit ObExprIs(common::ObIAllocator& alloc) : ObExprIsBase(alloc, T_OP_IS, N_IS){};
  virtual ~ObExprIs(){};

  virtual int calc_with_null(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      const common::ObObj& obj3, common::ObCastCtx& cast_ctx) const;
  virtual int calc_with_int(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObCastCtx& cast_ctx) const;
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_is_date_int_null(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int calc_is_null(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int int_is_true(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int int_is_false(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int float_is_true(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int float_is_false(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int double_is_true(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int double_is_false(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int number_is_true(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int number_is_false(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  // types and constants
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprIs);
  // function members
private:
  // data members
};

class ObExprIsNot : public ObExprIsBase {
public:
  explicit ObExprIsNot(common::ObIAllocator& alloc) : ObExprIsBase(alloc, T_OP_IS_NOT, N_IS_NOT){};
  virtual ~ObExprIsNot(){};

  virtual int calc_with_null(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      const common::ObObj& obj3, common::ObCastCtx& cast_ctx) const;
  virtual int calc_with_int(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObCastCtx& cast_ctx) const;
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_is_not_null(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int int_is_not_true(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int int_is_not_false(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int float_is_not_true(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int float_is_not_false(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int double_is_not_true(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int double_is_not_false(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int number_is_not_true(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int number_is_not_false(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  // types and constants
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprIsNot);
  // function members
private:
  // data members
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_EXPR_IS_H */
