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

#ifndef OCEANBASE_EXPR_OB_RT_DATUM_ARITH_H_
#define OCEANBASE_EXPR_OB_RT_DATUM_ARITH_H_

#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{

//
// Runtime datum arithmetic evaluation.
//
// Why need this:
// ObObj can do runtime arithmetic evaluation by the static interface of arithmetic expressions
// (e.g.: ObExprMul::calc). ObDatum can not do this, because ObDatum's memory is binding to ObExpr,
// it always need a ObExpr to do arithmetic evaluation. We must build the needed arithmetic
// expressions in compile time in static typing engine, see: the `linear_inter_expr_` expression of
//
// to use,  we need a convenient way to do runtime ObDatum arithmetic evaluation.
//
// Usage:
//
//  // Calculate: factor * (cur - prev) + prev
//
//  ObRTDatumArith arith(exec_context, session);
//
//  // prepare runtime datum arithmetic evaluation once
//  arith.setup_datum_metas(factor_meta, prev_meta, cur_meta);
//  // arith.ref(0): factor
//  // arith.ref(1): prev
//  // arith.ref(2): cur
//  // the argument of ref() is the index of input
//  arith.generate(arith.ref(0) * (arith.ref(2) - arith.ref(1)) + arith.ref(1))
//
//  // evaluate repeatedly
//  arith.eval(res_datum, factor_datum, prev_meta, cur_meta);
//
class ObRTDatumArith
{
public:
  struct Item
  {
    Item() : expr_(NULL), arith_(NULL) {}

    ObRawExpr *expr_;
    ObRTDatumArith *arith_;

    Item operator+(const Item &r);
    Item operator-(const Item &r);
    Item operator*(const Item &r);
    Item operator/(const Item &r);
  };

public:
  ObRTDatumArith(ObExecContext &ctx, ObSQLSessionInfo &session);
  virtual ~ObRTDatumArith();

  // Setup the input datum metas
  template <typename ...TS>
  int setup_datum_metas(TS &...input_meta)
  {
    ObDatumMeta metas[] = { input_meta... };
    return inner_setup_datum_metas(metas, ARRAYSIZEOF(metas));
  }

  // Get referenced item of %input_idx, which is the index of argument of setup_datum_metas()
  Item ref(const int64_t input_idx);
  int generate(const ObRTDatumArith::Item item);

  ObExpr *get_expr() const { return expr_; }

  // Evaluate the input datums' result, set the result's datum ptr to %res.
  // The %input should be correlate with datum metas in setup_datum_metas().
  template <typename ...TS>
  int eval(ObDatum *&res, TS &...input)
  {
    const ObDatum *args[] = { &input... };
    return inner_eval(res, args, ARRAYSIZEOF(args));
  }

private:
  Item build_arith_expr(const Item &l, const Item &r, ObItemType op);
  int inner_setup_datum_metas(const ObDatumMeta *metas, int64_t cnt);
  int inner_eval(ObDatum *&res, const ObDatum **args, int64_t arg_cnt);

private:
  common::ObArenaAllocator alloc_;

  ObExecContext &exec_ctx_;
  ObSQLSessionInfo &session_;
  ObRawExprFactory factory_;
  ObExprFrameInfo frame_info_;
  ObEvalCtx *eval_ctx_;
  ObExpr *expr_;

  common::ObFixedArray<ObRawExpr *, common::ObIAllocator> raw_cols_;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> cols_;

  DISABLE_COPY_ASSIGN(ObRTDatumArith);
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_EXPR_OB_RT_DAUM_ARITH_H_
