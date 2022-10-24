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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REGEXP_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REGEXP_

#include "lib/string/ob_string.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"

namespace oceanbase
{
namespace sql
{
class ObExprRegexp : public ObFuncExprOperator
{
  OB_UNIS_VERSION_V(1);
public:
  explicit  ObExprRegexp(common::ObIAllocator &alloc);
  virtual ~ObExprRegexp();

  virtual int assign(const ObExprOperator &other);

  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual inline void reset()
  {
    regexp_idx_ = common::OB_COMPACT_INVALID_INDEX;
    pattern_is_const_ = false;
    value_is_const_ = false;
    ObFuncExprOperator::reset();
  }

  inline int16_t get_regexp_idx() const { return regexp_idx_; }
  inline void set_regexp_idx(int16_t regexp_idx) { regexp_idx_ = regexp_idx; }
  inline void set_pattern_is_const(bool pattern_is_const) { pattern_is_const_ = pattern_is_const; }
  inline void set_value_is_const(bool value_is_const) { value_is_const_ = value_is_const; }

  virtual bool need_rt_ctx() const override { return true; }
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_regexp(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  inline int need_fast_calc(common::ObExprCtx &expr_ctx, bool &result) const;
private:
  int16_t regexp_idx_; // idx of posix_regexp_list_ in plan ctx, for regexp operator
  bool pattern_is_const_;
  bool value_is_const_;
private:
  // disallow copy
  ObExprRegexp(const ObExprRegexp &other);
  ObExprRegexp &operator=(const ObExprRegexp &ohter);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REGEXP_ */
