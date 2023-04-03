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

#ifndef OB_EXPR_ORAHASH
#define OB_EXPR_ORAHASH

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprOrahash : public ObFuncExprOperator
{
public:
  explicit ObExprOrahash(common::ObIAllocator& alloc);
  virtual ~ObExprOrahash();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  static int eval_orahash(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprOrahash);

  int get_int64_value(const common::ObObj &obj,
                            common::ObExprCtx &expr_ctx,
                            int64_t &val) const;
  bool is_applicable_type(const common::ObObj& intput) const;
  static bool is_valid_number(const int64_t& input);
  bool is_any_null(const common::ObObj *objs, const int64_t num) const;
  uint64_t hash_mod_oracle(uint64_t val, uint64_t buckets) const;

  static const int64_t MAX_BUCKETS = 4294967295; // buckets and seed share the same max value;
};
}
}

#endif
