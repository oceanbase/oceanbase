/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SEARCH_INDEX_INNER_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SEARCH_INDEX_INNER_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSearchIndexInnerPath : public ObFuncExprOperator
{
public:
  explicit ObExprSearchIndexInnerPath(common::ObIAllocator &alloc);
  virtual ~ObExprSearchIndexInnerPath();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_search_index_inner_path(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  static uint64_t make_extra(const ObRawExpr &raw_expr);
  static ObItemType get_pick_type(const uint64_t extra);
  static uint8_t get_bound_enc_type(const uint64_t extra);
  static int calc_pick_inner_path(const ObIJsonBase &j_base,
                                  const ObString &path_prefix,
                                  ObIAllocator &allocator,
                                  const ObItemType pick_type,
                                  const uint8_t bound_enc_type,
                                  ObString &result);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSearchIndexInnerPath);
};

class ObExprSearchIndexInnerValue : public ObFuncExprOperator
{
public:
  explicit ObExprSearchIndexInnerValue(common::ObIAllocator &alloc);
  virtual ~ObExprSearchIndexInnerValue();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_search_index_inner_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSearchIndexInnerValue);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SEARCH_INDEX_INNER_ */
