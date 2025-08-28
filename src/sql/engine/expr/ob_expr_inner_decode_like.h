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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INNER_DECODE_LIKE_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INNER_DECODE_LIKE_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
//This expression is used to extract like range.
//inner_decode_like(pattern, escape, is_start, column_type, column_collation, column_length)
//for example: inner_decode_like('123%', '\\', 1, 22 \*ObVarcharType*\, 45 \*CS_TYPE_UTF8MB4_GENERAL_CI*\, 4) = '123\min\min\min...' (length:16)
//             inner_decode_like('123%', '\\', 0, 22 \*ObVarcharType*\, 45 \*CS_TYPE_UTF8MB4_GENERAL_CI*\, 4) = '123\max\max\max...' (length:16)
class ObExprInnerDecodeLike : public ObStringExprOperator
{
public:
  explicit  ObExprInnerDecodeLike(common::ObIAllocator &alloc);
  virtual ~ObExprInnerDecodeLike() {};

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int eval_inner_decode_like(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  static int cast_like_obj_if_needed(ObEvalCtx &ctx, const ObExpr &pattern_expr, ObDatum *pattern_datum,
                                    const ObExpr &dst_expr, ObDatum * &cast_datum);
  // get prefix string (without wildcards) length of like pattern 
  static int get_pattern_prefix_len(const ObCollationType &cs_type, 
                                    const ObString &escape_str, 
                                    const ObString &pattern_str,
                                    int32_t &pattern_prefix_len);
  DISALLOW_COPY_AND_ASSIGN(ObExprInnerDecodeLike) const;
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INNER_DECODE_LIKE_
