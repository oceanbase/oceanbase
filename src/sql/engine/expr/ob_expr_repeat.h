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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REPEAT_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REPEAT_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRepeat : public ObStringExprOperator
{
public:
  explicit  ObExprRepeat(common::ObIAllocator &alloc);
  virtual ~ObExprRepeat();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &text,
                                ObExprResType &count,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &result,
                  const common::ObObj &text,
                  const common::ObObj &count,
                  common::ObIAllocator *allocator,
                  const common::ObObjType res_type,
                  const int64_t max_result_size);
  static int calc(common::ObObj &result,
                  const common::ObObjType type,
                  const common::ObString &text,
                  const int64_t count,
                  common::ObIAllocator *allocator,
                  const int64_t max_result_size);

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_repeat(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  static int repeat(common::ObString &output,
                    bool &is_null,
                    const common::ObString &input,
                    const int64_t count,
                    common::ObIAllocator &alloc,
                    const int64_t max_result_size);
  static int repeat_text(ObObjType res_type,
                         bool has_lob_header,
                         ObString &output,
                         bool &is_null,
                         const ObString &text,
                         const int64_t count,
                         ObIAllocator &allocator,
                         const int64_t max_result_size);
private:
  static const int64_t MEM_WARN_THRESHOLD = 100 * 1024 * 1024; // 100M
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprRepeat);
};

}
}
#endif //OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REPEAT_
