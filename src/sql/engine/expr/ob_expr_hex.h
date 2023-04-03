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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HEX_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HEX_

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"
namespace oceanbase
{
namespace sql
{
class ObExprHex : public ObStringExprOperator
{
public:
  explicit  ObExprHex(common::ObIAllocator &alloc);
  virtual ~ObExprHex();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &result, const common::ObObj &text, common::ObCastCtx &cast_ctx);

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_hex(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  // helper func
  static int get_uint64(const common::ObObj &obj, common::ObCastCtx &cast_ctx, uint64_t &out);
  static int number_uint64(const common::number::ObNumber &num_val, uint64_t &out);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprHex);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HEX_ */
