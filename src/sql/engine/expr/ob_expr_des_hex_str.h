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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_DES_HEX_STR_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_DES_HEX_STR_

#include "common/expression/ob_expr_string_buf.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprDesHexStr : public ObFuncExprOperator
{
public:
  explicit ObExprDesHexStr(common::ObIAllocator &alloc);
  virtual ~ObExprDesHexStr() {}

  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_des_hex_str(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  static int deserialize_hex_cstr(const char *buf,
                                  int64_t buf_len,
                                  common::ObExprStringBuf &string_buf,
                                  common::ObObj &obj);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDesHexStr);
};
} // namespace sql
} // namespace oceanbase
#endif //OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_DES_HEX_STR_
