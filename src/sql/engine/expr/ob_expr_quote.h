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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_QUOTE_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_QUOTE_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprQuote: public ObStringExprOperator
{
public:
  ObExprQuote();
  explicit  ObExprQuote(common::ObIAllocator &alloc);
  virtual ~ObExprQuote();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int calc_quote_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObDatum &res_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  static const int16_t APPEND_LEN = 2;
  static const int16_t LEN_OF_NULL = 4;
  // quote string
  static int calc(common::ObString &res_str, common::ObString str,
                  common::ObCollationType coll_type, common::ObIAllocator *allocator);
  static int string_write_buf(const common::ObString &str, char *buf, const int64_t buf_len, int64_t &pos);
  DISALLOW_COPY_AND_ASSIGN(ObExprQuote);

};
}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_QUOTE_H_ */
