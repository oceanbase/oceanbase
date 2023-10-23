/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_SQL_EXPR_FORMAT_BYTES_H
#define _OB_SQL_EXPR_FORMAT_BYTES_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprFormatBytes: public ObFuncExprOperator
{
public:
  explicit  ObExprFormatBytes(common::ObIAllocator &alloc);
  virtual ~ObExprFormatBytes();

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int eval_format_bytes(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int eval_format_bytes_batch(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  const ObBitVector &skip,
                                  const int64_t batch_size);
  static int eval_format_bytes_util(const ObExpr &expr, ObDatum &res_datum,
                                    ObDatum *param1, ObEvalCtx &ctx, int64_t index = 0);
  static const common::ObLength VALUE_BUF_LEN = 20;  //value's string buffer length
  static const common::ObLength LENGTH_FORMAT_BYTES = 11;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprFormatBytes);
  static const uint64_t kib = 1024;
  static const uint64_t mib = 1024 * kib;
  static const uint64_t gib = 1024 * mib;
  static const uint64_t tib = 1024 * gib;
  static const uint64_t pib = 1024 * tib;
  static const uint64_t eib = 1024 * pib;
};
}
}
#endif /* _OB_SQL_EXPR_FORMAT_BYTES_H */
