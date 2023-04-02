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
 * This file is for define of func json_pretty
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_PRETTY_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_PRETTY_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/json_type/ob_json_common.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonPretty : public ObFuncExprOperator
{
public:
  explicit ObExprJsonPretty(common::ObIAllocator &alloc);
  virtual ~ObExprJsonPretty();

  static int calc(ObEvalCtx &ctx, const ObDatum &data, ObDatumMeta meta, bool has_lob_header,
                  ObIAllocator *allocator, ObJsonBuffer &j_buf, bool &is_null);
  int calc_result_type1(ObExprResType &type,
                        ObExprResType &type1,
                        common::ObExprTypeCtx &type_ctx) const override;
  
  static int eval_json_pretty(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                    ObExpr &rt_expr) const override;
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprJsonPretty);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_PRETTY_H_