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
 * This file is for define of func json_unquote
 */


#ifndef OCEANBASE_SQL_OB_EXPR_JSON_UNQUOTE_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_UNQUOTE_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/json_type/ob_json_tree.h"
#include "sql/engine/expr/ob_expr_multi_mode_func_helper.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonUnquote : public ObFuncExprOperator
{
public:
  explicit ObExprJsonUnquote(common::ObIAllocator &alloc);
  virtual ~ObExprJsonUnquote();

  static int calc(ObEvalCtx &ctx, const ObDatum &data, ObDatumMeta meta, bool has_lob_header,
                  MultimodeAlloctor *allocator, ObJsonBuffer &j_buf, bool &is_null);

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;

  static int eval_json_unquote(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprJsonUnquote);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_UNQUOTE_H_