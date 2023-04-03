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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_CONCAT_WS_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_CONCAT_WS_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
//stands for Concatenate With Separator and is a special form of CONCAT()
class ObExprConcatWs: public ObStringExprOperator
{
public:
  ObExprConcatWs();
  explicit  ObExprConcatWs(common::ObIAllocator &alloc);
  virtual ~ObExprConcatWs();
   virtual int calc_result_typeN(ObExprResType &type,
                                 ObExprResType *types,
                                 int64_t param_num,
                                 common::ObExprTypeCtx &type_ctx) const;
  // connect two strings by separator
  static int concat_ws(const common::ObString obj1,
                       const common::ObString obj2,
                       const int64_t buf_len,
                       char **string_buf,
                       int64_t &pos);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int calc_concat_ws_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObDatum &res);
  static int calc(const common::ObString &sep_str, const common::ObIArray<common::ObString> &words,
                        common::ObIAllocator &alloc, common::ObString &res_str);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprConcatWs);
};

}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_CONCAT_WS_H_ */
