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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_MD5_CONCAT_WS_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_MD5_CONCAT_WS_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
// stands for concatenate with separator and null value, then compute md5 result
class ObExprMd5ConcatWs: public ObStringExprOperator
{
public:
  ObExprMd5ConcatWs();
  explicit  ObExprMd5ConcatWs(common::ObIAllocator &alloc);
  virtual ~ObExprMd5ConcatWs();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_md5_concat_ws_expr(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     ObDatum &res);
  static int calc_md5_concat_ws_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  template <typename StrVec, typename ResVec>
  static int vector_md5_concat_ws(VECTOR_EVAL_FUNC_ARG_DECL);
  static int concat_and_calc_md5(ObEvalCtx &ctx,
                                 const ObString sep_str,
                                 const ObIArray<ObString> &words,
                                 ObExprStrResAlloc &res_alloc,
                                 ObString &res);
  static const common::ObString::obstr_size_t MD5_LENGTH = 16;
  DISALLOW_COPY_AND_ASSIGN(ObExprMd5ConcatWs);
};

}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_MD5_CONCAT_WS_H_ */
