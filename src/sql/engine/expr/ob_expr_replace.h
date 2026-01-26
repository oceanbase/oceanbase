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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REPLACE_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REPLACE_

#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_string_searcher.h"
namespace oceanbase
{

namespace sql
{
class ObExprReplace : public ObStringExprOperator
{
#if OB_USE_MULTITARGET_CODE
  using ObStringSearcher = common::specific::avx2::ObStringSearcher;
#endif
public:
  explicit  ObExprReplace(common::ObIAllocator &alloc);
  virtual ~ObExprReplace();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_replace(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  static int eval_replace_vector(VECTOR_EVAL_FUNC_ARG_DECL);


  // This code of string_searcher is reused from ob_expr_like.h
  template <typename TextVec, typename FromVec, typename ToVec, typename ResVec>
  static int replace_vector_inner(VECTOR_EVAL_FUNC_ARG_DECL);

  // helper func
  // This function is used in "eval_replace_vector" to select which "replace" to use (simd or not).
  static int replace_dispatch(common::ObString &result,
                              const ObCollationType cs_type,
                              const common::ObString &text,
                              const common::ObString &from,
                              const common::ObString &to,
                              ObExprStringBuf &string_buf,
                              void *string_searcher, /*ObStringSearcher*/
                              const int64_t max_len = OB_MAX_VARCHAR_LENGTH,
                              bool need_check_from_to = true);

  static int replace(common::ObString &result,
                     const ObCollationType cs_type,
                     const common::ObString &text,
                     const common::ObString &from,
                     const common::ObString &to,
                     ObExprStringBuf &string_buf,
                     const int64_t max_len = OB_MAX_VARCHAR_LENGTH);

  static int replace_by_simd(common::ObString &result,
                             const ObCollationType cs_type,
                             const common::ObString &text,
                             const common::ObString &from,
                             const common::ObString &to,
                             ObExprStringBuf &string_buf,
                             void *string_searcher, /*ObStringSearcher*/
                             const int64_t max_len = OB_MAX_VARCHAR_LENGTH,
                             bool need_check_from_to = true);

  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprReplace);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REPLACE_
