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
 * This file contains implementation for array_to_string expression.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_TO_STRING
#define OCEANBASE_SQL_OB_EXPR_ARRAY_TO_STRING

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExprArrayToString : public ObFuncExprOperator
{
public:
  explicit ObExprArrayToString(common::ObIAllocator &alloc);

  virtual ~ObExprArrayToString();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_array_to_string(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_to_string_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                        const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_to_string_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                         const ObBitVector &skip, const EvalBound &bound);
  template <typename ResVec>
  static int set_text_res(ObStringBuffer &res_buf, const ObExpr &expr, ObEvalCtx &ctx,
                           ResVec *res_vec, int64_t batch_idx)
  {
    int ret = OB_SUCCESS;
    char *buf = nullptr;
    int64_t len = 0;
    ObTextStringVectorResult<ResVec> str_result(expr.datum_meta_.type_, &expr, &ctx, res_vec, batch_idx);
    if (OB_FAIL(str_result.init_with_batch_idx(res_buf.length(), batch_idx))) {
      SQL_ENG_LOG(WARN, "fail to init result", K(ret), K(res_buf.length()));
    } else if (OB_FAIL(str_result.get_reserved_buffer(buf, len))) {
      SQL_ENG_LOG(WARN, "fail to get reserver buffer", K(ret));
    } else if (len < res_buf.length()) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "get invalid res buf len", K(ret), K(len), K(res_buf.length()));
    } else if (OB_FALSE_IT(MEMCPY(buf, res_buf.ptr(), res_buf.length()))) {
    } else if (OB_FAIL(str_result.lseek(len, 0))) {
      SQL_ENG_LOG(WARN, "failed to lseek res.", K(ret), K(str_result), K(len));
    } else {
      str_result.set_result();
    }
    return ret;
  }
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayToString);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_TO_STRING