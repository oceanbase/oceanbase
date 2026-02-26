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
 * This file contains implementation for string_to_array expression.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_STRING_TO_ARRAY
#define OCEANBASE_SQL_OB_EXPR_STRING_TO_ARRAY

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExprStringToArray : public ObFuncExprOperator
{
public:
  explicit ObExprStringToArray(common::ObIAllocator &alloc);
  virtual ~ObExprStringToArray();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_string_to_array(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_string_to_array_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                        const ObBitVector &skip, const int64_t batch_size);
  static int eval_string_to_array_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                         const ObBitVector &skip, const EvalBound &bound);
  static int add_value_str_to_array(ObArrayBinary *binary_array, std::string value_str, bool has_null_str, std::string null_str);
  static int string_to_array(ObArrayBinary *binary_array,
                             std::string arr_str, std::string delimiter, std::string null_str,
                             ObCollationType cs_type, bool has_arr_str, bool has_delimiter, bool has_null_str);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprStringToArray);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_STRING_TO_ARRAY