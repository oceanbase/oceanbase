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

#ifndef OCEANBASE_SQL_ENGINE_OB_EXPR_TO_BASE64_
#define OCEANBASE_SQL_ENGINE_OB_EXPR_TO_BASE64_

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"

namespace oceanbase
{
namespace sql
{
class ObExprToBase64 : public ObStringExprOperator
{
public:
  explicit ObExprToBase64(common::ObIAllocator &alloc);
  virtual ~ObExprToBase64();
  virtual int calc_result_type1(ObExprResType &type,
                           ObExprResType &str,
                           common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const;
  static int eval_to_base64(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_to_base64_batch(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  const ObBitVector &skip,
                                  const int64_t batch_size);

  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToBase64);

  static int calc(common::ObObj &result,
                  const common::ObObj &obj1,
                  common::ObIAllocator *allocator);
  static const int64_t NCHAR_PER_BASE64 = 4;
  static const int64_t NCHAR_PER_BASE64_GROUP = 3;
  static const int64_t NCHAR_PER_STR_PAD = 2;
  static const int64_t NUM_OF_CHAR_PER_LINE_QUOTED_PRINTABLE = 76;
  static inline ObLength base64_needed_encoded_length(ObLength length_of_data)
  {
    ObLength nb_base64_chars;
    if (length_of_data == 0) {
      return 0;
    }
    nb_base64_chars = (length_of_data + NCHAR_PER_STR_PAD) / NCHAR_PER_BASE64_GROUP * NCHAR_PER_BASE64;
    return nb_base64_chars +            /* base64 char incl padding */
           (nb_base64_chars - 1) / 76 + /* newlines */
           1;                           /* NUL termination of string */
  }
};
}
}

#endif //OCEANBASE_SQL_ENGINE_OB_EXPR_TO_BASE64_
