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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_SUBSTRB_
#define OCEANBASE_SQL_ENGINE_EXPR_SUBSTRB_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprSubstrb : public ObStringExprOperator
{
public:
  explicit  ObExprSubstrb(common::ObIAllocator &alloc);
  virtual ~ObExprSubstrb();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &result,
                  const common::ObString &text,
                  int64_t start_pos,
                  int64_t length,
                  common::ObCollationType cs_type,
                  const common::ObObjType &res_type,
                  common::ObExprCtx &expr_ctx);
  static int calc(common::ObString &res_str, const common::ObString &text,
                  int64_t start, int64_t length, common::ObCollationType cs_type,
                  common::ObIAllocator &alloc);

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const;
  static int calc_substrb_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int handle_invalid_byte(char* ptr,
                                 const int64_t text_len,
                                 int64_t &start,
                                 int64_t &len,
                                 char reset_char,
                                 common::ObCollationType cs_type,
                                 const bool force_ignore_invalid_byte);
private:
  int calc_result_length_in_byte(const ObExprResType &type,
                                 ObExprResType *types_array,
                                 int64_t param_num,
                                 int64_t &res_len) const;
  static int ignore_invalid_byte(char* ptr,
                                 const int64_t text_len,
                                 int64_t &start,
                                 int64_t &len,
                                 common::ObCollationType cs_type);
  static int reset_invalid_byte(char* ptr,
                                const int64_t text_len,
                                int64_t start,
                                int64_t len,
                                char reset_char,
                                common::ObCollationType cs_type);
  // return the well formatted boundary of charset,
  // set boundary_len to -1 if no valid boundary found (invalid character of charset).
  static int get_well_formatted_boundary(common::ObCollationType cs_type,
                                         char *ptr,
                                         const int64_t len,
                                         int64_t pos,
                                         int64_t &boundary_pos,
                                         int64_t &boundary_len);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprSubstrb);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_SUBSTRB_ */
