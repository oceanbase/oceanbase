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

#ifndef _OB_SQL_EXPR_TO_OUTFILE_ROW_H_
#define _OB_SQL_EXPR_TO_OUTFILE_ROW_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {

class ObExprToOutfileRow : public ObStringExprOperator {
public:
  explicit ObExprToOutfileRow(common::ObIAllocator& alloc);
  virtual ~ObExprToOutfileRow();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs, int64_t param_num, common::ObExprCtx& expr_ctx) const;
  virtual int cg_expr(ObExprCGCtx& ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const;
  static int to_outfile_str(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& datum);
  static int copy_string_to_buf(char* buf, const int64_t buf_len, int64_t& pos, const common::ObString& str);
  static int copy_char_to_buf(char* buf, const int64_t buf_len, int64_t& pos, const char c);

private:
  enum ParameterEnum { PARAM_FIELD = 0, PARAM_LINE, PARAM_ENCLOSED, PARAM_OPTIONAL, PARAM_SELECT_ITEM };
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprToOutfileRow) const;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQL_EXPR_TO_OUTFILE_ROW_H_ */
