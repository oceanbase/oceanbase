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

namespace oceanbase {
namespace sql {
class ObExprReplace : public ObStringExprOperator {
public:
  explicit ObExprReplace(common::ObIAllocator& alloc);
  virtual ~ObExprReplace();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types_array, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;

  static int calc(common::ObObj& result, const common::ObObj& text, const common::ObObj& from, const common::ObObj& to,
      common::ObExprStringBuf& string_buf);

  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs_array, int64_t param_num, common::ObExprCtx& expr_ctx) const;

  ///@brief call function calc(), implementation of ObExprOperator::calc_result2()
  ///
  ///@param [out] result  result of REPLACE(str, str, str)
  ///@param [in] text input of str
  ///@param [in] text input of from which str
  ///@param [in] text input of to which str
  ///@param [in] allocator:ObExprStringBuf
  ///@return OB_SUCCESS success, others failure
  virtual int calc_result3(common::ObObj& result, const common::ObObj& text, const common::ObObj& from,
      const common::ObObj& to, common::ObExprCtx& expr_ctx) const;

  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;

  static int eval_replace(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprReplace);

  // helper func
  static int replace(common::ObString& result, const common::ObString& text, const common::ObString& from,
      const common::ObString& to, common::ObExprStringBuf& string_buf);
};

}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_REPLACE_
