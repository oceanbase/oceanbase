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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FUNC_DUMP_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FUNC_DUMP_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprFuncDump : public ObStringExprOperator
{
public:
  explicit  ObExprFuncDump(common::ObIAllocator &alloc);
  virtual ~ObExprFuncDump();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_dump(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  int calc_params(const common::ObObj *objs,
                  const int64_t param_num,
                  int64_t &fmt_enum,
                  int64_t &start_pos,
                  int64_t &print_value_len) const;
  int calc_number(const common::ObObj &input,
                  const int64_t fmt_enum,
                  int64_t &start_pos,
                  int64_t &print_value_len,
                  common::ObString &output) const;

  int calc_otimestamp(const common::ObObj &input,
                      const int64_t fmt_enum,
                      int64_t &start_pos,
                      int64_t &print_value_len,
                      common::ObString &output) const;

  int calc_string(const common::ObObj &input,
                  const int64_t fmt_enum,
                  int64_t &start_pos,
                  int64_t &print_value_len,
                  common::ObString &output) const;

  int calc_double(const common::ObObj &input,
                  const int64_t fmt_enum,
                  int64_t &start_pos,
                  int64_t &print_value_len,
                  common::ObString &output) const;
  int calc_interval(const common::ObObj &input,
                    const int64_t fmt_enum,
                    int64_t &start_pos,
                    int64_t &print_value_len,
                    common::ObString &output) const;

  DISALLOW_COPY_AND_ASSIGN(ObExprFuncDump);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FUNC_DUMP_
