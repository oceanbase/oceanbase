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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SYS_CONNECT_BY_PATH_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SYS_CONNECT_BY_PATH_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/connect_by/ob_nl_cnnt_by_with_index_op.h"

namespace oceanbase
{
namespace sql
{
class ObExprSysConnectByPath : public ObExprOperator
{
public:
  explicit ObExprSysConnectByPath(common::ObIAllocator &alloc);
  virtual ~ObExprSysConnectByPath() {}
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int calc_sys_path(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int eval_sys_connect_by_path(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum,
                                      ObNLConnectByOpBase *connect_by_op);

private:
  static int StringContain(const common::ObString &src_str,
      const common::ObString &sub_str, bool &is_contain, common::ObCollationType cs_type);
  DISALLOW_COPY_AND_ASSIGN(ObExprSysConnectByPath) const;
};
}
}
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SYS_CONNECT_BY_PATH_
