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

#ifndef _OCEANBASE_SQL_OB_EXPR_ERRNO_H_
#define _OCEANBASE_SQL_OB_EXPR_ERRNO_H_
#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprErrno : public ObFuncExprOperator
{
public:
  explicit ObExprErrno(common::ObIAllocator &alloc);
  virtual ~ObExprErrno();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                ObExprTypeCtx &type_ctx) const;
  static int eval_errno(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  static int conv2errno(int64_t value);
  static int get_value(const common::number::ObNumber &nmb, int64_t &value);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprErrno);
};

} //sql
} //oceanbase
#endif //_OCEANBASE_SQL_OB_EXPR_ERRNO_H_
