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

#ifndef _OCEANBASE_SQL_ENGINE_EXPR_NVL2_ORACLE_H_
#define _OCEANBASE_SQL_ENGINE_EXPR_NVL2_ORACLE_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_nvl.h"

namespace oceanbase
{
namespace sql
{
using namespace oceanbase::common;
class ObExprNvl2Oracle: public ObFuncExprOperator
{
public:
  explicit ObExprNvl2Oracle(ObIAllocator &alloc);
  virtual ~ObExprNvl2Oracle();

  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprResType &type3,
                                ObExprTypeCtx &type_ctx) const;
  static int calc_nvl2_oracle_expr_batch(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          const ObBitVector &skip,
                                          const int64_t batch_size);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprNvl2Oracle);
};
} // namespace sql
} // namespace oceanbase

#endif
