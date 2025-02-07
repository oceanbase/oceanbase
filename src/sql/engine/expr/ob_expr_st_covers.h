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
 * This file contains implementation for eval_st_covers.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_COVERS_H_
#define OCEANBASE_SQL_OB_EXPR_ST_COVERS_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExprPrivSTCovers : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTCovers(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTCovers();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_st_covers(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  template<typename ResType>
  static int eval_st_covers_common(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   MultimodeAlloctor &temp_allocator,
                                   ObString wkb1,
                                   ObString wkb2,
                                   ResType &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTCovers);
};
} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_COVERS_H_