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
 * This file contains implementation for eval_st_bestsrid.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_BESTSRID_H_
#define OCEANBASE_SQL_OB_EXPR_ST_BESTSRID_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprPrivSTBestsrid : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTBestsrid(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTBestsrid();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int eval_st_bestsrid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int get_geog_box(ObEvalCtx &ctx,
                          common::ObArenaAllocator &temp_allocator,
                          ObString wkb,
                          ObObjType input_type,
                          bool &is_geo_empty,
                          ObGeogBox *&geo_box);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTBestsrid);
};
} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_BESTSRID_H_