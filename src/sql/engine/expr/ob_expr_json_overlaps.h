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
 * This file contains implementation for json_overlaps.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_OVERLAPS_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_OVERLAPS_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/json_type/ob_json_tree.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonOverlaps : public ObFuncExprOperator
{
public:
  explicit ObExprJsonOverlaps(common::ObIAllocator &alloc);
  virtual ~ObExprJsonOverlaps();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprTypeCtx &type_ctx) const override;
                                
  static int eval_json_overlaps(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  static int json_overlaps_object(ObIJsonBase *json_a, ObIJsonBase *json_b, bool *result);
  static int json_overlaps_array(ObIJsonBase *json_a, ObIJsonBase *json_b, bool *result);
  static int json_overlaps(ObIJsonBase *json_a, ObIJsonBase *json_b, bool *result);
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonOverlaps);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_OVERLAPS_H_