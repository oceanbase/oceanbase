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
 * This file contains implementation for st_distance_sphere expr.
 */

#ifndef OCEANBASE_SQL_OB_ST_DISTANCE_SPHERE_
#define OCEANBASE_SQL_OB_ST_DISTANCE_SPHERE_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprSTDistanceSphere : public ObFuncExprOperator
{
public:
  // Sphere raduis initialized to default radius for SRID 0. Approximates Earth radius.
  static constexpr double DEFAULT_SRID0_SPHERE_RADIUS = 6370986.0;
  explicit ObExprSTDistanceSphere(common::ObIAllocator &alloc);
  virtual ~ObExprSTDistanceSphere();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int eval_st_distance_sphere(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static const char *get_func_name() { return N_ST_DISTANCE_SPHERE; }
private:
  static constexpr uint32_t ERR_INFO_LEN = 64;
  static void construct_err_info();
  DISALLOW_COPY_AND_ASSIGN(ObExprSTDistanceSphere);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_ST_DISTANCE_SPHERE_