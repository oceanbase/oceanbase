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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_WIDTH_BUCKET_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_WIDTH_BUCKET_H_

#include <cstdint>

#include "sql/engine/expr/ob_expr_res_type.h"
#include "objit/common/ob_item_type.h"
#include "ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprWidthBucket: public ObFuncExprOperator
{
public:
  explicit  ObExprWidthBucket(common::ObIAllocator &alloc);
  virtual ~ObExprWidthBucket() {}

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const;
  static int calc_width_bucket_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  static int calc_time_type(const int64_t time_sec1, const int64_t time_frac_sec1,
                            const int64_t time_sec2, const int64_t time_frac_sec2,
                            const int64_t time_sec3, const int64_t time_frac_sec3,
                            const int64_t multiple, common::ObIAllocator &alloc,
                            common::number::ObNumber &e_nmb, common::number::ObNumber &start_nmb,
                            common::number::ObNumber &end_nmb);
  static int construct_param_nmb(const int64_t expr_value,
                                const int64_t start_value,
                                const int64_t end_value,
                                common::ObIAllocator &alloc,
                                common::number::ObNumber &expr_nmb,
                                common::number::ObNumber &start_nmb,
                                common::number::ObNumber &end_nmb);
};
}
}


#endif  /* _OB_EXPR_WIDTH_BUCKET_H_ */
