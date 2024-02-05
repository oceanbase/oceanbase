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

#ifndef ENGINE_EXPR_OB_EXPR_FROM_UNIX_TIME_H_
#define ENGINE_EXPR_OB_EXPR_FROM_UNIX_TIME_H_

#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprFromUnixTime : public ObFuncExprOperator
{
public:
  explicit  ObExprFromUnixTime(common::ObIAllocator &alloc);
  virtual ~ObExprFromUnixTime();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *params,
                                int64_t params_count,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_one_temporal_fromtime(const ObExpr &expr,
                                        ObEvalCtx &eval_ctx,
                                        ObDatum &expr_datum);

  static int eval_one_param_fromtime(const ObExpr &expr,
                                     ObEvalCtx &eval_ctx,
                                     ObDatum &expr_datum);

  static int eval_fromtime_normal(const ObExpr &expr,
                                  ObEvalCtx &eval_ctx,
                                  ObDatum &expr_datum);

  static int eval_fromtime_special(const ObExpr &expr,
                                   ObEvalCtx &eval_ctx,
                                   ObDatum &expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  int set_scale_for_single_param(ObExprResType &type,
                                 const ObExprResType &type1) const;

  common::ObObjType calc_one_param_type(ObExprResType *params) const;

  static int get_usec_from_datum(const common::ObDatum &param_datum,
                                 common::ObIAllocator &alloc,
                                 int64_t &value);

  // static int eval_two_
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprFromUnixTime);
};

}
}

#endif /* ENGINE_EXPR_OB_EXPR_FROM_UNIX_TIME_H_ */
