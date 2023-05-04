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

#ifndef OCEANBASE_SQL_OB_EXPR_STR_TO_DATE_H_
#define OCEANBASE_SQL_OB_EXPR_STR_TO_DATE_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_to_temporal_base.h"

namespace oceanbase
{
namespace sql
{
class ObExprStrToDate : public ObFuncExprOperator
{
public:
  explicit  ObExprStrToDate(common::ObIAllocator &alloc);
  virtual ~ObExprStrToDate();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &date,
                                ObExprResType &format,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprStrToDate);
};

class ObExprOracleToDate : public ObExprToTemporalBase
{
public:
  explicit ObExprOracleToDate(common::ObIAllocator &alloc) :
    ObExprToTemporalBase(alloc, T_FUN_SYS_TO_DATE, N_TO_DATE)
  {
  }
  virtual ~ObExprOracleToDate() {}

  int set_my_result_from_ob_time(common::ObExprCtx &expr_ctx,
                                 common::ObTime &ob_time,
                                 common::ObObj &result) const;
  common::ObObjType get_my_target_obj_type() const
  {
    return common::ObDateTimeType;
  }

};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_STR_TO_DATE_H_
