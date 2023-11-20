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

#ifndef _OCEANBASE_SQL_OB_EXPR_TIME_STMAP_ADD_H_
#define _OCEANBASE_SQL_OB_EXPR_TIME_STMAP_ADD_H_
#define USING_LOG_PREFIX  SQL_ENG
#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase
{
namespace common
{
struct ObTimeConvertCtx;
}
namespace sql
{
class ObExprTimeStampAdd : public ObFuncExprOperator
{
public:
  explicit  ObExprTimeStampAdd(common::ObIAllocator &alloc);
  virtual ~ObExprTimeStampAdd();
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &unit,
                                ObExprResType &interval,
                                ObExprResType &timestamp,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const override;
  static void check_reset_status(const common::ObCastMode cast_mode,
                                 int &ret, common::ObObj &result);
  static int calc(const int64_t unit_value, common::ObTime &ot, const int64_t ts,
                  const common::ObTimeConvertCtx &cvrt_ctx, int64_t interval,
                  int64_t &delta);

  virtual int is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const;
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTimeStampAdd);
};

} //sql
} //oceanbase
#endif //_OCEANBASE_SQL_OB_EXPR_TIME_STMAP_ADD_H_
