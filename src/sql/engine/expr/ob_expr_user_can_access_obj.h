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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_USER_CAN_ACCESS_OBJ_
#define OCEANBASE_SQL_ENGINE_EXPR_USER_CAN_ACCESS_OBJ_

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/ob_priv_common.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace sql
{

class ObExprUserCanAccessObj : public ObFuncExprOperator
{
public:
  explicit  ObExprUserCanAccessObj(common::ObIAllocator &alloc);
  virtual ~ObExprUserCanAccessObj();
  virtual int calc_result_type3(
      ObExprResType &type,
      ObExprResType &arg1,
      ObExprResType &arg2,
      ObExprResType &arg3,
      common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx, 
      const ObRawExpr &raw_expr,
      ObExpr &rt_expr) const override;

  static int eval_user_can_access_obj(
      const ObExpr &expr, 
      ObEvalCtx &ctx, 
      ObDatum &res_datum);
  static int build_raw_obj_priv(
      uint64_t obj_type, 
      share::ObRawObjPrivArray &raw_obj_priv_array);

private:
  static int build_real_obj_type_for_sym(
      uint64_t tenant_id,
      share::schema::ObSchemaGetterGuard *schema_guard,
      uint64_t &obj_type,
      uint64_t &obj_id);

  static int check_user_access_obj(
      share::schema::ObSchemaGetterGuard *schema_guard,
      ObSQLSessionInfo *session,
      uint64_t obj_type,
      uint64_t obj_id,
      uint64_t owner_id,
      bool &can_access);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUserCanAccessObj);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_INITCAP_ */
