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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_can_access_trigger.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
ObExprCanAccessTrigger::ObExprCanAccessTrigger(ObIAllocator &alloc)
  : ObExprOperator(alloc, T_FUN_SYS_CAN_ACCESS_TRIGGER, N_CAN_ACCESS_TRIGGER, 2, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION, true)
{
}

ObExprCanAccessTrigger::~ObExprCanAccessTrigger()
{
}

int ObExprCanAccessTrigger::calc_result_type2(ObExprResType &type,
                                              ObExprResType &type1,
                                              ObExprResType &type2,
                                              ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  type2.set_calc_type(ObVarcharType);
  type2.set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  type.set_type(ObInt32Type);
  return ret;
}

int ObExprCanAccessTrigger::can_access_trigger(const ObExpr &expr,
                                           ObEvalCtx &ctx,
                                           ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  bool need_check = false;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    ObDatum &database_name = expr.locate_param_datum(ctx, 0);
    ObDatum &table_name = expr.locate_param_datum(ctx, 1);
    OX (res_datum.set_true());
    ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
    ObSessionPrivInfo session_priv;
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (OB_FAIL(session->check_feature_enable(
              ObCompatFeatureType::MYSQL_TRIGGER_PRIV_CHECK, need_check))) {
      LOG_WARN("failed to check feature enable", K(ret));
    } else if (need_check && lib::is_mysql_mode()) {
      if (OB_FAIL(session->get_session_priv_info(session_priv))) {
        LOG_WARN("faile to get session priv info", K(ret));
      } else {
        ObNeedPriv need_priv;
        need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
        need_priv.db_ = database_name.get_string();
        need_priv.priv_set_ = OB_PRIV_TRIGGER;
        need_priv.table_ = table_name.get_string();
        share::schema::ObSchemaGetterGuard *schema_guard =
        ctx.exec_ctx_.get_sql_ctx()->schema_guard_;
        if (OB_ISNULL(schema_guard)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema guard is NULL", K(ret));
        } else {
          OZ (schema_guard->check_single_table_priv(session_priv, session->get_enable_role_array(), need_priv), K(need_priv), K(ret));
          if(OB_ERR_NO_TABLE_PRIVILEGE == ret) {
            ret = OB_SUCCESS;
            OX (res_datum.set_false());
          }
        }
      }
    }
  }
  return ret;
}

int ObExprCanAccessTrigger::cg_expr(ObExprCGCtx &op_cg_ctx,
                                  const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprCanAccessTrigger::can_access_trigger;
  return OB_SUCCESS;
}

}
}