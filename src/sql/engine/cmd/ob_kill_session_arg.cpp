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
#include "sql/resolver/cmd/ob_kill_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/engine/ob_physical_plan.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER(ObKillSessionArg,
                    sess_id_,
                    tenant_id_,
                    user_id_,
                    is_query_,
                    has_user_super_privilege_);

int ObKillSessionArg::init(ObExecContext &ctx, const ObKillStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret), K(ctx));
  } else if (OB_FAIL(calculate_sessid(ctx, stmt))) {
    LOG_WARN("fail to calculate sessid", K(ret), K(ctx), K(stmt));
  } else {
    tenant_id_ = session->get_priv_tenant_id();
    user_id_ = session->get_user_id();
    is_query_ = stmt.is_query();
    has_user_super_privilege_ = session->has_user_super_privilege();
  }
  return ret;
}

int ObKillSessionArg::calculate_sessid(ObExecContext &ctx, const ObKillStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = ctx.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  HEAP_VAR(ObPhysicalPlan, phy_plan) {
    ObPhysicalPlanCtx phy_plan_ctx(ctx.get_allocator());
    ObExecContext *exec_ctx = NULL;//because ObExecContext is bigger than 10K, can't use it as local variable
    ObExprCtx expr_ctx;
    ObNewRow tmp_row;
    RowDesc row_desc;
    ObObj value_obj;
    ObRawExpr *value_expr = NULL;
    void *tmp_ptr = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(my_session) || OB_ISNULL(plan_ctx) || OB_ISNULL(ctx.get_sql_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data member from ObExecContext is Null", K(ret), K(my_session), K(plan_ctx));
    } else {
      ObArenaAllocator allocator(common::ObModIds::OB_SQL_EXPR_CALC,
                                 OB_MALLOC_NORMAL_BLOCK_SIZE,
                                 my_session->get_effective_tenant_id());
      ObSqlExpression sql_expr(allocator, 0);
      const int64_t cur_time = plan_ctx->has_cur_time() ?
          plan_ctx->get_cur_time().get_timestamp() : ObTimeUtility::current_time();
      phy_plan_ctx.set_cur_time(cur_time, *my_session);

      if (OB_UNLIKELY(NULL == (tmp_ptr = allocator.alloc(sizeof(ObExecContext))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc ObExecContext", K(ret));
      } else {
        exec_ctx = new (tmp_ptr) ObExecContext(allocator);
        phy_plan_ctx.set_phy_plan(&phy_plan);
        expr_ctx.phy_plan_ctx_ = &phy_plan_ctx;
        expr_ctx.my_session_ = my_session;
        expr_ctx.exec_ctx_ = exec_ctx;
        expr_ctx.calc_buf_ = &allocator;
        ObTempExpr *temp_expr = NULL;
        if (OB_ISNULL(value_expr = stmt.get_value_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get value expr", K(ret), K(value_expr));
        } else if (OB_FAIL(ObStaticEngineExprCG::gen_expr_with_row_desc(value_expr,
             row_desc, ctx.get_allocator(), ctx.get_my_session(),
             ctx.get_sql_ctx()->schema_guard_, temp_expr))) {
          LOG_WARN("fail to fill sql expression", K(ret));
        } else if (OB_ISNULL(temp_expr)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("fail to gen temp expr", K(ret));
        } else if (OB_FAIL(temp_expr->eval(ctx, tmp_row, value_obj))) {
          LOG_WARN("fail to calc value", K(ret), K(stmt.get_value_expr()));
        } else {
          const ObObj *res_obj = NULL;
          if (stmt.is_alter_system_kill()) {
            ObString str = value_obj.get_string();
            const char* index = str.find(',');
            if (index != NULL) {
              str = str.split_on(index);
            }
            value_obj.set_string(value_obj.get_type(), str);
          }
          EXPR_DEFINE_CAST_CTX(expr_ctx, CM_WARN_ON_FAIL);
          EXPR_CAST_OBJ_V2(ObIntType, value_obj, res_obj);
          ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == ret ? OB_SUCCESS : ret;
          if (OB_FAIL(ret)) {
            LOG_WARN("fail to cast expr", "orig type", value_obj.get_type(), "dest type", "ObUint32type", K(ret), K(res_obj));
          } else if (OB_ISNULL(res_obj)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to cast expr", "orig type", value_obj.get_type(), "dest type", "ObUint32type", K(ret), K(res_obj));
          } else {
            sess_id_ = static_cast<uint32_t>(res_obj->get_int());
          }
        }
      }
    }
  }
  return ret;
}

}
}
