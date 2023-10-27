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
#include "ob_expr_get_sys_var.h"
#include "lib/time/ob_time_utility.h"
#include "lib/number/ob_number_v2.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_cur_time.h"
#include "observer/ob_server_struct.h"
#include "share/system_variable/ob_system_variable.h"

namespace oceanbase
{
using namespace common;
using namespace observer;
using namespace share;
using namespace share::schema;
namespace sql
{

ObExprGetSysVar::ObExprGetSysVar(ObIAllocator &alloc)
    :ObFuncExprOperator(alloc, T_OP_GET_SYS_VAR, N_GET_SYS_VAR, 2, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                        INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprGetSysVar::~ObExprGetSysVar()
{
}

int ObExprGetSysVar::calc_result_type2(ObExprResType &type,
                                       ObExprResType &type1,
                                       ObExprResType &type2,
                                       common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type2);
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session is NULL");
  } else {
    ObString var_name = type1.get_param().get_varchar();
    bool is_exist = false;
    ObObjType data_type = ObMaxType;
    if (OB_FAIL(session->sys_variable_exists(var_name, is_exist))) {
      LOG_WARN("failed to check if sys variable exists", K(var_name), K(ret));
    } else {
      if (is_exist) {
        ObBasicSysVar *sys_var_ptr = NULL;
        if (OB_FAIL(session->get_sys_variable_by_name(var_name, sys_var_ptr))) {
          LOG_WARN("fail to get sys var from session", K(var_name), K(ret));
        } else if (OB_ISNULL(sys_var_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sys var is NULL", K(var_name), K(ret));
        } else if (sys_var_ptr->is_enum_type()) {
          // 用户在select enum类型的系统变量时，ObBasicSysVar将其记录为ObIntType
          // 但是为了方便展示，最终都会以字符串的形式返回，这种行为也与MySQL兼容
          // 所以这里显式设置返回类型为varchar
          data_type = ObVarcharType;
        } else {
          data_type = sys_var_ptr->get_meta_type();
        }
        if (OB_SUCC(ret)) {
          type.set_type(data_type);
          if (!var_name.compare(OB_SV_TIMESTAMP)) {
            type.set_scale(MAX_SCALE_FOR_TEMPORAL);
          }
          if (ob_is_string_type(data_type)) {
            type.set_collation_level(CS_LEVEL_SYSCONST);

            int64_t sys_var_val_length = OB_MAX_SYS_VAR_VAL_LENGTH;
            if (0 == var_name.compare(OB_SV_TCP_INVITED_NODES)) {
              uint64_t data_version = 0;
              if (OB_FAIL(GET_MIN_DATA_VERSION(session->get_effective_tenant_id(), data_version))) {
                LOG_WARN("fail to get tenant data version", KR(ret));
              } else if (data_version >= DATA_VERSION_4_2_1_1) {
               sys_var_val_length = OB_MAX_TCP_INVITED_NODES_LENGTH;
              }
            }

            if (OB_SUCC(ret)) {
              type.set_length(sys_var_val_length);
              if (is_oracle_mode()) {
                type.set_collation_type(session->get_nls_collation());
                type.set_length_semantics(session->get_actual_nls_length_semantics());
              } else {
                ObCollationType conn_coll = CS_TYPE_INVALID;
                OZ(session->get_collation_connection(conn_coll));
                OX(type.set_collation_type(conn_coll));
              }
            }
          }
        }
      } else {
        ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
        LOG_USER_ERROR(OB_ERR_SYS_VARIABLE_UNKNOWN, var_name.length(), var_name.ptr());
      }
    }
  }
  return ret;
}

int ObExprGetSysVar::calc_(ObObj &result, const ObString &var_name, const int64_t var_scope,
                           ObSQLSessionInfo *session, ObExecContext *exec_ctx, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ObBasicSysVar *sys_var_ptr = NULL;
  // 先从session中取出(session中包含所有的系统变量，包括only global的)
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(session->get_sys_variable_by_name(var_name, sys_var_ptr))) {
    LOG_WARN("fail to get sys var from session", K(var_name), K(ret));
  } else if (OB_ISNULL(sys_var_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys var is NULL", K(var_name), K(ret));
  } else if (ObSetVar::SET_SCOPE_NEXT_TRANS == static_cast<ObSetVar::SetScopeType>(var_scope)) {
    // 如果没指定scope，如果是session变量，直接从session中拿，如果是only global的，则从内部表中拿
    if (sys_var_ptr->is_session_scope()) {
      // get session variable
      if (OB_FAIL(get_session_var(result, var_name, alloc, session, exec_ctx))) {
        LOG_WARN("fail to get session var", K(var_name));
      }
    } else if (sys_var_ptr->is_global_scope()) {
      // get global variable
      if (OB_FAIL(get_sys_var_disp_obj(alloc, *session, var_name, result))) {
        LOG_WARN("get system variable disp obj failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("sys var is neither session nor global", K(ret), K(var_name));
    }
  } else if (ObSetVar::SET_SCOPE_GLOBAL == static_cast<ObSetVar::SetScopeType>(var_scope)) {
    if (!sys_var_ptr->is_global_scope()) {
      // 不是global变量，也不是global and session变量，是only session变量
      ret = OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR;
      ObString scope_name("SESSION");
      LOG_USER_ERROR(OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR, var_name.length(), var_name.ptr(), scope_name.length(), scope_name.ptr());
    } else {
      // get global variable
      if (OB_FAIL(get_sys_var_disp_obj(alloc, *session, var_name, result))) {
        LOG_WARN("get system variable disp obj failed", K(ret));
      }
    }
  } else if (ObSetVar::SET_SCOPE_SESSION == static_cast<ObSetVar::SetScopeType>(var_scope)) {
    if (!sys_var_ptr->is_session_scope()) {
      // 不是session变量，也不是global and session变量，是only global变量
      ret = OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR;
      ObString scope_name("GLOBAL");
      LOG_USER_ERROR(OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR, var_name.length(), var_name.ptr(), scope_name.length(), scope_name.ptr());
    } else {
      // get session variable
      if (OB_FAIL(get_session_var(result, var_name, alloc, session, exec_ctx))) {
        LOG_WARN("fail to get session var", K(var_name));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid sys var scope", K(var_scope));
  }
  return ret;
}

int ObExprGetSysVar::get_session_var(ObObj &result,
                                     const ObString &var_name,
                                     ObIAllocator &alloc,
                                     ObSQLSessionInfo *session,
                                     ObExecContext *exec_ctx)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_ISNULL(session) || OB_ISNULL(exec_ctx)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session or exec_ctx is NULL", K(ret), KP(session), KP(exec_ctx));
  } else if (OB_FAIL(session->sys_variable_exists(var_name, is_exist))) {
    LOG_WARN("failed to check if sys variable exists", K(var_name), K(ret));
  } else if (!is_exist) {
    ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
    LOG_USER_ERROR(OB_ERR_SYS_VARIABLE_UNKNOWN, var_name.length(), var_name.ptr());
  } else {
    if (0 == (var_name.compare(OB_SV_TIMESTAMP))) {
      if (OB_ISNULL(exec_ctx->get_physical_plan_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exec_ctx->get_physical_plan_ctx is NULL()", K(ret));
      } else {
        int64_t ts_value = exec_ctx->get_physical_plan_ctx()->get_cur_time().get_timestamp();
        number::ObNumber nmb;
        number::ObNumber nmb_unit;
        if (OB_FAIL(nmb.from(ts_value, alloc))) {
          LOG_WARN("get nmb from cur time failed", K(ret), K(ts_value));
        } else if (OB_FAIL(nmb_unit.from(USECS_PER_SEC, alloc))) {
          LOG_WARN("get nmb failed", K(ret), K(USECS_PER_SEC));
        } else {
          number::ObNumber value;
          if (OB_FAIL(nmb.div(nmb_unit, value, alloc))) {
            LOG_WARN( "failed to get the result of 'timestamp div 1000000'", K(ret));
          } else {
            result.set_number(value);
          }
        }
      }
    } else {
      ObBasicSysVar *sys_var = NULL;
      if (OB_FAIL(session->get_sys_variable_by_name(var_name, sys_var))) {
        LOG_WARN("fail to get sys var from session", K(var_name), K(ret));
      } else if (OB_ISNULL(sys_var)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sys var is NULL", K(var_name), K(ret));
      } else if (OB_FAIL(sys_var->to_select_obj(alloc, *session, result))) {
        LOG_WARN("fail to convert to select obj", K(*sys_var), K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && lib::is_oracle_mode() && result.is_null_oracle()) {
    result.set_null();
  }
  return ret;
}

int ObExprGetSysVar::get_sys_var_disp_obj(common::ObIAllocator &allocator,
                                          const ObSQLSessionInfo &session,
                                          const ObString &var_name,
                                          ObObj &disp_obj)
{
  int ret = OB_SUCCESS;
  ObBasicSysVar *sys_var = NULL;
  ObSysVarFactory sysvar_fac;
  ObObj value;
  ObSysVarClassType sys_var_id = SYS_VAR_INVALID;
  if (OB_FAIL(ObBasicSessionInfo::get_global_sys_variable(&session, allocator, var_name, value))) {
    LOG_WARN("get sys var disp obj failed", K(ret));
  } else if (SYS_VAR_INVALID == (sys_var_id = ObSysVarFactory::find_sys_var_id_by_name(var_name, false))) {
    ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
    LOG_WARN("unknown system variable", K(var_name));
  } else if (OB_FAIL(sysvar_fac.create_sys_var(sys_var_id, sys_var))) {
    LOG_WARN("create system variable obj failed", K(ret));
  } else if (OB_ISNULL(sys_var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("system variable is null");
  } else {
    sys_var->set_value(value);
    if (OB_FAIL(sys_var->to_select_obj(allocator, session, disp_obj))) {
      LOG_WARN("to select obj in sys_var failed", K(ret), K(var_name));
    }
  }
  if (OB_SUCC(ret) && lib::is_oracle_mode() && disp_obj.is_null_oracle()) {
    disp_obj.set_null();
  }
  return ret;
}

// for engine 3.0
int ObExprGetSysVar::calc_get_sys_val_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *name = NULL;
  ObDatum *scope = NULL;
  if (OB_UNLIKELY(2 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, name, scope))) {
    LOG_WARN("eval param failed", K(ret));
  } else {
    const ObString &var_name = name->get_string();
    int64_t var_scope = scope->get_int();
    ObObj result;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    if (OB_FAIL(calc_(result, var_name, var_scope, ctx.exec_ctx_.get_my_session(),
                      &ctx.exec_ctx_, calc_alloc))) {
      LOG_WARN("calc_ failed", K(ret), K(name), K(scope));
    } else {
      const ObObjType &obj_type = result.get_type();
      const ObObjType &res_type = expr.datum_meta_.type_;
      if (!result.is_null() && OB_UNLIKELY(obj_type != res_type)) {
        // 确保下编译期结果类型跟实际结果类型是一致的，否则从datum的内存空间可能会因为类型
        // 不一致出现问题
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("compile type and calc res type is different", K(ret), K(obj_type), K(res_type));
      } else if (ob_is_string_type(obj_type)) {
        ObString res_str;
        ObExprStrResAlloc str_alloc(expr, ctx);
        if (OB_FAIL(deep_copy_ob_string(str_alloc, result.get_string(), res_str))) {
          LOG_WARN("deep copy obstring failed", K(ret), K(result));
        } else {
          res_datum.set_string(res_str);
        }
      } else {
        if (OB_FAIL(res_datum.from_obj(result))) {
          LOG_WARN("get datum from obj failed", K(ret), K(result));
        }
      }
    }
  }
  return ret;
}

int ObExprGetSysVar::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_get_sys_val_expr;
  return ret;
}
}
}
