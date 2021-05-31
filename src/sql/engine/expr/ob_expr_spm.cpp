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

#include "sql/engine/expr/ob_expr_spm.h"

#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_common_rpc_proxy.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::obrpc;

namespace oceanbase {
namespace sql {
ObExprSpmLoadPlans::ObExprSpmLoadPlans(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SPM_LOAD_PLANS, N_SPM_LOAD_PLANS_FROM_PLAN_CACHE, 4, NOT_ROW_DIMENSION)
{}

ObExprSpmLoadPlans::~ObExprSpmLoadPlans()
{}

int ObExprSpmLoadPlans::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (4 != param_num || OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not 4", K(param_num), K(types), K(ret));
  } else {
    types[0].set_calc_type(ObVarcharType);
    types[1].set_calc_type(ObUInt64Type);
    types[2].set_calc_type(ObTinyIntType);
    types[3].set_calc_type(ObTinyIntType);
    type.set_int();
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  }

  return ret;
}

/*
 * objs[0]: sql_id          varchar
 * objs[1]: plan_hash_value int64_t
 * objs[2]: fixed           bool
 * objs[3]: enabled         bool
 * */
int ObExprSpmLoadPlans::calc_resultN(ObObj& result, const ObObj* objs, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_res = 0;

  ObBasicSessionInfo* session = NULL;
  if (OB_ISNULL(objs) || OB_UNLIKELY(4 != param_num)) {
    LOG_WARN("invalid argument", K(objs), K(param_num));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(session = expr_ctx.my_session_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(session), K(ret));
  }

  // analyse argument
  obrpc::ObAdminLoadBaselineArg load_baseline_arg;
  if (OB_SUCC(ret)) {
    // tenant_id
    uint64_t tenant_id = session->get_effective_tenant_id();
    if (OB_FAIL(load_baseline_arg.push_tenant(tenant_id))) {
      LOG_WARN("fail to push tenant id ", K(tenant_id), K(ret));
    } else if (!objs[0].is_varchar_or_char()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid sql id type", K(objs[0]), K(ret));
    } else {
      // sql_id
      load_baseline_arg.sql_id_ = objs[0].get_string();
      // plan_hash_value
      if (!objs[1].is_null()) {
        if (OB_FAIL(objs[1].get_uint64(load_baseline_arg.plan_hash_value_))) {
          LOG_WARN("fail to get plan hash value", K(ret));
        }
      }
      // fixed and enabled
      if (OB_SUCC(ret)) {
        if (!objs[2].is_null()) {
          load_baseline_arg.fixed_ = objs[2].is_true();
        }
        if (!objs[3].is_null()) {
          load_baseline_arg.enabled_ = objs[3].is_true();
        }
      }
    }
  }

  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(expr_ctx.exec_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr_ctx.exec_ctx_), K(ret));
  } else if (OB_ISNULL(task_exec_ctx = expr_ctx.exec_ctx_->get_task_executor_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->admin_load_baseline(load_baseline_arg))) {
    LOG_WARN("load baseline rpc failed", K(ret), "rpc_arg", load_baseline_arg);
  }

  result.set_int(tmp_res);
  return ret;
}

ObExprSpmAlterBaseline::ObExprSpmAlterBaseline(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SPM_ALTER_BASELINE, N_SPM_ALTER_BASELINE, 4, NOT_ROW_DIMENSION)
{}

ObExprSpmAlterBaseline::~ObExprSpmAlterBaseline()
{}

/**
 * sqlid T_VARCHAR
 * plan_hash_value int64
 * attribute_name T_VARCHAR
 * attribute_value T_VARCHAR
 */
int ObExprSpmAlterBaseline::calc_resultN(
    common::ObObj& result, const common::ObObj* objs, int64_t param_num, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObBasicSessionInfo* session = NULL;
  if (4 != param_num || OB_ISNULL(objs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not 4", K(param_num), K(objs), K(ret));
  } else if (OB_ISNULL(session = expr_ctx.my_session_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(session), K(ret));
  }
  obrpc::ObAlterPlanBaselineArg alter_baseline_arg;
  if (OB_SUCC(ret)) {
    alter_baseline_arg.plan_baseline_info_.key_.tenant_id_ = session->get_effective_tenant_id();
    alter_baseline_arg.exec_tenant_id_ = session->get_effective_tenant_id();
    if ((objs[0].is_null() && objs[1].is_null()) || (!objs[0].is_null() && !objs[0].is_varchar_or_char()) ||
        (!objs[1].is_null() && !objs[1].is_integer_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(objs[0]), K(objs[1]));
    } else {
      if (!objs[0].is_null()) {
        if (OB_FAIL(objs[0].get_string(alter_baseline_arg.plan_baseline_info_.sql_id_))) {
          LOG_WARN("fail to get sql_id");
        }
      }
      if (OB_SUCC(ret) && !objs[1].is_null()) {
        if (OB_FAIL(objs[1].get_uint64(alter_baseline_arg.plan_baseline_info_.plan_hash_value_))) {
          LOG_WARN("fail to get plan_hash_value");
        }
      }
      // update field
      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
        ObString name;
        ObString value;

        if (OB_FAIL(objs[2].get_string(name))) {
          LOG_WARN("fail to get alter attribute", K(objs[2]), K(ret));
        } else if (OB_FAIL(objs[3].get_string(value))) {
          LOG_WARN("fail to get alter value", K(objs[3]), K(ret));
        } else if (0 == name.case_compare("outline_data")) {
          alter_baseline_arg.field_update_bitmap_ = ObAlterPlanBaselineArg::OUTLINE_DATA;
          alter_baseline_arg.plan_baseline_info_.outline_data_ = value;
        } else if (0 == name.case_compare("fixed")) {
          alter_baseline_arg.field_update_bitmap_ = ObAlterPlanBaselineArg::FIXED;
          if (0 == value.case_compare("yes")) {
            alter_baseline_arg.plan_baseline_info_.fixed_ = true;
          } else if (0 == value.case_compare("no")) {
            alter_baseline_arg.plan_baseline_info_.fixed_ = false;
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K(name), K(value), K(ret));
          }
        } else if (0 == name.case_compare("enabled")) {
          alter_baseline_arg.field_update_bitmap_ = ObAlterPlanBaselineArg::ENABLED;
          if (0 == value.case_compare("yes")) {
            alter_baseline_arg.plan_baseline_info_.enabled_ = true;
          } else if (0 == value.case_compare("no")) {
            alter_baseline_arg.plan_baseline_info_.enabled_ = false;
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K(name), K(value), K(ret));
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid alter attribute", K(name), K(ret));
        }
      }  // update field end
    }
  }

  // executor
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc = NULL;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(expr_ctx.exec_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr_ctx.exec_ctx_), K(ret));
  } else if (OB_ISNULL(task_exec_ctx = expr_ctx.exec_ctx_->get_task_executor_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(task_exec_ctx));
  } else if (OB_FAIL(common_rpc->alter_plan_baseline(alter_baseline_arg))) {
    LOG_WARN("alter baseline rpc failed", K(ret), "rpc_arg", alter_baseline_arg);
  }

  result.set_int(0);
  return ret;
}

int ObExprSpmAlterBaseline::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  if (4 != param_num || OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not 4", K(param_num), K(types), K(ret));
  } else {
    types[0].set_calc_type(ObVarcharType);
    types[1].set_calc_type(ObUInt64Type);
    types[2].set_calc_type(ObVarcharType);
    types[3].set_calc_type(ObVarcharType);
    UNUSED(types);
    UNUSED(param_num);
    UNUSED(type_ctx);

    type.set_int();
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  }
  return ret;
}

ObExprSpmDropBaseline::ObExprSpmDropBaseline(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SPM_DROP_BASELINE, N_SPM_DROP_BASELINE, 2, NOT_ROW_DIMENSION)
{}

ObExprSpmDropBaseline::~ObExprSpmDropBaseline()
{}

int ObExprSpmDropBaseline::calc_result2(
    common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_ctx);
  int64_t tmp_res = 0;
  ObBasicSessionInfo* session = NULL;
  if (OB_ISNULL(session = expr_ctx.my_session_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(session), K(ret));
  }
  obrpc::ObDropPlanBaselineArg drop_baseline_arg;
  if (OB_SUCC(ret)) {
    drop_baseline_arg.tenant_id_ = session->get_effective_tenant_id();
    drop_baseline_arg.exec_tenant_id_ = session->get_effective_tenant_id();
    if ((obj1.is_null() && obj2.is_null()) || (!obj1.is_null() && !obj1.is_varchar_or_char()) ||
        (!obj2.is_null() && !obj2.is_integer_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(obj1), K(obj2));
    } else {
      if (!obj1.is_null()) {
        if (OB_FAIL(obj1.get_string(drop_baseline_arg.sql_id_))) {
          LOG_WARN("fail to get sql_id");
        }
      }
      if (OB_SUCC(ret) && !obj2.is_null()) {
        if (OB_FAIL(obj2.get_uint64(drop_baseline_arg.plan_hash_value_))) {
          LOG_WARN("fail to get plan_hash_value", K(obj2), K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(GCTX.rs_rpc_proxy_->drop_plan_baseline(drop_baseline_arg))) {
      LOG_WARN("create plan baseline failed", K(ret), K(drop_baseline_arg));
    }
  }

  result.set_int(tmp_res);
  return ret;
}

int ObExprSpmDropBaseline::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
{
  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObUInt64Type);
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type.set_int();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
