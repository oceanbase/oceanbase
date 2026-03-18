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

#include "sql/engine/expr/ob_expr_get_routine_param_type_str.h"
#include "pl/ob_pl_stmt.h"
#include "pl/ob_pl_type.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_package_guard.h"
#include "share/ob_server_struct.h"
#include "common/object/ob_obj_type.h"
#include "sql/ob_spi.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObExprGetRoutineParamTypeStr::ObExprGetRoutineParamTypeStr(ObIAllocator &alloc)
  : ObExprOperator(alloc, T_FUN_SYS_GET_ROUTINE_PARAM_TYPE_STR,
                  N_GET_ROUTINE_PARAM_TYPE_STR, 2, NOT_VALID_FOR_GENERATED_COL,
                  NOT_ROW_DIMENSION, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprGetRoutineParamTypeStr::~ObExprGetRoutineParamTypeStr()
{
}

int ObExprGetRoutineParamTypeStr::calc_result_type2(ObExprResType &type,
                                                            ObExprResType &type1,
                                                            ObExprResType &type2,
                                                            ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type1.set_calc_type(ObIntType);
  type2.set_calc_type(ObIntType);
  type.set_type(ObVarcharType);
  type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  type.set_collation_level(common::CS_LEVEL_IMPLICIT);
  type.set_length(static_cast<common::ObLength>(OB_MAX_SYS_PARAM_NAME_LENGTH));
  return ret;
}

int ObExprGetRoutineParamTypeStr::get_subtype_to_base_type(ObEvalCtx &ctx,
                                                           pl::ObPLPackageGuard &package_guard,
                                                           pl::ObPLDataType &dst_pl_type)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_PL
  const pl::ObUserDefinedType *user_type = NULL;
  const pl::ObUserDefinedSubType *sub_type = NULL;
  pl::ObPLResolveCtx resolve_ctx(ctx.get_expr_res_alloc(),
                                  *ctx.exec_ctx_.get_my_session(),
                                  *ctx.exec_ctx_.get_sql_ctx()->schema_guard_,
                                  package_guard,
                                  *GCTX.sql_proxy_,
                                  false);
  OZ (resolve_ctx.get_user_type(dst_pl_type.get_user_type_id(), user_type, &ctx.get_expr_res_alloc()));
  CK (OB_NOT_NULL(sub_type = static_cast<const pl::ObUserDefinedSubType *>(user_type)));
  if (OB_SUCC(ret) && common::is_dblink_type_id(sub_type->get_user_type_id())) {
    if (!user_type->is_collection_type() && !user_type->is_record_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "composite types other than collection type and record type are");
    }
  }
  OX (dst_pl_type = *(sub_type->get_base_type()));
#endif
  return ret;
}

int ObExprGetRoutineParamTypeStr::eval_routine_param_type_str(const ObExpr &expr,
                                                              ObEvalCtx &ctx,
                                                              ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(res_datum);
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("GET_ROUTINE_PARAM_TYPE_STR only support in oracle mode", K(ret));
#else
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    const share::schema::ObRoutineInfo *routine_info = NULL;
    ObDatum &first_param = expr.locate_param_datum(ctx, 0);
    ObDatum &second_param = expr.locate_param_datum(ctx, 1);
    int64_t routine_id = first_param.get_int();
    int64_t param_pos = second_param.get_int() - 1;
    uint64_t tenant_id = pl::get_tenant_id_by_object_id(routine_id);
    CK (OB_NOT_NULL(ctx.exec_ctx_.get_my_session()));
    CK (OB_NOT_NULL(ctx.exec_ctx_.get_sql_ctx()));
    CK (OB_NOT_NULL(ctx.exec_ctx_.get_sql_ctx()->schema_guard_));
    OZ (ctx.exec_ctx_.get_sql_ctx()->schema_guard_->get_routine_info(
          tenant_id, routine_id, routine_info), routine_id);
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(OB_ISNULL(routine_info))) {
      // refresh schema and try again
      share::schema::ObSchemaGetterGuard schema_guard;
      CK (OB_NOT_NULL(GCTX.schema_service_));
      OZ (ObSPIService::force_refresh_schema(tenant_id));
      OZ (GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
      OZ (schema_guard.get_routine_info(tenant_id, routine_id, routine_info));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(routine_info)) {
      res_datum.set_string("EXT", 3);
    } else {
      share::schema::ObRoutineParam *param = NULL;
      OZ (routine_info->get_routine_param(param_pos, param));
      CK (OB_NOT_NULL(param));
      if (OB_SUCC(ret)) {
        char *type_str = static_cast<char *>(ctx.get_expr_res_alloc().alloc(OB_MAX_SYS_PARAM_NAME_LENGTH));
        if (OB_ISNULL(type_str)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory for type str failed", K(ret));
        } else {
          bzero(type_str, OB_MAX_SYS_PARAM_NAME_LENGTH);
          int64_t type_pos = 0;
          pl::ObPLDataType dst_pl_type;
          pl::ObPLEnumSetCtx enum_set_ctx(ctx.get_expr_res_alloc());
          pl::ObPLPackageGuard package_guard(ctx.exec_ctx_.get_my_session()->get_effective_tenant_id());
          if (OB_SUCC(ret) && !package_guard.is_inited()) {
            OZ (package_guard.init());
          }
          CK (param->is_extern_type());
          CK (OB_NOT_NULL(GCTX.sql_proxy_));
          OX (dst_pl_type.set_enum_set_ctx(&enum_set_ctx));
          OZ (pl::ObPLDataType::transform_from_iparam(param,
                                                    *ctx.exec_ctx_.get_sql_ctx()->schema_guard_,
                                                    *ctx.exec_ctx_.get_my_session(),
                                                    *GCTX.sql_proxy_,
                                                    dst_pl_type,
                                                    NULL,
                                                    &package_guard.dblink_guard_,
                                                    NULL));
          if (OB_SUCC(ret) && dst_pl_type.is_subtype()) {
            OZ (get_subtype_to_base_type(ctx, package_guard, dst_pl_type));
          }
          OZ (pl::ObPLDataType::to_type_str(dst_pl_type, type_str, OB_MAX_SYS_PARAM_NAME_LENGTH, type_pos));
          OX (res_datum.set_string(type_str, type_pos));
        }
      }
    }
  }
#endif
  return ret;
}

int ObExprGetRoutineParamTypeStr::cg_expr(ObExprCGCtx &op_cg_ctx,
                                                   const ObRawExpr &raw_expr,
                                                   ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprGetRoutineParamTypeStr::eval_routine_param_type_str;
  return OB_SUCCESS;
}

}
}
