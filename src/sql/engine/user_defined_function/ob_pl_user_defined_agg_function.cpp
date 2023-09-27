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
#include "ob_pl_user_defined_agg_function.h"
#include "sql/ob_spi.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_stmt.h"
#include "pl/ob_pl_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/expr/ob_datum_cast.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

int ObPlAggUdfFunction::pick_routine(ObSEArray<const ObIRoutineInfo *, 4> &routine_infos,
                                     const ObIRoutineInfo *&routine_info,
                                     ObIArray<ObExprResType> &param_type)
{
  int ret = OB_SUCCESS;
  routine_info = NULL;
  if (routine_infos.count() == 1) {
    routine_info = routine_infos.at(0);
  } else {
    ObSEArray<ObRawExpr *, 4> mock_exec_expr;
    ObRawExprFactory *expr_factory = exec_ctx_->get_expr_factory();
    CK (OB_NOT_NULL(expr_factory));
    CK (OB_NOT_NULL(allocator_));
    CK (OB_NOT_NULL(session_info_));
    CK (OB_NOT_NULL(exec_ctx_->get_sql_ctx()));
    CK (OB_NOT_NULL(exec_ctx_->get_sql_ctx()->schema_guard_));
    CK (OB_NOT_NULL(exec_ctx_->get_sql_proxy()));
    for (int64_t i = 0; OB_SUCC(ret) && i < param_type.count(); ++i) {
      ObConstRawExpr *c_expr = NULL;
      OZ (expr_factory->create_raw_expr(T_QUESTIONMARK, c_expr));
      OX (c_expr->set_result_type(param_type.at(i)));
      OZ (mock_exec_expr.push_back(c_expr));
    }
    if (OB_SUCC(ret)) {
      pl::ObPLPackageGuard package_guard(session_info_->get_effective_tenant_id());
      pl::ObPLResolveCtx resolve_ctx(*allocator_,
                                  *session_info_,
                                  *exec_ctx_->get_sql_ctx()->schema_guard_,
                                  package_guard,
                                  *exec_ctx_->get_sql_proxy(),
                                  false /*is_ps*/);
      OZ (ObResolverUtils::pick_routine(resolve_ctx, mock_exec_expr, routine_infos, routine_info));
    }
  }
  return ret;
}

int ObPlAggUdfFunction::get_package_routine_info(const ObString &routine_name,
                                                 const ObRoutineInfo *&routine_info,
                                                 ObIArray<ObExprResType> &param_type)
{
  int ret = OB_SUCCESS;
  ObSqlCtx *sql_ctx = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  ObSEArray<const ObIRoutineInfo *, 4> routine_infos;
  ObRoutineType routine_type = share::schema::ObRoutineType::ROUTINE_FUNCTION_TYPE;
  routine_info = NULL;
  const ObIRoutineInfo *base_routine_info = NULL;
  const ObUDTTypeInfo *udt_info = NULL;
  if (OB_ISNULL(session_info_) || OB_ISNULL(exec_ctx_) ||
      OB_ISNULL(sql_ctx = exec_ctx_->get_sql_ctx()) ||
      OB_ISNULL(schema_guard = sql_ctx->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session_info_), K(exec_ctx_), K(sql_ctx),
                                    K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_udt_info(session_info_->get_effective_tenant_id(), type_id_, udt_info))) {
    LOG_WARN("get udt info failed", K(ret));
  } else if (OB_ISNULL(udt_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udt info is NULL", K(ret));
  } else if (OB_FAIL(schema_guard->get_udt_routine_infos(session_info_->get_effective_tenant_id(),
                                                         udt_info->get_database_id(),
                                                         type_id_,
                                                         routine_name,
                                                         routine_type,
                                                         routine_infos))) {
    LOG_WARN("failed to get package routine infos", K(ret));
  } else if (OB_FAIL(pick_routine(routine_infos, base_routine_info, param_type))) {
    LOG_WARN("get unexpected error", K(routine_infos), K(base_routine_info), K(ret), K(type_id_), K(routine_name), K(routine_type));
  } else if (OB_ISNULL(base_routine_info) ||
             OB_ISNULL(routine_info = static_cast<const ObRoutineInfo *>(base_routine_info))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(routine_infos), K(routine_info), K(ret));
  } else {
    LOG_TRACE("succeed to get package routine info", K(routine_name), K(*routine_info));
  }
  return ret;
}

int ObPlAggUdfFunction::init(ObSQLSessionInfo *session_info,
                             ObIAllocator *allocator,
                             ObExecContext *exec_ctx,
                             uint64_t type_id,
                             const ObIArray<ObExprResType> &params_type,
                             const ObExprResType result_type,
                             ObObjParam &pl_obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info) || OB_ISNULL(allocator) || OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session_info), K(allocator), K(exec_ctx), K(ret));
  } else if (OB_FAIL(params_type_.assign(params_type))) {
    LOG_WARN("failed to assign params type", K(ret));
  } else {
    session_info_ = session_info;
    allocator_ = allocator;
    exec_ctx_ = exec_ctx;
    type_id_ = type_id;
    result_type_ = result_type;
    if (OB_FAIL(process_init_pl_agg_udf(pl_obj))) {
      LOG_WARN("failed process init pl agg udf", K(ret));
    } else {
      LOG_TRACE("succeed to process init", K(type_id), K(params_type), K(result_type), K(pl_obj));
    }
  }
  return ret;
}

int ObPlAggUdfFunction::call_pl_engine_exectue_udf(ParamStore& udf_params,
                                                   const ObRoutineInfo *routine_info,
                                                   ObObj &result)
{
  int ret = OB_SUCCESS;
  pl::ObPL *pl_engine = NULL;
  ObSEArray<int64_t, 8> empty_subprogram_path;
  ObSEArray<int64_t, 8> empty_nocopy_params;
  uint64_t loc = 0;
  if (OB_ISNULL(routine_info) || OB_ISNULL(session_info_) ||
      OB_ISNULL(pl_engine = session_info_->get_pl_engine()) || OB_ISNULL(allocator_) ||
      OB_ISNULL(exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(pl_engine), K(allocator_), K(exec_ctx_),
                                    K(session_info_), K(routine_info), K(ret));
  } else if (udf_params.count() != routine_info->get_param_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udf parameter number is not equal to params desc count",
                                 K(ret), K(udf_params.count()), K(routine_info->get_param_count()));
  } else if (OB_FAIL(pl_engine->execute(*exec_ctx_,
                                        exec_ctx_->get_allocator(),
                                        -1,
                                        routine_info->get_routine_id(),
                                        empty_subprogram_path,
                                        udf_params,
                                        empty_nocopy_params,
                                        result,
                                        NULL,
                                        false,
                                        true,
                                        loc))) {
    //Compatible with oracle udf call, if udf is called in sql (or pl of sql), the error code
    //OB_READ_NOTHING will be overwritten with OB_SUCCESS
    if (OB_READ_NOTHING == ret && lib::is_oracle_mode()) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to execute pl engine", K(type_id_), K(routine_info->get_routine_id()),
                                              K(result), K(ret));
    }
  }
  if (OB_SUCC(ret)) {//check return value validity ==> return ODCICONST.Success ==> 0
    number::ObNumber num_res;
    int64_t dst_val = 0;
    if (OB_FAIL(result.get_number(num_res))) {
      LOG_WARN("failed to get number", K(ret));
    } else if (num_res == dst_val) {
      LOG_TRACE("succeed to call pl engine exectue udf", K(udf_params), K(result));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get invalid argument for udf return", K(ret));
    }
    if (OB_FAIL(ret)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ODCIAggregateInitialize/ODCIAggregateIterate/"\
                               "ODCIAggregateTerminate/ODCIAggregateMerge functions return result");
    }
  }
  return ret;
}

int ObPlAggUdfFunction::build_in_params_store(ObObjParam &pl_obj,
                                              bool is_out_param,
                                              const ObObj *obj_params,
                                              int64_t param_num,
                                              ObIArray<ObUDFParamDesc> &params_desc,
                                              ObIArray<ObExprResType> &params_type,
                                              ParamStore *&udf_params)
{
  int ret = OB_SUCCESS;
  udf_params = NULL;
  void *param_store_buf = NULL;
  if (OB_ISNULL(allocator_) ||
      (OB_ISNULL(obj_params) && param_num > 0) ||
      (OB_NOT_NULL(obj_params) && param_num < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(allocator_), K(obj_params), K(param_num), K(ret));
  } else if (param_num > 0 && OB_FAIL(check_types(obj_params, param_num, params_type))) {
    LOG_WARN("failed to check types", K(ret));
  } else if (OB_ISNULL(param_store_buf = allocator_->alloc(sizeof(ParamStore)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    udf_params = new(param_store_buf)ParamStore(ObWrapperAllocator(*allocator_));
    ObObjParam param;
    param.reset();
    if (is_out_param) {
      param.set_extend(pl_obj.get_ext(),
                       pl_obj.get_meta().get_extend_type(), pl_obj.get_val_len());
      param.set_param_meta();
    } else {
      pl_obj.copy_value_or_obj(param, true);
    }
    if (OB_FAIL(udf_params->push_back(param))) {
      LOG_WARN("failed to push back param", K(ret));
    } else if (obj_params != NULL &&
               OB_FAIL(ObExprUDF::process_in_params(obj_params, param_num, params_desc,
                                                    params_type_, *udf_params, *allocator_))) {
      LOG_WARN("failed to process in params", K(ret));
    } else {
      LOG_TRACE("succeed to build in params store", K(pl_obj), K(obj_params), K(params_desc),
                                                    K(params_type), K(udf_params));
    }
  }
  return ret;
}

int ObPlAggUdfFunction::process_init_pl_agg_udf(ObObjParam &pl_obj)
{
  int ret = OB_SUCCESS;
  const ObRoutineInfo *routine_info = NULL;
  const char *str = "ODCIAGGREGATEINITIALIZE"; //for pl agg udf, the type function name is stable
  ObString routine_name(strlen(str), str);
  ObObj tmp_result;
  ParamStore *udf_params = NULL;
  if (OB_ISNULL(exec_ctx_) || OB_ISNULL(exec_ctx_->get_sql_ctx()) ||
      OB_ISNULL(exec_ctx_->get_sql_ctx()->schema_guard_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(exec_ctx_), K(allocator_));
  } else {
    ObSEArray<ObUDFParamDesc, 5> params_desc;
    ObSEArray<ObExprResType, 5> params_type;
    pl::ObPLUDTNS ns(*exec_ctx_->get_sql_ctx()->schema_guard_);
    pl::ObPLDataType pl_type;
    pl_type.set_user_type_id(pl::PL_RECORD_TYPE, type_id_);
    pl_type.set_type_from(pl::PL_TYPE_UDT);
    if (OB_FAIL(ns.init_complex_obj(*allocator_, pl_type, pl_obj, false))) {
      LOG_WARN("failed to init complex obj", K(ret));
    } else if (OB_FAIL(build_in_params_store(pl_obj, true, NULL, 0, params_desc,
                                             params_type, udf_params))) {
      LOG_WARN("failed to build in params store", K(ret));
    } else if (OB_ISNULL(udf_params)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      //for pl agg udf, type member ODCIAggregateInitialize() must only have one param, and the
      //param is self. So, we can stable type(ObExtendType) and position(0, true) ==> IN OUT
      //see url:https://docs.oracle.com/cd/B28359_01/appdev.111/b28425/ext_agg_ref.htm#CACBJHHI
      common::ObArenaAllocator alloc;
      ObExprResType param_type(alloc);
      param_type.set_ext();
      param_type.set_udt_id(type_id_);
      if (OB_FAIL(params_type.push_back(param_type))) {
        LOG_WARN("failed to push back type", K(ret));
      } else if (OB_FAIL(params_desc.push_back(
          ObUDFParamDesc(ObUDFParamDesc::OutType::LOCAL_OUT, 0)))) {
        LOG_WARN("failed to push back param desc", K(ret));
      } else if (OB_FAIL(get_package_routine_info(routine_name, routine_info, params_type))) {
        LOG_WARN("failed to get package routine info", K(ret));
      } else if (OB_FAIL(call_pl_engine_exectue_udf(*udf_params, routine_info, tmp_result))) {
        LOG_WARN("failed to call pl engine exectue udf", K(ret));
      } else if (OB_UNLIKELY(udf_params->count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(udf_params->count()));
      } else {
        udf_params->at(0).copy_value_or_obj(pl_obj, true);
        LOG_TRACE("succeed to process init pl agg udf", K(pl_obj));
      }
    }
  }
  return ret;
}

int ObPlAggUdfFunction::process_calc_pl_agg_udf(ObObjParam &pl_obj,
                                                const ObObj *obj_params,
                                                int64_t param_num)
{
  int ret = OB_SUCCESS;
  const ObRoutineInfo *routine_info = NULL;
  const char *str = "ODCIAGGREGATEITERATE"; //for pl agg udf, the type function name is stable
  ObString routine_name(strlen(str), str);
  ParamStore *udf_params = NULL;
  ObObj tmp_result;
  bool is_null_params = false;
  if (OB_ISNULL(obj_params) || OB_UNLIKELY(param_num != params_type_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(param_num), K(params_type_.count()), K(obj_params), K(ret));
  } else if (OB_FAIL(check_params_validty(obj_params, param_num, is_null_params))) {
    LOG_WARN("failed to check params validty", K(ret));
  } else if (is_null_params) {
    /*no nothing*/
  } else if (OB_FAIL(process_obj_params(const_cast<ObObj*>(obj_params), param_num))) {
    LOG_WARN("failed to process obj params", K(ret));
  } else {
    ObSEArray<ObUDFParamDesc, 4> params_desc;
    ObSEArray<ObUDFParamDesc, 4> all_params_desc;
    ObSEArray<ObExprResType, 4> params_type;
    ObSEArray<ObExprResType, 4> all_params_type;
    //for pl agg udf, type member ODCIAggregateIterate() the first param must be self and is IN OUT,
    //the other param is IN, so we need rebuild relation infos.
    //see oracle url:https://docs.oracle.com/cd/B28359_01/appdev.111/b28425/ext_agg_ref.htm#CACBJHHI
    common::ObArenaAllocator alloc;
    ObExprResType param_type(alloc);
    param_type.set_ext();
    param_type.set_udt_id(type_id_);
    if (OB_FAIL(all_params_type.push_back(param_type))) {
      LOG_WARN("failed to push back type", K(ret));
    } else if (OB_FAIL(all_params_desc.push_back(
        ObUDFParamDesc(ObUDFParamDesc::LOCAL_OUT, 0)))) {
      LOG_WARN("failed to push back param desc", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < params_type_.count(); ++i) {
        if (OB_FAIL(params_type.push_back(params_type_.at(i)))) {
          LOG_WARN("failed to push back type", K(ret));
        } else if (OB_FAIL(all_params_type.push_back(params_type_.at(i)))) {
          LOG_WARN("failed to push back type", K(ret));
        } else if (OB_FAIL(params_desc.push_back(ObUDFParamDesc()))) {
          LOG_WARN("failed to push back params desc", K(ret));
        } else if (OB_FAIL(all_params_desc.push_back(ObUDFParamDesc()))) {
          LOG_WARN("failed to push back params desc", K(ret));
        } else {/*do nothing*/}
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(build_in_params_store(pl_obj, true, obj_params, param_num, params_desc,
                                          params_type, udf_params))) {
          LOG_WARN("failed to build in params store", K(ret));
        } else if (OB_ISNULL(udf_params)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(get_package_routine_info(routine_name, routine_info, all_params_type))) {
          LOG_WARN("failed to get package routine info", K(ret));
        } else if (OB_FAIL(call_pl_engine_exectue_udf(*udf_params, routine_info, tmp_result))) {
          LOG_WARN("failed to call pl engine exectue udf", K(ret));
        } else if (OB_UNLIKELY(udf_params->count() < 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(udf_params->count()));
        } else {
          udf_params->at(0).copy_value_or_obj(pl_obj, true);
          LOG_TRACE("Succeed to process calc pl agg udf", K(pl_obj), K(tmp_result));
        }
      }
    }
  }
  return ret;
}

int ObPlAggUdfFunction::process_obj_params(ObObj *obj_params,
                                           int64_t param_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj_params) || OB_ISNULL(allocator_) || OB_UNLIKELY(param_num <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(obj_params), K(param_num), K(allocator_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
      ObObj target_obj;
      if (ob_is_lob_tc(obj_params[i].get_type())) {
        if (OB_FAIL(ob_write_obj(*allocator_, obj_params[i], target_obj))) {
          LOG_WARN("failed to write obj", K(ret));
        } else {
          obj_params[i] = target_obj;
        }
      }
    }
  }
  return ret;
}

int ObPlAggUdfFunction::process_merge_pl_agg_udf(ObObjParam &pl_obj,
                                                 ObObjParam &pl_obj2)
{
  int ret = OB_SUCCESS;
  const ObRoutineInfo *routine_info = NULL;
  const char *str = "ODCIAGGREGATEMERGE"; //for pl agg udf, the type function name is stable
  ObString routine_name(strlen(str), str);
  ParamStore *udf_params = NULL;
  ObObj tmp_result;

  //for pl agg udf, type member ODCIAggregateMerge() the first param must be self and is IN OUT,
  //the second param is IN. so we need rebuild relation infos.
  //see oracle url:https://docs.oracle.com/cd/B28359_01/appdev.111/b28425/ext_agg_ref.htm#CACBJHHI
  ObSEArray<ObExprResType, 4> params_type;
  ObSEArray<ObUDFParamDesc, 4> params_desc;
  ObSEArray<ObUDFParamDesc, 4> all_params_desc;
  ObSEArray<ObExprResType, 4> all_params_type;
  common::ObArenaAllocator alloc;
  ObExprResType param_type(alloc);
  param_type.set_ext();
  param_type.set_udt_id(type_id_);
  if (OB_FAIL(params_type.push_back(param_type))) {
    LOG_WARN("failed to push back type", K(ret));
  } else if (OB_FAIL(params_desc.push_back(ObUDFParamDesc()))) {
    LOG_WARN("failed to push back param desc", K(ret));
  } else if (OB_FAIL(build_in_params_store(pl_obj, true, &pl_obj2, 1, params_desc,
                                            params_type, udf_params))) {
    LOG_WARN("failed to build in params store", K(ret));
  } else if (OB_ISNULL(udf_params)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(all_params_type.push_back(param_type))) {
    LOG_WARN("failed to push back type", K(ret));
  } else if (OB_FAIL(all_params_desc.push_back(
      ObUDFParamDesc(ObUDFParamDesc::LOCAL_OUT, 0)))) {
    LOG_WARN("failed to push back param desc", K(ret));
  } else if (OB_FAIL(all_params_type.push_back(param_type))) {
    LOG_WARN("failed to push back type", K(ret));
  } else if (OB_FAIL(all_params_desc.push_back(ObUDFParamDesc()))) {
    LOG_WARN("failed to push back param desc", K(ret));
  } else if (OB_FAIL(get_package_routine_info(routine_name, routine_info, all_params_type))) {
    LOG_WARN("failed to get package routine info", K(ret));
  } else if (OB_FAIL(call_pl_engine_exectue_udf(*udf_params, routine_info, tmp_result))) {
    LOG_WARN("failed to call pl engine exectue udf", K(ret));
  } else if (OB_UNLIKELY(udf_params->count() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(udf_params->count()));
  } else {
    udf_params->at(0).copy_value_or_obj(pl_obj, true);
    LOG_TRACE("Succeed to process merge pl agg udf", K(pl_obj), K(tmp_result));
  }

  return ret;
}

int ObPlAggUdfFunction::process_get_pl_agg_udf_result(ObObjParam &pl_obj,
                                                      ObObj &result)
{
  int ret = OB_SUCCESS;
  const ObRoutineInfo *routine_info = NULL;
  const char *str = "ODCIAGGREGATETERMINATE"; //for pl agg udf, the type function name is stable
  ObString routine_name(strlen(str), str);
  ParamStore *udf_params = NULL;
  ObObj tmp_result;

  //for pl agg udf, type member ODCIAggregateTerminate() the first param must be self and is IN,
  //the second param is OUT, the third param is number. so we need rebuild relation infos.
  //see oracle url:https://docs.oracle.com/cd/B28359_01/appdev.111/b28425/ext_agg_ref.htm#CACBJHHI
  ObSEArray<ObUDFParamDesc, 4> params_desc;
  ObSEArray<ObUDFParamDesc, 4> all_params_desc;
  ObSEArray<ObExprResType, 4> params_type;
  ObSEArray<ObExprResType, 4> all_params_type;
  result.set_meta_type(result_type_);
  if (OB_FAIL(params_type.push_back(result_type_))) {
    LOG_WARN("failed to push back type", K(ret));
  } else if (OB_FAIL(params_desc.push_back(
      ObUDFParamDesc(ObUDFParamDesc::LOCAL_OUT, 0)))) {
    LOG_WARN("failed to push back param desc", K(ret));
  } else if (OB_FAIL(build_in_params_store(pl_obj, false, &result, 1, params_desc,
                                            params_type, udf_params))) {
    LOG_WARN("failed to build in params store", K(ret));
  } else if (OB_ISNULL(udf_params)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    common::ObArenaAllocator alloc;
    ObExprResType param_type(alloc);
    ObExprResType flags_type(alloc);
    param_type.set_ext();
    param_type.set_udt_id(type_id_);
    flags_type.set_number();
    number::ObNumber num;
    ObObj number;
    number.set_number(ObNumberType, num);
    ObObjParam param;
    param.reset();
    number.copy_value_or_obj(param, true);
    if (OB_FAIL(udf_params->push_back(param))) {
      LOG_WARN("failed to push back obj param");
    } else if (OB_FAIL(all_params_type.push_back(param_type))) {
      LOG_WARN("failed to push back type", K(ret));
    } else if (OB_FAIL(all_params_desc.push_back(ObUDFParamDesc()))) {
      LOG_WARN("failed to push back param desc", K(ret));
    } else if (OB_FAIL(all_params_type.push_back(result_type_))) {
      LOG_WARN("failed to push back type", K(ret));
    } else if (OB_FAIL(all_params_desc.push_back(
        ObUDFParamDesc(ObUDFParamDesc::LOCAL_OUT, 1)))) {
      LOG_WARN("failed to push back param desc", K(ret));
    } else if (OB_FAIL(all_params_type.push_back(flags_type))) {
      LOG_WARN("failed to push back type", K(ret));
    } else if (OB_FAIL(all_params_desc.push_back(ObUDFParamDesc()))) {
      LOG_WARN("failed to push back param desc", K(ret));
    } else if (OB_FAIL(get_package_routine_info(routine_name, routine_info, all_params_type))) {
      LOG_WARN("failed to get package routine info", K(ret));
    } else if (OB_FAIL(call_pl_engine_exectue_udf(*udf_params, routine_info, tmp_result))) {
      LOG_WARN("failed to call pl engine exectue udf", K(ret));
    } else if (OB_UNLIKELY(udf_params->count() < 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(udf_params->count()));
    } else if (OB_FAIL(ObDatumCast::check_can_cast(udf_params->at(1).get_type(),
                                            udf_params->at(1).get_collation_type(),
                                            result_type_.get_type(),
                                            result_type_.get_collation_type()))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      ObString src_type = ob_obj_type_str(udf_params->at(1).get_type());
      ObString dts_type = ob_obj_type_str(result_type_.get_type());
      if (udf_params->at(1).is_blob() || udf_params->at(1).is_blob_locator()) {
        src_type = ObString("BLOB");
      } else if (udf_params->at(1).is_clob() || udf_params->at(1).is_clob_locator()) {
        src_type = ObString("CLOB");
      }
      if (result_type_.is_blob() || result_type_.is_blob_locator()) {
        dts_type = ObString("BLOB");
      } else if (result_type_.is_clob() || result_type_.is_clob_locator()) {
        dts_type = ObString("CLOB");
      }
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP,
                    src_type.ptr(),
                    dts_type.ptr());
      LOG_WARN("cast to expected type not supported",
                K(ret),
                K(udf_params->at(1)),
                K(result_type_));
    } else if (!ob_is_lob_tc(result_type_.get_type())) {
      ObObj src_obj;
      udf_params->at(1).copy_value_or_obj(src_obj, true);
      ObCastMode cast_mode = CM_NONE;
      if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session_info_, cast_mode))) {
        LOG_WARN("failed to get default cast mode", K(ret));
      } else {
        ObCastCtx cast_ctx(allocator_, NULL, cast_mode, ObCharset::get_system_collation(), NULL);
        if (OB_FAIL(ObObjCaster::to_type(result_type_.get_type(), cast_ctx, src_obj, result))) {
          LOG_WARN("failed to cast type", K(ret));
        } else {
          LOG_TRACE("succeed to process get pl agg udf result", K(src_obj), K(result));
        }
      }
    } else {
      udf_params->at(1).copy_value_or_obj(result, true);
      LOG_TRACE("succeed to process get pl agg udf result", K(result));
    }
  }

  return ret;
}

int ObPlAggUdfFunction::check_types(const ObObj *obj_params,
                                    int64_t param_num,
                                    ObIArray<ObExprResType> &params_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj_params) || OB_UNLIKELY(param_num < 1 || param_num != params_type.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(obj_params), K(param_num), K(params_type.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
      if (!obj_params[i].is_null()) {
        TYPE_CHECK(obj_params[i], params_type.at(i).get_type());
      }
    }
  }
  return ret;
}

int ObPlAggUdfFunction::check_params_validty(const ObObj *obj_params,
                                             int64_t param_num,
                                             bool &is_null_params)
{
  int ret = OB_SUCCESS;
  is_null_params = false;
  if (OB_UNLIKELY(param_num <= 0) || OB_ISNULL(obj_params)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(param_num), K(obj_params));
  } else {
    is_null_params = true;
    for (int64_t i = 0; is_null_params && i < param_num; ++i) {
      if (obj_params[i].is_null()) {
        /*do nothing*/
      } else {
        is_null_params = false;
      }
    }
  }
  return ret;
}

}
}
