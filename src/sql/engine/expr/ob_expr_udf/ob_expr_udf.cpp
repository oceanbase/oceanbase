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

#include "ob_expr_udf.h"
#include "observer/ob_server.h"
#include "pl/ob_pl_stmt.h"
#include "ob_udf_result_cache.h"
#include "pl/external_routine/ob_java_udf.h"
#include "pl/external_routine/ob_py_udf.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObExprUDF, ObFuncExprOperator),
                     udf_id_, result_type_, params_type_,
                     udf_package_id_, params_desc_, is_udt_udf_,
                     nocopy_params_, subprogram_path_, call_in_sql_, loc_, is_udt_cons_, out_params_type_);

ObExprUDF::ObExprUDF(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_UDF, N_UDF, PARAM_NUM_UNKNOWN, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE),
      udf_id_(OB_INVALID_ID),
      udf_package_id_(OB_INVALID_ID),
      subprogram_path_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc)),
      result_type_(),
      params_type_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc)),
      params_desc_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc)),
      nocopy_params_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc)),
      is_udt_udf_(false),
      call_in_sql_(true),
      loc_(0),
      is_udt_cons_(false),
      out_params_type_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc)) {}

void ObExprUDF::reset()
{
  udf_id_ = common::OB_INVALID_ID;
  udf_package_id_ = common::OB_INVALID_ID;
  subprogram_path_.reset();
  result_type_.reset();
  params_type_.reset();
  out_params_type_.reset();
  params_desc_.reset();
  nocopy_params_.reset();
  is_udt_udf_ = false;
  call_in_sql_ = true;
  loc_ = 0;
  is_udt_cons_ = false;
  ObFuncExprOperator::reset();
}

int ObExprUDF::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  const ObExprUDF *tmp_other = dynamic_cast<const ObExprUDF*>(&other);
  if (OB_UNLIKELY(OB_ISNULL(tmp_other))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cast failed, type of argument is wrong", K(ret), K(other));
  } else if (OB_FAIL(subprogram_path_.assign(tmp_other->get_subprogram_path()))) {
    LOG_WARN("failed to assign subprogram path", K(ret));
  } else if (OB_FAIL(result_type_.assign(tmp_other->get_result_type()))) {
    LOG_WARN("failed to assign result type", K(ret));
  } else if (OB_FAIL(params_type_.assign(tmp_other->get_params_type()))) {
    LOG_WARN("failed to assign params type", K(ret));
  } else if (OB_FAIL(params_desc_.assign(tmp_other->get_params_desc()))) {
    LOG_WARN("failed to assign params desc", K(ret));
  } else if (OB_FAIL(nocopy_params_.assign(tmp_other->get_nocopy_params()))) {
    LOG_WARN("failed to assign nocopy params", K(ret));
  } else if (OB_FAIL(out_params_type_.assign(tmp_other->get_out_params_type()))) {
    LOG_WARN("failed to assign out params type", K(ret));
  } else if (OB_FAIL(ObExprOperator::assign(other))) {
    LOG_WARN("failed to ObExprOperator::assign", K(ret));
  } else {
    udf_id_ = tmp_other->get_udf_id();
    udf_package_id_ = tmp_other->get_udf_package_id();
    is_udt_udf_ = tmp_other->get_is_udt_udf();
    // call_in_sql_ = tmp_other->get_call_in_sql();
    loc_ = tmp_other->get_loc();
    is_udt_cons_ = tmp_other->get_is_udt_cons();
  }
  return ret;
}


int ObExprUDF::calc_result_typeN(ObExprResType &type,
                                 ObExprResType *types,
                                 int64_t param_num,
                                 ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  CK (param_num == params_type_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
    if (params_desc_.at(i).is_out() && params_desc_.at(i).is_obj_access_out()) {
      ObObjMeta meta;
      meta.set_ext();
      types[i].set_calc_meta(meta);
    } else {
      if (udf_package_id_ == T_OBJ_XML
          && types[i].is_xml_sql_type() && params_type_.at(i).is_string_type()) {
        ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
        LOG_WARN("OBE-06553:PLS-306:wrong number or types of arguments in call procedure",
                 K(i), K(udf_package_id_), K(udf_id_), K(types[i]), K(params_type_.at(i)));
      } else {
        types[i].set_calc_accuracy(params_type_.at(i).get_accuracy());
        types[i].set_calc_meta(params_type_.at(i).get_obj_meta());
        if (params_type_.at(i).get_collation_type() == CS_TYPE_ANY) {
          if (types[i].is_string_or_lob_locator_type()) {
            types[i].set_calc_collation_type(types[i].get_collation_type());
            if (lib::is_oracle_mode() && types[i].get_calc_meta().is_clob()) {
              ObCollationType dest_collation = ob_is_nstring_type(types[i].get_calc_meta().get_type()) ?
                                                type_ctx.get_session()->get_nls_collation_nation()
                                              : type_ctx.get_session()->get_nls_collation();
              if (CS_TYPE_INVALID != dest_collation) {
                types[i].set_calc_collation_type(dest_collation);
              }
            }
          } else {
            types[i].set_calc_collation_type(type_ctx.get_session()->get_nls_collation());
          }
        } else if (types[i].is_enum_or_set()) {
          types[i].set_type(ObVarcharType);
          types[i].set_collation_level(params_type_.at(i).get_collation_level());
          types[i].set_collation_type(params_type_.at(i).get_collation_type());
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    type.set_accuracy(result_type_.get_accuracy());
    type.set_meta(result_type_.get_obj_meta());
    if (type.get_type() == ObRawType) {
      type.set_collation_level(CS_LEVEL_NUMERIC);
    } else if (type.is_string_or_lob_locator_type() && udf_package_id_ == T_OBJ_XML) {
      type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
    if (!type.is_ext()) {
      if (lib::is_oracle_mode()) {
        type.set_length(OB_MAX_ORACLE_VARCHAR_LENGTH);
      } else {
        type.set_length(result_type_.get_length());
      }
    }
  }
  if (OB_SUCC(ret) && lib::is_mysql_mode()) {
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() & ~CM_WARN_ON_FAIL);
  }
  return ret;
}

bool ObExprUDF::enable_eval_vector(ObExpr &expr)
{
  bool ret = true;
  ObObjType result_type = expr.datum_meta_.type_;
  // if in arg / result has ext type, do not set eval vector
  // later fix TODO:jiabokai.jbk
  if (ObExtendType == result_type || ObUserDefinedSQLType == result_type || ObCollectionSQLType == result_type) {
    ret = false;
  }
  for (int64_t i = 0; ret && i < expr.arg_cnt_; i++) {
    ObObjType param_type = expr.args_[i]->datum_meta_.type_;
    if (ObExtendType == param_type || ObUserDefinedSQLType == param_type || ObCollectionSQLType == param_type) {
      ret = false;
    }
  }
  return ret;
}

int ObExprUDF::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObIAllocator &alloc = *expr_cg_ctx.allocator_;
  const ObUDFRawExpr &fun_sys = static_cast<const ObUDFRawExpr &>(raw_expr);
  ObExprUDFInfo *info = OB_NEWx(ObExprUDFInfo, (&alloc), alloc, T_FUN_UDF);
  if (NULL == info) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(info->from_raw_expr(fun_sys))) {
    LOG_WARN("failed to init udf info from raw expr", K(ret));
  } else {
    info->is_called_in_sql_ = is_called_in_sql();
    rt_expr.extra_info_ = info;
    for (int64_t i = 0; !info->has_out_param_ && i < info->params_desc_.count(); ++i) {
      if (info->params_desc_.at(i).is_out()) {
        info->has_out_param_ = true;
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(expr_cg_ctx.schema_guard_)) {
      OX (info->is_deterministic_ = lib::is_oracle_mode() && info->is_deterministic_ && info->is_called_in_sql_);
      for (int64_t i = 0; OB_SUCC(ret) && info->is_deterministic_ && i < info->params_type_.count(); ++i) {
        if (ObExtendType == info->params_type_.at(i).get_type()) {
          info->is_deterministic_ = false;
          break;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (ObExternalRoutineType::INTERNAL_ROUTINE != fun_sys.get_external_routine_type()) {
    uint64_t routine_id = fun_sys.get_udf_id();
    uint64_t tenant_id = pl::get_tenant_id_by_object_id(routine_id);
    const ObRoutineInfo *routine_info = nullptr;

    if (OB_ISNULL(expr_cg_ctx.schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL schema_guard", K(ret));
    } else if (OB_FAIL(expr_cg_ctx.schema_guard_->get_routine_info(tenant_id, routine_id, routine_info))) {
      LOG_WARN("failed to get_routine_info", K(ret), K(tenant_id), K(routine_id), KPC(routine_info));
    } else if (OB_ISNULL(routine_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL routine_info", K(ret), K(tenant_id), K(routine_id));
    } else if (routine_info->get_external_routine_type() != fun_sys.get_external_routine_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected external_routine_type", K(ret), KPC(routine_info), K(fun_sys));
    } else if (OB_FAIL(ob_write_string(alloc, routine_info->get_external_routine_entry(), info->external_routine_entry_, true))) {
      LOG_WARN("failed to ob_write_string", K(ret), KPC(info), KPC(routine_info));
    } else if (OB_FAIL(ob_write_string(alloc, routine_info->get_external_routine_url(), info->external_routine_url_, true))) {
      LOG_WARN("failed to ob_write_string", K(ret), KPC(info), KPC(routine_info));
    } else if (OB_FAIL(ob_write_string(alloc, routine_info->get_external_routine_resource(), info->external_routine_resource_, true))) {
      LOG_WARN("failed to ob_write_string", K(ret), KPC(info), KPC(routine_info));
    } else if (FALSE_IT(info->external_routine_type_ = fun_sys.get_external_routine_type())) {
      // unreachable
    } else if (fun_sys.is_mysql_udtf()) {
      rt_expr.eval_func_ = eval_mysql_udtf;
    } else {
      rt_expr.eval_func_ = eval_external_udf;

      if (!info->is_py_udf() && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_4_1_0) {
        rt_expr.eval_vector_func_ = eval_external_udf_vector;
      }
    }
  } else {
    rt_expr.eval_func_ = eval_udf;
    if (OB_SUCC(ret) && is_called_in_sql() && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_4_0_0) {
      rt_expr.eval_batch_func_ = eval_udf_batch;
      if (enable_eval_vector(rt_expr)) {
        rt_expr.eval_vector_func_ = eval_udf_vector;
      } else {
        rt_expr.eval_vector_func_ = nullptr;
      }
      LOG_TRACE("UDF eval func : ",
                K(ret), KP(rt_expr.eval_batch_func_), KP(rt_expr.eval_vector_func_));
    }
  }

  return ret;
}

int ObExprUDFInfo::deep_copy(common::ObIAllocator &allocator,
                             const ObExprOperatorType type,
                             ObIExprExtraInfo *&copied_info) const
{
  int ret = common::OB_SUCCESS;
  ObExprUDFInfo *other = NULL;
  if (OB_FAIL(ObExprExtraInfoFactory::alloc(allocator, type, copied_info))) {
    LOG_WARN("failed to alloc udf info", K(ret), K(type));
  } else if (OB_ISNULL(other = static_cast<ObExprUDFInfo *>(copied_info))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected udf info ptr", K(ret));
  } else if (OB_FAIL(other->subprogram_path_.assign(subprogram_path_))) {
    LOG_WARN("failed to assign subprogram path", K(ret));
  } else if (OB_FAIL(other->result_type_.assign(result_type_))) {
    LOG_WARN("failed to assign result type", K(ret));
  } else if (OB_FAIL(other->params_type_.assign(params_type_))) {
    LOG_WARN("failed to assign params type", K(ret));
  } else if (OB_FAIL(other->params_desc_.assign(params_desc_))) {
    LOG_WARN("failed to assign params desc", K(ret));
  } else if (OB_FAIL(other->nocopy_params_.assign(nocopy_params_))) {
    LOG_WARN("failed to assign nocopy params", K(ret));
  } else if (OB_FAIL(other->out_params_type_.assign(out_params_type_))) {
    LOG_WARN("failed to assign out params type", K(ret));
  } else {
    other->udf_id_ = udf_id_;
    other->udf_package_id_ = udf_package_id_;
    other->is_udt_udf_ = is_udt_udf_;
    other->loc_ = loc_;
    other->is_udt_cons_ = is_udt_cons_;
    other->is_called_in_sql_ = is_called_in_sql_;
    other->dblink_id_ = dblink_id_;
    other->is_result_cache_ = is_result_cache_;
    other->is_deterministic_ = is_deterministic_;
    other->external_routine_type_ = external_routine_type_;
    other->is_mysql_udtf_ = is_mysql_udtf_;
    OZ(ob_write_string(allocator, external_routine_entry_, other->external_routine_entry_, true));
    OZ(ob_write_string(allocator, external_routine_url_, other->external_routine_url_, true));
    OZ(ob_write_string(allocator, external_routine_resource_, other->external_routine_resource_, true));
  }
  return ret;
}

OB_DEF_SERIALIZE(ObExprUDFInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              udf_id_,
              udf_package_id_,
              subprogram_path_,
              result_type_,
              params_type_,
              params_desc_,
              nocopy_params_,
              is_udt_udf_,
              loc_,
              is_udt_cons_,
              is_called_in_sql_,
              is_result_cache_,
              is_deterministic_,
              external_routine_type_,
              external_routine_entry_,
              external_routine_url_,
              external_routine_resource_,
              dblink_id_,
              is_mysql_udtf_,
              out_params_type_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExprUDFInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              udf_id_,
              udf_package_id_,
              subprogram_path_,
              result_type_,
              params_type_,
              params_desc_,
              nocopy_params_,
              is_udt_udf_,
              loc_,
              is_udt_cons_,
              is_called_in_sql_,
              is_result_cache_,
              is_deterministic_,
              external_routine_type_,
              external_routine_entry_,
              external_routine_url_,
              external_routine_resource_,
              dblink_id_,
              is_mysql_udtf_,
              out_params_type_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprUDFInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              udf_id_,
              udf_package_id_,
              subprogram_path_,
              result_type_,
              params_type_,
              params_desc_,
              nocopy_params_,
              is_udt_udf_,
              loc_,
              is_udt_cons_,
              is_called_in_sql_,
              is_result_cache_,
              is_deterministic_,
              external_routine_type_,
              external_routine_entry_,
              external_routine_url_,
              external_routine_resource_,
              dblink_id_,
              is_mysql_udtf_,
              out_params_type_);
  return len;
}

template <typename RE>
int ObExprUDFInfo::from_raw_expr(RE &raw_expr)
{
  int ret = OB_SUCCESS;
  // const ObUDFRawExpr &udf_expr = raw_expr;
  ObUDFRawExpr &udf_expr = const_cast<ObUDFRawExpr &>(static_cast<const ObUDFRawExpr&>(raw_expr));
  ObIArray<ObRawExprResType> &params_type = udf_expr.get_params_type();
  ObIArray<ObRawExprResType> &out_params_type = udf_expr.get_out_params_type();
  OZ (subprogram_path_.assign(udf_expr.get_subprogram_path()));
  OZ (params_type_.init(params_type.count()));
  OZ (out_params_type_.init(out_params_type.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < params_type.count(); ++i) {
    OZ (params_type_.push_back(params_type.at(i)));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < out_params_type.count(); ++i) {
    OZ (out_params_type_.push_back(out_params_type.at(i)));
  }
  OZ (params_desc_.assign(udf_expr.get_params_desc()));
  OZ (nocopy_params_.assign(udf_expr.get_nocopy_params()));
  OX (udf_id_ = udf_expr.get_udf_id());
  OX (udf_package_id_ = udf_expr.get_pkg_id());
  OX (result_type_ = udf_expr.get_result_type());
  OX (is_udt_udf_ = udf_expr.get_is_udt_udf());
  OX (loc_ = udf_expr.get_loc());
  OX (is_udt_cons_ = udf_expr.get_is_udt_cons());
  OX (dblink_id_ = udf_expr.get_dblink_id());
  OX (is_result_cache_ = udf_expr.is_result_cache());
  OX (is_deterministic_ = udf_expr.is_deterministic());
  OX (is_mysql_udtf_ = udf_expr.is_mysql_udtf());
  return ret;
}

int ObExprUDF::build_udf_ctx(const ObExpr &expr,
                             ObExecContext &exec_ctx,
                             ObExprUDFCtx *&udf_ctx)
{
  int ret = OB_SUCCESS;
  uint64_t udf_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  if (OB_ISNULL(udf_ctx = static_cast<ObExprUDFCtx *>(exec_ctx.get_expr_op_ctx(udf_ctx_id)))) {
    if (OB_FAIL(exec_ctx.create_expr_op_ctx(udf_ctx_id, udf_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret));
    } else if (OB_ISNULL(udf_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected udf ctx", K(ret), KP(udf_ctx));
    } else if (OB_FAIL(udf_ctx->init(expr, exec_ctx))) {
      LOG_WARN("failed to init udf ctx", K(ret));
    }
  } else if (OB_FAIL(udf_ctx->reuse(expr))) {
    LOG_WARN("failed to reuse udf ctx", K(ret));
  }
  return ret;
}

ObExprUDFEnvGuard::ObExprUDFEnvGuard(ObEvalCtx &ctx, ObExprUDFCtx &udf_ctx, int &ret, ObObj &tmp_result)
  : need_end_stmt_(false),
    cur_obj_count_(0),
    deep_in_objs_(),
    need_free_udt_(false),
    ctx_(ctx),
    udf_ctx_(udf_ctx),
    ret_(ret),
    tmp_result_(tmp_result)
{
  stmt::StmtType parent_stmt = ctx.exec_ctx_.get_sql_ctx()->stmt_type_;
  if (!udf_ctx_.get_session_info()->has_start_stmt()) {
    need_end_stmt_ = true;
    udf_ctx_.get_session_info()->set_start_stmt();
  }
  if (OB_NOT_NULL(ctx.exec_ctx_.get_pl_ctx())) {
    cur_obj_count_ = ctx_.exec_ctx_.get_pl_ctx()->get_objects().count();
  }
}

ObExprUDFEnvGuard::~ObExprUDFEnvGuard()
{
  int &ret = ret_;
  if (OB_FAIL(ret)) {
    udf_ctx_.reset_cacheobj_guard();
  }
  if (udf_ctx_.get_info()->is_udt_cons_ && udf_ctx_.get_param_store()->count() > 0) {
    int tmp = OB_SUCCESS;
    tmp = pl::ObUserDefinedType::destruct_obj(udf_ctx_.get_param_store()->at(0), ctx_.exec_ctx_.get_my_session());
    if (OB_SUCCESS != tmp) {
      LOG_WARN("fail to free udt self memory", K(ret_), K(tmp));
    }
  }
  if (deep_in_objs_.count() > 0) {
    int tmp = OB_SUCCESS;
    for (int64_t i = 0; i < deep_in_objs_.count(); ++i) {
      tmp = pl::ObUserDefinedType::destruct_obj(deep_in_objs_.at(i), ctx_.exec_ctx_.get_my_session());
      if (OB_SUCCESS != tmp) {
        LOG_WARN("fail to destruct obj of in param", K(ret_), K(tmp));
      }
    }
  }
  if (OB_FAIL(ret)
      && udf_ctx_.get_info()->is_called_in_sql_ && OB_NOT_NULL(ctx_.exec_ctx_.get_pl_ctx())) {
    ctx_.exec_ctx_.get_pl_ctx()->reset_obj_range_to_end(get_cur_obj_count());
  }
  // Release return value when pl eval failed,
  // if pl eval success, return value will released by process_return_value call.
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(ret) && (tmp_ret =
        pl::ObUserDefinedType::destruct_obj(tmp_result_, ctx_.exec_ctx_.get_my_session())) != OB_SUCCESS) {
    LOG_WARN("failed to destruct tmp result object", K(ret), K(tmp_ret));
  }
  if (need_end_stmt_) {
    udf_ctx_.get_session_info()->set_end_stmt();
  }
}


int ObExprUDF::eval_external_udf(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObObjMeta, 8> arg_types;
  using ColumnType = ObSEArray<ObObj, 1>;
  ObSEArray<ObIArray<ObObj>*, 8> args;
  ObArenaAllocator alloc;

  CK (OB_NOT_NULL(expr.extra_info_));

  if (OB_SUCC(ret)) {
    ObExprUDFInfo &udf_info = *static_cast<ObExprUDFInfo*>(expr.extra_info_);
    ObEvalCtx::TempAllocGuard memory_guard(ctx);

    if (OB_FAIL(SMART_CALL(expr.eval_param_value(ctx)))) {
      LOG_WARN("failed to eval_param_value", K(ret), K(expr));
    } else {
      CK (udf_info.params_type_.count() == expr.arg_cnt_);

      for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
        if (OB_ISNULL(expr.args_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL expr", K(ret), K(i), K(expr));
        } else if (OB_FAIL(arg_types.push_back(udf_info.params_type_.at(i)))) {
          LOG_WARN("failed to push_back arg datum meta", K(ret), K(i), K(expr));
        } else {
          const ObDatum &datum = expr.args_[i]->locate_expr_datum(ctx);
          ObObj obj;
          ColumnType *column = nullptr;

          if (OB_ISNULL(column = static_cast<ColumnType*>(alloc.alloc(sizeof(ColumnType))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory for column", K(ret), K(i));
          } else if (OB_ISNULL(column = new(column)ColumnType())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to construct column", K(ret));
          } else if (OB_FAIL(datum.to_obj(obj, expr.args_[i]->obj_meta_, expr.args_[i]->obj_datum_map_))) {
            LOG_WARN("failed to convert datum to obj", K(ret), K(datum), K(obj), K(i), K(expr.args_[i]->obj_meta_), K(expr.args_[i]->obj_datum_map_));
          } else if (OB_FAIL(column->push_back(obj))) {
            LOG_WARN("failed to push_back to column", K(ret), K(datum), K(obj), K(i));
          } else if (OB_FAIL(args.push_back(column))) {
            LOG_WARN("failed to push_back column to args", K(ret), K(i), KPC(column));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObSEArray<ObObj, 1> res_array;

      if (udf_info.is_py_udf()) {
        pl::ObPyUDFExecutor executor(ctx.exec_ctx_, udf_info);

        if (OB_FAIL(executor.init())) {
          LOG_WARN("failed to init python udf executor", K(ret));
        } else if (OB_FAIL(executor.execute(1, arg_types, args, alloc, res_array))) {
          LOG_WARN("failed to execute udf", K(ret));
        }
      } else {
        pl::ObJavaUDFExecutor executor(ctx.exec_ctx_, udf_info.external_routine_entry_);

        if (OB_FAIL(executor.init(udf_info.udf_id_,
                                  udf_info.external_routine_type_,
                                  udf_info.external_routine_url_,
                                  udf_info.external_routine_resource_))) {
          LOG_WARN("failed to init java udf executor", K(ret));
        } else if (OB_FAIL(executor.execute(1, "evaluate", arg_types, args, udf_info.result_type_, alloc, res_array))) {
          LOG_WARN("failed to execute udf", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (1 != res_array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected res_array count", K(ret), K(res_array));
      } else if (OB_FAIL(res.from_obj(res_array.at(0), expr.obj_datum_map_))) {
        LOG_WARN("failed to set res from result obobj", K(ret), K(res_array));
      } else if (OB_FAIL(expr.deep_copy_datum(ctx, res))) {
        LOG_WARN("failed to deep_copy_datum", K(ret), K(res_array), K(res));
      }
    }
  }

  for (int64_t i = 0; i < args.count(); ++i) {
    if (OB_NOT_NULL(args.at(i))) {
      args.at(i)->~ObIArray();
      args.at(i) = nullptr;
    }
  }
  args.reset();

  return ret;
}

int ObExprUDF::eval_external_udf_vector(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator alloc(ObMemAttr(MTL_ID(), GET_PL_MOD_STRING(pl::OB_PL_ARENA)));

  ObIVector *res_vec = expr.get_vector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  ObSEArray<ObObjMeta, 8> arg_types;
  using ColumnType = ObSEArray<ObObj, 256>;
  ObSEArray<ObIArray<ObObj>*, 8> args;
  ObArray<int64_t> vec_indices;
  ObIVector **arg_vec = nullptr;
  ObObj *row = nullptr;

  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(bound.end() - bound.start());

  if (OB_ISNULL(expr.extra_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL expr.extra_info_", K(ret), K(expr));
  } else if (OB_FAIL(vec_indices.reserve(bound.end() - bound.start()))) {
    LOG_WARN("failed to reserve space for indices", K(ret), K(bound));
  } else {
    ObExprUDFInfo &udf_info = *static_cast<ObExprUDFInfo *>(expr.extra_info_);

    if (0 == expr.arg_cnt_) {
      // do nothing
    } else if (OB_ISNULL(arg_vec = static_cast<ObIVector**>(alloc.alloc(sizeof(ObIVector*) * expr.arg_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(expr));
    } else if (FALSE_IT(memset(static_cast<void*>(arg_vec), 0, sizeof(ObIVector*) * expr.arg_cnt_))) {
      // unreachable
    } else if (OB_ISNULL(row = static_cast<ObObj*>(alloc.alloc(sizeof(ObObj) * expr.arg_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(expr));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
        ColumnType *column = nullptr;

        if (OB_ISNULL(new (row + i) ObObj())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to construct ObObj");
        } else if (arg_types.push_back(udf_info.params_type_.at(i))) {
          LOG_WARN("failed to push_back", K(ret), K(i), K(udf_info));
        } else if (OB_ISNULL(column = static_cast<ColumnType*>(alloc.alloc(sizeof(ColumnType))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory for column", K(ret), K(i));
        } else if (OB_ISNULL(column = new(column)ColumnType())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to construct column", K(ret));
        } else if (OB_FAIL(args.push_back(column))) {
          LOG_WARN("failed to push_back column to args", K(ret), K(i), KPC(column));
        } else if (OB_FAIL(SMART_CALL(expr.args_[i]->eval_vector(ctx, skip, bound)))) {
          LOG_WARN("failed to eval_vector", K(ret), K(i), K(expr));
        } else {
          arg_vec[i] = expr.args_[i]->get_vector(ctx);
        }
      }
    }

    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      }

      if (OB_FAIL(vec_indices.push_back(idx))) {
        LOG_WARN("failed to push_back idx", K(ret), K(vec_indices), K(idx));
      } else if (OB_FAIL(transfer_vec_to_obj(row, arg_vec, expr, idx))) {
        LOG_WARN("failed to transfer_vec_to_obj", K(ret), K(idx));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
        ObIArray<ObObj> *curr = args.at(i);

        if (OB_ISNULL(curr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL column", K(ret), K(args), K(i));
        } else if (OB_FAIL(curr->push_back(row[i]))) {
          LOG_WARN("failed to push_back row[i]", K(args), KPC(curr), K(row[i]), K(i), K(idx));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObArray<ObObj> res_array;

      pl::ObJavaUDFExecutor executor(ctx.exec_ctx_, udf_info.external_routine_entry_);

      int64_t batch_size = vec_indices.count();

      if (OB_FAIL(res_array.reserve(batch_size))) {
        LOG_WARN("failed to reserve space for results", K(ret), K(batch_size));
      } else if (OB_FAIL(executor.init(udf_info.udf_id_,
                                       udf_info.external_routine_type_,
                                       udf_info.external_routine_url_,
                                       udf_info.external_routine_resource_))) {
        LOG_WARN("failed to init java udf executor", K(ret));
      } else if (OB_FAIL(executor.execute(batch_size, "evaluate", arg_types, args, udf_info.result_type_, alloc, res_array))) {
        LOG_WARN("failed to execute udf", K(ret), K(batch_size), K(res_array));
      } else if (batch_size != res_array.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected res_array count", K(ret), K(res_array), K(batch_size));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
        int64_t idx = vec_indices.at(i);
        ObObj buf = res_array.at(i);

        batch_info_guard.set_batch_idx(idx);

        if (OB_FAIL(transfer_obj_to_vec(buf, res_vec, idx, expr, ctx))) {
          LOG_WARN("failed to transfer_obj_to_vec", K(ret), K(buf), K(res_vec), K(idx));
        } else {
          eval_flags.set(idx);
        }
      }
    }
  }

  for (int64_t i = 0; i < args.count(); ++i) {
    if (OB_NOT_NULL(args.at(i))) {
      args.at(i)->~ObIArray();
      args.at(i) = nullptr;
    }
  }
  args.reset();

  return ret;
}

int ObExprUDF::eval_udf_single(const ObExpr &expr, ObEvalCtx &eval_ctx, ObExprUDFCtx &udf_ctx, ObObj& result)
{
  int ret = OB_SUCCESS;
  bool found = false;
  ObObj tmp_result;
  observer::ObReqTimeGuard req_timeinfo_guard;

  if (OB_FAIL(udf_ctx.get_result_from_cache(tmp_result, found))) {
    LOG_WARN("failed to get result from udf cache", K(ret));
  } else if (found) {
    result = tmp_result;
  } else {
    try {
      ObExprUDFEnvGuard env_guard(eval_ctx, udf_ctx, ret, tmp_result);
      if (OB_FAIL(process_in_params(udf_ctx, env_guard.get_deep_in_objs()))) {
        LOG_WARN("failed to process in params", K(ret));
      } else if (OB_FAIL(GCTX.pl_engine_->execute(eval_ctx.exec_ctx_,
                                                  udf_ctx.get_allocator(),
                                                  udf_ctx.get_package_id(),
                                                  udf_ctx.get_info()->udf_id_,
                                                  udf_ctx.get_info()->subprogram_path_,
                                                  *udf_ctx.get_param_store(),
                                                  udf_ctx.get_info()->nocopy_params_,
                                                  tmp_result,
                                                  udf_ctx.get_pl_execute_arg(),
                                                  nullptr,
                                                  false,
                                                  true,
                                                  udf_ctx.get_info()->loc_,
                                                  udf_ctx.get_info()->is_called_in_sql_,
                                                  udf_ctx.get_info()->dblink_id_,
                                                  nullptr))) {
        LOG_WARN("failed to eval udf use pl engine", K(ret));
      }
      // Out Params will rewrite to Parent ParamStore, so this function must called after ~ObExprUDFEnvGuard
      if (OB_SUCC(ret) && OB_FAIL(process_out_params(udf_ctx, eval_ctx))) {
        LOG_WARN("failed to process out params", K(ret));
      }
      if (OB_SUCC(ret) && OB_FAIL(process_return_value(result, tmp_result, eval_ctx, udf_ctx, env_guard))) {
        LOG_WARN("failed to process return value", K(ret), K(result), K(tmp_result));
      }
      if (OB_SUCC(ret) && OB_FAIL(adjust_return_value(result, expr.obj_meta_, udf_ctx.get_allocator(), udf_ctx))) {
        LOG_WARN("failed to adjust return value", K(ret), K(result));
      }
    } catch(...) {
      LOG_ERROR("UDF eval resource not released by now, must be bug here !!!!");
      throw;
    }
    if (OB_READ_NOTHING == ret && udf_ctx.get_info()->is_called_in_sql_ && lib::is_oracle_mode()) {
      result.set_null();
      ret = OB_SUCCESS;
    }
    // add result cache
    if (OB_SUCC(ret) && OB_FAIL(udf_ctx.add_result_to_cache(result))) {
      LOG_WARN("failed to add result to cache", K(ret));
    }
  }
  return ret;
}

int ObExprUDF::eval_udf_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ObIVector *arg_vec[expr.arg_cnt_];
  ObIVector *res_vec = expr.get_vector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(bound.end() - bound.start());

  for (int64_t i = 0;  OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
    OZ (SMART_CALL(expr.args_[i]->eval_vector(ctx, skip, bound)));
    OX (arg_vec[i] = expr.args_[i]->get_vector(ctx));
  }

  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    ObObj result;
    ObExprUDFCtx *udf_ctx = nullptr;
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    }
    OZ (build_udf_ctx(expr, ctx.exec_ctx_, udf_ctx));
    OX (batch_info_guard.set_batch_idx(idx));
    OX (eval_flags.set(idx));
    OZ (transfer_vec_to_obj(udf_ctx->get_obj_stack(), arg_vec, expr, idx));
    OZ (eval_udf_single(expr, ctx, *udf_ctx, result));
    OZ (transfer_obj_to_vec(result, res_vec, idx, expr, ctx));
  }
  return ret;
}

int ObExprUDF::eval_udf_batch(
  const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;

  ObDatumVector res_datum = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(batch_size);

  OZ (SMART_CALL(expr.eval_batch_param_value(ctx, skip, batch_size)));

  for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
    if (skip.at(j) || eval_flags.at(j)) {
      continue;
    }
    ObObj result;
    ObExprUDFCtx *udf_ctx = nullptr;
    OZ (build_udf_ctx(expr, ctx.exec_ctx_, udf_ctx));
    OX (batch_info_guard.set_batch_idx(j));
    OX (eval_flags.set(j));
    OZ (transfer_datum_to_objs(expr, ctx, udf_ctx->get_obj_stack()));
    OZ (eval_udf_single(expr, ctx, *udf_ctx, result));
    OZ (res_datum.at(j)->from_obj(result, expr.obj_datum_map_));
    OZ (expr.deep_copy_datum(ctx, *res_datum.at(j)));
  }
  return ret;
}

int ObExprUDF::eval_udf(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObObj result;
  ObExprUDFCtx *udf_ctx = NULL;
  OZ (build_udf_ctx(expr, ctx.exec_ctx_, udf_ctx));
  OZ (SMART_CALL(expr.eval_param_value(ctx)));
  OZ (transfer_datum_to_objs(expr, ctx, udf_ctx->get_obj_stack()));
  OZ (eval_udf_single(expr, ctx, *udf_ctx, result));
  OZ (res.from_obj(result, expr.obj_datum_map_));
  OZ (expr.deep_copy_datum(ctx, res));
  return ret;
}

int ObExprUDF::eval_mysql_udtf(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;

  ObExprUDTFCtx *udtf_ctx = nullptr;

  if (OB_ISNULL(udtf_ctx = static_cast<ObExprUDTFCtx *>(ctx.exec_ctx_.get_expr_op_ctx(expr.expr_ctx_id_)))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("use UDTF without table function is not supported", K(ret), K(expr));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "use UDTF without table function is");
  }

  if (OB_SUCC(ret) && 0 > udtf_ctx->curr_) {
    if (OB_ISNULL(expr.extra_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL expr.extra_info_", K(ret), K(expr));
    } else if (OB_FAIL(SMART_CALL(expr.eval_param_value(ctx)))) {
      LOG_WARN("failed to eval param value", K(ret), K(expr));
    } else {
      ObExprUDFInfo &udf_info = *static_cast<ObExprUDFInfo*>(expr.extra_info_);

      ObSEArray<ObObjMeta, 8> arg_types;
      using ColumnType = ObSEArray<ObObj, 1>;
      ObSEArray<ObIArray<ObObj>*, 8> args;
      ObArenaAllocator alloc;

      for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
        if (OB_ISNULL(expr.args_[i])) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(arg_types.push_back(udf_info.params_type_.at(i)))) {
            LOG_WARN("failed to push_back arg datum meta", K(ret), K(i), K(expr));
        } else {
          const ObDatum &datum = expr.args_[i]->locate_expr_datum(ctx);
          ObObj obj;
          ColumnType *column = nullptr;

          if (OB_ISNULL(column = static_cast<ColumnType*>(alloc.alloc(sizeof(ColumnType))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory for column", K(ret), K(i));
          } else if (OB_ISNULL(column = new(column)ColumnType())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to construct column", K(ret));
          } else if (OB_FAIL(datum.to_obj(obj, expr.args_[i]->obj_meta_, expr.args_[i]->obj_datum_map_))) {
            LOG_WARN("failed to convert datum to obj", K(ret), K(datum), K(obj), K(i), K(expr.args_[i]->obj_meta_), K(expr.args_[i]->obj_datum_map_));
          } else if (OB_FAIL(column->push_back(obj))) {
            LOG_WARN("failed to push_back to column", K(ret), K(datum), K(obj), K(i));
          } else if (OB_FAIL(args.push_back(column))) {
            LOG_WARN("failed to push_back column to args", K(ret), K(i), KPC(column));
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObSEArray<ObObj, 8> res_array;

        pl::ObJavaUDFExecutor executor(ctx.exec_ctx_, udf_info.external_routine_entry_);

        executor.set_need_infer_result_size(true);

        if (OB_FAIL(executor.init(udf_info.udf_id_,
                                  udf_info.external_routine_type_,
                                  udf_info.external_routine_url_,
                                  udf_info.external_routine_resource_))) {
          LOG_WARN("failed to init java udf executor", K(ret));
        } else if (OB_FAIL(executor.execute(1, "process", arg_types, args, udf_info.result_type_, alloc, res_array))) {
          LOG_WARN("failed to execute udf", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < res_array.count(); ++i) {
            ObObj buf;

            if (OB_FAIL(deep_copy_obj(udtf_ctx->allocator_, res_array.at(i), buf))) {
              LOG_WARN("failed to deep_copy_obj", K(ret), K(res_array), K(i), K(buf));
            } else if (OB_FAIL(udtf_ctx->buffer_.push_back(buf))) {
              LOG_WARN("failed to push_back to UDTF buffer", K(ret), K(buf));
            }
          }

          if (OB_SUCC(ret)) {
            udtf_ctx->curr_ = 0;
          }
        }
      }

      for (int64_t i = 0; i < args.count(); ++i) {
        if (OB_NOT_NULL(args.at(i))) {
          args.at(i)->~ObIArray();
          args.at(i) = nullptr;
        }
      }
      args.reset();
    }
  }

  if (OB_SUCC(ret)) {
    ObObj obj;

    if (0 > udtf_ctx->curr_) {
      ret = OB_NOT_INIT;
      LOG_WARN("UDTF buffer is not initialized", K(ret), KPC(udtf_ctx), K(lbt()));
    } else if (udtf_ctx->curr_ >= udtf_ctx->buffer_.count()) {
      ret = OB_ITER_END;
      LOG_TRACE("finished to iterate UDTF", K(ret), KPC(udtf_ctx));
    } else if (OB_FAIL(udtf_ctx->buffer_.at(udtf_ctx->curr_, obj))) {
      LOG_WARN("failed to get obj from udtf_ctx", K(ret), KPC(udtf_ctx));
    } else if (OB_FAIL(res.from_obj(obj, expr.obj_datum_map_))) {
      LOG_WARN("failed to from obj", K(ret), K(obj));
    } else if (OB_FAIL(expr.deep_copy_datum(ctx, res))) {
      LOG_WARN("failed to deep copy datum", K(ret));
    } else {
      udtf_ctx->curr_ += 1;
    }
  }

  return ret;
}

} /* sql */
} /* oceanbase */
