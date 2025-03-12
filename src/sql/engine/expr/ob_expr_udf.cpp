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

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObExprUDF, ObFuncExprOperator),
                     udf_id_, result_type_, params_type_,
                     udf_package_id_, params_desc_, is_udt_udf_,
                     nocopy_params_, subprogram_path_, call_in_sql_, loc_, is_udt_cons_);

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
      is_udt_cons_(false) {}

ObExprUDF::~ObExprUDF() {}

void ObExprUDF::reset()
{
  udf_id_ = common::OB_INVALID_ID;
  udf_package_id_ = common::OB_INVALID_ID;
  params_type_.reset();
  params_desc_.reset();
  is_udt_udf_ = false;
  call_in_sql_ = true;
  nocopy_params_.reset();
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
  } else if (OB_FAIL(params_type_.assign(tmp_other->get_params_type()))) {
    LOG_WARN("failed to assign params type", K(ret));
  } else if (OB_FAIL(nocopy_params_.assign(tmp_other->get_nocopy_params()))) {
    LOG_WARN("failed to assign nocopy params", K(ret));
  } else if (OB_FAIL(result_type_.assign(tmp_other->get_result_type()))) {
    LOG_WARN("failed to assign result type", K(ret));
  } else if (OB_FAIL(params_desc_.assign(tmp_other->get_params_desc()))) {
    LOG_WARN("failed to assign params desc", K(ret));
  } else {
    udf_id_ = tmp_other->get_udf_id();
    udf_package_id_ = tmp_other->get_udf_package_id();
    is_udt_udf_ = tmp_other->get_is_udt_udf();
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("failed to ObExprOperator::assign", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObExprUDF::calc_result_typeN(ObExprResType &type,
                                 ObExprResType *types,
                                 int64_t param_num,
                                 ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  CK(param_num == params_type_.count());
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < param_num && OB_SUCC(ret); i++) {
      if (params_desc_.at(i).is_out()
          && params_desc_.at(i).is_obj_access_out()) {
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
  }
  return ret;
}

int ObExprUDF::check_types(ObObj &result, const ObObj *objs_stack, int64_t param_num) const
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(objs_stack));
  CK (param_num == params_type_.count());
  CK (param_num == params_desc_.count());
  UNUSED(result);
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
    if (!objs_stack[i].is_null()
        && (!params_desc_.at(i).is_out())) {
      TYPE_CHECK(objs_stack[i], params_type_.at(i).get_type());
    }
  }
  return ret;
}

int ObExprUDF::check_types(const ObExpr &expr, const ObExprUDFInfo &info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    if (!expr.args_[i]->obj_meta_.is_null()
        && (!info.params_desc_.at(i).is_out())) {
      if (expr.args_[i]->obj_meta_.get_type() != info.params_type_.at(i).get_type()) {
        if (info.params_type_.at(i).is_enum_or_set() && (ObVarcharType == expr.args_[i]->obj_meta_.get_type() || expr.args_[i]->obj_meta_.is_numeric_type())) {
          // do nothing ...
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("check param type failed", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObExprUDF::extract_allocator_and_restore_obj(const ObObj &obj, ObObj &new_obj, ObIAllocator *&composite_allocator)
{
  int ret = OB_SUCCESS;

  pl::ObPlCompiteWrite *composite_write = nullptr;
  CK (obj.is_ext());
  OX (composite_write = reinterpret_cast<pl::ObPlCompiteWrite *>(obj.get_ext()));
  CK (OB_NOT_NULL(composite_write));
  OX (new_obj.set_extend(composite_write->value_addr_, obj.get_meta().get_extend_type(), obj.get_val_len()));
  OX (composite_allocator = reinterpret_cast<ObIAllocator *>(composite_write->allocator_));

  return ret;
}

int ObExprUDF::need_deep_copy_in_parameter(const ObObj *objs_stack,
                                            int64_t param_num,
                                            const ObIArray<ObUDFParamDesc> &params_desc,
                                            const ObIArray<ObExprResType> &params_type,
                                            const ObObj &element,
                                            bool &need_deep_copy)
{
  int ret = OB_SUCCESS;
  need_deep_copy = false;
  for (int64_t i = 0; !need_deep_copy && i < param_num; ++i) {
    if (params_desc.at(i).is_out()
        && ObExtendType == params_type.at(i).get_type()) {
      ObObj value = objs_stack[i];
      if (params_desc.at(i).is_obj_access_out()) {
        ObIAllocator *dum_alloc = nullptr;
        OZ (extract_allocator_and_restore_obj(value, value, dum_alloc));
      }
      if (OB_SUCC(ret)) {
        if(element.get_ext() == value.get_ext()
          && value.is_pl_extend()
          && element.is_pl_extend()
          && value.get_meta().get_extend_type() == element.get_meta().get_extend_type()) {
          need_deep_copy = true;
        }
      }
    }
  }
  return ret;
}

int ObExprUDF::process_in_params(const ObObj *objs_stack,
                                 int64_t param_num,
                                 const ObIArray<ObUDFParamDesc> &params_desc,
                                 const ObIArray<ObExprResType> &params_type,
                                 ParamStore& iparams,
                                 ObIAllocator &allocator,
                                 ObIArray<ObObj> *deep_in_objs)
{
  int ret = OB_SUCCESS;
  CK (0 == param_num || OB_NOT_NULL(objs_stack));
  CK (param_num == params_desc.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    ObObjParam param;
    param.reset();
    if (params_type.at(i).is_null()) {
      // default value, mock a max obj to tell pl engine here need replace to default value.
      param.set_is_pl_mock_default_param(true);
    } else if (!params_desc.at(i).is_out()) { // in parameter
      if (ObExtendType == params_type.at(i).get_type()) {
        bool need_copy = false;
        OZ (need_deep_copy_in_parameter(objs_stack, param_num, params_desc, params_type, objs_stack[i], need_copy));
        if (need_copy) {
          OZ (pl::ObUserDefinedType::deep_copy_obj(allocator, objs_stack[i], param, true));
          if (OB_NOT_NULL(deep_in_objs)) {
            OZ (deep_in_objs->push_back(param));
          }
        } else {
          if (!objs_stack[i].is_null()) {
            param.set_extend(objs_stack[i].get_ext(),
                         objs_stack[i].get_meta().get_extend_type(), objs_stack[i].get_val_len());
            param.set_param_meta();
          } else {
            objs_stack[i].copy_value_or_obj(param, true);
          }
        }
      } else {
        objs_stack[i].copy_value_or_obj(param, true);
        param.set_param_meta();
      }
    } else if (params_desc.at(i).is_local_out()
              || params_desc.at(i).is_package_var_out()
              || params_desc.at(i).is_subprogram_var_out()) {
      if (ObExtendType != params_type.at(i).get_type()) {
        OZ (deep_copy_obj(allocator, objs_stack[i], param));
      } else {
        objs_stack[i].copy_value_or_obj(param, true);
      }
    } else {
      ObIAllocator *dum_alloc = nullptr;
      ObObj value = objs_stack[i];
      OZ (extract_allocator_and_restore_obj(value, value, dum_alloc));
      if (OB_SUCC(ret)) {
        if (params_type.at(i).get_type() == ObExtendType) {
          if (params_desc.at(i).is_obj_access_pure_out()) {
            OZ (pl::ObUserDefinedType::deep_copy_obj(allocator, value, param));
            if (OB_NOT_NULL(deep_in_objs)) {
              OZ (deep_in_objs->push_back(param));
            }
          } else {
            param.set_extend(value.get_ext(), value.get_meta().get_extend_type(), value.get_val_len());
            param.set_param_meta();
          }
        } else {
          void *ptr = NULL;
          ObObj *obj = NULL;
          CK (value.is_ext());
          OX (ptr = reinterpret_cast<void*>(value.get_ext()));
          CK (OB_NOT_NULL(ptr));
          OX (obj = reinterpret_cast<ObObj*>(ptr));
          CK (OB_NOT_NULL(obj));
          OX ((*obj).copy_value_or_obj(param, true));
          OX (param.set_param_meta());
        }
      }
    }
    if (OB_SUCC(ret) && params_type.at(i).get_type() == ObExtendType) {
      param.set_udt_id(params_type.at(i).get_udt_id());
    }
    OZ (iparams.push_back(param));
  }
  return ret;
}

int ObExprUDF::process_out_params(const ObObj *objs_stack,
                                  int64_t param_num,
                                  ParamStore& iparams,
                                  ObIAllocator &alloc,
                                  ObExecContext &exec_ctx,
                                  const ObIArray<int64_t> &nocopy_params,
                                  const ObIArray<ObUDFParamDesc> &params_desc,
                                  const ObIArray<ObExprResType> &params_type)
{
  int ret = OB_SUCCESS;
  UNUSED (param_num);
  CK (iparams.count() == params_desc.count());
  CK (0 == nocopy_params.count() || nocopy_params.count() == iparams.count());
  // 先处理NoCopy参数
  ObSEArray<bool, 16> dones;
  for (int64_t i = 0; OB_SUCC(ret) && i < iparams.count(); ++i) {
    if (!params_desc.at(i).is_out()) {
      OZ (dones.push_back(true));
    } else if (params_desc.at(i).is_local_out() && nocopy_params.at(i) != OB_INVALID_INDEX) {
      pl::ObPLExecCtx *ctx = nullptr;
      ObIAllocator *symbol_alloc = nullptr;
      ObIAllocator *cur_expr_allocator = nullptr;
      const ParamStore &param_store = exec_ctx.get_physical_plan_ctx()->get_param_store();
      int64_t position = params_desc.at(i).get_index();
      ObObjParam *modify = NULL;
      ObObjParam result;
      ObObjParam tmp;
      if (OB_NOT_NULL(exec_ctx.get_my_session()->get_pl_context())) {
        ctx = exec_ctx.get_my_session()->get_pl_context()->get_current_ctx();
        CK (OB_NOT_NULL(ctx));
        OX (symbol_alloc = ctx->allocator_);
        OX (cur_expr_allocator = ctx->get_top_expr_allocator());
        CK (OB_NOT_NULL(cur_expr_allocator));
      }
      CK (position < param_store.count());
      CK (OB_NOT_NULL(modify = const_cast<ObObjParam*>(&(param_store.at(position)))));
      OZ (sql::ObSPIService::spi_convert(exec_ctx.get_my_session(),
                                         nullptr != cur_expr_allocator ? cur_expr_allocator : &alloc,
                                         iparams.at(i),
                                         params_type.at(i),
                                         tmp));
      if (symbol_alloc != nullptr) {
        if (tmp.is_pl_extend() &&
            tmp.get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) {
          OZ (pl::ObUserDefinedType::deep_copy_obj(*symbol_alloc, tmp, result));
        } else {
          OZ (deep_copy_obj(*symbol_alloc, tmp, result));
        }
      } else {
        OX (result = tmp);
      }
      if (OB_SUCC(ret) && symbol_alloc != nullptr) {
        if (modify->is_pl_extend() &&
            modify->get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) {
          pl::ObUserDefinedType::destruct_objparam(*symbol_alloc, *modify, exec_ctx.get_my_session());
        } else if (modify->need_deep_copy()) {
          void *ptr = modify->get_deep_copy_obj_ptr();
          if (nullptr != ptr) {
            symbol_alloc->free(ptr);
          }
        }
      }
      OX (result.copy_value_or_obj(*modify, true));
      OX (modify->set_param_meta());
      if (OB_SUCC(ret) && iparams.at(i).is_ref_cursor_type()) {
        modify->set_is_ref_cursor_type(true);
      }
      OZ (dones.push_back(true));
    } else {
      OZ (dones.push_back(false));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < iparams.count(); ++i) {
    OZ (process_singal_out_param(i,
                                 dones,
                                 objs_stack,
                                 param_num,
                                 iparams,
                                 alloc,
                                 exec_ctx,
                                 nocopy_params,
                                 params_desc,
                                 params_type));
  }
  return ret;
}

int ObExprUDF::process_singal_out_param(int64_t i,
                                        ObIArray<bool> &dones,
                                        const ObObj *objs_stack,
                                        int64_t param_num,
                                        ParamStore& iparams,
                                        ObIAllocator &alloc,
                                        ObExecContext &exec_ctx,
                                        const ObIArray<int64_t> &nocopy_params,
                                        const ObIArray<ObUDFParamDesc> &params_desc,
                                        const ObIArray<ObExprResType> &params_type)
{
  int ret = OB_SUCCESS;
  if (dones.at(i)) {
    // already process, do nothing
  } else if (params_desc.at(i).is_local_out()) { //out param in paramstore of caller
    if (nocopy_params.count() > 0 && nocopy_params.at(i) != OB_INVALID_INDEX) {
      // nocopy parameter already process before, do nothing ....
    } else {
      pl::ObPLExecCtx *ctx = nullptr;
      ObIAllocator *symbol_alloc = nullptr;
      ObIAllocator *cur_expr_allocator = nullptr;
      const ParamStore &param_store = exec_ctx.get_physical_plan_ctx()->get_param_store();
      int64_t position = params_desc.at(i).get_index();
      ObObjParam *modify = NULL;
      ObObjParam result;
      ObObjParam tmp;
      if (OB_NOT_NULL(exec_ctx.get_my_session()->get_pl_context())) {
        ctx = exec_ctx.get_my_session()->get_pl_context()->get_current_ctx();
        CK (OB_NOT_NULL(ctx));
        OX (symbol_alloc = ctx->allocator_);
        OX (cur_expr_allocator = ctx->get_top_expr_allocator());
        CK (OB_NOT_NULL(cur_expr_allocator));
      }
      CK (position < param_store.count());
      CK (OB_NOT_NULL(modify = const_cast<ObObjParam*>(&(param_store.at(position)))));
      // ext type cannot convert. just copy it.
      if (iparams.at(i).is_ext()) {
        // caller param may ref cursor, which may not allocated.
        if (modify->is_null()) {
          OX (iparams.at(i).copy_value_or_obj(*modify, true));
          if (iparams.at(i).is_ref_cursor_type()) {
            modify->set_is_ref_cursor_type(true);
          }
          OX (modify->set_param_meta());
        } else if (!modify->is_ext()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("process function out param failed, type mismatch", K(ret),
                                                                 K(iparams.at(i)), K(*modify));
        } else {
          OX (iparams.at(i).copy_value_or_obj(*modify, true));
          OX (modify->set_param_meta());
          if (OB_SUCC(ret) && iparams.at(i).is_ref_cursor_type()) {
            modify->set_is_ref_cursor_type(true);
          }
        }
      } else {
        OZ (sql::ObSPIService::spi_convert(exec_ctx.get_my_session(),
                                           nullptr != cur_expr_allocator ? cur_expr_allocator : &alloc,
                                           iparams.at(i),
                                           params_type.at(i),
                                           tmp));
        if (symbol_alloc != nullptr) {
          OZ (deep_copy_obj(*symbol_alloc, tmp, result));
        } else {
          OX (result = tmp);
        }
        if (OB_SUCC(ret) && symbol_alloc != nullptr) {
          void *ptr = modify->get_deep_copy_obj_ptr();
          if (nullptr != ptr) {
            symbol_alloc->free(ptr);
          }
        }
        OX (result.copy_value_or_obj(*modify, true));
        OX (modify->set_param_meta());
        if (OB_SUCC(ret) && iparams.at(i).is_ref_cursor_type()) {
          modify->set_is_ref_cursor_type(true);
        }
      }
      OX (dones.at(i) = true);
    }
  } else if (params_desc.at(i).is_package_var_out()) {
    OZ (ObSPIService::spi_set_package_variable(
      &exec_ctx,
      NULL,
      params_desc.at(i).get_package_id(),
      params_desc.at(i).get_index(),
      iparams.at(i)));
    OX (dones.at(i) = true);
  } else if (params_desc.at(i).is_subprogram_var_out()) {
    OZ (pl::ObPLContext::set_subprogram_var_from_local(
      *exec_ctx.get_my_session(),
      params_desc.at(i).get_package_id(),
      params_desc.at(i).get_subprogram_id(),
      params_desc.at(i).get_index(),
      iparams.at(i)));
    OX (dones.at(i) = true);
  } else if (params_desc.at(i).is_obj_access_out() &&
             OB_INVALID_ID != params_desc.at(i).get_package_id() &&
             OB_INVALID_ID != params_desc.at(i).get_index()) {
    OZ (SMART_CALL(process_package_out_param(
      i, dones, objs_stack, param_num, iparams, alloc, exec_ctx, nocopy_params, params_desc, params_type)));
  } else if (!params_type.at(i).is_ext()) {
    void *ptr = NULL;
    ObObj *obj = NULL;
    ObObjParam result;
    ObObjParam tmp;
    ObArenaAllocator tmp_alloc;
    ObObj value = objs_stack[i];
    ObIAllocator *composite_allocator = nullptr;
    OZ (extract_allocator_and_restore_obj(value, value, composite_allocator));
    CK (value.is_ext());
    OX (ptr = reinterpret_cast<void*>(value.get_ext()));
    CK (OB_NOT_NULL(ptr));
    OX (obj = reinterpret_cast<ObObj*>(ptr));
    CK (OB_NOT_NULL(obj));
    OZ (sql::ObSPIService::spi_convert(exec_ctx.get_my_session(),
                                       &alloc, iparams.at(i),
                                       params_type.at(i),
                                       tmp));
    if (composite_allocator != nullptr) {
      OZ (deep_copy_obj(*composite_allocator, tmp, result));
    } else {
      OX (result = tmp);
    }
    if (OB_SUCC(ret) && composite_allocator != nullptr) {
      void *ptr = obj->get_deep_copy_obj_ptr();
      if (nullptr != ptr) {
        composite_allocator->free(ptr);
      }
    }
    OX (result.copy_value_or_obj(*obj, true));
    OX (result.set_param_meta());
    OX (dones.at(i) = true);
  } else if (params_desc.at(i).is_obj_access_pure_out()) { // objaccess complex type pure out sence
    ObObj &obj = iparams.at(i);
    ObObj origin_value = objs_stack[i];
    ObIAllocator *composite_allocator = nullptr;
    OZ (extract_allocator_and_restore_obj(origin_value, origin_value, composite_allocator));
    if (obj.is_ext() && obj.get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) {
      OZ (pl::ObUserDefinedType::deep_copy_obj(nullptr != composite_allocator ? *composite_allocator : alloc, obj, origin_value, true));
    }
    OX (dones.at(i) = true);
  }
  return ret;
}

int ObExprUDF::process_package_out_param(int64_t idx,
                                         ObIArray<bool> &dones,
                                         const ObObj *objs_stack,
                                         int64_t param_num,
                                         ParamStore& iparams,
                                         ObIAllocator &alloc,
                                         ObExecContext &exec_ctx,
                                         const ObIArray<int64_t> &nocopy_params,
                                         const ObIArray<ObUDFParamDesc> &params_desc,
                                         const ObIArray<ObExprResType> &params_type)
{
  int ret = OB_SUCCESS;
  // check if left out parameter is child of current
  for (int64_t i = idx + 1; OB_SUCC(ret) && i < iparams.count(); ++i) {
    if (!dones.at(i) && iparams.at(i).is_ext()) {
      bool is_child = false;
      ObIAllocator *dum_alloc = nullptr;
      ObObj origin_value_i = objs_stack[i];
      ObObj origin_value_idx = objs_stack[idx];
      OZ (extract_allocator_and_restore_obj(origin_value_i, origin_value_i, dum_alloc));
      OZ (extract_allocator_and_restore_obj(origin_value_idx, origin_value_idx, dum_alloc));
      OZ (is_child_of(origin_value_idx, origin_value_i, is_child));
      if (OB_SUCC(ret) && is_child) {
        OZ (SMART_CALL(process_singal_out_param(
          i, dones, objs_stack, param_num, iparams, alloc, exec_ctx, nocopy_params, params_desc, params_type)));
      }
    }
  }
  ObObj origin_value = objs_stack[idx];
  ObIAllocator *allocator = NULL;
  pl::ObPLExecCtx plctx(nullptr, &exec_ctx, nullptr,nullptr,nullptr,nullptr);
  ObIAllocator *composite_allocator = nullptr;
  OZ (extract_allocator_and_restore_obj(origin_value, origin_value, composite_allocator));
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(composite_allocator)) {
      allocator = composite_allocator;
    } else {
      ObIAllocator *pkg_allocator = NULL;
      OZ (ObSPIService::spi_get_package_allocator(&plctx, params_desc.at(idx).get_package_id(), pkg_allocator));
      OX (allocator = pkg_allocator);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!params_type.at(idx).is_ext()) {
    void *ptr = NULL;
    ObObj *obj = NULL;
    ObObjParam result;
    ObObjParam tmp;
    CK (origin_value.is_ext());
    OX (ptr = reinterpret_cast<void*>(origin_value.get_ext()));
    CK (OB_NOT_NULL(ptr));
    OX (obj = reinterpret_cast<ObObj*>(ptr));
    CK (OB_NOT_NULL(obj));
    OZ (sql::ObSPIService::spi_convert(exec_ctx.get_my_session(),
                                       &alloc,
                                       iparams.at(idx),
                                       params_type.at(idx),
                                       tmp));
    if (allocator != nullptr) {
      OZ (deep_copy_obj(*allocator, tmp, result));
    } else {
      OX (result = tmp);
    }
    if (OB_SUCC(ret) && allocator != nullptr) {
      void *ptr = obj->get_deep_copy_obj_ptr();
      if (nullptr != ptr) {
        allocator->free(ptr);
      }
    }
    OX (result.copy_value_or_obj(*obj, true));
    OX (result.set_param_meta());
  } else {
    ObObj &obj = iparams.at(idx);
    if (OB_SUCC(ret) && nullptr != allocator) {
      if (obj.is_ext() && obj.get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) {
        OZ (pl::ObUserDefinedType::deep_copy_obj(*allocator, obj, origin_value, true));
      }
    }
  }
  OZ (ObSPIService::spi_update_package_change_info(
    &plctx, params_desc.at(idx).get_package_id(), params_desc.at(idx).get_index()));
  OX (dones.at(idx) = true);
  return ret;
}

int ObExprUDF::is_child_of(const ObObj &parent, const ObObj &child, bool &is_child)
{
  int ret = OB_SUCCESS;
  if (parent.is_ext() && child.is_ext() && parent.get_ext() == child.get_ext()) {
    is_child = true;
  } else if (parent.is_pl_extend() && parent.get_ext() != 0) {
    switch (parent.get_meta().get_extend_type()) {
      case pl::PL_NESTED_TABLE_TYPE:
      case pl::PL_ASSOCIATIVE_ARRAY_TYPE:
      case pl::PL_VARRAY_TYPE: {
        pl::ObPLCollection* coll = reinterpret_cast<pl::ObPLCollection*>(parent.get_ext());
        CK (OB_NOT_NULL(coll));
        for (int64_t i = 0; OB_SUCC(ret) && i < coll->get_count(); ++i) {
          CK (OB_NOT_NULL(coll->get_data()));
          if (OB_FAIL(ret)) {
          } else if (!(coll->get_data()[i]).is_ext()) {
            ObObj tmp;
            tmp.set_ext(reinterpret_cast<int64_t>(&(coll->get_data()[i])));
            OZ (SMART_CALL(is_child_of(tmp, child, is_child)));
          } else {
            OZ (SMART_CALL(is_child_of(coll->get_data()[i], child, is_child)));
          }
        }
      } break;
      case pl::PL_RECORD_TYPE: {
        pl::ObPLRecord* record = reinterpret_cast<pl::ObPLRecord*>(parent.get_ext());
        CK (OB_NOT_NULL(record));
        for (int64_t i = 0; OB_SUCC(ret) && i < record->get_count(); ++i) {
          ObObj *obj = NULL;
          OZ (record->get_element(i, obj));
          CK (OB_NOT_NULL(obj));
          if (OB_FAIL(ret)) {
          } else if (!obj->is_ext()) {
            ObObj tmp;
            tmp.set_ext(reinterpret_cast<int64_t>(obj));
            OZ (SMART_CALL(is_child_of(tmp, child, is_child)));
          } else {
            OZ (SMART_CALL(is_child_of(*obj, child, is_child)));
          }
        }
      } break;
      default: {
      } break;
    }
  }
  return ret;
}

int ObExprUDF::before_calc_result(share::schema::ObSchemaGetterGuard &schema_guard,
                                  ObSqlCtx &sql_ctx,
                                  ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  // 通过SPI执行的UDF可能会存在exec_ctx_中的schema_guard是空的情况
  // 这里根据task_ctx中记录的schema_version重新获取schema_guard
  if (OB_ISNULL(exec_ctx.get_sql_ctx())
      || OB_ISNULL(exec_ctx.get_sql_ctx()->schema_guard_)) {
    sql::ObTaskExecutorCtx &task_ctx = exec_ctx.get_task_exec_ctx();
    const observer::ObGlobalContext &gctx = observer::ObServer::get_instance().get_gctx();
    if (OB_FAIL(gctx.schema_service_->get_tenant_schema_guard(
                exec_ctx.get_my_session()->get_effective_tenant_id(),
                schema_guard,
                task_ctx.get_query_tenant_begin_schema_version(),
                task_ctx.get_query_sys_begin_schema_version()))) {
      LOG_WARN("get schema guard failed", K(ret));
    }
  }
  // 通过分布式计划执行的function没有sqlctx信息, 构造一个
  if (OB_ISNULL(exec_ctx.get_sql_ctx())) {
    sql_ctx.session_info_ = exec_ctx.get_my_session();
    sql_ctx.schema_guard_ = &schema_guard;
    exec_ctx.set_sql_ctx(&sql_ctx);
  } else if (OB_ISNULL(exec_ctx.get_sql_ctx()->schema_guard_)) {
    exec_ctx.get_sql_ctx()->schema_guard_ = &schema_guard;
  }
  return ret;
}

int ObExprUDF::after_calc_result(share::schema::ObSchemaGetterGuard &schema_guard,
                                 ObSqlCtx &sql_ctx,
                                 ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  if (exec_ctx.get_sql_ctx() == &sql_ctx) {
    exec_ctx.set_sql_ctx(NULL);
  } else if (exec_ctx.get_sql_ctx()->schema_guard_ == &schema_guard) {
    exec_ctx.get_sql_ctx()->schema_guard_ = NULL;
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
  } else {
    OZ(info->from_raw_expr(fun_sys));
    info->is_called_in_sql_ = is_called_in_sql();
    rt_expr.extra_info_ = info;
  }
  rt_expr.eval_func_ = eval_udf;
  return ret;
}

int ObExprUDF::ObExprUDFCtx::init_param_store(ObIAllocator &allocator,
                                              int param_num)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(param_store_buf_ = allocator.alloc(sizeof(ParamStore)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    params_ = new(param_store_buf_)ParamStore(ObWrapperAllocator(allocator));
  }
  OZ (params_->prepare_allocate(param_num));
  OX (params_->reuse());
  return ret;
}

int ObExprUDF::build_udf_ctx(int64_t udf_ctx_id,
                             int64_t param_num,
                             ObExecContext &exec_ctx,
                             ObExprUDFCtx *&udf_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(udf_ctx = static_cast<ObExprUDFCtx *>(exec_ctx.get_expr_op_ctx(udf_ctx_id)))) {
    if (OB_FAIL(exec_ctx.create_expr_op_ctx(udf_ctx_id, udf_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret));
    } else if (OB_FAIL(udf_ctx->init_param_store(exec_ctx.get_allocator(), param_num))) {
      LOG_WARN("failed to init param", K(ret));
    }
  } else {
    OX (udf_ctx->reuse());
  }
  return ret;
}

int ObExprUDF::eval_udf(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObObj tmp_result;
  ObObj result;
  pl::ObPL *pl_engine = nullptr;
  ParamStore *udf_params = nullptr;
  uint64_t udf_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  ObExprUDFCtx *udf_ctx = nullptr;
  ObSQLSessionInfo *session = nullptr;
  ObIAllocator *alloc = &ctx.exec_ctx_.get_allocator();
  pl::ObPLExecCtx *pl_exec_ctx = nullptr;
  const ObExprUDFInfo *info = static_cast<ObExprUDFInfo *>(expr.extra_info_);
  if (OB_SUCC(ret) && expr.arg_cnt_ != info->params_desc_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udf parameter number is not equel to params desc count",
             K(ret), K(expr.arg_cnt_), K(info->params_desc_.count()), K(info->params_desc_));
  }

  CK (OB_NOT_NULL(info));
  OZ (check_types(expr, *info));


  CK (OB_NOT_NULL(session = ctx.exec_ctx_.get_my_session()));
  CK (OB_NOT_NULL(pl_engine = session->get_pl_engine()));
  OZ (SMART_CALL(expr.eval_param_value(ctx)));
  OZ (build_udf_ctx(udf_ctx_id, expr.arg_cnt_, ctx.exec_ctx_, udf_ctx));
  CK (OB_NOT_NULL(udf_params = udf_ctx->get_param_store()));
  if (OB_SUCC(ret) && OB_NOT_NULL(session->get_pl_context()) && !info->is_called_in_sql_) {
    pl_exec_ctx = session->get_pl_context()->get_current_ctx();
    CK (OB_NOT_NULL(pl_exec_ctx));
    OX (alloc = pl_exec_ctx->get_top_expr_allocator());
    CK (OB_NOT_NULL(alloc));
  }
  if (OB_FAIL(ret)) {
    // do nothing ...
  } else {
    bool need_end_stmt = false;
    bool need_free_udt = false;
    stmt::StmtType parent_stmt = ctx.exec_ctx_.get_sql_ctx()->stmt_type_;
    if (!session->has_start_stmt() && stmt::StmtType::T_SELECT == parent_stmt) {
      need_end_stmt = true;
      session->set_start_stmt();
    }

    ObEvalCtx::TempAllocGuard memory_guard(ctx);
    ObArenaAllocator &allocator = memory_guard.get_allocator();
    int64_t cur_obj_count = 0;
    if (OB_NOT_NULL(ctx.exec_ctx_.get_pl_ctx())) {
      cur_obj_count = ctx.exec_ctx_.get_pl_ctx()->get_objects().count();
    }

    ObObj *objs = nullptr;
    ObSEArray<ObObj, 2> deep_in_objs;
    if (expr.arg_cnt_ > 0) {
      objs = static_cast<ObObj *> (allocator.alloc(expr.arg_cnt_ * sizeof(ObObj)));
      if (OB_ISNULL(objs)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate objs memory failed", K(ret));
      }
      OZ (fill_obj_stack(expr, ctx, objs));
      OZ (process_in_params(
        objs, expr.arg_cnt_, info->params_desc_, info->params_type_, *udf_params, *alloc, &deep_in_objs));
    }

    share::schema::ObSchemaGetterGuard schema_guard;
    ObSqlCtx sql_ctx;
    OZ (before_calc_result(schema_guard, sql_ctx, ctx.exec_ctx_));

    // restore ctx.exec_ctx_ only when ctx.exec_ctx_ is successfully changed
    NAMED_DEFER(need_restore_exec_ctx,
                after_calc_result(schema_guard, sql_ctx, ctx.exec_ctx_));
    if (OB_FAIL(ret)) {
      need_restore_exec_ctx.deactivate();
    }

    if (OB_SUCC(ret) && info->is_udt_cons_) {
      pl::ObPLUDTNS ns(*ctx.exec_ctx_.get_sql_ctx()->schema_guard_);
      pl::ObPLDataType pl_type;
      pl_type.set_user_type_id(pl::PL_RECORD_TYPE, info->udf_package_id_);
      pl_type.set_type_from(pl::PL_TYPE_UDT);
      CK (0 < udf_params->count());
      OZ (ns.init_complex_obj(*alloc, *alloc, pl_type, udf_params->at(0), false, false));
      OX (need_free_udt = true);
    }
    try {
      int64_t package_id = info->is_udt_udf_ ?
           share::schema::ObUDTObjectType::mask_object_id(info->udf_package_id_)
           : info->udf_package_id_;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(pl_engine->execute(ctx.exec_ctx_,
                            info->is_called_in_sql_ ? allocator
                                                    : *alloc,
                            package_id,
                            info->udf_id_,
                            info->subprogram_path_,
                            *udf_params,
                            info->nocopy_params_,
                            tmp_result,
                            nullptr,
                            false,
                            true,
                            info->loc_,
                            info->is_called_in_sql_,
                            info->dblink_id_))) {
        LOG_WARN("fail to execute udf", K(ret), K(info), K(package_id), K(tmp_result));
        if (info->is_called_in_sql_ && OB_NOT_NULL(ctx.exec_ctx_.get_pl_ctx())) {
          ctx.exec_ctx_.get_pl_ctx()->reset_obj_range_to_end(cur_obj_count);
        }
        bool has_out_param = false;
        for (int64_t i = 0; !has_out_param && i < info->params_desc_.count(); ++i) {
          if (info->params_desc_.at(i).is_out()) {
            has_out_param = true;
          }
        }
        if (has_out_param) {
          int tmp = process_out_params(objs,
                                      expr.arg_cnt_,
                                      *udf_params,
                                      *alloc,
                                      ctx.exec_ctx_,
                                      info->nocopy_params_,
                                      info->params_desc_,
                                      info->params_type_);
          if (OB_SUCCESS != tmp) {
            LOG_WARN("fail to process out param", K(tmp), K(ret));
          }
        }
      }
    } catch(...) {
      throw;
    }
    if (OB_FAIL(ret)) {
    } else if (info->is_called_in_sql_) {
      // memory of ref cursor on session, do not copy it.
      if (tmp_result.is_pl_extend()
          && tmp_result.get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) {
        OZ (pl::ObUserDefinedType::deep_copy_obj(*alloc, tmp_result, result, true));
        OZ (pl::ObUserDefinedType::destruct_obj(tmp_result, ctx.exec_ctx_.get_my_session()));
        if (OB_NOT_NULL(ctx.exec_ctx_.get_pl_ctx())) {
          ctx.exec_ctx_.get_pl_ctx()->reset_obj_range_to_end(cur_obj_count);
          OZ (ctx.exec_ctx_.get_pl_ctx()->add(result));
        }
      } else {
        result = tmp_result;
        if (OB_NOT_NULL(ctx.exec_ctx_.get_pl_ctx())) {
          ctx.exec_ctx_.get_pl_ctx()->reset_obj_range_to_end(cur_obj_count);
        }
      }
    } else {
      result = tmp_result;
    }
    if (OB_SUCC(ret) && info->is_udt_cons_) {
      pl::ObPLComposite *obj_self = reinterpret_cast<pl::ObPLRecord *>(udf_params->at(0).get_ext());
      CK (OB_NOT_NULL(obj_self));
      if (OB_SUCC(ret) && obj_self->is_record()) {
        OX (obj_self->set_is_null(false));
      }
    }

    if (OB_READ_NOTHING == ret && info->is_called_in_sql_ && lib::is_oracle_mode()) {
      result.set_null();
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)
        && info->is_called_in_sql_
        && result.is_raw() && result.get_raw().length() > OB_MAX_ORACLE_RAW_SQL_COL_LENGTH
        && lib::is_oracle_mode()) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      ObString err_msg("raw variable length too long");
      LOG_WARN("raw variable length too long", K(ret), K(result.get_raw().length()));
      LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, err_msg.length(), err_msg.ptr());
    }
    OZ (process_out_params(objs,
                           expr.arg_cnt_,
                           *udf_params,
                           *alloc,
                           ctx.exec_ctx_,
                           info->nocopy_params_,
                           info->params_desc_,
                           info->params_type_));
    if (OB_SUCC(ret)) {
      if (!result.is_null()
          && result.get_type() != expr.datum_meta_.type_
          && ObLobType == expr.datum_meta_.type_) {
        ObLobLocator *value = nullptr;
        char *total_buf = NULL;
        ObString result_data = result.get_string();
        if (is_lob_storage(result.get_type())) {
          if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator, result, result_data))) {
            LOG_WARN("failed to get real data for lob", K(ret), K(result));
          }
        }
        const int64_t total_buf_len = sizeof(ObLobLocator) + result_data.length();
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(total_buf = expr.get_str_res_mem(ctx, total_buf_len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(total_buf_len));
        } else if (FALSE_IT(value = reinterpret_cast<ObLobLocator *> (total_buf))) {
        } else if (OB_FAIL(value->init(result_data))) {
          LOG_WARN("Failed to init lob locator", K(ret), K(value));
        } else {
          result.set_lob_locator(*value);
        }
      } else if (!result.is_null() && result.get_type() != expr.datum_meta_.type_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get unexpected result type", K(ret),
                         K(result.get_type()), K(expr.datum_meta_.type_));
      }
      OZ(res.from_obj(result, expr.obj_datum_map_));
      if (is_lob_storage(result.get_type())) {
        OZ(ob_adjust_lob_datum(result, expr.obj_meta_, expr.obj_datum_map_,
                              ctx.exec_ctx_.get_allocator(), res));
      }
      OZ(expr.deep_copy_datum(ctx, res));
    }
    if (need_free_udt && info->is_udt_cons_) {
      int tmp = OB_SUCCESS;
      tmp = pl::ObUserDefinedType::destruct_obj(udf_params->at(0), ctx.exec_ctx_.get_my_session());
      if (OB_SUCCESS != tmp) {
        LOG_WARN("fail to free udt self memory", K(ret), K(tmp));
      }
    }
    if (deep_in_objs.count() > 0) {
      int tmp = OB_SUCCESS;
      for (int64_t i = 0; i < deep_in_objs.count(); ++i) {
        tmp = pl::ObUserDefinedType::destruct_obj(deep_in_objs.at(i), ctx.exec_ctx_.get_my_session());
        if (OB_SUCCESS != tmp) {
          LOG_WARN("fail to destruct obj of in param", K(tmp));
        }
      }
    }
    if (need_end_stmt) {
      session->set_end_stmt();
    }
  }

  return ret;
}

int ObExprUDF::fill_obj_stack(const ObExpr &expr, ObEvalCtx &ctx, ObObj *objs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < expr.arg_cnt_ && OB_SUCC(ret); ++i) {
    objs[i].reset();
    ObDatum &param = expr.args_[i]->locate_expr_datum(ctx);
    if (OB_FAIL(param.to_obj(objs[i], expr.args_[i]->obj_meta_))) {
      LOG_WARN("failed to convert obj", K(ret), K(i));
    }
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
              is_called_in_sql_);
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
              is_called_in_sql_);
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
              is_called_in_sql_);
  return len;
}

int ObExprUDFInfo::deep_copy(common::ObIAllocator &allocator,
                         const ObExprOperatorType type,
                         ObIExprExtraInfo *&copied_info) const
{
  int ret = common::OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  ObExprUDFInfo &other = *static_cast<ObExprUDFInfo *>(copied_info);
  other.udf_id_ = udf_id_;
  other.udf_package_id_ = udf_package_id_;
  other.result_type_ = result_type_;
  other.is_udt_udf_ = is_udt_udf_;
  other.loc_ = loc_;
  other.is_udt_cons_ = is_udt_cons_;
  other.is_called_in_sql_ = is_called_in_sql_;
  other.dblink_id_ = dblink_id_;
  OZ(other.subprogram_path_.assign(subprogram_path_));
  OZ(other.params_type_.assign(params_type_));
  OZ(other.params_desc_.assign(params_desc_));
  OZ(other.nocopy_params_.assign(nocopy_params_));
  return ret;
}

template <typename RE>
int ObExprUDFInfo::from_raw_expr(RE &raw_expr)
{
  int ret = OB_SUCCESS;
  ObUDFRawExpr &udf_expr = const_cast<ObUDFRawExpr &> (static_cast<const ObUDFRawExpr&>(raw_expr));
  OZ(subprogram_path_.assign(udf_expr.get_subprogram_path()));
  OZ(params_type_.assign(udf_expr.get_params_type()));
  OZ(params_desc_.assign(udf_expr.get_params_desc()));
  OZ(nocopy_params_.assign(udf_expr.get_nocopy_params()));
  udf_id_ = udf_expr.get_udf_id();
  udf_package_id_ = udf_expr.get_pkg_id();
  result_type_ = udf_expr.get_result_type();
  is_udt_udf_ = udf_expr.get_is_udt_udf();
  loc_ = udf_expr.get_loc();
  is_udt_cons_ = udf_expr.get_is_udt_cons();
  dblink_id_ = udf_expr.get_dblink_id();
  return ret;
}

} /* sql */
} /* oceanbase */
