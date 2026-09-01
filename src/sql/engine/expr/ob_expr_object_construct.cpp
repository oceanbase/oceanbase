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

#include "ob_expr_object_construct.h"
#include "pl/ob_pl_resolver.h"
#include "sql/engine/expr/ob_expr_sql_udt_utils.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObExprObjectConstruct, ObFuncExprOperator), rowsize_, elem_types_, udt_id_);

ObExprObjectConstruct::ObExprObjectConstruct(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_PL_OBJECT_CONSTRUCT, N_PL_OBJECT_CONSTRUCT, PARAM_NUM_UNKNOWN, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE),
      rowsize_(0),
      udt_id_(OB_INVALID_ID),
      elem_types_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc)) {}

ObExprObjectConstruct::~ObExprObjectConstruct() {}

int ObExprObjectConstruct::calc_result_typeN(ObExprResType &type,
                                             ObExprResType *types,
                                             int64_t param_num,
                                             ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED (type_ctx);
  CK (param_num == elem_types_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
    // Treat a user-defined sql_udt input (including xml) as compatible with an EXT formal
    // when their udt_id matches. The framework will add an implicit UDT->EXT cast below.
    const bool sql_udt_matches_ext_elem =
        ObExtendType == elem_types_.at(i).get_type()
        && types[i].is_user_defined_sql_type()
        && elem_types_.at(i).get_udt_id() == types[i].get_udt_id();
    if ((ObExtendType == elem_types_.at(i).get_type()
          && types[i].get_type() != ObExtendType
          && types[i].get_type() != ObNullType
          && !sql_udt_matches_ext_elem)
        ||((ObExtendType == types[i].get_type() || types[i].is_user_defined_sql_type())
           && elem_types_.at(i).get_type() != ObExtendType)) {
      ObSchemaGetterGuard schema_guard;
      int64_t tenant_id = type_ctx.get_session()->get_effective_tenant_id();
      const ObUDTTypeInfo *udt_info = NULL;
      OZ (GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
      OZ (schema_guard.get_udt_info(tenant_id, udt_id_, udt_info));
      if (OB_SUCC(ret)) {
        ret = OB_ERR_CALL_WRONG_ARG;
        if (OB_NOT_NULL(udt_info)) {
          LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, udt_info->get_type_name().length(), udt_info->get_type_name().ptr());
        }
      }
      LOG_WARN("PLS-00306: wrong number or types of arguments in call", K(ret), K(types[i]), K(elem_types_.at(i)), K(i));
    } else if (sql_udt_matches_ext_elem && !types[i].is_xml_sql_type()) {
      // keep sql udt type as-is, no cast needed
    } else {
      types[i].set_calc_accuracy(elem_types_.at(i).get_accuracy());
      types[i].set_calc_meta(elem_types_.at(i).get_obj_meta());
      types[i].set_calc_type(elem_types_.at(i).get_type());
    }
  }
  if (OB_SUCC(ret)) {
    ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
    ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
    if (is_called_in_sql()
        && !is_inner_pl_udt_id(udt_id_)
        && OB_NOT_NULL(exec_ctx)
        && pl::ObPLDataType::is_schema_udt(exec_ctx->get_sql_ctx()->schema_guard_, udt_id_)
        && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_4_2_3
        && OB_NOT_NULL(session)
        && !session->disable_sql_udt_deduce_in_pl()
        && session->get_local_enable_pl_composite_as_sql_udt()) {
      type.set_type(ObUserDefinedSQLType);
      type.set_udt_id(udt_id_);
      uint16_t subschema_id = ObInvalidSqlType;
      if (OB_FAIL(exec_ctx->get_subschema_id_by_udt_id(udt_id_, subschema_id))) {
        LOG_WARN("failed to get subschema id", K(ret), K(udt_id_));
      }
      OX (type.set_sql_udt(subschema_id));
    } else {
      type.set_type(ObExtendType);
      type.set_extend_type(pl::PL_RECORD_TYPE);
      type.set_udt_id(udt_id_);
    }
  }
  return ret;
}

int ObExprObjectConstruct::check_types(ObEvalCtx &ctx, const ObObj *objs_stack,
                                       const common::ObIArray<ObExprResType> &elem_types,
                                       int64_t param_num,
                                       uint64_t udt_id)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(objs_stack));
  CK (OB_LIKELY(param_num == elem_types.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
    if (!objs_stack[i].is_null()) {
      TYPE_CHECK(objs_stack[i], elem_types.at(i).get_type());
      if (objs_stack[i].is_pl_extend()
        && objs_stack[i].get_meta().get_extend_type() != pl::PL_OPAQUE_TYPE
        && objs_stack[i].get_meta().get_extend_type() != pl::PL_CURSOR_TYPE
        && objs_stack[i].get_meta().get_extend_type() != pl::PL_REF_CURSOR_TYPE) {
        pl::ObPLComposite *composite = reinterpret_cast<pl::ObPLComposite*>(objs_stack[i].get_ext());
        CK (OB_NOT_NULL(composite));
        if (OB_SUCC(ret) && composite->get_id() != elem_types.at(i).get_udt_id()) {
          ObSchemaGetterGuard schema_guard;
          int64_t tenant_id = ctx.exec_ctx_.get_my_session()->get_effective_tenant_id();
          const ObUDTTypeInfo *udt_info = NULL;
          OZ (GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
          // Use the actual called type name instead of the expected parameter type name
          OZ (schema_guard.get_udt_info(pl::get_tenant_id_by_object_id(udt_id) , udt_id, udt_info));
          if (OB_SUCC(ret)) {
            ret = OB_ERR_CALL_WRONG_ARG;
            if (OB_NOT_NULL(udt_info)) {
              LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, udt_info->get_type_name().length(), udt_info->get_type_name().ptr());
            }
          }
          LOG_WARN("invalid argument. unexpected obj type", K(ret), KPC(composite), K(elem_types), K(i));
        }
      }
    }
  }
  return ret;
}

int ObExprObjectConstruct::cg_expr(ObExprCGCtx &op_cg_ctx,
                                   const ObRawExpr &raw_expr,
                                   ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObIAllocator &alloc = *op_cg_ctx.allocator_;
  const ObObjectConstructRawExpr &fun_sys
                      = static_cast<const ObObjectConstructRawExpr &>(raw_expr);
  ObExprObjectConstructInfo *info
              = OB_NEWx(ObExprObjectConstructInfo, (&alloc), alloc, T_FUN_PL_OBJECT_CONSTRUCT);
  if (NULL == info) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    OZ(info->from_raw_expr(fun_sys));
    rt_expr.extra_info_ = info;
  }
  rt_expr.eval_func_ = eval_object_construct;
 
  return ret;
}

int ObExprObjectConstruct::newx(ObEvalCtx &ctx, ObObj &result, uint64_t udt_id, ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  pl::ObPLPackageGuard package_guard(session->get_effective_tenant_id());
  ObSchemaGetterGuard *schema_guard_ptr = NULL;
  ObSchemaGetterGuard schema_guard;
  ObArenaAllocator tmp_alloc;
  CK (OB_NOT_NULL(alloc));
  if (OB_SUCC(ret)) {
    // if called by check_default_value in ddl resolver, no sql ctx, get guard from session cache
    if (OB_ISNULL(exec_ctx.get_sql_ctx()) || OB_ISNULL(exec_ctx.get_sql_ctx()->schema_guard_)) {
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(session->get_effective_tenant_id(), schema_guard))) {
        LOG_WARN("fail to get schema guard", K(ret));
      } else {
        schema_guard_ptr = &schema_guard;
      }
    } else {
      schema_guard_ptr = exec_ctx.get_sql_ctx()->schema_guard_;
    }
  }
  if (OB_SUCC(ret)) {
    pl::ObPLResolveCtx resolve_ctx(tmp_alloc,
                                  *session,
                                  *(schema_guard_ptr),
                                  package_guard,
                                  *(exec_ctx.get_sql_proxy()),
                                  false);
    pl::ObPLINS *ns = NULL;
    if (NULL == session->get_pl_top_context()) {
      OZ (package_guard.init());
      OX (ns = &resolve_ctx);
    } else {
      ns = session->get_pl_top_context()->get_current_ctx();
    }
    if (OB_SUCC(ret)) {
      ObObj new_composite;
      int64_t ptr = 0;
      int64_t init_size = OB_INVALID_SIZE;
      const pl::ObUserDefinedType *user_type = NULL;
      CK (OB_NOT_NULL(ns));
      OZ (ns->get_user_type(udt_id, user_type, &tmp_alloc));
      CK (OB_NOT_NULL(user_type));
      OZ (user_type->newx(*alloc, ns, ptr, true/*set_null*/));
      OZ (user_type->get_size(pl::PL_TYPE_INIT_SIZE, init_size));
      OX (new_composite.set_extend(ptr, user_type->get_type(), init_size));
      OX (result = new_composite);
    }
  }
  return ret;
}

int ObExprObjectConstruct::eval_object_construct(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  const ObExprObjectConstructInfo *info
                  = static_cast<ObExprObjectConstructInfo *>(expr.extra_info_);
  ObSQLSessionInfo *session = nullptr;
  CK(OB_NOT_NULL(info));
  CK(expr.arg_cnt_ >= info->elem_types_.count());
  CK(OB_NOT_NULL(session = ctx.exec_ctx_.get_my_session()));
  if (OB_FAIL(ret)) {
  } else if (expr.is_called_in_sql_
             && expr.obj_meta_.is_user_defined_sql_type()) {
    OZ (eval_object_construct_sql_udt(expr, ctx, res, info, session));
  } else {
    OZ (eval_object_construct_pl_extend(expr, ctx, res, info, session));
  }
  return ret;
}

int ObExprObjectConstruct::eval_object_construct_sql_udt(const ObExpr &expr,
                                                         ObEvalCtx &ctx,
                                                         ObDatum &res,
                                                         const ObExprObjectConstructInfo *info,
                                                         ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  ObIAllocator &tmp_alloc = tmp_alloc_g.get_allocator();
  pl::ObPLRecord *record = NULL;
  ObObj *objs = nullptr;
  ObSEArray<ObObj, 4> tmp_sql_udt_exts;

  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("failed to eval param", K(ret));
  } else if (expr.arg_cnt_ > 0
     && OB_ISNULL(objs = static_cast<ObObj *>(tmp_alloc.alloc(expr.arg_cnt_ * sizeof(ObObj))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc mem for objs", K(ret));
  } else if (OB_FAIL(fill_obj_stack(expr, ctx, objs))) {
    LOG_WARN("failed to convert obj", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    if (objs[i].is_common_user_defined_sql_type() && !objs[i].is_null()) {
      ObObj extend_v;
      ObSqlUDTMeta udt_meta;
      uint16_t subschema_id = objs[i].get_meta().get_subschema_id();
      OZ (ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, udt_meta));
      OZ (ObSqlUdtUtils::sql_udt_deserialize_to_pl_extend(&ctx.exec_ctx_, extend_v, objs[i], udt_meta, &tmp_alloc));
      OZ (tmp_sql_udt_exts.push_back(extend_v));
      OX (objs[i] = extend_v);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (expr.arg_cnt_ > 0 && OB_FAIL(check_types(ctx, objs, info->elem_types_, expr.arg_cnt_, info->udt_id_))) {
    LOG_WARN("failed to check types", K(ret));
  } else if (info->rowsize_ != pl::ObRecordType::get_init_size(expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowsize_ is not equel to input", K(ret), K(info->rowsize_), K(expr.arg_cnt_));
  } else if (OB_ISNULL(record = static_cast<pl::ObPLRecord*>(
               tmp_alloc.alloc(pl::ObRecordType::get_init_size(expr.arg_cnt_))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else {
    new(record)pl::ObPLRecord(info->udt_id_, expr.arg_cnt_);
    OZ (record->init_data(tmp_alloc, false));
    CK (OB_NOT_NULL(record->get_allocator()));
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      if (objs[i].is_null() && info->elem_types_.at(i).is_ext()) {
        OZ (newx(ctx, record->get_element()[i], info->elem_types_.at(i).get_udt_id(), record->get_allocator()));
        if (OB_SUCC(ret)) {
          pl::ObPLRecord *child_null_record =
            reinterpret_cast<pl::ObPLRecord *>(record->get_element()[i].get_ext());
          child_null_record->set_null();
        }
      } else {
        if (OB_SUCC(ret) &&
            (ObCharType == info->elem_types_.at(i).get_type() || ObNCharType == info->elem_types_.at(i).get_type())) {
          OZ (ObSPIService::spi_pad_char_or_varchar(session,
                                                    info->elem_types_.at(i).get_type(),
                                                    info->elem_types_.at(i).get_accuracy(),
                                                    &tmp_alloc,
                                                    &(objs[i])));
        }
        ObObj tmp;
        OZ (ObSPIService::spi_convert(*session, tmp_alloc, objs[i],
                                      info->elem_types_.at(i), tmp, false));
        if (OB_FAIL(ret)) {
        } else if (tmp.is_ext()) {
          OZ (pl::ObUserDefinedType::deep_copy_obj(*record->get_allocator(), tmp,
                                                    record->get_element()[i]));
        } else {
          OZ (deep_copy_obj(*record->get_allocator(), tmp, record->get_element()[i]));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObObj extend_obj;
      extend_obj.set_extend(reinterpret_cast<int64_t>(record),
                            pl::PL_RECORD_TYPE, pl::ObRecordType::get_init_size(expr.arg_cnt_));
      ObExprStrResAlloc expr_res_alloc(expr, ctx);
      ObString res_str;
      ObSqlUDTMeta sql_udt_meta;
      uint16_t subschema_id = expr.obj_meta_.get_subschema_id();
      if (ObInvalidSqlType == subschema_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid subschema id", K(ret), K(expr.obj_meta_), K(info->udt_id_));
      }
      OZ (ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, sql_udt_meta));
      OZ (ObSqlUdtUtils::pl_extend_serialize_to_sql_udt(
          expr_res_alloc, &ctx.exec_ctx_, res_str, extend_obj, sql_udt_meta));
      if (OB_SUCC(ret)) {
        if (res_str.empty()) {
          res.set_null();
        } else {
          ObObj sql_udt_obj;
          sql_udt_obj.set_sql_udt(res_str.ptr(), res_str.length(), subschema_id);
          sql_udt_obj.set_has_lob_header();
          OZ (res.from_obj(sql_udt_obj, expr.obj_datum_map_));
        }
      }
      pl::ObUserDefinedType::destruct_obj(extend_obj, nullptr);
    }
  }
  for (int64_t i = 0; i < tmp_sql_udt_exts.count(); ++i) {
    int tmp_ret = pl::ObUserDefinedType::destruct_obj(tmp_sql_udt_exts.at(i), nullptr);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to destruct tmp sql udt extend", K(tmp_ret), K(i));
    }
  }
  return ret;
}

int ObExprObjectConstruct::eval_object_construct_pl_extend(const ObExpr &expr,
                                                           ObEvalCtx &ctx,
                                                           ObDatum &res,
                                                           const ObExprObjectConstructInfo *info,
                                                           ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  pl::ObPLRecord *record = NULL;
  ObObj result;
  ObObj *objs = nullptr;
  ObSEArray<ObObj, 4> tmp_sql_udt_exts;
  ObIAllocator *alloc = nullptr;
  pl::ObPLExecCtx *pl_exec_ctx = nullptr;
  ObPLComplexTypeMgr *pl_complex_type_mgr = nullptr;
  OZ (ctx.get_pl_complex_type_mgr(pl_complex_type_mgr));
  OX (alloc = &pl_complex_type_mgr->alloc_);
  if (OB_NOT_NULL(session) &&
      OB_NOT_NULL(session->get_pl_top_context()) &&
      OB_NOT_NULL(pl_exec_ctx = session->get_pl_top_context()->get_current_ctx()) &&
      pl_exec_ctx->get_exec_ctx() == &ctx.exec_ctx_) {
    alloc = pl_exec_ctx->get_top_expr_allocator();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("failed to eval param ", K(ret));
  } else if (expr.arg_cnt_ > 0
     && OB_ISNULL(objs = static_cast<ObObj *>
        (alloc->alloc(expr.arg_cnt_ * sizeof(ObObj))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc mem for objs", K(ret));
  } else if (OB_FAIL(fill_obj_stack(expr, ctx, objs))) {
    LOG_WARN("failed to convert obj", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    if (objs[i].is_common_user_defined_sql_type() && !objs[i].is_null()) {
      ObObj extend_v;
      ObSqlUDTMeta udt_meta;
      uint16_t subschema_id = objs[i].get_meta().get_subschema_id();
      OZ (ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, udt_meta));
      OZ (ObSqlUdtUtils::sql_udt_deserialize_to_pl_extend(&ctx.exec_ctx_, extend_v, objs[i], udt_meta, alloc));
      OZ (tmp_sql_udt_exts.push_back(extend_v));
      OX (objs[i] = extend_v);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (expr.arg_cnt_ > 0 && OB_FAIL(check_types(ctx, objs, info->elem_types_, expr.arg_cnt_, info->udt_id_))) {
    LOG_WARN("failed to check types", K(ret));
  } else if (info->rowsize_ != pl::ObRecordType::get_init_size(expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowsize_ is not equel to input", K(ret), K(info->rowsize_), K(expr.arg_cnt_));
  } else if (OB_ISNULL(record
           = static_cast<pl::ObPLRecord*>
             (alloc->alloc(pl::ObRecordType::get_init_size(expr.arg_cnt_))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else {
    new(record)pl::ObPLRecord(info->udt_id_, expr.arg_cnt_);
    OZ (record->init_data(*alloc, false));
    CK (OB_NOT_NULL(record->get_allocator()));
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      if (objs[i].is_null() && info->elem_types_.at(i).is_ext()) {
        OZ (newx(ctx, record->get_element()[i], info->elem_types_.at(i).get_udt_id(), record->get_allocator()));
        if (OB_SUCC(ret)) {
          pl::ObPLRecord *child_null_record =
            reinterpret_cast<pl::ObPLRecord *>(record->get_element()[i].get_ext());
          child_null_record->set_null();
        }
      } else {
        if (OB_SUCC(ret) &&
            (ObCharType == info->elem_types_.at(i).get_type() || ObNCharType == info->elem_types_.at(i).get_type())) {
          OZ (ObSPIService::spi_pad_char_or_varchar(session,
                                                    info->elem_types_.at(i).get_type(),
                                                    info->elem_types_.at(i).get_accuracy(),
                                                    alloc,
                                                    &(objs[i])));
        }
        ObObj tmp;
        OZ (ObSPIService::spi_convert(*session, *alloc, objs[i],
                                      info->elem_types_.at(i), tmp, false));
        if (OB_FAIL(ret)) {
        } else if (tmp.is_ext()) {
          OZ (pl::ObUserDefinedType::deep_copy_obj(*record->get_allocator(), tmp,
                                                    record->get_element()[i]));
        } else {
          OZ (deep_copy_obj(*record->get_allocator(), tmp, record->get_element()[i]));
        }
      }
    }
    result.set_extend(reinterpret_cast<int64_t>(record),
                      pl::PL_RECORD_TYPE, pl::ObRecordType::get_init_size(expr.arg_cnt_));
    OZ(res.from_obj(result, expr.obj_datum_map_));
    if (OB_NOT_NULL(record->get_allocator())) {
      int tmp_ret = pl_complex_type_mgr->complex_type_objects_.push_back(result);
      if (tmp_ret != OB_SUCCESS) {
        int tmp = pl::ObUserDefinedType::destruct_obj(result, nullptr);
        LOG_WARN("fail to collect pl collection allocator, try to free memory", K(tmp_ret), K(tmp));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  for (int64_t i = 0; i < tmp_sql_udt_exts.count(); ++i) {
    int tmp_ret = pl::ObUserDefinedType::destruct_obj(tmp_sql_udt_exts.at(i), nullptr);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to destruct tmp sql udt extend", K(tmp_ret), K(i));
    }
  }
  return ret;
}

int ObExprObjectConstruct::fill_obj_stack(const ObExpr &expr, ObEvalCtx &ctx, ObObj *objs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < expr.arg_cnt_ && OB_SUCC(ret); ++i) {
    ObDatum &param = expr.locate_param_datum(ctx, i);
    if (OB_FAIL(param.to_obj(objs[i], expr.args_[i]->obj_meta_))) {
      LOG_WARN("failed to convert obj", K(ret), K(i));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObExprObjectConstructInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              rowsize_,
              udt_id_,
              elem_types_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExprObjectConstructInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              rowsize_,
              udt_id_,
              elem_types_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprObjectConstructInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              rowsize_,
              udt_id_,
              elem_types_);
  return len;
}

int ObExprObjectConstructInfo::deep_copy(common::ObIAllocator &allocator,
                                         const ObExprOperatorType type,
                                         ObIExprExtraInfo *&copied_info) const
{
  int ret = common::OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  ObExprObjectConstructInfo &other = *static_cast<ObExprObjectConstructInfo *>(copied_info);
  other.rowsize_ = rowsize_;
  other.udt_id_ = udt_id_;
  OZ(other.elem_types_.assign(elem_types_));
  return ret;
}

int ObExprObjectConstructInfo::from_raw_expr(const ObObjectConstructRawExpr &pl_expr)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObRawExprResType> &elem_types = pl_expr.get_elem_types();
  rowsize_ = pl_expr.get_rowsize();
  udt_id_ = pl_expr.get_udt_id();
  OZ(elem_types_.init(elem_types.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < elem_types.count(); ++i) {
    OZ(elem_types_.push_back(elem_types.at(i)));
  }
  return ret;
}

} /* sql */
} /* oceanbase */
