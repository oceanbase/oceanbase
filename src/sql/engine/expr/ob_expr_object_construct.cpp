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
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_user_type.h"
#include "sql/ob_spi.h"
#include "pl/ob_pl_resolver.h"

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
    if ((ObExtendType == elem_types_.at(i).get_type()
          && types[i].get_type() != ObExtendType
          && types[i].get_type() != ObNullType
          && !types[i].is_xml_sql_type())
        ||((ObExtendType == types[i].get_type() || types[i].is_xml_sql_type()) && elem_types_.at(i).get_type() != ObExtendType)) {
      ret = OB_ERR_CALL_WRONG_ARG;
      LOG_WARN("PLS-00306: wrong number or types of arguments in call", K(ret), K(types[i]), K(elem_types_.at(i)), K(i));
    } else {
      types[i].set_calc_accuracy(elem_types_.at(i).get_accuracy());
      types[i].set_calc_meta(elem_types_.at(i).get_obj_meta());
      types[i].set_calc_type(elem_types_.at(i).get_type());
    }
  }
  OX (type.set_type(ObExtendType));
  OX (type.set_extend_type(pl::PL_RECORD_TYPE));
  OX (type.set_udt_id(udt_id_));
  return ret;
}

int ObExprObjectConstruct::check_types(const ObObj *objs_stack,
                                       const common::ObIArray<ObExprResType> &elem_types,
                                       int64_t param_num)
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
          ret = OB_ERR_CALL_WRONG_ARG;
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

int ObExprObjectConstruct::newx(ObEvalCtx &ctx, ObObj &result, uint64_t udt_id)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  ObIAllocator &alloc = ctx.exec_ctx_.get_allocator();
  pl::ObPLPackageGuard package_guard(session->get_effective_tenant_id());
  ObSchemaGetterGuard *schema_guard_ptr = NULL;
  ObSchemaGetterGuard schema_guard;
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
  if (OB_SUCC(ret)) {
    pl::ObPLResolveCtx resolve_ctx(alloc,
                                  *session,
                                  *(schema_guard_ptr),
                                  package_guard,
                                  *(exec_ctx.get_sql_proxy()),
                                  false);
    pl::ObPLINS *ns = NULL;
    if (NULL == session->get_pl_context()) {
      OZ (package_guard.init());
      OX (ns = &resolve_ctx);
    } else {
      ns = session->get_pl_context()->get_current_ctx();
    }
    if (OB_SUCC(ret)) {
      ObObj new_composite;
      int64_t ptr = 0;
      int64_t init_size = OB_INVALID_SIZE;
      ObArenaAllocator tmp_alloc;
      const pl::ObUserDefinedType *user_type = NULL;
      CK (OB_NOT_NULL(ns));
      OZ (ns->get_user_type(udt_id, user_type, &tmp_alloc));
      CK (OB_NOT_NULL(user_type));
      OZ (user_type->newx(alloc, ns, ptr));
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
  pl::ObPLRecord *record = NULL;
  const ObExprObjectConstructInfo *info
                  = static_cast<ObExprObjectConstructInfo *>(expr.extra_info_);
  ObObj result;
  ObSQLSessionInfo *session = nullptr;
  CK(OB_NOT_NULL(info));
  CK(expr.arg_cnt_ >= info->elem_types_.count());
  CK(OB_NOT_NULL(session = ctx.exec_ctx_.get_my_session()));
  ObObj *objs = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("failed to eval param ", K(ret));
  } else if (expr.arg_cnt_ > 0
     && OB_ISNULL(objs = static_cast<ObObj *>
        (ctx.exec_ctx_.get_allocator().alloc(expr.arg_cnt_ * sizeof(ObObj))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc mem for objs", K(ret));
  } else if (OB_FAIL(fill_obj_stack(expr, ctx, objs))) {
    LOG_WARN("failed to convert obj", K(ret));
  } else if (expr.arg_cnt_ > 0 && OB_FAIL(check_types(objs, info->elem_types_, expr.arg_cnt_))) {
    LOG_WARN("failed to check types", K(ret));
  } else if (info->rowsize_ != pl::ObRecordType::get_init_size(expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowsize_ is not equel to input", K(ret), K(info->rowsize_), K(expr.arg_cnt_));
  } else if (OB_ISNULL(record
           = static_cast<pl::ObPLRecord*>
             (ctx.exec_ctx_.get_allocator().alloc(pl::ObRecordType::get_init_size(expr.arg_cnt_))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else {
    new(record)pl::ObPLRecord(info->udt_id_, expr.arg_cnt_);
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      if (objs[i].is_null() && info->elem_types_.at(i).is_ext()) {
        OZ (newx(ctx, record->get_element()[i], info->elem_types_.at(i).get_udt_id()));
        if (OB_SUCC(ret)) {
          // use _is_null to distinguish the following two situations:
          // SDO_GEOMETRY(2003, 4000, SDO_POINT_TYPE(NULL,NULL,NULL), NULL, NULL)
          // SDO_GEOMETRY(2003, 4000, NULL, NULL, NULL)
          pl::ObPLRecord *child_null_record =
            reinterpret_cast<pl::ObPLRecord *>(record->get_element()[i].get_ext());
          child_null_record->set_null();
        }
      } else {
        // param ObObj may have different accuracy with the argument, need conversion
        OZ (ObSPIService::spi_convert(*session,
                                      ctx.exec_ctx_.get_allocator(),
                                      objs[i],
                                      info->elem_types_.at(i),
                                      record->get_element()[i],
                                      false));
      }
      if (OB_SUCC(ret) &&
          (ObCharType == info->elem_types_.at(i).get_type() || ObNCharType == info->elem_types_.at(i).get_type())) {
        OZ (ObSPIService::spi_pad_char_or_varchar(session,
                                                  info->elem_types_.at(i).get_type(),
                                                  info->elem_types_.at(i).get_accuracy(),
                                                  &ctx.exec_ctx_.get_allocator(),
                                                  &(record->get_element()[i])));
      }
    }
    result.set_extend(reinterpret_cast<int64_t>(record),
                      pl::PL_RECORD_TYPE, pl::ObRecordType::get_init_size(expr.arg_cnt_));
    OZ(res.from_obj(result, expr.obj_datum_map_));
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

template <typename RE>
int ObExprObjectConstructInfo::from_raw_expr(RE &raw_expr)
{
  int ret = OB_SUCCESS;
  ObObjectConstructRawExpr &pl_expr
        = const_cast<ObObjectConstructRawExpr &>
            (static_cast<const ObObjectConstructRawExpr &>(raw_expr));
  rowsize_ = pl_expr.get_rowsize();
  udt_id_ = pl_expr.get_udt_id();
  OZ(elem_types_.assign(pl_expr.get_elem_types()));
  return ret;
}

} /* sql */
} /* oceanbase */
