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

#define USING_LOG_PREFIX  SQL_ENG

#include "ob_expr_get_package_var.h"
#include "pl/ob_pl_package.h"

namespace oceanbase
{
using namespace common;
using namespace pl;
namespace sql
{

ObExprGetPackageVar::ExtraInfo::ExtraInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
  : ObIExprExtraInfo(alloc, type),
  package_id_(OB_INVALID_INDEX),
  var_idx_(OB_INVALID_INDEX)
{
}

OB_SERIALIZE_MEMBER(ObExprGetPackageVar::ExtraInfo,
  package_id_,
  var_idx_);

void ObExprGetPackageVar::ExtraInfo::reset()
{
  package_id_ = OB_INVALID_INDEX;
  var_idx_ = OB_INVALID_INDEX;
}

int ObExprGetPackageVar::ExtraInfo::deep_copy(common::ObIAllocator &allocator,
                             const ObExprOperatorType type,
                             ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  ExtraInfo &other = *static_cast<ExtraInfo *>(copied_info);
  if (OB_SUCC(ret)) {
    OZ(other.assign(*this));
  }
  return ret;
}

int ObExprGetPackageVar::ExtraInfo::assign(const ObExprGetPackageVar::ExtraInfo &other)
{
  int ret = OB_SUCCESS;

  package_id_ = other.package_id_;
  var_idx_ = other.var_idx_;

  return ret;
}

int ObExprGetPackageVar::ExtraInfo::from_raw_expr(const ObRawExpr &raw_expr)
{
  int ret = 0;

  CK(3 == raw_expr.get_param_count());
  OX (package_id_ = static_cast<const ObConstRawExpr *>(raw_expr.get_param_expr(0))->get_value().get_uint64());
  OX (var_idx_ = static_cast<const ObConstRawExpr *>(raw_expr.get_param_expr(1))->get_value().get_int());

  return ret;
}

int ObExprGetPackageVar::calc(ObObj &result,
                              uint64_t package_id,
                              int64_t var_idx,
                              ObExecContext *exec_ctx,
                              ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = NULL;
  ObPL *pl_engine = NULL;
  ObPLPackageGuard *package_guard = NULL;
  share::schema::ObSchemaGetterGuard *schema_guard = NULL;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else if (OB_ISNULL(sql_proxy = exec_ctx->get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_ISNULL(pl_engine = session_info->get_pl_engine())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pl engine is null", K(ret));
  } else if (OB_FAIL(exec_ctx->get_package_guard(package_guard))) {
    LOG_WARN("get package guard failed", K(ret));
  } else if (OB_ISNULL(package_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("package guard is null", K(ret));
  } else if (OB_NOT_NULL(exec_ctx->get_sql_ctx())
             && OB_NOT_NULL(exec_ctx->get_sql_ctx()->schema_guard_)) {
    schema_guard = exec_ctx->get_sql_ctx()->schema_guard_;
  }
  if (OB_SUCC(ret) && OB_ISNULL(schema_guard)) {
    schema_guard = &session_info->get_cached_schema_guard_info().get_schema_guard();
  }
  if (OB_SUCC(ret)) {
    ObPLResolveCtx resolve_ctx(exec_ctx->get_allocator(),
                               *session_info,
                               *schema_guard,
                               *package_guard,
                               *sql_proxy,
                               false);
    ObPLPackageManager &package_manager = pl_engine->get_package_manager();
    if (OB_FAIL(package_manager.get_package_var_val(
        resolve_ctx, *exec_ctx, package_id, var_idx, result))) {
      LOG_WARN("get package var failed", K(ret));
    }
  } 
  return ret;
}

int ObExprGetPackageVar::calc_result_typeN(ObExprResType &type,
                                           ObExprResType *types_stack,
                                           int64_t param_num,
                                           ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType *result_type = reinterpret_cast<ObExprResType *>(types_stack[2].get_param().get_int());
  type.set_type(result_type->get_type());
  if (ob_is_string_tc(result_type->get_type())
      || ob_is_raw_tc(result_type->get_type())) {
    type.set_length(result_type->get_length());
    type.set_length_semantics(result_type->get_length_semantics());
    type.set_collation_type(result_type->get_collation_type());
    type.set_collation_level(result_type->get_collation_level());
  } else if (ob_is_number_tc(result_type->get_type()) ||
             ob_is_interval_tc(result_type->get_type()) ||
             ob_is_decimal_int_tc(result_type->get_type())) {
    type.set_precision(result_type->get_precision());
    type.set_scale(result_type->get_scale());
  } else if (ob_is_text_tc(result_type->get_type())
             || ob_is_lob_tc(result_type->get_type())) {
    type.set_length(result_type->get_length());
    type.set_collation_type(result_type->get_collation_type());
    type.set_collation_level(result_type->get_collation_level());
    type.set_scale(result_type->get_scale());
  } else if (ob_is_extend(result_type->get_type())) {
    type.set_extend_type(result_type->get_extend_type());
  }
  return ret;
}

int ObExprGetPackageVar::eval_get_package_var(const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *package_id_datum = NULL;
  ObDatum *var_idx_datum = NULL;
  uint64_t package_id = OB_INVALID_INDEX;
  int64_t var_idx = OB_INVALID_INDEX;
  const ExtraInfo *info = static_cast<const ExtraInfo *>(expr.extra_info_);
  if (OB_ISNULL(info) || info->package_id_ == OB_INVALID_INDEX || info->var_idx_ == OB_INVALID_INDEX) {
    OZ (expr.eval_param_value(ctx, package_id_datum, var_idx_datum));
    OX (package_id = package_id_datum->get_uint());
    OX (var_idx = var_idx_datum->get_int());
  } else {
    package_id = info->package_id_;
    var_idx = info->var_idx_;
  }
  if(OB_SUCC(ret)) {
    ObObj res_obj;
    OZ(calc(res_obj,
            package_id,
            var_idx,
            &ctx.exec_ctx_,
            ctx.exec_ctx_.get_my_session()),
            K(package_id), K(var_idx));
    if (OB_SUCC(ret)) {
      if (!res_obj.is_null() && res_obj.get_type() != expr.obj_meta_.get_type()) { // todo: need collect pkg basic type var dependency info
        ret = OB_ERR_WRONG_TYPE_FOR_VAR;
        LOG_WARN("result type no match with result type", K(ret), K(res_obj), K(expr.obj_meta_));
      } else if (expr.is_called_in_sql_ && ob_is_string_tc(res_obj.get_type())) {
        ObString res_str;
        ObExprStrResAlloc res_alloc(expr, ctx);
        OZ(res_obj.get_string(res_str));
        OZ(ObExprUtil::deep_copy_str(res_str, res_str, res_alloc));
        OX(res.set_string(res_str));
      } else if (expr.is_called_in_sql_ && ob_is_text_tc(res_obj.get_type())) {
        if (res_obj.has_lob_header() != expr.obj_meta_.has_lob_header()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid lob header", K(ret), K(res_obj.has_lob_header()), K(expr.obj_meta_.has_lob_header()));
        } else {
          ObString res_str;
          ObExprStrResAlloc res_alloc(expr, ctx);
          res_str = res_obj.get_string();
          OZ(ObExprUtil::deep_copy_str(res_str, res_str, res_alloc));
          if (OB_SUCC(ret)) {
            res.set_string(res_str);
          }
        }
      } else {
        OZ(res.from_obj(res_obj));
        OZ(expr.deep_copy_datum(ctx, res));
        if (is_lob_storage(res_obj.get_type())) {
          OZ(ob_adjust_lob_datum(res_obj, expr.obj_meta_, ctx.exec_ctx_.get_allocator(), res));
        }
      }
    }
  }
  return ret;
}

int ObExprGetPackageVar::cg_expr(ObExprCGCtx &ctx,
                                 const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;

  ObIAllocator &alloc = *ctx.allocator_;
  ExtraInfo *info = OB_NEWx(ExtraInfo, (&alloc), alloc, T_OP_GET_PACKAGE_VAR);
  if (NULL == info) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    CK(3 == rt_expr.arg_cnt_);
    OX (info->package_id_ = static_cast<const ObConstRawExpr *>(raw_expr.get_param_expr(0))->get_value().get_uint64());
    OX (info->var_idx_ = static_cast<const ObConstRawExpr *>(raw_expr.get_param_expr(1))->get_value().get_int());
    OX (rt_expr.extra_info_ = info);
    OX (rt_expr.eval_func_ = eval_get_package_var);
  }
  return ret;
}
} //namespace sql
} //namespace oceanbase
