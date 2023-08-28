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

#include "ob_pl_expr_subquery.h"
#include "observer/ob_server.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_spi.h"
#include "observer/ob_inner_sql_result.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_user_type.h"
#include "pl/ob_pl_resolver.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER(
    (ObExprOpSubQueryInPl, ObFuncExprOperator),
    id_, type_, route_sql_, result_type_, is_ignore_fail_, ps_sql_);


ObExprOpSubQueryInPl::ObExprOpSubQueryInPl(common::ObIAllocator &alloc)
    : ObFuncExprOperator(
        alloc, T_FUN_SUBQUERY, N_PL_SUBQUERY_CONSTRUCT,
        PARAM_NUM_UNKNOWN, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION, INTERNAL_IN_MYSQL_MODE),
      id_(common::OB_INVALID_ID),
      ps_sql_(ObString()),
      type_(stmt::T_NONE),
      route_sql_(ObString()),
      result_type_(),
      is_ignore_fail_(false),
      allocator_(alloc) {}

ObExprOpSubQueryInPl::~ObExprOpSubQueryInPl() {}

int ObExprOpSubQueryInPl::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  const ObExprOpSubQueryInPl *tmp_other = dynamic_cast<const ObExprOpSubQueryInPl*>(&other);
  if (OB_UNLIKELY(OB_ISNULL(tmp_other))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cast failed, type of argument is wrong", K(ret), K(other));
  } else if (OB_LIKELY(this != tmp_other)) {
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObExprOperator failed", K(other), K(ret));
    } else {
      OZ (deep_copy_ps_sql(tmp_other->ps_sql_));
      OX (this->type_ = tmp_other->type_);
      OX (this->result_type_ = tmp_other->result_type_);
      OZ (deep_copy_route_sql(tmp_other->route_sql_));
      OX (this->is_ignore_fail_ = tmp_other->is_ignore_fail_);
    }
  }
  return ret;
}

int ObExprOpSubQueryInPl::calc_result_typeN(ObExprResType &type,
                                            ObExprResType *types,
                                            int64_t param_num,
                                            ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(types);
  UNUSED(param_num);
  UNUSED(type_ctx);

  OX (type.set_type(result_type_.get_type()));
  OX (type.set_accuracy(result_type_.get_accuracy()));
  OX (type.set_meta(result_type_.get_obj_meta()));
  OX (type.set_length(result_type_.get_length()));
  return ret;
}

int ObExprOpSubQueryInPl::cg_expr(ObExprCGCtx &op_cg_ctx,
                                  const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;

  ObIAllocator &alloc = *op_cg_ctx.allocator_;
  const ObPlQueryRefRawExpr &fun_sys = static_cast<const ObPlQueryRefRawExpr &>(raw_expr);
  ObExprPlSubQueryInfo *info = OB_NEWx(ObExprPlSubQueryInfo, (&alloc), alloc, T_FUN_SUBQUERY);
  if (NULL == info) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    OZ(info->from_raw_expr(fun_sys, alloc));
    rt_expr.extra_info_ = info;
    rt_expr.eval_func_ = eval_subquery;
  }
  return ret;
}

int ObExprOpSubQueryInPl::eval_subquery(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        ObDatum &res)
{
  int ret = OB_SUCCESS;

  void *param_buf = nullptr;
  ParamStore *params = nullptr;
  ObSQLSessionInfo *session = nullptr;
  ObObj result;

  ObIAllocator &alloc = ctx.exec_ctx_.get_allocator();
  const ObExprPlSubQueryInfo *info = static_cast<ObExprPlSubQueryInfo *>(expr.extra_info_);
  ObObj *objs = nullptr;
  CK(0 == expr.arg_cnt_ ||
     OB_NOT_NULL(objs = static_cast<ObObj *> (alloc.alloc(expr.arg_cnt_ * sizeof(ObObj)))));
  CK(OB_NOT_NULL(info));
  CK(OB_NOT_NULL(session = ctx.exec_ctx_.get_my_session()));
  CK (OB_NOT_NULL(ctx.exec_ctx_.get_sql_ctx()));

  observer::ObQueryRetryCtrl retry_ctrl;
  int64_t tenant_version = 0;
  int64_t sys_version = 0;
  bool is_stack_overflow = false;

  if (info->id_ != common::OB_INVALID_ID) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "observer version need upate to 4.1 ver");
  }

  OZ(check_stack_overflow(is_stack_overflow));
  if (OB_SUCC(ret) && is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive",
             K(ret), K(is_stack_overflow), K(info->ps_sql_), K(info->type_));
  }
  
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("failed eval param value", K(ret));
  } else if (OB_ISNULL(param_buf = alloc.alloc(sizeof(ParamStore)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (FALSE_IT(params = new(param_buf)ParamStore(ObWrapperAllocator(alloc)))) {
  } else if (OB_FAIL(fill_obj_stack(expr, ctx, objs))) {
    LOG_WARN("failed to fill objs", K(ret));
  } else if (OB_FAIL(fill_param_store(objs, expr.arg_cnt_, *params))) {
    LOG_WARN("failed to process in params", K(ret));
  } else {
    pl::ObPLExecCtx pl_exec_ctx(&alloc, &ctx.exec_ctx_, params, nullptr, &ret, nullptr);

    SMART_VAR(ObSPIResultSet, spi_result) {
      OZ (spi_result.init(*session));
      OZ (spi_result.start_nested_stmt_if_need(&pl_exec_ctx, info->route_sql_, static_cast<stmt::StmtType>(info->type_), false));

      if (OB_SUCC(ret)) {
        ObSPIOutParams out_params;
        int64_t old_query_start_time = session->get_query_start_time();
        session->set_query_start_time(ObTimeUtility::current_time());
        bool is_retry = false;
        do {
          if (is_retry) {
            out_params.reset();
            spi_result.reset_member_for_retry(*session);
          }
          retry_ctrl.clear_state_before_each_retry(session->get_retry_info_for_update());
          if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(session->get_effective_tenant_id(),
                                                                  spi_result.get_scheme_guard()))) {
            LOG_WARN("get schema guard failed", K(ret));
          } else if (OB_FAIL(spi_result.get_scheme_guard().get_schema_version(session->get_effective_tenant_id(),
                                                            tenant_version))) {
            LOG_WARN("fail get schema version", K(ret));
          } else if (OB_FAIL(spi_result.get_scheme_guard().get_schema_version(OB_SYS_TENANT_ID, sys_version))) {
            LOG_WARN("fail get sys schema version", K(ret));
          } else {
            retry_ctrl.set_tenant_local_schema_version(tenant_version);
            retry_ctrl.set_sys_local_schema_version(sys_version);
            spi_result.get_sql_ctx().schema_guard_ = &spi_result.get_scheme_guard();
            OZ (ObSPIService::inner_open(&pl_exec_ctx,
                                        expr.arg_cnt_ == 0 ? info->route_sql_.ptr() : nullptr,
                                        info->ps_sql_.ptr(),
                                        info->type_,
                                        *params,
                                        spi_result,
                                        out_params));


            if (OB_FAIL(ret)) {
              LOG_WARN("inner open error", K(ret));
            } else if (OB_FAIL(get_result(spi_result.get_result_set(), result, alloc))) {
              if (OB_ERR_TOO_MANY_ROWS != ret) {
                int cli_ret = OB_SUCCESS;
                retry_ctrl.test_and_save_retry_state(GCTX,
                                                    spi_result.get_sql_ctx(),
                                                    *spi_result.get_result_set(),
                                                    ret, cli_ret, true, true, true);
                ret = cli_ret;
              }
            }
            session->set_session_in_retry(retry_ctrl.need_retry());

            int64_t close_ret = spi_result.get_result_set()->close();
            if (OB_SUCCESS != close_ret) {
              LOG_WARN("close spi result failed", K(ret), K(close_ret));
            }
            ret = OB_SUCCESS == ret ? close_ret : ret;
          }
          is_retry = true;
        } while (observer::RETRY_TYPE_NONE != retry_ctrl.get_retry_type());
        session->get_retry_info_for_update().clear();
        session->set_query_start_time(old_query_start_time);
      }
      spi_result.end_nested_stmt_if_need(&pl_exec_ctx, ret);
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("get result obj failed", K(ret));
  } else {
    if (!info->result_type_.is_ext()
        && (info->result_type_.get_obj_meta() != result.get_meta())) {
      ObObj conv_res;
      OZ (sql::ObSPIService::spi_convert(*session,
                                         alloc,
                                         result,
                                         info->result_type_,
                                         conv_res,
                                         info->is_ignore_fail_));
      if (OB_FAIL(ret)) {
        LOG_WARN("convert type error", K(ret));
      } else {
        result = conv_res;
      }
    }
  }

  if (OB_SUCC(ret)) {
    OZ(res.from_obj(result));
    if (is_lob_storage(result.get_type())) {
      OZ(ob_adjust_lob_datum(result, expr.obj_meta_, ctx.exec_ctx_.get_allocator(), res));
    }
    OZ(expr.deep_copy_datum(ctx, res));
  }

  return ret;
}

int ObExprOpSubQueryInPl::get_result(void *result_set,
                                     ObObj &result,
                                     ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ObNewRow current_row;
  int64_t row_count = 0;

  if (OB_FAIL(fetch_row(result_set, row_count, current_row))) {
      LOG_WARN("read result error", K(ret), K(row_count));
  } else {
    OZ (deep_copy_obj(alloc, current_row.get_cell(0), result));
  }
  /* check result has more row */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fetch_row(result_set, row_count, current_row))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("read result error", K(ret));
    }
  } else {
    ret = OB_ERR_TOO_MANY_ROWS;
  }

  return ret;
}

int ObExprOpSubQueryInPl::fetch_row(void *result_set, int64_t &row_count, ObNewRow &cur_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_set)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Argument passed in is NULL", K(result_set), K(ret));
  } else {
    ObResultSet *ob_result_set = static_cast<ObResultSet*>(result_set);
    const ObNewRow *row = NULL;
    if (OB_FAIL(ob_result_set->get_next_row(row))) {
      //上层判断返回值，这里不打印信息
    } else {
      cur_row = *row;
      ++row_count;
    }
  }

  LOG_DEBUG("spi fetch row", K(cur_row), K(row_count), K(ret));
  return ret;
}

int ObExprOpSubQueryInPl::fill_param_store(const ObObj *objs_stack,
                                           int64_t param_num,
                                           ParamStore& params)
{
  int ret = OB_SUCCESS;
  CK (0 == param_num || OB_NOT_NULL(objs_stack));
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    ObObjParam param;
    param.reset();
    objs_stack[i].copy_value_or_obj(param, true);
    param.set_param_meta();
    OZ (params.push_back(param));
  }

  return ret;
}

int ObExprOpSubQueryInPl::fill_obj_stack(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         ObObj *objs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < expr.arg_cnt_ && OB_SUCC(ret); ++i) {
    ObDatum &param = expr.args_[i]->locate_expr_datum(ctx);
    if (OB_FAIL(param.to_obj(objs[i], expr.args_[i]->obj_meta_))) {
      LOG_WARN("failed to convert obj", K(ret), K(i));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObExprPlSubQueryInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              id_,
              type_,
              route_sql_,
              result_type_,
              is_ignore_fail_,
              ps_sql_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExprPlSubQueryInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              id_,
              type_,
              route_sql_,
              result_type_,
              is_ignore_fail_,
              ps_sql_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprPlSubQueryInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              id_,
              type_,
              route_sql_,
              result_type_,
              is_ignore_fail_,
              ps_sql_);
  return len;
}

int ObExprPlSubQueryInfo::deep_copy(common::ObIAllocator &allocator,
                         const ObExprOperatorType type,
                         ObIExprExtraInfo *&copied_info) const
{
  int ret = common::OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  ObExprPlSubQueryInfo &other = *static_cast<ObExprPlSubQueryInfo *>(copied_info);
  other.type_ = type_;
  other.result_type_ = result_type_;
  other.is_ignore_fail_ = is_ignore_fail_;

  OZ(ob_write_string(allocator, route_sql_, other.route_sql_, true));
  OZ(ob_write_string(allocator, ps_sql_, other.ps_sql_, true));
  return ret;
}

template <typename RE>
int ObExprPlSubQueryInfo::from_raw_expr(RE &raw_expr, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ObPlQueryRefRawExpr &subquery_expr =
    const_cast<ObPlQueryRefRawExpr &> (static_cast<const ObPlQueryRefRawExpr&>(raw_expr));
  
  id_ = common::OB_INVALID_ID;
  type_ = subquery_expr.get_stmt_type();
  result_type_ = subquery_expr.get_result_type();
  is_ignore_fail_ = subquery_expr.is_ignore_fail();

  OZ(ob_write_string(alloc, subquery_expr.get_route_sql(), route_sql_, true));
  OZ(ob_write_string(alloc, subquery_expr.get_route_sql(), ps_sql_, true));
  return ret;
}

} /* sql */
} /* oceanbase */
