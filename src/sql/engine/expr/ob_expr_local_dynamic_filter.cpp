/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_EXE
#include "sql/engine/expr/ob_expr_local_dynamic_filter.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{

int ObExprLocalDynamicFilterContext::init_params(const common::ObIArray<ObDatum> &params)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    hash_set_.reuse();
  } else if (OB_FAIL(hash_set_.create(params.count() * 2, "LocalDynamicFilter", "LocalDynamicFilter"))) {
    LOG_WARN("failed to create hash set", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); i++) {
    if (OB_FAIL(hash_set_.set_refactored(params.at(i).get_uint64()))) {
      LOG_WARN("failed to set hash set", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObExprLocalDynamicFilterContext::is_filtered(const ObDatum &datum, bool &is_filtered)
{
  int ret = OB_SUCCESS;
  is_filtered = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local dynamic filter context is not inited", K(ret));
  } else {
    uint64_t col_value = datum.get_uint64();
    if (OB_FAIL(hash_set_.exist_refactored(col_value))) {
      if (OB_HASH_NOT_EXIST == ret) {
        is_filtered = true;
        ret = OB_SUCCESS;
      } else if (OB_HASH_EXIST == ret) {
        is_filtered = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to check if col value exists", K(ret));
      }
    }
  }
  return ret;
}

int ObExprLocalDynamicFilter::calc_result_typeN(ObExprResType &type, ObExprResType *types,
                                                int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  UNUSED(types);
  UNUSED(param_num);
  type.set_int32();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  return ret;
}

int ObExprLocalDynamicFilter::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                      ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_local_dynamic_filter;
  rt_expr.eval_batch_func_ = eval_local_dynamic_filter_batch;
  return ret;
}

int ObExprLocalDynamicFilter::eval_local_dynamic_filter(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObExprLocalDynamicFilterContext *local_dynamic_filter_ctx = nullptr;
  if (OB_ISNULL(expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (OB_ISNULL(local_dynamic_filter_ctx = static_cast<ObExprLocalDynamicFilterContext *>(
                                                  ctx.exec_ctx_.get_expr_op_ctx(expr.expr_ctx_id_))) ||
             !local_dynamic_filter_ctx->do_local_dynamic_filter()) {
    // not merge range opt, always return 1 means the row is not filtered
    res.set_int(1);
  } else {
    const ObDatum &datum = expr.args_[0]->locate_expr_datum(ctx);
    bool is_filtered = true;
    if (OB_FAIL(local_dynamic_filter_ctx->is_filtered(datum, is_filtered))) {
      LOG_WARN("failed to check if col value exists", K(ret));
    } else {
      res.set_int(is_filtered ? 0 : 1);
    }
    LOG_TRACE("local dynamic filter eval result", K(datum), K(ctx), K(is_filtered));
  }
  return ret;
}

int ObExprLocalDynamicFilter::eval_local_dynamic_filter_batch(const ObExpr &expr, ObEvalCtx &ctx,
  const ObBitVector &skip, const int64_t batch_size)
{
  return OB_NOT_IMPLEMENT;
}

int ObExprLocalDynamicFilter::prepare_storage_white_filter_data(const ObExpr &expr,
                                                                ObDynamicFilterExecutor &dynamic_filter,
                                                                ObEvalCtx &eval_ctx,
                                                                LocalDynamicFilterParams &params,
                                                                bool &is_data_prepared)
{
  int ret = OB_SUCCESS;
  params.reuse();
  is_data_prepared = false;
  const LocalDynamicFilterParams *filter_params = dynamic_filter.get_op().get_local_dynamic_filter_params();
  ObExprLocalDynamicFilterContext *local_dynamic_filter_ctx = nullptr;

  if (OB_ISNULL(local_dynamic_filter_ctx = static_cast<ObExprLocalDynamicFilterContext *>(
                                           eval_ctx.exec_ctx_.get_expr_op_ctx(expr.expr_ctx_id_))) ||
      !local_dynamic_filter_ctx->do_local_dynamic_filter()) {
    dynamic_filter.set_filter_action(DynamicFilterAction::PASS_ALL);
  } else if (OB_NOT_NULL(filter_params) && !filter_params->empty()) {
    if (OB_FAIL(params.assign(*filter_params))) {
      LOG_WARN("failed to assign local dynamic filter params", K(ret));
    } else {
      dynamic_filter.get_filter_node().set_op_type(WHITE_OP_IN);
      dynamic_filter.set_filter_action(DynamicFilterAction::DO_FILTER);
      dynamic_filter.set_filter_val_meta(expr.args_[0]->obj_meta_);
      dynamic_filter.hash_func_ = &pk_increment_hash_func;
      is_data_prepared = true;
    }
  } else {
    dynamic_filter.set_filter_action(DynamicFilterAction::PASS_ALL);
  }
  LOG_TRACE("local dynamic filter prepare filter params", K(dynamic_filter.get_filter_action()), K(expr), KPC(filter_params));
  return ret;
}

int ObExprLocalDynamicFilter::update_storage_white_filter_data(const ObExpr &expr,
                                                               ObDynamicFilterExecutor &dynamic_filter,
                                                               ObEvalCtx &eval_ctx,
                                                               LocalDynamicFilterParams &params,
                                                               bool &is_update)
{
  return OB_NOT_IMPLEMENT;
}

// just use pk increment value as hash val to leverage current logic of dynamic in filter
int ObExprLocalDynamicFilter::pk_increment_hash_func(const common::ObDatum &datum, const uint64_t seed, uint64_t &res)
{
  int ret = OB_SUCCESS;
  UNUSED(seed);
  res = 0;
  if (datum.is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("datum is null", K(ret));
  } else {
    res = datum.get_uint64();
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
