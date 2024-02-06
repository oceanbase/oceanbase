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
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_coalesce.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprCoalesce::ObExprCoalesce(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_COALESCE, N_COALESCE, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprCoalesce::~ObExprCoalesce()
{
}


int ObExprCoalesce::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types,
                                      int64_t param_num,
                                      ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObLengthSemantics default_length_semantics = (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics() : LS_BYTE);
  if (OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null types", K(ret));
  } else if (OB_UNLIKELY(param_num < 1 || (lib::is_oracle_mode() && param_num <= 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no enough param", K(ret), K(param_num));
  } else if (OB_FAIL(aggregate_result_type_for_case(
                       type,
                       types,
                       param_num,
                       type_ctx.get_coll_type(),
                       lib::is_oracle_mode(),
                       default_length_semantics,
                       type_ctx.get_session(),
                       true,
                       true,
                       is_called_in_sql_))) {
    LOG_WARN("failed to agg resul type", K(ret));
  } else {
    const ObSQLSessionInfo *session =
      dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cast basic session to sql session info failed", K(ret));
    } else {
      ObExprOperator::calc_result_flagN(type, types, param_num);
      ObObjType calc_type = enumset_calc_types_[OBJ_TYPE_TO_CLASS[type.get_type()]];
      bool is_expr_integer_type = (ob_is_int_tc(type.get_type()) ||
                             ob_is_uint_tc(type.get_type()));
      bool all_null_type = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
        all_null_type = (types[i].get_type() != ObNullType) ? false : all_null_type;
        if (ob_is_enumset_tc(types[i].get_type())) {
          if (OB_UNLIKELY(ObMaxType == calc_type)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(WARN, "invalid type of parameter ", K(i), K(ret));
          } else {
            types[i].set_calc_type(calc_type);
          }
        } else {
          bool is_arg_integer_type = (ob_is_int_tc(types[i].get_type()) ||
                                 ob_is_uint_tc(types[i].get_type()));
          if ((is_arg_integer_type && is_expr_integer_type) ||
              ObNullType == types[i].get_type()) {
            // 参数是int/uint tc时不能将calc type设置为结果的类型
            // eg: select coalesce(null, col_tinyint, col_mediumint) from t1;
            // 由于merge type对于null的处理，会多次类型推导时，结果类型发生变化
            types[i].set_calc_meta(types[i].get_obj_meta());
            types[i].set_calc_accuracy(types[i].get_accuracy());
          } else {
            types[i].set_calc_meta(type.get_obj_meta());
            types[i].set_calc_accuracy(type.get_accuracy());
          }
        }
      }
      if (!is_called_in_sql() && all_null_type) {
        ret = OB_ERR_COALESCE_AT_LEAST_ONE_NOT_NULL;
        LOG_USER_ERROR(OB_ERR_COALESCE_AT_LEAST_ONE_NOT_NULL);
      }
    }
  }
  return ret;
}

int calc_coalesce_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  res_datum.set_null();
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    ObDatum *child_res = NULL;
    if (OB_FAIL(expr.args_[i]->eval(ctx, child_res))) {
      LOG_WARN("eval arg failed", K(ret), K(i));
    } else if (!(child_res->is_null())) {
      // TODO: @shaoge coalesce的结果可以不用预分配内存，直接使用某个子节点的结果
      res_datum.set_datum(*child_res);
      break;
    }
  }
  return ret;
}

int ObExprCoalesce::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_coalesce_expr;
  uint64_t ob_version = GET_MIN_CLUSTER_VERSION();
  // 4.1.0 & 4.0 observers may run in same cluster, plan with batch func from observer(version4.1.0) may serialized to
  // observer(version4.0.0) to execute, thus batch func is not null only if min_cluster_version>=4.1.0
  if (ob_version >= CLUSTER_VERSION_4_1_0_0) {
    rt_expr.eval_batch_func_ = calc_batch_coalesce_expr;
  }
  return ret;
}

int ObExprCoalesce::calc_batch_coalesce_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                             const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("calculate batch coalesce expr", K(batch_size));

  ObDatum *results = expr.locate_batch_datums(ctx);
  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr results frame is not inited", K(ret));
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObBitVector &my_skip = expr.get_pvt_skip(ctx);
    my_skip.bit_calculate(skip, eval_flags, batch_size,
                          [](uint64_t l, uint64_t r) { return l | r; });
    int64_t skip_cnt = my_skip.accumulate_bit_cnt(batch_size);
    for (int arg_idx = 0; OB_SUCC(ret) && arg_idx < expr.arg_cnt_; arg_idx++) {
      if (skip_cnt >= batch_size) {
        break;
      } else if (OB_FAIL(expr.args_[arg_idx]->eval_batch(ctx, my_skip, batch_size))) {
        LOG_WARN("failed to eval batch results", K(arg_idx), K(ret));
      } else {
        ObDatumVector dv = expr.args_[arg_idx]->locate_expr_datumvector(ctx);
        ObBitVector::flip_foreach(
          my_skip,
          batch_size,
          [&](int64_t idx) __attribute__((always_inline)) {
            if (!dv.at(idx)->is_null()) {
              results[idx].set_datum(*dv.at(idx));
              eval_flags.set(idx);
              my_skip.set(idx);
              skip_cnt++;
            }
            return OB_SUCCESS;
          }
        );
      }
    }
    // if skip_map not set, set corresponding result to null
    if (OB_SUCC(ret)) {
      ObBitVector::flip_foreach(
        my_skip,
        batch_size,
        [&](int64_t idx) __attribute__((always_inline)) {
          results[idx].set_null();
          eval_flags.set(idx);
          return OB_SUCCESS;
        });
    }
  }
  return ret;
}

}
}
